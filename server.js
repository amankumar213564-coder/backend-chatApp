const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const mongoose = require("mongoose");
const cors = require("cors");
require("dotenv").config();

const PORT = process.env.PORT || 5000;

/* ==================== MONGODB CONNECT ==================== */
mongoose
  .connect(process.env.MONGO_URI)
  .then(() => console.log("✓ MongoDB connected"))
  .catch((err) => console.error("✗ MongoDB error:", err));

/* ==================== MESSAGE SCHEMA ==================== */
const MessageSchema = new mongoose.Schema(
  {
    id: { type: Number, required: true, unique: true },
    username: { type: String, required: true },
    message: String,
    file: String,
    fileType: String,
    type: { type: String, required: true }, // "message" or "file"
    time: String,
    clientId: String,
    status: { type: String, default: "sent" }, // sent, delivered, seen
  },
  { timestamps: true },
);

const Message = mongoose.model("Message", MessageSchema);

/* ==================== EXPRESS SETUP ==================== */
const app = express();
app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.json({
    status: "WebSocket server running",
    timestamp: new Date().toISOString(),
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", clients: clients.size });
});

const server = http.createServer(app);

/* ==================== WEBSOCKET SETUP ==================== */
const wss = new WebSocket.Server({
  server,
  perMessageDeflate: false,
  clientTracking: true,
});

let clients = new Set();
const clientMap = new Map(); // Map to store clientId -> ws connection

/* ==================== BROADCAST FUNCTION ==================== */
function broadcast(data, excludeClientId = null) {
  const msg = JSON.stringify(data);
  let count = 0;

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      // Don't send to the sender for some message types
      if (excludeClientId && client.clientId === excludeClientId) {
        return;
      }
      client.send(msg);
      count++;
    }
  });

  console.log(`📤 Broadcast sent to ${count} clients:`, data.type);
}

/* ==================== SEND USERS COUNT ==================== */
function sendUsersCount() {
  const activeUsers = Array.from(wss.clients).filter(
    (ws) => ws.readyState === WebSocket.OPEN,
  ).length;

  broadcast({
    type: "users",
    count: activeUsers,
  });

  console.log(`👥 Active users: ${activeUsers}`);
}

/* ==================== WEBSOCKET CONNECTION ==================== */
wss.on("connection", async (ws) => {
  console.log("🔗 New client connected");

  // Assign a unique ID to each connection
  const connectionId = Math.random().toString(36).slice(2);
  ws.connectionId = connectionId;

  clients.add(ws);
  sendUsersCount();

  /* ---- SEND LAST 50 MESSAGES FROM DB ---- */
  try {
    const history = await Message.find()
      .sort({ _id: -1 })
      .limit(50)
      .lean()
      .exec();

    if (history.length > 0) {
      ws.send(
        JSON.stringify({
          type: "history",
          messages: history.reverse(),
        }),
      );
      console.log(`📨 Sent ${history.length} message history to client`);
    }
  } catch (err) {
    console.error("Error fetching history:", err);
  }

  /* ---- HANDLE INCOMING MESSAGES ---- */
  ws.on("message", async (message) => {
    try {
      let data;

      try {
        data = JSON.parse(message);
      } catch (err) {
        console.error("JSON parse error:", err);
        return;
      }

      console.log(`📨 Received message type: ${data.type}`);

      /* ---- JOIN MESSAGE ---- */
      if (data.type === "join") {
        ws.username = data.username;
        ws.clientId = data.clientId;
        clientMap.set(data.clientId, ws);
        console.log(`✓ ${data.username} joined (${data.clientId})`);
        sendUsersCount();
        return;
      }

      /* ---- TYPING INDICATOR ---- */
      if (data.type === "typing") {
        broadcast(data);
        return;
      }

      /* ---- DELIVERY RECEIPT ---- */
      if (data.type === "delivered") {
        broadcast(data);
        try {
          await Message.updateOne({ id: data.id }, { status: "delivered" });
        } catch (err) {
          console.error("Error updating delivery status:", err);
        }
        return;
      }

      /* ---- SEEN RECEIPT ---- */
      if (data.type === "seen") {
        broadcast(data);
        try {
          await Message.updateOne({ id: data.id }, { status: "seen" });
        } catch (err) {
          console.error("Error updating seen status:", err);
        }
        return;
      }

      /* ---- MESSAGE OR FILE ---- */
      if (data.type === "message" || data.type === "file") {
        // Ensure required fields
        if (!data.id || !data.username || !data.clientId) {
          console.error("Missing required fields in message:", data);
          return;
        }

        // Validate message type
        if (data.type === "message" && !data.message) {
          console.error("Message content is empty");
          return;
        }

        if (data.type === "file" && !data.file) {
          console.error("File content is empty");
          return;
        }

        try {
          // Save to database
          const messageDoc = new Message({
            id: data.id,
            username: data.username,
            message: data.message || null,
            file: data.file || null,
            fileType: data.fileType || null,
            type: data.type,
            time: data.time || new Date().toLocaleTimeString(),
            clientId: data.clientId,
            status: "sent",
          });

          await messageDoc.save();
          console.log(`💾 Message saved to DB (ID: ${data.id})`);

          // Broadcast to all clients
          broadcast({
            ...data,
            status: "sent",
          });

          console.log(
            `📤 Message broadcasted: ${data.type} from ${data.username}`,
          );
        } catch (err) {
          console.error("Error saving message:", err);

          // If duplicate ID, just broadcast (message might already be saved)
          if (err.code === 11000) {
            console.log("Duplicate message ID, broadcasting anyway");
            broadcast({
              ...data,
              status: "sent",
            });
          }
        }

        return;
      }

      console.log(`⚠️ Unknown message type: ${data.type}`);
    } catch (err) {
      console.error("Error processing message:", err);
    }
  });

  /* ---- CLIENT DISCONNECT ---- */
  ws.on("close", () => {
    console.log(
      `❌ Client disconnected: ${ws.username || "Unknown"} (${ws.connectionId})`,
    );
    clients.delete(ws);

    if (ws.clientId) {
      clientMap.delete(ws.clientId);
    }

    sendUsersCount();
  });

  /* ---- ERROR HANDLING ---- */
  ws.on("error", (err) => {
    console.error(`❌ WebSocket error for ${ws.username}:`, err.message);
  });
});

/* ==================== SERVER STARTUP ==================== */
server.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════╗
║   🚀 Chat Server Running               ║
║   PORT: ${PORT}                           ║
║   WebSocket: Ready                     ║
║   MongoDB: Connected                   ║
╚════════════════════════════════════════╝
  `);
});

/* ==================== GRACEFUL SHUTDOWN ==================== */
process.on("SIGTERM", () => {
  console.log("SIGTERM received, shutting down gracefully...");
  wss.clients.forEach((ws) => {
    ws.close(1000, "Server shutting down");
  });
  server.close(() => {
    console.log("Server closed");
    process.exit(0);
  });
});
