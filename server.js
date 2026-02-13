const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const mongoose = require("mongoose");
const cors = require("cors");
require("dotenv").config();

const PORT = process.env.PORT || 8000;

console.log("Starting server...");
console.log("MONGO_URI:", process.env.MONGO_URI ? "✓ Found" : "✗ Missing");

if (!process.env.MONGO_URI) {
  console.error(`
❌ ERROR: MONGO_URI not found in .env
Create .env file with:
PORT=8000
MONGO_URI=mongodb+srv://chatapp:password@cluster0.xxx.mongodb.net/chatapp?retryWrites=true&w=majority
`);
  process.exit(1);
}

/* ==================== MONGODB CONNECTION ==================== */
const connectDB = async () => {
  try {
    console.log("\n🔄 Connecting to MongoDB...");

    await mongoose.connect(process.env.MONGO_URI);

    console.log("✅ MongoDB connected!\n");
    return true;
  } catch (error) {
    console.error("❌ MongoDB connection failed:", error.message);
    console.error("\nRetrying in 5 seconds...");
    await new Promise((resolve) => setTimeout(resolve, 5000));
    return connectDB();
  }
};

/* ==================== MESSAGE SCHEMA ==================== */
const messageSchema = new mongoose.Schema(
  {
    id: {
      type: Number,
      required: true,
      unique: true,
      sparse: true,
    },
    username: {
      type: String,
      required: true,
    },
    message: String,
    file: String,
    fileType: String,
    fileName: String,
    type: {
      type: String,
      enum: ["message", "file"],
      required: true,
    },
    time: String,
    clientId: String,
    status: {
      type: String,
      default: "sent",
    },
  },
  { timestamps: true },
);

const Message = mongoose.model("Message", messageSchema);

/* ==================== EXPRESS SETUP ==================== */
const app = express();
app.use(cors());
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

app.get("/", (req, res) => {
  res.json({
    status: "✅ Server running",
    mongodb:
      mongoose.connection.readyState === 1 ? "connected" : "disconnected",
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    mongodb:
      mongoose.connection.readyState === 1 ? "✅ connected" : "❌ disconnected",
    clients: wss.clients.size,
  });
});

// 🔥 DEBUG: Check database
app.get("/api/messages", async (req, res) => {
  try {
    const count = await Message.countDocuments();
    const messages = await Message.find().sort({ _id: -1 }).limit(20);
    res.json({
      total: count,
      count: messages.length,
      messages: messages,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const server = http.createServer(app);

/* ==================== WEBSOCKET ==================== */
const wss = new WebSocket.Server({ server, perMessageDeflate: false });

let clientCount = 0;

wss.on("connection", async (ws) => {
  clientCount++;
  console.log(`\n🔗 Client ${clientCount} connected`);

  // Send message history
  try {
    const messages = await Message.find()
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();
    console.log(`📨 Sending ${messages.length} messages to client`);

    ws.send(
      JSON.stringify({
        type: "history",
        messages: messages.reverse(),
      }),
    );
  } catch (err) {
    console.error("Error fetching history:", err.message);
  }

  // Broadcast user count
  const userCount = wss.clients.size;
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: "users", count: userCount }));
    }
  });

  ws.on("message", async (msg) => {
    try {
      const data = JSON.parse(msg);
      console.log(`\n📨 Received ${data.type}: from ${data.username}`);

      // Join
      if (data.type === "join") {
        ws.username = data.username;
        ws.clientId = data.clientId;
        console.log(`✅ ${data.username} joined`);
        return;
      }

      // Typing
      if (data.type === "typing") {
        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN && client !== ws) {
            client.send(JSON.stringify(data));
          }
        });
        return;
      }

      // Delivered/Seen
      if (data.type === "delivered" || data.type === "seen") {
        console.log(`${data.type} for message ${data.id}`);
        try {
          await Message.findOneAndUpdate(
            { id: data.id },
            { status: data.type },
          );
        } catch (err) {
          console.error("Error updating status:", err.message);
        }

        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
          }
        });
        return;
      }

      // Message or File
      if (data.type === "message" || data.type === "file") {
        console.log(`💾 SAVING ${data.type}...`);

        const messageData = {
          id: data.id,
          username: data.username,
          type: data.type,
          time: data.time,
          clientId: data.clientId,
          status: "sent",
        };

        if (data.type === "message") {
          messageData.message = data.message;
        } else {
          messageData.file = data.file;
          messageData.fileType = data.fileType;
          messageData.fileName = data.fileName;
        }

        try {
          // Check if already exists
          const existing = await Message.findOne({ id: data.id });

          if (existing) {
            console.log(`ℹ️  Message ${data.id} already saved`);
          } else {
            const newMessage = await Message.create(messageData);
            console.log(`✅ SAVED! Message ID: ${data.id}`);
          }

          // Broadcast to all clients
          wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
              client.send(
                JSON.stringify({
                  ...data,
                  status: "sent",
                }),
              );
            }
          });

          console.log(`📤 Broadcasted to ${wss.clients.size} clients`);
        } catch (error) {
          if (error.code === 11000) {
            console.log("⚠️  Duplicate ID, broadcasting anyway");
            wss.clients.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(data));
              }
            });
          } else {
            console.error("❌ ERROR SAVING:", error.message);
          }
        }
        return;
      }
    } catch (err) {
      console.error("Error processing message:", err.message);
    }
  });

  ws.on("close", () => {
    clientCount--;
    console.log(`❌ Client disconnected (${wss.clients.size} remaining)\n`);

    // Broadcast updated user count
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: "users", count: wss.clients.size }));
      }
    });
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
  });
});

/* ==================== START SERVER ==================== */
const startServer = async () => {
  await connectDB();

  server.listen(PORT, () => {
    console.log(`
╔════════════════════════════════════════╗
║   🚀 CHAT SERVER RUNNING               ║
║   Port: ${PORT}                            ║
║   WebSocket: ws://localhost:${PORT}     ║
║   MongoDB: ✅ CONNECTED                ║
╚════════════════════════════════════════╝

📝 Test:
   GET http://localhost:${PORT}/health
   GET http://localhost:${PORT}/api/messages

    `);
  });
};

startServer().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});

// Handle errors
process.on("unhandledRejection", (err) => {
  console.error("Unhandled Rejection:", err);
});

mongoose.connection.on("error", (err) => {
  console.error("MongoDB Error:", err.message);
});
