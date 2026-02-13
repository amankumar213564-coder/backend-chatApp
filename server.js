const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const mongoose = require("mongoose");
require("dotenv").config();

const PORT = process.env.PORT || 5000;

/* ------------------ MONGODB CONNECT ------------------ */
mongoose
  .connect(process.env.MONGO_URI)
  .then(() => console.log("MongoDB connected"))
  .catch((err) => console.log(err));

const MessageSchema = new mongoose.Schema({
  id: Number,
  username: String,
  message: String,
  file: String,
  fileType: String,
  type: String,
  time: String,
  clientId: String,
});

const Message = mongoose.model("Message", MessageSchema);

/* ------------------ SERVER ------------------ */
const app = express();
app.get("/", (req, res) => res.send("WebSocket server running"));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let clients = new Set();

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

function sendUsersCount() {
  broadcast({ type: "users", count: clients.size });
}

wss.on("connection", async (ws) => {
  clients.add(ws);
  sendUsersCount();

  /* SEND LAST 50 MESSAGES */
  const history = await Message.find().sort({ _id: -1 }).limit(50);
  ws.send(JSON.stringify({ type: "history", messages: history.reverse() }));

  ws.on("message", async (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    if (data.type === "join") {
      ws.username = data.username;
      return;
    }

    if (
      data.type === "typing" ||
      data.type === "delete" ||
      data.type === "delivered" ||
      data.type === "seen"
    ) {
      broadcast(data);
      return;
    }

    /* SAVE MESSAGE TO DB */
    await Message.create(data);

    data.time = data.time || new Date().toLocaleTimeString();
    broadcast(data);
  });

  ws.on("close", () => {
    clients.delete(ws);
    sendUsersCount();
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
