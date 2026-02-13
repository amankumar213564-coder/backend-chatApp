const express = require("express");
const http = require("http");
const WebSocket = require("ws");
require("dotenv").config();

const PORT = process.env.PORT || 5000;

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

wss.on("connection", (ws) => {
  clients.add(ws);
  sendUsersCount();

  ws.on("message", (message) => {
    let data;

    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    /* JOIN EVENT */
    if (data.type === "join") {
      ws.username = data.username;
      return;
    }

    /* TYPING */
    if (data.type === "typing") {
      broadcast(data);
      return;
    }

    /* DELETE MESSAGE */
    if (data.type === "delete") {
      broadcast(data);
      return;
    }

    /* DELIVERY RECEIPTS */
    if (data.type === "delivered" || data.type === "seen") {
      broadcast(data);
      return;
    }

    /* MESSAGE OR FILE */
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
