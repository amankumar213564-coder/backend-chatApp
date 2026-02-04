// server.js
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
require("dotenv").config();

const PORT = process.env.PORT || 5000;

const app = express();

// Simple route to test server
app.get("/", (req, res) => {
  res.send("WebSocket server is running");
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clients = new Set();

wss.on("connection", (ws) => {
  console.log("Client connected");
  clients.add(ws);

  // Send welcome message
  ws.send(JSON.stringify({ message: "Welcome to ChatApp!", sender: "system" }));

  // Broadcast received messages
  ws.on("message", (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      data = { message: msg, sender: "unknown" };
    }

    // Add timestamp
    data.time = new Date().toISOString();

    // Broadcast to all clients
    clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(data));
      }
    });
  });

  ws.on("close", () => {
    console.log("Client disconnected");
    clients.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
