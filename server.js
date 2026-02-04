// server.js
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
require('dotenv').config(); // Add this at the top



// Use PORT from environment or fallback to 5000
const PORT = process.env.PORT || 5000;

// Create Express app
const app = express();

// Optional: simple route to check server
app.get('/', (req, res) => {
  res.send('WebSocket server is running');
});

// Create HTTP server for Express
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Track connected clients
const clients = new Set();

wss.on('connection', (ws) => {
  console.log('Client connected');
  clients.add(ws);

  // Receive messages from clients
  ws.on('message', (message) => {
    console.log('Received:', message.toString());

    // Broadcast message to all connected clients
    clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message.toString());
      }
    });
  });

  // Handle client disconnect
  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });

  // Send a welcome message
  ws.send('Welcome to WebSocket server!');
});

// Start the server
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
