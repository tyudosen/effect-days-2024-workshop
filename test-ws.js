import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:3000');

ws.on('open', () => {
  console.log('WebSocket connected');
  
  // Send startup message
  const startupMsg = {
    _tag: "startup",
    name: "test-user",
    color: "red"
  };
  
  console.log('Sending startup message:', startupMsg);
  ws.send(JSON.stringify(startupMsg));
});

ws.on('message', (data) => {
  console.log('Received message:', data.toString());
});

ws.on('error', (error) => {
  console.log('WebSocket error:', error);
});

ws.on('close', () => {
  console.log('WebSocket closed');
});

// Send a chat message after 2 seconds
setTimeout(() => {
  const chatMsg = {
    _tag: "message",
    message: "Hello from test client!"
  };
  console.log('Sending chat message:', chatMsg);
  ws.send(JSON.stringify(chatMsg));
}, 2000);