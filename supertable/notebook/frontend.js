// Simple standalone WebSocket client for /ws/execute
// Override backend via ?backend=HOST:PORT if you embed this in a page.
const wsProto = (window.location.protocol === 'https:') ? 'wss' : 'ws';
const params = new URLSearchParams(window.location.search);
const backendOverride = params.get('backend');
const hostname = window.location.hostname || 'localhost';
const notebookPort = '8010';
const defaultBackend = (window.location.port === notebookPort)
  ? (window.location.host || `${hostname}:${notebookPort}`)
  : `${hostname}:${notebookPort}`;
const backend = backendOverride || defaultBackend;

const socket = new WebSocket(`${wsProto}://${backend}/ws/execute`);

socket.onopen = () => {
  console.log("WebSocket connected");
  // Example run:
  socket.send(JSON.stringify({ code: "import time; print('Starting...'); time.sleep(2); print('Done!')" }));
};

socket.onerror = (event) => {
  console.error("WebSocket error - check Network tab for the WS handshake", event);
};

socket.onmessage = (event) => {
  const response = JSON.parse(event.data);
  if (response.status === "output") {
    console.log("Python Output:", response.data);
  } else {
    console.log("Server:", response);
  }
};

socket.onclose = () => {
  console.log("WebSocket closed");
};
