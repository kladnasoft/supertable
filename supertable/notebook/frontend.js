const socket = new WebSocket('ws://localhost:8000/ws/execute');

socket.onmessage = (event) => {
    const response = JSON.parse(event.data);
    if (response.status === "output") {
        console.log("Python Output:", response.data);
    }
};

// To run code:
socket.send(JSON.stringify({ code: "import time; print('Starting...'); time.sleep(2); print('Done!')" }));