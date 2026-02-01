from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from supertable.notebook.warm_pool_manager import WarmPoolManager
import json

app = FastAPI()

# Initialize the pool on startup
pool = WarmPoolManager(pool_size=5)


@app.websocket("/ws/execute")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Wait for code from the frontend
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")

            # Stream the results back
            await websocket.send_json({"status": "start", "message": "Executing..."})

            for output_chunk in pool.execute_and_stream(code):
                await websocket.send_json({
                    "status": "output",
                    "data": output_chunk
                })

            await websocket.send_json({"status": "complete"})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)