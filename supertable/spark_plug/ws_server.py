import json
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from spark_manager import SparkClusterManager

app = FastAPI()
spark_manager = SparkClusterManager()


@app.websocket("/ws/spark")
async def spark_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            action = payload.get("action")

            if action == "START":
                # Stream connection progress to the frontend
                for step_message in spark_manager.start_cluster_generator():
                    await websocket.send_json({
                        "status": "progress",
                        "message": step_message
                    })
                await websocket.send_json({"status": "ready"})

            elif action == "EXECUTE":
                code = payload.get("code")
                await websocket.send_json({"status": "running"})
                result = spark_manager.execute_pyspark(code)
                await websocket.send_json({"status": "result", "data": result})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    uvicorn.run("ws_server:app", host="0.0.0.0", port=8000, reload=True)