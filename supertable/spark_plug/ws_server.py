import json
import uvicorn  # 1. Add this import
from starlette.websockets import WebSocket
from supertable.api.application import app
from supertable.spark_plug.spark_manager import SparkClusterManager

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
                msg = spark_manager.start_cluster()
                await websocket.send_json({"status": "ready", "message": msg})

            elif action == "EXECUTE":
                code = payload.get("code")
                result = spark_manager.execute_pyspark(code)
                await websocket.send_json({"status": "output", "data": result})
    except Exception as e:
        print(f"Error: {e}")

# 2. Add this block at the end of the file
if __name__ == "__main__":
    uvicorn.run(
        "ws_server:app", # Import string: "filename:instance_name"
        host="0.0.0.0",   # Makes it accessible on your local network
        port=8000,        # Standard port for notebooks
        reload=True       # Auto-restarts when you save changes (development only)
    )