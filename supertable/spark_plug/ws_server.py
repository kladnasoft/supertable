import json

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