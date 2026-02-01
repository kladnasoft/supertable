import json
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from starlette.websockets import WebSocket, WebSocketDisconnect
from fastapi import FastAPI
from spark_manager import SparkClusterManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic can go here
    yield
    # Shutdown logic: Ensure Spark stops when the server stops
    spark_manager.stop_cluster()


app = FastAPI(lifespan=lifespan)
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
                # Stream the connection progress to the frontend
                # Using a wrapper to run the generator in a thread if it blocks,
                # though Spark init is generally okay here.
                for step_message in spark_manager.start_cluster_generator():
                    await websocket.send_json({
                        "status": "progress",
                        "message": step_message
                    })
                await websocket.send_json({"status": "ready"})

            elif action == "EXECUTE":
                code = payload.get("code")
                await websocket.send_json({"status": "running"})

                # Execute in a threadpool to avoid blocking the event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, spark_manager.execute_pyspark, code)

                await websocket.send_json({"status": "result", "data": result})

    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    uvicorn.run(
        "ws_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )