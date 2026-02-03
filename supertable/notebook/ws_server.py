from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool
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
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")

            await websocket.send_json({"status": "start", "message": "Executing..."})

            # iterate_in_threadpool prevents the synchronous generator
            # from blocking the main FastAPI event loop
            async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
                await websocket.send_json({
                    "status": "output",
                    "data": output_chunk
                })

            await websocket.send_json({"status": "complete"})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)# ws_server_v20260201_2030_non-blocking-ws.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool
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
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")

            await websocket.send_json({"status": "start", "message": "Executing..."})

            # iterate_in_threadpool prevents the synchronous generator
            # from blocking the main FastAPI event loop
            async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
                await websocket.send_json({
                    "status": "output",
                    "data": output_chunk
                })

            await websocket.send_json({"status": "complete"})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)# ws_server_v20260201_2030_non-blocking-ws.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool
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
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")

            await websocket.send_json({"status": "start", "message": "Executing..."})

            # iterate_in_threadpool prevents the synchronous generator
            # from blocking the main FastAPI event loop
            async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
                await websocket.send_json({
                    "status": "output",
                    "data": output_chunk
                })

            await websocket.send_json({"status": "complete"})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)# ws_server_v20260201_2030_non-blocking-ws.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool
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
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")

            await websocket.send_json({"status": "start", "message": "Executing..."})

            # iterate_in_threadpool prevents the synchronous generator
            # from blocking the main FastAPI event loop
            async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
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