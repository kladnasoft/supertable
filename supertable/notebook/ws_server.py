from __future__ import annotations
from dotenv import load_dotenv
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool

from supertable.notebook.resource_config import HIGH_TIER, LOW_TIER
from supertable.notebook.warm_pool_manager import WarmPoolManager

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the pool on startup (lifespan is the recommended replacement for on_event).
    app.state.pools = {
        "no-internet": WarmPoolManager(pool_size=5, config=LOW_TIER),
        "internet": WarmPoolManager(pool_size=5, config=HIGH_TIER),
    }
    try:
        yield
    finally:
        pools = getattr(app.state, "pools", {})
        for pool in pools.values():
            try:
                pool.shutdown_pool()
            except Exception:
                pass


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/execute")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()

    pools = websocket.app.state.pools  # type: ignore[attr-defined]

    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            code = payload.get("code", "")
            if not isinstance(code, str):
                code = str(code)

            profile = payload.get("profile", "no-internet")
            if not isinstance(profile, str):
                profile = str(profile)
            profile_key = profile.strip().lower()
            if profile_key in {"internet", "high", "on", "true", "1"}:
                pool = pools["internet"]
            else:
                pool = pools["no-internet"]

            await websocket.send_json({"status": "start", "message": "Executing...", "profile": profile_key})

            # iterate_in_threadpool prevents the synchronous generator
            # from blocking the main FastAPI event loop.
            async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
                await websocket.send_json({"status": "output", "data": output_chunk})

            await websocket.send_json({"status": "complete"})

    except WebSocketDisconnect:
        print("Client disconnected")


if __name__ == "__main__":
    import os
    import uvicorn

    def _resolve_port() -> int:
        raw = os.getenv("SUPERTABLE_NOTEBOOK_PORT", "").strip()
        if not raw:
            return 8000
        try:
            port = int(raw)
        except ValueError:
            return 8000
        return port if 1 <= port <= 65535 else 8000

    uvicorn.run(app, host="0.0.0.0", port=_resolve_port())
