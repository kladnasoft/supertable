from __future__ import annotations

from dotenv import load_dotenv
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.concurrency import iterate_in_threadpool

from supertable.notebook.resource_config import HIGH_TIER, LOW_TIER
from supertable.notebook.warm_pool_manager import WarmPoolManager

load_dotenv()

# Enable stateful websocket execution by default.
# Set SUPERTABLE_NOTEBOOK_STATEFUL_WS=0 to fall back to legacy one-shot execution.
STATEFUL_WS_ENABLED = os.getenv("SUPERTABLE_NOTEBOOK_STATEFUL_WS", "1").strip() != "0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize pools on startup.
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

    # If the client does not provide a stable session_id, we keep a default
    # per WebSocket connection.
    default_session_id = f"ws_{uuid.uuid4().hex}"

    def _pick_pool(profile_key: str):
        if profile_key in {"internet", "high", "on", "true", "1"}:
            return pools["internet"], "internet"
        return pools["no-internet"], "no-internet"

    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data) if data else {}
            if not isinstance(payload, dict):
                payload = {}

            op = str(payload.get("op") or "").strip().lower()

            code = payload.get("code", "")
            if not isinstance(code, str):
                code = str(code)

            profile = payload.get("profile", "no-internet")
            if not isinstance(profile, str):
                profile = str(profile)
            profile_key = profile.strip().lower()

            session_id = payload.get("session_id") or payload.get("session") or default_session_id
            if not isinstance(session_id, str):
                session_id = str(session_id)
            session_id = session_id.strip() or default_session_id

            pool, profile_norm = _pick_pool(profile_key)

            # Control: reset/stop
            if op in {"reset", "stop"}:
                for k in ("internet", "no-internet"):
                    try:
                        pools[k].reset_session(session_id)
                    except Exception:
                        pass
                await websocket.send_json(
                    {"status": "complete", "reset": True, "session_id": session_id, "profile": profile_norm}
                )
                continue

            await websocket.send_json(
                {"status": "start", "message": "Executing...", "profile": profile_norm, "session_id": session_id}
            )

            if STATEFUL_WS_ENABLED:
                async for output_chunk in iterate_in_threadpool(pool.execute_in_session_and_stream(session_id, code)):
                    await websocket.send_json({"status": "output", "data": output_chunk})
            else:
                async for output_chunk in iterate_in_threadpool(pool.execute_and_stream(code)):
                    await websocket.send_json({"status": "output", "data": output_chunk})

            await websocket.send_json({"status": "complete", "session_id": session_id})

    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        try:
            await websocket.send_json({"status": "error", "error": str(e)})
        except Exception:
            pass


if __name__ == "__main__":
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
