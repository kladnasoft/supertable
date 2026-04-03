# route: supertable.api.application
"""
SuperTable API Server — standalone JSON API service.

Serves all 82 JSON endpoints (RBAC, execute, ingestion, monitoring,
quality, tables, tokens, env, OData management).  No HTML templates,
no static files, no login forms.

Usage:
    python -m supertable.api.application
    uvicorn supertable.api.application:app --port 8051

Environment:
    SUPERTABLE_API_HOST       — bind address   (default: 0.0.0.0)
    SUPERTABLE_API_PORT       — listen port    (default: 8051)
    SUPERTABLE_LOG_LEVEL      — DEBUG/INFO/WARNING/ERROR  (default: INFO)
    SUPERTABLE_LOG_FORMAT     — json / text    (default: json)
    SUPERTABLE_LOG_FILE       — optional log file path
    UVICORN_RELOAD            — hot reload     (default: 0)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI

# ---------------------------------------------------------------------------
# Structured logging — must be configured before any other import logs
# ---------------------------------------------------------------------------
from supertable.config.settings import settings
from supertable.logging import configure_logging, RequestLoggingMiddleware

configure_logging(service="api")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Instance identity — computed once at module load, stable for the lifetime
# of this process.  Used by the heartbeat and injected into monitoring payloads.
# ---------------------------------------------------------------------------
_INSTANCE_ID: str = uuid.uuid4().hex[:12]
_INSTANCE_HOSTNAME: str = socket.gethostname()
_INSTANCE_PID: int = os.getpid()
_INSTANCE_KEY: str = f"supertable:instances:api:{_INSTANCE_HOSTNAME}:{_INSTANCE_PID}"
_INSTANCE_TTL_S: int = 30          # Redis key TTL — expires after 2 missed heartbeats
_HEARTBEAT_INTERVAL_S: int = 15    # Refresh interval


def _instance_payload() -> str:
    """JSON string written to Redis on every heartbeat."""
    return json.dumps({
        "instance_id": _INSTANCE_ID,
        "hostname": _INSTANCE_HOSTNAME,
        "pid": _INSTANCE_PID,
        "started_at": _STARTED_AT,
        "version": "1.0.0",
    })


_STARTED_AT: float = time.time()


async def _heartbeat_task() -> None:
    """
    Asyncio background task — refreshes this instance's Redis key every
    _HEARTBEAT_INTERVAL_S seconds.  Runs for the entire lifetime of the
    process.  If Redis is unavailable for a cycle, logs a warning and
    retries on the next interval.
    """
    from supertable.server_common import redis_client as _rc  # noqa: import inside task

    payload = _instance_payload()
    while True:
        try:
            _rc.set(_INSTANCE_KEY, payload, ex=_INSTANCE_TTL_S)
        except Exception as exc:
            logger.warning("[heartbeat] failed to refresh instance key: %s", exc)
        await asyncio.sleep(_HEARTBEAT_INTERVAL_S)


def _deregister_instance() -> None:
    """Delete the instance key on clean shutdown so it disappears immediately."""
    try:
        from supertable.server_common import redis_client as _rc
        _rc.delete(_INSTANCE_KEY)
        logger.info("[heartbeat] instance deregistered: %s", _INSTANCE_KEY)
    except Exception as exc:
        logger.warning("[heartbeat] deregister failed (non-fatal): %s", exc)


@asynccontextmanager
async def _lifespan(application: FastAPI):
    """Manage the instance heartbeat background task."""
    task = asyncio.create_task(_heartbeat_task(), name="instance-heartbeat")
    logger.info(
        "[heartbeat] instance registered: id=%s host=%s pid=%d",
        _INSTANCE_ID, _INSTANCE_HOSTNAME, _INSTANCE_PID,
    )
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        _deregister_instance()


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SuperTable API",
    version="1.0.0",
    description="SuperTable Reflection API — JSON endpoints only",
    lifespan=_lifespan,
)

app.add_middleware(RequestLoggingMiddleware, service="api")

from supertable.audit.middleware import AuditMiddleware  # noqa: E402
app.add_middleware(AuditMiddleware, server="api")

# Rate limiting (opt-in via SUPERTABLE_API_RATE_LIMIT_ENABLED)
if settings.SUPERTABLE_API_RATE_LIMIT_ENABLED:
    from supertable.services.rate_limit import RateLimitMiddleware  # noqa: E402
    app.add_middleware(RateLimitMiddleware, rpm=settings.SUPERTABLE_API_RATE_LIMIT_RPM)

# ---------------------------------------------------------------------------
# Import server_common first (infrastructure: Settings, Redis, Catalog, router,
# session helpers, auth guards, etc.)
# ---------------------------------------------------------------------------
from supertable.server_common import router  # noqa: E402

# ---------------------------------------------------------------------------
# Import api.py to register ALL 82 JSON endpoints on the shared router.
# This import has side effects — every @router.* decorator in api.py fires
# and attaches the endpoint to the router instance created in server_common.py.
# ---------------------------------------------------------------------------
from supertable.api import api as _api  # noqa: F401, E402

# ---------------------------------------------------------------------------
# Mount the router with all registered endpoints onto the app.
# ---------------------------------------------------------------------------
app.include_router(router)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = settings.effective_api_host
    port = settings.SUPERTABLE_API_PORT
    reload_flag = settings.UVICORN_RELOAD

    uvicorn.run(
        "supertable.api.application:app",
        host=host,
        port=port,
        reload=reload_flag,
    )
