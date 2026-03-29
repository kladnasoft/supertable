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

import os

from fastapi import FastAPI

# ---------------------------------------------------------------------------
# Structured logging — must be configured before any other import logs
# ---------------------------------------------------------------------------
from supertable.config.settings import settings
from supertable.logging import configure_logging, RequestLoggingMiddleware

configure_logging(service="api")

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SuperTable API",
    version="1.0.0",
    description="SuperTable Reflection API — JSON endpoints only",
)

app.add_middleware(RequestLoggingMiddleware, service="api")

from supertable.audit.middleware import AuditMiddleware  # noqa: E402
app.add_middleware(AuditMiddleware, server="api")

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
