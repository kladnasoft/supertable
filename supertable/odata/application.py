# route: supertable.odata.application
"""
SuperTable OData Server — standalone OData 4.0 feed service.

An independent server that exposes SuperTable data as OData 4.0 feeds.
Authentication is via OData bearer tokens (st_od_*), created through
the API's /reflection/odata/endpoints management endpoints.  Each token
carries an RBAC role_name — all data access is scoped to that role.

Usage:
    python -m supertable.odata.application
    uvicorn supertable.odata.application:app --port 8052

Environment:
    SUPERTABLE_ODATA_HOST       — bind address   (default: 0.0.0.0)
    SUPERTABLE_ODATA_PORT       — listen port     (default: 8052)
    SUPERTABLE_LOG_LEVEL        — DEBUG/INFO/WARNING/ERROR  (default: INFO)
    SUPERTABLE_LOG_FORMAT       — json / text     (default: json)
    SUPERTABLE_LOG_FILE         — optional log file path
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

# ---------------------------------------------------------------------------
# Structured logging — must be configured before any other import logs
# ---------------------------------------------------------------------------
from supertable.config.settings import settings
from supertable.logging import configure_logging, RequestLoggingMiddleware

configure_logging(service="odata")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Instance registration via shared ServiceRegistry
# ---------------------------------------------------------------------------
from supertable.service_registry import ServiceRegistry

_registry = ServiceRegistry("odata", metadata={
    "port": getattr(settings, "SUPERTABLE_ODATA_PORT", 8052),
})


@asynccontextmanager
async def _lifespan(application: FastAPI):
    """Manage service registration heartbeat + startup audit event."""
    port = getattr(settings, "SUPERTABLE_ODATA_PORT", 8052)
    logger.info("OData service starting on port %s", port)

    # Audit: service start
    try:
        from supertable.audit import emit as _audit, EventCategory, Actions, Severity, make_detail
        _audit(
            category=EventCategory.SYSTEM,
            action=Actions.SERVICE_START,
            organization="",
            super_name="",
            resource_type="service",
            resource_id="odata",
            severity=Severity.INFO,
            detail=make_detail(port=port),
        )
    except Exception:
        pass

    # Start registry heartbeat
    await _registry.astart()
    task = asyncio.create_task(_registry.aheartbeat(), name="registry-odata")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        _registry.astop()


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SuperTable OData Service",
    version="1.0.0",
    description="SuperTable OData 4.0 feed server — independent service with bearer token auth",
    lifespan=_lifespan,
)

# CORS — OData clients (Power BI Web, Excel Online, simulators) make
# cross-origin requests.  Auth is via Bearer token, not cookies, so
# allow_origins=["*"] is safe here.
from starlette.middleware.cors import CORSMiddleware  # noqa: E402
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS", "HEAD"],
    allow_headers=["Authorization", "Accept", "Content-Type", "OData-Version", "OData-MaxVersion"],
    expose_headers=["OData-Version", "Content-Type"],
)

app.add_middleware(RequestLoggingMiddleware, service="odata")

# Audit middleware — captures 401/403/500 responses automatically
from supertable.audit.middleware import AuditMiddleware  # noqa: E402
app.add_middleware(AuditMiddleware, server="odata")

# ---------------------------------------------------------------------------
# Mount the OData router
# ---------------------------------------------------------------------------
from supertable.odata.odata_server import router as odata_router  # noqa: E402

app.include_router(odata_router)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = getattr(settings, "SUPERTABLE_ODATA_HOST", "0.0.0.0")
    port = getattr(settings, "SUPERTABLE_ODATA_PORT", 8052)

    uvicorn.run(
        "supertable.odata.application:app",
        host=host,
        port=port,
    )
