# route: supertable.audit.middleware
"""
FastAPI middleware for automatic audit event emission.

Captures:
  - Authentication failures (401 responses)
  - Authorization denials (403 responses)
  - Server errors (500+ responses → severity: critical)
  - Service identification (api / webui / mcp)

Does NOT capture:
  - Successful 200 responses for read endpoints (too noisy — handled
    explicitly by endpoint handlers when AUDIT_LOG_READS is enabled)
  - Health checks (/healthz, /health)

Action-specific events (query execution, RBAC changes, etc.) are emitted
by endpoint handlers, not by this middleware. The middleware provides a
safety net for auth/error events that might not have explicit audit calls.

Compliance: DORA Art. 10 (detection), SOC 2 CC6.1 (access logging).
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_SAFE_ERROR_TYPE_RE = re.compile(r"[^A-Za-z0-9_.-]")

# Paths excluded from audit logging (health probes are noise)
_EXCLUDED_PATHS = frozenset({
    "/healthz",
    "/health",
    "/favicon.ico",
    "/static",
})


def _is_excluded(path: str) -> bool:
    """Check if a path should be excluded from audit logging."""
    if path in _EXCLUDED_PATHS:
        return True
    if path.startswith("/static/"):
        return True
    return False


def _extract_org(request: Request) -> str:
    """Best-effort extraction of organization from request context.

    Tries multiple sources in priority order:
    1. request.state (set by session/auth middleware)
    2. Global deployment default from settings

    Query parameters are intentionally excluded: unauthenticated callers must
    not be able to select an audit tenant or initialize arbitrary tenant sinks.
    """
    state = getattr(request, "state", None)
    if state:
        org = getattr(state, "session_org", None)
        if org:
            return str(org)

    try:
        from supertable.config.settings import settings
        return settings.SUPERTABLE_ORGANIZATION or ""
    except Exception:
        return ""


def _safe_error_type(value: str) -> str:
    """Return a bounded diagnostic class name without exception text."""
    normalized = _SAFE_ERROR_TYPE_RE.sub("_", str(value or "ServerError"))[:128]
    return normalized or "ServerError"


def _error_reference(
    request: Request,
    status: int,
    error_type: str,
    start_ms: int,
) -> str:
    """Build a non-secret reference for correlating an error audit event."""
    correlation_id = str(
        getattr(getattr(request, "state", None), "correlation_id", "") or ""
    )
    material = "\x1f".join(
        (
            correlation_id,
            request.method,
            request.url.path,
            str(status),
            error_type,
            str(start_ms),
        )
    )
    return hashlib.sha256(material.encode("utf-8", errors="replace")).hexdigest()[:24]


class AuditMiddleware(BaseHTTPMiddleware):
    """Automatic audit logging for HTTP request/response pairs.

    Install after RequestLoggingMiddleware (so correlation_id is set).

    Usage:
        from supertable.audit.middleware import AuditMiddleware
        app.add_middleware(AuditMiddleware, server="api")
    """

    def __init__(self, app, server: str = "api"):
        super().__init__(app)
        self._server = server

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        if _is_excluded(path):
            return await call_next(request)

        start_ms = int(time.time() * 1000)
        response: Optional[Response] = None

        try:
            response = await call_next(request)
        except Exception as exc:
            # Exception messages may contain credentials, SQL, or user data.  Keep
            # only the bounded class name and a non-secret correlation reference.
            self._emit_error_event(request, 500, type(exc).__name__, start_ms)
            raise

        status = response.status_code if response else 500

        if status == 401:
            self._emit_auth_event(request, status, start_ms)
        elif status == 403:
            self._emit_authz_event(request, status, start_ms)
        elif status >= 500:
            # Never inspect or buffer the response body here.  Besides leaking
            # response details into the audit sink, doing so defeats streaming and
            # creates an unbounded memory amplification path for large responses.
            self._emit_error_event(request, status, "HTTPServerError", start_ms)

        return response

    def _emit_auth_event(self, request: Request, status: int, start_ms: int) -> None:
        """Emit authentication failure event."""
        try:
            from supertable.audit import emit, EventCategory, Actions, Severity, Outcome, make_detail

            org = _extract_org(request)
            if not org:
                return

            emit(
                category=EventCategory.AUTHENTICATION,
                action=Actions.TOKEN_AUTH_FAILURE,
                organization=org,
                actor_ip=request.client.host if request.client else "",
                actor_user_agent=(request.headers.get("user-agent") or "")[:256],
                correlation_id=getattr(getattr(request, "state", None), "correlation_id", ""),
                resource_type="endpoint",
                resource_id=request.url.path,
                detail=make_detail(
                    method=request.method,
                    path=request.url.path,
                    status=status,
                ),
                outcome=Outcome.FAILURE,
                reason="authentication_failed",
                severity=Severity.WARNING,
                server=self._server,
            )
        except Exception as exc:
            logger.debug(
                "[audit-middleware] auth event emit failed: %s",
                type(exc).__name__,
            )

    def _emit_authz_event(self, request: Request, status: int, start_ms: int) -> None:
        """Emit authorization denial event."""
        try:
            from supertable.audit import emit, EventCategory, Actions, Severity, Outcome, make_detail

            org = _extract_org(request)
            if not org:
                return

            emit(
                category=EventCategory.AUTHORIZATION,
                action=Actions.ACCESS_DENIED,
                organization=org,
                actor_ip=request.client.host if request.client else "",
                actor_user_agent=(request.headers.get("user-agent") or "")[:256],
                actor_username=getattr(getattr(request, "state", None), "session_username", ""),
                actor_id=getattr(getattr(request, "state", None), "session_user_hash", ""),
                correlation_id=getattr(getattr(request, "state", None), "correlation_id", ""),
                resource_type="endpoint",
                resource_id=request.url.path,
                detail=make_detail(
                    method=request.method,
                    path=request.url.path,
                    status=status,
                ),
                outcome=Outcome.DENIED,
                reason="authorization_denied",
                severity=Severity.WARNING,
                server=self._server,
            )
        except Exception as exc:
            logger.debug(
                "[audit-middleware] authz event emit failed: %s",
                type(exc).__name__,
            )

    def _emit_error_event(self, request: Request, status: int, error_type: str, start_ms: int) -> None:
        """Emit server error event."""
        try:
            from supertable.audit import emit, EventCategory, Actions, Severity, Outcome, make_detail

            org = _extract_org(request)
            if not org:
                return

            duration_ms = int(time.time() * 1000) - start_ms
            safe_error_type = _safe_error_type(error_type)

            emit(
                category=EventCategory.SYSTEM,
                action=Actions.HEALTH_CHECK_FAILURE,
                organization=org,
                actor_ip=request.client.host if request.client else "",
                actor_user_agent=(request.headers.get("user-agent") or "")[:256],
                actor_username=getattr(getattr(request, "state", None), "session_username", ""),
                correlation_id=getattr(getattr(request, "state", None), "correlation_id", ""),
                resource_type="endpoint",
                resource_id=request.url.path,
                detail=make_detail(
                    method=request.method,
                    path=request.url.path,
                    status=status,
                    error_type=safe_error_type,
                    error_ref=_error_reference(
                        request,
                        status,
                        safe_error_type,
                        start_ms,
                    ),
                    duration_ms=duration_ms,
                ),
                outcome=Outcome.FAILURE,
                reason=f"server_error_{status}",
                severity=Severity.CRITICAL,
                server=self._server,
            )
        except Exception as exc:
            logger.debug(
                "[audit-middleware] error event emit failed: %s",
                type(exc).__name__,
            )
