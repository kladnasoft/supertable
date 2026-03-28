# route: supertable.logging
"""
Structured logging for SuperTable API and UI servers.

Provides:
  - JSON-formatted log records for machine parsing (jq, ELK, Datadog, Loki)
  - Request/response timing middleware with correlation IDs
  - Correlation ID propagation across proxy hops (UI → API)
  - Clean human-readable fallback for development

Usage in application.py:

    from supertable.logging import configure_logging, RequestLoggingMiddleware

    configure_logging(service="api")          # or service="ui"
    app.add_middleware(RequestLoggingMiddleware, service="api")

Environment:
    SUPERTABLE_HOME           — root directory (default: ~/supertable)
    SUPERTABLE_LOG_LEVEL      — DEBUG / INFO / WARNING / ERROR  (default: INFO)
    SUPERTABLE_LOG_FORMAT     — json / text                     (default: json)
    SUPERTABLE_LOG_FILE       — file path (default: {SUPERTABLE_HOME}/log/st.log)
                                 Set to "none" to disable file logging.
    SUPERTABLE_CORRELATION_HEADER — header name for correlation ID
                                   (default: X-Correlation-ID)

Analysis examples (default log at ~/supertable/log/st.log):
    # All requests slower than 500ms
    cat ~/supertable/log/st.log | jq 'select(.duration_ms > 500)'

    # All 5xx responses
    cat ~/supertable/log/st.log | jq 'select(.status >= 500)'

    # All proxy errors
    cat ~/supertable/log/st.log | jq 'select(.event == "proxy_error")'

    # Request rate by endpoint (last hour)
    cat ~/supertable/log/st.log | jq -r 'select(.event == "request") | .path' | sort | uniq -c | sort -rn

    # Slowest endpoints (p95)
    cat ~/supertable/log/st.log | jq 'select(.event == "request")' | jq -s 'group_by(.path) | map({path: .[0].path, count: length, p95_ms: (sort_by(.duration_ms) | .[length * 95 / 100].duration_ms)})'

    # Trace a single request across UI and API
    cat ~/supertable/log/st.log | jq 'select(.correlation_id == "abc123")' | jq -s 'sort_by(.timestamp)'
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CORRELATION_HEADER = os.getenv("SUPERTABLE_CORRELATION_HEADER", "X-Correlation-ID")

# Fields to suppress from JSON output (security)
_REDACT_HEADERS = frozenset({
    "authorization", "cookie", "set-cookie",
    "x-api-key", "x-auth-token",
})

# Static paths to skip logging (noise reduction)
_SKIP_PATHS = frozenset({
    "/static/", "/favicon.ico", "/healthz",
})


# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------

class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line.

    Fields:
      timestamp     — ISO 8601 UTC
      level         — DEBUG / INFO / WARNING / ERROR / CRITICAL
      service       — api / ui (set at configure_logging time)
      logger        — Python logger name
      message       — log message
      event         — structured event type (request, proxy_error, startup, etc.)
      correlation_id — request correlation ID (if present)
      + any extra fields passed via `extra={"event": ..., "duration_ms": ...}`
    """

    def __init__(self, service: str = "app"):
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        doc: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Pull structured fields from `extra`
        for key in (
            "event", "correlation_id", "method", "path", "status",
            "duration_ms", "client_ip", "content_length",
            "proxy_target", "proxy_status", "error",
            "user", "org", "sup", "role", "proxied",
        ):
            val = getattr(record, key, None)
            if val is not None:
                doc[key] = val

        # Exception info
        if record.exc_info and record.exc_info[1]:
            doc["exception"] = self.formatException(record.exc_info)

        return json.dumps(doc, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Human-readable Formatter (for development)
# ---------------------------------------------------------------------------

class TextFormatter(logging.Formatter):
    """Compact single-line format for terminal use with color coding.

    Request events (white — normal):
      14:43:07.229  INFO  [api]  GET /reflection/supers → 200  35.2ms  cid=6488eafe78f9

    Warnings (yellow):
      14:43:11.422  WARN  [ui]   GET /reflection/compute/list → 404  12.8ms  cid=6f0c52bb45d5

    Errors (red):
      14:43:07.230  ERRO  [ui]   proxy GET /reflection/supers → FAILED  connection refused

    Internal app messages (dim grey):
      14:43:07.001  INFO  [api]  [duckdb.lite] persistent connection created

    Colors are auto-disabled when stdout is not a TTY (piped/redirected)
    or when NO_COLOR env var is set (https://no-color.org/).
    """

    # ANSI escape codes
    _RESET = "\033[0m"
    _DIM = "\033[90m"         # bright-black / dark grey for internal messages
    _CYAN = "\033[36m"        # proxied API calls
    _YELLOW = "\033[33m"      # warnings
    _RED = "\033[31m"         # errors
    _BOLD_RED = "\033[1;31m"  # critical

    def __init__(self, service: str = "app"):
        super().__init__()
        self._service = service
        # Color: enabled by default. Disabled by NO_COLOR env or SUPERTABLE_LOG_COLOR=0.
        # IDEs (PyCharm, VS Code) support ANSI but don't report isatty() — so we
        # default to ON and let users opt out explicitly.
        self._color = not (
            os.getenv("NO_COLOR") is not None
            or os.getenv("SUPERTABLE_LOG_COLOR", "").strip().lower() in ("0", "false", "no", "off")
        )

    def _wrap(self, color: str, text: str) -> str:
        if not self._color:
            return text
        return f"{color}{text}{self._RESET}"

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        level = record.levelname[:4].ljust(4)
        prefix = f"{ts}  {level}  [{self._service}]"
        event = getattr(record, "event", None)

        # ── Request event — compact fixed layout ──────────────────
        if event == "request":
            method = getattr(record, "method", "")
            path = getattr(record, "path", "")
            status = getattr(record, "status", "")
            duration = getattr(record, "duration_ms", "")
            cid = getattr(record, "correlation_id", "")
            proxied = getattr(record, "proxied", False)

            tag = "→api " if proxied else ""
            line = f"{prefix}  {tag}{method} {path} → {status}  {duration}ms"
            if cid:
                line += f"  cid={cid}"

            # Color by severity first, then by proxied status
            if record.levelno >= logging.ERROR:
                return self._wrap(self._RED, line)
            if record.levelno >= logging.WARNING:
                return self._wrap(self._YELLOW, line)
            if proxied:
                return self._wrap(self._CYAN, line)
            return line

        # ── Proxy event ───────────────────────────────────────────
        if event == "proxy":
            method = getattr(record, "method", "")
            path = getattr(record, "path", "")
            proxy_status = getattr(record, "proxy_status", "")
            duration = getattr(record, "duration_ms", "")
            return self._wrap(self._DIM, f"{prefix}  proxy {method} {path} → {proxy_status}  {duration}ms")

        # ── Proxy error ───────────────────────────────────────────
        if event == "proxy_error":
            method = getattr(record, "method", "")
            path = getattr(record, "path", "")
            error = getattr(record, "error", "")
            return self._wrap(self._RED, f"{prefix}  proxy {method} {path} → FAILED  {error}")

        # ── General messages ──────────────────────────────────────
        msg = record.getMessage()
        if record.exc_info and record.exc_info[1]:
            msg += "\n" + self.formatException(record.exc_info)

        line = f"{prefix}  {msg}"

        if record.levelno >= logging.ERROR:
            return self._wrap(self._RED, line)
        if record.levelno >= logging.WARNING:
            return self._wrap(self._YELLOW, line)

        # Internal/app messages (INFO and below) — dim grey
        if event is None:
            return self._wrap(self._DIM, line)

        return line


# ---------------------------------------------------------------------------
# configure_logging()
# ---------------------------------------------------------------------------

class _MaxLevelFilter(logging.Filter):
    """Only allow records at or below the given level (inclusive)."""

    def __init__(self, max_level: int):
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


def configure_logging(
    service: str = "app",
    level: Optional[str] = None,
    fmt: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """Configure the root logger for structured output.

    Args:
        service:  Service identifier embedded in every log line (e.g. "api", "ui").
        level:    Log level override. Default from SUPERTABLE_LOG_LEVEL or INFO.
        fmt:      Format override. "json" or "text". Default from SUPERTABLE_LOG_FORMAT or "json".
        log_file: Optional file path. Default from SUPERTABLE_LOG_FILE or
                  {SUPERTABLE_HOME}/log/st.log. Set to "none" to disable.
    """
    level = (level or os.getenv("SUPERTABLE_LOG_LEVEL", "INFO")).upper()
    fmt = (fmt or os.getenv("SUPERTABLE_LOG_FORMAT", "json")).lower()

    # Log file path:
    #   - Explicit parameter wins.
    #   - Then SUPERTABLE_LOG_FILE env var.
    #   - Then default: {SUPERTABLE_HOME}/log/st.log
    #   - Set to "none" or "off" to disable file logging entirely.
    if log_file is None:
        log_file = os.getenv("SUPERTABLE_LOG_FILE")
    if log_file is None:
        # Lazy import to avoid import-time side effects from homedir
        from supertable.config.homedir import get_app_home
        log_file = os.path.join(get_app_home(), "log", "st.log")
    if log_file.strip().lower() in ("none", "off", ""):
        log_file = None

    # Choose formatter
    if fmt == "text":
        formatter = TextFormatter(service=service)
    else:
        formatter = JSONFormatter(service=service)

    # Console handlers — split by severity so terminals show errors in red
    # INFO and below → stdout (white/normal)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(_MaxLevelFilter(logging.INFO))

    # WARNING and above → stderr (red in most terminals/IDEs)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.WARNING)

    handlers: list = [stdout_handler, stderr_handler]

    # File handler (optional)
    if log_file:
        log_file = os.path.expanduser(log_file)
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(JSONFormatter(service=service))  # always JSON for files
        handlers.append(file_handler)

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))

    # Remove existing handlers to avoid duplicates on re-configure
    for h in root.handlers[:]:
        root.removeHandler(h)

    for h in handlers:
        root.addHandler(h)

    # Quiet noisy libraries
    for noisy in ("httpx", "httpcore", "uvicorn.access", "hpack", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Let uvicorn.error through (startup/shutdown messages)
    logging.getLogger("uvicorn.error").setLevel(getattr(logging, level, logging.INFO))


# ---------------------------------------------------------------------------
# Request Logging Middleware
# ---------------------------------------------------------------------------

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request with timing, status, and correlation ID.

    Emits a structured log record with event="request" after each response.
    Injects/propagates the correlation ID header for cross-service tracing.

    Usage:
        app.add_middleware(RequestLoggingMiddleware, service="api")
    """

    def __init__(self, app, service: str = "app"):
        super().__init__(app)
        self.service = service
        self.logger = logging.getLogger(f"supertable.{service}.access")

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip noisy paths
        path = request.url.path
        if any(path.startswith(skip) for skip in _SKIP_PATHS):
            return await call_next(request)

        # Correlation ID — reuse incoming or generate new
        correlation_id = (
            request.headers.get(CORRELATION_HEADER)
            or request.headers.get(CORRELATION_HEADER.lower())
            or uuid.uuid4().hex[:12]
        )

        # Inject into request state so handlers/proxy can access it
        request.state.correlation_id = correlation_id

        t0 = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - t0) * 1000, 1)
            proxied = getattr(request.state, "proxied", False)
            self.logger.error(
                f"{request.method} {path} → 500 (unhandled)",
                extra={
                    "event": "request",
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "path": path,
                    "status": 500,
                    "duration_ms": duration_ms,
                    "client_ip": _client_ip(request),
                    "error": str(exc),
                    "proxied": proxied,
                },
            )
            raise

        duration_ms = round((time.perf_counter() - t0) * 1000, 1)

        # Propagate correlation ID to response
        response.headers[CORRELATION_HEADER] = correlation_id

        # Detect whether this request was handled locally or proxied to API
        proxied = getattr(request.state, "proxied", False)

        # Determine log level from status code
        status = response.status_code
        if status >= 500:
            log_fn = self.logger.error
        elif status >= 400:
            log_fn = self.logger.warning
        else:
            log_fn = self.logger.info

        log_fn(
            f"{request.method} {path} → {status} ({duration_ms}ms)",
            extra={
                "event": "request",
                "correlation_id": correlation_id,
                "method": request.method,
                "path": path,
                "status": status,
                "duration_ms": duration_ms,
                "client_ip": _client_ip(request),
                "content_length": response.headers.get("content-length"),
                "proxied": proxied,
            },
        )

        return response


def _client_ip(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(request, "client", None)
    return getattr(client, "host", "-") if client else "-"
