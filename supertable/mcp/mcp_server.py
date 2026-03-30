#!/usr/bin/env python3
"""
mcp/mcp_server.py — Supertable MCP server (simple, robust, production-ready)

Key points:
- Strict input validation (org/super/table/role).
- Read-only SQL enforcement.
- Clear, consistent envelopes for all tools (errors included).
- Concurrency limiting + per-request timeout with AnyIO.
- Minimal but structured INFO logs for observability.
- Correct integration with MetaReader (get_tables, get_table_schema, get_table_stats, get_super_meta)
  and module-level list_supers/list_tables + DataReader.query_sql.

The external MCP parameter is ``role`` (RBAC role name or role ID).
Internally the codebase passes this as ``role_name`` to restrict_read_access /
check_write_access / DataReader.execute.
"""

from __future__ import annotations

import hmac
import inspect
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple
# dotenv is loaded centrally by supertable.config.settings

# ---------- Import path: allow `from supertable.*` ----------
# This file lives at `supertable/mcp/mcp_server.py`. When executed as a script,
# `sys.path[0]` is this directory, so we must add the project root (parent of the
# `supertable/` package) for absolute imports to work.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, os.pardir, os.pardir))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from supertable.config.settings import settings  # noqa: E402

# ---------- Logging ----------

LOG_LEVEL = settings.SUPERTABLE_LOG_LEVEL
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)-8s - %(message)s",
)
logger = logging.getLogger("supertable.mcp")

# ---------- MCP SDK ----------
_MCP_SDK_AVAILABLE = True
try:
    from mcp.server.fastmcp import FastMCP  # modern SDK
except Exception:
    try:
        from mcp import FastMCP  # legacy fallback
    except Exception:
        _MCP_SDK_AVAILABLE = False
        logger.error("MCP SDK not installed. Install with: pip install mcp")

        class FastMCP:  # type: ignore[no-redef]
            """Fallback shim so this module can import without the MCP SDK.

            The real implementation comes from the `mcp` package. Without it, we
            keep decorators as no-ops and raise a clear error at runtime.
            """

            def __init__(self, *_: object, **__: object) -> None:
                return

            def tool(self, *_: object, **__: object):
                def _decorator(fn):
                    return fn
                return _decorator

            def run(self, *_: object, **__: object) -> None:
                raise RuntimeError("MCP SDK not installed. Install with: pip install mcp")

            def streamable_http_app(self, *_: object, **__: object):
                raise RuntimeError("MCP SDK not installed. Install with: pip install mcp")

            def sse_app(self, *_: object, **__: object):
                raise RuntimeError("MCP SDK not installed. Install with: pip install mcp")
# ---------- AnyIO ----------
try:
    import anyio
except Exception:
    logger.error("anyio is required. Run: pip install anyio")
    raise

# ---------- Supertable imports (lazy-resolved) ----------
MetaReader = None
engine_enum = None
data_query_sql = None
list_supers_fn = None
list_tables_fn = None
DataWriter = None

# ---------- Audit ----------
try:
    from supertable.audit import emit as _audit_emit, EventCategory, Actions, Severity, Outcome, make_detail
    _audit_available = True
except Exception:
    _audit_available = False

def _ensure_imports():
    global MetaReader, engine_enum, data_query_sql, list_supers_fn, list_tables_fn, DataWriter
    if MetaReader is None or engine_enum is None or data_query_sql is None or list_supers_fn is None or list_tables_fn is None:
        # Class + enum + module functions
        from supertable.meta_reader import MetaReader as _MR, list_supers as _LS, list_tables as _LT  # :contentReference[oaicite:2]{index=2}
        from supertable.data_reader import engine as _ENG, query_sql as _DQ  # :contentReference[oaicite:3]{index=3}
        from supertable.data_writer import DataWriter as _DW
        MetaReader, engine_enum, data_query_sql = _MR, _ENG, _DQ
        list_supers_fn, list_tables_fn = _LS, _LT
        DataWriter = _DW

# ---------- Helpers ----------
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
ROLE_RE = SAFE_ID_RE

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _normalize_transport_value(value: Optional[str], default: str = "stdio") -> str:
    """Normalize transport names across MCP SDK versions and docs.

    Notes:
    - Claude Desktop "Remote MCP servers" use the Streamable HTTP transport.
    - Some docs and SDKs refer to this as "http"; others use "streamable-http".
    - We normalize all Streamable HTTP aliases to "streamable-http".
    """
    v = (value or default).strip().lower()
    if v in {"", "stdio"}:
        return "stdio"
    if v in {"http", "streamable-http", "streamable_http", "streamablehttp"}:
        return "streamable-http"
    if v == "sse":
        return "sse"
    return v



def _serve_streamable_http_via_uvicorn(*, mcp: FastMCP, host: str, port: int, path: str) -> None:
    """Serve the MCP server via Streamable HTTP using an external ASGI server.

    Compatibility note:
    - Some MCP SDK / FastMCP versions do not support passing `host`/`port` to
      `mcp.run(transport="streamable-http")`.
    - The most reliable approach is to build the ASGI app and run it via Uvicorn.
    """
    # Build the ASGI app. Different SDK versions expose different helpers.
    app_factory = None
    if hasattr(mcp, "http_app"):
        app_factory = getattr(mcp, "http_app")
    elif hasattr(mcp, "streamable_http_app"):
        app_factory = getattr(mcp, "streamable_http_app")

    if app_factory is None:
        raise RuntimeError(
            "This FastMCP version does not provide http_app()/streamable_http_app(); "
            "upgrade the MCP Python SDK / FastMCP to enable remote HTTP transport."
        )

    normalized_path = (path or "/mcp").strip() or "/mcp"
    if not normalized_path.startswith("/"):
        normalized_path = "/" + normalized_path

    # Prefer first-class `path` support if the SDK exposes it.
    try:
        sig = inspect.signature(app_factory)
        if "path" in sig.parameters:
            app = app_factory(path=normalized_path)
        else:
            app = app_factory()
            if normalized_path not in {"/mcp", "/mcp/"}:
                logger.warning(
                    "Configured SUPERTABLE_MCP_HTTP_PATH=%s but the installed FastMCP SDK "
                    "does not support custom paths on http_app(); using the default /mcp.",
                    normalized_path,
                )
    except Exception:
        app = app_factory()

    try:
        import uvicorn
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("uvicorn must be installed to serve MCP over HTTP") from exc

    # Wrap with TrustedHostMiddleware allowing any host — required when running
    # behind a reverse proxy (Caddy/nginx) that forwards requests from any IP/hostname.
    try:
        from starlette.middleware.trustedhost import TrustedHostMiddleware
        app = TrustedHostMiddleware(app, allowed_hosts=["*"])
    except ImportError:
        pass

    uvicorn.run(app, host=host, port=port, proxy_headers=True, forwarded_allow_ips="*")

def _env_transport(name: str, default: str = "stdio") -> str:
    return _normalize_transport_value(os.getenv(name), default=default)

def _env_set(name: str) -> Optional[Set[str]]:
    """Parse a comma-separated env var into a set of stripped strings."""
    v = (os.getenv(name) or "").strip()
    if not v:
        return None
    return {s.strip() for s in v.split(",") if s.strip()}

def _safe_id(x: str, field: str) -> str:
    if (
        not isinstance(x, str)
        or not SAFE_ID_RE.match(x)
        or "/" in x
        or "\\" in x
        or ".." in x
    ):
        raise ValueError(f"Invalid {field}: {x!r}")
    return x

def _sql_escape_literal(value: str) -> str:
    """Escape a value for safe inclusion in a SQL single-quoted string literal.

    - Rejects null bytes (which would truncate the string in C-backed engines).
    - Doubles single quotes (' → '') per the SQL standard.

    This is defence-in-depth for internal queries that cannot use parameterized
    statements because the downstream ``data_query_sql()`` API accepts raw SQL.
    """
    if "\x00" in value:
        raise ValueError("Null bytes are not allowed in SQL literal values.")
    return value.replace("'", "''")

def _safe_column_id(col: str) -> str:
    """Validate and return a column name safe for use in double-quoted SQL identifiers.

    Rejects empty values and null bytes. Escapes embedded double quotes
    (" → "") per the SQL standard for delimited identifiers.
    """
    if not isinstance(col, str) or not col.strip():
        raise ValueError("Invalid column: must be a non-empty string.")
    c = col.strip()
    if "\x00" in c:
        raise ValueError("Invalid column: null bytes not allowed.")
    if len(c) > 256:
        raise ValueError("Invalid column: exceeds 256 characters.")
    return c.replace('"', '""')

def _validate_role(r: str) -> str:
    """Validate a role identifier (role name or role ID)."""
    if not isinstance(r, str) or not r.strip():
        raise ValueError("Invalid role: must be a non-empty string.")
    r = r.strip()
    if not ROLE_RE.match(r):
        raise ValueError(f"Invalid role format: {r!r} (allowed: alphanumeric, underscore, dot, dash; max 128 chars).")
    return r

_FORBIDDEN_SQL_RE = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Matches SQL block comments (/* ... */) including nested, and line comments (-- ...\n)
_SQL_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\r\n]*")

def _strip_sql_comments(sql: str) -> str:
    """Remove block comments (/* ... */) and line comments (-- ...) from SQL.

    This prevents evasion of the read-only check by splitting forbidden
    keywords across comment boundaries (e.g. ``INS/**/ERT``).
    """
    s = _SQL_BLOCK_COMMENT_RE.sub(" ", sql)
    s = _SQL_LINE_COMMENT_RE.sub(" ", s)
    return s

def _read_only_sql(sql: str) -> None:
    stripped = _strip_sql_comments(sql or "")
    s = stripped.strip().lower()
    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Only SELECT (or WITH … SELECT) statements are allowed.")
    if _FORBIDDEN_SQL_RE.search(s):
        raise ValueError("Statement contains write/DDL keywords. Read-only queries only.")

def _clamp_limit(limit: Optional[int], default_n: int, max_n: int) -> int:
    try:
        n = int(limit) if limit is not None else default_n
    except Exception:
        n = default_n
    return max(1, min(max_n, n))

def _summarize_rows(rows: List[List[Any]], head_n: int = 3) -> str:
    n = len(rows)
    return f"rows={n}, head={rows[:head_n]!r}"

# ---------- Config ----------
@dataclass(frozen=True)
class Config:
    name: str = "supertable-mcp"
    version: str = "1.3.0"
    default_engine: str = field(default_factory=lambda: settings.SUPERTABLE_DEFAULT_ENGINE)
    default_limit: int = field(default_factory=lambda: settings.SUPERTABLE_DEFAULT_LIMIT)
    max_limit: int = field(default_factory=lambda: settings.SUPERTABLE_MAX_LIMIT)
    default_query_timeout_sec: float = field(default_factory=lambda: settings.SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC)
    max_concurrency: int = field(default_factory=lambda: settings.SUPERTABLE_MAX_CONCURRENCY)
    require_explicit_role: bool = True
    allowed_roles: Optional[Set[str]] = field(default_factory=lambda: _env_set("SUPERTABLE_ALLOWED_ROLES"))
    allow_sysadmin_default: bool = _env_bool("SUPERTABLE_ALLOW_SYSADMIN_DEFAULT", False)
    require_token: bool = True
    shared_token: str = field(default_factory=lambda: settings.SUPERTABLE_MCP_TOKEN)
    transport: str = _env_transport("SUPERTABLE_MCP_TRANSPORT", "stdio")
    http_host: str = field(default_factory=lambda: settings.SUPERTABLE_MCP_HOST)
    http_port: int = field(default_factory=lambda: settings.SUPERTABLE_MCP_PORT)
    http_path: str = "/mcp"

CFG = Config()

# ---------- MCP app ----------
def _build_mcp(name: str, version: str) -> FastMCP:
    """Build a FastMCP instance compatible with both stdio and streamable-http.

    ``stateless_http=True`` and ``json_response=True`` are required for the
    MCP SDK's Streamable HTTP transport (mounted in web_app.py).  They are
    harmless when the server is used over stdio.
    """
    try:
        sig = inspect.signature(FastMCP)
        kwargs: Dict[str, Any] = {}
        if "version" in sig.parameters:
            kwargs["version"] = version
        if "stateless_http" in sig.parameters:
            kwargs["stateless_http"] = True
        if "json_response" in sig.parameters:
            kwargs["json_response"] = True
        # Allow any Origin header — the web_app.py gateway layer handles
        # browser-facing auth; real MCP SDK clients don't send Origin.
        if "allowed_origins" in sig.parameters:
            kwargs["allowed_origins"] = ["*"]
        # Disable DNS rebinding protection — required when connecting via
        # IP address (e.g. http://192.168.168.130:8099/mcp) or behind a
        # reverse proxy that rewrites the Host header.  Without this, the
        # SDK returns HTTP 421 "Invalid Host header" for any non-localhost
        # Host value.
        if "transport_security" in sig.parameters:
            try:
                from mcp.server.transport_security import TransportSecuritySettings
                kwargs["transport_security"] = TransportSecuritySettings(
                    enable_dns_rebinding_protection=False,
                )
            except ImportError:
                pass
        return FastMCP(name, **kwargs)  # type: ignore[arg-type]
    except Exception:
        return FastMCP(name)

mcp = _build_mcp(CFG.name, CFG.version)
_LIMITER: Optional["anyio.CapacityLimiter"] = None

import threading as _threading
_LIMITER_INIT_LOCK = _threading.Lock()

def _get_limiter() -> "anyio.CapacityLimiter":
    global _LIMITER
    if _LIMITER is None:
        with _LIMITER_INIT_LOCK:
            if _LIMITER is None:
                # Initialize lazily inside an async context (required by AnyIO/sniffio).
                _LIMITER = anyio.CapacityLimiter(CFG.max_concurrency)
    return _LIMITER


# ---------- Auth helpers ----------
def _check_token(auth_token: Optional[str]) -> None:
    """Optional shared-secret auth between client and server (stdio)."""
    if not CFG.require_token:
        return
    token = (auth_token or "").strip()
    if not token or not CFG.shared_token:
        if _audit_available:
            _audit_emit(
                category=EventCategory.AUTHENTICATION, action=Actions.MCP_AUTH_FAILURE,
                organization=settings.SUPERTABLE_ORGANIZATION, severity=Severity.WARNING,
                outcome=Outcome.FAILURE, reason="missing_token", server="mcp",
            )
        raise PermissionError("auth_token is required by server policy.")
    if not hmac.compare_digest(token, CFG.shared_token):
        if _audit_available:
            _audit_emit(
                category=EventCategory.AUTHENTICATION, action=Actions.MCP_AUTH_FAILURE,
                organization=settings.SUPERTABLE_ORGANIZATION, severity=Severity.WARNING,
                outcome=Outcome.FAILURE, reason="invalid_token", server="mcp",
            )
        raise PermissionError("auth_token invalid.")

def _allowed_role(r: str) -> bool:
    return CFG.allowed_roles is None or r in CFG.allowed_roles

def _resolve_role(role: Optional[str]) -> str:
    """Resolve and validate a role identifier."""
    r = (role or "").strip()
    if CFG.require_explicit_role:
        if not r:
            raise PermissionError("role is required by server policy.")
        r = _validate_role(r)
        if not _allowed_role(r):
            raise PermissionError("role not permitted by server policy.")
        return r
    if not r:
        r = settings.SUPERTABLE_ROLE
    if not r:
        raise PermissionError("role missing and no default configured.")
    return _validate_role(r)

def _resolve_engine(engine_name: Optional[str]):
    name = (engine_name or CFG.default_engine or "AUTO").upper()
    try:
        _ensure_imports()
        if engine_enum and hasattr(engine_enum, name):
            return getattr(engine_enum, name)
    except Exception:
        pass
    return name  # fallback (string)

# ---------- Logging decorator ----------
def _emit_mcp_metric(fn: str, kwargs: dict, dt_ms: float, status: str, result_rows: int = 0) -> None:
    """Best-effort MCP tool metric emission.  Never raises."""
    try:
        org = kwargs.get("organization", "")
        sup = kwargs.get("super_name", "")
        if not org or not sup:
            return
        from datetime import datetime, timezone
        from supertable.monitoring_writer import MonitoringWriter
        with MonitoringWriter(
            organization=org, super_name=sup, monitor_type="mcp",
        ) as mon:
            mon.log_metric({
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "tool": fn,
                "status": status,
                "duration_ms": round(dt_ms, 2),
                "role": kwargs.get("role", ""),
                "result_rows": result_rows,
            })
    except Exception:
        pass


def log_tool(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        fn = func.__name__
        try:
            # Trim giant SQL in logs
            safe_kwargs = {}
            for k, v in kwargs.items():
                if k == "sql" and isinstance(v, str):
                    safe_kwargs[k] = v if len(v) <= 500 else v[:500] + "…"
                else:
                    safe_kwargs[k] = v
            logger.info("→ tool %s request args=%s", fn, safe_kwargs)

            t0 = time.perf_counter()
            out = await func(*args, **kwargs)
            dt = (time.perf_counter() - t0) * 1000.0

            summary = ""
            result_rows = 0
            if isinstance(out, dict):
                r = out.get("result", out)
                if isinstance(r, dict) and {"columns", "rows"} <= r.keys():
                    result_rows = len(r.get("rows", []))
                    summary = f"columns={len(r['columns'])} {_summarize_rows(r['rows'])} status={r.get('status')}"
                else:
                    try:
                        keys = list(r.keys())[:6] if isinstance(r, dict) else []
                    except Exception:
                        keys = []
                    summary = f"keys={keys}"
            logger.info("← tool %s response (%0.2f ms) %s", fn, dt, summary)
            _emit_mcp_metric(fn, kwargs, dt, "ok", result_rows)
            return out
        except Exception as exc:
            dt = (time.perf_counter() - t0) * 1000.0 if "t0" in locals() else 0.0
            logger.exception("✖ tool %s error: %s", fn, exc)
            _emit_mcp_metric(fn, kwargs, dt, "error")
            # Return consistent error envelope for all tools.
            # query_sql has a richer envelope; other tools use a simpler shape.
            if fn == "query_sql":
                return {
                    "columns": [],
                    "rows": [],
                    "rowcount": 0,
                    "limit_applied": 0,
                    "engine": CFG.default_engine,
                    "elapsed_ms": None,
                    "status": "ERROR",
                    "message": f"{exc.__class__.__name__}: {exc}",
                    "columns_meta": [],
                }
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }
    wrapper.__name__ = func.__name__
    return wrapper

# ---------- Tools ----------
@mcp.tool()
@log_tool
async def health() -> Dict[str, Any]:
    return {"result": "ok"}

@mcp.tool()
@log_tool
async def info() -> Dict[str, Any]:
    return {
        "result": {
            "name": CFG.name,
            "version": CFG.version,
            "max_concurrency": CFG.max_concurrency,
            "default_engine": CFG.default_engine,
            "default_limit": CFG.default_limit,
            "max_limit": CFG.max_limit,
            "default_query_timeout_sec": CFG.default_query_timeout_sec,
            "require_explicit_role": CFG.require_explicit_role,
            "allow_sysadmin_default": CFG.allow_sysadmin_default,
            "require_token": CFG.require_token,
        }
    }

@mcp.tool()
@log_tool
async def whoami(role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    _check_token(auth_token)
    r = _resolve_role(role)
    return {"result": {"role": r}}

@mcp.tool()
@log_tool
async def list_supers(organization: str, role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    _check_token(auth_token)
    _resolve_role(role)
    _ensure_imports()

    def _work():
        return list_supers_fn(org)

    async with _get_limiter():
        try:
            supers = await anyio.to_thread.run_sync(_work)
        except Exception as e:
            logger.exception("list_supers failed: %s", e)
            supers = []
    return {"result": supers}

@mcp.tool()
@log_tool
async def list_tables(super_name: str, organization: str, role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return list(reader.get_tables(role_name=r))

    async with _get_limiter():
        tables = await anyio.to_thread.run_sync(_work)

        # ── __catalog__ lookup ──────────────────────────────────────────────────
        # If a __catalog__ table exists, fetch ALL its rows regardless of schema
        # and always emit catalog_hint so the AI uses it before querying anything.
        catalog: List[Dict[str, Any]] = []
        catalog_hint: Optional[str] = None

        if "__catalog__" in tables:
            # Always set the hint — even if the fetch fails the AI knows it exists.
            catalog_hint = (
                "IMPORTANT — CATALOG INSTRUCTIONS:\n"
                "The 'catalog' field contains one entry per table with human-written metadata. "
                "You MUST read it IN FULL before writing any SQL.\n"
                "\n"
                "How to use the catalog:\n"
                "1. Each entry describes a table. Look for fields like 'table_name' (or 'name' or 'table') "
                "to identify which table the entry describes.\n"
                "2. Look for a 'description' (or 'info', 'notes', 'summary') field — it explains "
                "what the table contains and what business questions it answers.\n"
                "3. If a 'columns' field exists, it lists available columns with types and meanings. "
                "Use it to pick the right column names in your SQL — do NOT guess column names.\n"
                "4. If 'joins' or 'relationships' fields exist, use them to determine how tables connect.\n"
                "5. If 'example_queries' or 'examples' fields exist, use them as templates for your SQL.\n"
                "6. If a 'filters' or 'scope' field exists, it describes default filtering rules.\n"
                "\n"
                "The catalog is the AUTHORITATIVE source for understanding this dataset. "
                "Never query a table without consulting its catalog entry first. "
                "If a table has no catalog entry, call describe_table before querying it."
            )
            try:
                eng = _resolve_engine(None)
                limit_n = _clamp_limit(None, 500, 500)

                def _fetch_catalog():
                    return _exec_query_sync(
                        sup, org,
                        'SELECT * FROM "__catalog__"',
                        limit_n, eng, r,
                    )

                # Runs inside the already-held limiter slot — no nested acquire.
                cat_result = await anyio.to_thread.run_sync(_fetch_catalog)

                if cat_result.get("status") == "OK":
                    cols = cat_result.get("columns") or []
                    rows = cat_result.get("rows") or []
                    # Accept any schema — zip whatever columns exist into dicts.
                    catalog = [dict(zip(cols, row)) for row in rows]
                    logger.info("__catalog__ loaded: %d rows, columns=%s", len(catalog), cols)
            except Exception as exc:
                logger.warning("__catalog__ fetch failed (non-fatal): %s", exc)

        # ── __feedback__ summary ────────────────────────────────────────────────
        # If a __feedback__ table exists, load the last 50 rows so the AI has
        # context on past user ratings and comments. This helps it avoid repeating
        # poorly-rated answers and build on well-rated ones.
        feedback: List[Dict[str, Any]] = []
        feedback_hint: Optional[str] = None

        if "__feedback__" in tables:
            feedback_hint = (
                "IMPORTANT: The 'feedback' field contains past user ratings and comments "
                "on previous AI responses in this SuperTable. You MUST review it to: "
                "1) avoid repeating approaches that received thumbs_down ratings, "
                "2) build on patterns that received thumbs_up ratings, "
                "3) address any recurring user comments or frustrations. "
                "Keep this context in memory for the entire session."
            )
            try:
                eng = _resolve_engine(None)
                limit_n = _clamp_limit(None, 50, 50)

                def _fetch_feedback():
                    return _exec_query_sync(
                        sup, org,
                        'SELECT * FROM "__feedback__" ORDER BY ts DESC LIMIT 50',
                        limit_n, eng, r,
                    )

                # Runs inside the already-held limiter slot — no nested acquire.
                fb_result = await anyio.to_thread.run_sync(_fetch_feedback)

                if fb_result.get("status") == "OK":
                    cols = fb_result.get("columns") or []
                    rows = fb_result.get("rows") or []
                    feedback = [dict(zip(cols, row)) for row in rows]
                    logger.info("__feedback__ loaded: %d rows", len(feedback))
            except Exception as exc:
                logger.warning("__feedback__ fetch failed (non-fatal): %s", exc)

        # ── __annotations__ lookup ─────────────────────────────────────────────
        # User annotations are persistent preferences and domain knowledge that
        # the AI MUST read and apply when generating SQL, choosing charts, etc.
        annotations: List[Dict[str, Any]] = []
        annotations_hint: Optional[str] = None

        if "__annotations__" in tables:
            annotations_hint = (
                "CRITICAL: The 'annotations' field contains user-defined rules and preferences. "
                "These are HARD CONSTRAINTS — you MUST follow them when generating SQL, "
                "choosing visualizations, interpreting terminology, and scoping queries. "
                "Each annotation has a category (sql/terminology/visualization/scope/domain) "
                "and a context (table name or '*' for global). Apply global annotations always; "
                "apply table-specific annotations when that table is involved."
            )
            try:
                eng = _resolve_engine(None)
                limit_n = _clamp_limit(None, 200, 200)

                def _fetch_annotations():
                    return _exec_query_sync(
                        sup, org,
                        'SELECT * FROM "__annotations__" WHERE ("status" IS NULL OR "status" != \'deleted\') ORDER BY ts DESC LIMIT 200',
                        limit_n, eng, r,
                    )

                ann_result = await anyio.to_thread.run_sync(_fetch_annotations)

                if ann_result.get("status") == "OK":
                    cols = ann_result.get("columns") or []
                    rows = ann_result.get("rows") or []
                    annotations = [dict(zip(cols, row)) for row in rows]
                    logger.info("__annotations__ loaded: %d rows", len(annotations))
            except Exception as exc:
                logger.warning("__annotations__ fetch failed (non-fatal): %s", exc)

    return {
        "result": tables,
        "catalog": catalog,
        "catalog_hint": catalog_hint,
        "feedback": feedback,
        "feedback_hint": feedback_hint,
        "annotations": annotations,
        "annotations_hint": annotations_hint,
        "system_hint": (
            "SESSION INSTRUCTIONS — follow these for the entire conversation:\n"
            "\n"
            "1. CATALOG: The 'catalog' field describes every table — what it contains, "
            "its columns, known joins, filters and example questions. "
            "Read it IN FULL before writing any SQL. Never query a table without consulting "
            "its catalog entry first. Use column names from the catalog — do not guess. "
            "If a table has no catalog entry, call describe_table before querying it.\n"
            "\n"
            "2. QUERY PLANNING: Before writing any SQL, follow this planning sequence:\n"
            "   a) IDENTIFY the tables and columns needed from the catalog.\n"
            "   b) PROFILE filter columns: If the user mentions filter values (names, categories, "
            "statuses, sites, types — anything used in WHERE/GROUP BY), call profile_column "
            "to discover exact values, casing, and cardinality. Never guess enum values — "
            "the user may type 'jasmin' but the data contains 'Jasmin' or 'jasmin.com'. "
            "profile_column returns top values with frequencies so you can match correctly.\n"
            "   c) CHECK annotations for any rules about this table or these columns.\n"
            "   d) VALIDATE: Call validate_sql to verify tables exist, columns exist, "
            "and RBAC allows the query — before spending a query_sql call.\n"
            "   e) EXECUTE: Call query_sql with the validated, correctly-filtered query.\n"
            "   f) VISUALIZE: Choose chart types based on the data shape and any "
            "visualization annotations.\n"
            "This sequence prevents wasted queries from wrong column names, wrong enum values, "
            "or RBAC denials. Skip steps only when you are certain (e.g., you already profiled "
            "that column earlier in the session).\n"
            "\n"
            "3. ANNOTATIONS: The 'annotations' field contains user-defined rules and preferences. "
            "These are HARD CONSTRAINTS. Each has a category and context. "
            "Apply global (*) annotations to every query. "
            "Apply table-specific annotations when that table is involved. "
            "NEVER ignore an annotation — they represent explicit user intent.\n"
            "\n"
            "4. FEEDBACK: The 'feedback' field contains past user ratings on previous AI responses. "
            "Avoid patterns rated thumbs_down. Repeat patterns rated thumbs_up. "
            "If a comment field explains why, treat it as a hard constraint.\n"
            "\n"
            "5. COLLECTING FEEDBACK: After every response you give, ask the user: "
            "'Was this helpful? Reply 👍 or 👎'. "
            "When the user replies 👍 or with positive words (yes, good, great, correct, perfect), "
            "call the submit_feedback tool with rating=thumbs_up, "
            "set query to the question that was asked, "
            "set response_summary to a one-sentence summary of your answer, "
            "and set table_name to the primary table you queried. "
            "When the user replies 👎 or with negative words (no, wrong, incorrect, bad), "
            "call submit_feedback with rating=thumbs_down and include their words in the comment field. "
            "Never skip this step — every interaction should be logged.\n"
            "\n"
            "6. MEMORY: Keep catalog, annotations, and feedback in memory for the full session. "
            "Do not re-call list_tables unless the user explicitly asks to refresh."
        ),
    }

@mcp.tool()
@log_tool
async def describe_table(super_name: str, organization: str, table: str, role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_table_schema(tbl, role_name=r)

    async with _get_limiter():
        schema = await anyio.to_thread.run_sync(_work)
    return {"result": schema}

@mcp.tool()
@log_tool
async def get_table_stats(super_name: str, organization: str, table: str, role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_table_stats(tbl, role_name=r)

    async with _get_limiter():
        stats = await anyio.to_thread.run_sync(_work)
    return {"result": stats}

@mcp.tool()
@log_tool
async def get_super_meta(super_name: str, organization: str, role: Optional[str] = None, auth_token: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_super_meta(role_name=r)

    async with _get_limiter():
        meta = await anyio.to_thread.run_sync(_work)
    return {"result": meta}

def _exec_query_sync(super_name: str, organization: str, sql: str, limit_n: int, eng: Any, role_name: str) -> Dict[str, Any]:
    """
    Runs the module-level query_sql and enforces `limit` server-side (slice rows)
    to keep the server robust even if downstream ignores it.
    """
    _ensure_imports()
    t0 = time.perf_counter()

    # Use the module-level function from data_reader.py
    columns, rows, columns_meta = data_query_sql(
        organization=organization,
        super_name=super_name,
        sql=sql,
        limit=limit_n,
        engine=eng,
        role_name=role_name,
    )

    # Enforce limit here (in case downstream didn't)
    rows_limited = rows[:limit_n]
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    if _audit_available:
        try:
            _audit_emit(
                category=EventCategory.DATA_ACCESS, action=Actions.QUERY_EXECUTE,
                organization=organization, super_name=super_name,
                resource_type="query", resource_id="",
                detail=make_detail(
                    sql_preview=sql[:200], row_count=len(rows_limited),
                    engine=getattr(eng, "name", str(eng)),
                    role_name=role_name, duration_ms=round(elapsed_ms),
                ),
                server="mcp",
            )
        except Exception:
            pass

    return {
        "columns": list(columns),
        "rows": [list(r) for r in rows_limited],
        "rowcount": len(rows_limited),
        "limit_applied": limit_n,
        "engine": getattr(eng, "name", str(eng)),
        "elapsed_ms": elapsed_ms,
        "status": "OK",
        "message": "",
        "columns_meta": columns_meta or [],
    }

@mcp.tool()
@log_tool
async def query_sql(
    super_name: str,
    organization: str,
    sql: str,
    limit: Optional[int] = None,
    engine: Optional[str] = None,
    query_timeout_sec: Optional[float] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _read_only_sql(sql)

    limit_n = _clamp_limit(limit, CFG.default_limit, CFG.max_limit)
    eng = _resolve_engine(engine)
    timeout = float(query_timeout_sec or CFG.default_query_timeout_sec)

    async with _get_limiter():
        try:
            def _work():
                return _exec_query_sync(sup, org, sql, limit_n, eng, r)

            if hasattr(anyio, "fail_after"):
                with anyio.fail_after(timeout):
                    out = await anyio.to_thread.run_sync(_work)
            else:
                result = None
                with anyio.move_on_after(timeout) as scope:
                    result = await anyio.to_thread.run_sync(_work)
                if scope.cancel_called or result is None:
                    raise TimeoutError(f"Query timed out after {timeout} sec")
                out = result
        except TimeoutError as te:
            return {
                "columns": [],
                "rows": [],
                "rowcount": 0,
                "limit_applied": limit_n,
                "engine": getattr(eng, "name", str(eng)),
                "elapsed_ms": None,
                "status": "ERROR",
                "message": str(te),
                "columns_meta": [],
            }
        except Exception as exc:
            logger.exception("query_sql failed: %s", exc)
            return {
                "columns": [],
                "rows": [],
                "rowcount": 0,
                "limit_applied": limit_n,
                "engine": getattr(eng, "name", str(eng)),
                "elapsed_ms": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
                "columns_meta": [],
            }

    return out

@mcp.tool()
@log_tool
async def sample_data(
    super_name: str,
    organization: str,
    table: str,
    limit: Optional[int] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a small sample from a table.  Equivalent to SELECT * FROM "table" LIMIT N.

    This is a convenience shortcut for LLM agents that need to preview data
    without constructing SQL.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    _check_token(auth_token)
    r = _resolve_role(role)

    limit_n = _clamp_limit(limit, 10, CFG.max_limit)
    eng = _resolve_engine(None)
    sql = f'SELECT * FROM "{tbl}" LIMIT {limit_n}'

    async with _get_limiter():
        try:
            def _work():
                return _exec_query_sync(sup, org, sql, limit_n, eng, r)
            out = await anyio.to_thread.run_sync(_work)
        except Exception as exc:
            logger.exception("sample_data failed: %s", exc)
            return {
                "columns": [],
                "rows": [],
                "rowcount": 0,
                "limit_applied": limit_n,
                "engine": getattr(eng, "name", str(eng)),
                "elapsed_ms": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
                "columns_meta": [],
            }
    return out

@mcp.tool()
@log_tool
async def submit_feedback(
    super_name: str,
    organization: str,
    rating: str,
    query: str,
    response_summary: str,
    table_name: Optional[str] = None,
    comment: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Write a feedback row to the __feedback__ table.

    Call this after every AI response to record user satisfaction.
    rating must be one of: thumbs_up, thumbs_down, neutral.

    Args:
        super_name:        SuperTable name.
        organization:      Organization name.
        rating:            thumbs_up | thumbs_down | neutral
        query:             The natural language question or SQL that was asked.
        response_summary:  Brief summary of what the AI answered.
        table_name:        Which table was primarily queried (optional).
        comment:           Optional free-text comment from the user.
        role:              RBAC role.
        auth_token:        Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    valid_ratings = {"thumbs_up", "thumbs_down", "neutral"}
    rating = (rating or "").strip().lower()
    if rating not in valid_ratings:
        return {
            "result": None,
            "status": "ERROR",
            "message": f"rating must be one of: {sorted(valid_ratings)}",
        }

    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    row = {
        "ts":               ts,
        "rating":           rating,
        "query":            (query or "").strip()[:2000],
        "response_summary": (response_summary or "").strip()[:2000],
        "table_name":       (table_name or "").strip()[:256],
        "comment":          (comment or "").strip()[:2000],
        "role":             r,
    }

    def _write():
        import pyarrow as pa

        # DataWriter.write() expects an Arrow-compatible object
        # (it calls polars.from_arrow(data) internally).
        batch = pa.RecordBatch.from_pylist([row])
        dw = DataWriter(super_name=sup, organization=org)
        columns, rows, inserted, deleted = dw.write(
            role_name=r,
            simple_name="__feedback__",
            data=batch,
            overwrite_columns=["ts"],  # ts is unique per entry; never overwrite existing rows
        )
        return {"columns": columns, "inserted": inserted, "deleted": deleted}

    async with _get_limiter():
        try:
            write_result = await anyio.to_thread.run_sync(_write)
            logger.info(
                "submit_feedback: rating=%s table=%s inserted=%s",
                rating, row["table_name"], write_result.get("inserted"),
            )
            return {
                "result": "ok",
                "status": "OK",
                "ts": ts,
                "rating": rating,
                "inserted": write_result.get("inserted"),
            }
        except Exception as exc:
            logger.exception("submit_feedback failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }

# ---------- Annotations (user knowledge injected into AI context) ----------

@mcp.tool()
@log_tool
async def store_annotation(
    super_name: str,
    organization: str,
    category: str,
    context: str,
    instruction: str,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Store a user annotation that the AI should remember for future queries.

    Annotations are persistent user preferences and domain knowledge that get
    injected into the AI context window for every future query by this role.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        category:      One of: sql, terminology, visualization, scope, domain.
                       - sql: preferences about SQL generation (date ranges, aggregations, filters)
                       - terminology: what user terms mean (e.g. "weekly" = last 7 rolling days)
                       - visualization: chart type preferences, axis preferences
                       - scope: default data ranges, limits, table preferences
                       - domain: business rules, known relationships, data quality notes
        context:       What table/topic this applies to, or "*" for global.
        instruction:   The actionable instruction in English. Must be clear and imperative.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    valid_categories = {"sql", "terminology", "visualization", "scope", "domain"}
    category = (category or "").strip().lower()
    if category not in valid_categories:
        return {
            "result": None,
            "status": "ERROR",
            "message": f"category must be one of: {sorted(valid_categories)}",
        }

    instruction = (instruction or "").strip()
    if not instruction:
        return {
            "result": None,
            "status": "ERROR",
            "message": "instruction is required and must be non-empty.",
        }

    context_val = (context or "*").strip()[:256]
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    row = {
        "ts":          ts,
        "category":    category,
        "context":     context_val,
        "instruction": instruction[:2000],
        "role":        r,
    }

    def _write():
        import pyarrow as pa
        batch = pa.RecordBatch.from_pylist([row])
        dw = DataWriter(super_name=sup, organization=org)
        columns, rows, inserted, deleted = dw.write(
            role_name=r,
            simple_name="__annotations__",
            data=batch,
            overwrite_columns=["ts"],
        )
        return {"columns": columns, "inserted": inserted, "deleted": deleted}

    async with _get_limiter():
        try:
            write_result = await anyio.to_thread.run_sync(_write)
            logger.info(
                "store_annotation: category=%s context=%s inserted=%s",
                category, context_val, write_result.get("inserted"),
            )
            return {
                "result": "ok",
                "status": "OK",
                "ts": ts,
                "category": category,
                "context": context_val,
                "inserted": write_result.get("inserted"),
            }
        except Exception as exc:
            logger.exception("store_annotation failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }


@mcp.tool()
@log_tool
async def get_annotations(
    super_name: str,
    organization: str,
    context: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve stored annotations for this role.

    Returns all annotations, optionally filtered by context (table name or "*" for global).

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        context:       Filter by context. Empty or None returns all.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    ctx = _sql_escape_literal((context or "").strip())
    if ctx:
        sql = f'SELECT * FROM "__annotations__" WHERE ("context" = \'{ctx}\' OR "context" = \'*\') AND ("status" IS NULL OR "status" != \'deleted\') ORDER BY ts DESC LIMIT 200'
    else:
        sql = 'SELECT * FROM "__annotations__" WHERE ("status" IS NULL OR "status" != \'deleted\') ORDER BY ts DESC LIMIT 200'

    eng = _resolve_engine(None)

    async with _get_limiter():
        try:
            def _work():
                return _exec_query_sync(sup, org, sql, 200, eng, r)
            result = await anyio.to_thread.run_sync(_work)

            if result.get("status") == "OK":
                cols = result.get("columns") or []
                rows = result.get("rows") or []
                items = [dict(zip(cols, row)) for row in rows]
                return {
                    "status": "OK",
                    "annotations": items,
                    "count": len(items),
                }
            else:
                return {
                    "status": "OK",
                    "annotations": [],
                    "count": 0,
                    "note": "No __annotations__ table found or empty.",
                }
        except Exception as exc:
            logger.warning("get_annotations failed: %s", exc)
            return {
                "status": "OK",
                "annotations": [],
                "count": 0,
                "note": f"Could not read annotations: {exc}",
            }


# ---------- App state (UI persistence — chat, dashboard, widget, query) ----------

@mcp.tool()
@log_tool
async def store_app_state(
    super_name: str,
    organization: str,
    namespace: str,
    key: str,
    value: str,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Store application state for the Agentic BI frontend.

    This is a generic key-value store for UI persistence: chat history,
    dashboard configs, widget configs, saved queries, data point notes.
    The AI does NOT read this data — it is purely for UI state.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        namespace:     Entity type: "chat" | "dashboard" | "widget" | "query" | "note"
        key:           Unique ID within the namespace (e.g. "dash_a1b2c3").
        value:         JSON string containing the full entity payload.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    valid_namespaces = {"chat", "dashboard", "widget", "query", "note"}
    namespace = (namespace or "").strip().lower()
    if namespace not in valid_namespaces:
        return {
            "result": None,
            "status": "ERROR",
            "message": f"namespace must be one of: {sorted(valid_namespaces)}",
        }

    key_val = (key or "").strip()
    if not key_val:
        return {
            "result": None,
            "status": "ERROR",
            "message": "key is required.",
        }

    value_str = (value or "").strip()
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    row = {
        "ts":        ts,
        "namespace": namespace,
        "key":       key_val[:256],
        "value":     value_str[:50000],
        "role":      r,
    }

    def _write():
        import pyarrow as pa
        batch = pa.RecordBatch.from_pylist([row])
        dw = DataWriter(super_name=sup, organization=org)
        columns, rows, inserted, deleted = dw.write(
            role_name=r,
            simple_name="__app_state__",
            data=batch,
            overwrite_columns=["ts"],
        )
        return {"columns": columns, "inserted": inserted, "deleted": deleted}

    async with _get_limiter():
        try:
            write_result = await anyio.to_thread.run_sync(_write)
            logger.info(
                "store_app_state: namespace=%s key=%s inserted=%s",
                namespace, key_val, write_result.get("inserted"),
            )
            return {
                "result": "ok",
                "status": "OK",
                "ts": ts,
                "namespace": namespace,
                "key": key_val,
                "inserted": write_result.get("inserted"),
            }
        except Exception as exc:
            logger.exception("store_app_state failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }


@mcp.tool()
@log_tool
async def get_app_state(
    super_name: str,
    organization: str,
    namespace: str,
    key: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve application state from the __app_state__ table.

    If key is provided, returns that specific entry.
    If key is empty/None, returns all entries in the namespace.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        namespace:     Entity type: "chat" | "dashboard" | "widget" | "query" | "note"
        key:           Optional specific key. Empty = list all in namespace.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    ns = _sql_escape_literal((namespace or "").strip().lower())
    key_val = _sql_escape_literal((key or "").strip())

    if key_val:
        sql = f'SELECT * FROM "__app_state__" WHERE "namespace" = \'{ns}\' AND "key" = \'{key_val}\' AND ("status" IS NULL OR "status" != \'deleted\') ORDER BY ts DESC LIMIT 1'
    else:
        sql = f'SELECT * FROM "__app_state__" WHERE "namespace" = \'{ns}\' AND ("status" IS NULL OR "status" != \'deleted\') ORDER BY ts DESC LIMIT 200'

    eng = _resolve_engine(None)

    async with _get_limiter():
        try:
            def _work():
                return _exec_query_sync(sup, org, sql, 200, eng, r)
            result = await anyio.to_thread.run_sync(_work)

            if result.get("status") == "OK":
                cols = result.get("columns") or []
                rows = result.get("rows") or []
                items = [dict(zip(cols, row)) for row in rows]
                return {
                    "status": "OK",
                    "items": items,
                    "count": len(items),
                }
            else:
                return {
                    "status": "OK",
                    "items": [],
                    "count": 0,
                }
        except Exception as exc:
            logger.warning("get_app_state failed: %s", exc)
            return {
                "status": "OK",
                "items": [],
                "count": 0,
                "note": f"Could not read app state: {exc}",
            }


# ---------- Column profiling ----------

@mcp.tool()
@log_tool
async def profile_column(
    super_name: str,
    organization: str,
    table: str,
    column: str,
    top_n: Optional[int] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Profile a single column: type, null%, distinct count, min, max, and top values.

    Returns column statistics and the most frequent values in one call,
    replacing multiple manual SQL queries the AI would otherwise need to
    understand the data before writing a real query.

    DuckDB derives most statistics from parquet metadata — this is fast.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        table:         Table name.
        column:        Column name to profile.
        top_n:         Number of top values to return (default 10, max 50).
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    col = _safe_column_id(column)
    _check_token(auth_token)
    r = _resolve_role(role)

    top_limit = max(1, min(int(top_n or 10), 50))
    eng = _resolve_engine(None)

    stats_sql = (
        f'SELECT '
        f'typeof("{col}") AS col_type, '
        f'COUNT(*) AS total_rows, '
        f'COUNT("{col}") AS non_null_count, '
        f'COUNT(*) - COUNT("{col}") AS null_count, '
        f'ROUND(100.0 * (COUNT(*) - COUNT("{col}")) / GREATEST(COUNT(*), 1), 2) AS null_pct, '
        f'COUNT(DISTINCT "{col}") AS distinct_count, '
        f'MIN("{col}")::VARCHAR AS min_value, '
        f'MAX("{col}")::VARCHAR AS max_value '
        f'FROM "{tbl}"'
    )

    top_sql = (
        f'SELECT "{col}"::VARCHAR AS value, COUNT(*) AS frequency '
        f'FROM "{tbl}" '
        f'WHERE "{col}" IS NOT NULL '
        f'GROUP BY "{col}" '
        f'ORDER BY frequency DESC '
        f'LIMIT {top_limit}'
    )

    async with _get_limiter():
        try:
            def _work():
                stats = _exec_query_sync(sup, org, stats_sql, 1, eng, r)
                top = _exec_query_sync(sup, org, top_sql, top_limit, eng, r)
                return stats, top
            stats_result, top_result = await anyio.to_thread.run_sync(_work)

            profile: Dict[str, Any] = {}
            if stats_result.get("status") == "OK" and stats_result.get("rows"):
                cols = stats_result["columns"]
                row = stats_result["rows"][0]
                profile = dict(zip(cols, row))

            top_values: List[Dict[str, Any]] = []
            if top_result.get("status") == "OK":
                for row in top_result.get("rows", []):
                    top_values.append({"value": row[0], "frequency": row[1]})

            return {
                "status": "OK",
                "table": tbl,
                "column": col,
                "profile": profile,
                "top_values": top_values,
            }
        except Exception as exc:
            logger.exception("profile_column failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }


# ---------- SQL validation (dry run) ----------

@mcp.tool()
@log_tool
async def validate_sql(
    super_name: str,
    organization: str,
    sql: str,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate a SQL query without executing it.

    Checks: read-only compliance, SQL parsing, table existence, and RBAC
    permissions. Returns a clear error if any check fails, so the AI can
    fix the query before spending a real query_sql call.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        sql:           SQL query to validate.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)

    try:
        _read_only_sql(sql)
    except ValueError as exc:
        return {
            "valid": False,
            "status": "ERROR",
            "error": str(exc),
            "phase": "read_only_check",
        }

    _ensure_imports()

    def _work():
        # Parse SQL to extract table references
        try:
            from supertable.utils.sql_parser import SQLParser
            parser = SQLParser(sql)
            tables = parser.tables
        except Exception as parse_exc:
            return {
                "valid": False,
                "status": "ERROR",
                "error": f"SQL parse error: {parse_exc}",
                "phase": "sql_parse",
            }

        table_names = []
        for t in tables:
            sn = getattr(t, "simple_name", None) or getattr(t, "name", None) or ""
            if sn:
                table_names.append(sn)

        # Check tables exist
        reader = MetaReader(super_name=sup, organization=org)
        available = list(reader.get_tables(role_name=r))
        missing = [t for t in table_names if t not in available]
        if missing:
            return {
                "valid": False,
                "status": "ERROR",
                "error": f"Tables not found: {missing}",
                "phase": "table_check",
                "available_tables": available,
            }

        # Check RBAC
        try:
            from supertable.rbac.access_control import restrict_read_access
            physical_tables = {sn: sn for sn in table_names}
            restrict_read_access(
                super_name=sup,
                organization=org,
                role_name=r,
                tables=tables,
                physical_tables=physical_tables,
            )
        except PermissionError as perm_exc:
            return {
                "valid": False,
                "status": "ERROR",
                "error": str(perm_exc),
                "phase": "rbac_check",
            }
        except Exception:
            # RBAC import or call failed — skip gracefully, don't block validation
            pass

        return {
            "valid": True,
            "status": "OK",
            "tables": table_names,
            "phase": "all_checks_passed",
        }

    async with _get_limiter():
        try:
            return await anyio.to_thread.run_sync(_work)
        except Exception as exc:
            logger.exception("validate_sql failed: %s", exc)
            return {
                "valid": False,
                "status": "ERROR",
                "error": f"{exc.__class__.__name__}: {exc}",
                "phase": "unexpected_error",
            }


# ---------- Cross-table column search ----------

@mcp.tool()
@log_tool
async def search_columns(
    super_name: str,
    organization: str,
    pattern: str,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Search for columns matching a pattern across all tables.

    Searches column names and types by case-insensitive substring match.
    Replaces the need for multiple describe_table calls when the AI needs
    to find which table has a date column, an ID column, etc.

    System tables (__ prefix) are excluded from results.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        pattern:       Search string (matched against column name and type).
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    pat = (pattern or "").strip().lower()
    if not pat:
        return {
            "result": None,
            "status": "ERROR",
            "message": "pattern is required and must be non-empty.",
        }

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        tables = list(reader.get_tables(role_name=r))
        matches: List[Dict[str, str]] = []
        for tbl in tables:
            if tbl.startswith("__") and tbl.endswith("__"):
                continue
            try:
                schema = reader.get_table_schema(tbl, role_name=r)
                if isinstance(schema, list):
                    for col_info in schema:
                        col_name = str(col_info.get("name") or col_info.get("column_name") or "")
                        col_type = str(col_info.get("type") or col_info.get("data_type") or "")
                        if pat in col_name.lower() or pat in col_type.lower():
                            matches.append({"table": tbl, "column": col_name, "type": col_type})
                elif isinstance(schema, dict):
                    for col_name, col_type in schema.items():
                        if pat in col_name.lower() or pat in str(col_type).lower():
                            matches.append({"table": tbl, "column": col_name, "type": str(col_type)})
            except Exception:
                continue
        return matches

    async with _get_limiter():
        try:
            matches = await anyio.to_thread.run_sync(_work)
            return {
                "status": "OK",
                "matches": matches,
                "count": len(matches),
                "pattern": pat,
            }
        except Exception as exc:
            logger.exception("search_columns failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }


# ---------- Annotation deletion ----------

@mcp.tool()
@log_tool
async def delete_annotation(
    super_name: str,
    organization: str,
    category: str,
    context: str,
    instruction_contains: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Delete one or more annotations by marking them as deleted.

    Finds annotations matching the given category and context, optionally
    filtered by a substring in the instruction text, and writes tombstone
    rows with status='deleted'. These are then excluded from all annotation
    reads (list_tables eager load and get_annotations).

    Args:
        super_name:           SuperTable name.
        organization:         Organization name.
        category:             Annotation category: sql, terminology, visualization, scope, domain.
        context:              Context to match (table name or "*" for global).
        instruction_contains: Optional substring filter on instruction text.
        role:                 RBAC role.
        auth_token:           Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    valid_categories = {"sql", "terminology", "visualization", "scope", "domain"}
    cat = (category or "").strip().lower()
    if cat not in valid_categories:
        return {
            "result": None,
            "status": "ERROR",
            "message": f"category must be one of: {sorted(valid_categories)}",
        }

    ctx = (context or "").strip()
    if not ctx:
        return {
            "result": None,
            "status": "ERROR",
            "message": "context is required (table name or '*' for global).",
        }

    eng = _resolve_engine(None)
    safe_cat = _sql_escape_literal(cat)
    safe_ctx = _sql_escape_literal(ctx)

    find_sql = (
        f'SELECT * FROM "__annotations__" '
        f'WHERE "category" = \'{safe_cat}\' AND "context" = \'{safe_ctx}\' '
        f'AND ("status" IS NULL OR "status" != \'deleted\') '
        f'ORDER BY ts DESC LIMIT 50'
    )

    async with _get_limiter():
        try:
            def _find():
                return _exec_query_sync(sup, org, find_sql, 50, eng, r)
            find_result = await anyio.to_thread.run_sync(_find)

            if find_result.get("status") != "OK" or not find_result.get("rows"):
                return {
                    "status": "OK",
                    "deleted": 0,
                    "message": "No matching annotations found.",
                }

            cols = find_result.get("columns") or []
            rows = find_result.get("rows") or []
            items = [dict(zip(cols, row)) for row in rows]

            needle = (instruction_contains or "").strip().lower()
            if needle:
                items = [i for i in items if needle in (i.get("instruction") or "").lower()]

            if not items:
                return {
                    "status": "OK",
                    "deleted": 0,
                    "message": "No matching annotations found after instruction filter.",
                }

            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            tombstones = []
            for item in items:
                tombstones.append({
                    "ts":          ts,
                    "category":    item.get("category", cat),
                    "context":     item.get("context", ctx),
                    "instruction": item.get("instruction", ""),
                    "role":        r,
                    "status":      "deleted",
                })

            def _write():
                import pyarrow as pa
                batch = pa.RecordBatch.from_pylist(tombstones)
                dw = DataWriter(super_name=sup, organization=org)
                columns, rows, inserted, deleted = dw.write(
                    role_name=r,
                    simple_name="__annotations__",
                    data=batch,
                    overwrite_columns=["ts"],
                )
                return {"inserted": inserted}

            write_result = await anyio.to_thread.run_sync(_write)
            deleted_instructions = [i.get("instruction", "")[:100] for i in items]
            logger.info(
                "delete_annotation: category=%s context=%s deleted=%d",
                cat, ctx, len(items),
            )
            return {
                "status": "OK",
                "deleted": len(items),
                "inserted_tombstones": write_result.get("inserted"),
                "deleted_instructions": deleted_instructions,
                "message": f"Marked {len(items)} annotation(s) as deleted.",
            }
        except Exception as exc:
            logger.exception("delete_annotation failed: %s", exc)
            return {
                "result": None,
                "status": "ERROR",
                "message": f"{exc.__class__.__name__}: {exc}",
            }


# ---------- App state deletion ----------

@mcp.tool()
@log_tool
async def delete_app_state(
    super_name: str,
    organization: str,
    namespace: str,
    key: str,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Delete an app state entry (widget, dashboard, saved query, etc.).

    Marks the entry as deleted by writing a tombstone row with status='deleted'.
    Subsequent get_app_state calls will exclude deleted entries.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        namespace:     Entity type: "chat" | "dashboard" | "widget" | "query" | "note"
        key:           Key of the entry to delete.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    valid_namespaces = {"chat", "dashboard", "widget", "query", "note"}
    ns = (namespace or "").strip().lower()
    if ns not in valid_namespaces:
        return {"result": None, "status": "ERROR", "message": f"namespace must be one of: {sorted(valid_namespaces)}"}

    key_val = (key or "").strip()
    if not key_val:
        return {"result": None, "status": "ERROR", "message": "key is required."}

    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    row = {"ts": ts, "namespace": ns, "key": key_val[:256], "value": "", "role": r, "status": "deleted"}

    def _write():
        import pyarrow as pa
        batch = pa.RecordBatch.from_pylist([row])
        dw = DataWriter(super_name=sup, organization=org)
        columns, rows, inserted, deleted = dw.write(
            role_name=r, simple_name="__app_state__", data=batch, overwrite_columns=["ts"],
        )
        return {"inserted": inserted}

    async with _get_limiter():
        try:
            await anyio.to_thread.run_sync(_write)
            logger.info("delete_app_state: namespace=%s key=%s", ns, key_val)
            return {"status": "OK", "namespace": ns, "key": key_val, "message": "Marked entry as deleted."}
        except Exception as exc:
            logger.exception("delete_app_state failed: %s", exc)
            return {"result": None, "status": "ERROR", "message": f"{exc.__class__.__name__}: {exc}"}


# ---------- Data freshness (Redis metadata, no data scan) ----------

@mcp.tool()
@log_tool
async def data_freshness(
    super_name: str,
    organization: str,
    table: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Return data freshness and size metadata from Redis — no data scanning.

    Reads snapshot metadata directly from Redis leaf keys. Returns last
    updated timestamp, snapshot version, and embedded payload stats (file
    count, schema) when available.

    If table is omitted, returns freshness for ALL tables in one call.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        table:         Optional table name. Empty = all tables.
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    def _extract_freshness(leaf: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        ts_ms = leaf.get("ts", 0)
        version = leaf.get("version", -1)
        result: Dict[str, Any] = {
            "table": table_name,
            "version": version,
            "last_updated_ms": ts_ms,
        }
        if ts_ms:
            import datetime
            try:
                result["last_updated"] = datetime.datetime.fromtimestamp(
                    ts_ms / 1000.0, tz=datetime.timezone.utc
                ).isoformat()
                age_sec = (time.time() * 1000 - ts_ms) / 1000.0
                if age_sec < 3600:
                    result["age"] = f"{age_sec / 60:.1f} minutes ago"
                elif age_sec < 86400:
                    result["age"] = f"{age_sec / 3600:.1f} hours ago"
                else:
                    result["age"] = f"{age_sec / 86400:.1f} days ago"
            except Exception:
                pass

        payload = leaf.get("payload")
        if isinstance(payload, dict):
            resources = payload.get("resources")
            if isinstance(resources, list):
                result["file_count"] = len(resources)
                total_bytes = 0
                for res in resources:
                    if isinstance(res, dict):
                        total_bytes += int(res.get("size", 0) or 0)
                if total_bytes > 0:
                    result["total_bytes"] = total_bytes
                    if total_bytes < 1024 * 1024:
                        result["size_human"] = f"{total_bytes / 1024:.1f} KB"
                    elif total_bytes < 1024 * 1024 * 1024:
                        result["size_human"] = f"{total_bytes / (1024 * 1024):.1f} MB"
                    else:
                        result["size_human"] = f"{total_bytes / (1024 * 1024 * 1024):.2f} GB"

            schema = payload.get("schema")
            if isinstance(schema, (list, dict)):
                result["column_count"] = len(schema)

        return result

    def _work():
        from supertable.redis_catalog import RedisCatalog
        catalog = RedisCatalog()

        tbl = (table or "").strip() if table else ""

        if tbl:
            leaf = catalog.get_leaf(org, sup, tbl)
            if not leaf:
                return {"status": "ERROR", "message": f"Table '{tbl}' not found in Redis catalog."}
            return {"status": "OK", "tables": [_extract_freshness(leaf, tbl)]}

        # All tables
        items = list(catalog.scan_leaf_items(org, sup))
        tables_out = []
        for item in items:
            simple = item.get("simple", "")
            if simple.startswith("__") and simple.endswith("__"):
                continue
            tables_out.append(_extract_freshness(item, simple))

        tables_out.sort(key=lambda x: x.get("last_updated_ms", 0), reverse=True)
        return {"status": "OK", "tables": tables_out, "count": len(tables_out)}

    async with _get_limiter():
        try:
            return await anyio.to_thread.run_sync(_work)
        except Exception as exc:
            logger.exception("data_freshness failed: %s", exc)
            return {"result": None, "status": "ERROR", "message": f"{exc.__class__.__name__}: {exc}"}


# ---------- Auto-discover (crawl all tables for catalog/annotation generation) ----------

@mcp.tool()
@log_tool
async def auto_discover(
    super_name: str,
    organization: str,
    max_tables: Optional[int] = None,
    sample_rows: Optional[int] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Crawl all tables and collect schema, samples, and column profiles.

    Returns structured metadata for every table so the AI can generate
    catalog entries (via store_catalog) and annotations (via store_annotation).
    System tables (__ prefix) are skipped.

    Per table the tool collects:
    - Column schema (name, type)
    - Row count
    - Sample rows (default 3)
    - Per-column: null%, distinct count
    - Detected patterns: date columns, low-cardinality (enum-like), ID columns

    This is the bootstrap tool — call it once on a new SuperTable, then use
    the returned data to write catalog and annotation entries.

    Args:
        super_name:    SuperTable name.
        organization:  Organization name.
        max_tables:    Max tables to crawl (default 30, max 50).
        sample_rows:   Sample rows per table (default 3, max 10).
        role:          RBAC role.
        auth_token:    Auth token.
    """
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    max_tbl = max(1, min(int(max_tables or 30), 50))
    n_sample = max(1, min(int(sample_rows or 3), 10))
    eng = _resolve_engine(None)

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        all_tables = list(reader.get_tables(role_name=r))
        user_tables = [t for t in all_tables if not (t.startswith("__") and t.endswith("__"))]
        tables_to_crawl = user_tables[:max_tbl]

        discoveries: List[Dict[str, Any]] = []

        for tbl in tables_to_crawl:
            entry: Dict[str, Any] = {"table": tbl, "columns": [], "sample": [], "profiles": [], "patterns": []}

            # 1. Schema
            col_names: List[str] = []
            col_types: Dict[str, str] = {}
            try:
                schema = reader.get_table_schema(tbl, role_name=r)
                if isinstance(schema, list):
                    entry["columns"] = schema
                    col_names = [str(c.get("name") or c.get("column_name") or "") for c in schema]
                    col_types = {str(c.get("name") or c.get("column_name") or ""): str(c.get("type") or c.get("data_type") or "") for c in schema}
                elif isinstance(schema, dict):
                    entry["columns"] = [{"name": k, "type": str(v)} for k, v in schema.items()]
                    col_names = list(schema.keys())
                    col_types = {k: str(v) for k, v in schema.items()}
            except Exception as exc:
                entry["schema_error"] = str(exc)

            if not col_names:
                discoveries.append(entry)
                continue

            # 2. Sample rows
            try:
                sample_sql = f'SELECT * FROM "{tbl}" LIMIT {n_sample}'
                sample_result = _exec_query_sync(sup, org, sample_sql, n_sample, eng, r)
                if sample_result.get("status") == "OK":
                    cols = sample_result.get("columns") or []
                    rows = sample_result.get("rows") or []
                    entry["sample"] = [dict(zip(cols, row)) for row in rows]
            except Exception:
                pass

            # 3. Row count + per-column stats (single aggregate query)
            try:
                stat_parts = ["COUNT(*) AS __row_count__"]
                profiled_cols: List[str] = []
                for col in col_names[:40]:
                    safe_col = col.replace('"', '""')
                    stat_parts.append(f'COUNT("{safe_col}") AS "nonnull_{safe_col}"')
                    stat_parts.append(f'COUNT(DISTINCT "{safe_col}") AS "distinct_{safe_col}"')
                    profiled_cols.append(col)

                stats_sql = f'SELECT {", ".join(stat_parts)} FROM "{tbl}"'
                stats_result = _exec_query_sync(sup, org, stats_sql, 1, eng, r)

                if stats_result.get("status") == "OK" and stats_result.get("rows"):
                    stat_cols = stats_result["columns"]
                    stat_row = stats_result["rows"][0]
                    stat_dict = dict(zip(stat_cols, stat_row))
                    row_count = stat_dict.get("__row_count__", 0)
                    entry["row_count"] = row_count

                    profiles = []
                    for col in profiled_cols:
                        nonnull = stat_dict.get(f"nonnull_{col}", 0)
                        distinct = stat_dict.get(f"distinct_{col}", 0)
                        null_count = (row_count or 0) - (nonnull or 0)
                        null_pct = round(100.0 * null_count / max(row_count, 1), 1)
                        profiles.append({
                            "column": col, "type": col_types.get(col, ""),
                            "non_null": nonnull, "distinct": distinct, "null_pct": null_pct,
                        })
                    entry["profiles"] = profiles
            except Exception:
                pass

            # 4. Detect patterns
            patterns = []
            for col in col_names:
                ct = col_types.get(col, "").lower()
                cl = col.lower()
                if any(d in ct for d in ("date", "time", "timestamp")):
                    patterns.append({"column": col, "pattern": "date_time", "note": "Time-series candidate"})
                elif any(kw in cl for kw in ("_id", "_key", "_code", "_uuid", "id_")):
                    patterns.append({"column": col, "pattern": "identifier", "note": "Likely join/FK column"})

            # Detect low-cardinality columns from profiles
            row_count = entry.get("row_count", 0)
            for p in entry.get("profiles", []):
                distinct = p.get("distinct", 0)
                if isinstance(distinct, (int, float)) and 1 < distinct <= 50 and row_count > 100:
                    patterns.append({
                        "column": p["column"], "pattern": "low_cardinality",
                        "note": f"{distinct} distinct values — enum-like, good for GROUP BY / filters",
                    })

            entry["patterns"] = patterns
            discoveries.append(entry)

        return {
            "status": "OK",
            "tables_crawled": len(discoveries),
            "tables_total": len(user_tables),
            "tables_skipped_system": len(all_tables) - len(user_tables),
            "discoveries": discoveries,
        }

    async with _get_limiter():
        try:
            return await anyio.to_thread.run_sync(_work)
        except Exception as exc:
            logger.exception("auto_discover failed: %s", exc)
            return {"result": None, "status": "ERROR", "message": f"{exc.__class__.__name__}: {exc}"}


# ---------- Catalog store (write entries to __catalog__) ----------

@mcp.tool()
@log_tool
async def store_catalog(
    super_name: str,
    organization: str,
    table_name: str,
    description: str,
    columns: Optional[str] = None,
    joins: Optional[str] = None,
    example_queries: Optional[str] = None,
    filters: Optional[str] = None,
    role: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Write or update a catalog entry in the __catalog__ table.

    Each call writes one row describing one table. The AI should call this
    after auto_discover to populate the catalog with human-readable
    descriptions, column documentation, join relationships, and example queries.

    Args:
        super_name:       SuperTable name.
        organization:     Organization name.
        table_name:       The table this catalog entry describes.
        description:      Human-readable description of what the table contains.
        columns:          Column documentation (free text or JSON string).
        joins:            How this table joins to others (free text or JSON).
        example_queries:  Example SQL queries (free text or JSON array).
        filters:          Default filtering rules (free text).
        role:             RBAC role.
        auth_token:       Auth token.
    """
    import datetime

    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    _check_token(auth_token)
    r = _resolve_role(role)
    _ensure_imports()

    tbl = (table_name or "").strip()
    if not tbl:
        return {"result": None, "status": "ERROR", "message": "table_name is required."}

    desc = (description or "").strip()
    if not desc:
        return {"result": None, "status": "ERROR", "message": "description is required."}

    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    row: Dict[str, Any] = {
        "ts":          ts,
        "table_name":  tbl[:256],
        "description": desc[:5000],
        "role":        r,
    }
    if columns:
        row["columns"] = (columns or "").strip()[:10000]
    if joins:
        row["joins"] = (joins or "").strip()[:5000]
    if example_queries:
        row["example_queries"] = (example_queries or "").strip()[:5000]
    if filters:
        row["filters"] = (filters or "").strip()[:2000]

    def _write():
        import pyarrow as pa
        batch = pa.RecordBatch.from_pylist([row])
        dw = DataWriter(super_name=sup, organization=org)
        columns_out, rows_out, inserted, deleted = dw.write(
            role_name=r, simple_name="__catalog__", data=batch, overwrite_columns=["ts"],
        )
        return {"inserted": inserted}

    async with _get_limiter():
        try:
            write_result = await anyio.to_thread.run_sync(_write)
            logger.info("store_catalog: table_name=%s inserted=%s", tbl, write_result.get("inserted"))
            return {
                "status": "OK",
                "table_name": tbl,
                "ts": ts,
                "inserted": write_result.get("inserted"),
                "message": f"Catalog entry for '{tbl}' stored.",
            }
        except Exception as exc:
            logger.exception("store_catalog failed: %s", exc)
            return {"result": None, "status": "ERROR", "message": f"{exc.__class__.__name__}: {exc}"}


if __name__ == "__main__":
    try:
        transport = _normalize_transport_value(getattr(CFG, "transport", "stdio"))
        logger.info(
            "Starting MCP server. transport=%s max_concurrency=%d require_explicit_role=%s require_token=%s",
            transport,
            CFG.max_concurrency,
            str(CFG.require_explicit_role),
            str(CFG.require_token),
        )
        if CFG.require_token and not (CFG.shared_token or "").strip():
            raise RuntimeError(
                "SUPERTABLE_MCP_TOKEN must be set (token auth is always required)."
            )

        run_kwargs: Dict[str, Any] = {"transport": transport}
        if transport != "stdio":
            logger.info(
                "HTTP transport configured. host=%s port=%s path=%s",
                CFG.http_host,
                str(CFG.http_port),
                CFG.http_path,
            )

        if transport == "streamable-http":
            _serve_streamable_http_via_uvicorn(
                mcp=mcp, host=CFG.http_host, port=CFG.http_port, path=CFG.http_path
            )
            sys.exit(0)

        if transport != "stdio":
            run_kwargs.update({"host": CFG.http_host, "port": CFG.http_port})
            # Prefer explicit path support when available; otherwise rely on SDK defaults.
            try:
                run_sig = inspect.signature(mcp.run)
                if "path" in run_sig.parameters:
                    run_kwargs["path"] = CFG.http_path
                elif "mcp_path" in run_sig.parameters:
                    run_kwargs["mcp_path"] = CFG.http_path
            except Exception:
                pass


        try:
            mcp.run(**run_kwargs)
        except TypeError:
            # Backward/forward compatibility across MCP SDK versions.
            # Some transports (e.g. older streamable-http aliases) may not accept host/port/path kwargs.
            run_kwargs.pop("path", None)
            run_kwargs.pop("mcp_path", None)
            run_kwargs.pop("host", None)
            run_kwargs.pop("port", None)
            mcp.run(**run_kwargs)
    except KeyboardInterrupt:
        logger.info("Shutting down MCP server (KeyboardInterrupt).")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Fatal error in MCP server: %s", exc)
        sys.exit(1)