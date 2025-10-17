#!/usr/bin/env python3
"""
mcp_server.py — Supertable MCP server (simple, robust, production-ready)

Key points:
- Strict input validation (org/super/table/user_hash).
- Read-only SQL enforcement.
- Clear, consistent envelopes for all tools (errors included).
- Concurrency limiting + per-request timeout with AnyIO.
- Minimal but structured INFO logs for observability.
- Correct integration with MetaReader (get_tables, get_table_schema, get_table_stats, get_super_meta)
  and module-level list_supers/list_tables + DataReader.query_sql.
"""

from __future__ import annotations

import inspect
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------- Import path: allow `from supertable.*` ----------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, os.pardir))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)-8s - %(message)s",
)
logger = logging.getLogger("supertable.mcp")

# ---------- MCP SDK ----------
try:
    from mcp.server.fastmcp import FastMCP  # modern SDK
except Exception:
    try:
        from mcp import FastMCP  # legacy fallback
    except Exception:
        logger.error("MCP SDK not installed. Run: pip install mcp")
        raise

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

def _ensure_imports():
    global MetaReader, engine_enum, data_query_sql, list_supers_fn, list_tables_fn
    if MetaReader is None or engine_enum is None or data_query_sql is None or list_supers_fn is None or list_tables_fn is None:
        # Class + enum + module functions
        from supertable.meta_reader import MetaReader as _MR, list_supers as _LS, list_tables as _LT  # :contentReference[oaicite:2]{index=2}
        from supertable.data_reader import engine as _ENG, query_sql as _DQ  # :contentReference[oaicite:3]{index=3}
        MetaReader, engine_enum, data_query_sql = _MR, _ENG, _DQ
        list_supers_fn, list_tables_fn = _LS, _LT

# ---------- Helpers ----------
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
USER_HASH_RE = re.compile(r"^[a-fA-F0-9]{32,64}$")

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_hashes(name: str) -> Optional[Set[str]]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return None
    return {s.strip().lower() for s in v.split(",") if s.strip()}

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

def _validate_user_hash(u: str) -> str:
    if not isinstance(u, str) or not USER_HASH_RE.match(u):
        raise ValueError("Invalid user_hash format (expect 32/64 hex).")
    return u.lower()

def _read_only_sql(sql: str) -> None:
    s = (sql or "").strip().lower()
    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Only SELECT (or WITH … SELECT) statements are allowed.")
    forbidden = ("insert", "update", "delete", "merge", "create", "alter", "drop", "truncate", "grant", "revoke")
    if any(tok in s for tok in forbidden):
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
    version: str = "1.2.0"
    default_engine: str = os.getenv("SUPERTABLE_DEFAULT_ENGINE", "AUTO")
    default_limit: int = int(os.getenv("SUPERTABLE_DEFAULT_LIMIT", "200"))
    max_limit: int = int(os.getenv("SUPERTABLE_MAX_LIMIT", "5000"))
    default_query_timeout_sec: float = float(os.getenv("SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC", "60"))
    max_concurrency: int = int(os.getenv("SUPERTABLE_MAX_CONCURRENCY", "6"))
    require_explicit_user_hash: bool = _env_bool("SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH", True)
    allowed_user_hashes: Optional[Set[str]] = field(default_factory=lambda: _env_hashes("SUPERTABLE_ALLOWED_USER_HASHES"))
    allow_sysadmin_default: bool = _env_bool("SUPERTABLE_ALLOW_SYSADMIN_DEFAULT", False)

CFG = Config()

# ---------- MCP app ----------
def _build_mcp(name: str, version: str) -> FastMCP:
    try:
        sig = inspect.signature(FastMCP)
        if "version" in sig.parameters:
            return FastMCP(name, version=version)  # type: ignore[arg-type]
        return FastMCP(name)
    except Exception:
        return FastMCP(name)

mcp = _build_mcp(CFG.name, CFG.version)
limiter = anyio.CapacityLimiter(CFG.max_concurrency)

# ---------- Auth helpers ----------
def _allowed_user_hash(u: str) -> bool:
    return CFG.allowed_user_hashes is None or u.lower() in CFG.allowed_user_hashes

def _resolve_user(user_hash: Optional[str]) -> str:
    if CFG.require_explicit_user_hash:
        if not user_hash:
            raise PermissionError("user_hash is required by server policy.")
        u = _validate_user_hash(user_hash)
        if not _allowed_user_hash(u):
            raise PermissionError("user_hash not permitted by server policy.")
        return u
    u = user_hash or os.getenv("SUPERTABLE_TEST_USER_HASH") or ""
    if not u:
        raise PermissionError("user_hash missing and no default configured.")
    return _validate_user_hash(u)

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
            if isinstance(out, dict):
                r = out.get("result", out)
                if isinstance(r, dict) and {"columns", "rows"} <= r.keys():
                    summary = f"columns={len(r['columns'])} {_summarize_rows(r['rows'])} status={r.get('status')}"
                else:
                    try:
                        keys = list(r.keys())[:6] if isinstance(r, dict) else []
                    except Exception:
                        keys = []
                    summary = f"keys={keys}"
            logger.info("← tool %s response (%0.2f ms) %s", fn, dt, summary)
            return out
        except Exception as exc:
            logger.exception("✖ tool %s error: %s", fn, exc)
            if fn == "query_sql":
                # Return consistent envelope on query errors
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
            raise
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
            "require_explicit_user_hash": CFG.require_explicit_user_hash,
            "allow_sysadmin_default": CFG.allow_sysadmin_default,
        }
    }

@mcp.tool()
@log_tool
async def whoami(user_hash: Optional[str] = None) -> Dict[str, Any]:
    u = _resolve_user(user_hash)
    return {"result": {"user_hash": u}}

@mcp.tool()
@log_tool
async def list_supers(organization: str) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    _ensure_imports()
    try:
        supers = list_supers_fn(org)  # module-level helper  :contentReference[oaicite:4]{index=4}
    except Exception as e:
        logger.exception("list_supers failed: %s", e)
        supers = []
    return {"result": supers}

@mcp.tool()
@log_tool
async def list_tables(super_name: str, organization: str, user_hash: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    u = _resolve_user(user_hash)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)  # class helper  :contentReference[oaicite:5]{index=5}
        return list(reader.get_tables(user_hash=u))            # correct method: get_tables

    async with limiter:
        tables = await anyio.to_thread.run_sync(_work)
    return {"result": tables}

@mcp.tool()
@log_tool
async def describe_table(super_name: str, organization: str, table: str, user_hash: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    u = _resolve_user(user_hash)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_table_schema(tbl, user_hash=u)

    async with limiter:
        schema = await anyio.to_thread.run_sync(_work)
    return {"result": schema}

@mcp.tool()
@log_tool
async def get_table_stats(super_name: str, organization: str, table: str, user_hash: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    tbl = _safe_id(table, "table")
    u = _resolve_user(user_hash)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_table_stats(tbl, user_hash=u)

    async with limiter:
        stats = await anyio.to_thread.run_sync(_work)
    return {"result": stats}

@mcp.tool()
@log_tool
async def get_super_meta(super_name: str, organization: str, user_hash: Optional[str] = None) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    u = _resolve_user(user_hash)
    _ensure_imports()

    def _work():
        reader = MetaReader(super_name=sup, organization=org)
        return reader.get_super_meta(user_hash=u)

    async with limiter:
        meta = await anyio.to_thread.run_sync(_work)
    return {"result": meta}

def _exec_query_sync(super_name: str, organization: str, sql: str, limit_n: int, eng: Any, user_hash: str) -> Dict[str, Any]:
    """
    Runs the module-level query_sql and enforces `limit` server-side (slice rows)
    to keep the server robust even if downstream ignores it. :contentReference[oaicite:6]{index=6}
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
        user_hash=user_hash,
    )

    # Enforce limit here (in case downstream didn't)
    rows_limited = rows[:limit_n]
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

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
    user_hash: Optional[str] = None,
) -> Dict[str, Any]:
    org = _safe_id(organization, "organization")
    sup = _safe_id(super_name, "super_name")
    u = _resolve_user(user_hash)
    _read_only_sql(sql)

    limit_n = _clamp_limit(limit, CFG.default_limit, CFG.max_limit)
    eng = _resolve_engine(engine)
    timeout = float(query_timeout_sec or CFG.default_query_timeout_sec)

    async with limiter:
        try:
            if hasattr(anyio, "fail_after"):
                with anyio.fail_after(timeout):
                    out = await anyio.to_thread.run_sync(
                        _exec_query_sync, sup, org, sql, limit_n, eng, u
                    )
            else:
                result = None
                with anyio.move_on_after(timeout) as scope:
                    result = await anyio.to_thread.run_sync(
                        _exec_query_sync, sup, org, sql, limit_n, eng, u
                    )
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

# ---------- Entrypoint ----------
if __name__ == "__main__":
    try:
        logger.info(
            "Starting MCP server (stdio). max_concurrency=%d require_explicit_user_hash=%s",
            CFG.max_concurrency,
            str(CFG.require_explicit_user_hash),
        )
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Shutting down MCP server (KeyboardInterrupt).")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Fatal error in MCP server: %s", exc)
        sys.exit(1)
