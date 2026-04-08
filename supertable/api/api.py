# route: supertable.api.api
"""
Unified API endpoint registry for the Reflection UI.

Every HTTP endpoint (GET / POST / PUT / DELETE) that was previously
scattered across common.py, execute.py, tables.py, ingestion.py,
security.py, monitoring.py, and engine.py is now registered here.

Business-logic helpers remain in their original modules and are
imported where needed.
"""
from __future__ import annotations

import json
import logging
import os

from supertable.config.settings import settings as _cfg
import re
import time
import asyncio
import tempfile
import uuid
from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, Depends, HTTPException, Query, Request, Form, File, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared symbols — all come from common.py (single source of truth)
# ---------------------------------------------------------------------------

from supertable.server_common import (
    router,
    settings,
    redis_client,
    catalog,
    _is_authorized,
    _no_store,
    _get_provided_token,
    _fmt_ts,
    _required_token,
    _set_session_cookie,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
    logged_in_guard_api,
    admin_guard_api,
    is_superuser,
    list_users,
    list_roles,
)


# ---------------------------------------------------------------------------
# Helpers imported from original modules (business logic stays in place)
# ---------------------------------------------------------------------------

# -- execute helpers --
from supertable.services.execute import (
    _clean_sql_query,
    _apply_limit_safely,
    _sanitize_for_json,
)

# -- ingestion helpers --
from supertable.services.ingestion import (
    _STAGING_NAME_RE,
    _staging_base_dir,
    _staging_index_path,
    _pipe_index_path,
    _pipe_key,
    _redis_json_load,
    _read_json_if_exists,
    _write_json_atomic,
    _flatten_tree_leaves,
    _get_staging_names,
    _load_pipe_index,
    _get_pipes,
    _scan_pipes,
    _make_redis_helpers,
)

# -- security helpers --
from supertable.services.security import (
    _list_roles as _sec_list_roles,
    _get_role as _sec_get_role,
    _create_role as _sec_create_role,
    _update_role as _sec_update_role,
    _delete_role as _sec_delete_role,
    _list_users_redis,
    _list_endpoints,
    _create_endpoint,
    _update_endpoint,
    _regenerate_endpoint_token,
    _delete_endpoint,
    validate_username,
)

# -- monitoring helpers --
from supertable.services.monitoring import (
    _read_monitoring_list,
    read_lock_state,
    read_error_feed,
    compute_timeseries,
    compute_catalog_stats,
)
from supertable.monitoring_writer import MonitoringWriter

# -- audit --
from supertable.audit import emit as _audit, EventCategory, Actions, Severity, Outcome, make_detail, audit_context


# ---------------------------------------------------------------------------
# Lazy-import helpers (formerly closures inside attach_execute_routes)
# ---------------------------------------------------------------------------

def _get_data_reader():
    try:
        from supertable.data_reader import DataReader as _DR
        return _DR
    except Exception:
        from data_reader import DataReader as _DR  # type: ignore
        return _DR


def _get_meta_reader():
    try:
        from supertable.meta_reader import MetaReader as _MR
        return _MR
    except Exception:
        from meta_reader import MetaReader as _MR  # type: ignore
        return _MR


# ---------------------------------------------------------------------------
# Tables helpers (formerly closures inside attach_tables_routes)
# ---------------------------------------------------------------------------

from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

try:
    from supertable.meta_reader import MetaReader  # type: ignore
except Exception:  # pragma: no cover
    from meta_reader import MetaReader  # type: ignore


def _resolve_role(request: Request, role_name: str = "", user_hash: str = "") -> str:
    """Resolve role: prefer explicit param, then session cookie, then fallbacks.

    Never returns empty string — if all else fails, returns 'superadmin'
    for superuser sessions or the first role from the user's roles list.
    """
    role = (role_name or user_hash or "").strip()
    if role:
        return role
    sess = get_session(request) or {}
    role = (sess.get("role_name") or "").strip()
    if role:
        return role
    # Superuser sessions always have superadmin access
    if sess.get("is_superuser"):
        return "superadmin"
    # Try the first role from the user's roles list
    roles_list = sess.get("roles") or []
    if roles_list and isinstance(roles_list, list):
        first = str(roles_list[0]).strip()
        if first:
            return first
    # Final fallback: if auth is via admin token (no session cookie),
    # is_superuser() checks the token header directly.
    if is_superuser(request):
        return "superadmin"
    # Last resort so RBAC layer gets something to check rather than empty string
    return "superadmin"


def _get_redis_items(pattern: str) -> List[str]:
    rc = RedisCatalog()
    try:
        items: List[str] = []
        cursor = 0
        while True:
            cursor, keys = rc.r.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                if isinstance(key, (bytes, bytearray)):
                    items.append(key.decode("utf-8"))
                else:
                    items.append(str(key))
            if cursor == 0:
                break
        return items
    except Exception as e:
        logger.error("Error scanning Redis keys for pattern %s: %s", pattern, e)
        return []


def list_supers(organization: str) -> List[str]:
    organization = (organization or "").strip()
    if not organization:
        return []
    pattern = f"supertable:{organization}:*:meta:root"
    items = _get_redis_items(pattern)
    supers: List[str] = []
    for item in items:
        parts = str(item).split(":")
        if len(parts) >= 3:
            supers.append(parts[2])
    return sorted({s for s in supers if s})


def list_supers_with_flags(organization: str) -> List[Dict[str, Any]]:
    """Return supers with read_only flag from root meta."""
    organization = (organization or "").strip()
    if not organization:
        return []
    rc = RedisCatalog()
    pattern = f"supertable:{organization}:*:meta:root"
    items = _get_redis_items(pattern)
    seen: Dict[str, Dict[str, Any]] = {}
    for item in items:
        parts = str(item).split(":")
        if len(parts) < 3:
            continue
        sup_name = parts[2]
        if sup_name in seen:
            continue
        read_only = False
        clone_type = None
        cloned_from = None
        try:
            raw = rc.r.get(item)
            if raw:
                root = json.loads(raw)
                read_only = bool(root.get("read_only"))
                clone_type = root.get("clone_type")
                cloned_from = root.get("cloned_from")
        except Exception:
            pass
        seen[sup_name] = {
            "name": sup_name,
            "read_only": read_only,
            "clone_type": clone_type,
            "cloned_from": cloned_from,
        }
    return sorted(seen.values(), key=lambda x: x["name"])


# ---------------------------------------------------------------------------
# Ingestion: build redis helper closures bound to the shared redis_client
# ---------------------------------------------------------------------------

_rh = _make_redis_helpers(redis_client)
_redis_list_stagings = _rh["redis_list_stagings"]
_redis_get_staging_meta = _rh["redis_get_staging_meta"]
_redis_list_pipes = _rh["redis_list_pipes"]
_redis_get_pipe_meta = _rh["redis_get_pipe_meta"]
_redis_upsert_staging_meta = _rh["redis_upsert_staging_meta"]
_redis_upsert_pipe_meta = _rh["redis_upsert_pipe_meta"]
_redis_delete_pipe_meta = _rh["redis_delete_pipe_meta"]
_redis_delete_staging_cascade = _rh["redis_delete_staging_cascade"]


# ---------------------------------------------------------------------------
# Common.py env helpers
# ---------------------------------------------------------------------------


def _get_org_from_env_fallback() -> str:
    return (settings.SUPERTABLE_ORGANIZATION or "").strip()


# ---------------------------------------------------------------------------
# Token catalog helpers (imported from common.py's module scope)
# ---------------------------------------------------------------------------

from supertable.server_common import (
    _catalog_list_tokens,
    _catalog_create_token,
    _catalog_delete_token,
)


# ---------------------------------------------------------------------------
# MetaReader cache (imported from server_common.py)
# ---------------------------------------------------------------------------

from supertable.server_common import _get_meta_reader as _common_get_meta_reader


# ---------------------------------------------------------------------------
# Simple Redis-backed rate limiter for mutation endpoints
# ---------------------------------------------------------------------------

def _check_rate_limit(request: Request, action: str, max_per_minute: int = 30) -> None:
    """Raise HTTP 429 if the caller exceeds the per-minute rate limit.

    Key is scoped per session + action to prevent abuse from a single admin.
    Lightweight: single INCR + conditional EXPIRE, no Lua scripts.
    """
    try:
        session_id = ""
        if hasattr(request, "cookies"):
            session_id = request.cookies.get("session_id", "")
        if not session_id:
            session_id = request.client.host if request.client else "unknown"
        rate_key = f"supertable:ratelimit:{session_id}:{action}"
        count = redis_client.incr(rate_key)
        if count == 1:
            redis_client.expire(rate_key, 60)
        if count > max_per_minute:
            raise HTTPException(status_code=429, detail=f"Rate limit exceeded for {action}. Max {max_per_minute}/minute.")
    except HTTPException:
        raise
    except Exception:
        pass  # Never block on rate limiter failures


# ═══════════════════════════════════════════════════════════════════════════
#
#   ENDPOINT REGISTRATIONS  —  grouped by domain
#
# ═══════════════════════════════════════════════════════════════════════════


# ───────────────────────────── Root / Health ──────────────────────────────

@router.get("/api/v1/health", response_class=PlainTextResponse)
def healthz():
    """Shallow health check — Redis ping only. Use /healthz/deep for full check."""
    try:
        pong = redis_client.ping()
        return "ok" if pong else "not-ok"
    except Exception as e:
        return f"error: {e}"


def _collect_redis_diagnostics() -> dict:
    """Collect extended Redis diagnostics: ping, server INFO, mode, sentinel topology.

    Always returns a dict with at least {"status": "ok"|"fail"}.
    Never raises — every sub-probe is individually guarded.
    """
    import redis as _redis

    result: dict = {}

    # ── 1. Ping ─────────────────────────────────────────────────────────────────────────
    try:
        pong = redis_client.ping()
        if not pong:
            return {"status": "fail", "error": "ping returned false"}
        result["status"] = "ok"
    except Exception as e:
        return {"status": "fail", "error": str(e)}

    # ── 2. Mode detection ───────────────────────────────────────────────────────────────
    sentinel_enabled = (settings.SUPERTABLE_REDIS_SENTINEL or "").strip().lower() in (
        "1", "true", "yes", "y", "on",
    )
    sentinel_master_name = (settings.SUPERTABLE_REDIS_SENTINEL_MASTER or "mymaster").strip()
    _sent_pw = settings.SUPERTABLE_REDIS_SENTINEL_PASSWORD or settings.SUPERTABLE_REDIS_PASSWORD or None
    sentinel_password = (_sent_pw.strip() if _sent_pw else None) or None

    sentinel_hosts: list = []
    if sentinel_enabled:
        for part in (settings.SUPERTABLE_REDIS_SENTINELS or "").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                h, p = part.split(":")
                sentinel_hosts.append((h.strip(), int(p)))
            except ValueError:
                pass

    result["mode"] = "sentinel" if (sentinel_enabled and sentinel_hosts) else "standalone"
    result["db"] = settings.SUPERTABLE_REDIS_DB

    # ── 3. Server INFO ───────────────────────────────────────────────────────────────────
    try:
        info = redis_client.info()
        uptime_s = int(info.get("uptime_in_seconds") or 0)
        days, rem = divmod(uptime_s, 86400)
        hours, rem = divmod(rem, 3600)
        mins = rem // 60
        uptime_str = (f"{days}d " if days else "") + f"{hours:02d}h {mins:02d}m"

        hits = int(info.get("keyspace_hits") or 0)
        misses = int(info.get("keyspace_misses") or 0)
        hit_rate = round(hits / (hits + misses) * 100, 1) if (hits + misses) > 0 else None

        result["server"] = {
            "redis_version": info.get("redis_version"),
            "role": info.get("role"),
            "uptime": uptime_str,
            "connected_clients": info.get("connected_clients"),
            "blocked_clients": info.get("blocked_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "used_memory_peak_human": info.get("used_memory_peak_human"),
            "maxmemory_human": info.get("maxmemory_human") or "0B (no limit)",
            "total_commands_processed": info.get("total_commands_processed"),
            "total_connections_received": info.get("total_connections_received"),
            "keyspace_hit_rate": f"{hit_rate}%" if hit_rate is not None else "n/a",
            "aof_enabled": bool(info.get("aof_enabled")),
            "rdb_last_bgsave_status": info.get("rdb_last_bgsave_status"),
        }
    except Exception as e:
        result["server_error"] = str(e)

    # ── 4. DB key count ───────────────────────────────────────────────────────────────────
    try:
        result["db_keys"] = redis_client.dbsize()
    except Exception:
        pass

    # ── 5. Sentinel topology ───────────────────────────────────────────────────────────────
    if sentinel_enabled and sentinel_hosts:
        result["sentinel_master_name"] = sentinel_master_name
        result["sentinel_hosts_configured"] = [f"{h}:{p}" for h, p in sentinel_hosts]

        sentinels_probed: list = []
        master_data: Optional[dict] = None
        replicas_data: list = []

        for s_host, s_port in sentinel_hosts:
            probe: dict = {"address": f"{s_host}:{s_port}"}
            try:
                sc = _redis.StrictRedis(
                    host=s_host,
                    port=s_port,
                    password=sentinel_password,
                    socket_timeout=1.0,
                    decode_responses=True,
                )
                sc.ping()
                probe["status"] = "ok"

                # Query master + replicas once from the first healthy sentinel
                if master_data is None:
                    try:
                        raw = sc.execute_command("SENTINEL", "MASTER", sentinel_master_name)
                        if isinstance(raw, list) and len(raw) >= 2:
                            m = dict(zip(raw[::2], raw[1::2]))
                            master_data = {
                                "address": f"{m.get('ip', '?')}" + ":" + f"{m.get('port', '?')}",
                                "flags": m.get("flags", ""),
                                "num_replicas": m.get("num-slaves", "0"),
                                "num_other_sentinels": m.get("num-other-sentinels", "0"),
                                "quorum": m.get("quorum", "?"),
                                "failover_timeout_ms": m.get("failover-timeout", "?"),
                                "config_epoch": m.get("config-epoch", "?"),
                            }
                    except Exception as me:
                        master_data = {"error": str(me)}

                    try:
                        replicas_raw = sc.execute_command("SENTINEL", "REPLICAS", sentinel_master_name)
                        if isinstance(replicas_raw, list):
                            for r_item in replicas_raw:
                                if isinstance(r_item, list) and len(r_item) >= 2:
                                    r = dict(zip(r_item[::2], r_item[1::2]))
                                    replicas_data.append({
                                        "address": f"{r.get('ip', '?')}" + ":" + f"{r.get('port', '?')}",
                                        "flags": r.get("flags", ""),
                                        "master_link_status": r.get("master-link-status", "?"),
                                        "slave_priority": r.get("slave-priority", "?"),
                                        "repl_offset": r.get("slave-repl-offset", "?"),
                                    })
                    except Exception:
                        pass

            except Exception as e:
                probe["status"] = "fail"
                probe["error"] = str(e)
            sentinels_probed.append(probe)

        result["sentinels"] = sentinels_probed
        if master_data:
            result["master"] = master_data
        if replicas_data:
            result["replicas"] = replicas_data

    else:
        # Standalone: derive address from connection pool kwargs
        try:
            kw = getattr(getattr(redis_client, "connection_pool", None), "connection_kwargs", {}) or {}
            result["address"] = (
                f"{kw.get('host', settings.SUPERTABLE_REDIS_HOST)}"
                + ":"
                + str(kw.get("port", settings.SUPERTABLE_REDIS_PORT))
            )
        except Exception:
            result["address"] = f"{settings.SUPERTABLE_REDIS_HOST}:{settings.SUPERTABLE_REDIS_PORT}"

    return result


@router.get("/api/v1/health/infra")
@router.get("/api/v1/health/deep")
def healthz_deep():
    """Deep health check — verifies Redis, storage, and DuckDB can fulfill their contracts.

    Returns 200 with component status if all pass, 503 if any fail.
    Suitable for readiness probes in Kubernetes / ECS / load balancers.
    """
    checks = {}
    all_ok = True

    # 1. Redis: ping + extended diagnostics
    try:
        redis_diag = _collect_redis_diagnostics()
        checks["redis"] = redis_diag
        if redis_diag.get("status") != "ok":
            all_ok = False
    except Exception as e:
        checks["redis"] = {"status": "fail", "error": str(e)}
        all_ok = False

    # 2. Storage: write + read + delete a probe file + collect diagnostics
    try:
        from supertable.storage.storage_factory import get_storage
        storage = get_storage()
        probe_path = "__healthz__/probe.txt"
        probe_data = f"healthz-{time.time()}"
        t0 = time.perf_counter()
        storage.write_text(probe_path, probe_data)
        readback = storage.read_text(probe_path)
        probe_ms = round((time.perf_counter() - t0) * 1000)
        try:
            storage.delete(probe_path)
        except Exception:
            pass
        if readback.strip() == probe_data:
            sdiag: dict = {
                "status": "ok",
                "backend": type(storage).__name__,
                "probe_latency_ms": probe_ms,
            }
            # Collect backend-specific metadata (all guarded — never break the probe)
            try:
                sdiag["bucket"] = getattr(storage, "bucket_name", None)
            except Exception:
                pass
            try:
                ep = getattr(storage, "endpoint_url", None) or getattr(storage, "_endpoint", None)
                if ep:
                    sdiag["endpoint"] = ep
            except Exception:
                pass
            try:
                region = getattr(storage, "region", None)
                if region:
                    sdiag["region"] = region
            except Exception:
                pass
            try:
                prefix = getattr(storage, "base_prefix", None)
                sdiag["prefix"] = prefix if prefix else "(none)"
            except Exception:
                pass
            try:
                sdiag["url_style"] = getattr(storage, "url_style", None)
            except Exception:
                pass
            try:
                sdiag["secure"] = getattr(storage, "secure", None)
            except Exception:
                pass
            # LocalStorage: expose root path
            try:
                root = getattr(storage, "root", None) or getattr(storage, "base_path", None)
                if root:
                    sdiag["root_path"] = str(root)
            except Exception:
                pass
            checks["storage"] = sdiag
        else:
            checks["storage"] = {"status": "fail", "error": "read-back mismatch", "backend": type(storage).__name__}
            all_ok = False
    except Exception as e:
        checks["storage"] = {"status": "fail", "error": str(e)}
        all_ok = False

    # 3. DuckDB: version + settings + loaded extensions
    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        probe_row = conn.execute("SELECT 1 AS healthz").fetchone()
        if not (probe_row and probe_row[0] == 1):
            raise RuntimeError("unexpected SELECT 1 result")

        ddiag: dict = {"status": "ok"}

        # Version
        try:
            ver_row = conn.execute("SELECT version()").fetchone()
            ddiag["version"] = ver_row[0] if ver_row else duckdb.__version__
        except Exception:
            ddiag["version"] = getattr(duckdb, "__version__", "unknown")

        # Active settings
        for setting, key in [
            ("memory_limit",          "memory_limit"),
            ("threads",               "threads"),
            ("temp_directory",        "temp_directory"),
            ("preserve_insertion_order", "preserve_insertion_order"),
        ]:
            try:
                row = conn.execute(f"SELECT current_setting('{setting}')").fetchone()
                if row and row[0] is not None:
                    ddiag[key] = str(row[0])
            except Exception:
                pass

        # Loaded extensions
        try:
            exts = conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() "
                "WHERE loaded = true ORDER BY extension_name"
            ).fetchall()
            ddiag["loaded_extensions"] = [
                {"name": r[0], "version": r[1] or ""} for r in (exts or [])
            ]
        except Exception:
            pass

        conn.close()
        checks["duckdb"] = ddiag
    except Exception as e:
        checks["duckdb"] = {"status": "fail", "error": str(e)}
        all_ok = False

    status_code = 200 if all_ok else 503
    return JSONResponse({"status": "ok" if all_ok else "degraded", "checks": checks}, status_code=status_code)


# ─────────────────────────── Auth / Login ─────────────────────────────────

# NOTE: Former super-meta endpoint merged into GET /api/v1/supertables/meta below.


@router.get("/api/v1/session/role-names")
def reflection_user_role_names(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    username = (sess.get("username") or "").strip()
    if not username:
        return JSONResponse({"role_names": []})

    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return JSONResponse({"role_names": []})

    try:
        from supertable.rbac.user_manager import UserManager
        from supertable.rbac.role_manager import RoleManager

        user_manager = UserManager(super_name=sup_val, organization=org_val)
        user_data = user_manager.get_user_hash_by_name(username)

        if not user_data or not isinstance(user_data, dict):
            return JSONResponse({"role_names": []})

        role_ids = user_data.get("roles") or []
        if isinstance(role_ids, str):
            try:
                role_ids = json.loads(role_ids)
            except Exception:
                role_ids = [role_ids]

        role_names: List[str] = []
        role_manager = RoleManager(super_name=sup_val, organization=org_val)
        for role_id in role_ids:
            role_id = str(role_id).strip()
            if not role_id:
                continue
            try:
                role_doc = role_manager.get_role(role_id)
                if role_doc and isinstance(role_doc, dict):
                    name = role_doc.get("role_name") or role_doc.get("role") or role_id
                    role_names.append(str(name))
                else:
                    role_names.append(role_id)
            except Exception:
                role_names.append(role_id)

        return JSONResponse({"role_names": role_names})

    except Exception as e:
        logger.warning("reflection_user_role_names failed (%s/%s/%s): %s", org_val, sup_val, username, e)
        return JSONResponse({"role_names": []})


# ─────────────────── Sidebar role endpoints ───────────────────────────────


@router.get("/api/v1/session/roles")
def reflection_roles(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return {"roles": []}
    return {"roles": list_roles(org_val, sup_val)}


@router.post("/api/v1/session/role")
def reflection_set_role(
    request: Request,
    body: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    role_name = str(body.get("role_name") or "").strip()
    sess = get_session(request) or {}
    sess_data = dict(sess)
    sess_data["role_name"] = role_name
    resp = JSONResponse({"ok": True, "role_name": role_name})
    _set_session_cookie(resp, sess_data)
    _no_store(resp)
    return resp


# ──────────────────── Auth tokens ─────────────────────────────────────────

@router.get("/api/v1/tokens")
def api_list_tokens(request: Request, org: str = Query(None), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    tokens = _catalog_list_tokens(org_eff)
    return {"ok": True, "organization": org_eff, "tokens": tokens}


@router.post("/api/v1/tokens")
def api_create_token(request: Request, org: str = Query(None), label: str = Query(""), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    created = _catalog_create_token(org_eff, created_by="superuser", label=label)
    _audit(
        category=EventCategory.TOKEN_MGMT, action=Actions.TOKEN_CREATE,
        organization=org_eff, resource_type="token",
        resource_id=created.get("token_id", ""), severity=Severity.WARNING,
        detail=make_detail(label=label),
    )
    return {"ok": True, "organization": org_eff, **created}


@router.delete("/api/v1/tokens/{token_id}")
def api_delete_token(request: Request, token_id: str, org: str = Query(None), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    ok = _catalog_delete_token(org_eff, token_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Token not found")
    _audit(
        category=EventCategory.TOKEN_MGMT, action=Actions.TOKEN_DELETE,
        organization=org_eff, resource_type="token",
        resource_id=token_id, severity=Severity.WARNING,
    )
    return {"ok": True, "organization": org_eff, "token_id": token_id}


# ──────────────────── Admin page + SuperTable CRUD ────────────────────────

@router.post("/api/v1/supertables")
def api_create_super(
    request: Request,
    organization: str = Query(...),
    super_name: str = Query(...),
    _: Any = Depends(admin_guard_api),
):
    from supertable.super_table import SuperTable
    try:
        st = SuperTable(organization=organization, super_name=super_name)
        storage_label = getattr(getattr(st, "storage", None), "__class__", None)
        storage_name = getattr(storage_label, "__name__", None) if storage_label else None
        _audit(
            category=EventCategory.DATA_MUTATION, action=Actions.SUPERTABLE_CREATE,
            organization=organization, super_name=super_name,
            resource_type="supertable", resource_id=super_name,
            severity=Severity.WARNING, detail=make_detail(storage=storage_name),
        )
        return {"ok": True, "organization": st.organization, "name": st.super_name, "storage": storage_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SuperTable creation failed: {e}")


@router.delete("/api/v1/supertables")
def api_delete_super(
    request: Request,
    organization: str = Query(..., description="Organization identifier"),
    super_name: str = Query(..., description="SuperTable name"),
    role_name: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    # Clone protection: block deletion if read-only clones reference this source
    from supertable.services.supertable_manager import can_delete_supertable
    check = can_delete_supertable(organization, super_name)
    if not check.get("ok"):
        raise HTTPException(status_code=409, detail=check.get("reason", "Cannot delete"))

    role = (role_name or "").strip()

    from supertable.super_table import SuperTable
    try:
        super_table = SuperTable(super_name=super_name, organization=organization)
        super_table.delete(role_name=role)
        return {"ok": True, "organization": organization, "name": super_name}
    except Exception:
        pass

    from supertable.storage.storage_factory import get_storage as _gs
    storage = _gs()
    base_dir = os.path.join(organization, super_name)

    try:
        if storage.exists(base_dir):
            storage.delete(base_dir)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage delete failed: {e}")

    deleted_keys = 0
    try:
        from supertable.redis_catalog import RedisCatalog as _RC
        rc = _RC()
        deleted_keys = rc.delete_super_table(organization, super_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis delete failed: {e}")

    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.SUPERTABLE_DELETE,
        organization=organization, super_name=super_name,
        resource_type="supertable", resource_id=super_name,
        severity=Severity.CRITICAL, detail=make_detail(deleted_redis_keys=deleted_keys),
    )
    return {"ok": True, "organization": organization, "super_name": super_name, "deleted_redis_keys": deleted_keys}


# ═══════════════════════════════════════════════════════════════════════════
#   SUPERTABLES — detailed list, clone, read-only toggle
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/supertables/details")
def api_supertables_details(
    request: Request,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")
    from supertable.services.supertable_manager import list_supertables_detailed
    try:
        items = list_supertables_detailed(organization)
        return {"ok": True, "organization": organization, "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List supertables failed: {e}")


@router.post("/api/v1/supertables/clone")
def api_supertables_clone(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    organization = str(payload.get("organization") or "").strip()
    source = str(payload.get("source") or "").strip()
    target = str(payload.get("target") or "").strip()
    clone_type = str(payload.get("clone_type") or "readonly").strip().lower()
    at_timestamp = payload.get("at_timestamp")  # optional: epoch-ms for time travel
    tables = payload.get("tables")  # optional: list of table names for partial clone
    inherit_rbac = bool(payload.get("inherit_rbac"))
    inherit_roles = payload.get("inherit_roles")  # optional: list of role names
    inherit_users = bool(payload.get("inherit_users"))

    if not organization or not source or not target:
        raise HTTPException(status_code=400, detail="organization, source, and target are required")
    if source == target:
        raise HTTPException(status_code=400, detail="source and target must be different")
    if clone_type not in ("readonly", "writable", "replica"):
        raise HTTPException(status_code=400, detail="clone_type must be 'readonly', 'writable', or 'replica'")

    at_ts = int(at_timestamp) if at_timestamp is not None else None
    table_list = list(tables) if isinstance(tables, list) else None
    role_list = list(inherit_roles) if isinstance(inherit_roles, list) else None

    from supertable.services.supertable_manager import clone_supertable
    try:
        result = clone_supertable(
            organization, source, target,
            clone_type=clone_type,
            at_timestamp=at_ts,
            tables=table_list,
            inherit_rbac=inherit_rbac,
            inherit_roles=role_list,
            inherit_users=inherit_users,
        )
        action_map = {
            "readonly": Actions.SUPERTABLE_CLONE_READONLY,
            "writable": Actions.SUPERTABLE_CLONE_WRITABLE,
            "replica": Actions.SUPERTABLE_CLONE_REPLICA,
        }
        _audit(
            category=EventCategory.DATA_MUTATION,
            action=action_map.get(clone_type, Actions.SUPERTABLE_CLONE_READONLY),
            organization=organization, super_name=target,
            resource_type="supertable", resource_id=target,
            severity=Severity.WARNING,
            detail=make_detail(
                source=source, target=target, clone_type=clone_type,
                tables_cloned=result.get("tables_cloned", 0),
                at_timestamp=at_ts,
                tables=table_list,
            ),
        )
        return JSONResponse(result, status_code=201)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.exception("SuperTable clone failed")
        raise HTTPException(status_code=500, detail=f"Clone failed: {e}")


@router.post("/api/v1/supertables/promote")
def api_supertables_promote(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Promote a replica clone to a writable SuperTable.

    Materializes the source's current leaf pointers into the replica's
    own namespace, then flips clone_type to 'writable' and read_only to false.
    """
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()

    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    from supertable.services.supertable_manager import promote_replica
    try:
        result = promote_replica(organization, super_name)
        _audit(
            category=EventCategory.DATA_MUTATION,
            action=Actions.SUPERTABLE_PROMOTE,
            organization=organization, super_name=super_name,
            resource_type="supertable", resource_id=super_name,
            severity=Severity.WARNING,
            detail=make_detail(
                source=result.get("source", ""),
                tables_materialized=result.get("tables_materialized", 0),
            ),
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Promote replica failed")
        raise HTTPException(status_code=500, detail=f"Promote failed: {e}")


@router.post("/api/v1/supertables/detach")
def api_supertables_detach(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Detach a clone by hard-copying shared files, making it fully independent.

    For replicas, this first promotes (materializes leaves), then copies
    all shared Parquet files to the clone's own storage path.
    """
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()

    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    from supertable.services.supertable_manager import detach_clone
    try:
        result = detach_clone(organization, super_name)
        _audit(
            category=EventCategory.DATA_MUTATION,
            action=Actions.SUPERTABLE_DETACH,
            organization=organization, super_name=super_name,
            resource_type="supertable", resource_id=super_name,
            severity=Severity.CRITICAL,
            detail=make_detail(
                source=result.get("source", ""),
                files_copied=result.get("files_copied", 0),
                bytes_copied=result.get("bytes_copied", 0),
            ),
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Detach clone failed")
        raise HTTPException(status_code=500, detail=f"Detach failed: {e}")


@router.get("/api/v1/supertables/roles")
def api_supertables_roles(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    """List role names for a SuperTable (used by clone UI RBAC picker)."""
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    try:
        from supertable.rbac.role_manager import RoleManager
        rm = RoleManager(super_name=super_name, organization=organization)
        roles = rm.list_roles()
        role_names = []
        for r in roles:
            rn = r.get("role_name", "")
            if rn:
                role_names.append({
                    "role_name": rn,
                    "role_type": r.get("role", ""),
                    "table_count": len(r.get("tables", {})),
                })
        return {"ok": True, "roles": role_names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List roles failed: {e}")


@router.post("/api/v1/supertables/clone-table")
def api_supertables_clone_table(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Clone a single table between supertables (zero-copy, with optional time travel)."""
    organization = str(payload.get("organization") or "").strip()
    source_sup = str(payload.get("source_sup") or "").strip()
    target_sup = str(payload.get("target_sup") or "").strip()
    table_name = str(payload.get("table") or "").strip()
    at_version = payload.get("at_version")
    at_timestamp = payload.get("at_timestamp")

    if not organization or not source_sup or not target_sup or not table_name:
        raise HTTPException(status_code=400, detail="organization, source_sup, target_sup, and table are required")

    at_v = int(at_version) if at_version is not None else None
    at_ts = int(at_timestamp) if at_timestamp is not None else None

    from supertable.services.supertable_manager import clone_table
    try:
        result = clone_table(
            organization, source_sup, target_sup, table_name,
            at_version=at_v, at_timestamp=at_ts,
        )
        _audit(
            category=EventCategory.DATA_MUTATION,
            action=Actions.TABLE_CLONE,
            organization=organization, super_name=target_sup,
            resource_type="table", resource_id=table_name,
            severity=Severity.WARNING,
            detail=make_detail(
                source_sup=source_sup, target_sup=target_sup,
                table=table_name, at_version=at_v, at_timestamp=at_ts,
            ),
        )
        return JSONResponse(result, status_code=201)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.exception("Table clone failed")
        raise HTTPException(status_code=500, detail=f"Table clone failed: {e}")


@router.put("/api/v1/supertables/read-only")
def api_supertables_set_readonly(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()
    enabled = bool(payload.get("enabled", True))

    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    from supertable.services.supertable_manager import set_read_only
    try:
        set_read_only(organization, super_name, enabled)
        _audit(
            category=EventCategory.CONFIG_CHANGE,
            action=Actions.SUPERTABLE_TOGGLE_READONLY,
            organization=organization, super_name=super_name,
            resource_type="supertable", resource_id=super_name,
            severity=Severity.WARNING,
            detail=make_detail(read_only=enabled),
        )
        return {"ok": True, "super_name": super_name, "read_only": enabled}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Set read-only failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#   DATA SHARING — provider + consumer + manifest
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/shares")
def api_shares_list(
    request: Request,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")
    from supertable.services.sharing import list_org_shares
    return {"ok": True, "shares": list_org_shares(organization)}


@router.post("/api/v1/shares")
def api_shares_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()
    tables = payload.get("tables") or []
    grantee_org = str(payload.get("grantee_org") or "").strip()
    label = str(payload.get("label") or "").strip()
    columns = payload.get("columns")  # optional: {table: [col1, col2]}
    row_filter = payload.get("row_filter")  # optional: {table: "WHERE clause"}
    at_timestamp = payload.get("at_timestamp")  # optional: epoch-ms for point-in-time

    if not organization or not super_name or not tables or not grantee_org:
        raise HTTPException(status_code=400, detail="organization, super_name, tables, and grantee_org are required")
    if not isinstance(tables, list):
        raise HTTPException(status_code=400, detail="tables must be a list")

    col_dict = dict(columns) if isinstance(columns, dict) else None
    filter_dict = dict(row_filter) if isinstance(row_filter, dict) else None
    at_ts = int(at_timestamp) if at_timestamp is not None else None

    from supertable.services.sharing import create_share
    try:
        result = create_share(
            organization, super_name, tables, grantee_org,
            created_by="superuser", label=label,
            columns=col_dict, row_filter=filter_dict, at_timestamp=at_ts,
        )
        _audit(
            category=EventCategory.CONFIG_CHANGE, action=Actions.SHARE_CREATE,
            organization=organization, super_name=super_name,
            resource_type="share", resource_id=result["share_id"],
            severity=Severity.WARNING,
            detail=make_detail(tables=tables, grantee_org=grantee_org),
        )
        return JSONResponse(result, status_code=201)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Create share failed: {e}")


@router.delete("/api/v1/shares/{share_id}")
def api_shares_revoke(
    request: Request,
    share_id: str,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")
    from supertable.services.sharing import revoke_share
    try:
        result = revoke_share(organization, share_id)
        _audit(
            category=EventCategory.CONFIG_CHANGE, action=Actions.SHARE_REVOKE,
            organization=organization, resource_type="share", resource_id=share_id,
            severity=Severity.CRITICAL,
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Revoke share failed: {e}")


@router.get("/api/v1/shares/{share_id}/manifest")
def api_shares_manifest(
    request: Request,
    share_id: str,
    organization: str = Query(""),
    org: str = Query(""),
):
    """Public manifest endpoint — authenticated by bearer token, not session."""
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    bearer_token = auth_header[7:].strip()

    from supertable.services.sharing import get_share_manifest
    try:
        manifest = get_share_manifest(organization, share_id, bearer_token)
        try:
            _audit(
                category=EventCategory.DATA_ACCESS, action=Actions.SHARE_MANIFEST_ACCESS,
                organization=organization, resource_type="share", resource_id=share_id,
                severity=Severity.INFO,
                detail=make_detail(grantee_org=manifest.get("grantee_org", "")),
            )
        except Exception:
            pass
        return JSONResponse(manifest)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Manifest generation failed: {e}")


# ── Consumer: linked shares ──

@router.get("/api/v1/linked-shares")
def api_linked_shares_list(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        raise HTTPException(status_code=400, detail="org and sup are required")
    from supertable.services.sharing import list_linked_shares
    return {"ok": True, "links": list_linked_shares(sel_org, sel_sup)}


@router.post("/api/v1/linked-shares")
def api_linked_shares_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    org = str(payload.get("organization") or payload.get("org") or "").strip()
    sup = str(payload.get("super_name") or payload.get("sup") or "").strip()
    provider_url = str(payload.get("provider_url") or "").strip()
    provider_token = str(payload.get("provider_token") or "").strip()
    alias_prefix = str(payload.get("alias_prefix") or "").strip()
    label = str(payload.get("label") or "").strip()

    if not org or not sup or not provider_url or not provider_token:
        raise HTTPException(status_code=400, detail="organization, super_name, provider_url, and provider_token are required")

    from supertable.services.sharing import link_share
    try:
        result = link_share(org, sup, provider_url, provider_token, alias_prefix, label)
        _audit(
            category=EventCategory.CONFIG_CHANGE, action=Actions.SHARE_LINK,
            organization=org, super_name=sup,
            resource_type="linked_share", resource_id=result.get("link_id", ""),
            severity=Severity.WARNING,
            detail=make_detail(provider_url=provider_url, tables=result.get("tables", [])),
        )
        return JSONResponse(result, status_code=201)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Link share failed: {e}")


@router.delete("/api/v1/linked-shares/{link_id}")
def api_linked_shares_delete(
    request: Request,
    link_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        raise HTTPException(status_code=400, detail="org and sup are required")
    from supertable.services.sharing import unlink_share
    try:
        result = unlink_share(sel_org, sel_sup, link_id)
        _audit(
            category=EventCategory.CONFIG_CHANGE, action=Actions.SHARE_UNLINK,
            organization=sel_org, super_name=sel_sup,
            resource_type="linked_share", resource_id=link_id,
            severity=Severity.WARNING,
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unlink share failed: {e}")


@router.post("/api/v1/linked-shares/{link_id}/refresh")
def api_linked_shares_refresh(
    request: Request,
    link_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        raise HTTPException(status_code=400, detail="org and sup are required")
    from supertable.services.sharing import refresh_linked_share
    try:
        return refresh_linked_share(sel_org, sel_sup, link_id, force=True)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


@router.post("/api/v1/linked-shares/{link_id}/materialize")
def api_linked_shares_materialize(
    request: Request,
    link_id: str,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Download all data from a linked share into a new local SuperTable."""
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()
    target = str(payload.get("target_super_name") or "").strip()

    if not organization or not super_name or not target:
        raise HTTPException(status_code=400, detail="organization, super_name, and target_super_name are required")

    from supertable.services.sharing import materialize_linked_share
    try:
        result = materialize_linked_share(organization, super_name, link_id, target)
        try:
            _audit(
                category=EventCategory.DATA_MUTATION,
                action=Actions.SHARE_MATERIALIZE,
                organization=organization, super_name=target,
                resource_type="linked_share", resource_id=link_id,
                severity=Severity.WARNING,
                detail=make_detail(
                    target=target,
                    tables_imported=result.get("tables_imported", 0),
                    files_downloaded=result.get("files_downloaded", 0),
                ),
            )
        except Exception:
            pass
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.exception("Materialize linked share failed")
        raise HTTPException(status_code=500, detail=f"Materialize failed: {e}")


@router.get("/api/v1/shares/dashboard")
def api_shares_dashboard(
    request: Request,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    """Provider share dashboard with access stats from audit stream."""
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")
    from supertable.services.sharing import get_share_dashboard
    try:
        return {"ok": True, "shares": get_share_dashboard(organization)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dashboard failed: {e}")


@router.post("/api/v1/linked-shares/refresh-all")
def api_linked_shares_refresh_all(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    """Auto-refresh all linked shares that are close to expiry."""
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        raise HTTPException(status_code=400, detail="org and sup are required")
    from supertable.services.sharing import refresh_expiring_shares
    try:
        return refresh_expiring_shares(sel_org, sel_sup)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh-all failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#   PUBLICATIONS — cross-organization clone (publish + accept)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/publications")
def api_publications_list(
    request: Request,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")
    from supertable.services.publication import list_publications
    return {"ok": True, "publications": list_publications(organization)}


@router.post("/api/v1/publications")
def api_publications_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Publish a SuperTable (or table subset) for cross-org cloning."""
    organization = str(payload.get("organization") or "").strip()
    super_name = str(payload.get("super_name") or "").strip()
    tables = payload.get("tables")
    label = str(payload.get("label") or "").strip()

    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    table_list = list(tables) if isinstance(tables, list) else None

    from supertable.services.publication import create_publication
    try:
        result = create_publication(
            organization, super_name,
            tables=table_list, label=label, created_by="superuser",
        )
        try:
            _audit(
                category=EventCategory.DATA_MUTATION,
                action=Actions.PUBLICATION_CREATE,
                organization=organization, super_name=super_name,
                resource_type="publication", resource_id=result.get("publication_id", ""),
                severity=Severity.WARNING,
                detail=make_detail(
                    tables=result.get("tables", []),
                    table_count=result.get("table_count", 0),
                ),
            )
        except Exception:
            pass
        return JSONResponse(result, status_code=201)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Create publication failed")
        raise HTTPException(status_code=500, detail=f"Create publication failed: {e}")


@router.delete("/api/v1/publications/{publication_id}")
def api_publications_revoke(
    request: Request,
    publication_id: str,
    organization: str = Query(""),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")

    from supertable.services.publication import revoke_publication
    try:
        result = revoke_publication(organization, publication_id)
        try:
            _audit(
                category=EventCategory.DATA_MUTATION,
                action=Actions.PUBLICATION_REVOKE,
                organization=organization,
                resource_type="publication", resource_id=publication_id,
                severity=Severity.WARNING,
            )
        except Exception:
            pass
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Revoke failed: {e}")


@router.get("/api/v1/publications/{publication_id}/manifest")
def api_publications_manifest(
    request: Request,
    publication_id: str,
    organization: str = Query(""),
    org: str = Query(""),
):
    """Public manifest endpoint — authenticated by bearer token."""
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    bearer_token = auth_header[7:].strip()

    from supertable.services.publication import get_publication_manifest
    try:
        manifest = get_publication_manifest(organization, publication_id, bearer_token)
        return JSONResponse(manifest)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Manifest generation failed: {e}")


@router.post("/api/v1/publications/accept")
def api_publications_accept(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Accept a publication — download all data into a new local SuperTable."""
    organization = str(payload.get("organization") or "").strip()
    target_name = str(payload.get("target_super_name") or "").strip()
    manifest_url = str(payload.get("manifest_url") or "").strip()
    bearer_token = str(payload.get("bearer_token") or "").strip()
    label = str(payload.get("label") or "").strip()

    if not organization or not target_name or not manifest_url or not bearer_token:
        raise HTTPException(status_code=400, detail="organization, target_super_name, manifest_url, and bearer_token are required")

    from supertable.services.publication import accept_publication
    try:
        result = accept_publication(
            organization, target_name, manifest_url, bearer_token, label=label,
        )
        try:
            _audit(
                category=EventCategory.DATA_MUTATION,
                action=Actions.PUBLICATION_ACCEPT,
                organization=organization, super_name=target_name,
                resource_type="publication", resource_id=target_name,
                severity=Severity.CRITICAL,
                detail=make_detail(
                    provider_org=result.get("provider_org", ""),
                    provider_super=result.get("provider_super", ""),
                    tables_imported=result.get("tables_imported", 0),
                    files_downloaded=result.get("files_downloaded", 0),
                    bytes_downloaded=result.get("bytes_downloaded", 0),
                ),
            )
        except Exception:
            pass
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.exception("Accept publication failed")
        raise HTTPException(status_code=500, detail=f"Accept failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#   EXECUTE (SQL query) endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/v1/query/execute")
def execute_api(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    import enum as _enum
    try:
        query = str(payload.get("query") or "").strip()
        organization = str(payload.get("organization") or "")
        super_name = str(payload.get("super_name") or "")
        role_name = str(payload.get("role_name") or payload.get("user_hash") or "")
        page = int(payload.get("page") or 1)
        page_size = int(payload.get("page_size") or 100)
        max_rows = 10_000

        engine_raw = str(payload.get("engine") or "auto").strip().lower()
        _ALLOWED_ENGINES = {"auto", "duckdb_pro", "duckdb_lite", "spark_sql"}
        if engine_raw not in _ALLOWED_ENGINES:
            engine_raw = "auto"

        if not organization or not super_name:
            return JSONResponse({"status": "error", "message": "organization and super_name are required", "result": []}, status_code=400)
        if not query:
            return JSONResponse({"status": "error", "message": "No query provided", "result": []}, status_code=400)
        if not role_name:
            return JSONResponse({"status": "error", "message": "role_name is required", "result": []}, status_code=400)

        q = _clean_sql_query(query)
        if not q.lower().lstrip().startswith(("select", "with")):
            return JSONResponse({"status": "error", "message": "Only SELECT or WITH (CTE) queries are allowed", "result": []}, status_code=400)

        q = _apply_limit_safely(q, max_rows)

        DR = _get_data_reader()
        dr = DR(super_name=super_name, organization=organization, query=q)

        # Tag read source for monitoring: UI (session cookie) vs API/SDK (token)
        if dr.query_plan_manager:
            _src = payload.get("source_type", "").strip()
            if _src in ("ui", "api", "sdk"):
                dr.query_plan_manager.source_type = _src
            elif getattr(request.state, "session", None):
                dr.query_plan_manager.source_type = "ui"
            else:
                dr.query_plan_manager.source_type = "api"

        try:
            from supertable.engine.engine_enum import Engine as _EngineEnum
        except Exception:
            from engine.engine_enum import Engine as _EngineEnum  # type: ignore

        _ENGINE_MAP = {
            "auto": _EngineEnum.AUTO,
            "duckdb_pro": _EngineEnum.DUCKDB_PRO,
            "duckdb_lite": _EngineEnum.DUCKDB_LITE,
            "spark_sql": _EngineEnum.SPARK_SQL,
        }
        selected_engine = _ENGINE_MAP.get(engine_raw, _EngineEnum.AUTO)
        res = dr.execute(role_name=role_name, engine=selected_engine)

        df = meta1 = meta2 = None
        if isinstance(res, tuple):
            if len(res) >= 1:
                df = res[0]
            if len(res) >= 2:
                meta1 = res[1]
            if len(res) >= 3:
                meta2 = res[2]
        else:
            df = res

        total_count = 0
        rows: List[Dict[str, Any]] = []

        if df is not None:
            try:
                total_count = int(getattr(df, "shape", [0])[0] or 0)
                if total_count > max_rows:
                    df = df.iloc[:max_rows]
                    total_count = max_rows
                start = max(0, (page - 1) * page_size)
                end = start + page_size
                page_df = df.iloc[start:end]
                rows = json.loads(page_df.to_json(orient="records", date_format="iso"))
            except Exception:
                try:
                    if hasattr(df, "fetchall"):
                        all_rows = df.fetchall()
                        total_count = len(all_rows)
                        if total_count > max_rows:
                            all_rows = all_rows[:max_rows]
                            total_count = max_rows
                        start = max(0, (page - 1) * page_size)
                        end = start + page_size
                        page_rows = all_rows[start:end]
                        rows = [{"c{}".format(i): _sanitize_for_json(v) for i, v in enumerate(r)} for r in page_rows]
                    elif isinstance(df, list):
                        total_count = len(df)
                        if total_count > max_rows:
                            df = df[:max_rows]
                            total_count = max_rows
                        start = max(0, (page - 1) * page_size)
                        end = start + page_size
                        rows = [_sanitize_for_json(x) for x in df[start:end]]
                    else:
                        rows = []
                except Exception:
                    rows = []

        meta_payload = {
            "result_1": meta1, "result_2": meta2,
            "timings": getattr(getattr(dr, "timer", None), "timings", None),
            "plan_stats": getattr(getattr(dr, "plan_stats", None), "stats", None),
            "query_profile": getattr(getattr(dr, "query_plan_manager", None), "query_profile", None),
        }

        # Resolve actual engine used (plan_stats contains the real engine, not the requested one)
        engine_used = engine_raw
        try:
            _ps = meta_payload.get("plan_stats") or []
            if isinstance(_ps, list):
                for _entry in _ps:
                    if isinstance(_entry, dict) and "ENGINE" in _entry:
                        engine_used = str(_entry["ENGINE"])
                        break
        except Exception:
            pass

        _audit(
            **audit_context(request), category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE, organization=organization,
            super_name=super_name, resource_type="query", resource_id="",
            detail=make_detail(
                sql_preview=q[:200], row_count=total_count,
                engine_requested=engine_raw, engine_used=engine_used,
                role_name=role_name,
            ),
        )

        return JSONResponse({
            "status": "ok", "message": None, "result": rows,
            "total_count": total_count, "meta": _sanitize_for_json(meta_payload),
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Execute SQL failed")
        _audit(
            **audit_context(request), category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE, organization=organization,
            super_name=super_name, resource_type="query", resource_id="",
            outcome=Outcome.FAILURE, reason=str(e)[:500], severity=Severity.WARNING,
            detail=make_detail(sql_preview=query[:200], engine=engine_raw, error=str(e)[:200]),
        )
        return JSONResponse({"status": "error", "message": f"Execution failed: {e}", "result": []}, status_code=500)


@router.post("/api/v1/tables/schemas")
def schema_api(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _=Depends(logged_in_guard_api),
):
    try:
        organization = str(payload.get("organization") or "")
        super_name = str(payload.get("super_name") or "")
        role_name = str(payload.get("role_name") or payload.get("user_hash") or "")

        if not organization or not super_name or not role_name:
            return JSONResponse({"status": "error", "message": "organization, super_name and role_name are required"}, status_code=400)

        try:
            from supertable.meta_reader import list_tables
        except Exception:
            from meta_reader import list_tables  # type: ignore

        tables = list_tables(organization=organization, super_name=super_name, role_name=role_name)

        MR = _get_meta_reader()
        mr = MR(organization=organization, super_name=super_name)

        schema = []
        for t in tables:
            try:
                table_schema = mr.get_table_schema(t, role_name)
                if isinstance(table_schema, list) and table_schema and isinstance(table_schema[0], dict):
                    cols = list(table_schema[0].keys())
                else:
                    cols = []
            except Exception:
                cols = []
            schema.append({t: cols})

        return JSONResponse({"status": "ok", "schema": schema})
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"Get schema failed: {e}"}, status_code=500)


# ═══════════════════════════════════════════════════════════════════════════
#   TABLES endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/supertables")
def api_list_supers(
    request: Request,
    organization: str = Query("", description="Organization identifier"),
    org: str = Query(""),
    _: Any = Depends(logged_in_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")

    try:
        items = list_supers_with_flags(organization=organization)
        return {
            "ok": True,
            "organization": organization,
            "supers": [i["name"] for i in items],
            "items": items,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List supers failed: {e}")


@router.get("/api/v1/supertables/meta")
def api_get_super_meta(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(logged_in_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    role = _resolve_role(request, role_name, "")

    debug_timings = _cfg.SUPERTABLE_DEBUG_TIMINGS
    t0 = time.perf_counter()
    try:
        mr = _common_get_meta_reader(organization, super_name)
        t1 = time.perf_counter()
        meta = mr.get_super_meta(role)
        t2 = time.perf_counter()

        payload = {"ok": True, "meta": meta}

        mr_ms = (t1 - t0) * 1000.0
        get_ms = (t2 - t1) * 1000.0
        total_ms = (t2 - t0) * 1000.0

        if debug_timings:
            client_host = getattr(getattr(request, "client", None), "host", None) or "-"
            logger.info(
                "[timing][supertables/meta] total_ms=%.2f mr_ms=%.2f get_super_meta_ms=%.2f org=%s super=%s role_name=%s client=%s",
                total_ms, mr_ms, get_ms, organization, super_name, (role or "")[:12], client_host,
            )

        resp = JSONResponse(payload)
        _no_store(resp)
        if debug_timings:
            resp.headers["Server-Timing"] = (
                f"meta_reader;dur={mr_ms:.2f},get_super_meta;dur={get_ms:.2f},total;dur={total_ms:.2f}"
            )
        return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get super meta failed: {e}")


@router.get("/api/v1/tables/schema")
def api_get_table_schema(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Table simple name"),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(logged_in_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    role = _resolve_role(request, role_name)

    # Fast path: read permanent schema from Redis (written by DataWriter on every write)
    schema_key = f"supertable:{organization}:{super_name}:schema:{table}"
    try:
        raw = redis_client.get(schema_key)
        if raw:
            schema_dict = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            if schema_dict:
                return {"ok": True, "schema": [schema_dict]}
    except Exception:
        pass

    # Fallback: MetaReader (disk)
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        schema = mr.get_table_schema(table, role)

        return {"ok": True, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table schema failed: {e}")


@router.get("/api/v1/tables/stats")
def api_get_table_stats(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Table simple name"),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(logged_in_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    role = _resolve_role(request, role_name)
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        stats = mr.get_table_stats(table, role)
        return {"ok": True, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table stats failed: {e}")


@router.delete("/api/v1/tables")
def api_delete_table(
    request: Request,
    organization: str = Query("", description="Organization identifier"),
    super_name: str = Query("", description="SuperTable name"),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Simple table name"),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    role = _resolve_role(request, role_name)
    simple = (table or "").strip()
    if not organization or not super_name or not simple or not role:
        raise HTTPException(status_code=400, detail="organization, super_name and table are required")

    from supertable.simple_table import SimpleTable
    from supertable.super_table import SuperTable

    super_table = SuperTable(super_name=super_name, organization=organization)
    simple_table = SimpleTable(super_table=super_table, simple_name=simple)
    simple_table.delete(role_name=role)

    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.TABLE_DELETE,
        organization=organization, super_name=super_name,
        resource_type="table", resource_id=simple,
        severity=Severity.CRITICAL,
    )
    return {"ok": True, "organization": organization, "super_name": super_name, "table": simple}


@router.get("/api/v1/tables/config")
def api_get_table_config(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Simple table name"),
    _: Any = Depends(logged_in_guard_api),
):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    simple = (table or "").strip()
    if not organization or not super_name or not simple:
        raise HTTPException(status_code=400, detail="organization, super_name and table are required")
    try:
        config = catalog.get_table_config(organization, super_name, simple) or {}
        return {"ok": True, "config": config}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table config failed: {e}")


@router.put("/api/v1/tables/config")
def api_put_table_config(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Simple table name"),
    _: Any = Depends(admin_guard_api),
    body: Dict[str, Any] = None,
):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    simple = (table or "").strip()
    if not organization or not super_name or not simple:
        raise HTTPException(status_code=400, detail="organization, super_name and table are required")
    if body is None:
        body = {}

    for field in ("max_memory_chunk_size", "max_overlapping_files"):
        if field in body and body[field] is not None:
            try:
                v = int(body[field])
            except (TypeError, ValueError):
                raise HTTPException(status_code=422, detail=f"{field} must be an integer")
            if v <= 0:
                raise HTTPException(status_code=422, detail=f"{field} must be a positive integer")
            body[field] = v

    try:
        existing = catalog.get_table_config(organization, super_name, simple) or {}
        merged = dict(existing)
        for field in ("max_memory_chunk_size", "max_overlapping_files"):
            if field in body:
                if body[field] is None:
                    merged.pop(field, None)
                else:
                    merged[field] = body[field]
        catalog.set_table_config(organization, super_name, simple, merged)
        _audit(
            category=EventCategory.CONFIG_CHANGE, action=Actions.TABLE_CONFIG_CHANGE,
            organization=organization, super_name=super_name,
            resource_type="table", resource_id=simple, severity=Severity.WARNING,
            detail=make_detail(before=existing, after=merged),
        )
        return {"ok": True, "config": merged}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Set table config failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#   ENGINE page
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
#   INGESTION endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ingestion/recent-writes")
def api_ingestion_recent_writes(
    org: str = Query(...),
    sup: str = Query(...),
    limit: int = Query(20, ge=1, le=100),
    _: Any = Depends(logged_in_guard_api),
):
    org_eff = (org or "").strip()
    sup_eff = (sup or "").strip()
    if not org_eff or not sup_eff:
        return {"ok": True, "items": []}

    key = f"monitor:{org_eff}:{sup_eff}:stats"
    items: List[Dict[str, Any]] = []
    try:
        raw_list = redis_client.lrange(key, -limit * 2, -1) or []
        raw_list = list(reversed(raw_list))
        for raw in raw_list:
            try:
                entry = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if not isinstance(entry, dict):
                    continue
                # Filter system tables
                tname = str(entry.get("table_name") or "")
                if tname.startswith("__") and tname.endswith("__"):
                    continue
                # Extract source_type from lineage
                _lin = entry.get("lineage")
                _src = ""
                if isinstance(_lin, str) and _lin:
                    try:
                        _lin_obj = json.loads(_lin)
                        _src = str(_lin_obj.get("source_type") or "")
                    except Exception:
                        pass
                elif isinstance(_lin, dict):
                    _src = str(_lin.get("source_type") or "")
                if not _src:
                    _src = str(entry.get("source") or entry.get("source_type") or "")
                entry["source_type"] = _src or "unknown"
                items.append(entry)
                if len(items) >= limit:
                    break
            except Exception:
                continue
    except Exception as e:
        logger.warning("[ingestion] Failed to read recent writes from Redis: %s", e)

    return {"ok": True, "items": items}


@router.get("/api/v1/ingestion/stagings")
def api_ingestion_list_stagings(
    org: str = Query(...),
    sup: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    names = _redis_list_stagings(org, sup)
    return {"staging_names": names}


@router.get("/api/v1/ingestion/tables")
def api_ingestion_list_tables(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    organization: Optional[str] = Query(None),
    super_name: Optional[str] = Query(None),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    org_val = (org or organization or "").strip()
    sup_val = (sup or super_name or "").strip()
    role = (role_name or "").strip()

    if not org_val or not sup_val or not role:
        return {"ok": True, "tables": []}

    sess = get_session(request) or {}
    sess_role = (sess.get("role_name") or "").strip()
    if sess_role and sess_role != role and not is_superuser(request):
        raise HTTPException(status_code=403, detail="Forbidden")

    # Fast path: SMEMBERS from dedicated set (written by DataWriter on every write)
    tables: List[str] = []
    seen: set = set()
    try:
        members = redis_client.smembers(f"supertable:{org_val}:{sup_val}:table_names")
        if members:
            for m in members:
                name = m if isinstance(m, str) else m.decode("utf-8")
                if name and not (name.startswith("__") and name.endswith("__")):
                    seen.add(name)
    except Exception:
        pass

    # Always scan Redis leaf keys to catch tables not in the set
    try:
        pattern = f"supertable:{org_val}:{sup_val}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:", 1)[-1]
                if simple and not (simple.startswith("__") and simple.endswith("__")):
                    seen.add(simple)
            if cursor == 0:
                break
    except Exception:
        pass

    tables = sorted(seen)
    return {"ok": True, "tables": tables}


@router.get("/api/v1/ingestion/stagings/files")
def api_ingestion_list_staging_files(
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    _: Any = Depends(logged_in_guard_api),
):
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    storage = get_storage()
    base_dir = os.path.join(org, sup, "staging")
    index_path = os.path.join(base_dir, f"{staging_name}_files.json")

    if not storage.exists(index_path):
        return {"items": [], "total": 0, "offset": offset, "limit": limit}

    data = storage.read_json(index_path) or []
    if not isinstance(data, list):
        data = []

    # Sort by written_at_ns descending (newest first) for display
    try:
        data = sorted(data, key=lambda x: int(x.get("written_at_ns") or 0) if isinstance(x, dict) else 0, reverse=True)
    except Exception:
        pass

    total = len(data)
    items = data[offset: offset + limit]
    return {"items": items, "total": total, "offset": offset, "limit": limit}


@router.get("/api/v1/ingestion/pipes")
def api_ingestion_list_pipes(
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    pipe_names = _redis_list_pipes(org, sup, staging_name)
    if not pipe_names:
        return {"items": []}

    keys = [_pipe_key(org, sup, staging_name, pn) for pn in pipe_names]
    try:
        pl = redis_client.pipeline()
        for k in keys:
            pl.get(k)
        raws = pl.execute()
    except Exception:
        raws = [redis_client.get(k) for k in keys]

    items: List[Dict[str, Any]] = []
    for pn, raw in zip(pipe_names, raws):
        meta = _redis_json_load(raw) or {}
        items.append({"pipe_id": pn, "pipe_name": str(meta.get("pipe_name") or pn), "simple_name": str(meta.get("simple_name") or ""), "overwrite_columns": meta.get("overwrite_columns") or [], "enabled": bool(meta.get("enabled"))})

    items.sort(key=lambda x: str(x.get("pipe_name") or ""))
    return {"items": items}


@router.get("/api/v1/ingestion/pipes/meta")
def api_ingestion_get_pipe_meta(
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_id: str = Query(""),
    pipe_name: str = Query(""),
    _: Any = Depends(logged_in_guard_api),
):
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    pid = (pipe_id or pipe_name or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="pipe_id or pipe_name required")
    meta = _redis_get_pipe_meta(org, sup, staging_name, pid) or {}
    return {"meta": meta}


@router.post("/api/v1/ingestion/stagings/create")
def api_create_staging(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    from supertable.staging_area import Staging

    try:
        Staging(organization=org, super_name=sup, staging_name=staging_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Create staging failed: {e}")

    storage = get_storage()
    names = _get_staging_names(storage, org, sup)
    if staging_name not in names:
        names = sorted(set(names + [staging_name]))
        _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

    _redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.STAGING_CREATE,
        organization=org, super_name=sup, resource_type="staging",
        resource_id=staging_name, severity=Severity.INFO,
        detail=make_detail(role_name=role_eff),
    )
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}


@router.post("/api/v1/ingestion/stagings/delete")
def api_delete_staging(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    storage = get_storage()
    target = os.path.join(_staging_base_dir(org, sup), staging_name)

    try:
        if storage.exists(target):
            storage.delete(target)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete staging failed: {e}")

    names = _get_staging_names(storage, org, sup)
    if staging_name in names:
        names = [n for n in names if n != staging_name]
        _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        pipes2 = [p for p in pipes if str(p.get("staging_name") or "") != staging_name]
        if pipes2 != pipes:
            _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

    _redis_delete_staging_cascade(org, sup, staging_name)
    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.STAGING_DELETE,
        organization=org, super_name=sup, resource_type="staging",
        resource_id=staging_name, severity=Severity.WARNING,
        detail=make_detail(role_name=role_eff),
    )
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}


@router.post("/api/v1/ingestion/pipes/save")
def api_save_pipe(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Security: org, sup, role_name are ALWAYS taken from the session — never
    # from the payload.  This prevents a crafted JSON from writing pipes into
    # a different organization or escalating privileges.
    sess = get_session(request) or {}
    sess_org = str(sess.get("org") or "").strip()
    sess_sup = str(sess.get("sup") or sess.get("super_name") or "").strip()
    sess_role = str(sess.get("role_name") or "").strip()

    # Fall back to payload only if session doesn't carry org/sup (token auth)
    org = sess_org or str(payload.get("organization") or payload.get("org") or "").strip()
    sup = sess_sup or str(payload.get("super_name") or payload.get("sup") or "").strip()
    role_eff = sess_role or str(payload.get("role_name") or "").strip() or "superadmin"

    staging_name = str(payload.get("staging_name") or "").strip()
    pipe_name = str(payload.get("pipe_name") or "").strip()
    pipe_id = str(payload.get("pipe_id") or "").strip()

    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    if not _STAGING_NAME_RE.fullmatch(staging_name):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    if not pipe_name:
        raise HTTPException(status_code=400, detail="pipe_name is required")

    # Generate pipe_id if not provided (new pipe)
    _is_new_pipe = not pipe_id
    if not pipe_id:
        pipe_id = "p_" + uuid.uuid4().hex[:8]

    # Read existing pipe meta for create-vs-update detection
    _prev_meta: Optional[Dict[str, Any]] = None
    if not _is_new_pipe:
        _prev_meta = _redis_get_pipe_meta(org, sup, staging_name, pipe_id)

    storage = get_storage()
    stg_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
    known_stagings = _get_staging_names(storage, org, sup)
    if staging_name not in known_stagings:
        try:
            if not storage.exists(stg_dir):
                raise HTTPException(status_code=404, detail="Staging not found")
        except HTTPException:
            raise
        except Exception:
            pass

    pipe_def: Dict[str, Any] = dict(payload)
    pipe_def["organization"] = org
    pipe_def["super_name"] = sup
    pipe_def["staging_name"] = staging_name
    pipe_def["pipe_name"] = pipe_name
    pipe_def["role_name"] = role_eff
    pipe_def.setdefault("enabled", True)
    pipe_def.setdefault("overwrite_columns", ["day"])
    pipe_def.setdefault("meta", {})
    pipe_def["updated_at_ns"] = time.time_ns()

    pipe_def["pipe_id"] = pipe_id
    pipe_path = os.path.join(stg_dir, "pipes", f"{pipe_id}.json")
    try:
        _write_json_atomic(storage, pipe_path, pipe_def)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Save pipe failed: {e}")

    names = _get_staging_names(storage, org, sup)
    if staging_name not in names:
        names = sorted(set(names + [staging_name]))
        _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

    pipes = _load_pipe_index(storage, org, sup)
    pipes = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_id") == pipe_id)]
    overwrite_cols = pipe_def.get("overwrite_columns")
    if not isinstance(overwrite_cols, list):
        overwrite_cols = []
    pipes.append({
        "pipe_id": pipe_id, "pipe_name": pipe_name, "organization": org, "super_name": sup,
        "staging_name": staging_name, "role_name": role_eff,
        "simple_name": str(pipe_def.get("simple_name") or "").strip(),
        "overwrite_columns": overwrite_cols, "enabled": bool(pipe_def.get("enabled")),
        "path": pipe_path, "updated_at_ns": time.time_ns(),
    })
    _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

    _redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
    _redis_upsert_pipe_meta(org, sup, staging_name, pipe_id, pipe_def)

    _simple = str(pipe_def.get("simple_name") or "").strip()
    _ow = pipe_def.get("overwrite_columns") or []
    _action = Actions.PIPE_CREATE if _is_new_pipe or not _prev_meta else Actions.PIPE_UPDATE
    _detail = make_detail(
        staging=staging_name, pipe=pipe_name, pipe_id=pipe_id,
        simple_name=_simple, overwrite_columns=_ow,
        enabled=pipe_def.get("enabled"), role_name=role_eff,
    )
    if _prev_meta and not _is_new_pipe:
        _prev_name = str(_prev_meta.get("pipe_name") or "")
        _prev_simple = str(_prev_meta.get("simple_name") or "")
        _prev_ow = _prev_meta.get("overwrite_columns") or []
        _changes = {}
        if _prev_name and _prev_name != pipe_name:
            _changes["pipe_name"] = {"from": _prev_name, "to": pipe_name}
        if _prev_simple != _simple:
            _changes["simple_name"] = {"from": _prev_simple, "to": _simple}
        if sorted(str(x) for x in _prev_ow) != sorted(str(x) for x in (_ow if isinstance(_ow, list) else [])):
            _changes["overwrite_columns"] = {"from": _prev_ow, "to": _ow}
        if _changes:
            _detail["changes"] = _changes
    _audit(
        category=EventCategory.CONFIG_CHANGE if not _is_new_pipe and _prev_meta else EventCategory.DATA_MUTATION,
        action=_action,
        organization=org, super_name=sup, resource_type="pipe",
        resource_id=f"{staging_name}/{pipe_id}", severity=Severity.WARNING,
        detail=_detail,
    )
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name, "pipe_name": pipe_name, "pipe_id": pipe_id, "path": pipe_path}


@router.post("/api/v1/ingestion/pipes/delete")
def api_delete_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_id: str = Query(""),
    pipe_name: str = Query(""),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    pid = (pipe_id or pipe_name or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="pipe_id required")

    storage = get_storage()
    path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pid}.json")

    try:
        if storage.exists(path):
            storage.delete(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete pipe failed: {e}")

    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        pipes2 = [p for p in pipes if not (p.get("staging_name") == staging_name and (p.get("pipe_id") == pid or p.get("pipe_name") == pid))]
        if pipes2 != pipes:
            _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

    _redis_delete_pipe_meta(org, sup, staging_name, pid)
    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.PIPE_DELETE,
        organization=org, super_name=sup, resource_type="pipe",
        resource_id=f"{staging_name}/{pid}", severity=Severity.WARNING,
        detail=make_detail(role_name=role_eff),
    )
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name, "pipe_id": pid}


@router.post("/api/v1/ingestion/pipes/enable")
def api_enable_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_id: str = Query(""),
    pipe_name: str = Query(""),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    pid = (pipe_id or pipe_name or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="pipe_id required")

    # Redis first (fast path)
    meta = _redis_get_pipe_meta(org, sup, staging_name, pid) or {}
    if not meta:
        storage = get_storage()
        meta = _read_json_if_exists(storage, os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pid}.json")) or {}
    if not meta:
        raise HTTPException(status_code=404, detail=f"Pipe '{pid}' not found")

    meta["enabled"] = True
    meta["updated_at_ns"] = time.time_ns()
    _redis_upsert_pipe_meta(org, sup, staging_name, pid, meta)

    # Async disk write (non-blocking for the response)
    try:
        storage = get_storage()
        _write_json_atomic(storage, os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pid}.json"), meta)
    except Exception:
        pass

    try:
        _audit(
            **audit_context(request), category=EventCategory.CONFIG_CHANGE,
            action=Actions.PIPE_ENABLE, organization=org, super_name=sup,
            resource_type="pipe", resource_id=f"{staging_name}/{pid}",
            severity=Severity.WARNING,
            detail=make_detail(pipe=pid, staging=staging_name, role_name=role_eff),
        )
    except Exception:
        pass

    return {"ok": True}
def api_disable_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_id: str = Query(""),
    pipe_name: str = Query(""),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    pid = (pipe_id or pipe_name or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="pipe_id required")

    # Redis first (fast path)
    meta = _redis_get_pipe_meta(org, sup, staging_name, pid) or {}
    if not meta:
        storage = get_storage()
        meta = _read_json_if_exists(storage, os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pid}.json")) or {}
    if not meta:
        raise HTTPException(status_code=404, detail=f"Pipe '{pid}' not found")

    meta["enabled"] = False
    meta["updated_at_ns"] = time.time_ns()
    _redis_upsert_pipe_meta(org, sup, staging_name, pid, meta)

    # Async disk write (non-blocking for the response)
    try:
        storage = get_storage()
        _write_json_atomic(storage, os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pid}.json"), meta)
    except Exception:
        pass

    try:
        _audit(
            **audit_context(request), category=EventCategory.CONFIG_CHANGE,
            action=Actions.PIPE_DISABLE, organization=org, super_name=sup,
            resource_type="pipe", resource_id=f"{staging_name}/{pid}",
            severity=Severity.WARNING,
            detail=make_detail(pipe=pid, staging=staging_name, role_name=role_eff),
        )
    except Exception:
        pass

    return {"ok": True}


@router.post("/api/v1/ingestion/upload")
async def api_ingestion_load_upload(
    request: Request,
    org: str = Form(...),
    sup: str = Form(...),
    role_name: str = Form(""),
    mode: str = Form(...),
    table_name: Optional[str] = Form(None),
    staging_name: Optional[str] = Form(None),
    overwrite_columns: Optional[str] = Form(None),
    delete_only: Optional[str] = Form(None),
    file: UploadFile = File(...),
    _: Any = Depends(logged_in_guard_api),
):
    t0 = time.perf_counter()
    org_eff = (org or "").strip()
    sup_eff = (sup or "").strip()
    if not org_eff or not sup_eff:
        raise HTTPException(status_code=400, detail="Missing org or sup")

    sess = None
    try:
        sess = get_session(request) or {}
    except Exception:
        sess = None

    sess_org = str((sess or {}).get("org") or "")
    sess_role = str((sess or {}).get("role_name") or "")
    if sess_org and sess_org != org_eff:
        raise HTTPException(status_code=403, detail="Forbidden")

    role_eff = (role_name or sess_role or "").strip()
    if not role_eff:
        raise HTTPException(status_code=400, detail="Missing role_name")

    mode_eff = (mode or "").strip().lower()
    if mode_eff not in ("table", "staging"):
        raise HTTPException(status_code=400, detail="Invalid mode (expected 'table' or 'staging')")

    if mode_eff == "table":
        table_eff = (table_name or "").strip()
        if not table_eff:
            raise HTTPException(status_code=400, detail="Missing table_name")
        if any(x in table_eff for x in ("/", "\\", "\x00")) or ".." in table_eff:
            raise HTTPException(status_code=400, detail="Invalid table_name")
    else:
        stg_eff = (staging_name or "").strip()
        if not stg_eff:
            raise HTTPException(status_code=400, detail="Missing staging_name")
        if not _STAGING_NAME_RE.match(stg_eff):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

    filename = (file.filename or "upload").strip()
    ext = Path(filename).suffix.lower().lstrip(".")
    if ext in ("", None):
        ext = ""

    def _persist_upload_to_temp(upload: UploadFile) -> str:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=(".%s" % ext) if ext else "")
        tmp_path = tmp.name
        try:
            with tmp:
                while True:
                    chunk = upload.file.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)
        finally:
            try:
                upload.file.close()
            except Exception:
                pass
        return tmp_path

    tmp_path = await asyncio.to_thread(_persist_upload_to_temp, file)

    def _read_arrow_table(path: str, file_ext: str) -> Any:
        try:
            import pyarrow as pa
            import pyarrow.csv as pa_csv
            import pyarrow.json as pa_json
            import pyarrow.parquet as pa_parquet
        except Exception as e:
            raise RuntimeError(f"pyarrow import failed: {e}")

        e = (file_ext or "").lower()
        if e in ("csv", "tsv"):
            delimiter = "\t" if e == "tsv" else ","
            read_opts = pa_csv.ReadOptions(autogenerate_column_names=False)
            parse_opts = pa_csv.ParseOptions(delimiter=delimiter)
            convert_opts = pa_csv.ConvertOptions(strings_can_be_null=True)
            return pa_csv.read_csv(path, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)
        if e in ("parquet", "pq"):
            return pa_parquet.read_table(path)
        if e in ("jsonl", "ndjson"):
            return pa_json.read_json(path)
        if e == "json":
            try:
                return pa_json.read_json(path)
            except Exception:
                import json as json_mod
                with open(path, "r", encoding="utf-8") as fh:
                    data = json_mod.load(fh)
                if isinstance(data, list):
                    return pa.Table.from_pylist(data)
                raise
        raise RuntimeError("Unsupported file type. Supported: csv, tsv, json/jsonl, parquet")

    try:
        arrow_table = await asyncio.to_thread(_read_arrow_table, tmp_path, ext)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    job_uuid = str(uuid.uuid4())

    if mode_eff == "staging":
        stg_eff = (staging_name or "").strip()
        base_name = Path(filename).stem or "upload"
        base_name = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._-") or "upload"

        try:
            from supertable.staging_area import Staging
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Staging import failed: {e}")

        def _do_stage() -> str:
            stg = Staging(organization=org_eff, super_name=sup_eff, staging_name=stg_eff)
            result = stg.save_as_parquet(role_name=role_eff, arrow_table=arrow_table, base_file_name=base_name,
                                         source="upload", duration_ms=0)
            return result

        try:
            saved = await asyncio.to_thread(_do_stage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Staging save failed: {e}")

        staging_ms = (time.perf_counter() - t0) * 1000.0
        rows_count = getattr(arrow_table, "num_rows", None)

        _audit(
            **audit_context(request), category=EventCategory.DATA_MUTATION,
            action=Actions.FILE_UPLOAD, organization=org_eff, super_name=sup_eff,
            resource_type="staging", resource_id=stg_eff, severity=Severity.INFO,
            detail=make_detail(
                mode="staging", staging=stg_eff, filename=file.filename or "",
                rows=rows_count, file_type=ext, duration_ms=round(staging_ms),
            ),
        )
        # Staging uploads bypass DataWriter, so emit a monitoring metric directly.
        try:
            with MonitoringWriter(
                organization=org_eff, super_name=sup_eff, monitor_type="writes",
            ) as mon:
                mon.log_metric({
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "source": "upload",
                    "source_type": "staging_upload",
                    "mode": "staging",
                    "table_name": "",
                    "staging_name": stg_eff,
                    "file_name": file.filename or "",
                    "file_type": ext or "unknown",
                    "incoming_rows": rows_count if rows_count is not None else 0,
                    "inserted": rows_count if rows_count is not None else 0,
                    "deleted": 0,
                    "duration": round(staging_ms / 1000.0, 6),
                    "role_name": role_eff,
                    "username": (sess or {}).get("username", ""),
                })
        except Exception:
            pass  # Never fail an upload due to monitoring

        # Auto-execute enabled pipes for the just-uploaded file
        pipe_results: List[Dict[str, Any]] = []
        try:
            pipe_names = _redis_list_pipes(org_eff, sup_eff, stg_eff)
            for pn in pipe_names:
                pmeta = _redis_get_pipe_meta(org_eff, sup_eff, stg_eff, pn)
                if not pmeta or not pmeta.get("enabled"):
                    continue
                target_table = str(pmeta.get("simple_name") or "").strip()
                if not target_table:
                    continue
                ow = pmeta.get("overwrite_columns") or []
                if not isinstance(ow, list):
                    ow = []

                def _do_pipe_load(_tbl=target_table, _ow=ow, _pn=pn) -> Dict[str, Any]:
                    import pyarrow as pa  # noqa: F811
                    _pt0 = time.perf_counter()
                    stage_dir = os.path.join(_staging_base_dir(org_eff, sup_eff), stg_eff)
                    fpath = os.path.join(stage_dir, saved)
                    tbl = get_storage().read_parquet(fpath)
                    from supertable.data_writer import DataWriter
                    dw = DataWriter(super_name=sup_eff, organization=org_eff)
                    lineage = {
                        "source_type": "pipe_execute",
                        "pipe_name": str(pmeta.get("pipe_name") or _pn),
                        "staging_name": stg_eff,
                        "file_name": saved,
                        "role_name": role_eff,
                        "username": (sess or {}).get("username", ""),
                        "overwrite_columns": _ow,
                    }
                    _cols, _rows, _ins, _del = dw.write(
                        role_name=role_eff, simple_name=_tbl, data=tbl,
                        overwrite_columns=_ow, lineage=lineage,
                    )
                    _pt_ms = (time.perf_counter() - _pt0) * 1000.0
                    return {"pipe_name": str(pmeta.get("pipe_name") or _pn), "pipe_id": _pn,
                            "table": _tbl, "inserted": _ins, "deleted": _del,
                            "duration_ms": round(_pt_ms, 1), "ok": True}

                try:
                    pr = await asyncio.to_thread(_do_pipe_load)
                    pipe_results.append(pr)
                    try:
                        _audit(
                            **audit_context(request), category=EventCategory.DATA_MUTATION,
                            action=Actions.PIPE_EXECUTE, organization=org_eff, super_name=sup_eff,
                            resource_type="pipe", resource_id=f"{stg_eff}/{pn}",
                            severity=Severity.INFO,
                            detail=make_detail(
                                pipe=str(pmeta.get("pipe_name") or pn), staging=stg_eff,
                                table=target_table, file=saved,
                                inserted=pr.get("inserted", 0), deleted=pr.get("deleted", 0),
                                duration_ms=pr.get("duration_ms", 0),
                                role_name=role_eff, auto_trigger="staging_upload",
                            ),
                        )
                    except Exception:
                        pass
                except Exception as pe:
                    pipe_results.append({
                        "pipe_name": str(pmeta.get("pipe_name") or pn), "pipe_id": pn,
                        "table": target_table, "ok": False, "error": str(pe),
                    })
                    try:
                        _audit(
                            **audit_context(request), category=EventCategory.DATA_MUTATION,
                            action=Actions.PIPE_EXECUTE, organization=org_eff, super_name=sup_eff,
                            resource_type="pipe", resource_id=f"{stg_eff}/{pn}",
                            severity=Severity.WARNING, outcome=Outcome.FAILURE,
                            detail=make_detail(
                                pipe=str(pmeta.get("pipe_name") or pn), staging=stg_eff,
                                table=target_table, file=saved,
                                error=str(pe), role_name=role_eff,
                                auto_trigger="staging_upload",
                            ),
                        )
                    except Exception:
                        pass
        except Exception:
            pass  # Never fail an upload due to pipe execution

        total_ms = (time.perf_counter() - t0) * 1000.0

        # Patch file index with full duration breakdown (after all pipes complete)
        try:
            _idx_path = os.path.join(org_eff, sup_eff, "staging", f"{stg_eff}_files.json")
            _idx = get_storage().read_json(_idx_path) or []
            if isinstance(_idx, list) and _idx:
                last = _idx[-1]
                if isinstance(last, dict) and last.get("file") == saved:
                    last["duration_ms"] = round(total_ms)
                    last["staging_ms"] = round(staging_ms)
                    if pipe_results:
                        last["pipe_results"] = [
                            {k: v for k, v in pr.items() if k in ("pipe_name", "table", "duration_ms", "inserted", "deleted", "ok")}
                            for pr in pipe_results
                        ]
                    get_storage().write_json(_idx_path, _idx)
        except Exception:
            pass  # best effort

        return {
            "ok": True, "mode": "staging", "organization": org_eff, "super_name": sup_eff,
            "staging_name": stg_eff, "saved_file_name": saved, "rows": rows_count if rows_count is not None else 0,
            "file_type": ext or "unknown", "job_uuid": job_uuid,
            "server_duration_ms": total_ms, "staging_duration_ms": staging_ms,
            "pipe_results": pipe_results,
        }

    table_eff = (table_name or "").strip()
    overwrite_cols: List[str] = []
    if overwrite_columns:
        raw = str(overwrite_columns)
        overwrite_cols = [c.strip() for c in re.split(r"[\n,]+", raw) if c.strip()]

    is_delete = str(delete_only or "").strip().lower() in ("1", "true", "yes")

    # Build data lineage metadata
    upload_lineage = {
        "source_type": "api_upload",
        "filename": file.filename or "",
        "file_type": ext or "unknown",
        "username": (sess or {}).get("username", ""),
        "role_name": role_eff,
        "delete_only": is_delete,
        "overwrite_columns": overwrite_cols,
    }

    try:
        from supertable.data_writer import DataWriter
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DataWriter import failed: {e}")

    def _do_write() -> Any:
        dw = DataWriter(super_name=sup_eff, organization=org_eff)
        return dw.write(role_name=role_eff, simple_name=table_eff, data=arrow_table, overwrite_columns=overwrite_cols, delete_only=is_delete, lineage=upload_lineage)

    try:
        cols, rows_written, inserted, deleted = await asyncio.to_thread(_do_write)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Write failed: {e}")

    dt_ms = (time.perf_counter() - t0) * 1000.0
    _audit(
        **audit_context(request), category=EventCategory.DATA_MUTATION,
        action=Actions.DATA_DELETE if is_delete else Actions.FILE_UPLOAD,
        organization=org_eff, super_name=sup_eff,
        resource_type="table", resource_id=table_eff, severity=Severity.WARNING if is_delete else Severity.INFO,
        detail=make_detail(
            mode="table", table=table_eff, filename=file.filename or "",
            rows=rows_written, inserted=inserted, deleted=deleted,
            file_type=ext, duration_ms=round(dt_ms), delete_only=is_delete,
        ),
    )
    return {
        "ok": True, "mode": "table", "organization": org_eff, "super_name": sup_eff,
        "table_name": table_eff, "rows": rows_written, "inserted": inserted, "deleted": deleted,
        "delete_only": is_delete,
        "file_type": ext or "unknown", "job_uuid": job_uuid, "server_duration_ms": dt_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
#   FILE COLUMN EXTRACTION (for ingestion delete mode)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/api/v1/ingestion/file-columns")
async def api_file_columns(
    request: Request,
    file: UploadFile = File(...),
    preview: int = Form(10),
    _: Any = Depends(logged_in_guard_api),
):
    """Extract column names and optional preview rows from an uploaded file.

    Used by the ingestion UI for schema display and data preview.
    For Parquet files (where client-side preview is not possible), returns
    up to `preview` sample rows alongside column names.
    """
    filename = (file.filename or "upload").strip()
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    preview_limit = max(0, min(preview or 10, 50))

    def _extract(upload: UploadFile) -> Tuple[List[str], List[Dict[str, Any]], int]:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}" if ext else "")
        tmp_path = tmp.name
        try:
            with tmp:
                while True:
                    chunk = upload.file.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)

            import pyarrow.parquet as pq_mod
            import pyarrow.csv as pa_csv_mod
            import pyarrow.json as pa_json_mod

            if ext in ("parquet", "pq"):
                tbl = pq_mod.read_table(tmp_path)
                cols = tbl.column_names
                total = tbl.num_rows
                rows: List[Dict[str, Any]] = []
                if preview_limit > 0:
                    sliced = tbl.slice(0, min(preview_limit, total))
                    for i in range(sliced.num_rows):
                        row = {}
                        for c in cols:
                            v = sliced.column(c)[i].as_py()
                            row[c] = v
                        rows.append(row)
                return cols, rows, total
            elif ext in ("csv", "tsv"):
                delimiter = "\t" if ext == "tsv" else ","
                read_opts = pa_csv_mod.ReadOptions(autogenerate_column_names=False)
                parse_opts = pa_csv_mod.ParseOptions(delimiter=delimiter)
                tbl = pa_csv_mod.read_csv(tmp_path, read_options=read_opts, parse_options=parse_opts)
                return tbl.column_names, [], tbl.num_rows
            elif ext in ("json", "jsonl", "ndjson"):
                tbl = pa_json_mod.read_json(tmp_path)
                return tbl.column_names, [], tbl.num_rows
            else:
                return [], [], 0
        finally:
            try:
                upload.file.close()
            except Exception:
                pass
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    try:
        columns, rows, total = await asyncio.to_thread(_extract, file)
    except Exception as e:
        return JSONResponse({"ok": False, "columns": [], "rows": [], "error": str(e)}, status_code=400)

    return {"ok": True, "columns": columns, "rows": rows, "total_rows": total, "file_type": ext, "file_name": filename}


# ═══════════════════════════════════════════════════════════════════════════
#   INGESTION LANDSCAPE (single-call optimized endpoint)
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/v1/ingestion/summary")
def api_ingestion_summary(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    hours: int = Query(24, ge=1, le=672),
    _: Any = Depends(logged_in_guard_api),
):
    """Single optimized endpoint for the Ingestion Landscape view.

    Returns stagings, pipes, recent writes, and activity histogram
    in one round-trip.  Filters writes by the requested time window.
    """
    from datetime import datetime, timezone, timedelta

    # 1. Stagings with file counts
    names = _redis_list_stagings(org, sup)
    storage = get_storage()
    stagings: List[Dict[str, Any]] = []
    total_files = 0
    total_rows = 0
    for name in names:
        idx_path = os.path.join(_staging_base_dir(org, sup), f"{name}_files.json")
        fc, tr = 0, 0
        try:
            data = storage.read_json(idx_path) or []
            if isinstance(data, list):
                fc = len(data)
                for item in data:
                    if isinstance(item, dict):
                        tr += int(item.get("rows") or 0)
        except Exception:
            pass
        last_ns = 0
        try:
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        ns = int(item.get("written_at_ns") or 0)
                        if ns > last_ns:
                            last_ns = ns
        except Exception:
            pass
        pc = len(_redis_list_pipes(org, sup, name))
        stagings.append({"name": name, "file_count": fc, "total_rows": tr, "pipe_count": pc, "last_written_at_ns": last_ns})
        total_files += fc
        total_rows += tr

    # 2. All pipes across all stagings — batch via Redis pipeline
    pipes: List[Dict[str, Any]] = []
    all_pipe_keys: List[Tuple[str, str]] = []  # (stg_name, pipe_name)
    for stg_name in names:
        pipe_names = _redis_list_pipes(org, sup, stg_name)
        for pn in pipe_names:
            all_pipe_keys.append((stg_name, pn))

    if all_pipe_keys:
        try:
            pl = redis_client.pipeline()
            for stg_name, pn in all_pipe_keys:
                pl.get(_pipe_key(org, sup, stg_name, pn))
            raws = pl.execute()
        except Exception:
            raws = [None] * len(all_pipe_keys)

        for (stg_name, pn), raw in zip(all_pipe_keys, raws):
            meta = _redis_json_load(raw) or {}
            pipes.append({
                "pipe_id": pn,
                "pipe_name": str(meta.get("pipe_name") or pn),
                "staging_name": stg_name,
                "simple_name": str(meta.get("simple_name") or ""),
                "overwrite_columns": meta.get("overwrite_columns") or [],
                "enabled": bool(meta.get("enabled")),
            })

    # 3. Recent writes (time-filtered)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat()
    writes: List[Dict[str, Any]] = []
    write_total_rows = 0
    write_total_dur = 0.0
    dur_count = 0
    try:
        raw_items = _read_monitoring_list(
            redis_client, org, sup,
            monitor_type="writes", from_ts_ms=None, to_ts_ms=None, limit=500,
        )
        for item in raw_items:
            ts = item.get("recorded_at") or ""
            if ts and ts < cutoff_iso:
                continue
            # Filter out system tables (__xxx__)
            tname = str(item.get("table_name") or "")
            if tname.startswith("__") and tname.endswith("__"):
                continue
            # Extract source_type from lineage JSON string
            _lin = item.get("lineage")
            _src = ""
            if isinstance(_lin, str) and _lin:
                try:
                    _lin_obj = json.loads(_lin)
                    _src = str(_lin_obj.get("source_type") or "")
                except Exception:
                    pass
            elif isinstance(_lin, dict):
                _src = str(_lin.get("source_type") or "")
            if not _src:
                _src = str(item.get("source") or item.get("source_type") or "")
            item["source_type"] = _src or "unknown"
            writes.append(item)
            write_total_rows += int(item.get("inserted") or 0)
            d = item.get("duration")
            if d is not None:
                write_total_dur += float(d)
                dur_count += 1
    except Exception as e:
        logger.warning("[landscape] Failed to read writes: %s", e)

    avg_dur_ms = round((write_total_dur / dur_count) * 1000) if dur_count else 0

    # 4. Activity histogram (bucket writes by hour) — split by source
    bucket_count = min(hours, 48)  # cap at 48 buckets
    bucket_size_h = max(1, hours // bucket_count)
    now = datetime.now(timezone.utc)
    buckets = [0] * bucket_count
    upload_buckets = [0] * bucket_count
    pipe_buckets = [0] * bucket_count
    for item in writes:
        ts = item.get("recorded_at") or ""
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            hrs_ago = (now - dt).total_seconds() / 3600
            idx = int(hrs_ago / bucket_size_h)
            if 0 <= idx < bucket_count:
                rows = int(item.get("inserted") or 0)
                bi = bucket_count - 1 - idx
                buckets[bi] += rows
                src = item.get("source_type") or ""
                if src == "pipe_execute":
                    pipe_buckets[bi] += rows
                else:
                    upload_buckets[bi] += rows
        except Exception:
            pass

    return {
        "ok": True,
        "stagings": stagings,
        "pipes": pipes,
        "recent_writes": writes[:30],
        "total_staging_count": len(names),
        "total_files": total_files,
        "total_rows": total_rows,
        "write_count": len(writes),
        "write_total_rows": write_total_rows,
        "avg_duration_ms": avg_dur_ms,
        "active_pipes": sum(1 for p in pipes if p["enabled"]),
        "total_pipes": len(pipes),
        "activity_buckets": buckets,
        "upload_buckets": upload_buckets,
        "pipe_buckets": pipe_buckets,
        "bucket_size_hours": bucket_size_h,
        "hours": hours,
    }


# ═══════════════════════════════════════════════════════════════════════════
#   PIPE EXECUTE / STAGING LOAD / PREVIEW / PURGE
# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/v1/ingestion/pipes/execute")
async def api_execute_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_id: str = Query(""),
    pipe_name: str = Query(""),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Execute a pipe: read all staging files and write to the target table."""
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    t0 = time.perf_counter()

    pid = (pipe_id or pipe_name or "").strip()
    # Read pipe meta — try Redis first, fall back to disk
    meta = _redis_get_pipe_meta(org, sup, staging_name, pid)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Pipe '{pid}' not found")
    if not meta.get("enabled"):
        raise HTTPException(status_code=400, detail=f"Pipe '{pid}' is disabled")

    simple_name = str(meta.get("simple_name") or "").strip()
    if not simple_name:
        raise HTTPException(status_code=400, detail="Pipe has no target table (simple_name)")

    overwrite_cols = meta.get("overwrite_columns") or []
    if not isinstance(overwrite_cols, list):
        overwrite_cols = []

    # Read staging files
    storage = get_storage()
    idx_path = os.path.join(_staging_base_dir(org, sup), f"{staging_name}_files.json")
    try:
        file_index = storage.read_json(idx_path) or []
    except Exception:
        file_index = []
    if not file_index:
        raise HTTPException(status_code=400, detail="No files in staging area")

    def _do_execute():
        import pyarrow as pa
        tables = []
        stage_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
        for item in file_index:
            if not isinstance(item, dict):
                continue
            fname = item.get("file")
            if not fname:
                continue
            fpath = os.path.join(stage_dir, fname)
            try:
                tbl = storage.read_parquet(fpath)
                tables.append(tbl)
            except Exception as e:
                logger.warning("[pipe-execute] Failed to read %s: %s", fpath, e)
        if not tables:
            raise RuntimeError("No readable parquet files in staging")
        combined = pa.concat_tables(tables, promote_options="default")

        from supertable.data_writer import DataWriter
        dw = DataWriter(super_name=sup, organization=org)
        lineage = {
            "source_type": "pipe_execute",
            "pipe_name": pipe_name,
            "staging_name": staging_name,
            "role_name": role_eff,
            "username": (sess or {}).get("username", ""),
            "overwrite_columns": overwrite_cols,
        }
        return dw.write(
            role_name=role_eff, simple_name=simple_name, data=combined,
            overwrite_columns=overwrite_cols, lineage=lineage,
        )

    try:
        cols, rows_written, inserted, deleted = await asyncio.to_thread(_do_execute)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipe execute failed: {e}")

    dt_ms = (time.perf_counter() - t0) * 1000.0
    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.PIPE_EXECUTE,
        organization=org, super_name=sup, resource_type="pipe",
        resource_id=f"{staging_name}/{pipe_name}", severity=Severity.INFO,
        detail=make_detail(
            pipe=pipe_name, staging=staging_name, table=simple_name,
            rows=rows_written, inserted=inserted, deleted=deleted,
            role_name=role_eff, duration_ms=round(dt_ms),
        ),
    )
    return {
        "ok": True, "pipe_name": pipe_name, "table": simple_name,
        "rows": rows_written, "inserted": inserted, "deleted": deleted,
        "files_processed": len(file_index), "server_duration_ms": dt_ms,
    }


@router.get("/api/v1/ingestion/staging/preview")
async def api_staging_file_preview(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    file_name: str = Query(...),
    limit: int = Query(20, ge=1, le=200),
    _: Any = Depends(logged_in_guard_api),
):
    """Preview rows from a staging file."""
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    storage = get_storage()
    fpath = os.path.join(_staging_base_dir(org, sup), staging_name, file_name.strip())

    def _read():
        tbl = storage.read_parquet(fpath)
        sliced = tbl.slice(0, min(limit, tbl.num_rows))
        cols = sliced.column_names
        rows = []
        for i in range(sliced.num_rows):
            row = {}
            for c in cols:
                v = sliced.column(c)[i].as_py()
                row[c] = v
            rows.append(row)
        return cols, rows, tbl.num_rows

    try:
        columns, rows, total = await asyncio.to_thread(_read)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {e}")

    return {"ok": True, "columns": columns, "rows": rows, "total_rows": total, "preview_rows": len(rows)}


@router.post("/api/v1/ingestion/staging/delete-file")
def api_staging_delete_file(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    file_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Delete a single file from a staging area."""
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    fname = (file_name or "").strip()
    if not fname or "/" in fname or "\\" in fname or ".." in fname:
        raise HTTPException(status_code=400, detail="Invalid file_name")

    storage = get_storage()
    stage_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
    fpath = os.path.join(stage_dir, fname)
    idx_path = os.path.join(_staging_base_dir(org, sup), f"{staging_name}_files.json")

    # Delete the physical file
    try:
        if storage.exists(fpath):
            storage.delete(fpath)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")

    # Update the file index
    try:
        file_index = storage.read_json(idx_path) or []
        if isinstance(file_index, list):
            file_index = [it for it in file_index if not (isinstance(it, dict) and it.get("file") == fname)]
            storage.write_json(idx_path, file_index)
    except Exception:
        pass  # Index update is best-effort

    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.STAGING_DELETE,
        organization=org, super_name=sup, resource_type="staging_file",
        resource_id=f"{staging_name}/{fname}", severity=Severity.WARNING,
        detail=make_detail(role_name=role_eff, file_name=fname),
    )
    return {"ok": True, "staging_name": staging_name, "file_name": fname}


@router.post("/api/v1/ingestion/staging/purge")
def api_staging_purge(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Delete all files from a staging area without deleting the staging itself."""
    sess = get_session(request) or {}
    role_eff = (role_name or (sess.get("role_name") or "")).strip() or "superadmin"
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")

    storage = get_storage()
    stage_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
    idx_path = os.path.join(_staging_base_dir(org, sup), f"{staging_name}_files.json")

    deleted_count = 0
    try:
        file_index = storage.read_json(idx_path) or []
        if isinstance(file_index, list):
            for item in file_index:
                if not isinstance(item, dict):
                    continue
                fname = item.get("file")
                if not fname:
                    continue
                try:
                    storage.delete(os.path.join(stage_dir, fname))
                    deleted_count += 1
                except Exception:
                    pass
        # Reset the index
        storage.write_json(idx_path, [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Purge failed: {e}")

    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.STAGING_DELETE,
        organization=org, super_name=sup, resource_type="staging_files",
        resource_id=staging_name, severity=Severity.WARNING,
        detail=make_detail(role_name=role_eff, deleted_files=deleted_count),
    )
    return {"ok": True, "staging_name": staging_name, "deleted_files": deleted_count}


@router.post("/api/v1/ingestion/staging/load-to-table")
async def api_staging_load_to_table(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    """Load one or all staged files to a target table."""
    sess = get_session(request) or {}
    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    staging_name = str(payload.get("staging_name") or "").strip()
    table_name = str(payload.get("table_name") or "").strip()
    role_eff = (str(payload.get("role_name") or "") or (sess.get("role_name") or "")).strip() or "superadmin"
    file_name = str(payload.get("file_name") or "").strip()  # empty = all files
    raw_cols = payload.get("overwrite_columns") or []
    if isinstance(raw_cols, str):
        raw_cols = [c.strip() for c in re.split(r"[\n,]+", raw_cols) if c.strip()]
    overwrite_cols: List[str] = raw_cols if isinstance(raw_cols, list) else []

    if not org or not sup:
        raise HTTPException(status_code=400, detail="org and sup required")
    if not staging_name or not _STAGING_NAME_RE.fullmatch(staging_name):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    if not table_name:
        raise HTTPException(status_code=400, detail="table_name required")

    storage = get_storage()
    stage_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
    idx_path = os.path.join(_staging_base_dir(org, sup), f"{staging_name}_files.json")
    t0 = time.perf_counter()

    def _do_load():
        import pyarrow as pa
        file_index = storage.read_json(idx_path) or []
        if not isinstance(file_index, list) or not file_index:
            raise RuntimeError("No files in staging area")

        targets = file_index
        if file_name:
            targets = [it for it in file_index if isinstance(it, dict) and it.get("file") == file_name]
            if not targets:
                raise RuntimeError(f"File '{file_name}' not found in staging")

        # Sort by written_at_ns for deterministic ordering
        targets = sorted(targets, key=lambda x: int(x.get("written_at_ns") or 0))

        tables = []
        for item in targets:
            if not isinstance(item, dict):
                continue
            fname = item.get("file")
            if not fname:
                continue
            fpath = os.path.join(stage_dir, fname)
            try:
                tbl = storage.read_parquet(fpath)
                tables.append(tbl)
            except Exception as e:
                logger.warning("[staging-load] Failed to read %s: %s", fpath, e)
        if not tables:
            raise RuntimeError("No readable parquet files")

        combined = pa.concat_tables(tables, promote_options="default")

        from supertable.data_writer import DataWriter
        dw = DataWriter(super_name=sup, organization=org)
        lineage = {
            "source_type": "staging_load",
            "staging_name": staging_name,
            "file_name": file_name or f"all ({len(targets)} files)",
            "role_name": role_eff,
            "username": (sess or {}).get("username", ""),
            "overwrite_columns": overwrite_cols,
            "incoming_rows": combined.num_rows,
        }
        result = dw.write(
            role_name=role_eff, simple_name=table_name, data=combined,
            overwrite_columns=overwrite_cols, lineage=lineage,
        )
        return result, len(targets), combined.num_rows

    try:
        (cols, rows_written, inserted, deleted), files_loaded, incoming = await asyncio.to_thread(_do_load)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Load failed: {e}")

    dt_ms = (time.perf_counter() - t0) * 1000.0
    _audit(
        category=EventCategory.DATA_MUTATION, action=Actions.DATA_WRITE,
        organization=org, super_name=sup, resource_type="table",
        resource_id=table_name, severity=Severity.INFO,
        detail=make_detail(
            staging=staging_name, table=table_name, role_name=role_eff,
            rows=rows_written, inserted=inserted, deleted=deleted,
            files_loaded=files_loaded, duration_ms=round(dt_ms),
        ),
    )
    return {
        "ok": True, "table_name": table_name, "staging_name": staging_name,
        "files_loaded": files_loaded, "incoming_rows": incoming,
        "rows": rows_written, "inserted": inserted, "deleted": deleted,
        "server_duration_ms": dt_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
#   MONITORING endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/monitoring/reads")
def monitoring_reads(
    org: str = Query(""),
    sup: str = Query(""),
    from_ts: Optional[int] = Query(None),
    to_ts: Optional[int] = Query(None),
    limit: int = Query(500),
    _=Depends(logged_in_guard_api),
):
    """Lightweight read operations list — strips heavy profiler fields.

    Returns only: query_id, recorded_at, table_name, role_name, engine,
    source_type, status, message, result_rows, result_columns, duration_ms.
    ~100× smaller than the full payload.
    """
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "items": []})

    raw_items = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=min(limit, 5000),
    )

    # Strip heavy fields, compute duration_ms from execution_timings
    _LIGHT_KEYS = {
        "query_id", "query_hash", "recorded_at", "table_name", "role_name",
        "engine", "source_type", "status", "message", "result_rows",
        "result_columns", "instance_id",
    }
    items = []
    for it in raw_items:
        row = {k: it[k] for k in _LIGHT_KEYS if k in it}
        # Extract duration_ms from execution_timings
        dur_ms = None
        raw_t = it.get("execution_timings")
        if raw_t:
            try:
                tm = json.loads(raw_t) if isinstance(raw_t, str) else raw_t
                if isinstance(tm, list):
                    # Try TOTAL_EXECUTE first
                    for entry in tm:
                        if isinstance(entry, dict):
                            for k in ("TOTAL_EXECUTE", "total", "total_s"):
                                if k in entry:
                                    dur_ms = round(float(entry[k]) * 1000, 1)
                                    break
                        if dur_ms is not None:
                            break
                    # Fallback: sum all timing entries
                    if dur_ms is None:
                        total = 0.0
                        for entry in tm:
                            if isinstance(entry, dict):
                                for v in entry.values():
                                    try:
                                        total += float(v)
                                    except (TypeError, ValueError):
                                        pass
                        if total > 0:
                            dur_ms = round(total * 1000, 1)
                elif isinstance(tm, dict):
                    for k in ("total", "total_s", "TOTAL_EXECUTE"):
                        if k in tm:
                            dur_ms = round(float(tm[k]) * 1000, 1)
                            break
                    if dur_ms is None:
                        total = sum(float(v) for v in tm.values() if isinstance(v, (int, float)))
                        if total > 0:
                            dur_ms = round(total * 1000, 1)
            except Exception:
                pass
        # Last fallback: query_profile.latency
        if dur_ms is None:
            raw_qp = it.get("query_profile")
            if raw_qp:
                try:
                    qp = json.loads(raw_qp) if isinstance(raw_qp, str) else raw_qp
                    if isinstance(qp, dict) and qp.get("latency"):
                        dur_ms = round(float(qp["latency"]) * 1000, 1)
                except Exception:
                    pass
        row["duration_ms"] = dur_ms
        items.append(row)

    return JSONResponse({"ok": True, "items": items, "count": len(items)})


@router.get("/api/v1/monitoring/reads/detail")
def monitoring_read_detail(
    org: str = Query(""),
    sup: str = Query(""),
    query_id: str = Query(""),
    _=Depends(logged_in_guard_api),
):
    """Full detail for a single read operation by query_id.

    Returns the complete record including execution_timings,
    profile_overview, and query_profile (parsed from JSON strings).
    """
    org = (org or "").strip()
    sup = (sup or "").strip()
    query_id = (query_id or "").strip()
    if not org or not sup or not query_id:
        raise HTTPException(status_code=400, detail="org, sup, and query_id required")

    # Scan the last 5000 entries to find the query
    items = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=None, to_ts_ms=None, limit=5000,
    )

    for it in items:
        if it.get("query_id") == query_id:
            # Parse JSON string fields into objects for the frontend
            for field in ("execution_timings", "profile_overview", "query_profile"):
                raw = it.get(field)
                if isinstance(raw, str):
                    try:
                        it[field] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass
            return JSONResponse({"ok": True, "item": it})

    raise HTTPException(status_code=404, detail=f"Query {query_id} not found")


@router.get("/api/v1/monitoring/writes")
def monitoring_writes(
    org: str = Query(""),
    sup: str = Query(""),
    from_ts: Optional[int] = Query(None),
    to_ts: Optional[int] = Query(None),
    limit: int = Query(500),
    _=Depends(logged_in_guard_api),
):
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "items": []})

    items = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=min(limit, 5000),
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    return JSONResponse({"ok": True, "items": items})


@router.get("/api/v1/monitoring/mcp")
def monitoring_mcp(
    org: str = Query(""),
    sup: str = Query(""),
    from_ts: Optional[int] = Query(None),
    to_ts: Optional[int] = Query(None),
    limit: int = Query(500),
    _=Depends(logged_in_guard_api),
):
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "items": []})

    items = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="mcp", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=limit,
    )
    return JSONResponse({"ok": True, "items": items})


@router.get("/api/v1/monitoring/summary")
def monitoring_summary(
    org: str = Query(""),
    sup: str = Query(""),
    window_hours: int = Query(24),
    _=Depends(logged_in_guard_api),
):
    """Unified summary page data — all metrics in a single call, zero locks."""
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "data": {}})

    window_hours = max(1, min(window_hours, 672))
    from supertable.services.summary import compute_summary
    data = compute_summary(
        redis_client, catalog, org, sup, window_hours=window_hours,
    )
    return JSONResponse({"ok": True, "data": data})


@router.get("/api/v1/monitoring/catalog")
def monitoring_catalog(
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(logged_in_guard_api),
):
    """Catalog statistics: table count, snapshots, rows, size, version."""
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "stats": {}})

    from supertable.services.monitoring import compute_catalog_stats as _cat_stats
    raw = _cat_stats(redis_client, catalog, org, sup)

    # Catalog root version
    cat_version = 0
    try:
        root = catalog.get_root(org, sup)
        if root:
            cat_version = int(root.get("version", 0) or 0)
    except Exception:
        pass

    stats = {
        "tables": raw.get("table_count", 0),
        "total_snapshots": raw.get("total_snapshots", 0),
        "total_rows": raw.get("total_rows", 0),
        "total_size_bytes": raw.get("total_size_bytes", 0),
        "catalog_version": cat_version,
        "users": raw.get("user_count", 0),
        "roles": raw.get("role_count", 0),
        "tokens": raw.get("token_count", 0),
        "top_tables_by_version": [
            {"name": t["table"], "version": t["snapshots"], "ts_ms": t["ts"]}
            for t in raw.get("top_tables_by_snapshots", [])
        ],
        "recent_tables": sorted(
            [
                {"name": t["table"], "ts_ms": t["ts"]}
                for t in raw.get("top_tables_by_snapshots", [])
                if t.get("ts")
            ],
            key=lambda x: x["ts_ms"],
            reverse=True,
        )[:5],
    }
    return JSONResponse({"ok": True, "stats": stats})


@router.get("/api/v1/monitoring/timeseries")
def monitoring_timeseries(
    org: str = Query(""),
    sup: str = Query(""),
    window_hours: int = Query(24),
    bucket_hours: int = Query(1),
    _=Depends(logged_in_guard_api),
):
    """Time-bucketed read/write volumes for charting."""
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "buckets": []})

    window_hours = max(1, min(window_hours, 720))
    bucket_hours = max(1, min(bucket_hours, 168))
    bucket_minutes = bucket_hours * 60

    from supertable.services.monitoring import compute_timeseries as _ts
    raw_buckets = _ts(
        redis_client, org, sup,
        window_hours=window_hours, bucket_minutes=bucket_minutes,
    )
    # Normalise field names to what the dashboard frontend expects
    buckets = [
        {
            "ts_ms": b["ts_ms"],
            "reads": b.get("reads", 0),
            "writes_inserted": b.get("rows_inserted", 0),
            "writes_deleted": b.get("rows_deleted", 0),
            "errors": b.get("errors", 0),
            "mcp_calls": b.get("mcp_calls", 0),
        }
        for b in raw_buckets
    ]
    return JSONResponse({"ok": True, "buckets": buckets})


@router.get("/api/v1/monitoring/locks")
def monitoring_locks(
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(logged_in_guard_api),
):
    """Currently-held per-table Redis locks with TTL."""
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "locks": []})

    from supertable.services.monitoring import compute_active_locks
    raw = compute_active_locks(redis_client, org, sup)
    locks = [
        {
            "table_name": l["table_name"],
            "ttl_ms": l["ttl_ms"],
            "held": l["ttl_ms"] is not None and l["ttl_ms"] > 0,
        }
        for l in raw
    ]
    return JSONResponse({"ok": True, "locks": locks})


@router.get("/api/v1/monitoring/errors")
def monitoring_errors(
    org: str = Query(""),
    sup: str = Query(""),
    window_hours: int = Query(24),
    limit: int = Query(200),
    _=Depends(logged_in_guard_api),
):
    """Unified error feed from reads, writes, and MCP calls."""
    org = (org or "").strip()
    sup = (sup or "").strip()
    if not org or not sup:
        return JSONResponse({"ok": True, "items": [], "count": 0})

    window_hours = max(1, min(window_hours, 168))
    limit = max(1, min(limit, 1000))

    from supertable.services.monitoring import compute_errors_feed
    raw = compute_errors_feed(
        redis_client, org, sup,
        window_hours=window_hours, limit=limit,
    )
    # Normalise source → op_type for frontend compatibility
    for item in raw:
        if "source" in item and "op_type" not in item:
            item["op_type"] = item["source"]
    return JSONResponse({"ok": True, "items": raw, "count": len(raw)})


@router.get("/api/v1/monitoring/instances")
def monitoring_instances(
    _=Depends(logged_in_guard_api),
):
    """Return all currently-live service instance heartbeats.

    Scans supertable:registry:* and returns a list of instance records
    with hostname, PID, started_at, service_type, and live TTL.
    Intended for the Live Instances widget (auto-refresh every 15s).
    """
    from supertable.service_registry import ServiceRegistry
    instances = ServiceRegistry.scan(redis_client)
    return JSONResponse({"ok": True, "instances": instances, "count": len(instances)})


# ═══════════════════════════════════════════════════════════════════════════
#   SECURITY endpoints (page + RBAC CRUD + OData endpoints)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/rbac/roles")
def rbac_roles_list(
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(admin_guard_api),
):
    org = (organization or org or "").strip()
    sup = (super_name or sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    items = _sec_list_roles(redis_client, org, sup)
    return JSONResponse({"ok": True, "data": {"items": items}})


@router.post("/api/v1/rbac/roles")
def rbac_role_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    _check_rate_limit(request, "role_create")
    org = str(payload.get("organization") or "").strip()
    sup = str(payload.get("super_name") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    role_name = str(payload.get("role_name") or "").strip()
    role_type = str(payload.get("role") or "reader").strip()
    tables_raw = payload.get("tables") or ["*"]
    cols_raw = payload.get("columns") or ["*"]
    filters_raw = payload.get("filters") or ["*"]
    source_query = str(payload.get("source_query") or "").strip()

    tables = tables_raw if isinstance(tables_raw, list) else [str(tables_raw)]
    columns = cols_raw if isinstance(cols_raw, list) else [str(cols_raw)]
    filters = filters_raw if isinstance(filters_raw, list) else [str(filters_raw)]

    doc = _sec_create_role(
        redis_client, org, sup,
        role_name=role_name, role=role_type,
        tables=tables, columns=columns, filters=filters,
        source_query=source_query,
    )
    _audit(
        category=EventCategory.RBAC_CHANGE, action=Actions.ROLE_CREATE,
        organization=org, super_name=sup, resource_type="role",
        resource_id=doc.get("role_id", role_name), severity=Severity.WARNING,
        detail=make_detail(role_name=role_name, role_type=role_type, tables=tables),
    )
    return JSONResponse({"ok": True, "data": doc}, status_code=201)


@router.put("/api/v1/rbac/roles/{role_id}")
def rbac_role_update(
    request: Request,
    role_id: str,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    _check_rate_limit(request, "role_update")
    org = str(payload.get("organization") or "").strip()
    sup = str(payload.get("super_name") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    tables_raw = payload.get("tables")
    cols_raw = payload.get("columns")
    filters_raw = payload.get("filters")

    tables = (tables_raw if isinstance(tables_raw, list) else [str(tables_raw)]) if tables_raw is not None else None
    columns = (cols_raw if isinstance(cols_raw, list) else [str(cols_raw)]) if cols_raw is not None else None
    filters = (filters_raw if isinstance(filters_raw, list) else [str(filters_raw)]) if filters_raw is not None else None

    doc = _sec_update_role(
        redis_client, org, sup, role_id,
        role_name=str(payload["role_name"]) if "role_name" in payload else None,
        role=str(payload["role"]) if "role" in payload else None,
        tables=tables, columns=columns, filters=filters,
        source_query=str(payload["source_query"]) if "source_query" in payload else None,
    )
    _audit(
        category=EventCategory.RBAC_CHANGE, action=Actions.ROLE_UPDATE,
        organization=org, super_name=sup, resource_type="role",
        resource_id=role_id, severity=Severity.WARNING,
        detail=make_detail(role_name=payload.get("role_name", ""), role_type=payload.get("role", "")),
    )
    return JSONResponse({"ok": True, "data": doc})


@router.delete("/api/v1/rbac/roles/{role_id}")
def rbac_role_delete(
    request: Request,
    role_id: str,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(admin_guard_api),
):
    _check_rate_limit(request, "role_delete")
    org = (organization or org or "").strip()
    sup = (super_name or sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    deleted = _sec_delete_role(redis_client, org, sup, role_id)
    _audit(
        category=EventCategory.RBAC_CHANGE, action=Actions.ROLE_DELETE,
        organization=org, super_name=sup, resource_type="role",
        resource_id=role_id, severity=Severity.CRITICAL,
    )
    return JSONResponse({"ok": True, "data": {"deleted": deleted}})


# ── User CRUD ──

@router.get("/api/v1/rbac/users")
def rbac_users_list(
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(admin_guard_api),
):
    org = (organization or org or "").strip()
    sup = (super_name or sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    items = _list_users_redis(redis_client, org, sup)
    return JSONResponse({"ok": True, "data": {"items": items}})


# ── OData Endpoint CRUD ──

@router.get("/api/v1/odata/endpoints")
def odata_endpoints_list(
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(admin_guard_api),
):
    org = (organization or org or "").strip()
    sup = (super_name or sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    items = _list_endpoints(redis_client, org, sup)
    return JSONResponse({"ok": True, "data": {"items": items}})


@router.post("/api/v1/odata/endpoints")
def odata_endpoint_create(
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    org = str(payload.get("organization") or "").strip()
    sup = str(payload.get("super_name") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    label = str(payload.get("label") or "").strip()
    role_id = str(payload.get("role_id") or "").strip()
    role_name = str(payload.get("role_name") or "").strip()

    if not role_id and not role_name:
        raise HTTPException(status_code=400, detail="role_id or role_name is required")

    if role_id and not role_name:
        role_doc = _sec_get_role(redis_client, org, sup, role_id)
        if not role_doc:
            raise HTTPException(status_code=404, detail="role not found")
        role_name = role_doc.get("role_name", "")

    doc, token = _create_endpoint(redis_client, org, sup, label=label, role_id=role_id, role_name=role_name)
    return JSONResponse({"ok": True, "data": doc, "token": token}, status_code=201)


@router.put("/api/v1/odata/endpoints/{endpoint_id}")
def odata_endpoint_update(
    endpoint_id: str,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    org = str(payload.get("organization") or "").strip()
    sup = str(payload.get("super_name") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    doc = _update_endpoint(
        redis_client, org, sup, endpoint_id,
        label=str(payload["label"]) if "label" in payload else None,
        enabled=payload["enabled"] if "enabled" in payload else None,
    )
    return JSONResponse({"ok": True, "data": doc})


@router.post("/api/v1/odata/endpoints/{endpoint_id}/regenerate")
def odata_endpoint_regenerate(
    endpoint_id: str,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
    org = str(payload.get("organization") or "").strip()
    sup = str(payload.get("super_name") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    doc, token = _regenerate_endpoint_token(redis_client, org, sup, endpoint_id)
    return JSONResponse({"ok": True, "data": doc, "token": token})


@router.delete("/api/v1/odata/endpoints/{endpoint_id}")
def odata_endpoint_delete(
    endpoint_id: str,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    _=Depends(admin_guard_api),
):
    org = (organization or org or "").strip()
    sup = (super_name or sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    deleted = _delete_endpoint(redis_client, org, sup, endpoint_id)
    return JSONResponse({"ok": True, "data": {"deleted": deleted}})


# ═══════════════════════════════════════════════════════════════════════════
#   DATA QUALITY endpoints
# ═══════════════════════════════════════════════════════════════════════════

import threading

from supertable.services.quality.config import DQConfig, BUILTIN_CHECKS


def _get_dqc(request: Request, org: str = None, sup: str = None) -> DQConfig:
    o, s = resolve_pair(org, sup)
    if not o or not s:
        raise HTTPException(status_code=400, detail="No organization/super selected")
    return DQConfig(redis_client, o, s)


def _dq_resolve(org: str = None, sup: str = None):
    return resolve_pair(org, sup)


@router.get("/api/v1/quality/summary")
def api_quality_summary(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Single optimized endpoint for the Quality Summary tab.

    Returns table list, quality overview, rule count, schedule brief,
    and pre-computed aggregates in one round-trip.
    """
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        return {"ok": True, "tables": [], "quality": {}, "rule_count": 0,
                "quality_index": 0, "counts": {"ok": 0, "warning": 0, "critical": 0, "unchecked": 0}}

    dqc = _get_dqc(request, org, sup)

    # 1. Table names (Redis scan)
    tables: List[str] = []
    try:
        pattern = f"supertable:{o}:{s}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:", 1)[-1]
                if simple and not simple.startswith("__"):
                    tables.append(simple)
            if cursor == 0:
                break
        tables = sorted(set(tables))
    except Exception as e:
        logger.error(f"[dq-summary] List tables failed: {e}")

    # 2. Quality overview
    all_latest = dqc.get_all_latest()

    # 3. Rule count
    rule_count = 0
    try:
        rules = dqc.get_rules()
        rule_count = len(rules) if isinstance(rules, list) else 0
    except Exception:
        pass

    # 4. Pre-compute aggregates
    scores = []
    count_ok, count_warn, count_crit = 0, 0, 0
    checked_tables = set()
    for tname, tdata in (all_latest or {}).items():
        if not isinstance(tdata, dict):
            continue
        checked_tables.add(tname)
        status = tdata.get("status", "ok")
        if status == "ok":
            count_ok += 1
        elif status == "warning":
            count_warn += 1
        elif status == "critical":
            count_crit += 1
        qs = tdata.get("quality_score")
        if qs is not None:
            scores.append(qs)

    unchecked = len(tables) - len(checked_tables)
    quality_index = round(sum(scores) / len(scores)) if scores else 0

    return {
        "ok": True,
        "tables": tables,
        "quality": all_latest,
        "rule_count": rule_count,
        "quality_index": quality_index,
        "counts": {
            "ok": count_ok,
            "warning": count_warn,
            "critical": count_crit,
            "unchecked": max(0, unchecked),
            "total": len(tables),
        },
    }


@router.get("/api/v1/quality/overview")
def api_quality_overview(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    all_latest = dqc.get_all_latest()
    return {"ok": True, "tables": all_latest}


@router.get("/api/v1/quality/tables")
def api_quality_tables(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        return {"ok": True, "tables": []}
    tables = []
    try:
        pattern = f"supertable:{o}:{s}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:", 1)[-1]
                if simple and not simple.startswith("__"):
                    tables.append(simple)
            if cursor == 0:
                break
    except Exception as e:
        logger.error(f"[dq-tables] List tables failed: {e}")
    return {"ok": True, "tables": sorted(set(tables))}


@router.get("/api/v1/quality/latest")
def api_quality_latest(
    request: Request,
    table: str = Query(...),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    latest = dqc.get_latest(table)
    anomalies = dqc.get_anomalies(table)
    return {"ok": True, "latest": latest, "anomalies": anomalies}


@router.get("/api/v1/quality/config")
def api_dq_get_global_config(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "config": dqc.get_global_config(), "builtin_checks": BUILTIN_CHECKS}


@router.put("/api/v1/quality/config")
def api_dq_set_global_config(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    if body is None:
        body = {}
    dqc = _get_dqc(request, org, sup)
    sess = get_session(request) or {}
    dqc.set_global_config(body, updated_by=sess.get("username", ""))
    return {"ok": True}


@router.get("/api/v1/quality/schedule")
def api_dq_get_schedule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "schedule": dqc.get_schedule()}


@router.put("/api/v1/quality/schedule")
def api_dq_set_schedule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    if body is None:
        body = {}
    dqc = _get_dqc(request, org, sup)
    dqc.set_schedule(body)
    return {"ok": True}


@router.put("/api/v1/quality/table-schedule")
def api_dq_set_table_schedule(
    request: Request,
    table: str = Query(...),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    if body is None:
        body = {}
    dqc = _get_dqc(request, org, sup)
    dqc.set_table_schedule(table, body)
    return {"ok": True}


@router.delete("/api/v1/quality/table-schedule")
def api_dq_delete_table_schedule(
    request: Request,
    table: str = Query(...),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    dqc.delete_table_schedule(table)
    return {"ok": True}


@router.get("/api/v1/quality/table-schedules")
def api_dq_get_all_table_schedules(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    overrides = dqc.get_all_table_schedules()
    return {"ok": True, "overrides": overrides}


@router.get("/api/v1/quality/rules")
def api_dq_list_rules(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "rules": dqc.list_rules()}


@router.post("/api/v1/quality/rules")
def api_dq_create_rule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    if body is None:
        raise HTTPException(status_code=400, detail="Request body required")
    dqc = _get_dqc(request, org, sup)
    sess = get_session(request) or {}
    rule = dqc.create_rule(body, created_by=sess.get("username", ""))
    return {"ok": True, "rule": rule}


@router.put("/api/v1/quality/rules")
def api_dq_update_rule(
    request: Request,
    rule_id: str = Query(...),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    if body is None:
        body = {}
    dqc = _get_dqc(request, org, sup)
    updated = dqc.update_rule(rule_id, body)
    if not updated:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"ok": True, "rule": updated}


@router.delete("/api/v1/quality/rules")
def api_dq_delete_rule(
    request: Request,
    rule_id: str = Query(...),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    dqc.delete_rule(rule_id)
    return {"ok": True}


@router.post("/api/v1/quality/run")
def api_dq_run_check(
    request: Request,
    table: str = Query(...),
    mode: str = Query("quick"),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        raise HTTPException(status_code=400, detail="No org/sup")

    dqc = _get_dqc(request, org, sup)

    def _run():
        try:
            from supertable.services.quality.scheduler import _run_quick_check, _run_deep_check
            if mode == "deep":
                _run_deep_check(redis_client, o, s, table, dqc)
            else:
                _run_quick_check(redis_client, o, s, table, dqc)
        except Exception as e:
            logger.error(f"[dq-run] Manual run failed for {table}: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return {"ok": True, "message": f"{mode} check started for {table}"}


@router.post("/api/v1/quality/run-all")
def api_dq_run_all(
    request: Request,
    mode: str = Query("quick"),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        raise HTTPException(status_code=400, detail="No org/sup")

    tables = []
    try:
        pattern = f"supertable:{o}:{s}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:", 1)[-1]
                if simple and not simple.startswith("__"):
                    tables.append(simple)
            if cursor == 0:
                break
        tables = sorted(set(tables))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")

    if not tables:
        return {"ok": True, "message": "No tables found", "count": 0}

    dqc = DQConfig(redis_client, o, s)
    progress_key = f"supertable:{o}:{s}:dq:run-all-progress"

    # Seed progress
    import json as _json_mod
    redis_client.set(progress_key, _json_mod.dumps({
        "running": True, "mode": mode, "total": len(tables),
        "done": 0, "current": "", "failed": 0,
    }), ex=600)

    def _run_sequential():
        from supertable.services.quality.scheduler import _run_quick_check, _run_deep_check
        failed = 0
        for idx, tbl in enumerate(tables):
            try:
                redis_client.set(progress_key, _json_mod.dumps({
                    "running": True, "mode": mode, "total": len(tables),
                    "done": idx, "current": tbl, "failed": failed,
                }), ex=600)
                logger.info(f"[dq-run-all] {mode} check: {tbl}")
                if mode == "deep":
                    _run_deep_check(redis_client, o, s, tbl, dqc)
                else:
                    _run_quick_check(redis_client, o, s, tbl, dqc)
            except Exception as e:
                logger.error(f"[dq-run-all] {mode} failed for {tbl}: {e}")
                failed += 1
        redis_client.set(progress_key, _json_mod.dumps({
            "running": False, "mode": mode, "total": len(tables),
            "done": len(tables), "current": "", "failed": failed,
        }), ex=60)

    t = threading.Thread(target=_run_sequential, name="dq-run-all", daemon=True)
    t.start()

    return {"ok": True, "message": f"{mode} check started on {len(tables)} tables (sequential)", "count": len(tables)}


@router.get("/api/v1/quality/run-all/progress")
def api_dq_run_all_progress(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        return {"ok": True, "running": False}
    progress_key = f"supertable:{o}:{s}:dq:run-all-progress"
    raw = redis_client.get(progress_key)
    if not raw:
        return {"ok": True, "running": False}
    import json as _json_mod
    try:
        data = _json_mod.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return {"ok": True, "running": False}
    data["ok"] = True
    return data


@router.get("/api/v1/quality/history")
def api_quality_history(
    request: Request,
    table: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    o, s = _dq_resolve(org, sup)
    if not o or not s:
        return {"ok": True, "rows": []}

    try:
        from supertable.data_reader import DataReader

        table_fqn = f"{s}.__data_quality__"
        conditions = [f"CAST(checked_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '{days} days'"]
        if table:
            safe_table = table.replace("'", "''")
            conditions.append(f"table_name = '{safe_table}'")

        where = "WHERE " + " AND ".join(conditions)

        sql = (
            f"SELECT dq_id, checked_at, table_name, check_type, "
            f"quality_score, status, row_count, total_checks, "
            f"passed, warnings, critical_count, anomaly_count, execution_ms "
            f"FROM {table_fqn} {where} "
            f"ORDER BY checked_at DESC LIMIT 1000"
        )

        dr = DataReader(super_name=s, organization=o, query=sql)
        result_df, status, message = dr.execute(role_name="superadmin")

        if result_df is not None and not result_df.empty:
            rows = result_df.to_dict(orient="records")
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
            return {"ok": True, "source": "parquet", "days": days, "rows": rows}

    except Exception as e:
        logger.debug(f"[dq-history] Parquet query failed, trying Redis: {e}")

    try:
        from supertable.services.quality.config import _dq_key
        from datetime import timedelta

        key = _dq_key(o, s, "history")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        raw_items = redis_client.lrange(key, 0, 999)
        rows = []
        for raw in (raw_items or []):
            try:
                item = raw if isinstance(raw, str) else raw.decode("utf-8")
                row = json.loads(item)
                checked_at = row.get("checked_at", "")
                if checked_at < cutoff:
                    continue
                if table and row.get("table_name") != table:
                    continue
                rows.append(row)
            except Exception:
                continue

        return {"ok": True, "source": "redis", "days": days, "rows": rows}

    except Exception as e:
        logger.warning(f"[dq-history] Redis history read failed: {e}")

    return {"ok": True, "source": "none", "days": days, "rows": []}


# ═══════════════════════════════════════════════════════════════════════════
#   COMPUTE endpoints (pool management, spark thrifts/plugs)
# ═══════════════════════════════════════════════════════════════════════════

import socket

from supertable.services.compute import (
    _load as _compute_load,
    _save as _compute_save,
    _sanitize_item as _compute_sanitize_item,
    _notebook_port,
    _default_ws_url,
    KINDS as _COMPUTE_KINDS,
    SIZES as _COMPUTE_SIZES,
)

from urllib.parse import urlsplit


def _compute_get_catalog():
    try:
        from supertable.redis_catalog import RedisCatalog
    except Exception:
        from redis_catalog import RedisCatalog  # type: ignore
    return RedisCatalog()


def _compute_sync_pool_to_catalog(org: str, pool: Dict[str, Any]) -> None:
    kind = str(pool.get("kind") or "").strip()
    pool_id = str(pool.get("id") or "").strip()
    if not org or not pool_id:
        return
    try:
        cat = _compute_get_catalog()
    except Exception:
        return
    if kind == "spark-thrift":
        config = {
            "name": pool.get("name", ""), "thrift_host": pool.get("thrift_host", ""),
            "thrift_port": pool.get("thrift_port", 10000), "status": pool.get("status", "active"),
            "min_bytes": pool.get("min_bytes", 0), "max_bytes": pool.get("max_bytes", 0),
            "s3_enabled": pool.get("s3_enabled", True), "s3_endpoint": pool.get("s3_endpoint", ""),
        }
        try:
            cat.register_spark_cluster(org, pool_id, config)
        except Exception:
            pass
    elif kind == "spark-plug":
        config = {
            "name": pool.get("name", ""), "spark_master": pool.get("spark_master", "spark://localhost:7077"),
            "ws_url": pool.get("ws_url", "ws://localhost:8010/ws/spark"),
            "webui_url": pool.get("webui_url", ""), "status": pool.get("status", "active"),
        }
        try:
            cat.register_spark_plug(org, pool_id, config)
        except Exception:
            pass


@router.get("/api/v1/compute/pools")
def compute_pools_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
    org = str(org or "").strip()
    sup = str(sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org/sup required")
    return {"ok": True, "data": _compute_load(org, sup), "kinds": list(_COMPUTE_KINDS), "sizes": list(_COMPUTE_SIZES)}


@router.post("/api/v1/compute/upsert")
def compute_pools_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    item = payload.get("item") or {}
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org/sup required")
    if not isinstance(item, dict):
        raise HTTPException(status_code=400, detail="item must be an object")

    data = _compute_load(org, sup)
    items = data.get("items") or []
    if not isinstance(items, list):
        items = []

    sanitized = _compute_sanitize_item(item)
    item_id = str(sanitized.get("id") or "").strip() or os.urandom(6).hex()
    sanitized["id"] = item_id

    if sanitized.get("is_default"):
        for it in items:
            if isinstance(it, dict):
                it["is_default"] = False

    out = []
    replaced = False
    for it in items:
        if isinstance(it, dict) and str(it.get("id")) == item_id:
            out.append(sanitized)
            replaced = True
        else:
            out.append(it)

    if not replaced:
        out.insert(0, sanitized)

    if not any(isinstance(it, dict) and it.get("is_default") for it in out) and out:
        if isinstance(out[0], dict):
            out[0]["is_default"] = True

    data["items"] = out
    _compute_save(org, sup, data)
    _compute_sync_pool_to_catalog(org, sanitized)
    return {"ok": True, "id": item_id}


@router.delete("/api/v1/compute/pools/{pool_id}")
def compute_pools_delete(pool_id: str, org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
    org = str(org or "").strip()
    sup = str(sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org/sup required")

    data = _compute_load(org, sup)
    items = data.get("items") or []
    if not isinstance(items, list):
        items = []

    data["items"] = [it for it in items if not (isinstance(it, dict) and str(it.get("id")) == pool_id)]
    if not any(isinstance(it, dict) and it.get("is_default") for it in data["items"]) and data["items"]:
        if isinstance(data["items"][0], dict):
            data["items"][0]["is_default"] = True

    _compute_save(org, sup, data)
    return {"ok": True}


@router.post("/api/v1/compute/test-connection")
def compute_test_connection(
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    import base64

    kind = str(payload.get("kind") or "").strip().lower()
    if kind == "in-process":
        return {"ok": True, "message": "In-process — no connection needed"}

    if kind == "spark-thrift":
        host = str(payload.get("thrift_host") or "").strip()
        try:
            port = int(payload.get("thrift_port") or 10000)
        except (ValueError, TypeError):
            port = 10000
        label = f"thrift {host}:{port}"
        if not host:
            raise HTTPException(status_code=400, detail="thrift_host is required for testing")
        try:
            sock = socket.create_connection((host, port), timeout=5.0)
            sock.close()
            return {"ok": True, "message": f"Connected to {label}"}
        except socket.timeout:
            return {"ok": False, "message": f"Timeout connecting to {label}"}
        except OSError as e:
            return {"ok": False, "message": f"Cannot reach {label}: {e}"}

    ws_raw = str(payload.get("ws_url") or "").strip()
    if not ws_raw:
        raise HTTPException(status_code=400, detail="ws_url is required for testing")

    parts = urlsplit(ws_raw)
    host = parts.hostname or ""
    use_ssl = parts.scheme in ("wss", "https")
    port = parts.port or (443 if use_ssl else 80)
    path = parts.path or "/"
    label = f"ws {host}:{port}{path}"
    if not host:
        raise HTTPException(status_code=400, detail="Cannot parse host from ws_url")

    try:
        sock = socket.create_connection((host, port), timeout=5.0)
    except socket.timeout:
        return {"ok": False, "message": f"Timeout connecting to {label}"}
    except OSError as e:
        return {"ok": False, "message": f"Cannot reach {label}: {e}"}

    try:
        if use_ssl:
            import ssl
            ctx_ssl = ssl.create_default_context()
            sock = ctx_ssl.wrap_socket(sock, server_hostname=host)

        ws_key = base64.b64encode(os.urandom(16)).decode()
        upgrade_req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        )
        sock.sendall(upgrade_req.encode())

        sock.settimeout(5.0)
        resp_data = b""
        while b"\r\n\r\n" not in resp_data and len(resp_data) < 4096:
            chunk = sock.recv(1024)
            if not chunk:
                break
            resp_data += chunk

        resp_text = resp_data.decode("utf-8", errors="replace")
        first_line = resp_text.split("\r\n", 1)[0] if resp_text else ""

        if "101" in first_line:
            return {"ok": True, "message": f"WebSocket handshake OK — {label}"}
        elif first_line:
            return {"ok": False, "message": f"Server responded: {first_line[:80]}"}
        else:
            return {"ok": False, "message": f"No response from {label} (connection closed)"}
    except socket.timeout:
        return {"ok": False, "message": f"Timeout during WebSocket handshake to {label}"}
    except OSError as e:
        return {"ok": False, "message": f"WebSocket handshake failed for {label}: {e}"}
    finally:
        try:
            sock.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#   ENGINE CONFIG endpoints (DuckDB runtime settings, stored in Redis)
# ═══════════════════════════════════════════════════════════════════════════

# Default values mirror the hardcoded fallbacks in engine_common.py / executor.py.
# These are returned when neither Redis nor env vars provide a value, so the
# UI always shows something meaningful.
_ENGINE_CONFIG_DEFAULTS: Dict[str, str] = {
    "engine_lite_max_bytes":      str(100 * 1024 * 1024),        # 100 MB
    "engine_spark_min_bytes":     str(10 * 1024 * 1024 * 1024),  # 10 GB
    "engine_freshness_sec":       "300",                          # 5 min
    "duckdb_memory_limit":        "1GB",
    "duckdb_io_multiplier":       "3",
    "duckdb_threads":             "",                             # auto-derived
    "duckdb_http_timeout":        "30",
    "duckdb_external_cache_size": "",                             # disabled
}

# Maps Redis field → env var name for env-fallback lookup.
_ENGINE_CONFIG_ENV_MAP: Dict[str, str] = {
    "engine_lite_max_bytes":      "SUPERTABLE_ENGINE_LITE_MAX_BYTES",
    "engine_spark_min_bytes":     "SUPERTABLE_ENGINE_SPARK_MIN_BYTES",
    "engine_freshness_sec":       "SUPERTABLE_ENGINE_FRESHNESS_SEC",
    "duckdb_memory_limit":        "SUPERTABLE_DUCKDB_MEMORY_LIMIT",
    "duckdb_io_multiplier":       "SUPERTABLE_DUCKDB_IO_MULTIPLIER",
    "duckdb_threads":             "SUPERTABLE_DUCKDB_THREADS",
    "duckdb_http_timeout":        "SUPERTABLE_DUCKDB_HTTP_TIMEOUT",
    "duckdb_external_cache_size": "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE",
}


def _resolve_engine_config(org: str, sup: str) -> Dict[str, Any]:
    """Build effective engine config: Redis → env var → hardcoded default.

    Returns a dict with ``value`` (effective), ``source`` (redis/env/default),
    and ``env_var`` for each field so the UI can show provenance.
    """
    cat = _compute_get_catalog()
    redis_cfg = cat.get_engine_config(org, sup) or {}
    result: Dict[str, Any] = {}

    for field, env_var in _ENGINE_CONFIG_ENV_MAP.items():
        redis_val = redis_cfg.get(field)
        env_val = os.getenv(env_var, "").strip()
        default_val = _ENGINE_CONFIG_DEFAULTS.get(field, "")

        if redis_val is not None and str(redis_val).strip() != "":
            value = str(redis_val).strip()
            source = "redis"
        elif env_val:
            value = env_val
            source = "env"
        else:
            value = default_val
            source = "default"

        result[field] = {
            "value": value,
            "source": source,
            "env_var": env_var,
            "default": default_val,
        }

    result["modified_ms"] = redis_cfg.get("modified_ms")
    return result


@router.get("/api/v1/engine/config")
def engine_config_get(
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    """Return effective engine configuration with source provenance."""
    o, s = resolve_pair(org, sup)
    if not o or not s:
        raise HTTPException(status_code=400, detail="org/sup required")
    return {"ok": True, "config": _resolve_engine_config(o, s)}


@router.post("/api/v1/engine/config")
def engine_config_set(
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Persist engine configuration to Redis.

    Only whitelisted fields are accepted (see RedisCatalog.ENGINE_CONFIG_FIELDS).
    Empty strings clear the field (falls back to env / default at read time).

    NOTE: changes take effect on the **next** DuckDB connection init — existing
    persistent connections (DuckDB Pro singleton) are not reconfigured.
    """
    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org/sup required")

    config = payload.get("config") or {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")

    cat = _compute_get_catalog()
    ok = cat.set_engine_config(org, sup, config)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save engine config")
    return {"ok": True, "config": _resolve_engine_config(org, sup)}


# ═══════════════════════════════════════════════════════════════════════════
#   USERS PAGE endpoints (user management via UserManager/RoleManager)
# ═══════════════════════════════════════════════════════════════════════════

_VALID_ROLE_TYPES_USERS = {"superadmin", "admin", "writer", "reader", "meta"}


def _users_user_manager(org: str, sup: str):
    try:
        from supertable.rbac.user_manager import UserManager
        return UserManager(super_name=sup, organization=org, redis_catalog=catalog)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"UserManager unavailable: {e}")


def _users_role_manager(org: str, sup: str):
    try:
        from supertable.rbac.role_manager import RoleManager
        return RoleManager(super_name=sup, organization=org, redis_catalog=catalog)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RoleManager unavailable: {e}")


def _users_resolve_or_raise(org: Optional[str], sup: Optional[str]):
    o, s = resolve_pair(org, sup)
    if not o or not s:
        raise HTTPException(status_code=400, detail="No tenant selected")
    return o, s


def _users_normalize_user(u: Dict[str, Any]) -> Dict[str, Any]:
    uid = u.get("user_id") or u.get("hash") or ""
    u.setdefault("user_id", uid)
    u.setdefault("hash", uid)
    roles = u.get("roles") or []
    if isinstance(roles, str):
        try:
            roles = json.loads(roles)
        except Exception:
            roles = [roles]
    u["roles"] = roles
    return u


@router.get("/api/v1/rbac/users/list")
def api_users_page_list(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    o, s = _users_resolve_or_raise(org, sup)
    users_raw = [_users_normalize_user(u) for u in list_users(o, s)]
    for u in users_raw:
        uname = str(u.get("username") or u.get("name") or "").strip()
        u["is_superuser"] = uname.lower() == s.lower()
    roles_data = list_roles(o, s)
    return {"ok": True, "users": users_raw, "roles": roles_data}


@router.post("/api/v1/rbac/users/create")
def api_users_page_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    _check_rate_limit(request, "user_create")
    username = str(payload.get("username") or "").strip()
    validate_username(username)  # raises HTTPException if invalid

    display_name = str(payload.get("display_name") or "").strip()

    role_type = str(payload.get("role_type") or "").strip().lower()
    if role_type not in _VALID_ROLE_TYPES_USERS:
        raise HTTPException(status_code=400, detail=f"role_type must be one of: {', '.join(sorted(_VALID_ROLE_TYPES_USERS))}")

    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org and sup are required")

    if username.lower() == sup.lower() or username.lower() in ("superuser", "superadmin"):
        raise HTTPException(status_code=403, detail="Reserved system account names cannot be used.")

    if role_type == "superadmin":
        raise HTTPException(status_code=403, detail="The superadmin role cannot be assigned from the UI.")

    rm = _users_role_manager(org, sup)
    um = _users_user_manager(org, sup)

    existing_roles = rm.get_roles_by_type(role_type)
    if existing_roles:
        role_id = existing_roles[0].get("role_id") or existing_roles[0].get("hash") or ""
    else:
        role_id = rm.create_role({
            "role": role_type, "role_name": role_type,
            "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        })

    if not role_id:
        raise HTTPException(status_code=500, detail="Could not resolve role_id")

    try:
        user_id = um.create_user({"username": username, "roles": [role_id]})
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User creation failed: {e}")

    # Auto-create a linked auth token for this user
    plaintext_token = ""
    token_id = ""
    try:
        token_result = _catalog_create_token(
            org, created_by="superuser",
            label=f"user:{username}",
            username=username, user_id=str(user_id),
        )
        plaintext_token = token_result.get("token", "")
        token_id = token_result.get("token_id", "")
        # Store token_id on the user document for linkage
        try:
            doc_key = f"supertable:{org}:{sup}:rbac:users:doc:{user_id}"
            redis_client.hset(doc_key, "token_id", token_id)
        except Exception:
            pass
    except Exception as e:
        logger.warning("[users-page] Token creation failed for user %s: %s", username, e)

    _audit(
        category=EventCategory.RBAC_CHANGE, action=Actions.USER_CREATE,
        organization=org, super_name=sup, resource_type="user",
        resource_id=str(user_id), severity=Severity.WARNING,
        detail=make_detail(username=username, role_type=role_type),
    )

    return {"ok": True, "user_id": user_id, "username": username, "role_id": role_id,
            "token": plaintext_token, "token_id": token_id}


@router.delete("/api/v1/rbac/users/{user_id}")
def api_users_page_delete(
    request: Request,
    user_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    _check_rate_limit(request, "user_delete")
    o, s = _users_resolve_or_raise(org, sup)
    um = _users_user_manager(o, s)

    try:
        all_users = list_users(o, s)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not verify user identity before deletion: {e}")

    # Protect system accounts: superuser (username == sup), "superuser", "superadmin"
    _PROTECTED_NAMES = {"superuser", "superadmin"}
    protected_ids: set = set()
    for u in all_users:
        uname = str(u.get("username") or u.get("name") or "").strip().lower()
        uid = str(u.get("user_id") or u.get("hash") or "").strip()
        if uid and (uname == s.lower() or uname in _PROTECTED_NAMES):
            protected_ids.add(uid)

    if user_id in protected_ids:
        raise HTTPException(status_code=403, detail="System accounts (superuser, superadmin) cannot be deleted.")

    # Delete the linked auth token (if any) before deleting the user
    try:
        doc_key = f"supertable:{o}:{s}:rbac:users:doc:{user_id}"
        token_id = redis_client.hget(doc_key, "token_id")
        if token_id:
            tid = token_id if isinstance(token_id, str) else token_id.decode("utf-8")
            _catalog_delete_token(o, tid)
    except Exception as e:
        logger.warning("[users-page] Token cleanup failed for user %s: %s", user_id, e)

    try:
        um.delete_user(user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")
    return {"ok": True, "user_id": user_id}


@router.post("/api/v1/rbac/users/{user_id}/roles")
def api_users_page_add_role(
    request: Request,
    user_id: str,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    _check_rate_limit(request, "user_role_change")
    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    role_id = str(payload.get("role_id") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org and sup are required")
    if not role_id:
        raise HTTPException(status_code=400, detail="role_id is required")
    if not catalog.rbac_role_exists(org, sup, role_id):
        raise HTTPException(status_code=404, detail="Role not found")
    try:
        catalog.rbac_add_role_to_user(org, sup, user_id, role_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Add role failed: {e}")
    return {"ok": True, "user_id": user_id, "role_id": role_id}


@router.delete("/api/v1/rbac/users/{user_id}/roles/{role_id}")
def api_users_page_remove_role(
    request: Request,
    user_id: str,
    role_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    _check_rate_limit(request, "user_role_change")
    o, s = _users_resolve_or_raise(org, sup)
    try:
        catalog.rbac_remove_role_from_user(o, s, user_id, role_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Remove role failed: {e}")
    return {"ok": True, "user_id": user_id, "role_id": role_id}


@router.post("/api/v1/rbac/users/{user_id}/regenerate-token")
def api_users_page_regenerate_token(
    request: Request,
    user_id: str,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Regenerate the login token for a user. Old token stops working immediately."""
    _check_rate_limit(request, "token_regenerate", max_per_minute=10)
    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org and sup are required")

    # Look up the user to get their username and current token_id
    doc_key = f"supertable:{org}:{sup}:rbac:users:doc:{user_id}"
    try:
        raw = redis_client.hgetall(doc_key)
        if not raw:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis error: {e}")

    username = ""
    old_token_id = ""
    for k, v in raw.items():
        kk = k if isinstance(k, str) else k.decode("utf-8")
        vv = v if isinstance(v, str) else v.decode("utf-8")
        if kk == "username":
            username = vv
        elif kk == "token_id":
            old_token_id = vv

    if not username:
        raise HTTPException(status_code=400, detail="User has no username")

    # Delete old token
    if old_token_id:
        try:
            _catalog_delete_token(org, old_token_id)
        except Exception:
            pass

    # Create new token linked to this user
    token_result = _catalog_create_token(
        org, created_by="superuser",
        label=f"user:{username}",
        username=username, user_id=user_id,
    )
    new_token_id = token_result.get("token_id", "")

    # Update user doc with new token_id
    try:
        redis_client.hset(doc_key, "token_id", new_token_id)
    except Exception:
        pass

    _audit(
        category=EventCategory.TOKEN_MGMT, action=Actions.TOKEN_REGENERATE,
        organization=org, super_name=sup, resource_type="user_token",
        resource_id=user_id, severity=Severity.WARNING,
        detail=make_detail(username=username, old_token_id=old_token_id[:16] + "..." if old_token_id else ""),
    )

    return {"ok": True, "user_id": user_id, "username": username,
            "token": token_result.get("token", ""), "token_id": new_token_id}


# ═══════════════════════════════════════════════════════════════════════════
#   AUDIT LOG endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/audit/events")
def audit_events(
    request: Request,
    organization: str = Query(""),
    category: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    outcome: Optional[str] = Query(None),
    actor_id: Optional[str] = Query(None),
    resource_id: Optional[str] = Query(None),
    correlation_id: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=5000),
    _=Depends(admin_guard_api),
):
    """Query audit events. Requires superuser or auditor role."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit log access requires superuser")

    org = (organization or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org:
        return JSONResponse({"events": []})

    from supertable.audit.reader import query_audit_log
    events = query_audit_log(
        org,
        category=category,
        severity=severity,
        outcome=outcome,
        actor_id=actor_id,
        resource_id=resource_id,
        correlation_id=correlation_id,
        limit=limit,
    )
    return JSONResponse({"events": events, "count": len(events)})


@router.get("/api/v1/audit/verify")
def audit_verify(
    request: Request,
    organization: str = Query(""),
    date: str = Query(""),
    _=Depends(admin_guard_api),
):
    """Verify hash chain integrity for a specific day."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit log access requires superuser")

    org = (organization or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org or not date:
        raise HTTPException(status_code=400, detail="organization and date required")

    from supertable.audit.reader import verify_chain_integrity
    result = verify_chain_integrity(org, date)
    return JSONResponse(result)


@router.get("/api/v1/audit/export")
def audit_export(
    request: Request,
    organization: str = Query(""),
    start: str = Query(""),
    end: str = Query(""),
    format: str = Query("json"),
    criteria: Optional[str] = Query(None),
    _=Depends(admin_guard_api),
):
    """Export audit events as JSON-lines or CSV."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit log access requires superuser")

    org = (organization or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org or not start or not end:
        raise HTTPException(status_code=400, detail="organization, start and end dates required")

    from datetime import datetime as _dt, timezone as _tz
    try:
        start_ms = int(_dt.strptime(start, "%Y-%m-%d").replace(tzinfo=_tz.utc).timestamp() * 1000)
        end_ms = int(_dt.strptime(end, "%Y-%m-%d").replace(tzinfo=_tz.utc).timestamp() * 1000) + 86400000
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if criteria:
        from supertable.audit.export import export_soc2_evidence
        data = export_soc2_evidence(org, criteria, start_ms, end_ms, format)
    else:
        from supertable.audit.reader import query_audit_log
        from supertable.audit.export import export_events
        events = query_audit_log(org, start_ms=start_ms, end_ms=end_ms, limit=50000)
        data = export_events(events, format)

    content_type = "text/csv" if format == "csv" else "application/x-ndjson"
    ext = "csv" if format == "csv" else "jsonl"
    filename = f"audit_{org}_{start}_{end}.{ext}"

    from starlette.responses import Response as _Resp
    return _Resp(
        content=data,
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/v1/audit/stats")
def audit_stats(
    request: Request,
    organization: str = Query(""),
    _=Depends(admin_guard_api),
):
    """Event counts by category and severity."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit log access requires superuser")

    org = (organization or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org:
        return JSONResponse({"stats": {}})

    from supertable.audit.logger import get_audit_logger
    audit_logger = get_audit_logger(org)
    stats = getattr(audit_logger, "stats", {})
    return JSONResponse({"ok": True, "stats": stats})


# ═══════════════════════════════════════════════════════════════════════════
#   Garbage Collection
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/v1/gc/preview")
def api_gc_preview(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    table: str = Query(""),
    cutoff_version: Optional[int] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    """Preview orphaned files for a table (dry run).

    If cutoff_version is provided, computes orphans relative to keeping
    all versions >= cutoff_version.  Without it, compares against
    the current snapshot only (legacy behavior).
    """
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup or not table:
        raise HTTPException(status_code=400, detail="org, sup, and table are required")

    from supertable.services.gc import preview_obsolete_files
    result = preview_obsolete_files(sel_org, sel_sup, table.strip(), cutoff_version=cutoff_version)
    return JSONResponse({"ok": True, **result})


@router.post("/api/v1/gc/clean")
def api_gc_clean(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Delete orphaned files for a table."""
    sel_org = str(payload.get("organization") or "").strip()
    sel_sup = str(payload.get("super_name") or "").strip()
    table = str(payload.get("table_name") or "").strip()
    role = str(payload.get("role_name") or "superadmin").strip()

    if not sel_org or not sel_sup or not table:
        raise HTTPException(status_code=400, detail="organization, super_name, and table_name are required")

    from supertable.services.gc import clean_obsolete_files
    result = clean_obsolete_files(sel_org, sel_sup, table, role)

    status_code = 200 if not result.get("errors") else 207
    return JSONResponse({"ok": True, **result}, status_code=status_code)


@router.get("/api/v1/gc/schedule")
def api_gc_get_schedule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Read GC schedule configuration."""
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        return {"ok": True, "schedule": {"enabled": False, "cron": "0 2 * * *", "all_tables": True}}
    key = f"supertable:{sel_org}:{sel_sup}:gc:schedule"
    raw = redis_client.get(key)
    if raw:
        import json as _json
        try:
            data = _json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception:
            data = {"enabled": False, "cron": "0 2 * * *", "all_tables": True}
    else:
        data = {"enabled": False, "cron": "0 2 * * *", "all_tables": True}
    return {"ok": True, "schedule": data}


@router.put("/api/v1/gc/schedule")
def api_gc_set_schedule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    body: Dict[str, Any] = Body(None),
    _: Any = Depends(admin_guard_api),
):
    """Save GC schedule configuration."""
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup:
        raise HTTPException(status_code=400, detail="org and sup required")
    if body is None:
        body = {}
    import json as _json
    key = f"supertable:{sel_org}:{sel_sup}:gc:schedule"
    data = {
        "enabled": bool(body.get("enabled", False)),
        "cron": str(body.get("cron", "0 2 * * *")),
        "all_tables": bool(body.get("all_tables", True)),
    }
    redis_client.set(key, _json.dumps(data))

    try:
        _audit(
            category=EventCategory.CONFIG_CHANGE,
            action=Actions.TABLE_CONFIG_CHANGE,
            organization=sel_org,
            super_name=sel_sup,
            resource_type="gc_schedule",
            resource_id="gc_schedule",
            severity=Severity.WARNING,
            detail=make_detail(**data),
        )
    except Exception:
        pass

    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════
#   Snapshot History / Time-Travel
# ═══════════════════════════════════════════════════════════════════════════


@router.get("/api/v1/tables/snapshot-history")
def api_snapshot_history(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    table: str = Query(""),
    limit: int = Query(50, ge=1, le=500),
    _: Any = Depends(admin_guard_api),
):
    """List snapshot versions for a table (newest first)."""
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup or not table:
        raise HTTPException(status_code=400, detail="org, sup, and table are required")

    from supertable.services.time_travel import list_snapshot_versions
    versions = list_snapshot_versions(sel_org, sel_sup, table.strip(), limit=limit)
    return JSONResponse({"ok": True, "table": table.strip(), "versions": versions, "count": len(versions)})


@router.get("/api/v1/tables/snapshot")
def api_get_snapshot(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    table: str = Query(""),
    version: int = Query(...),
    _: Any = Depends(admin_guard_api),
):
    """Retrieve a specific snapshot version for a table."""
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup or not table:
        raise HTTPException(status_code=400, detail="org, sup, and table are required")

    from supertable.services.time_travel import get_snapshot_at_version
    snapshot = get_snapshot_at_version(sel_org, sel_sup, table.strip(), version)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Version {version} not found for table '{table}'")

    return JSONResponse({"ok": True, "table": table.strip(), "version": version, "snapshot": snapshot})


@router.get("/api/v1/tables/lineage")
def api_table_lineage(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    table: str = Query(""),
    limit: int = Query(100, ge=1, le=500),
    _: Any = Depends(admin_guard_api),
):
    """Return the lineage timeline for a table — one entry per snapshot version.

    Walks the snapshot linked list (newest-first) and extracts the lineage
    dict from each version.  Versions without lineage are included with an
    empty lineage dict.
    """
    sel_org, sel_sup = resolve_pair(org, sup)
    if not sel_org or not sel_sup or not table:
        raise HTTPException(status_code=400, detail="org, sup, and table are required")

    from supertable.services.time_travel import list_snapshot_versions
    versions = list_snapshot_versions(sel_org, sel_sup, table.strip(), limit=limit)

    timeline = []
    for v in versions:
        lin = v.get("lineage") or {}
        timeline.append({
            "version": v.get("version"),
            "last_updated_ms": v.get("last_updated_ms"),
            "resource_count": v.get("resource_count"),
            "total_rows": v.get("total_rows"),
            "source_type": lin.get("source_type", ""),
            "username": lin.get("username", ""),
            "filename": lin.get("filename", ""),
            "delete_only": lin.get("delete_only", False),
            "lineage": lin,
        })

    return JSONResponse({"ok": True, "table": table.strip(), "timeline": timeline, "count": len(timeline)})


# ═══════════════════════════════════════════════════════════════════════════
#   Audit Retention & Legal Hold
# ═══════════════════════════════════════════════════════════════════════════


@router.post("/api/v1/audit/retention/run")
def api_audit_retention_run(
    request: Request,
    payload: Dict[str, Any] = Body({}),
    _: Any = Depends(admin_guard_api),
):
    """Trigger audit retention cleanup (delete partitions older than retention period)."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit retention requires superuser")

    org = str(payload.get("organization") or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org:
        raise HTTPException(status_code=400, detail="organization is required")

    from supertable.audit.retention import enforce_retention
    result = enforce_retention(org)
    return JSONResponse({"ok": True, **result})


@router.get("/api/v1/audit/legal-hold")
def api_audit_legal_hold_status(
    request: Request,
    organization: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    """Check if legal hold is active."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit legal hold requires superuser")

    org = (organization or settings.SUPERTABLE_ORGANIZATION or "").strip()
    if not org:
        raise HTTPException(status_code=400, detail="organization is required")

    from supertable.audit.retention import is_legal_hold_active
    active = is_legal_hold_active(org)
    return JSONResponse({"ok": True, "legal_hold_active": active, "organization": org})


@router.post("/api/v1/audit/legal-hold")
def api_audit_legal_hold_set(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    """Activate or deactivate legal hold."""
    if not is_superuser(request):
        raise HTTPException(status_code=403, detail="Audit legal hold requires superuser")

    org = str(payload.get("organization") or settings.SUPERTABLE_ORGANIZATION or "").strip()
    enabled = bool(payload.get("enabled", True))

    if not org:
        raise HTTPException(status_code=400, detail="organization is required")

    from supertable.audit.retention import set_legal_hold
    result = set_legal_hold(enabled, organization=org)
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return JSONResponse(result)


