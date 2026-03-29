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
)

# -- monitoring helpers --
from supertable.services.monitoring import _read_monitoring_list


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
    """Resolve role: prefer explicit param, then session cookie."""
    role = (role_name or user_hash or "").strip()
    if not role:
        sess = get_session(request) or {}
        role = (sess.get("role_name") or "").strip()
    return role


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


# ═══════════════════════════════════════════════════════════════════════════
#
#   ENDPOINT REGISTRATIONS  —  grouped by domain
#
# ═══════════════════════════════════════════════════════════════════════════


# ───────────────────────────── Root / Health ──────────────────────────────

@router.get("/healthz", response_class=PlainTextResponse)
def healthz():
    try:
        pong = redis_client.ping()
        return "ok" if pong else "not-ok"
    except Exception as e:
        return f"error: {e}"


# ─────────────────────────── Auth / Login ─────────────────────────────────

@router.get("/reflection/super-meta")
def reflection_super_meta(
    request: Request,
    organization: str = Query(...),
    super_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    t0 = time.perf_counter()
    organization = organization.strip()
    super_name = super_name.strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    resolved_role = (role_name or "").strip()
    if not resolved_role:
        sess = get_session(request) or {}
        resolved_role = (sess.get("role_name") or "").strip()

    try:
        t1 = time.perf_counter()
        meta_reader = _common_get_meta_reader(organization, super_name)
        t2 = time.perf_counter()
        result = meta_reader.get_super_meta(resolved_role)
        t3 = time.perf_counter()
    except Exception as e:
        logger.warning("MetaReader.get_super_meta failed (%s/%s): %s", organization, super_name, e)
        raise HTTPException(status_code=500, detail=str(e))

    resp = JSONResponse({"meta": result})
    _no_store(resp)
    t4 = time.perf_counter()
    logger.debug(
        "super-meta %s/%s timings: total=%.1fms | meta_reader=%.1fms | get_super_meta=%.1fms | response=%.1fms",
        organization, super_name,
        (t4 - t0) * 1000, (t2 - t1) * 1000, (t3 - t2) * 1000, (t4 - t3) * 1000,
    )
    return resp


@router.get("/reflection/user-role-names")
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


@router.get("/reflection/roles")
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


@router.post("/reflection/set-role")
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

@router.get("/reflection/tokens")
def api_list_tokens(request: Request, org: str = Query(None), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    tokens = _catalog_list_tokens(org_eff)
    return {"ok": True, "organization": org_eff, "tokens": tokens}


@router.post("/reflection/tokens")
def api_create_token(request: Request, org: str = Query(None), label: str = Query(""), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    created = _catalog_create_token(org_eff, created_by="superuser", label=label)
    return {"ok": True, "organization": org_eff, **created}


@router.delete("/reflection/tokens/{token_id}")
def api_delete_token(request: Request, token_id: str, org: str = Query(None), _: Any = Depends(admin_guard_api)):
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    org_eff = (org or _get_org_from_env_fallback()).strip()
    ok = _catalog_delete_token(org_eff, token_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Token not found")
    return {"ok": True, "organization": org_eff, "token_id": token_id}


# ──────────────────── Admin page + SuperTable CRUD ────────────────────────

@router.post("/reflection/super")
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
        return {"ok": True, "organization": st.organization, "name": st.super_name, "storage": storage_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SuperTable creation failed: {e}")


@router.delete("/reflection/super")
def api_delete_super(
    request: Request,
    organization: str = Query(..., description="Organization identifier"),
    super_name: str = Query(..., description="SuperTable name"),
    role_name: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

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

    return {"ok": True, "organization": organization, "super_name": super_name, "deleted_redis_keys": deleted_keys}


# ═══════════════════════════════════════════════════════════════════════════
#   EXECUTE (SQL query) endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/reflection/execute")
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

        return JSONResponse({
            "status": "ok", "message": None, "result": rows,
            "total_count": total_count, "meta": _sanitize_for_json(meta_payload),
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Execute SQL failed")
        return JSONResponse({"status": "error", "message": f"Execution failed: {e}", "result": []}, status_code=500)


@router.post("/reflection/schema")
def schema_api(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
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

@router.get("/reflection/supers")
def api_list_supers(
    request: Request,
    organization: str = Query("", description="Organization identifier"),
    org: str = Query(""),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    if not organization:
        raise HTTPException(status_code=400, detail="organization is required")

    try:
        return {"ok": True, "organization": organization, "supers": list_supers(organization=organization)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List supers failed: {e}")


@router.get("/reflection/super")
def api_get_super_meta(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    role = _resolve_role(request, role_name, "")

    debug_timings = _cfg.SUPERTABLE_DEBUG_TIMINGS
    t0 = time.perf_counter()
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        t1 = time.perf_counter()
        meta = mr.get_super_meta(role)
        t2 = time.perf_counter()

        payload = {"ok": True, "meta": meta}
        if not debug_timings:
            return payload

        mr_ms = (t1 - t0) * 1000.0
        get_ms = (t2 - t1) * 1000.0
        total_ms = (t2 - t0) * 1000.0

        client_host = getattr(getattr(request, "client", None), "host", None) or "-"
        logger.info(
            "[timing][reflection/super] total_ms=%.2f mr_ms=%.2f get_super_meta_ms=%.2f org=%s super=%s role_name=%s client=%s",
            total_ms, mr_ms, get_ms, organization, super_name, (role or "")[:12], client_host,
        )

        resp = JSONResponse(payload)
        resp.headers["Server-Timing"] = (
            f"meta_reader;dur={mr_ms:.2f},get_super_meta;dur={get_ms:.2f},total;dur={total_ms:.2f}"
        )
        return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get super meta failed: {e}")


@router.get("/reflection/schema")
def api_get_table_schema(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Table simple name"),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(admin_guard_api),
):
    organization = (organization or org or "").strip()
    super_name = (super_name or sup or "").strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    role = _resolve_role(request, role_name)
    try:
        mr = MetaReader(organization=organization, super_name=super_name)
        schema = mr.get_table_schema(table, role)
        return {"ok": True, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get table schema failed: {e}")


@router.get("/reflection/stats")
def api_get_table_stats(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Table simple name"),
    role_name: str = Query("", alias="role_name"),
    _: Any = Depends(admin_guard_api),
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


@router.delete("/reflection/table")
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

    return {"ok": True, "organization": organization, "super_name": super_name, "table": simple}


@router.get("/reflection/table/config")
def api_get_table_config(
    request: Request,
    organization: str = Query(""),
    super_name: str = Query(""),
    org: str = Query(""),
    sup: str = Query(""),
    table: str = Query(..., description="Simple table name"),
    _: Any = Depends(admin_guard_api),
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


@router.put("/reflection/table/config")
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
        return {"ok": True, "config": merged}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Set table config failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#   ENGINE page
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
#   INGESTION endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/reflection/ingestion/recent-writes")
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
        raw_list = redis_client.lrange(key, -limit, -1) or []
        raw_list = list(reversed(raw_list))
        for raw in raw_list:
            try:
                entry = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if isinstance(entry, dict):
                    items.append(entry)
            except Exception:
                continue
    except Exception as e:
        logger.warning("[ingestion] Failed to read recent writes from Redis: %s", e)

    return {"ok": True, "items": items}


@router.get("/reflection/ingestion/stagings")
def api_ingestion_list_stagings(
    org: str = Query(...),
    sup: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    names = _redis_list_stagings(org, sup)
    return {"staging_names": names}


@router.get("/reflection/ingestion/tables")
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

    try:
        mr = MetaReader(organization=org_val, super_name=sup_val)
        meta = mr.get_super_meta(role) or {}
    except Exception as e:
        logger.warning("Failed to fetch super meta for ingestion tables (%s/%s): %s", org_val, sup_val, e)
        return {"ok": True, "tables": []}

    super_meta = meta.get("super", {}) if isinstance(meta, dict) else {}
    raw_tables = []
    try:
        raw_tables = super_meta.get("tables") if isinstance(super_meta, dict) else None
    except Exception:
        raw_tables = None

    names: List[str] = []
    if isinstance(raw_tables, list):
        for t in raw_tables:
            if isinstance(t, str):
                name = t.strip()
            elif isinstance(t, dict):
                name = str(t.get("name") or "").strip()
            else:
                name = str(t or "").strip()
            if name and name not in names:
                names.append(name)

    return {"ok": True, "tables": names}


@router.get("/reflection/ingestion/staging/files")
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

    total = len(data)
    items = data[offset: offset + limit]
    return {"items": items, "total": total, "offset": offset, "limit": limit}


@router.get("/reflection/ingestion/pipes")
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
        items.append({"pipe_name": pn, "simple_name": str(meta.get("simple_name") or ""), "enabled": bool(meta.get("enabled"))})

    items.sort(key=lambda x: str(x.get("pipe_name") or ""))
    return {"items": items}


@router.get("/reflection/ingestion/pipe/meta")
def api_ingestion_get_pipe_meta(
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    if not _STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid pipe_name")
    meta = _redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {}
    return {"meta": meta}


@router.post("/reflection/staging/create")
def api_create_staging(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
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
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}


@router.post("/reflection/staging/delete")
def api_delete_staging(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
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
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}


@router.post("/reflection/pipes/save")
def api_save_pipe(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    org = str(payload.get("organization") or payload.get("org") or "").strip()
    sup = str(payload.get("super_name") or payload.get("sup") or "").strip()
    staging_name = str(payload.get("staging_name") or "").strip()
    pipe_name = str(payload.get("pipe_name") or "").strip()

    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name are required")
    if not _STAGING_NAME_RE.fullmatch(staging_name):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    if not _STAGING_NAME_RE.fullmatch(pipe_name):
        raise HTTPException(status_code=400, detail="Invalid pipe_name")

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
    pipe_def.setdefault("enabled", True)
    pipe_def.setdefault("overwrite_columns", ["day"])
    pipe_def.setdefault("meta", {})
    pipe_def["updated_at_ns"] = time.time_ns()

    pipe_path = os.path.join(stg_dir, "pipes", f"{pipe_name}.json")
    try:
        _write_json_atomic(storage, pipe_path, pipe_def)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Save pipe failed: {e}")

    names = _get_staging_names(storage, org, sup)
    if staging_name not in names:
        names = sorted(set(names + [staging_name]))
        _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

    pipes = _load_pipe_index(storage, org, sup)
    pipes = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name)]
    overwrite_cols = pipe_def.get("overwrite_columns")
    if not isinstance(overwrite_cols, list):
        overwrite_cols = []
    pipes.append({
        "pipe_name": pipe_name, "organization": org, "super_name": sup,
        "staging_name": staging_name, "role_name": str(pipe_def.get("role_name") or "").strip(),
        "simple_name": str(pipe_def.get("simple_name") or "").strip(),
        "overwrite_columns": overwrite_cols, "enabled": bool(pipe_def.get("enabled")),
        "path": pipe_path, "updated_at_ns": time.time_ns(),
    })
    _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

    _redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
    _redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, pipe_def)

    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name, "pipe_name": pipe_name, "path": pipe_path}


@router.post("/reflection/pipes/delete")
def api_delete_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid staging_name")
    if not _STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
        raise HTTPException(status_code=400, detail="Invalid pipe_name")

    storage = get_storage()
    path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")

    try:
        if storage.exists(path):
            storage.delete(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete pipe failed: {e}")

    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        pipes2 = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name.strip())]
        if pipes2 != pipes:
            _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

    _redis_delete_pipe_meta(org, sup, staging_name, pipe_name.strip())
    return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name, "pipe_name": pipe_name.strip()}


@router.post("/reflection/pipes/enable")
def api_enable_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    from supertable.super_pipe import SuperPipe

    try:
        pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
        pipe.set_enabled(pipe_name=pipe_name, enabled=True)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enable pipe failed: {e}")

    storage = get_storage()
    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        for p in pipes:
            if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                p["enabled"] = True
                p["updated_at_ns"] = time.time_ns()
        _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

    p_path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
    meta = _read_json_if_exists(storage, p_path) or (_redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
    if isinstance(meta, dict):
        meta["enabled"] = True
    _redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, meta if isinstance(meta, dict) else {})
    return {"ok": True}


@router.post("/reflection/pipes/disable")
def api_disable_pipe(
    request: Request,
    org: str = Query(...),
    sup: str = Query(...),
    staging_name: str = Query(...),
    pipe_name: str = Query(...),
    _: Any = Depends(logged_in_guard_api),
):
    from supertable.super_pipe import SuperPipe

    try:
        pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
        pipe.set_enabled(pipe_name=pipe_name, enabled=False)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Disable pipe failed: {e}")

    storage = get_storage()
    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        for p in pipes:
            if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                p["enabled"] = False
                p["updated_at_ns"] = time.time_ns()
        _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

    p_path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
    meta = _read_json_if_exists(storage, p_path) or (_redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
    if isinstance(meta, dict):
        meta["enabled"] = False
    _redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, meta if isinstance(meta, dict) else {})
    return {"ok": True}


@router.post("/reflection/ingestion/load/upload")
async def api_ingestion_load_upload(
    request: Request,
    org: str = Form(...),
    sup: str = Form(...),
    role_name: str = Form(""),
    mode: str = Form(...),
    table_name: Optional[str] = Form(None),
    staging_name: Optional[str] = Form(None),
    overwrite_columns: Optional[str] = Form(None),
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
            return stg.save_as_parquet(arrow_table=arrow_table, base_file_name=base_name)

        try:
            saved = await asyncio.to_thread(_do_stage)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Staging save failed: {e}")

        dt_ms = (time.perf_counter() - t0) * 1000.0
        rows_count = getattr(arrow_table, "num_rows", None)
        return {
            "ok": True, "mode": "staging", "organization": org_eff, "super_name": sup_eff,
            "staging_name": stg_eff, "saved_file_name": saved, "rows": rows_count if rows_count is not None else 0,
            "file_type": ext or "unknown", "job_uuid": job_uuid, "server_duration_ms": dt_ms,
        }

    table_eff = (table_name or "").strip()
    overwrite_cols: List[str] = []
    if overwrite_columns:
        raw = str(overwrite_columns)
        overwrite_cols = [c.strip() for c in re.split(r"[\n,]+", raw) if c.strip()]

    try:
        from supertable.data_writer import DataWriter
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DataWriter import failed: {e}")

    def _do_write() -> Any:
        dw = DataWriter(super_name=sup_eff, organization=org_eff)
        return dw.write(role_name=role_eff, simple_name=table_eff, data=arrow_table, overwrite_columns=overwrite_cols)

    try:
        cols, rows_written, inserted, deleted = await asyncio.to_thread(_do_write)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Write failed: {e}")

    dt_ms = (time.perf_counter() - t0) * 1000.0
    return {
        "ok": True, "mode": "table", "organization": org_eff, "super_name": sup_eff,
        "table_name": table_eff, "rows": rows_written, "inserted": inserted, "deleted": deleted,
        "file_type": ext or "unknown", "job_uuid": job_uuid, "server_duration_ms": dt_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
#   MONITORING endpoints
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/reflection/monitoring/reads")
def monitoring_reads(
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
        monitor_type="plans", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=limit,
    )
    return JSONResponse({"ok": True, "items": items})


@router.get("/reflection/monitoring/writes")
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
        monitor_type="writes", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    return JSONResponse({"ok": True, "items": items})


# ═══════════════════════════════════════════════════════════════════════════
#   SECURITY endpoints (page + RBAC CRUD + OData endpoints)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/reflection/rbac/roles")
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


@router.post("/reflection/rbac/roles")
def rbac_role_create(
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
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
    return JSONResponse({"ok": True, "data": doc}, status_code=201)


@router.put("/reflection/rbac/roles/{role_id}")
def rbac_role_update(
    role_id: str,
    payload: Dict[str, Any] = Body(...),
    _=Depends(admin_guard_api),
):
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
    return JSONResponse({"ok": True, "data": doc})


@router.delete("/reflection/rbac/roles/{role_id}")
def rbac_role_delete(
    role_id: str,
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
    deleted = _sec_delete_role(redis_client, org, sup, role_id)
    return JSONResponse({"ok": True, "data": {"deleted": deleted}})


# ── User CRUD ──

@router.get("/reflection/rbac/users")
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

@router.get("/reflection/odata/endpoints")
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


@router.post("/reflection/odata/endpoints")
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


@router.put("/reflection/odata/endpoints/{endpoint_id}")
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


@router.post("/reflection/odata/endpoints/{endpoint_id}/regenerate")
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


@router.delete("/reflection/odata/endpoints/{endpoint_id}")
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


@router.get("/reflection/quality/overview")
def api_quality_overview(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    all_latest = dqc.get_all_latest()
    return {"ok": True, "tables": all_latest}


@router.get("/reflection/quality/tables")
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


@router.get("/reflection/quality/latest")
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


@router.get("/reflection/quality/global-config")
def api_dq_get_global_config(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "config": dqc.get_global_config(), "builtin_checks": BUILTIN_CHECKS}


@router.put("/reflection/quality/global-config")
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


@router.get("/reflection/quality/schedule")
def api_dq_get_schedule(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "schedule": dqc.get_schedule()}


@router.put("/reflection/quality/schedule")
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


@router.put("/reflection/quality/table-schedule")
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


@router.delete("/reflection/quality/table-schedule")
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


@router.get("/reflection/quality/table-schedules")
def api_dq_get_all_table_schedules(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    overrides = dqc.get_all_table_schedules()
    return {"ok": True, "overrides": overrides}


@router.get("/reflection/quality/rules")
def api_dq_list_rules(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    dqc = _get_dqc(request, org, sup)
    return {"ok": True, "rules": dqc.list_rules()}


@router.post("/reflection/quality/rules")
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


@router.put("/reflection/quality/rules")
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


@router.delete("/reflection/quality/rules")
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


@router.post("/reflection/quality/run")
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


@router.post("/reflection/quality/run-all")
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

    def _run_sequential():
        from supertable.services.quality.scheduler import _run_quick_check, _run_deep_check
        for tbl in tables:
            try:
                logger.info(f"[dq-run-all] {mode} check: {tbl}")
                if mode == "deep":
                    _run_deep_check(redis_client, o, s, tbl, dqc)
                else:
                    _run_quick_check(redis_client, o, s, tbl, dqc)
            except Exception as e:
                logger.error(f"[dq-run-all] {mode} failed for {tbl}: {e}")

    t = threading.Thread(target=_run_sequential, name="dq-run-all", daemon=True)
    t.start()

    return {"ok": True, "message": f"{mode} check started on {len(tables)} tables (sequential)", "count": len(tables)}


@router.get("/reflection/quality/history")
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
        conditions = [f"checked_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'"]
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


@router.get("/reflection/compute/list")
def compute_pools_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
    org = str(org or "").strip()
    sup = str(sup or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org/sup required")
    return {"ok": True, "data": _compute_load(org, sup), "kinds": list(_COMPUTE_KINDS), "sizes": list(_COMPUTE_SIZES)}


@router.post("/reflection/compute/upsert")
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


@router.delete("/reflection/compute/{pool_id}")
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


@router.post("/reflection/compute/test-connection")
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


@router.get("/reflection/engine-config")
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


@router.post("/reflection/engine-config")
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


@router.get("/reflection/users-page/list")
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


@router.post("/reflection/users-page/create")
def api_users_page_create(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
    username = str(payload.get("username") or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")

    role_type = str(payload.get("role_type") or "").strip().lower()
    if role_type not in _VALID_ROLE_TYPES_USERS:
        raise HTTPException(status_code=400, detail=f"role_type must be one of: {', '.join(sorted(_VALID_ROLE_TYPES_USERS))}")

    org = str(payload.get("org") or "").strip()
    sup = str(payload.get("sup") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="org and sup are required")

    if username.lower() == sup.lower():
        raise HTTPException(status_code=403, detail="The superuser account cannot be created from the UI.")

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
            "tables": ["*"], "columns": ["*"], "filters": ["*"],
        })

    if not role_id:
        raise HTTPException(status_code=500, detail="Could not resolve role_id")

    try:
        user_id = um.create_user({"username": username, "roles": [role_id]})
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User creation failed: {e}")

    return {"ok": True, "user_id": user_id, "username": username, "role_id": role_id}


@router.delete("/reflection/users-page/user/{user_id}")
def api_users_page_delete(
    request: Request,
    user_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    o, s = _users_resolve_or_raise(org, sup)
    um = _users_user_manager(o, s)

    try:
        all_users = list_users(o, s)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not verify user identity before deletion: {e}")

    superuser_ids: set = {
        str(u.get("user_id") or u.get("hash") or "").strip()
        for u in all_users
        if str(u.get("username") or u.get("name") or "").strip().lower() == s.lower()
    }
    superuser_ids.discard("")

    if user_id in superuser_ids:
        raise HTTPException(status_code=403, detail="The superuser account cannot be deleted from the UI.")

    try:
        um.delete_user(user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")
    return {"ok": True, "user_id": user_id}


@router.post("/reflection/users-page/user/{user_id}/role")
def api_users_page_add_role(
    request: Request,
    user_id: str,
    payload: Dict[str, Any] = Body(...),
    _: Any = Depends(admin_guard_api),
):
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


@router.delete("/reflection/users-page/user/{user_id}/role/{role_id}")
def api_users_page_remove_role(
    request: Request,
    user_id: str,
    role_id: str,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(admin_guard_api),
):
    o, s = _users_resolve_or_raise(org, sup)
    try:
        catalog.rbac_remove_role_from_user(o, s, user_id, role_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Remove role failed: {e}")
    return {"ok": True, "user_id": user_id, "role_id": role_id}


# ═══════════════════════════════════════════════════════════════════════════
#   Backward-compat route aliases
# ═══════════════════════════════════════════════════════════════════════════

from supertable.server_common import _add_reflection_alias_routes

_add_reflection_alias_routes(router)
