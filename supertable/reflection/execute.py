# route: supertable.reflection.execute
"""
Execute tab: SQL execution page, query API, and schema autocomplete API.
"""
from __future__ import annotations

import enum
import json
import re
import uuid
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import Query, HTTPException, Request, Depends, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# SQL helpers
# ────────────────────────────────────────────────────────────────────

def _clean_sql_query(query: str) -> str:
    """Remove comments and trailing semicolons."""
    q = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    q = re.sub(r'/\*.*?\*/', '', q, flags=re.DOTALL)
    q = re.sub(r';+$', '', q)
    return q.strip()


def _apply_limit_safely(query: str, max_rows: int) -> str:
    """Ensure a LIMIT is present and not above *max_rows + 1*."""
    limit_pattern = r'(?<!\w)(limit)\s+(\d+)(?!\w)(?=[^;]*$|;)'
    m = re.search(limit_pattern, query, re.IGNORECASE)
    if m:
        cur = int(m.group(2))
        if cur > max_rows + 1:
            return re.sub(limit_pattern, f'LIMIT {max_rows + 1}', query,
                          flags=re.IGNORECASE, count=1)
        return query
    return f"{query.rstrip(';').strip()} LIMIT {max_rows + 1}"


# ────────────────────────────────────────────────────────────────────
# JSON sanitizer (Decimal, datetime, numpy, Enum, …)
# ────────────────────────────────────────────────────────────────────

def _sanitize_for_json(obj: Any) -> Any:
    """Convert arbitrary objects into JSON-safe structures."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="ignore")
        except Exception:
            return str(obj)
    if isinstance(obj, enum.Enum):
        return getattr(obj, "name", str(obj))

    # numpy scalars (without importing numpy)
    np_names = (
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float16", "float32", "float64",
    )
    if obj.__class__.__name__ in np_names:
        try:
            return obj.item()
        except Exception:
            return float(obj) if "float" in obj.__class__.__name__ else int(obj)

    # Containers
    if isinstance(obj, dict):
        return {_sanitize_for_json(k): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, set):
        return [_sanitize_for_json(x) for x in obj]

    # Objects with dict-like view
    for attr in ("_asdict", "dict", "__dict__"):
        if hasattr(obj, attr):
            try:
                return _sanitize_for_json(getattr(obj, attr)())
            except Exception:
                pass

    return str(obj)


# ────────────────────────────────────────────────────────────────────
# Route attachment
# ────────────────────────────────────────────────────────────────────

def attach_execute_routes(
    router,
    *,
    templates,
    is_authorized,
    no_store,
    get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
    logged_in_guard_api,
    admin_guard_api,
):
    """Register GET /reflection/execute, POST /reflection/execute,
    and POST /reflection/schema on the shared router."""

    # Lazy imports — resolved at call time, not module load time.

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

    # ── GET /reflection/execute ─────────────────────────────────────

    @router.get("/reflection/execute")
    def execute_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        leaf: Optional[str] = Query(None),
    ):
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        provided = get_provided_token(request) or ""

        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)
        tenants = [
            {"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)}
            for o, s in pairs
        ]

        sess = get_session(request) or {}
        role_name = sess.get("role_name", "") or ""

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
            "initial_leaf": leaf,
            "default_role_name": role_name,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("execute.html", ctx)
        no_store(resp)
        return resp

    # ── POST /reflection/execute ────────────────────────────────────

    @router.post("/reflection/execute")
    def execute_api(
        request: Request,
        payload: Dict[str, Any] = Body(...),
        _=Depends(admin_guard_api),
    ):
        """Run a read-only SQL (SELECT/WITH) and return paginated JSON."""
        try:
            query = str(payload.get("query") or "").strip()
            organization = str(payload.get("organization") or "")
            super_name = str(payload.get("super_name") or "")
            role_name = str(
                payload.get("role_name") or payload.get("user_hash") or ""
            )
            page = int(payload.get("page") or 1)
            page_size = int(payload.get("page_size") or 100)
            max_rows = 10_000

            # Engine selection — validate against allowed values
            engine_raw = str(payload.get("engine") or "auto").strip().lower()
            _ALLOWED_ENGINES = {
                "auto", "duckdb_pro", "duckdb_lite", "spark_sql",
            }
            if engine_raw not in _ALLOWED_ENGINES:
                engine_raw = "auto"

            if not organization or not super_name:
                return JSONResponse(
                    {"status": "error",
                     "message": "organization and super_name are required",
                     "result": []},
                    status_code=400,
                )
            if not query:
                return JSONResponse(
                    {"status": "error", "message": "No query provided",
                     "result": []},
                    status_code=400,
                )
            if not role_name:
                return JSONResponse(
                    {"status": "error", "message": "role_name is required",
                     "result": []},
                    status_code=400,
                )

            q = _clean_sql_query(query)
            if not q.lower().lstrip().startswith(("select", "with")):
                return JSONResponse(
                    {"status": "error",
                     "message": "Only SELECT or WITH (CTE) queries are allowed",
                     "result": []},
                    status_code=400,
                )

            q = _apply_limit_safely(q, max_rows)

            DR = _get_data_reader()
            dr = DR(super_name=super_name, organization=organization, query=q)

            # Resolve the engine enum
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
                    rows = json.loads(
                        page_df.to_json(orient="records", date_format="iso")
                    )
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
                            rows = [
                                {"c{}".format(i): _sanitize_for_json(v)
                                 for i, v in enumerate(r)}
                                for r in page_rows
                            ]
                        elif isinstance(df, list):
                            total_count = len(df)
                            if total_count > max_rows:
                                df = df[:max_rows]
                                total_count = max_rows
                            start = max(0, (page - 1) * page_size)
                            end = start + page_size
                            rows = [_sanitize_for_json(x)
                                    for x in df[start:end]]
                        else:
                            rows = []
                    except Exception:
                        rows = []

            meta_payload = {
                "result_1": meta1,
                "result_2": meta2,
                "timings": getattr(
                    getattr(dr, "timer", None), "timings", None
                ),
                "plan_stats": getattr(
                    getattr(dr, "plan_stats", None), "stats", None
                ),
                "query_profile": getattr(
                    getattr(dr, "query_plan_manager", None), "query_profile", None
                ),
            }

            return JSONResponse({
                "status": "ok",
                "message": None,
                "result": rows,
                "total_count": total_count,
                "meta": _sanitize_for_json(meta_payload),
            })

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Execute SQL failed")
            return JSONResponse(
                {"status": "error",
                 "message": f"Execution failed: {e}",
                 "result": []},
                status_code=500,
            )

    # ── POST /reflection/schema ─────────────────────────────────────

    @router.post("/reflection/schema")
    def schema_api(
        request: Request,
        payload: Dict[str, Any] = Body(...),
        _=Depends(admin_guard_api),
    ):
        """Return table schemas for IntelliSense autocomplete:
        ``{ "schema": [ {table: [col, …]}, … ] }``

        Uses ``list_tables`` to discover tables and
        ``MetaReader.get_table_schema`` for column names.
        """
        try:
            organization = str(payload.get("organization") or "")
            super_name = str(payload.get("super_name") or "")
            role_name = str(
                payload.get("role_name") or payload.get("user_hash") or ""
            )

            if not organization or not super_name or not role_name:
                return JSONResponse(
                    {"status": "error",
                     "message": "organization, super_name and role_name are required"},
                    status_code=400,
                )

            # list_tables is a module-level function in meta_reader
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
                    if (isinstance(table_schema, list)
                            and table_schema
                            and isinstance(table_schema[0], dict)):
                        cols = list(table_schema[0].keys())
                    else:
                        cols = []
                except Exception:
                    cols = []
                schema.append({t: cols})

            return JSONResponse({"status": "ok", "schema": schema})
        except Exception as e:
            return JSONResponse(
                {"status": "error",
                 "message": f"Get schema failed: {e}"},
                status_code=500,
            )