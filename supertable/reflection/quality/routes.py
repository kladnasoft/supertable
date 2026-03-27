# route: supertable.reflection.quality.routes
# supertable/reflection/quality/routes.py
"""
FastAPI routes for Data Quality module.

Page:
  GET  /reflection/quality           — Data Quality dashboard page

APIs:
  GET  /reflection/quality/overview  — all tables' latest results
  GET  /reflection/quality/latest    — single table latest result
  GET  /reflection/quality/column    — single column latest result
  GET  /reflection/quality/config    — get effective config for a table
  PUT  /reflection/quality/config    — update table config (check toggles + thresholds)
  GET  /reflection/quality/global-config  — get global config
  PUT  /reflection/quality/global-config  — update global config
  GET  /reflection/quality/schedule  — get schedule config
  PUT  /reflection/quality/schedule  — update schedule config
  GET  /reflection/quality/rules     — list custom rules
  POST /reflection/quality/rules     — create custom rule
  PUT  /reflection/quality/rules     — update custom rule
  DELETE /reflection/quality/rules   — delete custom rule
  POST /reflection/quality/run       — trigger immediate check (quick or deep)
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

logger = logging.getLogger(__name__)


def attach_quality_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], str],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Dict[str, Any]],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
    catalog: Any = None,
    redis_client: Any = None,
    get_session: Any = None,
) -> None:
    """Register Data Quality routes on the shared router."""

    from supertable.reflection.quality.config import DQConfig, BUILTIN_CHECKS

    def _get_dqc(request: Request, org: str = None, sup: str = None) -> DQConfig:
        o, s = resolve_pair(org, sup)
        if not o or not s:
            raise HTTPException(status_code=400, detail="No organization/super selected")
        return DQConfig(redis_client, o, s)

    def _resolve(org: str = None, sup: str = None):
        return resolve_pair(org, sup)

    # ── Page ──────────────────────────────────────────────────────

    @router.get("/reflection/quality", response_class=HTMLResponse)
    def quality_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
    ):
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)
        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        ctx = {
            "request": request,
            "tenants": tenants,
            "selected_org": sel_org or "",
            "selected_sup": sel_sup or "",
            "builtin_checks": BUILTIN_CHECKS,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("quality.html", ctx)
        no_store(resp)
        return resp

    # ── Overview (all tables) ─────────────────────────────────────

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

    # ── List all tables from catalog (not just ones with quality data) ─

    @router.get("/reflection/quality/tables")
    def api_quality_tables(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Return all table names from the catalog — used by Run All Now."""
        o, s = _resolve(org, sup)
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

    # ── Single table latest ───────────────────────────────────────

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

    # ── Single column latest ──────────────────────────────────────

    @router.get("/reflection/quality/column")
    def api_quality_column(
        request: Request,
        table: str = Query(...),
        column: str = Query(...),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        col_data = dqc.get_latest_column(table, column)
        return {"ok": True, "column": col_data}

    # ── Config (per-table) ────────────────────────────────────────

    @router.get("/reflection/quality/config")
    def api_get_config(
        request: Request,
        table: str = Query(...),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        effective = dqc.get_effective_config(table)
        table_overrides = dqc.get_table_config(table)
        return {
            "ok": True,
            "effective": effective,
            "table_overrides": table_overrides,
            "builtin_checks": BUILTIN_CHECKS,
        }

    @router.put("/reflection/quality/config")
    def api_set_config(
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
        sess = get_session(request) or {} if get_session else {}
        dqc.set_table_config(table, body, updated_by=sess.get("username", ""))
        return {"ok": True}

    # ── Global config ─────────────────────────────────────────────

    @router.get("/reflection/quality/global-config")
    def api_get_global_config(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        return {"ok": True, "config": dqc.get_global_config(), "builtin_checks": BUILTIN_CHECKS}

    @router.put("/reflection/quality/global-config")
    def api_set_global_config(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        body: Dict[str, Any] = Body(None),
        _: Any = Depends(admin_guard_api),
    ):
        if body is None:
            body = {}
        dqc = _get_dqc(request, org, sup)
        sess = get_session(request) or {} if get_session else {}
        dqc.set_global_config(body, updated_by=sess.get("username", ""))
        return {"ok": True}

    # ── Schedule ──────────────────────────────────────────────────

    @router.get("/reflection/quality/schedule")
    def api_get_schedule(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        return {"ok": True, "schedule": dqc.get_schedule()}

    @router.put("/reflection/quality/schedule")
    def api_set_schedule(
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

    # ── Per-table schedule overrides ──────────────────────────────

    @router.get("/reflection/quality/table-schedule")
    def api_get_table_schedule(
        request: Request,
        table: str = Query(...),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        ts = dqc.get_table_schedule(table)
        return {"ok": True, "table": table, "schedule": ts}

    @router.put("/reflection/quality/table-schedule")
    def api_set_table_schedule(
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
    def api_delete_table_schedule(
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
    def api_get_all_table_schedules(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        """Return all per-table schedule overrides in one call."""
        dqc = _get_dqc(request, org, sup)
        overrides = dqc.get_all_table_schedules()
        return {"ok": True, "overrides": overrides}

    # ── Custom rules ──────────────────────────────────────────────

    @router.get("/reflection/quality/rules")
    def api_list_rules(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        return {"ok": True, "rules": dqc.list_rules()}

    @router.post("/reflection/quality/rules")
    def api_create_rule(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        body: Dict[str, Any] = Body(None),
        _: Any = Depends(admin_guard_api),
    ):
        if body is None:
            raise HTTPException(status_code=400, detail="Request body required")
        dqc = _get_dqc(request, org, sup)
        sess = get_session(request) or {} if get_session else {}
        rule = dqc.create_rule(body, created_by=sess.get("username", ""))
        return {"ok": True, "rule": rule}

    @router.put("/reflection/quality/rules")
    def api_update_rule(
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
    def api_delete_rule(
        request: Request,
        rule_id: str = Query(...),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        dqc = _get_dqc(request, org, sup)
        dqc.delete_rule(rule_id)
        return {"ok": True}

    # ── Trigger immediate check ───────────────────────────────────

    @router.post("/reflection/quality/run")
    def api_run_check(
        request: Request,
        table: str = Query(...),
        mode: str = Query("quick"),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        """Trigger an immediate quality check on one table (background thread)."""
        o, s = _resolve(org, sup)
        if not o or not s:
            raise HTTPException(status_code=400, detail="No org/sup")

        dqc = _get_dqc(request, org, sup)

        def _run():
            try:
                from supertable.reflection.quality.scheduler import _run_quick_check, _run_deep_check
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
    def api_run_all(
        request: Request,
        mode: str = Query("quick"),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        """Run quality checks on ALL tables — sequentially in one background thread.

        This avoids DuckDB segfaults from parallel concurrent queries.
        """
        o, s = _resolve(org, sup)
        if not o or not s:
            raise HTTPException(status_code=400, detail="No org/sup")

        # Collect table names from catalog
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
            from supertable.reflection.quality.scheduler import _run_quick_check, _run_deep_check
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

    # ── History (quality score over time) ─────────────────────────

    @router.get("/reflection/quality/history")
    def api_quality_history(
        request: Request,
        table: Optional[str] = Query(None),
        days: int = Query(7, ge=1, le=365),
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Return quality check history for charting.

        Filters by date range (last N days). Tries __data_quality__ Parquet
        table first, falls back to Redis LIST.
        If table param is provided, filters to that table only.
        """
        o, s = _resolve(org, sup)
        if not o or not s:
            return {"ok": True, "rows": []}

        # ── Strategy 1: Query __data_quality__ via DataReader ─────
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
                # Convert timestamps to ISO strings
                for row in rows:
                    for k, v in row.items():
                        if hasattr(v, "isoformat"):
                            row[k] = v.isoformat()
                return {"ok": True, "source": "parquet", "days": days, "rows": rows}

        except Exception as e:
            logger.debug(f"[dq-history] Parquet query failed, trying Redis: {e}")

        # ── Strategy 2: Read from Redis LIST fallback ─────────────
        try:
            from supertable.reflection.quality.config import _dq_key
            from datetime import datetime, timezone, timedelta

            key = _dq_key(o, s, "history")
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Read up to 1000 entries and filter by date + table
            raw_items = redis_client.lrange(key, 0, 999)
            rows = []
            for raw in (raw_items or []):
                try:
                    item = raw if isinstance(raw, str) else raw.decode("utf-8")
                    row = json.loads(item)
                    # Date filter
                    checked_at = row.get("checked_at", "")
                    if checked_at < cutoff:
                        continue
                    # Table filter
                    if table and row.get("table_name") != table:
                        continue
                    rows.append(row)
                except Exception:
                    continue

            return {"ok": True, "source": "redis", "days": days, "rows": rows}

        except Exception as e:
            logger.warning(f"[dq-history] Redis history read failed: {e}")

        return {"ok": True, "source": "none", "days": days, "rows": []}
