# route: supertable.reflection.monitoring
"""
Monitoring page — unified read/write operation monitoring.

Serves:
  GET /reflection/monitoring              — HTML page
  GET /reflection/monitoring/reads        — JSON read operations (from Redis monitor:plans list)
  GET /reflection/monitoring/writes       — JSON write operations (from Redis monitor:writes list)

Write payload shape (pushed by MonitoringWriter):
  {
    "query_id": "...",
    "recorded_at": "2026-03-15T02:32:46.150812+00:00",
    "organization": "...",
    "super_name": "...",
    "role_name": "superadmin",
    "table_name": "facts",
    "incoming_rows": 100,
    "inserted": 100,
    "deleted": 0,
    "duration": 0.255799,
    ...
  }
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

logger = logging.getLogger(__name__)


def _parse_ts_ms(value: Any) -> Optional[int]:
    """
    Parse a timestamp value into epoch milliseconds.

    Handles:
      - ISO 8601 string  ("2026-03-15T02:32:46.150812+00:00")
      - Epoch seconds     (1742003566.15)
      - Epoch milliseconds (1742003566150)
    """
    if value is None:
        return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp() * 1_000)
        except Exception:
            pass
        try:
            f = float(s)
            return int(f) if f > 1e12 else int(f * 1_000)
        except Exception:
            return None

    try:
        f = float(value)
        return int(f) if f > 1e12 else int(f * 1_000)
    except Exception:
        return None


def attach_monitoring_routes(
    router: APIRouter,
    *,
    templates: Any,
    redis_client: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[str, str]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    logged_in_guard_api: Any,
) -> None:
    """Attach monitoring page and API routes."""

    @router.get("/reflection/monitoring", response_class=HTMLResponse)
    def monitoring_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
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

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("monitoring.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/monitoring/reads")
    def monitoring_reads(
        org: str = Query(""),
        sup: str = Query(""),
        from_ts: Optional[int] = Query(None),
        to_ts: Optional[int] = Query(None),
        limit: int = Query(500),
        _=Depends(logged_in_guard_api),
    ):
        """
        Return read (query execution) operations from monitoring.

        Query params:
          from_ts — epoch ms, inclusive lower bound
          to_ts   — epoch ms, inclusive upper bound
          limit   — max items returned (default 500)
        """
        org = (org or "").strip()
        sup = (sup or "").strip()
        if not org or not sup:
            return JSONResponse({"ok": True, "items": []})

        items = _read_monitoring_list(
            redis_client, org, sup,
            monitor_type="plans",
            from_ts_ms=from_ts,
            to_ts_ms=to_ts,
            limit=limit,
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
        """
        Return write operations from monitoring.

        Reads from Redis list  monitor:{org}:{sup}:writes
        Each item has 'recorded_at' as ISO 8601 string.

        Query params:
          from_ts — epoch ms, inclusive lower bound
          to_ts   — epoch ms, inclusive upper bound
          limit   — max items returned (default 500)
        """
        org = (org or "").strip()
        sup = (sup or "").strip()
        if not org or not sup:
            return JSONResponse({"ok": True, "items": []})

        items = _read_monitoring_list(
            redis_client, org, sup,
            monitor_type="writes",
            from_ts_ms=from_ts,
            to_ts_ms=to_ts,
            limit=limit,
            ts_fields=("recorded_at", "execution_time", "timestamp"),
        )
        return JSONResponse({"ok": True, "items": items})


def _read_monitoring_list(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    monitor_type: str,
    from_ts_ms: Optional[int] = None,
    to_ts_ms: Optional[int] = None,
    limit: int = 500,
    ts_fields: Tuple[str, ...] = ("execution_time", "recorded_at", "timestamp"),
) -> List[Dict[str, Any]]:
    """
    Read monitoring entries from the Redis list, with optional time-range filtering.

    The MonitoringWriter pushes JSON payloads via RPUSH to:
        monitor:{org}:{sup}:{monitor_type}

    We read from the tail (newest first), parse each JSON payload,
    and filter by timestamp when from_ts_ms / to_ts_ms are provided.

    Over-reads 3x limit from Redis to compensate for filtered-out items.
    """
    key = f"monitor:{org}:{sup}:{monitor_type}"
    items: List[Dict[str, Any]] = []

    fetch_count = limit * 3 if (from_ts_ms is not None or to_ts_ms is not None) else limit

    try:
        raw_list = redis_client.lrange(key, -fetch_count, -1)
        if not raw_list:
            return []

        for raw in reversed(raw_list):
            if len(items) >= limit:
                break

            try:
                s = raw if isinstance(raw, str) else raw.decode("utf-8")
                item = json.loads(s)
                if not isinstance(item, dict):
                    continue
            except Exception:
                continue

            # Server-side time filtering
            if from_ts_ms is not None or to_ts_ms is not None:
                item_ts = None
                for field in ts_fields:
                    if field in item:
                        item_ts = _parse_ts_ms(item[field])
                        if item_ts is not None:
                            break

                if item_ts is not None:
                    if from_ts_ms is not None and item_ts < from_ts_ms:
                        continue
                    if to_ts_ms is not None and item_ts > to_ts_ms:
                        continue

            items.append(item)

    except Exception as e:
        logger.warning("[monitoring] failed to read Redis list %s: %s", key, e)

    return items
