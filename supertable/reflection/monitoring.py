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



# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints previously registered here have moved to supertable.api.api.
# This function is preserved so existing callers do not break.
# ---------------------------------------------------------------------------

def attach_monitoring_routes(
    router,
    *,
    templates,
    redis_client,
    is_authorized,
    no_store,
    get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    logged_in_guard_api,
):
    """No-op — endpoints moved to supertable.api.api."""
    pass


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
