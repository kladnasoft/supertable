# route: supertable.services.monitoring
"""
Monitoring page — unified read/write/MCP operation monitoring.

Serves:
  GET /reflection/monitoring              — HTML page
  GET /reflection/monitoring/reads        — JSON read operations (from Redis monitor:plans list)
  GET /reflection/monitoring/writes       — JSON write operations (from Redis monitor:writes list)
  GET /reflection/monitoring/mcp          — JSON MCP tool call metrics
  GET /reflection/monitoring/summary      — Aggregated dashboard metrics

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
import time
from collections import Counter
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


# ---------------------------------------------------------------------------
# Percentile helper
# ---------------------------------------------------------------------------

def _percentile(sorted_values: List[float], p: float) -> float:
    """Compute the p-th percentile from a pre-sorted list of floats."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_values):
        return sorted_values[-1]
    return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])


# ---------------------------------------------------------------------------
# Extract duration from various payload shapes
# ---------------------------------------------------------------------------

def _extract_duration_ms(item: Dict[str, Any]) -> Optional[float]:
    """
    Extract duration in milliseconds from a monitoring payload.

    Handles the different shapes used by reads (execution_timings JSON string
    or nested dict) and writes (duration in seconds as a float).
    """
    # Writes store duration in seconds as a top-level float
    raw_dur = item.get("duration")
    if raw_dur is not None:
        try:
            return float(raw_dur) * 1000.0
        except (TypeError, ValueError):
            pass

    # MCP stores duration_ms directly
    raw_ms = item.get("duration_ms")
    if raw_ms is not None:
        try:
            return float(raw_ms)
        except (TypeError, ValueError):
            pass

    # Reads store execution_timings as a JSON string or dict
    raw_timings = item.get("execution_timings")
    if raw_timings is not None:
        timings = raw_timings
        if isinstance(timings, str):
            try:
                timings = json.loads(timings)
            except Exception:
                return None
        if isinstance(timings, dict):
            total = timings.get("total") or timings.get("total_s")
            if total is not None:
                try:
                    return float(total) * 1000.0
                except (TypeError, ValueError):
                    pass
    return None


# ---------------------------------------------------------------------------
# Per-category summary builders
# ---------------------------------------------------------------------------

def _summarize_read_ops(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize read operation metrics."""
    durations: List[float] = []
    total_rows = 0
    errors = 0
    engine_counts: Counter = Counter()
    table_counts: Counter = Counter()

    for it in items:
        status = it.get("status", "ok")
        if status and str(status).upper() in ("ERROR", "FAIL", "FAILED"):
            errors += 1

        dur = _extract_duration_ms(it)
        if dur is not None and dur >= 0:
            durations.append(dur)

        rr = it.get("result_rows")
        if rr is not None:
            try:
                total_rows += int(rr)
            except (TypeError, ValueError):
                pass

        engine = it.get("engine") or "unknown"
        engine_counts[engine] += 1

        table = it.get("table_name") or it.get("table")
        if table:
            table_counts[table] += 1

    durations.sort()
    return {
        "count": len(items),
        "errors": errors,
        "total_rows": total_rows,
        "p50_ms": round(_percentile(durations, 50), 2),
        "p95_ms": round(_percentile(durations, 95), 2),
        "p99_ms": round(_percentile(durations, 99), 2),
        "by_engine": dict(engine_counts.most_common(10)),
        "by_table": dict(table_counts.most_common(10)),
    }


def _summarize_write_ops(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize write operation metrics."""
    durations: List[float] = []
    total_inserted = 0
    total_deleted = 0
    errors = 0
    table_counts: Counter = Counter()

    for it in items:
        if it.get("error"):
            errors += 1

        dur = _extract_duration_ms(it)
        if dur is not None and dur >= 0:
            durations.append(dur)

        try:
            total_inserted += int(it.get("inserted") or 0)
        except (TypeError, ValueError):
            pass
        try:
            total_deleted += int(it.get("deleted") or 0)
        except (TypeError, ValueError):
            pass

        table = it.get("table_name") or it.get("target_table")
        if table:
            table_counts[table] += 1

    durations.sort()
    return {
        "count": len(items),
        "errors": errors,
        "total_inserted": total_inserted,
        "total_deleted": total_deleted,
        "p50_ms": round(_percentile(durations, 50), 2),
        "p95_ms": round(_percentile(durations, 95), 2),
        "p99_ms": round(_percentile(durations, 99), 2),
        "by_table": dict(table_counts.most_common(10)),
    }


def _summarize_mcp_ops(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize MCP tool call metrics."""
    durations: List[float] = []
    errors = 0
    tool_counts: Counter = Counter()
    tool_errors: Counter = Counter()

    for it in items:
        status = it.get("status", "ok")
        is_error = str(status).lower() in ("error", "fail", "failed")
        if is_error:
            errors += 1

        dur = _extract_duration_ms(it)
        if dur is not None and dur >= 0:
            durations.append(dur)

        tool = it.get("tool") or "unknown"
        tool_counts[tool] += 1
        if is_error:
            tool_errors[tool] += 1

    durations.sort()
    return {
        "count": len(items),
        "errors": errors,
        "p50_ms": round(_percentile(durations, 50), 2),
        "p95_ms": round(_percentile(durations, 95), 2),
        "p99_ms": round(_percentile(durations, 99), 2),
        "by_tool": dict(tool_counts.most_common(15)),
        "errors_by_tool": dict(tool_errors.most_common(10)) if errors else {},
    }


# ---------------------------------------------------------------------------
# Public: compute monitoring summary
# ---------------------------------------------------------------------------

def compute_monitoring_summary(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 24,
) -> Dict[str, Any]:
    """
    Compute aggregate metrics from the last N hours of monitoring data.

    Reads from the three Redis monitor lists (plans, writes, mcp) and
    computes counts, error rates, latency percentiles, and top-N breakdowns.
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    scan_limit = 10000  # Upper bound per category to keep computation bounded

    reads = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=from_ms, limit=scan_limit,
    )
    writes = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes", from_ts_ms=from_ms, limit=scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    mcp_calls = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="mcp", from_ts_ms=from_ms, limit=scan_limit,
    )

    return {
        "window_hours": window_hours,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "reads": _summarize_read_ops(reads),
        "writes": _summarize_write_ops(writes),
        "mcp": _summarize_mcp_ops(mcp_calls),
    }
