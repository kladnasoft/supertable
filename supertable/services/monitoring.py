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


# ---------------------------------------------------------------------------
# Lock state — scan active per-table Redis locks
# ---------------------------------------------------------------------------

def read_lock_state(
    redis_client: Any,
    org: str,
    sup: str,
) -> List[Dict[str, Any]]:
    """
    Scan all active per-table Redis locks for the given org/sup.

    The lock key pattern is: supertable:{org}:{sup}:lock:leaf:{table}
    Uses SCAN + PTTL — never blocks Redis.

    Returns a list of {table_name, ttl_ms, held} dicts ordered by table name.
    ttl_ms == -1 means the key has no TTL (should not happen with correct lock usage).
    ttl_ms == -2 means the key no longer exists (race between SCAN and PTTL).
    """
    pattern = f"supertable:{org}:{sup}:lock:leaf:*"
    prefix_len = len(f"supertable:{org}:{sup}:lock:leaf:")
    locks: List[Dict[str, Any]] = []

    try:
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(
                cursor=cursor, match=pattern, count=200
            )
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                table_name = k[prefix_len:]
                try:
                    ttl_ms_val = int(redis_client.pttl(k))
                except Exception:
                    ttl_ms_val = -2
                locks.append({
                    "table_name": table_name,
                    "ttl_ms": ttl_ms_val,
                    "held": ttl_ms_val > 0,
                })
            if cursor == 0:
                break
    except Exception as e:
        logger.warning(
            "[monitoring] lock scan failed for %s/%s: %s", org, sup, e
        )

    locks.sort(key=lambda x: x.get("table_name", ""))
    return locks


# ---------------------------------------------------------------------------
# Error feed — merge error-only entries across all three op types
# ---------------------------------------------------------------------------

def read_error_feed(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    from_ts_ms: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Merge error-only entries from reads (plans), writes, and mcp monitor lists.

    Each entry is annotated with an 'op_type' field ('read'|'write'|'mcp')
    so the UI can render type-appropriate badges. Returns newest-first.
    """
    per_type_limit = max(limit, 500)

    reads = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans",
        from_ts_ms=from_ts_ms,
        limit=per_type_limit,
    )
    writes = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes",
        from_ts_ms=from_ts_ms,
        limit=per_type_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    mcp_calls = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="mcp",
        from_ts_ms=from_ts_ms,
        limit=per_type_limit,
    )

    errors: List[Dict[str, Any]] = []

    for item in reads:
        status = str(item.get("status", "")).upper()
        if status in ("ERROR", "FAIL", "FAILED") or item.get("error"):
            item["op_type"] = "read"
            errors.append(item)

    for item in writes:
        if item.get("error"):
            item["op_type"] = "write"
            errors.append(item)

    for item in mcp_calls:
        status = str(item.get("status", "")).lower()
        if status in ("error", "fail", "failed"):
            item["op_type"] = "mcp"
            errors.append(item)

    def _sort_key(it: Dict[str, Any]) -> int:
        for field in ("execution_time", "recorded_at", "timestamp"):
            ts = _parse_ts_ms(it.get(field))
            if ts is not None:
                return ts
        return 0

    errors.sort(key=_sort_key, reverse=True)
    return errors[:limit]


# ---------------------------------------------------------------------------
# Timeseries — bucket reads/writes/mcp/errors into time slots
# ---------------------------------------------------------------------------

def compute_timeseries(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    bucket_hours: int = 1,
    window_hours: int = 24,
    scan_limit: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Bucket read, write, and MCP operations into equal-sized time slots.

    Returns a list of bucket dicts ordered oldest-first:
      {ts_ms, reads, writes_inserted, writes_deleted, errors, mcp_calls}

    bucket_hours: slot width in hours (1 = hourly, 24 = daily).
    window_hours: total window covered (determines number of buckets).
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    bucket_ms = bucket_hours * 3600 * 1000
    n_buckets = max(1, window_hours // bucket_hours)

    # Build empty bucket array
    buckets: List[Dict[str, Any]] = [
        {
            "ts_ms": from_ms + i * bucket_ms,
            "reads": 0,
            "writes_inserted": 0,
            "writes_deleted": 0,
            "errors": 0,
            "mcp_calls": 0,
        }
        for i in range(n_buckets)
    ]

    def _idx(ts_ms: int) -> int:
        offset = ts_ms - from_ms
        if offset < 0:
            return -1
        idx = int(offset // bucket_ms)
        return idx if idx < n_buckets else -1

    # ── Read ops ────────────────────────────────────────────────────────────
    reads = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=from_ms, limit=scan_limit,
    )
    for it in reads:
        ts = _parse_ts_ms(
            it.get("execution_time") or it.get("recorded_at") or it.get("timestamp")
        )
        if ts is None:
            continue
        idx = _idx(ts)
        if idx < 0:
            continue
        buckets[idx]["reads"] += 1
        if str(it.get("status", "")).upper() in ("ERROR", "FAIL", "FAILED"):
            buckets[idx]["errors"] += 1

    # ── Write ops ────────────────────────────────────────────────────────────
    writes = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes", from_ts_ms=from_ms, limit=scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    for it in writes:
        ts = _parse_ts_ms(
            it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
        )
        if ts is None:
            continue
        idx = _idx(ts)
        if idx < 0:
            continue
        try:
            buckets[idx]["writes_inserted"] += int(it.get("inserted") or 0)
        except (TypeError, ValueError):
            pass
        try:
            buckets[idx]["writes_deleted"] += int(it.get("deleted") or 0)
        except (TypeError, ValueError):
            pass
        if it.get("error"):
            buckets[idx]["errors"] += 1

    # ── MCP ops ─────────────────────────────────────────────────────────────
    mcp_calls = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="mcp", from_ts_ms=from_ms, limit=scan_limit,
    )
    for it in mcp_calls:
        ts = _parse_ts_ms(
            it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
        )
        if ts is None:
            continue
        idx = _idx(ts)
        if idx < 0:
            continue
        buckets[idx]["mcp_calls"] += 1
        if str(it.get("status", "")).lower() in ("error", "fail", "failed"):
            buckets[idx]["errors"] += 1

    return buckets


# ---------------------------------------------------------------------------
# Catalog stats — table / user / role / token counts
# ---------------------------------------------------------------------------

def compute_catalog_stats(
    catalog: Any,
    org: str,
    sup: str,
) -> Dict[str, Any]:
    """
    Compute catalog-level statistics for the given org/sup.

    Returns:
      tables              — total leaf (simple table) count
      total_snapshots     — sum of all leaf version values
      users               — active user count
      roles               — role count
      tokens              — API token count
      top_tables_by_version — top-10 tables by snapshot count (most active writes)
      recent_tables         — 5 most recently updated tables

    Never raises. Every sub-probe is individually guarded.
    """
    stats: Dict[str, Any] = {
        "tables": 0,
        "total_snapshots": 0,
        "users": 0,
        "roles": 0,
        "tokens": 0,
        "top_tables_by_version": [],
        "recent_tables": [],
    }

    # ── Leaf (table) scan ────────────────────────────────────────────────────
    try:
        table_entries: List[Dict[str, Any]] = []
        for leaf in catalog.scan_leaf_items(org, sup, count=1000):
            if not isinstance(leaf, dict):
                continue
            name = str(leaf.get("simple") or leaf.get("name") or "")
            version = int(leaf.get("version") or 0)
            ts_ms = int(leaf.get("ts") or leaf.get("updated_ms") or 0)
            table_entries.append({"name": name, "version": version, "ts_ms": ts_ms})

        stats["tables"] = len(table_entries)
        stats["total_snapshots"] = sum(e["version"] for e in table_entries)

        stats["top_tables_by_version"] = sorted(
            table_entries, key=lambda e: e["version"], reverse=True
        )[:10]
        stats["recent_tables"] = sorted(
            table_entries, key=lambda e: e["ts_ms"], reverse=True
        )[:5]
    except Exception as e:
        logger.warning(
            "[monitoring] catalog leaf scan failed for %s/%s: %s", org, sup, e
        )

    # ── Users ────────────────────────────────────────────────────────────────
    try:
        users = catalog.get_users(org, sup)
        stats["users"] = len(users) if users else 0
    except Exception:
        pass

    # ── Roles ────────────────────────────────────────────────────────────────
    try:
        roles = catalog.get_roles(org, sup)
        stats["roles"] = len(roles) if roles else 0
    except Exception:
        pass

    # ── API tokens ───────────────────────────────────────────────────────────
    try:
        tokens = catalog.list_auth_tokens(org)
        stats["tokens"] = len(tokens) if tokens else 0
    except Exception:
        pass

    return stats


# ---------------------------------------------------------------------------
# Catalog statistics
# ---------------------------------------------------------------------------

def compute_catalog_stats(
    redis_client: Any,
    catalog: Any,
    org: str,
    sup: str,
) -> Dict[str, Any]:
    """
    Aggregate catalog-level statistics for the monitoring dashboard.

    Scans leaf metadata to count tables + total snapshot versions, then
    reads RBAC user/role counts and token count from the catalog.
    """
    result: Dict[str, Any] = {}

    # Table count + top tables by snapshot version (write activity proxy)
    try:
        table_versions: Dict[str, int] = {}
        table_timestamps: Dict[str, int] = {}
        total_snapshots = 0
        for leaf in catalog.scan_leaf_items(org, sup):
            name: str = leaf.get("simple") or ""
            ver = 0
            try:
                ver = int(leaf.get("version") or 0)
            except (TypeError, ValueError):
                pass
            ts = 0
            try:
                ts = int(leaf.get("ts") or 0)
            except (TypeError, ValueError):
                pass
            total_snapshots += ver
            if name:
                table_versions[name] = ver
                table_timestamps[name] = ts
        result["table_count"] = len(table_versions)
        result["total_snapshots"] = total_snapshots
        result["top_tables_by_snapshots"] = [
            {"table": t, "snapshots": v, "ts": table_timestamps.get(t, 0)}
            for t, v in sorted(table_versions.items(), key=lambda x: -x[1])[:10]
        ]
        # Most recently written table
        if table_timestamps:
            most_recent = max(table_timestamps, key=lambda t: table_timestamps[t])
            result["most_recent_table"] = most_recent
            result["most_recent_ts"] = table_timestamps[most_recent]
        else:
            result["most_recent_table"] = None
            result["most_recent_ts"] = 0
    except Exception as e:
        logger.warning("[monitoring] catalog table scan failed: %s", e)
        result["table_count"] = 0
        result["total_snapshots"] = 0
        result["top_tables_by_snapshots"] = []
        result["most_recent_table"] = None
        result["most_recent_ts"] = 0

    # User count
    try:
        users = catalog.get_users(org, sup)
        result["user_count"] = len(users)
    except Exception:
        result["user_count"] = 0

    # Role count
    try:
        roles = catalog.get_roles(org, sup)
        result["role_count"] = len(roles)
    except Exception:
        result["role_count"] = 0

    # API token count
    try:
        tokens = catalog.list_auth_tokens(org)
        result["token_count"] = len(tokens)
    except Exception:
        result["token_count"] = 0

    return result


# ---------------------------------------------------------------------------
# Time-series buckets
# ---------------------------------------------------------------------------

def compute_timeseries(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 24,
    bucket_minutes: int = 60,
) -> List[Dict[str, Any]]:
    """
    Return time-bucketed read/write volumes aligned to bucket_minutes boundaries.

    Each bucket: {ts_ms, reads, rows_inserted, rows_deleted, errors}
    Returned oldest-first. Gap buckets within the range are filled with zeros
    so the chart never has discontinuities.
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    bucket_ms = bucket_minutes * 60 * 1000
    scan_limit = 5000

    reads = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=from_ms, limit=scan_limit,
    )
    writes = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes", from_ts_ms=from_ms, limit=scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )

    buckets: Dict[int, Dict[str, int]] = {}

    def _bk(ts_ms: int) -> int:
        return (ts_ms // bucket_ms) * bucket_ms

    def _ensure(bk: int) -> None:
        if bk not in buckets:
            buckets[bk] = {"reads": 0, "rows_inserted": 0, "rows_deleted": 0, "errors": 0}

    for it in reads:
        ts = _parse_ts_ms(
            it.get("execution_time") or it.get("recorded_at") or it.get("timestamp")
        )
        if ts is None:
            continue
        bk = _bk(ts)
        _ensure(bk)
        buckets[bk]["reads"] += 1
        if str(it.get("status", "ok")).upper() in ("ERROR", "FAIL", "FAILED"):
            buckets[bk]["errors"] += 1

    for it in writes:
        ts = _parse_ts_ms(
            it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
        )
        if ts is None:
            continue
        bk = _bk(ts)
        _ensure(bk)
        try:
            buckets[bk]["rows_inserted"] += int(it.get("inserted") or 0)
        except (TypeError, ValueError):
            pass
        try:
            buckets[bk]["rows_deleted"] += int(it.get("deleted") or 0)
        except (TypeError, ValueError):
            pass
        if it.get("error"):
            buckets[bk]["errors"] += 1

    # Fill gap buckets so the chart has no holes
    if buckets:
        min_bk = min(buckets)
        max_bk = max(buckets)
        cursor = min_bk
        while cursor <= max_bk:
            _ensure(cursor)
            cursor += bucket_ms

    return [{"ts_ms": bk, **v} for bk, v in sorted(buckets.items())]


# ---------------------------------------------------------------------------
# Active lock scan
# ---------------------------------------------------------------------------

def compute_active_locks(
    redis_client: Any,
    org: str,
    sup: str,
) -> List[Dict[str, Any]]:
    """
    Scan for currently-held per-table write locks.

    Lock keys follow the pattern: supertable:{org}:{sup}:lock:leaf:{table_name}
    Returns [{table_name, ttl_ms}] sorted by table name, keys with expired/missing
    TTL are excluded.
    """
    pattern = f"supertable:{org}:{sup}:lock:leaf:*"
    prefix_len = len(f"supertable:{org}:{sup}:lock:leaf:")
    results: List[Dict[str, Any]] = []

    try:
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode("utf-8")
                table_name = key_str[prefix_len:]
                try:
                    ttl_ms = redis_client.pttl(key_str)
                    if ttl_ms > 0:
                        results.append({"table_name": table_name, "ttl_ms": int(ttl_ms)})
                except Exception:
                    pass
            if cursor == 0:
                break
    except Exception as e:
        logger.warning("[monitoring] lock scan failed: %s", e)

    results.sort(key=lambda x: x["table_name"])
    return results


# ---------------------------------------------------------------------------
# Unified error feed
# ---------------------------------------------------------------------------

def compute_errors_feed(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 24,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Merge failed reads, writes, and MCP calls into a single newest-first feed.

    Each item is tagged with 'source': 'read' | 'write' | 'mcp'.
    Items are sorted by timestamp descending before the limit is applied.
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    scan_limit = 2000

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

    errors: List[Dict[str, Any]] = []

    for it in reads:
        if str(it.get("status", "ok")).upper() in ("ERROR", "FAIL", "FAILED") or it.get("error_detail"):
            sort_ts = _parse_ts_ms(
                it.get("execution_time") or it.get("recorded_at") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "read", "_sort_ts": sort_ts})

    for it in writes:
        if it.get("error"):
            sort_ts = _parse_ts_ms(
                it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "write", "_sort_ts": sort_ts})

    for it in mcp_calls:
        if str(it.get("status", "ok")).lower() in ("error", "fail", "failed"):
            sort_ts = _parse_ts_ms(
                it.get("recorded_at") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "mcp", "_sort_ts": sort_ts})

    errors.sort(key=lambda x: x["_sort_ts"], reverse=True)
    for e in errors:
        e.pop("_sort_ts", None)

    return errors[:limit]


# ---------------------------------------------------------------------------
# Catalog statistics
# ---------------------------------------------------------------------------

def compute_catalog_stats(
    redis_client: Any,
    catalog: Any,
    org: str,
    sup: str,
) -> Dict[str, Any]:
    """
    Catalog-level statistics for the monitoring dashboard.

    Counts tables (leaf keys), snapshot versions, users, roles, and active
    API tokens.  All calls are individually guarded — a Redis failure in one
    probe never aborts the others.
    """
    result: Dict[str, Any] = {}

    # Table count + snapshot sum + top-10 tables by version
    try:
        table_versions: Dict[str, int] = {}
        table_ts: Dict[str, int] = {}
        total_snapshots = 0
        for leaf in catalog.scan_leaf_items(org, sup):
            name = leaf.get("simple") or ""
            ver = 0
            try:
                ver = int(leaf.get("version") or 0)
            except (TypeError, ValueError):
                pass
            ts = 0
            try:
                ts = int(leaf.get("ts") or 0)
            except (TypeError, ValueError):
                pass
            total_snapshots += ver
            if name:
                table_versions[name] = ver
                table_ts[name] = ts
        result["table_count"] = len(table_versions)
        result["total_snapshots"] = total_snapshots
        result["top_tables_by_snapshots"] = [
            {"table": t, "snapshots": v, "ts": table_ts.get(t, 0)}
            for t, v in sorted(table_versions.items(), key=lambda x: -x[1])[:10]
        ]
        # Most recently written table
        if table_ts:
            newest = max(table_ts, key=lambda t: table_ts[t])
            result["newest_table"] = {"table": newest, "ts": table_ts[newest]}
        else:
            result["newest_table"] = None
    except Exception as e:
        logger.warning("[monitoring] catalog table scan failed: %s", e)
        result["table_count"] = 0
        result["total_snapshots"] = 0
        result["top_tables_by_snapshots"] = []
        result["newest_table"] = None

    # User count
    try:
        users = catalog.get_users(org, sup)
        result["user_count"] = len(users)
    except Exception:
        result["user_count"] = 0

    # Role count
    try:
        roles = catalog.get_roles(org, sup)
        result["role_count"] = len(roles)
    except Exception:
        result["role_count"] = 0

    # Active API token count (org-scoped, not sup-scoped)
    try:
        tokens = catalog.list_auth_tokens(org)
        result["token_count"] = len(tokens)
    except Exception:
        result["token_count"] = 0

    return result


# ---------------------------------------------------------------------------
# Time-series aggregation
# ---------------------------------------------------------------------------

def compute_timeseries(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 24,
    bucket_minutes: int = 60,
) -> List[Dict[str, Any]]:
    """
    Return time-bucketed read/write/error volumes for charting.

    Buckets are wall-clock aligned to bucket_minutes boundaries.
    Returns [{ts_ms, reads, rows_inserted, rows_deleted, errors}] oldest-first.

    Gap buckets (no activity) are filled with zeros so the chart has a
    continuous x-axis.
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    bucket_ms = bucket_minutes * 60 * 1000
    scan_limit = 5000

    reads = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="plans", from_ts_ms=from_ms, limit=scan_limit,
    )
    writes = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="writes", from_ts_ms=from_ms, limit=scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )

    buckets: Dict[int, Dict[str, int]] = {}

    def _bk(ts_ms: int) -> int:
        return (ts_ms // bucket_ms) * bucket_ms

    def _ensure(bk: int) -> None:
        if bk not in buckets:
            buckets[bk] = {"reads": 0, "rows_inserted": 0, "rows_deleted": 0, "errors": 0}

    for it in reads:
        ts = _parse_ts_ms(
            it.get("execution_time") or it.get("recorded_at") or it.get("timestamp")
        )
        if ts is None:
            continue
        bk = _bk(ts)
        _ensure(bk)
        buckets[bk]["reads"] += 1
        status = (it.get("status") or "ok").upper()
        if status in ("ERROR", "FAIL", "FAILED"):
            buckets[bk]["errors"] += 1

    for it in writes:
        ts = _parse_ts_ms(
            it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
        )
        if ts is None:
            continue
        bk = _bk(ts)
        _ensure(bk)
        try:
            buckets[bk]["rows_inserted"] += int(it.get("inserted") or 0)
        except (TypeError, ValueError):
            pass
        try:
            buckets[bk]["rows_deleted"] += int(it.get("deleted") or 0)
        except (TypeError, ValueError):
            pass
        if it.get("error"):
            buckets[bk]["errors"] += 1

    # Fill gap buckets so the chart x-axis is continuous
    if buckets:
        min_bk = min(buckets)
        max_bk = max(buckets)
        bk = min_bk
        while bk <= max_bk:
            _ensure(bk)
            bk += bucket_ms

    return [{"ts_ms": bk, **v} for bk, v in sorted(buckets.items())]


# ---------------------------------------------------------------------------
# Active lock scanner
# ---------------------------------------------------------------------------

def compute_active_locks(
    redis_client: Any,
    org: str,
    sup: str,
) -> List[Dict[str, Any]]:
    """
    Scan for currently-held per-table Redis locks.

    Lock key pattern: supertable:{org}:{sup}:lock:leaf:{table_name}

    Returns [{table_name, ttl_ms}] for keys that still exist (TTL > 0),
    sorted by table name.  Keys with TTL == -1 (no expiry — should never
    happen but guarded) are included with ttl_ms=None.
    """
    pattern = f"supertable:{org}:{sup}:lock:leaf:*"
    prefix = f"supertable:{org}:{sup}:lock:leaf:"
    results: List[Dict[str, Any]] = []

    try:
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode("utf-8")
                table_name = key_str[len(prefix):]
                try:
                    ttl_ms = redis_client.pttl(key_str)
                    # pttl returns -2 if key doesn't exist, -1 if no TTL
                    if ttl_ms == -2:
                        continue
                    results.append({
                        "table_name": table_name,
                        "ttl_ms": ttl_ms if ttl_ms >= 0 else None,
                    })
                except Exception:
                    pass
            if cursor == 0:
                break
    except Exception as e:
        logger.warning("[monitoring] lock scan failed: %s", e)

    results.sort(key=lambda x: x["table_name"])
    return results


# ---------------------------------------------------------------------------
# Unified error feed
# ---------------------------------------------------------------------------

def compute_errors_feed(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 24,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Unified error feed: merges failed reads, writes, and MCP calls.

    Each item is tagged with a ``source`` field: ``"read"`` | ``"write"`` | ``"mcp"``.
    Returns newest-first, up to limit items.

    A temporary ``_sort_ts`` key is used for sorting and stripped before return.
    """
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)
    scan_limit = 2000

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

    errors: List[Dict[str, Any]] = []

    for it in reads:
        status = (it.get("status") or "ok").upper()
        if status in ("ERROR", "FAIL", "FAILED"):
            sort_ts = _parse_ts_ms(
                it.get("execution_time") or it.get("recorded_at") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "read", "_sort_ts": sort_ts})

    for it in writes:
        if it.get("error"):
            sort_ts = _parse_ts_ms(
                it.get("recorded_at") or it.get("execution_time") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "write", "_sort_ts": sort_ts})

    for it in mcp_calls:
        status = (it.get("status") or "ok").lower()
        if status in ("error", "fail", "failed"):
            sort_ts = _parse_ts_ms(
                it.get("recorded_at") or it.get("timestamp")
            ) or 0
            errors.append({**it, "source": "mcp", "_sort_ts": sort_ts})

    errors.sort(key=lambda x: x["_sort_ts"], reverse=True)
    for e in errors:
        e.pop("_sort_ts", None)

    return errors[:limit]
