# route: supertable.services.summary
"""
Summary page data aggregation service.

Collects platform-wide metrics from multiple data sources in a single call,
optimized for minimal Redis round trips and zero locking.

Data sources (all read-only):
  - Monitoring lists  (LRANGE × 3)  → ops counts, timeseries buckets
  - Catalog metadata  (SCAN + SMEMBERS) → tables, users, roles, tokens
  - Redis INFO        (INFO command)  → version, keys, hit rate
  - Storage probe     (write/read/delete) → backend, latency
  - DuckDB probe      (in-process)    → version, memory, threads
  - Ingestion index   (SMEMBERS)      → stagings, pipes
  - OData index       (SCAN/SMEMBERS) → endpoint count
  - Quality overview  (hash reads)    → scores, alerts
"""
from __future__ import annotations

import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bucket resolution by time window
# ---------------------------------------------------------------------------

_WINDOW_BUCKETS: List[Tuple[int, int]] = [
    #  (window_hours, bucket_minutes)
    (1,    1),     # 1h  → 60 × 1-min bars
    (6,    5),     # 6h  → 72 × 5-min bars
    (24,   60),    # 24h → 24 × 1-hour bars
    (168,  240),   # 7d  → 42 × 4-hour bars
    (336,  480),   # 14d → 42 × 8-hour bars
    (672,  720),   # 28d → 56 × 12-hour bars
]


def _bucket_minutes_for(window_hours: int) -> int:
    """Select optimal bucket width for a given time window."""
    for wh, bm in _WINDOW_BUCKETS:
        if window_hours <= wh:
            return bm
    return 240


# ---------------------------------------------------------------------------
# Monitoring: single-pass read + aggregate + bucket
# ---------------------------------------------------------------------------

def _parse_ts_ms(value: Any) -> Optional[int]:
    """Parse a timestamp into epoch milliseconds (ISO 8601, epoch-s, epoch-ms)."""
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


def _extract_duration_ms(item: Dict[str, Any]) -> Optional[float]:
    """Extract duration in ms from a monitoring payload (writes or reads)."""
    raw = item.get("duration")
    if raw is not None:
        try:
            return float(raw) * 1000.0
        except (TypeError, ValueError):
            pass
    raw = item.get("duration_ms")
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    timings = item.get("execution_timings")
    if timings is not None:
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


def _read_raw_list(
    redis_client: Any,
    key: str,
    from_ms: int,
    limit: int,
    ts_fields: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    """Read and time-filter a Redis monitoring list. Returns newest-first."""
    items: List[Dict[str, Any]] = []
    fetch_count = limit * 3
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
            item_ts = None
            for field in ts_fields:
                if field in item:
                    item_ts = _parse_ts_ms(item[field])
                    if item_ts is not None:
                        break
            if item_ts is not None and item_ts < from_ms:
                continue
            item["_ts_ms"] = item_ts or 0
            items.append(item)
    except Exception as exc:
        logger.warning("[summary] failed to read Redis list %s: %s", key, exc)
    return items


def _extract_engine(item: Dict[str, Any]) -> Optional[str]:
    """Extract engine name from profile_overview JSON string (fallback for old entries)."""
    raw = item.get("profile_overview")
    if not raw or not isinstance(raw, str):
        return None
    try:
        entries = json.loads(raw)
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict) and "ENGINE" in entry:
                    return str(entry["ENGINE"])
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _compute_ops_and_timeseries(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    window_hours: int,
) -> Dict[str, Any]:
    """
    Single-pass: read the 3 monitoring lists once, produce both
    aggregate counters AND time-bucketed bar chart data.

    Returns {reads, writes, mcp, errors, timeseries, rows_ingested, ...}.
    """
    now_ms = int(time.time() * 1_000)
    from_ms = now_ms - (window_hours * 3_600_000)
    scan_limit = 2_000  # Summary only needs enough to fill buckets, not full detail

    bucket_min = _bucket_minutes_for(window_hours)
    bucket_ms = bucket_min * 60_000
    n_buckets = max(1, (window_hours * 60) // bucket_min)

    # Pre-allocate bucket array (oldest-first)
    buckets = [
        {"ts_ms": from_ms + i * bucket_ms, "reads": 0, "writes": 0, "errors": 0, "mcp": 0, "odata": 0}
        for i in range(n_buckets)
    ]

    def _idx(ts: int) -> int:
        offset = ts - from_ms
        if offset < 0:
            return -1
        i = int(offset // bucket_ms)
        return i if i < n_buckets else n_buckets - 1

    # ── Read all 3 lists from Redis ─────────────────────────────────────
    reads_key = f"monitor:{org}:{sup}:plans"
    writes_key = f"monitor:{org}:{sup}:writes"
    mcp_key = f"monitor:{org}:{sup}:mcp"

    read_items = _read_raw_list(
        redis_client, reads_key, from_ms, scan_limit,
        ts_fields=("execution_time", "recorded_at", "timestamp"),
    )
    write_items = _read_raw_list(
        redis_client, writes_key, from_ms, scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    mcp_items = _read_raw_list(
        redis_client, mcp_key, from_ms, scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )

    # ── Aggregate reads ──────────────────────────────────────────────────
    read_count = len(read_items)
    read_errors = 0
    read_durations: List[float] = []
    engine_counts: Counter = Counter()

    for it in read_items:
        status = str(it.get("status", "ok")).upper()
        if status in ("ERROR", "FAIL", "FAILED"):
            read_errors += 1
        dur = _extract_duration_ms(it)
        if dur is not None and dur >= 0:
            read_durations.append(dur)
        engine = it.get("engine") or _extract_engine(it) or "unknown"
        engine_counts[engine] += 1
        idx = _idx(it["_ts_ms"])
        if idx >= 0:
            buckets[idx]["reads"] += 1
            if status in ("ERROR", "FAIL", "FAILED"):
                buckets[idx]["errors"] += 1

    read_durations.sort()
    avg_read_ms = round(sum(read_durations) / len(read_durations), 1) if read_durations else 0.0

    # ── Aggregate writes ─────────────────────────────────────────────────
    write_count = len(write_items)
    write_errors = 0
    total_inserted = 0
    total_deleted = 0

    for it in write_items:
        if it.get("error"):
            write_errors += 1
        try:
            total_inserted += int(it.get("inserted") or 0)
        except (TypeError, ValueError):
            pass
        try:
            total_deleted += int(it.get("deleted") or 0)
        except (TypeError, ValueError):
            pass
        idx = _idx(it["_ts_ms"])
        if idx >= 0:
            buckets[idx]["writes"] += 1
            if it.get("error"):
                buckets[idx]["errors"] += 1

    # ── Aggregate MCP ────────────────────────────────────────────────────
    mcp_count = len(mcp_items)
    mcp_errors = 0

    for it in mcp_items:
        status = str(it.get("status", "ok")).lower()
        if status in ("error", "fail", "failed"):
            mcp_errors += 1
        idx = _idx(it["_ts_ms"])
        if idx >= 0:
            buckets[idx]["mcp"] += 1
            if status in ("error", "fail", "failed"):
                buckets[idx]["errors"] += 1

    # ── Aggregate OData ──────────────────────────────────────────────────
    odata_key = f"monitor:{org}:{sup}:odata"
    odata_items = _read_raw_list(
        redis_client, odata_key, from_ms, scan_limit,
        ts_fields=("recorded_at", "execution_time", "timestamp"),
    )
    odata_count = len(odata_items)
    odata_errors = 0

    for it in odata_items:
        status = str(it.get("status", "ok")).lower()
        if status in ("error", "fail", "failed"):
            odata_errors += 1
        idx = _idx(it["_ts_ms"])
        if idx >= 0:
            buckets[idx]["odata"] += 1
            if status in ("error", "fail", "failed"):
                buckets[idx]["errors"] += 1

    total_errors = read_errors + write_errors + mcp_errors + odata_errors

    # ── Clean up _ts_ms from items (not leaked to caller) ────────────────
    # (items are not returned, so no cleanup needed)

    return {
        "read_ops": read_count,
        "write_ops": write_count,
        "mcp_calls": mcp_count,
        "odata_calls": odata_count,
        "errors": total_errors,
        "rows_ingested": total_inserted,
        "rows_deleted": total_deleted,
        "avg_read_ms": avg_read_ms,
        "engine_distribution": dict(engine_counts.most_common(10)),
        "timeseries": buckets,
        "bucket_minutes": bucket_min,
    }


# ---------------------------------------------------------------------------
# Infrastructure probes — each individually guarded
# ---------------------------------------------------------------------------

def _probe_redis(redis_client: Any) -> Dict[str, Any]:
    """Redis: version, key count, hit rate. Single pipeline: INFO + DBSIZE."""
    result: Dict[str, Any] = {"status": "fail"}
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.ping()
        pipe.info()
        pipe.dbsize()
        pong, info, db_keys = pipe.execute()

        if not pong:
            return result
        result["status"] = "ok"
        result["version"] = info.get("redis_version", "?")
        result["db_keys"] = db_keys

        hits = int(info.get("keyspace_hits") or 0)
        misses = int(info.get("keyspace_misses") or 0)
        if hits + misses > 0:
            result["hit_rate"] = round(hits / (hits + misses) * 100, 1)
        else:
            result["hit_rate"] = None
    except Exception as exc:
        result["error"] = str(exc)

    return result


def _probe_storage() -> Dict[str, Any]:
    """Storage: backend type, bucket name. Lightweight — no write/read/delete probe."""
    result: Dict[str, Any] = {"status": "fail"}
    try:
        from supertable.storage.storage_factory import get_storage
        storage = get_storage()
        result["backend"] = type(storage).__name__
        result["status"] = "ok"
        try:
            result["bucket"] = getattr(storage, "bucket_name", None)
        except Exception:
            pass
    except Exception as exc:
        result["error"] = str(exc)
    return result


_duckdb_cache: Optional[Dict[str, Any]] = None


def _probe_duckdb() -> Dict[str, Any]:
    """DuckDB: version, memory limit, thread count. Cached after first probe."""
    global _duckdb_cache
    if _duckdb_cache is not None:
        return _duckdb_cache

    result: Dict[str, Any] = {"status": "fail"}
    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        probe = conn.execute("SELECT 1").fetchone()
        if not (probe and probe[0] == 1):
            raise RuntimeError("SELECT 1 failed")
        result["status"] = "ok"

        try:
            row = conn.execute("SELECT version()").fetchone()
            result["version"] = row[0] if row else duckdb.__version__
        except Exception:
            result["version"] = getattr(duckdb, "__version__", "?")

        for setting, key in [
            ("memory_limit", "memory_limit"),
            ("threads", "threads"),
        ]:
            try:
                row = conn.execute(f"SELECT current_setting('{setting}')").fetchone()
                if row and row[0] is not None:
                    result[key] = str(row[0])
            except Exception:
                pass

        conn.close()
        _duckdb_cache = result
    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Ingestion counts
# ---------------------------------------------------------------------------

def _count_ingestion(
    redis_client: Any,
    catalog: Any,
    org: str,
    sup: str,
) -> Dict[str, Any]:
    """Count staging areas and pipes (total + enabled). Never raises."""
    result: Dict[str, Any] = {
        "staging_count": 0,
        "pipe_total": 0,
        "pipe_enabled": 0,
    }

    try:
        from supertable.services.ingestion import _make_redis_helpers
        helpers = _make_redis_helpers(redis_client)
        list_stagings = helpers["redis_list_stagings"]
        list_pipes = helpers["redis_list_pipes"]
        get_pipe_meta = helpers["redis_get_pipe_meta"]

        stagings = list_stagings(org, sup)
        result["staging_count"] = len(stagings)

        total = 0
        enabled = 0
        for stg in stagings:
            pipes = list_pipes(org, sup, stg)
            for pipe_name in pipes:
                total += 1
                meta = get_pipe_meta(org, sup, stg, pipe_name)
                if meta and str(meta.get("enabled", "true")).lower() in ("1", "true", "yes"):
                    enabled += 1
        result["pipe_total"] = total
        result["pipe_enabled"] = enabled
    except Exception as exc:
        logger.warning("[summary] ingestion count failed: %s", exc)

    return result


# ---------------------------------------------------------------------------
# OData endpoint count
# ---------------------------------------------------------------------------

def _count_odata(redis_client: Any, org: str, sup: str) -> int:
    """Count OData sharing endpoints. Never raises."""
    try:
        from supertable.services.security import _list_endpoints
        items = _list_endpoints(redis_client, org, sup)
        return len(items) if items else 0
    except Exception:
        return 0


def _count_linked_shares(redis_client: Any, org: str, sup: str) -> int:
    """Count linked shares (SCARD on index set). Never raises."""
    try:
        key = f"supertable:{org}:{sup}:linked_shares:index"
        return int(redis_client.scard(key) or 0)
    except Exception:
        return 0


def _count_supertables(redis_client: Any, org: str) -> int:
    """Count SuperTables for an organization (SCAN meta:root keys). Never raises."""
    try:
        pattern = f"supertable:{org}:*:meta:root"
        names: set = set()
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=200)
            for k in keys:
                k = k if isinstance(k, str) else k.decode("utf-8")
                parts = k.split(":")
                if len(parts) >= 3:
                    names.add(parts[2])
            if cursor == 0:
                break
        return len(names)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Quality overview
# ---------------------------------------------------------------------------

def _quality_summary(
    redis_client: Any,
    catalog: Any,
    org: str,
    sup: str,
) -> Dict[str, Any]:
    """
    Aggregate quality results matching the quality page's exact algorithm:
    - Per-table status counts (ok / warning / critical)
    - Average quality_score across all checked tables → Health Score
    - Anomaly list for alerts
    Never raises.
    """
    result: Dict[str, Any] = {
        "score": None,
        "tables_checked": 0,
        "tables_ok": 0,
        "tables_warning": 0,
        "tables_critical": 0,
        "total_anomalies": 0,
        "alerts": [],
    }
    try:
        from supertable.services.quality.config import DQConfig
        dqc = DQConfig(redis_client, org, sup)
        all_latest = dqc.get_all_latest()
        if not isinstance(all_latest, dict) or not all_latest:
            return result

        scores: List[int] = []
        for table_name, table_data in all_latest.items():
            if not isinstance(table_data, dict):
                continue
            result["tables_checked"] += 1

            status = str(table_data.get("status", "ok")).lower()
            if status == "ok":
                result["tables_ok"] += 1
            elif status == "warning":
                result["tables_warning"] += 1
            elif status in ("critical", "error", "fail"):
                result["tables_critical"] += 1

            qs = table_data.get("quality_score")
            if qs is not None:
                scores.append(int(qs))

            for anomaly in (table_data.get("anomalies") or []):
                if not isinstance(anomaly, dict):
                    continue
                result["total_anomalies"] += 1
                result["alerts"].append({
                    "table": table_name,
                    "check": anomaly.get("check_id", ""),
                    "name": anomaly.get("name", anomaly.get("check_id", "")),
                    "column": anomaly.get("column", ""),
                    "severity": anomaly.get("severity", "warning"),
                })

        if scores:
            result["score"] = round(sum(scores) / len(scores))

    except Exception as exc:
        logger.warning("[summary] quality overview failed: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Public: compute_summary — the single call the API endpoint invokes
# ---------------------------------------------------------------------------

def compute_summary(
    redis_client: Any,
    catalog: Any,
    org: str,
    sup: str,
    *,
    window_hours: int = 1,
) -> Dict[str, Any]:
    """
    Aggregate all summary page data in a single call.

    Zero locks. All data sources are read-only. Each probe is individually
    guarded — a failure in one section never blocks the others.

    Performance profile (typical):
      - 3× LRANGE for monitoring lists        ~2-5ms
      - SCAN + SMEMBERS for catalog stats      ~1-3ms
      - Redis INFO + DBSIZE                    ~1ms
      - Storage write+read+delete probe        ~20-40ms
      - DuckDB in-process probe                ~1ms
      - Ingestion SMEMBERS per staging          ~1-2ms
      - Quality hash reads                     ~1-3ms
      ─────────────────────────────────────────
      Total: ~30-60ms typical
    """
    window_hours = max(1, min(window_hours, 672))  # clamp 1h – 28d

    # ── 1. Monitoring: ops aggregates + timeseries (single pass) ────────
    ops: Dict[str, Any] = {}
    try:
        ops = _compute_ops_and_timeseries(
            redis_client, org, sup, window_hours=window_hours,
        )
    except Exception as exc:
        logger.warning("[summary] monitoring aggregation failed: %s", exc)
        ops = {
            "read_ops": 0, "write_ops": 0, "mcp_calls": 0, "odata_calls": 0, "errors": 0,
            "rows_ingested": 0, "rows_deleted": 0, "avg_read_ms": 0,
            "engine_distribution": {}, "timeseries": [], "bucket_minutes": 1,
        }

    # ── 2. Catalog stats (tables, users, roles, tokens) ─────────────────
    cat: Dict[str, Any] = {}
    try:
        from supertable.services.monitoring import compute_catalog_stats
        cat = compute_catalog_stats(redis_client, catalog, org, sup)
    except Exception as exc:
        logger.warning("[summary] catalog stats failed: %s", exc)

    # ── 3. Ingestion counts ─────────────────────────────────────────────
    ingestion = _count_ingestion(redis_client, catalog, org, sup)
    ingestion["odata_endpoints"] = _count_odata(redis_client, org, sup)
    ingestion["linked_shares"] = _count_linked_shares(redis_client, org, sup)

    # ── 4. SuperTable count (org-wide) ───────────────────────────────────
    supertable_count = _count_supertables(redis_client, org)

    # ── 5. Quality overview ─────────────────────────────────────────────
    quality = _quality_summary(redis_client, catalog, org, sup)

    # ── Assemble response ───────────────────────────────────────────────
    return {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": window_hours,

        # Operations (time-bound, responds to window_hours)
        "operations": {
            "read_ops": ops.get("read_ops", 0),
            "write_ops": ops.get("write_ops", 0),
            "mcp_calls": ops.get("mcp_calls", 0),
            "odata_calls": ops.get("odata_calls", 0),
            "rows_ingested": ops.get("rows_ingested", 0),
            "rows_deleted": ops.get("rows_deleted", 0),
            "errors": ops.get("errors", 0),
        },

        # Platform state (point-in-time)
        "platform": {
            "supertable_count": supertable_count,
            "table_count": cat.get("table_count", 0),
            "total_snapshots": cat.get("total_snapshots", 0),
            "total_rows": cat.get("total_rows", 0),
            "total_size_bytes": cat.get("total_size_bytes", 0),
            "user_count": cat.get("user_count", 0),
            "role_count": cat.get("role_count", 0),
            "token_count": cat.get("token_count", 0),
        },

        # Timeseries for line chart
        "timeseries": ops.get("timeseries", []),
        "bucket_minutes": ops.get("bucket_minutes", 1),

        # Engine distribution
        "engine_distribution": ops.get("engine_distribution", {}),

        # Ingestion
        "ingestion": ingestion,

        # Quality — matches quality page's algorithm
        "quality": {
            "score": quality["score"],
            "tables_checked": quality["tables_checked"],
            "tables_ok": quality["tables_ok"],
            "tables_warning": quality["tables_warning"],
            "tables_critical": quality["tables_critical"],
            "total_anomalies": quality["total_anomalies"],
            "alerts": quality["alerts"][:10],
        },
    }
