# Data Island Core — Monitoring V2 Design

## Status: Design Proposal
## Author: Principal Engineer
## Date: 2026-03-30

---

## 1. Problem Statement

The current monitoring subsystem has **two instrumentation points** covering only data reads and writes. This leaves operations teams blind to MCP/agentic traffic, ingestion pipeline health, lock contention, storage latency, data quality execution, and RBAC change velocity. There is also a live bug where the writes monitoring page reads from an empty Redis key.

### 1.1 Bug: Write Monitoring Key Mismatch

`DataWriter` writes metrics to `monitor_type="stats"` (Redis key: `monitor:{org}:{sup}:stats`).
The API endpoint `/reflection/monitoring/writes` reads from `monitor_type="writes"` (Redis key: `monitor:{org}:{sup}:writes`).

**Result:** The writes tab on the Monitoring page is always empty.

**Fix:** Change `DataWriter` to use `monitor_type="writes"` (matching the API), and add a one-time migration to copy any existing `stats` data.

### 1.2 Coverage Gaps

| Area | Current coverage | Operational impact of gap |
|------|-----------------|--------------------------|
| Data reads | ✅ `plan_extender.py` → `monitor:plans` | — |
| Data writes | ⚠️ `data_writer.py` → `monitor:stats` (broken key) | Writes page shows nothing |
| MCP tool calls | ❌ Only Python logging, no Redis metrics | Can't measure agentic BI adoption, latency, error rate |
| File uploads | ❌ Audited but not metricked | Can't track ingestion throughput or failure rate |
| Lock contention | ❌ DEBUG-level logging only | Can't detect hot tables or lock storms |
| Storage I/O | ❌ Nothing | Can't detect S3/MinIO latency degradation |
| Data quality | ❌ Nothing | Quality checks run in the dark |
| RBAC changes | ❌ Audit-only | Can't detect misconfiguration storms |
| Query engine routing | ⚠️ Partial (in read plan stats) | Can't compare engine performance at aggregate level |

---

## 2. Design Principles

1. **Backward compatible.** Existing `MonitoringWriter` API and Redis key structure unchanged (except the bug fix). New metric types are additive.
2. **Never block the request path.** All metric emission is non-blocking, enqueue-only. Failures are swallowed with a warning log. Same safety model as today.
3. **Minimal diff per tier.** Each tier is independently deployable and adds value on its own.
4. **Common schema, flexible dimensions.** All metrics share a base shape. Domain-specific fields go into a typed `dimensions` dict, not top-level keys.
5. **Aggregate in Redis, not in Python.** Rolling counters via `INCRBY`/`HSET` for real-time dashboards. Raw event lists for drill-down.

---

## 3. Tier 1 — Fix + Low-Hanging Fruit

### 3.1 Fix the Write Key Mismatch

**File:** `supertable/data_writer.py`

Change `monitor_type="stats"` to `monitor_type="writes"` (two locations, lines ~507).

**File:** `supertable/api/api.py`

No change needed — already reads from `"writes"`.

**Migration:** Add a one-time key rename in `MonitoringWriter` init or a startup hook: `RENAME monitor:{org}:{sup}:stats monitor:{org}:{sup}:writes` (best-effort, skip if source key absent).

### 3.2 MCP Tool Metrics

**File:** `supertable/mcp/mcp_server.py`

Enhance the existing `log_tool` decorator to emit a monitoring metric after each tool call:

```python
# Inside log_tool wrapper, after successful execution:
try:
    from supertable.monitoring_writer import MonitoringWriter
    org = kwargs.get("organization", "")
    sup = kwargs.get("super_name", "")
    if org and sup:
        with MonitoringWriter(
            organization=org, super_name=sup, monitor_type="mcp",
        ) as mon:
            mon.log_metric({
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "tool": fn,
                "status": "ok" if not isinstance(out, dict) or out.get("status") != "ERROR" else "error",
                "duration_ms": round(dt, 2),
                "role": kwargs.get("role", ""),
                "result_rows": _extract_row_count(out),
            })
except Exception:
    pass  # Never fail a tool call due to monitoring
```

**File:** `supertable/api/api.py`

Add a new endpoint:

```python
@router.get("/reflection/monitoring/mcp")
def monitoring_mcp(
    org: str = Query(""), sup: str = Query(""),
    from_ts: Optional[int] = Query(None), to_ts: Optional[int] = Query(None),
    limit: int = Query(500),
    _=Depends(logged_in_guard_api),
):
    ...
    items = _read_monitoring_list(
        redis_client, org, sup,
        monitor_type="mcp", from_ts_ms=from_ts, to_ts_ms=to_ts, limit=limit,
    )
    return JSONResponse({"ok": True, "items": items})
```

### 3.3 Ingestion Upload Metrics

**File:** `supertable/api/api.py` — in `api_ingestion_load_upload()`

After the existing audit call, add a monitoring metric:

```python
try:
    with MonitoringWriter(
        organization=org_eff, super_name=sup_eff, monitor_type="writes",
    ) as mon:
        mon.log_metric({
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "source": "upload",
            "table_name": table_eff if mode_eff == "table" else "",
            "staging_name": stg_eff if mode_eff == "staging" else "",
            "file_name": filename,
            "file_type": ext,
            "file_size": file.size or 0,
            "incoming_rows": getattr(arrow_table, "num_rows", 0),
            "inserted": inserted if mode_eff == "table" else 0,
            "deleted": deleted if mode_eff == "table" else 0,
            "duration": round(dt_ms / 1000, 6),
            "mode": mode_eff,
            "role_name": role_eff,
            "username": (sess or {}).get("username", ""),
        })
except Exception:
    pass
```

This means upload-based writes also appear in the Writes tab alongside DataWriter writes.

### 3.4 Summary Endpoint

**File:** `supertable/services/monitoring.py`

Add a function that computes aggregate stats from existing Redis lists:

```python
def compute_monitoring_summary(
    redis_client: Any, org: str, sup: str, *, window_hours: int = 24,
) -> Dict[str, Any]:
    """Compute aggregate metrics from the last N hours of monitoring data."""
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (window_hours * 3600 * 1000)

    reads = _read_monitoring_list(redis_client, org, sup, monitor_type="plans", from_ts_ms=from_ms, limit=10000)
    writes = _read_monitoring_list(redis_client, org, sup, monitor_type="writes", from_ts_ms=from_ms, limit=10000)
    mcp_calls = _read_monitoring_list(redis_client, org, sup, monitor_type="mcp", from_ts_ms=from_ms, limit=10000)

    return {
        "window_hours": window_hours,
        "reads": _summarize_ops(reads, duration_field="execution_timings"),
        "writes": _summarize_ops(writes, duration_field="duration"),
        "mcp": _summarize_mcp(mcp_calls),
    }
```

**File:** `supertable/api/api.py`

```python
@router.get("/reflection/monitoring/summary")
def monitoring_summary(
    org: str = Query(""), sup: str = Query(""),
    window_hours: int = Query(24),
    _=Depends(logged_in_guard_api),
):
    ...
```

Returns:

```json
{
  "ok": true,
  "summary": {
    "window_hours": 24,
    "reads": {
      "count": 1234, "errors": 5,
      "total_rows": 567890,
      "p50_ms": 45.2, "p95_ms": 320.1, "p99_ms": 890.5,
      "by_engine": {"duckdb_lite": 1100, "duckdb_pro": 134},
      "by_table": {"orders": 500, "customers": 400, "...": "..."}
    },
    "writes": {
      "count": 89, "errors": 1,
      "total_inserted": 45000, "total_deleted": 200,
      "p50_ms": 120.0, "p95_ms": 550.0,
      "by_table": {"orders": 50, "events": 39}
    },
    "mcp": {
      "count": 340, "errors": 12,
      "p50_ms": 80.0, "p95_ms": 400.0,
      "by_tool": {"query_sql": 200, "list_tables": 80, "describe_table": 60}
    }
  }
}
```

### 3.5 Tier 1 File Change Summary

| File | Change |
|------|--------|
| `supertable/data_writer.py` | `monitor_type="stats"` → `"writes"` (2 locations) |
| `supertable/mcp/mcp_server.py` | Enhance `log_tool` to emit `monitor_type="mcp"` metrics |
| `supertable/api/api.py` | Add `/reflection/monitoring/mcp` + `/reflection/monitoring/summary` endpoints |
| `supertable/services/monitoring.py` | Add `compute_monitoring_summary()` + `_summarize_ops()` + `_summarize_mcp()` |

Total: 4 files changed. ~150 lines added. Zero existing behavior changed (beyond the bug fix).

---

## 4. Tier 2 — Structured Metrics Layer

### 4.1 MetricEvent Dataclass

**New file:** `supertable/monitoring_event.py`

A lightweight, standardized schema that all metric sources share:

```python
@dataclass
class MetricEvent:
    """Canonical metric payload for all monitored operations."""
    timestamp_iso: str          # ISO 8601 UTC
    category: str               # "read" | "write" | "mcp" | "ingestion" | "lock" | "storage" | "quality" | "rbac"
    operation: str              # Specific operation: "query_sql", "data_write", "acquire_lock", etc.
    organization: str
    super_name: str
    status: str                 # "ok" | "error" | "timeout"
    duration_ms: float          # Wall-clock duration in milliseconds
    dimensions: Dict[str, str]  # Domain-specific string dimensions (table, role, engine, tool, ...)
    counters: Dict[str, int]    # Domain-specific numeric counters (rows, bytes, files, ...)
    error: Optional[str]        # Error message if status != "ok", truncated to 500 chars

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "recorded_at": self.timestamp_iso,
            "category": self.category,
            "operation": self.operation,
            "organization": self.organization,
            "super_name": self.super_name,
            "status": self.status,
            "duration_ms": self.duration_ms,
        }
        # Flatten dimensions and counters into top level for backward compat
        d.update(self.dimensions)
        d.update(self.counters)
        if self.error:
            d["error"] = self.error
        return d
```

### 4.2 Monitor Type Taxonomy

| `monitor_type` | Redis key suffix | Category | Emitted by |
|---------------|-----------------|----------|-----------|
| `plans` | `monitor:{o}:{s}:plans` | `read` | `plan_extender.py` (existing) |
| `writes` | `monitor:{o}:{s}:writes` | `write` | `data_writer.py` (fixed), upload endpoint |
| `mcp` | `monitor:{o}:{s}:mcp` | `mcp` | `mcp_server.py` `log_tool` decorator |
| `locks` | `monitor:{o}:{s}:locks` | `lock` | `redis_catalog.acquire_simple_lock()` |
| `storage` | `monitor:{o}:{s}:storage` | `storage` | Storage interface wrapper |
| `quality` | `monitor:{o}:{s}:quality` | `quality` | `services/quality/scheduler.py` |

### 4.3 Lock Contention Metrics

**File:** `supertable/redis_catalog.py` — in `acquire_simple_lock()` and `release_simple_lock()`

After acquiring or timing out on a lock, emit:

```python
# dimensions: table_name, outcome ("acquired" | "timeout" | "released")
# counters: wait_ms (time spent waiting), hold_ms (for release)
```

This enables a "Hot Tables" dashboard: tables sorted by lock wait time.

### 4.4 Storage I/O Metrics

**File:** `supertable/storage/storage_interface.py` or a new `storage/monitored_storage.py` decorator

Wrap the `StorageInterface` methods to time each call:

```python
# dimensions: backend ("s3" | "minio" | "local" | ...), operation ("read_parquet" | "write_bytes" | ...)
# counters: bytes_transferred, file_count
```

This requires a decorator/proxy pattern rather than modifying each backend, to keep the diff minimal.

### 4.5 Data Quality Metrics

**File:** `supertable/services/quality/scheduler.py`

After each quality check run completes, emit:

```python
# dimensions: table_name, check_id, check_group ("quick" | "deep")
# counters: checks_run, checks_passed, checks_failed, anomalies_detected
# duration_ms: total check execution time
```

### 4.6 Rolling Aggregation in Redis

Instead of scanning raw lists on every summary request, maintain rolling counters:

```
monitor:{org}:{sup}:agg:{category}:{minute_bucket}  → HASH
  count:ok        → int
  count:error     → int
  duration_sum_ms → int
  rows_sum        → int
```

Minute buckets are keyed by `YYYYMMDD-HHMM`. Each bucket gets a TTL of 48 hours (auto-cleanup, no manual trimming). The summary endpoint reads the last N buckets and sums them — O(N) where N is the number of minutes in the window, not the number of events.

**Implementation:** Add an `_update_aggregates()` call inside `_AsyncMonitoringLogger._ship_batch()` after the raw RPUSH. This is a single `HINCRBY` pipeline — negligible overhead.

### 4.7 Tier 2 File Change Summary

| File | Change |
|------|--------|
| `supertable/monitoring_event.py` | **New.** `MetricEvent` dataclass |
| `supertable/monitoring_writer.py` | Add `_update_aggregates()` in `_ship_batch()` |
| `supertable/redis_catalog.py` | Emit lock metrics in `acquire_simple_lock()` / `release_simple_lock()` |
| `supertable/storage/storage_interface.py` | Add timing decorator for storage ops |
| `supertable/services/quality/scheduler.py` | Emit quality check metrics |
| `supertable/services/monitoring.py` | Read from aggregate hashes for summary endpoint |
| `supertable/config/settings.py` | Add `SUPERTABLE_MONITOR_AGG_TTL_HOURS` (default: 48) |

Total: 7 files (1 new, 6 modified). ~300 lines added.

---

## 5. Tier 3 — Observability Platform Readiness

### 5.1 Prometheus `/metrics` Endpoint

**New file:** `supertable/services/prometheus.py`

Expose standard Prometheus metrics at `/reflection/metrics`:

```
# HELP supertable_reads_total Total read operations
# TYPE supertable_reads_total counter
supertable_reads_total{org="acme",super="example",engine="duckdb_lite"} 12345

# HELP supertable_read_duration_seconds Read operation duration
# TYPE supertable_read_duration_seconds histogram
supertable_read_duration_seconds_bucket{le="0.1"} 8000
supertable_read_duration_seconds_bucket{le="0.5"} 11000
...

# HELP supertable_writes_total Total write operations
# TYPE supertable_writes_total counter

# HELP supertable_mcp_calls_total MCP tool invocations
# TYPE supertable_mcp_calls_total counter

# HELP supertable_lock_wait_seconds Time waiting to acquire table lock
# TYPE supertable_lock_wait_seconds histogram

# HELP supertable_storage_operation_seconds Storage backend operation duration
# TYPE supertable_storage_operation_seconds histogram
```

Use the `prometheus_client` library. Metrics are collected from the same `MetricEvent` path — the Prometheus exporter subscribes to the monitoring queue alongside the Redis writer.

**Configuration:** `SUPERTABLE_PROMETHEUS_ENABLED` (default: `false`), `SUPERTABLE_PROMETHEUS_PATH` (default: `/reflection/metrics`).

### 5.2 OpenTelemetry Trace Spans

Add optional trace context propagation for critical paths:

```
[write request]
  └─ span: data_writer.write
       ├─ span: lock.acquire (lock wait time)
       ├─ span: snapshot.read
       ├─ span: overlap.detect
       ├─ span: process.write_files
       ├─ span: catalog.update_pointer
       ├─ span: mirror.sync
       └─ span: monitor.emit
```

Enabled via `SUPERTABLE_OTEL_ENABLED` (default: `false`). Uses `opentelemetry-api` (trace-only, no auto-instrumentation bloat). Trace IDs propagated via the existing correlation ID header.

### 5.3 Alerting Webhooks

Extend the existing `SUPERTABLE_AUDIT_ALERT_WEBHOOK` pattern to monitoring:

```
SUPERTABLE_MONITOR_ALERT_WEBHOOK=https://hooks.slack.com/...
SUPERTABLE_MONITOR_ALERT_RULES=lock_timeout:5m:3,error_rate:1m:0.1,p95_latency:5m:2000
```

Rule format: `metric:window:threshold`. When the threshold is exceeded in the window, fire a webhook POST with the metric details. Evaluation runs on the existing background worker thread (no new threads).

### 5.4 Tier 3 File Change Summary

| File | Change |
|------|--------|
| `supertable/services/prometheus.py` | **New.** Prometheus exporter |
| `supertable/services/tracing.py` | **New.** OpenTelemetry span helpers |
| `supertable/services/alerting.py` | **New.** Webhook alerting engine |
| `supertable/config/settings.py` | Add Prometheus, OTEL, alerting settings |
| `supertable/api/api.py` | Register `/reflection/metrics` endpoint |
| `supertable/monitoring_writer.py` | Add alert evaluation hook in worker loop |

---

## 6. WebUI Enhancements

### 6.1 Monitoring Page — MCP Tab (Tier 1)

Add a third section node (alongside Reads / Writes) for **MCP** in `monitoring.html`:

- Left panel: list of recent MCP tool calls, filterable by tool name
- Center panel: tool call detail (tool name, role, duration, result rows, error)
- Right panel: aggregate stats (total calls, error rate, top tools by frequency, p50/p95 latency)

### 6.2 Monitoring Dashboard (Tier 2)

Add a new page `/reflection/monitoring/dashboard` with:

- Time-series sparklines for reads/writes/MCP over the last 24h (from aggregate hashes)
- Hot tables panel (highest lock contention)
- Storage latency panel (p50/p95 by backend)
- Quality health panel (checks passed/failed/anomalies)

### 6.3 Sidebar Entry

Add to `NAV_ITEMS` in `sidebar.html` (already exists for Monitoring; the dashboard would be a sub-page or replace the current page).

---

## 7. Implementation Order

```
Tier 1a — Bug fix (data_writer.py monitor_type)          → 1 hour
Tier 1b — MCP tool metrics (mcp_server.py)                → 2 hours
Tier 1c — Upload metrics + summary endpoint               → 2 hours
Tier 1d — MCP tab in monitoring.html                      → 3 hours
                                                    Total → ~1 day

Tier 2a — MetricEvent dataclass                           → 1 hour
Tier 2b — Rolling aggregation in Redis                    → 3 hours
Tier 2c — Lock contention metrics                         → 2 hours
Tier 2d — Storage I/O metrics                             → 2 hours
Tier 2e — Quality check metrics                           → 2 hours
Tier 2f — Dashboard page                                  → 4 hours
                                                    Total → ~2 days

Tier 3a — Prometheus exporter                             → 4 hours
Tier 3b — OpenTelemetry spans                             → 4 hours
Tier 3c — Alerting webhooks                               → 4 hours
                                                    Total → ~2 days
```

---

## 8. Configuration Summary (All Tiers)

| Variable | Type | Default | Tier | Description |
|----------|------|---------|------|-------------|
| `SUPERTABLE_MONITORING_ENABLED` | bool | `true` | existing | Master switch |
| `SUPERTABLE_MONITOR_CACHE_MAX` | int | `256` | existing | Max cached writer instances |
| `SUPERTABLE_MONITOR_AGG_TTL_HOURS` | int | `48` | 2 | Rolling aggregate bucket TTL |
| `SUPERTABLE_MONITOR_MCP_ENABLED` | bool | `true` | 1 | MCP tool metrics |
| `SUPERTABLE_MONITOR_LOCKS_ENABLED` | bool | `true` | 2 | Lock contention metrics |
| `SUPERTABLE_MONITOR_STORAGE_ENABLED` | bool | `false` | 2 | Storage I/O metrics (high volume) |
| `SUPERTABLE_PROMETHEUS_ENABLED` | bool | `false` | 3 | Prometheus endpoint |
| `SUPERTABLE_OTEL_ENABLED` | bool | `false` | 3 | OpenTelemetry tracing |
| `SUPERTABLE_MONITOR_ALERT_WEBHOOK` | str | `""` | 3 | Alerting webhook URL |
| `SUPERTABLE_MONITOR_ALERT_RULES` | str | `""` | 3 | Alerting rules |

---

## 9. Security Considerations

- Monitoring endpoints use existing auth guards (`logged_in_guard_api`). No new auth surface.
- Metric payloads must never contain: SQL query text (use query_hash), credentials, PII, or file contents.
- MCP metrics log tool name and duration, not the agent's prompt or response.
- Prometheus endpoint, if enabled, should be behind the same auth or network-level restriction.
- Storage metrics log operation name and duration, not file paths (which could leak table names to unauthorized metrics consumers).

## 10. Performance Impact

- Tier 1: Negligible. Same `log_metric()` enqueue pattern (~microseconds). Summary endpoint scans Redis lists — bounded by `limit` param.
- Tier 2: Rolling aggregation adds one `HINCRBY` pipeline per batch ship (batched, not per-event). Lock/storage metrics are per-operation but enqueue-only.
- Tier 3: Prometheus scrape reads from in-memory counters (sub-millisecond). OTEL spans add ~5µs per span creation. Alerting runs on existing background thread.

## 11. Failure Modes

All monitoring is non-blocking, fire-and-forget:

| Failure | Behavior |
|---------|----------|
| Redis unavailable | Metrics dropped, warning logged. Business operations unaffected. |
| Queue full (10K items) | New metrics dropped. `total_dropped` counter incremented. |
| Background worker crash | Daemon thread — restarts on next `get_monitoring_logger()` call. |
| Aggregate hash write fails | Raw events still stored in lists. Summary endpoint falls back to list scan. |
| Prometheus scrape timeout | Standard Prometheus retry. No data loss. |
