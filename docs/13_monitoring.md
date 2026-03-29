# Data Island Core — Monitoring

## Overview

The monitoring subsystem captures metrics for every read and write operation — query timing, row counts, engine selection, data sizes, and error rates. Metrics are collected asynchronously via a background worker thread (same pattern as the audit logger) and stored in Redis lists keyed by organization and SuperTable. The WebUI's Monitoring page and the monitoring API endpoints read from these lists.

Monitoring is separate from audit logging. The audit log captures security-relevant events for compliance. Monitoring captures performance metrics for operations teams.

---

## How it works

### Write path

After every `DataWriter.write()` completes and the lock is released, a metrics payload is enqueued to the `MonitoringWriter`:

```python
monitoring_logger.log_metric({
    "query_id": qid,
    "recorded_at": "2025-03-29T14:00:00Z",
    "organization": "acme",
    "super_name": "example",
    "table_name": "orders",
    "role_name": "admin",
    "incoming_rows": 5000,
    "inserted": 5000,
    "deleted": 0,
    "duration": 1.234,
    "overwrite_columns": ["order_id"],
    "lineage": {"source_type": "api_upload"},
    ...
})
```

### Read path

After every `DataReader.execute()` completes, the execution plan (containing timing, engine used, row count, and status) is available via `QueryPlanManager`. The API stores this in monitoring.

### Storage in Redis

Metrics are stored as JSON strings in Redis lists:

```
supertable:{org}:{sup}:monitoring:reads   — read operation metrics
supertable:{org}:{sup}:monitoring:writes  — write operation metrics
```

Lists are bounded — oldest entries are trimmed when the list exceeds a configurable maximum.

### MonitoringWriter architecture

`MonitoringLogger` follows the same queue + background worker pattern as the audit logger:

1. `log_metric(payload)` is non-blocking — it enqueues the payload into an in-memory queue
2. A background daemon thread drains the queue and writes to Redis via `RPUSH`
3. If monitoring is disabled (`SUPERTABLE_MONITORING_ENABLED=false`), a `NullMonitoringLogger` is used (zero overhead, all calls are no-ops)

The logger is cached per organization/SuperTable pair via an LRU cache (bounded by `SUPERTABLE_MONITOR_CACHE_MAX`, default: 256).

---

## Structured logging

`supertable/logging.py` provides structured JSON logging and request tracing:

**JSON log format** — When `SUPERTABLE_LOG_FORMAT=json`, all log entries are JSON objects with timestamp, level, logger name, message, and correlation ID. This format is parseable by log aggregation tools (ELK, Datadog, Splunk).

**RequestLoggingMiddleware** — FastAPI middleware that logs every HTTP request with method, path, status code, duration, and correlation ID. Excludes health checks and static assets.

**Correlation ID propagation** — Each request receives a correlation ID from the `X-Correlation-ID` header (or auto-generated). The ID is available in all log entries and audit events for request tracing.

---

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `SUPERTABLE_MONITORING_ENABLED` | `true` | Enable metrics collection |
| `SUPERTABLE_MONITOR_CACHE_MAX` | `256` | Max cached monitoring writer instances |
| `SUPERTABLE_LOG_LEVEL` | `INFO` | Log level |
| `SUPERTABLE_LOG_FORMAT` | `json` | Log format: `json` or `text` |
| `SUPERTABLE_LOG_FILE` | (auto) | Log file path (default: `{SUPERTABLE_HOME}/log/st.log`) |
| `SUPERTABLE_CORRELATION_HEADER` | `X-Correlation-ID` | Header for correlation ID |

---

## API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/reflection/monitoring/reads` | Recent read operation metrics |
| `GET` | `/reflection/monitoring/writes` | Recent write operation metrics |

---

## Module structure

```
supertable/
  monitoring_writer.py       MonitoringLogger, NullMonitoringLogger, queue + worker (537 lines)
  logging.py                 Structured JSON logging, RequestLoggingMiddleware (436 lines)
  services/monitoring.py     Helper functions for monitoring API endpoints (144 lines)
```

---

## Frequently asked questions

**How long are monitoring metrics retained?**
Metrics are stored in Redis lists with bounded length. Old entries are trimmed automatically. The retention is governed by the list max length (not time-based).

**Can I disable monitoring to save Redis memory?**
Yes. Set `SUPERTABLE_MONITORING_ENABLED=false`. A `NullMonitoringLogger` is used — zero overhead, no Redis writes. Audit logging continues independently.

**What is the performance impact of monitoring?**
Negligible. `log_metric()` enqueues and returns in microseconds. The background worker writes to Redis asynchronously. Monitoring never blocks the request path.
