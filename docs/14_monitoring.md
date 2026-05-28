# 14. Monitoring

## Overview

SuperTable records read, write, and tool-call metrics as JSON payloads in
Redis lists. The `MonitoringWriter` component pushes payloads via `RPUSH`;
downstream consumers (dashboards, log shippers) read from the tail.

A structured JSON logging layer (`supertable.logging`) runs alongside the
monitoring writer, providing correlation-ID-based request tracing.

## Monitored Operations

**As of SDK 2.2.0** monitoring lives at the **org level** — one list per
`(org, monitor_type)` — *not* per supertable. A cross-supertable query
(e.g. a join across `sales` and `customers`) writes exactly one entry
whose payload carries `supertables: [...]` for per-supertable attribution
at read time. The full key is built by `redis_keys.monitor(org, monitor_type)`:

```
supertable:{org}:monitor:{monitor_type}
```

| Category | `monitor_type` | Source | Key Fields |
|----------|----------------|--------|------------|
| **Reads** | `plans`  | Query execution                 | query_id, engine, table_name, result_rows, execution_timings, status, supertables |
| **Writes** | `writes` | Data ingestion                  | query_id, table_name, incoming_rows, inserted, deleted, duration, supertables |
| **MCP** | `mcp`    | MCP tool invocations            | tool, organization, rows, duration_ms, supertables |
| **OData** | `odata`  | OData reads                     | endpoint_id, entity_set, rows_returned, role_name, supertables |
| **Errors** | `errors` | Any handler that caught         | service, action, error_code, message, supertables |
| **Locks** | `locks`  | Redis lock acquisitions / releases | lock_key, action, holder, supertables |

The category is selected via the `monitor_type` argument when constructing the
writer. The closed-set of valid `monitor_type` values is enforced inside
`redis_keys.monitor(...)`; any other value raises at key-build time.

## Writing Metrics

`monitoring_writer.py` exposes two entry points:

```python
from supertable.monitoring_writer import (
    MonitoringWriter, get_monitoring_logger,
)

# 1. Context-managed (auto-flush on exit)
with MonitoringWriter(super_name, organization, monitor_type="writes") as mw:
    mw.log_metric({
        "query_id": "abc",
        "table_name": "facts",
        "duration": 0.42,
        "inserted": 100,
        "deleted": 0,
    })

# 2. Long-lived singleton (background flush thread)
mw = get_monitoring_logger(
    super_name=super_name,
    organization=organization,
    monitor_type="metrics",
)
mw.log_metric({"query_id": "q1", "rows_read": 1234})
mw.request_flush()
```

`get_monitoring_logger` returns a thread-safe singleton per `(super, org,
monitor_type)` triple. Payloads are buffered and flushed in batches. The
writer captures back-pressure stats (`queue.qsize()`, `current_batch`,
`queue_stats`) which can be inspected for sizing decisions.

## Duration Conventions

Different categories use different duration shapes:

| Source | Field | Format |
|--------|-------|--------|
| Writes | `duration` | Seconds as float |
| Stats / Metrics | `duration_ms` | Milliseconds directly |
| Reads | `execution_timings` | Dict with `total_s` (seconds) per stage |

Consumers should normalize on milliseconds.

## Structured JSON Logging

### Configuration

```python
from supertable.logging import configure_logging, RequestLoggingMiddleware

configure_logging(service="api")          # or service="ui"
app.add_middleware(RequestLoggingMiddleware, service="api")
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SUPERTABLE_LOG_LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |
| `SUPERTABLE_LOG_FORMAT` | `json` | `json` or `text` |
| `SUPERTABLE_LOG_FILE` | `{SUPERTABLE_HOME}/log/st.log` | Log file path (`none` to disable) |
| `SUPERTABLE_CORRELATION_HEADER` | `X-Correlation-ID` | Header name for correlation IDs |

### JSON Format

Each log record is emitted as a single JSON line with fields:

| Field | Description |
|-------|-------------|
| `timestamp` | ISO 8601 UTC |
| `level` | DEBUG / INFO / WARNING / ERROR / CRITICAL |
| `service` | `api` / `ui` / caller-defined |
| `logger` | Python logger name |
| `message` | Log message |
| `event` | Structured event type (`request`, `proxy_error`, `startup`) |
| `correlation_id` | Request correlation ID |
| `method` | HTTP method |
| `path` | Request path |
| `status` | HTTP status code |
| `duration_ms` | Request duration in milliseconds |
| `client_ip` | Client IP address |

Security: `authorization`, `cookie`, `set-cookie`, `x-api-key`, and
`x-auth-token` headers are always redacted.

Noise reduction: static paths (`/static/`, `/favicon.ico`, `/healthz`) are
excluded from request logging.

### Text Format (Development)

Compact single-line format with color coding:

```
14:43:07.229  INFO  [api]  GET /reflection/supers -> 200  35.2ms  cid=6488eafe78f9
14:43:11.422  WARN  [ui]   GET /reflection/compute/list -> 404  12.8ms
```

Auto-disabled when not a TTY or when `NO_COLOR` / `SUPERTABLE_LOG_COLOR=0` is
set.

### Correlation IDs

Correlation IDs propagate across HTTP hops. The middleware:

1. Reads the correlation ID from the configured header.
2. Generates a new UUID if missing.
3. Passes it to downstream services via the same header.
4. Includes it in every log record for end-to-end request tracing.

### Log Analysis Examples

```bash
# All requests slower than 500ms
cat ~/supertable/log/st.log | jq 'select(.duration_ms > 500)'

# All 5xx responses
cat ~/supertable/log/st.log | jq 'select(.status >= 500)'

# Trace a single request
cat ~/supertable/log/st.log | jq 'select(.correlation_id == "abc123")'
```

## Source Files

- `supertable/monitoring_writer.py` — `MonitoringWriter` class, the
  `get_monitoring_logger` singleton factory, batched flushing, plus
  the `NullMonitoringLogger` no-op fallback.
- `supertable/logging.py` — JSON / text formatters,
  `configure_logging`, `RequestLoggingMiddleware`, correlation propagation.
- `supertable/redis_keys.py` — `monitor(org, monitor_type)` and
  `registry(org, service_type, host, pid)` key builders. The SDK
  owns the **key shape** only; the actual heartbeat writer lives in
  `dataisland-core/services/common/service_registry.py` (core
  services) or in `lighthouse/bootstrap.py` (REST-mediated for
  Lighthouse).
