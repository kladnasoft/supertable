# 14. Monitoring

## Overview

SuperTable records read, write, and tool-call metrics as JSON payloads
in **daily-partitioned** Redis LISTs. The `MonitoringWriter` component
pushes payloads via `RPUSH` to a key whose suffix is today's UTC date;
an external orchestrator drains older partitions into internal sink
tables (`__reads__`, `__writes__`, `__mcp__`).

A structured JSON logging layer (`supertable.logging`) runs alongside
the monitoring writer, providing correlation-ID-based request tracing.

## Monitored Operations

Monitoring lives at the **org level** — one daily LIST per
`(org, monitor_type, date)` — *not* per supertable. A cross-supertable
query (e.g. a join across `sales` and `customers`) writes exactly one
entry whose payload carries `supertables: [...]` for per-supertable
attribution at read time.

The Redis key shape (built by `redis_keys.monitor_partition(org, monitor_type, date)`):

```
supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}
```

Once midnight (UTC) passes, the writer rolls into the new day's key
automatically — the date is recomputed per batch ship, never cached.

| Category | `monitor_type` | Source | Sink Table | Key Fields |
|----------|----------------|--------|-----------|------------|
| **Reads**     | `plans`   | Query execution                    | `__reads__`   | query_id, engine, table_name, result_rows, execution_timings, status, supertables |
| **Writes**    | `writes`  | Data ingestion (`DataWriter.write`)| `__writes__`  | query_id, table_name, incoming_rows, inserted, deleted, duration, supertables |
| **MCP**       | `mcp`     | MCP tool invocations               | `__mcp__`     | tool, organization, rows, duration_ms, supertables |
| **Compaction**| `compact` | Explicit compaction (`DataWriter.compact`) | `__compact__` | query_id, table_name, files_before, files_after, files_compacted, tombstone_rows_removed, duration, supertables |
| **OData**     | `odata`   | OData reads                        | _(none)_      | endpoint_id, entity_set, rows_returned, role_name, supertables |
| **Errors**    | `errors`  | Any handler that caught            | _(none)_      | service, action, error_code, message, supertables |
| **Locks**     | `locks`   | Redis lock acquisitions / releases | _(none)_      | lock_key, action, holder, supertables |

The category is selected via the `monitor_type` argument when
constructing the writer. The closed-set of valid `monitor_type` values
is enforced inside `redis_keys.monitor_partition(...)`; any other value
raises at key-build time.

The sink-table mapping is exposed as `MONITORING_SINK_TABLE_FOR`:

```python
from supertable.monitoring import MONITORING_SINK_TABLE_FOR
MONITORING_SINK_TABLE_FOR["plans"]   # "__reads__"
MONITORING_SINK_TABLE_FOR["writes"]  # "__writes__"
MONITORING_SINK_TABLE_FOR["mcp"]     # "__mcp__"
```

## Writing Metrics

`monitoring_writer.py` exposes two entry points:

```python
from supertable.monitoring_writer import (
    MonitoringWriter, get_monitoring_logger,
)

# 1. Context-managed (auto-flush on exit)
with MonitoringWriter(organization, monitor_type="writes") as mw:
    mw.log_metric({
        "query_id": "abc",
        "table_name": "facts",
        "duration": 0.42,
        "inserted": 100,
        "deleted": 0,
        "supertables": ["sales"],
    })

# 2. Long-lived singleton (background flush thread)
mw = get_monitoring_logger(
    organization=organization,
    monitor_type="plans",
)
mw.log_metric({"query_id": "q1", "rows_read": 1234})
mw.request_flush()
```

`get_monitoring_logger` returns a thread-safe singleton per `(org,
monitor_type)` pair — **the date is deliberately not part of the cache
key** so one logger handles the daily rollover by recomputing the
Redis key per batch ship. Payloads are buffered and flushed in
batches. The writer captures back-pressure stats (`queue.qsize()`,
`current_batch`, `queue_stats`) which can be inspected for sizing
decisions.

## Reading the Live Tail

For "recent N" UI views (the most common monitoring read):

```python
from supertable.monitoring import read_recent
from supertable.redis_catalog import RedisCatalog

catalog = RedisCatalog()

# Last 100 writes — walks today, yesterday, … up to 7 days back
recent_writes = read_recent(
    catalog,
    organization="acme",
    monitor_type="writes",
    limit=100,
)

# Last 1000 plans, only look at last 3 days
recent_plans = read_recent(
    catalog,
    organization="acme",
    monitor_type="plans",
    limit=1000,
    max_days_back=3,
)
```

Properties:

- **Newest first.** `recent_writes[0]` is the most recent push.
- **Cross-day walk.** Today → yesterday → … up to `max_days_back`
  (default 7, clamp 1–90). Stops as soon as `limit` is reached.
- **Read-only.** No `RENAME`, no `DEL`, no `XADD`. Safe to call
  concurrently from multiple processes (UI poll, debug session,
  alert sweep) without coordination.
- **Tolerant.** Missing partitions (already drained or never
  existed) are silently skipped; malformed payloads are silently
  dropped; Redis errors per day return partial results rather than
  raising.

## Draining Old Partitions to Internal Sink Tables

The SDK does **not** spawn any daemon for this. The drain side
exposes pure orchestration primitives — your service owns the
scheduling (see also chap. 17 for the same model on GC).

```python
from supertable.monitoring import (
    list_drainable_partitions, drain_partition,
    MONITORING_SINK_TABLE_FOR,
)
from supertable.redis_catalog import RedisCatalog
from supertable.data_writer import DataWriter

catalog = RedisCatalog()

for part in list_drainable_partitions(catalog, organization="acme"):
    rows = drain_partition(
        catalog,
        organization=part.organization,
        monitor_type=part.monitor_type,
        date=part.date,
    )
    if not rows:
        continue

    sink_table = MONITORING_SINK_TABLE_FOR.get(part.monitor_type)
    if sink_table is None:
        continue   # this monitor_type has no agreed sink yet

    # Convert `rows` (list[dict]) into your internal table's schema and
    # write via DataWriter. The write does NOT emit a monitoring metric
    # — see "Loop guard" below.
    dw = DataWriter(super_name=internal_super, organization=part.organization)
    dw.write(role_name="system", simple_name=sink_table,
             data=to_arrow(rows), overwrite_columns=[])
```

### `list_drainable_partitions(catalog, *, organization, monitor_type=None)`

Discovers partitions whose date is **strictly older than today (UTC)**
— today's partition is excluded because the writer is still appending
to it. Returns `List[MonitorPartition]` sorted ascending. Pure read,
never raises.

`MonitorPartition` is a `NamedTuple` of `(organization, monitor_type,
date)` so you can iterate as tuples or address fields by name.

### `drain_partition(catalog, *, organization, monitor_type, date)`

Atomically takes a snapshot of one partition and deletes the source:

```
RENAME src → :_drain    (atomic snapshot)
LRANGE :_drain 0 -1
DEL :_drain
```

Returns `List[Dict[str, Any]]` (parsed payloads). The `RENAME` step
takes the source key out from under any concurrent writer in the rare
day-boundary race; once the drain handle exists, new writes create a
fresh empty source key for the new day.

**Crash recovery.** If your process crashes between `RENAME` and `DEL`,
the next call sees the `:_drain` handle already populated; the
`RENAME` fails (because the handle exists), and we read the handle
directly. No data lost.

Best for partitions ≤ ~100k records — the function materialises
everything into RAM in one shot.

### `iter_partition_chunks(catalog, *, organization, monitor_type, date, chunk_size=10000)`

Memory-bounded variant for huge partitions. Same `RENAME` semantics,
then `LRANGE` in slices. The `:_drain` handle is `DEL`-ed when the
iterator exhausts.

**Resume semantics:** if your process crashes mid-iteration, the
`:_drain` handle survives. Calling `iter_partition_chunks` or
`drain_partition` again resumes from the handle — no data is lost.
The trade-off is that re-reading does not skip already-consumed
chunks; the orchestrator must keep sink-table writes idempotent
(e.g., include a stable `query_id` so the sink can ignore replays).

If the caller breaks out of the iterator early (e.g. `break` after the
first chunk), the drain handle is left intact and the next call
resumes from there.

## Loop Guard — `MONITORING_SINK_TABLES`

Writing the drained partition back into `__writes__` would itself
emit a new `writes` metric, leading to a 1:1 self-amplifying loop
(one new metric per drained record, every day). The data writer
explicitly **suppresses** monitoring emission for sink-table writes:

```python
# In supertable/data_writer.py:
from supertable.monitoring.partitions import MONITORING_SINK_TABLES

if stats_payload is not None and simple_name not in MONITORING_SINK_TABLES:
    with MonitoringWriter(organization=..., monitor_type="writes") as monitor:
        monitor.log_metric(stats_payload)
```

The canonical set (single source of truth in
`supertable/monitoring/partitions.py`):

```python
MONITORING_SINK_TABLES = frozenset(
    {"__writes__", "__reads__", "__mcp__", "__plans__"}
)
```

The same guard is applied in `plan_extender.py` so SELECTs targeting
sink tables don't generate a `plans` metric either. The detection is
robust to comma-joined target lists and whitespace.

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
  `get_monitoring_logger` singleton factory, batched flushing to
  today's partition, plus the `NullMonitoringLogger` no-op fallback.
- `supertable/monitoring/__init__.py` — re-exports the orchestration
  surface (`list_drainable_partitions`, `drain_partition`,
  `iter_partition_chunks`, `read_recent`, `MonitorPartition`,
  `MONITORING_SINK_TABLES`, `MONITORING_SINK_TABLE_FOR`).
- `supertable/monitoring/partitions.py` — orchestration primitives,
  drain semantics, sink-table set.
- `supertable/logging.py` — JSON / text formatters,
  `configure_logging`, `RequestLoggingMiddleware`, correlation
  propagation.
- `supertable/redis_keys.py` — `monitor_partition`,
  `monitor_partition_drain`, `monitor_partition_pattern`,
  `monitor_partition_pattern_for_org`, `parse_monitor_partition_key`.
