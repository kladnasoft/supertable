# Monitoring and quality scheduling

Monitoring publishes operational metrics to daily Redis lists. The package includes readers and drain helpers for these lists, but no scheduled process that archives them into tables. Data-quality scheduling is a separate, explicitly started background thread.

Implementation: [monitoring_writer.py](../supertable/monitoring_writer.py), [monitoring/partitions.py](../supertable/monitoring/partitions.py), [plan_extender.py](../supertable/plan_extender.py), and [quality/scheduler.py](../supertable/quality/scheduler.py).

## 1. Emit a metric

```python
from supertable.monitoring_writer import MonitoringWriter

with MonitoringWriter(organization="acme", monitor_type="writes") as monitor:
    monitor.log_metric({
        "super_name": "warehouse",
        "table_name": "orders",
        "incoming_rows": 100,
        "duration": 0.25,
    })
    monitor.request_flush(timeout_s=2.0)
```

The constructor is keyword-only: `MonitoringWriter(*, organization, monitor_type="plans", redis_connector=None)`. `write(payload)` and `enqueue(payload)` alias `log_metric(payload)`. `get_monitoring_logger` exposes the underlying cached logger with the same arguments.

Supported metric types are `plans`, `writes`, `mcp`, `odata`, `errors`, `locks`, and `compact`. These names are validated by Redis key construction; an unsupported type can fail during asynchronous shipping. Use a supported value when creating the writer.

When missing, the logger adds `recorded_at=time.time()` and `instance_id="<hostname>:<pid>"`. Existing values are preserved. Built-in emitters provide ISO UTC timestamp strings, so `recorded_at` does not have one enforced type for all producers. Payloads must be JSON-serializable.

## 2. Queue and lifetime behavior

`SUPERTABLE_MONITORING_ENABLED=true` by default. When false, the factory returns `NullMonitoringLogger`, whose `log_metric` only increments `total_received` and does not ship the payload. `SUPERTABLE_MONITOR_CACHE_MAX=256` limits cached loggers, keyed by organization and metric type. Reusing a cache key reuses the first logger and its Redis connector. Cache eviction removes the oldest inserted entry and signals its worker to stop without a final drain.

An active logger has a daemon worker and a queue of 10,000 payloads. `log_metric` enqueues without blocking; a full queue drops the new metric. The worker sends batches of up to 200 and gathers additional items for up to 50 ms. It attempts a Redis pipeline first, then attempts individual sends if the pipeline raises. Partial pipeline success followed by retries can duplicate entries. Failed individual sends are dropped.

If no Redis connector is available, shipping falls back to a debug log and counts the metric as processed. `total_processed` therefore does not always mean Redis persistence. Queue statistics are available through `queue_stats`, guarded by `queue_stats_lock`: `total_received`, `total_processed`, `total_dropped`, and `current_size`. `current_batch` exposes the batch being shipped.

`request_flush(timeout_s=2.0)` drains at most one batch under the shipping lock. Its timeout bounds draining after acquiring that lock, not all waiting or Redis I/O. Context-manager exit also attempts one batch; it does not stop the cached worker or guarantee an empty queue. Metric delivery is best effort and does not determine whether a table operation succeeds.

## 3. Built-in metrics and recursion guards

`DataWriter.write` emits a `writes` metric after releasing its table lock. Payloads include table and role identity, overwrite options, incoming rows/columns, inserted/deleted counts, resource changes, lineage, total duration, and detailed timings and counters. `DataWriter.compact` similarly emits `compact` metrics including file counts and removed tombstone rows. Monitoring failures are caught and logged.

Execution-plan extension emits `plans` metrics with query ID/hash, SQL truncated to 500 characters, role, source type, engine, status/message, result shape, timings, profile summary, and query profile. Nested profiles are JSON strings within the metric. The code also removes the temporary local plan JSON after extending the plan. SQL in these monitoring records is not passed through the audit encryption helper.

The sink mapping is:

| Metric type | Intended sink table |
| --- | --- |
| `plans` | `__reads__` |
| `writes` | `__writes__` |
| `mcp` | `__mcp__` |
| `compact` | `__compact__` |

This mapping does not create or populate those tables. `MONITORING_SINK_TABLES` additionally contains the legacy `__plans__` name. Writes and compaction targeting any name in that set skip their operational metric. Query-plan extension skips metrics if any comma-separated name in `original_table` matches a sink. The check uses those exact names, not a blanket exclusion of every internal table. These guards prevent metrics from generating additional metrics when a caller persists or queries monitoring data.

## 4. Daily Redis partitions

With prefix `supertable`, the list key is:

```text
supertable:<organization>:monitor:<type>:doc:YYYY-MM-DD
```

The date is UTC shipping time, not the event's `recorded_at` field. Each batch appends JSON records with `RPUSH` and sets absolute expiration to midnight UTC on the partition date plus seven days. Repeated writes do not extend a partition's lifetime beyond that date-based expiration. The seven-day lifetime is a module constant.

```python
from supertable import RedisCatalog
from supertable.monitoring import list_drainable_partitions, read_recent

catalog = RedisCatalog()
recent = read_recent(
    catalog,
    organization="acme",
    monitor_type="writes",
    limit=100,
    max_days_back=7,
)
closed_days = list_drainable_partitions(catalog, organization="acme")
```

`read_recent` is nondestructive and returns newest appended records first, visiting today and then older days. Ordering is based on list position and partition date, not payload timestamps. `limit` is capped at 1,000,000; `max_days_back` is clamped to 1–90. Nonpositive limits return an empty list. Malformed JSON and non-object entries are skipped.

`list_drainable_partitions(catalog, *, organization, monitor_type=None)` returns sorted `MonitorPartition(organization, monitor_type, date)` records for existing date partitions strictly older than today. It does not return the current UTC day, future dates, or `:_drain` keys. An empty organization, invalid type, missing Redis client, or scan failure returns an empty list.

## 5. Drain a completed partition

`drain_partition(catalog, *, organization, monitor_type, date)` attempts `RENAMENX` from the daily list to the same key with `:_drain` appended, reads that drain list in full, and deletes it before returning the parsed records. If a prior drain handle exists it reads that handle instead. A read failure leaves the handle; a delete failure can redeliver entries on a later call.

For a bounded in-memory read, use:

```python
from supertable.monitoring import iter_partition_chunks

for rows in iter_partition_chunks(
    catalog,
    organization="acme",
    monitor_type="writes",
    date=completed_utc_date,
    chunk_size=10_000,
):
    persist_rows(rows)
```

`completed_utc_date` is a selected past `YYYY-MM-DD` date; `persist_rows` is the caller's durable sink. `chunk_size` defaults to 10,000 and is clamped to 1–1,000,000. The iterator reads slices of a renamed list and deletes the handle only after full iteration. Closing it early or encountering a read failure leaves the handle for a retry, which starts at the beginning and can redeliver earlier chunks.

Neither drain function validates that its supplied date is complete. Neither implements a destination commit or acknowledges successful archival. The whole-list helper can lose data between deleting Redis entries and saving its return value. The chunk helper can redeliver after partial persistence, and concurrent drainers can read the same handle. Callers must coordinate one drainer, make the destination idempotent, and include leftover drain handles in recovery: the ordinary partition-listing function does not discover them. No automatic archival, recovery scheduler, or exactly-once delivery is implemented here.

## 6. Start quality scheduling explicitly

Quality configuration and results live in Redis under the lake. The [Python SDK](15_python_sdk.md) describes configuration and rule APIs. To enable the schedule and start the process-local worker:

```python
from supertable import RedisCatalog
from supertable.quality import start_scheduler
from supertable.quality.config import DQConfig

catalog = RedisCatalog()
quality = DQConfig(catalog.r, "acme", "warehouse")
schedule = quality.get_schedule()
schedule["enabled"] = True
quality.set_schedule(schedule)
start_scheduler()
```

`start_scheduler()` starts one daemon thread per process, returning `True` when started and `False` when already running. It waits ten seconds, then scans on a 60-second tick. It is exported by `supertable.quality` but is not automatically started by table writes or module import. The thread has no public stop method.

Default lake schedule:

| Field | Default |
| --- | --- |
| `enabled` | `false` |
| `quick_cron` | `0 */4 * * *` |
| `deep_cron` | `0 2 * * *` |
| `custom_cron` | `0 */6 * * *` |
| `post_ingest` | `true` |
| `post_ingest_quick` | `true` |
| `post_ingest_custom` | `true` |
| `post_ingest_deep` | `false` |
| `cooldown_seconds` | Scheduler fallback: `300` |

Despite their names, the `*_cron` fields are reduced to elapsed-second intervals by a small parser. `*/N` in the hour field becomes N hours; `*/N` in minutes becomes N minutes; a fixed hour with unrestricted day/month/weekday becomes 24 hours. Other expressions fall back to four hours. The scheduler does not calculate calendar firing times: `0 2 * * *` means an interval of 24 hours, not specifically 02:00. Local last-run dictionaries start empty after process restart, making checks immediately eligible on the first tick.

Discovery scans native lake roots and their table leaves. Tables beginning with `__` are excluded. A table schedule can disable a table or override quick/deep/custom intervals and deep/custom enable flags. Deep scheduling requires at least one enabled check whose ID begins with `D`; custom scheduling requires enabled rules matching the table.

## 7. Post-ingest and concurrency behavior

After a write, `notify_ingest(redis, org, sup, table_name)` sets a ten-minute pending flag when the lake schedule is enabled and `post_ingest` allows it. Internal names wrapped in double underscores are ignored. Disabled lake schedules are memoized for 60 seconds, up to 1,024 entries; `set_schedule` clears this memo in the current process.

A scheduler tick checks interval work first, then pending ingestion work. Quick, deep, and custom modes share one table running lock and one cooldown key. The running lock uses a random ownership token, `SET NX EX 300`, and token-checked release, without renewal. A check exceeding five minutes can overlap another runner after its lease expires. Successful dispatch sets a cooldown, normally five minutes.

The first mode that runs can prevent subsequent modes from running during the same tick. Pending flags are removed after any requested post-ingest mode reports success, so they do not guarantee that every enabled mode ran. Some check functions catch failures or return early internally; the outer dispatcher can still apply cooldown in those cases. Local interval dictionaries are per process; Redis locks and cooldowns provide the shared coordination.

Quick and deep checks execute as the built-in `superadmin` profiler role. They read schema and query the table, then update latest results, per-column results, anomalies, and attempted history records. Custom rules execute using their persisted `created_by_role`; rules lacking that field are skipped. Generated queries mark their plan source as `system`. Quality checks do not reject or roll back the ingest that triggered them.

[quality/history.py](../supertable/quality/history.py) attempts to append history rows to the lake's `__data_quality__` table as `superadmin`. Rows include check type, score/status, counts, execution time, and JSON fields for anomalies, column statistics, and rule results. If that write fails, the scheduler calls `write_history_via_sql`; despite its name, that fallback prepends JSON to a Redis list and retains at most 1,000 entries. There is no implemented replay from that fallback list into the history table.
