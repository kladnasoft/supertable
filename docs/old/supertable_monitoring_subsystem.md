# Feature / Component Reverse Engineering: Supertable Monitoring Subsystem

## 1. Executive Summary

This code implements the **monitoring data pipeline** for the Supertable platform — a write-side ingestion path (`monitoring_writer.py`) and a read-side query path (`monitoring_reader.py`) for operational telemetry data.

- **Main responsibility:** Collect runtime metrics from Supertable query execution (and potentially other operations), buffer them asynchronously, ship them to Redis for downstream persistence, and later read the persisted Parquet files back via DuckDB for analysis.
- **Likely product capability:** Self-service observability for Supertable "supers" (datasets/workspaces). Operators and admins can inspect query execution history, performance metrics, and system behavior over time windows.
- **Business purpose:** Operational visibility, debugging, performance optimization, and potentially usage-based billing or SLA enforcement for a multi-tenant data platform.
- **Technical role:** An internal platform subsystem that sits alongside the core query engine. The writer is embedded in the hot path (query execution) and must be non-blocking. The reader reuses the same DuckDB/storage infrastructure as the primary data-read path.

---

## 2. Functional Overview

### What can the product do because of this code?

1. **Emit operational metrics during query execution** without impacting query latency (non-blocking, bounded-queue, async shipping).
2. **Persist those metrics** via Redis as an intermediate buffer, with an implied downstream consumer that writes Parquet files to object storage.
3. **Query historical monitoring data** over arbitrary time windows via DuckDB, using the same storage and path-resolution infrastructure as normal data reads.
4. **Gracefully degrade** — monitoring never crashes the query engine. If Redis is down, metrics are logged or dropped. If monitoring is disabled, a no-op logger is substituted.

### Target users / actors

| Actor | Interaction |
|---|---|
| **Query engine (internal)** | Calls `MonitoringWriter.log_metric()` on every query execution to record telemetry |
| **Platform operator / admin** | Reads monitoring data via `MonitoringReader.read()` to diagnose performance, inspect query plans, or audit usage |
| **Background worker (inferred)** | Consumes Redis lists and writes Parquet files to storage (not present in provided code) |
| **Debug/monitoring scripts** | Access `.queue_stats`, `.current_batch`, `.queue` attributes for live introspection |

### Business workflows

- **Query observability:** Every query plan execution emits metrics → Redis → Parquet → queryable via DuckDB. The default `monitor_type` is `"plans"`, strongly suggesting query execution plan monitoring.
- **Operational health:** Queue stats (received, processed, dropped, current size) enable real-time health monitoring of the ingestion pipeline itself.
- **Multi-tenant isolation:** All paths are scoped to `organization/super_name/monitor_type`, ensuring tenant-level data separation.

### Strategic significance

This subsystem makes the platform **operationally self-aware**. Without it, the product would be a black box — operators would have no built-in way to understand query performance, detect regressions, or audit system usage. This is foundational for any enterprise or SaaS data platform.

---

## 3. Technical Overview

### Architectural style

- **Producer/consumer pattern** with Redis as the message broker
- **Async-buffered writer** with a background daemon thread, bounded queue, and batch shipping
- **Storage-agnostic reader** using the platform's existing storage abstraction and DuckDB query engine
- **Null-object pattern** for graceful degradation when monitoring is disabled
- **Singleton/cache pattern** for writer instances to avoid thread/connection proliferation

### Major components

| Component | Role |
|---|---|
| `_AsyncMonitoringLogger` | Core async writer — bounded queue, background thread, batch Redis shipping |
| `NullMonitoringLogger` | No-op fallback that satisfies the same interface |
| `MonitoringWriter` | Backwards-compatible facade over `get_monitoring_logger()` |
| `get_monitoring_logger()` | Factory function with singleton cache, env-flag gating, and LRU eviction |
| `MonitoringReader` | DuckDB-based reader that resolves storage paths and queries Parquet files |

### Key design patterns

- **Non-blocking ingestion:** `log_metric()` uses `put_nowait()` — never blocks the caller. Full queue → metric is dropped with a counter increment.
- **Batch formation:** Background worker waits briefly (`batch_wait_s=50ms`) to coalesce items before shipping, reducing Redis round-trips.
- **Pipeline fallback:** If Redis pipeline `execute()` fails, falls back to per-item `rpush`.
- **Catalog-based file discovery:** Reader uses a JSON stats catalog to discover Parquet file locations, with time-window filtering where metadata is available.
- **Path reuse:** Reader explicitly reuses `DataEstimator._to_duckdb_path()` and `DuckDBExecutor._configure_httpfs_and_s3()` from the core read path, ensuring storage compatibility.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `supertable/monitoring_writer.py` | Async metric ingestion and Redis shipping | `MonitoringWriter`, `_AsyncMonitoringLogger`, `NullMonitoringLogger`, `get_monitoring_logger()`, `_MonitorKey`, `MonitoringLogger` (Protocol) | Write side of the monitoring pipeline; embedded in hot path |
| `supertable/monitoring_reader.py` | Historical metric querying via DuckDB | `MonitoringReader` | Read side of the monitoring pipeline; operator/admin-facing |

---

## 5. Detailed Functional Capabilities

### 5.1 Non-blocking Metric Ingestion

- **Description:** Accepts metric payloads from the query engine without blocking the caller thread.
- **Business purpose:** Ensures monitoring instrumentation has zero impact on query latency.
- **Trigger/input:** `log_metric(payload: Dict[str, Any])` called by query execution code.
- **Processing behavior:**
  1. Adds `recorded_at` timestamp if not present.
  2. Increments `total_received` counter.
  3. Attempts `put_nowait()` on bounded queue (default 10,000).
  4. If full, increments `total_dropped` and logs a warning.
- **Output/result:** Payload enqueued for async processing. No return value.
- **Dependencies:** Python `queue.Queue`, `threading.Lock`.
- **Constraints:** Queue is bounded (`max_queue=10_000`). Drops are silent (only logged, not raised).
- **Risks:** Under sustained load exceeding drain rate, metrics will be silently dropped. No backpressure signal to the caller.
- **Confidence:** Explicit.

### 5.2 Batched Async Redis Shipping

- **Description:** Background daemon thread drains the queue, forms batches, and ships them to Redis via pipelined `rpush`.
- **Business purpose:** Efficient metric delivery with minimal Redis round-trips; decouples ingestion from network I/O.
- **Trigger:** Background thread starts on logger construction; loops continuously.
- **Processing behavior:**
  1. Blocks on `queue.get(timeout=0.5)` for the first item.
  2. Drains additional items up to `batch_max` (200) with brief `batch_wait_s` (50ms) coalescing window.
  3. Acquires `_ship_lock` to prevent concurrent shipping with `request_flush()`.
  4. Ships via Redis pipeline; on pipeline failure, falls back to per-item `rpush`.
  5. Tracks `total_processed` and `total_dropped` per batch.
  6. On shipping exception, applies exponential backoff (0.1s → 5s cap).
- **Output:** Metrics serialized as compact JSON pushed to Redis list key `monitor:{org}:{super}:{type}`.
- **Dependencies:** `RedisConnector` (optional), `json`, `threading`.
- **Constraints:** Daemon thread — killed on process exit. Context manager `__exit__` attempts a best-effort flush.
- **Risks:** If Redis is permanently unavailable and `ship_to_redis=True`, metrics are dropped after backoff. No dead-letter queue.
- **Confidence:** Explicit.

### 5.3 Graceful Degradation (Null Logger)

- **Description:** When monitoring is disabled (`SUPERTABLE_MONITORING_ENABLED=false`) or construction fails, a `NullMonitoringLogger` is returned.
- **Business purpose:** Monitoring never breaks query execution. Production safety net.
- **Trigger:** `get_monitoring_logger()` checks env flag and catches all construction errors.
- **Processing:** `log_metric()` increments `total_received` but discards payload. All other methods are no-ops.
- **Output:** No data shipped.
- **Dependencies:** None beyond stdlib.
- **Confidence:** Explicit.

### 5.4 Singleton Logger Cache with LRU Eviction

- **Description:** `get_monitoring_logger()` caches logger instances per `(org, super_name, monitor_type)` key, with a max-size cap and LRU eviction.
- **Business purpose:** Prevents unbounded thread/Redis-connection growth in multi-tenant environments.
- **Trigger:** Every call to `get_monitoring_logger()`.
- **Processing:**
  1. Check `_MONITORS` dict under lock.
  2. If cached, return existing.
  3. If at capacity (`SUPERTABLE_MONITOR_CACHE_MAX`, default 256), evict oldest (Python dict insertion order).
  4. Eviction signals the evicted logger's `_stop` event so its daemon thread exits.
- **Constraints:** Eviction is FIFO, not true LRU (no access-time update on cache hit).
- **Risks:** Under high cardinality (many org/super combinations), frequent eviction may cause thread churn.
- **Confidence:** Explicit. The code comment says "LRU" but the implementation is insertion-order FIFO.

### 5.5 Context Manager Flush Semantics

- **Description:** `_AsyncMonitoringLogger.__exit__` performs a best-effort flush with a low-contention lock strategy.
- **Business purpose:** Critical for short-lived processes (CLI tools, Lambda functions) where the daemon thread would be killed before draining the queue.
- **Processing:**
  1. Attempts non-blocking lock acquire with 100ms timeout.
  2. If acquired, drains and ships remaining queue items.
  3. If not acquired (worker is mid-ship), skips — the worker will deliver.
- **Confidence:** Explicit.

### 5.6 Historical Monitoring Data Query

- **Description:** `MonitoringReader.read()` queries persisted monitoring Parquet files over a time window using DuckDB.
- **Business purpose:** Enables operators to inspect query execution metrics, performance trends, and system behavior.
- **Trigger:** API call with `role_name`, optional `from_ts_ms`/`to_ts_ms`, and `limit`.
- **Processing:**
  1. Performs RBAC check via `check_meta_access()`.
  2. Defaults to last 24 hours if no time range specified.
  3. Loads stats catalog from storage (tries 3 candidate paths: new → legacy).
  4. Filters Parquet file keys by time window using catalog metadata.
  5. Resolves storage keys to DuckDB-readable paths (presigned URLs / S3 paths).
  6. Configures DuckDB connection for httpfs/S3.
  7. Executes `parquet_scan()` with `union_by_name=TRUE` and `HIVE_PARTITIONING=TRUE`.
  8. Filters by `execution_time` column, orders descending, applies limit.
  9. On schema mismatch (e.g., missing `execution_time` column), falls back to unfiltered scan.
  10. On total failure, returns empty DataFrame.
- **Output:** `pd.DataFrame` with monitoring records.
- **Dependencies:** `duckdb`, `pandas`, `DataEstimator`, `DuckDBExecutor`, `get_storage()`, `check_meta_access`.
- **Constraints:** Assumes Parquet files have an `execution_time` column for time filtering. Falls back gracefully if not.
- **Risks:** Stale catalogs may reference non-existent files. DuckDB's `parquet_scan` behavior on missing files depends on version.
- **Confidence:** Explicit.

### 5.7 Multi-Format Catalog Parsing

- **Description:** The reader tolerates multiple catalog JSON shapes for file discovery.
- **Business purpose:** Backward compatibility across catalog format evolution.
- **Supported shapes:**
  - `{"files": [{"path": "...", ...}]}` — structured entries with optional time bounds
  - `{"files": ["org/super/...parquet"]}` — plain string entries
  - `{"tables": {"facts": {"files": [...]}}}` — nested table-scoped entries
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 `monitoring_writer.py`

#### `MonitoringLogger` (Protocol)

| Attribute | Details |
|---|---|
| **Type** | Protocol (structural typing interface) |
| **Purpose** | Defines the contract all monitoring loggers must satisfy |
| **Methods** | `log_metric()`, `request_flush()`, `__enter__()`, `__exit__()` |
| **Debug attributes** | `queue`, `current_batch`, `queue_stats`, `queue_stats_lock` |
| **Importance** | Critical — defines the interface boundary |

#### `NullMonitoringLogger`

| Attribute | Details |
|---|---|
| **Type** | Class |
| **Purpose** | No-op implementation for disabled/failed monitoring |
| **Key behavior** | Increments `total_received` on `log_metric()` but discards payload |
| **Importance** | Important — safety net |

#### `_MonitorKey`

| Attribute | Details |
|---|---|
| **Type** | Frozen dataclass |
| **Fields** | `organization`, `super_name`, `monitor_type` |
| **Properties** | `path_key` → `"org/super/type"`, `redis_list_key` → `"monitor:org:super:type"` |
| **Purpose** | Identifies a monitoring stream; used as cache key and Redis key |
| **Importance** | Supporting |

#### `_AsyncMonitoringLogger`

| Attribute | Details |
|---|---|
| **Type** | Class (core implementation) |
| **Purpose** | Production-grade async metric logger with batched Redis shipping |
| **Constructor params** | `key: _MonitorKey`, `redis_connector`, `max_queue=10_000`, `ship_to_redis=True`, `batch_max=200`, `batch_wait_s=0.05` |
| **Importance** | Critical |

Key methods:

| Method | Purpose | Side Effects |
|---|---|---|
| `log_metric(payload)` | Non-blocking enqueue | Mutates queue_stats |
| `request_flush(timeout_s)` | Synchronous drain + ship | Acquires _ship_lock, ships to Redis |
| `_try_flush()` | Low-contention flush for __exit__ | Non-blocking lock attempt |
| `_start_worker()` | Launches background daemon thread | Starts thread |
| `_worker()` | Main loop: dequeue → batch → ship | Continuous background operation |
| `_drain_more(max_items, wait_s)` | Coalesces queue items into batch | Reads from queue |
| `_drain_batch(deadline, allow_wait)` | Drains up to batch_max | Reads from queue |
| `_ship_batch(batch)` | Ships via Redis pipeline with per-item fallback | Redis writes, updates stats |
| `_ship_one(payload)` | Ships single item (fallback or non-Redis mode) | Redis rpush or debug log |

#### `get_monitoring_logger()`

| Attribute | Details |
|---|---|
| **Type** | Module-level factory function |
| **Purpose** | Singleton cache with env-flag gating and eviction |
| **Returns** | `MonitoringLogger` (either cached `_AsyncMonitoringLogger` or `NullMonitoringLogger`) |
| **Side effects** | May create thread, Redis connection; may evict old loggers |
| **Importance** | Critical — primary entry point for writers |

#### `MonitoringWriter`

| Attribute | Details |
|---|---|
| **Type** | Class (facade) |
| **Purpose** | Backwards-compatible wrapper; delegates to `get_monitoring_logger()` |
| **Aliases** | `write()`, `enqueue()` → both forward to `log_metric()` |
| **`__getattr__`** | Forwards unknown attribute access to underlying logger (debug field access) |
| **Importance** | Important — public API surface |

#### `_evict_oldest_monitor()`

| Attribute | Details |
|---|---|
| **Type** | Module-level function |
| **Purpose** | Evicts oldest cached logger and signals its worker thread to stop |
| **Called by** | `get_monitoring_logger()` when cache is full |
| **Importance** | Supporting |

#### `_monitoring_enabled()`

| Attribute | Details |
|---|---|
| **Type** | Module-level function |
| **Purpose** | Reads `SUPERTABLE_MONITORING_ENABLED` env var (default: `true`) |
| **Importance** | Supporting |

---

### 6.2 `monitoring_reader.py`

#### `MonitoringReader`

| Attribute | Details |
|---|---|
| **Type** | Class |
| **Purpose** | Reads monitoring Parquet data via DuckDB |
| **Constructor params** | `super_name`, `organization`, `monitor_type` |
| **Importance** | Critical |

Key methods:

| Method | Purpose | Returns |
|---|---|---|
| `read(role_name, from_ts_ms, to_ts_ms, limit)` | Main entry point; RBAC-checked time-windowed query | `pd.DataFrame` |
| `_load_stats_catalog()` | Finds and loads the first valid stats catalog JSON | `(path, dict)` |
| `_iter_file_entries(catalog)` | Iterates file entries from any catalog shape | `Iterable[Any]` |
| `_entry_to_key(entry)` | Normalizes a file entry to `{"key": ..., "min_ts_ms": ...}` | `Optional[Dict]` |
| `_select_keys_for_window(catalog, from_ts_ms, to_ts_ms)` | Filters Parquet keys by time window | `List[str]` |
| `_build_sql(paths_sql_array, from_ts_ms, to_ts_ms, limit)` | Constructs DuckDB SQL | `str` |

---

## 7. End-to-End Workflows

### 7.1 Metric Ingestion Workflow (Write Path)

1. **Trigger:** Query engine calls `MonitoringWriter.log_metric(payload)` or equivalent.
2. `MonitoringWriter` delegates to cached `_AsyncMonitoringLogger.log_metric()`.
3. `log_metric()` adds `recorded_at` timestamp if missing.
4. Increments `total_received` counter under lock.
5. Attempts `queue.put_nowait(payload)`.
6. **Decision:** Queue full? → Increment `total_dropped`, log warning, return. Not full? → Continue.
7. Updates `current_size` stat.
8. Background worker thread (running continuously):
   a. Blocks on `queue.get(timeout=0.5)`.
   b. Drains additional items up to 200 with 50ms coalescing window.
   c. Acquires `_ship_lock`.
   d. Attempts Redis pipeline `rpush` for all items.
   e. **Decision:** Pipeline succeeds? → Count all as processed. Fails? → Fall back to per-item shipping.
   f. Updates `total_processed` and `total_dropped`.
   g. On exception: exponential backoff (0.1s → 5s).
9. **Success:** Metrics available in Redis list `monitor:{org}:{super}:{type}`.
10. **Failure modes:** Queue full (dropped), Redis unavailable (backoff + eventual drop), process exit before flush (partial loss mitigated by context manager).
11. **Observability:** `queue_stats` dict, log messages with `[monitor]` prefix.

### 7.2 Monitoring Data Query Workflow (Read Path)

1. **Trigger:** Operator calls `MonitoringReader.read(role_name=..., ...)`.
2. `check_meta_access()` verifies RBAC authorization.
3. Defaults time window to last 24 hours if not specified.
4. Validates `from_ts_ms <= to_ts_ms`.
5. `_load_stats_catalog()` tries 3 candidate paths in storage (new → legacy).
6. **Decision:** No catalog found? → Raise `FileNotFoundError`.
7. `_select_keys_for_window()` filters Parquet file keys by time-range metadata.
8. **Decision:** No keys selected? → Return empty DataFrame.
9. Resolves storage keys to DuckDB-readable paths via `DataEstimator._to_duckdb_path()`.
10. Creates transient DuckDB connection.
11. Configures httpfs/S3 via `DuckDBExecutor._configure_httpfs_and_s3()`.
12. Executes `parquet_scan()` with `execution_time` filter, ORDER BY DESC, LIMIT.
13. **Decision:** Query fails (schema mismatch)? → Falls back to unfiltered `SELECT * ... LIMIT`.
14. **Decision:** Fallback also fails? → Returns empty DataFrame.
15. Closes DuckDB connection.
16. **Success:** Returns `pd.DataFrame` with monitoring records.
17. **Failure modes:** Catalog missing, storage unavailable, Parquet schema mismatch (all handled gracefully).
18. **Observability:** Logger messages with `[monitoring_reader]` prefix.

---

## 8. Data Model and Information Flow

### Core entities

| Entity | Shape | Description |
|---|---|---|
| **Metric payload** | `Dict[str, Any]` | Arbitrary key-value metric. Must include `recorded_at` (auto-added) and likely `execution_time` for querying. |
| **Stats catalog** | JSON file in storage | Index of Parquet files with optional time-range metadata per file. |
| **Parquet files** | Columnar storage in object store | Persisted monitoring records with `execution_time` column and Hive partitioning. |
| **Queue stats** | `Dict[str, int]` | `total_received`, `total_processed`, `total_dropped`, `current_size`. |
| **_MonitorKey** | Frozen dataclass | `(organization, super_name, monitor_type)` — identifies a monitoring stream. |

### Information flow

```
Query Engine
    │
    ▼
MonitoringWriter.log_metric(payload)
    │
    ▼
Bounded Queue (max 10,000)
    │
    ▼  (background daemon thread, batched)
Redis List: "monitor:{org}:{super}:{type}"
    │
    ▼  (inferred: external consumer, NOT in this code)
Parquet files in object storage
    │
    ▼
Stats catalog (_stats.json)
    │
    ▼
MonitoringReader.read()  ──►  DuckDB parquet_scan  ──►  pd.DataFrame
```

### Key storage paths

| Path pattern | Purpose |
|---|---|
| `{org}/{super}/monitoring/{type}/_stats.json` | Primary stats catalog location |
| `{org}/{super}/monitoring/stats/_stats.json` | Legacy catalog location |
| `{org}/{super}/_stats.json` | Fallback catalog location |
| `monitor:{org}:{super}:{type}` | Redis list key for metric buffering |

### Stateful behavior

- Writer: Stateful (background thread, queue, connection cache).
- Reader: Stateless per call (creates/closes DuckDB connection each invocation).

---

## 9. Dependencies and Integrations

### Internal dependencies

| Module | Purpose |
|---|---|
| `supertable.config.defaults.logger` | Shared logging configuration |
| `supertable.redis_connector.RedisConnector` | Redis client wrapper (optional) |
| `supertable.storage.storage_factory.get_storage()` | Storage abstraction (S3, local, etc.) |
| `supertable.engine.duckdb_transient.DuckDBExecutor` | DuckDB S3/httpfs configuration |
| `supertable.engine.data_estimator.DataEstimator` | Storage key → DuckDB path resolution |
| `supertable.rbac.access_control.check_meta_access` | RBAC authorization for monitoring reads |

### Third-party dependencies

| Package | Purpose |
|---|---|
| `duckdb` | Analytical SQL engine for Parquet queries |
| `pandas` | DataFrame output from monitoring reads |
| `redis` (via RedisConnector) | Message broker for metric shipping |

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SUPERTABLE_MONITORING_ENABLED` | `"true"` | Feature flag to enable/disable monitoring |
| `SUPERTABLE_MONITOR_CACHE_MAX` | `"256"` | Max cached logger instances (controls thread/connection count) |

### External systems

| System | Role | Failure impact |
|---|---|---|
| **Redis** | Intermediate metric buffer | Metrics degraded to debug logs or dropped |
| **Object storage** (S3/compatible) | Parquet file persistence | Reader returns empty DataFrame |
| **DuckDB** | Query engine for Parquet | Reader returns empty DataFrame |

---

## 10. Architecture Positioning

### Where this code sits

```
┌─────────────────────────────────────────────────┐
│              Query Execution Layer               │
│  (calls MonitoringWriter.log_metric per query)   │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│        monitoring_writer.py  [THIS CODE]         │
│  _AsyncMonitoringLogger → Redis list             │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│     Redis (intermediate buffer)                  │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│  Parquet Writer Worker (NOT in provided code)    │
│  Consumes Redis → writes Parquet to storage      │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│       Object Storage (S3 / compatible)           │
│  Parquet files + _stats.json catalogs            │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│       monitoring_reader.py  [THIS CODE]          │
│  MonitoringReader → DuckDB → pd.DataFrame        │
└─────────────────────────────────────────────────┘
```

### Likely upstream callers

- Query plan executor (writes metrics).
- API layer or admin CLI (invokes `MonitoringReader.read()`).

### Likely downstream consumers (inferred, not in code)

- **Redis-to-Parquet worker:** Consumes the Redis list and writes Parquet files to object storage. Also maintains the `_stats.json` catalog.
- **Admin dashboard or API endpoint:** Presents monitoring data to operators.

### Boundary observations

- The writer is designed to be embedded in the hot path with zero blocking guarantees.
- The reader is designed for operator-facing, non-latency-critical queries.
- The two components are decoupled — connected only through the shared storage catalog and Parquet files.
- The reader deliberately reuses core platform path-resolution (`DataEstimator`) and DuckDB configuration (`DuckDBExecutor`) to avoid divergence.

### Coupling

- Tightly coupled to `DataEstimator._to_duckdb_path()` and `DuckDBExecutor._configure_httpfs_and_s3()` (accesses private methods via `# noqa: SLF001`).
- Loosely coupled to Redis (optional dependency with graceful fallback).

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Operators of a multi-tenant data platform need visibility into query execution performance and system health. |
| **Why it matters** | Without monitoring, the platform is a black box. Operators cannot diagnose slow queries, identify hot tenants, detect regressions, or enforce SLAs. |
| **Revenue relevance** | Indirectly revenue-related: monitoring data could feed usage-based billing, tier enforcement, or upsell triggers. The `"plans"` monitor type suggests query plan analysis, which is premium observability. |
| **Efficiency relevance** | Directly efficiency-related: non-blocking design ensures monitoring overhead is near-zero on query latency. |
| **What would be lost without it** | Complete loss of operational visibility. Debugging would require manual log analysis. No historical performance data. No self-service diagnostics. |
| **Differentiating vs commodity** | The implementation is production-grade (bounded queues, graceful degradation, batch shipping, singleton caching with eviction) but the capability itself (query monitoring) is expected in any enterprise data platform. The differentiator is the tight integration with the existing DuckDB/storage infrastructure. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate-high. The writer manages concurrency (threads, locks, queues, non-blocking flushes) with careful edge-case handling. |
| **Architectural maturity** | High. Null-object pattern, Protocol-based interface, singleton cache with bounded eviction, backward-compatible facade, and reuse of core infrastructure show deliberate design. |
| **Maintainability** | Good. Well-documented with inline comments explaining design decisions (e.g., the `_try_flush` 100ms timeout rationale). |
| **Extensibility** | Moderate. New `monitor_type` values can be added trivially. New shipping backends would require modifying `_ship_batch()`. |
| **Operational sensitivity** | High. The writer runs in the query hot path. Queue exhaustion or Redis failures must not propagate. The code handles this carefully. |
| **Performance** | Batch coalescing (50ms window, 200 items), pipeline Redis commands, and non-blocking enqueue are all performance-conscious choices. |
| **Reliability** | Strong degradation guarantees. Multiple fallback levels: pipeline → per-item → debug log → NullLogger. Reader falls back from filtered to unfiltered to empty DataFrame. |
| **Security** | RBAC enforcement on reads via `check_meta_access()`. No visible auth on the write side (presumably controlled by caller context). |
| **Testing clues** | `NullMonitoringLogger` is explicitly designed for test scenarios. The Protocol-based interface enables mock injection. No test files provided. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Monitoring metrics are shipped to Redis lists as JSON.
- Redis is an optional dependency; monitoring degrades gracefully without it.
- The reader queries Parquet files via DuckDB using the same path resolution as normal data reads.
- RBAC is enforced on monitoring reads.
- The default monitor type is `"plans"` (query plan monitoring).
- Logger instances are cached per `(org, super, type)` with a max of 256 entries (configurable).
- The writer is non-blocking; full queues result in dropped metrics.
- The reader supports three catalog JSON formats for backward compatibility.
- The `execution_time` column is the expected time-range filter in Parquet files.
- Multiple catalog path candidates are tried (new → legacy).

### Reasonable Inferences

- **A Redis-to-Parquet worker exists** that consumes the Redis lists and produces the Parquet files + catalogs that the reader queries. (Not provided.)
- **The `"plans"` monitor type** tracks query execution plan telemetry (execution time, plan details, resource usage).
- **The stats catalog is maintained by the Parquet writer**, not by the reader or writer in this code.
- **The `execution_time` field** is a millisecond-epoch timestamp representing when a query was executed.
- **This code is used in a SaaS/multi-tenant context** — the singleton cache eviction, org-scoped paths, and RBAC all point to this.
- **The facade aliases (`write()`, `enqueue()`)** exist because the monitoring API evolved over time and older code uses different method names.

### Unknown / Not Visible in Provided Snippet

- The Redis-to-Parquet consumer (critical missing piece).
- The full schema of monitoring metric payloads.
- How `check_meta_access()` authorization works (what roles have access).
- How `DataEstimator._to_duckdb_path()` resolves paths (presigned URLs? S3 direct?).
- What API or UI exposes `MonitoringReader.read()` to operators.
- Whether there are other `monitor_type` values beyond `"plans"`.
- Retention/cleanup policy for monitoring Parquet files.
- Whether monitoring data feeds into billing, alerting, or automated actions.
- The Parquet file partitioning scheme (Hive partitioning is enabled but partition keys are unknown).

---

## 14. AI Consumption Notes

- **Canonical feature name:** `Supertable Monitoring Subsystem`
- **Alternative names / aliases:** monitoring writer, monitoring reader, monitoring logger, stats logging, query plan monitoring
- **Main responsibilities:** (1) Non-blocking metric ingestion with async Redis shipping, (2) DuckDB-based historical monitoring data query over Parquet files
- **Important entities:** `MonitoringWriter`, `_AsyncMonitoringLogger`, `NullMonitoringLogger`, `MonitoringReader`, `_MonitorKey`, `MonitoringLogger` (Protocol), stats catalog (`_stats.json`), metric payload, queue stats
- **Important workflows:** metric ingestion (log → queue → batch → Redis), monitoring read (RBAC → catalog → key selection → DuckDB parquet_scan → DataFrame)
- **Integration points:** Redis (message buffer), object storage (Parquet persistence), DuckDB (query engine), `DataEstimator` (path resolution), `DuckDBExecutor` (S3/httpfs config), `check_meta_access` (RBAC)
- **Business purpose keywords:** operational observability, query monitoring, multi-tenant metrics, performance diagnostics, usage tracking
- **Architecture keywords:** producer-consumer, async buffering, bounded queue, batch shipping, null-object pattern, singleton cache, catalog-based discovery, Parquet analytics
- **Follow-up files for completeness:**
  - Redis-to-Parquet consumer (the missing middle of the pipeline)
  - `supertable.redis_connector` — Redis client wrapper
  - `supertable.engine.data_estimator` — path resolution logic
  - `supertable.engine.duckdb_transient` — DuckDB executor configuration
  - `supertable.rbac.access_control` — RBAC implementation
  - `supertable.storage.storage_factory` — storage abstraction
  - Any API layer or admin endpoint that calls `MonitoringReader.read()`
  - Examples referenced: `examples/2.4.2. write_monitoring_parallel.py`

---

## 15. Suggested Documentation Tags

`monitoring`, `observability`, `metrics-ingestion`, `query-monitoring`, `redis`, `duckdb`, `parquet`, `async-processing`, `background-worker`, `bounded-queue`, `batch-shipping`, `multi-tenant`, `graceful-degradation`, `null-object-pattern`, `singleton-cache`, `RBAC`, `storage-abstraction`, `time-series-query`, `operational-visibility`, `platform-infrastructure`, `data-pipeline`, `producer-consumer`, `backward-compatibility`, `context-manager`

---

## Merge Readiness

- **Suggested canonical name:** `Supertable Monitoring Subsystem`
- **Standalone or partial:** **Partial.** The write side and read side are present, but the critical middle piece (Redis consumer → Parquet writer → catalog maintenance) is missing.
- **Related components to merge later:**
  - Redis-to-Parquet consumer/worker
  - `DataEstimator` and `DuckDBExecutor` (shared infrastructure)
  - RBAC (`check_meta_access`) module
  - Storage factory and storage backends
  - API/admin layer that exposes monitoring reads
  - Any alerting or dashboard system consuming monitoring data
- **What would most improve certainty:**
  1. The Redis-to-Parquet worker code
  2. The metric payload schema documentation or examples
  3. The API endpoint that exposes `MonitoringReader.read()`
  4. The Parquet partitioning scheme
- **Recommended merge key phrases:** `monitoring`, `MonitoringWriter`, `MonitoringReader`, `_stats.json`, `monitor_type`, `execution_time`, `query plan monitoring`, `supertable monitoring`
