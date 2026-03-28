# Feature / Component Reverse Engineering

## 1. Executive Summary

This code implements the **core data ingestion and storage engine** for the **Supertable** product — a multi-tenant, versioned, columnar data platform built on Parquet files with Redis-backed metadata coordination.

The central component is `DataWriter`, which orchestrates the full write path: validating incoming Arrow data, acquiring distributed locks, detecting overlapping files via column-level min/max statistics, performing merge/overwrite/delete operations on Parquet files, updating versioned snapshot metadata atomically via Redis Lua scripts, optionally mirroring to Delta/Iceberg/Parquet formats, emitting monitoring metrics asynchronously, and triggering downstream data quality checks.

The system implements a **log-structured merge (LSM)-style storage pattern** where data is organized into `SuperTable → SimpleTable` hierarchies, with each SimpleTable maintaining an append-only chain of versioned snapshots pointing to Parquet data files. Concurrent write safety is enforced through Redis-based distributed locking with compare-and-swap (CAS) leaf pointer updates.

**Business purpose**: This is the foundational data persistence layer of a commercial data platform, enabling multi-tenant data ingestion with ACID-like semantics (atomicity via snapshot versioning, isolation via distributed locks, durability via Parquet on object storage). It supports RBAC-gated writes, deduplication-on-read via primary keys, idempotent ingestion via `newer_than` filtering, soft-delete (tombstone) semantics, and automatic small-file compaction.

**Technical role**: Write-path orchestrator and storage engine sitting between API/ingest layers (upstream) and query/read layers (downstream), coordinating between object storage (MinIO/local), Redis (metadata catalog), and optional Spark/Delta mirror systems.

---

## 2. Functional Overview

### Product Capabilities Enabled

| Capability | Description |
|---|---|
| **Multi-tenant data ingestion** | Organizations write data into named tables (SimpleTables) within named databases (SuperTables), with RBAC enforcement at every write |
| **Overlap-aware upsert** | Incoming data is merged with existing Parquet files based on composite key columns (overwrite_columns), replacing matched rows while preserving unmatched rows |
| **Idempotent ingestion** | The `newer_than` parameter allows replayed or out-of-order data to be automatically skipped if existing data is already newer |
| **Soft deletes (tombstones)** | When `dedup_on_read` is configured with primary keys, deletes are recorded as tombstone entries rather than rewriting files, with automatic compaction when thresholds are breached |
| **Physical deletes** | The `delete_only` mode removes rows matching specified key columns from Parquet files by rewriting them |
| **Automatic compaction** | Small non-overlapping files are automatically merged when file count or total size exceeds configurable thresholds |
| **Table configuration** | Per-table settings for primary keys, dedup behavior, memory limits, file count limits, and tombstone compaction thresholds |
| **Schema evolution** | Union-based schema alignment automatically handles column additions and type promotions across data batches |
| **Format mirroring** | After writes, data can be optionally mirrored to Delta Lake, Iceberg, or standalone Parquet formats |
| **Write monitoring** | Every write operation emits detailed metrics (rows inserted/deleted, duration, lineage) asynchronously to Redis for observability |
| **Data quality scheduling** | After successful writes, a debounced notification triggers downstream data quality checks |
| **Versioned snapshots** | Each write produces a new immutable snapshot with a linked history chain, enabling time-travel and audit capabilities |
| **Data lineage tracking** | Optional lineage metadata (source_type, source_id, job_id, etc.) is recorded with each write for traceability |

### Target Users / Actors

- **Data engineers** — configure tables, write data via API
- **ETL pipelines / staging ingestion** — automated data loading through `DataWriter.write()`
- **Administrators** — manage SuperTables, configure RBAC, delete tables
- **Query engines (DuckDB)** — downstream consumers of the Parquet files and snapshot metadata
- **Monitoring systems** — consume async metrics from Redis lists

### Business Value

This is a **core revenue-critical component** — without it, no data enters the platform. It implements the entire write-side contract that makes Supertable usable as a data warehouse or lakehouse product. The dedup-on-read, tombstone, idempotency, and compaction features collectively provide a **managed lakehouse experience** without requiring users to operate Apache Spark, Delta Lake, or Iceberg infrastructure directly.

---

## 3. Technical Overview

### Architectural Style

- **Log-structured merge tree (LSM) variant**: Data files are immutable Parquet; writes produce new files and sunset old ones; compaction merges small files
- **Snapshot-based MVCC**: Each write creates a new snapshot JSON referencing the current set of data files, with a pointer to the previous snapshot
- **Redis as coordination plane**: All metadata (pointers, locks, config, RBAC) lives in Redis; heavy data lives in object storage
- **Compare-and-swap (CAS) via Lua scripts**: Atomic version bumps on leaf and root pointers prevent lost updates
- **Distributed locking**: Per-table Redis locks with TTL, timeout, and compare-and-delete semantics

### Major Control Flow (DataWriter.write)

1. RBAC access check
2. Convert Arrow → Polars DataFrame
3. Inject `__timestamp__` if dedup-on-read is configured
4. Validate inputs (table name, columns, constraints)
5. Acquire per-table distributed Redis lock (30s TTL, 60s timeout)
6. Read current snapshot via Redis leaf pointer
7. Find overlapping files using column min/max statistics
8. Filter stale incoming rows via `newer_than` column (optional)
9. Process tombstone/soft-delete path OR physical merge/delete path
10. Write new Parquet files to object storage
11. Update snapshot metadata
12. CAS-set leaf pointer + atomic root version bump
13. Mirror to Delta/Iceberg if enabled
14. Release lock
15. Enqueue monitoring metrics (async, outside lock)
16. Notify data quality scheduler (debounced)

### Key Design Patterns

- **Optimistic concurrency control**: CAS on Redis leaf pointers
- **File-level statistics (zonemaps)**: Min/max per column enable overlap detection without reading file contents
- **Anti-join merging**: Polars anti-joins on composite keys for row-level upsert semantics
- **Bounded async monitoring**: Background thread with bounded queue, batch shipping to Redis, graceful degradation
- **Singleton cache with LRU eviction**: Monitoring loggers are cached per-key with bounded pool size
- **Sentinel-aware Redis connectivity**: Supports Redis Sentinel with automatic fallback to direct connection

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `data_writer.py` | Write-path orchestrator | `DataWriter`, `write()`, `configure_table()`, `validation()` | **Critical** — main entry point for all data ingestion |
| `processing.py` | Parquet merge/rewrite engine | `process_overlapping_files()`, `process_delete_only()`, `find_overlapping_files()`, `filter_stale_incoming_rows()`, `compact_tombstones()`, `write_parquet_and_collect_resources()` | **Critical** — core data processing logic |
| `simple_table.py` | Table-level storage abstraction | `SimpleTable`, `init_simple_table()`, `update()`, `get_simple_table_snapshot()`, `delete()` | **Critical** — manages per-table snapshot chain |
| `super_table.py` | Database-level coordination | `SuperTable`, `init_super_table()`, `read_simple_table_snapshot()`, `delete()` | **Important** — manages database-level metadata and storage |
| `redis_catalog.py` | Redis metadata catalog | `RedisCatalog`, CAS Lua scripts, RBAC ops, staging/pipe ops, Spark cluster ops, auth tokens | **Critical** — all metadata coordination and distributed locking |
| `redis_connector.py` | Redis connection management | `RedisOptions`, `create_redis_client()`, `RedisConnector` | **Important** — Sentinel-aware connection factory |
| `monitoring_writer.py` | Async metrics pipeline | `MonitoringWriter`, `_AsyncMonitoringLogger`, `NullMonitoringLogger`, `get_monitoring_logger()` | **Important** — decoupled observability |

---

## 5. Detailed Functional Capabilities

### 5.1 Data Ingestion with Overlap Handling

- **Description**: Accepts Arrow tables and writes them as Parquet files, merging with existing data based on composite key columns
- **Business purpose**: Core data write capability; enables upsert semantics without full table rewrites
- **Trigger/input**: `DataWriter.write(role_name, simple_name, data, overwrite_columns, ...)`
- **Processing**: Converts Arrow→Polars; finds overlapping files via statistics; anti-joins existing rows against incoming keys; concatenates non-deleted rows with incoming data; writes new Parquet files sorted by `__timestamp__` and key columns
- **Output**: Tuple `(total_columns, total_rows, inserted, deleted)` plus updated snapshot in storage and Redis
- **Dependencies**: Polars, PyArrow, storage backend, RedisCatalog
- **Constraints**: Requires exclusive per-table lock; overwrite_columns must exist in DataFrame; table names must match `^[A-Za-z_][A-Za-z0-9_]*$`
- **Risks**: Lock timeout (60s) under contention; large overlapping file sets cause high memory usage
- **Confidence**: Explicit

### 5.2 Idempotent Ingestion (newer_than)

- **Description**: Filters out incoming rows whose timestamp/version is not newer than existing data for the same key
- **Business purpose**: Enables replay-safe ingestion pipelines; prevents stale data from overwriting newer data
- **Trigger/input**: `newer_than` parameter specifying a column name used for freshness comparison
- **Processing**: Left-joins incoming data against max(newer_than_col) per key from overlapping files; drops rows where incoming ≤ existing
- **Output**: Filtered DataFrame; `skipped_stale` count in monitoring
- **Dependencies**: `filter_stale_incoming_rows()` in processing.py
- **Constraints**: Requires `overwrite_columns` to be set; newer_than column must exist in incoming data
- **Risks**: Files missing the newer_than column are treated as allowing overwrites (permissive default)
- **Confidence**: Explicit

### 5.3 Soft Deletes (Tombstones)

- **Description**: When dedup-on-read is configured with primary keys, `delete_only=True` appends key tuples to a tombstone list rather than rewriting files
- **Business purpose**: Fast logical deletes without expensive file I/O; deferred physical removal via compaction
- **Trigger/input**: `delete_only=True` with `dedup_on_read` and `primary_keys` configured
- **Processing**: Extracts key tuples from incoming DataFrame; merges with existing tombstone list; deduplicates; triggers compaction if threshold breached
- **Output**: Tombstone block stored in snapshot: `{"primary_keys": [...], "deleted_keys": [...], "total_tombstones": N}`
- **Dependencies**: `extract_key_tuples()`, `compact_tombstones()`, `_tombstone_threshold()`
- **Constraints**: Default compaction threshold is 1000 tombstones; configurable via `tombstone_compact_total`
- **Risks**: Unbounded tombstone growth if compaction fails; tombstone reconciliation on insert could miss keys if primary_keys change
- **Confidence**: Explicit

### 5.4 Tombstone Compaction

- **Description**: When tombstone count exceeds threshold, physically removes tombstoned rows from Parquet files
- **Business purpose**: Reclaims storage and prevents read-side performance degradation from large tombstone lists
- **Trigger/input**: `len(merged_tombstones) >= threshold` during a soft-delete write
- **Processing**: Builds a Polars DataFrame of tombstone keys; iterates all resource files; uses min/max statistics to skip files that cannot contain tombstoned keys; anti-joins to remove rows; rewrites modified files
- **Output**: `(compacted_rows, new_resources, sunset_files)`; clears tombstone list after successful compaction
- **Dependencies**: `_tombstone_overlaps_stats()`, `write_parquet_and_collect_resources()`
- **Constraints**: Requires all primary key columns present in data files; stats-based pruning is conservative (false positives possible, no false negatives)
- **Confidence**: Explicit

### 5.5 Tombstone Reconciliation on Insert

- **Description**: When new data is written (non-delete) that matches existing tombstone keys, those keys are removed from the tombstone list so the new rows become visible
- **Business purpose**: Ensures re-inserting a previously-deleted key works correctly without manual tombstone management
- **Trigger/input**: Any non-delete write when existing tombstones and primary keys are present
- **Processing**: Extracts incoming key tuples; filters tombstone list via set difference
- **Output**: Pruned tombstone list stored in snapshot
- **Confidence**: Explicit

### 5.6 Automatic Small-File Compaction

- **Description**: Merges small non-overlapping Parquet files when total size or file count exceeds configurable thresholds
- **Business purpose**: Prevents query performance degradation from many small files (the "small files problem" in data lakes)
- **Trigger/input**: Automatic during write when `total_size > MAX_MEMORY_CHUNK_SIZE` or `file_count >= MAX_OVERLAPPING_FILES`
- **Processing**: Reads small files, concatenates with union-schema alignment, writes merged output, sunsets originals. Also compacts "skipped" overlapping files (those flagged as overlapping but with zero actual row matches) when their count exceeds MAX_OVERLAPPING_FILES.
- **Output**: Fewer, larger Parquet files replacing many small ones
- **Dependencies**: `process_files_without_overlap()`, `prune_not_overlapping_files_by_threshold()`
- **Constraints**: Configurable per-table via `max_memory_chunk_size` and `max_overlapping_files`; defaults from global config
- **Risks**: Large compaction batches may cause memory pressure
- **Confidence**: Explicit

### 5.7 Table Configuration (Dedup-on-Read)

- **Description**: Persists per-table settings for primary keys, dedup behavior, memory limits, file limits, and tombstone thresholds
- **Business purpose**: Enables customization of write behavior per table; supports deduplication semantics without schema changes
- **Trigger/input**: `DataWriter.configure_table(role_name, simple_name, primary_keys, dedup_on_read, ...)`
- **Processing**: Validates inputs; merges with existing config; stores in Redis; caches locally
- **Output**: Config stored at `supertable:{org}:{sup}:meta:table_config:{simple}` in Redis
- **Dependencies**: `RedisCatalog.set_table_config()`, `RedisCatalog.get_table_config()`
- **Constraints**: primary_keys must be non-empty list; limit overrides must be positive integers
- **Confidence**: Explicit

### 5.8 Versioned Snapshot Management

- **Description**: Each write produces an immutable snapshot JSON containing the list of data file resources, schema, version number, timestamp, and link to previous snapshot
- **Business purpose**: Enables version tracking, audit trail, and potential time-travel queries; provides atomic state transitions
- **Trigger/input**: Every successful write
- **Processing**: Builds updated resource list (adding new, removing sunset); increments version; collects schema; writes JSON to storage; updates Redis leaf pointer via CAS Lua script; bumps root version
- **Output**: Snapshot JSON on storage + Redis leaf pointer with optional inline payload (avoids storage reads for metadata-only queries)
- **Constraints**: Snapshot payload in Redis strips per-file stats to reduce storage (~73% reduction)
- **Confidence**: Explicit

### 5.9 Async Monitoring / Metrics Pipeline

- **Description**: Decoupled, non-blocking metric emission via bounded in-memory queue and background daemon thread shipping to Redis lists
- **Business purpose**: Provides write operation observability without impacting write latency or holding data locks during metric persistence
- **Trigger/input**: After lock release in `DataWriter.write()`
- **Processing**: Metrics are enqueued (never blocks; drops on full queue); background thread drains in batches (up to 200 items) and pushes to Redis via pipeline; context manager `__exit__` triggers flush
- **Output**: JSON payloads in Redis list `monitor:{org}:{super_name}:{type}`
- **Dependencies**: `MonitoringWriter`, `_AsyncMonitoringLogger`, Redis
- **Constraints**: Queue bounded at 10,000 items; singleton cache bounded at 256 loggers (env-configurable); dropped metrics counted in stats
- **Risks**: Short-lived processes (Lambda, CLI) may lose metrics if daemon thread doesn't flush before exit; mitigated by `_try_flush()` in `__exit__`
- **Confidence**: Explicit

### 5.10 Redis Sentinel Support with Graceful Fallback

- **Description**: Redis connection factory supports Sentinel mode for high availability with configurable strict/fallback behavior
- **Business purpose**: Production-grade Redis connectivity supporting HA deployments without code changes
- **Trigger/input**: Environment variables: `SUPERTABLE_REDIS_SENTINEL=true`, `SUPERTABLE_REDIS_SENTINELS`, `SUPERTABLE_REDIS_SENTINEL_MASTER`
- **Processing**: Attempts Sentinel connection with 3s deadline and 200ms retry; on failure, falls back to direct Redis unless `SUPERTABLE_REDIS_SENTINEL_STRICT=true`
- **Output**: `redis.Redis` client instance
- **Constraints**: Sentinel socket timeout 500ms; fail-fast probe with up to 3s total wait
- **Risks**: Comprehensive error message includes Docker-specific troubleshooting hints; dual-failure scenario (both Sentinel and direct Redis down) raises `redis.ConnectionError`
- **Confidence**: Explicit

### 5.11 RBAC-Gated Write Access

- **Description**: Every write and configure operation is gated by `check_write_access()` which verifies the caller's role has write permission for the target table
- **Business purpose**: Multi-tenant security; prevents unauthorized data modifications
- **Trigger/input**: `role_name` parameter on `write()` and `configure_table()`
- **Processing**: Delegates to `supertable.rbac.access_control.check_write_access()`
- **Constraints**: Access check happens before any data processing
- **Confidence**: Explicit (import and call visible; implementation in external module)

### 5.12 Format Mirroring (Delta / Iceberg / Parquet)

- **Description**: After successful write and leaf pointer update, data can be optionally mirrored to Delta Lake, Iceberg, or standalone Parquet formats
- **Business purpose**: Interoperability with external tools (Spark, Presto, Trino) without requiring them to understand Supertable's native format
- **Trigger/input**: Mirror configuration stored in Redis (`meta:mirrors`); triggered automatically on write
- **Processing**: `MirrorFormats.mirror_if_enabled()` called after CAS leaf update; failures are logged but do not fail the write
- **Output**: Data in Delta/Iceberg/Parquet format on storage
- **Constraints**: Mirroring is best-effort; errors are swallowed
- **Confidence**: Explicit (call visible; implementation in external module `supertable.mirroring.mirror_formats`)

### 5.13 Data Quality Notification

- **Description**: After a successful write, a debounced notification is sent to a data quality scheduler via Redis
- **Business purpose**: Triggers automated data quality checks (profiling, anomaly detection) after fresh data arrives
- **Trigger/input**: Successful completion of `write()`, after lock release
- **Processing**: `notify_ingest()` sets a debounced "pending" flag in Redis; DQ scheduler picks it up on next tick
- **Constraints**: Best-effort; exceptions are silently caught to never fail a write
- **Confidence**: Explicit (call visible; implementation in external module `supertable.reflection.quality.scheduler`)

---

## 6. Classes, Functions, and Methods

### DataWriter (`data_writer.py`)

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `DataWriter.__init__` | method | Initialize writer with SuperTable context | `super_name: str, organization: str` | — | Creates SuperTable, RedisCatalog; initializes config cache | Critical |
| `DataWriter.configure_table` | method | Set per-table config (primary keys, dedup, limits) | `role_name, simple_name, primary_keys, dedup_on_read, max_memory_chunk_size, max_overlapping_files, tombstone_compact_total` | None | Writes config to Redis; updates local cache | Important |
| `DataWriter._get_table_config` | method | Retrieve table config with local caching | `simple_name: str` | `dict` | Populates cache on miss | Supporting |
| `DataWriter.write` | method | Full write-path orchestrator | `role_name, simple_name, data, overwrite_columns, compression_level, newer_than, delete_only, lineage` | `(total_columns, total_rows, inserted, deleted)` | Writes Parquet files; updates Redis metadata; emits monitoring; notifies DQ scheduler | Critical |
| `DataWriter.validation` | method | Input validation | `dataframe, simple_name, overwrite_columns, newer_than, delete_only` | None (raises on error) | — | Important |
| `_safe_json` | function | Non-raising JSON serializer | `obj` | `str` | — | Supporting |

### SimpleTable (`simple_table.py`)

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `SimpleTable.__init__` | method | Initialize table-level abstraction; bootstrap if needed | `super_table: SuperTable, simple_name: str` | — | May create storage dirs and initial snapshot | Critical |
| `SimpleTable.init_simple_table` | method | First-time initialization: create dirs, bootstrap empty snapshot | — | None | Writes empty snapshot JSON; sets Redis leaf via CAS | Important |
| `SimpleTable.get_simple_table_snapshot` | method | Read current snapshot via Redis leaf pointer | — | `(snapshot_dict, path)` | Reads from Redis (fast path) or storage (fallback) | Critical |
| `SimpleTable.update` | method | Build and write new snapshot after data changes | `new_resources, sunset_files, model_df, last_snapshot, last_snapshot_path` | `(snapshot_dict, snapshot_path)` | Writes new snapshot JSON to storage | Critical |
| `SimpleTable.delete` | method | Delete table from storage and Redis | `role_name: str` | None | Deletes storage folder and Redis keys | Important |
| `_spark_type_from_polars_dtype` | function | Map Polars dtype → Spark/Delta type string | `dtype` | `str` | — | Supporting |
| `_schema_list_from_polars_df` | function | Build Delta-friendly schema from Polars DataFrame | `model_df` | `List[Dict]` | — | Supporting |

### SuperTable (`super_table.py`)

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `SuperTable.__init__` | method | Initialize database-level coordination object | `super_name: str, organization: str` | — | Gets storage; creates Redis root if needed; initializes RBAC | Critical |
| `SuperTable.init_super_table` | method | First-time: create storage dir, bootstrap Redis root | — | None | `storage.makedirs()`; `catalog.ensure_root()` | Important |
| `SuperTable.read_simple_table_snapshot` | method | Read heavy snapshot JSON from storage | `simple_table_path: str` | `Dict[str, Any]` | — | Important |
| `SuperTable.delete` | method | Delete entire SuperTable (storage + all Redis keys) | `role_name` | None | Deletes storage tree and all Redis keys via SCAN | Critical (destructive) |

### RedisCatalog (`redis_catalog.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `RedisCatalog.__init__` | method | Connect to Redis; register Lua scripts; initialize locker | Critical |
| `acquire_simple_lock` / `release_simple_lock` | methods | Distributed per-table write lock | Critical |
| `set_leaf_path_cas` / `set_leaf_payload_cas` | methods | Atomic CAS update of leaf pointer (with optional inline payload) | Critical |
| `bump_root` | method | Atomic version increment on root pointer | Critical |
| `get_leaf` / `leaf_exists` / `get_root` / `root_exists` | methods | Metadata reads | Important |
| `set_table_config` / `get_table_config` | methods | Per-table configuration persistence | Important |
| `get_mirrors` / `set_mirrors` / `enable_mirror` / `disable_mirror` | methods | Mirror format management | Important |
| `rbac_create_role` / `rbac_delete_role` / `rbac_create_user` / etc. | methods | Full RBAC CRUD with atomic Lua scripts | Important |
| `create_auth_token` / `validate_auth_token` / `delete_auth_token` | methods | SHA-256-hashed bearer token management | Important |
| `upsert_staging_meta` / `upsert_pipe_meta` / etc. | methods | Staging/pipe metadata for UI listing | Supporting |
| `register_spark_cluster` / `select_spark_cluster` / etc. | methods | Spark Thrift cluster registry and selection | Supporting |
| `register_spark_plug` / `list_spark_plugs` / etc. | methods | Spark Plug (PySpark notebook runtime) registry | Supporting |
| `scan_leaf_keys` / `scan_leaf_items` | methods | SCAN-based listing with pipeline batching | Supporting |
| `delete_simple_table` / `delete_super_table` | methods | Metadata cleanup via targeted or SCAN-based deletion | Important |

### Processing (`processing.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `find_overlapping_files` | function | Build candidate file set using column statistics for overlap detection | Critical |
| `process_overlapping_files` | function | Full merge pipeline: compact non-overlapping, merge overlapping, write output | Critical |
| `process_files_with_overlap` | function | Anti-join existing rows against incoming keys; merge survivors | Critical |
| `process_files_without_overlap` | function | Read and concatenate small non-overlapping files for compaction | Important |
| `process_delete_only` | function | Per-file row deletion via anti-join; independent file rewrites | Important |
| `write_parquet_and_collect_resources` | function | Write Parquet with zstd compression, statistics, and row-group sizing; collect resource metadata | Critical |
| `collect_column_statistics` | function | Vectorized min/max computation for all columns | Important |
| `filter_stale_incoming_rows` | function | Newer-than idempotency filtering | Important |
| `extract_key_tuples` / `reconcile_tombstones` / `compact_tombstones` | functions | Tombstone lifecycle management | Important |
| `concat_with_union` / `_union_schema` / `_align_to_schema` | functions | Schema-safe DataFrame concatenation with type promotion | Important |
| `_resolve_limits` | function | Per-table or global limit resolution | Supporting |

### MonitoringWriter / _AsyncMonitoringLogger (`monitoring_writer.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `MonitoringWriter` | class | Backward-compatible facade for metric logging | Important |
| `_AsyncMonitoringLogger` | class | Core async logger: bounded queue, batch shipping, background thread | Important |
| `NullMonitoringLogger` | class | No-op logger for disabled/failed monitoring | Supporting |
| `get_monitoring_logger` | function | Singleton factory with LRU eviction | Supporting |

### RedisConnector / RedisOptions (`redis_connector.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `RedisOptions` | dataclass | Environment-driven Redis config (URL, split vars, Sentinel) | Important |
| `create_redis_client` | function | Sentinel-aware connection factory with fallback | Important |
| `RedisConnector` | class | Thin wrapper holding a Redis client | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Standard Write (Upsert with Overwrite Columns)

1. **Trigger**: `DataWriter.write(role_name, table_name, arrow_data, overwrite_columns=["id"])`
2. **RBAC check**: `check_write_access()` verifies role has write permission
3. **Convert**: Arrow table → Polars DataFrame; record incoming rows/columns
4. **Dedup timestamp**: If table has `dedup_on_read=True`, inject `__timestamp__` column with current UTC time
5. **Validate**: Table name regex, column existence, parameter consistency
6. **Acquire lock**: Redis `SET NX EX` on `supertable:{org}:{sup}:lock:leaf:{table}` with 30s TTL, up to 60s wait
7. **Read snapshot**: Get Redis leaf → extract `path` → read snapshot JSON (use inline payload if available)
8. **Find overlaps**: For each resource file, compare incoming unique values against file's min/max stats per overwrite column; classify as `(file, has_overlap, file_size)`
9. **Newer-than filter** (optional): Left-join incoming vs existing max values per key; drop stale rows
10. **Tombstone reconciliation**: If existing tombstones match incoming keys, remove those tombstones
11. **Process overlapping files**:
    - Phase 1: Read & compact non-overlapping small files (if threshold met)
    - Phase 2: For each overlapping file, anti-join against incoming keys → merge survivors with incoming data
    - Phase 3: If too many "skipped" files accumulated, compact them
    - Final: Flush remaining merged data as Parquet
12. **Write Parquet**: Sorted by `__timestamp__` + key columns; zstd compressed; statistics enabled; row group size 122,880
13. **Update snapshot**: Increment version; update schema (with schemaString for Delta); reference new resources; sunset old ones
14. **CAS leaf update**: Lua script atomically increments leaf version and stores new path + payload
15. **Bump root**: Lua script atomically increments root version
16. **Mirror** (optional): `MirrorFormats.mirror_if_enabled()` — best-effort
17. **Release lock**: Compare-and-delete via Lua
18. **Emit monitoring**: Enqueue stats payload to `MonitoringWriter`; context manager `__exit__` triggers flush
19. **Notify DQ**: `notify_ingest()` sets debounced pending flag in Redis
20. **Return**: `(total_columns, total_rows, inserted, deleted)`

**Failure modes**:
- Lock timeout → `TimeoutError` raised; no data written
- Snapshot not found → `FileNotFoundError`
- Redis CAS failure → exception propagated; lock still released in `finally`
- Parquet write failure → caught, falls back to local write
- Monitoring failure → logged, never fails the write
- Mirror failure → logged, never fails the write
- DQ notification failure → silently caught

**Observability**: Detailed per-stage timing logged at INFO level; monitoring payload includes query_id, durations, row counts, lineage

### 7.2 Soft Delete (Tombstone Path)

1. **Trigger**: `write(delete_only=True)` with `dedup_on_read` and `primary_keys` configured
2. Steps 1–9 same as standard write
3. **Extract keys**: Get unique key tuples from incoming DataFrame
4. **Merge tombstones**: Append to existing tombstone list; deduplicate
5. **Check threshold**: If `len(tombstones) >= tombstone_compact_total`
   - Read all resource files whose stats overlap with tombstone keys
   - Anti-join to physically remove tombstoned rows
   - Write compacted files; sunset originals; clear tombstone list
6. **Store tombstones**: Update snapshot with tombstone block
7. Steps 12–20 same as standard write

### 7.3 Table Initialization

1. **Trigger**: `SimpleTable.__init__()` when Redis leaf does not exist
2. Create storage directories (best-effort; object storage may no-op)
3. Generate initial empty snapshot JSON (version=0, no resources, empty schema)
4. Write snapshot to storage
5. CAS-set Redis leaf pointer to the snapshot path (with inline payload if supported)

### 7.4 SuperTable Initialization

1. **Trigger**: `SuperTable.__init__()` when Redis root does not exist
2. Create base storage directory
3. Initialize Redis root pointer (version=0) via `ensure_root()`
4. Initialize RBAC scaffolding: `RoleManager`, `UserManager`

---

## 8. Data Model and Information Flow

### Core Entities

**Snapshot JSON** (stored on object storage, optionally cached in Redis leaf):
```json
{
  "simple_name": "orders",
  "location": "org/super/tables/orders",
  "snapshot_version": 42,
  "last_updated_ms": 1711612345000,
  "previous_snapshot": "org/super/tables/orders/snapshots/prev.json",
  "schema": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}],
  "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
  "resources": [
    {
      "file": "org/super/tables/orders/data/data_abc123.parquet",
      "file_size": 1048576,
      "rows": 50000,
      "columns": 8,
      "stats": {"id": {"min": 1, "max": 9999}, "date": {"min": "2024-01-01", "max": "2024-12-31"}}
    }
  ],
  "tombstones": {
    "primary_keys": ["id"],
    "deleted_keys": [[42], [99]],
    "total_tombstones": 2
  }
}
```

**Redis Leaf Pointer** (`supertable:{org}:{sup}:meta:leaf:{simple}`):
```json
{
  "version": 42,
  "ts": 1711612345000,
  "path": "org/super/tables/orders/snapshots/snapshot_42.json",
  "payload": { /* stripped snapshot (no per-resource stats) */ }
}
```

**Redis Root Pointer** (`supertable:{org}:{sup}:meta:root`):
```json
{"version": 157, "ts": 1711612345000}
```

**Table Config** (`supertable:{org}:{sup}:meta:table_config:{simple}`):
```json
{
  "primary_keys": ["id"],
  "dedup_on_read": true,
  "max_memory_chunk_size": 16777216,
  "max_overlapping_files": 100,
  "tombstone_compact_total": 1000,
  "modified_ms": 1711612345000
}
```

**Monitoring Payload** (pushed to Redis list `monitor:{org}:{sup}:stats`):
```json
{
  "query_id": "uuid",
  "recorded_at": "2024-03-28T12:00:00+00:00",
  "organization": "org",
  "super_name": "super",
  "table_name": "orders",
  "role_name": "admin",
  "incoming_rows": 1000,
  "inserted": 950,
  "deleted": 50,
  "total_rows": 10950,
  "duration": 1.234,
  "lineage": "{\"source_type\":\"staging_ingest\"}"
}
```

### Information Flow

```
Arrow Table (input)
    → Polars DataFrame (in-memory)
    → Overlap detection via Redis-cached statistics
    → Anti-join merge with existing Parquet (from object storage)
    → New Parquet file (to object storage, zstd compressed)
    → New Snapshot JSON (to object storage)
    → Redis Leaf Pointer CAS update (atomic)
    → Redis Root Pointer bump (atomic)
    → Monitoring payload → Redis list (async)
    → DQ notification → Redis flag (debounced)
```

### Storage Layout

```
{organization}/
  {super_name}/
    super/                          ← SuperTable metadata dir
    tables/
      {simple_name}/
        data/
          data_{uuid}.parquet       ← Data files (immutable)
        snapshots/
          tables_{uuid}.json        ← Snapshot files (immutable)
```

---

## 9. Dependencies and Integrations

### External Dependencies

| Dependency | Type | Purpose |
|---|---|---|
| `redis` (redis-py) | Third-party | Metadata coordination, locking, monitoring transport |
| `redis.sentinel` | Third-party | High-availability Redis connectivity |
| `polars` | Third-party | In-memory columnar DataFrame processing; schema alignment; anti-joins |
| `pyarrow` + `pyarrow.parquet` | Third-party | Parquet file I/O with zstd compression and statistics |
| `dotenv` | Third-party | Environment variable loading from `.env` files |

### Internal Dependencies

| Module | Purpose |
|---|---|
| `supertable.config.defaults` | Logger instance; global default config values (`MAX_MEMORY_CHUNK_SIZE`, `MAX_OVERLAPPING_FILES`) |
| `supertable.config.homedir` | Application home directory initialization |
| `supertable.storage.storage_factory` / `storage_interface` | Abstract storage backend (MinIO, local filesystem, etc.) |
| `supertable.locking.redis_lock` | `RedisLocking` — compare-and-delete distributed lock implementation |
| `supertable.rbac.access_control` | `check_write_access()` — RBAC enforcement |
| `supertable.rbac.role_manager` / `user_manager` | RBAC bootstrapping during SuperTable init |
| `supertable.mirroring.mirror_formats` | `MirrorFormats.mirror_if_enabled()` — Delta/Iceberg/Parquet mirroring |
| `supertable.reflection.quality.scheduler` | `notify_ingest()` — Data quality scheduling |
| `supertable.utils.helper` | `generate_filename()`, `collect_schema()` — utility functions |
| `supertable.utils.timer` | `Timer` class for performance measurement |

### Infrastructure Dependencies

| System | Role |
|---|---|
| **Redis** | Metadata catalog, distributed locks, monitoring transport, RBAC, auth tokens, staging/pipe metadata |
| **Object Storage (MinIO / local FS)** | Persistent storage for Parquet data files and snapshot JSONs |
| **Optional: Spark Thrift / Spark Plug** | Registered in Redis for compute offloading (not directly used in write path) |

---

## 10. Architecture Positioning

### Layer Classification

- `DataWriter` — **orchestration layer** (coordinates storage, catalog, processing, monitoring)
- `processing.py` — **domain logic** (merge algorithms, statistics, compaction, tombstones)
- `SimpleTable` / `SuperTable` — **storage abstraction layer** (snapshot management, directory layout)
- `RedisCatalog` — **infrastructure adapter** (Redis-backed metadata catalog with distributed locking)
- `MonitoringWriter` — **infrastructure adapter** (async metrics pipeline)
- `RedisConnector` — **infrastructure adapter** (connection factory)

### Upstream Callers (Inferred)

- REST API layer (likely Flask/FastAPI) handling `/write` endpoints
- Staging/Pipe ingestion framework (`supertable.staging`, `supertable.pipe`)
- Spark jobs and batch pipelines
- Manual/admin tools

### Downstream Consumers

- **Query engine** (likely DuckDB-based): reads Parquet files referenced by snapshots; applies dedup-on-read window function using `__timestamp__` and primary keys; filters tombstoned keys
- **Mirror systems**: Delta Lake, Iceberg, Parquet mirror targets
- **Monitoring dashboard**: consumes Redis list metrics
- **Data quality system**: reacts to `notify_ingest` flags
- **RBAC system**: consumes user/role metadata from Redis

### Boundaries

- **Storage is pluggable**: `StorageInterface` abstraction allows MinIO, S3, local filesystem, etc.
- **Redis is critical**: Without Redis, no metadata coordination, locking, or monitoring is possible
- **Write path is single-process per table**: Distributed lock ensures only one writer per SimpleTable at a time
- **Read path is decoupled**: Readers access immutable Parquet files referenced by the Redis leaf pointer; no lock required for reads

### Coupling

- Tight coupling to Redis (Lua scripts, key naming conventions)
- Tight coupling to Polars for DataFrame operations
- Moderate coupling to PyArrow for Parquet I/O
- Loose coupling to storage backend via interface
- Loose coupling to monitoring (graceful degradation)

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Revenue criticality** | **Highest** — this is the write path; no data ingestion means no product value |
| **Business problem solved** | Managed lakehouse data ingestion with ACID-like semantics, eliminating the need for users to operate Spark/Delta/Iceberg infrastructure |
| **Differentiating vs commodity** | **Differentiating** — the combination of Redis-coordinated metadata, Parquet-native storage, statistics-based overlap detection, tombstone semantics, automatic compaction, and multi-format mirroring is a custom lakehouse implementation not trivially replicated |
| **Loss impact** | Total product failure — no data can be written |
| **Strategic significance** | This is the **storage engine core** of the product; its correctness, performance, and reliability directly determine the product's quality and market viability |
| **Monetization** | Multi-tenancy (`organization`), RBAC, table configuration, and mirror formats are all per-tenant features suitable for tiered pricing |
| **Compliance** | Data lineage tracking, RBAC enforcement, and auth token management support audit and compliance requirements |

---

## 12. Technical Depth Assessment

| Aspect | Assessment |
|---|---|
| **Complexity** | **High** — multiple merge strategies, tombstone lifecycle, concurrent write safety, schema evolution, multi-phase processing |
| **Architectural maturity** | **High** — clear separation of concerns; CAS-based consistency; graceful degradation in monitoring; pluggable storage; Sentinel support |
| **Maintainability** | **Moderate** — `DataWriter.write()` is a 400+ line method with deep nesting; processing.py has clear function decomposition but complex state threading |
| **Extensibility** | **Good** — table config is extensible; mirror formats are plug-in; storage is interface-based; monitoring is decoupled |
| **Performance** | **Carefully optimized** — statistics-based file pruning avoids unnecessary reads; anti-joins instead of row-by-row filtering; sorted Parquet writes for zonemap effectiveness; batch Redis pipelines; configurable memory chunk sizes; row group sizing tuned for DuckDB |
| **Reliability** | **Strong** — CAS prevents lost updates; lock TTL prevents deadlocks; monitoring never fails writes; mirror never fails writes; DQ notification never fails writes; comprehensive error handling with best-effort fallbacks |
| **Security** | RBAC enforced at every write; auth tokens stored as SHA-256 hashes; plaintext token returned only once on creation |
| **Operational sensitivity** | **High** — Redis availability is critical; lock contention under concurrent writes; memory pressure during large compaction; storage availability for Parquet I/O |
| **Testing clues** | No tests visible in provided code; `# pragma: no cover` markers suggest test coverage tooling is used |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Write path uses Redis distributed locks (SET NX EX) with 30s TTL and 60s timeout
- CAS leaf updates use Lua scripts with monotonic version increments
- Parquet files are written with zstd compression, dictionary encoding, and statistics enabled
- Row group size is 122,880 rows
- Monitoring is async, bounded, and non-blocking to the write path
- Tombstone compaction threshold defaults to 1000; configurable per-table
- Schema evolution uses union-based alignment with type promotion rules
- Redis Sentinel mode supports graceful fallback to direct connection
- Inline snapshot payload in Redis leaf strips per-file stats to save ~73% space
- All writes check RBAC access before proceeding
- Data is sorted by `__timestamp__` then overwrite columns before Parquet write
- `newer_than` filtering uses left-join with max aggregation per key group
- Mirror formats supported: DELTA, ICEBERG, PARQUET

### Reasonable Inferences

- The **read path uses DuckDB** — evidenced by Parquet optimization for zonemaps/row-group skipping, and the product name "Supertable" aligning with a DuckDB-based analytics product
- The **dedup_on_read** feature likely wraps queries in a `ROW_NUMBER() OVER (PARTITION BY primary_keys ORDER BY __timestamp__ DESC)` window function on the read side
- The **`StorageInterface`** likely supports S3/MinIO, local filesystem, and possibly GCS/Azure Blob
- The product is **multi-tenant SaaS** — evidenced by `organization` scoping on all operations
- **Staging/Pipe** functionality represents an ETL pipeline framework within the product
- **Spark Thrift/Plug** integration suggests the product can offload heavy compute to external Spark clusters when data exceeds DuckDB capacity

### Unknown / Not Visible in Provided Snippet

- Full `StorageInterface` API and implementations
- `RedisLocking` implementation details (acquire retry/backoff strategy)
- `check_write_access()` implementation and permission model
- `MirrorFormats.mirror_if_enabled()` implementation
- `notify_ingest()` implementation and DQ scheduler behavior
- `collect_schema()` and `generate_filename()` implementations
- Read-path implementation (how queries consume snapshots and apply dedup/tombstone filtering)
- API layer that exposes `DataWriter` to external callers
- Deployment topology (single-node vs distributed)
- Backup and disaster recovery mechanisms
- How `schemaString` is consumed by Delta mirrors
- Whether snapshot chain (previous_snapshot) is used for time-travel queries

---

## 14. AI Consumption Notes

- **Canonical feature name**: `Supertable Data Writer / Storage Engine`
- **Alternative names**: DataWriter, write path, ingestion engine, table writer
- **Main responsibilities**: Data ingestion, Parquet merge/upsert, snapshot versioning, distributed locking, tombstone management, compaction, monitoring, format mirroring
- **Important entities**: `DataWriter`, `SimpleTable`, `SuperTable`, `RedisCatalog`, `MonitoringWriter`, snapshot JSON, resource entries, tombstone block, table config
- **Important workflows**: Standard write (upsert), soft delete (tombstone), physical delete, table initialization, tombstone compaction, small-file compaction, monitoring emission
- **Integration points**: Redis (catalog/locks/monitoring), Object Storage (Parquet/JSON), RBAC system, Mirror system (Delta/Iceberg), DQ scheduler, Spark cluster registry
- **Business purpose keywords**: data ingestion, lakehouse, upsert, ACID, multi-tenant, RBAC, deduplication, compaction, versioning, lineage
- **Architecture keywords**: LSM, CAS, distributed lock, snapshot MVCC, anti-join merge, Parquet, Redis Lua, Sentinel, zonemap statistics
- **Follow-up files**: `supertable/storage/storage_interface.py`, `supertable/storage/storage_factory.py`, `supertable/locking/redis_lock.py`, `supertable/rbac/access_control.py`, `supertable/rbac/role_manager.py`, `supertable/mirroring/mirror_formats.py`, `supertable/reflection/quality/scheduler.py`, `supertable/config/defaults.py`, `supertable/utils/helper.py`, read-path / query engine code

---

## 15. Suggested Documentation Tags

`data-ingestion`, `storage-engine`, `write-path`, `parquet`, `lakehouse`, `redis-catalog`, `distributed-locking`, `snapshot-versioning`, `upsert`, `merge`, `compaction`, `tombstone`, `soft-delete`, `dedup-on-read`, `idempotent-ingestion`, `schema-evolution`, `multi-tenant`, `rbac`, `access-control`, `monitoring`, `async-metrics`, `format-mirroring`, `delta-lake`, `iceberg`, `data-lineage`, `data-quality`, `redis-sentinel`, `cas`, `lua-scripts`, `polars`, `pyarrow`, `zstd-compression`, `zonemaps`, `anti-join`, `object-storage`, `minio`, `spark-integration`, `auth-tokens`, `staging-pipes`, `domain-logic`, `orchestration-layer`, `infrastructure-adapter`, `core-platform`

---

## Merge Readiness

- **Suggested canonical name**: `Supertable Storage Engine — Write Path`
- **Standalone or partial**: **Partial** — this covers the complete write path but the read path, API layer, RBAC implementation, mirror implementation, storage backends, and DQ scheduler are in separate modules
- **Related components to merge**: Read/query engine, StorageInterface implementations, RedisLocking, RBAC (access_control, role_manager, user_manager), MirrorFormats, DQ scheduler, Staging/Pipe framework, API layer
- **Files that would most improve certainty**: `storage_interface.py`, `storage_factory.py`, `redis_lock.py`, `access_control.py`, `mirror_formats.py`, `scheduler.py` (DQ), read-path/query module
- **Recommended merge key phrases**: "supertable write path", "data writer", "storage engine", "redis catalog", "snapshot management", "parquet merge", "tombstone compaction", "monitoring writer"
