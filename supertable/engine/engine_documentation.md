# Supertable Query Engine

## Overview

The query engine executes SQL against Parquet files stored locally or in object
storage. It resolves table references from Redis metadata, prunes physical
files, estimates projected compressed bytes, applies access/deletion policy,
and executes through DuckDB, IslandDB, or Spark.

## Execution Flow

```
query_sql() / DataReader.execute()
  │
  ├─ SQLParser            Parse SQL, extract table aliases + columns
  ├─ restrict_read_access  RBAC gate (raise on deny, return view defs)
  ├─ DataEstimator         Resolve snapshots from Redis, collect files, validate columns
  │    └─ Returns Reflection (files, bytes, freshness_ms, schema)
  ├─ Wire pinned rbac_views + tombstone_views onto Reflection
  ├─ Executor._auto_pick   Choose engine (DuckDB / IslandDB / bounded IslandDB / Spark)
  └─ Engine.execute
       ├─ Create reflection views (parquet_scan)
       ├─ Create deletion-vector view (sealed composite anti-join)
       ├─ Create RBAC view         (column + row filtering)
       ├─ Rewrite SQL table refs → hashed view names
       └─ Execute + return DataFrame
```

## Engines

### DuckDB (`duckdb_engine.py`)
Scoped DuckDB executor with request-private views and cursors. It reuses its
connection and caches only within the same organization and storage
authorization identity. Released DuckDB versions do not expose a safely
bounded reusable on-disk Parquet object cache; the application-owned shared
cache below provides that layer.

### IslandDB (`islanddb.py`)
SuperTable's specialised Parquet executor. It consumes the estimator's exact
surviving files and conservative row-group hints. Local/whole-object paths use
one parallel multi-file scan; remote paths use an Arrow-compatible, sealed
range reader so only requested footer and compressed column ranges are fetched.
The exact SQL predicate is still executed after the hint. IslandDB is
deliberately not a general SQL implementation: a static capability gate rejects
semantics that have not passed DuckDB differential tests. Explicit IslandDB
never silently falls back to DuckDB, so benchmarks cannot report a false native
win.

Current native coverage includes numeric projection/filtering, aggregates,
ordering, conservative inner/left/cross joins, and sealed composite tombstone
anti-joins. RBAC filters, unproved string collation/coercion behavior, typed
empty snapshots, and unsupported SQL stay on DuckDB/Spark.

Decoded input, result, and operator-state estimates are planned against cgroup
CPU/memory limits. Because Polars and Arrow use process-global worker pools,
IslandDB reserves the pool's full CPU width at admission; decoded scan-memory
figures are conservative routing guards, not a per-query allocator ceiling.
Result collection and query-private spill do have hard byte quotas. Large
supported GROUP BY / ORDER BY shapes spill; memory-heavy joins and unsupported
spill shapes route to DuckDB or Spark. A public Arrow batch stream avoids
materialising large results.
For sealed local full-scan ORDER BY plans whose first key has a complete integer
domain, IslandDB range-partitions each bounded scan block with one stable native
scatter, writes each row once, and sorts independent ranges with aggregate-
memory-admitted Arrow C++ workers. Skew, incomplete bounds, unsupported key
types, or insufficient descriptor headroom retain the conservative bounded
fallback. Range-writer buffers are globally charged, and public FixedSizeBinary
payloads are exposed as Arrow Binary without copying their value buffers.
The engine-level streaming entry point is `Executor.execute_stream(...)`; it
returns an `ArrowBatchStream` (also exported from `supertable.engine`) whose
context lifetime owns cache leases, resource reservations, cancellation, and
spill cleanup. Closing the stream or its Arrow reader releases all four.

IslandDB telemetry distinguishes estimator candidates, executable physical
scan-node occurrences, and runtime observations. Candidate files/row groups/
rows and decoded/compressed byte counts are metadata estimates with explicit
completeness flags. Planned units come from the footer metadata used to build
the relation and count repeated scan occurrences; they are not relabelled as
native-runtime counters. Observed files/row groups/rows remain NULL and
`measured=false` until the underlying scanner exposes comparable counters.
The legacy `selected_row_groups`, `logical_scan_bytes`, `decoded_bytes`, and
`peak_memory_bytes` fields remain compatibility aliases with explicit scopes.
Profiles also expose result completion/outcome, producer versus stream/facade
phase timings (some are deliberately nested and therefore non-additive), Linux
process block-I/O provenance, and absolute process RSS
baseline/peak/final samples. The atomically persisted JSON cannot contain the
duration of its own write; that value is available only in the post-commit
query-tokenized in-memory profile and is marked unavailable in the persisted
artifact. The legacy `elapsed_ms` keeps the engine-through-stream-close boundary
used by adaptive history; materialized facade time is reported as a separate
phase so old/new Island and DuckDB engine samples are not silently mixed.

### Shared Parquet cache (`file_cache.py`)
An organization/storage/version-namespaced full-object cache used by both
DuckDB and IslandDB. It uses stable raw resource keys rather than presigned
URLs, bounded streaming downloads, atomic manifest-last publication,
cross-process `flock` singleflight, query-time eviction leases, size/version/
footer seals, and byte-cap/TTL/LRU eviction. LocalStorage is no-copy.

IslandDB populates misses when explicitly selected. DuckDB consults existing
hits without populating cold objects. This is intentional: on a cold selective
remote query, DuckDB range reads may transfer far less than caching the complete
source file. Projection/predicate pushdown reduces decoded/query bytes, not the
first full-object cache fill.

### Spark Thrift (`spark_thrift.py`)
Connects to a remote Spark Thrift Server via PyHive. Registers parquet files as temp views (batched unions for multi-file tables), applies timestamp CAST wrappers for DuckDB-written nanos columns, transpiles SQL from DuckDB dialect to Spark dialect. Per-statement timeout via `_execute_with_stmt_timeout`. Best for work that exceeds the safely admitted single-node plan.

Spark currently admits only snapshots without an active deletion vector. Its
view-registration path cannot yet preserve the protected
`__supertable_source_file__` identity required to anti-join persisted
`(__file__, __rowid__)` entries safely. AUTO therefore excludes those plans,
and explicit Spark fails closed before cluster selection or connection.

## Engine Auto-Selection

`Executor._auto_pick()` first applies SQL capability, immutable-identity,
resource, result, tombstone, and Spark-fleet availability gates. It then
compares deterministic costs using the sealed post-pruning scan estimate,
decoded bytes, file/row-group fanout, query shape, freshness, spill work, and
compatible scoped observations. Incomplete evidence routes conservatively to
DuckDB. Spark is considered only when an active registered cluster accepts the
job's byte window. Redis `auto_policy` intervals can force a preferred engine
for an estimated-scan range but cannot bypass any safety gate.

Freshness is derived from `Reflection.freshness_ms` — the max
`last_updated_ms` across all snapshots involved in the query. Unknown
freshness (0) is treated as stable.

IslandDB is enabled in AUTO by default. It may replace DuckDB only for a supported,
stable query whose decoded working set and operator/result state fit a bounded
native or spill plan. A cold remote query remains eligible because the sealed
range reader does not require a whole-object fill. Unsupported, incomplete, or
over-budget queries stay on DuckDB; Spark retains the fleet-first lane. Set
`SUPERTABLE_ISLAND_AUTO_ENABLED=false` to disable the IslandDB AUTO candidate.

## View Chain

Views are layered in this order (each wraps the previous):

```
parquet_scan → tombstone/system-column view → RBAC view → user query
```

**RBAC view**: Column projection (`allowed_columns`) + row filter (`WHERE`
clause) from the pinned role/share definition. It is applied after system
columns and deleted rows have been removed.

**Tombstone view**: sealed composite
`(__supertable_source_file__, __rowid__)` anti-join against the persisted
deletion-vector identity `(__file__, __rowid__)`. The same layer always strips
reserved system columns before user SQL/RBAC can expose them.

Each view name includes a per-query UUID suffix to prevent collisions under concurrency.

## Key Data Classes

**`Reflection`** — Output of `DataEstimator.estimate()`. Carries everything the executor needs:
- `supers: List[SuperSnapshot]` — resolved file lists per table
- `reflection_bytes: int` — projected compressed scan-byte estimate used by routing
- `source_bytes: int` — whole-file bytes after file pruning
- `source_bytes_complete: bool` — whether every survivor size is trustworthy;
  incomplete estimates stay on DuckDB rather than being routed as tiny jobs
- `freshness_ms: int` — max last_updated_ms across snapshots
- `rbac_views: Dict[str, RbacViewDef]` — per-alias RBAC filters
- `tombstone_views: Dict[str, TombstoneDef]` — per-alias, snapshot-pinned deletion-vector definitions

**`TableDefinition`** — From `SQLParser.get_table_tuples()`:
- `super_name, simple_name, alias` — table identity
- `columns: List[str]` — requested columns (`[]` means SELECT *)

**`RbacViewDef`** — `allowed_columns` + `where_clause`
**`TombstoneDef`** — pinned artifact path/key, expected row count/digest,
and exact snapshot resource membership

## Environment Variables

### Engine Selection
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_ENGINE_ISLAND_MIN_BYTES` | `104857600` (100 MB) | Upper bound for DuckDB in AUTO mode |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | `0` | Fallback only; an active Spark cluster's `min_bytes` normally drives fleet routing |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | `300` (5 min) | Age threshold separating fresh vs stable data |

### IslandDB / Shared Cache
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_ISLAND_CACHE_ENABLED` | `true` | Enable shared-cache lookup and explicit IslandDB localization |
| `SUPERTABLE_ISLAND_CACHE_DIR` | DuckDB cache root / app home | Application-owned Parquet cache root |
| `SUPERTABLE_ISLAND_CACHE_MAX_BYTES` | `21474836480` (20 GiB) | Hard admission/eviction byte cap |
| `SUPERTABLE_ISLAND_CACHE_TTL_SEC` | `0` | Idle entry TTL; `0` keeps immutable objects until capacity pressure/corruption |
| `SUPERTABLE_ISLAND_CACHE_WORKERS` | `4` | Concurrent bounded downloads per query |
| `SUPERTABLE_ISLAND_AUTO_ENABLED` | `true` | Allow supported, resource-admitted queries to use IslandDB in AUTO |
| `SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED` | `true` | Cache sealed Parquet footer/column byte ranges instead of cold whole objects |
| `SUPERTABLE_ISLAND_RANGE_CACHE_DIR` | Island cache sibling | Persistent range-cache root |
| `SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES` | `20 GiB` | Hard range-cache capacity |
| `SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC` | `0` | Idle range TTL; `0` keeps immutable ranges until capacity/corruption rotation |
| `SUPERTABLE_ISLAND_MEMORY_FRACTION` | `0.60` | Per-query fraction of cgroup-available memory |
| `SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION` | `0.80` | Aggregate native-query reservation ceiling |
| `SUPERTABLE_ISLAND_MAX_MEMORY_BYTES` | `0` (auto) | Optional absolute native memory ceiling |
| `SUPERTABLE_ISLAND_MAX_RESULT_BYTES` | `512 MiB` | Materialized result cap; larger results require Arrow streaming |
| `SUPERTABLE_ISLAND_CPU_MAX` | `0` (auto) | Optional cap below cpuset/cpu.max capacity |
| `SUPERTABLE_ISLAND_IO_WORKERS_MAX` | `16` | Adaptive remote range-read concurrency cap |
| `SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC` | `300` | Cooperative wall-clock deadline checked at native batch boundaries; non-positive disables it |
| `SUPERTABLE_ISLAND_SPILL_ENABLED` | `true` | Enable the sealed external sort/group spill subset |
| `SUPERTABLE_ISLAND_SPILL_DIR` | `$SUPERTABLE_HOME/island_spill` | Query-private spill root |
| `SUPERTABLE_ISLAND_SPILL_MAX_BYTES` | `64 GiB` | Hard per-query spill quota |
| `SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES` | `512 MiB` | Disk reserve that spill may not consume |

### DuckDB Configuration
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `1GB` | DuckDB memory cap (both DuckDB and IslandDB) |
| `SUPERTABLE_DUCKDB_THREADS` | auto-derived | Explicit thread count override |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | `3` | CPU × multiplier for IO threads (auto mode) |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | DuckDB default (30s) | HTTP timeout in seconds |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | `1` (on) | Parquet footer cache on persistent connection |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | unset (off) | Best-effort DuckDB block-cache cap; released builds may keep it connection-local/in-memory |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | unset | DuckDB cache target and fallback root for the application-owned object cache |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | `view` | `view` (lazy) or `table` (eager materialisation) |

### S3 / Storage
| Variable | Default | Description |
|---|---|---|
| `STORAGE_ENDPOINT_URL` | unset | S3-compatible endpoint |
| `STORAGE_REGION` | `us-east-1` | S3 region |
| `STORAGE_ACCESS_KEY` | unset | S3 access key |
| `STORAGE_SECRET_KEY` | unset | S3 secret key |
| `STORAGE_SESSION_TOKEN` | unset | STS session token |
| `STORAGE_BUCKET` | unset | Default bucket |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Path-style vs vhost-style URLs |
| `STORAGE_USE_SSL` | unset (off) | Enable HTTPS for S3 |
| `SUPERTABLE_DUCKDB_PRESIGNED` | unset (off) | Force presigned URLs for all paths |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | unset (off) | Force HTTP URLs instead of `s3://` |

### Spark Thrift
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | `300` (5 min) | Overall query timeout (watchdog) |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | `120` (2 min) | Per-statement timeout (EXPLAIN, user query) |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | `30` | Socket connect timeout |
| `SUPERTABLE_SPARK_BATCH_SIZE` | `50` | Files per batch when registering views |

## File Map

```
supertable/engine/
├── __init__.py              Package exports (Engine, Executor, PlanStats, DataEstimator)
├── engine_enum.py           Engine enum (AUTO, DUCKDB, ISLANDDB, SPARK_SQL) + dialect
├── engine_common.py         Shared: storage config, protected deletion-vector/RBAC views, query rewriting, connection init
├── executor.py              Engine router + auto-pick logic
├── duckdb_engine.py         Scoped DuckDB executor and view lifecycle
├── islanddb.py              Conservative native lazy-Parquet executor
├── file_cache.py            Shared atomic local Parquet object cache
├── range_cache.py           Conditional persistent Parquet byte-range cache
├── island_resources.py      cgroup-aware planner, governor, Arrow stream API
├── island_spill.py          Hard-quota external sort/group primitives
├── spark_thrift.py          Spark Thrift executor (PyHive, s3a paths, timestamp CAST, transpile)
├── data_estimator.py        Snapshot resolution, file collection, column validation
├── plan_stats.py            Simple stat accumulator for execution plans
└── tests/
    ├── conftest.py          Shared fixtures (mock Redis, DuckDB connection, clean env)
    ├── test_engine.py        Core executor and engine behavior
    ├── test_executor_safety_regressions.py  Routing/security boundaries
    └── test_tombstone_source_rowid_integrity.py  Composite deletion identity

supertable/data_classes.py   Reflection, SuperSnapshot, RbacViewDef, TombstoneDef, TableDefinition
supertable/data_reader.py    Facade: parse → RBAC → estimate → wire views → execute → extend plan
supertable/utils/sql_parser.py  SQLParser: table/column extraction via sqlglot

supertable/rbac/
├── access_control.py        restrict_read_access (returns RbacViewDef dict), check_write_access, check_meta_access
├── permissions.py           RoleType enum, Permission enum, ROLE_PERMISSIONS matrix
├── role_manager.py          CRUD for roles (Redis-backed)
├── filter_builder.py        JSON filter → SQL WHERE clause
├── row_column_security.py   Role validation + content hashing
└── user_manager.py          CRUD for users (Redis-backed)
```

## RBAC Flow

```
restrict_read_access(role_name, tables)
  ├─ Resolve role from Redis via RoleManager
  ├─ Check READ permission (raise PermissionError if denied)
  ├─ Superadmin/Admin → return {} (no filtering)
  └─ Reader/Writer with restrictions:
       ├─ Validate table access (role.tables)
       ├─ Validate column access (role.columns vs requested columns)
       ├─ Build WHERE clause from role.filters via FilterBuilder
       └─ Return {alias: RbacViewDef(allowed_columns, where_clause)}
```

The returned dict is set on `Reflection.rbac_views` before execution. DuckDB
and Spark create filtered views from these definitions; IslandDB's capability
gate currently rejects any non-empty RBAC view so policy can never be bypassed.
`augment_rbac_columns()` ensures internal columns needed by downstream views
pass through the RBAC layer without becoming user-visible.

## SQL Parsing

`SQLParser(super_name, query, dialect)` uses sqlglot to extract:
- Table references with schema resolution (missing schema → default `super_name`)
- Per-alias column lists (qualified and unqualified resolution)
- Star semantics: `SELECT *` → `columns=[]` for all tables; `SELECT t.*` → `columns=[]` for alias `t`
- SELECT alias detection to avoid recording computed aliases as physical columns

Output: `List[TableDefinition]` consumed by RBAC, estimator, and executors.
