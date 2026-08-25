# Data Reader

## Overview

The `DataReader` class is the central facade for executing SQL queries against SuperTable data. It orchestrates the full query lifecycle: parsing SQL, enforcing access control, resolving data files, estimating data size, selecting the execution engine, building the view chain, executing the query, and recording the execution plan.

The class lives in `supertable/data_reader.py` and is consumed by the API server, OData server, MCP server, and the SQL editor UI.

## Query Execution Flow

Every query follows the same pipeline, regardless of which server initiates it:

```
User SQL
    |
    v
[1] Parse          -- SQLParser extracts tables, columns, aliases
    |
    v
[2] RBAC check     -- restrict_read_access() validates permissions
    |
    v
[3] Pre-flight     -- _assert_targets_exist() refuses to create on read
    |
    v
[4] Estimate       -- DataEstimator resolves files and byte totals
    |
    v
[5] Build views    -- snapshot-pinned deletion-vector + RBAC definitions
    |
    v
[6] Select engine  -- AUTO applies capability/safety gates, then cost routing
    |
    v
[7] Execute        -- Executor runs query against chosen backend
    |
    v
[8] Record plan    -- extend_execution_plan() writes timing + stats
    |
    v
[9] Return         -- (DataFrame, Status, message)
```

## Reads Never Create Tables

The SDK enforces an invariant: **reads never mint catalog entries**.
Two layers protect this:

1. **`DataReader._assert_targets_exist()`** runs immediately inside
   the `try` block in `execute()`. For each `(super_name, simple_name)`
   pair in `physical_tables`, it checks `catalog.root_exists(...)` and
   `catalog.leaf_exists(...)` in Redis. A miss raises a typed
   exception which the surrounding `except` turns into the standard
   `(empty_df, Status.ERROR, "SuperTable not found: …")` /
   `(empty_df, Status.ERROR, "Table not found: …")` tuple. No
   side-effects land before the check.

2. **Constructor opt-out:** `SuperTable.__init__` and
   `SimpleTable.__init__` both accept `create_if_missing: bool = True`
   (default preserves the writer's auto-create behaviour). Every
   read-side caller — `DataEstimator`, `MetaReader.__init__`, every
   `SimpleTable(...)` call in `meta_reader.py` — passes
   `create_if_missing=False`. If any future code path forgets the
   edge check, the constructor still refuses to materialise.

### Public exceptions

```python
from supertable import (
    SupertableLookupError,
    SuperTableNotFoundError,
    TableNotFoundError,
)
```

All three live in `supertable/errors.py` and inherit from the stdlib
`LookupError` (so legacy `except LookupError` / `except KeyError`
callers keep working).

| Class | When raised | Attributes |
|-------|-------------|-----------|
| `SupertableLookupError` | base class (do not instantiate directly) | `organization` |
| `SuperTableNotFoundError` | the supertable's `meta:root` is missing | `organization`, `super_name` |
| `TableNotFoundError` | the simple table's `meta:leaf:doc:{simple}` is missing | `organization`, `super_name`, `simple_name` |

The string representation is the canonical form
`SuperTable not found: org/super` or
`Table not found: org/super/simple` — safe to surface in API responses.

## The DataReader Class

### Constructor

```python
class DataReader:
    def __init__(self, super_name: str, organization: str, query: str):
        self.super_name = super_name
        self.organization = organization
        self.query = query
        self.storage: StorageInterface = get_storage()
        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None
```

The constructor accepts the SuperTable name, organization (tenant), and raw SQL query. It initializes the storage backend via `get_storage()` from the storage factory.

### The `execute()` Method

```python
def execute(
    self,
    role_name: str,
    with_scan: bool = False,
    engine: Engine = Engine.AUTO,
) -> Tuple[pd.DataFrame, Status, Optional[str]]:
```

This is the primary entry point. It returns a tuple of:
- `pd.DataFrame`: the query results (empty on error).
- `Status`: an enum with values `OK` or `ERROR`.
- `Optional[str]`: error message (None on success).

Every SELECT is parsed as exactly one statement and receives a server-side
outer `LIMIT`; a larger client `LIMIT` is clamped to
`SUPERTABLE_MAX_LIMIT`. SQL byte size, AST nodes, nesting, and join count are
also bounded before catalog or engine work begins. DuckDB execution consumes
`SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` and interrupts the query-private cursor
when that deadline expires.

The parsed statement also passes a closed function capability policy. Ordinary
aggregates, casts, string/date operations, and supported window analytics are
available; settings/secret introspection, filesystem helpers, dynamic table
functions, unknown extension/UDF calls, qualified function calls, and public
collection-result amplifiers fail closed. The built-in deep-quality profiler
has a separate internal capability for collection aggregates, and only when
their direct subquery has a literal `LIMIT` of at most 10.

DuckDB's bare `USER`, `SESSION_USER`, `CURRENT_ROLE`, `CURRENT_CATALOG`, and
`CURRENT_SCHEMA` expressions are also rejected because they expose session or
catalog identity even though the parser models them as columns. A quoted or
table-qualified data column with the same name remains available.

`EXPLAIN ANALYZE` is not exposed on this untrusted path because DuckDB includes
physical filenames in its result. Plain `EXPLAIN` remains available for normal
managed sources, but is rejected for signed data/deletion-vector URLs and for
queries with RBAC or share restrictions whose expanded plans could expose
hidden policy columns or literal values.

### The `execute_stream()` Method

```python
stream, status, message = reader.execute_stream(
    role_name="reader",
    engine=engine.ISLANDDB,
)
with stream:
    for record_batch in stream:
        send(record_batch)
```

`execute_stream()` runs the same SQL limits, catalog preflight, aggregate-child
authorization, snapshot pinning, RBAC, and tombstone setup as `execute()`, but
returns the public cancellable `ArrowBatchStream` instead of a pandas frame.
DuckDB and IslandDB retain their query resources until exhaustion or close;
`AUTO` may select either. Spark requests fail explicitly rather than being
silently materialized under the streaming API.

### Step-by-Step Walkthrough

**Step 1 -- Parse the SQL**

```python
parser = SQLParser(
    super_name=self.super_name,
    query=self.query,
    dialect=engine.dialect,
)
tables = parser.get_table_tuples()
physical_tables = parser.get_physical_tables()
```

`SQLParser` uses sqlglot to parse exactly one read-only query with the correct
dialect (`"duckdb"` or `"spark"`). It rejects mutation/locking nodes,
unmanaged sources, and functions outside the closed capability list before it
extracts:
- `tables`: all table references including CTE aliases, as `TableDefinition` objects.
- `physical_tables`: only real tables (excludes CTE aliases), used for file resolution.

**Step 2 -- RBAC Check**

```python
rbac_views = restrict_read_access(
    super_name=self.super_name,
    organization=self.organization,
    role_name=role_name,
    tables=tables,
    physical_tables=physical_tables,
)
```

`restrict_read_access()` validates that the role has permission to read the requested tables and returns per-alias `RbacViewDef` objects describing column and row filters. If access is denied, this function raises an exception.

**Step 3 -- Pre-flight catalog check**

```python
self._assert_targets_exist(physical_tables)
```

Two Redis `EXISTS` calls per referenced `(super, simple)` pair —
`root_exists(...)` and `leaf_exists(...)`. On a miss this raises
`SuperTableNotFoundError` / `TableNotFoundError`, which the
surrounding `except` turns into the same
`(empty_df, Status.ERROR, message)` shape as every other read
failure. Microseconds of cost; **zero catalog state is touched
before the check.**

This is the edge that guarantees the "reads never create tables"
invariant — without it, the `SuperTable(super_name, organization)`
construction inside `DataEstimator.estimate()` would silently
bootstrap a missing supertable as a side effect of resolving the
query.

**Step 4 -- Estimate Data Size**

```python
estimator = DataEstimator(
    organization=self.organization,
    storage=self.storage,
    tables=physical_tables,
)
reflection = estimator.estimate()
```

The `DataEstimator` walks the Redis catalog to find the current
snapshot for each table, collects parquet file paths, sums byte
sizes, and produces a `Reflection` dataclass. Only `physical_tables`
are passed (not CTE aliases) so the estimator resolves actual data
files. Internally the estimator constructs `SuperTable(...,
create_if_missing=False)` as defence in depth — even if a future
code path skipped the edge check, the constructor would refuse to
materialise.

**Step 5 -- Build View Definitions**

After estimation, `DataReader` attaches the authorized RBAC definitions and the
deletion vector from the same pinned snapshot to the `Reflection`. It never
re-reads the current Redis leaf after estimation: combining an older file set
with a newer vector could hide an old row without including its replacement.

**RBAC views:**
```python
reflection.rbac_views = rbac_views
```

There is no read-time primary-key collapse. Overwrites and deletes are writer
decisions represented by physical row identities in the snapshot's deletion
vector. If the pinned snapshot has an active vector, the reader resolves its
sealed artifact and constructs an executor definition equivalent to:

```python
reflection.tombstone_views[td.alias] = TombstoneDef(
    tombstone_path=resolved_tombstone,
    cache_key=snapshot.tombstone_key,
    expected_rows=snapshot.tombstone_rows,
    tombstone_digest=snapshot.tombstone_digest,
    resource_keys=tuple(snapshot.resource_keys),
    snapshot_resource_keys=tuple(snapshot.snapshot_resource_keys),
    tombstone_format=snapshot.tombstone_format,
    segments=resolved_v2_segments,
)
```

The executor may remove a row only when the protected source identity
`(__supertable_source_file__, __rowid__)` matches the persisted deletion-vector
identity `(__file__, __rowid__)`. Row-id-only filtering is not a safe
substitute.

**Linked-share row filters** (provider-side row filter on shared tables):
```python
share_row_filter = payload.get("_row_filter")
if share_row_filter:
    # Merged with existing RBAC where_clause via AND
    reflection.rbac_views[td.alias] = RbacViewDef(
        allowed_columns=["*"],
        where_clause=share_row_filter,
    )
```

**Step 6 -- Select Engine and Execute**

```python
executor = Executor(storage=self.storage, organization=self.organization)
result_df, engine_used = executor.execute(
    engine=engine,
    reflection=reflection,
    parser=parser,
    query_manager=self.query_plan_manager,
    timer=self.timer,
    plan_stats=self.plan_stats,
    log_prefix=self._lp(""),
)
```

The `Executor` applies the AUTO selection logic (documented in the Query Engine
chapter) and delegates to the chosen backend. Active deletion vectors are a
hard Spark eligibility gate: AUTO excludes Spark, while an explicit Spark
request fails before cluster selection or connection with the internal
composite-identity capability error. The public inline facade returns the
stable sanitized `Query execution failed` message; it never ignores the vector
or returns deleted rows.

**Step 7 -- Record Execution Plan**

```python
extend_execution_plan(
    query_plan_manager=self.query_plan_manager,
    role_name=role_name,
    timing=self.timer.timings,
    plan_stats=self.plan_stats,
    status=str(status.value),
    message=message,
    result_shape=result_df.shape,
)
```

The execution plan captures timing breakdowns (CONNECTING, EXECUTING_QUERY, EXTENDING_PLAN, TOTAL_EXECUTE), engine choice, file counts, byte totals, and result shape for debugging and monitoring.

## SQL Sanitization

### LIMIT Enforcement

`_ensure_sql_limit(sql, default_limit)` in `data_reader.py` appends
`LIMIT <default_limit>` only if the outermost query has no LIMIT clause. Uses
regex to detect existing LIMIT patterns, avoiding interference with subqueries
or CTEs.

### SQL String Escaping

In `engine_common.py`:
- `sanitize_sql_string()` escapes single quotes in SQL string literals used in SET statements.
- `escape_parquet_path()` escapes file paths embedded in SQL.
- `quote_if_needed()` quotes column names containing special characters, handling the `*` wildcard.

## The `query_sql()` Convenience Function

For callers that want a simpler columnar interface, `data_reader.py` exports
`query_sql()`:

```python
def query_sql(
    organization: str,
    super_name: str,
    sql: str,
    limit: int,
    engine: Any,
    role_name: str,
) -> Tuple[List[str], List[List[Any]], List[Dict[str, Any]]]:
```

This function:
1. Applies `_ensure_sql_limit()` and clamps it to the server maximum.
2. Routes DuckDB, IslandDB, and AUTO SELECTs through `execute_stream()` and
   converts bounded Arrow batches without constructing a pandas result.
3. Accounts for each encoded row before retaining it and cancels the stream
   when the complete JSON response would exceed
   `SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES`.
4. Routes an explicit Spark SELECT through its bounded materialized path. The
   Thrift cursor fetches at most 256 rows upstream, enforces the inline row and
   pre-materialization byte budgets while fetching, and honors the request
   deadline/cancellation token. The complete public payload then passes the
   same authoritative exact JSON byte accounting as every other engine.

DuckDB fetches at most `SUPERTABLE_RESULT_STREAM_BATCH_ROWS` rows per Arrow
batch (hard-clamped to 1–4096). A single row remains the indivisible memory
floor, but a wide response cannot be prefetched in the old 64K-row batch.
Spark remains unavailable through `execute_stream()` and `query_sql_stream()`;
only the bounded inline `query_sql()` facade materializes Spark results. That
facade does not widen Spark's data capability: an active deletion vector still
fails closed before fleet selection.

Returns:
- `columns`: list of column name strings.
- `rows`: list of row lists (each row is a list of scalar values).
- `columns_meta`: list of dicts with `name`, `type`, and `nullable` for each column.

## Snapshot Linked List

Every write to a SimpleTable creates a new snapshot that references the
previous snapshot via the `previous_snapshot` field. This forms a linked list:

```
[current snapshot v7] --previous_snapshot--> [v6] --> [v5] --> ... --> [v1]
```

Redis stores only the current leaf pointer. Historical snapshots are JSON
files on the configured storage backend, which can be inspected directly with
`SuperTable.read_simple_table_snapshot(path)` to drive ad-hoc point-in-time
queries against the parquet files listed in each snapshot.

## View Chain

The successful read path builds protected SQL views on top of the raw parquet
files. DuckDB and IslandDB can apply active deletion vectors; Spark uses the
same protected projection only when no vector is active.

```
[Base]  parquet_scan(files) -> reflection table
   |
   v
[Protected projection]
        composite (__supertable_source_file__, __rowid__) anti-join against
        validated deletion vector; strip internal columns
    |
    v
[RBAC]  SELECT allowed_columns FROM protected WHERE role_filter
   |
   v
  User query references the top-most view
```

### Base Layer

The reflection table (or view) registers parquet files with the query engine:

```sql
SELECT <columns> FROM parquet_scan(
    ['s3://bucket/file1.parquet', 's3://bucket/file2.parquet'],
    union_by_name=TRUE,
    HIVE_PARTITIONING=FALSE
);
```

`union_by_name=TRUE` handles schema evolution across parquet files with different column sets.

### RBAC Layer

Applies column-level and row-level security based on the authenticated role:

- **Column filtering**: projects only the columns the role is allowed to see.
- **Row filtering**: applies a WHERE clause from the role's filter definition.
- **Share filters**: linked-share row filters are merged with RBAC filters via AND.

### Protected Deletion-Vector Layer

The vector contains the stable logical source-object key and row ID for each
obsolete/deleted physical row. Before the anti-join, the executor validates the
artifact's schema, row count, digest, referenced snapshot files, and source
row-id integrity. It then joins on both source file and row ID and strips
`__rowid__`, `__timestamp__`, and protected filename columns before RBAC or user
SQL can see them.

Spark cannot yet carry the stable logical source key through its resolved
Parquet views, so an active vector is rejected before any Spark fleet I/O.

## Data Classes

The data classes used throughout the query pipeline are defined in `supertable/data_classes.py`:

### `TableDefinition`

```python
@dataclass
class TableDefinition:
    super_name: str
    simple_name: str
    alias: str
    columns: List[str] = field(default_factory=list)
```

Represents a table reference extracted from the SQL query by `SQLParser`.

### `SuperSnapshot`

```python
@dataclass
class SuperSnapshot:
    super_name: str
    simple_name: str
    simple_version: int
    files: List[str] = field(default_factory=list)
    columns: Set[str] = field(default_factory=set)
```

Represents a resolved table with its parquet file list and available columns.

### `RbacViewDef`

```python
@dataclass
class RbacViewDef:
    allowed_columns: List[str] = field(default_factory=lambda: ["*"])
    where_clause: str = ""
```

Column and row filter definitions produced by `restrict_read_access()`.

### `TombstoneDef`

```python
@dataclass
class TombstoneDef:
    tombstone_path: Optional[str] = None
    cache_key: Optional[str] = None
    expected_rows: Optional[int] = None
    tombstone_digest: Optional[str] = None
    resource_keys: Tuple[str, ...] = field(default_factory=tuple)
    snapshot_resource_keys: Optional[Tuple[str, ...]] = None
    tombstone_format: Optional[int] = None
    segments: Tuple[TombstoneSegmentDef, ...] = field(default_factory=tuple)
```

Snapshot-pinned, sealed deletion-vector identity and the exact set of data
objects against which it may be applied.

### `Reflection`

```python
@dataclass
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]
    freshness_ms: int = 0
    rbac_views: Dict[str, RbacViewDef] = field(default_factory=dict)
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)
```

The aggregate result of data estimation, carrying everything the executor needs to build views and run the query.

## Business Context

The `DataReader` is the single point through which all data leaves SuperTable. This design provides several guarantees:

- **Uniform security enforcement**: every query path passes through the same RBAC check and view chain. There is no way to bypass column or row restrictions by using a different interface.

- **Consistent data view**: writer-produced deletion vectors and their exact
  composite anti-join ensure consumers do not see obsolete or soft-deleted
  physical rows. Engines that cannot prove the same view are excluded by AUTO
  or fail closed; they never return an approximate result.

- **Auditable execution**: every query produces an execution plan with timing
  breakdowns, engine choice, file counts, and result shape. Successful and
  failed materialized outcomes emit attributed audit events; audit delivery
  failures do not replace the original query result or exception.

- **Snapshot linked list for compliance**: every write chains via `previous_snapshot`, so older parquet sets remain reachable for point-in-time inspection without maintaining separate historical tables.

- **Tenant isolation**: the `organization` parameter scopes every operation to a single tenant. Combined with RBAC, this prevents cross-tenant data access even when multiple organizations share the same SuperTable deployment.
