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
[3] Estimate       -- DataEstimator resolves files and byte totals
    |
    v
[4] Build views    -- dedup, tombstone, RBAC views attached to Reflection
    |
    v
[5] Select engine  -- AUTO picks Lite/Pro/Spark based on size + freshness
    |
    v
[6] Execute        -- Executor runs query against chosen backend
    |
    v
[7] Record plan    -- extend_execution_plan() writes timing + stats
    |
    v
[8] Return         -- (DataFrame, Status, message)
```

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

`SQLParser` uses sqlglot to parse the query with the correct dialect (`"duckdb"` or `"spark"`). It extracts:
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

**Step 3 -- Estimate Data Size**

```python
estimator = DataEstimator(
    organization=self.organization,
    storage=self.storage,
    tables=physical_tables,
)
reflection = estimator.estimate()
```

The `DataEstimator` walks the Redis catalog to find the current snapshot for each table, collects parquet file paths, sums byte sizes, and produces a `Reflection` dataclass. Only `physical_tables` are passed (not CTE aliases) so the estimator resolves actual data files.

**Step 4 -- Build View Definitions**

After estimation, the `DataReader` attaches view definitions to the `Reflection` object for the executor to consume:

**RBAC views:**
```python
reflection.rbac_views = rbac_views
```

**Dedup-on-read views** (from table config in Redis):
```python
catalog = RedisCatalog()
for td in tables:
    tbl_cfg = catalog.get_table_config(
        self.organization, td.super_name, td.simple_name,
    )
    if tbl_cfg and tbl_cfg.get("dedup_on_read"):
        pk = tbl_cfg.get("primary_keys", [])
        if pk:
            reflection.dedup_views[td.alias] = DedupViewDef(
                primary_keys=pk,
                order_column="__timestamp__",
                visible_columns=list(td.columns or []),
            )
```

**Tombstone views** (from snapshot metadata in Redis):
```python
leaf = catalog.get_leaf(
    self.organization, td.super_name, td.simple_name,
)
payload = (leaf or {}).get("payload")
tomb_block = payload.get("tombstones")
if tomb_block:
    reflection.tombstone_views[td.alias] = TombstoneDef(
        primary_keys=tomb_block.get("primary_keys", []),
        deleted_keys=tomb_block.get("deleted_keys", []),
    )
```

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

**Step 5 -- Select Engine and Execute**

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

The `Executor` applies the AUTO selection logic (documented in the Query Engine chapter) and delegates to the chosen backend.

**Step 6 -- Record Execution Plan**

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
1. Applies `_ensure_sql_limit()` to cap result size.
2. Creates a `DataReader` and calls `execute()`.
3. Converts the DataFrame to columnar format: `(columns, rows, columns_meta)`.
4. Sanitizes pandas NA variants (`pd.NA`, `pd.NaT`, `np.nan`) to Python `None` for JSON serialization.

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

The view chain is a stack of SQL views built on top of raw parquet files. Each layer adds a data integrity or security concern:

```
[Base]  parquet_scan(files) -> reflection table
   |
   v
[RBAC]  SELECT allowed_columns FROM base WHERE role_filter
   |
   v
[Tombstone]  SELECT * FROM rbac WHERE NOT EXISTS (deleted_keys)
   |
   v
[Dedup]  SELECT visible_cols FROM (ROW_NUMBER() OVER ...) WHERE __rn__=1
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

### Tombstone Layer

Excludes soft-deleted rows using an anti-join against a VALUES list of deleted composite keys. Positioned after RBAC (so deleted rows are never visible) and before dedup (so deleted rows do not participate in the ROW_NUMBER window).

### Dedup Layer

Keeps only the latest row per primary key combination using `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY __timestamp__ DESC)`. Only `visible_columns` are projected in the outer SELECT, hiding internal columns (`__rn__`, `__timestamp__`) from the user query.

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

### `DedupViewDef`

```python
@dataclass
class DedupViewDef:
    primary_keys: List[str] = field(default_factory=list)
    order_column: str = "__timestamp__"
    visible_columns: List[str] = field(default_factory=list)
```

Dedup-on-read configuration from the table config in Redis.

### `TombstoneDef`

```python
@dataclass
class TombstoneDef:
    primary_keys: List[str] = field(default_factory=list)
    deleted_keys: List = field(default_factory=list)
```

Soft-delete keys from the snapshot metadata.

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
    dedup_views: Dict[str, DedupViewDef] = field(default_factory=dict)
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)
```

The aggregate result of data estimation, carrying everything the executor needs to build views and run the query.

## Business Context

The `DataReader` is the single point through which all data leaves SuperTable. This design provides several guarantees:

- **Uniform security enforcement**: every query path passes through the same RBAC check and view chain. There is no way to bypass column or row restrictions by using a different interface.

- **Consistent data view**: dedup-on-read and tombstone filtering ensure that all consumers see the same logical state of the data, even when the underlying parquet files contain historical duplicates or soft-deleted rows.

- **Auditable execution**: every query produces an execution plan with timing breakdowns, engine choice, file counts, and result shape. This enables performance debugging and compliance auditing.

- **Snapshot linked list for compliance**: every write chains via `previous_snapshot`, so older parquet sets remain reachable for point-in-time inspection without maintaining separate historical tables.

- **Tenant isolation**: the `organization` parameter scopes every operation to a single tenant. Combined with RBAC, this prevents cross-tenant data access even when multiple organizations share the same SuperTable deployment.
