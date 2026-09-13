# Python SDK

This guide uses the public Python classes in the current source tree. It assumes Redis is reachable and the configured storage backend is available. The examples use a dedicated `acme/warehouse` namespace and the `superadmin` role created during SuperTable initialization.

Sources: [package exports](../supertable/__init__.py), [project metadata](../pyproject.toml).

## Install and configure

Python 3.10 or newer is declared by the package. From the repository directory:

```bash
python -m pip install -e .
```

Cloud extras are `s3`, `minio`, `azure`, `gcp`, `all-cloud`, and `all`; for example, `python -m pip install -e '.[s3]'`. The base dependencies include PyArrow, Polars, DuckDB, Redis, and SQLGlot. See [configuration](02_configuration.md) and [storage](04_storage.md) for backend options.

Set environment variables before importing the package:

```bash
export SUPERTABLE_HOME=/tmp/supertable-sdk-example
export STORAGE_TYPE=LOCAL
export SUPERTABLE_REDIS_HOST=localhost
export SUPERTABLE_REDIS_PORT=6379
export SUPERTABLE_REDIS_DB=0
```

Settings are loaded into a process-wide object. Importing the home-directory module, including through normal package imports, creates/resolves the application home and changes the process working directory to it. Use absolute paths when your application must retain a reference to its original directory. Local storage still requires Redis for metadata and locks.

`pyproject.toml` and `supertable.__version__` identify this source as `3.3.0`; `setup.py` also contains a separate `3.0.7` literal. Use the checked-out APIs as the reference for this guide.

## Create and write a table

```python
import pyarrow as pa
from supertable import SuperTable, DataWriter, DataReader, engine
from supertable.data_reader import Status

organization = "acme"
super_name = "warehouse"
role_name = "superadmin"

super_table = SuperTable(super_name=super_name, organization=organization)
writer = DataWriter(super_name=super_name, organization=organization)

columns, rows, inserted, deleted = writer.write(
    role_name=role_name,
    simple_name="orders",
    data=pa.table({"order_id": [1, 2], "amount": [12.5, 18.0]}),
    overwrite_columns=["order_id"],
)
print({"columns": columns, "inserted": inserted, "deleted": deleted})

reader = DataReader(
    super_name=super_name,
    organization=organization,
    query="SELECT order_id, amount FROM orders ORDER BY order_id",
)
frame, status, message = reader.execute(role_name=role_name, engine=engine.DUCKDB)
if status is not Status.OK:
    raise RuntimeError(message)
print(frame)
```

`write` takes Arrow input and returns `(incoming_column_count, inserted_rows, inserted_rows, deleted_rows)`. This is not the final table row count. Reads return a Polars DataFrame, a `Status` enum, and an optional message. Some authorization, parsing, or catalog errors can raise before the reader's execution exception handler, so callers must also handle exceptions.

`overwrite_columns=[]` appends. A nonempty list replaces existing matches through tombstones. It does not enforce uniqueness within the incoming batch. For conditional replacement, delete-only calls, configuration, and compaction, see [data writer](06_data_writer.md).

## Stream Arrow batches

```python
reader = DataReader(
    super_name=super_name,
    organization=organization,
    query="SELECT order_id, amount FROM orders ORDER BY order_id",
)
handle = reader.stream(
    role_name=role_name,
    engine=engine.DUCKDB,
    batch_rows=10_000,
)
try:
    for batch in handle.batches():
        print(batch.num_rows)
finally:
    handle.close()
```

Close a handle even if iteration ends early. The handle releases its query resources and triggers its completion callback on close. Normal reads hide `__rowid__` and `__timestamp__`; DuckDB streaming with `expose_rowid=True` can expose the row ID, subject to the role's allowed-column view.

Use `engine.DUCKDB` for these examples. `engine.AUTO` can select a registered Spark cluster for sufficiently large estimates, and the current Spark streaming return path has a defect described in [query engines](09_query_engine.md). `DataReader` instances hold mutable execution/stream state; create a separate reader for independent queries.

## Return JSON-friendly rows

```python
from supertable.data_reader import query_sql

query_info = {}
column_names, result_rows, column_metadata = query_sql(
    organization=organization,
    super_name=super_name,
    sql="SELECT order_id, amount FROM orders",
    limit=100,
    engine=engine.DUCKDB,
    role_name=role_name,
    out=query_info,
)
```

`query_sql` returns names, lists of row values, and dictionaries containing `name`, `type`, and `nullable`. It attempts to turn NaN into null and can populate `query_id` and `query_hash` in `out`. This is not a guarantee that every value is directly serializable by the standard JSON encoder; datetime and other typed values can remain.

For SELECT queries the helper appends a default limit unless its trailing-limit pattern already matches. An explicit existing limit is not capped by `limit`. Prefer SQL without a trailing semicolon when allowing the helper to append a limit. See [reader helper behavior](10_data_reader.md).

## Inspect metadata and snapshots

```python
from supertable import MetaReader, SimpleTable, list_tables

meta = MetaReader(super_name=super_name, organization=organization)
visible_tables = meta.get_tables(role_name=role_name)
schema = meta.get_table_schema(table_name="orders", role_name=role_name)
table_stats = meta.get_table_stats(table_name="orders", role_name=role_name)
summary = meta.get_super_meta(role_name=role_name)

same_tables = list_tables(
    organization=organization,
    super_name=super_name,
    role_name=role_name,
)
table = SimpleTable(super_table, "orders", create_if_missing=False)
snapshot, snapshot_path = table.get_simple_table_snapshot()
```

`MetaReader` requires an existing SuperTable. Metadata methods require `META` access. Table listing filters out tables without a matching grant; aggregate schema/statistics and `get_super_meta` require a `"*"` table grant. Metadata methods do not apply READ row/column filters. `get_table_schema` returns a one-element list containing a name-to-type mapping. Passing the SuperTable name in place of `table_name` requests the aggregate schema or statistics. `get_table_stats` returns snapshot information with `previous_snapshot`, `schema`, and `location` removed.

`get_super_meta` returns a `{"super": ...}` wrapper with totals and per-table summaries. Its row totals subtract tombstones, and its byte totals sum the current resource list's physical file sizes, including bytes occupied by tombstoned rows until compaction. Neither measures actual query I/O. The summary cache checks the root version and its configured TTL.

`get_simple_table_snapshot` is a low-level storage/catalog method without a role argument. It returns the embedded leaf payload when usable and otherwise reads the snapshot JSON. A historical chain can be traversed by reading each `previous_snapshot` path through `super_table.read_simple_table_snapshot(path)`. The high-level reader has no snapshot-version/as-of parameter.

Sources: [metadata API](../supertable/meta_reader.py), [snapshot API](../supertable/simple_table.py).

## Manage roles and users

```python
from supertable import RoleManager, UserManager

roles = RoleManager(
    super_name=super_name,
    organization=organization,
    actor_role_name="superadmin",
)
role_id = roles.create_role({
    "role_name": "order_reader",
    "role": "reader",
    "tables": {
        "orders": {"columns": ["order_id", "amount"], "filters": ["*"]}
    },
})
users = UserManager(
    super_name=super_name,
    organization=organization,
    actor_role_name="superadmin",
)
user_id = users.create_user({"username": "analyst", "roles": [role_id]})
```

Manager mutations require `actor_role_name` with the `RBAC` permission. User role assignments contain role IDs, while read/write calls accept a role name. The SDK does not authenticate a caller merely because it was given a user ID or `role_name`; an application must select the authorized role. See [RBAC](11_rbac.md) for permissions, filters, tokens, and the boundaries of low-level access.

## Stage input

```python
from supertable import Staging

stage = Staging(
    organization=organization,
    super_name=super_name,
    staging_name="incoming",
)
filename = stage.save_as_parquet(
    role_name=role_name,
    arrow_table=pa.table({"order_id": [3], "amount": [9.0]}),
    base_file_name="orders.parquet",
)
```

This writes a staged file and updates its index. It does not publish table data. `SuperPipe` manages Redis pipe definitions; this package contains no worker that consumes those definitions and ingests the files. See [ingestion and result jobs](07_ingestion.md).

## Quality rules

```python
from supertable import RedisCatalog
from supertable.quality.config import DQConfig

quality = DQConfig(
    RedisCatalog().r,
    organization,
    super_name,
    actor_role_name="superadmin",
)
rule = quality.create_rule({
    "table_name": "orders",
    "rule_type": "column_min",
    "column_name": "amount",
    "threshold": 0,
})
```

Creating, updating, or deleting rules requires `WRITE` authority on the target table and records the acting role for later execution. Rule creation stores metadata; it does not immediately run the query. The scheduler must be explicitly started and configured as described in [monitoring and quality](14_monitoring.md).

The checker builds SQL for `column_min`, `column_max`, `null_rate_max`, `row_count_min`, `distinct_in`, and `custom_sql`. Numeric threshold rules count violations or compare an aggregate with the threshold. `custom_sql` evaluates the first value of the first returned row against an optional maximum threshold. Unknown rule types produce no generated SQL. A successful metadata write does not validate that a rule's SQL will execute successfully.

Sources: [quality configuration](../supertable/quality/config.py), [rule SQL and evaluation](../supertable/quality/checker.py).

## Export, compact, and delete

| Call | Behavior |
| --- | --- |
| `writer.configure_table(role_name, "orders", ...)` | Set positive per-table compaction limits. |
| `writer.compact(role_name, "orders", small_only=True)` | Drain nonempty tombstones and rewrite selected resources under the table lock. |
| `table.export_to(target_dir, compression_level=3, small_only=False)` | Write compacted Parquet to a target directory and return `files`, `files_written`, `total_rows`, `total_bytes`. |
| `table.delete(role_name)` | Require `WRITE`, remove the table storage tree, then its catalog metadata. |
| `super_table.delete(role_name)` | Require `CONTROL` for `*`, remove the SuperTable storage tree, then its catalog metadata. |

Export does not publish a table snapshot, and its method has no authorization parameter. It loads tombstones when available and passes dead IDs to compaction, but uses permissive artifact reads; it is not a strict verification/export transaction. Deletion removes storage before catalog metadata, so failures can leave a partially completed operation.

## Import reference

The package root exports `SuperTable`, `SimpleTable`, `DataWriter`, `DataReader`, `engine`, `MetaReader`, `list_supers`, `list_tables`, `Staging`, `SuperPipe`, `RedisCatalog`, `RoleManager`, `UserManager`, and the lookup/lock-loss errors. `Status` and `query_sql` must be imported from `supertable.data_reader`.

The root lazily resolves `query_odata_sql_stream` and `query_sql_policy_fingerprint` through `supertable.odata`. These helpers prepare query streams and policy metadata; they do not create an HTTP server. Their requirements and engine limitations are described in [data reader](10_data_reader.md).
