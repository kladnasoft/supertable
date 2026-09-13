# Data model

SuperTable stores table contents in Parquet, snapshot documents in the selected storage backend, and current metadata in Redis. This chapter describes the objects written by the active Python implementation.

## Namespaces and objects

| Object | Identity | Responsibility |
| --- | --- | --- |
| Organization | `organization` | Namespace for SuperTables and organization-level services. |
| SuperTable | `(organization, super_name)` | Redis root, table namespace, roles, users, configuration. |
| SimpleTable | `(organization, super_name, simple_name)` | Versioned resource list and associated schema, statistics, and deletion vector. |
| Resource | Storage path in a snapshot | A Parquet file with physical row, column, and byte counts. |
| Snapshot | Unique JSON storage path | State of one SimpleTable at one update. |

`SuperTable(..., create_if_missing=True)` creates a missing root and initializes the default role/user records. `SimpleTable(..., create_if_missing=True)` writes an initial snapshot and publishes its leaf pointer. Passing `False` makes these constructors raise a typed lookup error for an absent object. Constructing `DataWriter` uses the creating SuperTable constructor; constructing `MetaReader` uses the noncreating constructor.

The writer's local validation checks table names of 1–128 characters against `^[A-Za-z_][A-Za-z0-9_]*$` and rejects a SimpleTable name equal to its SuperTable name. Redis key construction imposes additional constraints: ordinary segments are lowercase and at most 64 characters, with a separate double-underscore form for internal names. For an ordinary SimpleTable name that passes both layers, use `[a-z][a-z0-9_]{0,63}`. Uppercase names, ordinary leading underscores, and names longer than 64 characters can pass writer validation but fail catalog key construction. See [Redis layout](16_redis_layout.md) for the complete segment rules.

Reserved SuperTable names are checked by `is_reserved_super_name`; organization and SuperTable segments also pass through Redis key validation. These checks belong to their named entry points and do not establish a general schema constraint on every low-level operation.

Sources: [SuperTable](../supertable/super_table.py), [SimpleTable](../supertable/simple_table.py), [writer validation](../supertable/data_writer.py), [reserved namespaces](../supertable/redis_keys.py).

## Storage layout

Paths below are logical paths supplied to the storage backend. Cloud backends can add their configured object prefix. Local paths are normally relative to the application home selected at import time.

```text
<organization>/<super_name>/
  super/
  tables/<simple_name>/
    snapshots/<milliseconds>_<random>_tables.json
    data/year=YYYY/month=MM/day=DD/<milliseconds>_<random>_data.parquet
    tombstone/year=YYYY/month=MM/day=DD/hour=HH/<milliseconds>_<random>_deleted.parquet
    stats/year=YYYY/month=MM/day=DD/hour=HH/<milliseconds>_<random>_stats.parquet
```

Data files receive a daily UTC path when the frame contains `__timestamp__`; otherwise the writer uses the supplied data directory directly. The path date is the current write date, including during compaction, rather than a partition extracted from business data. Tombstone and statistics artifacts use the current UTC hour. Snapshot names contain milliseconds and eight random bytes encoded as hexadecimal.

Sources: [Parquet and artifact writers](../supertable/processing.py), [filename and partition helpers](../supertable/utils/helper.py), [storage](04_storage.md).

## Snapshot document

The initial snapshot contains the following fields:

| Field | Initial value / meaning |
| --- | --- |
| `simple_name` | SimpleTable name. |
| `location` | Logical table directory. |
| `snapshot_version` | `0`; incremented by `SimpleTable.update`. |
| `last_updated_ms` | Timestamp in milliseconds. |
| `previous_snapshot` | `None` initially; previous snapshot path after an update. |
| `schema` | Empty list initially; normally a name-to-Polars-type dictionary after a write. |
| `resources` | Empty list initially; current Parquet resources after writes. |
| `tombstone` | `None`, a legacy single path, or a list of deletion-vector part paths. |
| `tombstone_rows` | Number of row IDs represented by the current deletion vector. |
| `stats_file` | Statistics Parquet path, or `None`. |
| `stats_rows` | Number of rows in the statistics artifact, not data rows. |

Updates may also add:

- `lineage`: caller-provided dictionary or a writer-generated operation description.
- `rowid_high_watermark`: highest reserved incoming row ID recorded by the writer.
- `schemaString`: JSON serialization with `type="struct"` and `fields` set to the collected schema. Since the usual collector returns a dictionary, this field is not guaranteed to be a Spark-compatible field list.

A typical resource entry is:

```json
{
  "file": "acme/warehouse/tables/orders/data/year=2026/month=09/day=13/example_data.parquet",
  "file_size": 16384,
  "rows": 250,
  "columns": 5
}
```

`columns` is a count in new resources written by `processing.py`. Some readers also accept older list-based representations. Resource rows are physical counts, including rows that remain in the file but have been tombstoned.

`SimpleTable.update` merges retained and new resources, sets the predecessor path, writes a new JSON document, and returns `(snapshot_dict, snapshot_path)`. It does **not** publish the Redis pointer. `DataWriter` performs that later.

Source: [snapshot construction and update](../supertable/simple_table.py).

## Row identity and deletion

For non-delete writes, Redis reserves an integer range before acquiring the table lock. The writer assigns these values to `__rowid__` as `Int64` and sets `__timestamp__` to the write's current UTC time. Supplying columns with those names does not preserve their incoming values. Failed or filtered writes can leave gaps in allocated IDs.

Overwrite keys identify existing rows to retire. An overwrite produces new data files plus deletion-vector rows with columns `file` and `__rowid__`. Reads apply the deletion vector to hide retired rows. A table's logical row-count estimate is:

```text
max(0, sum(resource.rows) - tombstone_rows)
```

This is the formula used by the metadata summary and the row-identity helper. It is based on metadata, not a fresh count of visible rows.

`rowid_high_watermark` is carried forward and raised to cover newly reserved ranges. OData identity checks reject a missing or negative watermark and a live-row count larger than the watermark. This is a metadata consistency check; it does not scan all row IDs to prove uniqueness.

Sources: [row allocation and mutations](../supertable/data_writer.py), [deletion-vector format](../supertable/processing.py), [identity checks](../supertable/odata/row_identity.py).

## Schema behavior

Ordinary writes record the incoming frame's schema, including system columns. They do not compute a complete schema union over every retained file. Explicit compaction and writes that perform small-file compaction construct a model schema from rewritten files, with a fallback to the prior schema. That metadata helper keeps the first observed type for a repeated column name; it does not reconcile types across all output files.

When compaction combines frames, missing columns are filled with nulls. For mixed numeric types, the union helpers select `Int64` or `Float64` and use permissive casts; unsupported combinations can become strings. These conversions are not always lossless: for example, an unsigned value outside the `Int64` range can become null during a conflicting-integer cast. Native Parquet queries use name-based schema union; metadata schemas and the columns physically present across all files can therefore differ.

There is no primary-key constraint or incoming-batch uniqueness check in `DataWriter.write`. Multiple incoming rows with the same overwrite key can survive together. `newer_than` compares incoming values against existing matching data; it does not choose a single winner within the incoming batch.

Sources: [schema collection](../supertable/utils/helper.py), [schema union and overwrite matching](../supertable/processing.py), [write path](06_data_writer.md).

## Statistics artifacts

Statistics are extracted from Parquet row-group metadata. Each row identifies a data file, row group, and user column. The schema contains:

- Identity/type fields: `file_path`, `row_group_id`, `column_name`, `physical_type`, `logical_type`.
- Bound pairs: `min_bigint`/`max_bigint`, `min_double`/`max_double`, `min_timestamp`/`max_timestamp`, `min_string`/`max_string`.
- Other metadata: `null_count`, `row_group_rows`, `compressed_bytes`, `stats_available`, `min_is_exact`, `max_is_exact`.

`__rowid__` and `__timestamp__` are excluded from this artifact. Unsupported or unavailable bounds cannot establish that a file is irrelevant. Write-overlap pruning and read-predicate pruning use the stored bounds conservatively. Updating statistics removes entries for sunset resources and adds entries for newly written files.

Source: [`STATS_SCHEMA`, extraction, and pruning](../supertable/processing.py).

## Versions and historical files

The Redis leaf document has its own version counter, separate from `snapshot_version`. The root has another counter updated after leaf publication. These counters describe different objects and should not be substituted for one another.

Snapshot predecessor links preserve a chain of table states. Removing a resource from the current snapshot does not delete its physical object. The normal write and compact paths retain those old files; they do not implement a general vacuum or a transactional rollback API. Deleting a SimpleTable removes its native `tables/<simple_name>/` tree and catalog entries, but does not remove its separately located mirrors. Deleting the containing SuperTable removes the entire `<organization>/<super_name>/` tree, including mirrors.

See [catalog publication](05_redis_catalog.md), [writer commit sequence](06_data_writer.md), and [known implementation gaps](TODO.md).

## Query planning objects

[`data_classes.py`](../supertable/data_classes.py) defines internal values used between parsing, estimation, and execution:

| Type | Fields / use |
| --- | --- |
| `TableDefinition` | SuperTable, SimpleTable, SQL alias, referenced columns. |
| `SuperSnapshot` | Table identity, leaf version, resolved file paths, referenced columns. |
| `Reflection` | Storage type, estimated bytes, file count, snapshots, freshness, RBAC and tombstone view mappings. |
| `RbacViewDef` | Allowed columns and SQL row predicate. |
| `TombstoneDef` | Deletion-vector path(s) and cache key. |
| `PredInterval` | Typed lower/upper predicate bounds and inclusivity. |

These objects carry a query's resolved inputs; they are not the stored table snapshot format.
