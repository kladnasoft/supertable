# 15. Python SDK

## Overview

The `supertable` Python package is a versioned data warehouse library for SQL
analytics. It provides the core data management classes
(`SuperTable`, `SimpleTable`, `DataWriter`, `DataReader`, `MetaReader`,
`RedisCatalog`, `RoleManager`, `UserManager`) and pluggable backends for
storage, query engines, locking, mirroring, and audit logging.

The current version is published as `__version__` on the top-level package:

```python
import supertable
print(supertable.__version__)   # "2.1.1"
```

All public classes can be imported directly from the top-level package:

```python
from supertable import (
    SuperTable, SimpleTable,
    DataWriter, DataReader, engine,
    MetaReader, list_supers, list_tables,
    Staging, SuperPipe,
    RedisCatalog,
    RoleManager, UserManager,
    # Read-side lookup errors — raised when a SELECT or MetaReader
    # references a supertable/table that doesn't exist. All inherit
    # from LookupError so legacy ``except LookupError`` keeps working.
    SupertableLookupError, SuperTableNotFoundError, TableNotFoundError,
)
```

The monitoring drain primitives and the GC orchestration primitive
live in their own subpackages so deployments that don't need them
don't pay the import cost:

```python
from supertable.monitoring import (
    list_drainable_partitions, drain_partition, iter_partition_chunks,
    read_recent, MonitorPartition,
    MONITORING_SINK_TABLE_FOR, MONITORING_SINK_TABLES,
)
from supertable.gc.cleaner import GCCleaner
# Optional daemon wrapper (most deployments call tick() directly):
from supertable.gc.daemon import run_forever, run_all_orgs
```

## Installation

### Basic Install

```bash
pip install supertable
```

### With Cloud Storage Backends

```bash
pip install "supertable[s3]"     # AWS S3
pip install "supertable[minio]"  # MinIO
pip install "supertable[azure]"  # Azure Blob Storage
pip install "supertable[gcp]"    # Google Cloud Storage
pip install "supertable[all]"    # everything
```

### Optional Extras

| Extra | Packages | Description |
|-------|----------|-------------|
| `s3` | `boto3>=1.34,<2.0` | AWS S3 storage backend |
| `minio` | `minio>=7.2,<8.0` | MinIO storage backend |
| `azure` | `azure-storage-blob>=12.26.0` | Azure Blob Storage backend |
| `gcp` | `google-cloud-storage>=3.1.0` | Google Cloud Storage backend |
| `all` | All extras | Full installation |

### Requirements

- Python >= 3.10
- Redis 6+ reachable from the host
- Object storage backend (or local disk)

## Core Classes

### SuperTable

The main coordination object. Ensures storage and Redis metadata are
initialised.

```python
from supertable.super_table import SuperTable

st = SuperTable(super_name="example", organization="my-org")
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `super_name` | `str` | SuperTable name |
| `organization` | `str` | Organization namespace |
| `storage` | `StorageInterface` | Storage backend instance |
| `catalog` | `RedisCatalog` | Redis catalog instance |

| Method | Description |
|--------|-------------|
| `read_simple_table_snapshot(path)` | Read a snapshot JSON from storage |
| `delete(role_name)` | Drop the SuperTable. Destructive. |

### DataWriter

Writes Arrow tables into a SimpleTable.

```python
from supertable.data_writer import DataWriter

dw = DataWriter(super_name="example", organization="my-org")
columns, rows, inserted, deleted = dw.write(
    role_name="superadmin",
    simple_name="facts",
    data=arrow_table,
    overwrite_columns=["day", "client"],
)
```

`write()` returns a tuple `(total_columns, total_rows, inserted, deleted)`.
Optional kwargs: `compression_level=1`, `newer_than=None`, `delete_only=False`,
`lineage=None` (dict with conventional keys — see the
`DataWriter.write` docstring).

### DataReader

Executes SQL queries against the SuperTable.

```python
from supertable.data_reader import DataReader, engine

dr = DataReader(
    super_name="example",
    organization="my-org",
    query="SELECT day, client, sum(value) AS total FROM facts GROUP BY day, client LIMIT 10",
)
df, status, message = dr.execute(role_name="superadmin", engine=engine.AUTO)
print(f"rows={df.shape[0]} cols={df.shape[1]} status={status}")
print(f"timings: {dr.timer.timings}")
print(f"plan_stats: {dr.plan_stats.stats}")
```

Engine values: `engine.AUTO`, `engine.DUCKDB_LITE`, `engine.DUCKDB_PRO`,
`engine.SPARK_SQL`.

### MetaReader

Inspects metadata.

```python
from supertable.meta_reader import MetaReader, list_supers, list_tables

list_supers(organization="my-org")
list_tables(organization="my-org", super_name="example")

mr = MetaReader(organization="my-org", super_name="example")
mr.get_super_meta(role_name="superadmin")
mr.get_table_schema("facts", role_name="superadmin")
mr.get_table_stats("facts", role_name="superadmin")
```

### RoleManager / UserManager

```python
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

rm = RoleManager(super_name="example", organization="my-org")
rm.create_role({"role": "reader", "tables": {"facts": {"columns": ["*"], "filters": []}}})

um = UserManager(super_name="example", organization="my-org")
um.create_user({"username": "alice", "roles": [role_id]})
```

### Read-side errors

`DataReader.execute()` and `MetaReader.__init__()` fail fast against
missing supertables / tables — they do **not** silently bootstrap as
a side effect of opening the reader.

```python
from supertable import (
    DataReader, MetaReader,
    SuperTableNotFoundError, TableNotFoundError,
)

# SELECT against a missing supertable returns Status.ERROR with a
# typed message — the catalog state is not touched.
dr = DataReader(super_name="ghost", organization="acme",
                query="SELECT 1 FROM users")
df, status, message = dr.execute(role_name="superadmin")
# status == Status.ERROR
# message == "SuperTable not found: acme/ghost"

# MetaReader is read-only by contract — opening one against a missing
# name raises SuperTableNotFoundError directly.
try:
    mr = MetaReader(super_name="ghost", organization="acme")
except SuperTableNotFoundError as e:
    print(e.organization, e.super_name)   # "acme" "ghost"
```

Both errors inherit from the stdlib `LookupError` so existing
`except LookupError` / `except KeyError` catchers keep working.

### Constructor opt-out for read-only sessions

`SuperTable.__init__` and `SimpleTable.__init__` both accept
`create_if_missing: bool = True`. The default preserves the writer's
auto-create behaviour; **read-side code passes `False`** so a missing
name surfaces as `SuperTableNotFoundError` / `TableNotFoundError`
instead of being materialised by the constructor:

```python
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable

# Writer-style: bootstrap if missing (default).
st = SuperTable("acme", "my-org")

# Reader-style: refuse to bootstrap.
try:
    st = SuperTable("ghost", "my-org", create_if_missing=False)
except SuperTableNotFoundError:
    ...
```

### Monitoring orchestration

Recent-tail reads (UI "last 100"):

```python
from supertable.monitoring import read_recent
from supertable.redis_catalog import RedisCatalog

last_100 = read_recent(
    RedisCatalog(),
    organization="acme",
    monitor_type="writes",
    limit=100,
    max_days_back=7,   # default; clamp [1, 90]
)
# newest first, list[dict], read-only, never raises
```

Drain orchestration — flush yesterday's partitions into internal
sink tables (your service owns the loop; the SDK only provides
primitives):

```python
from supertable.monitoring import (
    list_drainable_partitions, drain_partition,
    MONITORING_SINK_TABLE_FOR,
)
from supertable.data_writer import DataWriter

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
        continue
    DataWriter(internal_super, part.organization).write(
        role_name="system", simple_name=sink_table,
        data=to_arrow(rows), overwrite_columns=[],
    )
```

For huge partitions, swap `drain_partition` for `iter_partition_chunks`
to stream in memory-bounded slices. See chap. 14 for atomicity and
crash-recovery semantics.

### GC orchestration

Deferred-deletion of sunset parquets and pruned snapshot JSONs runs
under the same caller-owned-scheduling model:

```python
from supertable.gc.cleaner import GCCleaner
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

cleaner = GCCleaner(
    org="acme",
    catalog=RedisCatalog(),
    storage=get_storage(),
    # optional overrides — settings.SUPERTABLE_GC_* are the defaults
    delay_sec=1800, batch_size=500,
)

# Inside your service's own scheduler:
while not service.shutdown_requested:
    stats = cleaner.tick()
    service.publish_metrics(stats)
    service.sleep(60)
```

For single-node installs that don't have a scheduler, a convenience
daemon wraps `tick()` with a tick/sleep/repeat loop and a CLI:

```bash
python -m supertable.gc.daemon --org acme
python -m supertable.gc.daemon --all-orgs   # multi-tenant
```

See chap. 17 for the full lifecycle.

### Staging / SuperPipe

```python
from supertable.staging_area import Staging
from supertable.super_pipe import SuperPipe

stage = Staging(organization="my-org", super_name="example", staging_name="stage_demo")
stage.save_as_parquet(role_name="superadmin", arrow_table=arrow_table, base_file_name="batch_1")

pipe = SuperPipe(organization="my-org", super_name="example", staging_name="stage_demo")
pipe.create(
    role_name="superadmin",
    pipe_name="pipe_01",
    simple_name="facts",
    user_hash=user_hash,
    overwrite_columns=["day"],
)
```

## Demos

The package ships two runnable demos under `supertable.demo`:

- `supertable.demo.quickstart` — numbered API tutorial. Run via the
  `supertable-demo-quickstart` console script or `python -m
  supertable.demo.quickstart`.
- `supertable.demo.webshop` — synthetic webshop dataset generator + loader.
  Console scripts: `supertable-demo-webshop-generate`,
  `supertable-demo-webshop-load`, `supertable-demo-webshop-topup`.

Each individual quickstart step can also be invoked directly, e.g.
`python -m supertable.demo.quickstart.s03_08_read_snapshot_history`.

### Quickstart index

| Module | Description |
|--------|-------------|
| `controller` | Run all quickstart steps in order (used by `python -m supertable.demo.quickstart`) |
| `s01_01_01_create_super_table` | Create a SuperTable |
| `s01_01_02_enable_mirroring_formats` | Enable Delta / Iceberg mirroring |
| `s01_02_create_roles` | Create RBAC roles |
| `s01_03_create_users` | Create users + default superuser |
| `s02_01_write_dummy_data` | Write 7 fixtures into the same SimpleTable |
| `s02_02_write_single_data` | Single write with `lineage` dict |
| `s02_03_01_write_staging` | Save Arrow table into a staging area |
| `s02_03_02_create_pipe` | Configure an automated ingestion pipe |
| `s02_04_01_write_monitoring_simple` | Single metric via MonitoringWriter |
| `s02_04_02_write_monitoring_parallel` | Parallel metric writes |
| `s02_05_write_tombstone` | Soft-delete via `delete_only=True` |
| `s03_01_read_data_error` | Read returning an error response |
| `s03_02_01_read_super_data_ok` | Read with `engine.AUTO` and `engine.SPARK_SQL` |
| `s03_02_02_read_table_data_ok` | Aggregation query against `facts` |
| `s03_03_read_meta` | Schema and stats via `MetaReader` |
| `s03_04_read_staging` | List files in a staging area |
| `s03_06_01_read_roles` | Inspect roles via `RoleManager` |
| `s03_06_02_read_user` | Inspect users via `UserManager` |
| `s03_07_01_estimate_read` | Pre-flight bytes estimate |
| `s03_07_02_estimate_files` | Pre-flight file breakdown |
| `s03_08_read_snapshot_history` | Walk the snapshot linked list |
| `s04_01_03_delete_pipe` | Delete an ingestion pipe |
| `s05_01_delete_table` | Drop a SimpleTable (destructive) |
| `s05_02_delete_super_table` | Drop a SuperTable (destructive) |

`supertable.demo.quickstart.defaults` centralises the constants used by
every script (`organization`, `super_name`, `simple_name`, `role_name`,
`staging_name`, `overwrite_columns`).

## Project Metadata

| Field | Value |
|-------|-------|
| Package name | `supertable` |
| Python | `>=3.10` |
| License | Super Table Public Use License (STPUL) v1.0 |
| Homepage | https://github.com/kladnasoft/supertable |

## Source Files

- `pyproject.toml` — package metadata and extras.
- `supertable/super_table.py` — `SuperTable` class (`create_if_missing` kwarg).
- `supertable/simple_table.py` — `SimpleTable` class (`create_if_missing` kwarg).
- `supertable/data_writer.py` — `DataWriter` class + monitoring-sink loop guard + GC enqueue post-CAS.
- `supertable/data_reader.py` — `DataReader` class with `_assert_targets_exist` pre-flight, `query_sql()` helper.
- `supertable/meta_reader.py` — `MetaReader`, `list_supers`, `list_tables` (read-only by contract).
- `supertable/errors.py` — `SupertableLookupError`, `SuperTableNotFoundError`, `TableNotFoundError`.
- `supertable/redis_catalog.py` — `RedisCatalog` class.
- `supertable/staging_area.py` — `Staging` class.
- `supertable/super_pipe.py` — `SuperPipe` class.
- `supertable/rbac/` — `RoleManager`, `UserManager`, filter / permission utilities.
- `supertable/monitoring/partitions.py` — drain orchestration primitives (`list_drainable_partitions`, `drain_partition`, `iter_partition_chunks`, `read_recent`), `MonitorPartition` NamedTuple, `MONITORING_SINK_TABLES` / `MONITORING_SINK_TABLE_FOR` constants.
- `supertable/monitoring_writer.py` — `MonitoringWriter` (daily-partitioned RPUSH), `get_monitoring_logger` singleton factory, `NullMonitoringLogger` fallback.
- `supertable/gc/cleaner.py` — `GCCleaner` orchestration primitive (`tick()`).
- `supertable/gc/daemon.py` — `run_forever`, `run_all_orgs`, `python -m supertable.gc.daemon` CLI.
- `supertable/gc/queue.py` — `enqueue_deletions`, `collect_old_snapshot_paths`, `nuke_stream` helpers used by `DataWriter`.
- `supertable/demo/quickstart/` — numbered API tutorial steps.
- `supertable/demo/webshop/` — synthetic webshop dataset demo.
