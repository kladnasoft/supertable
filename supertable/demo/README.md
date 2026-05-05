# SuperTable demos

Self-contained demonstrations bundled with `pip install supertable`. Both
demos talk to a live Redis + storage backend, so configure your environment
first — see [../../docs/02_configuration.md](../../docs/02_configuration.md).

## quickstart

A numbered sequence of small, focused scripts that walks through the full
SDK surface: create a SuperTable, configure RBAC, write fixtures, exercise
ingestion / staging / pipes, run reads with different engines, inspect
metadata, walk snapshot history, and tear everything down.

Run all steps in order:

```bash
supertable-demo-quickstart
# or
python -m supertable.demo.quickstart
```

Run any individual step directly:

```bash
python -m supertable.demo.quickstart.s01_01_01_create_super_table
python -m supertable.demo.quickstart.s03_08_read_snapshot_history
```

### Quickstart steps

| Module | Concept |
|---|---|
| `s01_01_01_create_super_table` | Bootstrap a SuperTable + Redis catalog |
| `s01_01_02_enable_mirroring_formats` | Turn on Delta / Iceberg mirroring |
| `s01_02_create_roles` / `s01_03_create_users` | RBAC setup |
| `s02_01_write_dummy_data` | Multi-batch upsert with schema evolution |
| `s02_02_write_single_data` | Single write with `lineage` payload |
| `s02_03_01_write_staging` / `s02_03_02_create_pipe` | Staging area + pipe |
| `s02_04_01_write_monitoring_simple` / `s02_04_02_write_monitoring_parallel` | `MonitoringWriter` |
| `s02_05_write_tombstone` | Soft-delete via `delete_only=True` |
| `s03_02_01_read_super_data_ok` / `s03_02_02_read_table_data_ok` | Reads with `engine.AUTO` and `engine.SPARK_SQL` |
| `s03_03_read_meta` | Schema and stats via `MetaReader` |
| `s03_07_01_estimate_read` / `s03_07_02_estimate_files` | Pre-flight estimation |
| `s03_08_read_snapshot_history` | Walk the snapshot linked list |
| `s05_01_delete_table` / `s05_02_delete_super_table` | Destructive teardown (commented out in the controller) |

Shared constants live in `supertable.demo.quickstart.defaults`; fixtures in
`supertable.demo.quickstart.dummy_data`.

## webshop

A larger end-to-end demo: synthesise a realistic webshop dataset
(categories, products, customers, sessions, orders, inventory) and load it
into SuperTable. Three console-script entry points:

| Console script | Module | Purpose |
|---|---|---|
| `supertable-demo-webshop-generate` | `supertable.demo.webshop.generate` | One-shot historical generation (writes parquet to disk) |
| `supertable-demo-webshop-load` | `supertable.demo.webshop.load` | Load the generated parquet files into SuperTable via `DataWriter` |
| `supertable-demo-webshop-topup` | `supertable.demo.webshop.topup` | Continuous incremental top-up against SuperTable |

Typical flow:

```bash
# 1. Generate ~1.2M synthetic rows on disk
supertable-demo-webshop-generate

# 2. Load into SuperTable
supertable-demo-webshop-load

# 3. (Optional) keep the data fresh
supertable-demo-webshop-topup --sleep-minutes 5
```

The `WebshopDataGenerator` engine itself lives in
`supertable.demo.webshop.core`; shared connection / output settings are in
`supertable.demo.webshop.defaults`.
