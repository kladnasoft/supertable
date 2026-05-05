# SuperTable

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)
![Version](https://img.shields.io/badge/version-2.0.0-brightgreen)

**SuperTable — versioned data lake library for SQL analytics.**

SuperTable stores structured data as immutable Parquet snapshots on object
storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local disk), keeps
metadata, locks, and audit state in Redis, and queries everything through
DuckDB (embedded) or Spark SQL. It is a Python library — there is no
separate server process.

## Installation

```bash
pip install supertable                # core + local storage
pip install "supertable[s3]"          # AWS S3
pip install "supertable[minio]"       # MinIO
pip install "supertable[azure]"       # Azure Blob
pip install "supertable[gcp]"         # Google Cloud Storage
pip install "supertable[all]"         # everything
```

Requirements: Python 3.10+, a reachable Redis 6+, and a configured storage
backend (or local disk for development). See
[docs/02_configuration.md](docs/02_configuration.md) for environment
variables.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                Python application                 │
│   (notebooks, ETL jobs, FastAPI handlers, etc.)   │
└──────────┬─────────────────────────┬──────────────┘
           │ DataWriter / DataReader │
           ▼                         ▼
   ┌───────────────┐        ┌────────────────────┐
   │  RedisCatalog │        │  StorageInterface  │
   │  metadata     │        │  Parquet files     │
   │  locks        │        │  S3 / MinIO /      │
   │  audit chain  │        │  Azure / GCP /     │
   └───────────────┘        │  Local             │
                            └────────────────────┘
```

Data is organised as **Organization → SuperTable → SimpleTable**. Each
`SimpleTable` is a versioned, append-only collection of Parquet files
backed by a snapshot linked list — every write produces a new immutable
snapshot whose `previous_snapshot` points at the predecessor.

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Metadata store | Redis 6+ (standalone or Sentinel HA) |
| Query engine (primary) | DuckDB |
| Query engine (large) | Spark SQL via Thrift |
| Data format | Apache Parquet |
| Object storage | MinIO / S3 / Azure / GCP / local |
| Mirror formats | Delta Lake, Apache Iceberg, Parquet |
| Audit storage | Redis Streams + Parquet |

---

## Quick example

```python
from supertable import SuperTable, DataWriter, DataReader, engine

# Bootstrap catalogue + storage
SuperTable(super_name="example", organization="my-org")

# Write
dw = DataWriter(super_name="example", organization="my-org")
columns, rows, inserted, deleted = dw.write(
    role_name="superadmin",
    simple_name="facts",
    data=arrow_table,
    overwrite_columns=["day", "client"],
    lineage={"source_type": "manual", "source_id": "my-job"},
)

# Read
dr = DataReader(
    super_name="example",
    organization="my-org",
    query="SELECT day, sum(value) FROM facts GROUP BY day LIMIT 10",
)
df, status, message = dr.execute(role_name="superadmin", engine=engine.AUTO)
```

---

## Demos

The package ships two runnable demos under `supertable.demo`:

```bash
# Numbered tutorial — runs the full lifecycle end-to-end.
supertable-demo-quickstart
# or
python -m supertable.demo.quickstart

# Synthetic webshop dataset.
supertable-demo-webshop-generate    # build ~1.2M rows on disk
supertable-demo-webshop-load        # load them into SuperTable
supertable-demo-webshop-topup       # continuous incremental refresh
```

Both demos are also runnable as module steps. Examples:

```bash
python -m supertable.demo.quickstart.s01_01_01_create_super_table
python -m supertable.demo.quickstart.s03_08_read_snapshot_history
python -m supertable.demo.webshop.generate
```

See [supertable/demo/README.md](supertable/demo/README.md) for the full
script index.

---

## What's included

- **Versioned tables** with snapshot isolation, upsert (`overwrite_columns`),
  soft deletes (`delete_only=True`), schema evolution, and staleness filtering
- **DuckDB query engine** — embedded, zero-copy reads from object storage
- **Spark SQL via Thrift** — for queries exceeding DuckDB memory limits
- **RBAC** — role types (superadmin, admin, writer, reader, meta) with
  row-level and column-level security enforced through view chains
- **Audit logging** — tamper-evident SHA-256 hash chain in Redis Streams with
  Parquet export
- **Monitoring** — `MonitoringWriter` pushes read/write/metric payloads to
  Redis lists; structured JSON logging with correlation IDs
- **Ingestion** — staging areas (`Staging`) and automated ingestion pipes
  (`SuperPipe`)
- **Mirroring** — optional Delta Lake / Iceberg / Parquet export after every
  write
- **Snapshot history** — every write chains to `previous_snapshot`, enabling
  point-in-time inspection without separate historical tables

---

## Documentation

See [docs/00_index.md](docs/00_index.md) for the full table of contents.

| # | Document | Description |
|---|---|---|
| 01 | [Platform Overview](docs/01_platform_overview.md) | Architecture, package layout, deployment, data flow |
| 02 | [Configuration](docs/02_configuration.md) | Environment variables and runtime settings |
| 03 | [Data Model](docs/03_data_model.md) | Organization → SuperTable → SimpleTable hierarchy |
| 04 | [Storage Backends](docs/04_storage.md) | StorageInterface, S3, MinIO, Azure, GCP, local |
| 05 | [Redis Catalog](docs/05_redis_catalog.md) | Metadata store, key naming, operations, CAS |
| 06 | [Data Writer](docs/06_data_writer.md) | Write pipeline, locking, dedup, tombstones |
| 07 | [Ingestion & Pipes](docs/07_ingestion.md) | Staging areas, automated ingestion pipes |
| 08 | [Distributed Locking](docs/08_locking.md) | Redis locks, file locks, deadlock prevention |
| 09 | [Query Engine](docs/09_query_engine.md) | DuckDB Lite/Pro, Spark SQL, auto selection |
| 10 | [Data Reader](docs/10_data_reader.md) | Read facade, snapshot history, view chain |
| 11 | [RBAC & Access Control](docs/11_rbac.md) | Roles, users, row/column security |
| 12 | [Audit Logging](docs/12_audit.md) | SHA-256 hash chain, DORA/SOC 2, SIEM |
| 13 | [Table Mirroring](docs/13_mirroring.md) | Delta Lake, Iceberg, Parquet export |
| 14 | [Monitoring](docs/14_monitoring.md) | Metrics writer, structured logging |
| 15 | [Python SDK](docs/15_python_sdk.md) | Core classes, demos, example index |

---

## License

Super Table Public Use License (STPUL) v1.0 — see [LICENSE](LICENSE).

Copyright © Kladna Soft Kft. All rights reserved.
