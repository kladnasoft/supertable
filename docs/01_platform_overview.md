# SuperTable — Platform Overview

## What is SuperTable

SuperTable is a versioned data lake library for SQL analytics. Every write
creates an immutable Parquet snapshot on object storage; metadata, locks, and
audit state live in Redis. The library is distributed as the
`pip install supertable` package and can be embedded in Python applications,
notebooks, and pipelines.

Core capabilities:

- **Versioned, append-only storage** — every write creates an immutable snapshot.
- **Multi-cloud portability** — same code runs against AWS S3, Azure Blob,
  Google Cloud Storage, MinIO, or local disk via a pluggable storage backend.
- **SQL-first analytics** — automatic engine selection (DuckDB for
  small/medium, Spark SQL via Thrift for large datasets).
- **RBAC with row + column security** — role-based access control enforced at
  read time through view chains.
- **Tamper-evident audit log** — SHA-256 hash chain in Redis Streams with
  Parquet export for long-term retention.
- **Mirror formats** — optional export to Delta Lake, Iceberg, or plain
  Parquet after every write.

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

There is no built-in HTTP server, web UI, OData, or MCP integration in this
package. Those layers, when present, are deployed as separate services that
import the SDK.

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Metadata store | Redis 7 (standalone or Sentinel HA) |
| Query engines | DuckDB (embedded), Spark SQL (via Thrift) |
| Data format | Apache Parquet (via PyArrow) |
| Data processing | Polars, Pandas, NumPy |
| SQL parsing | SQLGlot |
| Storage SDKs | boto3, minio, azure-storage-blob, google-cloud-storage |

## Installation

```bash
pip install supertable              # core + local storage
pip install supertable[s3]          # + AWS S3
pip install supertable[minio]       # + MinIO
pip install supertable[azure]       # + Azure Blob
pip install supertable[gcp]         # + Google Cloud Storage
pip install supertable[all]         # all cloud backends
```

## Package Structure

```
supertable/
├── audit/               # Audit logging & compliance
├── config/              # Configuration (env vars, defaults, homedir, settings)
├── engine/              # Query engines (DuckDB Lite/Pro, Spark Thrift)
├── locking/             # Distributed locking (Redis + file)
├── mirroring/           # Table format export (Delta, Iceberg, Parquet)
├── rbac/                # Role-based access control
├── storage/             # Storage backends (S3, MinIO, Azure, GCP, Local)
├── utils/               # SQL parser, helpers, timer
├── super_table.py       # Core coordination class
├── simple_table.py      # Versioned append-only table
├── data_reader.py       # Read interface
├── data_writer.py       # Write interface
├── meta_reader.py       # Metadata reading
├── monitoring_writer.py # Metrics ingestion to Redis lists
├── processing.py        # Parquet processing engine
├── redis_catalog.py     # Redis metadata operations
├── redis_connector.py   # Redis connection management
├── staging_area.py      # Staging areas for ingestion
├── super_pipe.py        # Automated ingestion pipes
├── server_common.py     # Shared helpers
├── service_registry.py  # Process heartbeat & discovery
├── plan_extender.py     # Query plan augmentation
├── query_plan_manager.py# Plan persistence
├── logging.py           # Structured logging
└── data_classes.py      # Shared data structures
```

## Data Flow

```
Write Path:
  DataWriter.write() → acquire lock → validate schema → resolve overlap
    → write Parquet → update Redis catalog → mirror (optional) → release lock

Read Path:
  DataReader.execute() → resolve tables → build view chain → estimate size
    → select engine (DuckDB Lite/Pro/Spark) → execute SQL → return DataFrame

View Chain (transparent to caller):
  Base Parquet files → Dedup view → Tombstone view → RBAC view → User query
```

## Service Registry

Every long-lived process that uses the SDK can register itself in Redis
with a 30-second TTL key, refreshed every 15 seconds
(`service_registry.py`). If the process crashes, the key expires
automatically.

Key pattern:
`dataisland:{org}:registry:{service_type}:{hostname}:{pid}`

The registry is a **platform concern**, not a SuperTable concern —
every dataisland-core service plus any SDK-using app (Lighthouse,
Gatekeeper, …) writes a heartbeat here. The `dataisland:` prefix
keeps it cleanly separated from the SuperTable SDK's own state under
`supertable:`. See [16 Redis Key Layout](16_redis_layout.md) for the
full prefix policy.

## Cross-References

| Doc | Topic |
|-----|-------|
| [02 Configuration](02_configuration.md) | All environment variables |
| [03 Data Model](03_data_model.md) | Organization → SuperTable → SimpleTable hierarchy |
| [04 Storage](04_storage.md) | Storage backends (S3, MinIO, Azure, GCP, Local) |
| [05 Redis Catalog](05_redis_catalog.md) | Metadata store and key naming |
| [06 Data Writer](06_data_writer.md) | Write pipeline with locking and dedup |
| [07 Ingestion](07_ingestion.md) | Staging areas and pipes |
| [08 Locking](08_locking.md) | Distributed locking (Redis + file) |
| [09 Query Engine](09_query_engine.md) | DuckDB Lite/Pro, Spark SQL, engine selection |
| [10 Data Reader](10_data_reader.md) | Read facade, view chain, plan stats |
| [11 RBAC](11_rbac.md) | Roles, users, row/column-level security |
| [12 Audit](12_audit.md) | Compliance logging, hash chains, SIEM |
| [13 Mirroring](13_mirroring.md) | Delta Lake, Iceberg, Parquet export |
| [14 Monitoring](14_monitoring.md) | Metrics ingestion |
| [15 Python SDK](15_python_sdk.md) | pip package usage |
