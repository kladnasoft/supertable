# SuperTable — Documentation Index

## Foundation

| # | Document | Description |
|---|----------|-------------|
| 01 | [Platform Overview](01_platform_overview.md) | Architecture, package layout, deployment, data flow |
| 02 | [Configuration](02_configuration.md) | Environment variables and runtime settings |

## Data Foundation

| # | Document | Description |
|---|----------|-------------|
| 03 | [Data Model](03_data_model.md) | Organization → SuperTable → SimpleTable hierarchy, snapshots |
| 04 | [Storage Backends](04_storage.md) | StorageInterface, S3, MinIO, Azure, GCP, Local |
| 05 | [Redis Catalog](05_redis_catalog.md) | Metadata store key naming, operations, CAS |

## Write Path

| # | Document | Description |
|---|----------|-------------|
| 06 | [Data Writer](06_data_writer.md) | Write pipeline, locking, dedup, tombstones, schema alignment |
| 07 | [Ingestion & Pipes](07_ingestion.md) | Staging areas, automated ingestion pipes |
| 08 | [Distributed Locking](08_locking.md) | Redis locks with heartbeat, file locks, deadlock prevention |

## Read Path

| # | Document | Description |
|---|----------|-------------|
| 09 | [Query Engine](09_query_engine.md) | DuckDB Lite/Pro, Spark SQL, auto selection, view chains |
| 10 | [Data Reader](10_data_reader.md) | Read facade, snapshot history, view chain, plan stats |

## Security & Compliance

| # | Document | Description |
|---|----------|-------------|
| 11 | [RBAC & Access Control](11_rbac.md) | Roles, users, row/column security, filter builder |
| 12 | [Audit Logging](12_audit.md) | Event logging, SHA-256 hash chain, DORA/SOC 2 |

## Data Operations & Observability

| # | Document | Description |
|---|----------|-------------|
| 13 | [Table Mirroring](13_mirroring.md) | Delta Lake, Apache Iceberg, Parquet export after writes |
| 14 | [Monitoring](14_monitoring.md) | Metrics writer, structured JSON logging, correlation IDs |

## SDK

| # | Document | Description |
|---|----------|-------------|
| 15 | [Python SDK](15_python_sdk.md) | pip install, optional cloud extras, core classes, usage |
