# Data Island Core — Documentation Index

## Foundation

| # | Document | Description |
|---|---|---|
| 01 | [Platform Overview](01_platform_overview.md) | Architecture, deployment, directory structure, quick start |
| 02 | [Configuration & Settings](02_configuration.md) | All 100+ environment variables with types, defaults, and descriptions |

## Storage & Data

| # | Document | Description |
|---|---|---|
| 03 | [Storage Backends](03_storage.md) | StorageInterface, S3, MinIO, Azure, GCP, local — setup and usage |
| 04 | [Redis Catalog](04_redis_catalog.md) | Metadata store — key naming, all catalog operations, inspection |
| 05 | [Data Model](05_data_model.md) | Organization → SuperTable → SimpleTable hierarchy, snapshots, versioning |

## Read & Write Path

| # | Document | Description |
|---|---|---|
| 06 | [Data Writer](06_data_writer.md) | Write pipeline — locking, overlap detection, dedup, tombstones |
| 07 | [Query Engine](07_query_engine.md) | SQL execution — DuckDB Lite/Pro, Spark SQL, engine selection |
| 08 | [Ingestion & Pipes](08_ingestion.md) | File upload, staging areas, pipe configuration and execution |

## Security & Compliance

| # | Document | Description |
|---|---|---|
| 09 | [RBAC & Access Control](09_rbac.md) | Roles, users, row-level and column-level security, permissions |
| 10 | [Audit Log](10_audit_log.md) | Event logging, hash chain integrity, DORA/SOC 2 compliance, SIEM |

## API & Connectivity

| # | Document | Description |
|---|---|---|
| 11 | [REST API](11_rest_api.md) | All 76 endpoints — auth, session, request/response formats |
| 12 | [MCP Server](12_mcp.md) | AI tool integration — tools, transport, auth, client setup |

## Operations

| # | Document | Description |
|---|---|---|
| 13 | [Monitoring](13_monitoring.md) | Read/write metrics, structured logging, correlation IDs |
| 14 | [Data Quality](14_data_quality.md) | 16 built-in checks, anomaly detection, quality scores, scheduling |
| 15 | [Mirroring](15_mirroring.md) | Delta Lake, Iceberg, Parquet export after writes |
| 16 | [Locking](16_locking.md) | Redis distributed locks, heartbeat renewal, deadlock prevention |
