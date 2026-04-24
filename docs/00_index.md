# SuperTable — Documentation Index

## Foundation

| # | Document | Description |
|---|----------|-------------|
| 01 | [Platform Overview](01_platform_overview.md) | Architecture, 4-server design, tech stack, deployment modes, data flow |
| 02 | [Configuration](02_configuration.md) | All 100+ environment variables with types, defaults, and fallback chains |

## Data Foundation

| # | Document | Description |
|---|----------|-------------|
| 03 | [Data Model](03_data_model.md) | Organization → SuperTable → SimpleTable hierarchy, versioned snapshots, dataclasses |
| 04 | [Storage Backends](04_storage.md) | StorageInterface, S3, MinIO, Azure, GCP, Local — setup, auth, and factory pattern |
| 05 | [Redis Catalog](05_redis_catalog.md) | Metadata store — key naming taxonomy, all catalog operations, CAS, Sentinel |

## Write Path

| # | Document | Description |
|---|----------|-------------|
| 06 | [Data Writer](06_data_writer.md) | Write pipeline — locking, overlap detection, dedup, tombstones, schema alignment |
| 07 | [Ingestion & Pipes](07_ingestion.md) | File upload, staging areas, pipe configuration, automated ingestion |
| 08 | [Distributed Locking](08_locking.md) | Redis locks with heartbeat, Lua atomics, file locks, deadlock prevention |

## Read Path

| # | Document | Description |
|---|----------|-------------|
| 09 | [Query Engine](09_query_engine.md) | DuckDB Lite/Pro, Spark SQL, auto engine selection, view chain construction |
| 10 | [Data Reader](10_data_reader.md) | Read facade, SQL execution, time travel, view chain, compute service |

## Security & Compliance

| # | Document | Description |
|---|----------|-------------|
| 11 | [RBAC & Access Control](11_rbac.md) | Roles, users, row-level and column-level security, permissions, filter builder |
| 12 | [Audit Logging](12_audit.md) | Event logging, SHA-256 hash chain, DORA/SOC 2 compliance, SIEM integration |
| 13 | [Authentication & Tokens](13_security_services.md) | Auth modes, token lifecycle, superuser mechanism, session cookies |

## API & Connectivity

| # | Document | Description |
|---|----------|-------------|
| 14 | [REST API Reference](14_rest_api.md) | All 110+ endpoints grouped by domain — auth, request/response formats |
| 15 | [OData Protocol](15_odata.md) | OData 4.0 for Power BI, Excel, Tableau — token auth, query options, schema |
| 16 | [MCP Server](16_mcp.md) | AI assistant integration — 24 tools, stdio/HTTP transport, Claude Desktop setup |
| 17 | [Web UI](17_webui.md) | Browser management console — 19 pages, reverse proxy, session auth |

## Data Operations

| # | Document | Description |
|---|----------|-------------|
| 18 | [Data Sharing](18_sharing.md) | Cross-org sharing, linked shares, materialization, publications, presigned URLs |
| 19 | [Table Mirroring](19_mirroring.md) | Delta Lake, Apache Iceberg, Parquet export after writes |
| 20 | [Data Quality](20_data_quality.md) | 16 built-in checks, anomaly detection (A1-A5), scoring, scheduling |
| 21 | [Garbage Collection](21_garbage_collection.md) | Orphan Parquet cleanup, snapshot pruning, cron scheduling |

## Operations & Observability

| # | Document | Description |
|---|----------|-------------|
| 22 | [Monitoring & Metrics](22_monitoring.md) | Read/write/MCP metrics, structured logging, correlation IDs, service registry |
| 23 | [Rate Limiting](23_rate_limiting.md) | Redis-backed sliding window rate limiter, per-IP tracking, HTTP 429 |
| 24 | [Error Codes](24_error_codes.md) | Complete error catalog — 30+ codes with HTTP status and messages |

## SDK

| # | Document | Description |
|---|----------|-------------|
| 25 | [Python SDK](25_python_sdk.md) | pip install, optional cloud extras, core classes, usage examples |
