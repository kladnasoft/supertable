# SuperTable documentation

These pages describe the active Python source tree at revision `5fe87d0`, reviewed on 2026-09-13. The content was rebuilt from executable code and configuration declarations, using the existing documentation filenames as the chapter structure. Existing documentation prose, code comments, and docstrings were excluded from the reconstruction.

The project metadata and package version identify this tree as `3.3.0`; the separate version literal in `setup.py` is listed among the [implementation gaps](TODO.md).

## Start here

1. Read the [platform overview](01_platform_overview.md) for the components and their boundaries.
2. Set up [configuration](02_configuration.md) and a [storage backend](04_storage.md).
3. Follow the [Python SDK](15_python_sdk.md) example to create, write, and query a table.
4. Use the [data model](03_data_model.md), [writer](06_data_writer.md), and [reader](10_data_reader.md) chapters to understand state changes and result semantics.
5. Review the [implementation gaps](TODO.md) relevant to an integration before relying on that interface.

## Chapters

| Chapter | Contents |
| --- | --- |
| [01 — Platform overview](01_platform_overview.md) | Components, data flow, integration and deployment boundaries. |
| [02 — Configuration](02_configuration.md) | Environment loading, defaults, runtime settings and consumer-specific behavior. |
| [03 — Data model](03_data_model.md) | Namespaces, snapshots, resources, row identity, schema and statistics. |
| [04 — Storage](04_storage.md) | Storage interface, backend selection, paths and operation semantics. |
| [05 — Redis catalog](05_redis_catalog.md) | Catalog operations, publication, history, metadata and service configuration. |
| [06 — Data writer](06_data_writer.md) | Append, replacement, deletion, compaction, results and commit sequence. |
| [07 — Ingestion](07_ingestion.md) | Stage files, pipe definitions, external consumption, and result jobs. |
| [08 — Locking](08_locking.md) | Redis leases, heartbeat, ownership checks and file locks. |
| [09 — Query engine](09_query_engine.md) | SQL admission, estimation, pruning, routing and engine execution. |
| [10 — Data reader](10_data_reader.md) | Read/stream APIs, return values, metadata and OData helpers. |
| [11 — RBAC](11_rbac.md) | Roles, users, permissions, row/column restrictions and authority boundaries. |
| [12 — Audit](12_audit.md) | Event model, initialization, delivery, storage, readers and verification limits. |
| [13 — Mirroring](13_mirroring.md) | Format selection, copied resources, Delta/Iceberg metadata and limitations. |
| [14 — Monitoring](14_monitoring.md) | Metrics, queues, partitions and quality scheduling. |
| [15 — Python SDK](15_python_sdk.md) | Installation, complete write/read example and API usage. |
| [16 — Redis layout](16_redis_layout.md) | Key namespaces, data types and key builders. |
| [TODO — Implementation gaps](TODO.md) | Code-supported limitations and concrete areas requiring follow-up. |

## Reading conventions

The SDK walkthrough uses `acme` as the organization, `warehouse` as the SuperTable, and `orders` as a SimpleTable. Other chapters state their example-specific prerequisites. `role_name` means the role resolved by the SDK's access checks. Paths shown for table artifacts are logical storage paths; cloud prefixes and the local application home are covered in the storage chapter.

Defaults describe declarations and their actual consumers. A declared option does not necessarily affect every path, and the chapters call out unused or overridden settings. Method names are not treated as guarantees: for example, catalog methods named `*_cas` do not accept an expected version.

Source links point to implementation files so behavior can be checked when code changes. SDK examples explicitly select DuckDB because of the currently documented Spark result-delivery limitation. External services are prerequisites where stated; listing an integration or helper does not imply that this library starts an HTTP server or a worker for it.
