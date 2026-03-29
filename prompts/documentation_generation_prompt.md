# Data Island Core — Full Product Documentation Prompt

> **Usage:** Open a new Claude conversation with computer use enabled.
> Paste this entire prompt. Claude will clone the repo, analyze the code, and produce 16 markdown documentation files — one per module — delivered as a zip.

---

## PROMPT START

You are a Principal Technical Writer documenting a production data platform called **Data Island Core** (package: `supertable/`). Your job is to produce 16 complete, user-facing documentation files in markdown — one per module — by reading the actual source code.

### Repository

Clone and analyze this repository:

```
https://github.com/kladnasoft/supertable.git
```

The Python package root is `supertable/`. Work from the `master` branch.

### Existing reference material

The repo may contain architecture documents uploaded previously. These are internal design docs — useful for understanding intent, but the documentation you produce must be written for three audiences simultaneously:

- **Operators** — how to configure, deploy, monitor, and troubleshoot
- **Developers** — how to integrate, extend, and understand internals
- **Auditors / Compliance** — how the system satisfies regulatory requirements (where applicable)

### Quality standard — the "Audit Log" style

Every document you produce must follow this exact structure and voice. This is the reference template (derived from the audit log documentation already written for this project):

```
# Data Island Core — {Module Name}

## Overview
2-3 paragraphs. What this module does, why it exists, who cares about it.
No jargon in the first sentence. State the value proposition plainly.

## How it works
Explain the mechanics — data flow, lifecycle, key algorithms.
Use numbered steps for sequential processes.
Use diagrams described in text (not ASCII art) where helpful.
Keep it concrete: name the actual classes, files, and functions.

## {Primary usage section — varies by module}
For API modules: endpoint reference with curl examples.
For library modules: code examples showing import and usage.
For infrastructure modules: configuration and operational procedures.
Always show the exact function signatures, env vars, or endpoints.

## {Secondary sections — varies by module}
Architecture details, storage formats, schemas, protocols.
Each section should be self-contained — a reader skipping to it
should understand it without reading everything above.

## Configuration
Table of all environment variables with defaults, types, and descriptions.
Use this exact format:
| Environment variable | Default | Description |
|---|---|---|

## {Compliance / Security section — only where applicable}
Map features to DORA articles or SOC 2 criteria if relevant.

## Module structure
File listing with one-line descriptions. Example:
```
supertable/module/
  __init__.py          Public API and re-exports
  core.py              Main logic
  helpers.py           Internal utilities
```

## Performance characteristics (where measurable)
Table of metrics: latency, throughput, memory, storage.

## Frequently asked questions
5-10 Q&A pairs covering the most likely operator/developer questions.
Each answer should be 1-3 sentences, direct, no hedging.
```

### Voice and style rules

1. **No filler.** Every sentence must convey information. Cut "it is important to note that" — just state the fact.
2. **Name things.** Say `data_writer.py` not "the writer module." Say `SUPERTABLE_AUDIT_ENABLED` not "the audit toggle."
3. **Code over prose.** When explaining how to do something, show the code/command/curl first, then explain what it does — not the reverse.
4. **Present tense, active voice.** "The writer acquires a lock" not "A lock is acquired by the writer."
5. **No marketing language.** No "powerful," "seamless," "enterprise-grade," "cutting-edge." State capabilities factually.
6. **Concrete numbers.** "Retains events for 2,555 days (7 years)" not "retains events for an extended period."
7. **FAQ answers are direct.** Start with yes/no or the answer, then explain. Never start with "Great question!"

---

## The 16 Documents

Produce them in this exact order (layer by layer). For each document, read ALL source files listed before writing. Do not guess — read the code.

---

### Document 1: Platform Overview
**Filename:** `01_platform_overview.md`
**Source files to read:**
- `supertable/__init__.py`
- `supertable/api/application.py`
- `supertable/webui/application.py`
- `supertable/mcp/mcp_server.py` (just the docstring + config section)
- `supertable/server_common.py` (just the docstring + imports)
- `Dockerfile*` and `docker-compose*` if present
- `README.md` if present

**Must cover:**
- What Data Island Core is (one paragraph)
- Architecture: three servers (API :8051, WebUI :8050, MCP), shared Redis, shared storage
- How the servers relate (WebUI proxies to API, MCP is standalone)
- Directory structure overview (map every top-level package to its purpose)
- Deployment: Docker, bare metal, environment variables for startup
- Quick start: minimum viable setup (Redis + storage + one server)

---

### Document 2: Configuration & Settings
**Filename:** `02_configuration.md`
**Source files to read:**
- `supertable/config/settings.py` — the `Settings` dataclass and `_build_settings()`
- `supertable/config/defaults.py`
- `supertable/config/homedir.py`
- `supertable/config/__init__.py`

**Must cover:**
- Every field in the `Settings` dataclass with type, default, env var name, and description
- Group settings by section (same sections as in the dataclass: Core, Redis, API, Monitoring, Audit, etc.)
- How settings are loaded (env vars → `_build_settings()` → frozen dataclass)
- The `SUPERTABLE_HOME` directory structure
- Convenience properties (`effective_api_host`, `effective_redis_url`, etc.)
- How to override settings for dev vs production

---

### Document 3: Storage Backends
**Filename:** `03_storage.md`
**Source files to read:**
- `supertable/storage/storage_interface.py`
- `supertable/storage/storage_factory.py`
- `supertable/storage/s3_storage.py`
- `supertable/storage/minio_storage.py`
- `supertable/storage/azure_storage.py`
- `supertable/storage/gcp_storage.py`
- `supertable/storage/local_storage.py`
- `supertable/storage/__init__.py`

**Must cover:**
- The `StorageInterface` abstract class — every method with signature and semantics
- How `storage_factory.get_storage()` selects the backend (env var `SUPERTABLE_STORAGE_TYPE`)
- Per-backend configuration (env vars, credentials, bucket/container setup)
- File path conventions (org/super_name/simple_name/version/...)
- Which methods each backend implements vs raises NotImplementedError
- How to add a new storage backend

---

### Document 4: Redis Catalog
**Filename:** `04_redis_catalog.md`
**Source files to read:**
- `supertable/redis_catalog.py`
- `supertable/redis_connector.py`

**Must cover:**
- What the catalog stores (table metadata, snapshots, RBAC, tokens, locks, staging, pipes, monitoring, quality)
- Key naming conventions (document every `_*_key()` helper)
- Every public method on `RedisCatalog` grouped by domain (snapshot management, RBAC, tokens, locks, staging, config, monitoring)
- `RedisConnector` — connection pooling, SSL, health checks
- Redis data structures used (hashes, sorted sets, lists, streams)
- How to inspect catalog state manually (`redis-cli` examples)

---

### Document 5: Data Model
**Filename:** `05_data_model.md`
**Source files to read:**
- `supertable/super_table.py`
- `supertable/simple_table.py`
- `supertable/data_classes.py`
- `supertable/meta_reader.py`

**Must cover:**
- The three-level hierarchy: Organization → SuperTable → SimpleTable
- What a SuperTable is (virtual database), what a SimpleTable is (versioned table)
- Snapshot model: versions, files, columns, tombstones
- Data classes: `TableDefinition`, `SuperSnapshot`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef`
- MetaReader: how it aggregates metadata across tables, caching behavior
- File layout on storage for each level

---

### Document 6: Data Writer
**Filename:** `06_data_writer.md`
**Source files to read:**
- `supertable/data_writer.py`
- `supertable/processing.py`
- `supertable/locking/redis_lock.py`
- `supertable/locking/file_lock.py`

**Must cover:**
- The write pipeline step by step (access check → convert → validate → lock → snapshot → overlap → process → write → unlock → monitor → quality notify)
- Overlap handling: what happens when incoming data overlaps existing data
- Dedup-on-write vs dedup-on-read
- Overwrite columns: what they are, how they work
- `newer_than` filtering
- `delete_only` mode
- Tombstone management
- Locking: per-simple Redis lock with TTL, file lock fallback
- Monitoring integration (stats payload)
- Lineage tracking (the `lineage` parameter)

---

### Document 7: Data Reader & Query Engine
**Filename:** `07_query_engine.md`
**Source files to read:**
- `supertable/data_reader.py`
- `supertable/engine/engine_enum.py`
- `supertable/engine/engine_common.py`
- `supertable/engine/executor.py`
- `supertable/engine/duckdb_lite.py`
- `supertable/engine/duckdb_pro.py`
- `supertable/engine/spark_thrift.py`
- `supertable/engine/data_estimator.py`
- `supertable/engine/plan_stats.py`
- `supertable/plan_extender.py`
- `supertable/query_plan_manager.py`

**Must cover:**
- How a query flows: SQL → DataReader → engine selection → executor → results
- Engine selection logic (AUTO routing based on data size)
- DuckDB Lite vs DuckDB Pro vs Spark SQL: when each is used, tradeoffs
- RBAC integration in the read path (row/column filtering)
- Query plan management and optimization
- Data estimation (how the system predicts query cost)
- Plan statistics collection
- How to force a specific engine

---

### Document 8: Ingestion & Pipes
**Filename:** `08_ingestion.md`
**Source files to read:**
- `supertable/staging_area.py`
- `supertable/super_pipe.py`
- `supertable/services/ingestion.py`
- `supertable/api/api.py` — only the ingestion endpoint section (search for `INGESTION endpoints`)

**Must cover:**
- Staging areas: what they are, file format support (CSV, JSON, Parquet), schema detection
- The upload flow: browser → API → staging or direct-to-table
- Pipes: what they are, how they connect staging to tables
- Pipe configuration: role, target table, overwrite columns, enabled/disabled
- Pipe scheduling and execution
- API endpoints for staging CRUD, pipe CRUD, file upload
- Error handling and validation

---

### Document 9: RBAC & Access Control
**Filename:** `09_rbac.md`
**Source files to read:**
- `supertable/rbac/access_control.py`
- `supertable/rbac/role_manager.py`
- `supertable/rbac/user_manager.py`
- `supertable/rbac/permissions.py`
- `supertable/rbac/row_column_security.py`
- `supertable/rbac/filter_builder.py`
- `supertable/rbac/__init__.py`

**Must cover:**
- Role types and what each can do
- Row-level security: how filters are defined and applied
- Column-level security: how column restrictions work
- The permission evaluation flow (request → role lookup → filter build → query rewrite)
- User management: creation, deletion, role assignment
- Token authentication: how tokens are created, validated, revoked
- How RBAC integrates with DataReader and DataWriter
- API endpoints for RBAC management

---

### Document 10: Audit Log
**Filename:** `10_audit_log.md`
**Note:** This document already exists. Copy it from the file already produced (`audit_log_documentation.md`). Do not regenerate — use the existing content verbatim.

---

### Document 11: REST API
**Filename:** `11_rest_api.md`
**Source files to read:**
- `supertable/api/api.py` — every `@router.*` endpoint
- `supertable/api/application.py`
- `supertable/api/session.py`
- `supertable/server_common.py` — auth guards, session helpers

**Must cover:**
- Authentication: session cookies (WebUI) vs bearer tokens (API)
- Every endpoint grouped by domain (Admin, Execute, Tables, Ingestion, Monitoring, RBAC, Audit, OData)
- For each endpoint: method, path, parameters (query/body), response format, auth requirement
- Error response format
- Rate limiting (if any)
- Correlation ID propagation
- Session management: cookie format, HMAC signing, expiry

---

### Document 12: MCP Server
**Filename:** `12_mcp.md`
**Source files to read:**
- `supertable/mcp/mcp_server.py`
- `supertable/mcp/mcp_client.py`
- `supertable/mcp/mcp_stdio_proxy.py`
- `supertable/mcp/web_app.py`
- `supertable/mcp/web_client.py`
- `supertable/mcp/web_server.py`
- `supertable/mcp/__init__.py`

**Must cover:**
- What MCP is and how Data Island Core implements it
- Transport modes: stdio vs HTTP (streamable-HTTP + SSE)
- Available tools: list_supers, list_tables, describe_table, query_sql, sample_data, get_table_stats, get_super_meta, write_data
- Tool parameters, return formats, error handling
- Authentication: shared secret token, role-based access
- Configuration: env vars for transport, timeouts, limits, allowed roles
- Rate limiting and concurrency control (anyio CapacityLimiter)
- How to connect from Claude Desktop, ChatGPT, Cursor, etc.

---

### Document 13: Monitoring
**Filename:** `13_monitoring.md`
**Source files to read:**
- `supertable/monitoring_writer.py`
- `supertable/services/monitoring.py`
- `supertable/logging.py`

**Must cover:**
- What gets monitored: read metrics, write metrics, query timing
- MonitoringWriter: queue + background worker architecture (same pattern as audit)
- How metrics flow: endpoint → MonitoringWriter → Redis list → monitoring page
- NullMonitoringLogger: when monitoring is disabled
- Structured logging: JSON format, correlation IDs, request/response timing
- RequestLoggingMiddleware: what it captures
- The monitoring API endpoints
- How to query monitoring data (Redis keys, data format)
- Log file location and rotation

---

### Document 14: Data Quality
**Filename:** `14_data_quality.md`
**Source files to read:**
- `supertable/services/quality/checker.py`
- `supertable/services/quality/config.py`
- `supertable/services/quality/anomaly.py`
- `supertable/services/quality/history.py`
- `supertable/services/quality/scheduler.py`
- `supertable/services/quality/__init__.py`

**Must cover:**
- What data quality checks exist (list every built-in check from config.py)
- How checks are triggered (on-ingest via scheduler, manual via API)
- Anomaly detection: algorithms used, thresholds, sensitivity
- Quality scores: how they're computed, what they mean
- History tracking: how results are stored and queried over time
- Scheduler: debouncing, cooldown, Redis-based coordination
- API endpoints for quality data
- How to add custom checks

---

### Document 15: Mirroring
**Filename:** `15_mirroring.md`
**Source files to read:**
- `supertable/mirroring/mirror_formats.py`
- `supertable/mirroring/mirror_parquet.py`
- `supertable/mirroring/mirror_delta.py`
- `supertable/mirroring/mirror_iceberg.py`
- `supertable/mirroring/__init__.py`

**Must cover:**
- What mirroring is: automatic export of table data to standard formats after every write
- Supported formats: Parquet, Delta Lake, Apache Iceberg
- How mirroring is enabled/disabled per table
- Mirror format configuration
- File layout for each format on storage
- How mirroring integrates with the write pipeline (called from data_writer.py)
- Performance impact of mirroring

---

### Document 16: Locking
**Filename:** `16_locking.md`
**Source files to read:**
- `supertable/locking/__init__.py`
- `supertable/locking/redis_lock.py`
- `supertable/locking/file_lock.py`

**Must cover:**
- Why locking exists (concurrent write protection)
- Redis distributed locks: acquire, release, extend, TTL, timeout
- File-based locks: when they're used (fallback when Redis is unavailable)
- Lock granularity: per-SimpleTable
- How the DataWriter uses locks
- Deadlock prevention
- Lock monitoring and debugging

---

## Execution Instructions

### Process for each document

1. **Read first.** Use the `view` tool to read every source file listed for that document. Read the full file, not just the first 50 lines. If a file is longer than 500 lines, read it in chunks.
2. **Analyze.** Identify every public function, class, configuration option, and API endpoint.
3. **Write.** Produce the markdown following the structure template above.
4. **Verify.** Ensure every public function/class mentioned in the doc actually exists in the code. Do not invent functions.
5. **Save.** Write the file to `/home/claude/docs/{filename}`.

### For Document 10 (Audit Log)

Do NOT regenerate this document. Instead, check if `audit_log_documentation.md` exists in `/mnt/user-data/uploads/`. If yes, copy it as `10_audit_log.md`. If not, read the `supertable/audit/` module and write it fresh following the same style.

### Final deliverable

After all 16 documents are written:

1. Create a `00_index.md` that lists all 16 documents with one-sentence descriptions.
2. Zip everything into `/mnt/user-data/outputs/data_island_core_docs.zip`.
3. Present the zip for download.

### Critical rules

- **Read the code, don't guess.** If you can't find a function, it doesn't exist. Don't document imaginary features.
- **Name every env var exactly as it appears in settings.py.** Don't abbreviate or rename.
- **Include file:line references** when describing where something is implemented (e.g., "defined in `data_writer.py:142`").
- **Don't skip sections.** Every document must have Overview, How it works, Configuration (if applicable), Module structure, and FAQ.
- **One document at a time.** Complete and save each document before starting the next. This prevents context loss.
- **If the conversation runs long,** save progress and tell the user which documents are complete and which remain. The user can say "continue" to resume.

## PROMPT END
