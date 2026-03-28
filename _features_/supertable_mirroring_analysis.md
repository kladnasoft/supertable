# Feature / Component Reverse Engineering

## 1. Executive Summary

This code implements the **format mirroring subsystem** for **Supertable**, a data platform product. The subsystem takes an internal "simple snapshot" (Supertable's native representation of a table's current data files and schema) and projects it into one or more industry-standard open table formats: **Delta Lake**, **Apache Iceberg**, and **plain Parquet**.

- **Main responsibility**: After any successful Supertable snapshot update, mirror the current table state to Delta, Iceberg, and/or Parquet directory layouts so external engines (Spark, Trino, Databricks, Snowflake, AWS Athena, etc.) can query the same data without importing it.
- **Likely product capability**: Open-format interoperability layer — users configure which formats are enabled per SuperTable, and the platform automatically maintains spec-compliant projections of every table.
- **Business purpose**: Eliminates vendor lock-in, enables federated/external querying, and positions Supertable as a data lakehouse hub rather than a silo.
- **Technical role**: Post-write hook layer — sits between the core ingestion/compaction pipeline and the storage backend, writing format-specific metadata and co-locating data files.

---

## 2. Functional Overview

### Product Capability

This code enables the following user-facing and platform capabilities:

- **Multi-format table export**: An organization can enable Delta, Iceberg, and/or Parquet mirroring for any SuperTable. Once enabled, every data update automatically maintains a compliant external-format projection alongside the native Supertable catalog.
- **Zero-copy interop where possible**: The writers prefer server-side copy (e.g., MinIO `copy_object`) to avoid re-downloading/uploading data; the underlying Parquet data files are shared, only metadata differs.
- **External query engine compatibility**: The Delta mirror produces spec-compliant `_delta_log` NDJSON commits readable by Spark and Databricks. The Iceberg mirror produces v2 `metadata.json` files and Avro manifest/manifest-list files readable by Trino, Spark, Snowflake, and other Iceberg-capable engines. The Parquet mirror provides a flat directory of Parquet files suitable for any engine that supports directory-based table scans.
- **Admin-level format management**: Formats can be enabled, disabled, or bulk-set per SuperTable via Redis-backed configuration with atomic operations.

### Target Actors

| Actor | Interaction |
|---|---|
| Data engineer / admin | Enables/disables mirror formats per SuperTable |
| External query engine | Reads Delta `_delta_log`, Iceberg metadata, or Parquet directory |
| Supertable ingestion pipeline | Calls `mirror_if_enabled()` after each snapshot commit |
| Platform operator | Monitors mirror health via structured logging |

### Business Value

- **Interoperability / open standards adoption**: Customers can query Supertable-managed data from Spark, Databricks, Trino, Snowflake, Athena, or any tool supporting Delta/Iceberg/Parquet without ETL.
- **Vendor lock-in reduction**: Data is always available in open formats, lowering switching costs and increasing trust.
- **Strategic positioning**: Supertable becomes a lakehouse metadata layer rather than a proprietary store. This is a differentiating feature for enterprise sales.

---

## 3. Technical Overview

### Architectural Style

- **Strategy pattern with static dispatch**: `MirrorFormats.mirror_if_enabled()` acts as an orchestrator that checks a Redis-backed config and dispatches to per-format writer functions.
- **Storage-agnostic abstraction**: All writers interact through a `storage` object (duck-typed interface supporting `makedirs`, `write_bytes`, `write_json`, `copy`, `ls`/`listdir`/`list_files`, `exists`, `delete`, `read_bytes`, `read_parquet`). This decouples the mirroring logic from the storage backend (MinIO, S3, Azure ABFSS, local filesystem).
- **Latest-only projection (snapshot replacement)**: All three formats write a "replace" / "overwrite" representation of the current state, not an incremental changelog. Each mirror commit represents the full current table state.

### Major Control Flow

1. Caller invokes `MirrorFormats.mirror_if_enabled(super_table, table_name, simple_snapshot)`
2. Orchestrator reads enabled formats from Redis (or uses caller-supplied list)
3. Creates required directory scaffolding for each enabled format
4. Dispatches to `write_delta_table()`, `write_iceberg_table()`, and/or `write_parquet_table()`
5. Each writer:
   - Resolves prior state (existing co-located files)
   - Copies new data files into the format-specific directory (hash-prefixed to avoid collisions)
   - Deletes obsolete files
   - Writes format-specific metadata (Delta commit log, Iceberg metadata+manifests, or nothing for Parquet)
   - Skips writing if no changes detected (no-op optimization)

### Key Design Patterns

- **Hash-prefixed file co-location**: Data files are copied into format-specific directories with an `<md5_8char>_<original_name>` naming scheme to avoid basename collisions across source paths.
- **Defensive storage probing**: Writers use `getattr`/`hasattr` checks to adapt to different storage backends (MinIO client, generic `copy()`, fallback byte-level read/write).
- **Idempotent no-op detection**: Delta and Parquet writers compare previous vs. current file sets and skip writes when nothing changed.
- **Dual-mode Iceberg**: Iceberg has both a spec-compliant "standard" writer (v2 metadata + real Avro manifests) and a legacy "iceberg-lite" JSON-only fallback. The standard writer is preferred; lite is used only when standard fails.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `mirror_formats.py` | Orchestration, config management, dispatch | `FormatMirror` (enum), `MirrorFormats` (static class) | Central entry point for enabling/disabling/executing format mirroring |
| `mirror_delta.py` | Delta Lake spec-compliant commit writer | `write_delta_table()`, schema normalization helpers, stats normalization, co-location/copy utilities | Produces `_delta_log` NDJSON commits with add/remove/metaData/protocol/commitInfo actions |
| `mirror_iceberg.py` | Apache Iceberg v2 metadata + Avro manifest writer | `write_iceberg_table()` (dual-mode), `_write_iceberg_standard()`, custom Avro binary encoder, schema builder | Produces standard Iceberg v2 metadata + Avro manifests; falls back to JSON-only "iceberg-lite" |
| `mirror_parquet.py` | Plain Parquet directory mirror | `write_parquet_table()`, co-location utilities | Copies data files into a flat directory; removes obsolete files; no metadata layer |

---

## 5. Detailed Functional Capabilities

### 5.1 Format Configuration Management

- **Description**: CRUD operations for the set of enabled mirror formats per SuperTable, stored in Redis.
- **Business purpose**: Allows admins/APIs to control which external formats are maintained, enabling cost/performance trade-offs.
- **Trigger/input**: API calls passing `super_table` object plus format names.
- **Processing**: Normalizes format strings to uppercase enum values, deduplicates, calls `RedisCatalog` methods (`get_mirrors`, `set_mirrors`, `enable_mirror`, `disable_mirror`).
- **Output**: Ordered list of currently-enabled format strings.
- **Dependencies**: `RedisCatalog` (internal), Redis (external).
- **Constraints**: Format names must be one of `DELTA`, `ICEBERG`, `PARQUET`.
- **Confidence**: **Explicit** — all config methods clearly implemented.

### 5.2 Delta Lake Mirroring

- **Description**: Produces a spec-compliant Delta Lake `_delta_log` with NDJSON commit files containing `commitInfo`, `protocol`, `metaData`, `remove`, and `add` actions. Data files are co-located under a `files/` subdirectory.
- **Business purpose**: Enables Spark, Databricks, and Delta-compatible engines to query Supertable data natively.
- **Trigger/input**: `write_delta_table(super_table, table_name, simple_snapshot)` called from orchestrator.
- **Processing**:
  1. Reads `snapshot_version` → Delta version number.
  2. Resolves schema from `schemaString`, `schema_string`, `schema` list, or infers from Parquet file headers.
  3. Lists existing co-located files; copies new files using `_binary_copy_if_possible`.
  4. Computes diff (adds vs. removes). Skips commit if no changes.
  5. Generates NDJSON commit with proper Delta actions (commitInfo, protocol v1/v4, metaData with Spark StructType schema, remove actions, add actions with optional file-level stats).
  6. Writes commit atomically; skips if version file already exists (idempotency guard).
  7. Optionally writes checkpoint (currently disabled via `WRITE_CHECKPOINT = False`).
- **Output**: `_delta_log/<version>.json` commit file; co-located Parquet files under `files/`.
- **Dependencies**: `pyarrow` (optional, for schema inference); storage backend.
- **Constraints**:
  - Engine string is hardcoded: `Apache-Spark/3.4.3.5.3.20250511.1 Delta-Lake/2.4.0.24`.
  - Protocol: `minReaderVersion=1`, `minWriterVersion=4`.
  - `latest.json` is intentionally NOT written (removed per design note).
  - Change Data Feed and auto-optimize flags are enabled in configuration metadata.
- **Risks**: If `snapshot_version` is not monotonically increasing, Delta readers may see inconsistent state. Schema inference fallback relies on ability to read Parquet headers.
- **Confidence**: **Explicit** — fully implemented with detailed inline comments.

### 5.3 Iceberg Mirroring (Standard v2)

- **Description**: Produces spec-compliant Apache Iceberg v2 table metadata including `vN.metadata.json`, Avro-encoded manifest files, and Avro-encoded manifest lists. Uses custom pure-Python Avro binary encoder (no dependency on `fastavro`).
- **Business purpose**: Enables Trino, Spark, Snowflake, Athena, and other Iceberg-compatible engines to query Supertable data.
- **Trigger/input**: `write_iceberg_table(super_table, table_name, simple_snapshot)`.
- **Processing**:
  1. Attempts `_write_iceberg_standard()` first.
  2. Reads `version-hint.text` to discover prior metadata version and schema field IDs (for stable column ID assignment).
  3. Copies data files into `data/` directory with hash-prefixed names.
  4. Builds manifest entry records matching Iceberg manifest file Avro schema (field IDs 100–145).
  5. Serializes manifest entries to Avro OCF using custom encoder.
  6. Builds manifest list records matching Iceberg manifest list Avro schema (field IDs 500–520).
  7. Serializes manifest list to Avro OCF.
  8. Writes `vN.metadata.json` with full v2 structure (schemas, partition-specs, sort-orders, snapshots, refs/main branch).
  9. Updates `version-hint.text` and `latest.json`.
  10. On failure, falls back to `_write_iceberg_table_iceberg_lite()` (JSON-only).
- **Output**: `metadata/vN.metadata.json`, `metadata/<uuid>.avro` (manifest), `metadata/snap-<id>-<ver>-<uuid>.avro` (manifest list), `data/<hash>_<file>`, `version-hint.text`, `latest.json`.
- **Dependencies**: No external library requirements (custom Avro encoder); storage backend.
- **Constraints**:
  - Unpartitioned tables only (`partition-spec: []`).
  - No sort orders.
  - Single-snapshot metadata (no history/time-travel; current-only).
  - Column-level stats (bounds, null counts) are left as null.
- **Risks**: Custom Avro encoder correctness is critical — any encoding bug would make manifests unreadable. The encoder handles unions, records, arrays, maps, fixed, and all primitives but has no test suite visible in the provided code.
- **Confidence**: **Explicit** — substantial implementation present with clear Iceberg spec alignment.

### 5.4 Iceberg-Lite (Legacy Fallback)

- **Description**: Writes a simplified JSON-only representation of Iceberg metadata. Not spec-compliant (manifests are JSON, not Avro), but provides basic discoverability.
- **Business purpose**: Backward compatibility; used only when the standard Iceberg writer fails.
- **Processing**: Writes `metadata/<version>.json` with a v2-like structure and `manifests/<version>.json` with a data-file listing. Writes `latest.json` pointer.
- **Constraints**: Not readable by standard Iceberg engines (JSON manifests instead of Avro).
- **Confidence**: **Explicit**.

### 5.5 Parquet Directory Mirroring

- **Description**: Copies current snapshot Parquet files into a flat `parquet/<table>/files/` directory and removes obsolete files. No metadata/transaction log is written.
- **Business purpose**: Simplest interop format — any engine that can read a directory of Parquet files can access the data.
- **Processing**:
  1. Lists existing co-located files.
  2. Copies new files with hash-prefixed names (skips if destination already exists — perf optimization).
  3. Deletes files that are no longer in the current snapshot.
  4. No-op if no add/remove changes.
- **Output**: Parquet files under `<org>/<super>/parquet/<table>/files/`.
- **Dependencies**: Storage backend.
- **Confidence**: **Explicit**.

### 5.6 Storage-Agnostic Binary Copy

- **Description**: All three writers share a `_binary_copy_if_possible()` function that tries three copy strategies in priority order: MinIO server-side copy (`copy_object`), generic `storage.copy()`, and byte-level read/write fallback.
- **Business purpose**: Maximizes performance across storage backends; server-side copy avoids data transfer through the application.
- **Processing**: Duck-typed detection of storage capabilities via `getattr`/`hasattr`.
- **Constraints**: MinIO fast path requires `minio.commonconfig.CopySource` import.
- **Confidence**: **Explicit**.

---

## 6. Classes, Functions, and Methods

### 6.1 `mirror_formats.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `FormatMirror` | Enum | Defines valid mirror formats (`DELTA`, `ICEBERG`, `PARQUET`) | Supporting |
| `FormatMirror.normalize()` | Static method | Validates and deduplicates format strings | Supporting |
| `MirrorFormats` | Static class | Orchestrates config reads/writes and dispatches mirroring | **Critical** |
| `MirrorFormats.get_enabled()` | Static method | Reads enabled formats from Redis | Important |
| `MirrorFormats.set_with_lock()` | Static method | Atomically sets enabled formats in Redis | Important |
| `MirrorFormats.enable_with_lock()` | Static method | Enables a single format | Important |
| `MirrorFormats.disable_with_lock()` | Static method | Disables a single format | Important |
| `MirrorFormats.mirror_if_enabled()` | Static method | Main entry point: checks config, creates dirs, dispatches to writers | **Critical** |

### 6.2 `mirror_delta.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `write_delta_table()` | Function | Produces a Delta commit for a snapshot | **Critical** |
| `_schema_to_structtype_json()` | Function | Converts various schema representations to Spark StructType JSON | **Critical** |
| `_normalize_type()` | Function | Maps Arrow/Polars type strings to Spark SQL types | Important |
| `_spark_type_from_pyarrow()` | Function | Maps PyArrow DataType objects to Spark type strings | Important |
| `_infer_schema_list_from_any_parquet()` | Function | Best-effort schema inference by reading Parquet file headers | Important |
| `_normalize_delta_stats()` | Function | Normalizes legacy and Delta-format file stats to Delta `add.stats` JSON | Important |
| `_binary_copy_if_possible()` | Function | Multi-strategy file copy (MinIO → generic → byte fallback) | Important |
| `_co_locate_or_reuse_path()` | Function | Copies a source Parquet file into the table dir with hash-prefixed name | Important |
| `_list_co_located_paths()` | Function | Lists existing files in the table's co-location directory | Supporting |
| `_stable_table_id()` | Function | Generates deterministic UUIDv5 for Delta metaData.id | Supporting |
| `_write_checkpoint_if_possible()` | Function | Checkpoint writer (disabled by default) | Supporting |
| `_pad_version()` | Function | Zero-pads version to 20-digit string for Delta log filenames | Supporting |

### 6.3 `mirror_iceberg.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `write_iceberg_table()` | Function (patched entrypoint) | Dual-mode dispatcher: tries standard, falls back to lite | **Critical** |
| `_write_iceberg_standard()` | Function | Spec-compliant Iceberg v2 writer with Avro manifests | **Critical** |
| `_write_iceberg_table_iceberg_lite()` | Function (original) | Legacy JSON-only Iceberg mirror | Important |
| `_iceberg_schema_from_snapshot()` | Function | Builds Iceberg schema with stable field IDs from snapshot schema | **Critical** |
| `_load_prior_schema_ids()` | Function | Reads prior metadata for field ID stability and table UUID continuity | Important |
| `_avro_ocf_dump()` | Function | Serializes records to Avro OCF binary format | **Critical** |
| `_encode_datum()` | Function | Recursive Avro datum encoder (all types) | **Critical** |
| `_manifest_file_avro_schema()` | Function | Returns Iceberg manifest file Avro schema (data file entries) | Important |
| `_manifest_list_avro_schema()` | Function | Returns Iceberg manifest list Avro schema | Important |
| `_storage_path_to_uri()` | Function | Converts object paths to S3 URIs for Iceberg file_path fields | Important |
| `_stable_table_uuid()` | Function | Deterministic UUIDv5 for Iceberg table-uuid | Supporting |
| `_new_snapshot_id()` | Function | Generates unique snapshot ID by XOR-ing UUID4 halves | Supporting |
| Avro primitive encoders | Functions | `_encode_long`, `_encode_string`, `_encode_bool`, etc. | Supporting |

### 6.4 `mirror_parquet.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `write_parquet_table()` | Function | Copies/removes Parquet files in a flat directory mirror | **Critical** |
| `_binary_copy_if_possible()` | Function | Multi-strategy file copy (identical pattern to Delta) | Important |
| `_co_locate_or_reuse_path()` | Function | Copy with existence check for skip optimization | Important |
| `_list_co_located_paths()` | Function | Lists existing files in mirror directory | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Mirror-on-Snapshot-Update

1. **Trigger**: Supertable ingestion pipeline completes a snapshot update for a table. Caller invokes `MirrorFormats.mirror_if_enabled(super_table, table_name, simple_snapshot)`.
2. **Read config**: If `mirrors` parameter not supplied, reads enabled formats from Redis via `RedisCatalog.get_mirrors()`.
3. **Early exit**: If no formats enabled, return immediately.
4. **Create directory scaffolding**: For each enabled format, creates the required directory structure via `storage.makedirs()`.
5. **Delta dispatch** (if enabled):
   a. Determine version from `simple_snapshot["snapshot_version"]`.
   b. Resolve schema (explicit → inferred from Parquet headers).
   c. List previously co-located files.
   d. Copy new data files with hash-prefixed names.
   e. Compute diff (adds/removes). If empty → no-op.
   f. Build NDJSON commit (commitInfo, protocol, metaData, removes, adds).
   g. Write commit file atomically. Guard against duplicate version.
   h. Attempt checkpoint (currently disabled).
   i. Delete obsolete co-located files.
6. **Iceberg dispatch** (if enabled):
   a. Try standard writer: load prior metadata → build schema with stable IDs → copy data files → build Avro manifest → build Avro manifest list → write metadata JSON → update version-hint.
   b. On failure: fall back to iceberg-lite JSON writer.
7. **Parquet dispatch** (if enabled):
   a. List existing files in mirror directory.
   b. Copy new files (skip if already exists).
   c. Delete obsolete files.
   d. No-op if no changes.
8. **Completion**: All enabled formats are now up-to-date with the latest snapshot.

**Failure modes**:
- File copy failure → `RuntimeError` raised (Delta, Parquet) or warning logged with fallback to original path (Iceberg).
- Iceberg standard writer failure → automatic fallback to lite.
- Individual file deletion failure → warning logged, non-fatal.
- Redis unavailable → `get_enabled()` likely raises or returns empty (not explicit; depends on `RedisCatalog`).

### 7.2 Enable/Disable Format

1. **Trigger**: Admin API call.
2. `MirrorFormats.enable_with_lock(super_table, "DELTA")` or `disable_with_lock()`.
3. Delegates to `RedisCatalog.enable_mirror()` / `disable_mirror()`.
4. Logs resulting format list.
5. **No retroactive mirroring**: Enabling a format only affects future snapshot updates; existing snapshots are not retroactively mirrored (not visible in code, but implied by the "mirror after snapshot" design).

---

## 8. Data Model and Information Flow

### Core Input: `simple_snapshot`

A dictionary passed from the Supertable core pipeline. Expected keys:

| Key | Type | Description |
|---|---|---|
| `resources` | `List[Dict]` | List of data file descriptors, each containing `file` (path/URI), `file_size`, `rows`/`numRecords`, optionally `stats_json`/`stats` |
| `snapshot_version` | `int` | Monotonically increasing version number |
| `schema` | `List[Dict]` | List of `{"name": str, "type": str, "nullable": bool}` |
| `schemaString` / `schema_string` | `str` (JSON) | Spark StructType JSON (preferred for Delta) |
| `delta_meta_id` / `metadata_id` | `str` (optional) | Override for Delta metaData.id |
| `createdTime` | `int` (optional) | Override for Delta metaData.createdTime |

### Output Layouts

**Delta**: `<org>/<super>/delta/<table>/_delta_log/<version>.json` + `files/<hash>_<name>.parquet`

**Iceberg (standard)**: `<org>/<super>/iceberg/<table>/metadata/vN.metadata.json` + `metadata/<uuid>.avro` (manifest) + `metadata/snap-<id>-<ver>-<uuid>.avro` (manifest list) + `data/<hash>_<name>.parquet` + `metadata/version-hint.text` + `latest.json`

**Iceberg (lite)**: `<org>/<super>/iceberg/<table>/metadata/<version>.json` + `manifests/<version>.json` + `latest.json`

**Parquet**: `<org>/<super>/parquet/<table>/files/<hash>_<name>.parquet`

### Configuration State

Stored in Redis at key `supertable:{org}:{super}:meta:mirrors` as `{"formats": ["DELTA", ...], "ts": <epoch_ms>}` (inferred from `RedisCatalog` method names and MirrorFormats doc).

---

## 9. Dependencies and Integrations

| Dependency | Type | Purpose |
|---|---|---|
| `supertable.redis_catalog.RedisCatalog` | Internal module | Reads/writes mirror format configuration in Redis |
| `supertable.config.defaults.logger` | Internal module | Structured logging |
| `redis` (via RedisCatalog) | External service | Mirror config persistence |
| `pyarrow` / `pyarrow.parquet` | Optional third-party | Schema inference from Parquet file headers (Delta writer) |
| `minio.commonconfig.CopySource` | Optional third-party | Server-side MinIO copy (performance optimization) |
| `storage` object (duck-typed) | Internal interface | Storage backend abstraction (S3/MinIO/Azure/local) |
| `json`, `os`, `io`, `hashlib`, `uuid`, `datetime`, `struct`, `enum` | Standard library | Core utilities |

### Storage Interface (inferred duck-typed contract)

The `super_table.storage` object must support a subset of:
`makedirs(path)`, `write_bytes(path, data)`, `write_json(path, obj)`, `read_bytes(path)`, `read_json(path)`, `read_parquet(path)`, `copy(src, dst)`, `delete(path)`, `exists(path)`, `ls(path)`, `listdir(path)`, `list_files(path, pattern)`.

Optionally exposes `.client` (MinIO client) and `.bucket`/`.bucket_name` for server-side copy.

---

## 10. Architecture Positioning

```
┌─────────────────────────┐
│  Supertable Ingestion   │
│  (snapshot commit)      │
└──────────┬──────────────┘
           │ calls mirror_if_enabled()
           ▼
┌─────────────────────────┐     ┌──────────────┐
│  MirrorFormats          │────▶│ RedisCatalog  │
│  (orchestrator)         │     │ (config)      │
└──┬───────┬──────────┬───┘     └──────────────┘
   │       │          │
   ▼       ▼          ▼
┌──────┐ ┌───────┐ ┌────────┐
│Delta │ │Iceberg│ │Parquet │
│Writer│ │Writer │ │Writer  │
└──┬───┘ └──┬────┘ └──┬─────┘
   │        │         │
   ▼        ▼         ▼
┌─────────────────────────┐
│  Storage Backend        │
│  (S3/MinIO/Azure/local) │
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│  External Query Engines │
│  (Spark, Trino, etc.)   │
└─────────────────────────┘
```

- **Upstream**: Supertable ingestion/compaction pipeline (provides `simple_snapshot`)
- **Downstream**: Object storage (writes); external query engines (reads)
- **Layer**: Adapter / projection layer — translates internal representation to external standards
- **Boundaries**: Clean separation — each writer is a standalone module; orchestrator handles config only
- **Coupling**: Loosely coupled to storage via duck-typed interface; tightly coupled to `simple_snapshot` schema
- **Scalability**: Per-table, per-format execution; no cross-table coordination required; mirrors run in the caller's process/thread

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Customers need to query Supertable-managed data from external engines without ETL or data duplication |
| **Revenue relevance** | Likely a premium/enterprise feature; enables interoperability that unblocks adoption by teams using Spark, Databricks, Snowflake, Trino |
| **Strategic significance** | **High** — positions Supertable as a lakehouse hub; aligns with industry trends toward open table formats |
| **Differentiation** | Moderate — the concept exists in competitors (e.g., Databricks UniForm), but auto-mirroring to three formats from a single source is a strong selling point |
| **Value if removed** | Users would be locked into Supertable-only access patterns; external tools could not read the data natively |
| **Compliance relevance** | Supports data portability requirements (GDPR right to data portability, enterprise data governance policies) |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | High — Delta commit log generation, custom Avro binary encoder, multi-format schema normalization |
| **Architectural maturity** | Good — clean separation of concerns, strategy dispatch, storage abstraction |
| **Maintainability** | Moderate — shared patterns are duplicated across files (e.g., `_binary_copy_if_possible`, `_list_co_located_paths`); could be consolidated. Inline comments are thorough. |
| **Extensibility** | Good — adding a new format requires a new writer function and a new enum value; the orchestrator's if-chain is simple to extend |
| **Operational sensitivity** | High — incorrect Delta/Iceberg metadata generation could cause external engines to crash or return wrong results |
| **Performance** | Server-side copy preferred; existence checks to skip redundant copies; no-op detection to skip redundant commits |
| **Reliability** | Multiple fallback paths (copy strategies, Iceberg standard→lite); idempotency guards (Delta version file existence check, Parquet `exists` check) |
| **Security** | No explicit auth/encryption handling visible — relies on storage backend's security model |
| **Testing** | No test code visible; the custom Avro encoder is a significant correctness risk without thorough test coverage |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Mirror formats are Delta, Iceberg, and Parquet.
- Configuration is stored in Redis via `RedisCatalog`.
- Delta writer produces spec-compliant NDJSON commit logs with protocol v1/v4.
- Iceberg standard writer produces v2 metadata JSON and Avro-encoded manifest/manifest-list files using a custom pure-Python encoder.
- Iceberg falls back to JSON-only "lite" mode on standard writer failure.
- Parquet mirror is a flat copy of data files with no metadata.
- All writers use hash-prefixed file co-location to avoid name collisions.
- `_binary_copy_if_possible` supports MinIO server-side copy, generic `storage.copy()`, and byte-level fallback.
- Delta checkpointing is implemented but disabled (`WRITE_CHECKPOINT = False`).
- Delta engine string is hardcoded to a specific Spark/Delta version.
- Schema inference from Parquet headers is performed when snapshot lacks schema (Delta only).
- Iceberg writer preserves field IDs across versions by reading prior metadata.

### Reasonable Inferences

- `super_table` is a domain object representing a Supertable instance, with `.organization`, `.super_name`, and `.storage` attributes.
- `mirror_if_enabled()` is called synchronously within the same transaction/lock context as the snapshot commit (docstring says "Caller SHOULD still hold the per-simple lock").
- The Redis key for mirror config follows the pattern `supertable:{org}:{super}:meta:mirrors` (from class docstring).
- The Delta engine string impersonates Spark/Delta for compatibility with readers that validate engine info.
- The system is designed for object storage (S3, MinIO, Azure Blob) based on path patterns and `abfss://` handling.

### Unknown / Not Visible in Provided Snippet

- How `super_table` is constructed and what the full `StorageInterface` contract looks like.
- How `RedisCatalog`'s `get_mirrors`, `set_mirrors`, `enable_mirror`, `disable_mirror` are implemented.
- Whether mirroring is triggered synchronously or enqueued as a background job.
- Whether there is any retry/recovery mechanism for failed mirrors.
- How external engines discover the mirrored tables (catalog registration, HMS, AWS Glue, Unity Catalog, etc.).
- Whether there is a cleanup mechanism for orphaned data files across formats.
- Test coverage for the custom Avro encoder.
- Whether the "latest-only" semantics mean time-travel is intentionally not supported, or if it is planned.
- How `snapshot_version` monotonicity is guaranteed.

---

## 14. AI Consumption Notes

- **Canonical feature name**: `Supertable Format Mirroring`
- **Alternative names**: mirror, format mirror, Delta mirror, Iceberg mirror, Parquet mirror, open format projection
- **Main responsibilities**: Convert Supertable simple snapshots to Delta Lake, Apache Iceberg v2, and Parquet directory layouts
- **Important entities**: `MirrorFormats`, `FormatMirror`, `simple_snapshot`, `super_table`, `RedisCatalog`, `storage` interface
- **Important workflows**: mirror-on-snapshot-update, enable/disable format, Delta commit generation, Iceberg metadata+Avro manifest generation
- **Integration points**: Redis (config), object storage (S3/MinIO/Azure), external query engines (consumers), Supertable ingestion pipeline (producer)
- **Business purpose keywords**: interoperability, open table format, vendor lock-in, lakehouse, Delta Lake, Apache Iceberg, Parquet, external query, federated access
- **Architecture keywords**: adapter layer, projection, strategy dispatch, storage-agnostic, duck-typed interface, Avro binary encoding
- **Follow-up files for completeness**:
  - `supertable.redis_catalog.RedisCatalog` — understand config storage contract
  - `supertable.storage` / `StorageInterface` — understand full storage abstraction
  - `super_table` domain object definition — understand `.organization`, `.super_name` patterns
  - Caller of `mirror_if_enabled()` — understand trigger context and locking semantics
  - Any API layer exposing enable/disable endpoints

---

## 15. Suggested Documentation Tags

- `business-domain:data-platform`
- `business-domain:lakehouse`
- `feature-type:interoperability`
- `feature-type:format-conversion`
- `feature-type:data-mirroring`
- `technical-layer:adapter`
- `technical-layer:storage`
- `architecture-style:strategy-pattern`
- `architecture-style:duck-typed-abstraction`
- `dependency:redis`
- `dependency:pyarrow` (optional)
- `dependency:minio` (optional)
- `format:delta-lake`
- `format:apache-iceberg`
- `format:parquet`
- `operational:latency-sensitive`
- `operational:data-integrity-critical`
- `spec-compliance:delta-lake-protocol-v1-v4`
- `spec-compliance:iceberg-v2`
- `encoding:avro-binary` (custom implementation)

---

## Merge Readiness

- **Suggested canonical name**: `supertable.mirroring` — Format Mirroring Subsystem
- **Standalone or partial**: **Substantially standalone** — this is a complete, self-contained subsystem. However, it depends on external contracts (`RedisCatalog`, `StorageInterface`, `super_table` domain object) that would need separate analysis for full documentation.
- **Related components to merge later**:
  - `supertable.redis_catalog` (config storage)
  - `supertable.storage` / `StorageInterface` (storage abstraction)
  - Supertable ingestion/snapshot pipeline (upstream caller)
  - API layer for mirror format management (if exists)
  - External catalog registration (HMS, Glue, Unity Catalog — if exists)
- **What additional files would most improve certainty**:
  - `RedisCatalog` implementation (config key structure, atomicity guarantees)
  - Storage interface definition (full method contract)
  - The caller of `mirror_if_enabled()` (trigger context, locking, error handling)
- **Recommended merge key phrases**: `format mirroring`, `mirror_formats`, `mirror_delta`, `mirror_iceberg`, `mirror_parquet`, `open table format`, `Delta Lake mirror`, `Iceberg mirror`, `simple_snapshot`
