# Feature / Component Reverse Engineering: Supertable Staging Area & Pipe Management

## 1. Executive Summary

This code implements the **data staging and pipe management** subsystem for the Supertable platform — a two-layer ingestion architecture where data is first landed into **staging areas** (temporary Parquet holding zones) and then routed into the core dataset via **pipes** (declarative ingestion rule definitions).

- **Main responsibility:** Provide a managed, RBAC-protected, concurrency-safe mechanism for writing intermediate data into staging areas and defining pipes that describe how that staged data should be merged into the main Supertable.
- **Likely product capability:** A structured data ingestion pipeline where external data producers land files into staging, and pipes define the upsert/overwrite semantics for promoting staged data into the production dataset.
- **Business purpose:** Controlled, auditable, multi-step data ingestion for a multi-tenant analytical data platform — separating the "data landing" concern from the "data merging" concern.
- **Technical role:** Domain logic layer sitting between the data-producer interface (API/SDK) and the core storage engine. Uses Redis for metadata/coordination and object storage for Parquet files.

---

## 2. Functional Overview

### What can the product do because of this code?

1. **Create named staging areas** within a Supertable, each with its own directory and file index.
2. **Write Parquet files to a staging area** with RBAC enforcement, distributed locking, and file index tracking.
3. **List, inspect, and delete staging areas** and their contents.
4. **Define pipes** — declarative ingestion rules that associate a staging area with a target table name and overwrite column semantics.
5. **Enable/disable pipes** at runtime without deleting the definition.
6. **Prevent duplicate pipe configurations** (same `simple_name` + `overwrite_columns` combination).
7. **Inspect the directory structure** of all staging areas for a given Supertable (backward-compatible admin view).

### Target users / actors

| Actor | Interaction |
|---|---|
| **Data producer (API/SDK user)** | Writes Parquet files into staging areas via `Staging.save_as_parquet()` |
| **Data engineer / admin** | Creates pipes via `SuperPipe.create()` to define how staged data merges into the main dataset |
| **Platform operator** | Inspects staging structure via `Staging.get_directory_structure()`, enables/disables pipes |
| **Merge/ingest worker (inferred)** | Reads pipe definitions and staging files to execute the actual data merge (not present in this code) |

### Business workflows

1. **Data landing:** Producer authenticates → creates/opens a staging area → writes one or more Parquet files → files are indexed in a JSON manifest.
2. **Pipe definition:** Engineer creates a pipe that binds a staging area to a target table with overwrite semantics (which columns determine upsert identity).
3. **Data promotion (inferred):** A downstream worker reads enabled pipes, discovers staged files, and merges them into the main Supertable using the overwrite column rules.
4. **Cleanup:** Staging areas can be deleted (files + index + Redis metadata) after data is promoted.

### Strategic significance

This subsystem implements a **controlled ingestion pattern** — a critical capability for any enterprise data platform. It provides:
- **Data isolation:** Staged data is quarantined until explicitly promoted, preventing dirty writes to production datasets.
- **Upsert semantics:** Pipes define which columns constitute the row identity for overwrite operations, enabling idempotent data loads.
- **Concurrency safety:** Redis-based distributed locking prevents race conditions during concurrent writes to the same staging area.
- **Auditability:** File indices track what was written and when (`written_at_ns`, row counts).

---

## 3. Technical Overview

### Architectural style

- **Dual-mode class pattern** (`Staging`): A single class operates in either "manager mode" (listing/inspecting) or "stage mode" (read/write operations on a specific stage).
- **Redis-backed metadata:** All staging and pipe metadata lives in Redis via `RedisCatalog`. Object storage holds only the Parquet files and JSON file indices.
- **Distributed locking:** Redis `SET NX EX` with Lua-script atomic release (compare-and-delete pattern) — a standard Redis distributed lock implementation.
- **RBAC enforcement:** Write operations require `check_write_access()`, read/inspect operations require `check_meta_access()`.
- **Immutable append model:** Parquet files are written with nanosecond-timestamp suffixes, never overwritten. The file index is a JSON append log.

### Major components

| Component | Role |
|---|---|
| `Staging` | Dual-mode class for staging area management (manager mode) and staged file operations (stage mode) |
| `SuperPipe` | Redis-only pipe definition manager — CRUD for ingestion rule definitions |
| `StageInfo` | Data class representing stage metadata (name, path, files, count) |
| `_resolve_super_name()` | Backward-compatibility helper for extracting super_name from various object shapes |

### Key design patterns

- **Distributed mutex:** Redis `SET key token NX EX 30` for lock acquisition, Lua `GET+DEL` for atomic release. Prevents concurrent stage/pipe mutations on the same staging area.
- **Semantic deduplication:** Pipes are uniquely identified by `(simple_name, overwrite_columns)` within a staging area — duplicate combinations are rejected.
- **Append-only file index:** The `_files.json` manifest is read-modify-write under lock, appending entries with timestamps and row counts.
- **Fail-fast validation:** Both `Staging` and `SuperPipe` validate that the parent entity exists (Supertable, staging area) in their constructors before allowing any operations.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `supertable/staging_area.py` | Staging area lifecycle and Parquet file management | `Staging`, `StageInfo`, `_resolve_super_name()` | Core data landing mechanism |
| `supertable/super_pipe.py` | Pipe definition CRUD (ingestion rule management) | `SuperPipe` | Defines how staged data gets merged into the main dataset |

---

## 5. Detailed Functional Capabilities

### 5.1 Staging Area Creation and Initialization

- **Description:** Creates a named staging area within a Supertable, including the physical directory in object storage and metadata registration in Redis.
- **Business purpose:** Provides an isolated landing zone for incoming data before it is promoted to the main dataset.
- **Trigger:** `Staging(organization=..., super_name=..., staging_name="my_stage")` constructor.
- **Processing:**
  1. Validates Supertable existence via `RedisCatalog.root_exists()`.
  2. Acquires Redis distributed lock for the staging area (30s TTL).
  3. Creates physical directory `{org}/{super}/staging/{stage_name}/` if absent.
  4. Registers metadata in Redis via `catalog.upsert_staging_meta()` with `created_at_ms`.
  5. Creates empty file index `{org}/{super}/staging/{stage_name}_files.json` if absent.
  6. Releases lock (atomic Lua compare-and-delete).
- **Output:** Initialized `Staging` instance ready for file operations.
- **Dependencies:** `get_storage()`, `RedisCatalog`, Redis distributed lock.
- **Constraints:** Lock TTL is 30 seconds. If initialization takes longer, lock may expire and another process could acquire it.
- **Risks:** Lock expiry during slow storage operations (e.g., high-latency S3 `makedirs`) could lead to concurrent initialization. No retry on lock failure — raises immediately.
- **Confidence:** Explicit.

### 5.2 Parquet File Write to Staging

- **Description:** Writes a PyArrow Table as a Parquet file into a staging area, updating the file index.
- **Business purpose:** The primary data ingestion operation — landing data in a staging area for later promotion.
- **Trigger:** `staging.save_as_parquet(role_name=..., arrow_table=..., base_file_name=...)`.
- **Processing:**
  1. Validates stage mode.
  2. Checks write RBAC via `check_write_access()`.
  3. Acquires Redis distributed lock.
  4. Generates unique filename: `{base_name}_{time_ns}.parquet`.
  5. Writes Parquet file to `{stage_dir}/{file_name}` via storage backend.
  6. Reads current file index JSON, appends new entry (`file`, `written_at_ns`, `rows`), writes back.
  7. Releases lock.
- **Output:** Returns the generated file name string.
- **Dependencies:** `pyarrow`, `get_storage()`, Redis lock.
- **Constraints:** File index is a read-modify-write operation under lock — non-atomic with respect to the Parquet write. If the process crashes between Parquet write and index update, the file exists but is not indexed.
- **Risks:** Orphaned Parquet files if crash occurs after write but before index update. No deduplication of file content — same data written twice produces two files.
- **Confidence:** Explicit.

### 5.3 Staging Area Directory Structure Inspection

- **Description:** Returns a comprehensive JSON-serializable view of all staging areas within a Supertable.
- **Business purpose:** Admin/operator visibility into the staging layer — what stages exist, how many files each contains, what Redis metadata is associated.
- **Trigger:** `staging.get_directory_structure(role_name=...)` in manager mode.
- **Processing:**
  1. Checks meta-level RBAC.
  2. Queries Redis for all staging names via `catalog.list_stagings()`.
  3. For each stage: checks physical existence, reads file index, reads Redis metadata.
  4. Assembles structured dict with counts, paths, existence flags.
- **Output:** Dict with `organization`, `super_name`, `base_staging_dir`, `stages` list (each with `name`, `path`, `files`, `file_count`, `redis_meta`, existence flags).
- **Confidence:** Explicit.

### 5.4 Staging Area Deletion

- **Description:** Deletes a staging area completely — files, file index, and Redis metadata.
- **Business purpose:** Cleanup after data promotion or abandonment of a staging area.
- **Trigger:** `staging.delete(role_name=...)`.
- **Processing:**
  1. Checks write RBAC.
  2. Acquires Redis distributed lock.
  3. Recursively deletes the stage directory and all files.
  4. Deletes the file index JSON.
  5. Removes Redis metadata via `catalog.delete_staging_meta()`.
  6. Releases lock.
- **Constraints:** Deletion is not atomic across storage and Redis. If Redis delete succeeds but storage delete fails (or vice versa), state becomes inconsistent.
- **Confidence:** Explicit.

### 5.5 Pipe Creation with Semantic Deduplication

- **Description:** Creates a pipe definition — a declarative rule binding a staging area to a target table with overwrite column semantics.
- **Business purpose:** Defines *how* staged data should be merged into the main dataset. The `overwrite_columns` field specifies which columns constitute the row identity for upsert operations.
- **Trigger:** `super_pipe.create(role_name=..., pipe_name=..., simple_name=..., user_hash=..., overwrite_columns=[...])`.
- **Processing:**
  1. Checks write RBAC (against `simple_name` as the table).
  2. Acquires Redis distributed lock (same lock key as staging area, 10s TTL).
  3. Lists existing pipes via `catalog.list_pipe_metas()`.
  4. Checks for semantic duplicates: same `simple_name` + `overwrite_columns` under a different `pipe_name` → raises `ValueError`.
  5. If the same `pipe_name` already exists with the same config, it's an upsert (no error).
  6. Stores pipe definition in Redis: `staging_name`, `pipe_name`, `user_hash`, `simple_name`, `overwrite_columns`, `transformation` (empty list), `updated_at_ns`, `enabled`.
  7. Returns a pseudo-URI: `redis://{org}/{super}/{staging}/{pipe}`.
- **Output:** String URI for the created pipe.
- **Dependencies:** `RedisCatalog` only (no storage operations).
- **Constraints:** Lock TTL is 10s (shorter than staging's 30s). The `transformation` field is always an empty list — transformation support appears planned but not yet implemented.
- **Risks:** The semantic dedup check iterates all existing pipes linearly — could be slow at high pipe counts.
- **Confidence:** Explicit.

### 5.6 Pipe Enable/Disable

- **Description:** Toggles a pipe's `enabled` flag without deleting the definition.
- **Business purpose:** Allows pausing data ingestion for a specific pipe without losing its configuration.
- **Trigger:** `super_pipe.set_enabled(pipe_name=..., enabled=..., role_name=...)`.
- **Processing:** Read-modify-write of pipe metadata in Redis under lock.
- **Confidence:** Explicit.

### 5.7 Pipe Deletion

- **Description:** Removes a pipe definition from Redis.
- **Business purpose:** Permanent removal of an ingestion rule.
- **Trigger:** `super_pipe.delete(pipe_name=..., role_name=...)`.
- **Output:** `bool` indicating whether the pipe existed and was deleted.
- **Confidence:** Explicit.

### 5.8 Pipe Read

- **Description:** Retrieves a pipe definition from Redis.
- **Business purpose:** Inspect pipe configuration.
- **Trigger:** `super_pipe.read(pipe_name=..., role_name=...)`.
- **Output:** Dict with all pipe metadata fields.
- **Note:** This is the only pipe operation that does NOT acquire a lock (read-only).
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 `staging_area.py`

#### `_resolve_super_name(super_table)`

| Attribute | Details |
|---|---|
| **Type** | Module-level function |
| **Purpose** | Backward-compatibility helper — extracts `super_name` from various object types |
| **Probes** | `super_name`, `name`, `supertable`, `table_name` attributes |
| **Returns** | `Optional[str]` |
| **Importance** | Supporting |

#### `StageInfo`

| Attribute | Details |
|---|---|
| **Type** | Frozen dataclass |
| **Fields** | `name`, `path`, `files_index_path`, `files` (list), `file_count` |
| **Purpose** | Structured representation of a staging area |
| **Note** | Defined but not used within this file — likely consumed by callers |
| **Importance** | Supporting |

#### `Staging`

| Attribute | Details |
|---|---|
| **Type** | Class (dual-mode) |
| **Modes** | Manager mode (`staging_name=None`), Stage mode (`staging_name` provided) |
| **Constructor validation** | Checks Supertable existence via `RedisCatalog.root_exists()` |
| **Importance** | Critical |

Key methods:

| Method | Mode | Purpose | RBAC | Lock | Side Effects |
|---|---|---|---|---|---|
| `open(staging_name)` | Manager | Returns a stage-mode `Staging` instance | None | None | Creates new instance |
| `get_directory_structure(role_name)` | Manager | Lists all stages with file details | `check_meta_access` | None | Read-only |
| `save_as_parquet(role_name, arrow_table, base_file_name)` | Stage | Writes Parquet + updates index | `check_write_access` | Yes (30s) | Storage write, index update |
| `list_files(role_name)` | Stage | Lists indexed file names | `check_meta_access` | None | Read-only |
| `delete(role_name)` | Stage | Deletes stage: files + index + Redis | `check_write_access` | Yes (30s) | Destructive |
| `_init_stage()` | Stage | Creates directory, Redis meta, file index | N/A | Called under lock | Storage + Redis writes |
| `_with_lock(fn)` | Stage | Executes `fn` under Redis distributed lock | N/A | Acquires/releases | Redis SET/DEL |
| `_require_stage_mode()` | Both | Guard: raises if in manager mode | N/A | None | None |

### 6.2 `super_pipe.py`

#### `SuperPipe`

| Attribute | Details |
|---|---|
| **Type** | Class |
| **Purpose** | Redis-only CRUD for pipe (ingestion rule) definitions |
| **Constructor validation** | Checks staging existence via `RedisCatalog.get_staging_meta()` |
| **Importance** | Critical |

Key methods:

| Method | Purpose | RBAC | Lock | Returns |
|---|---|---|---|---|
| `create(role_name, pipe_name, simple_name, user_hash, overwrite_columns, enabled)` | Create/upsert pipe definition | `check_write_access` (on `simple_name`) | Yes (10s) | URI string |
| `set_enabled(pipe_name, enabled, role_name)` | Toggle pipe enabled flag | `check_write_access` | Yes (10s) | None |
| `delete(pipe_name, role_name)` | Delete pipe definition | `check_write_access` | Yes (10s) | `bool` |
| `read(pipe_name, role_name)` | Read pipe definition | `check_meta_access` | No | `Dict` |
| `_with_lock(fn)` | Executes under Redis distributed lock | N/A | Acquires/releases | Any |

---

## 7. End-to-End Workflows

### 7.1 Data Landing Workflow (Staging Write)

1. **Trigger:** Data producer calls `Staging(organization=..., super_name=..., staging_name="daily_load")`.
2. Constructor validates Supertable exists in Redis (`root_exists`).
3. Acquires Redis lock `supertable:{org}:{super}:lock:stage:daily_load` (30s TTL).
4. Creates `{org}/{super}/staging/daily_load/` directory if absent.
5. Registers staging metadata in Redis.
6. Creates empty `daily_load_files.json` index if absent.
7. Releases lock.
8. Producer calls `staging.save_as_parquet(role_name="writer", arrow_table=table, base_file_name="orders")`.
9. RBAC check: `check_write_access()`.
10. Acquires Redis lock again.
11. Generates filename: `orders_1709312456789012345.parquet`.
12. Writes Parquet file to `{org}/{super}/staging/daily_load/orders_1709312456789012345.parquet`.
13. Reads `daily_load_files.json`, appends `{"file": "orders_...", "written_at_ns": ..., "rows": N}`, writes back.
14. Releases lock.
15. **Success:** File staged, index updated, ready for pipe-driven promotion.

### 7.2 Pipe-Based Ingestion Rule Definition

1. **Trigger:** Engineer calls `SuperPipe(organization=..., super_name=..., staging_name="daily_load")`.
2. Constructor validates staging exists in Redis.
3. Engineer calls `pipe.create(role_name="admin", pipe_name="orders_upsert", simple_name="orders", user_hash="abc123", overwrite_columns=["order_id"])`.
4. RBAC check: `check_write_access()` against table `"orders"`.
5. Acquires Redis lock `supertable:{org}:{super}:lock:stage:daily_load` (10s TTL).
6. Lists existing pipes; checks no other pipe has `simple_name="orders"` + `overwrite_columns=["order_id"]`.
7. Stores pipe definition in Redis with `enabled=True`, empty `transformation` list.
8. Releases lock.
9. Returns `redis://{org}/{super}/daily_load/orders_upsert`.
10. **Success:** Pipe defined. An inferred downstream worker can now read this pipe, find staged files, and merge them into the `orders` table using `order_id` as the upsert key.

### 7.3 Staging Inspection Workflow

1. **Trigger:** Operator calls `staging.get_directory_structure(role_name="admin")` in manager mode.
2. RBAC check.
3. Queries Redis for all staging names.
4. For each stage: reads file index, checks physical existence, reads Redis metadata.
5. Returns comprehensive dict with stage list, file counts, paths, metadata.
6. **Success:** Operator has full visibility into the staging layer.

### 7.4 Staging Cleanup Workflow

1. **Trigger:** Operator calls `staging.delete(role_name="admin")`.
2. RBAC check.
3. Acquires Redis lock.
4. Recursively deletes stage directory and all Parquet files.
5. Deletes file index JSON.
6. Removes Redis metadata.
7. Releases lock.
8. **Success:** Staging area fully cleaned up.

---

## 8. Data Model and Information Flow

### Core entities

| Entity | Storage | Shape | Description |
|---|---|---|---|
| **Staging area** | Redis + object storage | Directory + JSON index + Redis hash | Named data landing zone within a Supertable |
| **File index** | Object storage (JSON) | `[{"file": str, "written_at_ns": int, "rows": int}, ...]` | Append-only manifest of staged Parquet files |
| **Staging metadata** | Redis | `{"path": str, "created_at_ms": int}` | Creation-time metadata for a staging area |
| **Pipe definition** | Redis only | `{"staging_name", "pipe_name", "user_hash", "simple_name", "overwrite_columns", "transformation", "updated_at_ns", "enabled"}` | Declarative ingestion rule binding staging to target table |
| **Staged Parquet file** | Object storage | Apache Parquet | Actual data file with nanosecond-timestamp suffix |

### Physical storage layout

```
{org}/{super}/
  staging/
    {stage_name}/
      {base_name}_{time_ns}.parquet
      {base_name}_{time_ns}.parquet
      ...
    {stage_name}_files.json          ← file index (sibling to directory)
```

### Redis key patterns

| Key pattern | Type | Purpose |
|---|---|---|
| `supertable:{org}:{super}:meta:staging:{stage_name}` | Hash (inferred) | Staging area metadata |
| `supertable:{org}:{super}:lock:stage:{stage_name}` | String (NX/EX) | Distributed lock for staging mutations |
| Pipe metadata keys (via `RedisCatalog`) | Hash (inferred) | Pipe definitions |
| Staging index set (via `RedisCatalog.list_stagings()`) | Set (inferred) | Fast listing of all staging names |

### Information flow

```
Data Producer
    │
    ▼
Staging.save_as_parquet()
    │
    ├──► Object Storage: Parquet file
    └──► Object Storage: _files.json index (appended)
         Redis: staging metadata

Data Engineer
    │
    ▼
SuperPipe.create()
    │
    └──► Redis: pipe definition
              │
              ▼  (inferred: merge worker reads pipes + files)
         Main Supertable dataset
```

### Pipe definition schema

```json
{
  "staging_name": "daily_load",
  "pipe_name": "orders_upsert",
  "user_hash": "abc123",
  "simple_name": "orders",
  "overwrite_columns": ["order_id"],
  "transformation": [],
  "updated_at_ns": 1709312456789012345,
  "enabled": true
}
```

Key field semantics:
- `simple_name`: The target table within the Supertable (what the staged data maps to).
- `overwrite_columns`: Columns that define row identity for upsert/merge operations. When empty, likely implies full append.
- `user_hash`: Tracks which user/credential created the pipe (audit/provenance).
- `transformation`: Empty list — placeholder for future transformation rules.
- `enabled`: Runtime toggle to pause/resume the pipe without deleting it.

---

## 9. Dependencies and Integrations

### Internal dependencies

| Module | Purpose | Used by |
|---|---|---|
| `supertable.config.defaults.logger` | Shared logging | Both files |
| `supertable.storage.storage_factory.get_storage()` | Storage abstraction (S3/local) | `Staging` |
| `supertable.redis_catalog.RedisCatalog` | Redis metadata operations | Both files |
| `supertable.rbac.access_control.check_write_access` | Write RBAC enforcement | Both files |
| `supertable.rbac.access_control.check_meta_access` | Read/inspect RBAC enforcement | Both files |

### Third-party dependencies

| Package | Purpose |
|---|---|
| `pyarrow` | Arrow Table input for Parquet writes |
| `redis` (via `RedisCatalog.r`) | Distributed locking, metadata storage |

### External systems

| System | Role | Failure impact |
|---|---|---|
| **Redis** | Metadata storage, distributed locking, pipe definitions | All operations fail — no fallback (unlike monitoring) |
| **Object storage** | Parquet file and file index persistence | Write/read operations fail |

### Key difference from monitoring subsystem

Unlike the monitoring writer (which gracefully degrades without Redis), the staging and pipe subsystems **require Redis** — it is not optional. This is because Redis stores the authoritative metadata and provides the coordination mechanism.

---

## 10. Architecture Positioning

### Where this code sits

```
┌─────────────────────────────────────────────────┐
│           External Data Producer                 │
│     (API client, ETL job, SDK user)              │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│      staging_area.py  [THIS CODE]                │
│  Staging.save_as_parquet() → storage + index     │
└───────────────────────┬─────────────────────────┘
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
┌──────────────────┐  ┌─────────────────────────┐
│  Object Storage  │  │  super_pipe.py [THIS]    │
│  Parquet files   │  │  Pipe definitions        │
│  + file indices  │  │  (Redis only)            │
└──────────────────┘  └──────────┬──────────────┘
              │                  │
              └────────┬─────────┘
                       ▼
┌─────────────────────────────────────────────────┐
│     Merge/Ingest Worker (NOT in provided code)   │
│  Reads pipes → discovers staged files →          │
│  merges into main Supertable using               │
│  overwrite_columns for upsert semantics          │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│       Main Supertable Dataset                    │
│  (Parquet-based analytical store)                │
└─────────────────────────────────────────────────┘
```

### Relationship to previously analyzed code

| Component | Relationship |
|---|---|
| `MonitoringWriter` | Shares `get_storage()` and `RedisCatalog` dependencies. Different concern (observability vs. ingestion). |
| `MonitoringReader` | Shares `get_storage()` and DuckDB infrastructure. The reader could potentially query staged data too. |
| `DataEstimator` / `DuckDBExecutor` | Not used here — staging operates at the storage/Redis layer, not the query layer. |
| `check_write_access` / `check_meta_access` | Shared RBAC enforcement used across all subsystems. |

### Boundary observations

- Staging and Pipe are **write-path components** — they handle data landing and ingestion rule definition.
- The actual **merge/promotion logic** (reading pipes, reading staged files, executing upserts) is NOT present in these files.
- The pipe's `transformation` field (always `[]`) suggests planned support for in-pipe data transformations.
- Both classes share the same Redis lock key pattern (`lock:stage:{staging_name}`), ensuring mutual exclusion between staging mutations and pipe mutations on the same staging area.

### Coupling considerations

- **Tight coupling to Redis:** Both classes directly access `self.catalog.r` (raw Redis client) for distributed locking, bypassing the `RedisCatalog` abstraction. This couples them to Redis's specific `SET NX EX` and Lua `eval` semantics.
- **Tight coupling to storage abstraction:** `Staging` depends on `storage.exists()`, `storage.makedirs()`, `storage.write_parquet()`, `storage.write_json()`, `storage.read_json()`, `storage.delete()`, `storage.delete_recursive()` — a rich storage interface.
- **Loose coupling between Staging and SuperPipe:** They are connected by the staging name and Redis metadata, but are independent classes. A pipe validates staging existence but does not directly interact with the `Staging` class.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Data producers need a structured, safe way to load data into the platform. Raw writes to the production dataset risk corruption, schema conflicts, and data quality issues. |
| **Why it matters** | The staging + pipe pattern is a best practice for enterprise data platforms. It enables preview-before-commit, rollback (delete staging), and controlled merge semantics (overwrite columns). |
| **Revenue relevance** | Directly revenue-enabling: this is the primary data ingestion mechanism. Without it, users cannot load data into Supertable. |
| **Efficiency relevance** | The distributed locking and batch-append model enable concurrent data producers without conflicts. The file index provides cheap metadata queries without listing object storage. |
| **What would be lost without it** | No structured data ingestion. Users would need direct storage access, losing RBAC, auditing, concurrency safety, and merge semantics. |
| **Differentiating vs commodity** | The staging concept is common, but the **pipe abstraction with declarative overwrite columns** is a differentiating feature. It allows users to define upsert semantics declaratively rather than writing custom merge logic. The `transformation` placeholder suggests further differentiation is planned. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Moderate. Dual-mode class adds conceptual complexity but reduces API surface. Distributed locking is correctly implemented with atomic release. |
| **Architectural maturity** | Good. Separation of staging (data) from pipes (rules) follows the command/query separation principle. Redis as metadata store is appropriate for this coordination-heavy workload. |
| **Maintainability** | Good. Clear method responsibilities, RBAC enforcement on every public method, logging on mutations. |
| **Extensibility** | Good. New pipe fields (e.g., transformations, scheduling, validation rules) can be added to the Redis metadata without schema migrations. New staging operations can be added to the `Staging` class. |
| **Operational sensitivity** | **High.** Redis availability is critical — no fallback. Lock contention under high concurrency could cause `RuntimeError` exceptions. Lock TTL expiry under slow storage could cause unsafe concurrent access. |
| **Performance** | File index read-modify-write is O(n) in the number of files — could degrade for very large staging areas. Lock TTL differences (30s for staging, 10s for pipes) suggest different expected operation durations. |
| **Reliability** | **Risk:** Non-atomic multi-system operations. The sequence "write Parquet → update index → update Redis" can fail at any step, leaving inconsistent state. No compensation/rollback mechanism. |
| **Security** | RBAC enforced on all public methods. Write operations use `check_write_access`, read operations use `check_meta_access`. The `user_hash` in pipe definitions provides audit trail for pipe creators. |
| **Testing clues** | The `_resolve_super_name()` function with its multi-attribute probing suggests diverse calling patterns that should be tested. The backward-compatibility comments reference specific example files. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Staging areas are scoped to `{organization}/{super_name}/staging/{stage_name}/`.
- Parquet files get nanosecond-timestamp suffixes for uniqueness.
- File indices are JSON arrays stored as sibling files to the staging directory.
- Pipe definitions are stored exclusively in Redis (no storage persistence).
- Semantic deduplication prevents duplicate `(simple_name, overwrite_columns)` pipe configurations.
- Distributed locking uses Redis `SET NX EX` with Lua atomic release.
- Staging and pipe mutations share the same lock key pattern, preventing concurrent staging + pipe modifications.
- RBAC is enforced on all public operations.
- The `transformation` field in pipes is always an empty list.
- Pipe operations validate staging existence before proceeding.
- The `StageInfo` dataclass is defined but not used within these files.

### Reasonable Inferences

- **A merge/ingest worker exists** that reads enabled pipe definitions, discovers staged files via the file index, and merges data into the main Supertable using `overwrite_columns` for upsert semantics. This worker is the critical missing piece.
- **`overwrite_columns`** defines the columns used to detect duplicate rows during the merge — rows with matching values in these columns are overwritten; others are appended.
- **`simple_name`** is the logical table name within the Supertable that the staged data maps to. The Supertable likely contains multiple logical tables (fact tables, dimension tables).
- **`user_hash`** serves as an audit/provenance identifier — tracking which API credential or user created a pipe.
- **The `transformation` field** is planned for future use — enabling in-pipe data transformations (column renames, type casts, filters) before merge.
- **The backward-compatibility code** (e.g., `_resolve_super_name`, dual-mode `Staging`, method aliases) indicates this is an evolving API with active users of older patterns.

### Unknown / Not Visible in Provided Snippet

- The merge/ingest worker that consumes pipes and staged files.
- How `RedisCatalog` stores pipe and staging metadata internally (key structure, data format).
- Whether `list_stagings()` and `list_pipe_metas()` use Redis Sets, sorted sets, or key-pattern scans.
- The full RBAC model — what roles exist, what `check_write_access` vs `check_meta_access` enforce.
- Whether staging areas are automatically cleaned up after merge, or if cleanup is manual.
- Whether there is a UI for staging/pipe management or if it's API/SDK-only.
- The error handling and retry behavior of the downstream merge worker.
- How `overwrite_columns=[]` (empty) is interpreted by the merge logic.
- Whether pipes support scheduling or are consumed continuously.
- The relationship between `pipe_name` and `simple_name` — whether they are always different or can be the same.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `Supertable Staging Area & Pipe Management`
- **Alternative names / aliases:** staging, stage, pipe, SuperPipe, data landing zone, ingestion rules, upsert pipe
- **Main responsibilities:** (1) Create and manage staging areas for Parquet file landing, (2) Define pipes — declarative ingestion rules with upsert semantics, (3) RBAC-protected CRUD with distributed locking
- **Important entities:** `Staging`, `SuperPipe`, `StageInfo`, file index (`_files.json`), pipe definition, staging metadata, distributed lock
- **Important workflows:** data landing (write Parquet to staging), pipe creation (define merge rules), staging inspection (list stages/files), staging deletion (cleanup)
- **Integration points:** Redis (`RedisCatalog`, distributed locks), object storage (Parquet files, JSON indices), RBAC (`check_write_access`, `check_meta_access`), PyArrow (table input)
- **Business purpose keywords:** data ingestion, staging, data landing, pipe, upsert, overwrite columns, merge rules, data promotion, ETL, data pipeline
- **Architecture keywords:** staging area, pipe abstraction, distributed lock, Redis metadata, dual-mode class, semantic deduplication, append-only index, multi-tenant
- **Follow-up files for completeness:**
  - **Merge/ingest worker** — the component that reads pipes and staged files and executes the merge (critical missing piece)
  - `supertable/redis_catalog.py` — Redis metadata operations (`upsert_staging_meta`, `list_stagings`, `upsert_pipe_meta`, etc.)
  - RBAC module — `supertable/rbac/access_control.py`
  - Storage backend implementation — `supertable/storage/`
  - API layer that exposes staging and pipe operations
  - `examples/3.4. read_staging.py` — referenced backward-compatibility example
  - SuperTable class definition — the parent entity

---

## 15. Suggested Documentation Tags

`staging-area`, `data-ingestion`, `pipe-management`, `upsert-semantics`, `overwrite-columns`, `distributed-locking`, `redis-metadata`, `parquet-landing`, `file-index`, `RBAC`, `multi-tenant`, `data-pipeline`, `data-promotion`, `merge-rules`, `append-only`, `concurrency-control`, `dual-mode-class`, `backward-compatibility`, `semantic-deduplication`, `pyarrow`, `object-storage`, `ETL`, `data-landing-zone`

---

## Merge Readiness

- **Suggested canonical name:** `Supertable Staging Area & Pipe Management`
- **Standalone or partial:** **Partial.** The staging write side and pipe definition side are complete, but the merge/promotion execution logic (the consumer of pipes and staged files) is missing.
- **Related components to merge later:**
  - Merge/ingest worker (reads pipes → reads staged Parquet → merges into main dataset)
  - `RedisCatalog` — the full Redis metadata abstraction
  - RBAC module — authorization model and role definitions
  - Storage backend — how Parquet writes and JSON index operations work at the storage level
  - SuperTable class — the parent entity and its lifecycle
  - Previously analyzed: `Supertable Monitoring Subsystem` (shares storage and Redis dependencies)
- **What would most improve certainty:**
  1. The merge/ingest worker code
  2. `RedisCatalog` implementation (to understand Redis key structures)
  3. How `overwrite_columns` is consumed by the merge logic
  4. Whether `transformation` is actively developed or abandoned
- **Recommended merge key phrases:** `staging`, `Staging`, `SuperPipe`, `pipe`, `staging_name`, `overwrite_columns`, `simple_name`, `_files.json`, `data landing`, `data promotion`, `upsert`
