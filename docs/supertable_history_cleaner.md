# Feature / Component Reverse Engineering: Supertable HistoryCleaner (Snapshot & Data File Garbage Collection)

## 1. Executive Summary

This code implements the **garbage collection / history cleanup** subsystem for the Supertable platform — a storage-level maintenance operation that removes stale snapshot JSONs and orphaned Parquet data files that are no longer referenced by the current state of any SimpleTable.

- **Main responsibility:** Identify and delete files in object storage that are older than the current data version and not referenced by any active snapshot, reclaiming storage space and preventing unbounded growth.
- **Likely product capability:** Automated or operator-triggered storage hygiene for the analytical data store. Ensures that after repeated writes, merges, and compactions, old superseded files are cleaned up.
- **Business purpose:** Storage cost control, operational cleanliness, and preventing confusion from stale data artifacts in a multi-tenant data platform.
- **Technical role:** A maintenance/operational utility that reads the authoritative state from Redis (root metadata, leaf pointers, snapshot JSONs) and deletes storage objects that fall below a freshness threshold. It is a destructive operation protected by the highest RBAC level (`check_control_access`).

---

## 2. Functional Overview

### What can the product do because of this code?

1. **Clean up stale Parquet data files** — files from previous data versions that have been superseded by newer writes/merges.
2. **Clean up old snapshot JSONs** — historical snapshot metadata files that are no longer the current snapshot for any SimpleTable.
3. **Clean up legacy files** in the SuperTable root directory from older product versions.
4. **Safely preserve active data** — never deletes the current snapshot or any file listed in the active `resources` array.

### Target users / actors

| Actor | Interaction |
|---|---|
| **Platform operator / admin** | Triggers `HistoryCleaner.clean()` to reclaim storage |
| **Scheduled maintenance job (inferred)** | Likely invoked periodically via a cron job or background task |
| **RBAC-authorized user with CONTROL access** | Only users with the highest permission level can execute cleanup |

### Business workflows

1. **Storage reclamation:** Operator or scheduled job triggers cleanup → stale files identified by timestamp → deleted from storage → storage costs reduced.
2. **Post-compaction cleanup:** After data compaction or merge operations produce new consolidated Parquet files, the old pre-compaction files become stale and are candidates for deletion.
3. **Legacy migration cleanup:** Files from older product versions (pre-Redis-catalog era) that linger in the SuperTable root directory are cleaned up.

### Strategic significance

Every append/merge/compaction cycle produces new files while leaving old ones behind. Without garbage collection, storage grows monotonically and costs escalate. This subsystem is the **storage cost control mechanism** — essential for any production data platform with mutable semantics (upserts, overwrites, compaction).

---

## 3. Technical Overview

### Architectural style

- **Timestamp-based garbage collection:** Files are named with a millisecond-epoch prefix (e.g., `1699999999999_data.parquet`). Files whose timestamp prefix is at or below a freshness threshold are considered stale.
- **Active-set exclusion:** The current snapshot JSON and all files listed in its `resources` array are explicitly excluded from deletion, regardless of their timestamp.
- **Two-phase threshold:** The freshness threshold is `max(root.ts, snapshot.last_updated_ms)` — the more conservative (newer) of the SuperTable-level and SimpleTable-level timestamps.
- **Storage-agnostic:** All file operations go through the storage abstraction (`self.storage`), making this work on S3, local filesystem, or any other backend.

### Key design patterns

- **Conservative deletion:** Multiple safety layers prevent accidental data loss:
  1. Active resources are explicitly excluded.
  2. Current snapshot path is explicitly excluded.
  3. Only timestamp-prefixed filenames are considered (non-conforming files are ignored).
  4. The stricter of two timestamps is used as the threshold.
  5. RBAC requires `CONTROL` access (the highest permission level).
- **Scan-based discovery:** SimpleTables are discovered via `catalog.scan_leaf_items()` — a Redis SCAN operation — rather than storage listing, ensuring alignment with the authoritative catalog.
- **Graceful error handling:** Missing folders, unreadable snapshots, and delete failures are all logged and skipped, never causing the entire cleanup to abort.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `supertable/history_cleaner.py` | Garbage collection of stale snapshots and Parquet files | `HistoryCleaner` | Core storage maintenance utility |

---

## 5. Detailed Functional Capabilities

### 5.1 Full History Cleanup

- **Description:** Orchestrates cleanup across the entire SuperTable — both legacy files in the super directory and per-SimpleTable stale files.
- **Business purpose:** Comprehensive storage reclamation in a single operation.
- **Trigger:** `history_cleaner.clean(role_name=...)`.
- **Processing:**
  1. RBAC check: `check_control_access()` (highest privilege level).
  2. Reads root metadata from Redis to get `root_ts` (SuperTable-level timestamp).
  3. **Phase 1 — Legacy cleanup:** Collects all Parquet/JSON files under the SuperTable directory. Deletes those with timestamp prefix ≤ `root_ts`.
  4. **Phase 2 — Per-SimpleTable cleanup:** Iterates all SimpleTables via `catalog.scan_leaf_items()`. For each:
     a. Reads the current snapshot JSON from storage (path comes from the Redis leaf pointer).
     b. Builds the **active set**: current snapshot path + all files in `resources[]`.
     c. Collects all files in the SimpleTable's `location` directory.
     d. Excludes active files from candidates.
     e. Computes threshold: `max(root_ts, snapshot.last_updated_ms)`.
     f. Deletes files with timestamp prefix ≤ threshold.
  5. Logs total deleted count.
- **Output:** None (side effect: files deleted from storage).
- **Dependencies:** `SuperTable`, `RedisCatalog`, storage backend, `check_control_access`.
- **Constraints:** RBAC requires CONTROL access. Snapshot must be readable for per-table cleanup to proceed.
- **Risks:** If a snapshot is corrupted or unreadable, that table's cleanup is skipped entirely (safe but incomplete). Race condition if a new snapshot is being written concurrently — discussed in detail below.
- **Confidence:** Explicit.

### 5.2 File Collection

- **Description:** Collects candidate Parquet and JSON files from a storage directory.
- **Business purpose:** Discovers files that may be stale.
- **Trigger:** `collect_files(location)`.
- **Processing:** Lists files in three sub-paths:
  1. `{location}/data/*.parquet` — data files.
  2. `{location}/snapshots/*.json` — snapshot metadata files.
  3. `{location}/*.json` — legacy/root-level JSON files.
- **Output:** `List[str]` of file paths.
- **Constraints:** Missing directories are silently handled (empty result for that sub-path).
- **Confidence:** Explicit.

### 5.3 Timestamp-Based Stale File Identification

- **Description:** Filters files by their filename timestamp prefix against a threshold.
- **Business purpose:** Determines which files are old enough to be safely deleted.
- **Trigger:** `get_files_to_delete(designated_files, threshold_ms)`.
- **Processing:**
  1. For each file, extracts `os.path.basename()`.
  2. Splits on `_`, takes the first segment as the timestamp.
  3. Parses as integer (millisecond epoch).
  4. If `timestamp ≤ threshold_ms` → mark for deletion.
  5. Non-numeric prefixes are silently skipped (never deleted).
- **Output:** `List[str]` of files to delete.
- **Key safety property:** Files that don't follow the `{ms_timestamp}_*` naming convention are never deleted.
- **Confidence:** Explicit.

### 5.4 File Deletion

- **Description:** Deletes files from storage one at a time, counting successes and logging failures.
- **Business purpose:** The actual cleanup operation.
- **Trigger:** `delete_files(files_to_delete)`.
- **Processing:** Iterates files, calls `storage.delete()` per file, catches and logs errors.
- **Output:** `int` — count of successfully deleted files.
- **Constraints:** Deletion is sequential, not parallelized. Large cleanup operations may be slow.
- **Confidence:** Explicit.

---

## 6. Classes, Functions, and Methods

### `HistoryCleaner`

| Attribute | Details |
|---|---|
| **Type** | Class |
| **Purpose** | Garbage collection of stale snapshots and data files |
| **Constructor** | Creates `SuperTable` (for storage backend and directory paths), instantiates `RedisCatalog` |
| **Instance state** | `self.super_table`, `self.storage`, `self.catalog` |
| **Importance** | Critical |

Methods:

| Method | Purpose | RBAC | Destructive | Returns |
|---|---|---|---|---|
| `clean(role_name)` | Full cleanup orchestration | `check_control_access` | Yes | `None` |
| `collect_files(location)` | List Parquet/JSON files in a directory | None | No | `List[str]` |
| `get_files_to_delete(designated_files, threshold_ms)` | Filter files by timestamp threshold | None | No | `List[str]` |
| `delete_files(files_to_delete)` | Delete files from storage | None | Yes | `int` (count) |

---

## 7. End-to-End Workflows

### 7.1 Full Cleanup Workflow

1. **Trigger:** Operator calls `HistoryCleaner("sales_data", "acme").clean(role_name="admin")`.
2. **RBAC:** `check_control_access()` verifies `admin` has CONTROL-level access.
3. **Root timestamp:** Reads Redis `meta:root` for `acme/sales_data` → `{"ts": 1709312456000, "version": 42}` → `root_ts = 1709312456000`.
4. **Phase 1 — Legacy cleanup:**
   a. Collects files from `{super_dir}/data/*.parquet`, `{super_dir}/snapshots/*.json`, `{super_dir}/*.json`.
   b. Example files found: `1709000000000_old.parquet`, `1709312456000_current.parquet`.
   c. `get_files_to_delete()`: `1709000000000 ≤ 1709312456000` → stale. `1709312456000 ≤ 1709312456000` → stale (equal is included).
   d. Deletes stale files.
5. **Phase 2 — Per-SimpleTable cleanup:**
   a. `catalog.scan_leaf_items("acme", "sales_data")` yields: `[{"simple": "orders", "path": "acme/sales_data/orders/snapshots/1709312456000_snap.json"}, ...]`.
   b. For `orders`:
      - Reads snapshot JSON from storage.
      - `location = "acme/sales_data/orders"`.
      - `resources = [{"file": "acme/sales_data/orders/data/1709312456000_chunk1.parquet"}, ...]`.
      - Active set: `{snapshot_path, resource_file_1, resource_file_2, ...}`.
      - Collects all files in `acme/sales_data/orders/`.
      - Removes active files from candidates.
      - `threshold = max(1709312456000, 1709312400000) = 1709312456000`.
      - Filters candidates by timestamp ≤ threshold.
      - Deletes stale files.
   c. Repeats for all SimpleTables.
6. **Result:** Logs `[history-cleaner] Total files deleted: 47`.

### Concurrency / Race Condition Analysis

| Scenario | Behavior | Risk |
|---|---|---|
| Cleanup runs during active write | New files have timestamps > threshold → safe (not deleted). Current snapshot and active resources are excluded → safe. | **Low risk.** The threshold is derived from the current state; new files will be newer. |
| Snapshot is being rotated mid-cleanup | If new snapshot is written between reading the old snapshot and deleting files, files referenced by the new snapshot might have timestamps ≤ threshold. | **Moderate risk.** However, the new snapshot's resources would have been written with the new timestamp prefix, making them newer than threshold. The old snapshot's resources are still in the active set read earlier. Net risk is low in practice. |
| Cleanup runs while another cleanup runs | Both could try to delete the same files. `storage.delete()` on an already-deleted file would either succeed (idempotent) or fail (caught and logged). | **Low risk.** No data integrity issue, only duplicate work. |

---

## 8. Data Model and Information Flow

### Core entities

| Entity | Source | Role in Cleanup |
|---|---|---|
| **Root metadata** | Redis `meta:root` | Provides `ts` — the SuperTable-level freshness timestamp |
| **Leaf pointer** | Redis `meta:leaf:{table}` (via `scan_leaf_items`) | Provides `path` to the current snapshot JSON and `simple` name |
| **Snapshot JSON** | Object storage | Contains `location` (SimpleTable directory), `resources` (active files), `last_updated_ms` |
| **Resources** | Nested in snapshot `resources[]` | Active Parquet files that must NOT be deleted |
| **Parquet data files** | Object storage `{location}/data/` | Candidates for deletion if stale |
| **Snapshot JSON files** | Object storage `{location}/snapshots/` | Candidates for deletion if stale and not the current snapshot |

### File naming convention

```
{millisecond_epoch}_{arbitrary_suffix}.{parquet|json}
```

Examples:
- `1699999999999_chunk_0.parquet`
- `1709312456000_snapshot.json`

The leading numeric prefix is the **creation timestamp** and serves as the version identifier for garbage collection.

### Freshness threshold derivation

```
threshold = max(
    root_metadata.ts,              ← SuperTable-level timestamp from Redis
    snapshot.last_updated_ms       ← SimpleTable-level timestamp from snapshot JSON
)
```

The `max()` ensures the **stricter (newer)** threshold is used, preventing premature deletion when one timestamp source lags behind the other.

### Deletion decision flow

```
For each file in storage:
    │
    ├─ Is it the current snapshot path?      → KEEP (never delete)
    ├─ Is it in the active resources set?    → KEEP (never delete)
    ├─ Does filename start with a number?    → Continue
    │   └─ No                                → SKIP (ignore non-conforming)
    ├─ Is timestamp_prefix ≤ threshold?      → DELETE (stale)
    └─ Is timestamp_prefix > threshold?      → KEEP (too new)
```

### Physical storage layout (inferred from `collect_files`)

```
{org}/{super}/                          ← SuperTable root (legacy files)
  *.json                                ← Legacy JSONs
  data/*.parquet                        ← Legacy data files
  snapshots/*.json                      ← Legacy snapshots

{org}/{super}/{simple_table}/           ← SimpleTable location
  data/*.parquet                        ← Data files (active + stale)
  snapshots/*.json                      ← Snapshot files (current + stale)
  *.json                                ← Root-level JSONs
```

---

## 9. Dependencies and Integrations

### Internal dependencies

| Module | Purpose | Usage |
|---|---|---|
| `supertable.config.defaults.logger` | Shared logging | Debug and info logging throughout |
| `supertable.super_table.SuperTable` | SuperTable entity (provides `super_dir`, `storage`) | Constructor dependency |
| `supertable.redis_catalog.RedisCatalog` | Redis metadata operations | `get_root()`, `scan_leaf_items()` |
| `supertable.rbac.access_control.check_control_access` | CONTROL-level RBAC enforcement | Guards the `clean()` method |

### External systems

| System | Role | Failure impact |
|---|---|---|
| **Redis** | Authoritative metadata source (root timestamp, leaf pointers) | Cleanup cannot determine what is active vs stale. `get_root()` returns `{}` (root_ts=0), `scan_leaf_items()` returns nothing → only legacy cleanup runs. |
| **Object storage** | Source of files to clean and target of deletions | Snapshot read failures → that table skipped. Delete failures → individual files skipped. |

### New RBAC level revealed

This is the first analyzed file that uses `check_control_access` (previous files used `check_write_access` and `check_meta_access`). This establishes a **three-tier RBAC model**:

| Level | Permission | Used By |
|---|---|---|
| `check_meta_access` | Read/inspect metadata | MetaReader, MonitoringReader, Staging (list), SuperPipe (read) |
| `check_write_access` | Write/mutate data | Staging (save/delete), SuperPipe (create/delete/enable) |
| `check_control_access` | Destructive/administrative operations | HistoryCleaner |

---

## 10. Architecture Positioning

### Where this code sits

```
┌─────────────────────────────────────────────────┐
│    Operator / Scheduled Job / Admin CLI          │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│       history_cleaner.py  [THIS CODE]            │
│  HistoryCleaner.clean()                          │
└───────┬──────────────────────────┬──────────────┘
        │                          │
        ▼                          ▼
┌──────────────┐         ┌─────────────────────┐
│    Redis     │         │  Object Storage     │
│  (read-only) │         │  (read + delete)    │
│  root, leafs │         │  parquet, json      │
└──────────────┘         └─────────────────────┘
```

### Relationship to previously analyzed components

| Component | Relationship |
|---|---|
| **Staging Area** | Staging writes Parquet files that eventually become SimpleTable resources. After merge/compaction, old staged data may linger and need cleanup. |
| **SuperPipe** | Pipes trigger merges that produce new snapshots and resource files, making old versions stale. HistoryCleaner reclaims the old versions. |
| **MetaReader** | MetaReader reads the same `meta:root` and `meta:leaf:*` keys that HistoryCleaner uses to determine the active state. They share the same Redis data model. |
| **MonitoringReader** | MonitoringReader queries Parquet files too, but monitoring data lives under a separate `monitoring/` path and is NOT cleaned by HistoryCleaner (which targets the `data/` and `snapshots/` sub-paths). |

### Lifecycle position

HistoryCleaner sits at the **end of the data lifecycle**:

```
Data Producer → Staging → Pipe → Merge/Compaction → Active Snapshot
                                                          │
                                                    (new version)
                                                          │
                                          Previous version becomes stale
                                                          │
                                                          ▼
                                                   HistoryCleaner
                                                   (garbage collection)
```

### Coupling considerations

- **Tightly coupled to file naming convention:** The `{ms_timestamp}_*` prefix pattern is the sole mechanism for determining file age. Any file not following this convention is invisible to the cleaner.
- **Tightly coupled to snapshot structure:** Depends on `resources[].file` and `location` fields in the snapshot JSON.
- **Loosely coupled to Redis:** Uses `RedisCatalog` abstraction methods (`get_root`, `scan_leaf_items`) rather than raw Redis commands (unlike staging/pipe which access `catalog.r` directly).

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Every write, merge, and compaction operation creates new files while leaving old ones behind. Without cleanup, storage grows without bound, increasing costs and operational complexity. |
| **Why it matters** | Cloud object storage costs are proportional to stored volume. A platform with hundreds of tenants, each performing daily writes, can accumulate terabytes of stale data quickly. |
| **Revenue relevance** | Cost-efficiency related. Directly reduces infrastructure costs. In a SaaS model, storage costs eat into margins. For on-prem deployments, prevents storage capacity exhaustion. |
| **Efficiency relevance** | High. Without cleanup, storage listing operations become slower (more objects to enumerate), and costs escalate. Cleanup also reduces confusion when debugging — operators only see current files. |
| **What would be lost without it** | Unbounded storage growth. Over time, this would lead to cost overruns, slower storage operations, and operational confusion from stale artifacts. |
| **Differentiating vs commodity** | Commodity functionality — every data platform needs garbage collection. However, the conservative multi-safety-layer design (active set exclusion, timestamp threshold, RBAC control, non-conforming file skip) is production-quality. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | Low-moderate. The algorithm is straightforward (timestamp comparison + active set exclusion). The multi-phase design (legacy + per-table) adds modest complexity. |
| **Architectural maturity** | Good. Conservative deletion policy with multiple safety layers. Uses the authoritative catalog (Redis) for state, not heuristics. |
| **Maintainability** | Good. Clear method separation (collect, filter, delete). Easy to understand and modify. |
| **Extensibility** | Moderate. Adding new file types or directory structures would require modifying `collect_files()`. Adding new retention policies (e.g., "keep last N snapshots") would require modifying `get_files_to_delete()`. |
| **Operational sensitivity** | **Very high.** This is a destructive operation. Bugs here can cause **permanent data loss**. The conservative design reflects this — multiple safety checks, CONTROL-level RBAC, and non-conforming files are never touched. |
| **Performance** | Sequential file deletion (one at a time) could be slow for large cleanup jobs. No parallelism. SCAN-based SimpleTable discovery is efficient. |
| **Reliability** | Good degradation. Individual file read/delete failures are caught and logged, allowing the rest of the cleanup to proceed. Missing directories are handled gracefully. |
| **Security** | Protected by `check_control_access` — the highest RBAC tier. Only administrators can trigger cleanup. |
| **Testing clues** | The timestamp extraction logic (`filename.split("_")[0]`) is a clear unit test target. The active-set exclusion logic is critical and should be heavily tested. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Files are named with millisecond-epoch timestamp prefixes (e.g., `1699999999999_*.parquet`).
- The freshness threshold is `max(root.ts, snapshot.last_updated_ms)`.
- Files with timestamp ≤ threshold AND not in the active set are deleted.
- The current snapshot path and all `resources[].file` entries are explicitly excluded from deletion.
- Files without numeric prefixes are never deleted.
- Cleanup is protected by `check_control_access` (CONTROL-level RBAC).
- SimpleTables are discovered via `catalog.scan_leaf_items()` from Redis.
- Files are collected from `data/*.parquet`, `snapshots/*.json`, and `*.json` under each location.
- Individual failures (unreadable snapshot, delete error, missing directory) are logged and skipped.
- Deletion is sequential (one file at a time).
- The `≤` comparison means files whose timestamp **equals** the threshold are also deleted. This is safe because the threshold is derived from the current state, and active files from that state are in the exclusion set.

### Reasonable Inferences

- **This is typically run as a scheduled maintenance job** — the absence of any scheduling logic in this file suggests it is invoked externally (cron, background task, admin CLI).
- **The `location` field in snapshot JSON** is the base directory for a SimpleTable's data — e.g., `acme/sales_data/orders/`.
- **Legacy cleanup (Phase 1)** exists because older product versions may have stored files directly under the SuperTable directory before the current SimpleTable-based layout was adopted.
- **The root `ts` timestamp** is updated on every write/merge operation at the SuperTable level.
- **`snapshot.last_updated_ms`** is set when a SimpleTable's snapshot is created or updated.
- **Monitoring data is NOT cleaned by this code** — `collect_files()` only looks in `data/` and `snapshots/` sub-paths, not `monitoring/`.
- **There is no "keep last N versions" retention policy** — the cleanup is purely timestamp-based. Once a file is older than the threshold and not active, it's deleted.

### Unknown / Not Visible in Provided Snippet

- How `scan_leaf_items()` works internally (Redis SCAN with key parsing, or a dedicated set/index).
- The exact structure of the leaf pointer dict returned by `scan_leaf_items()` (only `simple` and `path` fields are used).
- Whether cleanup is run on a schedule and how frequently.
- Whether there is a dry-run mode (preview what would be deleted without deleting).
- Whether there is a minimum retention period (e.g., keep files for at least 7 days regardless of threshold).
- How large a typical cleanup job is (hundreds vs millions of files).
- Whether the storage backend supports batch delete operations that could improve performance.
- Whether monitoring data has its own cleanup mechanism.
- Whether there is a mechanism to trigger cleanup automatically after merge/compaction operations.

---

## 14. AI Consumption Notes

- **Canonical feature name:** `Supertable HistoryCleaner (Snapshot & Data File Garbage Collection)`
- **Alternative names / aliases:** history cleaner, garbage collection, GC, storage cleanup, stale file cleanup, history pruning
- **Main responsibilities:** (1) Identify stale Parquet and snapshot JSON files via timestamp comparison, (2) Exclude active resources and current snapshots, (3) Delete stale files from storage
- **Important entities:** `HistoryCleaner`, root metadata (`ts`), leaf pointers (`path`, `simple`), snapshot JSON (`resources`, `location`, `last_updated_ms`), timestamp-prefixed files
- **Important workflows:** full cleanup (RBAC → root timestamp → legacy cleanup → per-SimpleTable cleanup → active set exclusion → timestamp filtering → deletion)
- **Integration points:** Redis (`RedisCatalog.get_root()`, `scan_leaf_items()`), object storage (list, read, delete), RBAC (`check_control_access`), `SuperTable`
- **Business purpose keywords:** garbage collection, storage reclamation, stale file cleanup, retention, cost control, maintenance, operational hygiene
- **Architecture keywords:** timestamp-based GC, active-set exclusion, conservative deletion, storage-agnostic, RBAC-protected destructive operation, scan-based discovery
- **Follow-up files for completeness:**
  - `supertable/super_table.py` — SuperTable entity (`super_dir`, storage backend)
  - `supertable/simple_table.py` — SimpleTable snapshot structure (`resources`, `location`, `last_updated_ms`)
  - `supertable/redis_catalog.py` — `scan_leaf_items()` implementation
  - RBAC module — `check_control_access` implementation and CONTROL role definition
  - Any scheduling/cron configuration that invokes cleanup
  - Compaction/merge code that creates new snapshots (the upstream producer of stale files)

---

## 15. Suggested Documentation Tags

`garbage-collection`, `history-cleanup`, `storage-reclamation`, `stale-file-deletion`, `timestamp-based-gc`, `active-set-exclusion`, `snapshot-management`, `parquet-cleanup`, `RBAC-control-access`, `destructive-operation`, `maintenance`, `operational-hygiene`, `storage-cost-control`, `conservative-deletion`, `multi-tenant`, `storage-agnostic`, `legacy-cleanup`

---

## Merge Readiness

- **Suggested canonical name:** `Supertable HistoryCleaner (Snapshot & Data File Garbage Collection)`
- **Standalone or partial:** **Mostly standalone** as a self-contained maintenance utility. Understanding is enhanced by knowing the snapshot structure and merge/compaction lifecycle.
- **Related components to merge later:**
  - `SuperTable` and `SimpleTable` — the entities whose files are cleaned
  - `RedisCatalog` — `scan_leaf_items()` and `get_root()` implementations
  - Merge/compaction code — the upstream operation that creates stale files
  - RBAC module — the `CONTROL` access level definition
  - Any scheduling infrastructure that invokes cleanup
  - Previously analyzed: `MetaReader` (reads the same root/leaf metadata), `Staging & Pipe` (upstream data flow that eventually produces files to clean), `Monitoring` (separate data path, not cleaned here)
- **What would most improve certainty:**
  1. `SimpleTable` snapshot structure (authoritative `resources` and `location` fields)
  2. Merge/compaction code (what creates new snapshots and makes old ones stale)
  3. Scheduling/invocation mechanism for cleanup
  4. Whether monitoring data has its own separate cleanup path
- **Recommended merge key phrases:** `HistoryCleaner`, `history_cleaner`, `garbage collection`, `stale files`, `threshold`, `active resources`, `snapshot cleanup`, `storage reclamation`, `check_control_access`, `scan_leaf_items`
