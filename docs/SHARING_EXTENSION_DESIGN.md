# Data Sharing — Extension Design

## Current State Analysis

### What exists

| Component | Status | Notes |
|---|---|---|
| **Provider: create share** | ✅ Works | Generates share_id + bearer token, stores in Redis |
| **Provider: manifest endpoint** | ✅ Works | Returns presigned URLs for Parquet files |
| **Provider: revoke share** | ✅ Works | Deletes share doc from Redis |
| **Consumer: link share** | ✅ Works | Fetches manifest, caches in Redis |
| **Consumer: refresh manifest** | ✅ Works | Lazy refresh before presigned URL expiry |
| **Consumer: get_shared_table_files()** | ✅ Exists | Returns presigned URLs for a shared table |
| **Consumer: query shared tables** | ❌ **NOT IMPLEMENTED** | Query engine has no linked-share awareness |
| **UI: provider create/revoke** | ✅ Works | sharing.html provider tab |
| **UI: consumer link/unlink/refresh** | ✅ Works | sharing.html consumer tab |
| **UI: consumer query** | ❌ Impossible | No engine integration |

### Critical gap

The **query engine cannot read linked shares**. `DataEstimator._collect_snapshots_from_redis()` only scans local leaf pointers (`supertable:{org}:{sup}:meta:leaf:*`). It has no concept of linked shares. The `get_shared_table_files()` function in `sharing.py` returns presigned URLs ready for DuckDB httpfs, but nothing in the read path calls it.

This means consumers can link a share and see it in the UI, but they **cannot `SELECT * FROM shared_table`**. The sharing feature is essentially a protocol without the last mile.

### How DuckDB already handles presigned URLs

`DataEstimator._to_duckdb_path()` already converts storage keys to DuckDB-readable paths — including presigned URLs. If a `resources[].file` entry is already a URL (contains `://`), it's passed through directly (line 219-221). DuckDB reads it via httpfs. This means **if we inject presigned URLs into the `SuperSnapshot` object, the query engine will read them natively**. No engine changes needed.

### Relationship to new clone system

The clone architecture we just built provides three patterns:

| Pattern | Data location | Freshness | Storage cost |
|---|---|---|---|
| **Sharing** (linked share) | Provider's storage | Real-time via presigned URLs | Zero (consumer side) |
| **Replica clone** | Source's leaf pointers | Real-time via resolver | Zero |
| **Publication** (cross-org clone) | Consumer's storage (hard copy) | Frozen at accept time | Full copy |

Sharing is the cross-org equivalent of replica clones — live access, zero copy. Publications are the cross-org equivalent of snapshot/detach — full independence.

---

## Extension 1: Make Linked Shares Queryable

**Priority: CRITICAL — this completes the sharing feature.**

### Problem

`DataEstimator._collect_snapshots_from_redis()` only reads local leaves. Linked shares store their data as cached manifests in Redis, not as leaf pointers.

### Solution

When building the `Reflection` for a query, check if any requested tables match a linked share. If so, inject the shared table's presigned URLs into the `SuperSnapshot` alongside local tables.

### Implementation

**Option A — Virtual leaf injection (cleanest)**

When a linked share is registered, create virtual leaf pointers in the consumer's namespace. The leaf payload's `resources[].file` entries contain the presigned URLs from the manifest. When the manifest is refreshed, the leaf pointers are updated with fresh URLs.

```python
# In sharing.py, after link_share() caches the manifest:
def _materialize_linked_share_leaves(org, sup, link_id, manifest):
    """Create virtual leaf pointers from a linked share's manifest.
    
    These leaves look identical to local leaves but their resources
    contain presigned URLs instead of local storage paths.
    """
    catalog = RedisCatalog()
    alias = doc.get("alias_prefix", "")
    
    for tbl in manifest.get("tables", []):
        table_name = tbl.get("table", "")
        if not table_name:
            continue
        local_name = f"{alias}{table_name}" if alias else table_name
        
        snapshot = {
            "simple_name": local_name,
            "snapshot_version": tbl.get("snapshot_version", 0),
            "last_updated_ms": tbl.get("last_updated_ms", 0),
            "schema": tbl.get("schema", []),
            "resources": tbl.get("resources", []),  # presigned URLs
            "_linked_share": link_id,  # marker for identification
        }
        
        catalog.set_leaf_payload_cas(
            org, sup, local_name, snapshot,
            path=f"__linked_share__/{link_id}/{table_name}",
        )
```

**Why this is the cleanest approach:**
- Zero changes to `DataEstimator`, `DataReader`, or the query engine
- The presigned URLs flow through `_to_duckdb_path()` as-is (the `://` check at line 219 passes them through)
- `MetaReader._get_all_tables()` automatically picks up linked-share tables
- RBAC works normally — the admin can create roles that reference shared table names
- The alias_prefix feature naturally works (linked tables appear as `prefix_orders`, etc.)

**On manifest refresh:** When `refresh_linked_share()` runs, it also calls `_materialize_linked_share_leaves()` with the fresh manifest. This updates the presigned URLs in the leaf payloads.

**On unlink:** Delete the virtual leaves alongside the linked share doc.

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `_materialize_linked_share_leaves()`, call from `link_share()` and `refresh_linked_share()`. Add leaf cleanup to `unlink_share()`. | Medium (60 lines) |
| `redis_catalog.py` | No changes — `set_leaf_payload_cas` already works for this | None |
| `data_estimator.py` | No changes — presigned URLs pass through `_to_duckdb_path()` | None |
| `data_reader.py` | No changes — reads through catalog which already handles leaves | None |

**Effort: ~60 lines in 1 file. Zero engine changes.**

### Risk: Presigned URL expiry during long queries

If a query takes longer than the presigned URL TTL (default 4h), DuckDB file reads will fail mid-query. Mitigation: the refresh buffer (10 min before expiry) should trigger a refresh before any query starts. For extra safety, the estimator could check URL expiry from the manifest metadata and trigger a refresh if close to expiry.

---

## Extension 2: Share-to-Clone Conversion (Materialize)

**Priority: HIGH — bridges live sharing and independence.**

### Concept

A "Materialize" button on a linked share downloads all shared data into local storage, creating a standalone read-only clone. This is like `accept_publication()` but sourced from an already-linked share's cached manifest instead of a remote URL.

### When to use

- Consumer wants to keep a snapshot of shared data before the provider revokes
- Consumer needs offline access (no dependency on provider's presigned URLs)
- Consumer wants to run heavy analytics without hitting provider's presigned URLs repeatedly

### Implementation

```python
def materialize_linked_share(
    organization: str,
    super_name: str,
    link_id: str,
    target_super_name: str,
) -> Dict[str, Any]:
    """Download all data from a linked share into a new local SuperTable."""
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        raise FileNotFoundError(f"Linked share '{link_id}' not found")
    
    # Force-refresh manifest to get fresh presigned URLs
    refresh_linked_share(organization, super_name, link_id, force=True)
    doc = catalog.get_linked_share(organization, super_name, link_id)
    manifest = doc.get("cached_manifest", {})
    
    # Reuse accept_publication logic with the cached manifest
    from supertable.services.publication import _download_manifest_tables
    return _download_manifest_tables(
        organization, target_super_name, manifest, source_label="linked_share",
    )
```

This reuses the `accept_publication()` download logic. We'd extract the file download + snapshot creation into a shared helper (`_download_manifest_tables()`) used by both `accept_publication()` and `materialize_linked_share()`.

### UI

Add a "Materialize" button on each linked share card in `sharing.html`. Shows a dialog asking for the target SuperTable name, then triggers the download with a progress indicator.

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `materialize_linked_share()` | Small (20 lines) |
| `services/publication.py` | Extract `_download_manifest_tables()` helper | Small (refactor, ~15 lines) |
| `api/api.py` | New `POST /api/v1/linked-shares/{link_id}/materialize` | Small (25 lines) |
| `sharing.html` | Materialize button + target name dialog | Small (30 lines) |
| `audit/events.py` | Add `SHARE_MATERIALIZE` action | Trivial (1 line) |

**Effort: ~90 lines across 5 files.**

---

## Extension 3: Share with Column/Row Filtering

**Priority: MEDIUM — security enhancement.**

### Concept

When creating a share, the provider can specify column filters (only share certain columns) and row filters (SQL WHERE predicate). These filters are applied at manifest generation time — the consumer never sees the filtered data.

### How it works

Currently, `get_share_manifest()` reads the full snapshot and generates presigned URLs for every file and every column. With filtering:

**Column filtering:** The manifest's `schema` array is filtered to only include allowed columns. At query time, DuckDB only reads those columns from the Parquet files (Parquet is columnar — unreferenced columns are never read).

**Row filtering:** This is harder. Parquet files are immutable — we can't filter rows without rewriting the file. Two approaches:

**(A) Lazy filtering (recommended):** Include all files in the manifest but add a `filter` field:

```json
{
  "table": "orders",
  "schema": [{"name": "id"}, {"name": "amount"}],  // filtered columns
  "filter": "region = 'EU' AND amount > 100",
  "resources": [...]
}
```

The consumer's query engine adds the filter as a WHERE clause. This requires the consumer's engine to understand and apply the filter — which it already does for RBAC (`RbacViewDef.where_clause`).

When materializing virtual leaves (Extension 1), we'd store the filter in the leaf payload and the data reader would apply it via the existing RBAC view machinery.

**(B) Eager filtering:** At manifest generation time, read the Parquet files, filter rows, write filtered files to a temporary location, and serve presigned URLs to those filtered files. More secure (consumer can't bypass the filter) but expensive (provider does I/O on every manifest request).

### Recommendation

Column filtering is easy and should be done. Row filtering via approach (A) is practical and reuses existing RBAC machinery. Approach (B) is overkill for most use cases.

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `columns` and `filter` params to `create_share()`. Apply column filter in `get_share_manifest()`. Store filter in share doc. | Medium (40 lines) |
| `services/sharing.py` | In `_materialize_linked_share_leaves()`, read filter from manifest and store as `where_clause` in leaf metadata | Small (10 lines) |
| `api/api.py` | Accept `columns` and `filter` in share creation endpoint | Small (5 lines) |
| `sharing.html` | Column picker + filter input in create share modal | Medium (50 lines) |

**Effort: ~105 lines across 3 files.**

---

## Extension 4: Share Health Dashboard

**Priority: LOW — operational visibility.**

### Concept

Both provider and consumer need visibility into share health:

**Provider wants to know:**
- Which shares are being actively consumed (manifest access frequency)
- When was each share's manifest last fetched
- Which consumers are accessing (from audit logs)

**Consumer wants to know:**
- Is the share still valid (provider hasn't revoked)
- How fresh is the cached data (last refresh timestamp)
- Are presigned URLs close to expiry
- How much data is shared (table count, row count, file count)

### Implementation

Most of this data already exists:
- `SHARE_MANIFEST_ACCESS` audit events track every manifest fetch with timestamp
- Linked share docs store `last_refreshed_ms`, `expires_ms`, `cached_total_rows`
- Share docs store `created_ms`, `enabled`, table list

We just need to aggregate and surface it.

### Provider dashboard

New endpoint: `GET /api/v1/shares/dashboard` returns enriched share list with access stats from the audit stream.

```json
{
  "shares": [
    {
      "share_id": "sh_abc123",
      "grantee_org": "partner",
      "tables": ["orders", "products"],
      "created_ms": 1711734000000,
      "last_accessed_ms": 1711820000000,
      "access_count_7d": 42,
      "status": "active"
    }
  ]
}
```

### Consumer dashboard

Enriched linked-share list already returns `last_refreshed_ms`, `expires_ms`, `cached_total_rows`. Add:
- `presign_expires_in_s`: seconds until presigned URLs expire
- `status`: "healthy" | "expiring_soon" | "expired" | "refresh_failed"
- `latency_ms`: last manifest fetch duration

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `get_share_dashboard()` with audit log aggregation | Medium (40 lines) |
| `api/api.py` | New `GET /api/v1/shares/dashboard` endpoint | Small (15 lines) |
| `sharing.html` | Health badges on share cards (status color, access count) | Small (30 lines) |

**Effort: ~85 lines across 3 files.**

---

## Extension 5: Share Versioning (Point-in-Time Shares)

**Priority: LOW — advanced use case.**

### Concept

Currently, a share always serves the latest snapshot. A point-in-time share would serve data as it was at a specific timestamp, using the time_travel service.

### Use case

Regulatory data exchange: "Share the orders table as it was on December 31, 2024" — guaranteed immutable, auditable.

### Implementation

Add optional `at_timestamp` to `create_share()`. When set, `get_share_manifest()` uses `_resolve_snapshot_for_table()` (from supertable_manager) instead of the current leaf pointer.

Since the snapshot is pinned, presigned URLs always point to the same files. No refresh needed — URLs just need to be re-presigned when they expire.

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `at_timestamp` to `create_share()` and `get_share_manifest()` | Small (20 lines) |
| `api/api.py` | Accept `at_timestamp` in share creation | Trivial (3 lines) |
| `sharing.html` | Add datetime picker to create share modal | Small (15 lines) |

**Effort: ~38 lines across 3 files.**

---

## Extension 6: Auto-Refresh Subscription

**Priority: LOW — convenience.**

### Concept

Currently, the consumer must manually refresh linked shares (or the lazy refresh triggers 10 min before expiry). An auto-refresh subscription would proactively refresh all linked shares on a schedule.

### Implementation

This doesn't need a background job. Instead, add a middleware or startup hook that checks all linked shares on every page load (or API request) and triggers lazy refresh for any that are within the buffer window.

A lighter approach: the sidebar's workspace loading already runs on every page load. Add a `refresh_expiring_shares()` call there that checks all linked shares for the current SuperTable and refreshes any that are close to expiry.

### Changes needed

| File | Change | Effort |
|---|---|---|
| `services/sharing.py` | Add `refresh_expiring_shares(org, sup)` — batch check + refresh | Small (25 lines) |
| `sidebar.html` | Call refresh on workspace load (fire-and-forget) | Small (10 lines) |

**Effort: ~35 lines across 2 files.**

---

## Priority Matrix

| # | Extension | Impact | Effort | Priority |
|---|---|---|---|---|
| 1 | **Make linked shares queryable** | **Critical** — completes the feature | Small (60 lines) | **P0** |
| 2 | **Share-to-clone conversion** | High — bridges sharing + clone | Small (90 lines) | **P1** |
| 3 | **Column/row filtering** | Medium — security | Medium (105 lines) | **P2** |
| 5 | **Point-in-time shares** | Medium — regulatory | Small (38 lines) | **P2** |
| 4 | **Health dashboard** | Low — visibility | Medium (85 lines) | **P3** |
| 6 | **Auto-refresh** | Low — convenience | Small (35 lines) | **P3** |

### Recommended implementation order

```
Phase A (critical):    Make linked shares queryable (virtual leaf injection)
Phase B (high value):  Share-to-clone conversion (materialize)
Phase C (security):    Column/row filtering + point-in-time shares
Phase D (polish):      Health dashboard + auto-refresh
```

### Total effort

| Phase | Lines | Files | Duration |
|---|---|---|---|
| A | ~60 | 1 | Half day |
| B | ~90 | 5 | Half day |
| C | ~143 | 4 | 1 day |
| D | ~120 | 4 | Half day |
| **Total** | **~413** | **~8 unique files** | **~2.5 days** |

---

## Summary of File Changes

### `supertable/services/sharing.py`
- Add `_materialize_linked_share_leaves()` — create virtual leaves from manifest (Phase A)
- Wire into `link_share()` and `refresh_linked_share()` (Phase A)
- Add leaf cleanup to `unlink_share()` (Phase A)
- Add `materialize_linked_share()` — download to local clone (Phase B)
- Add `columns` and `filter` params to `create_share()` (Phase C)
- Apply column filter in `get_share_manifest()` (Phase C)
- Add `at_timestamp` support (Phase C)
- Add `get_share_dashboard()` with audit aggregation (Phase D)
- Add `refresh_expiring_shares()` batch refresh (Phase D)

### `supertable/services/publication.py`
- Extract `_download_manifest_tables()` helper for reuse (Phase B)

### `supertable/api/api.py`
- New `POST /api/v1/linked-shares/{link_id}/materialize` (Phase B)
- Accept `columns`, `filter`, `at_timestamp` in share creation (Phase C)
- New `GET /api/v1/shares/dashboard` (Phase D)

### `supertable/audit/events.py`
- Add `SHARE_MATERIALIZE` (Phase B)

### `supertable/webui/templates/sharing.html`
- Materialize button on linked share cards (Phase B)
- Column picker + filter input in create share modal (Phase C)
- Health badges on share cards (Phase D)

### `supertable/webui/templates/sidebar.html`
- Auto-refresh on workspace load (Phase D)

### No changes needed
- `data_estimator.py` — presigned URLs already pass through `_to_duckdb_path()`
- `data_reader.py` — reads through catalog, no direct leaf access
- `redis_catalog.py` — `set_leaf_payload_cas` handles virtual leaves
- `meta_reader.py` — uses `scan_leaf_keys()` which picks up virtual leaves
