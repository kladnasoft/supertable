# Clone Architecture — Comprehensive Design

## Table of Contents

1. Current State Inventory
2. Feature 1: Replica Clone (live read-only mirror)
3. Feature 2: GC Clone Protection
4. Feature 3: Version Picker for Clone UI
5. Feature 4: Partial Clone (table subset)
6. Feature 5: Promote / Lock / Unlock lifecycle
7. Feature 6: Clone with RBAC Inheritance
8. Feature 7: Cross-Organization Clone (publish + accept)
9. Feature 8: Detach / Hard Copy
10. Implementation Priority & Effort
11. Summary of File Changes

---

## 1. Current State Inventory

### What exists today

| Capability | Status | Location |
|---|---|---|
| Clone SuperTable (all tables) | ✅ Working | `supertable_manager.clone_supertable()` |
| Clone single table | ✅ Working | `supertable_manager.clone_table()` |
| Read-only clone | ✅ Working | Root flag `read_only: true`, enforced in `access_control._check_readonly_guard()` |
| Writable clone (copy-on-write) | ✅ Working | Same files, new writes go to clone's path |
| Time-travel clone (`at_timestamp`) | ✅ Working | `_resolve_snapshot_for_table()` walks snapshot chain |
| Version-pinned clone (`at_version`) | ✅ Single table only | `clone_table()` accepts `at_version`, `clone_supertable()` does not |
| Clone deletion protection | ✅ SuperTable level | `can_delete_supertable()` blocks if clones reference source |
| Divergence tracking | ✅ Working | `list_supertables_detailed()` computes shared/own files |
| Lock / Unlock (read-only toggle) | ✅ Working | `set_read_only()` + UI buttons |
| Clone UI (SuperTable) | ✅ Working | `supertables.html` modal with type toggle + time-travel |
| Clone UI (single table) | ✅ Working | `supertables.html` modal with version picker |

### What does NOT exist

| Capability | Status |
|---|---|
| Replica clone (auto-sync read-only) | ❌ Not implemented |
| GC protection for shared files | ❌ GC can delete files referenced by clones |
| Partial clone (subset of tables) | ❌ All-or-nothing at SuperTable level |
| RBAC inheritance on clone | ❌ Clone gets empty RBAC |
| Cross-organization clone | ❌ No cross-org data copy |
| Detach / hard copy | ❌ No way to copy shared files to make clone independent |
| Version picker exposed in UI | ⚠️ `at_timestamp` exists in UI but per-table `at_version` is API-only |

### Redis key anatomy

```
supertable:{org}:{sup}:meta:root          → JSON: {version, ts, read_only, cloned_from, clone_type, clone_ts}
supertable:{org}:{sup}:meta:leaf:{table}  → JSON: {path, payload, version, ts}
supertable:{org}:{sup}:lock:leaf:{table}  → Redis lock (NX + TTL)
```

### Zero-copy clone mechanics

When a table is cloned, `_clone_single_table()` creates a new snapshot JSON that references the **same Parquet file paths** as the source. No bytes are copied. The clone's snapshot lives under the clone's storage path, but its `resources[].file` entries point to files under the **source's** storage path. This is what makes it zero-copy — and what makes GC dangerous.

---

## 2. Feature 1: Replica Clone (Live Read-Only Mirror)

### Concept

A replica is a read-only SuperTable that has **no leaf pointers of its own**. When any reader asks for a table in a replica, the catalog transparently redirects to the source's current leaf pointer. The replica always shows the source's latest data — no sync jobs, no cron, no background workers.

### Root meta schema

```json
{
  "read_only": true,
  "cloned_from": "production",
  "clone_type": "replica",
  "clone_ts": 1711734000000,
  "replica_tables": null
}
```

When `clone_type` is `"replica"`, the catalog knows to resolve leaf requests against the source. The `replica_tables` field (null = all tables) enables future partial replicas.

### Catalog changes — `redis_catalog.py`

Add a private resolver method and modify the four leaf-access methods:

```python
def _resolve_replica_source(self, org: str, sup: str) -> Optional[str]:
    """If sup is a replica, return the source super name. Otherwise None."""
    try:
        root = self.get_root(org, sup)
        if root and root.get("clone_type") == "replica":
            source = root.get("cloned_from", "")
            if source and source != sup:
                return source
    except Exception:
        pass
    return None
```

Then modify:

```python
def get_leaf(self, org, sup, simple):
    source = self._resolve_replica_source(org, sup)
    if source:
        return self._get_leaf_raw(org, source, simple)  # read from source
    return self._get_leaf_raw(org, sup, simple)
```

Same pattern for `leaf_exists()`, `scan_leaf_keys()`, `scan_leaf_items()`. The original implementations become `_*_raw()` private methods. The public methods add the one-line resolver check.

**Critical constraint**: No chaining. If source is itself a replica, we do NOT follow — we stop at one level. This prevents cycles and keeps the logic auditable.

### Read path impact

Because `get_leaf()` and `scan_leaf_items()` are the only entry points used by:
- `DataReader` → `DataEstimator._collect_snapshots_from_redis()`
- `MetaReader._get_all_tables()` (uses direct SCAN, needs update)
- `MetaReader.get_table_schema()`, `get_table_stats()`, `get_super_meta()` (all call `get_leaf`)

All readers transparently see the source's data through the replica without any changes to `data_reader.py`, `data_estimator.py`, or the query engine.

**One exception**: `MetaReader._get_all_tables()` does a raw Redis SCAN for leaf keys instead of going through `RedisCatalog.scan_leaf_keys()`. This must be changed to use the catalog method so the replica resolver kicks in.

### Write path impact

`_check_readonly_guard()` already blocks writes when `read_only: true`. Since replicas have `read_only: true`, writes are automatically blocked. No changes needed to `data_writer.py`.

### Creation flow

```python
def clone_supertable(..., clone_type="replica"):
    # 1. Validate source exists
    # 2. Create root with {clone_type: "replica", read_only: true, cloned_from: source}
    # 3. Do NOT clone any leaf pointers (this is the key difference)
    # 4. Initialize RBAC (or inherit — see Feature 6)
    # Done. No snapshot copies, no storage writes.
```

A replica creation is essentially instant — it writes one Redis key (the root) and nothing else.

### API changes

Extend `POST /api/v1/supertables/clone` to accept `clone_type: "replica"` alongside `"readonly"` and `"writable"`.

### UI changes

Add a third option to the clone type toggle in `supertables.html`:

```
[✎ Writable] [🔒 Snapshot] [🔗 Replica]
```

With hint: "Live read-only mirror. Always shows the source's latest data."

### Effort estimate

| Component | Effort |
|---|---|
| `redis_catalog.py` — resolver + 4 method updates | Medium (100 lines) |
| `meta_reader.py` — change `_get_all_tables()` to use catalog | Small (10 lines) |
| `supertable_manager.py` — replica creation path | Small (20 lines) |
| `api/api.py` — accept `clone_type: "replica"` | Small (5 lines) |
| `supertables.html` — third clone type option + badge | Small (30 lines) |
| `sidebar.html` — replica icon in workspace dropdown | Small (5 lines) |
| **Total** | **~170 lines, 6 files** |

---

## 3. Feature 2: GC Clone Protection

### Problem

`preview_obsolete_files()` in `gc.py` compares files in the table's `data/` directory against the current snapshot's `resources[]`. Files not in the manifest are marked orphaned. But clones reference the **source's** files — if the source runs GC, it deletes files that the clone's snapshot still points to.

### Solution

Before marking a file as orphaned, check if any clone still references it. We already have `_find_clones()` in `supertable_manager.py` that scans all roots for `cloned_from`. Extend it to collect file references.

### Implementation

```python
def _collect_clone_referenced_files(org: str, source_sup: str, simple_name: str) -> Set[str]:
    """Collect all parquet file paths referenced by clones of this source table."""
    catalog = RedisCatalog()
    storage = get_storage()
    clones = _find_clones(catalog, org, source_sup)
    referenced: Set[str] = set()

    for clone_name in clones:
        root = catalog.get_root(org, clone_name)
        if not root:
            continue

        # For replicas: they have no leaf pointers, they read from source.
        # So replica clones don't hold independent file references — skip.
        if root.get("clone_type") == "replica":
            continue

        # For readonly/writable clones: read their snapshot to get file refs
        leaf = catalog._get_leaf_raw(org, clone_name, simple_name)
        if not leaf:
            continue
        payload = leaf.get("payload") if isinstance(leaf, dict) else None
        if not isinstance(payload, dict):
            path = leaf.get("path", "")
            if path:
                try:
                    payload = storage.read_json(path)
                except Exception:
                    continue
        if isinstance(payload, dict):
            for res in payload.get("resources", []):
                f = res.get("file", "")
                if f:
                    referenced.add(f)

    return referenced
```

Then in `preview_obsolete_files()`:

```python
# After building live_files set from current snapshot...
clone_files = _collect_clone_referenced_files(organization, super_name, simple_name)
protected_files = live_files | clone_files

for file_path in storage_files:
    if file_path not in protected_files:
        result["orphaned_data_files"].append(file_path)
```

### Effort estimate

| Component | Effort |
|---|---|
| `services/gc.py` — add `_collect_clone_referenced_files()` + integrate into preview + clean | Medium (60 lines) |
| `services/supertable_manager.py` — export `_find_clones()` or move to shared util | Small (5 lines) |
| **Total** | **~65 lines, 2 files** |

---

## 4. Feature 3: Version Picker for Clone UI

### Current state

The SuperTable clone modal already has a `datetime-local` input for `at_timestamp`. The single-table clone modal has a `number` input for `at_version`. Both work. What's missing is a version **browser** — users need to see available versions before picking one.

### Solution

Add a "Browse versions" button next to the time-travel input that fetches snapshot history via the existing `GET /api/v1/platform/time-travel/versions` endpoint (or the `list_snapshot_versions()` function) and shows them in a dropdown.

### For SuperTable clone

Since `at_timestamp` is the only meaningful parameter at the SuperTable level (each table has its own version counter), show a timeline of the SuperTable's root version bumps. Add a new lightweight endpoint:

```
GET /api/v1/supertables/version-history?organization=X&super_name=Y&limit=50
```

Returns: `[{version, ts, table_count}, ...]` — root version bumps with timestamps. The user picks a timestamp, which feeds into `at_timestamp`.

### For single-table clone

Already has `at_version` in the UI. Add a "Browse" button that calls the existing time-travel API and populates a `<select>` with version entries showing version number, timestamp, row count, and file count.

### Effort estimate

| Component | Effort |
|---|---|
| `api/api.py` — new `GET /api/v1/supertables/version-history` endpoint | Small (30 lines) |
| `supertables.html` — version browser dropdown for both modals | Medium (80 lines JS + HTML) |
| **Total** | **~110 lines, 2 files** |

---

## 5. Feature 4: Partial Clone (Table Subset)

### Concept

When cloning a SuperTable, allow selecting specific tables instead of all. This applies to all clone types: snapshot (read-only), writable, and replica.

### API changes

Extend `POST /api/v1/supertables/clone` to accept an optional `tables: [str]` parameter. If null/empty, clone all tables (current behavior). If specified, clone only those tables.

### Implementation

In `clone_supertable()`:

```python
def clone_supertable(
    organization, source_name, target_name,
    read_only=False, at_timestamp=None, tables=None,
):
    # ... existing validation ...

    if tables:
        # Validate all requested tables exist
        for t in tables:
            if not catalog.leaf_exists(organization, source_name, t):
                raise FileNotFoundError(f"Table '{t}' not found in '{source_name}'")
        leaf_items = [
            item for item in catalog.scan_leaf_items(organization, source_name)
            if item.get("simple") in tables
        ]
    else:
        leaf_items = list(catalog.scan_leaf_items(organization, source_name))

    for leaf_item in leaf_items:
        # ... existing clone logic ...
```

For replicas, store the table subset in root meta:

```json
{
  "clone_type": "replica",
  "replica_tables": ["orders", "products", "customers"]
}
```

The catalog resolver for replicas filters on this list:

```python
def scan_leaf_keys(self, org, sup, count=1000):
    source = self._resolve_replica_source(org, sup)
    if source:
        root = self.get_root(org, sup)
        allowed = root.get("replica_tables")  # null = all
        for key in self._scan_leaf_keys_raw(org, source, count):
            if allowed is None or key.rsplit(":", 1)[-1] in allowed:
                yield key
        return
    yield from self._scan_leaf_keys_raw(org, sup, count)
```

### UI changes

Add a multi-select table picker to the clone modal. Fetch the table list from the source SuperTable when the source is selected. Default: all selected.

### Effort estimate

| Component | Effort |
|---|---|
| `supertable_manager.py` — `tables` filter param | Small (15 lines) |
| `redis_catalog.py` — `replica_tables` filter in resolver | Small (10 lines) |
| `api/api.py` — accept `tables` param in clone endpoint | Small (5 lines) |
| `supertables.html` — multi-select table picker in clone modal | Medium (60 lines) |
| **Total** | **~90 lines, 4 files** |

---

## 6. Feature 5: Promote / Lock / Unlock Lifecycle

### Current state

Lock (`set_read_only(org, sup, true)`) and Unlock (`set_read_only(org, sup, false)`) already work. The UI has Lock/Unlock buttons on each SuperTable card.

### Your observation is correct

"Promote replica to writable" is conceptually the same as Unlock — it flips `read_only: false`. But for a replica, there's an additional step: we need to **materialize** the leaf pointers. A replica has no leaves of its own — it reads from the source. If we just flip `read_only: false`, writes would try to update non-existent leaves.

### Solution

When unlocking a replica (or any transition from `clone_type: "replica"` to another type):

1. Read all leaf pointers from the source via the resolver
2. Copy them into the clone's own leaf namespace (materialize)
3. Update root: `clone_type: "writable"`, `read_only: false`

This is a "snapshot-then-promote" operation — it freezes the replica at the source's current state, then makes it writable.

### Implementation

```python
def promote_replica(organization: str, super_name: str) -> Dict[str, Any]:
    """Convert a replica into a writable clone (materializes leaf pointers)."""
    catalog = RedisCatalog()
    storage = get_storage()
    root = catalog.get_root(organization, super_name)
    if not root or root.get("clone_type") != "replica":
        raise ValueError("Not a replica")

    source = root.get("cloned_from", "")
    if not source:
        raise ValueError("Replica has no source reference")

    # Materialize: copy source leaves into clone
    tables_materialized = 0
    for leaf_item in catalog._scan_leaf_items_raw(organization, source):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue
        snapshot = _get_snapshot_from_leaf(leaf_item, storage)
        if snapshot:
            _clone_single_table(
                catalog, storage, organization, source, super_name,
                simple_name, snapshot, _now_ms(),
            )
            tables_materialized += 1

    # Update root
    catalog.update_root_flags(organization, super_name, {
        "clone_type": "writable",
        "read_only": False,
        "promoted_from_replica_ms": _now_ms(),
    })

    return {"ok": True, "tables_materialized": tables_materialized}
```

### UI changes

Add a "Promote" button on replica cards (instead of plain Unlock). Show a confirmation dialog explaining that this freezes the current state and makes it writable.

### Effort estimate

| Component | Effort |
|---|---|
| `supertable_manager.py` — `promote_replica()` | Medium (40 lines) |
| `api/api.py` — new endpoint or extend set_read_only | Small (15 lines) |
| `supertables.html` — promote button + confirmation | Small (20 lines) |
| **Total** | **~75 lines, 3 files** |

---

## 7. Feature 6: Clone with RBAC Inheritance

### Current state

`_init_target_rbac()` creates fresh empty RBAC managers (RoleManager + UserManager) for the clone. No roles or users are copied.

### Design options

**Option A: Copy all roles + all users** — Simple, but may copy more access than intended. A production SuperTable might have 20 roles; the dev clone needs 3.

**Option B: Copy all roles, no users** — The role definitions (column filters, row filters, table permissions) are copied, but no users are assigned. The admin then assigns users manually. This is safer — roles define the security policy, users define who gets access. You want the same policy but may not want the same people.

**Option C: Selective — let the user choose** — UI shows a checklist of roles from the source. Checked roles get copied. Users can optionally be included.

### Recommendation

**Option C** for the API, **Option B as the default** for backward compatibility. The API accepts:

```json
{
  "inherit_rbac": true,
  "inherit_roles": ["analyst", "data_engineer"],
  "inherit_users": false
}
```

If `inherit_rbac: true` and `inherit_roles` is null/empty, copy all roles. If `inherit_users: true`, copy user-role assignments too.

### Implementation

```python
def _clone_rbac(organization, source_sup, target_sup, role_filter=None, include_users=False):
    """Copy RBAC roles (and optionally users) from source to target."""
    from supertable.rbac.role_manager import RoleManager
    from supertable.rbac.user_manager import UserManager

    src_rm = RoleManager(super_name=source_sup, organization=organization)
    tgt_rm = RoleManager(super_name=target_sup, organization=organization)

    roles = src_rm.list_roles()
    for role in roles:
        name = role.get("role_name", "")
        if role_filter and name not in role_filter:
            continue
        # Deep copy role definition to target
        tgt_rm.create_role(role)

    if include_users:
        src_um = UserManager(super_name=source_sup, organization=organization)
        tgt_um = UserManager(super_name=target_sup, organization=organization)
        users = src_um.list_users()
        for user in users:
            tgt_um.create_user(user)
```

### Effort estimate

| Component | Effort |
|---|---|
| `supertable_manager.py` — `_clone_rbac()` + wire into clone flow | Medium (50 lines) |
| `api/api.py` — accept `inherit_rbac`, `inherit_roles`, `inherit_users` | Small (10 lines) |
| `supertables.html` — RBAC checklist in clone modal | Medium (50 lines) |
| **Total** | **~110 lines, 3 files** |

---

## 8. Feature 7: Cross-Organization Clone (Publish + Accept)

### Concept

Organization A publishes a SuperTable. Organization B accepts it, triggering a hard copy of all Parquet files into B's storage namespace. After acceptance, B owns the data independently.

This is different from data sharing (which is live, presigned-URL-based, no-copy). Cross-org clone is a one-time data transfer that creates an independent copy.

### Protocol

**Step 1: Publish** — Org A creates a "publication" (similar to a share, but for cloning):

```
POST /api/v1/publications
{
    "organization": "org-a",
    "super_name": "production",
    "tables": ["orders", "customers"],  // optional subset
    "label": "Q1 2025 dataset"
}
```

Returns: `publication_id` + `bearer_token` + `manifest_url` (reuses the existing manifest infrastructure from sharing)

**Step 2: Accept** — Org B registers the publication and triggers a hard copy:

```
POST /api/v1/publications/accept
{
    "organization": "org-b",
    "target_super_name": "imported-q1",
    "manifest_url": "https://org-a.example.com/api/v1/publications/pub_abc/manifest",
    "bearer_token": "..."
}
```

The accept flow:
1. Fetch manifest (gets presigned URLs for all Parquet files)
2. For each file, download via presigned URL → write to org-b's storage under `org-b/imported-q1/tables/{table}/data/`
3. Create snapshot JSONs pointing to the new local paths
4. Set leaf pointers
5. Mark root with `{"imported_from": "org-a", "import_ts": ..., "read_only": true}`

### Relationship to existing sharing

This reuses the manifest infrastructure (`get_share_manifest()` returns presigned URLs). The difference is that sharing gives consumers live query access via presigned URLs, while cross-org clone does a one-time physical copy.

### Effort estimate

| Component | Effort |
|---|---|
| `services/publication.py` — new module, publish + accept logic | Large (200 lines) |
| `api/api.py` — new endpoints (publish, accept, list publications) | Medium (60 lines) |
| `supertables.html` — publish button + accept modal | Medium (80 lines) |
| **Total** | **~340 lines, 3 new + 1 modified** |

---

## 9. Feature 8: Detach / Hard Copy

### Concept

A clone (snapshot or writable) shares Parquet files with its source. Detach makes the clone independent by copying the shared files into the clone's own storage path, then updating the snapshot references.

### When to use

- Before deleting the source SuperTable
- To remove the GC protection constraint
- To make a writable clone fully independent after it has partially diverged
- After cross-org clone when you want to sever the connection

### Implementation

```python
def detach_clone(organization: str, super_name: str) -> Dict[str, Any]:
    """Copy all shared files to the clone's own storage, making it independent."""
    catalog = RedisCatalog()
    storage = get_storage()

    root = catalog.get_root(organization, super_name)
    if not root or not root.get("cloned_from"):
        raise ValueError("Not a clone")

    source = root["cloned_from"]
    source_prefix = f"{organization}/{source}/"
    clone_prefix = f"{organization}/{super_name}/"
    files_copied = 0
    bytes_copied = 0

    for leaf_item in catalog.scan_leaf_items(organization, super_name):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue

        # Read snapshot
        snapshot = _get_snapshot_from_leaf(leaf_item, storage)
        if not snapshot:
            continue

        updated_resources = []
        for res in snapshot.get("resources", []):
            file_path = res.get("file", "")
            if not file_path:
                updated_resources.append(res)
                continue

            # Is this a shared file (lives under source's path)?
            if file_path.startswith(source_prefix):
                # Copy to clone's own path
                new_path = file_path.replace(source_prefix, clone_prefix, 1)
                data = storage.read_bytes(file_path)
                storage.write_bytes(new_path, data)
                files_copied += 1
                bytes_copied += len(data)
                res = dict(res)
                res["file"] = new_path

            updated_resources.append(res)

        # Update snapshot with new file paths
        snapshot["resources"] = updated_resources
        snap_path = leaf_item.get("path", "")
        if snap_path:
            storage.write_json(snap_path, snapshot)
            catalog.set_leaf_payload_cas(
                organization, super_name, simple_name,
                snapshot, snap_path, now_ms=_now_ms(),
            )

    # Remove cloned_from reference
    catalog.update_root_flags(organization, super_name, {
        "cloned_from": None,
        "clone_type": None,
        "clone_ts": None,
        "detached_ms": _now_ms(),
        "detach_files_copied": files_copied,
        "detach_bytes_copied": bytes_copied,
    })

    return {
        "ok": True,
        "files_copied": files_copied,
        "bytes_copied": bytes_copied,
    }
```

### For replicas

A replica has no leaves — detaching a replica is equivalent to: materialize (copy leaf pointers from source → create snapshot JSONs with hard-copied files) → remove `cloned_from`. This combines promote + detach in one operation.

### UI changes

Add a "Detach" button on clone cards. Show a confirmation with estimated size (files × avg size). Show progress during copy.

### Effort estimate

| Component | Effort |
|---|---|
| `supertable_manager.py` — `detach_clone()` | Medium (80 lines) |
| `api/api.py` — new `POST /api/v1/supertables/detach` endpoint | Small (20 lines) |
| `supertables.html` — detach button + confirmation + progress | Medium (50 lines) |
| **Total** | **~150 lines, 3 files** |

---

## 10. Implementation Priority & Effort

### Priority matrix

| # | Feature | Risk if deferred | Effort | Dependencies | Priority |
|---|---|---|---|---|---|
| 2 | GC clone protection | **Critical** — data loss | Small (65 lines) | None | **P0 — Do first** |
| 1 | Replica clone | High — most requested | Medium (170 lines) | None | **P1** |
| 5 | Promote lifecycle | Medium — needed for replicas | Small (75 lines) | Feature 1 | **P1** |
| 3 | Version picker UI | Low — API exists | Small (110 lines) | None | **P2** |
| 8 | Detach / hard copy | Medium — enables independence | Medium (150 lines) | None | **P2** |
| 4 | Partial clone | Low — nice to have | Small (90 lines) | Feature 1 | **P3** |
| 6 | RBAC inheritance | Low — manual workaround | Medium (110 lines) | None | **P3** |
| 7 | Cross-org clone | Low — sharing covers most cases | Large (340 lines) | Feature 8 | **P4** |

### Recommended implementation order

```
Phase 1 (Safety):     GC clone protection
Phase 2 (Core):       Replica clone → Promote lifecycle
Phase 3 (UX):         Version picker → Detach
Phase 4 (Extensions): Partial clone → RBAC inheritance
Phase 5 (Advanced):   Cross-org clone
```

### Total effort estimate

| Phase | Lines | Files | Duration estimate |
|---|---|---|---|
| Phase 1 | ~65 | 2 | Half day |
| Phase 2 | ~245 | 9 | 2 days |
| Phase 3 | ~260 | 5 | 1.5 days |
| Phase 4 | ~200 | 7 | 1 day |
| Phase 5 | ~340 | 4 | 2 days |
| **Total** | **~1,110** | **~15 unique files** | **~7 days** |

---

## 11. Summary of File Changes

### `supertable/redis_catalog.py`
- Add `_resolve_replica_source()` method
- Rename `get_leaf` → `_get_leaf_raw`, wrap with resolver
- Same for `leaf_exists`, `scan_leaf_keys`, `scan_leaf_items`
- Add `replica_tables` filter to `scan_leaf_keys` and `scan_leaf_items`

### `supertable/meta_reader.py`
- Change `_get_all_tables()` to use `catalog.scan_leaf_keys()` instead of raw Redis SCAN

### `supertable/services/supertable_manager.py`
- Add `promote_replica()` — materialize leaves + flip flags
- Add `detach_clone()` — hard copy shared files + update snapshots
- Add `_clone_rbac()` — copy roles/users from source
- Extend `clone_supertable()` — `tables` filter, `clone_type: "replica"` path
- Export `_find_clones()` for GC use

### `supertable/services/gc.py`
- Add `_collect_clone_referenced_files()` — scan all clone snapshots
- Integrate into `preview_obsolete_files()` and `clean_obsolete_files()`

### `supertable/services/publication.py` (new)
- `create_publication()`, `get_publication_manifest()`, `accept_publication()`

### `supertable/api/api.py`
- Extend `POST /api/v1/supertables/clone` — `clone_type: "replica"`, `tables: [str]`, RBAC params
- New `POST /api/v1/supertables/promote` — replica → writable
- New `POST /api/v1/supertables/detach` — hard copy + independence
- New `GET /api/v1/supertables/version-history` — root version timeline
- New publication endpoints (Phase 5)

### `supertable/webui/templates/supertables.html`
- Third clone type option (Replica) in clone modal
- Version browser dropdown
- Promote button for replicas
- Detach button for clones
- RBAC inheritance checklist
- Table subset picker
- Replica badge

### `supertable/webui/templates/sidebar.html`
- Replica icon (🔗) in workspace dropdown alongside read-only lock (🔒)

### `supertable/rbac/access_control.py`
- No changes needed — `_check_readonly_guard()` already handles replicas via `read_only: true`

### `supertable/data_reader.py`
- No changes needed — reads go through `RedisCatalog.get_leaf()` which handles the resolver

### `supertable/data_writer.py`
- No changes needed — write protection via `_check_readonly_guard()` in RBAC layer

### `supertable/mcp/mcp_server.py`
- No changes needed — MCP tools use `MetaReader` and `DataReader` which go through the catalog

### `supertable/audit/events.py`
- Add new actions: `SUPERTABLE_CLONE_REPLICA`, `SUPERTABLE_PROMOTE`, `SUPERTABLE_DETACH`, `PUBLICATION_CREATE`, `PUBLICATION_ACCEPT`
