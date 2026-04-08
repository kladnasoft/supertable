# route: supertable.services.gc
"""
Garbage collection for orphaned Parquet data files.

When a write with overwrite_columns replaces data, the old Parquet files
are removed from the snapshot manifest but NOT deleted from storage.
Over time these orphaned files accumulate and waste storage.

This module compares files on storage (in the table's ``data/`` dir)
against the current snapshot's ``resources[]`` manifest.  Files present
on storage but absent from the manifest are orphaned and can be safely
deleted.

Safety guarantees:
  - A per-table Redis lock is acquired before reading the snapshot,
    ensuring no concurrent write changes the manifest mid-scan.
  - Old snapshot JSON files are also candidates for cleanup: only
    the current snapshot is kept; all previous snapshot files are
    marked for deletion.
  - Clone protection: files referenced by any clone's snapshot are
    excluded from orphan detection — even if they appear unreferenced
    by the source's current snapshot.
  - The ``preview`` function never deletes — it returns the list
    so operators can review before committing.
  - Every cleanup is audited via a GC_EXECUTE event.

Compliance: operational hygiene (SOC 2 A1.1 — capacity management).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Clone protection — collect file references held by clones
# ---------------------------------------------------------------------------

def _collect_clone_referenced_files(
    organization: str,
    super_name: str,
    simple_name: str,
) -> Set[str]:
    """Return parquet file paths that any clone still references for this table.

    Scans all SuperTables cloned from *super_name* and collects file paths
    from their snapshot manifests.  Replica clones are skipped because they
    hold no independent file references (they read through to the source).
    """
    from supertable.redis_catalog import RedisCatalog
    from supertable.storage.storage_factory import get_storage
    from supertable.services.supertable_manager import find_clones

    referenced: Set[str] = set()
    try:
        catalog = RedisCatalog()
        storage = get_storage()
        clones = find_clones(organization, super_name)

        for clone_name in clones:
            root = catalog.get_root(organization, clone_name)
            if not root:
                continue
            # Replicas have no leaf pointers — they read from source. Skip.
            if root.get("clone_type") == "replica":
                continue

            leaf = catalog.get_leaf(organization, clone_name, simple_name)
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
    except Exception as e:
        logger.warning("[gc] clone reference scan failed (safe — no files will be deleted): %s", e)

    return referenced


def preview_obsolete_files(
    organization: str,
    super_name: str,
    simple_name: str,
    cutoff_version: Optional[int] = None,
) -> Dict[str, Any]:
    """Identify reclaimable files for a table.

    Two modes:

    **Without cutoff_version** (legacy, used by gc/clean):
        Scans storage and compares against the current snapshot manifest.
        Finds ALL orphaned files including leftovers from failed writes.

    **With cutoff_version** (version-aware, used by UI):
        Walks the snapshot chain, collects file sets for kept versions
        (>= cutoff) and removed versions (< cutoff).  Reclaimable data
        files = files ONLY in removed versions, not in any kept version.
        Uses ``file_size`` from resource entries — no storage scan needed.
    """
    from supertable.redis_catalog import RedisCatalog
    from supertable.storage.storage_factory import get_storage

    catalog = RedisCatalog()
    storage = get_storage()

    result: Dict[str, Any] = {
        "table": simple_name,
        "orphaned_data_files": [],
        "orphaned_snapshot_files": [],
        "current_snapshot_path": "",
        "current_version": 0,
        "live_file_count": 0,
    }

    leaf = catalog.get_leaf(organization, super_name, simple_name)
    if not leaf or not leaf.get("path"):
        result["error"] = "Table not found or has no snapshot"
        return result

    leaf_payload = leaf.get("payload") if isinstance(leaf, dict) else None
    if isinstance(leaf_payload, dict) and leaf_payload.get("_linked_share"):
        result["skipped"] = True
        result["reason"] = "Virtual share leaf (no local data files)"
        return result

    current_path = leaf["path"]
    result["current_snapshot_path"] = current_path

    # ── Walk the full snapshot chain ──────────────────────────────────────
    # Collect per-version: file paths, file sizes, snapshot JSON paths
    kept_files: Set[str] = set()             # files in versions >= cutoff
    removed_files: Dict[str, int] = {}       # file → file_size, versions < cutoff
    kept_snapshot_paths: Set[str] = set()
    removed_snapshot_paths: List[str] = []

    walk_path: Optional[str] = current_path
    walk_data = leaf.get("payload") if isinstance(leaf, dict) else None
    if not isinstance(walk_data, dict) or not isinstance(walk_data.get("resources"), list):
        walk_data = None
    visited: set = set()
    max_walk = 500

    while walk_path and len(visited) < max_walk:
        if walk_path in visited:
            break
        visited.add(walk_path)

        if walk_data is None:
            try:
                walk_data = storage.read_json(walk_path)
            except Exception:
                break
        if not isinstance(walk_data, dict):
            break

        ver = int(walk_data.get("snapshot_version", 0))
        resources = walk_data.get("resources") or []

        if result["current_version"] == 0:
            result["current_version"] = ver

        is_kept = (cutoff_version is None) or (ver >= cutoff_version)

        if is_kept:
            kept_snapshot_paths.add(walk_path)
            for res in resources:
                f = res.get("file", "")
                if f:
                    kept_files.add(f)
        else:
            removed_snapshot_paths.append(walk_path)
            for res in resources:
                f = res.get("file", "")
                if f and f not in removed_files:
                    removed_files[f] = int(res.get("file_size", 0) or 0)

        prev_path = walk_data.get("previous_snapshot")
        walk_path = prev_path if isinstance(prev_path, str) and prev_path else None
        walk_data = None

    result["live_file_count"] = len(kept_files)

    # ── Compute reclaimable files ─────────────────────────────────────────
    if cutoff_version is not None:
        # Version-aware mode: diff removed vs kept manifests
        clone_protected = _collect_clone_referenced_files(organization, super_name, simple_name)
        protected = kept_files | clone_protected
        result["clone_protected_file_count"] = len(clone_protected)

        data_bytes = 0
        for f, size in removed_files.items():
            if f not in protected:
                result["orphaned_data_files"].append(f)
                data_bytes += size

        # Snapshot JSONs for removed versions
        snapshot_bytes = 0
        for sp in removed_snapshot_paths:
            result["orphaned_snapshot_files"].append(sp)
            try:
                snapshot_bytes += storage.size(sp)
            except Exception:
                pass

        result["orphaned_data_bytes"] = data_bytes
        result["orphaned_snapshot_bytes"] = snapshot_bytes
        result["total_orphaned_bytes"] = data_bytes + snapshot_bytes
    else:
        # Legacy mode: storage scan (finds ALL orphans including failed writes)
        clone_protected = _collect_clone_referenced_files(organization, super_name, simple_name)
        protected = kept_files | clone_protected
        result["clone_protected_file_count"] = len(clone_protected)

        data_dir = os.path.join(
            organization, super_name, "tables", simple_name, "data",
        )
        try:
            storage_files = storage.list_files(data_dir, "*.parquet")
        except Exception:
            storage_files = []

        data_bytes = 0
        for file_path in storage_files:
            if file_path not in protected:
                result["orphaned_data_files"].append(file_path)
                try:
                    data_bytes += storage.size(file_path)
                except Exception:
                    pass

        snapshot_dir = os.path.join(
            organization, super_name, "tables", simple_name, "snapshots",
        )
        try:
            snapshot_files = storage.list_files(snapshot_dir, "*.json")
        except Exception:
            snapshot_files = []

        snapshot_bytes = 0
        for snap_path in snapshot_files:
            if snap_path != current_path:
                result["orphaned_snapshot_files"].append(snap_path)
                try:
                    snapshot_bytes += storage.size(snap_path)
                except Exception:
                    pass

        result["orphaned_data_bytes"] = data_bytes
        result["orphaned_snapshot_bytes"] = snapshot_bytes
        result["total_orphaned_bytes"] = data_bytes + snapshot_bytes

    return result


def clean_obsolete_files(
    organization: str,
    super_name: str,
    simple_name: str,
    role_name: str,
) -> Dict[str, Any]:
    """Delete orphaned files for a table with locking and audit.

    Acquires a per-table Redis lock to ensure no concurrent write
    changes the manifest during the scan.  Releases the lock before
    audit emission.

    Returns:
        {
            "table": str,
            "deleted_data_files": int,
            "deleted_snapshot_files": int,
            "errors": [str, ...],
        }
    """
    from supertable.redis_catalog import RedisCatalog
    from supertable.storage.storage_factory import get_storage
    from supertable.rbac.access_control import check_write_access

    catalog = RedisCatalog()
    storage = get_storage()

    # RBAC check — GC requires write access
    check_write_access(
        super_name=super_name,
        organization=organization,
        role_name=role_name,
        table_name=simple_name,
    )

    result: Dict[str, Any] = {
        "table": simple_name,
        "deleted_data_files": 0,
        "deleted_snapshot_files": 0,
        "errors": [],
    }

    # Acquire per-table lock
    token = catalog.acquire_simple_lock(organization, super_name, simple_name, ttl_s=30, timeout_s=60)
    if not token:
        result["errors"].append(f"Could not acquire lock for '{simple_name}'")
        return result

    try:
        preview = preview_obsolete_files(organization, super_name, simple_name)
        if preview.get("error"):
            result["errors"].append(preview["error"])
            return result

        orphaned_data = preview.get("orphaned_data_files", [])
        orphaned_snaps = preview.get("orphaned_snapshot_files", [])

        # Delete orphaned data files
        for file_path in orphaned_data:
            try:
                storage.delete(file_path)
                result["deleted_data_files"] += 1
            except FileNotFoundError:
                pass  # Already gone
            except Exception as e:
                result["errors"].append(f"{file_path}: {e}")

        # Delete orphaned snapshot files
        for snap_path in orphaned_snaps:
            try:
                storage.delete(snap_path)
                result["deleted_snapshot_files"] += 1
            except FileNotFoundError:
                pass
            except Exception as e:
                result["errors"].append(f"{snap_path}: {e}")

    finally:
        catalog.release_simple_lock(organization, super_name, simple_name, token)

    # Audit AFTER lock release
    try:
        from supertable.audit import emit as _audit, EventCategory, Actions, Severity, make_detail
        _audit(
            category=EventCategory.SYSTEM,
            action=Actions.GC_EXECUTE,
            organization=organization,
            super_name=super_name,
            resource_type="table",
            resource_id=simple_name,
            severity=Severity.WARNING,
            detail=make_detail(
                role_name=role_name,
                deleted_data_files=result["deleted_data_files"],
                deleted_snapshot_files=result["deleted_snapshot_files"],
                error_count=len(result["errors"]),
            ),
        )
    except Exception:
        pass

    return result
