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
  - The ``preview`` function never deletes — it returns the list
    so operators can review before committing.
  - Every cleanup is audited via a GC_EXECUTE event.

Compliance: operational hygiene (SOC 2 A1.1 — capacity management).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def preview_obsolete_files(
    organization: str,
    super_name: str,
    simple_name: str,
) -> Dict[str, Any]:
    """Identify orphaned files without deleting them.

    Returns:
        {
            "table": str,
            "orphaned_data_files": [str, ...],
            "orphaned_snapshot_files": [str, ...],
            "current_snapshot_path": str,
            "current_version": int,
            "live_file_count": int,
        }
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

    # Read the current snapshot from Redis leaf pointer
    leaf = catalog.get_leaf(organization, super_name, simple_name)
    if not leaf or not leaf.get("path"):
        result["error"] = "Table not found or has no snapshot"
        return result

    current_path = leaf["path"]
    result["current_snapshot_path"] = current_path

    # Get snapshot data (prefer Redis cached payload)
    snapshot = leaf.get("payload") if isinstance(leaf, dict) else None
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("resources"), list):
        try:
            from supertable.super_table import SuperTable
            st = SuperTable(super_name, organization)
            snapshot = st.read_simple_table_snapshot(current_path)
        except Exception as e:
            result["error"] = f"Failed to read snapshot: {e}"
            return result

    result["current_version"] = int(snapshot.get("snapshot_version", 0))

    # Build set of live data file paths from the manifest
    resources = snapshot.get("resources", []) or []
    live_files: Set[str] = set()
    for res in resources:
        f = res.get("file", "")
        if f:
            live_files.add(f)
    result["live_file_count"] = len(live_files)

    # Scan data directory for actual files on storage
    data_dir = os.path.join(
        organization, super_name, "tables", simple_name, "data",
    )
    try:
        storage_files = storage.list_files(data_dir, "*.parquet")
    except Exception:
        storage_files = []

    for file_path in storage_files:
        if file_path not in live_files:
            result["orphaned_data_files"].append(file_path)

    # Scan snapshot directory for old snapshot JSON files
    snapshot_dir = os.path.join(
        organization, super_name, "tables", simple_name, "snapshots",
    )
    try:
        snapshot_files = storage.list_files(snapshot_dir, "*.json")
    except Exception:
        snapshot_files = []

    for snap_path in snapshot_files:
        if snap_path != current_path:
            result["orphaned_snapshot_files"].append(snap_path)

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
