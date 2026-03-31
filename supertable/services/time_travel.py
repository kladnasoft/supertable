# route: supertable.services.time_travel
"""
Time-travel service — snapshot version history and point-in-time reads.

Every SimpleTable write produces a new snapshot that references the
previous snapshot via the ``previous_snapshot`` field.  This module
walks that linked list to expose version history and point-in-time
data access.

The linked list is walked on storage (not Redis) because Redis only
stores the *current* leaf pointer.  Historical snapshots are JSON
files on the configured storage backend.

Safety: all reads are non-mutating.  No locks are acquired.  Storage
reads may be slow for tables with hundreds of versions, so the walk
is bounded by a configurable max depth.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Maximum number of versions to walk (prevents unbounded I/O on tables
# with thousands of writes).  Callers can override via the limit param.
_DEFAULT_MAX_VERSIONS = 200


def list_snapshot_versions(
    organization: str,
    super_name: str,
    simple_name: str,
    limit: int = _DEFAULT_MAX_VERSIONS,
) -> List[Dict[str, Any]]:
    """Walk the snapshot linked list and return version metadata.

    Returns a list of dicts ordered newest-first:
    [
        {
            "version": 7,
            "snapshot_path": "acme/example/tables/orders/snapshots/tables_..._abc.json",
            "last_updated_ms": 1711734000000,
            "resource_count": 5,
            "total_rows": 25000,
        },
        ...
    ]

    Does NOT return file contents or schema — just enough for a
    version picker UI.
    """
    from supertable.redis_catalog import RedisCatalog
    from supertable.storage.storage_factory import get_storage

    catalog = RedisCatalog()
    storage = get_storage()

    # Start from the current Redis leaf pointer
    leaf = catalog.get_leaf(organization, super_name, simple_name)
    if not leaf or not leaf.get("path"):
        return []

    current_path = leaf["path"]

    # Virtual share leaves have no local snapshot history
    if current_path.startswith("__linked_share__/"):
        return []

    # Try to use the cached payload first
    current_data = leaf.get("payload") if isinstance(leaf, dict) else None
    if not isinstance(current_data, dict) or not isinstance(current_data.get("resources"), list):
        current_data = None

    versions: List[Dict[str, Any]] = []
    visited: set = set()  # cycle detection

    while current_path and len(versions) < limit:
        if current_path in visited:
            logger.warning("[time-travel] Cycle detected at %s", current_path)
            break
        visited.add(current_path)

        # Read snapshot if we don't already have it
        if current_data is None:
            try:
                current_data = storage.read_json(current_path)
            except FileNotFoundError:
                logger.debug("[time-travel] Snapshot not found: %s", current_path)
                break
            except Exception as e:
                logger.warning("[time-travel] Failed to read %s: %s", current_path, e)
                break

        if not isinstance(current_data, dict):
            break

        resources = current_data.get("resources", []) or []
        total_rows = sum(int(r.get("rows", 0)) for r in resources if isinstance(r, dict))

        versions.append({
            "version": int(current_data.get("snapshot_version", 0)),
            "snapshot_path": current_path,
            "last_updated_ms": int(current_data.get("last_updated_ms", 0)),
            "resource_count": len(resources),
            "total_rows": total_rows,
            "lineage": current_data.get("lineage") or {},
        })

        # Walk to previous snapshot
        prev_path = current_data.get("previous_snapshot")
        current_path = prev_path if isinstance(prev_path, str) and prev_path else None
        current_data = None  # will be loaded from storage on next iteration

    return versions


def get_snapshot_at_version(
    organization: str,
    super_name: str,
    simple_name: str,
    version: int,
) -> Optional[Dict[str, Any]]:
    """Retrieve the full snapshot dict for a specific version number.

    Walks the linked list from the current head until it finds the
    requested version.  Returns None if the version is not found
    (either too old and pruned, or never existed).
    """
    versions = list_snapshot_versions(
        organization, super_name, simple_name, limit=_DEFAULT_MAX_VERSIONS,
    )

    target_path = None
    for v in versions:
        if v["version"] == version:
            target_path = v["snapshot_path"]
            break

    if not target_path:
        return None

    try:
        from supertable.storage.storage_factory import get_storage
        storage = get_storage()
        return storage.read_json(target_path)
    except Exception as e:
        logger.warning("[time-travel] Failed to read version %d at %s: %s", version, target_path, e)
        return None


def get_snapshot_file_list(
    organization: str,
    super_name: str,
    simple_name: str,
    version: int,
) -> List[str]:
    """Get the list of Parquet file paths for a specific version.

    This is the minimum needed to query a table at a historical version:
    register these files with DuckDB as the table's data source.
    """
    snapshot = get_snapshot_at_version(organization, super_name, simple_name, version)
    if not snapshot:
        return []

    resources = snapshot.get("resources", []) or []
    return [r["file"] for r in resources if isinstance(r, dict) and r.get("file")]
