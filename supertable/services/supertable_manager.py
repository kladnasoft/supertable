# route: supertable.services.supertable_manager
"""
Business logic for SuperTable lifecycle: list, clone (read-only and
copy-on-write writable), table-level clone, time-travel cloning,
delete-protection, read-only toggle, and storage divergence tracking.

Clone architecture
------------------
Both clone modes (read-only and writable) are zero-copy at creation time.
The cloned snapshot's ``resources[]`` entries point to the *source*
SuperTable's parquet files.  No data is copied.

For writable clones, the existing DataWriter naturally provides
copy-on-write semantics: when a write occurs on the clone, new parquet
files are written to the *clone's* data directory, and the snapshot
drops the old source paths and adds the new clone paths.  GC only
scans the clone's own ``data/`` directory, so source files are safe.

For read-only clones, writes are blocked by the ``_check_readonly_guard``
in ``rbac/access_control.py``.

Used by api/api.py endpoint handlers.  No FastAPI dependencies.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _snap_filename() -> str:
    return f"tables_{uuid.uuid4().hex[:12]}.json"


def _compute_divergence(
    resources: List[Dict[str, Any]],
    source_name: str,
    organization: str,
) -> Dict[str, Any]:
    """Compute storage divergence for a cloned table's resource list."""
    if not resources:
        return {"shared_files": 0, "own_files": 0, "total_files": 0, "divergence_pct": 0.0}

    source_prefix = f"{organization}/{source_name}/"
    shared = sum(1 for r in resources if r.get("file", "").startswith(source_prefix))
    total = len(resources)
    own = total - shared
    pct = round(own / max(total, 1) * 100, 1)
    return {"shared_files": shared, "own_files": own, "total_files": total, "divergence_pct": pct}


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

def list_supertables_detailed(organization: str) -> List[Dict[str, Any]]:
    """Return all supertables with metadata, table counts, and divergence."""
    organization = (organization or "").strip()
    if not organization:
        return []

    catalog = RedisCatalog()
    storage = get_storage()
    pattern = f"supertable:{organization}:*:meta:root"
    cursor = 0
    results: List[Dict[str, Any]] = []

    try:
        while True:
            cursor, keys = catalog.r.scan(cursor=cursor, match=pattern, count=500)
            for k in keys:
                ks = k if isinstance(k, str) else k.decode("utf-8")
                parts = ks.split(":")
                if len(parts) < 4:
                    continue
                sup_name = parts[2]
                root: Dict[str, Any] = {}
                try:
                    raw = catalog.r.get(ks)
                    root = json.loads(raw) if raw else {}
                except Exception:
                    pass

                # Count tables + collect divergence for clones
                table_count = 0
                divergence: Optional[Dict[str, Any]] = None
                cloned_from = root.get("cloned_from")

                try:
                    for leaf_item in catalog.scan_leaf_items(organization, sup_name):
                        table_count += 1
                        if cloned_from:
                            payload = leaf_item.get("payload")
                            if isinstance(payload, dict):
                                resources = payload.get("resources", [])
                            else:
                                src_path = leaf_item.get("path", "")
                                try:
                                    snap = storage.read_json(src_path) if src_path else {}
                                    resources = snap.get("resources", [])
                                except Exception:
                                    resources = []
                            d = _compute_divergence(resources, cloned_from, organization)
                            if divergence is None:
                                divergence = {"shared_files": 0, "own_files": 0, "total_files": 0}
                            divergence["shared_files"] += d["shared_files"]
                            divergence["own_files"] += d["own_files"]
                            divergence["total_files"] += d["total_files"]
                except Exception:
                    pass

                if divergence and divergence["total_files"] > 0:
                    divergence["divergence_pct"] = round(
                        divergence["own_files"] / divergence["total_files"] * 100, 1
                    )
                elif divergence:
                    divergence["divergence_pct"] = 0.0

                results.append({
                    "name": sup_name,
                    "table_count": table_count,
                    "version": root.get("version", 0),
                    "ts": root.get("ts", 0),
                    "read_only": bool(root.get("read_only")),
                    "cloned_from": cloned_from,
                    "clone_type": root.get("clone_type"),
                    "clone_ts": root.get("clone_ts"),
                    "divergence": divergence,
                })
            if cursor == 0:
                break
    except Exception as e:
        logger.error("list_supertables_detailed failed: %s", e)

    return sorted(results, key=lambda x: x["name"])


# ---------------------------------------------------------------------------
# Delete protection
# ---------------------------------------------------------------------------

def can_delete_supertable(organization: str, super_name: str) -> Dict[str, Any]:
    """Block deletion if any clone references this supertable."""
    catalog = RedisCatalog()
    clones = _find_clones(catalog, organization, super_name)
    if clones:
        return {
            "ok": False,
            "reason": f"Cannot delete: {len(clones)} clone(s) still reference this SuperTable.",
            "clones": clones,
        }
    return {"ok": True}


def _find_clones(catalog: RedisCatalog, org: str, source_sup: str) -> List[str]:
    """Find all supertables that were cloned from *source_sup*."""
    clones: List[str] = []
    pattern = f"supertable:{org}:*:meta:root"
    cursor = 0
    try:
        while True:
            cursor, keys = catalog.r.scan(cursor=cursor, match=pattern, count=500)
            for k in keys:
                ks = k if isinstance(k, str) else k.decode("utf-8")
                parts = ks.split(":")
                if len(parts) < 4:
                    continue
                sup_name = parts[2]
                if sup_name == source_sup:
                    continue
                try:
                    raw = catalog.r.get(ks)
                    if not raw:
                        continue
                    doc = json.loads(raw)
                    if doc.get("cloned_from") == source_sup:
                        clones.append(sup_name)
                except Exception:
                    continue
            if cursor == 0:
                break
    except Exception as e:
        logger.error("_find_clones error: %s", e)
    return clones


# ---------------------------------------------------------------------------
# Read-only toggle
# ---------------------------------------------------------------------------

def set_read_only(organization: str, super_name: str, enabled: bool) -> bool:
    catalog = RedisCatalog()
    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")
    return catalog.update_root_flags(organization, super_name, {"read_only": enabled})


# ---------------------------------------------------------------------------
# Snapshot resolution (time travel)
# ---------------------------------------------------------------------------

def _resolve_snapshot_for_table(
    catalog: RedisCatalog,
    storage,
    organization: str,
    source_name: str,
    simple_name: str,
    at_version: Optional[int] = None,
    at_timestamp: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve the snapshot to clone for a single table.

    - *at_version*: walk snapshot chain to find exact version.
    - *at_timestamp*: find the latest snapshot with last_updated_ms <= ts.
    - Neither: return current (latest) snapshot.
    """
    if at_version is not None or at_timestamp is not None:
        from supertable.services.time_travel import list_snapshot_versions, get_snapshot_at_version

        versions = list_snapshot_versions(organization, source_name, simple_name, limit=500)
        if at_version is not None:
            return get_snapshot_at_version(organization, source_name, simple_name, at_version)
        else:
            for v in versions:  # newest first
                if v.get("last_updated_ms", 0) <= at_timestamp:
                    return get_snapshot_at_version(
                        organization, source_name, simple_name, v["version"]
                    )
            return None

    # Latest snapshot from leaf pointer
    leaf = catalog.get_leaf(organization, source_name, simple_name)
    if not leaf:
        return None

    payload = leaf.get("payload") if isinstance(leaf, dict) else None
    if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
        return payload

    path = leaf.get("path", "")
    if path:
        try:
            return storage.read_json(path)
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Clone helper — write one table's snapshot + leaf pointer
# ---------------------------------------------------------------------------

def _clone_single_table(
    catalog: RedisCatalog,
    storage,
    organization: str,
    source_name: str,
    target_name: str,
    simple_name: str,
    snapshot: Dict[str, Any],
    now_ms: int,
) -> bool:
    """Write a cloned snapshot + leaf pointer for one table (zero-copy)."""
    clone_snapshot = dict(snapshot)
    clone_snapshot["snapshot_version"] = 0
    clone_snapshot["previous_snapshot"] = None
    clone_snapshot["last_updated_ms"] = now_ms

    # Create directories (data dir needed for future writes)
    for subdir in ("snapshots", "data"):
        d = os.path.join(organization, target_name, "tables", simple_name, subdir)
        try:
            storage.makedirs(d)
        except Exception:
            pass

    target_snap_path = os.path.join(
        organization, target_name, "tables", simple_name, "snapshots", _snap_filename()
    )
    storage.write_json(target_snap_path, clone_snapshot)

    try:
        catalog.set_leaf_payload_cas(
            organization, target_name, simple_name,
            clone_snapshot, target_snap_path, now_ms=now_ms,
        )
    except Exception:
        catalog.set_leaf_path_cas(
            organization, target_name, simple_name,
            target_snap_path, now_ms=now_ms,
        )
    return True


def _init_target_rbac(organization: str, target_name: str) -> None:
    try:
        from supertable.rbac.role_manager import RoleManager
        from supertable.rbac.user_manager import UserManager
        RoleManager(super_name=target_name, organization=organization)
        UserManager(super_name=target_name, organization=organization)
    except Exception as e:
        logger.warning("RBAC init for '%s' failed: %s", target_name, e)


# ---------------------------------------------------------------------------
# Clone — SuperTable level
# ---------------------------------------------------------------------------

def clone_supertable(
    organization: str,
    source_name: str,
    target_name: str,
    read_only: bool = False,
    at_timestamp: Optional[int] = None,
) -> Dict[str, Any]:
    """Clone all tables from source to target (zero-copy).

    Both read-only and writable clones are zero-copy.  Writable clones
    diverge via copy-on-write when DataWriter creates new parquet files.

    Args:
        read_only: If True, writes to the clone are blocked.
        at_timestamp: Optional epoch-ms for time-travel clone.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    if not catalog.root_exists(organization, source_name):
        raise FileNotFoundError(f"Source SuperTable '{source_name}' not found")
    if catalog.root_exists(organization, target_name):
        raise ValueError(f"Target SuperTable '{target_name}' already exists")

    now_ms = _now_ms()
    clone_type = "readonly" if read_only else "writable"

    catalog.ensure_root(organization, target_name)
    catalog.update_root_flags(organization, target_name, {
        "read_only": read_only,
        "cloned_from": source_name,
        "clone_type": clone_type,
        "clone_ts": now_ms,
    })

    target_super_dir = os.path.join(organization, target_name, "super")
    try:
        storage.makedirs(target_super_dir)
    except Exception:
        pass

    tables_cloned = 0
    tables_skipped = 0

    for leaf_item in catalog.scan_leaf_items(organization, source_name):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue

        snapshot = _resolve_snapshot_for_table(
            catalog, storage, organization, source_name, simple_name,
            at_timestamp=at_timestamp,
        )
        if not snapshot:
            tables_skipped += 1
            continue

        if _clone_single_table(
            catalog, storage, organization, source_name, target_name,
            simple_name, snapshot, now_ms,
        ):
            tables_cloned += 1

    _init_target_rbac(organization, target_name)

    return {
        "ok": True,
        "source": source_name,
        "target": target_name,
        "clone_type": clone_type,
        "tables_cloned": tables_cloned,
        "tables_skipped": tables_skipped,
    }


# ---------------------------------------------------------------------------
# Clone — single table
# ---------------------------------------------------------------------------

def clone_table(
    organization: str,
    source_sup: str,
    target_sup: str,
    table_name: str,
    at_version: Optional[int] = None,
    at_timestamp: Optional[int] = None,
) -> Dict[str, Any]:
    """Clone a single table between supertables (zero-copy).

    Target supertable must exist.  Table must not already exist in target.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    if not catalog.root_exists(organization, source_sup):
        raise FileNotFoundError(f"Source SuperTable '{source_sup}' not found")
    if not catalog.root_exists(organization, target_sup):
        raise FileNotFoundError(f"Target SuperTable '{target_sup}' not found")
    if catalog.leaf_exists(organization, target_sup, table_name):
        raise ValueError(f"Table '{table_name}' already exists in target '{target_sup}'")
    if not catalog.leaf_exists(organization, source_sup, table_name):
        raise FileNotFoundError(f"Table '{table_name}' not found in source '{source_sup}'")

    snapshot = _resolve_snapshot_for_table(
        catalog, storage, organization, source_sup, table_name,
        at_version=at_version, at_timestamp=at_timestamp,
    )
    if not snapshot:
        detail = ""
        if at_version is not None:
            detail = f" at version {at_version}"
        elif at_timestamp is not None:
            detail = f" at timestamp {at_timestamp}"
        raise FileNotFoundError(f"Snapshot for '{table_name}'{detail} not found")

    now_ms = _now_ms()
    _clone_single_table(
        catalog, storage, organization, source_sup, target_sup,
        table_name, snapshot, now_ms,
    )

    return {
        "ok": True,
        "source_sup": source_sup,
        "target_sup": target_sup,
        "table": table_name,
        "snapshot_version": snapshot.get("snapshot_version", 0),
        "resource_count": len(snapshot.get("resources", [])),
    }
