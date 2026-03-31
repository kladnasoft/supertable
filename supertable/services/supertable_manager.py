# route: supertable.services.supertable_manager
"""
Business logic for SuperTable lifecycle: list, clone (read-only snapshot,
copy-on-write writable, and live replica), table-level clone, time-travel
cloning, delete-protection, read-only toggle, promote, detach, and
storage divergence tracking.

Clone architecture
------------------
Three clone modes:

**Snapshot (read-only)** — zero-copy at creation.  Leaf pointers are
copied from source.  Writes are blocked.  Data is frozen at clone time.

**Writable** — zero-copy at creation, copy-on-write on first mutation.
New files go to the clone's own storage path.  Diverges from source
over time.

**Replica** — no leaf pointers created.  Reads resolve through the
source's current leaves in real time.  Always shows latest data.
Zero storage overhead.  Writes are blocked.  Can be promoted to
writable (materializes leaves first) or detached (hard-copies shared
files to become fully independent).

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
                        # Skip virtual share leaves — they are not real tables
                        payload = leaf_item.get("payload")
                        if isinstance(payload, dict) and payload.get("_linked_share"):
                            continue

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


def find_clones(organization: str, source_sup: str) -> List[str]:
    """Find all supertables that were cloned from *source_sup*.

    Public wrapper used by gc.py for clone file protection and by
    can_delete_supertable() for deletion guard.
    """
    catalog = RedisCatalog()
    return _find_clones(catalog, organization, source_sup)


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

def _is_virtual_share_leaf(snapshot: Optional[Dict[str, Any]]) -> bool:
    """Return True if the snapshot is a virtual share leaf (not real data)."""
    if not isinstance(snapshot, dict):
        return False
    return bool(snapshot.get("_linked_share"))


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

    # Strip internal markers that don't belong in cloned snapshots
    for key in ("_linked_share", "_provider_org", "_row_filter", "_allowed_columns"):
        clone_snapshot.pop(key, None)

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


def _clone_rbac(
    organization: str,
    source_name: str,
    target_name: str,
    inherit_roles: Optional[List[str]] = None,
    inherit_users: bool = False,
) -> Dict[str, Any]:
    """Copy RBAC roles (and optionally users) from source to target.

    Args:
        inherit_roles: List of role names to copy.  None or empty = all roles.
        inherit_users: If True, also copy user-role assignments.

    Returns dict with counts of cloned roles and users.
    """
    from supertable.rbac.role_manager import RoleManager
    from supertable.rbac.user_manager import UserManager

    src_rm = RoleManager(super_name=source_name, organization=organization)
    tgt_rm = RoleManager(super_name=target_name, organization=organization)

    roles_cloned = 0
    users_cloned = 0

    # Build source_role_id → target_role_id mapping for user translation
    role_id_map: Dict[str, str] = {}

    roles = src_rm.list_roles()
    for role in roles:
        rname = role.get("role_name", "")
        # Skip superadmin — already created by _init_target_rbac
        if rname == "superadmin":
            continue
        # Filter if specific roles requested
        if inherit_roles and rname not in inherit_roles:
            continue

        source_role_id = role.get("role_id", "")

        try:
            role_data = {
                "role": role.get("role", "reader"),
                "role_name": rname,
                "tables": role.get("tables", {}),
            }
            tgt_rm.create_role(role_data)
            roles_cloned += 1

            # Look up the target role ID by name to build the mapping
            if source_role_id:
                target_role = tgt_rm.get_role_by_name(rname)
                target_role_id = target_role.get("role_id", "")
                if target_role_id:
                    role_id_map[source_role_id] = target_role_id
        except Exception as e:
            logger.warning("Clone RBAC role '%s' failed: %s", rname, e)

    if inherit_users:
        try:
            src_um = UserManager(super_name=source_name, organization=organization)
            tgt_um = UserManager(super_name=target_name, organization=organization)
            users = src_um.list_users()
            for user in users:
                source_roles = user.get("roles", [])
                # Translate source role IDs to target role IDs
                translated_roles = []
                for rid in source_roles:
                    if rid in role_id_map:
                        translated_roles.append(role_id_map[rid])
                    # else: role wasn't cloned (filtered out or superadmin) — skip it
                try:
                    user_data = {
                        "username": user.get("username", ""),
                        "roles": translated_roles,
                    }
                    # Only clone if user has at least one translated role
                    if translated_roles or not source_roles:
                        tgt_um.create_user(user_data)
                        users_cloned += 1
                except Exception as e:
                    logger.warning("Clone user '%s' failed: %s", user.get("username", "?"), e)
        except Exception as e:
            logger.warning("Clone users failed: %s", e)

    return {"roles_cloned": roles_cloned, "users_cloned": users_cloned}


# ---------------------------------------------------------------------------
# Clone — SuperTable level
# ---------------------------------------------------------------------------

def clone_supertable(
    organization: str,
    source_name: str,
    target_name: str,
    read_only: bool = False,
    clone_type: Optional[str] = None,
    at_timestamp: Optional[int] = None,
    tables: Optional[List[str]] = None,
    inherit_rbac: bool = False,
    inherit_roles: Optional[List[str]] = None,
    inherit_users: bool = False,
) -> Dict[str, Any]:
    """Clone all (or selected) tables from source to target (zero-copy).

    Both read-only and writable clones are zero-copy.  Writable clones
    diverge via copy-on-write when DataWriter creates new parquet files.

    Replica clones create NO leaf pointers — reads resolve through
    the source's current leaves at query time, always showing the
    latest data.

    Args:
        read_only: If True, writes to the clone are blocked (snapshot clones).
        clone_type: Explicit type: 'readonly', 'writable', or 'replica'.
                    If not provided, derived from *read_only*.
        at_timestamp: Optional epoch-ms for time-travel clone.
        tables: Optional list of table names to clone.  None = all.
        inherit_rbac: If True, copy roles from source.
        inherit_roles: Specific role names to copy.  None = all (when inherit_rbac=True).
        inherit_users: If True, also copy user-role assignments.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    if not catalog.root_exists(organization, source_name):
        raise FileNotFoundError(f"Source SuperTable '{source_name}' not found")
    if catalog.root_exists(organization, target_name):
        raise ValueError(f"Target SuperTable '{target_name}' already exists")

    # Resolve clone_type
    if clone_type is None:
        clone_type = "readonly" if read_only else "writable"
    clone_type = clone_type.strip().lower()
    if clone_type not in ("readonly", "writable", "replica"):
        raise ValueError(f"Invalid clone_type: {clone_type}")

    is_read_only = clone_type in ("readonly", "replica")

    # Validate requested tables
    if tables:
        for t in tables:
            if not catalog.leaf_exists(organization, source_name, t):
                raise FileNotFoundError(f"Table '{t}' not found in '{source_name}'")

    now_ms = _now_ms()

    catalog.ensure_root(organization, target_name)

    root_flags: Dict[str, Any] = {
        "read_only": is_read_only,
        "cloned_from": source_name,
        "clone_type": clone_type,
        "clone_ts": now_ms,
    }
    if clone_type == "replica" and tables:
        root_flags["replica_tables"] = tables
    catalog.update_root_flags(organization, target_name, root_flags)

    # Replica: no leaf pointers — reads resolve through source at query time.
    if clone_type == "replica":
        _init_target_rbac(organization, target_name)
        rbac_result = {}
        if inherit_rbac:
            try:
                rbac_result = _clone_rbac(organization, source_name, target_name, inherit_roles, inherit_users)
            except Exception as e:
                logger.warning("RBAC inheritance failed: %s", e)
        return {
            "ok": True,
            "source": source_name,
            "target": target_name,
            "clone_type": clone_type,
            "tables_cloned": 0,
            "tables_skipped": 0,
            "roles_cloned": rbac_result.get("roles_cloned", 0),
            "users_cloned": rbac_result.get("users_cloned", 0),
            "note": "Replica — reads resolve through source in real time.",
        }

    # Snapshot / writable: copy leaf pointers (zero-copy file references)
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
        if tables and simple_name not in tables:
            continue

        snapshot = _resolve_snapshot_for_table(
            catalog, storage, organization, source_name, simple_name,
            at_timestamp=at_timestamp,
        )
        if not snapshot:
            tables_skipped += 1
            continue

        # Skip virtual share leaves — they contain presigned URLs, not real data
        if _is_virtual_share_leaf(snapshot):
            tables_skipped += 1
            continue

        if _clone_single_table(
            catalog, storage, organization, source_name, target_name,
            simple_name, snapshot, now_ms,
        ):
            tables_cloned += 1

    _init_target_rbac(organization, target_name)
    rbac_result = {}
    if inherit_rbac:
        try:
            rbac_result = _clone_rbac(organization, source_name, target_name, inherit_roles, inherit_users)
        except Exception as e:
            logger.warning("RBAC inheritance failed: %s", e)

    return {
        "ok": True,
        "source": source_name,
        "target": target_name,
        "clone_type": clone_type,
        "tables_cloned": tables_cloned,
        "tables_skipped": tables_skipped,
        "roles_cloned": rbac_result.get("roles_cloned", 0),
        "users_cloned": rbac_result.get("users_cloned", 0),
    }


# ---------------------------------------------------------------------------
# Promote replica → writable
# ---------------------------------------------------------------------------

def _get_snapshot_from_leaf(leaf_item: Dict[str, Any], storage) -> Optional[Dict[str, Any]]:
    """Extract snapshot dict from a scan_leaf_items result."""
    payload = leaf_item.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
        return payload
    path = leaf_item.get("path", "")
    if path:
        try:
            return storage.read_json(path)
        except Exception:
            pass
    return None


def promote_replica(organization: str, super_name: str) -> Dict[str, Any]:
    """Convert a replica into a writable clone.

    Materializes the source's current leaf pointers into the replica's own
    namespace (creating snapshot JSONs), then flips the root flags to
    ``clone_type: writable, read_only: false``.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    root = catalog.get_root(organization, super_name)
    if not root:
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")
    if root.get("clone_type") != "replica":
        raise ValueError(f"'{super_name}' is not a replica (clone_type={root.get('clone_type')})")

    source = root.get("cloned_from", "")
    if not source:
        raise ValueError("Replica has no source reference")

    # Materialize: copy source leaves into clone's own namespace
    now_ms = _now_ms()
    tables_materialized = 0
    allowed = root.get("replica_tables")

    target_super_dir = os.path.join(organization, super_name, "super")
    try:
        storage.makedirs(target_super_dir)
    except Exception:
        pass

    for leaf_item in catalog.scan_leaf_items(organization, source):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue
        if isinstance(allowed, list) and allowed and simple_name not in allowed:
            continue

        snapshot = _get_snapshot_from_leaf(leaf_item, storage)
        if not snapshot:
            continue

        # Skip virtual share leaves — they contain presigned URLs, not real data
        if _is_virtual_share_leaf(snapshot):
            continue

        _clone_single_table(
            catalog, storage, organization, source, super_name,
            simple_name, snapshot, now_ms,
        )
        tables_materialized += 1

    catalog.update_root_flags(organization, super_name, {
        "clone_type": "writable",
        "read_only": False,
        "promoted_from_replica_ms": now_ms,
        "replica_tables": None,
    })

    return {
        "ok": True,
        "super_name": super_name,
        "source": source,
        "tables_materialized": tables_materialized,
    }


# ---------------------------------------------------------------------------
# Detach — hard copy shared files to make clone independent
# ---------------------------------------------------------------------------

def detach_clone(organization: str, super_name: str) -> Dict[str, Any]:
    """Copy all shared files to the clone's own storage, making it independent.

    Scans the clone's snapshots, finds files that still live under the
    source's storage path, copies them to the clone's own path, and
    updates snapshot references.  Removes the ``cloned_from`` reference
    when done.

    For replicas: materializes first (promote), then detaches.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    root = catalog.get_root(organization, super_name)
    if not root or not root.get("cloned_from"):
        raise ValueError(f"'{super_name}' is not a clone")

    source = root["cloned_from"]

    # Replicas: materialize first, then detach
    if root.get("clone_type") == "replica":
        promote_replica(organization, super_name)
        # Re-read root after promotion
        root = catalog.get_root(organization, super_name)

    source_prefix = f"{organization}/{source}/"
    clone_prefix = f"{organization}/{super_name}/"
    files_copied = 0
    bytes_copied = 0

    # Collect table names first, then process each under its own lock
    table_names = []
    for leaf_item in catalog.scan_leaf_items(organization, super_name):
        simple_name = leaf_item.get("simple", "")
        if simple_name:
            table_names.append(simple_name)

    for simple_name in table_names:
        # Acquire per-table lock to prevent concurrent DataWriter conflicts
        token = catalog.acquire_simple_lock(organization, super_name, simple_name, ttl_s=60, timeout_s=120)
        if not token:
            logger.warning("[detach] Could not acquire lock for '%s', skipping", simple_name)
            continue

        try:
            # Re-read leaf under lock (may have changed since scan)
            leaf = catalog._get_leaf_raw(organization, super_name, simple_name)
            if not leaf:
                continue
            snapshot = leaf.get("payload") if isinstance(leaf, dict) else None
            if not isinstance(snapshot, dict):
                path = leaf.get("path", "")
                if path:
                    try:
                        snapshot = storage.read_json(path)
                    except Exception:
                        continue
            if not snapshot:
                continue

            # Skip virtual share leaves — not real data, nothing to detach
            if _is_virtual_share_leaf(snapshot):
                continue

            updated = False
            updated_resources = []
            for res in snapshot.get("resources", []):
                file_path = res.get("file", "")
                if not file_path:
                    updated_resources.append(res)
                    continue

                if file_path.startswith(source_prefix):
                    new_path = file_path.replace(source_prefix, clone_prefix, 1)
                    try:
                        data = storage.read_bytes(file_path)
                        storage.write_bytes(new_path, data)
                        files_copied += 1
                        bytes_copied += len(data)
                        res = dict(res)
                        res["file"] = new_path
                        updated = True
                    except Exception as e:
                        logger.warning("[detach] Failed to copy %s → %s: %s", file_path, new_path, e)

                updated_resources.append(res)

            if updated:
                snapshot["resources"] = updated_resources
                snap_path = leaf.get("path", "") if isinstance(leaf, dict) else ""
                if snap_path:
                    new_snap_path = snap_path.replace(source_prefix, clone_prefix, 1) if snap_path.startswith(source_prefix) else snap_path
                    storage.write_json(new_snap_path, snapshot)
                    try:
                        catalog.set_leaf_payload_cas(
                            organization, super_name, simple_name,
                            snapshot, new_snap_path, now_ms=_now_ms(),
                        )
                    except Exception:
                        catalog.set_leaf_path_cas(
                            organization, super_name, simple_name,
                            new_snap_path, now_ms=_now_ms(),
                        )
        finally:
            catalog.release_simple_lock(organization, super_name, simple_name, token)

    # Remove clone references — this SuperTable is now fully independent
    catalog.update_root_flags(organization, super_name, {
        "cloned_from": None,
        "clone_type": None,
        "clone_ts": None,
        "replica_tables": None,
        "detached_ms": _now_ms(),
        "detach_files_copied": files_copied,
        "detach_bytes_copied": bytes_copied,
    })

    return {
        "ok": True,
        "super_name": super_name,
        "source": source,
        "files_copied": files_copied,
        "bytes_copied": bytes_copied,
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
