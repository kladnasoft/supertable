# route: supertable.services.supertable_manager
"""
Business logic for SuperTable lifecycle: list, clone, delete-protection,
and read-only flag management.

Used by api/api.py endpoint handlers.  No FastAPI dependencies.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supertable.config.settings import settings
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

def list_supertables_detailed(organization: str) -> List[Dict[str, Any]]:
    """Return all supertables with metadata: name, table count, root flags."""
    organization = (organization or "").strip()
    if not organization:
        return []

    catalog = RedisCatalog()
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

                # Count tables via leaf keys
                table_count = 0
                try:
                    leaf_pattern = f"supertable:{organization}:{sup_name}:meta:leaf:*"
                    tc_cursor = 0
                    while True:
                        tc_cursor, leaf_keys = catalog.r.scan(
                            cursor=tc_cursor, match=leaf_pattern, count=500
                        )
                        table_count += len(leaf_keys)
                        if tc_cursor == 0:
                            break
                except Exception:
                    pass

                results.append({
                    "name": sup_name,
                    "table_count": table_count,
                    "version": root.get("version", 0),
                    "ts": root.get("ts", 0),
                    "read_only": bool(root.get("read_only")),
                    "cloned_from": root.get("cloned_from"),
                    "clone_type": root.get("clone_type"),
                    "clone_ts": root.get("clone_ts"),
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
    """Check whether a supertable can be deleted.

    Returns ``{"ok": True}`` or ``{"ok": False, "reason": ..., "clones": [...]}``.
    """
    catalog = RedisCatalog()
    clones = catalog.find_readonly_clones(organization, super_name)
    if clones:
        return {
            "ok": False,
            "reason": f"Cannot delete: {len(clones)} read-only clone(s) reference this SuperTable.",
            "clones": clones,
        }
    return {"ok": True}


# ---------------------------------------------------------------------------
# Read-only toggle
# ---------------------------------------------------------------------------

def set_read_only(organization: str, super_name: str, enabled: bool) -> bool:
    """Set or clear the read_only flag on a supertable root."""
    catalog = RedisCatalog()
    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")
    return catalog.update_root_flags(organization, super_name, {"read_only": enabled})


# ---------------------------------------------------------------------------
# Clone — read-only (snapshot clone, zero data copy)
# ---------------------------------------------------------------------------

def clone_readonly(
    organization: str,
    source_name: str,
    target_name: str,
) -> Dict[str, Any]:
    """Create a read-only snapshot clone.

    The clone's tables reference the *source* data files directly.
    No parquet files are copied.  The clone is write-protected via the
    ``read_only`` flag on meta:root.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    # Validate source exists
    if not catalog.root_exists(organization, source_name):
        raise FileNotFoundError(f"Source SuperTable '{source_name}' not found")

    # Validate target does not exist
    if catalog.root_exists(organization, target_name):
        raise ValueError(f"Target SuperTable '{target_name}' already exists")

    now_ms = int(time.time() * 1000)

    # 1. Create target root in Redis
    catalog.ensure_root(organization, target_name)
    catalog.update_root_flags(organization, target_name, {
        "read_only": True,
        "cloned_from": source_name,
        "clone_type": "readonly",
        "clone_ts": now_ms,
    })

    # 2. Create minimal storage directory
    target_super_dir = os.path.join(organization, target_name, "super")
    try:
        storage.makedirs(target_super_dir)
    except Exception:
        pass

    # 3. Copy latest snapshot for each table (references source data paths)
    tables_cloned = 0
    for leaf_item in catalog.scan_leaf_items(organization, source_name):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue

        # Read source snapshot payload (prefer Redis payload, fall back to storage)
        snapshot = None
        payload = leaf_item.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
            snapshot = payload
        else:
            src_path = leaf_item.get("path", "")
            if src_path:
                try:
                    snapshot = storage.read_json(src_path)
                except Exception:
                    logger.warning("clone_readonly: cannot read snapshot %s", src_path)
                    continue

        if not snapshot:
            continue

        # Build clone snapshot — same resources (pointing to source data), reset version
        clone_snapshot = dict(snapshot)
        clone_snapshot["snapshot_version"] = 0
        clone_snapshot["previous_snapshot"] = None
        clone_snapshot["last_updated_ms"] = now_ms
        # resources[].file paths stay as-is — they point to source storage

        # Write snapshot JSON to target's snapshot dir
        target_snap_dir = os.path.join(
            organization, target_name, "tables", simple_name, "snapshots"
        )
        try:
            storage.makedirs(target_snap_dir)
        except Exception:
            pass

        snap_filename = f"tables_{uuid.uuid4().hex[:12]}.json"
        target_snap_path = os.path.join(target_snap_dir, snap_filename)
        storage.write_json(target_snap_path, clone_snapshot)

        # Create leaf pointer in Redis
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

        tables_cloned += 1

    # 4. Initialize RBAC scaffolding (creates default superadmin role)
    try:
        from supertable.rbac.role_manager import RoleManager
        from supertable.rbac.user_manager import UserManager
        RoleManager(super_name=target_name, organization=organization)
        UserManager(super_name=target_name, organization=organization)
    except Exception as e:
        logger.warning("clone_readonly: RBAC init for '%s' failed: %s", target_name, e)

    return {
        "ok": True,
        "source": source_name,
        "target": target_name,
        "clone_type": "readonly",
        "tables_cloned": tables_cloned,
    }


# ---------------------------------------------------------------------------
# Clone — full (deep copy of data + RBAC roles)
# ---------------------------------------------------------------------------

def clone_full(
    organization: str,
    source_name: str,
    target_name: str,
) -> Dict[str, Any]:
    """Create a fully independent clone.

    Copies all parquet data files, rewrites snapshot paths, and clones
    RBAC roles.  The result is a standalone SuperTable that shares
    nothing with the source.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    # Validate source exists
    if not catalog.root_exists(organization, source_name):
        raise FileNotFoundError(f"Source SuperTable '{source_name}' not found")

    # Validate target does not exist
    if catalog.root_exists(organization, target_name):
        raise ValueError(f"Target SuperTable '{target_name}' already exists")

    now_ms = int(time.time() * 1000)

    # 1. Create target root
    catalog.ensure_root(organization, target_name)
    catalog.update_root_flags(organization, target_name, {
        "read_only": False,
        "cloned_from": source_name,
        "clone_type": "full",
        "clone_ts": now_ms,
    })

    # 2. Clone tables (data + snapshots)
    tables_cloned = 0
    files_copied = 0

    for leaf_item in catalog.scan_leaf_items(organization, source_name):
        simple_name = leaf_item.get("simple", "")
        if not simple_name:
            continue

        # Read source snapshot
        snapshot = None
        payload = leaf_item.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
            snapshot = payload
        else:
            src_path = leaf_item.get("path", "")
            if src_path:
                try:
                    snapshot = storage.read_json(src_path)
                except Exception:
                    logger.warning("clone_full: cannot read snapshot %s", src_path)
                    continue

        if not snapshot:
            continue

        # Create target directories
        target_data_dir = os.path.join(
            organization, target_name, "tables", simple_name, "data"
        )
        target_snap_dir = os.path.join(
            organization, target_name, "tables", simple_name, "snapshots"
        )
        for d in (target_data_dir, target_snap_dir):
            try:
                storage.makedirs(d)
            except Exception:
                pass

        # Copy data files and rewrite resource paths
        source_prefix = os.path.join(organization, source_name) + os.sep
        target_prefix = os.path.join(organization, target_name) + os.sep
        # Also handle forward-slash (object storage normalizes to /)
        source_prefix_fwd = f"{organization}/{source_name}/"
        target_prefix_fwd = f"{organization}/{target_name}/"

        new_resources = []
        for res in snapshot.get("resources", []):
            src_file = res.get("file", "")
            if not src_file:
                new_resources.append(res)
                continue

            # Compute target path by replacing the source prefix
            if src_file.startswith(source_prefix_fwd):
                tgt_file = target_prefix_fwd + src_file[len(source_prefix_fwd):]
            elif src_file.startswith(source_prefix):
                tgt_file = target_prefix + src_file[len(source_prefix):]
            else:
                # Relative or unexpected path — copy to target data dir
                basename = os.path.basename(src_file)
                tgt_file = os.path.join(target_data_dir, basename)

            # Copy the parquet file
            try:
                storage.copy(src_file, tgt_file)
                files_copied += 1
            except Exception as e:
                logger.warning("clone_full: copy failed %s → %s: %s", src_file, tgt_file, e)
                # Still include the resource with the new path
                pass

            new_res = dict(res)
            new_res["file"] = tgt_file
            new_resources.append(new_res)

        # Build clone snapshot with rewritten paths
        clone_snapshot = dict(snapshot)
        clone_snapshot["resources"] = new_resources
        clone_snapshot["snapshot_version"] = 0
        clone_snapshot["previous_snapshot"] = None
        clone_snapshot["last_updated_ms"] = now_ms

        snap_filename = f"tables_{uuid.uuid4().hex[:12]}.json"
        target_snap_path = os.path.join(target_snap_dir, snap_filename)
        storage.write_json(target_snap_path, clone_snapshot)

        # Create leaf pointer in Redis
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

        tables_cloned += 1

    # 3. Clone RBAC roles (not users — those are org-scoped tokens)
    roles_cloned = 0
    try:
        source_roles = catalog.get_roles(organization, source_name)
        for role_doc in source_roles:
            role_type = role_doc.get("role", "")
            role_name = role_doc.get("role_name", "")
            # Skip superadmin — already auto-created by target's RoleManager init
            if role_type == "superadmin" or role_name == "superadmin":
                continue
            new_role_id = uuid.uuid4().hex
            new_doc = dict(role_doc)
            new_doc["role_id"] = new_role_id
            try:
                catalog.rbac_create_role(organization, target_name, new_role_id, new_doc)
                roles_cloned += 1
            except Exception as e:
                logger.warning("clone_full: role clone failed: %s", e)
    except Exception as e:
        logger.warning("clone_full: RBAC clone failed: %s", e)

    # 4. Initialize RBAC scaffolding for target
    try:
        from supertable.rbac.role_manager import RoleManager
        from supertable.rbac.user_manager import UserManager
        RoleManager(super_name=target_name, organization=organization)
        UserManager(super_name=target_name, organization=organization)
    except Exception as e:
        logger.warning("clone_full: RBAC init for '%s' failed: %s", target_name, e)

    return {
        "ok": True,
        "source": source_name,
        "target": target_name,
        "clone_type": "full",
        "tables_cloned": tables_cloned,
        "files_copied": files_copied,
        "roles_cloned": roles_cloned,
    }
