import os
from datetime import datetime
from typing import Dict, Any, List

from supertable.locking import Locking
from supertable.config.defaults import default, logger
from supertable.rbac.access_control import check_write_access
from supertable.utils.helper import generate_filename

# Storage backend
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface

from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

from supertable.mirroring.mirror_formats import MirrorFormats


def read_super_table(super_table_meta: Dict[str, Any], storage: StorageInterface) -> Dict[str, Any]:
    """
    Read the JSON file pointed by super_table_meta['current'] using the provided storage backend.
    """
    super_table_path = super_table_meta.get("current")
    if not super_table_path:
        raise FileNotFoundError(f"No 'current' path in super_table_meta: {super_table_meta}")
    return storage.read_json(super_table_path)


class SuperTable:
    def __init__(self, super_name: str, organization: str):
        """
        Initialize the super table abstraction. Storage backend is selected by configuration.
        """
        self.identity = "super"
        self.super_name = super_name
        self.organization = organization

        self.storage = get_storage()

        self.super_dir = os.path.join(self.organization, self.super_name, self.identity)
        self.super_meta_path = os.path.join(self.organization, self.super_name, "_super.json")
        logger.debug(f"super_dir: {self.super_dir}")
        logger.debug(f"super_meta: {self.super_meta_path}")

        # One locking coordinator for all super/meta operations.
        self.locking = Locking(identity=self.super_name, working_dir=self.super_dir)
        self.init_super_table()

    # ------------------------------------------------------------------ init
    def init_super_table(self) -> None:
        self.storage.makedirs(self.super_dir)

        if not self.storage.exists(self.super_meta_path):
            initial_super = {
                "last_updated_ms": int(datetime.now().timestamp() * 1000),
                "version": 0,
                "tables": 0,
                "snapshots": [],
            }
            new_super_path = os.path.join(self.super_dir, generate_filename(alias=self.identity))
            self.storage.write_json(new_super_path, initial_super)

            # NOTE: add and persist mirrors in meta (empty by default)
            meta_data = {
                "current": new_super_path,
                "previous": None,
                "version": 0,
                "tables": 0,
                "format_mirrors": [],  # <-- mirrors persisted in meta
            }
            self.storage.write_json(self.super_meta_path, meta_data)

            # Initialize RBAC scaffolding
            RoleManager(super_name=self.super_name, organization=self.organization)
            UserManager(super_name=self.super_name, organization=self.organization)

    # --------------------------------------------------------------- utilities
    def delete(self, user_hash: str):
        check_write_access(super_name=self.super_name,
                           organization=self.organization,
                           user_hash=user_hash,
                           table_name=self.super_name)

        super_table_folder = os.path.join(self.organization, self.super_name)
        self.storage.delete(super_table_folder)
        logger.info(f"Deleted Supertable: {super_table_folder}")

    # ------------------------------ meta getters (with/without locking)
    def get_super_meta_with_lock(self) -> Dict[str, Any]:
        """
        Acquire a short, exclusive lock on the *meta path resource*, read meta JSON,
        and release the lock. Using the same resource as writers ensures readers
        contend correctly with updates.
        """
        timeout = default.DEFAULT_TIMEOUT_SEC
        duration = default.DEFAULT_LOCK_DURATION_SEC

        acquired = self.locking.lock_resources(
            [self.super_meta_path],
            timeout_seconds=timeout,
            lock_duration_seconds=duration,
        )
        try:
            if not acquired:
                raise RuntimeError(f"Failed to acquire lock for super table meta: {self.super_meta_path}")

            if not self.storage.exists(self.super_meta_path):
                raise FileNotFoundError(f"Super table meta file not found: {self.super_meta_path}")
            if self.storage.size(self.super_meta_path) == 0:
                raise ValueError(f"Super table meta file is empty: {self.super_meta_path}")

            return self.storage.read_json(self.super_meta_path)
        finally:
            try:
                self.locking.release_lock([self.super_meta_path])
            except Exception:
                pass

    def get_super_meta_with_shared_lock(self) -> Dict[str, Any]:
        """
        Acquire a very short, exclusive lock on the meta path and read meta via storage.
        (Name kept for compatibility; backend does not implement true shared locks.)
        """
        timeout = default.DEFAULT_TIMEOUT_SEC
        duration = default.DEFAULT_LOCK_DURATION_SEC

        acquired = self.locking.lock_resources(
            [self.super_meta_path],
            timeout_seconds=timeout,
            lock_duration_seconds=duration,
        )
        try:
            if not acquired:
                # Tolerate by returning empty to callers that handle it defensively.
                return {}

            if not self.storage.exists(self.super_meta_path):
                return {}
            if self.storage.size(self.super_meta_path) == 0:
                return {}

            return self.storage.read_json(self.super_meta_path)
        finally:
            try:
                self.locking.release_lock([self.super_meta_path])
            except Exception:
                pass

    def get_super_meta(self) -> Dict[str, Any]:
        """
        Read meta JSON without taking any locks (caller must ensure safety).
        """
        if not self.storage.exists(self.super_meta_path):
            raise FileNotFoundError(f"Super table meta file not found: {self.super_meta_path}")
        if self.storage.size(self.super_meta_path) == 0:
            raise ValueError(f"Super table meta file is empty: {self.super_meta_path}")
        return self.storage.read_json(self.super_meta_path)

    def get_super_path(self) -> str:
        super_table = self.get_super_meta()
        return super_table.get("current", "")

    def get_super_path_with_lock(self) -> str:
        super_table = self.get_super_meta_with_lock()
        return super_table.get("current", "")

    def get_super_table(self) -> Dict[str, Any]:
        super_table_meta = self.get_super_meta()
        return read_super_table(super_table_meta, self.storage)

    def get_super_table_with_lock(self) -> Dict[str, Any]:
        super_table_meta = self.get_super_meta_with_lock()
        return read_super_table(super_table_meta, self.storage)

    def get_super_table_and_path_with_lock(self):
        super_table_meta = self.get_super_meta_with_lock()
        current_path = super_table_meta.get("current", "")
        return (
            read_super_table(super_table_meta, self.storage),
            current_path,
            super_table_meta,
        )

    def get_super_table_and_path_with_shared_lock(self):
        super_table_meta = self.get_super_meta_with_shared_lock()
        current_path = super_table_meta.get("current", "")
        return (
            read_super_table(super_table_meta, self.storage),
            current_path,
            super_table_meta,
        )

    def read_simple_table_snapshot(self, simple_table_path: str) -> Dict[str, Any]:
        if not simple_table_path or not self.storage.exists(simple_table_path):
            raise FileNotFoundError(f"Simple table snapshot not found: {simple_table_path}")
        if self.storage.size(simple_table_path) == 0:
            raise ValueError(f"Simple table snapshot is empty: {simple_table_path}")
        return self.storage.read_json(simple_table_path)

    # ---------------------------------------------------------- update helpers
    def update_super_table(self, table_name, simple_table_path, simple_table_content):
        """
        Update the super-table snapshot and meta to point to `simple_table_path`.

        DEADLOCK PREVENTION:
        This method is called while the caller already holds the meta-path lock
        via `update_with_lock()`. Therefore, DO NOT acquire the same lock again
        from inside this method. Read meta WITHOUT locking here.
        """
        files = len(simple_table_content)
        rows = sum(item["rows"] for item in simple_table_content)
        file_size = sum(item["file_size"] for item in simple_table_content)

        # Read without lock (outer caller holds it)
        last_super_meta = self.get_super_meta()
        last_super_path = last_super_meta.get("current", "")
        last_super_table = read_super_table(last_super_meta, self.storage)

        # Preserve mirrors across meta rewrites
        mirrors: List[str] = list(last_super_meta.get("format_mirrors", []))  # <-- preserve mirrors

        last_updated_ms = int(datetime.now().timestamp() * 1000)
        last_super_version = last_super_table.get("version", 0)

        new_super_snapshot = {
            "last_updated_ms": last_updated_ms,
            "version": last_super_version + 1,
            "snapshots": [
                snap for snap in last_super_table["snapshots"]
                if snap["table_name"] != table_name
            ],
        }
        new_super_snapshot["snapshots"].append({
            "table_name": table_name,
            "last_updated_ms": last_updated_ms,
            "path": simple_table_path,
            "files": files,
            "rows": rows,
            "file_size": file_size,
        })

        new_super_snapshot_path = os.path.join(
            self.super_dir, generate_filename(alias=self.identity)
        )
        snapshots = new_super_snapshot["snapshots"]
        new_super_snapshot["tables"] = len(snapshots)

        # Summaries
        file_count = sum(s["files"] for s in snapshots)
        total_rows = sum(s["rows"] for s in snapshots)
        total_file_size = sum(s["file_size"] for s in snapshots)

        # Write the new snapshot
        self.storage.write_json(new_super_snapshot_path, new_super_snapshot)

        # Update the main meta pointer (preserve mirrors)
        meta_data = {
            "current": new_super_snapshot_path,
            "previous": last_super_path,
            "last_updated_ms": int(datetime.now().timestamp() * 1000),
            "file_count": file_count,
            "total_rows": total_rows,
            "total_file_size": total_file_size,
            "tables": len(snapshots),
            "version": last_super_version + 1,
            "format_mirrors": mirrors,
        }
        self.storage.write_json(self.super_meta_path, meta_data)

        # ---- MIRROR (if enabled)
        try:
            # Read the full simple-table snapshot for schema + resources
            simple_snapshot = self.read_simple_table_snapshot(simple_table_path)
            MirrorFormats.mirror_if_enabled(
                super_table=self,
                table_name=table_name,
                simple_snapshot=simple_snapshot,
                mirrors=mirrors,  # avoid re-reading meta here
            )
        except Exception as e:
            # Non-fatal: we keep the super catalog consistent even if mirror fails
            logger.error(f"[mirror] Failed to write mirrors for table '{table_name}': {e}")

    def update_with_lock(self, table_name, simple_table_path, simple_table_content):
        """
        Public API to update super snapshot/meta.
        Locks the meta path resource so readers and writers contend on the same key.
        """
        timeout = default.DEFAULT_TIMEOUT_SEC
        duration = default.DEFAULT_LOCK_DURATION_SEC

        if not self.locking.lock_resources(
            [self.super_meta_path],
            timeout_seconds=timeout,
            lock_duration_seconds=duration,
        ):
            raise RuntimeError("Failed to acquire lock for meta resources")

        try:
            # We now hold the exclusive meta-path lock → safe to call the unlocked updater.
            self.update_super_table(table_name, simple_table_path, simple_table_content)
        finally:
            try:
                self.locking.release_lock([self.super_meta_path])
            except Exception:
                pass

    # -------------------------- remove helpers (deadlock-safe like update)
    def _remove_table_unlocked(self, table_name: str):
        """
        Internal helper that performs the meta/snapshot update to remove a table.
        Caller MUST hold the meta-path exclusive lock already. This function must NOT lock.
        """
        # Read without taking a lock (outer caller holds it)
        last_super_meta = self.get_super_meta()
        last_super_path = last_super_meta.get("current", "")
        last_super_table = read_super_table(last_super_meta, self.storage)

        # Preserve mirrors across meta rewrites
        mirrors: List[str] = list(last_super_meta.get("format_mirrors", []))

        last_updated_ms = int(datetime.now().timestamp() * 1000)
        last_super_version = last_super_table.get("version", 0)

        # Drop the target table snapshot from the list
        new_snapshots = [
            snap for snap in last_super_table.get("snapshots", [])
            if snap.get("table_name") != table_name
        ]
        new_super_snapshot = {
            "last_updated_ms": last_updated_ms,
            "version": last_super_version + 1,
            "snapshots": new_snapshots,
            "tables": len(new_snapshots),
        }

        new_super_snapshot_path = os.path.join(
            self.super_dir, generate_filename(alias=self.identity)
        )

        # Summaries
        file_count = sum(int(s.get("files", 0)) for s in new_snapshots)
        total_rows = sum(int(s.get("rows", 0)) for s in new_snapshots)
        total_file_size = sum(int(s.get("file_size", 0)) for s in new_snapshots)

        # Write the new snapshot & update meta pointer
        self.storage.write_json(new_super_snapshot_path, new_super_snapshot)
        meta_data = {
            "current": new_super_snapshot_path,
            "previous": last_super_path,
            "last_updated_ms": int(datetime.now().timestamp() * 1000),
            "file_count": file_count,
            "total_rows": total_rows,
            "total_file_size": total_file_size,
            "tables": len(new_snapshots),
            "version": last_super_version + 1,
            "format_mirrors": mirrors,
        }
        self.storage.write_json(self.super_meta_path, meta_data)

    def remove_table(self, table_name: str):
        """
        Deprecated external use. Use remove_table_with_lock().
        Kept for backward-compat but now assumes LOCK IS ALREADY HELD.
        """
        self._remove_table_unlocked(table_name)

    def remove_table_with_lock(self, table_name: str):
        """
        Public API to remove a table from the super table.
        Acquires the meta-path exclusive lock once and avoids self-deadlock by calling
        the *_unlocked() variant internally.
        """
        timeout = default.DEFAULT_TIMEOUT_SEC
        duration = default.DEFAULT_LOCK_DURATION_SEC

        if not self.locking.lock_resources(
            [self.super_meta_path],
            timeout_seconds=timeout,
            lock_duration_seconds=duration,
        ):
            raise RuntimeError("Failed to acquire lock for meta resources")

        try:
            self._remove_table_unlocked(table_name)
        finally:
            try:
                self.locking.release_lock([self.super_meta_path])
            except Exception:
                pass
