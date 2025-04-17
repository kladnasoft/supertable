import os
from datetime import datetime
from typing import Dict, Any

from supertable.locking import Locking
from supertable.config.defaults import default, logger
from supertable.rbac.access_control import check_write_access
from supertable.utils.helper import generate_filename
from supertable.config.homedir import app_home

# Import the factory
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface

from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager


def read_super_table(super_table_meta: Dict[str, Any], storage: StorageInterface) -> Dict[str, Any]:
    """
    Reads the JSON file indicated by super_table_meta['current'] using the provided storage backend.
    """
    super_table_path = super_table_meta.get("current")
    if not super_table_path:
        raise FileNotFoundError(f"No 'current' path in super_table_meta: {super_table_meta}")
    return storage.read_json(super_table_path)


class SuperTable:
    def __init__(self, super_name: str, organization: str):
        """
        Only input is super_name. The storage backend is chosen via default.STORAGE_TYPE.
        """
        self.identity = "super"
        self.super_name = super_name
        self.organization = organization

        # Always call the factory to determine which storage to use
        self.storage = get_storage()

        self.super_dir = os.path.join(self.organization, self.super_name, self.identity)
        self.super_meta_path = os.path.join(self.organization, self.super_name, "_super.json")
        logger.debug(f"super_dir: {self.super_dir}")
        logger.debug(f"super_meta: {self.super_meta_path}")
        self.locking = Locking(identity=self.super_name, working_dir=self.super_dir)
        self.init_super_table()

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

            meta_data = {
                "current": new_super_path,
                "previous": None,
                "version": 0,
                "tables": 0,
            }
            self.storage.write_json(self.super_meta_path, meta_data)

            role_manager = RoleManager(super_name=self.super_name, organization=self.organization)
            user_manager = UserManager(super_name=self.super_name, organization=self.organization)

    def delete(self, user_hash: str):
        check_write_access(super_name=self.super_name,
                           organization=self.organization,
                           user_hash=user_hash,
                           table_name=self.super_name)

        super_table_folder = os.path.join(self.organization, self.super_name)
        self.storage.delete(super_table_folder)

        logger.info(f"Deleted Supertable: {super_table_folder}")



    def get_super_meta_with_lock(self) -> Dict[str, Any]:
        try:
            locked = self.locking.self_lock(
                timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
                lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC,
            )
            if not locked:
                raise RuntimeError(f"Failed to acquire locks for super table: {self.super_meta_path}")

            if not self.storage.exists(self.super_meta_path):
                raise FileNotFoundError(f"Super table meta file not found: {self.super_meta_path}")

            if self.storage.size(self.super_meta_path) == 0:
                raise ValueError(f"Super table meta file is empty: {self.super_meta_path}")

            return self.storage.read_json(self.super_meta_path)

        finally:
            self.locking.release_lock()

    def get_super_meta_with_shared_lock(self) -> Dict[str, Any]:
        """
        Retains the existing local-based shared-lock approach (lock_shared_and_read).
        For remote backends, you'd have to implement equivalent read locks if needed.
        """
        last_super_table = self.locking.lock_shared_and_read(self.super_meta_path)
        return last_super_table

    def get_super_meta(self) -> Dict[str, Any]:
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

    def update_super_table(self, table_name, simple_table_path, simple_table_content):
        files = len(simple_table_content)
        rows = sum(item["rows"] for item in simple_table_content)
        file_size = sum(item["file_size"] for item in simple_table_content)

        last_super_table, last_super_path, _ = self.get_super_table_and_path_with_lock()
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

        # Update the main meta pointer
        meta_data = {
            "current": new_super_snapshot_path,
            "previous": last_super_path,
            "last_updated_ms": int(datetime.now().timestamp() * 1000),
            "file_count": file_count,
            "total_rows": total_rows,
            "total_file_size": total_file_size,
            "tables": len(snapshots),
            "version": last_super_version + 1,
        }
        self.storage.write_json(self.super_meta_path, meta_data)

    def update_with_lock(self, table_name, simple_table_path, simple_table_content):
        if not self.locking.self_lock(
            timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
            lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC,
        ):
            raise RuntimeError("Failed to acquire locks for meta resources")

        self.update_super_table(table_name, simple_table_path, simple_table_content)
        self.locking.release_lock()

    def remove_table(self, table_name):
        last_super_table, last_super_path, _ = self.get_super_table_and_path_with_lock()
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

        # Update the main meta pointer
        meta_data = {
            "current": new_super_snapshot_path,
            "previous": last_super_path,
            "last_updated_ms": int(datetime.now().timestamp() * 1000),
            "file_count": file_count,
            "total_rows": total_rows,
            "total_file_size": total_file_size,
            "tables": len(snapshots),
            "version": last_super_version + 1,
        }
        self.storage.write_json(self.super_meta_path, meta_data)

    def remove_table_with_lock(self, table_name):
        if not self.locking.self_lock(
            timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
            lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC,
        ):
            raise RuntimeError("Failed to acquire locks for meta resources")

        self.remove_table(table_name)
        self.locking.release_lock()