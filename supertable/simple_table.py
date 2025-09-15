import os

from datetime import datetime
from supertable.config.defaults import logger
from supertable.locking import Locking
from supertable.super_table import SuperTable
from supertable.utils.helper import collect_schema, generate_filename
from supertable.config.defaults import default
from supertable.rbac.access_control import check_write_access


class SimpleTable:
    def __init__(self, super_table: SuperTable, simple_name: str):
        self.super_table = super_table
        self.identity = "simple"
        self.simple_name = simple_name

        # We'll use the same storage backend the SuperTable is using.
        self.storage = self.super_table.storage

        self.simple_dir = os.path.join(super_table.organization,
            super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")
        logger.debug(f"simple_dir: {self.simple_dir}")
        logger.debug(f"data_dir: {self.data_dir}")
        logger.debug(f"snapshot_dir: {self.snapshot_dir}")

        self.init_simple_table()

        self.locking = Locking(identity=self.simple_name, working_dir=self.simple_dir)

    def init_simple_table(self):
        # Create directories in the configured storage (may be a no-op for some backends).
        if not self.storage.exists(self.simple_dir):
            self.storage.makedirs(self.simple_dir)
        if not self.storage.exists(self.data_dir):
            self.storage.makedirs(self.data_dir)
        if not self.storage.exists(self.snapshot_dir):
            self.storage.makedirs(self.snapshot_dir)

        # --- FIX: read super-table metadata WITHOUT taking a shared lock to avoid self-deadlock ---
        super_snapshot = self.super_table.get_super_table()  # no lock
        snapshots = super_snapshot.get("snapshots", [])

        has_entry = any(s.get("table_name") == self.simple_name for s in snapshots)
        if not has_entry:
            initial_snapshot_file = generate_filename(alias=self.identity)
            new_simple_path = os.path.join(self.snapshot_dir, initial_snapshot_file)

            snapshot_data = {
                "simple_name": self.simple_name,
                "location": self.simple_dir,
                "snapshot_version": 0,
                "last_updated_ms": int(datetime.now().timestamp() * 1000),
                "previous_snapshot": None,
                "schema": [],
                "resources": [],
            }

            self.write_snapshot_file(new_simple_path, snapshot_data)
            # This acquires the exclusive lock on the super table as needed.
            self.super_table.update_with_lock(self.simple_name, new_simple_path, [])

    def delete(self, user_hash: str):
        check_write_access(super_name=self.super_table.super_name,
                           organization=self.super_table.organization,
                           user_hash=user_hash,
                           table_name=self.simple_name)

        self.super_table.remove_table_with_lock(self.simple_name)
        simple_table_folder = os.path.join(self.super_table.organization, self.super_table.super_name, self.identity, self.simple_name)
        self.storage.delete(simple_table_folder)

        logger.info(f"Deleted Table: {simple_table_folder}")


    def _any_existing_snapshots(self) -> bool:
        """
        Checks if the snapshot directory has any existing snapshot file.
        If the storage is remote, this logic may vary. For local storage,
        we'll assume it checks the directory content.
        """
        if hasattr(self.storage, "exists") and default.STORAGE_TYPE.upper() == "LOCAL":
            if os.path.exists(self.snapshot_dir) and os.path.isdir(self.snapshot_dir):
                return len(os.listdir(self.snapshot_dir)) > 0
        return False

    def get_simple_meta_with_lock(self):
        super_table = self.super_table.get_super_table_with_lock()
        snapshots = super_table.get("snapshots", {})

        for snapshot in snapshots:
            if snapshot.get("table_name") == self.simple_name:
                return snapshot

        return None

    def get_simple_meta_with_shared_lock(self):
        super_table, _, _ = self.super_table.get_super_table_and_path_with_shared_lock()
        snapshots = super_table.get("snapshots", {})

        for snapshot in snapshots:
            if snapshot.get("table_name") == self.simple_name:
                return snapshot

        return {}

    def get_simple_table_with_lock(self):
        self.super_table.locking.lock_resources(self.simple_name)
        simple_meta = self.get_simple_meta_with_shared_lock()
        simple_table_path = simple_meta.get("path")

        if not simple_table_path:
            raise FileNotFoundError("No path found in simple table metadata.")

        # Use the storage read instead of direct open(...)
        simple_table = self.storage.read_json(simple_table_path)
        return simple_table, simple_table_path

    def get_simple_table_with_shared_lock(self):
        simple_meta = self.get_simple_meta_with_shared_lock()
        simple_table_path = simple_meta.get("path", "")

        if not simple_table_path:
            raise FileNotFoundError("No path found in simple table metadata.")

        # Use the storage read instead of direct open(...)
        simple_table = self.storage.read_json(simple_table_path)
        return simple_table, simple_table_path

    def update(
        self,
        new_resources,
        sunset_files,
        model_df,
    ):

        # Read current snapshot path and data
        last_simple_table, last_simple_table_path = (
            self.get_simple_table_with_shared_lock()
        )

        current_resources = last_simple_table.get("resources", {})

        updated_resources = [
            res for res in current_resources if res["file"] not in sunset_files
        ]

        # Add new resources to the updated resources
        updated_resources.extend(new_resources)
        last_simple_table["resources"] = updated_resources

        # Update snapshot metadata
        last_simple_table["previous_snapshot"] = last_simple_table_path
        last_simple_table["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        last_simple_table["snapshot_version"] += 1

        # Update schema
        last_simple_table["schema"] = collect_schema(model_df)

        # Write the updated snapshot data to a file
        new_simple_path = os.path.join(
            self.snapshot_dir, generate_filename(alias=self.identity)
        )
        self.write_snapshot_file(new_simple_path, last_simple_table)

        return updated_resources, new_simple_path

    def write_snapshot_file(self, snapshot_path, snapshot_data):
        # Use storage interface to write JSON, instead of open(...)
        self.storage.write_json(snapshot_path, snapshot_data)
