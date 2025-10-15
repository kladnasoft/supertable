# supertable/simple_table.py

from __future__ import annotations

import os
from datetime import datetime

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable
from supertable.utils.helper import collect_schema, generate_filename
from supertable.rbac.access_control import check_write_access


class SimpleTable:
    """
    Simple-table layout on storage (heavy data) + Redis leaf pointer (meta).
    """

    def __init__(self, super_table: SuperTable, simple_name: str):
        self.super_table = super_table
        self.identity = "tables"
        self.simple_name = simple_name

        # Storage is the same as SuperTable's
        self.storage = self.super_table.storage
        self.catalog = RedisCatalog()


        # Data layout
        self.simple_dir = os.path.join(
            super_table.organization, super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")

        logger.debug(f"simple_dir: {self.simple_dir}")
        logger.debug(f"data_dir: {self.data_dir}")
        logger.debug(f"snapshot_dir: {self.snapshot_dir}")

        # Fast path: if meta:leaf exists, don't touch storage
        if self.catalog.leaf_exists(
                self.super_table.organization, self.super_table.super_name, self.simple_name
        ):
            logger.debug(
                f"[SimpleTable] Leaf exists in Redis for "
                f"{self.super_table.organization}/{self.super_table.super_name}/{self.simple_name}; "
                f"skipping storage mkdirs and bootstrap."
            )
            return

        self.init_simple_table()

    def init_simple_table(self) -> None:
        """
        Initialize simple table:
          * If Redis meta:leaf already exists -> skip any folder checks/creations and bootstrapping.
          * Otherwise, create folders and bootstrap an initial empty snapshot and leaf pointer.
        """

        # First-time initialization: ensure directories in storage (best-effort)
        for p in (self.simple_dir, self.data_dir, self.snapshot_dir):
            try:
                if not self.storage.exists(p):
                    self.storage.makedirs(p)
            except Exception:
                # Object storage may no-op; that's fine
                pass

        # Bootstrap leaf pointer in Redis (version=0, empty initial snapshot)
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
        self.storage.write_json(new_simple_path, snapshot_data)

        # CAS set leaf to that path (version becomes 0)
        self.catalog.set_leaf_path_cas(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
            new_simple_path,
            now_ms=int(datetime.now().timestamp() * 1000),
        )

    def delete(self, user_hash: str) -> None:
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            user_hash=user_hash,
            table_name=self.simple_name,
        )

        # Remove folder (heavy data) from storage
        simple_table_folder = os.path.join(
            self.super_table.organization, self.super_table.super_name, self.identity, self.simple_name
        )
        self.storage.delete(simple_table_folder)

        logger.info(f"Deleted Table (storage): {simple_table_folder}")

    def get_simple_table_snapshot(self):
        """
        Read the current heavy snapshot via the Redis leaf pointer.
        """
        ptr = self.catalog.get_leaf(self.super_table.organization, self.super_table.super_name, self.simple_name)
        if not ptr or not ptr.get("path"):
            raise FileNotFoundError("No path found in simple table leaf pointer.")
        path = ptr["path"]
        data = self.storage.read_json(path)
        return data, path

    def update(self, new_resources, sunset_files, model_df):
        """
        Build and write a new heavy snapshot on storage.
        Returns: (snapshot_dict, snapshot_path)
        """
        # Read current snapshot
        last_simple_table, last_simple_table_path = self.get_simple_table_snapshot()

        current_resources = last_simple_table.get("resources", [])
        updated_resources = [res for res in current_resources if res.get("file") not in set(sunset_files)]
        updated_resources.extend(new_resources)
        last_simple_table["resources"] = updated_resources

        # Update metadata
        last_simple_table["previous_snapshot"] = last_simple_table_path
        last_simple_table["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        last_simple_table["snapshot_version"] = int(last_simple_table.get("snapshot_version", 0)) + 1
        last_simple_table["schema"] = collect_schema(model_df)

        # Write new heavy snapshot file
        new_simple_path = os.path.join(self.snapshot_dir, generate_filename(alias=self.identity))
        self.storage.write_json(new_simple_path, last_simple_table)

        return last_simple_table, new_simple_path
