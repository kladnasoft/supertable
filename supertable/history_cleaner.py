# history_cleaner.py

import os
from typing import List, Set

from supertable.config.defaults import logger
from supertable.super_table import SuperTable
from supertable.redis_catalog import RedisCatalog
from supertable.rbac.access_control import check_write_access


class HistoryCleaner:
    """
    Cleans stale snapshot JSONs and parquet data files.

    Works with the current architecture:
      - SuperTable "meta:root" lives in Redis (version + ts)
      - Each SimpleTable has a Redis leaf pointer -> path to the current heavy snapshot JSON on storage
      - Active data files are listed under the snapshot's "resources"

    Deletion policy (unchanged in spirit):
      - Only delete files whose leading timestamp in the filename (e.g., 1699999999999_*.parquet/.json)
        is <= a freshness threshold.
      - Threshold is derived as:
          threshold = max(root.ts (Redis), simple_table.last_updated_ms (snapshot JSON))
      - Never delete the current snapshot JSON or any files referenced in "resources".
    """

    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.storage = self.super_table.storage
        self.catalog = RedisCatalog()

    # --------------------------------------------------------------------- main

    def clean(self, user_hash: str) -> None:
        """
        Perform cleanup for:
          1) Legacy files possibly under the SuperTable folder (if any).
          2) All SimpleTables discovered via Redis leaf pointers.
        """
        # RBAC
        check_write_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            user_hash=user_hash,
            table_name=self.super_table.super_name,
        )

        org = self.super_table.organization
        sup = self.super_table.super_name

        # Root timestamp from Redis (fallback: 0)
        root_obj = self.catalog.get_root(org, sup) or {}
        root_ts = int(root_obj.get("ts", 0))

        total_deleted = 0

        # 1) Clean (legacy) files under the SuperTable folder, if present
        #    There is no "current super snapshot" file anymore, so all timestamped files
        #    not newer than root_ts can be considered stale.
        super_files = self.collect_files(self.super_table.super_dir)
        legacy_to_delete = self.get_files_to_delete(super_files, root_ts)
        total_deleted += self.delete_files(legacy_to_delete)
        if legacy_to_delete:
            logger.debug(f"{len(legacy_to_delete)} files cleaned in super dir: {self.super_table.super_dir}")

        # 2) Iterate all simple tables via Redis leaf pointers
        for leaf in self.catalog.scan_leaf_items(org, sup, count=1000):
            simple_name = leaf.get("simple")
            current_snapshot_path = leaf.get("path")

            if not current_snapshot_path:
                logger.debug(f"[history-cleaner] No snapshot path for simple '{simple_name}', skipping.")
                continue

            # Safely read snapshot JSON
            try:
                snapshot = self.storage.read_json(current_snapshot_path)
            except Exception as e:
                logger.debug(f"[history-cleaner] Cannot read snapshot for '{simple_name}': {e}")
                continue

            location = snapshot.get("location")
            if not location:
                logger.debug(f"[history-cleaner] No 'location' in snapshot for '{simple_name}', skipping.")
                continue

            # Active resources (keep!)
            active_files: Set[str] = set()
            for entry in snapshot.get("resources", []) or []:
                f = entry.get("file")
                if f:
                    active_files.add(f)

            # Candidate files in this simple table folder
            all_files = set(self.collect_files(location))

            # Never delete the current snapshot itself or any active resource files
            designated_files = list(all_files - active_files - {current_snapshot_path})

            # Use the stricter freshness threshold:
            # the newer of (root.ts from Redis) and (snapshot.last_updated_ms)
            simple_ts = int(snapshot.get("last_updated_ms", 0))
            threshold = max(root_ts, simple_ts)

            files_to_delete = self.get_files_to_delete(designated_files, threshold)
            total_deleted += self.delete_files(files_to_delete)

            logger.debug(f"{len(files_to_delete)} files cleaned for table: {simple_name}")

        logger.info(f"[history-cleaner] Total files deleted: {total_deleted}")

    # --------------------------------------------------------------------- utils

    def collect_files(self, location: str) -> List[str]:
        """
        Collect parquet & JSON files under 'location' in a storage-agnostic way.
        Handles missing folders gracefully.
        """
        out: List[str] = []
        try:
            out.extend(self.storage.list_files(os.path.join(location, "data"), "*.parquet"))
        except Exception:
            pass
        try:
            out.extend(self.storage.list_files(os.path.join(location, "snapshots"), "*.json"))
        except Exception:
            pass
        try:
            # legacy/extra JSONs possibly placed directly under the location root
            out.extend(self.storage.list_files(location, "*.json"))
        except Exception:
            pass
        return out

    def get_files_to_delete(self, designated_files: List[str], threshold_ms: int) -> List[str]:
        """
        Keep files whose leading numeric prefix (milliseconds epoch) is <= threshold_ms.
        Filename format expected: "<ms>_anything.ext"
        Non-conforming names are ignored (not deleted).
        """
        files_to_delete: List[str] = []
        for file in designated_files:
            filename = os.path.basename(file)
            ts_part = filename.split("_")[0]
            try:
                ts_val = int(ts_part)
                if ts_val <= threshold_ms:
                    files_to_delete.append(file)
            except ValueError:
                # Skip files not following the timestamped naming convention
                continue
        return files_to_delete

    def delete_files(self, files_to_delete: List[str]) -> int:
        """
        Deletes files via the storage interface. Returns count deleted.
        """
        deleted = 0
        for file in files_to_delete:
            try:
                self.storage.delete(file)
                deleted += 1
                logger.debug(f"Deleted file: {file}")
            except Exception as e:
                logger.debug(f"Skip delete (error) {file}: {e}")
        return deleted
