import os
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa

from supertable.super_table import SuperTable
from supertable.config.defaults import logger


class StagingArea:
    """
    Lightweight staging area that reuses the SuperTable's storage backend.

    Improvements:
    - Safer timestamping (UTC, sortable).
    - More resilient parquet writer (fallback to pyarrow if storage expects a file on disk).
    - `read_parquet` now gracefully handles both:
        • a relative path like "mytable/1694548200000_file.parquet", or
        • just a bare filename; if not found at root, it searches subfolders.
    - Added small helpers & clearer logging.
    """

    def __init__(self, super_table: SuperTable, organization: str):
        self.super_table = super_table
        self.identity = "staging"
        self.organization = organization

        # Reuse the same storage interface as super_table
        self.storage = self.super_table.storage

        # Directory/prefix for staging
        self.staging_dir = os.path.join(self.organization, super_table.super_name, self.identity)
        logger.debug(f"staging_dir: {self.staging_dir}")
        self.init_staging_area()

    # --------------------------------------------------------------------- #
    # Init / structure
    # --------------------------------------------------------------------- #
    def init_staging_area(self) -> None:
        """Ensure that the staging directory exists in the chosen storage backend."""
        if not self.storage.exists(self.staging_dir):
            self.storage.makedirs(self.staging_dir)

    def get_directory_structure(self):
        """
        Returns a nested dictionary representing the folder structure under self.staging_dir,
        using storage.get_directory_structure.
        """
        return self.storage.get_directory_structure(self.staging_dir)

    # --------------------------------------------------------------------- #
    # Write
    # --------------------------------------------------------------------- #
    def save_as_parquet(self, arrow_table: pa.Table, table_name: str, file_name: str) -> str:
        """
        Saves a PyArrow table as a Parquet file in the staging area and returns its path.

        Final path format:
            <org>/<super>/staging/<table_name>/<UTC yyyymmddHHMMSS>_<file_name>.parquet
        """
        # Create subdirectory for this table
        directory_path = os.path.join(self.staging_dir, table_name)
        self.storage.makedirs(directory_path)

        # Sortable UTC timestamp
        utc_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        file_name_with_timestamp = f"{utc_timestamp}_{file_name}.parquet"
        file_path = os.path.join(directory_path, file_name_with_timestamp)
        logger.debug(f"[staging] writing parquet -> {file_path}")

        # Prefer storage's parquet writer; if it requires a local file path, fallback to pyarrow
        try:
            self.storage.write_parquet(arrow_table, file_path)
        except TypeError:
            # Some storage layers accept file-like objects only; fall back to writing locally
            import pyarrow.parquet as pq
            pq.write_table(arrow_table, file_path, compression="zstd")
        except Exception as e:
            logger.error(f"[staging] write_parquet failed for {file_path}: {e}")
            raise

        return file_path

    # --------------------------------------------------------------------- #
    # Read
    # --------------------------------------------------------------------- #
    def read_parquet(self, file_name_or_relpath: str) -> pa.Table:
        """
        Read a Parquet file from the staging area.

        Accepts:
          - A relative path under the staging dir (e.g., "mytable/20250101_foo.parquet"),
          - Or just a file name; if not found directly under staging root, searches subfolders.

        Returns:
          PyArrow Table
        """
        # Scenario A: caller already passed a relative path (contains a path separator)
        if os.sep in file_name_or_relpath:
            file_path = os.path.join(self.staging_dir, file_name_or_relpath)
            logger.debug(f"[staging] reading parquet (relpath) <- {file_path}")
            return self._read_parquet_at(file_path)

        # Scenario B: bare file name
        direct_path = os.path.join(self.staging_dir, file_name_or_relpath)
        if self.storage.exists(direct_path):
            logger.debug(f"[staging] reading parquet (direct) <- {direct_path}")
            return self._read_parquet_at(direct_path)

        # Search subfolders for a matching filename
        logger.debug(f"[staging] searching for '{file_name_or_relpath}' under {self.staging_dir}")
        found_path = self._find_in_subdirs(file_name_or_relpath)
        if not found_path:
            raise FileNotFoundError(
                f"Parquet file '{file_name_or_relpath}' not found under staging dir '{self.staging_dir}'"
            )

        logger.debug(f"[staging] reading parquet (found) <- {found_path}")
        return self._read_parquet_at(found_path)

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #
    def _read_parquet_at(self, path: str) -> pa.Table:
        try:
            return self.storage.read_parquet(path)
        except Exception as e:
            logger.error(f"[staging] read_parquet failed for {path}: {e}")
            raise

    def _find_in_subdirs(self, target_filename: str) -> Optional[str]:
        """
        Walk the staging directory (using storage.get_directory_structure) and
        return the first path that ends with /<target_filename>.
        """
        try:
            tree = self.storage.get_directory_structure(self.staging_dir)
        except Exception as e:
            logger.error(f"[staging] get_directory_structure failed: {e}")
            return None

        # DFS walk over the nested dict structure
        def dfs(prefix: str, node) -> Optional[str]:
            if node is None:
                # File at the current prefix; check if it matches the target
                if os.path.basename(prefix) == target_filename:
                    return prefix
                return None

            if not isinstance(node, dict):
                return None

            for name, child in node.items():
                child_prefix = os.path.join(prefix, name)
                found = dfs(child_prefix, child)
                if found:
                    return found
            return None

        return dfs(self.staging_dir, tree)
