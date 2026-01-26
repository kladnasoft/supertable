import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa

from supertable.super_table import SuperTable
from supertable.config.defaults import logger

from typing import Any

# --------------------------------------------------------------------------- #
# New interface: Staging (no SuperTable involvement)
# --------------------------------------------------------------------------- #


class Staging:
    """Lightweight staging helper that reuses the same storage backend/config as SuperTable.

    Interface:
        staging = Staging(organization=organization, super_name=super_name, staging_name=staging_name)

        saved_file_name = staging.save_as_parquet(
            arrow_table=arrow_table,
            base_file_name="dummy_file_01",
        )

    Behavior:
    - Uses the same storage backend as SuperTable via `get_storage()` (no SuperTable instance required).
    - Validates that the given `super_name` already exists (via RedisCatalog root); if missing,
      raises an error and DOES NOT create a new SuperTable.
    - Ensures the staging folder exists under the existing supertable; creates it if missing.

    Layout:
        <organization>/<super_name>/staging/<staging_name>/files/<base_file_name>_<timestamp_ns>.parquet

    Pipe metadata:
        <organization>/<super_name>/staging/<staging_name>/pipes/<pipe_name>.json
    """

    def __init__(self, *, organization: str, super_name: str, staging_name: str):
        self.organization = organization
        self.super_name = super_name
        self.staging_name = staging_name
        self.identity = "staging"

        # Same backend/configuration as SuperTable, but fully decoupled from the SuperTable object.
        from supertable.storage.storage_factory import get_storage  # noqa: WPS433
        from supertable.redis_catalog import RedisCatalog  # noqa: WPS433

        self.storage = get_storage()
        self.catalog = RedisCatalog()

        # Validate supertable existence (do not create).
        if not self.catalog.root_exists(self.organization, self.super_name):
            raise FileNotFoundError(
                f"SuperTable does not exist: organization={self.organization!r}, super_name={self.super_name!r}"
            )

        # Stage within an existing supertable
        self.base_dir = os.path.join(self.organization, self.super_name, self.identity)
        self.staging_dir = os.path.join(self.base_dir, self.staging_name)

        # Staging subfolders
        self.files_dir = os.path.join(self.staging_dir, "files")
        self.pipes_dir = os.path.join(self.staging_dir, "pipes")

        logger.debug(f"[staging] init: staging_dir={self.staging_dir}")
        self.init_staging_area()

    def init_staging_area(self) -> None:
        """Ensure that the staging directory and subfolders exist under the existing SuperTable."""
        # Base dir may or may not be present (object storage). Create best-effort.
        if not self.storage.exists(self.base_dir):
            logger.debug(f"[staging] mkdirs -> {self.base_dir}")
            self.storage.makedirs(self.base_dir)

        if not self.storage.exists(self.staging_dir):
            logger.debug(f"[staging] mkdirs -> {self.staging_dir}")
            self.storage.makedirs(self.staging_dir)
        else:
            logger.debug(f"[staging] exists -> {self.staging_dir}")

        # Required subfolders
        if not self.storage.exists(self.files_dir):
            logger.debug(f"[staging] mkdirs -> {self.files_dir}")
            self.storage.makedirs(self.files_dir)

        if not self.storage.exists(self.pipes_dir):
            logger.debug(f"[staging] mkdirs -> {self.pipes_dir}")
            self.storage.makedirs(self.pipes_dir)


    def save_as_parquet(self, arrow_table: pa.Table, base_file_name: str) -> str:
        """Save a PyArrow table as a parquet file and return the saved file name.

        Returns:
            saved_file_name: "{base_file_name}_{timestamp_ns}.parquet"
        """
        op_id = str(uuid.uuid4())
        t0 = time.perf_counter()

        # Normalize base_file_name (user passes without extension, but tolerate ".parquet")
        if base_file_name.lower().endswith(".parquet"):
            base_file_name = base_file_name[: -len(".parquet")]

        ts_ns = time.time_ns()
        saved_file_name = f"{base_file_name}_{ts_ns}.parquet"
        file_path = os.path.join(self.files_dir, saved_file_name)

        logger.debug(f"[staging][{op_id}] writing parquet -> {file_path}")

        try:
            rows = getattr(arrow_table, "num_rows", None)
            cols = getattr(arrow_table, "num_columns", None)
            logger.debug(f"[staging][{op_id}] table: rows={rows}, cols={cols}")
        except Exception:
            rows = cols = None

        try:
            self.storage.write_parquet(arrow_table, file_path)
            method = "storage.write_parquet"
        except TypeError:
            # Some storage layers accept file-like objects only; fall back to pyarrow
            import pyarrow.parquet as pq

            logger.debug(f"[staging][{op_id}] fallback -> pyarrow.parquet.write_table")
            pq.write_table(arrow_table, file_path, compression="zstd")
            method = "pyarrow.parquet.write_table"
        except Exception as e:
            logger.error(f"[staging][{op_id}] write_parquet failed for {file_path}: {e}")
            raise

        elapsed = time.perf_counter() - t0
        total_str = f"[94m{elapsed:.3f}[32m"
        logger.info(
            f"[32m[staging][{op_id}] Summary: total={total_str}s | "
            f"file={saved_file_name} | method={method} | rows={rows} | cols={cols}"
        )

        # After storing the file, activate any enabled pipes on this staging.
        try:
            self._trigger_pipes(saved_file_path=file_path, saved_file_name=saved_file_name)
        except Exception as e:
            # Do not fail the upload if a downstream pipe load fails; log and continue.
            logger.error(f"[staging][{op_id}] pipe trigger failed for {saved_file_name}: {e}")

        return saved_file_name

    def _trigger_pipes(self, *, saved_file_path: str, saved_file_name: str) -> None:
        """Load a newly uploaded staging file into any enabled pipes defined on this staging.

        Pipe definitions are stored as JSON in:
            <org>/<super>/staging/<staging_name>/pipes/<pipe_name>.json
        """
        try:
            tree = self.storage.get_directory_structure(self.pipes_dir)
        except Exception as e:
            logger.error(f"[staging] get_directory_structure failed for pipes: {e}")
            return

        pipe_paths = []

        def dfs(prefix: str, node) -> None:
            if node is None:
                if prefix.endswith(".json"):
                    pipe_paths.append(prefix)
                return
            if not isinstance(node, dict):
                return
            for name, child in node.items():
                dfs(os.path.join(prefix, name), child)

        dfs(self.pipes_dir, tree)

        if not pipe_paths:
            logger.debug(f"[staging] no pipes found under {self.pipes_dir}")
            return

        try:
            data = self.storage.read_parquet(saved_file_path)
        except Exception as e:
            logger.error(f"[staging] read_parquet failed for {saved_file_path}: {e}")
            return

        from supertable.data_writer import DataWriter  # noqa: WPS433

        writer = DataWriter(super_name=self.super_name, organization=self.organization)

        for pipe_path in pipe_paths:
            try:
                pipe_def = self.storage.read_json(pipe_path)
            except Exception as e:
                logger.error(f"[staging] failed to read pipe definition {pipe_path}: {e}")
                continue

            if not isinstance(pipe_def, dict):
                logger.error(f"[staging] invalid pipe definition (not dict) at {pipe_path}")
                continue

            if not pipe_def.get("enabled", True):
                continue

            if pipe_def.get("organization") != self.organization:
                continue
            if pipe_def.get("super_name") != self.super_name:
                continue
            if pipe_def.get("staging_name") != self.staging_name:
                continue

            user_hash = pipe_def.get("user_hash")
            simple_name = pipe_def.get("simple_name")
            overwrite_columns = pipe_def.get("overwrite_columns") or []

            if not user_hash or not simple_name:
                logger.error(f"[staging] pipe missing required fields at {pipe_path}")
                continue

            try:
                columns, rows, inserted, deleted = writer.write(
                    user_hash=user_hash,
                    simple_name=simple_name,
                    data=data,
                    overwrite_columns=overwrite_columns,
                )
                logger.info(
                    f"[pipe] loaded file={saved_file_name} -> simple={simple_name} "
                    f"(inserted={inserted}, deleted={deleted}, cols={columns}, rows={rows})"
                )
            except Exception as e:
                logger.error(
                    f"[pipe] load failed for pipe={os.path.basename(pipe_path)} "
                    f"file={saved_file_name} simple={simple_name}: {e}"
                )
                continue


class StagingArea:
    """
    Lightweight staging area that reuses the SuperTable's storage backend.

    Improvements:
    - Safer timestamping (UTC, sortable).
    - More resilient parquet writer (fallback to pyarrow if storage expects a file on disk).
    - `read_parquet` now gracefully handles both:
        • a relative path like "mytable/1694548200000_file.parquet", or
        • just a bare filename; if not found at root, it searches subfolders.
    - Added small helpers & clearer logging (DEBUG throughout, single INFO summary per op).
    """

    def __init__(self, super_table: SuperTable, organization: str):
        self.super_table = super_table
        self.identity = "staging"
        self.organization = organization

        # Reuse the same storage interface as super_table
        self.storage = self.super_table.storage

        # Directory/prefix for staging
        self.staging_dir = os.path.join(self.organization, super_table.super_name, self.identity)
        logger.debug(f"[staging] init: staging_dir={self.staging_dir}")
        self.init_staging_area()

    # --------------------------------------------------------------------- #
    # Init / structure
    # --------------------------------------------------------------------- #
    def init_staging_area(self) -> None:
        """Ensure that the staging directory exists in the chosen storage backend."""
        if not self.storage.exists(self.staging_dir):
            logger.debug(f"[staging] mkdirs -> {self.staging_dir}")
            self.storage.makedirs(self.staging_dir)
        else:
            logger.debug(f"[staging] exists -> {self.staging_dir}")

    def get_directory_structure(self):
        """
        Returns a nested dictionary representing the folder structure under self.staging_dir,
        using storage.get_directory_structure.
        """
        logger.debug(f"[staging] dir-structure <- {self.staging_dir}")
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
        op_id = str(uuid.uuid4())
        t0 = time.perf_counter()

        # Create subdirectory for this table
        directory_path = os.path.join(self.staging_dir, table_name)
        if not self.storage.exists(directory_path):
            logger.debug(f"[staging][{op_id}] mkdirs -> {directory_path}")
            self.storage.makedirs(directory_path)
        else:
            logger.debug(f"[staging][{op_id}] exists -> {directory_path}")

        # Sortable UTC timestamp
        utc_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        file_name_with_timestamp = f"{utc_timestamp}_{file_name}.parquet"
        file_path = os.path.join(directory_path, file_name_with_timestamp)
        logger.debug(f"[staging][{op_id}] writing parquet -> {file_path}")

        # Prefer storage's parquet writer; if it requires a local file path, fallback to pyarrow
        try:
            rows = getattr(arrow_table, "num_rows", None)
            cols = getattr(arrow_table, "num_columns", None)
            logger.debug(f"[staging][{op_id}] table: rows={rows}, cols={cols}")
        except Exception:
            rows = cols = None

        try:
            self.storage.write_parquet(arrow_table, file_path)
            method = "storage.write_parquet"
        except TypeError:
            # Some storage layers accept file-like objects only; fall back to writing locally
            import pyarrow.parquet as pq
            logger.debug(f"[staging][{op_id}] fallback -> pyarrow.parquet.write_table")
            pq.write_table(arrow_table, file_path, compression="zstd")
            method = "pyarrow.parquet.write_table"
        except Exception as e:
            logger.error(f"[staging][{op_id}] write_parquet failed for {file_path}: {e}")
            raise

        elapsed = time.perf_counter() - t0
        # Blue total, dark green rest, reset at end
        total_str = f"\033[94m{elapsed:.3f}\033[32m"
        logger.info(
            f"\033[32m[staging][{op_id}] Summary: total={total_str}s | "
            f"table={table_name} | file={os.path.basename(file_path)} | method={method} | "
            f"rows={rows} | cols={cols}"
        )

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
        op_id = str(uuid.uuid4())
        t0 = time.perf_counter()

        # Scenario A: caller already passed a relative path (contains a path separator)
        if os.sep in file_name_or_relpath:
            file_path = os.path.join(self.staging_dir, file_name_or_relpath)
            logger.debug(f"[staging][{op_id}] reading parquet (relpath) <- {file_path}")
            table = self._read_parquet_at(file_path)
            elapsed = time.perf_counter() - t0
            total_str = f"\033[94m{elapsed:.3f}\033[32m"
            logger.info(
                f"\033[32m[staging][{op_id}] Summary: total={total_str}s| "
                f"mode=relpath | file={file_name_or_relpath} | rows={getattr(table, 'num_rows', None)} | "
                f"cols={getattr(table, 'num_columns', None)}"
            )
            return table

        # Scenario B: bare file name
        direct_path = os.path.join(self.staging_dir, file_name_or_relpath)
        if self.storage.exists(direct_path):
            logger.debug(f"[staging][{op_id}] reading parquet (direct) <- {direct_path}")
            table = self._read_parquet_at(direct_path)
            elapsed = time.perf_counter() - t0
            total_str = f"\033[94m{elapsed:.3f}\033[32m"
            logger.info(
                f"\033[32m[staging][{op_id}] Summary: total={total_str}s | "
                f"mode=direct | file={file_name_or_relpath} | rows={getattr(table, 'num_rows', None)} | "
                f"cols={getattr(table, 'num_columns', None)}"
            )
            return table

        # Search subfolders for a matching filename
        logger.debug(f"[staging][{op_id}] searching for '{file_name_or_relpath}' under {self.staging_dir}")
        found_path = self._find_in_subdirs(file_name_or_relpath)
        if not found_path:
            elapsed = time.perf_counter() - t0
            logger.error(
                f"[staging][{op_id}] not found after {elapsed:.3f}s: '{file_name_or_relpath}' in '{self.staging_dir}'"
            )
            raise FileNotFoundError(
                f"Parquet file '{file_name_or_relpath}' not found under staging dir '{self.staging_dir}'"
            )

        logger.debug(f"[staging][{op_id}] reading parquet (found) <- {found_path}")
        table = self._read_parquet_at(found_path)
        elapsed = time.perf_counter() - t0
        total_str = f"\033[94m{elapsed or 0.0:.3f}\033[32m"
        logger.info(
            f"\033[32m[staging][{op_id}] Summary: total={total_str}s | "
            f"mode=search | file={file_name_or_relpath} | resolved={os.path.relpath(found_path, self.staging_dir)} | "
            f"rows={getattr(table, 'num_rows', None)} | cols={getattr(table, 'num_columns', None)}"
        )
        return table

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
