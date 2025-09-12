import os
import logging
from typing import List, Optional, Dict, Any, Set, Tuple

from supertable.rbac.access_control import check_meta_access
from supertable.storage.storage_factory import get_storage

from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable

logger = logging.getLogger(__name__)


def _prune_dict(d: Dict[str, Any], keys_to_remove: Set[str]) -> Dict[str, Any]:
    """Return a shallow copy of d with selected keys removed (non-mutating)."""
    return {k: v for k, v in d.items() if k not in keys_to_remove}


class MetaReader:
    """
    Read-only metadata helper for SuperTable & SimpleTable.
    Optimizations / fixes:
      - Use shared-lock readers to avoid self-deadlocks during read paths.
      - Avoid in-place mutation of JSON dicts (copy before pruning keys).
      - Consistent logging instead of print().
      - Safer handling when snapshot paths are missing.
    """

    def __init__(self, super_name: str, organization: str):
        # Create a SuperTable object (which internally sets up the storage backend).
        self.super_table = SuperTable(super_name=super_name, organization=organization)

    def get_table_schema(self, table_name: str, user_hash: str) -> Optional[List[Dict[str, Any]]]:
        try:
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=table_name,
            )
        except PermissionError as e:
            logger.warning(
                "[get_table_schema] Access denied for user '%s' on table '%s': %s",
                user_hash, table_name, str(e)
            )
            return None

        # Read super-table snapshot with a shared lock (read-only)
        super_table_data, _, _ = self.super_table.get_super_table_and_path_with_shared_lock()
        snapshots = super_table_data.get("snapshots", [])

        schema_items: Set[Tuple[str, Any]] = set()

        if table_name == self.super_table.super_name:
            # Aggregate schema across all simple tables
            for snapshot in snapshots:
                simple_path = snapshot.get("path")
                if not simple_path:
                    continue
                try:
                    simple_table_data = self.super_table.read_simple_table_snapshot(simple_path)
                except FileNotFoundError:
                    logger.debug("Simple table snapshot missing at %s", simple_path)
                    continue
                schema = simple_table_data.get("schema", {}) or {}
                for key, value in schema.items():
                    schema_items.add((key, value))
        else:
            # Single table
            simple_path = next(
                (snapshot.get("path") for snapshot in snapshots
                 if snapshot.get("table_name") == table_name),
                None,
            )
            if simple_path:
                try:
                    simple_table_data = self.super_table.read_simple_table_snapshot(simple_path)
                except FileNotFoundError:
                    logger.debug("Simple table snapshot missing at %s", simple_path)
                    return [{}]
                schema = simple_table_data.get("schema", {}) or {}
                for key, value in schema.items():
                    schema_items.add((key, value))

        distinct_schema = dict(sorted(schema_items))
        return [distinct_schema]

    def collect_simple_table_schema(self, schemas: set, table_name: str, user_hash: str) -> None:
        try:
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=table_name,
            )
        except PermissionError as e:
            logger.warning(
                "[collect_simple_table_schema] Access denied for user '%s' on table '%s': %s",
                user_hash, table_name, str(e)
            )
            return

        # Use shared-lock (read-only) to avoid contention
        simple_table = SimpleTable(self.super_table, table_name)
        try:
            simple_table_data, _ = simple_table.get_simple_table_with_shared_lock()
        except FileNotFoundError:
            logger.debug("Simple table snapshot missing for %s", table_name)
            return

        schema = simple_table_data.get("schema", {}) or {}
        schema_tuple = tuple(sorted(schema.items()))
        schemas.add(schema_tuple)

    def get_table_stats(self, table_name: str, user_hash: str) -> List[Dict[str, Any]]:
        try:
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=table_name,
            )
        except PermissionError as e:
            logger.warning(
                "[get_table_stats] Access denied for user '%s' on table '%s': %s",
                user_hash, table_name, str(e)
            )
            return []

        keys_to_remove = {"previous_snapshot", "schema", "location"}
        stats: List[Dict[str, Any]] = []

        if table_name == self.super_table.super_name:
            # Read super-table (shared-lock) and iterate simple tables
            super_table_data, _, _ = self.super_table.get_super_table_and_path_with_shared_lock()
            for snapshot in super_table_data.get("snapshots", []):
                simple_name = snapshot.get("table_name")
                if not simple_name:
                    continue
                st = SimpleTable(self.super_table, simple_name)
                try:
                    st_data, _ = st.get_simple_table_with_shared_lock()
                except FileNotFoundError:
                    logger.debug("Simple table snapshot missing for %s", simple_name)
                    continue
                stats.append(_prune_dict(st_data, keys_to_remove))
        else:
            st = SimpleTable(self.super_table, table_name)
            try:
                st_data, _ = st.get_simple_table_with_shared_lock()
            except FileNotFoundError:
                logger.debug("Simple table snapshot missing for %s", table_name)
                return []
            stats.append(_prune_dict(st_data, keys_to_remove))

        return stats

    def get_super_meta(self, user_hash: str) -> Optional[Dict[str, Any]]:
        try:
            # Checking meta access for the super table itself
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=self.super_table.super_name,
            )
        except PermissionError as e:
            logger.warning(
                "[get_super_meta] Access denied for user '%s' on super '%s': %s",
                user_hash, self.super_table.super_name, str(e)
            )
            return None

        # Shared-lock for read-only
        super_table_data, current_path, super_table_meta = (
            self.super_table.get_super_table_and_path_with_shared_lock()
        )

        simple_table_info = [
            {
                "name": snapshot.get("table_name"),
                "files": snapshot.get("files", 0),
                "rows": snapshot.get("rows", 0),
                "size": snapshot.get("file_size", 0),
                "updated_utc": snapshot.get("last_updated_ms", 0),
            }
            for snapshot in super_table_data.get("snapshots", [])
        ]

        result = {
            "super": {
                "name": self.super_table.super_name,
                "files": super_table_meta.get("file_count", 0),
                "rows": super_table_meta.get("total_rows", 0),
                "size": super_table_meta.get("total_file_size", 0),
                "updated_utc": super_table_data.get("last_updated_ms", 0),
                "tables": simple_table_info,
                "meta_path": current_path,
            }
        }
        return result


def find_tables(organization: str) -> List[str]:
    """
    Searches the organization's directory for subdirectories that contain a
    "super" folder and a "_super.json" file. Uses the storage interface's
    get_directory_structure() for portability.
    """
    storage = get_storage()

    # Normalize base path for the org (e.g., "kladna-soft/")
    base_path = organization.rstrip("/")
    if base_path:
        base_path += "/"
    else:
        base_path = ""

    try:
        dir_structure = storage.get_directory_structure(base_path)  # nested dict
    except Exception as e:
        logger.error("get_directory_structure failed for '%s': %s", base_path, e)
        return []

    found_tables: Set[str] = set()

    def walk_structure(parent_rel: str, substructure: Dict[str, Any]) -> None:
        """
        parent_rel is the path relative to base_path (no leading slash).
        substructure is a dict: { name -> None (file) or dict (subdir) }
        """
        if not isinstance(substructure, dict):
            return

        files = [name for name, val in substructure.items() if val is None]
        dirs = [name for name, val in substructure.items() if isinstance(val, dict)]

        # Detect table root: has 'super' dir and '_super.json' file at this level
        if "super" in dirs and "_super.json" in files:
            # The folder name is the last component of parent_rel (strip trailing '/')
            folder_rel = parent_rel.rstrip("/")

            # If we are scanning at org root and the table folder is directly under it,
            # parent_rel will be something like 'example' or 'org/example'.
            if not folder_rel:
                # This means base_path itself is the table root (rare). Use basename of CWD as fallback.
                folder_name = os.path.basename(os.getcwd())
            else:
                folder_name = os.path.basename(folder_rel)

            found_tables.add(folder_name)
            # No need to recurse deeper within a detected table root
            return

        # Recurse into subdirectories
        for d in dirs:
            next_parent = f"{parent_rel}{d}/" if parent_rel else f"{d}/"
            walk_structure(next_parent, substructure[d])

    # Kick off the walk from the org base
    walk_structure("", dir_structure)

    return sorted(found_tables)
