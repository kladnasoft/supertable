import os
import logging
from datetime import datetime
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
    Optimized for Redis-based metadata with minimal locking.
    """

    def __init__(self, super_name: str, organization: str):
        # Create a SuperTable object (which internally sets up the storage backend).
        self.super_table = SuperTable(super_name=super_name, organization=organization)

    def _get_all_tables(self) -> List[str]:
        """
        Get all tables for this super table by scanning Redis keys.
        """
        try:
            # Pattern to match all leaf pointers for this organization/super
            pattern = f"supertable:{self.super_table.organization}:{self.super_table.super_name}:meta:leaf:*"

            tables = []
            cursor = 0
            while True:
                cursor, keys = self.catalog.r.scan(cursor=cursor, match=pattern, count=1000)
                for key in keys:
                    # Handle both bytes and string keys
                    if isinstance(key, bytes):
                        key_str = key.decode('utf-8')
                    else:
                        key_str = str(key)

                    # Extract table name from key: supertable:org:super:meta:leaf:table_name
                    table_name = key_str.split(':')[-1]
                    if table_name not in tables:
                        tables.append(table_name)
                if cursor == 0:
                    break
            return tables
        except Exception as e:
            logger.error(f"Error getting tables from Redis: {e}")
            return []

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

        schema_items: Set[Tuple[str, Any]] = set()

        if table_name == self.super_table.super_name:
            # Aggregate schema across all simple tables
            tables = self._get_all_tables()
            for table in tables:
                try:
                    simple_table = SimpleTable(self.super_table, table)
                    simple_table_data, _ = simple_table.get_simple_table_snapshot()
                    schema = simple_table_data.get("schema", {}) or {}
                    for key, value in schema.items():
                        schema_items.add((key, value))
                except (FileNotFoundError, KeyError) as e:
                    logger.debug("Failed to read schema for table %s: %s", table, e)
                    continue
        else:
            # Single table
            try:
                simple_table = SimpleTable(self.super_table, table_name)
                simple_table_data, _ = simple_table.get_simple_table_snapshot()
                schema = simple_table_data.get("schema", {}) or {}
                for key, value in schema.items():
                    schema_items.add((key, value))
            except (FileNotFoundError, KeyError) as e:
                logger.debug("Failed to read schema for table %s: %s", table_name, e)
                return [{}]

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

        try:
            simple_table = SimpleTable(self.super_table, table_name)
            simple_table_data, _ = simple_table.get_simple_table_snapshot()
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
            # Get all tables and their stats
            tables = self._get_all_tables()
            for table in tables:
                try:
                    st = SimpleTable(self.super_table, table)
                    st_data, _ = st.get_simple_table_snapshot()
                    stats.append(_prune_dict(st_data, keys_to_remove))
                except (FileNotFoundError, KeyError):
                    logger.debug("Simple table snapshot missing for %s", table)
                    continue
        else:
            # Single table
            try:
                st = SimpleTable(self.super_table, table_name)
                st_data, _ = st.get_simple_table_snapshot()
                stats.append(_prune_dict(st_data, keys_to_remove))
            except (FileNotFoundError, KeyError):
                logger.debug("Simple table snapshot missing for %s", table_name)
                return []

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

        # Get all tables from Redis
        tables = self._get_all_tables()

        simple_table_info = []
        total_files = 0
        total_rows = 0
        total_size = 0

        for table in tables:
            try:
                st = SimpleTable(self.super_table, table)
                st_data, _ = st.get_simple_table_snapshot()

                # Calculate table stats
                resources = st_data.get("resources", [])
                table_files = len(resources)
                table_rows = sum(res.get("rows", 0) for res in resources)
                table_size = sum(res.get("file_size", 0) for res in resources)

                simple_table_info.append({
                    "name": table,
                    "files": table_files,
                    "rows": table_rows,
                    "size": table_size,
                    "updated_utc": st_data.get("last_updated_ms", 0),
                })

                total_files += table_files
                total_rows += table_rows
                total_size += table_size

            except (FileNotFoundError, KeyError) as e:
                logger.debug("Failed to get stats for table %s: %s", table, e)
                continue

        result = {
            "super": {
                "name": self.super_table.super_name,
                "files": total_files,
                "rows": total_rows,
                "size": total_size,
                "updated_utc": int(datetime.now().timestamp() * 1000),
                "tables": simple_table_info,
                "meta_path": f"redis://{self.super_table.organization}/{self.super_table.super_name}",
            }
        }
        return result

    @property
    def catalog(self):
        """Get the Redis catalog from super_table."""
        return self.super_table.catalog


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
