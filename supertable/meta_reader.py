import os

from supertable.rbac.access_control import check_meta_access
from supertable.storage.storage_factory import get_storage

from supertable.utils.helper import format_size
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable

class MetaReader:
    def __init__(self, super_name: str, organization: str):
        # Create a SuperTable object (which internally sets up the storage backend).
        self.super_table = SuperTable(super_name=super_name, organization=organization)

    def get_table_schema(self, table_name: str, user_hash: str):
        try:
            # 1) Attempt the meta-access check
            check_meta_access(super_name=self.super_table.super_name, organization=self.super_table.organization, user_hash=user_hash, table_name=table_name)
        except PermissionError as e:
            # 2) Handle the denied permission
            print(f"[get_table_schema] Access Denied for user '{user_hash}' on table '{table_name}': {str(e)}")
            return None  # or raise e if you want to bubble it up

        # 3) Proceed if no error
        schema_items = set()
        super_table_data = self.super_table.get_super_table()
        snapshots = super_table_data.get("snapshots", [])

        if table_name == self.super_table.super_name:
            for snapshot in snapshots:
                simple_path = snapshot.get("path")
                simple_table_data = self.super_table.read_simple_table_snapshot(simple_path)
                schema = simple_table_data.get("schema", {})
                for key, value in schema.items():
                    schema_items.add((key, value))
        else:
            simple_path = next(
                (
                    snapshot["path"]
                    for snapshot in snapshots
                    if snapshot.get("table_name") == table_name
                ),
                None,
            )
            if simple_path:
                simple_table_data = self.super_table.read_simple_table_snapshot(simple_path)
                schema = simple_table_data.get("schema", {})
                for key, value in schema.items():
                    schema_items.add((key, value))

        distinct_schema = dict(sorted(schema_items))
        return [distinct_schema]

    def collect_simple_table_schema(self, schemas: set, table_name: str, user_hash: str):
        try:
            check_meta_access(super_name=self.super_table.super_name, organization=self.super_table.organization, user_hash=user_hash, table_name=table_name)
        except PermissionError as e:
            print(f"[collect_simple_table_schema] Access Denied for user '{user_hash}' on table '{table_name}': {str(e)}")
            return  # Return or raise as desired

        simple_table = SimpleTable(self.super_table, table_name)
        simple_table_data, _ = simple_table.get_simple_table_with_lock()
        schema = simple_table_data.get("schema", {})
        schema_tuple = tuple(sorted(schema.items()))
        schemas.add(schema_tuple)

    def get_table_stats(self, table_name: str, user_hash: str):
        try:
            check_meta_access(super_name=self.super_table.super_name, organization=self.super_table.organization,
                              user_hash=user_hash, table_name=table_name)

        except PermissionError as e:
            print(f"[get_table_stats] Access Denied for user '{user_hash}' on table '{table_name}': {str(e)}")
            return []  # Return empty list or raise

        keys_to_remove = ["previous_snapshot", "schema", "location"]
        stats = []

        if table_name == self.super_table.super_name:
            super_table_data = self.super_table.get_super_table()
            snapshots = super_table_data.get("snapshots", [])
            for snapshot in snapshots:
                simple_name = snapshot.get("table_name")
                simple_table = SimpleTable(self.super_table, simple_name)
                simple_table_data, _ = simple_table.get_simple_table_with_lock()
                for key in keys_to_remove:
                    simple_table_data.pop(key, None)
                stats.append(simple_table_data)
        else:
            simple_table = SimpleTable(self.super_table, table_name)
            simple_table_data, _ = simple_table.get_simple_table_with_lock()
            for key in keys_to_remove:
                simple_table_data.pop(key, None)
            stats.append(simple_table_data)

        return stats

    def get_super_meta(self, user_hash: str):
        try:
            # Checking meta access for the super table itself
            check_meta_access(super_name=self.super_table.super_name, organization=self.super_table.organization,
                              user_hash=user_hash, table_name=self.super_table.super_name)

        except PermissionError as e:
            print(f"[get_super_meta] Access Denied for user '{user_hash}' on super '{self.super_table.super_name}': {str(e)}")
            return None

        super_table_data, current_path, super_table_meta = (
            self.super_table.get_super_table_and_path_with_lock()
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
            }
        }
        return result


def find_tables(organization: str) -> list:
    """
    Searches the current path ('.') in the storage backend for subdirectories
    containing a "super" folder and a "_super.json" file, similarly to the
    original os.walk-based logic. Returns a list of matching directory names.
    """

    storage = get_storage()
    # Search within the organization path
    base_path = f"{organization}/" if organization else "."
    dir_structure = storage.get_directory_structure(base_path)  # returns a nested dict
    found_tables = set()

    # We'll define a recursive helper that walks through the nested structure.
    def walk_structure(parent_path: str, substructure: dict):
        # 'substructure' is a dict of { name -> None or dict(...) }
        if not isinstance(substructure, dict):
            return

        # Separate out files from directories
        files = [key for key, val in substructure.items() if val is None]
        dirs = [key for key, val in substructure.items() if isinstance(val, dict)]

        # Check if this level has 'super' (a subdirectory) and '_super.json' (a file)
        if "super" in dirs and "_super.json" in files:
            # We found a match here. The directory name is the final component of parent_path
            # If parent_path == '.', let's treat it as the current directory's name:
            if parent_path == '.':
                folder_name = os.path.basename(os.getcwd())
            else:
                folder_name = os.path.basename(parent_path.rstrip('/.'))
            found_tables.add(folder_name)
            # Once found, we *could* stop recursing deeper, because we've identified this folder
            # as a table. If you have subfolders also containing "super/_super.json," decide
            # if you want to keep going. Typically, we skip further recursion:
            return

        # Otherwise, continue walking subdirectories
        for d in dirs:
            new_path = d if parent_path == '.' else f"{parent_path}/{d}"
            walk_structure(new_path, substructure[d])

    # Start the recursive walk from '.'
    walk_structure('.', dir_structure)

    # Convert the set to a sorted list or just a list
    return sorted(found_tables)
