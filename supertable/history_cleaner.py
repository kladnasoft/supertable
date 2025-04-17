import os
from supertable.config.defaults import logger
from supertable.super_table import SuperTable
from supertable.rbac.access_control import check_write_access

class HistoryCleaner:
    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        # Grab the storage object from the super_table
        self.storage = self.super_table.storage

    def clean(self, user_hash):
        # Acquire the shared lock and read super table meta
        super_table_data, super_table_path, super_table_meta = (
            self.super_table.get_super_table_and_path_with_shared_lock()
        )
        last_updated_ms = super_table_data["last_updated_ms"]

        check_write_access(super_name=self.super_table.super_name,
                           organization=self.super_table.organization,
                           user_hash=user_hash,
                           table_name=self.super_table.super_name)

        # Collect all files in the super_table.super_dir
        super_files = self.collect_files(self.super_table.super_dir)
        # Remove the current super_table pointer from the list
        if super_table_path in super_files:
            super_files.remove(super_table_path)

        files_to_delete = self.get_files_to_delete(super_files, last_updated_ms)
        self.delete_files(files_to_delete)

        logger.debug(
            f"{len(files_to_delete)} files cleaned for table: {self.super_table.super_name}"
        )

        # Iterate over each snapshot in the super table
        for snapshot in super_table_data["snapshots"]:
            # Note: If your snapshot dict uses "table_name" instead of "simple_name", adjust here:
            simple_table_name = snapshot["table_name"]    # or snapshot["simple_name"] if appropriate
            simple_table_path = snapshot["path"]

            # Read the simple table's own snapshot JSON
            simple_table_data = self.storage.read_json(simple_table_path)

            location = simple_table_data["location"]
            active_files = [entry["file"] for entry in simple_table_data.get("resources", [])]

            designated_files = set(self.collect_files(location)) - set(active_files)
            # Remove the current snapshot path from the designated_files
            if simple_table_path in designated_files:
                designated_files.remove(simple_table_path)

            # Convert back to list
            designated_files = list(designated_files)

            files_to_delete = self.get_files_to_delete(designated_files, last_updated_ms)
            self.delete_files(files_to_delete)

            logger.debug(f"{len(files_to_delete)} files cleaned for table: {simple_table_name}")

    def collect_files(self, location):
        """
        Collect parquet & JSON files under 'location'.
        This uses the storage interface's list_files instead of glob.
        """
        # Example usage for a typical directory layout:
        parquet_files = self.storage.list_files(os.path.join(location, "data"), "*.parquet")
        json_files = self.storage.list_files(os.path.join(location, "snapshots"), "*.json")
        super_json_files = self.storage.list_files(location, "*.json")
        return parquet_files + json_files + super_json_files

    def get_files_to_delete(self, designated_files, last_updated_ms):
        """
        Compare the numeric timestamp in the file name (e.g. 1678900000_*.json)
        against `last_updated_ms`.
        """
        files_to_delete = []
        for file in designated_files:
            # Example: "1678900000_fileinfo.json" => "1678900000"
            filename = os.path.basename(file)
            timestamp_str = filename.split("_")[0]
            try:
                timestamp_val = int(timestamp_str)
                if timestamp_val <= last_updated_ms:
                    files_to_delete.append(file)
            except ValueError:
                # If the file doesn't match the pattern, skip or handle differently
                pass

        return files_to_delete

    def delete_files(self, files_to_delete):
        """
        Deletes files using the storage interface's `delete` method.
        """
        for file in files_to_delete:
            self.storage.delete(file)
            logger.debug(f"Deleted file: {file}")
