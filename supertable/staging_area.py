import os
import pyarrow as pa
from datetime import datetime

from supertable.super_table import SuperTable
from supertable.config.defaults import default, logger


class StagingArea:
    def __init__(self, super_table: SuperTable, organization: str):
        """
        The Staging area for a given SuperTable.
        """
        self.super_table = super_table
        self.identity = "staging"
        self.organization = organization

        # Reuse the same storage interface used by the super_table
        self.storage = self.super_table.storage

        # The local or remote directory/prefix for staging
        self.staging_dir = os.path.join(self.organization, super_table.super_name, self.identity)
        logger.debug(f"staging_dir: {self.staging_dir}")
        self.init_staging_area()

    def init_staging_area(self):
        """
        Ensure that the staging directory exists in the chosen storage backend.
        """
        if not self.storage.exists(self.staging_dir):
            self.storage.makedirs(self.staging_dir)

    def get_directory_structure(self):
        """
        Creates a nested dictionary that represents the folder structure
        under self.staging_dir, using storage.get_directory_structure.
        """
        return self.storage.get_directory_structure(self.staging_dir)

    def save_as_parquet(self, arrow_table: pa.Table, table_name: str, file_name: str) -> str:
        """
        Saves a PyArrow table as a Parquet file in the staging area,
        returning the final path. Uses the storage's write_parquet method.
        """
        # Create a subdirectory for this hyper table
        directory_path = os.path.join(self.staging_dir, table_name)
        self.storage.makedirs(directory_path)

        utc_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        file_name_with_timestamp = f"{utc_timestamp}_{file_name}.parquet"
        file_path = os.path.join(directory_path, file_name_with_timestamp)
        logger.debug(f"file_path: {file_path}")

        # Write the Parquet file via the storage interface
        self.storage.write_parquet(arrow_table, file_path)

        return file_path

    def read_parquet(self, file_name: str ) -> pa.Table:
        file_path = os.path.join(self.staging_dir, file_name)

        logger.debug(f"Staging file_path: {file_path}")

        # Write the Parquet file via the storage interface
        return self.storage.read_parquet(file_path)
