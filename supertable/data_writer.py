import time
import uuid
from datetime import datetime

import polars
import re

from polars import DataFrame

from supertable.config.defaults import logger
from supertable.monitoring_logger import MonitoringLogger
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.utils.timer import Timer
from supertable.processing import (
    process_overlapping_files,
    find_and_lock_overlapping_files,
)
from supertable.rbac.access_control import check_write_access

class DataWriter:
    def __init__(self, super_name: str, organization: str):
        self.super_table = SuperTable(super_name, organization)

    timer = Timer()

    @timer
    def write(self, user_hash, simple_name, data, overwrite_columns, compression_level=1):

        start_inner = time.time()

        logger.debug("Checking for Write Access")
        check_write_access(super_name=self.super_table.super_name,
                           organization=self.super_table.organization,
                           user_hash=user_hash,
                           table_name=simple_name)
        logger.debug("Passed Write Access Check")

        # Convert the input dataset from Arrow format to a Polars DataFrame
        logger.debug("Converting data to DataFrame")
        dataframe: DataFrame = polars.from_arrow(data)
        logger.debug("Converted data to DataFrame")

        logger.debug("Validating the dataframe")
        self.validation(dataframe, simple_name, overwrite_columns)
        logger.debug("dataframe is valid")

        logger.debug(f"Reading Simple Table Metadata {simple_name}")
        simple_table = SimpleTable(self.super_table, simple_name)
        last_simple_table, _ = simple_table.get_simple_table_with_shared_lock()
        logger.debug(f"last_simple_table: {last_simple_table}")

        # Find files that have overlapping data and lock them to prevent concurrent modifications
        overlapping_files = find_and_lock_overlapping_files(
            last_simple_table, dataframe, overwrite_columns, simple_table.locking
        )
        logger.debug(f"overlapping_files: {overlapping_files}")

        # Process the overlapping files by filtering, merging, and updating resources
        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = (
            process_overlapping_files(
                dataframe,
                overlapping_files,
                overwrite_columns,
                simple_table.data_dir,
                compression_level,
            )
        )

        new_simple_table_snapshot, new_simple_table_path = simple_table.lock_and_update(
            new_resources, sunset_files, dataframe
        )

        self.super_table.update_with_lock(
            simple_name, new_simple_table_path, new_simple_table_snapshot
        )

        simple_table.locking.release_lock()
        # self.super_table.locking.release_lock()

        stats = {
            "query_id": str(uuid.uuid4()),
            "recorded_at": datetime.utcnow().isoformat(),
            "super_name": self.super_table.super_name,
            "table_name": simple_name,
            "overwrite_columns": overwrite_columns,
            "inserted": inserted,
            "deleted": deleted,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "new_resources": len(new_resources),
            "sunset_files": len(sunset_files),
            "duration": round(time.time() - start_inner, 6)
        }

        # Instantiate and use MonitoringLogger within the function
        with MonitoringLogger(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                monitor_type="stats",
        ) as monitor:
            monitor.log_metric(stats)

        return total_columns, total_rows, inserted, deleted

    def validation(
        self, dataframe: DataFrame, simple_name: str, overwrite_columns: list
    ):
        if len(simple_name) == 0 or len(simple_name) > 128:
            raise ValueError("SimpleTable name can't be empty or longer than 128")

        if simple_name == self.super_table.super_name:
            raise ValueError("SimpleTable name can't match with SuperTable name")

        # Regular expression pattern for a valid table name
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
        if not re.match(pattern, simple_name):
            raise ValueError(
                f"Invalid table name: '{simple_name}'. Table names must start with a letter or underscore and contain only alphanumeric characters and underscores."
            )

        # Validate the overwrite columns
        if overwrite_columns and not all(
            col in dataframe.columns for col in overwrite_columns
        ):
            raise ValueError("Some overwrite columns are not present in the dataset")

        # Ensure overwrite_columns is a list
        if isinstance(overwrite_columns, str):
            raise ValueError("overwrite columns must be list")
