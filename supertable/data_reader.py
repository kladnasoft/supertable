from enum import Enum

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.super_table import SuperTable
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.plan_extender import PlanStats, extend_execution_plan
from supertable.rbac.access_control import restrict_read_access

class Status(Enum):
    OK = "ok"
    ERROR = "error"


class DataReader:
    def __init__(self, super_name, organization, query):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.parser = SQLParser(query)
        self.parser.parse_sql()
        self.timer = None
        self.plan_stats = None
        self.query_plan_manager = None

    def filter_snapshots(self, super_table_data, super_table_meta):
        snapshots = super_table_data.get("snapshots")
        file_count = super_table_meta.get("file_count", 0)
        total_rows = super_table_meta.get("total_rows", 0)
        total_file_size = super_table_meta.get("total_file_size", 0)
        self.plan_stats.add_stat({"TABLE_FILES": file_count})
        self.plan_stats.add_stat({"TABLE_SIZE": total_file_size})
        self.plan_stats.add_stat({"TABLE_ROWS": total_rows})

        if self.super_table.super_name.lower() == self.parser.original_table.lower():
            filtered_snapshots = [
                s for s in snapshots
                if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))
            ]
            return filtered_snapshots
        else:
            filtered_snapshots = [
                entry
                for entry in snapshots
                if entry["table_name"].lower() == self.parser.original_table.lower()
            ]

            return filtered_snapshots

    timer = Timer()
    @timer
    def execute(self, user_hash: str, with_scan: bool=False):
        status = Status.ERROR
        message = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        try:
            super_table_data, super_table_path, super_table_meta = self.super_table.get_super_table_and_path_with_shared_lock()

            self.timer.capture_and_reset_timing(event="META")

            self.query_plan_manager = QueryPlanManager(super_name=self.super_table.super_name,
                                                       organization=self.super_table.organization,
                                                       current_meta_path=super_table_path,
                                                       parser=self.parser)

            snapshots = self.filter_snapshots(super_table_data=super_table_data,
                                              super_table_meta=super_table_meta)
            logger.debug(f"Filtered snapshots: {snapshots}")

            parquet_files, schema = self.process_snapshots(snapshots=snapshots,
                                                           with_scan=with_scan)
            logger.debug(f"Parquet Files: {parquet_files}")

            missing_columns = (
                set([column.lower() for column in self.parser.columns_list])
                - set("*")
                - schema
            )
            logger.debug(f"Mising Columns: {missing_columns}")

            if len(snapshots) == 0 or missing_columns or not parquet_files:
                message = (
                    f"Missing column(s): {', '.join ( missing_columns )}"
                    if missing_columns
                    else "No parquet files found"
                )
                logger.warning(f"Filter Result: {message}")
                return pd.DataFrame(), status, message

            restrict_read_access(super_name=self.super_table.super_name,
                                 organization=self.super_table.organization,
                                 user_hash=user_hash,
                                 table_name=self.parser.reflection_table,
                                 table_schema=schema,
                                 parsed_columns=self.parser.columns_list,
                                 parser=self.parser)

            self.timer.capture_and_reset_timing(event="FILTERING")

            result = self.execute_with_duckdb(parquet_files=parquet_files,
                                              query_manager=self.query_plan_manager)

            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(f"Exception: {e}")
            result = pd.DataFrame()
        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")

        try:
            extend_execution_plan(super_table=self.super_table,
                                  user_hash=user_hash,
                                  query_plan_manager=self.query_plan_manager,
                                  timing=self.timer.timings,
                                  status=status.value,
                                  message=message,
                                  result_shape=result.shape,
                                  plan_stats=self.plan_stats)
        except Exception as e:
            logger.error(f"Exception: {e}")

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        return result, status, message

    def process_snapshots(self, snapshots, with_scan):
        parquet_files = []
        reflection_file_size = 0
        reflection_rows = 0

        schema = set()
        for snapshot in snapshots:
            current_snapshot_path = snapshot["path"]
            current_snapshot_data = self.super_table.read_simple_table_snapshot(
                current_snapshot_path
            )

            current_schema = current_snapshot_data.get("schema", {})
            resources = current_snapshot_data.get("resources", {})
            schema.update(dict_keys_to_lowercase(current_schema).keys())

            for resource in resources:
                file_size = resource.get("file_size", 0)
                file_rows = resource.get("rows", 0)

                if (
                    with_scan
                    or self.parser.columns_csv == "*"
                    or any(
                        col in dict_keys_to_lowercase(current_schema).keys()
                        for col in [
                            column.lower() for column in self.parser.columns_list
                        ]
                    )
                ):
                    parquet_files.append(resource["file"])
                    reflection_file_size += file_size
                    reflection_rows += file_rows

            logger.debug(f"snapshots: {len ( snapshots )}")
            logger.debug(f"parquet_files: {len ( parquet_files )}")
            logger.debug(f"schema: {schema}")

        self.plan_stats.add_stat({"REFLECTIONS": len(parquet_files)})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_ROWS": reflection_rows})

        return parquet_files, schema

    def execute_with_duckdb(self, parquet_files, query_manager: QueryPlanManager):
        # Use DuckDB to read and query the parquet files directly
        con = duckdb.connect()

        con.execute("PRAGMA memory_limit='2GB';")
        con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
        con.execute("PRAGMA enable_profiling='json';")
        #con.execute("SET profiling_mode = 'standard';")
        con.execute(f"PRAGMA profile_output = '{query_manager.query_plan_path}';")

        # Read and register parquet files directly with DuckDB
        parquet_files_str = ", ".join(f"'{file}'" for file in parquet_files)
        logger.debug(f"parquet files: {len(parquet_files)}")

        self.timer.capture_and_reset_timing("CONNECTING")

        create_table = f"""
CREATE TABLE {self.parser.reflection_table} 
AS 
SELECT {self.parser.columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=True, HIVE_PARTITIONING=TRUE);
          """

        logger.debug(f"create_table: {create_table}")
        con.execute(create_table)

        create_view = f"""
CREATE VIEW {self.parser.rbac_view}
AS
{self.parser.view_definition}
"""
        logger.debug(f"create_view: {create_view}")
        con.execute(create_view)

        self.timer.capture_and_reset_timing("CREATING_REFLECTION")
        logger.debug(f"Executing Query: {self.parser.executing_query}")
        result = con.execute(query=self.parser.executing_query).fetchdf()
        logger.debug(f"result.shape: {result.shape}")
        return result
