from enum import Enum
from typing import Iterable, Set, Tuple, List

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.super_table import SuperTable
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.plan_extender import extend_execution_plan
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access


class Status(Enum):
    OK = "ok"
    ERROR = "error"


def _lower_set(items: Iterable[str]) -> Set[str]:
    return {str(x).lower() for x in items}


def _quote_if_needed(col: str) -> str:
    """
    Quote a column identifier only if it contains characters that
    would require quoting in DuckDB (anything except letters, digits, or _).
    Keeps '*' unchanged.
    """
    col = col.strip()
    if col == "*":
        return "*"
    if all(ch.isalnum() or ch == "_" for ch in col):
        return col
    return '"' + col.replace('"', '""') + '"'


class DataReader:
    def __init__(self, super_name, organization, query):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.parser = SQLParser(query)
        self.parser.parse_sql()
        self.timer = None
        self.plan_stats = None
        self.query_plan_manager = None

        # log-context prefix (filled once a QueryPlanManager is created)
        self._log_ctx = ""

    def _lp(self, msg: str) -> str:
        """Prefix log lines with query correlation info."""
        return f"{self._log_ctx}{msg}"

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
                s
                for s in snapshots
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

    def execute(self, user_hash: str, with_scan: bool = False):
        status = Status.ERROR
        message = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        try:
            super_table_data, super_table_path, super_table_meta = (
                self.super_table.get_super_table_and_path_with_shared_lock()
            )
            self.timer.capture_and_reset_timing(event="META")

            # --- Planning / IDs -------------------------------------------------
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                current_meta_path=super_table_path,
                parser=self.parser,
            )
            # set correlation prefix now that we have ids
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "

            logger.debug(self._lp(f"Using temp dir {self.query_plan_manager.temp_dir}"))
            logger.debug(
                self._lp(
                    f"DuckDB version: {getattr(duckdb, '__version__', 'unknown')} "
                    f"| SQL table={self.parser.reflection_table} | RBAC view={self.parser.rbac_view}"
                )
            )

            # --- Snapshot selection -------------------------------------------
            snapshots = self.filter_snapshots(
                super_table_data=super_table_data, super_table_meta=super_table_meta
            )
            logger.debug(self._lp(f"Filtered snapshots: {len(snapshots)}"))
            if logger.isEnabledFor(20) and snapshots:
                snap_names = ", ".join(s["table_name"] for s in snapshots[:8])
                extra = "" if len(snapshots) <= 8 else f" …(+{len(snapshots)-8})"
                logger.debug(self._lp(f"Reading from simple tables: {snap_names}{extra}"))

            parquet_files, schema = self.process_snapshots(
                snapshots=snapshots, with_scan=with_scan
            )

            # Correct missing-columns detection (keep original behavior when '*' is used)
            missing_columns: Set[str] = set()
            if self.parser.columns_csv != "*":
                requested = _lower_set(self.parser.columns_list)
                missing_columns = requested - schema

            logger.debug(self._lp(f"Processed Snapshots: {len(snapshots)}"))
            logger.debug(self._lp(f"Processed Parquet Files: {len(parquet_files)}"))
            logger.debug(self._lp(f"Processed Schema: {len(schema)}"))
            logger.debug(self._lp(f"Missing Columns: {missing_columns}"))

            if logger.isEnabledFor(20):
                preview_cols = ", ".join(sorted(list(schema))[:10])
                more = "" if len(schema) <= 10 else f" …(+{len(schema)-10})"
                logger.debug(self._lp(f"Schema columns ({len(schema)}): {preview_cols}{more}"))

            if len(snapshots) == 0 or missing_columns or not parquet_files:
                message = (
                    f"Missing column(s): {', '.join(missing_columns)}"
                    if missing_columns
                    else "No parquet files found"
                )
                logger.warning(self._lp(f"Filter Result: {message}"))
                return pd.DataFrame(), status, message

            # RBAC check (logs paths it reads)
            restrict_read_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                user_hash=user_hash,
                table_name=self.parser.reflection_table,
                table_schema=schema,
                parsed_columns=self.parser.columns_list,
                parser=self.parser,
            )
            self.timer.capture_and_reset_timing(event="FILTERING")

            # --- Execute with DuckDB ------------------------------------------
            logger.debug(
                self._lp(
                    f"Executing: columns='{self.parser.columns_csv}' | files={len(parquet_files)} "
                    f"| with_scan={with_scan} | limit={getattr(self.parser, 'limit', None)}"
                )
            )
            result = self.execute_with_duckdb(
                parquet_files=parquet_files, query_manager=self.query_plan_manager
            )
            logger.debug(self._lp(f"Finished query: rows={result.shape[0]}, cols={result.shape[1]}"))

            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result = pd.DataFrame()
        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")

        # --- Monitoring extension (best-effort) -------------------------------
        try:
            extend_execution_plan(
                super_table=self.super_table,
                query_plan_manager=self.query_plan_manager,
                user_hash=user_hash,
                timing=self.timer.timings,
                plan_stats=self.plan_stats,
                status=str(status.value),
                message=message,
                result_shape=result.shape,
            )
        except Exception as e:
            logger.error(self._lp(f"extend_execution_plan exception: {e}"))

        # Final timing logs
        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        try:
            total = next((t["TOTAL_EXECUTE"] for t in self.timer.timings if "TOTAL_EXECUTE" in t), None)
            meta = next((t["META"] for t in self.timer.timings if "META" in t), 0.0)
            filt = next((t["FILTERING"] for t in self.timer.timings if "FILTERING" in t), 0.0)
            conn = next((t["CONNECTING"] for t in self.timer.timings if "CONNECTING" in t), 0.0)
            create = next((t["CREATING_REFLECTION"] for t in self.timer.timings if "CREATING_REFLECTION" in t), 0.0)
            execq = next((t["EXECUTING_QUERY"] for t in self.timer.timings if "EXECUTING_QUERY" in t), 0.0)
            extend = next((t["EXTENDING_PLAN"] for t in self.timer.timings if "EXTENDING_PLAN" in t), 0.0)

            # blue highlight for total
            total_str = f"\033[94m{total or 0.0:.3f}\033[32m"

            logger.info(
                f"[read][qid={self.query_plan_manager.query_id}] "
                "Timing(s): "
                f"total={total_str} | "
                f"meta={meta:.3f} | filter={filt:.3f} | connect={conn:.3f} | "
                f"create={create:.3f} | execute={execq:.3f} | extend={extend:.3f}"
            )
        except Exception:
            pass

        return result, status, message

    def process_snapshots(self, snapshots, with_scan) -> Tuple[List[str], Set[str]]:
        parquet_files: List[str] = []
        reflection_file_size = 0
        reflection_rows = 0

        schema: Set[str] = set()
        for snapshot in snapshots:
            current_snapshot_path = snapshot["path"]
            current_snapshot_data = self.super_table.read_simple_table_snapshot(
                current_snapshot_path
            )

            current_schema = current_snapshot_data.get("schema", {})
            # Ensure dict-like; merge column names (lowercased) into schema set
            schema.update(dict_keys_to_lowercase(current_schema).keys())

            # Ensure resources is a list (not {}), keep integers
            resources = current_snapshot_data.get("resources", []) or []
            for resource in resources:
                file_size = int(resource.get("file_size", 0))
                file_rows = int(resource.get("rows", 0))

                if (
                    with_scan
                    or self.parser.columns_csv == "*"
                    or any(
                        col in dict_keys_to_lowercase(current_schema).keys()
                        for col in [column.lower() for column in self.parser.columns_list]
                    )
                ):
                    parquet_files.append(resource["file"])
                    reflection_file_size += file_size
                    reflection_rows += file_rows

        self.plan_stats.add_stat({"REFLECTIONS": len(parquet_files)})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_ROWS": reflection_rows})

        if logger.isEnabledFor(20):  # INFO
            logger.debug(
                self._lp(
                    f"Selected parquet files: {len(parquet_files)} | "
                    f"total_rows={reflection_rows} | approx_size={reflection_file_size} bytes"
                )
            )

        return parquet_files, schema

    def execute_with_duckdb(self, parquet_files, query_manager: QueryPlanManager):
        """
        Use DuckDB to read and query the parquet files directly.
        Keep semantics aligned with the original behaviour.
        """
        con = duckdb.connect()
        try:
            # Measure connection time similar to the original logs
            self.timer.capture_and_reset_timing("CONNECTING")

            # Use broadly-compatible PRAGMAs
            con.execute("PRAGMA memory_limit='2GB';")
            con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
            con.execute("PRAGMA enable_profiling='json';")
            con.execute(f"PRAGMA profile_output = '{query_manager.query_plan_path}';")
            con.execute("PRAGMA default_collation='nocase';")

            parquet_files_str = ", ".join(f"'{file}'" for file in parquet_files)
            logger.debug(self._lp(f"Parsed Columns: {self.parser.columns_csv}"))

            # Project columns safely (preserve '*' exactly)
            if self.parser.columns_csv == "*":
                safe_columns_csv = "*"
            else:
                cols = [c for c in self.parser.columns_csv.split(",") if c.strip()]
                safe_columns_csv = ", ".join(_quote_if_needed(c) for c in cols)
            logger.debug(self._lp(f"Safe Columns: {safe_columns_csv}"))

            # Preserve original pattern: CREATE TABLE + CREATE VIEW
            create_table = f"""
CREATE TABLE {self.parser.reflection_table}
AS
SELECT {safe_columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=TRUE, HIVE_PARTITIONING=TRUE);
"""
            try:
                con.execute(create_table)
            except Exception as e:
                logger.error(self._lp(f"Error creating table: {create_table}\nError: {str(e)}"))
                raise

            create_view = f"""
CREATE VIEW {self.parser.rbac_view}
AS
{self.parser.view_definition}
"""
            logger.debug(self._lp(f"create_view: \n{create_view}"))
            con.execute(create_view)

            self.timer.capture_and_reset_timing("CREATING_REFLECTION")
            logger.debug(self._lp(f"Executing Query: {self.parser.executing_query}"))
            result = con.execute(query=self.parser.executing_query).fetchdf()
            logger.debug(self._lp(f"result.shape: (rows={result.shape[0]}, cols={result.shape[1]})"))
            return result
        finally:
            try:
                con.close()
            except Exception:
                pass
