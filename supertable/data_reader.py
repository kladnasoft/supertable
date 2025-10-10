# supertable/data_reader.py

from __future__ import annotations

from enum import Enum
from typing import Iterable, Set, Tuple, List, Dict

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
from supertable.redis_catalog import RedisCatalog


class Status(Enum):
    OK = "ok"
    ERROR = "error"


def _lower_set(items: Iterable[str]) -> Set[str]:
    return {str(x).lower() for x in items}


def _quote_if_needed(col: str) -> str:
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
        self._log_ctx = ""
        self.catalog = RedisCatalog()

    def _lp(self, msg: str) -> str:
        return f"{self._log_ctx}{msg}"

    def _collect_snapshots_from_redis(self) -> List[Dict]:
        """
        Build a list shaped like old 'snapshots' entries from Redis leaves.
        Each item: {"table_name": <simple>, "path": <snapshot_path>, "files": 0, "rows": 0, "file_size": 0, "last_updated_ms": ts}
        File/row/size will be recomputed when reading heavy snapshot inside process_snapshots().
        """
        items = list(self.catalog.scan_leaf_items(self.super_table.organization, self.super_table.super_name, count=512))
        snapshots = []
        for it in items:
            if not it.get("path"):
                continue
            snapshots.append(
                {
                    "table_name": it["simple"],
                    "last_updated_ms": int(it.get("ts", 0)),
                    "path": it["path"],
                    "files": 0,
                    "rows": 0,
                    "file_size": 0,
                }
            )
        return snapshots

    def filter_snapshots(self, snapshots: List[Dict]) -> List[Dict]:
        if self.super_table.super_name.lower() == self.parser.original_table.lower():
            return [s for s in snapshots if not (s["table_name"].startswith("__") and s["table_name"].endswith("__"))]
        else:
            return [s for s in snapshots if s["table_name"].lower() == self.parser.original_table.lower()]

    timer = Timer()

    def execute(self, user_hash: str, with_scan: bool = False):
        status = Status.ERROR
        message = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        try:
            # --- Planning / IDs -------------------------------------------------
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                current_meta_path="redis://meta/root",  # informational only
                parser=self.parser,
            )
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "

            logger.debug(self._lp(f"DuckDB version: {getattr(duckdb, '__version__', 'unknown')} "
                                  f"| SQL table={self.parser.reflection_table} | RBAC view={self.parser.rbac_view}"))

            # --- Snapshot selection via Redis ---------------------------------
            snapshots_all = self._collect_snapshots_from_redis()
            snapshots = self.filter_snapshots(snapshots_all)
            logger.debug(self._lp(f"Filtered snapshots: {len(snapshots)}"))
            if logger.isEnabledFor(20) and snapshots:
                snap_names = ", ".join(s["table_name"] for s in snapshots[:8])
                extra = "" if len(snapshots) <= 8 else f" …(+{len(snapshots)-8})"
                logger.debug(self._lp(f"Reading from simple tables: {snap_names}{extra}"))

            parquet_files, schema = self.process_snapshots(snapshots=snapshots, with_scan=with_scan)

            missing_columns: Set[str] = set()
            if self.parser.columns_csv != "*":
                requested = _lower_set(self.parser.columns_list)
                missing_columns = requested - schema

            logger.debug(self._lp(f"Processed Snapshots: {len(snapshots)}"))
            logger.debug(self._lp(f"Processed Parquet Files: {len(parquet_files)}"))
            logger.debug(self._lp(f"Processed Schema: {len(schema)}"))
            logger.debug(self._lp(f"Missing Columns: {missing_columns}"))

            if len(snapshots) == 0 or missing_columns or not parquet_files:
                message = (
                    f"Missing column(s): {', '.join(missing_columns)}" if missing_columns else "No parquet files found"
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
            result = self.execute_with_duckdb(parquet_files=parquet_files, query_manager=self.query_plan_manager)
            logger.debug(self._lp(f"Finished query: rows={result.shape[0]}, cols={result.shape[1]}"))

            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result = pd.DataFrame()

        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")

        # Monitoring extension (best-effort)
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

            total_str = f"\033[94m{(total or 0.0):.3f}\033[32m"
            logger.info(
                f"[read][qid={self.query_plan_manager.query_id}] Timing(s): "
                f"total={total_str} | meta={meta:.3f} | filter={filt:.3f} | connect={conn:.3f} | "
                f"create={create:.3f} | execute={execq:.3f} | extend={extend:.3f}"
            )
        except Exception:
            pass

        return result, status, message

    def process_snapshots(self, snapshots: List[Dict], with_scan: bool) -> Tuple[List[str], Set[str]]:
        parquet_files: List[str] = []
        reflection_file_size = 0
        reflection_rows = 0

        schema: Set[str] = set()
        for snapshot in snapshots:
            current_snapshot_path = snapshot["path"]
            current_snapshot_data = self.super_table.read_simple_table_snapshot(current_snapshot_path)

            current_schema = current_snapshot_data.get("schema", {})
            schema.update(dict_keys_to_lowercase(current_schema).keys())

            resources = current_snapshot_data.get("resources", []) or []
            for resource in resources:
                file_size = int(resource.get("file_size", 0))
                file_rows = int(resource.get("rows", 0))

                parquet_files.append(resource["file"])
                reflection_file_size += file_size
                reflection_rows += file_rows

        self.plan_stats.add_stat({"REFLECTIONS": len(parquet_files)})
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_ROWS": reflection_rows})

        if logger.isEnabledFor(20):
            logger.debug(
                self._lp(
                    f"Selected parquet files: {len(parquet_files)} | "
                    f"total_rows={reflection_rows} | approx_size={reflection_file_size} bytes"
                )
            )

        return parquet_files, schema

    def execute_with_duckdb(self, parquet_files, query_manager: QueryPlanManager):
        con = duckdb.connect()
        try:
            self.timer.capture_and_reset_timing("CONNECTING")
            con.execute("PRAGMA memory_limit='2GB';")
            con.execute(f"PRAGMA temp_directory='{query_manager.temp_dir}';")
            con.execute("PRAGMA enable_profiling='json';")
            con.execute(f"PRAGMA profile_output = '{query_manager.query_plan_path}';")
            con.execute("PRAGMA default_collation='nocase';")

            parquet_files_str = ", ".join(f"'{file}'" for file in parquet_files)
            if self.parser.columns_csv == "*":
                safe_columns_csv = "*"
            else:
                cols = [c for c in self.parser.columns_csv.split(",") if c.strip()]
                safe_columns_csv = ", ".join(_quote_if_needed(c) for c in cols)

            create_table = f"""
CREATE TABLE {self.parser.reflection_table}
AS
SELECT {safe_columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=TRUE, HIVE_PARTITIONING=TRUE);
"""
            con.execute(create_table)

            create_view = f"""
CREATE VIEW {self.parser.rbac_view}
AS
{self.parser.view_definition}
"""
            con.execute(create_view)

            self.timer.capture_and_reset_timing("CREATING_REFLECTION")
            result = con.execute(query=self.parser.executing_query).fetchdf()
            return result
        finally:
            try:
                con.close()
            except Exception:
                pass
