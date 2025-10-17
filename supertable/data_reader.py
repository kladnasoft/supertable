# supertable/data_reader.py

from __future__ import annotations

from enum import Enum
from typing import Optional, Tuple, Any, List, Dict

import pandas as pd

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.super_table import SuperTable
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.plan_extender import extend_execution_plan
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access  # noqa: F401

from supertable.data_estimator import DataEstimator
from supertable.executor import Executor, Engine as _Engine


class Status(Enum):
    OK = "ok"
    ERROR = "error"


# Expose an enum named `engine` to match your call style:
class engine(Enum):  # noqa: N801
    AUTO = _Engine.AUTO.value
    DUCKDB = _Engine.DUCKDB.value
    SPARK = _Engine.SPARK.value

    def to_internal(self) -> _Engine:
        return _Engine(self.value)


class DataReader:
    """
    Facade — preserves the original interface; now delegates:
      - Estimation to DataEstimator
      - Execution to Executor (DuckDB/Spark)
    """

    def __init__(self, super_name: str, organization: str, query: str):
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.parser = SQLParser(query)
        self.parser.parse_sql()

        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None

        self._log_ctx = ""

    def _lp(self, msg: str) -> str:
        return f"{self._log_ctx}{msg}"

    def execute(
        self,
        user_hash: str,
        with_scan: bool = False,
        engine: engine = engine.AUTO,
    ) -> Tuple[pd.DataFrame, Status, Optional[str]]:
        status = Status.ERROR
        message: Optional[str] = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        # Make executor aware of storage for presign retry
        executor = Executor(storage=self.super_table.storage)

        try:
            # Initialize plan manager and query id/hash (same as before)
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                current_meta_path="redis://meta/root",
                parser=self.parser,
            )
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "

            # 1) ESTIMATE
            estimator = DataEstimator(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                query=self.parser.original_query,
            )
            estimation = estimator.estimate(user_hash=user_hash, with_scan=with_scan)

            file_list = list(estimation.get("FILE_LIST", []))
            bytes_total = int(estimation.get("BYTES_AFFECTED", 0))
            storage_type = str(estimation.get("STORAGE_TYPE", "UnknownStorage"))

            preview = ", ".join(file_list[:3]) + (" ..." if len(file_list) > 3 else "")
            logger.info(self._lp(f"[estimate] storage={storage_type} | files={len(file_list)} | bytes={bytes_total}"))
            # logger.info(self._lp(f"[paths] {preview}"))

            if not file_list:
                message = "No parquet files found"
                return pd.DataFrame(), status, message

            # 2) EXECUTE
            result_df, engine_used = executor.execute(
                engine=engine.to_internal(),
                file_list=file_list,
                bytes_total=bytes_total,
                parser=self.parser,
                query_manager=self.query_plan_manager,
                timer=self.timer,
                plan_stats=self.plan_stats,
                log_prefix=self._lp(""),
            )
            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result_df = pd.DataFrame()

        # Extend plan + timings
        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")
        try:
            extend_execution_plan(
                super_table=self.super_table,
                query_plan_manager=self.query_plan_manager,
                user_hash=user_hash,
                timing=self.timer.timings,
                plan_stats=self.plan_stats,
                status=str(status.value),
                message=message,
                result_shape=result_df.shape,
            )
        except Exception as e:
            logger.error(self._lp(f"extend_execution_plan exception: {e}"))

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        return result_df, status, message

def query_sql(
        organization: str,
        super_name: str,
        sql: str,
        limit: int,
        engine: Any,
        user_hash: str,
) -> Tuple[List[str], List[List[Any]], List[Dict[str, Any]]]:
    """
    Execute SQL query and return results in the format expected by MCP server.
    Returns: (columns, rows, columns_meta)
    """
    reader = DataReader(organization=organization, super_name=super_name, query=sql)

    # Execute the query
    result_df, status, message = reader.execute(
        user_hash=user_hash,
        engine=engine,
        with_scan=False,
    )

    if status == Status.ERROR:
        raise RuntimeError(f"Query execution failed: {message}")

    # Convert DataFrame to the expected format
    columns = list(result_df.columns)
    rows = result_df.values.tolist()

    # Create basic column metadata
    columns_meta = [
        {
            "name": col,
            "type": str(result_df[col].dtype),
            "nullable": True
        }
        for col in columns
    ]

    return columns, rows, columns_meta