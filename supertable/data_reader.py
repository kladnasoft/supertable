# supertable/data_reader.py

from __future__ import annotations

from enum import Enum
from typing import Optional, Tuple, Any, List, Dict

import pandas as pd

from supertable.config.defaults import logger
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.plan_extender import extend_execution_plan
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access  # noqa: F401

from supertable.data_estimator import DataEstimator
from supertable.executor import Executor, Engine as _Engine
from supertable.data_classes import TableDefinition


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


from collections import defaultdict
from typing import List, Tuple



class DataReader:
    """
    Facade — preserves the original interface; now delegates:
      - Estimation to DataEstimator
      - Execution to Executor (DuckDB/Spark)
    """

    def __init__(self, super_name: str, organization: str, query: str):
        self.super_name = super_name
        self.organization = organization
        self.parser = SQLParser(super_name=super_name, query=query)
        self.tables = self.parser.get_table_tuples()

        self.storage: StorageInterface = get_storage()

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

        # RBAC check before returning
        restrict_read_access(
            super_name=self.super_name,
            organization=self.organization,
            user_hash=user_hash,
            tables=self.tables
        )

        try:
            # Make executor aware of storage for presign retry
            executor = Executor(storage=self.storage)

            # Initialize plan manager and query id/hash (same as before)
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_name,
                organization=self.organization,
                current_meta_path="redis://meta/root",
                query=self.parser.original_query,
            )
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "

            # 1) ESTIMATE
            estimator = DataEstimator(
                organization=self.organization,
                storage=self.storage,
                tables=self.tables
            )
            reflection = estimator.estimate()

            logger.info(self._lp(f"[estimate] storage={reflection.storage_type} | files={reflection.total_reflections} | bytes={reflection.reflection_bytes}"))


            if not reflection.supers:
                message = "No parquet files found"
                return pd.DataFrame(), status, message

            # 2) EXECUTE
            result_df, engine_used = executor.execute(
                engine=engine.to_internal(),
                reflection=reflection,
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