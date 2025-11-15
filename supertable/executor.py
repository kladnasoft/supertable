# supertable/executor.py

from __future__ import annotations

from enum import Enum
from typing import List, Optional, Tuple

import pandas as pd

from supertable.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.execute_duckdb import DuckDBExecutor
from supertable.execute_spark import SparkExecutor
from supertable.data_classes import Reflection

class Engine(Enum):
    AUTO = "auto"
    DUCKDB = "duckdb"
    SPARK = "spark"


class Executor:
    """
    Chooses execution engine and runs the query against the provided file list.
    """

    def __init__(self, storage: Optional[object] = None):
        self.duckdb_exec = DuckDBExecutor(storage=storage)
        self.spark_exec: Optional[SparkExecutor] = None

    def _auto_pick(self, bytes_total: int) -> Engine:
        try:
            from pyspark.sql import SparkSession # noqa: F401
            spark_available = True
        except Exception:
            spark_available = False
        LARGE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
        if spark_available and bytes_total >= LARGE_BYTES:
            return Engine.SPARK
        return Engine.DUCKDB

    def execute(
        self,
        engine: Engine,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer: Timer,
        plan_stats: PlanStats,
        log_prefix: str,
    ) -> Tuple[pd.DataFrame, str]:
        chosen = engine if engine != Engine.AUTO else self._auto_pick(reflection.reflection_bytes)

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        if chosen == Engine.DUCKDB:
            df = self.duckdb_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "duckdb"
        elif chosen == Engine.SPARK:
            if self.spark_exec is None:
                self.spark_exec = SparkExecutor()
            df = self.spark_exec.execute(
                selected=selected,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "spark"
        else:
            raise ValueError(f"Unsupported engine: {engine}")

        plan_stats.add_stat({"ENGINE": used})
        return df, used
