# supertable/engine/executor.py

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd

from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb_lite import DuckDBLite
from supertable.engine.duckdb_pro import DuckDBPro
from supertable.data_classes import Reflection


# Module-level singleton for the pro executor so the persistent
# connection survives across Executor instances (which are per-request).
_pro_singleton: Optional[DuckDBPro] = None
_pro_lock = __import__("threading").Lock()


def _get_pro(storage: Optional[object] = None) -> DuckDBPro:
    global _pro_singleton
    if _pro_singleton is None:
        with _pro_lock:
            if _pro_singleton is None:
                _pro_singleton = DuckDBPro(storage=storage)
    return _pro_singleton


class Executor:
    """
    Chooses execution engine and runs the query against the provided file list.
    """

    def __init__(self, storage: Optional[object] = None, organization: str = ""):
        self.storage = storage
        self.organization = organization
        self.lite_exec = DuckDBLite(storage=storage)
        self.spark_exec = None

    def _auto_pick(self, bytes_total: int) -> Engine:
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            spark_available = True
        except Exception:
            spark_available = False
        LARGE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
        if spark_available and bytes_total >= LARGE_BYTES:
            return Engine.SPARK_SQL
        return Engine.DUCKDB_PRO

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

        if chosen == Engine.DUCKDB_LITE:
            df = self.lite_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "duckdb_lite"

        elif chosen == Engine.DUCKDB_PRO:
            pro = _get_pro(storage=self.storage)
            df = pro.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "duckdb_pro"

        elif chosen == Engine.SPARK_SQL:
            if self.spark_exec is None:
                from supertable.engine.spark_thrift import SparkThriftExecutor
                self.spark_exec = SparkThriftExecutor(
                    storage=self.storage, organization=self.organization,
                )
            # force=True when user explicitly requested Spark (not via AUTO)
            df = self.spark_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                force=(engine == Engine.SPARK_SQL),
            )
            used = "spark_sql"

        else:
            raise ValueError(f"Unsupported engine: {engine}")

        plan_stats.add_stat({"ENGINE": used})
        return df, used
