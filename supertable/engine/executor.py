# supertable/engine/executor.py

from __future__ import annotations

from enum import Enum
from typing import Optional, Tuple

import pandas as pd

from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.engine.duckdb_transient import DuckDBTransient
from supertable.engine.duckdb_pinned import DuckDBPinned
from supertable.data_classes import Reflection


class Engine(Enum):
    AUTO = "auto"
    DUCKDB_TRANSIENT = "duckdb_transient"
    DUCKDB_PINNED = "duckdb_pinned"
    SPARK = "spark"

    # Backward compatibility alias
    DUCKDB = "duckdb_pinned"


# Module-level singleton for the pinned executor so the persistent
# connection survives across Executor instances (which are per-request).
_pinned_singleton: Optional[DuckDBPinned] = None
_pinned_lock = __import__("threading").Lock()


def _get_pinned(storage: Optional[object] = None) -> DuckDBPinned:
    global _pinned_singleton
    if _pinned_singleton is None:
        with _pinned_lock:
            if _pinned_singleton is None:
                _pinned_singleton = DuckDBPinned(storage=storage)
    return _pinned_singleton


class Executor:
    """
    Chooses execution engine and runs the query against the provided file list.
    """

    def __init__(self, storage: Optional[object] = None):
        self.storage = storage
        self.transient_exec = DuckDBTransient(storage=storage)
        self.spark_exec = None

    def _auto_pick(self, bytes_total: int) -> Engine:
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            spark_available = True
        except Exception:
            spark_available = False
        LARGE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
        if spark_available and bytes_total >= LARGE_BYTES:
            return Engine.SPARK
        return Engine.DUCKDB_PINNED

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

        if chosen == Engine.DUCKDB_TRANSIENT:
            df = self.transient_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "duckdb_transient"

        elif chosen in (Engine.DUCKDB_PINNED, Engine.DUCKDB):
            pinned = _get_pinned(storage=self.storage)
            df = pinned.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
            )
            used = "duckdb_pinned"

        elif chosen == Engine.SPARK:
            if self.spark_exec is None:
                from supertable.engine.spark_thrift import SparkThriftExecutor
                self.spark_exec = SparkThriftExecutor(storage=self.storage)
            # force=True when user explicitly requested Spark (not via AUTO)
            df = self.spark_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                force=(engine == Engine.SPARK),
            )
            used = "spark_thrift"

        else:
            raise ValueError(f"Unsupported engine: {engine}")

        plan_stats.add_stat({"ENGINE": used})
        return df, used