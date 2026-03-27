# supertable/engine/executor.py

from __future__ import annotations

import os
import time
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
from supertable.config.defaults import logger


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

    def _auto_pick(self, reflection: Reflection) -> Engine:
        """Select the best engine based on data size and freshness.

        Decision matrix (all thresholds configurable via env vars):

                              Data freshness
                         FRESH (<threshold)   STABLE (>=threshold)
                    ┌─────────────────────┬─────────────────────┐
          Small     │       LITE          │       LITE          │
          (<lite)   │  cheap anyway       │  cheap anyway       │
                    ├─────────────────────┼─────────────────────┤
          Medium    │       LITE          │       PRO           │
          (lite–spk)│  cache would churn  │  cache pays off     │
                    ├─────────────────────┼─────────────────────┤
          Large     │       SPARK *       │       SPARK *       │
          (>=spark) │  too big for DuckDB │  too big for DuckDB │
                    └─────────────────────┴─────────────────────┘

        * Spark only if pyspark is available; falls back to PRO otherwise.

        Env var overrides:
          SUPERTABLE_ENGINE_LITE_MAX_BYTES   – upper bound for Lite (default 100 MB)
          SUPERTABLE_ENGINE_SPARK_MIN_BYTES  – lower bound for Spark (default 10 GB)
          SUPERTABLE_ENGINE_FRESHNESS_SEC    – age threshold in seconds (default 300)
        """
        bytes_total = reflection.reflection_bytes

        # --- thresholds (env-configurable) ---
        try:
            lite_max = int(os.getenv("SUPERTABLE_ENGINE_LITE_MAX_BYTES",
                                     str(100 * 1024 * 1024)))
        except ValueError:
            lite_max = 100 * 1024 * 1024  # 100 MB

        try:
            spark_min = int(os.getenv("SUPERTABLE_ENGINE_SPARK_MIN_BYTES",
                                      str(10 * 1024 * 1024 * 1024)))
        except ValueError:
            spark_min = 10 * 1024 * 1024 * 1024  # 10 GB

        try:
            freshness_threshold_s = int(os.getenv("SUPERTABLE_ENGINE_FRESHNESS_SEC", "300"))
        except ValueError:
            freshness_threshold_s = 300  # 5 min

        # --- freshness: how long ago was the most recent snapshot updated ---
        if reflection.freshness_ms > 0:
            age_s = (time.time() * 1000 - reflection.freshness_ms) / 1000.0
            data_is_fresh = age_s < freshness_threshold_s
        else:
            # Unknown freshness — assume stable so Pro gets a chance to cache.
            age_s = -1
            data_is_fresh = False

        # --- Spark gate ---
        spark_available = False
        if bytes_total >= spark_min:
            try:
                from pyspark.sql import SparkSession  # noqa: F401
                spark_available = True
            except Exception:
                pass

        # --- decision ---
        if spark_available and bytes_total >= spark_min:
            chosen = Engine.SPARK_SQL
            reason = f"bytes={bytes_total} >= spark_min={spark_min}"
        elif bytes_total <= lite_max:
            chosen = Engine.DUCKDB_LITE
            reason = f"bytes={bytes_total} <= lite_max={lite_max}"
        elif data_is_fresh:
            chosen = Engine.DUCKDB_LITE
            reason = f"data fresh (age={age_s:.0f}s < {freshness_threshold_s}s), cache would churn"
        else:
            chosen = Engine.DUCKDB_PRO
            reason = f"stable data (age={age_s:.0f}s >= {freshness_threshold_s}s), cache pays off"

        logger.info(
            f"[engine.auto] {chosen.value} — {reason} "
            f"(files={reflection.total_reflections}, bytes={bytes_total})"
        )
        return chosen

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
        chosen = engine if engine != Engine.AUTO else self._auto_pick(reflection)

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
