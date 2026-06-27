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
from supertable.engine.engine_config import resolve_engine_configs, EngineRuntimeConfig
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
        self._catalog = None  # lazily created RedisCatalog for live config reads

    def _get_catalog(self):
        """Lazily create a RedisCatalog for live engine-config reads.

        Returns None when Redis is unreachable so config resolution degrades to
        environment variables and built-in defaults instead of failing a query.
        """
        if self._catalog is None:
            try:
                from supertable.redis_catalog import RedisCatalog
                self._catalog = RedisCatalog()
            except Exception:
                self._catalog = False  # sentinel: construction failed, do not retry
        return self._catalog or None

    def _active_spark_clusters(self) -> list:
        """Active Spark Thrift clusters registered for this org (best-effort).

        Returns ``[]`` when no catalog is reachable or none are active, which
        makes AUTO stay on DuckDB instead of routing to a fleet that cannot run
        the job.
        """
        catalog = self._get_catalog()
        if catalog is None:
            return []
        try:
            clusters = catalog.list_spark_clusters(self.organization) or []
        except Exception:
            return []
        return [
            c for c in clusters
            if isinstance(c, dict) and c.get("status") == "active"
        ]

    def _spark_min_bytes(self, cfg: EngineRuntimeConfig, active_clusters: Optional[list] = None) -> int:
        """Byte size at which AUTO hands a query to the Spark fleet.

        Fleet-driven: the **smallest** ``min_bytes`` across active clusters —
        the lowest job size any active cluster will accept.  A job at or above
        this triggers Spark; :meth:`RedisCatalog.select_spark_cluster` then
        picks (at random) one of the clusters whose ``[min_bytes, max_bytes]``
        window contains the job.

        Falls back to the ``engine_spark_min_bytes`` policy value only when no
        active cluster is known (catalog down / empty fleet).  In that case
        :meth:`_auto_pick` gates on an active cluster existing, so AUTO won't
        route to Spark regardless of the returned bound.
        """
        if active_clusters is None:
            active_clusters = self._active_spark_clusters()
        mins = []
        for c in active_clusters:
            try:
                mins.append(int(c.get("min_bytes", 0)))
            except (TypeError, ValueError):
                continue
        if mins:
            return min(mins)
        return cfg.engine_spark_min_bytes

    def _auto_pick(self, reflection: Reflection, cfg: EngineRuntimeConfig) -> Engine:
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
          (>=spark) │  hand off to fleet  │  hand off to fleet  │
                    └─────────────────────┴─────────────────────┘

        * Spark is chosen only when an **active Spark cluster is registered**
          for the org and the job reaches the fleet's minimum accepted size
          (the smallest ``min_bytes`` across active clusters — see
          :meth:`_spark_min_bytes`).  With no active cluster, AUTO stays on
          DuckDB regardless of size.  The concrete cluster is chosen later by
          :meth:`RedisCatalog.select_spark_cluster`, at random among the active
          clusters whose ``[min_bytes, max_bytes]`` window contains the job.

        Env var overrides:
          SUPERTABLE_ENGINE_LITE_MAX_BYTES   – upper bound for Lite (default 100 MB)
          SUPERTABLE_ENGINE_SPARK_MIN_BYTES  – Spark floor used only when no
                                               active cluster is registered
          SUPERTABLE_ENGINE_FRESHNESS_SEC    – age threshold in seconds (default 300)
        """
        bytes_total = reflection.reflection_bytes

        # --- thresholds (resolved live from org system config) ---
        lite_max = cfg.engine_lite_max_bytes
        freshness_threshold_s = cfg.engine_freshness_sec

        # --- Spark fleet: the registered clusters decide availability + floor ---
        active_clusters = self._active_spark_clusters()
        spark_available = bool(active_clusters)
        spark_min = self._spark_min_bytes(cfg, active_clusters)

        # --- freshness: how long ago was the most recent snapshot updated ---
        if reflection.freshness_ms > 0:
            age_s = (time.time() * 1000 - reflection.freshness_ms) / 1000.0
            data_is_fresh = age_s < freshness_threshold_s
        else:
            # Unknown freshness — assume stable so Pro gets a chance to cache.
            age_s = -1
            data_is_fresh = False

        # --- decision ---
        if spark_available and bytes_total >= spark_min:
            chosen = Engine.SPARK_SQL
            reason = (f"bytes={bytes_total} >= fleet_min={spark_min} "
                      f"({len(active_clusters)} active cluster(s))")
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
        explain: bool = False,
        explain_options: str = "",
    ) -> Tuple[pd.DataFrame, str]:
        # Resolve engine config live (Redis → env → default) for this query so
        # UI changes take effect immediately without restart or cache.  Lite and
        # Pro carry independent DuckDB pragmas; the shared auto-pick thresholds
        # are identical in both, so either may drive the routing decision.
        cfgs = resolve_engine_configs(self.organization, self._get_catalog())
        lite_cfg = cfgs["lite"]
        pro_cfg = cfgs["pro"]

        chosen = engine if engine != Engine.AUTO else self._auto_pick(reflection, lite_cfg)

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        if chosen == Engine.DUCKDB_LITE:
            df = self.lite_exec.execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=lite_cfg,
                explain=explain,
                explain_options=explain_options,
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
                engine_config=pro_cfg,
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
