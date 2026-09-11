# supertable/engine/executor.py

from __future__ import annotations

import os
import time
from typing import Optional, Tuple

import polars as pl

from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser

from supertable.config.settings import settings
from supertable.engine.arrow_result import materialize
from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb import DuckDBEngine
from supertable.engine.engine_config import resolve_engine_configs, EngineRuntimeConfig
from supertable.data_classes import Reflection
from supertable.config.defaults import logger


class Executor:
    """
    Chooses execution engine and runs the query against the provided file list.
    """

    def __init__(self, storage: Optional[object] = None, organization: str = ""):
        self.storage = storage
        self.organization = organization
        self.duck_exec = DuckDBEngine(storage=storage)
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
        """Pick DuckDB or Spark.

        With one DuckDB engine the choice is binary: hand the query to Spark
        only when an **active Spark cluster is registered** for the org AND the
        job reaches the fleet's minimum accepted size (the smallest
        ``min_bytes`` across active clusters — see :meth:`_spark_min_bytes`).
        Everything else runs on DuckDB, so with no cluster registered AUTO
        always picks DuckDB.

        The concrete cluster is chosen later by
        :meth:`RedisCatalog.select_spark_cluster`, at random among the active
        clusters whose ``[min_bytes, max_bytes]`` window contains the job.

        Data freshness no longer participates: it existed only to decide when
        the pro flavour's materialised cache would pay for itself, and that
        flavour is gone.

        Env var override:
          SUPERTABLE_ENGINE_SPARK_MIN_BYTES  – Spark floor used only when no
                                               active cluster is registered
        """
        bytes_total = reflection.reflection_bytes

        # There is ONE DuckDB engine, so AUTO is a single question: is Spark
        # available and is this query big enough to be worth shipping to it?
        # If no Spark cluster is registered, AUTO always picks DuckDB. The old
        # size/freshness matrix existed only to decide between the lite and pro
        # DuckDB flavours, and pro is gone.
        active_clusters = self._active_spark_clusters()
        spark_available = bool(active_clusters)
        spark_min = self._spark_min_bytes(cfg, active_clusters)

        if spark_available and bytes_total >= spark_min:
            chosen = Engine.SPARK_SQL
            reason = (f"bytes={bytes_total} >= fleet_min={spark_min} "
                      f"({len(active_clusters)} active cluster(s))")
        elif spark_available:
            chosen = Engine.DUCKDB
            reason = f"bytes={bytes_total} < fleet_min={spark_min}"
        else:
            chosen = Engine.DUCKDB
            reason = "no active Spark cluster"

        logger.info(
            f"[engine.auto] {chosen.value} — {reason} "
            f"(files={reflection.total_reflections}, bytes={bytes_total})"
        )
        return chosen

    def _get_spark(self):
        """Lazily build the Spark executor; shared by execute() and stream()."""
        if self.spark_exec is None:
            from supertable.engine.spark_thrift import SparkThriftExecutor
            self.spark_exec = SparkThriftExecutor(
                storage=self.storage, organization=self.organization,
            )
        return self.spark_exec

    def stream(
        self,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer: Timer,
        log_prefix: str,
        engine: Engine = Engine.AUTO,
        batch_rows: int = 0,
        explain: bool = False,
        explain_options: str = "",
        expose_rowid: bool = False,
    ):
        """Return an open Arrow stream for this query.

        Both engines stream. DuckDB pulls Arrow batches straight out of the
        result; Spark pulls rows with ``fetchmany`` over Thrift and converts
        each batch to Arrow. Neither collects the whole result first.

        AUTO routes the same way a buffered query would, so a caller does not
        get a different engine merely for asking to stream.
        """
        cfgs = resolve_engine_configs(self.organization, self._get_catalog())
        duck_cfg = cfgs["lite"]
        chosen = engine if engine != Engine.AUTO else self._auto_pick(
            reflection, duck_cfg)

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        if chosen == Engine.SPARK_SQL:
            rows = int(batch_rows or settings.SUPERTABLE_STREAM_BATCH_ROWS)
            return self._get_spark().execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                stream_batch_rows=rows,
            )

        return self.duck_exec.stream(
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer_capture=timer_capture,
            log_prefix=log_prefix,
            engine_config=duck_cfg,
            batch_rows=batch_rows,
            explain=explain,
            explain_options=explain_options,
            expose_rowid=expose_rowid,
        )

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
    ) -> Tuple[pl.DataFrame, str]:
        # Resolve engine config live (Redis → env → default) for this query so
        # UI changes take effect immediately without restart or cache.  Lite and
        # Pro carry independent DuckDB pragmas; the shared auto-pick thresholds
        # are identical in both, so either may drive the routing decision.
        cfgs = resolve_engine_configs(self.organization, self._get_catalog())
        duck_cfg = cfgs["lite"]

        chosen = engine if engine != Engine.AUTO else self._auto_pick(reflection, duck_cfg)

        def timer_capture(evt: str):
            timer.capture_and_reset_timing(evt)

        # Buffering is now just a consumer that keeps every batch. There is
        # one read implementation, so a buffered result and a streamed result
        # cannot drift — they are produced by the same code with the same view
        # chain, the same deletion-vector anti-join and the same RBAC filter.
        if chosen == Engine.DUCKDB:
            handle = self.duck_exec.stream(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=duck_cfg,
                explain=explain,
                explain_options=explain_options,
            )
            df = materialize(handle)
            used = "duckdb"

        elif chosen == Engine.SPARK_SQL:
            handle = self._get_spark().execute(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                force=(engine == Engine.SPARK_SQL),
                stream_batch_rows=settings.SUPERTABLE_STREAM_BATCH_ROWS,
            )
            df = materialize(handle)
            used = "spark_sql"

        else:
            raise ValueError(f"Unsupported engine: {engine}")

        plan_stats.add_stat({"ENGINE": used})
        return df, used
