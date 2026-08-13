# supertable/engine/tests/test_engine_routing.py
"""Tests for fleet-driven Spark routing.

Under AUTO, the registered Spark Thrift fleet decides routing:

  * :meth:`Executor._auto_pick` routes to Spark only when at least one
    **active, size-window-compatible** cluster is registered AND the candidate
    scan reaches that eligible fleet's minimum accepted size. With no fitting
    active cluster, AUTO stays on DuckDB regardless of size.
  * :meth:`RedisCatalog.select_spark_cluster` then picks, at random, one of the
    active clusters whose ``[min_bytes, max_bytes]`` window contains the job.

The ``engine_spark_min_bytes`` policy value is only a fallback used when no
active cluster is known (in which case AUTO won't pick Spark anyway).
"""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace

import pytest

import supertable.engine.executor as executor_module
from supertable.engine.executor import Executor
from supertable.engine.engine_config import AutoRoutingRule, EngineRuntimeConfig
from supertable.engine.engine_enum import Engine
from supertable.engine.island_resources import (
    ExecutionAdvice,
    ResultMemoryLimitExceeded,
)
from supertable.engine.plan_stats import PlanStats
from supertable.redis_catalog import RedisCatalog

GIB = 1024 ** 3


def _cfg(spark_min_bytes: int) -> EngineRuntimeConfig:
    return EngineRuntimeConfig(
        engine_lite_max_bytes=100 * 1024 * 1024,
        engine_spark_min_bytes=spark_min_bytes,
        engine_freshness_sec=300,
        duckdb_memory_limit="1GB",
        duckdb_io_multiplier=3.0,
        duckdb_threads=None,
        duckdb_http_timeout=None,
        duckdb_external_cache_size="",
    )


class _Catalog:
    def __init__(self, clusters):
        self._clusters = clusters

    def list_spark_clusters(self, org):
        return self._clusters


class _RaisingCatalog:
    def list_spark_clusters(self, org):
        raise RuntimeError("redis down")


def _executor(catalog) -> Executor:
    # Bypass __init__ (which builds a DuckDBLite); we only exercise routing.
    e = Executor.__new__(Executor)
    e._catalog = catalog
    e.organization = "kladna-soft"
    return e


def _active(min_bytes, max_bytes=0):
    return {"status": "active", "min_bytes": min_bytes, "max_bytes": max_bytes}


def _reflection(bytes_total, freshness_ms=0):
    return SimpleNamespace(
        reflection_bytes=bytes_total,
        freshness_ms=freshness_ms,
        total_reflections=1,
    )


# --------------------------------------------------------------------------- #
# _spark_min_bytes — the fleet-driven trigger size
# --------------------------------------------------------------------------- #

def test_no_catalog_uses_policy_fallback():
    # ``False`` is the executor's "catalog unavailable, do not build" sentinel,
    # so _get_catalog() returns None without contacting Redis.
    assert _executor(False)._spark_min_bytes(_cfg(10 * GIB)) == 10 * GIB


def test_empty_fleet_uses_policy_fallback():
    assert _executor(_Catalog([]))._spark_min_bytes(_cfg(10 * GIB)) == 10 * GIB


def test_catalog_error_falls_back_to_policy():
    assert _executor(_RaisingCatalog())._spark_min_bytes(_cfg(10 * GIB)) == 10 * GIB


def test_fleet_min_drives_even_below_policy():
    # A cluster accepts >=1 GiB: the fleet min now drives the trigger, so AUTO
    # hands jobs to Spark at 1 GiB — the policy value no longer raises the bar.
    cat = _Catalog([_active(1 * GIB)])
    assert _executor(cat)._spark_min_bytes(_cfg(10 * GIB)) == 1 * GIB


def test_smallest_active_cluster_drives_the_fleet_min():
    cat = _Catalog([_active(50 * GIB), _active(20 * GIB), _active(30 * GIB)])
    assert _executor(cat)._spark_min_bytes(_cfg(10 * GIB)) == 20 * GIB


def test_inactive_clusters_are_ignored():
    cat = _Catalog([
        {"status": "draining", "min_bytes": 1 * GIB, "max_bytes": 0},
        {"status": "offline", "min_bytes": 2 * GIB, "max_bytes": 0},
    ])
    # No active cluster -> policy fallback.
    assert _executor(cat)._spark_min_bytes(_cfg(10 * GIB)) == 10 * GIB


def test_malformed_min_bytes_is_skipped_not_fatal():
    cat = _Catalog([
        {"status": "active", "min_bytes": "not-a-number", "max_bytes": 0},
        _active(7 * GIB),
    ])
    assert _executor(cat)._spark_min_bytes(_cfg(1 * GIB)) == 7 * GIB


# --------------------------------------------------------------------------- #
# _auto_pick — the active-cluster gate
# --------------------------------------------------------------------------- #

def test_auto_routes_to_spark_when_active_cluster_and_big_enough():
    cat = _Catalog([_active(1 * GIB)])
    chosen = _executor(cat)._auto_pick(_reflection(2 * GIB), _cfg(0))
    assert chosen == Engine.SPARK_SQL


def test_auto_stays_on_duckdb_below_fleet_min():
    # 512 MiB < the cluster's 1 GiB floor -> DuckDB, not Spark.
    cat = _Catalog([_active(1 * GIB)])
    chosen = _executor(cat)._auto_pick(_reflection(512 * 1024 * 1024), _cfg(0))
    assert chosen != Engine.SPARK_SQL


def test_auto_never_spark_without_active_cluster_even_when_huge():
    # No active cluster: a 500 GiB job still stays on DuckDB (Pro for stable data).
    cat = _Catalog([])
    chosen = _executor(cat)._auto_pick(_reflection(500 * GIB), _cfg(10 * GIB))
    assert chosen == Engine.DUCKDB_PRO


def test_auto_unknown_source_size_never_routes_spark_or_island(monkeypatch):
    cat = _Catalog([_active(1)])
    executor = _executor(cat)
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
    )
    reflection = _reflection(0)
    reflection.source_bytes_complete = False
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(reflection, _cfg(0), parser=object())

    assert chosen is Engine.DUCKDB_PRO


def _bounded_island_plan():
    return SimpleNamespace(
        advice=ExecutionAdvice.ISLAND_IN_MEMORY,
        cpu_workers=4,
        io_workers=8,
    )


def test_auto_routes_supported_bounded_query_to_islanddb(monkeypatch):
    """Decoded working-set bounds, not whole-file warmth, admit IslandDB."""
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: _bounded_island_plan(),
    )
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        _reflection(512 * 1024 * 1024), _cfg(10 * GIB), parser=object(),
    )

    assert chosen is Engine.ISLANDDB


def test_redis_policy_selects_island_by_effective_scan_even_when_auto_flag_off(
    monkeypatch,
):
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: _bounded_island_plan(),
    )
    disabled = dataclasses.replace(
        executor_module.settings, SUPERTABLE_ISLAND_AUTO_ENABLED=False,
    )
    monkeypatch.setattr(executor_module, "settings", disabled)
    stats = PlanStats()

    chosen = executor._auto_pick(
        _reflection(50 * 1024 * 1024),
        _cfg(10 * GIB),
        parser=object(),
        plan_stats=stats,
        routing_policy=(AutoRoutingRule(0, 100 * 1024 * 1024, Engine.ISLANDDB),),
    )

    assert chosen is Engine.ISLANDDB
    routing = next(x["AUTO_ROUTING"] for x in stats.stats if "AUTO_ROUTING" in x)
    assert routing["manual_policy"]["engine"] == "islanddb"


def test_unsafe_redis_spark_rule_falls_back_to_safe_cost_model():
    # No active Spark fleet: the manual choice cannot bypass availability.
    chosen = _executor(_Catalog([]))._auto_pick(
        _reflection(200 * 1024 * 1024),
        _cfg(0),
        routing_policy=(AutoRoutingRule(100 * 1024 * 1024, None, Engine.SPARK_SQL),),
    )
    assert chosen is not Engine.SPARK_SQL


def test_auto_uses_range_native_query_without_whole_file_warmth(monkeypatch):
    """Cold Island scans are eligible because only sealed ranges are fetched."""
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: _bounded_island_plan(),
    )
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        _reflection(512 * 1024 * 1024), _cfg(10 * GIB), parser=object(),
    )

    assert chosen is Engine.ISLANDDB


def test_auto_uses_trusted_row_group_size_before_spark_threshold(monkeypatch):
    """A huge table with one tiny candidate row group is not a huge job."""
    executor = _executor(_Catalog([_active(1 * GIB)]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: _bounded_island_plan(),
    )
    reflection = _reflection(10 * GIB)
    reflection.row_group_scan_bytes = 8 * 1024 * 1024
    reflection.row_group_scan_bytes_complete = True
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        reflection, _cfg(0), parser=object(),
    )

    assert chosen is Engine.ISLANDDB


def test_auto_materialized_result_fails_before_fetch_when_streaming_is_required(
    monkeypatch,
):
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: SimpleNamespace(
            advice=ExecutionAdvice.STREAM_RESULT,
            cpu_workers=2,
            io_workers=2,
            estimated_spill_bytes=0,
        ),
    )
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)
    stats = PlanStats()

    with pytest.raises(ResultMemoryLimitExceeded, match="execute_stream"):
        executor._auto_pick(
            _reflection(512 * 1024 * 1024),
            _cfg(10 * GIB),
            parser=object(),
            plan_stats=stats,
        )

    blocked = next(
        item["AUTO_ROUTING_BLOCKED"]
        for item in stats.stats if "AUTO_ROUTING_BLOCKED" in item
    )
    assert blocked["reason_code"] == "streaming_result_required"


def test_auto_routes_decoded_heavy_small_object_to_spark(monkeypatch):
    """Compressed bytes cannot override a native decoded/operator warning."""
    executor = _executor(_Catalog([_active(1)]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: SimpleNamespace(
            advice=ExecutionAdvice.ROUTE_SPARK,
            cpu_workers=1,
            io_workers=1,
        ),
    )
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        _reflection(32 * 1024 * 1024), _cfg(0), parser=object(),
    )

    assert chosen is Engine.SPARK_SQL


@pytest.mark.parametrize(
    "advice",
    [
        ExecutionAdvice.ROUTE_DUCKDB,
        ExecutionAdvice.ROUTE_SPARK,
    ],
)
def test_auto_never_demotes_native_memory_warning_to_lite_without_spark(
    monkeypatch, advice,
):
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: SimpleNamespace(supported=True),
        resource_plan=lambda reflection, parser, streaming_result: SimpleNamespace(
            advice=advice,
            cpu_workers=1,
            io_workers=1,
        ),
    )
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        _reflection(32 * 1024 * 1024), _cfg(0), parser=object(),
    )

    assert chosen is Engine.DUCKDB_PRO


def test_auto_does_not_route_spark_when_job_fits_no_cluster_window():
    catalog = _Catalog([
        _active(1 * GIB, 2 * GIB),
        _active(5 * GIB, 8 * GIB),
    ])

    chosen = _executor(catalog)._auto_pick(_reflection(3 * GIB), _cfg(0))

    assert chosen is Engine.DUCKDB_PRO


def test_auto_capability_or_cache_probe_error_falls_back_to_pro(monkeypatch):
    executor = _executor(_Catalog([]))
    executor.island_exec = SimpleNamespace(
        can_execute=lambda reflection, parser: (_ for _ in ()).throw(
            RuntimeError("optional probe failed")
        ),
    )
    executor._get_file_cache = lambda: SimpleNamespace()
    enabled = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_AUTO_ENABLED=True,
    )
    monkeypatch.setattr(executor_module, "settings", enabled)

    chosen = executor._auto_pick(
        _reflection(512 * 1024 * 1024), _cfg(10 * GIB), parser=object(),
    )

    assert chosen is Engine.DUCKDB_PRO


# --------------------------------------------------------------------------- #
# select_spark_cluster — fit window + random pick
# --------------------------------------------------------------------------- #

class _SelCatalog(RedisCatalog):
    """RedisCatalog with a canned cluster list (no Redis connection)."""

    def __init__(self, clusters):
        self._clusters = clusters

    def list_spark_clusters(self, org):
        return self._clusters


def _c(name, min_bytes, max_bytes):
    return {
        "status": "active", "name": name, "cluster_id": name,
        "min_bytes": min_bytes, "max_bytes": max_bytes,
    }


def test_select_only_clusters_whose_window_fits():
    cat = _SelCatalog([
        _c("A", 0, 2 * GIB),       # accepts <= 2 GiB
        _c("B", 5 * GIB, 0),       # accepts >= 5 GiB
    ])
    # 1 GiB job -> only A fits, every time.
    for _ in range(50):
        assert cat.select_spark_cluster("org", 1 * GIB)["name"] == "A"


def test_select_random_among_fitting():
    cat = _SelCatalog([_c("A", 0, 0), _c("B", 0, 0)])  # both unbounded
    seen = {cat.select_spark_cluster("org", 1 * GIB)["name"] for _ in range(200)}
    assert seen == {"A", "B"}


def test_select_none_when_no_window_fits():
    cat = _SelCatalog([_c("A", 10 * GIB, 0)])
    assert cat.select_spark_cluster("org", 1 * GIB) is None


def test_select_force_ignores_size_window():
    cat = _SelCatalog([_c("A", 10 * GIB, 0)])
    # force=True skips the size filter — a tiny job still selects a cluster.
    assert cat.select_spark_cluster("org", 1, force=True)["name"] == "A"
