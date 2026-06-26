# supertable/engine/tests/test_engine_routing.py
"""Tests for fleet-driven Spark routing.

Under AUTO, the registered Spark Thrift fleet decides routing:

  * :meth:`Executor._auto_pick` routes to Spark only when at least one
    **active** cluster is registered AND the job reaches the fleet's minimum
    accepted size — the smallest ``min_bytes`` across active clusters
    (:meth:`Executor._spark_min_bytes`).  With no active cluster, AUTO stays on
    DuckDB regardless of size.
  * :meth:`RedisCatalog.select_spark_cluster` then picks, at random, one of the
    active clusters whose ``[min_bytes, max_bytes]`` window contains the job.

The ``engine_spark_min_bytes`` policy value is only a fallback used when no
active cluster is known (in which case AUTO won't pick Spark anyway).
"""

from __future__ import annotations

from types import SimpleNamespace

from supertable.engine.executor import Executor
from supertable.engine.engine_config import EngineRuntimeConfig
from supertable.engine.engine_enum import Engine
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
