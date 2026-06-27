"""Read-path pruning — real-wiring integration & monitoring observability.

The differential suite (``test_read_pruning_differential.py``) proves the pruning
*logic* in isolation (DuckDB over hand-built file lists). This module exercises
the **live estimator wiring**: ``DataEstimator.estimate()`` ->
``DataEstimator._prune_files`` -> ``processing.load_stats`` ->
``processing.prune_files_by_predicates``, over REAL local-disk parquet files and a
REAL stats artifact read back from disk, and asserts that:

  * the reflection's file list is actually narrowed when a WHERE prunes
    (real-wiring; the estimator is the component that owns ``_prune_files``);
  * parity holds — the same query with pruning disabled yields a superset file
    list and byte-identical result rows;
  * the read monitoring payload (``PlanStats.stats``, consumed by
    ``plan_extender.extend_execution_plan``) surfaces the pruning effect
    (``FILES_BEFORE_PRUNE`` / ``FILES_PRUNED`` plus the profiler's
    ``read_pruned_files`` / stats-cache counters), 0/absent for a no-prune query.

Scope note — why the estimator level, not a full ``DataReader.execute()`` round
trip:  a real ``DataWriter -> DataReader.execute()`` round trip needs the hermetic
environment (LOCAL storage + fakeredis) pinned *before* ``supertable`` is first
imported.  That bootstrap lives in the repo-root ``tests/conftest.py`` and only
covers ``tests/characterization`` — it does **not** apply to ``supertable/tests``,
which mock Redis per-test and run against a MinIO-typed settings singleton.
Re-pinning the storage backend mid-process is not reliable.  ``_prune_files`` —
the entire subject under test — runs inside the estimator, so we drive the real
estimator over real artifacts and mock only the Redis snapshot-discovery seam.
"""

from __future__ import annotations

import dataclasses
import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from supertable.config.settings import settings
from supertable.processing import (
    STATS_SCHEMA,
    _stats_rows_for_metadata,
    _STATS_CACHE,
)
import sys

from supertable.storage.local_storage import LocalStorage
from supertable.utils.sql_parser import SQLParser
from supertable.engine.data_estimator import DataEstimator
import supertable.super_table  # noqa: F401  (ensure module is in sys.modules)
import supertable.processing as processing

# ``supertable.engine`` is shadowed by the ``Engine`` enum at the package level,
# so ``import supertable.engine.data_estimator`` fails attribute resolution. The
# modules are already loaded by the from-imports above; fetch them by name.
data_estimator_mod = sys.modules["supertable.engine.data_estimator"]
super_table_mod = sys.modules["supertable.super_table"]

_ORG = "intorg"
_SUPER = "maindb"
_SIMPLE = "orders"


# ---------------------------------------------------------------------------
# Real dataset: orders range-partitioned by id into 3 files, 2 row groups each.
# ---------------------------------------------------------------------------

@pytest.fixture
def orders_on_disk(tmp_path):
    """Write 3 real parquet files + a real stats artifact; return wiring info."""
    def mk(lo, hi):
        ids = list(range(lo, hi + 1))
        return pl.DataFrame({"id": ids, "val": [f"v{i}" for i in ids]})

    files = []
    for idx, (lo, hi) in enumerate([(1, 100), (101, 200), (201, 300)]):
        path = str(tmp_path / f"orders_{idx}.parquet")
        pq.write_table(mk(lo, hi).to_arrow(), path,
                       write_statistics=True, row_group_size=50)
        files.append(path)

    # REAL stats artifact: genuine footer min/max via the production extractor,
    # persisted to disk and read back through load_stats during the test.
    rows = []
    for p in files:
        rows.extend(_stats_rows_for_metadata(p, pq.read_metadata(p)))
    stats_path = str(tmp_path / "orders_stats.parquet")
    pl.DataFrame(rows, schema=STATS_SCHEMA).write_parquet(stats_path)

    payload = {
        "snapshot_version": 7,
        "schema": [{"name": "id", "type": "BIGINT"},
                   {"name": "val", "type": "VARCHAR"}],
        "stats_file": stats_path,
        "resources": [
            {"file": p, "file_size": os.path.getsize(p)} for p in files
        ],
    }
    leaf_item = {
        "simple": _SIMPLE,
        "path": "redis://leaf/orders",
        "version": 7,
        "ts": 1_700_000_000_000,
        "payload": payload,
    }
    return {"files": files, "stats_path": stats_path, "leaf_item": leaf_item}


@contextmanager
def _wired(orders_on_disk, *, pruning: bool):
    """Run with the Redis seam mocked, a LOCAL storage for stats reads, and the
    URL-resolution settings pinned so reflection files stay as local paths
    (presign off, no endpoint -> _to_duckdb_path returns the raw key)."""
    local = LocalStorage()
    _STATS_CACHE.clear()
    saved_storage = processing._storage

    est_cat = MagicMock()
    est_cat.scan_leaf_items.return_value = [orders_on_disk["leaf_item"]]
    super_cat = MagicMock()
    super_cat.root_exists.return_value = True  # SuperTable() returns early

    # ``settings`` is a frozen dataclass; clone it with the read-path pruning gate
    # toggled and URL resolution pinned to local (presign off, no endpoint) so the
    # reflection's file keys resolve to the raw local paths DuckDB can read.
    test_settings = dataclasses.replace(
        settings,
        SUPERTABLE_READ_PRUNING_ENABLED=pruning,
        SUPERTABLE_DUCKDB_PRESIGNED=False,
        STORAGE_ENDPOINT_URL="",
    )

    with patch.object(data_estimator_mod, "RedisCatalog", return_value=est_cat), \
         patch.object(super_table_mod, "RedisCatalog", return_value=super_cat), \
         patch.object(data_estimator_mod, "settings", test_settings):
        processing._storage = local  # load_stats reads the on-disk stats parquet
        try:
            yield local
        finally:
            processing._storage = saved_storage
            _STATS_CACHE.clear()


def _estimate(query: str, orders_on_disk, *, pruning: bool):
    with _wired(orders_on_disk, pruning=pruning) as local:
        parser = SQLParser(_SUPER, query, "duckdb")
        tables = parser.get_physical_tables()
        constraints = parser.get_predicate_constraints()
        est = DataEstimator(
            organization=_ORG,
            storage=local,
            tables=tables,
            predicate_constraints=constraints,
        )
        reflection = est.estimate()
        return reflection, est.plan_stats


def _stat(plan_stats, key):
    for entry in plan_stats.stats:
        if isinstance(entry, dict) and key in entry:
            return entry[key]
    return None


def _files(reflection):
    return list(reflection.supers[0].files)


def _duckdb_rows(query: str, files: list[str]) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        flist = "[" + ", ".join("'" + f + "'" for f in files) + "]"
        con.execute(f'CREATE VIEW "{_SIMPLE}" AS SELECT * FROM read_parquet({flist})')
        df = con.execute(query).fetchdf()
    finally:
        con.close()
    if df.empty:
        return df.reset_index(drop=True)
    return df.sort_values(by=list(df.columns)).reset_index(drop=True)


_PRUNE_Q = f"SELECT * FROM {_SIMPLE} WHERE id BETWEEN 120 AND 180"
_NOPRUNE_Q = f"SELECT * FROM {_SIMPLE} WHERE id = 50 OR val = 'zzz'"


# ---------------------------------------------------------------------------
# Task 2 — real-wiring: file list narrowed + parity
# ---------------------------------------------------------------------------

class TestEstimatorRealWiringPruning:

    def test_reflection_file_list_is_narrowed(self, orders_on_disk):
        # id∈[120,180] lives only in the middle file -> 2 of 3 files dropped by
        # the real estimator path reading the real stats artifact from disk.
        reflection, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=True)
        assert reflection.total_reflections == 1
        assert len(_files(reflection)) == 1

    def test_pruning_disabled_keeps_all_files(self, orders_on_disk):
        reflection, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=False)
        assert reflection.total_reflections == 3
        assert len(_files(reflection)) == 3

    def test_pruned_is_subset_of_full(self, orders_on_disk):
        pruned, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=True)
        full, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=False)
        assert set(_files(pruned)).issubset(set(_files(full)))

    def test_result_parity_pruned_vs_full(self, orders_on_disk):
        # The reflection files resolve to local paths (presign off, no endpoint),
        # so DuckDB can read them directly: pruned result == full result.
        pruned, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=True)
        full, _ = _estimate(_PRUNE_Q, orders_on_disk, pruning=False)
        res_pruned = _duckdb_rows(_PRUNE_Q, _files(pruned))
        res_full = _duckdb_rows(_PRUNE_Q, _files(full))
        assert_frame_equal(res_pruned, res_full, check_dtype=False)
        assert len(res_pruned) == 61  # ids 120..180 inclusive

    def test_no_prune_query_keeps_all_files(self, orders_on_disk):
        # OR across columns bails predicate extraction -> no constraint -> no drop.
        reflection, _ = _estimate(_NOPRUNE_Q, orders_on_disk, pruning=True)
        assert reflection.total_reflections == 3


# ---------------------------------------------------------------------------
# Task 1 — monitoring observability surfaced via PlanStats
# ---------------------------------------------------------------------------

class TestReadPruningObservability:

    def test_pruned_count_in_payload(self, orders_on_disk):
        _, ps = _estimate(_PRUNE_Q, orders_on_disk, pruning=True)
        assert _stat(ps, "FILES_BEFORE_PRUNE") == 3
        assert _stat(ps, "FILES_PRUNED") == 2

    def test_profiler_counts_folded_into_payload(self, orders_on_disk):
        _, ps = _estimate(_PRUNE_Q, orders_on_disk, pruning=True)
        counts = _stat(ps, "PRUNE_COUNTS")
        assert counts is not None
        assert counts.get("read_pruned_files") == 2
        # Stats artifact was loaded from disk exactly once (cold cache).
        assert counts.get("stats_cache_miss") == 1

    def test_no_prune_query_reports_zero(self, orders_on_disk):
        _, ps = _estimate(_NOPRUNE_Q, orders_on_disk, pruning=True)
        assert _stat(ps, "FILES_PRUNED") == 0
        # The pruned-file counter is absent/zero when nothing was dropped (the
        # OR predicate yields no usable constraint). Any stats-IO counters that
        # did fire are irrelevant to the pruned count.
        counts = _stat(ps, "PRUNE_COUNTS") or {}
        assert not counts.get("read_pruned_files")

    def test_disabled_pruning_omits_pruning_stats(self, orders_on_disk):
        _, ps = _estimate(_PRUNE_Q, orders_on_disk, pruning=False)
        assert _stat(ps, "FILES_BEFORE_PRUNE") is None
        assert _stat(ps, "FILES_PRUNED") is None

    def test_reflection_stats_reach_injected_plan_stats(self, orders_on_disk):
        # The estimator records onto the PlanStats it was given (the same object
        # DataReader threads to extend_execution_plan), so REFLECTIONS surfaces.
        from supertable.engine.plan_stats import PlanStats
        with _wired(orders_on_disk, pruning=True) as local:
            parser = SQLParser(_SUPER, _PRUNE_Q, "duckdb")
            injected = PlanStats()
            est = DataEstimator(
                organization=_ORG,
                storage=local,
                tables=parser.get_physical_tables(),
                predicate_constraints=parser.get_predicate_constraints(),
                plan_stats=injected,
            )
            est.estimate()
            assert est.plan_stats is injected
            assert _stat(injected, "REFLECTIONS") == 1
            assert _stat(injected, "FILES_PRUNED") == 2
