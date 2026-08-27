from __future__ import annotations

import dataclasses
import gc
import json
import math
import os
import re
import shutil
import struct
import subprocess
import sys
import threading
import time
import types
from contextlib import contextmanager
from datetime import datetime

import duckdb
import pandas as pd
import polars as pl
import pytest
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import supertable.engine.islanddb as islanddb_module


def test_island_telemetry_rss_limit_signals_event(monkeypatch):
    values = iter((100, 102))
    monkeypatch.setattr(islanddb_module, "_process_rss_bytes", lambda: next(values, 102))
    event = threading.Event()
    telemetry = islanddb_module._IslandTelemetry(rss_limit_bytes=1, limit_event=event)
    try:
        assert event.wait(1.0)
    finally:
        telemetry.finish()

from supertable.data_classes import (
    IntegerDomainBound,
    RbacViewDef,
    Reflection,
    ResourceObjectSeal,
    RowGroupSelection,
    SuperSnapshot,
    TombstoneDef,
)
from supertable.engine.duckdb_engine import DuckDB
from supertable.engine.engine_common import SOURCE_FILE_COL
from supertable.engine.engine_config import IslandRuntimeConfig, resolve_engine_config
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor
from supertable.engine.islanddb import (
    IslandDB,
    IslandExecutionTimeout,
    IslandIntegrityError,
    IslandUnsupportedError,
    _fixed_size_binary_to_binary,
)
from supertable.engine.island_resources import (
    ContainerResources,
    ExecutionAdvice,
    QueryResourcePlan,
    ResourcePlanner,
    ResourcePolicy,
    ResourceReservationCancelled,
)
from supertable.processing import (
    TOMBSTONE_FILE_COL,
    parquet_footer_sha256,
    tombstone_digest,
)
from supertable.query_plan_manager import QueryPlanManager
from supertable.engine.plan_stats import PlanStats
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata, write_all
from supertable.utils.timer import Timer
from supertable.utils.sql_parser import SQLParser


@pytest.fixture(autouse=True)
def _catalog_without_engine_overrides(monkeypatch):
    """Public Executor tests use an explicit absent config document."""
    import supertable.redis_catalog as redis_catalog_module

    class Catalog:
        @staticmethod
        def get_engine_config(_organization):
            return None

        @staticmethod
        def list_spark_clusters(_organization):
            return []

    monkeypatch.setattr(redis_catalog_module, "RedisCatalog", Catalog)


def _snapshot(name, paths, keys, *, types=None):
    types = types or {
        "id": "Int64", "v": "Int64", "__rowid__": "Int64",
        "__timestamp__": "Int64",
    }
    candidate_rows = sum(
        pq.read_metadata(path).num_rows for path in paths
    )
    return SuperSnapshot(
        "s", name, 1, list(map(str, paths)), set(types),
        resource_keys=list(keys),
        resource_sizes=[os.path.getsize(path) for path in paths],
        column_types=types,
        snapshot_resource_keys=list(keys),
        candidate_rows=candidate_rows,
        candidate_rows_complete=True,
    )


def _reflection(*snapshots, projected=None):
    raw = sum(sum(s.resource_sizes) for s in snapshots)
    # Direct engine tests bypass DataEstimator, so provide the same two sealed
    # resource bounds production supplies.  Arrow ``nbytes`` is an exact
    # decoded-buffer measurement for these immutable local fixtures (and is
    # deliberately computed outside the timed engine execution).
    decoded = sum(
        pq.read_table(path).nbytes
        for snapshot in snapshots
        for path in snapshot.files
    )
    return Reflection(
        "LocalStorage", projected if projected is not None else raw,
        sum(len(s.files) for s in snapshots), list(snapshots),
        source_bytes=raw,
        row_group_scan_bytes=raw,
        row_group_scan_bytes_complete=True,
        decoded_bytes=decoded,
        decoded_bytes_complete=True,
    )


def _run_island(tmp_path, reflection, query):
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.temp_dir = str(tmp_path)
    manager.query_plan_path = str(tmp_path / "island-plan.json")
    engine = IslandDB()
    result = engine.execute(reflection, parser, manager, lambda _: None)
    return result, engine


def _run_duckdb(tmp_path, reflection, query):
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.temp_dir = str(tmp_path)
    manager.query_plan_path = str(tmp_path / "duck-plan.json")
    return DuckDB().execute(
        reflection, parser, manager, lambda _: None,
        engine_config=resolve_engine_config("", None, "lite"),
    )


@pytest.fixture(autouse=True)
def _isolate_local_scan_plan_cache():
    import supertable.engine.islanddb as island_module

    island_module._clear_local_scan_plan_cache()
    yield
    island_module._clear_local_scan_plan_cache()


def test_local_scan_plan_warm_hit_skips_footer_schema_validation(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    path = tmp_path / "warm.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot("t", [path], ["raw/warm.parquet"])
    engine = IslandDB(range_cache=False)

    calls = 0
    original = engine._base_relation_uncached

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_base_relation_uncached", counted)
    cold_metadata = {}
    cold = engine._base_relation(
        snapshot, object_metadata_out=cold_metadata,
    ).select("id").collect()
    cold_calls = calls
    warm_metadata = {}
    warm = engine._base_relation(
        snapshot, object_metadata_out=warm_metadata,
    ).select("id").collect()

    assert cold_calls == 1
    assert calls == cold_calls
    assert warm.equals(cold)
    assert warm_metadata == cold_metadata
    assert cold_metadata["raw/warm.parquet"].identity_token()
    assert len(island_module._LOCAL_SCAN_PLANS) == 1


def test_local_scan_plan_concurrent_miss_is_singleflight(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    import supertable.engine.islanddb as island_module

    path = tmp_path / "concurrent.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot("t", [path], ["raw/concurrent.parquet"])
    engine = IslandDB(range_cache=False)
    original = engine._base_relation_uncached
    entered = threading.Event()
    release = threading.Event()
    calls_lock = threading.Lock()
    start = threading.Barrier(3)
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
        entered.set()
        assert release.wait(timeout=5)
        return original(*args, **kwargs)

    def relation():
        start.wait(timeout=5)
        return engine._base_relation(snapshot)

    monkeypatch.setattr(engine, "_base_relation_uncached", counted)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(relation)
        second = pool.submit(relation)
        start.wait(timeout=5)
        assert entered.wait(timeout=5)
        assert len(island_module._LOCAL_SCAN_PLAN_BUILDS) == 1
        release.set()
        left = first.result(timeout=5)
        right = second.result(timeout=5)

    assert calls == 1
    assert left is not right
    assert left.select("id").collect().equals(right.select("id").collect())


def test_local_scan_plan_reuses_physical_validation_across_row_group_hints(
    tmp_path, monkeypatch,
):
    path = tmp_path / "query-hints.parquet"
    pl.DataFrame({
        "id": list(range(20)), "v": list(range(20)),
        "__rowid__": list(range(1, 21)), "__timestamp__": [1] * 20,
    }).write_parquet(path, row_group_size=10)
    snapshot = _snapshot("t", [path], ["raw/query-hints.parquet"])
    footer = parquet_footer_sha256(pq.read_metadata(path))
    first = dataclasses.replace(snapshot, row_group_selections={
        "raw/query-hints.parquet": RowGroupSelection(2, (0,), footer),
    })
    second = dataclasses.replace(snapshot, row_group_selections={
        "raw/query-hints.parquet": RowGroupSelection(2, (1,), footer),
    })
    engine = IslandDB(range_cache=False)
    original = engine._base_relation_uncached
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_base_relation_uncached", counted)
    low = (
        engine._base_relation(first)
        .filter(pl.col("id") < 10)
        .select(pl.col("id").sum())
        .collect()
        .item()
    )
    high = (
        engine._base_relation(second)
        .filter(pl.col("id") >= 10)
        .select(pl.col("id").sum())
        .collect()
        .item()
    )

    assert calls == 1
    assert low == sum(range(10))
    assert high == sum(range(10, 20))


def test_local_scan_plan_key_change_and_file_mutation_force_revalidation(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    path = tmp_path / "mutable-contract.parquet"

    def write(values):
        pl.DataFrame({
            "id": values, "v": values,
            "__rowid__": list(range(1, len(values) + 1)),
            "__timestamp__": [1] * len(values),
        }).write_parquet(path)

    write([1, 2])
    snapshot = _snapshot("t", [path], ["raw/v1.parquet"])
    engine = IslandDB(range_cache=False)
    calls = 0
    original = engine._base_relation_uncached

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_base_relation_uncached", counted)
    engine._base_relation(snapshot)

    changed_key = dataclasses.replace(
        snapshot, resource_keys=["raw/v2.parquet"],
        snapshot_resource_keys=["raw/v2.parquet"],
    )
    engine._base_relation(changed_key)
    assert calls == 2

    # Violate the immutable-file contract with a same-schema rewrite. The old
    # key remains identical, so the filesystem seal itself must reject the hit.
    write([7, 8])
    assert os.path.getsize(path) == snapshot.resource_sizes[0]
    actual = engine._base_relation(snapshot).select("id").collect()
    assert calls == 3
    assert actual.get_column("id").to_list() == [7, 8]


def test_local_scan_plan_cache_is_entry_and_byte_bounded(tmp_path, monkeypatch):
    import supertable.engine.islanddb as island_module

    monkeypatch.setattr(island_module, "_LOCAL_SCAN_PLAN_MAX_ENTRIES", 2)
    monkeypatch.setattr(island_module, "_LOCAL_SCAN_PLAN_MAX_BYTES", 1 << 30)
    engine = IslandDB(range_cache=False)
    for index in range(3):
        path = tmp_path / f"bounded-{index}.parquet"
        pl.DataFrame({
            "id": [index], "v": [index], "__rowid__": [index + 1],
            "__timestamp__": [1],
        }).write_parquet(path)
        engine._base_relation(
            _snapshot(f"t{index}", [path], [f"raw/{index}.parquet"]),
        )
    assert len(island_module._LOCAL_SCAN_PLANS) == 2

    island_module._clear_local_scan_plan_cache()
    monkeypatch.setattr(island_module, "_LOCAL_SCAN_PLAN_MAX_ENTRIES", 10)
    monkeypatch.setattr(island_module, "_LOCAL_SCAN_PLAN_MAX_BYTES", 1)
    path = tmp_path / "too-large.parquet"
    pl.DataFrame({
        "id": [1], "v": [1], "__rowid__": [1], "__timestamp__": [1],
    }).write_parquet(path)
    engine._base_relation(_snapshot("large", [path], ["raw/large.parquet"]))
    assert not island_module._LOCAL_SCAN_PLANS
    assert island_module._LOCAL_SCAN_PLAN_BYTES == 0


@pytest.mark.parametrize(
    ("scan_bytes", "complete", "columns", "expected"),
    [
        (9 * 1024 * 1024, True, {"a", "b", "c", "d", "e"}, "columns"),
        (9 * 1024 * 1024, False, {"a", "b", "c", "d", "e"}, "auto"),
        (33 * 1024 * 1024, True, {"a", "b", "c", "d", "e"}, "auto"),
        (9 * 1024 * 1024, True, {"a", "b", "c"}, "auto"),
        (9 * 1024 * 1024, True, set("abcdefghi"), "auto"),
        (9 * 1024 * 1024, True, None, "auto"),
    ],
)
def test_local_scan_parallel_strategy_is_narrowly_bounded(
    scan_bytes, complete, columns, expected,
):
    reflection = Reflection(
        "LocalStorage", scan_bytes, 1, [],
        row_group_scan_bytes=scan_bytes,
        row_group_scan_bytes_complete=complete,
    )

    assert IslandDB._local_scan_parallel_strategy(
        reflection, columns,
    ) == expected


def test_local_scan_parallel_strategy_keeps_auto_with_active_tombstone():
    reflection = Reflection(
        "LocalStorage", 9 * 1024 * 1024, 1, [],
        row_group_scan_bytes=9 * 1024 * 1024,
        row_group_scan_bytes_complete=True,
        tombstone_views={
            "t": TombstoneDef(
                "/sealed/dv.parquet", "raw/dv", 1, "0" * 64,
                ("raw/data.parquet",), ("raw/data.parquet",),
            ),
        },
    )

    assert IslandDB._local_scan_parallel_strategy(
        reflection, {"a", "b", "c", "d", "e"},
    ) == "auto"


def test_prepare_lazy_query_applies_column_parallel_hint_only_to_local_scan(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    path = tmp_path / "parallel-columns.parquet"
    pl.DataFrame({
        "id": [1, 2], "a": [10, 20], "b": [30, 40], "c": [50, 60],
        "d": [70, 80], "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot(
        "t", [path], ["raw/parallel-columns.parquet"],
        types={
            "id": "Int64", "a": "Int64", "b": "Int64", "c": "Int64",
            "d": "Int64", "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    reflection = _reflection(snapshot)
    query = "SELECT id, a, b, c, d FROM s.t WHERE id > 0"
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB(range_cache=False)
    observed = []
    original_scan = island_module.pl.scan_parquet

    def capture_scan(*args, **kwargs):
        observed.append(kwargs.get("parallel"))
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(island_module.pl, "scan_parquet", capture_scan)
    lazy, _, _, plan = engine._prepare_lazy_query(
        reflection, parser, lambda _: None, "",
    )

    assert lazy.collect().height == 2
    assert observed == ["columns"]
    assert "PROJECT 5/" in plan


def test_prepare_lazy_query_does_not_force_columns_with_tombstone(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    path = tmp_path / "parallel-dv-data.parquet"
    resource_key = "raw/parallel-dv-data.parquet"
    pl.DataFrame({
        "id": [1, 2], "a": [10, 20], "b": [30, 40], "c": [50, 60],
        "d": [70, 80], "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot(
        "t", [path], [resource_key],
        types={
            "id": "Int64", "a": "Int64", "b": "Int64", "c": "Int64",
            "d": "Int64", "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    reflection = _reflection(snapshot)
    dv_path = tmp_path / "parallel-dv.parquet"
    dv = pl.DataFrame(
        {TOMBSTONE_FILE_COL: [resource_key], "__rowid__": [1]},
        schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64},
    )
    dv.write_parquet(dv_path)
    reflection.tombstone_views["t"] = TombstoneDef(
        str(dv_path), "raw/parallel-dv.parquet", 1, tombstone_digest(dv),
        (resource_key,), (resource_key,),
    )
    parser = SQLParser(
        "s", "SELECT id, a, b, c, d FROM s.t WHERE id > 0", "duckdb",
    )
    engine = IslandDB(range_cache=False)
    observed = []
    original_scan = island_module.pl.scan_parquet

    def capture_scan(*args, **kwargs):
        observed.append(kwargs.get("parallel"))
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(island_module.pl, "scan_parquet", capture_scan)
    lazy, _, _, _ = engine._prepare_lazy_query(
        reflection, parser, lambda _: None, "",
    )
    result = lazy.collect()

    assert result.get_column("id").to_list() == [2]
    assert observed
    assert set(observed) == {"auto"}


def test_arrow_dataset_applies_validated_row_group_hint_and_exact_filter(tmp_path):
    path = tmp_path / "groups.parquet"
    pl.DataFrame({
        "id": list(range(40)),
        "v": list(range(40)),
        "__rowid__": list(range(1, 41)),
        "__timestamp__": [1] * 40,
    }).write_parquet(path, row_group_size=10)
    snapshot = _snapshot("t", [path], ["raw/groups.parquet"])
    snapshot.row_group_selections = {
        "raw/groups.parquet": RowGroupSelection(
            4, (2,), parquet_footer_sha256(pq.read_metadata(path)),
        ),
    }
    snapshot.candidate_row_groups = 1
    snapshot.candidate_row_groups_complete = True
    reflection = _reflection(snapshot)
    reflection.row_group_scan_bytes = os.path.getsize(path)
    reflection.row_group_scan_bytes_complete = True
    reflection.decoded_bytes = 4096
    reflection.decoded_bytes_complete = True
    query = "SELECT id, v FROM s.t WHERE id BETWEEN 23 AND 25 ORDER BY id"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, engine = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    # Local Polars deliberately scans the conservative physical superset and
    # performs its own footer-statistics pruning. Telemetry must describe that
    # executable plan, not relabel the one-group estimator hint as observed I/O.
    assert engine.last_profile.planned_row_groups == 4
    assert engine.last_profile.planned_row_groups_complete is True
    assert engine.last_profile.selected_row_groups == 4
    assert engine.last_profile.observed_row_groups is None
    assert engine.last_profile.observed_row_groups_measured is False
    assert engine.last_profile.estimated_candidate_row_groups == 1
    assert engine.last_profile.estimated_candidate_row_groups_complete is True


def test_native_debug_log_hashes_sql_instead_of_logging_literals(
    tmp_path, caplog,
):
    secret = "731234567890123456"
    path = tmp_path / "sql-log-confidentiality.parquet"
    pl.DataFrame({
        "id": [1],
        "v": [int(secret)],
        "__rowid__": [1],
        "__timestamp__": [1],
    }).write_parquet(path)
    snapshot = _snapshot(
        "t",
        [path],
        ["raw/sql-log-confidentiality.parquet"],
        types={
            "id": "Int64",
            "v": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    query = f"SELECT id FROM s.t WHERE v = {secret}"
    caplog.set_level("DEBUG", logger="supertable.config.defaults")

    actual, _engine = _run_island(tmp_path, _reflection(snapshot), query)

    assert actual["id"].tolist() == [1]
    assert secret not in caplog.text
    assert "[islanddb] executing sql_sha256=" in caplog.text


def test_stale_row_group_footer_count_scans_all_and_keeps_result(tmp_path):
    path = tmp_path / "groups.parquet"
    pl.DataFrame({
        "id": list(range(40)),
        "v": list(range(40)),
        "__rowid__": list(range(1, 41)),
        "__timestamp__": [1] * 40,
    }).write_parquet(path, row_group_size=10)
    snapshot = _snapshot("t", [path], ["raw/groups.parquet"])
    snapshot.row_group_selections = {
        # The actual footer has four groups. The executor must ignore this
        # stale hint rather than clamping ids or dropping group 3.
        "raw/groups.parquet": RowGroupSelection(
            3, (1,), parquet_footer_sha256(pq.read_metadata(path)),
        ),
    }
    reflection = _reflection(snapshot)
    query = "SELECT id FROM s.t WHERE id = 35"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)


def test_same_count_stale_footer_seal_scans_all(tmp_path):
    stale = tmp_path / "stale.parquet"
    live = tmp_path / "live.parquet"
    # Both files have two groups, but their ranges are deliberately reversed.
    pl.DataFrame({
        "id": list(range(20)), "v": list(range(20)),
        "__rowid__": list(range(1, 21)), "__timestamp__": [1] * 20,
    }).write_parquet(stale, row_group_size=10)
    pl.DataFrame({
        "id": list(range(10, 20)) + list(range(10)),
        "v": list(range(10, 20)) + list(range(10)),
        "__rowid__": list(range(1, 21)), "__timestamp__": [1] * 20,
    }).write_parquet(live, row_group_size=10)
    snapshot = _snapshot("t", [live], ["raw/live.parquet"])
    snapshot.row_group_selections = {
        # The stale footer says id=15 belongs to group 1. The live footer has
        # id=15 in group 0. Count-only validation would silently lose the row.
        "raw/live.parquet": RowGroupSelection(
            2, (1,), parquet_footer_sha256(pq.read_metadata(stale)),
        ),
    }
    reflection = _reflection(snapshot)
    reflection.row_group_scan_bytes = os.path.getsize(live)
    reflection.row_group_scan_bytes_complete = True
    reflection.decoded_bytes = 4096
    reflection.decoded_bytes_complete = True
    query = "SELECT id FROM s.t WHERE id = 15"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    # Resource planning must reject the stale subset too. Otherwise execution
    # scans all groups correctly but reserves memory/I/O for only one group.
    assert IslandDB()._footer_working_set(reflection)[2] == 2


def test_local_string_footer_working_set_uses_exact_utf8_seal(tmp_path):
    values = ["é", "𐍈𐍈", None]
    reflection, maximum = _string_reflection(tmp_path, values)
    snapshot = reflection.supers[0]

    compressed, decoded, groups = IslandDB()._footer_working_set(
        reflection,
        required_columns={id(snapshot): {"label"}},
    )

    assert compressed > 0
    assert groups == 1
    assert decoded == len(values) * (maximum + 16 + 1)

    snapshot.column_max_value_bytes = {}
    assert IslandDB()._footer_working_set(
        reflection,
        required_columns={id(snapshot): {"label"}},
    ) is None


def test_malformed_row_group_mapping_fails_open_to_all_groups(tmp_path):
    path = tmp_path / "malformed-hint.parquet"
    pl.DataFrame({
        "id": list(range(20)), "v": list(range(20)),
        "__rowid__": list(range(1, 21)), "__timestamp__": [1] * 20,
    }).write_parquet(path, row_group_size=10)
    snapshot = _snapshot("t", [path], ["raw/malformed.parquet"])

    class BrokenSelections:
        def get(self, _key):
            raise RuntimeError("corrupt optional hint mapping")

        def values(self):
            raise RuntimeError("corrupt optional hint mapping")

    snapshot.row_group_selections = BrokenSelections()
    reflection = _reflection(snapshot)
    reflection.row_group_scan_bytes = os.path.getsize(path)
    reflection.row_group_scan_bytes_complete = True
    reflection.decoded_bytes = 4096
    reflection.decoded_bytes_complete = True
    query = "SELECT id FROM s.t WHERE id = 15"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.fixture
def numeric_reflection(tmp_path):
    paths = []
    for index, rows in enumerate((range(0, 50), range(50, 100))):
        path = tmp_path / f"part-{index}.parquet"
        values = list(rows)
        pl.DataFrame({
            "id": values,
            "v": [value * 3 for value in values],
            "label": [f"r{value}" for value in values],
            "__rowid__": [value + 1 for value in values],
            "__timestamp__": [1] * len(values),
        }).write_parquet(path, row_group_size=10, compression="zstd")
        paths.append(path)
    snap = _snapshot(
        "t", paths, ["raw/part-0.parquet", "raw/part-1.parquet"],
        types={
            "id": "Int64", "v": "Int64", "label": "String",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    snap.column_max_value_bytes = {"label": 3}
    return _reflection(snap, projected=1024)


@pytest.mark.parametrize("query", [
    "SELECT id, v FROM s.t WHERE id BETWEEN 31 AND 36 ORDER BY id",
    "SELECT id, label FROM s.t WHERE id BETWEEN 31 AND 36 ORDER BY id",
    "SELECT count(*) AS n, sum(v) AS total, min(v) AS lo, max(v) AS hi "
    "FROM s.t WHERE id >= 40 AND id < 70",
    "SELECT count(*) AS n, sum(v) AS total, min(v) AS lo, max(v) AS hi "
    "FROM s.t WHERE id < 0",
    "SELECT id, v FROM s.t WHERE id IN (1, 9, 77) ORDER BY id",
])
def test_native_numeric_queries_match_duckdb(tmp_path, numeric_reflection, query):
    expected = _run_duckdb(tmp_path, numeric_reflection, query)
    actual, engine = _run_island(tmp_path, numeric_reflection, query)
    pd.testing.assert_frame_equal(actual, expected)
    assert engine.last_profile.native is True
    assert engine.last_profile.source_bytes == numeric_reflection.source_bytes
    assert engine.last_profile.cpu_time_ms >= 0
    assert engine.last_profile.logical_scan_bytes == numeric_reflection.row_group_scan_bytes
    assert engine.last_profile.logical_scan_bytes_complete is True
    assert engine.last_profile.physical_read_bytes >= 0
    assert engine.last_profile.physical_read_bytes_measured is True
    assert engine.last_profile.decoded_bytes == numeric_reflection.decoded_bytes
    assert engine.last_profile.decoded_bytes_complete is True
    assert engine.last_profile.result_rows == len(actual)
    assert engine.last_profile.result_bytes >= 0
    assert engine.last_profile.peak_memory_bytes >= 0
    assert engine.last_profile.peak_memory_scope == (
        "process_rss_peak_delta_after_admission_until_profile_finalize"
    )
    assert engine.last_profile.spill_bytes == 0
    assert engine.last_profile.spill_bytes_measured is False
    # Native multi-file execution is one Arrow Dataset scan.  Polars labels
    # that bridge PYTHON SCAN while still pushing projection/predicate into the
    # Arrow scanner (both are visible in the optimized plan).
    assert "SCAN" in engine.last_profile.optimized_plan


def test_profile_separates_plan_estimates_observations_and_facade_phases(
    tmp_path, numeric_reflection,
):
    for snapshot in numeric_reflection.supers:
        snapshot.candidate_row_groups = sum(
            pq.read_metadata(path).num_row_groups for path in snapshot.files
        )
        snapshot.candidate_row_groups_complete = True
    actual, engine = _run_island(
        tmp_path,
        numeric_reflection,
        "SELECT id, v FROM s.t WHERE id BETWEEN 31 AND 36 ORDER BY id",
    )
    profile = engine.last_profile

    assert len(actual) == 6
    assert profile.planned_files == 2
    assert profile.planned_files_complete is True
    assert profile.planned_row_groups == 10
    assert profile.planned_row_groups_complete is True
    assert profile.planned_rows == 100
    assert profile.planned_rows_complete is True
    assert profile.planned_units_scope == "scan_node_occurrences"
    assert profile.estimated_candidate_files == 2
    assert profile.estimated_candidate_files_complete is True
    assert profile.estimated_candidate_row_groups == 10
    assert profile.estimated_candidate_row_groups_complete is True
    assert profile.observed_files is None
    assert profile.observed_files_measured is False
    assert profile.observed_row_groups is None
    assert profile.observed_row_groups_measured is False
    assert profile.observed_rows_scanned is None
    assert profile.observed_rows_scanned_measured is False
    assert profile.estimated_candidate_rows == 100
    assert profile.estimated_candidate_rows_complete is True
    assert profile.rows_scanned == 0
    assert profile.rows_scanned_measured is False
    assert profile.execution_outcome == "completed"
    assert profile.result_complete is True
    assert profile.result_rows == 6
    assert profile.result_rows_scope == "arrow_output_rows"
    assert profile.result_batches >= 1
    assert profile.result_batches_scope == "arrow_output_record_batches"
    assert profile.result_bytes_scope == "arrow_output_batch_logical_nbytes"

    assert profile.rss_scope == (
        "process_rss_sampled_10ms_after_admission_until_profile_finalize"
    )
    assert profile.rss_measured is True
    assert profile.rss_baseline_bytes is not None
    assert profile.rss_peak_bytes is not None
    assert profile.rss_final_bytes is not None
    assert profile.rss_peak_bytes >= profile.rss_baseline_bytes
    assert profile.rss_peak_bytes >= profile.rss_final_bytes
    assert profile.rss_peak_delta_bytes == (
        profile.rss_peak_bytes - profile.rss_baseline_bytes
    )
    assert profile.rss_retained_delta_bytes == (
        profile.rss_final_bytes - profile.rss_baseline_bytes
    )
    assert profile.peak_memory_bytes == (
        profile.rss_peak_bytes - profile.rss_baseline_bytes
    )
    assert profile.peak_memory_scope == (
        "process_rss_peak_delta_after_admission_until_profile_finalize"
    )
    assert profile.physical_read_scope == (
        "linux_proc_self_io_block_read_delta_after_admission_"
        "until_profile_finalize"
    )
    assert profile.elapsed_scope == (
        "engine_after_admission_through_stream_close_"
        "excludes_facade_and_profile_persist"
    )

    required_phases = {
        "range_cache_setup_ms",
        "prepare_execution_inside_call_ms",
        "admission_wait_ms",
        "relation_prepare_and_eager_integrity_ms",
        "first_batch_acquire_ms",
        "producer_active_ms",
        "producer_cleanup_ms",
        "stream_lifetime_ms",
        "facade_collect_arrow_table_ms",
        "facade_arrow_to_polars_ms",
        "facade_dtype_normalize_ms",
        "facade_polars_to_pandas_ms",
        "facade_total_ms",
        "engine_elapsed_excluding_profile_persist_ms",
        "total_execution_and_facade_excluding_profile_persist_ms",
    }
    assert required_phases <= profile.phase_timings_ms.keys()
    assert profile.phase_timings_scope == "monotonic_wall_nested_non_additive"
    assert all(profile.phase_timings_ms[name] >= 0 for name in required_phases)
    assert profile.profile_persist_ms is not None
    assert profile.profile_persist_ms_measured is True
    assert profile.profile_persist_succeeded is True
    assert profile.phase_timings_ms["profile_persist_ms"] >= 0

    # A single atomic profile write cannot contain its own duration. The
    # artifact marks that measurement unavailable; only the post-commit
    # in-memory profile carries it.
    persisted = json.loads((tmp_path / "island-plan.json").read_text())
    assert persisted["profile_persist_ms"] is None
    assert persisted["profile_persist_ms_measured"] is False
    assert persisted["profile_persist_succeeded"] is None
    assert "profile_persist_ms" not in persisted["phase_timings_ms"]
    assert persisted["execution_outcome"] == "completed"
    assert persisted["result_complete"] is True
    assert required_phases <= persisted["phase_timings_ms"].keys()


def test_process_telemetry_marks_counter_reset_unmeasured(monkeypatch):
    import supertable.engine.islanddb as island_module

    telemetry = island_module._IslandTelemetry.__new__(
        island_module._IslandTelemetry
    )
    telemetry.cpu_started = time.process_time()
    telemetry.read_started = 1_000
    telemetry.rss_started = 200
    telemetry.rss_peak = 350
    telemetry._stop = types.SimpleNamespace(set=lambda: None)
    telemetry._thread = types.SimpleNamespace(join=lambda timeout: None)
    monkeypatch.setattr(island_module, "_proc_counter", lambda _name: 900)
    monkeypatch.setattr(island_module, "_process_rss_bytes", lambda: 275)

    measured = telemetry.finish()

    assert measured["physical_read_bytes"] == 0
    assert measured["physical_read_bytes_measured"] is False
    assert measured["rss_measured"] is True
    assert measured["rss_baseline_bytes"] == 200
    assert measured["rss_peak_bytes"] == 350
    assert measured["rss_final_bytes"] == 275
    assert measured["rss_peak_delta_bytes"] == 150
    assert measured["rss_retained_delta_bytes"] == 75


def test_stream_close_before_first_batch_is_profiled_as_partial(
    tmp_path, numeric_reflection,
):
    query = "SELECT id, v FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "early-close-plan.json")
    engine = IslandDB()

    stream = engine.execute_stream(
        numeric_reflection, parser, manager, lambda _: None,
    )
    stream.close()

    assert engine.last_profile.execution_outcome == "closed_early"
    assert engine.last_profile.result_complete is False
    assert engine.last_profile.result_rows == 0
    assert engine.last_profile.observed_rows_scanned is None
    assert engine.last_profile.observed_rows_scanned_measured is False
    persisted = json.loads((tmp_path / "early-close-plan.json").read_text())
    assert persisted["execution_outcome"] == "closed_early"
    assert persisted["result_complete"] is False


def test_stream_cancel_before_first_batch_is_not_a_successful_sample(
    tmp_path, numeric_reflection,
):
    query = "SELECT id, v FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "cancelled-plan.json")
    engine = IslandDB()

    stream = engine.execute_stream(
        numeric_reflection, parser, manager, lambda _: None,
    )
    stream.cancel()

    assert engine.last_profile.execution_outcome == "cancelled"
    assert engine.last_profile.result_complete is False
    assert engine.last_profile.result_rows == 0
    persisted = json.loads((tmp_path / "cancelled-plan.json").read_text())
    assert persisted["execution_outcome"] == "cancelled"
    assert persisted["result_complete"] is False


def test_stream_lifetime_includes_consumer_idle_but_producer_time_does_not(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "consumer-idle-plan.json")
    engine = IslandDB()
    schema = pa.schema([pa.field("id", pa.int64())])
    batches = [
        pa.record_batch([pa.array([1])], schema=schema),
        pa.record_batch([pa.array([2])], schema=schema),
    ]
    monkeypatch.setattr(
        engine, "_lazy_batches", lambda *_args, **_kwargs: (schema, iter(batches)),
    )

    stream = engine.execute_stream(
        numeric_reflection, parser, manager, lambda _: None,
    )
    assert next(stream).column(0).to_pylist() == [1]
    time.sleep(0.05)
    assert next(stream).column(0).to_pylist() == [2]
    with pytest.raises(StopIteration):
        next(stream)

    phases = engine.last_profile.phase_timings_ms
    assert engine.last_profile.result_complete is True
    assert phases["stream_lifetime_ms"] >= 45.0
    assert phases["producer_active_ms"] < phases["stream_lifetime_ms"] - 30.0


def test_stream_producer_failure_is_not_relabelled_as_early_close(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "failed-stream-plan.json")
    engine = IslandDB()

    def broken_batches(*_args, **_kwargs):
        def fail():
            raise RuntimeError("native producer failed")
            yield  # pragma: no cover - make this a generator

        return pa.schema([pa.field("id", pa.int64())]), fail()

    monkeypatch.setattr(engine, "_lazy_batches", broken_batches)
    stream = engine.execute_stream(
        numeric_reflection, parser, manager, lambda _: None,
    )

    with pytest.raises(RuntimeError, match="native producer failed"):
        next(stream)

    assert engine.last_profile.execution_outcome == "failed"
    assert engine.last_profile.result_complete is False
    persisted = json.loads((tmp_path / "failed-stream-plan.json").read_text())
    assert persisted["execution_outcome"] == "failed"
    assert persisted["result_complete"] is False


def test_materialized_producer_failure_is_not_relabelled_as_facade_failure(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "failed-materialized-plan.json")
    engine = IslandDB()

    def broken_batches(*_args, **_kwargs):
        def fail():
            raise RuntimeError("materialized producer failed")
            yield  # pragma: no cover - make this a generator

        return pa.schema([pa.field("id", pa.int64())]), fail()

    monkeypatch.setattr(engine, "_lazy_batches", broken_batches)
    with pytest.raises(RuntimeError, match="materialized producer failed"):
        engine.execute(numeric_reflection, parser, manager, lambda _: None)

    assert engine.last_profile.execution_outcome == "failed"
    assert engine.last_profile.result_complete is False
    persisted = json.loads(
        (tmp_path / "failed-materialized-plan.json").read_text()
    )
    assert persisted["execution_outcome"] == "failed"


def test_materialized_facade_failure_is_profiled_separately(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "failed-facade-plan.json")
    engine = IslandDB()
    monkeypatch.setattr(
        engine,
        "_to_duckdb_pandas",
        lambda _result: (_ for _ in ()).throw(RuntimeError("facade failed")),
    )

    with pytest.raises(RuntimeError, match="facade failed"):
        engine.execute(numeric_reflection, parser, manager, lambda _: None)

    assert engine.last_profile.execution_outcome == "facade_failed"
    assert engine.last_profile.result_complete is False
    persisted = json.loads((tmp_path / "failed-facade-plan.json").read_text())
    assert persisted["execution_outcome"] == "facade_failed"
    assert persisted["result_complete"] is False


@pytest.mark.parametrize(
    "cancel_stage",
    ["arrow", "polars", "pandas", "pre-return"],
)
def test_materialized_cancel_is_checked_after_every_conversion_boundary(
    tmp_path, numeric_reflection, monkeypatch, cancel_stage,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(
        tmp_path / f"cancel-{cancel_stage}-materialized-plan.json"
    )
    engine = IslandDB()
    cancel_event = threading.Event()

    if cancel_stage == "arrow":
        original_collect = islanddb_module.ArrowBatchStream.collect_table

        def cancel_after_collect(stream, *, max_bytes):
            table = original_collect(stream, max_bytes=max_bytes)
            cancel_event.set()
            return table

        monkeypatch.setattr(
            islanddb_module.ArrowBatchStream,
            "collect_table",
            cancel_after_collect,
        )
    elif cancel_stage == "polars":
        original_from_arrow = islanddb_module.pl.from_arrow

        def cancel_after_polars(table):
            frame = original_from_arrow(table)
            cancel_event.set()
            return frame

        monkeypatch.setattr(
            islanddb_module.pl, "from_arrow", cancel_after_polars,
        )
    elif cancel_stage == "pandas":
        original_to_pandas = engine._to_duckdb_pandas

        def cancel_after_pandas(frame):
            result = original_to_pandas(frame)
            cancel_event.set()
            return result

        monkeypatch.setattr(engine, "_to_duckdb_pandas", cancel_after_pandas)
    else:
        original_add_phase = islanddb_module._QueryExecutionMetrics.add_phase

        def cancel_after_pandas_telemetry(metrics, name, elapsed_ms):
            original_add_phase(metrics, name, elapsed_ms)
            if name == "facade_polars_to_pandas_ms":
                cancel_event.set()

        monkeypatch.setattr(
            islanddb_module._QueryExecutionMetrics,
            "add_phase",
            cancel_after_pandas_telemetry,
        )

    with pytest.raises(ResourceReservationCancelled, match="cancelled"):
        engine.execute(
            numeric_reflection,
            parser,
            manager,
            lambda _: None,
            cancel_event=cancel_event,
        )

    assert engine.last_profile.execution_outcome == "cancelled"
    assert engine.last_profile.result_complete is False


def test_materialized_deadline_is_rechecked_immediately_before_return(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "deadline-materialized-plan.json")
    engine = IslandDB()
    clock = [0.0]
    monkeypatch.setattr(islanddb_module, "_monotonic", lambda: clock[0])
    original_add_phase = islanddb_module._QueryExecutionMetrics.add_phase

    def expire_after_pandas_telemetry(metrics, name, elapsed_ms):
        original_add_phase(metrics, name, elapsed_ms)
        if name == "facade_polars_to_pandas_ms":
            clock[0] = 101.0

    monkeypatch.setattr(
        islanddb_module._QueryExecutionMetrics,
        "add_phase",
        expire_after_pandas_telemetry,
    )

    with pytest.raises(IslandExecutionTimeout, match="before returning"):
        engine.execute(
            numeric_reflection,
            parser,
            manager,
            lambda _: None,
            deadline_monotonic=100.0,
        )

    assert engine.last_profile.execution_outcome == "timed_out"
    assert engine.last_profile.result_complete is False


def test_profile_writer_failure_never_fails_materialized_query(
    tmp_path, numeric_reflection, monkeypatch, caplog,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "unwritable-plan.json")
    engine = IslandDB()

    secret = (
        "profile sink unavailable at /srv/private/PROFILE_PATH_TOKEN; "
        "https://storage.invalid/REMOTE_PATH_TOKEN?token=QUERY_TOKEN"
    )

    def fail_profile(*_args, **_kwargs):
        raise OSError(secret)

    monkeypatch.setattr(engine, "_write_profile", fail_profile)
    caplog.set_level("DEBUG", logger="supertable.config.defaults")
    result = engine.execute(
        numeric_reflection, parser, manager, lambda _: None,
    )

    assert result["n"].iloc[0] == 100
    assert engine.last_profile.execution_outcome == "completed"
    assert engine.last_profile.result_complete is True
    assert engine.last_profile.profile_persist_ms_measured is True
    assert engine.last_profile.profile_persist_succeeded is False
    assert manager._island_profile is engine.last_profile
    assert manager._island_profile_token == engine.last_profile.telemetry_query_id
    assert not (tmp_path / "unwritable-plan.json").exists()
    assert "PROFILE_PATH_TOKEN" not in caplog.text
    assert "REMOTE_PATH_TOKEN" not in caplog.text
    assert "QUERY_TOKEN" not in caplog.text
    assert "/srv/private" not in caplog.text
    assert "error_type=OSError" in caplog.text


def test_telemetry_failure_never_masks_pre_stream_error_or_leaks_slot(
    numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    engine = IslandDB()
    monkeypatch.setattr(
        engine,
        "_prepare_lazy_query",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("original engine error")
        ),
    )
    monkeypatch.setattr(
        islanddb_module._IslandTelemetry,
        "finish",
        lambda _self: (_ for _ in ()).throw(RuntimeError("telemetry failed")),
    )

    with pytest.raises(RuntimeError, match="original engine error"):
        engine.execute_stream(
            numeric_reflection, parser, manager, lambda _: None,
        )

    assert islanddb_module._ISLAND_EXECUTION_SLOT.acquire(blocking=False)
    islanddb_module._ISLAND_EXECUTION_SLOT.release()
    assert islanddb_module._ARROW_POOL_LOCK.acquire(blocking=False)
    islanddb_module._ARROW_POOL_LOCK.release()
    assert not any(
        thread.name == "islanddb-telemetry" and thread.is_alive()
        for thread in __import__("threading").enumerate()
    )


def test_telemetry_constructor_failure_is_fail_open_and_releases_resources(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "no-sampler-profile.json")
    engine = IslandDB()

    class BrokenTelemetry:
        def __init__(self):
            raise RuntimeError("sampler thread unavailable")

    monkeypatch.setattr(islanddb_module, "_IslandTelemetry", BrokenTelemetry)
    result = engine.execute(
        numeric_reflection, parser, manager, lambda _: None,
    )

    assert result["n"].iloc[0] == 100
    assert engine.last_profile.result_complete is True
    assert engine.last_profile.cpu_time_measured is False
    assert engine.last_profile.cpu_time_scope == "unavailable"
    assert engine.last_profile.rss_measured is False
    assert engine.last_profile.physical_read_bytes_measured is False
    assert islanddb_module._ISLAND_EXECUTION_SLOT.acquire(blocking=False)
    islanddb_module._ISLAND_EXECUTION_SLOT.release()
    assert islanddb_module._ARROW_POOL_LOCK.acquire(blocking=False)
    islanddb_module._ARROW_POOL_LOCK.release()


def test_profile_finalization_failure_cannot_reuse_previous_query_profile(
    tmp_path, numeric_reflection, monkeypatch,
):
    engine = IslandDB()
    first_query = "SELECT count(*) AS n FROM s.t"
    first_parser = SQLParser("s", first_query, "duckdb")
    first_manager = QueryPlanManager("s", "island-tests", "", first_query)
    first_manager.query_plan_path = str(tmp_path / "first-profile.json")
    first = engine.execute(
        numeric_reflection, first_parser, first_manager, lambda _: None,
    )
    first_profile = first_manager._island_profile
    assert first["n"].iloc[0] == 100
    assert first_profile.result_complete is True

    second_query = "SELECT count(*) AS n FROM s.t WHERE id >= 50"
    second_parser = SQLParser("s", second_query, "duckdb")
    second_manager = QueryPlanManager("s", "island-tests", "", second_query)
    second_manager.query_plan_path = str(tmp_path / "second-profile.json")
    monkeypatch.setattr(
        islanddb_module._QueryExecutionMetrics,
        "snapshot",
        lambda _self: (_ for _ in ()).throw(
            RuntimeError("profile construction failed")
        ),
    )
    second = engine.execute(
        numeric_reflection, second_parser, second_manager, lambda _: None,
    )

    assert second["n"].iloc[0] == 50
    assert second_manager._island_profile is None
    assert second_manager._island_profile_token != first_profile.telemetry_query_id
    assert engine.last_profile.telemetry_query_id == (
        second_manager._island_profile_token
    )
    assert engine.last_profile.execution_outcome == "telemetry_pending"


def test_profile_persistence_never_dereferences_shared_last_profile(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "query-local-profile.json")
    engine = IslandDB()
    original_write = engine._write_profile

    def clobber_shared_pointer(path, profile):
        engine.last_profile = islanddb_module.IslandProfile(
            telemetry_query_id="another-query",
            execution_outcome="telemetry_pending",
        )
        return original_write(path, profile)

    monkeypatch.setattr(engine, "_write_profile", clobber_shared_pointer)
    result = engine.execute(
        numeric_reflection, parser, manager, lambda _: None,
    )
    persisted = json.loads(
        (tmp_path / "query-local-profile.json").read_text()
    )

    assert result["n"].iloc[0] == 100
    assert persisted["telemetry_query_id"] == manager._island_profile_token
    assert persisted["execution_outcome"] == "completed"
    assert manager._island_profile.telemetry_query_id == (
        manager._island_profile_token
    )
    assert engine.last_profile is manager._island_profile


def test_projection_and_predicate_are_pushed_into_parquet(tmp_path, numeric_reflection):
    _, engine = _run_island(
        tmp_path, numeric_reflection,
        "SELECT v FROM s.t WHERE id >= 90 ORDER BY v",
    )
    plan = engine.last_profile.optimized_plan
    # Predicate + output are the only physical columns. The native local
    # scanner reports 2/5; the Arrow-fragment path includes its virtual source
    # partition column and reports 2/6.
    assert "PROJECT 2/5 COLUMNS" in plan or "PROJECT 2/6 COLUMNS" in plan
    assert "SELECTION:" in plan


def test_range_cache_initialization_failure_releases_all_admission_slots(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    query = "SELECT count(*) AS n FROM s.t WHERE id >= 10"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "cache-failure-plan.json")
    engine = IslandDB()
    before = engine._governor.snapshot()["active_queries"]
    numeric_reflection.supers[0].files[0] = "s3://benchmark/remote.parquet"
    monkeypatch.setattr(
        engine, "_get_range_cache",
        lambda *_args: (_ for _ in ()).throw(
            OSError("cache directory unavailable")
        ),
    )

    with pytest.raises(OSError, match="cache directory unavailable"):
        engine.execute_stream(
            numeric_reflection, parser, manager, lambda _: None,
        )

    assert engine._governor.snapshot()["active_queries"] == before
    assert island_module._ISLAND_EXECUTION_SLOT.acquire(blocking=False)
    island_module._ISLAND_EXECUTION_SLOT.release()
    assert island_module._ARROW_POOL_LOCK.acquire(blocking=False)
    island_module._ARROW_POOL_LOCK.release()


def test_island_constructor_never_waits_for_an_open_arrow_stream_gate():
    import supertable.engine.islanddb as island_module

    finished = threading.Event()
    failures = []

    def construct():
        try:
            IslandDB(range_cache=False)
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    assert island_module._ARROW_POOL_LOCK.acquire(timeout=1)
    worker = threading.Thread(target=construct, daemon=True)
    try:
        worker.start()
        assert finished.wait(timeout=2)
    finally:
        island_module._ARROW_POOL_LOCK.release()
        worker.join(timeout=2)

    assert not failures
    assert not worker.is_alive()


@pytest.mark.parametrize("stop_kind", ["deadline", "cancel"])
def test_idle_island_stream_self_finalizes_and_releases_resources(
    tmp_path,
    numeric_reflection,
    stop_kind,
):
    query = "SELECT id FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / f"idle-{stop_kind}.json")
    engine = IslandDB()
    cancelled = threading.Event()
    active_before = engine._governor.snapshot()["active_queries"]
    stream = engine.execute_stream(
        numeric_reflection,
        parser,
        manager,
        lambda _: None,
        deadline_monotonic=time.monotonic() + (
            10.0 if stop_kind == "cancel" else 0.5
        ),
        cancel_event=cancelled,
    )
    assert engine._governor.snapshot()["active_queries"] == active_before + 1
    if stop_kind == "cancel":
        cancelled.set()

    wait_until = time.monotonic() + 2.0
    while (
        (
            not stream.closed
            or engine._governor.snapshot()["active_queries"] != active_before
        )
        and time.monotonic() < wait_until
    ):
        time.sleep(0.01)

    assert stream.closed is True
    assert engine._governor.snapshot()["active_queries"] == active_before
    assert manager._island_profile.execution_outcome == (
        "cancelled" if stop_kind == "cancel" else "timed_out"
    )
    expected_error = (
        ResourceReservationCancelled
        if stop_kind == "cancel" else IslandExecutionTimeout
    )
    with pytest.raises(expected_error):
        next(stream)
    assert islanddb_module._ISLAND_EXECUTION_SLOT.acquire(blocking=False)
    islanddb_module._ISLAND_EXECUTION_SLOT.release()
    assert islanddb_module._ARROW_POOL_LOCK.acquire(blocking=False)
    islanddb_module._ARROW_POOL_LOCK.release()
    assert not any(
        thread.name == "supertable-island-result-lifecycle"
        and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_active_island_timeout_is_not_reclassified_as_cancellation():
    cancelled = threading.Event()
    timeout_started = threading.Event()
    timeout_callbacks = []
    terminal_callbacks = []
    schema = pa.schema([pa.field("id", pa.int64())])

    def producer():
        timeout_started.set()
        cancelled.set()
        # Give the lifecycle watcher a deterministic opportunity to observe
        # the shared cancel event while the active producer unwinds.
        time.sleep(0.1)
        raise IslandExecutionTimeout("active producer deadline")
        yield  # pragma: no cover - retain generator shape

    inner = islanddb_module.ArrowBatchStream(
        schema,
        producer(),
        cancel_event=cancelled,
    )
    stream = islanddb_module._IslandResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=1.0,
        cancel_event=cancelled,
        timeout_event=timeout_started,
        on_timeout=lambda: timeout_callbacks.append(True),
    )
    stream.add_terminal_callback(terminal_callbacks.append)
    assert stream.cancel_event is cancelled

    with pytest.raises(IslandExecutionTimeout, match="active producer"):
        next(stream)

    assert stream.terminal_kind == "timed_out"
    assert terminal_callbacks == ["timed_out"]
    assert timeout_callbacks == [True]


def test_active_island_cancellation_records_cancelled_terminal_once():
    terminal_callbacks = []
    schema = pa.schema([pa.field("id", pa.int64())])

    def producer():
        raise ResourceReservationCancelled("producer cancelled")
        yield  # pragma: no cover - retain generator shape

    inner = islanddb_module.ArrowBatchStream(schema, producer())
    stream = islanddb_module._IslandResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=None,
        cancel_event=None,
    )
    stream.add_terminal_callback(terminal_callbacks.append)

    with pytest.raises(ResourceReservationCancelled, match="producer cancelled"):
        next(stream)

    assert stream.terminal_kind == "cancelled"
    assert terminal_callbacks == ["cancelled"]


@pytest.mark.parametrize("stop_kind", ["deadline", "facade_cancel"])
def test_idle_executor_island_stream_releases_outer_cache_lease(
    tmp_path,
    numeric_reflection,
    monkeypatch,
    stop_kind,
):
    import supertable.engine.executor as executor_module

    class TrackingCache:
        source_is_local = True

        def __init__(self):
            self.active = 0

        @contextmanager
        def localized(self, reflection, **_kwargs):
            self.active += 1
            try:
                yield reflection, None
            finally:
                self.active -= 1

    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=False,
        ),
    )
    cache = TrackingCache()
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    monkeypatch.setattr(
        executor_module, "_reflection_has_remote_paths", lambda _reflection: True,
    )
    monkeypatch.setattr(executor, "_get_file_cache", lambda *_args: cache)
    query = "SELECT id FROM s.t ORDER BY id"
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "idle-executor-island.json")
    deadline = time.monotonic() + (
        10.0 if stop_kind == "facade_cancel" else 0.5
    )

    stream, used = executor.execute_stream(
        Engine.ISLANDDB,
        numeric_reflection,
        SQLParser("s", query, "duckdb"),
        manager,
        Timer(),
        PlanStats(),
        "",
        deadline_monotonic=deadline,
    )
    assert used == "islanddb"
    assert cache.active == 1
    if stop_kind == "facade_cancel":
        stream.cancel_event.set()

    wait_until = time.monotonic() + 2.0
    while cache.active and time.monotonic() < wait_until:
        time.sleep(0.01)

    assert cache.active == 0
    assert stream.closed is True
    assert manager._island_profile.execution_outcome == (
        "cancelled" if stop_kind == "facade_cancel" else "timed_out"
    )
    expected_error = (
        ResourceReservationCancelled
        if stop_kind == "facade_cancel" else IslandExecutionTimeout
    )
    with pytest.raises(expected_error):
        next(stream)


def test_integer_sum_widens_like_duckdb_hugeint(tmp_path):
    path = tmp_path / "wide-sum.parquet"
    maximum = 2**63 - 1
    pl.DataFrame({
        "id": [1, 2], "v": [maximum, maximum],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot("t", [path], ["raw/wide-sum"]))
    query = "SELECT count(*) AS n, sum(v) AS total FROM s.t"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert actual["total"].iloc[0] > 2**63


def test_integer_sum_arrow_stream_is_decimal128_and_exact(tmp_path):
    """The streaming contract must not inherit fetchdf's lossy float cast."""
    path = tmp_path / "exact-stream-sum.parquet"
    values = [2**53, 1]
    pl.DataFrame({
        "id": [1, 2], "v": values,
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot("t", [path], ["raw/exact-stream-sum"]))
    query = "SELECT sum(v) AS total FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.temp_dir = str(tmp_path)
    manager.query_plan_path = str(tmp_path / "exact-stream-plan.json")

    stream = IslandDB().execute_stream(
        reflection, parser, manager, lambda _: None,
    )
    table = stream.collect_table(max_bytes=1024 * 1024)

    assert table.schema.field("total").type == pa.decimal128(38, 0)
    assert int(table["total"][0].as_py()) == sum(values)


def test_implicit_aggregate_names_match_duckdb_in_arrow_and_pandas(
    tmp_path, numeric_reflection,
):
    query = (
        "SELECT count(*), count(v), sum(t.v), min(id), max(v) FROM s.t AS t"
    )
    capability = IslandDB().can_execute(
        numeric_reflection,
        SQLParser("s", query, "duckdb"),
        streaming_result=True,
    )
    assert capability.supported
    assert not capability.spark_supported

    def collect(engine, label, *, config=None):
        parser = SQLParser("s", query, "duckdb")
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(
            tmp_path / f"{label}-implicit-aggregate-plan.json"
        )
        stream = engine.execute_stream(
            numeric_reflection,
            parser,
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return pa.Table.from_batches(list(stream), schema=stream.schema)

    duck_table = collect(
        DuckDB(),
        "duckdb",
        config=resolve_engine_config("", None, "lite"),
    )
    island_table = collect(IslandDB(), "island")
    assert island_table.schema == duck_table.schema
    assert island_table.to_pylist() == duck_table.to_pylist()
    assert island_table.column_names == [
        "count_star()", "count(v)", "sum(t.v)", "min(id)", "max(v)",
    ]

    expected = _run_duckdb(tmp_path, numeric_reflection, query)
    actual, _ = _run_island(tmp_path, numeric_reflection, query)
    pd.testing.assert_frame_equal(actual, expected)


def _binary_reflection(tmp_path, chunks, *, arrow_types=None):
    paths = []
    keys = []
    maximum = 0
    arrow_types = arrow_types or [pa.binary()] * len(chunks)
    for index, (values, arrow_type) in enumerate(zip(chunks, arrow_types)):
        path = tmp_path / f"binary-{index}.parquet"
        value_array = pa.array(values, type=arrow_type)
        maximum = max(
            maximum,
            max((len(value) for value in values if value is not None), default=0),
        )
        pq.write_table(pa.table({
            "id": pa.array(list(range(len(values))), type=pa.int64()),
            "payload": value_array,
            "__rowid__": pa.array(
                list(range(index * 100 + 1, index * 100 + len(values) + 1)),
                type=pa.int64(),
            ),
            "__timestamp__": pa.array([1] * len(values), type=pa.int64()),
        }), path)
        paths.append(path)
        keys.append(f"raw/binary-{index}.parquet")
    snapshot = _snapshot(
        "t", paths, keys,
        types={
            "id": "Int64", "payload": "Binary",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    snapshot.column_max_value_bytes = {"payload": maximum}
    return _reflection(snapshot)


def _string_reflection(tmp_path, values):
    path = tmp_path / "strings.parquet"
    maximum = max(
        (len(value.encode("utf-8")) for value in values if value is not None),
        default=0,
    )
    pq.write_table(pa.table({
        "label": pa.array(values, type=pa.string()),
        "__rowid__": pa.array(
            list(range(1, len(values) + 1)), type=pa.int64(),
        ),
        "__timestamp__": pa.array([1] * len(values), type=pa.int64()),
    }), path)
    snapshot = _snapshot(
        "strings",
        [path],
        ["raw/strings.parquet"],
        types={
            "label": "String", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    snapshot.column_max_value_bytes = {"label": maximum}
    return _reflection(snapshot), maximum


def test_fixed_size_binary_public_normalization_is_zero_copy_and_slice_safe():
    parent = pa.array(
        [
            b"lead",
            None,
            b"\x80\xff\x00\x7f",
            b"\x00\x00\x00\x00",
            None,
            b"tail",
        ],
        type=pa.binary(4),
    )
    source = parent.slice(1, 4)
    expected = source.to_pylist()
    source_data = source.buffers()[1]
    expected_data_address = source_data.address + (
        source.offset * source.type.byte_width
    )
    offsets_cache = {}

    normalized = _fixed_size_binary_to_binary(
        source, offsets_cache=offsets_cache,
    )
    peer = _fixed_size_binary_to_binary(
        pa.array([b"1111", b"2222", b"3333", b"4444"], type=pa.binary(4)),
        offsets_cache=offsets_cache,
    )

    assert normalized.type == pa.binary()
    assert normalized.offset == 0
    assert normalized.to_pylist() == expected
    assert normalized.null_count == 2
    assert normalized.buffers()[2].address == expected_data_address
    assert normalized.buffers()[2].size == len(source) * 4
    assert normalized.buffers()[1].address == peer.buffers()[1].address
    assert normalized.buffers()[1].is_mutable is False
    normalized.validate(full=True)

    # The public array must own all buffers it references; neither the sliced
    # input nor its parent may be required to keep the values alive.
    del parent, source, source_data
    gc.collect()
    assert normalized.to_pylist() == expected


@pytest.mark.parametrize("start", range(1, 10))
def test_fixed_size_binary_public_normalization_rebases_every_bitmap_offset(
    start,
):
    values = [
        None if index % 3 == 0 else index.to_bytes(2, "big")
        for index in range(20)
    ]
    source = pa.array(values, type=pa.binary(2)).slice(start, 7)

    normalized = _fixed_size_binary_to_binary(source)

    assert normalized.to_pylist() == source.to_pylist()
    assert normalized.null_count == source.null_count
    normalized.validate(full=True)


@pytest.mark.parametrize(
    "source",
    [
        pa.array([], type=pa.binary(7)),
        pa.array([b"aa", b"bb"], type=pa.binary(2)).slice(1, 0),
        pa.array([b"", None, b""], type=pa.binary(0)).slice(1, 2),
    ],
)
def test_fixed_size_binary_public_normalization_handles_empty_values_and_rows(
    source,
):
    normalized = _fixed_size_binary_to_binary(source)

    assert normalized.type == pa.binary()
    assert normalized.to_pylist() == source.to_pylist()
    assert normalized.null_count == source.null_count
    normalized.validate(full=True)


def test_fixed_size_binary_public_schema_and_values_match_duckdb(tmp_path):
    source = pa.array(
        [b"\x80\xff\x00\x7f", None, b"\x00\x00\x00\x00"],
        type=pa.binary(4),
    )
    path = tmp_path / "fixed-binary-arrow-parity.parquet"
    pq.write_table(pa.table({"payload": source}), path)
    with duckdb.connect() as connection:
        duck_table = connection.execute(
            "SELECT payload FROM read_parquet(?)", [str(path)],
        ).to_arrow_table()
    island_table = pa.Table.from_arrays(
        [_fixed_size_binary_to_binary(source)], names=["payload"],
    )

    assert island_table.schema == duck_table.schema
    assert island_table.to_pylist() == duck_table.to_pylist()


def test_binary_scalar_extrema_match_duckdb_unsigned_lexicographic_multi_file(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [
        [None, b"", b"\x00", b"\x00\xff", b"\x7f"],
        [b"\x7f\xff", b"\x80", b"\xff", b"\xff\x00", None],
    ])
    query = (
        "SELECT min(t.payload) AS lo, max(t.payload) AS hi FROM s.t AS t"
    )

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert isinstance(actual.loc[0, "lo"], bytearray)
    assert isinstance(actual.loc[0, "hi"], bytearray)
    assert bytes(actual.loc[0, "lo"]) == b""
    assert bytes(actual.loc[0, "hi"]) == b"\xff\x00"


@pytest.mark.parametrize("chunks", [[[None, None], [None]], [[], []]])
def test_binary_scalar_extrema_match_duckdb_for_null_and_empty_inputs(
    tmp_path, chunks,
):
    reflection = _binary_reflection(tmp_path, chunks)
    query = "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert actual.loc[0, "lo"] is pd.NA
    assert actual.loc[0, "hi"] is pd.NA


def test_binary_scalar_extrema_arrow_stream_retains_canonical_binary(tmp_path):
    reflection = _binary_reflection(tmp_path, [[b"\x80", b"", b"\xff"]])
    query = "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "binary-stream-plan.json")

    table = IslandDB().execute_stream(
        reflection, parser, manager, lambda _: None,
    ).collect_table(max_bytes=1024 * 1024)

    assert table.schema.field("lo").type == pa.binary()
    assert table.schema.field("hi").type == pa.binary()
    assert table["lo"][0].as_py() == b""
    assert table["hi"][0].as_py() == b"\xff"


def test_empty_lazy_batches_preserve_schema_at_supported_polars_floor():
    lazy = pl.DataFrame(
        schema={"id": pl.Int64, "payload": pl.Binary}
    ).lazy()

    schema, batches = IslandDB._lazy_batches(lazy, batch_rows=2)

    assert schema.names == ["id", "payload"]
    assert schema.field("payload").type == pa.binary()
    assert list(batches) == []


@pytest.mark.parametrize("projection", ["*", "t.*"])
def test_arrow_stream_star_accepts_binary_when_materialized_facade_does_not(
    tmp_path, projection,
):
    reflection = _binary_reflection(tmp_path, [[b"a", None, b"\xff"]])
    query = f"SELECT {projection} FROM s.t AS t"
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB()

    stream_capability = engine.can_execute(
        reflection, parser, streaming_result=True,
    )
    materialized_capability = engine.can_execute(
        reflection, parser, streaming_result=False,
    )

    assert stream_capability.supported
    assert stream_capability.spark_supported is False
    assert not materialized_capability.supported
    assert any(
        "pandas parity" in reason
        for reason in materialized_capability.reasons
    )


@pytest.mark.parametrize("values", [["alpha", None, "İ"], []])
def test_string_lazy_batches_use_duckdb_arrow_schema(values):
    frame = pl.DataFrame(
        {"label": values},
        schema={"label": pl.String},
    )

    schema, batches = IslandDB._lazy_batches(
        frame.lazy(), batch_rows=2,
    )
    island_table = pa.Table.from_batches(list(batches), schema=schema)
    with duckdb.connect() as connection:
        connection.register("strings", frame.to_arrow())
        duck_table = connection.execute(
            "SELECT label FROM strings"
        ).to_arrow_table()

    assert schema == duck_table.schema
    assert schema.field("label").type == pa.string()
    assert island_table.to_pylist() == duck_table.to_pylist()


def test_string_group_by_uses_duckdb_nocase_equivalence_classes(
    tmp_path,
):
    values = [
        "A", "a", "İ", "i", "I", "ı", "É", "é", "ß", "ẞ",
        "Σ", "σ", "ς", None,
    ]
    reflection, _ = _string_reflection(tmp_path, values)
    query = (
        "SELECT label, count(*) AS n FROM s.strings "
        "GROUP BY label ORDER BY label"
    )
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB()
    capability = engine.can_execute(
        reflection, parser, streaming_result=True,
    )
    assert capability.supported
    assert not capability.spark_supported

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(
        reflection, parser, streaming_result=True,
    )
    assert estimate.estimates_complete is True
    assert estimate.spillable is False

    def collect(active_engine, label, *, config=None):
        active_parser = SQLParser("s", query, "duckdb")
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(tmp_path / f"{label}-nocase-plan.json")
        stream = active_engine.execute_stream(
            reflection,
            active_parser,
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return pa.Table.from_batches(list(stream), schema=stream.schema)

    expected = collect(
        DuckDB(),
        "duckdb",
        config=resolve_engine_config("", None, "lite"),
    )
    actual = collect(IslandDB(), "island")

    def canonical(rows):
        result = []
        for row in rows:
            label = pc.utf8_lower(
                pa.array([row["label"]], type=pa.string()),
            )[0].as_py()
            result.append({"label": label, "n": row["n"]})
        return result

    assert actual.schema == expected.schema
    assert canonical(actual.to_pylist()) == canonical(expected.to_pylist())
    assert canonical(actual.to_pylist()) == [
        {"label": "a", "n": 2},
        {"label": "i", "n": 3},
        {"label": "ß", "n": 2},
        {"label": "é", "n": 2},
        {"label": "ı", "n": 1},
        {"label": "ς", "n": 1},
        {"label": "σ", "n": 2},
        {"label": None, "n": 1},
    ]


def test_nocase_group_plan_budgets_expanding_hidden_string_key(tmp_path):
    # U+023A is two UTF-8 bytes while its lowercase U+2C65 is three. Model a
    # high-cardinality snapshot so the non-spillable normalized group/sort
    # state, rather than this tiny physical fixture, drives admission.
    value = "Ⱥ" * 256
    reflection, source_bound = _string_reflection(tmp_path, [value])
    selected_rows = 10_000
    reflection.supers[0].candidate_rows = selected_rows
    query = (
        "SELECT label, count(*) AS n FROM s.strings "
        "GROUP BY label ORDER BY label"
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(
        reflection,
        SQLParser("s", query, "duckdb"),
        streaming_result=True,
    )

    actual_lower_bytes = len(value.lower().encode("utf-8"))
    source_storage = max(24, source_bound + 17)
    lower_storage = max(
        24,
        source_bound
        * islanddb_module._UTF8_LOWER_ADMISSION_EXPANSION
        + 17,
    )
    group_state_per_key = (
        512 + 128 + 128 + source_storage + lower_storage
    )
    result_per_row = source_storage + 24
    expected_state = selected_rows * (
        group_state_per_key + result_per_row + lower_storage
    )

    assert actual_lower_bytes > source_bound
    assert estimate.estimates_complete is True
    assert estimate.spillable is False
    assert estimate.operator_state_bytes == expected_state
    assert estimate.result_bytes == selected_rows * result_per_row


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, x FROM s.t ORDER BY x, id",
        "SELECT x AS x, count(*) AS n FROM s.t GROUP BY x ORDER BY x",
    ],
)
def test_float_arrow_output_canonicalizes_negative_zero_like_duckdb(
    tmp_path, query,
):
    path = tmp_path / "signed-zero.parquet"
    pq.write_table(
        pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "x": pa.array([-0.0, 0.0, 1.0], type=pa.float64()),
            "__rowid__": pa.array([1, 2, 3], type=pa.int64()),
            "__timestamp__": pa.array([1, 1, 1], type=pa.int64()),
        }),
        path,
        row_group_size=1,
    )
    reflection = _reflection(_snapshot(
        "t",
        [path],
        ["raw/signed-zero.parquet"],
        types={
            "id": "Int64", "x": "Float64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))

    def collect(engine, label, *, config=None):
        parser = SQLParser("s", query, "duckdb")
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(tmp_path / f"{label}-zero-plan.json")
        stream = engine.execute_stream(
            reflection,
            parser,
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return pa.Table.from_batches(list(stream), schema=stream.schema)

    expected = collect(
        DuckDB(),
        "duckdb",
        config=resolve_engine_config("", None, "lite"),
    )
    actual = collect(IslandDB(), "island")

    capability = IslandDB().can_execute(
        reflection,
        SQLParser("s", query, "duckdb"),
        streaming_result=True,
    )

    assert actual.schema == expected.schema
    assert actual.to_pylist() == expected.to_pylist()
    assert capability.supported is True
    assert capability.spark_supported is False
    island_zeros = [
        value.as_py() for value in actual["x"]
        if value.as_py() == 0.0
    ]
    duckdb_zeros = [
        value.as_py() for value in expected["x"]
        if value.as_py() == 0.0
    ]
    assert island_zeros
    assert duckdb_zeros
    assert all(math.copysign(1.0, value) == 1.0 for value in island_zeros)
    assert all(math.copysign(1.0, value) == 1.0 for value in duckdb_zeros)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, x FROM s.t ORDER BY id",
        "SELECT id, x FROM s.t ORDER BY x, id",
    ],
)
def test_float_arrow_output_matches_duckdb_nan_payload_contract(
    tmp_path, query,
):
    def float_from_bits(bits: int) -> float:
        return struct.unpack("<d", struct.pack("<Q", bits))[0]

    path = tmp_path / "nan-payloads.parquet"
    pq.write_table(pa.table({
        "id": pa.array([1, 2, 3, 4], type=pa.int64()),
        "x": pa.array([
            float_from_bits(0x7FF8000000000001),
            float_from_bits(0xFFF8000000000002),
            -0.0,
            1.0,
        ], type=pa.float64()),
        "__rowid__": pa.array([1, 2, 3, 4], type=pa.int64()),
        "__timestamp__": pa.array([1, 1, 1, 1], type=pa.int64()),
    }), path, row_group_size=1)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/nan-payloads.parquet"],
        types={
            "id": "Int64", "x": "Float64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    def collect(engine, label, *, config=None):
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(tmp_path / f"{label}-nan-plan.json")
        stream = engine.execute_stream(
            reflection,
            SQLParser("s", query, "duckdb"),
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return stream.collect_table(max_bytes=1024 * 1024)

    expected = collect(
        DuckDB(), "duckdb", config=resolve_engine_config("", None, "lite"),
    )
    actual = collect(IslandDB(), "island")

    expected_values = expected["x"].combine_chunks().buffers()[1].to_pybytes()
    actual_values = actual["x"].combine_chunks().buffers()[1].to_pybytes()
    assert actual.schema == expected.schema
    assert actual_values == expected_values


def test_float_sum_with_string_group_matches_duckdb_arrow_contract(
    tmp_path, monkeypatch,
):
    path = tmp_path / "billed.parquet"
    systems = ["A", "a", "İ", "i", "I", None, None]
    pq.write_table(pa.table({
        "system": pa.array(systems, type=pa.string()),
        "billed_total": pa.array(
            [1.25, 2.5, 1e16, 1.0, -1e16, None, None],
            type=pa.float64(),
        ),
        "tax": pa.array(
            [0.5, 1.25, 2.0, 3.0, 4.0, None, None],
            type=pa.float32(),
        ),
        "__rowid__": pa.array(list(range(1, 8)), type=pa.int64()),
        "__timestamp__": pa.array([1] * 7, type=pa.int64()),
    }), path, row_group_size=2)
    snapshot = _snapshot(
        "billing",
        [path],
        ["raw/billed.parquet"],
        types={
            "system": "String", "billed_total": "Float64",
            "tax": "Float32", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    snapshot.column_max_value_bytes = {
        "system": max(len(value.encode("utf-8")) for value in systems if value),
    }
    reflection = _reflection(snapshot)
    query = (
        "SELECT system, sum(billed_total), sum(tax) AS tax_sum "
        "FROM s.billing GROUP BY system ORDER BY system"
    )
    capability = IslandDB().can_execute(
        reflection,
        SQLParser("s", query, "duckdb"),
        streaming_result=True,
    )
    assert capability.supported
    assert not capability.spark_supported

    def collect(active_engine, label, *, config=None):
        parser = SQLParser("s", query, "duckdb")
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(tmp_path / f"{label}-billing-plan.json")
        stream = active_engine.execute_stream(
            reflection,
            parser,
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return pa.Table.from_batches(list(stream), schema=stream.schema)

    expected = collect(
        DuckDB(),
        "duckdb",
        config=resolve_engine_config("", None, "lite"),
    )
    actual = collect(IslandDB(), "island")

    assert actual.schema == expected.schema
    assert actual.schema.field("sum(billed_total)").type == pa.float64()
    assert actual.schema.field("tax_sum").type == pa.float64()
    def canonical(rows):
        result = []
        for row in rows:
            system = pc.utf8_lower(
                pa.array([row["system"]], type=pa.string()),
            )[0].as_py()
            result.append({**row, "system": system})
        return result

    # DuckDB's parallel NOCASE hash aggregation may return either spelling
    # from one collation-equivalent group (for example I or İ). Compare the
    # defined SQL value: equivalence class plus aggregate payload.
    assert canonical(actual.to_pylist()) == canonical(expected.to_pylist())

    import supertable.engine.executor as executor_module

    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_AUTO_ENABLED=True,
        ),
    )
    reflection.reflection_bytes = 512 * 1024**2
    reflection.row_group_scan_bytes = 512 * 1024**2
    stats = PlanStats()
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "auto-billing-plan.json")
    stream, used = Executor(
        storage=LocalStorage(), organization="island-tests",
    ).execute_stream(
        Engine.AUTO,
        reflection,
        SQLParser("s", query, "duckdb"),
        manager,
        Timer(),
        stats,
        "",
    )
    with stream:
        auto_result = pa.Table.from_batches(list(stream), schema=stream.schema)

    assert used == "islanddb"
    assert auto_result.schema == expected.schema
    assert canonical(auto_result.to_pylist()) == canonical(expected.to_pylist())
    assert any(
        item.get("ENGINE_CAPABILITY", {}).get("supported") is True
        for item in stats.stats
    )
    assert any("ISLAND_RESOURCES" in item for item in stats.stats)
    assert any(
        item.get("AUTO_ROUTING_OUTCOME", {}).get("actual_engine")
        == "islanddb"
        for item in stats.stats
    )


@pytest.mark.parametrize("grouped", [False, True])
def test_float32_sum_widens_before_reduction(tmp_path, grouped):
    path = tmp_path / f"float32-cancellation-{grouped}.parquet"
    values = [1e8] + [1.0] * 10_000 + [-1e8]
    pq.write_table(pa.table({
        "bucket": pa.array([1] * len(values), type=pa.int32()),
        "value": pa.array(values, type=pa.float32()),
        "__rowid__": pa.array(
            list(range(1, len(values) + 1)), type=pa.int64(),
        ),
        "__timestamp__": pa.array([1] * len(values), type=pa.int64()),
    }), path)
    reflection = _reflection(_snapshot(
        "floats", [path], [f"raw/{path.name}"],
        types={
            "bucket": "Int32", "value": "Float32",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    ))
    query = (
        "SELECT bucket, sum(value) AS total FROM s.floats "
        "GROUP BY bucket ORDER BY bucket"
        if grouped else
        "SELECT sum(value) AS total FROM s.floats"
    )

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert actual["total"].iloc[0] == 10_000.0


def test_float_sum_null_and_nonfinite_semantics_match_duckdb(tmp_path):
    path = tmp_path / "float-sum-edge.parquet"
    rows = 3_000
    pattern = [1e16, 1.0, -1e16] * (rows // 3)
    pq.write_table(pa.table({
        "unstable": pa.array(pattern, type=pa.float64()),
        "all_null": pa.array([None] * rows, type=pa.float64()),
        "nan_total": pa.array(
            [float("nan"), 1.0] + [None] * (rows - 2),
            type=pa.float64(),
        ),
        "infinite": pa.array(
            [float("inf"), 1.0] + [None] * (rows - 2),
            type=pa.float64(),
        ),
        "__rowid__": pa.array(list(range(1, rows + 1)), type=pa.int64()),
        "__timestamp__": pa.array([1] * rows, type=pa.int64()),
    }), path, row_group_size=17)
    snapshot = _snapshot(
        "floats",
        [path],
        ["raw/float-sum-edge.parquet"],
        types={
            "unstable": "Float64", "all_null": "Float64",
            "nan_total": "Float64", "infinite": "Float64",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    reflection = _reflection(snapshot)
    query = (
        "SELECT sum(unstable) AS unstable, sum(all_null) AS all_null, "
        "sum(nan_total) AS nan_total, sum(infinite) AS infinite "
        "FROM s.floats"
    )

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    # Parallel floating SUM is order-dependent in both engines. The two values
    # need not be bit-identical, but both must remain inside the standard
    # forward-error bound for an IEEE-754 reduction of the same inputs.
    exact = math.fsum(pattern)
    forward_error = rows * math.ulp(1.0) * math.fsum(
        abs(value) for value in pattern
    )
    assert abs(actual["unstable"].iloc[0] - exact) <= forward_error
    assert abs(expected["unstable"].iloc[0] - exact) <= forward_error
    assert pd.isna(actual["all_null"].iloc[0])
    assert pd.isna(expected["all_null"].iloc[0])
    assert math.isnan(actual["nan_total"].iloc[0])
    assert math.isnan(expected["nan_total"].iloc[0])
    assert actual["infinite"].iloc[0] == float("inf")
    assert expected["infinite"].iloc[0] == float("inf")


def test_binary_scalar_extrema_empty_arrow_stream_retains_canonical_binary(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [[]])
    query = "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "empty-binary-stream-plan.json")

    table = IslandDB().execute_stream(
        reflection, parser, manager, lambda _: None,
    ).collect_table(max_bytes=1024 * 1024)

    assert table.schema.field("lo").type == pa.binary()
    assert table.schema.field("hi").type == pa.binary()
    assert table.num_rows == 1
    assert table["lo"][0].as_py() is None
    assert table["hi"][0].as_py() is None


def test_datetime_us_scalar_extrema_match_duckdb_null_and_precision(tmp_path):
    path = tmp_path / "datetime-extrema.parquet"
    values = [
        None,
        datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime(2026, 8, 13, 20, 1, 2, 345678),
    ]
    pq.write_table(pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "event_ts": pa.array(values, type=pa.timestamp("us")),
        "__rowid__": pa.array([1, 2, 3], type=pa.int64()),
        "__timestamp__": pa.array([1, 1, 1], type=pa.int64()),
    }), path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/datetime-extrema.parquet"],
        types={
            "id": "Int64",
            "event_ts": "Datetime(time_unit='us', time_zone=None)",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    ))
    query = "SELECT min(event_ts) AS lo, max(event_ts) AS hi FROM s.t"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize("query", [
    "SELECT payload FROM s.t",
    "SELECT count(*) AS n FROM s.t WHERE payload=payload",
    "SELECT count(*) AS n FROM s.t ORDER BY payload",
    "SELECT payload, count(*) AS n FROM s.t GROUP BY payload",
    "SELECT max(payload) AS hi FROM s.t GROUP BY id",
])
def test_binary_uses_outside_scalar_direct_extrema_remain_rejected(
    tmp_path, query,
):
    reflection = _binary_reflection(tmp_path, [[b"a", b"b"]])

    capability = IslandDB().can_execute(
        reflection, SQLParser("s", query, "duckdb"),
    )

    assert capability.supported is False


def test_binary_and_datetime_raw_projection_is_arrow_stream_only(tmp_path):
    path = tmp_path / "rich-stream.parquet"
    pq.write_table(pa.table({
        "id": pa.array([2, 1], type=pa.int64()),
        "event_ts": pa.array(
            [datetime(2026, 1, 2), datetime(2026, 1, 1)],
            type=pa.timestamp("us"),
        ),
        "payload": pa.array([b"bbbb", b"aaaa"], type=pa.binary(4)),
        "__rowid__": pa.array([2, 1], type=pa.int64()),
        "__timestamp__": pa.array([1, 1], type=pa.int64()),
    }), path)
    snapshot = _snapshot(
        "t", [path], ["raw/rich-stream.parquet"],
        types={
            "id": "Int64",
            "event_ts": "Datetime(time_unit='us', time_zone=None)",
            "payload": "Binary",
            "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    snapshot.column_max_value_bytes = {"payload": 4}
    reflection = _reflection(snapshot)
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    query = "SELECT id, event_ts, payload FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB(range_cache=False)

    materialized = engine.can_execute(reflection, parser)
    streaming = engine.can_execute(
        reflection, parser, streaming_result=True,
    )

    assert materialized.supported is False
    assert any("pandas parity" in reason for reason in materialized.reasons)
    assert streaming.supported is True, streaming.reasons
    assert streaming.spark_supported is False

    def collect(active_engine, label, *, config=None):
        manager = QueryPlanManager("s", "island-tests", "", query)
        manager.query_plan_path = str(tmp_path / f"{label}-rich-stream.json")
        stream = active_engine.execute_stream(
            reflection,
            SQLParser("s", query, "duckdb"),
            manager,
            lambda _: None,
            engine_config=config,
        )
        with stream:
            return pa.Table.from_batches(list(stream), schema=stream.schema)

    expected = collect(
        DuckDB(),
        "duckdb",
        config=resolve_engine_config("", None, "lite"),
    )
    actual = collect(engine, "island")
    assert actual.schema == expected.schema
    assert actual.to_pylist() == expected.to_pylist()

    binary_order = engine.can_execute(
        reflection,
        SQLParser(
            "s", "SELECT id, payload FROM s.t ORDER BY payload, id", "duckdb",
        ),
        streaming_result=True,
    )
    assert binary_order.supported is False
    assert any("unproven DuckDB semantics" in reason for reason in binary_order.reasons)


@pytest.mark.parametrize("bound", [None, -1, True, "64"])
def test_binary_extrema_missing_or_malformed_value_bound_routes_away(
    tmp_path, bound,
):
    reflection = _binary_reflection(tmp_path, [[b"a", b"longer"]])
    reflection.supers[0].column_max_value_bytes = (
        {} if bound is None else {"payload": bound}
    )
    parser = SQLParser(
        "s", "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t", "duckdb",
    )
    engine = IslandDB()

    assert engine.can_execute(reflection, parser).supported is True
    plan = engine.resource_plan(reflection, parser, streaming_result=False)
    assert plan.advice is ExecutionAdvice.ROUTE_DUCKDB
    assert "incomplete" in plan.reason


def test_binary_extrema_rejects_mixed_arrow_physical_types(tmp_path):
    reflection = _binary_reflection(
        tmp_path,
        [[b"a"], [b"b"]],
        arrow_types=[pa.binary(), pa.large_binary()],
    )
    query = "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t"

    with pytest.raises(IslandUnsupportedError, match="physical Arrow.*payload"):
        _run_island(tmp_path, reflection, query)


def test_numeric_inner_join_matches_duckdb(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    pl.DataFrame({
        "id": [1, 2, 3], "v": [10, 20, 30],
        "__rowid__": [1, 2, 3], "__timestamp__": [1, 1, 1],
    }).write_parquet(a)
    pl.DataFrame({
        "id": [2, 3, 4], "v": [200, 300, 400],
        "__rowid__": [4, 5, 6], "__timestamp__": [1, 1, 1],
    }).write_parquet(b)
    reflection = _reflection(
        _snapshot("a", [a], ["raw/a"]),
        _snapshot("b", [b], ["raw/b"]),
    )
    query = (
        "SELECT a.id, a.v AS av, b.v AS bv FROM s.a a "
        "JOIN s.b b ON a.id=b.id WHERE a.id>=2 ORDER BY a.id"
    )
    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)
    pd.testing.assert_frame_equal(actual, expected)


def test_group_by_physical_column_shadowing_select_alias_matches_duckdb(tmp_path):
    path = tmp_path / "group-alias-shadow.parquet"
    pl.DataFrame({
        "x": [1, 1, 2],
        "y": [10, 10, 10],
        "__rowid__": [1, 2, 3],
        "__timestamp__": [1, 1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/group-alias-shadow"],
        types={
            "x": "Int64", "y": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    query = (
        "SELECT y, count(*) AS x FROM s.t "
        "GROUP BY y, x ORDER BY x"
    )

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)


def test_nocase_group_order_uses_aggregate_alias_that_shadows_group_key(tmp_path):
    path = tmp_path / "group-string-alias-shadow.parquet"
    pl.DataFrame({
        "x": ["a", "A", "b"],
        "y": [10, 10, 10],
        "__rowid__": [1, 2, 3],
        "__timestamp__": [1, 1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/group-string-alias-shadow"],
        types={
            "x": "String", "y": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    reflection.supers[0].column_max_value_bytes = {"x": 1}
    query = (
        "SELECT y, count(*) AS x FROM s.t "
        "GROUP BY y, x ORDER BY x"
    )

    capability = IslandDB().can_execute(reflection, SQLParser(
        "s", query, "duckdb",
    ))
    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    assert capability.supported is True
    pd.testing.assert_frame_equal(actual, expected)


def test_order_alias_shadowing_physical_column_uses_projected_type_in_gate(tmp_path):
    path = tmp_path / "order-alias-shadow.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "label": ["a", "B"],
        "__rowid__": [1, 2],
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/order-alias-shadow"],
        types={
            "id": "Int64", "label": "String", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    query = "SELECT label AS id FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")

    capability = IslandDB().can_execute(reflection, parser)

    assert capability.supported is False
    assert "column id type String has unproven DuckDB semantics" in capability.reasons
    with pytest.raises(IslandUnsupportedError, match="column id type String"):
        _run_island(tmp_path, reflection, query)


def test_schema_evolution_union_by_name_matches_duckdb(tmp_path):
    first = tmp_path / "evolved-1.parquet"
    second = tmp_path / "evolved-2.parquet"
    pl.DataFrame({
        "id": pl.Series([1, 2], dtype=pl.Int64),
        "v": pl.Series([10, 20], dtype=pl.Int64),
        "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
        "__timestamp__": pl.Series([1, 1], dtype=pl.Int64),
    }).write_parquet(first)
    pl.DataFrame({
        "id": pl.Series([3], dtype=pl.Int64),
        "v": pl.Series([30], dtype=pl.Int64),
        "extra": [300],
        "__rowid__": pl.Series([3], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
    }).write_parquet(second)
    reflection = _reflection(_snapshot(
        "t", [first, second], ["raw/evolved-1", "raw/evolved-2"],
        types={
            "id": "Int64", "v": "Int64", "extra": "Int64",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    ))
    query = "SELECT id, v, extra FROM s.t WHERE id >= 1 ORDER BY id"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, _ = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)


def test_mixed_physical_column_types_are_rejected(tmp_path):
    signed = tmp_path / "signed.parquet"
    unsigned = tmp_path / "unsigned.parquet"
    pl.DataFrame({
        "id": pl.Series([1], dtype=pl.Int64),
        "v": pl.Series([2**63 - 1], dtype=pl.Int64),
        "__rowid__": pl.Series([1], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
    }).write_parquet(signed)
    pl.DataFrame({
        "id": pl.Series([2], dtype=pl.Int64),
        "v": pl.Series([2**63], dtype=pl.UInt64),
        "__rowid__": pl.Series([2], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
    }).write_parquet(unsigned)
    reflection = _reflection(_snapshot(
        "t", [signed, unsigned], ["raw/signed", "raw/unsigned"],
        types={
            "id": "Int64", "v": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))

    with pytest.raises(IslandUnsupportedError, match="physical schema"):
        _run_island(
            tmp_path, reflection,
            "SELECT v, count(*) AS n FROM s.t GROUP BY v ORDER BY v",
        )


def test_case_variant_physical_columns_are_rejected(tmp_path):
    upper = tmp_path / "upper.parquet"
    lower = tmp_path / "lower.parquet"
    pl.DataFrame({
        "ID": pl.Series([1], dtype=pl.Int64),
        "__rowid__": pl.Series([1], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
    }).write_parquet(upper)
    pl.DataFrame({
        "id": pl.Series([2], dtype=pl.Int64),
        "__rowid__": pl.Series([2], dtype=pl.Int64),
        "__timestamp__": pl.Series([1], dtype=pl.Int64),
    }).write_parquet(lower)
    reflection = _reflection(_snapshot(
        "t", [upper, lower], ["raw/upper", "raw/lower"],
        types={
            "ID": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))

    with pytest.raises(IslandUnsupportedError, match="physical schema"):
        _run_island(tmp_path, reflection, "SELECT ID FROM s.t")


def test_case_insensitive_query_spelling_is_not_native(numeric_reflection):
    parser = SQLParser("s", "SELECT ID FROM s.t", "duckdb")

    capability = IslandDB().can_execute(numeric_reflection, parser)

    assert not capability.supported
    assert any("does not exactly match" in reason for reason in capability.reasons)


@pytest.mark.parametrize("declared, referenced", [("Foo", "foo"), ("t", "T")])
def test_case_variant_table_qualifier_is_not_native(
    numeric_reflection, declared, referenced,
):
    query = (
        f"SELECT {referenced}.id FROM s.t AS {declared} "
        f"ORDER BY {referenced}.id"
    )
    capability = IslandDB().can_execute(
        numeric_reflection, SQLParser("s", query, "duckdb"),
    )

    assert not capability.supported
    assert "does not exactly match its declared alias" in "; ".join(
        capability.reasons
    )


def test_stale_pinned_numeric_type_is_rejected(tmp_path):
    path = tmp_path / "stale-schema.parquet"
    pl.DataFrame({
        "id": pl.Series([2**53, 2**53 + 1, 2**53 + 2], dtype=pl.Int64),
        "__rowid__": pl.Series([1, 2, 3], dtype=pl.Int64),
        "__timestamp__": pl.Series([1, 1, 1], dtype=pl.Int64),
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/stale-schema"],
        types={
            "id": "Float64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))

    with pytest.raises(IslandUnsupportedError, match="pinned/physical schema"):
        _run_island(
            tmp_path, reflection,
            "SELECT id FROM s.t WHERE id = 9007199254740992.0",
        )


def test_composite_tombstone_matches_duckdb(tmp_path, numeric_reflection):
    snap = numeric_reflection.supers[0]
    dv_path = tmp_path / "dv.parquet"
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [snap.resource_keys[1]],
        "__rowid__": [78],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    dv.write_parquet(dv_path)
    numeric_reflection.tombstone_views["t"] = TombstoneDef(
        tombstone_path=str(dv_path), cache_key="raw/dv", expected_rows=1,
        tombstone_digest=tombstone_digest(dv),
        resource_keys=tuple(snap.resource_keys),
        snapshot_resource_keys=tuple(snap.resource_keys),
    )
    query = "SELECT id, v FROM s.t WHERE id BETWEEN 76 AND 78 ORDER BY id"
    expected = _run_duckdb(tmp_path, numeric_reflection, query)
    actual, _ = _run_island(tmp_path, numeric_reflection, query)
    pd.testing.assert_frame_equal(actual, expected)
    assert actual["id"].tolist() == [76, 78]


def test_pruned_snapshot_ignores_tombstones_for_unselected_resources(
    tmp_path, numeric_reflection,
):
    original = numeric_reflection.supers[0]
    selected = dataclasses.replace(
        original,
        files=[original.files[1]],
        resource_keys=[original.resource_keys[1]],
        resource_sizes=[original.resource_sizes[1]],
        snapshot_resource_keys=list(original.resource_keys),
    )
    numeric_reflection.supers = [selected]
    dv_path = tmp_path / "pruned-dv.parquet"
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [original.resource_keys[0]],
        "__rowid__": [1],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    dv.write_parquet(dv_path)
    numeric_reflection.tombstone_views["t"] = TombstoneDef(
        tombstone_path=str(dv_path), cache_key="raw/pruned-dv",
        expected_rows=1, tombstone_digest=tombstone_digest(dv),
        resource_keys=tuple(selected.resource_keys),
        snapshot_resource_keys=tuple(original.resource_keys),
    )

    actual, engine = _run_island(
        tmp_path, numeric_reflection,
        "SELECT count(*) AS n FROM s.t",
    )

    assert actual["n"].tolist() == [
        pq.ParquetFile(original.files[1]).metadata.num_rows
    ]
    assert "ANTI JOIN" not in engine.last_profile.optimized_plan


def test_tombstone_cache_reuses_digest_sealed_frame_without_reread(
    tmp_path, numeric_reflection, monkeypatch,
):
    snap = numeric_reflection.supers[0]
    dv_path = tmp_path / "cached-dv.parquet"
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [snap.resource_keys[0]],
        "__rowid__": [2],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    dv.write_parquet(dv_path)
    tombstone = TombstoneDef(
        tombstone_path=str(dv_path), cache_key="raw/cached-dv", expected_rows=1,
        tombstone_digest=tombstone_digest(dv),
        resource_keys=tuple(snap.resource_keys),
        snapshot_resource_keys=tuple(snap.resource_keys),
    )
    engine = IslandDB(organization=f"org-{tmp_path.name}")
    first = engine._load_tombstone(tombstone)

    monkeypatch.setattr(
        pl, "read_parquet",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("sealed tombstone cache hit reread storage")
        ),
    )
    second = engine._load_tombstone(tombstone)

    assert second is first


def test_tombstone_cache_identity_includes_digest_and_never_serves_old_content(
    tmp_path,
):
    dv_path = tmp_path / "rotated-dv.parquet"
    first = pl.DataFrame(
        {TOMBSTONE_FILE_COL: ["raw/f"], "__rowid__": [1]},
        schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64},
    )
    first.write_parquet(dv_path)
    engine = IslandDB(organization=f"org-{tmp_path.name}")
    old = TombstoneDef(
        str(dv_path), "raw/reused-key", 1, tombstone_digest(first),
        ("raw/f",), ("raw/f",),
    )
    assert engine._load_tombstone(old).get_column("__rowid__").to_list() == [1]

    second = pl.DataFrame(
        {TOMBSTONE_FILE_COL: ["raw/f"], "__rowid__": [2]},
        schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64},
    )
    second.write_parquet(dv_path)
    current = TombstoneDef(
        str(dv_path), "raw/reused-key", 1, tombstone_digest(second),
        ("raw/f",), ("raw/f",),
    )
    assert engine._load_tombstone(current).get_column("__rowid__").to_list() == [2]


def test_resource_plan_counts_tombstone_state_per_self_join_alias(tmp_path):
    path = tmp_path / "tombstone-plan.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    resource_key = "raw/path-with-a-long-resource-name.parquet"
    reflection = _reflection(_snapshot("t", [path], [resource_key]))
    tombstone = TombstoneDef(
        tombstone_path=str(tmp_path / "not-read-by-planner.parquet"),
        cache_key="raw/shared-dv.parquet",
        expected_rows=10,
        tombstone_digest="0" * 64,
        resource_keys=(resource_key,),
        # Exercise the legacy fallback used by the executor's validator too.
        snapshot_resource_keys=None,
    )
    reflection.tombstone_views = {"a": tombstone, "b": tombstone}
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    parser = SQLParser(
        "s",
        "SELECT a.id AS aid, b.v AS bv "
        "FROM s.t a JOIN s.t b ON a.id=b.id",
        "duckdb",
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=True)

    per_alias = 10 * (len(resource_key.encode("utf-8")) + 8 + 8 + 1 + 128)
    assert estimate.operator_state_bytes == reflection.decoded_bytes * 4 + 2 * per_alias
    assert estimate.decoded_scan_bytes == reflection.decoded_bytes + 2 * per_alias
    assert estimate.selected_decoded_bytes == reflection.decoded_bytes
    assert estimate.selected_decoded_bytes_complete is True


def test_scalar_aggregate_plan_charges_every_output_and_reduction(tmp_path):
    path = tmp_path / "wide-scalar-plan.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot("t", [path], ["raw/wide-scalar-plan"]))
    parser = SQLParser(
        "s",
        "SELECT count(*) AS n, sum(v) AS total, min(id) AS lo, max(v) AS hi "
        "FROM s.t",
        "duckdb",
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=False)

    assert estimate.operator_state_bytes == 4 * 4096
    assert estimate.result_bytes == 4 * 4096
    assert estimate.estimated_result_rows == 1


def test_scalar_aggregate_redundant_order_does_not_request_spill(tmp_path):
    path = tmp_path / "ordered-scalar-plan.parquet"
    pl.DataFrame({
        "id": [1, 2], "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/ordered-scalar-plan"],
        types={
            "id": "Int64", "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    ))
    reflection.decoded_bytes = 420 * 1024**2
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    resources = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=(0, 1, 2, 3),
        cpuset_cpus=(0, 1, 2, 3),
        memory_limit_bytes=1024**3,
        memory_available_bytes=1024**3,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )
    parser = SQLParser(
        "s", "SELECT count(*) AS n FROM s.t ORDER BY n", "duckdb",
    )

    plan = engine.resource_plan(reflection, parser, streaming_result=True)

    assert plan.advice is ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.estimated_spill_bytes == 0


def test_sealed_low_cardinality_group_uses_compact_bounded_operator_plan(tmp_path):
    path = tmp_path / "bounded-group-plan.parquet"
    pl.DataFrame({
        "dimension": [0, 1], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot(
        "t", [path], ["raw/bounded-group-plan"],
        types={
            "dimension": "Int64", "v": "Int64",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    )
    # The physical fixture is tiny, but the domain models the 10-GiB benchmark
    # snapshot whose complete footer stats prove only 1,024 possible keys.
    snapshot.candidate_rows = 6_413_677
    snapshot.integer_domain_bounds = {
        "dimension": IntegerDomainBound(0, 1023),
    }
    reflection = _reflection(snapshot)
    reflection.decoded_bytes = 10 * 1024**3
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    parser = SQLParser(
        "s",
        "SELECT dimension, count(*) AS n FROM s.t "
        "GROUP BY dimension ORDER BY dimension",
        "duckdb",
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=False)

    grouped_state = 1024 * (512 + 128 + 128)
    grouped_result = 1024 * 2 * 24
    assert estimate.requires_bounded_group_operator is True
    assert estimate.operator_state_bytes == grouped_state + grouped_result
    assert estimate.result_bytes == grouped_result
    assert estimate.estimated_result_rows == 1024
    assert estimate.operator_state_bytes < reflection.decoded_bytes // 1000


def test_missing_or_multikey_group_domain_keeps_conservative_state(tmp_path):
    path = tmp_path / "unknown-group-plan.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot("t", [path], ["raw/unknown-group-plan"])
    snapshot.integer_domain_bounds = {"id": IntegerDomainBound(1, 2)}
    reflection = _reflection(snapshot)
    parser = SQLParser(
        "s",
        "SELECT id, v, count(*) AS n FROM s.t GROUP BY id, v ORDER BY id",
        "duckdb",
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=True)

    assert estimate.requires_bounded_group_operator is False
    worst_groups = snapshot.candidate_rows
    group_state = worst_groups * (512 + 2 * 128 + 128)
    result_state = worst_groups * 3 * 24
    assert estimate.operator_state_bytes == group_state + result_state
    assert estimate.estimated_result_rows == snapshot.candidate_rows


def test_sealed_high_cardinality_id_group_remains_external(tmp_path):
    path = tmp_path / "high-card-group-plan.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot("t", [path], ["raw/high-card-group-plan"])
    selected_rows = 6_413_677
    snapshot.candidate_rows = selected_rows
    snapshot.integer_domain_bounds = {
        "id": IntegerDomainBound(0, selected_rows - 1),
    }
    reflection = _reflection(snapshot)
    reflection.decoded_bytes = 128 * 1024**2
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    parser = SQLParser(
        "s",
        "SELECT id, count(*) AS n FROM s.t GROUP BY id ORDER BY id",
        "duckdb",
    )
    resources = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=(0, 1, 2, 3),
        cpuset_cpus=(0, 1, 2, 3),
        memory_limit_bytes=4 * 1024**3,
        memory_available_bytes=4 * 1024**3,
    )
    engine = IslandDB()
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=128 * 1024**3),
    )

    plan = engine.resource_plan(reflection, parser, streaming_result=True)

    assert plan.advice is ExecutionAdvice.ISLAND_SPILL
    assert plan.estimated_spill_bytes > reflection.decoded_bytes
    assert plan.spill_budget_bytes == plan.estimated_spill_bytes


@pytest.mark.parametrize(
    "query",
    [
        "SELECT v FROM s.t ORDER BY v",
        "SELECT v, count(*) AS n FROM s.t GROUP BY v",
    ],
)
def test_float_blocking_key_routes_away_when_it_requires_spill(
    tmp_path, query,
):
    path = tmp_path / "float-blocking-plan.parquet"
    pl.DataFrame({
        "v": [1.0, 2.0],
        "__rowid__": [1, 2],
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/float-blocking-plan"],
        types={
            "v": "Float64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    reflection.decoded_bytes = 420 * 1024**2
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    resources = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=(0, 1, 2, 3),
        cpuset_cpus=(0, 1, 2, 3),
        memory_limit_bytes=1024**3,
        memory_available_bytes=1024**3,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )

    plan = engine.resource_plan(
        reflection, SQLParser("s", query, "duckdb"), streaming_result=True,
    )

    assert plan.advice is ExecutionAdvice.ROUTE_DUCKDB
    assert "no bounded spill implementation" in plan.reason


def test_binary_scalar_plan_charges_exact_value_per_reduction_and_worker(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [[b"x" * 257, b"small"]])
    parser = SQLParser(
        "s", "SELECT min(payload) AS lo, max(payload) AS hi FROM s.t", "duckdb",
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._resources = dataclasses.replace(engine._resources, cpu_count=3)
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=False)

    per_extremum = 257 + 128
    assert estimate.estimates_complete is True
    assert estimate.operator_state_bytes == 2 * 4096 + 3 * 2 * per_extremum
    assert estimate.result_bytes == 2 * 4096 + 2 * per_extremum
    assert estimate.estimated_result_rows == 1


def test_wide_binary_extrema_with_tombstones_routes_before_memory_exhaustion(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [[b"x" * 257, b"small"]])
    selected_decoded = 420 * 1024**2
    reflection.decoded_bytes = selected_decoded
    reflection.selected_decoded_bytes = selected_decoded
    reflection.selected_decoded_bytes_complete = True
    resource_key = reflection.supers[0].resource_keys[0]
    reflection.tombstone_views = {
        "t": TombstoneDef(
            tombstone_path=str(tmp_path / "sealed-dv.parquet"),
            cache_key="raw/sealed-dv.parquet",
            expected_rows=1,
            tombstone_digest="0" * 64,
            resource_keys=(resource_key,),
            snapshot_resource_keys=(resource_key,),
        ),
    }
    parser = SQLParser(
        "s", "SELECT max(payload) AS hi FROM s.t", "duckdb",
    )
    resources = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=(0, 1, 2, 3),
        cpuset_cpus=(0, 1, 2, 3),
        memory_limit_bytes=1024**3,
        memory_available_bytes=1024**3,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )

    plan = engine.resource_plan(reflection, parser, streaming_result=True)

    assert plan.advice is ExecutionAdvice.ROUTE_DUCKDB
    assert plan.operator_memory_bytes < selected_decoded
    assert "operator state exceeds memory" in plan.reason


def test_binary_projection_plan_uses_sealed_width_for_result_and_batch_rows(
    tmp_path,
):
    maximum = 20_000
    reflection = _binary_reflection(
        tmp_path, [[b"a" * maximum, b"b" * maximum]],
    )
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    parser = SQLParser("s", "SELECT payload FROM s.t", "duckdb")
    resources = ContainerResources(
        cpu_count=2,
        cpu_capacity=2.0,
        affinity_cpus=(0, 1),
        cpuset_cpus=(0, 1),
        memory_limit_bytes=512 * 1024**2,
        memory_available_bytes=512 * 1024**2,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )

    plan = engine.resource_plan(reflection, parser, streaming_result=True)
    row_width = maximum + 9

    assert plan.advice is ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.batch_rows <= plan.batch_bytes // row_width

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=True)
    assert estimate.result_bytes == 2 * row_width
    assert estimate.estimated_result_rows == 2
    assert estimate.estimates_complete is True


def test_single_binary_result_row_over_stream_cap_is_rejected_before_scan(
    tmp_path, monkeypatch,
):
    maximum = 5 * 1024**2
    reflection = _binary_reflection(tmp_path, [[b"x" * maximum]])
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    query = "SELECT payload FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "oversized-row-plan.json")
    engine = IslandDB()
    monkeypatch.setattr(
        engine,
        "_prepare_lazy_query",
        lambda *_args, **_kwargs: pytest.fail(
            "oversized row must be rejected before relation execution"
        ),
    )

    with pytest.raises(IslandUnsupportedError, match="one sealed result row"):
        engine.execute_stream(
            reflection,
            parser,
            manager,
            lambda _: None,
            max_batch_bytes=4 * 1024**2,
        )


def test_string_projection_plan_uses_sealed_width_for_result_and_batch_rows(
    tmp_path,
):
    reflection, maximum = _string_reflection(
        tmp_path, ["é" * 10_000, "𐍈" * 5_000],
    )
    reflection.selected_decoded_bytes = reflection.decoded_bytes
    reflection.selected_decoded_bytes_complete = True
    parser = SQLParser("s", "SELECT label FROM s.strings", "duckdb")
    resources = ContainerResources(
        cpu_count=2,
        cpu_capacity=2.0,
        affinity_cpus=(0, 1),
        cpuset_cpus=(0, 1),
        memory_limit_bytes=512 * 1024**2,
        memory_available_bytes=512 * 1024**2,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )

    plan = engine.resource_plan(reflection, parser, streaming_result=True)
    row_width = maximum + 17
    assert plan.advice is ExecutionAdvice.ISLAND_IN_MEMORY
    assert plan.batch_rows <= plan.batch_bytes // row_width

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=True)
    assert estimate.result_bytes == 2 * row_width
    assert estimate.estimated_result_rows == 2
    assert estimate.estimates_complete is True


@pytest.mark.parametrize("bound", [None, -1, True, "20000"])
def test_string_projection_malformed_or_missing_width_routes_to_duckdb(
    tmp_path, bound,
):
    reflection, _ = _string_reflection(tmp_path, ["payload"])
    reflection.supers[0].column_max_value_bytes = (
        {} if bound is None else {"label": bound}
    )
    parser = SQLParser("s", "SELECT label FROM s.strings", "duckdb")

    plan = IslandDB().resource_plan(
        reflection, parser, streaming_result=True,
    )

    assert plan.advice is ExecutionAdvice.ROUTE_DUCKDB
    assert "incomplete" in plan.reason


def test_binary_join_result_width_sums_each_physical_snapshot_seal(tmp_path):
    left_path = tmp_path / "binary-join-left.parquet"
    right_path = tmp_path / "binary-join-right.parquet"
    left_width = 10_000
    right_width = 20_000
    pq.write_table(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "payload": pa.array([b"a" * left_width] * 2, type=pa.binary()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
        "__timestamp__": pa.array([1, 1], type=pa.int64()),
    }), left_path)
    pq.write_table(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "payload": pa.array([b"b" * right_width] * 2, type=pa.binary()),
        "__rowid__": pa.array([3, 4], type=pa.int64()),
        "__timestamp__": pa.array([1, 1], type=pa.int64()),
    }), right_path)
    column_types = {
        "id": "Int64", "payload": "Binary",
        "__rowid__": "Int64", "__timestamp__": "Int64",
    }
    left = _snapshot(
        "left_t", [left_path], ["raw/binary-join-left"], types=column_types,
    )
    right = _snapshot(
        "right_t", [right_path], ["raw/binary-join-right"], types=column_types,
    )
    left.column_max_value_bytes = {"payload": left_width}
    right.column_max_value_bytes = {"payload": right_width}
    reflection = _reflection(left, right)
    parser = SQLParser(
        "s",
        "SELECT l.payload AS left_payload, r.payload AS right_payload "
        "FROM s.left_t l JOIN s.right_t r ON l.id = r.id",
        "duckdb",
    )
    assert IslandDB().can_execute(
        reflection, parser, streaming_result=True,
    ).supported

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=True)
    row_width = left_width + 9 + right_width + 9

    assert estimate.estimated_result_rows == 4
    assert estimate.result_bytes == 4 * row_width
    assert estimate.estimates_complete is True


def test_binary_self_join_star_width_counts_occurrences_and_qualified_star(
    tmp_path,
):
    maximum = 4_000
    reflection = _binary_reflection(
        tmp_path, [[b"a" * maximum, b"b" * maximum]],
    )

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine = IslandDB()
    engine._planner = CapturePlanner()
    bare = SQLParser(
        "s", "SELECT * FROM s.t a CROSS JOIN s.t b", "duckdb",
    )
    qualified = SQLParser(
        "s", "SELECT a.* FROM s.t a CROSS JOIN s.t b", "duckdb",
    )

    bare_estimate = engine.resource_plan(
        reflection, bare, streaming_result=True,
    )
    qualified_estimate = engine.resource_plan(
        reflection, qualified, streaming_result=True,
    )
    one_occurrence_width = 24 + maximum + 9

    assert bare_estimate.estimated_result_rows == 4
    assert bare_estimate.result_bytes == 4 * 2 * one_occurrence_width
    assert qualified_estimate.result_bytes == 4 * one_occurrence_width


def test_missing_binary_projection_width_routes_instead_of_using_24_bytes(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [[b"payload"]])
    reflection.supers[0].column_max_value_bytes = {}
    parser = SQLParser("s", "SELECT payload FROM s.t", "duckdb")

    plan = IslandDB().resource_plan(
        reflection, parser, streaming_result=True,
    )

    assert plan.advice is ExecutionAdvice.ROUTE_DUCKDB
    assert "incomplete" in plan.reason


def test_wide_binary_result_triggers_stream_admission_from_sealed_width(
    tmp_path,
):
    maximum = 10_000
    reflection = _binary_reflection(tmp_path, [[b"x" * maximum]])
    # Model a catalog-sealed large snapshot without constructing a 100-MiB
    # fixture. The planner consumes this same immutable candidate-row field in
    # production after DataEstimator validates every selected row group.
    reflection.supers[0].candidate_rows = 10_000
    resources = ContainerResources(
        cpu_count=2,
        cpu_capacity=2.0,
        affinity_cpus=(0, 1),
        cpuset_cpus=(0, 1),
        memory_limit_bytes=512 * 1024**2,
        memory_available_bytes=512 * 1024**2,
    )
    engine = IslandDB()
    engine._resources = resources
    engine._planner = ResourcePlanner(
        resources,
        spill_root=tmp_path,
        disk_usage=lambda _: types.SimpleNamespace(free=16 * 1024**3),
    )
    parser = SQLParser("s", "SELECT payload FROM s.t", "duckdb")

    plan = engine.resource_plan(reflection, parser, streaming_result=False)

    assert plan.advice is ExecutionAdvice.STREAM_RESULT
    assert plan.batch_rows <= plan.batch_bytes // (maximum + 9)


def test_island_instances_share_governor_across_live_memory_limit_change(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as islanddb_module

    def resources(limit, available):
        return ContainerResources(
            cpu_count=1,
            cpu_capacity=1.0,
            affinity_cpus=(0,),
            cpuset_cpus=(0,),
            memory_limit_bytes=limit,
            memory_available_bytes=available,
        )

    samples = iter([
        resources(2 * 1024**3, 2 * 1024**3),
        resources(1024**3, 256 * 1024**2),
    ])
    monkeypatch.setattr(
        IslandDB, "_detect_resources", staticmethod(lambda: next(samples)),
    )
    monkeypatch.setattr(
        islanddb_module,
        "settings",
        dataclasses.replace(
            islanddb_module.settings,
            SUPERTABLE_ISLAND_SPILL_DIR=str(tmp_path / "shared-governor-spill"),
            SUPERTABLE_ISLAND_MAX_RESULT_BYTES=0,
        ),
    )

    first = IslandDB()
    second = IslandDB()

    assert first._governor is second._governor
    assert first._policy != second._policy
    assert second._governor.snapshot()["memory_capacity"] == int(
        256 * 1024**2 * second._policy.global_memory_fraction
    )


def test_tombstone_rowid_proof_opens_only_referenced_files(
    tmp_path, numeric_reflection, monkeypatch,
):
    snap = numeric_reflection.supers[0]
    dv_path = tmp_path / "targeted-dv.parquet"
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [snap.resource_keys[1]],
        "__rowid__": [78],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    dv.write_parquet(dv_path)
    numeric_reflection.tombstone_views["t"] = TombstoneDef(
        tombstone_path=str(dv_path), cache_key="raw/targeted-dv",
        expected_rows=1, tombstone_digest=tombstone_digest(dv),
        resource_keys=tuple(snap.resource_keys),
        snapshot_resource_keys=tuple(snap.resource_keys),
    )
    engine = IslandDB()
    original = engine._base_relation
    proof_keys = []

    def observed(snapshot, **kwargs):
        if kwargs.get("row_group_hints") is False:
            proof_keys.append(tuple(snapshot.resource_keys))
        return original(snapshot, **kwargs)

    monkeypatch.setattr(engine, "_base_relation", observed)
    query = "SELECT id FROM s.t WHERE id BETWEEN 76 AND 78 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.temp_dir = str(tmp_path)
    manager.query_plan_path = str(tmp_path / "targeted-proof-plan.json")

    result = engine.execute(
        numeric_reflection, parser, manager, lambda _: None,
    )

    assert result["id"].tolist() == [76, 78]
    assert proof_keys == [(snap.resource_keys[1],)]


def test_remote_rowid_proof_cache_is_bound_to_object_identity(
    tmp_path, monkeypatch,
):
    resource_key = f"raw/{tmp_path.name}/proof.parquet"
    snapshot = SuperSnapshot(
        "s", "t", 1, ["https://remote.invalid/proof.parquet"], {"id"},
        resource_keys=[resource_key], resource_sizes=[123],
        column_types={"id": "Int64"},
    )
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [resource_key],
        "__rowid__": [7],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    engine = IslandDB(storage=object(), organization=f"org-{tmp_path.name}")
    scans = []

    def proof_scan(proof_snapshot, **kwargs):
        scans.append((tuple(proof_snapshot.resource_keys), kwargs))
        return pl.DataFrame({
            SOURCE_FILE_COL: [resource_key],
            "__rowid__": pl.Series([7], dtype=pl.Int64),
        }).lazy()

    monkeypatch.setattr(engine, "_base_relation", proof_scan)
    first = ObjectMetadata(size=123, version="v1")
    second = ObjectMetadata(size=123, version="v2")

    engine._validate_source_rowids(
        snapshot, dv, object_metadata={resource_key: first},
    )
    engine._validate_source_rowids(
        snapshot, dv, object_metadata={resource_key: first},
    )
    assert len(scans) == 1
    assert scans[0][1]["expected_object_metadata"] == {resource_key: first}

    engine._validate_source_rowids(
        snapshot, dv, object_metadata={resource_key: second},
    )
    assert len(scans) == 2

    # A path/count alone is not an immutable content seal; local/unsealed
    # sources are deliberately re-proved on every query.
    engine._validate_source_rowids(snapshot, dv, object_metadata={})
    engine._validate_source_rowids(snapshot, dv, object_metadata={})
    assert len(scans) == 4


def test_local_rowid_proof_reuses_scan_from_validated_file_identity(
    tmp_path, monkeypatch,
):
    path = tmp_path / "local-proof.parquet"
    resource_key = f"raw/{tmp_path.name}/local-proof.parquet"
    pl.DataFrame({
        "id": [10, 20], "v": [1, 2], "__rowid__": [7, 8],
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = _snapshot("t", [path], [resource_key])
    dv = pl.DataFrame({
        TOMBSTONE_FILE_COL: [resource_key], "__rowid__": [7],
    }, schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64})
    engine = IslandDB(organization=f"org-{tmp_path.name}", range_cache=False)
    object_metadata = {}
    engine._base_relation(snapshot, object_metadata_out=object_metadata)
    assert object_metadata[resource_key].identity_token()

    original = engine._base_relation
    proof_scans = 0

    def counted(proof_snapshot, **kwargs):
        nonlocal proof_scans
        if kwargs.get("row_group_hints") is False:
            proof_scans += 1
        return original(proof_snapshot, **kwargs)

    monkeypatch.setattr(engine, "_base_relation", counted)
    engine._validate_source_rowids(
        snapshot, dv, object_metadata=object_metadata,
    )
    engine._validate_source_rowids(
        snapshot, dv, object_metadata=object_metadata,
    )

    assert proof_scans == 1

    # A same-size rewrite is a new immutable identity. A proof scan pinned to
    # metadata from the earlier relation must fail closed, never validate the
    # old deletion vector against replacement bytes.
    pl.DataFrame({
        "id": [30, 40], "v": [3, 4], "__rowid__": [7, 8],
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    assert os.path.getsize(path) == snapshot.resource_sizes[0]
    with pytest.raises(IslandIntegrityError, match="identity.*changed"):
        engine._base_relation(
            snapshot, expected_object_metadata=object_metadata,
        )


def test_one_arrow_dataset_handles_128_files_with_composite_tombstones(tmp_path):
    paths = []
    keys = []
    deleted_files = []
    for index in range(128):
        path = tmp_path / f"part-{index:03d}.parquet"
        key = f"raw/part-{index:03d}.parquet"
        pl.DataFrame({
            "id": [index],
            "v": [index * 2],
            "__rowid__": [index + 1],
            "__timestamp__": [1],
        }).write_parquet(path)
        paths.append(path)
        keys.append(key)
        if index % 3 == 0:
            deleted_files.append((key, index + 1))
    snapshot = _snapshot("t", paths, keys)
    reflection = _reflection(snapshot)
    dv_path = tmp_path / "dv.parquet"
    dv = pl.DataFrame(
        deleted_files,
        schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64},
        orient="row",
    )
    dv.write_parquet(dv_path)
    reflection.tombstone_views["t"] = TombstoneDef(
        str(dv_path), "raw/dv", dv.height, tombstone_digest(dv),
        tuple(keys), tuple(keys),
    )
    query = "SELECT count(*) AS n, sum(v) AS total FROM s.t"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, engine = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert engine.last_profile.files == 128
    assert "Parquet SCAN" in engine.last_profile.optimized_plan
    assert "strict_cast(Enum" in engine.last_profile.optimized_plan


def test_duplicate_source_rowid_fails_closed(tmp_path):
    path = tmp_path / "bad.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20], "__rowid__": [7, 7],
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    snap = _snapshot("t", [path], ["raw/bad"])
    reflection = _reflection(snap)
    dv_path = tmp_path / "bad-dv.parquet"
    dv = pl.DataFrame(
        {TOMBSTONE_FILE_COL: ["raw/bad"], "__rowid__": [7]},
        schema={TOMBSTONE_FILE_COL: pl.String, "__rowid__": pl.Int64},
    )
    dv.write_parquet(dv_path)
    reflection.tombstone_views["t"] = TombstoneDef(
        str(dv_path), "raw/dv", 1, tombstone_digest(dv),
        ("raw/bad",), ("raw/bad",),
    )
    with pytest.raises(IslandIntegrityError, match="exactly one physical row"):
        _run_island(tmp_path, reflection, "SELECT id FROM s.t WHERE id=1")


@pytest.mark.parametrize("query, reason", [
    ("SELECT id FROM s.t WHERE label='r1'", "NOCASE"),
    ("SELECT label FROM s.t ORDER BY label", "unproven DuckDB semantics"),
    ("SELECT label=label AS same FROM s.t", "non-numeric semantics"),
    ("SELECT id FROM s.t LIMIT 1", "LIMIT/OFFSET"),
    ("SELECT id FROM s.t ORDER BY id LIMIT 1", "LIMIT/OFFSET"),
    ("SELECT id FROM s.t ORDER BY id DESC", "DESC/NULLS FIRST"),
    ("SELECT -id AS negative_id FROM s.t", "signed expression"),
    ("SELECT avg(v) FROM s.t", "AVG reduction"),
    ("SELECT sum(-v) AS total FROM s.t", "signed expression"),
    ("SELECT sum(1) AS total FROM s.t", "one direct column"),
    ("SELECT sum(\"v\") FROM s.t", "explicit alias"),
    ("SELECT count(t.*) AS n FROM s.t AS t", "qualified aggregate star"),
    ("SELECT date_trunc('day', id) FROM s.t", "unsupported SQL nodes"),
    ("SELECT id AND v AS x FROM s.t", "Boolean coercion"),
    ("SELECT id OR v AS x FROM s.t", "Boolean coercion"),
    ("SELECT id IS TRUE AS x FROM s.t", "IS TRUE/FALSE/NULL"),
    ("SELECT id FROM s.t WHERE id", "Boolean coercion"),
    ("SELECT id FROM s.t WHERE id IN (TRUE)", "mixed/unproven types"),
])
def test_unproven_semantics_are_rejected_before_execution(
    tmp_path, numeric_reflection, query, reason,
):
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB()
    capability = engine.can_execute(numeric_reflection, parser)
    assert not capability.supported
    assert reason in "; ".join(capability.reasons)
    manager = QueryPlanManager("s", "island-tests", "", query)
    with pytest.raises(IslandUnsupportedError, match=reason):
        engine.execute(numeric_reflection, parser, manager, lambda _: None)


def test_prepare_execution_revalidates_mutated_original_sql(
    numeric_reflection,
):
    parser = SQLParser(
        "s", "SELECT count(*) AS n FROM s.t", "duckdb",
    )
    parser.original_query = "SELECT * FROM read_parquet('/unauthorized')"

    with pytest.raises(
        IslandUnsupportedError,
        match="backend SQL validation failed",
    ):
        IslandDB().prepare_execution(
            numeric_reflection,
            parser,
            streaming_result=True,
        )


@pytest.mark.parametrize("streaming", [False, True])
def test_prepared_execution_pins_validated_sql_against_later_mutation(
    tmp_path,
    numeric_reflection,
    streaming,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / f"pinned-{streaming}.json")
    engine = IslandDB()
    prepared = engine.prepare_execution(
        numeric_reflection,
        parser,
        streaming_result=streaming,
    )
    parser.original_query = "SELECT sum(v) AS n FROM s.t"

    if streaming:
        table = engine.execute_stream(
            numeric_reflection,
            parser,
            manager,
            lambda _: None,
            _prepared=prepared,
        ).collect_table(max_bytes=1024 * 1024)
        assert table["n"].to_pylist() == [100]
    else:
        result = engine.execute(
            numeric_reflection,
            parser,
            manager,
            lambda _: None,
            _prepared=prepared,
        )
        assert result["n"].tolist() == [100]


@pytest.mark.parametrize("streaming", [False, True])
def test_prepared_execution_rejects_changed_semantic_reflection(
    tmp_path,
    numeric_reflection,
    streaming,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / f"reflection-{streaming}.json")
    engine = IslandDB()
    prepared = engine.prepare_execution(
        numeric_reflection,
        parser,
        streaming_result=streaming,
    )
    changed = dataclasses.replace(
        numeric_reflection,
        decoded_bytes=numeric_reflection.decoded_bytes + 1,
    )

    with pytest.raises(
        IslandIntegrityError,
        match="prepared IslandDB plan does not match execution reflection",
    ):
        if streaming:
            engine.execute_stream(
                changed,
                parser,
                manager,
                lambda _: None,
                _prepared=prepared,
            )
        else:
            engine.execute(
                changed,
                parser,
                manager,
                lambda _: None,
                _prepared=prepared,
            )


def test_prepared_execution_requires_explicit_cache_localization_rebind(
    tmp_path,
    numeric_reflection,
):
    query = "SELECT count(*) AS n FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "localized-reflection.json")
    engine = IslandDB()
    prepared = engine.prepare_execution(
        numeric_reflection,
        parser,
        streaming_result=True,
    )
    original_snapshot = numeric_reflection.supers[0]
    localized_paths = []
    for index, original_path in enumerate(original_snapshot.files):
        localized_path = tmp_path / f"cache-localized-{index}.parquet"
        shutil.copyfile(original_path, localized_path)
        localized_paths.append(str(localized_path))
    localized_snapshot = dataclasses.replace(
        original_snapshot,
        files=localized_paths,
    )
    localized_reflection = dataclasses.replace(
        numeric_reflection,
        supers=[localized_snapshot],
    )

    with pytest.raises(
        IslandIntegrityError,
        match="prepared IslandDB plan does not match execution reflection",
    ):
        engine.execute_stream(
            localized_reflection,
            parser,
            manager,
            lambda _: None,
            _prepared=prepared,
        )

    localized_prepared = engine._rebind_cache_localized_prepared(
        prepared,
        numeric_reflection,
        localized_reflection,
    )
    table = engine.execute_stream(
        localized_reflection,
        parser,
        manager,
        lambda _: None,
        _prepared=localized_prepared,
    ).collect_table(max_bytes=1024 * 1024)

    assert table["n"].to_pylist() == [100]


def test_stream_prepared_plan_cannot_bypass_materialized_capability_gate(
    tmp_path,
):
    reflection = _binary_reflection(tmp_path, [[b"payload"]])
    query = "SELECT payload FROM s.t"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    engine = IslandDB()
    prepared = engine.prepare_execution(
        reflection,
        parser,
        streaming_result=True,
    )

    with pytest.raises(
        IslandIntegrityError,
        match="prepared IslandDB plan has the wrong result mode",
    ):
        engine.execute(
            reflection,
            parser,
            manager,
            lambda _: None,
            _prepared=prepared,
        )


def test_response_limit_is_native_when_candidate_count_proves_it_redundant(
    numeric_reflection,
):
    parser = SQLParser("s", "SELECT id FROM s.t LIMIT 10000", "duckdb")

    capability = IslandDB().can_execute(numeric_reflection, parser)

    assert capability.supported, capability.reasons


def test_left_join_empty_right_preserves_left_cardinality_bound(tmp_path):
    left_path = tmp_path / "left.parquet"
    right_path = tmp_path / "right-empty.parquet"
    schema = {
        "id": pl.Int64,
        "v": pl.Int64,
        "__rowid__": pl.Int64,
        "__timestamp__": pl.Int64,
    }
    pl.DataFrame({
        "id": [1, 2, 3],
        "v": [10, 20, 30],
        "__rowid__": [1, 2, 3],
        "__timestamp__": [1, 1, 1],
    }, schema=schema).write_parquet(left_path)
    pl.DataFrame(schema=schema).write_parquet(right_path)
    reflection = _reflection(
        _snapshot("left_t", [left_path], ["raw/left"]),
        _snapshot("right_t", [right_path], ["raw/right-empty"]),
    )

    engine = IslandDB()
    outer_bounds = {
        "SELECT l.id AS id FROM s.left_t l "
        "LEFT JOIN s.right_t r ON l.id=r.id": 3,
        "SELECT l.id AS id FROM s.left_t l "
        "FULL JOIN s.right_t r ON l.id=r.id": 3,
        "SELECT l.id AS id FROM s.right_t r "
        "RIGHT JOIN s.left_t l ON l.id=r.id": 3,
    }
    for sql, expected_bound in outer_bounds.items():
        bound_parser = SQLParser("s", sql, "duckdb")
        assert engine._candidate_result_upper_bound(
            engine._query_root(bound_parser), bound_parser, reflection,
        ) == expected_bound

    truncating = SQLParser(
        "s",
        "SELECT l.id AS id FROM s.left_t l "
        "LEFT JOIN s.right_t r ON l.id=r.id LIMIT 1",
        "duckdb",
    )
    capability = engine.can_execute(reflection, truncating)
    assert not capability.supported
    assert "LIMIT/OFFSET" in "; ".join(capability.reasons)

    bounded_sql = (
        "SELECT l.id AS id FROM s.left_t l "
        "LEFT JOIN s.right_t r ON l.id=r.id ORDER BY l.id LIMIT 3"
    )
    parser = SQLParser("s", bounded_sql, "duckdb")
    assert engine.can_execute(reflection, parser).supported

    class CapturePlanner:
        def plan(self, estimate, *, streaming_result):
            return estimate

    engine._planner = CapturePlanner()
    estimate = engine.resource_plan(reflection, parser, streaming_result=False)
    assert estimate.estimated_result_rows == 3
    assert estimate.result_bytes == 3 * 24

    expected = _run_duckdb(tmp_path, reflection, bounded_sql)
    actual, _ = _run_island(tmp_path, reflection, bounded_sql)
    pd.testing.assert_frame_equal(actual, expected)


def test_unproven_projected_pandas_type_is_rejected(tmp_path):
    path = tmp_path / "decimal.parquet"
    frame = pl.DataFrame({
        "id": [1], "amount": [1], "__rowid__": [1], "__timestamp__": [1],
    }).with_columns(pl.col("amount").cast(pl.Decimal(12, 2)))
    frame.write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/decimal"],
        types={
            "id": "Int64", "amount": "Decimal(precision=12, scale=2)",
            "__rowid__": "Int64", "__timestamp__": "Int64",
        },
    ))
    parser = SQLParser("s", "SELECT amount FROM s.t", "duckdb")

    capability = IslandDB().can_execute(reflection, parser)

    assert not capability.supported
    assert any("pandas parity" in reason for reason in capability.reasons)


@pytest.mark.parametrize("join_sql, reason", [
    ("SELECT a.id AS aid FROM s.a a JOIN s.b b USING(id)", "USING"),
    ("SELECT * FROM s.a a JOIN s.b b ON a.id=b.id", "SELECT *"),
    ("SELECT a.id, b.id FROM s.a a JOIN s.b b ON a.id=b.id", "duplicate output"),
])
def test_unproven_join_forms_and_output_names_are_rejected(
    tmp_path, join_sql, reason,
):
    a = tmp_path / "cap-a.parquet"
    b = tmp_path / "cap-b.parquet"
    for path, rowid in ((a, 1), (b, 2)):
        pl.DataFrame({
            "id": [1], "v": [1], "__rowid__": [rowid],
            "__timestamp__": [1],
        }).write_parquet(path)
    reflection = _reflection(
        _snapshot("a", [a], ["raw/cap-a"]),
        _snapshot("b", [b], ["raw/cap-b"]),
    )

    capability = IslandDB().can_execute(
        reflection, SQLParser("s", join_sql, "duckdb"),
    )

    assert not capability.supported
    assert reason in "; ".join(capability.reasons)


@pytest.mark.parametrize(
    "projection", ["*, id", "id, t.*"],
)
def test_star_expansion_duplicate_output_is_rejected(
    numeric_reflection, projection,
):
    parser = SQLParser(
        "s", f"SELECT {projection} FROM s.t AS t", "duckdb",
    )

    capability = IslandDB().can_execute(numeric_reflection, parser)

    assert capability.supported is False
    assert "duplicate output name 'id'" in "; ".join(capability.reasons)


def test_natural_join_is_rejected_at_shared_parser_boundary():
    with pytest.raises(ValueError, match="NATURAL JOIN is not supported"):
        SQLParser(
            "s",
            "SELECT a.id AS aid FROM s.a a NATURAL JOIN s.b b",
            "duckdb",
        )


@pytest.mark.parametrize("projection", ["*", "a.*, b.*"])
def test_self_join_stars_are_rejected(tmp_path, projection):
    path = tmp_path / "self-join.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [10, 20],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot("t", [path], ["raw/self-join"]))
    query = (
        f"SELECT {projection} FROM s.t a JOIN s.t b ON a.id=b.id "
        "ORDER BY a.id"
    )

    capability = IslandDB().can_execute(
        reflection, SQLParser("s", query, "duckdb"),
    )

    assert not capability.supported
    assert "output naming is not native" in "; ".join(capability.reasons)


def test_float_min_max_nan_semantics_are_rejected(tmp_path):
    path = tmp_path / "float.parquet"
    pl.DataFrame({
        "id": [1, 2], "v": [1.0, float("nan")],
        "__rowid__": [1, 2], "__timestamp__": [1, 1],
    }).write_parquet(path)
    reflection = _reflection(_snapshot(
        "t", [path], ["raw/float"],
        types={
            "id": "Int64", "v": "Float64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    ))
    parser = SQLParser("s", "SELECT max(v) AS hi FROM s.t", "duckdb")

    capability = IslandDB().can_execute(reflection, parser)

    assert not capability.supported
    assert any("NaN" in reason for reason in capability.reasons)


def test_rbac_never_silently_bypasses_native_engine(tmp_path, numeric_reflection):
    numeric_reflection.rbac_views["t"] = RbacViewDef(["id"], "id > 10")
    parser = SQLParser("s", "SELECT id FROM s.t WHERE id=20", "duckdb")
    capability = IslandDB().can_execute(numeric_reflection, parser)
    assert not capability.supported
    assert any("RBAC" in reason for reason in capability.reasons)


def test_static_island_cpu_limit_bootstraps_polars_pool_before_import():
    env = os.environ.copy()
    env["SUPERTABLE_ISLAND_CPU_MAX"] = "1"
    env.pop("POLARS_MAX_THREADS", None)
    observed = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "import supertable; import polars as pl; "
            "print(pl.thread_pool_size())",
        ],
        cwd=str(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(__file__),
        )))),
        env=env,
        text=True,
    ).strip()

    assert observed == "1"


def test_initialized_polars_pool_wider_than_cpu_cap_remains_supported(
    numeric_reflection, monkeypatch,
):
    monkeypatch.setattr(
        islanddb_module,
        "settings",
        dataclasses.replace(
            islanddb_module.settings,
            SUPERTABLE_ISLAND_CPU_MAX=1,
        ),
    )
    monkeypatch.setattr(islanddb_module.pl, "thread_pool_size", lambda: 8)
    engine = IslandDB()

    capability = engine.can_execute(
        numeric_reflection,
        SQLParser("s", "SELECT id FROM s.t", "duckdb"),
        streaming_result=True,
    )

    assert capability.supported


def test_live_island_cpu_and_memory_config_are_pinned_in_prepared_query(
    numeric_reflection, monkeypatch,
):
    detected = ContainerResources(
        cpu_count=8,
        cpu_capacity=8.0,
        affinity_cpus=tuple(range(8)),
        cpuset_cpus=tuple(range(8)),
        memory_limit_bytes=2 * 1024**3,
        memory_available_bytes=2 * 1024**3,
    )
    monkeypatch.setattr(
        islanddb_module.ContainerResources,
        "detect",
        classmethod(lambda cls: detected),
    )
    monkeypatch.setattr(islanddb_module.pl, "thread_pool_size", lambda: 4)
    engine = IslandDB(range_cache=False)
    parser = SQLParser("s", "SELECT id FROM s.t", "duckdb")
    first_config = IslandRuntimeConfig(
        cpu_max=4,
        memory_max_bytes=256 * 1024**2,
        cache_max_bytes=111,
        range_cache_max_bytes=222,
    )

    prepared = engine.prepare_execution(
        numeric_reflection,
        parser,
        streaming_result=True,
        engine_config=first_config,
    )

    assert prepared.runtime_config is first_config
    assert prepared.resources.cpu_count == 4
    assert prepared.resources.cpu_capacity == 4.0
    assert prepared.resources.memory_limit_bytes == 256 * 1024**2
    assert prepared.resources.memory_available_bytes == 256 * 1024**2
    assert prepared.policy.max_query_memory_bytes == 256 * 1024**2
    assert prepared.resource_plan.cpu_workers == 4
    assert prepared.resource_plan.memory_budget_bytes <= 256 * 1024**2

    # A later query gets the newly resolved immutable policy while the already
    # prepared query retains its original resources and governor.
    second_config = dataclasses.replace(
        first_config,
        memory_max_bytes=512 * 1024**2,
    )
    second = engine.prepare_execution(
        numeric_reflection,
        parser,
        streaming_result=True,
        engine_config=second_config,
    )
    assert second.resources.memory_limit_bytes == 512 * 1024**2
    assert second.policy.max_query_memory_bytes == 512 * 1024**2
    assert prepared.resources.memory_limit_bytes == 256 * 1024**2


def test_prepared_query_uses_one_live_container_resource_sample(
    numeric_reflection, monkeypatch,
):
    constrained = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=tuple(range(4)),
        cpuset_cpus=tuple(range(4)),
        memory_limit_bytes=256 * 1024**2,
        memory_available_bytes=256 * 1024**2,
    )
    wider_later_sample = dataclasses.replace(
        constrained,
        memory_limit_bytes=2 * 1024**3,
        memory_available_bytes=2 * 1024**3,
    )
    monkeypatch.setattr(islanddb_module.pl, "thread_pool_size", lambda: 4)
    engine = IslandDB(range_cache=False)
    samples = iter((constrained, wider_later_sample))
    calls = []

    def detect(config=None):
        calls.append(config)
        return next(samples)

    monkeypatch.setattr(engine, "_detect_resources", detect)
    config = IslandRuntimeConfig(
        cpu_max=None,
        memory_max_bytes=None,
        cache_max_bytes=None,
        range_cache_max_bytes=None,
    )
    prepared = engine.prepare_execution(
        numeric_reflection,
        SQLParser("s", "SELECT id FROM s.t", "duckdb"),
        streaming_result=True,
        engine_config=config,
    )

    assert calls == [config]
    assert prepared.resources is constrained
    assert (
        prepared.resource_plan.memory_budget_bytes
        <= prepared.governor.snapshot()["memory_capacity"]
    )


def test_unconfigured_island_memory_policy_uses_all_container_availability(
    monkeypatch,
):
    resources = ContainerResources(
        cpu_count=4,
        cpu_capacity=4.0,
        affinity_cpus=(0, 1, 2, 3),
        cpuset_cpus=(0, 1, 2, 3),
        memory_limit_bytes=2 * 1024**3,
        memory_available_bytes=1536 * 1024**2,
    )
    monkeypatch.setattr(
        islanddb_module,
        "settings",
        dataclasses.replace(
            islanddb_module.settings,
            SUPERTABLE_ISLAND_MEMORY_FRACTION=1.0,
            SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION=1.0,
            SUPERTABLE_ISLAND_MAX_MEMORY_BYTES=0,
            SUPERTABLE_ISLAND_MAX_RESULT_BYTES=0,
        ),
    )

    policy = IslandDB._resource_policy(resources)

    assert policy.query_memory_fraction == 1.0
    assert policy.global_memory_fraction == 1.0
    assert policy.max_query_memory_bytes == 0
    assert policy.max_result_memory_bytes == 1536 * 1024**2


def test_live_island_whole_file_cache_capacity_rebuilds_for_next_query(
    tmp_path, monkeypatch,
):
    import supertable.engine.executor as executor_module

    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "whole-cache"),
        ),
    )
    executor = Executor.__new__(Executor)
    executor.storage = LocalStorage()
    executor.organization = "live-config"
    executor._file_cache = None
    first_config = IslandRuntimeConfig(None, None, 1024, None)
    second_config = dataclasses.replace(first_config, cache_max_bytes=2048)

    first = executor._get_file_cache(first_config)
    second = executor._get_file_cache(second_config)

    assert first.max_bytes == 1024
    assert second.max_bytes == 2048
    assert second is not first


def test_unconfigured_whole_file_cache_uses_stable_filesystem_budget(
    tmp_path, monkeypatch,
):
    import supertable.engine.executor as executor_module
    import supertable.engine.file_cache as file_cache_module

    budgets = iter((1234, 5678))
    monkeypatch.setattr(
        file_cache_module,
        "automatic_cache_max_bytes",
        lambda *_args, **_kwargs: next(budgets),
    )
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "whole-auto"),
            SUPERTABLE_ISLAND_CACHE_MAX_BYTES=0,
        ),
    )
    executor = Executor.__new__(Executor)
    executor.storage = LocalStorage()
    executor.organization = "auto-cache"
    executor._file_cache = None

    first = executor._get_file_cache()
    second = executor._get_file_cache()

    assert first is second
    assert first.max_bytes == 1234
    assert first._supertable_automatic_capacity is True


def test_live_automatic_whole_cache_overrides_positive_process_fallback(
    tmp_path, monkeypatch,
):
    import supertable.engine.executor as executor_module
    import supertable.engine.file_cache as file_cache_module

    monkeypatch.setattr(
        file_cache_module,
        "automatic_cache_max_bytes",
        lambda *_args, **_kwargs: 4321,
    )
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "whole-live-auto"),
            SUPERTABLE_ISLAND_CACHE_MAX_BYTES=12345,
        ),
    )
    executor = Executor.__new__(Executor)
    executor.storage = LocalStorage()
    executor.organization = "live-auto-cache"
    executor._file_cache = None

    cache = executor._get_file_cache(
        IslandRuntimeConfig(None, None, None, None),
    )

    assert cache.max_bytes == 4321
    assert cache._supertable_automatic_capacity is True


def test_automatic_whole_cache_rebuilds_when_peer_live_limit_changes(
    tmp_path, monkeypatch,
):
    import supertable.engine.executor as executor_module
    import supertable.engine.file_cache as file_cache_module

    peer_limits = []
    budgets = iter((4096, 1024))

    def automatic_budget(*_args, **kwargs):
        peer_limits.append(tuple(kwargs["peer_limits"]))
        return next(budgets)

    monkeypatch.setattr(
        file_cache_module, "automatic_cache_max_bytes", automatic_budget,
    )
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "whole-peer-live"),
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "range-peer-live"),
        ),
    )
    executor = Executor.__new__(Executor)
    executor.storage = LocalStorage()
    executor.organization = "peer-live"
    executor._file_cache = None
    automatic = IslandRuntimeConfig(None, None, None, None)

    first = executor._get_file_cache(automatic)
    second = executor._get_file_cache(dataclasses.replace(
        automatic, range_cache_max_bytes=2048,
    ))

    assert first.max_bytes == 4096
    assert second.max_bytes == 1024
    assert second is not first
    assert peer_limits[0][0][1] == 0
    assert peer_limits[1][0][1] == 2048


def test_public_executor_dispatches_explicit_islanddb(
    tmp_path, numeric_reflection,
):
    # LocalStorage's stable catalog identity is the local path itself; unlike
    # the earlier backend-agnostic fixtures it needs no synthetic raw/ prefix.
    snap = numeric_reflection.supers[0]
    snap.resource_keys = list(snap.files)
    snap.snapshot_resource_keys = list(snap.files)
    query = "SELECT id, v FROM s.t WHERE id >= 98 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.temp_dir = str(tmp_path)
    manager.query_plan_path = str(tmp_path / "executor-plan.json")
    stats = PlanStats()
    result, used = Executor(
        storage=LocalStorage(), organization="island-tests",
    ).execute(
        Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(), stats, "",
    )
    assert used == "islanddb"
    assert result["id"].tolist() == [98, 99]
    assert any(item.get("ENGINE") == "islanddb" for item in stats.stats)
    # Local data is already in the shared filesystem namespace. IslandDB
    # neither duplicates it into the object cache nor performs range I/O.
    assert not any(
        "FILE_CACHE_LOCALIZED_FILES" in item for item in stats.stats
    )
    telemetry = next(
        item["ISLAND_TELEMETRY"]
        for item in stats.stats
        if "ISLAND_TELEMETRY" in item
    )
    assert telemetry["telemetry_query_id"] == manager._island_profile_token
    assert telemetry["planned_files_complete"] is True
    assert telemetry["planned_row_groups_complete"] is True
    assert telemetry["planned_rows_complete"] is True
    assert telemetry["physical_read_bytes"] >= 0
    assert telemetry["physical_read_bytes_measured"] is True
    assert telemetry["physical_read_scope"] == (
        "linux_proc_self_io_block_read_delta_after_admission_"
        "until_profile_finalize"
    )


def test_auto_materialized_query_pins_one_live_island_config_snapshot(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    snap = numeric_reflection.supers[0]
    snap.resource_keys = list(snap.files)
    snap.snapshot_resource_keys = list(snap.files)
    runtime = IslandRuntimeConfig(
        cpu_max=int(pl.thread_pool_size()),
        memory_max_bytes=None,
        cache_max_bytes=None,
        range_cache_max_bytes=None,
    )
    duck_config = resolve_engine_config("", None, "duckdb")
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": duck_config, "islanddb": runtime}, ()),
    )
    executor = Executor(storage=LocalStorage(), organization="live-config")
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    seen = {}

    def pick(*_args, island_config=None, **_kwargs):
        seen["route"] = island_config
        return Engine.ISLANDDB

    original_prepare = executor.island_exec.prepare_execution
    original_execute = executor.island_exec.execute
    original_get_file_cache = executor._get_file_cache

    def get_file_cache(config=None):
        seen["file_cache"] = config
        return original_get_file_cache(config)

    def prepare(*args, **kwargs):
        seen["prepare"] = kwargs.get("engine_config")
        prepared = original_prepare(*args, **kwargs)
        seen["prepared"] = prepared.runtime_config
        return prepared

    def execute(**kwargs):
        seen["execute"] = kwargs.get("engine_config")
        seen["execute_prepared"] = kwargs["_prepared"].runtime_config
        return original_execute(**kwargs)

    monkeypatch.setattr(executor, "_auto_pick", pick)
    monkeypatch.setattr(executor, "_get_file_cache", get_file_cache)
    monkeypatch.setattr(executor.island_exec, "prepare_execution", prepare)
    monkeypatch.setattr(executor.island_exec, "execute", execute)
    query = "SELECT id, v FROM s.t WHERE id >= 98 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "live-config", "", query)
    manager.query_plan_path = str(tmp_path / "materialized-live-config.json")

    result, used = executor.execute(
        Engine.AUTO,
        numeric_reflection,
        parser,
        manager,
        Timer(),
        PlanStats(),
        "",
    )

    assert used == "islanddb"
    assert result["id"].tolist() == [98, 99]
    assert set(seen.values()) == {runtime}


def test_auto_arrow_stream_pins_one_live_island_config_snapshot(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    snap = numeric_reflection.supers[0]
    snap.resource_keys = list(snap.files)
    snap.snapshot_resource_keys = list(snap.files)
    runtime = IslandRuntimeConfig(
        cpu_max=int(pl.thread_pool_size()),
        memory_max_bytes=None,
        cache_max_bytes=None,
        range_cache_max_bytes=None,
    )
    duck_config = resolve_engine_config("", None, "duckdb")
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": duck_config, "islanddb": runtime}, ()),
    )
    executor = Executor(storage=LocalStorage(), organization="live-config")
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    seen = {}

    def pick(*_args, island_config=None, **_kwargs):
        seen["route"] = island_config
        return Engine.ISLANDDB

    original_prepare = executor.island_exec.prepare_execution
    original_execute_stream = executor.island_exec.execute_stream
    original_get_file_cache = executor._get_file_cache
    original_get_range_cache = executor.island_exec._get_range_cache

    def get_file_cache(config=None):
        seen["file_cache"] = config
        return original_get_file_cache(config)

    def get_range_cache(config=None):
        seen["range_cache"] = config
        return original_get_range_cache(config)

    def prepare(*args, **kwargs):
        seen["prepare"] = kwargs.get("engine_config")
        prepared = original_prepare(*args, **kwargs)
        seen["prepared"] = prepared.runtime_config
        return prepared

    def execute_stream(**kwargs):
        seen["execute"] = kwargs.get("engine_config")
        seen["execute_prepared"] = kwargs["_prepared"].runtime_config
        return original_execute_stream(**kwargs)

    monkeypatch.setattr(executor, "_auto_pick", pick)
    monkeypatch.setattr(executor, "_get_file_cache", get_file_cache)
    monkeypatch.setattr(
        executor.island_exec, "_get_range_cache", get_range_cache,
    )
    monkeypatch.setattr(executor.island_exec, "prepare_execution", prepare)
    monkeypatch.setattr(
        executor.island_exec, "execute_stream", execute_stream,
    )
    query = "SELECT id, v FROM s.t WHERE id >= 98 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "live-config", "", query)
    manager.query_plan_path = str(tmp_path / "stream-live-config.json")

    stream, used = executor.execute_stream(
        Engine.AUTO,
        numeric_reflection,
        parser,
        manager,
        Timer(),
        PlanStats(),
        "",
    )
    with stream:
        rows = stream.collect_table(
            max_bytes=1024 * 1024,
        ).column("id").to_pylist()

    assert used == "islanddb"
    assert rows == [98, 99]
    assert set(seen.values()) == {runtime}


def test_public_executor_arrow_stream_is_batched_and_exact(
    tmp_path, numeric_reflection,
):
    query = "SELECT id, v FROM s.t WHERE id >= 95 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "executor-stream-plan.json")
    stats = PlanStats()
    stream, used = Executor(
        storage=LocalStorage(), organization="island-tests",
    ).execute_stream(
        Engine.ISLANDDB,
        numeric_reflection,
        parser,
        manager,
        Timer(),
        stats,
        "",
    )

    with stream:
        table = stream.collect_table(max_bytes=1024 * 1024)

    assert used == "islanddb"
    assert table.column("id").to_pylist() == [95, 96, 97, 98, 99]
    assert any(item.get("RESULT_MODE") == "arrow_stream" for item in stats.stats)
    telemetry = next(
        item["ISLAND_TELEMETRY"]
        for item in stats.stats
        if "ISLAND_TELEMETRY" in item
    )
    assert telemetry["result_complete"] is True
    assert telemetry["execution_outcome"] == "completed"
    assert telemetry["result_rows"] == 5


def test_local_arrow_query_does_not_construct_unused_disk_caches(
    tmp_path, numeric_reflection, monkeypatch,
):
    snap = numeric_reflection.supers[0]
    snap.resource_keys = list(snap.files)
    snap.snapshot_resource_keys = list(snap.files)
    executor = Executor(
        storage=LocalStorage(), organization="local-cache-bypass",
    )

    def unexpected_cache(*_args, **_kwargs):
        pytest.fail("a fully local query must not size or construct disk caches")

    monkeypatch.setattr(executor, "_get_file_cache", unexpected_cache)
    monkeypatch.setattr(
        executor.island_exec, "_get_range_cache", unexpected_cache,
    )
    query = "SELECT id FROM s.t WHERE id >= 98 ORDER BY id"
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "local-cache-bypass.json")

    stream, used = executor.execute_stream(
        Engine.ISLANDDB,
        numeric_reflection,
        SQLParser("s", query, "duckdb"),
        manager,
        Timer(),
        PlanStats(),
        "",
    )
    with stream:
        rows = stream.collect_table(max_bytes=1024**2)["id"].to_pylist()

    assert used == "islanddb"
    assert rows == [98, 99]


def test_public_island_stream_bounds_arbitrary_width_at_native_producer(
    tmp_path, monkeypatch,
):
    path = tmp_path / "wide-result.parquet"
    payload = "x" * (3 * 1024 * 1024)
    pl.DataFrame({
        "payload": [payload] * 4,
        "__rowid__": [1, 2, 3, 4],
        "__timestamp__": [1, 1, 1, 1],
    }).write_parquet(path, compression="zstd")
    snapshot = _snapshot(
        "wide",
        [path],
        ["raw/wide-result.parquet"],
        types={
            "payload": "String",
            "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    snapshot.column_max_value_bytes = {"payload": len(payload.encode("utf-8"))}
    reflection = _reflection(snapshot)
    query = "SELECT payload FROM s.wide"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "wide-result-plan.json")

    engine = IslandDB()
    observed_batch_rows = []
    original_lazy_batches = engine._lazy_batches

    def capture_lazy_batches(lazy_result, *, batch_rows):
        observed_batch_rows.append(batch_rows)
        return original_lazy_batches(lazy_result, batch_rows=batch_rows)

    monkeypatch.setattr(engine, "_lazy_batches", capture_lazy_batches)
    stream = engine.execute_stream(
        reflection,
        parser,
        manager,
        lambda _event: None,
    )
    try:
        first = next(stream)
        assert observed_batch_rows == [1]
        assert first.num_rows == 1
        assert 3 * 1024 * 1024 <= first.nbytes < 4 * 1024 * 1024
    finally:
        stream.close()


def test_public_executor_partial_stream_publishes_fallback_when_profile_write_fails(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = "SELECT id FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "missing-stream-profile.json")
    stats = PlanStats()
    executor = Executor(
        storage=LocalStorage(), organization="island-tests",
    )
    monkeypatch.setattr(
        executor.island_exec,
        "_write_profile",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("profile unavailable")
        ),
    )

    stream, used = executor.execute_stream(
        Engine.ISLANDDB,
        numeric_reflection,
        parser,
        manager,
        Timer(),
        stats,
        "",
    )
    stream.close()

    assert used == "islanddb"
    telemetry = next(
        item["ISLAND_TELEMETRY"]
        for item in stats.stats
        if "ISLAND_TELEMETRY" in item
    )
    assert telemetry["result_complete"] is False
    assert telemetry["execution_outcome"] == "closed_early"
    assert telemetry["profile_persist_succeeded"] is False
    assert not (tmp_path / "missing-stream-profile.json").exists()


def test_auto_arrow_stream_records_selected_and_actual_engine(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings, SUPERTABLE_ISLAND_AUTO_ENABLED=True,
        ),
    )
    # Model a medium selective relation so AUTO chooses the bounded native
    # stream while the immutable fixture itself stays tiny and fast.
    numeric_reflection.reflection_bytes = 512 * 1024 * 1024
    numeric_reflection.row_group_scan_bytes = 512 * 1024 * 1024
    query = "SELECT id, v FROM s.t WHERE id >= 95 ORDER BY id"
    stats = PlanStats()

    stream, used = Executor(
        storage=LocalStorage(), organization="island-tests",
    ).execute_stream(
        Engine.AUTO,
        numeric_reflection,
        SQLParser("s", query, "duckdb"),
        QueryPlanManager("s", "island-tests", "", query),
        Timer(),
        stats,
        "",
    )
    with stream:
        result = stream.collect_table(max_bytes=1024 * 1024)

    assert used == "islanddb"
    assert result["id"].to_pylist() == [95, 96, 97, 98, 99]
    outcome = next(
        item["AUTO_ROUTING_OUTCOME"]
        for item in stats.stats if "AUTO_ROUTING_OUTCOME" in item
    )
    assert outcome == {
        "selected_engine": "islanddb",
        "actual_engine": "islanddb",
        "fallback": False,
    }


def test_stream_holds_whole_file_cache_lease_until_closed(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    class Metrics:
        coverage_ratio = 1.0

        @staticmethod
        def to_plan_stats():
            return {"FILE_CACHE_COVERAGE": 1.0}

        @staticmethod
        def as_dict():
            return {"coverage_ratio": 1.0}

    class Cache:
        source_is_local = True

        def __init__(self):
            self.active = 0

        @contextmanager
        def localized(self, reflection, *, populate, **_kwargs):
            assert populate is True
            self.active += 1
            try:
                yield reflection, Metrics()
            finally:
                self.active -= 1

    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=False,
        ),
    )
    monkeypatch.setattr(
        executor_module, "_reflection_has_remote_paths", lambda _reflection: True,
    )
    cache = Cache()
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    executor._file_cache = cache
    query = "SELECT id FROM s.t WHERE id >= 98 ORDER BY id"
    stream, used = executor.execute_stream(
        Engine.ISLANDDB,
        numeric_reflection,
        SQLParser("s", query, "duckdb"),
        QueryPlanManager("s", "island-tests", "", query),
        Timer(),
        PlanStats(),
        "",
    )

    assert used == "islanddb"
    assert cache.active == 1
    with stream:
        result = stream.collect_table(max_bytes=1024 * 1024)
        assert result["id"].to_pylist() == [98, 99]
    assert cache.active == 0


@pytest.mark.parametrize("count_projection", ["count(*) AS n", "count(*)"])
def test_forced_external_group_and_sort_spill_matches_duckdb(
    tmp_path, numeric_reflection, monkeypatch, count_projection,
):
    query = (
        f"SELECT id, {count_projection} FROM s.t "
        "GROUP BY id ORDER BY id"
    )
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    engine = IslandDB()
    engine._spill_root = tmp_path / "LOCAL_SPILL_PATH_TOKEN" / "spill"
    manager.query_plan_path = str(tmp_path / "island-profile.json")
    forced = QueryResourcePlan(
        advice=ExecutionAdvice.ISLAND_SPILL,
        cpu_workers=2,
        io_workers=2,
        batch_bytes=64 * 1024,
        batch_rows=16,
        memory_budget_bytes=4 * 1024 * 1024,
        scan_memory_bytes=1 * 1024 * 1024,
        operator_memory_bytes=512 * 1024,
        result_memory_bytes=1 * 1024 * 1024,
        spill_budget_bytes=16 * 1024 * 1024,
        estimated_spill_bytes=4 * 1024 * 1024,
        reason="forced spill regression",
    )
    monkeypatch.setattr(
        engine, "resource_plan",
        lambda reflection, parser, streaming_result: forced,
    )

    with engine.execute_stream(
        numeric_reflection, parser, manager, lambda _: None,
    ) as stream:
        result = stream.collect_table(max_bytes=2 * 1024 * 1024).to_pandas()
    expected = _run_duckdb(tmp_path, numeric_reflection, query)

    pd.testing.assert_frame_equal(result, expected)
    assert engine.last_profile.spill["triggered"] is True
    profile_doc = engine.last_profile.as_dict()
    rendered_profile = json.dumps(profile_doc, sort_keys=True)
    assert "LOCAL_SPILL_PATH_TOKEN" not in rendered_profile
    assert str(engine._spill_root) not in rendered_profile
    assert "directory" not in engine.last_profile.spill
    assert "directory_metadata" in engine.last_profile.spill
    assert "path_sha256=" in engine.last_profile.spill["directory_metadata"]
    persisted = (tmp_path / "island-profile.json").read_text(encoding="utf-8")
    assert "LOCAL_SPILL_PATH_TOKEN" not in persisted
    stats = PlanStats()
    Executor._publish_island_profile(stats, manager, "")
    assert "LOCAL_SPILL_PATH_TOKEN" not in repr(stats.stats)
    assert not list(engine._spill_root.glob("island-*"))


def test_spill_direct_arrow_projection_uses_sealed_schema_and_full_hints(
    numeric_reflection,
):
    import sqlglot

    snapshot = numeric_reflection.supers[0]
    selections = {}
    for path, resource_key in zip(snapshot.files, snapshot.resource_keys):
        metadata = pq.read_metadata(path)
        selections[resource_key] = RowGroupSelection(
            metadata.num_row_groups,
            tuple(range(metadata.num_row_groups)),
            parquet_footer_sha256(metadata),
        )
    snapshot = dataclasses.replace(snapshot, row_group_selections=selections)
    reflection = dataclasses.replace(numeric_reflection, supers=[snapshot])
    query = (
        "SELECT id, count(v) AS n FROM s.t "
        "GROUP BY id ORDER BY id"
    )
    parser = SQLParser("s", query, "duckdb")
    engine = IslandDB(range_cache=False)

    direct = engine._direct_local_projection_batches(
        reflection,
        parser,
        sqlglot.parse_one(query, read="duckdb"),
        column_names=["id", "v"],
        batch_rows=13,
    )

    assert direct is not None
    schema, batches, optimized_plan = direct
    table = pa.Table.from_batches(list(batches), schema=schema)
    assert schema == pa.schema([("id", pa.int64()), ("v", pa.int64())])
    assert table.num_rows == 100
    assert table.column("id").to_pylist() == list(range(100))
    assert "ARROW NATIVE DIRECT PROJECTION" in optimized_plan


def test_order_only_spill_uses_direct_scan_and_normalizes_rich_stream(
    tmp_path, monkeypatch, caplog,
):
    import supertable.engine.islanddb as island_module

    arrow_cast = island_module.pc.cast
    fixed_binary_casts = []

    def audited_cast(values, target_type=None, *args, **kwargs):
        if (
            isinstance(values, pa.Array)
            and pa.types.is_fixed_size_binary(values.type)
            and target_type == pa.binary()
        ):
            fixed_binary_casts.append((values.type, target_type))
        return arrow_cast(values, target_type, *args, **kwargs)

    monkeypatch.setattr(island_module.pc, "cast", audited_cast)
    path = tmp_path / "direct-rich-sort.parquet"
    values = [
        (3, datetime(2026, 1, 3), b"cccc"),
        (1, datetime(2026, 1, 1), b"aaaa"),
        (2, datetime(2026, 1, 2), b"bbbb"),
    ]
    pq.write_table(pa.table({
        "id": pa.array([value[0] for value in values], type=pa.int64()),
        "event_ts": pa.array(
            [value[1] for value in values], type=pa.timestamp("us"),
        ),
        "payload": pa.array(
            [value[2] for value in values], type=pa.binary(4),
        ),
        "__rowid__": pa.array([3, 1, 2], type=pa.int64()),
        "__timestamp__": pa.array([1, 1, 1], type=pa.int64()),
    }), path)
    snapshot = _snapshot(
        "t", [path], ["raw/direct-rich-sort.parquet"],
        types={
            "id": "Int64",
            "event_ts": "Datetime(time_unit='us', time_zone=None)",
            "payload": "Binary",
            "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    snapshot = dataclasses.replace(
        snapshot,
        integer_domain_bounds={"id": IntegerDomainBound(1, 3)},
    )
    reflection = _reflection(snapshot)
    query = "SELECT id, event_ts, payload FROM s.t ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "direct-rich-plan.json")
    engine = IslandDB(range_cache=False)
    engine._spill_root = tmp_path / "direct-rich-spill"
    forced = QueryResourcePlan(
        advice=ExecutionAdvice.ISLAND_SPILL,
        cpu_workers=2,
        io_workers=2,
        batch_bytes=64 * 1024,
        batch_rows=2,
        memory_budget_bytes=4 * 1024 * 1024,
        scan_memory_bytes=1 * 1024 * 1024,
        operator_memory_bytes=512 * 1024,
        result_memory_bytes=1 * 1024 * 1024,
        spill_budget_bytes=16 * 1024 * 1024,
        estimated_spill_bytes=4 * 1024 * 1024,
        reason="forced direct rich stream regression",
    )
    monkeypatch.setattr(
        engine, "resource_plan",
        lambda reflection, parser, streaming_result: forced,
    )
    caplog.set_level("DEBUG", logger="supertable.config.defaults")

    with engine.execute_stream(
        reflection, parser, manager, lambda _: None,
    ) as stream:
        result = stream.collect_table(max_bytes=1024 * 1024)

    assert result.schema == pa.schema([
        ("id", pa.int64()),
        ("event_ts", pa.timestamp("us")),
        ("payload", pa.binary()),
    ])
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("event_ts").to_pylist() == [
        datetime(2026, 1, 1),
        datetime(2026, 1, 2),
        datetime(2026, 1, 3),
    ]
    assert result.column("payload").to_pylist() == [b"aaaa", b"bbbb", b"cccc"]
    assert fixed_binary_casts == []
    assert "ARROW NATIVE DIRECT PROJECTION" in engine.last_profile.optimized_plan
    assert "ARROW NATIVE RANGE PARTITION SORT" in engine.last_profile.optimized_plan
    assert query not in caplog.text
    assert "executing direct Arrow projection sql_sha256=" in caplog.text
    assert engine.last_profile.spill["triggered"] is True
    assert engine.last_profile.spill_bytes > 0
    assert not list((tmp_path / "direct-rich-spill").glob("island-*"))


def test_spill_direct_arrow_projection_falls_back_on_semantic_uncertainty(
    numeric_reflection,
):
    import sqlglot

    snapshot = numeric_reflection.supers[0]
    metadata = pq.read_metadata(snapshot.files[0])
    partial_snapshot = dataclasses.replace(snapshot, row_group_selections={
        snapshot.resource_keys[0]: RowGroupSelection(
            metadata.num_row_groups,
            (0,),
            parquet_footer_sha256(metadata),
        ),
    })
    cases = [
        (
            dataclasses.replace(numeric_reflection, supers=[partial_snapshot]),
            "SELECT id, count(v) AS n FROM s.t GROUP BY id ORDER BY id",
        ),
        (
            numeric_reflection,
            "SELECT id, count(v) AS n FROM s.t "
            "WHERE v > 0 GROUP BY id ORDER BY id",
        ),
        (
            dataclasses.replace(numeric_reflection, rbac_views={"t": object()}),
            "SELECT id, count(v) AS n FROM s.t GROUP BY id ORDER BY id",
        ),
        (
            dataclasses.replace(
                numeric_reflection, tombstone_views={"t": object()},
            ),
            "SELECT id, count(v) AS n FROM s.t GROUP BY id ORDER BY id",
        ),
    ]
    engine = IslandDB(range_cache=False)

    for reflection, query in cases:
        parser = SQLParser("s", query, "duckdb")
        assert engine._direct_local_projection_batches(
            reflection,
            parser,
            sqlglot.parse_one(query, read="duckdb"),
            column_names=["id", "v"],
            batch_rows=16,
        ) is None


def test_eager_spill_scan_deadline_unwinds_and_reclaims_every_resource(
    tmp_path, numeric_reflection, monkeypatch,
):
    """A pre-stream scan cannot run forever waiting for client cancellation."""
    import supertable.engine.islanddb as island_module

    query = "SELECT id, count(*) AS n FROM s.t GROUP BY id ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "deadline-plan.json")
    engine = IslandDB()
    engine._spill_root = tmp_path / "deadline-spill"
    forced = QueryResourcePlan(
        advice=ExecutionAdvice.ISLAND_SPILL,
        cpu_workers=2,
        io_workers=2,
        batch_bytes=64 * 1024,
        batch_rows=16,
        memory_budget_bytes=4 * 1024 * 1024,
        scan_memory_bytes=1 * 1024 * 1024,
        operator_memory_bytes=512 * 1024,
        result_memory_bytes=1 * 1024 * 1024,
        spill_budget_bytes=16 * 1024 * 1024,
        estimated_spill_bytes=4 * 1024 * 1024,
        reason="forced deadline regression",
    )
    monkeypatch.setattr(
        engine, "resource_plan",
        lambda reflection, parser, streaming_result: forced,
    )
    monkeypatch.setattr(
        island_module,
        "settings",
        dataclasses.replace(
            island_module.settings,
            SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC=1.0,
        ),
    )
    clock = [100.0]
    monkeypatch.setattr(island_module, "_monotonic", lambda: clock[0])
    input_schema = pa.schema([("id", pa.int64())])
    input_closed = False

    def crosses_deadline_before_first_batch():
        nonlocal input_closed
        try:
            clock[0] = 102.0
            yield pa.record_batch([[1, 2]], schema=input_schema)
        finally:
            input_closed = True

    monkeypatch.setattr(
        engine,
        "_direct_local_projection_batches",
        lambda *args, **kwargs: (
            input_schema,
            crosses_deadline_before_first_batch(),
            "test direct scan",
        ),
    )
    active_before = engine._governor.snapshot()["active_queries"]

    with pytest.raises(IslandExecutionTimeout, match="timed out after 1 seconds"):
        # external_group_aggregate eagerly consumes its input before returning
        # ArrowBatchStream, so this proves a client handle is not required.
        engine.execute_stream(
            numeric_reflection, parser, manager, lambda _: None,
        )

    assert input_closed is True
    assert engine._governor.snapshot()["active_queries"] == active_before
    assert not list((tmp_path / "deadline-spill").glob("island-*"))
    assert island_module._ISLAND_EXECUTION_SLOT.acquire(blocking=False)
    island_module._ISLAND_EXECUTION_SLOT.release()
    assert island_module._ARROW_POOL_LOCK.acquire(blocking=False)
    island_module._ARROW_POOL_LOCK.release()


class _CachePreflightSpy:
    source_is_local = False

    def __init__(self, can_populate=True):
        self.can_populate = can_populate
        self.entered = 0

    def can_populate_all(self, reflection):
        return self.can_populate

    @contextmanager
    def localized(self, *args, **kwargs):
        self.entered += 1
        raise AssertionError("cache localization must not start")
        yield  # pragma: no cover


def test_explicit_unsupported_query_rejects_before_cache_io(
    tmp_path, numeric_reflection,
):
    query = "SELECT id FROM s.t WHERE label='r1'"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    cache = _CachePreflightSpy()
    executor._file_cache = cache

    with pytest.raises(IslandUnsupportedError) as caught:
        executor.execute(
            Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(),
            PlanStats(), "",
        )

    assert re.fullmatch(
        r"IslandDB materialized preparation failed; "
        r"error_type=IslandUnsupportedError; "
        r"diagnostic_id=[0-9a-f]{16}; diagnostic_bytes=[0-9]+",
        str(caught.value),
    )
    assert "NOCASE" not in str(caught.value)
    assert cache.entered == 0


def test_explicit_range_island_uses_whole_file_cache_hit_only_without_admission(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    class Metrics:
        coverage_ratio = 0.0

        @staticmethod
        def to_plan_stats():
            return {"FILE_CACHE_COVERAGE": 0.0}

        @staticmethod
        def as_dict():
            return {"coverage_ratio": 0.0}

    class HitOnlyCache(_CachePreflightSpy):
        @contextmanager
        def localized(self, reflection, *, populate, **_kwargs):
            assert populate is False
            self.entered += 1
            yield reflection, Metrics()

    query = "SELECT id FROM s.t WHERE id=1"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    cache = HitOnlyCache(can_populate=False)
    executor._file_cache = cache
    monkeypatch.setattr(
        executor_module, "_reflection_has_remote_paths", lambda _reflection: True,
    )

    result, used = executor.execute(
        Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(),
        PlanStats(), "",
    )

    # Range mode never asks the whole-object tier to populate, but consumes any
    # complete immutable hit under an eviction lease before falling back to
    # selective ranges for misses.
    assert cache.entered == 1
    assert used == "islanddb"
    assert result["id"].tolist() == [1]


def test_streaming_range_island_uses_whole_file_cache_hit_only_for_stream_lifetime(
    tmp_path, numeric_reflection, monkeypatch,
):
    import supertable.engine.executor as executor_module

    class Metrics:
        coverage_ratio = 0.0

        @staticmethod
        def to_plan_stats():
            return {"FILE_CACHE_COVERAGE": 0.0}

        @staticmethod
        def as_dict():
            return {"coverage_ratio": 0.0}

    class HitOnlyCache(_CachePreflightSpy):
        source_is_local = False

        def __init__(self):
            super().__init__(can_populate=False)
            self.active = 0

        @contextmanager
        def localized(
            self, reflection, *, populate, tolerate_corrupt_hits=False,
        ):
            assert populate is False
            assert tolerate_corrupt_hits is True
            self.entered += 1
            self.active += 1
            try:
                yield reflection, Metrics()
            finally:
                self.active -= 1

    query = "SELECT id FROM s.t WHERE id >= 98 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    cache = HitOnlyCache()
    executor._file_cache = cache
    monkeypatch.setattr(
        executor_module, "_reflection_has_remote_paths", lambda _reflection: True,
    )

    stream, used = executor.execute_stream(
        Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(),
        PlanStats(), "",
    )

    assert used == "islanddb"
    assert cache.entered == 1
    assert cache.active == 1
    with stream:
        assert stream.collect_table(max_bytes=1024 * 1024)["id"].to_pylist() == [98, 99]
    assert cache.active == 0


class _RemoteParquet:
    def __init__(self, key: str, payload: bytes):
        self.key = key
        self.payload = payload
        self.downloads = 0

    def cache_namespace(self):
        return {"provider": "test-remote", "bucket": "island"}

    def is_local_storage(self):
        return False

    def stat_object(self, key):
        assert key == self.key
        return ObjectMetadata(size=len(self.payload), version="immutable-v1")

    def download_to_file(self, key, sink, *, expected=None, chunk_size=1024):
        assert key == self.key
        self.downloads += 1
        return write_all(sink, self.payload)

    def read_range(self, key, offset, length, *, expected=None):
        assert key == self.key
        if expected is not None:
            assert expected == self.stat_object(key)
        self.downloads += 1
        return self.payload[offset:offset + length]


class _SealedRemoteParquet(_RemoteParquet):
    def __init__(self, key: str, payload: bytes):
        super().__init__(key, payload)
        self.stats = 0
        self.expected_reads = []

    def stat_object(self, key):
        self.stats += 1
        return super().stat_object(key)

    def read_range(self, key, offset, length, *, expected=None):
        self.expected_reads.append(expected)
        # Do not call stat here: a provider uses this expected version/ETag as
        # the conditional GET authority, which is the behavior under test.
        assert key == self.key
        assert expected is not None
        assert expected.version == "immutable-v1"
        self.downloads += 1
        return self.payload[offset:offset + length]


def _remote_snapshot(storage, resolved_path, *, sealed):
    kwargs = {}
    if sealed:
        kwargs["resource_object_seals"] = {
            storage.key: ResourceObjectSeal(
                size=len(storage.payload), version="immutable-v1",
            ),
        }
    return SuperSnapshot(
        "s", "t", 1, [resolved_path],
        {"id", "v", "__rowid__", "__timestamp__"},
        resource_keys=[storage.key], resource_sizes=[len(storage.payload)],
        column_types={
            "id": "Int64", "v": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        }, snapshot_resource_keys=[storage.key],
        candidate_rows=3, candidate_rows_complete=True,
        **kwargs,
    )


def test_remote_snapshot_object_seal_elides_stat_and_pins_ranges(
    tmp_path, monkeypatch,
):
    from supertable.engine.range_cache import RangeCache

    local = tmp_path / "sealed-source.parquet"
    pl.DataFrame({
        "id": [1, 2, 3], "v": [10, 20, 30],
        "__rowid__": [1, 2, 3], "__timestamp__": [1, 1, 1],
    }).write_parquet(local)
    storage = _SealedRemoteParquet("raw/sealed-source.parquet", local.read_bytes())
    snapshot = _remote_snapshot(
        storage, "https://remote.invalid/sealed-source.parquet", sealed=True,
    )
    cache = RangeCache(
        storage, "sealed-object-test", root=str(tmp_path / "ranges"),
        max_bytes=10 * 1024 * 1024, ttl=0,
    )
    engine = IslandDB(
        storage=storage, organization="sealed-object-test", range_cache=cache,
    )

    result = engine._base_relation(snapshot).select(["id", "v"]).collect()

    assert result.sort("id").to_dict(as_series=False) == {
        "id": [1, 2, 3], "v": [10, 20, 30],
    }
    assert storage.stats == 0
    assert storage.expected_reads
    assert all(
        expected == ObjectMetadata(
            size=len(storage.payload), version="immutable-v1",
        )
        for expected in storage.expected_reads
    )


def test_remote_legacy_snapshot_still_stats_object(tmp_path):
    from supertable.engine.range_cache import RangeCache

    local = tmp_path / "legacy-source.parquet"
    pl.DataFrame({
        "id": [1, 2, 3], "v": [10, 20, 30],
        "__rowid__": [1, 2, 3], "__timestamp__": [1, 1, 1],
    }).write_parquet(local)
    storage = _SealedRemoteParquet("raw/legacy-source.parquet", local.read_bytes())
    snapshot = _remote_snapshot(
        storage, "https://remote.invalid/legacy-source.parquet", sealed=False,
    )
    cache = RangeCache(
        storage, "legacy-object-test", root=str(tmp_path / "legacy-ranges"),
        max_bytes=10 * 1024 * 1024, ttl=0,
    )

    IslandDB(
        storage=storage, organization="legacy-object-test", range_cache=cache,
    )._base_relation(snapshot).select("id").collect()

    assert storage.stats == 1


def test_provider_object_seal_is_ignored_after_whole_file_localization(tmp_path):
    local = tmp_path / "localized-source.parquet"
    pl.DataFrame({
        "id": [1, 2, 3], "v": [10, 20, 30],
        "__rowid__": [1, 2, 3], "__timestamp__": [1, 1, 1],
    }).write_parquet(local)
    storage = _SealedRemoteParquet("raw/localized-source.parquet", local.read_bytes())
    snapshot = _remote_snapshot(storage, str(local), sealed=True)

    result = IslandDB(storage=storage)._base_relation(snapshot).select("id").collect()

    assert result["id"].to_list() == [1, 2, 3]
    assert storage.stats == 0


def test_island_range_reads_remote_file_and_matches_duckdb(
    tmp_path, monkeypatch,
):
    import supertable.engine.executor as executor_module

    local = tmp_path / "source.parquet"
    pl.DataFrame({
        "id": [1, 2, 3], "v": [10, 20, 30],
        "__rowid__": [1, 2, 3], "__timestamp__": [1, 1, 1],
    }).write_parquet(local)
    storage = _RemoteParquet("raw/source.parquet", local.read_bytes())
    snapshot = SuperSnapshot(
        "s", "t", 1, ["https://unreachable.invalid/source.parquet"],
        {"id", "v", "__rowid__", "__timestamp__"},
        resource_keys=[storage.key], resource_sizes=[len(storage.payload)],
        column_types={
            "id": "Int64", "v": "Int64", "__rowid__": "Int64",
            "__timestamp__": "Int64",
        }, snapshot_resource_keys=[storage.key],
        candidate_rows=3, candidate_rows_complete=True,
    )
    reflection = Reflection(
        "Remote", len(storage.payload), 1, [snapshot],
        source_bytes=len(storage.payload),
        row_group_scan_bytes=len(storage.payload),
        row_group_scan_bytes_complete=True,
        decoded_bytes=len(storage.payload) * 4,
        decoded_bytes_complete=True,
    )
    clone = dataclasses.replace(
        executor_module.settings,
        SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "shared"),
        SUPERTABLE_ISLAND_CACHE_MAX_BYTES=10 * 1024 * 1024,
        SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "ranges"),
    )
    monkeypatch.setattr(executor_module, "settings", clone)
    query = "SELECT id, v FROM s.t WHERE id >= 2 ORDER BY id"

    def execute(engine):
        parser = SQLParser("s", query, "duckdb")
        manager = QueryPlanManager("s", "shared-cache-test", "", query)
        manager.temp_dir = str(tmp_path)
        manager.query_plan_path = str(tmp_path / f"{engine.value}.json")
        return Executor(storage=storage, organization="shared-cache-test").execute(
            engine, reflection, parser, manager, Timer(), PlanStats(), "",
        )[0]

    island = execute(Engine.ISLANDDB)
    first_requests = storage.downloads
    island_warm = execute(Engine.ISLANDDB)
    pd.testing.assert_frame_equal(island, island_warm)
    assert first_requests > 0
    assert storage.downloads == first_requests


def test_range_cache_registry_reuses_process_indexes_and_is_bounded(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    storage = _RemoteParquet("raw/source.parquet", b"parquet-placeholder")
    settings_clone = dataclasses.replace(
        island_module.settings,
        SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
        SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "ranges"),
        SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES=1024 * 1024,
        SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC=0,
    )
    monkeypatch.setattr(island_module, "settings", settings_clone)
    monkeypatch.setattr(island_module, "_RANGE_CACHE_REGISTRY_MAX_ENTRIES", 1)
    island_module._clear_range_cache_registry()

    first = IslandDB(storage=storage, organization="org")
    second = IslandDB(storage=storage, organization="org")
    assert first._get_range_cache() is second._get_range_cache()

    third = IslandDB(storage=storage, organization="other-org")
    third_cache = third._get_range_cache()
    assert third_cache is not first.range_cache
    assert len(island_module._RANGE_CACHE_REGISTRY) == 1

    # Registry eviction only drops the shared index reference; an engine/live
    # reader that already owns the old cache remains valid.
    assert first._get_range_cache() is first.range_cache


def test_live_island_range_cache_capacity_rebuilds_for_next_query(
    tmp_path, monkeypatch,
):
    import supertable.engine.islanddb as island_module

    storage = _RemoteParquet("raw/source.parquet", b"parquet-placeholder")
    monkeypatch.setattr(
        island_module,
        "settings",
        dataclasses.replace(
            island_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "ranges"),
        ),
    )
    island_module._clear_range_cache_registry()
    engine = IslandDB(storage=storage, organization="live-config")
    first_config = IslandRuntimeConfig(None, None, None, 1024)
    second_config = dataclasses.replace(
        first_config, range_cache_max_bytes=2048,
    )

    first = engine._get_range_cache(first_config)
    second = engine._get_range_cache(second_config)

    assert first.max_bytes == 1024
    assert second.max_bytes == 2048
    assert second is not first


def test_unconfigured_range_cache_uses_stable_filesystem_budget(
    tmp_path, monkeypatch,
):
    import supertable.engine.file_cache as file_cache_module
    import supertable.engine.islanddb as island_module

    budgets = iter((2345, 6789))
    monkeypatch.setattr(
        file_cache_module,
        "automatic_cache_max_bytes",
        lambda *_args, **_kwargs: next(budgets),
    )
    monkeypatch.setattr(
        island_module,
        "settings",
        dataclasses.replace(
            island_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "ranges-auto"),
            SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES=0,
        ),
    )
    island_module._clear_range_cache_registry()
    storage = _RemoteParquet("raw/source.parquet", b"parquet-placeholder")
    engine = IslandDB(storage=storage, organization="auto-cache")

    first = engine._get_range_cache()
    second = engine._get_range_cache()

    assert first is second
    assert first.max_bytes == 2345
    assert first._supertable_automatic_capacity is True


def test_live_automatic_range_cache_overrides_positive_process_fallback(
    tmp_path, monkeypatch,
):
    import supertable.engine.file_cache as file_cache_module
    import supertable.engine.islanddb as island_module

    monkeypatch.setattr(
        file_cache_module,
        "automatic_cache_max_bytes",
        lambda *_args, **_kwargs: 5432,
    )
    monkeypatch.setattr(
        island_module,
        "settings",
        dataclasses.replace(
            island_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "range-live-auto"),
            SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES=23456,
        ),
    )
    island_module._clear_range_cache_registry()
    engine = IslandDB(
        storage=_RemoteParquet("raw/source.parquet", b"placeholder"),
        organization="live-auto-cache",
    )

    cache = engine._get_range_cache(
        IslandRuntimeConfig(None, None, None, None),
    )

    assert cache.max_bytes == 5432
    assert cache._supertable_automatic_capacity is True


def test_automatic_range_cache_rebuilds_when_peer_live_limit_changes(
    tmp_path, monkeypatch,
):
    import supertable.engine.file_cache as file_cache_module
    import supertable.engine.islanddb as island_module

    peer_limits = []
    budgets = iter((8192, 2048))

    def automatic_budget(*_args, **kwargs):
        peer_limits.append(tuple(kwargs["peer_limits"]))
        return next(budgets)

    monkeypatch.setattr(
        file_cache_module, "automatic_cache_max_bytes", automatic_budget,
    )
    monkeypatch.setattr(
        island_module,
        "settings",
        dataclasses.replace(
            island_module.settings,
            SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_RANGE_CACHE_DIR=str(tmp_path / "range-peer-live"),
            SUPERTABLE_ISLAND_CACHE_ENABLED=True,
            SUPERTABLE_ISLAND_CACHE_DIR=str(tmp_path / "whole-peer-live"),
        ),
    )
    island_module._clear_range_cache_registry()
    engine = IslandDB(
        storage=_RemoteParquet("raw/source.parquet", b"placeholder"),
        organization="peer-live",
    )
    automatic = IslandRuntimeConfig(None, None, None, None)

    first = engine._get_range_cache(automatic)
    second = engine._get_range_cache(dataclasses.replace(
        automatic, cache_max_bytes=4096,
    ))

    assert first.max_bytes == 8192
    assert second.max_bytes == 2048
    assert second is not first
    assert peer_limits[0][0][1] == 0
    assert peer_limits[1][0][1] == 4096
