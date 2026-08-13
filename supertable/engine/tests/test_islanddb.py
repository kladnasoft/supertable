from __future__ import annotations

import os
import dataclasses
from contextlib import contextmanager

import pandas as pd
import polars as pl
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_classes import (
    RbacViewDef,
    Reflection,
    RowGroupSelection,
    SuperSnapshot,
    TombstoneDef,
)
from supertable.engine.duckdb_lite import DuckDBLite
from supertable.engine.engine_common import SOURCE_FILE_COL
from supertable.engine.engine_config import resolve_engine_config
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor
from supertable.engine.islanddb import (
    IslandDB,
    IslandIntegrityError,
    IslandUnsupportedError,
)
from supertable.engine.island_resources import (
    ContainerResources,
    ExecutionAdvice,
    QueryResourcePlan,
    ResourcePolicy,
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
    return DuckDBLite().execute(
        reflection, parser, manager, lambda _: None,
        engine_config=resolve_engine_config("", None, "lite"),
    )


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
    reflection = _reflection(snapshot)
    reflection.row_group_scan_bytes = os.path.getsize(path)
    reflection.row_group_scan_bytes_complete = True
    reflection.decoded_bytes = 4096
    reflection.decoded_bytes_complete = True
    query = "SELECT id, v FROM s.t WHERE id BETWEEN 23 AND 25 ORDER BY id"

    expected = _run_duckdb(tmp_path, reflection, query)
    actual, engine = _run_island(tmp_path, reflection, query)

    pd.testing.assert_frame_equal(actual, expected)
    assert engine.last_profile.selected_row_groups == 1


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
    assert engine.last_profile.peak_memory_scope == "process_rss_delta"
    assert engine.last_profile.spill_bytes == 0
    assert engine.last_profile.spill_bytes_measured is False
    # Native multi-file execution is one Arrow Dataset scan.  Polars labels
    # that bridge PYTHON SCAN while still pushing projection/predicate into the
    # Arrow scanner (both are visible in the optimized plan).
    assert "SCAN" in engine.last_profile.optimized_plan


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


def test_range_cache_initialization_failure_releases_governor(tmp_path, numeric_reflection, monkeypatch):
    query = "SELECT count(*) AS n FROM s.t WHERE id >= 10"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    manager.query_plan_path = str(tmp_path / "cache-failure-plan.json")
    engine = IslandDB()
    before = engine._governor.snapshot()["active_queries"]
    monkeypatch.setattr(
        engine, "_get_range_cache",
        lambda: (_ for _ in ()).throw(OSError("cache directory unavailable")),
    )

    with pytest.raises(OSError, match="cache directory unavailable"):
        engine.execute_stream(
            numeric_reflection, parser, manager, lambda _: None,
        )

    assert engine._governor.snapshot()["active_queries"] == before


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
    policy = ResourcePolicy()
    monkeypatch.setattr(
        IslandDB, "_detect_resources", staticmethod(lambda: next(samples)),
    )
    monkeypatch.setattr(
        IslandDB, "_resource_policy", staticmethod(lambda: policy),
    )
    monkeypatch.setattr(
        islanddb_module,
        "settings",
        dataclasses.replace(
            islanddb_module.settings,
            SUPERTABLE_ISLAND_SPILL_DIR=str(tmp_path / "shared-governor-spill"),
        ),
    )

    first = IslandDB()
    second = IslandDB()

    assert first._governor is second._governor
    assert second._governor.snapshot()["memory_capacity"] == int(
        256 * 1024**2 * policy.global_memory_fraction
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
    ("SELECT label=label AS same FROM s.t", "non-numeric semantics"),
    ("SELECT id FROM s.t LIMIT 1", "LIMIT/OFFSET"),
    ("SELECT id FROM s.t ORDER BY id LIMIT 1", "LIMIT/OFFSET"),
    ("SELECT id FROM s.t ORDER BY id DESC", "DESC/NULLS FIRST"),
    ("SELECT -id AS negative_id FROM s.t", "signed expression"),
    ("SELECT avg(v) FROM s.t", "AVG reduction"),
    ("SELECT sum(-v) AS total FROM s.t", "signed expression"),
    ("SELECT sum(1) AS total FROM s.t", "one direct column"),
    ("SELECT count(*) FROM s.t", "explicit alias"),
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


def test_response_limit_is_native_when_candidate_count_proves_it_redundant(
    numeric_reflection,
):
    parser = SQLParser("s", "SELECT id FROM s.t LIMIT 10000", "duckdb")

    capability = IslandDB().can_execute(numeric_reflection, parser)

    assert capability.supported, capability.reasons


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
    ("SELECT a.id AS aid FROM s.a a NATURAL JOIN s.b b", "NATURAL"),
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


def test_public_executor_arrow_stream_is_batched_and_exact(
    tmp_path, numeric_reflection,
):
    query = "SELECT id, v FROM s.t WHERE id >= 95 ORDER BY id"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
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
        def localized(self, reflection, *, populate):
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


def test_forced_external_group_and_sort_spill_matches_duckdb(
    tmp_path, numeric_reflection, monkeypatch,
):
    query = (
        "SELECT id, count(*) AS n FROM s.t "
        "GROUP BY id ORDER BY id"
    )
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    engine = IslandDB()
    engine._spill_root = tmp_path / "spill"
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
    assert not list((tmp_path / "spill").glob("island-*"))


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

    with pytest.raises(IslandUnsupportedError, match="NOCASE"):
        executor.execute(
            Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(),
            PlanStats(), "",
        )

    assert cache.entered == 0


def test_explicit_island_does_not_require_whole_file_cache_admission(
    tmp_path, numeric_reflection,
):
    query = "SELECT id FROM s.t WHERE id=1"
    parser = SQLParser("s", query, "duckdb")
    manager = QueryPlanManager("s", "island-tests", "", query)
    executor = Executor(storage=LocalStorage(), organization="island-tests")
    cache = _CachePreflightSpy(can_populate=False)
    executor._file_cache = cache

    result, used = executor.execute(
        Engine.ISLANDDB, numeric_reflection, parser, manager, Timer(),
        PlanStats(), "",
    )

    assert cache.entered == 0
    assert used == "islanddb"
    assert result["id"].tolist() == [1]


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
