"""Adversarial regressions for executor isolation and deletion vectors."""

from __future__ import annotations

from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor
import threading
import base64
import hashlib
import gc
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest

from supertable.data_classes import Reflection, SuperSnapshot, TombstoneDef
from supertable.engine.duckdb_engine import DuckDB
from supertable.engine import duckdb_engine as duckdb_engine_module


from supertable.engine import executor as executor_module
from supertable.engine import engine_common as engine_common_module
from supertable.engine.engine_enum import Engine
from supertable.engine.engine_common import (
    TombstoneCache,
    ValidatedTombstoneTable,
    configure_httpfs_and_s3,
    create_reflection_view,
    create_tombstone_view,
    create_typed_empty_view,
    snapshot_duckdb_type,
    snapshot_spark_type,
    validate_tombstone_relation,
)
from supertable.processing import tombstone_digest
from supertable.engine.spark_thrift import (
    _resolve_spark_file,
    _spark_create_empty_view,
    _spark_create_tombstone_view,
    _to_s3a_path,
)
from supertable.utils.sql_parser import SQLParser


def _write_source(con, path, *, ident: int, rowid: int, internal=False):
    extra = ", 'attacker'::VARCHAR AS __supertable_source_file__" if internal else ""
    con.execute(
        f"COPY (SELECT {ident}::INTEGER AS id, {rowid}::BIGINT AS __rowid__, "
        f"1::BIGINT AS __timestamp__{extra}) TO ? (FORMAT PARQUET)", [str(path)],
    )


def _write_dv(con, path, rows):
    values = ",".join(
        "('" + str(file_key).replace("'", "''") + f"',{int(rowid)})"
        for file_key, rowid in rows
    )
    con.execute(
        "COPY (SELECT CAST(__file__ AS VARCHAR) AS __file__, "
        "CAST(__rowid__ AS BIGINT) AS __rowid__ FROM (VALUES "
        f"{values}) AS t(__file__, __rowid__)) TO ? (FORMAT PARQUET)", [str(path)],
    )


def _dv_digest(rows):
    records = sorted(
        (
            base64.b64encode(str(file_key).encode("utf-8")).decode("ascii"),
            int(rowid),
        )
        for file_key, rowid in rows
    )
    payload = b"supertable-tombstone-v1\n" + b"\n".join(
        f"{encoded}:{rowid:016x}".encode("ascii")
        for encoded, rowid in records
    )
    return hashlib.sha256(payload).hexdigest()


def test_duckdb_composite_tombstone_does_not_delete_same_rowid_in_other_file(tmp_path):
    con = duckdb.connect()
    a, b, dv = tmp_path / "a.parquet", tmp_path / "b.parquet", tmp_path / "dv.parquet"
    _write_source(con, a, ident=1, rowid=7)
    _write_source(con, b, ident=2, rowid=7)
    _write_dv(con, dv, [("raw/a", 7)])

    create_reflection_view(
        con, "src", [str(a), str(b)], resource_keys=["raw/a", "raw/b"],
    )
    tomb = TombstoneDef(
        str(dv), "raw/dv", 1, resource_keys=("raw/a", "raw/b"),
    )
    create_tombstone_view(con, "src", "live", tomb)

    assert con.execute("SELECT id FROM live ORDER BY id").fetchall() == [(2,)]


def test_reflection_rejects_noninjective_resolved_file_identity(tmp_path):
    con = duckdb.connect()
    src = tmp_path / "same.parquet"
    _write_source(con, src, ident=1, rowid=7)

    with pytest.raises(RuntimeError, match="multiple stable resource keys"):
        create_reflection_view(
            con,
            "src",
            [str(src), str(src)],
            resource_keys=["raw/key-a", "raw/key-b"],
        )


@pytest.mark.parametrize("cached", [False, True])
def test_composite_file_identity_is_binary_under_nocase_collation(
        tmp_path, cached,
):
    con = duckdb.connect()
    con.execute("SET default_collation='nocase'")
    upper = tmp_path / "upper.parquet"
    lower = tmp_path / "lower.parquet"
    dv = tmp_path / "dv.parquet"
    _write_source(con, upper, ident=1, rowid=7)
    _write_source(con, lower, ident=2, rowid=7)
    _write_dv(con, dv, [("raw/Foo.parquet", 7)])
    keys = ("raw/Foo.parquet", "raw/foo.parquet")
    create_reflection_view(
        con, "src", [str(upper), str(lower)], resource_keys=list(keys),
    )
    tomb = TombstoneDef(str(dv), "raw/dv", 1, resource_keys=keys)
    cached_table = None
    if cached:
        con.execute("CREATE TABLE cached_dv AS SELECT * FROM read_parquet(?)", [str(dv)])
        cached_table = ValidatedTombstoneTable(
            "cached_dv", row_count=1, referenced_files={"raw/Foo.parquet"},
        )

    create_tombstone_view(con, "src", "live", tomb, dv_table=cached_table)

    assert con.execute("SELECT id FROM live ORDER BY id").fetchall() == [(2,)]


def test_scan_filename_mapping_is_binary_under_nocase_collation(tmp_path):
    con = duckdb.connect()
    con.execute("SET default_collation='nocase'")
    upper = tmp_path / "Foo.parquet"
    lower = tmp_path / "foo.parquet"
    dv = tmp_path / "dv-lower.parquet"
    _write_source(con, upper, ident=1, rowid=1)
    _write_source(con, lower, ident=2, rowid=2)
    _write_dv(con, dv, [("raw/foo.parquet", 2)])
    keys = ("raw/Foo.parquet", "raw/foo.parquet")
    create_reflection_view(
        con, "src_case", [str(upper), str(lower)], resource_keys=list(keys),
    )
    tomb = TombstoneDef(str(dv), "raw/dv", 1, resource_keys=keys)

    create_tombstone_view(con, "src_case", "live_case", tomb)

    assert con.execute("SELECT id FROM live_case ORDER BY id").fetchall() == [(1,)]


def test_physical_internal_column_cannot_spoof_composite_identity(tmp_path):
    con = duckdb.connect()
    src, dv = tmp_path / "src.parquet", tmp_path / "dv.parquet"
    _write_source(con, src, ident=1, rowid=7, internal=True)
    _write_dv(con, dv, [("raw/src", 7)])
    create_reflection_view(con, "src", [str(src)], resource_keys=["raw/src"])
    tomb = TombstoneDef(str(dv), "raw/dv", 1, resource_keys=("raw/src",))
    create_tombstone_view(con, "src", "live", tomb)
    assert con.execute("SELECT * FROM live").fetchall() == []


def test_dv_foreign_file_and_pinned_count_fail_closed(tmp_path):
    con = duckdb.connect()
    src, dv = tmp_path / "src.parquet", tmp_path / "dv.parquet"
    _write_source(con, src, ident=1, rowid=7)
    _write_dv(con, dv, [("foreign/file", 7)])
    create_reflection_view(con, "src", [str(src)], resource_keys=["raw/src"])

    foreign = TombstoneDef(str(dv), "raw/dv", 1, resource_keys=("raw/src",))
    with pytest.raises(RuntimeError, match="outside the pinned"):
        create_tombstone_view(con, "src", "live", foreign)

    wrong_count = TombstoneDef(
        str(dv), "raw/dv", 2, resource_keys=("raw/src",),
    )
    with pytest.raises(RuntimeError, match="expected 2, got 1"):
        create_tombstone_view(con, "src", "live2", wrong_count)


@pytest.mark.parametrize("bad_rowid", [0, -1])
def test_nonpositive_dv_rowid_fails_closed(tmp_path, bad_rowid):
    con = duckdb.connect()
    src, dv = tmp_path / "src.parquet", tmp_path / "dv.parquet"
    _write_source(con, src, ident=1, rowid=7)
    _write_dv(con, dv, [("raw/src", bad_rowid)])
    create_reflection_view(con, "src", [str(src)], resource_keys=["raw/src"])
    tomb = TombstoneDef(
        str(dv), "raw/dv", 1, resource_keys=("raw/src",),
    )
    with pytest.raises(RuntimeError, match="must be positive"):
        create_tombstone_view(con, "src", "live", tomb)


def test_same_count_dv_substitution_is_rejected_by_pinned_digest(tmp_path):
    con = duckdb.connect()
    src, dv = tmp_path / "src.parquet", tmp_path / "dv.parquet"
    _write_source(con, src, ident=1, rowid=7)
    _write_dv(con, dv, [("raw/src", 8)])  # substituted row, same cardinality
    create_reflection_view(con, "src", [str(src)], resource_keys=["raw/src"])
    tomb = TombstoneDef(
        str(dv), "raw/dv", 1,
        tombstone_digest=_dv_digest([("raw/src", 7)]),
        resource_keys=("raw/src",), snapshot_resource_keys=("raw/src",),
    )
    with pytest.raises(RuntimeError, match="digest"):
        create_tombstone_view(con, "src", "live", tomb)


def test_processing_tombstone_seal_matches_duckdb_reader_validation(tmp_path):
    """Writer and reader must hash exactly the same logical DV row stream."""
    files = [
        "raw/ž-file/" + "segment-" * 20,
        "raw/a file/" + "another-long-segment-" * 12,
        "raw/z-file/" + "third-long-segment-" * 12,
        "raw/b-file/" + "fourth-long-segment-" * 12,
    ]
    frame = pl.DataFrame(
        {
            "__file__": [
                files[2], files[0], files[0], files[2], files[2],
                files[1], files[1], files[3], files[3],
            ],
            "__rowid__": [41, 36, 37, 44, 45, 61, 62, 56, 57],
        },
        schema={"__file__": pl.String, "__rowid__": pl.Int64},
    )
    path = tmp_path / "sealed-dv.parquet"
    frame.write_parquet(path)
    expected = tombstone_digest(frame)
    con = duckdb.connect()
    con.execute("SET threads=8")
    escaped_path = str(path).replace("'", "''")
    parquet_relation = (
        f"read_parquet('{escaped_path}', hive_partitioning=false)"
    )
    con.execute(
        f"CREATE TABLE cached_dv AS SELECT __file__, __rowid__ "
        f"FROM {parquet_relation}"
    )

    for relation in (parquet_relation, "cached_dv"):
        count, actual = validate_tombstone_relation(
            con,
            relation,
            expected_rows=9,
            expected_digest=expected,
            allowed_files=frame.get_column("__file__").to_list(),
        )
        assert count == 9
        assert actual == expected


def test_dv_entry_for_pruned_file_is_valid_against_full_snapshot(tmp_path):
    con = duckdb.connect()
    selected, dv = tmp_path / "selected.parquet", tmp_path / "dv.parquet"
    _write_source(con, selected, ident=1, rowid=7)
    _write_dv(con, dv, [("raw/pruned", 9)])
    create_reflection_view(
        con, "src", [str(selected)], resource_keys=["raw/selected"],
    )
    tomb = TombstoneDef(
        tombstone_path=str(dv), cache_key="raw/dv", expected_rows=1,
        resource_keys=("raw/selected",),
        snapshot_resource_keys=("raw/selected", "raw/pruned"),
    )
    create_tombstone_view(con, "src", "live", tomb)
    assert con.execute("SELECT id FROM live").fetchall() == [(1,)]


def test_cached_dv_reuse_checks_conflicting_pinned_count(tmp_path):
    con = duckdb.connect()
    dv = tmp_path / "dv.parquet"
    _write_dv(con, dv, [("raw/src", 7)])
    cache = TombstoneCache(2, ttl_seconds=60)
    cache.acquire(con, "raw/dv", str(dv), expected_rows=1)
    with pytest.raises(RuntimeError, match="expected 2, got 1"):
        cache.acquire(con, "raw/dv", str(dv), expected_rows=2)


def test_dv_cache_name_collision_never_overwrites_inflight_vector(
    tmp_path, monkeypatch,
):
    con = duckdb.connect()
    first_path = tmp_path / "first-dv.parquet"
    second_path = tmp_path / "second-dv.parquet"
    _write_dv(con, first_path, [("raw/first", 7)])
    _write_dv(con, second_path, [("raw/second", 8)])
    monkeypatch.setattr(engine_common_module, "dv_table_name", lambda _key: "dv_forced")

    cache = TombstoneCache(8, ttl_seconds=60)
    first = cache.acquire(con, "raw/key-one", str(first_path), expected_rows=1)
    second = cache.acquire(con, "raw/key-two", str(second_path), expected_rows=1)

    assert first != second
    assert con.execute(
        f"SELECT __file__, __rowid__ FROM {first}"
    ).fetchall() == [("raw/first", 7)]
    assert con.execute(
        f"SELECT __file__, __rowid__ FROM {second}"
    ).fetchall() == [("raw/second", 8)]


def test_dv_cache_global_lru_cap_bounds_many_tables(tmp_path):
    con = duckdb.connect()
    dv = tmp_path / "dv.parquet"
    _write_dv(con, dv, [("raw/src", 7)])
    cache = TombstoneCache(
        capacity=8, ttl_seconds=10_000, global_capacity=2,
    )
    keys = [f"org/s/t{i}/tombstone/v.parquet" for i in range(3)]
    for key in keys:
        cache.acquire(con, key, str(dv), expected_rows=1)
        cache.release(con, key)
    assert [entry["cache_key"] for entry in cache.snapshot()] == keys[-2:]


def test_dv_membership_validation_scales_with_single_list_parameter(tmp_path):
    con = duckdb.connect()
    src, dv = tmp_path / "src.parquet", tmp_path / "dv.parquet"
    _write_source(con, src, ident=1, rowid=7)
    _write_dv(con, dv, [("raw/selected", 7)])
    create_reflection_view(
        con, "src", [str(src)], resource_keys=["raw/selected"],
    )
    allowed = tuple(["raw/selected"] + [f"raw/f{i}" for i in range(10_000)])
    tomb = TombstoneDef(
        str(dv), "raw/dv", 1,
        resource_keys=("raw/selected",), snapshot_resource_keys=allowed,
    )
    create_tombstone_view(con, "src", "live", tomb)
    assert con.execute("SELECT * FROM live").fetchall() == []


def test_typed_empty_view_preserves_polars_snapshot_types():
    con = duckdb.connect()
    create_typed_empty_view(
        con,
        "empty_t",
        {
            "id": "Int64",
            "u": "UInt64",
            "name": "String",
            "at": "Datetime(time_unit='us', time_zone=None)",
            "amount": "Decimal(precision=20, scale=4)",
            "nothing": "Null",
            "clock": "Time",
            "elapsed": "Duration(time_unit='ns')",
            "category": "Categorical",
            "state": "Enum(categories=['new', 'done'])",
            "ids": "List(Int64)",
            "pair": "Array(Int64, shape=(2,))",
            "payload": "Struct({'a': Int64, 'tags': List(String)})",
        },
    )
    assert con.execute("SELECT * FROM empty_t").fetchall() == []
    types = {row[0]: row[1] for row in con.execute("DESCRIBE empty_t").fetchall()}
    assert types == {
        "id": "BIGINT", "u": "UBIGINT", "name": "VARCHAR",
        "at": "TIMESTAMP", "amount": "DECIMAL(20,4)",
        "nothing": "INTEGER", "clock": "TIME", "elapsed": "BIGINT",
        "category": "VARCHAR", "state": "VARCHAR",
        "ids": "BIGINT[]", "pair": "BIGINT[2]",
        "payload": "STRUCT(a BIGINT, tags VARCHAR[])",
    }


def test_spark_typed_empty_view_supports_nested_polars_types():
    cursor = MagicMock()
    _spark_create_empty_view(
        cursor,
        "empty_t",
        {
            "elapsed": "Duration(time_unit='us')",
            "category": "Categorical",
            "state": "Enum(categories=['new', 'done'])",
            "ids": "List(Int64)",
            "pair": "Array(Int64, shape=(2,))",
            "payload": "Struct({'a': Int64, 'tags': List(String)})",
        },
    )
    sql = cursor.execute.call_args.args[0]
    assert "CAST(NULL AS long) AS `elapsed`" in sql
    assert "CAST(NULL AS string) AS `category`" in sql
    assert "CAST(NULL AS string) AS `state`" in sql
    assert "array<long>" in sql
    assert "struct<`a`:long, `tags`:array<string>>" in sql


def test_typed_empty_special_types_match_real_polars_parquet_scan(tmp_path):
    schema = {
        "nothing": pl.Null,
        "clock": pl.Time,
        "duration_ms": pl.Duration("ms"),
        "duration_us": pl.Duration("us"),
        "duration_ns": pl.Duration("ns"),
        "category": pl.Categorical,
        "state": pl.Enum(["new", "done"]),
    }
    parquet_path = tmp_path / "special_types.parquet"
    pl.DataFrame(schema=schema).write_parquet(parquet_path)

    con = duckdb.connect()
    scanned = {
        row[0]: row[1]
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)],
        ).fetchall()
    }
    create_typed_empty_view(
        con, "empty_special", {name: str(dtype) for name, dtype in schema.items()},
    )
    reconstructed = {
        row[0]: row[1]
        for row in con.execute("DESCRIBE empty_special").fetchall()
    }
    assert reconstructed == scanned


@pytest.mark.parametrize(
    "invalid_type",
    [
        "Duration(time_unit='s')",
        "Duration('us')",
        "Enum(categories='not-a-list')",
        "Enum(categories=['duplicate', 'duplicate'])",
    ],
)
def test_typed_empty_special_types_reject_non_polars_parameters(invalid_type):
    with pytest.raises(RuntimeError, match="Unsupported snapshot column type"):
        snapshot_duckdb_type(invalid_type)
    with pytest.raises(RuntimeError, match="Unsupported snapshot column type"):
        snapshot_spark_type(invalid_type)


@pytest.mark.parametrize("unsupported_type", ["Null", "Time"])
def test_spark_typed_empty_rejects_types_spark_cannot_scan_safely(
    unsupported_type,
):
    cursor = MagicMock()
    with pytest.raises(RuntimeError, match="Unsupported snapshot column type"):
        _spark_create_empty_view(cursor, "empty_t", {"value": unsupported_type})
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("executor_cls", [DuckDB])
def test_executor_returns_typed_empty_result_for_zero_resource_snapshot(
    executor_cls, tmp_path,
):
    parser = SQLParser("s", "SELECT id FROM t", "duckdb")
    reflection = Reflection(
        "local", 0, 0,
        [SuperSnapshot(
            "s", "t", 2, [], {"id", "__rowid__", "__timestamp__"}, [],
            column_types={"id": "Int64", "__rowid__": "Int64", "__timestamp__": "Int64"},
        )],
    )
    qm = MagicMock(
        temp_dir=str(tmp_path), query_plan_path=str(tmp_path / "plan.json"),
    )
    executor = executor_cls()
    try:
        result = executor.execute(reflection, parser, qm, lambda _event: None)
    finally:
        executor._reset_connection()
    assert list(result.columns) == ["id"]
    assert result.empty


def test_duckdb_concurrent_same_snapshot_views_are_request_isolated(
    tmp_path, monkeypatch,
):
    """Same table/version but different survivor files must not share DDL."""
    setup = duckdb.connect()
    paths = []
    for value in (11, 22):
        path = tmp_path / f"lite_f{value}.parquet"
        _write_source(setup, path, ident=value, rowid=value)
        paths.append(path)

    engine = DuckDB()
    created = threading.Barrier(2)
    original_create = duckdb_engine_module.create_reflection_view_with_presign_retry

    def create_then_align(*args, **kwargs):
        result = original_create(*args, **kwargs)
        created.wait(timeout=10)
        return result

    monkeypatch.setattr(
        duckdb_engine_module,
        "create_reflection_view_with_presign_retry",
        create_then_align,
    )

    def run(idx):
        value = (11, 22)[idx]
        parser = SQLParser("s", "SELECT id FROM t", "duckdb")
        reflection = Reflection(
            "local", 1, 1,
            [SuperSnapshot(
                "s", "t", 7, [str(paths[idx])],
                {"id", "__rowid__", "__timestamp__"}, [f"raw/f{value}"],
                column_types={
                    "id": "Int64", "__rowid__": "Int64",
                    "__timestamp__": "Int64",
                },
            )],
        )
        qm = MagicMock(
            temp_dir=str(tmp_path),
            query_plan_path=str(tmp_path / f"duckdb-plan-{idx}.json"),
        )
        return engine.execute(
            reflection, parser, qm, lambda _event: None,
        )["id"].tolist()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, (0, 1)))
    finally:
        engine._reset_connection()
    assert results == [[11], [22]]


def test_duckdb_request_names_use_full_uuid_for_concurrent_active_dv_isolation(
        tmp_path, monkeypatch,
):
    """UUIDs sharing their first 32 bits must still own distinct DDL names."""
    paths = []
    for value in (11, 22):
        path = tmp_path / f"collision_f{value}.parquet"
        pl.DataFrame({
            "id": [value],
            "__rowid__": pl.Series([value], dtype=pl.Int64),
            "__timestamp__": pl.Series([1], dtype=pl.Int64),
        }).write_parquet(path)
        paths.append(path)
    dv = tmp_path / "collision_dv.parquet"
    dv_rows = [("raw/f11", 11)]
    _write_dv(duckdb.connect(), dv, dv_rows)

    # These two UUIDs collide under the former ``hex[:8]`` naming scheme.
    uuid_values = iter((
        SimpleNamespace(hex="deadbeef" + "1" * 24),
        SimpleNamespace(hex="deadbeef" + "2" * 24),
    ))
    uuid_lock = threading.Lock()

    def next_uuid():
        with uuid_lock:
            return next(uuid_values)

    monkeypatch.setattr(duckdb_engine_module._uuid, "uuid4", next_uuid)

    engine = DuckDB()
    source_created = threading.Barrier(2, timeout=10)
    original_create = duckdb_engine_module.create_reflection_view_with_presign_retry

    def create_then_align(*args, **kwargs):
        result = original_create(*args, **kwargs)
        source_created.wait()
        return result

    monkeypatch.setattr(
        duckdb_engine_module,
        "create_reflection_view_with_presign_retry",
        create_then_align,
    )

    def run(idx):
        key = f"raw/f{(11, 22)[idx]}"
        parser = SQLParser("s", "SELECT id FROM t", "duckdb")
        snapshot = SuperSnapshot(
            "s", "t", 7, [str(paths[idx])],
            {"id", "__rowid__", "__timestamp__"}, [key],
            column_types={
                "id": "Int64", "__rowid__": "Int64",
                "__timestamp__": "Int64",
            },
            snapshot_resource_keys=["raw/f11", "raw/f22"],
        )
        tomb = TombstoneDef(
            tombstone_path=str(dv), cache_key="raw/collision-dv",
            expected_rows=1, tombstone_digest=_dv_digest(dv_rows),
            resource_keys=(key,),
            snapshot_resource_keys=("raw/f11", "raw/f22"),
        )
        reflection = Reflection(
            "local", 1, 1, [snapshot], tombstone_views={"t": tomb},
        )
        qm = MagicMock(
            temp_dir=str(tmp_path),
            query_plan_path=str(tmp_path / f"collision-plan-{idx}.json"),
        )
        return engine.execute(
            reflection, parser, qm, lambda _event: None,
        )["id"].tolist()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, (0, 1)))
    finally:
        engine._reset_connection()

    assert results == [[], [22]]


def test_duckdb_singleton_reuses_validated_dv_across_executors(
    tmp_path, monkeypatch,
):
    setup = duckdb.connect()
    source, dv = tmp_path / "duckdb-source.parquet", tmp_path / "duckdb-dv.parquet"
    _write_source(setup, source, ident=1, rowid=7)
    rows = [("raw/source", 7)]
    _write_dv(setup, dv, rows)

    reflection = Reflection(
        "local", 1, 1,
        [SuperSnapshot(
            "s", "t", 1, [str(source)],
            {"id", "__rowid__", "__timestamp__"}, ["raw/source"],
            column_types={
                "id": "Int64", "__rowid__": "Int64",
                "__timestamp__": "Int64",
            },
            snapshot_resource_keys=["raw/source"],
        )],
        tombstone_views={
            "t": TombstoneDef(
                tombstone_path=str(dv), cache_key="raw/dv", expected_rows=1,
                tombstone_digest=_dv_digest(rows),
                resource_keys=("raw/source",),
                snapshot_resource_keys=("raw/source",),
            ),
        },
    )

    validation_calls = 0
    original_validate = engine_common_module._validate_tombstone_relation_details

    def counted_validate(*args, **kwargs):
        nonlocal validation_calls
        validation_calls += 1
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(
        engine_common_module,
        "_validate_tombstone_relation_details",
        counted_validate,
    )
    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()
    first = executor_module.Executor(organization="org")
    second = executor_module.Executor(organization="org")
    assert first.duckdb_exec is second.duckdb_exec

    try:
        for idx, executor in enumerate((first, second)):
            parser = SQLParser("s", "SELECT id FROM t", "duckdb")
            qm = MagicMock(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / f"duckdb-cache-plan-{idx}.json"),
            )
            result = executor.duckdb_exec.execute(
                reflection, parser, qm, lambda _event: None,
            )
            assert result.empty
        assert validation_calls == 1
    finally:
        first.duckdb_exec._reset_connection()
        executor_module._duckdb_singleton = None
        executor_module._duckdb_singletons.clear()


def test_duckdb_singletons_are_scoped_by_org_and_storage():
    class Storage:
        def __init__(self, bucket):
            self.bucket_name = bucket
            self.base_prefix = ""

    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()
    a = executor_module._get_duckdb(Storage("a"), organization="org-1")
    b = executor_module._get_duckdb(Storage("b"), organization="org-1")
    c = executor_module._get_duckdb(Storage("a"), organization="org-2")
    assert len({id(a), id(b), id(c)}) == 3
    for engine in (a, b, c):
        engine._reset_connection()
    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()


def test_builtin_storage_identity_reuses_local_and_seals_full_credentials():
    from supertable.storage.local_storage import LocalStorage

    assert executor_module._storage_identity(LocalStorage()) == (
        executor_module._storage_identity(LocalStorage())
    )

    class S3Like:
        bucket_name = "bucket"
        endpoint_url = "https://s3.example"
        region = "eu-west-1"
        url_style = "path"
        secure = True
        base_prefix = "tenant"
        _aws_access_key_id = "access-one"
        _aws_session_token = "session-one"

        def __init__(self, secret):
            self._aws_secret_access_key = secret

    S3Like.__module__ = "supertable.storage.s3_storage"
    same_a = executor_module._storage_identity(S3Like("secret-one"))
    same_b = executor_module._storage_identity(S3Like("secret-one"))
    different = executor_module._storage_identity(S3Like("secret-two"))
    assert same_a == same_b
    assert same_a != different
    assert "access-one" not in repr(same_a)
    assert "secret-one" not in repr(same_a)
    assert "session-one" not in repr(same_a)


def test_scoped_lite_registry_releases_unreferenced_noncurrent_scope():
    class Storage:
        def __init__(self, bucket):
            self.bucket_name = bucket

    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()
    first = executor_module._get_duckdb(Storage("first"), "org")
    second = executor_module._get_duckdb(Storage("second"), "org")
    assert len(executor_module._duckdb_singletons) == 2
    del first
    gc.collect()
    assert list(executor_module._duckdb_singletons.values()) == [second]
    second._reset_connection()
    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()


def test_auto_never_routes_active_tombstone_to_spark(monkeypatch):
    executor = executor_module.Executor(organization="org")
    monkeypatch.setattr(
        executor,
        "_active_spark_clusters",
        lambda: [{"status": "active", "min_bytes": 1}],
    )
    reflection = Reflection(
        "object", 10_000, 1,
        [SuperSnapshot("s", "t", 1, ["f"], {"id"}, ["raw/f"])],
        tombstone_views={
            "t": TombstoneDef(
                tombstone_path="signed://dv", cache_key="raw/dv",
                expected_rows=1, tombstone_digest="0" * 64,
                resource_keys=("raw/f",), snapshot_resource_keys=("raw/f",),
            ),
        },
    )
    cfg = MagicMock(
        engine_island_min_bytes=100,
        engine_spark_min_bytes=1,
        engine_freshness_sec=300,
    )

    assert executor._auto_pick(reflection, cfg) is Engine.DUCKDB


def test_local_paths_never_attempt_httpfs_load():
    con = MagicMock()
    configure_httpfs_and_s3(con, ["/tmp/local.parquet"])
    con.execute.assert_not_called()


def test_spark_resolver_never_reinterprets_gcs_or_azure_as_s3(monkeypatch):
    from dataclasses import replace
    from supertable.config.settings import settings
    from supertable.engine import spark_thrift

    monkeypatch.setattr(
        spark_thrift,
        "settings",
        replace(settings, SUPERTABLE_SPARK_PRESIGNED=False),
    )
    gcs = "https://storage.googleapis.com/my-bucket/base/f.parquet?sig=x"
    azure = "https://acct.blob.core.windows.net/container/base/f.parquet?sig=x"
    assert _to_s3a_path(gcs) == "gs://my-bucket/base/f.parquet"
    assert _resolve_spark_file(None, gcs) == "gs://my-bucket/base/f.parquet"
    assert _to_s3a_path(azure) == (
        "abfss://container@acct.dfs.core.windows.net/base/f.parquet"
    )
    assert _resolve_spark_file(None, azure).startswith("abfss://container@acct.")


def test_spark_resolver_handles_aws_virtual_host_url():
    url = "https://bucket.s3.eu-west-1.amazonaws.com/a/f.parquet?sig=x"
    assert _to_s3a_path(url) == "s3a://bucket/a/f.parquet"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM read_parquet('/tmp/secret.parquet')",
        "SELECT * FROM parquet_scan('s3://foreign/data.parquet')",
        "SELECT * FROM sqlite_scan('/tmp/db', 'users')",
        "SELECT * FROM '/tmp/secret.parquet'",
    ],
)
def test_catalog_parser_rejects_external_table_sources(query):
    with pytest.raises(ValueError, match="External|table-valued"):
        SQLParser("s", query, "duckdb")


def test_catalog_parser_rejects_time_travel_and_cross_scope_alias_reuse():
    with pytest.raises(ValueError, match="AS OF|historical"):
        SQLParser("s", "SELECT * FROM t AT (VERSION => 1)", "duckdb")
    with pytest.raises(ValueError, match="multiple physical tables"):
        SQLParser(
            "s",
            "SELECT * FROM orders x WHERE EXISTS (SELECT 1 FROM customers x)",
            "duckdb",
        )


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM t WHERE id = 1",
        "COPY (SELECT * FROM t) TO '/tmp/leak.parquet'",
        "CREATE TABLE leaked AS SELECT * FROM t",
        "ATTACH '/tmp/foreign.duckdb' AS foreign",
        "PRAGMA database_list",
    ],
)
def test_catalog_parser_rejects_non_query_commands(query):
    with pytest.raises(ValueError, match="Only read-only"):
        SQLParser("s", query, "duckdb")
    with pytest.raises(ValueError, match="multiple physical tables"):
        SQLParser(
            "s",
            "SELECT * FROM orders a WHERE EXISTS (SELECT 1 FROM customers A)",
            "duckdb",
        )


def test_spark_describe_failure_with_tombstone_fails_closed():
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("metastore unavailable")
    tomb = TombstoneDef(
        "s3://bucket/dv.parquet", "raw/dv", 1,
        resource_keys=("raw/f",),
    )
    with pytest.raises(RuntimeError, match="Cannot validate source schema"):
        _spark_create_tombstone_view(cursor, "src", "live", tomb)


def test_spark_active_tombstone_rejected_until_composite_identity_exists():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("id", "int"), ("__rowid__", "bigint")]
    tomb = TombstoneDef("s3://bucket/dv.parquet", "raw/dv", 2)
    with pytest.raises(RuntimeError, match="composite source-file"):
        _spark_create_tombstone_view(cursor, "src", "live", tomb)


def test_spark_protected_projection_quotes_embedded_backtick_column():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("odd`name", "string"),
        ("__rowid__", "bigint"),
        ("__timestamp__", "timestamp"),
    ]

    _spark_create_tombstone_view(cursor, "src", "live", None)

    sql = cursor.execute.call_args_list[-1].args[0]
    assert "SELECT src.`odd``name` FROM src AS src" in sql
    assert "odd`name" not in sql.replace("odd``name", "")


def test_reserved_rowid_case_variant_fails_closed_in_duckdb_and_spark(tmp_path):
    con = duckdb.connect()
    con.execute('CREATE TABLE bad (id INTEGER, "__ROWID__" BIGINT)')
    with pytest.raises(RuntimeError, match="reserved system column"):
        create_tombstone_view(con, "bad", "live", None)

    cursor = MagicMock()
    cursor.fetchall.return_value = [("id", "int"), ("__ROWID__", "bigint")]
    with pytest.raises(RuntimeError, match="reserved system column"):
        _spark_create_tombstone_view(cursor, "bad", "live", None)
