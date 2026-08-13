from __future__ import annotations

import random
import stat
import threading
from collections import namedtuple
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import supertable.engine.island_spill as spill_module

from supertable.engine.island_spill import (
    AggregateSpec,
    SpillBudgetExceeded,
    SpillCancelled,
    SpillDiskFull,
    SpillMemoryLimitExceeded,
    SpillSession,
    UnsupportedSpillOperation,
    external_group_aggregate,
    external_sort,
)


DiskUsage = namedtuple("DiskUsage", "total used free")
MIB = 1024 * 1024


def _batches(table: pa.Table, rows: int = 1000):
    return table.to_batches(max_chunksize=rows)


def test_spill_session_is_private_and_always_cleaned(tmp_path):
    with SpillSession(tmp_path, budget_bytes=MIB, min_free_bytes=0, query_id="a/b") as session:
        directory = session.directory
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        with session.open_output("data.bin") as output:
            output.write(b"abc")
        assert session.used_bytes == 3
        assert session.peak_used_bytes == 3
    assert not directory.exists()
    assert list(tmp_path.iterdir()) == []


def test_spill_hard_quota_removes_partial_file(tmp_path):
    schema = pa.schema([("id", pa.int64())])
    with SpillSession(tmp_path, budget_bytes=64, min_free_bytes=0) as session:
        with pytest.raises(SpillBudgetExceeded):
            session.write_ipc_batches(
                "too-big.arrow",
                schema,
                [pa.record_batch([list(range(100))], schema=schema)],
            )
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_partial_write_failure_does_not_undercharge_older_runs(tmp_path):
    class WritePrefixThenFail:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def write(self, value):
            self.wrapped.write(value[:2])
            raise OSError("injected partial write")

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    with SpillSession(tmp_path, budget_bytes=MIB, min_free_bytes=0) as session:
        with session.open_output("older.bin") as output:
            output.write(b"old")
        assert session.used_bytes == 3

        with session.open_output("partial.bin") as output:
            output._file = WritePrefixThenFail(output._file)
            with pytest.raises(OSError, match="injected partial write"):
                output.write(b"abcdef")
            assert session.used_bytes == 5

        session.remove(session.directory / "partial.bin")
        assert session.used_bytes == 3


def test_spill_free_space_preflight_fails_before_private_dir(tmp_path):
    with pytest.raises(SpillDiskFull):
        with SpillSession(
            tmp_path,
            budget_bytes=MIB,
            min_free_bytes=10,
            disk_usage=lambda _: DiskUsage(100, 95, 5),
        ):
            pass
    assert list(tmp_path.iterdir()) == []


def test_spill_cancellation_blocks_additional_writes_and_cleans(tmp_path):
    cancelled = threading.Event()
    with pytest.raises(SpillCancelled):
        with SpillSession(
            tmp_path,
            budget_bytes=MIB,
            min_free_bytes=0,
            cancel_event=cancelled,
        ) as session:
            directory = session.directory
            cancelled.set()
            session.open_output("cancelled.bin")
    assert not directory.exists()


def test_forced_external_sort_matches_arrow_and_reclaims_runs(tmp_path):
    rng = random.Random(42)
    rows = 40_000
    keys = [rng.randrange(0, 5000) for _ in range(rows)]
    values = list(range(rows))
    table = pa.table({"key": keys, "value": values})
    expected = pc.take(
        table,
        pc.sort_indices(table, sort_keys=[("key", "ascending"), ("value", "descending")]),
    )

    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 2000),
            schema=table.schema,
            sort_keys=[("key", "ascending"), ("value", "descending")],
            session=session,
            memory_budget_bytes=256 * 1024,
            output_batch_rows=257,
            max_open_runs=2,
        )
        assert session.used_bytes > 0
        actual = stream.collect_table(max_bytes=4 * MIB)
        assert actual.equals(expected)
        assert session.used_bytes == 0


def test_external_sort_null_placement_and_descending(tmp_path):
    table = pa.table({"key": [3, None, 1, 2, None], "value": [0, 1, 2, 3, 4]})
    expected = pc.take(
        table,
        pc.sort_indices(table, sort_keys=[("key", "descending")], null_placement="at_start"),
    )
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        actual = external_sort(
            _batches(table, 2),
            schema=table.schema,
            sort_keys=[("key", "descending")],
            null_placement="at_start",
            session=session,
            memory_budget_bytes=256 * 1024,
        ).collect_table(max_bytes=MIB)
    assert actual.equals(expected)


def test_external_sort_rejects_unsealed_float_order(tmp_path):
    table = pa.table({"key": [1.0, float("nan")]})
    with SpillSession(tmp_path, budget_bytes=MIB, min_free_bytes=0) as session:
        with pytest.raises(UnsupportedSpillOperation):
            external_sort(
                _batches(table),
                schema=table.schema,
                sort_keys=["key"],
                session=session,
                memory_budget_bytes=256 * 1024,
            )


def test_external_sort_rejects_single_oversized_input_batch(tmp_path):
    table = pa.table({"payload": ["x" * 1024] * 400, "key": list(range(400))})
    assert table.nbytes > 256 * 1024
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        with pytest.raises(SpillMemoryLimitExceeded):
            external_sort(
                table.to_batches(),
                schema=table.schema,
                sort_keys=["key"],
                session=session,
                memory_budget_bytes=256 * 1024,
            )
        assert session.used_bytes == 0


def test_forced_spill_group_aggregate_matches_reference(tmp_path):
    rng = random.Random(7)
    groups = [rng.randrange(0, 400) for _ in range(30_000)]
    values = [None if index % 11 == 0 else rng.randrange(-50, 100) for index in range(30_000)]
    table = pa.table({"group": groups, "value": values}, schema=pa.schema([
        ("group", pa.int64()),
        ("value", pa.int64()),
    ]))
    reference = {}
    for group, value in zip(groups, values):
        state = reference.setdefault(group, {"rows": 0, "count": 0, "sum": None, "min": None, "max": None})
        state["rows"] += 1
        if value is not None:
            state["count"] += 1
            state["sum"] = value if state["sum"] is None else state["sum"] + value
            state["min"] = value if state["min"] is None else min(state["min"], value)
            state["max"] = value if state["max"] is None else max(state["max"], value)
    expected_rows = [
        {"group": group, **reference[group]}
        for group in sorted(reference)
    ]

    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        stream = external_group_aggregate(
            _batches(table, 1500),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[
                AggregateSpec("rows", "count_star"),
                AggregateSpec("count", "count", "value"),
                AggregateSpec("sum", "sum", "value"),
                AggregateSpec("min", "min", "value"),
                AggregateSpec("max", "max", "value"),
            ],
            session=session,
            memory_budget_bytes=256 * 1024,
            output_batch_rows=31,
            max_open_runs=2,
        )
        actual = stream.collect_table(max_bytes=MIB)
        expected = pa.Table.from_pylist(expected_rows, schema=actual.schema)
        assert actual.equals(expected)
        assert session.used_bytes == 0


def test_partial_result_close_reclaims_sort_runs(tmp_path):
    table = pa.table({"key": list(reversed(range(20_000)))})
    with SpillSession(tmp_path, budget_bytes=16 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 1000),
            schema=table.schema,
            sort_keys=["key"],
            session=session,
            memory_budget_bytes=256 * 1024,
            output_batch_rows=100,
        )
        next(stream)
        assert session.used_bytes > 0
        stream.close()
        assert session.used_bytes == 0


def test_unstarted_sort_and_group_streams_reclaim_eager_runs(tmp_path):
    table = pa.table({"group": list(reversed(range(20_000))), "value": list(range(20_000))})
    with SpillSession(tmp_path, budget_bytes=16 * MIB, min_free_bytes=0) as session:
        sorted_stream = external_sort(
            _batches(table, 1000),
            schema=table.schema,
            sort_keys=["group"],
            session=session,
            memory_budget_bytes=256 * 1024,
        )
        assert session.used_bytes > 0
        sorted_stream.close()
        assert session.used_bytes == 0

        grouped_stream = external_group_aggregate(
            _batches(table, 1000),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[AggregateSpec("total", "sum", "value")],
            session=session,
            memory_budget_bytes=256 * 1024,
        )
        assert session.used_bytes > 0
        grouped_stream.close()
        assert session.used_bytes == 0


def test_record_batch_row_extracts_each_column_once():
    calls = []

    class Scalar:
        def __init__(self, value):
            self.value = value

        def as_py(self):
            return self.value

    class Batch:
        schema = pa.schema([(f"c{index}", pa.int64()) for index in range(32)])

        def column(self, index):
            calls.append(index)
            return [Scalar(index)]

    row = spill_module._record_batch_row(Batch(), 0)

    assert row == {f"c{index}": index for index in range(32)}
    assert calls == list(range(32))


def test_external_sort_bounds_sort_index_rows_for_tiny_encoded_values(
    tmp_path, monkeypatch,
):
    # Boolean Arrow buffers use one bit per value, while sort_indices needs an
    # integer index per row. A byte-only run bound therefore understates the
    # actual sort workspace by roughly 64x.
    table = pa.table({
        "key": [bool(index % 2) for index in range(100_000)],
    })
    memory_budget = 256 * 1024
    maximum_run_rows = memory_budget // 32
    observed_rows = []
    original = spill_module.pc.sort_indices

    def bounded_sort_indices(values, *args, **kwargs):
        observed_rows.append(values.num_rows)
        assert values.num_rows <= maximum_run_rows
        return original(values, *args, **kwargs)

    monkeypatch.setattr(spill_module.pc, "sort_indices", bounded_sort_indices)
    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        result = external_sort(
            _batches(table, 100_000),
            schema=table.schema,
            sort_keys=[("key", "ascending")],
            session=session,
            memory_budget_bytes=memory_budget,
            output_batch_rows=100_000,
            max_open_runs=32,
        ).collect_table(max_bytes=4 * MIB)

    assert observed_rows
    assert result.num_rows == table.num_rows
    assert result["key"].to_pylist() == [False] * 50_000 + [True] * 50_000


def test_external_sort_caps_merge_fan_in_by_memory(tmp_path, monkeypatch):
    table = pa.table({"key": list(reversed(range(80_000)))})
    observed_fan_in = []
    original = spill_module._merge_run_batches

    def bounded_merge(paths, **kwargs):
        observed_fan_in.append(len(paths))
        assert len(paths) <= 4
        return original(paths, **kwargs)

    monkeypatch.setattr(spill_module, "_merge_run_batches", bounded_merge)
    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        result = external_sort(
            _batches(table, 1000),
            schema=table.schema,
            sort_keys=["key"],
            session=session,
            memory_budget_bytes=512 * 1024,
            output_batch_rows=100_000,
            max_open_runs=32,
        ).collect_table(max_bytes=2 * MIB)

    assert observed_fan_in
    assert result["key"].to_pylist() == list(range(80_000))


def test_external_group_supports_grouping_without_aggregates(tmp_path):
    table = pa.table({"group": [2, 1, 2, None, 1, None]})
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 2),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[],
            session=session,
            memory_budget_bytes=256 * 1024,
        ).collect_table(max_bytes=MIB)

    assert result.to_pylist() == [
        {"group": None},
        {"group": 1},
        {"group": 2},
    ]


def test_external_group_integer_sum_is_wide_and_exact(tmp_path):
    table = pa.table({
        "group": [1, 1],
        "value": [2**62, 2**62],
    })
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 1),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[AggregateSpec("total", "sum", "value")],
            session=session,
            memory_budget_bytes=256 * 1024,
        ).collect_table(max_bytes=MIB)

    assert result.schema.field("total").type == pa.decimal128(38, 0)
    assert int(result["total"][0].as_py()) == 2**63


def test_external_group_decimal_sum_widens_precision_and_preserves_scale(tmp_path):
    schema = pa.schema([
        ("group", pa.int64()),
        ("value", pa.decimal128(10, 2)),
    ])
    table = pa.table({
        "group": [1, 1],
        "value": [Decimal("99999999.99"), Decimal("0.02")],
    }, schema=schema)
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 1),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[AggregateSpec("total", "sum", "value")],
            session=session,
            memory_budget_bytes=256 * 1024,
        ).collect_table(max_bytes=MIB)

    assert result.schema.field("total").type == pa.decimal128(38, 2)
    assert result["total"][0].as_py() == Decimal("100000000.01")
