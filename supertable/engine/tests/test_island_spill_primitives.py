from __future__ import annotations

import random
import stat
import threading
import time
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
    SpillDeadlineExceeded,
    SpillDiskFull,
    SpillMemoryLimitExceeded,
    SpillNumericOverflow,
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


def test_spill_deadline_removes_partially_written_ipc_and_private_dir(tmp_path):
    clock = [10.0]
    schema = pa.schema([("id", pa.int64())])

    def batches_crossing_deadline():
        yield pa.record_batch([[1]], schema=schema)
        clock[0] = 12.0
        yield pa.record_batch([[2]], schema=schema)

    with pytest.raises(SpillDeadlineExceeded, match="timed out"):
        with SpillSession(
            tmp_path,
            budget_bytes=MIB,
            min_free_bytes=0,
            deadline_monotonic=11.0,
            monotonic=lambda: clock[0],
        ) as session:
            directory = session.directory
            session.write_ipc_batches(
                "deadline.arrow", schema, batches_crossing_deadline(),
            )

    assert not directory.exists()
    assert list(tmp_path.iterdir()) == []


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


@pytest.mark.parametrize(
    ("dtype", "minimum", "maximum", "partitions", "expected"),
    [
        (pa.int8(), -128, 127, 4, [-64, 0, 64]),
        (
            pa.uint64(), 0, 2**64 - 1, 4,
            [2**62, 2**63, 3 * 2**62],
        ),
        (pa.int64(), -(2**63), 2**63 - 1, 2, [0]),
    ],
)
def test_integer_range_cuts_are_exact_and_overflow_safe(
    dtype, minimum, maximum, partitions, expected,
):
    cuts = spill_module._integer_range_cuts(
        dtype, minimum, maximum, partitions,
    )
    assert [int(value) for value in cuts] == expected


@pytest.mark.parametrize(
    ("memory_budget", "buckets", "expected"),
    [
        (1024 * MIB, 108, 512 * 1024),
        (1024 * MIB, 256, 256 * 1024),
        (128 * MIB, 128, 64 * 1024),
        (MIB, 2, 0),
    ],
)
def test_range_writer_buffers_are_globally_bounded(
    memory_budget, buckets, expected,
):
    per_writer = spill_module._range_writer_buffer_bytes(
        memory_budget, buckets,
    )
    assert per_writer == expected
    assert per_writer * buckets <= memory_budget // 16
    assert per_writer == 0 or 64 * 1024 <= per_writer <= 512 * 1024


def test_buffered_range_ipc_coalesces_raw_writes_and_round_trips_exactly(
    tmp_path, monkeypatch,
):
    rows_per_batch = 10
    batch_count = 100
    schema = pa.schema([
        ("id", pa.int64()),
        *((f"payload_{index}", pa.binary()) for index in range(8)),
    ])
    batches = []
    for batch_index in range(batch_count):
        ids = list(range(
            batch_index * rows_per_batch,
            (batch_index + 1) * rows_per_batch,
        ))
        batches.append(pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                *(
                    pa.array(
                        [bytes([65 + index]) * 32] * rows_per_batch,
                        type=pa.binary(),
                    )
                    for index in range(8)
                ),
            ],
            schema=schema,
        ))
    expected = pa.Table.from_batches(batches, schema=schema)
    raw_calls = {"plain.arrow": 0, "buffered.arrow": 0}
    original_write = spill_module._BudgetedFile.write

    def observed_write(output, value):
        raw_calls[output.path.name] += 1
        return original_write(output, value)

    monkeypatch.setattr(
        spill_module._BudgetedFile, "write", observed_write,
    )
    with SpillSession(tmp_path, budget_bytes=8 * MIB, min_free_bytes=0) as session:
        for name, buffer_bytes in (
            ("plain.arrow", 0),
            ("buffered.arrow", 64 * 1024),
        ):
            writer = spill_module._RangeIPCWriter(
                session,
                name,
                schema,
                buffer_bytes=buffer_bytes,
            )
            for batch in batches:
                writer.write_batch(batch)
            writer.close()
            source = pa.memory_map(str(writer.path), "r")
            try:
                actual = pa.ipc.open_file(source).read_all()
            finally:
                source.close()
            assert actual.equals(expected)
            session.remove(writer.path)
        assert session.used_bytes == 0

    assert raw_calls["plain.arrow"] > 1000
    assert raw_calls["buffered.arrow"] < raw_calls["plain.arrow"] // 8


def test_buffered_range_ipc_quota_and_cancel_cleanup_are_exact(tmp_path):
    schema = pa.schema([("id", pa.int64()), ("payload", pa.binary())])
    batch = pa.record_batch([
        pa.array(range(10), type=pa.int64()),
        pa.array([b"x" * 400] * 10, type=pa.binary()),
    ], schema=schema)

    with SpillSession(tmp_path, budget_bytes=256, min_free_bytes=0) as session:
        writer = spill_module._RangeIPCWriter(
            session,
            "quota.arrow",
            schema,
            buffer_bytes=64 * 1024,
        )
        writer.write_batch(batch)
        with pytest.raises(SpillBudgetExceeded):
            writer.close()
        writer.abort()
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []

    cancelled = threading.Event()
    with SpillSession(
        tmp_path,
        budget_bytes=MIB,
        min_free_bytes=0,
        cancel_event=cancelled,
    ) as session:
        writer = spill_module._RangeIPCWriter(
            session,
            "cancel.arrow",
            schema,
            buffer_bytes=64 * 1024,
        )
        writer.write_batch(batch)
        cancelled.set()
        writer.abort()
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


@pytest.mark.parametrize(
    ("direction", "null_placement"),
    [
        ("ascending", "at_start"),
        ("ascending", "at_end"),
        ("descending", "at_start"),
        ("descending", "at_end"),
    ],
)
def test_native_integer_range_sort_matches_arrow_at_cuts_nulls_and_ties(
    tmp_path, monkeypatch, direction, null_placement,
):
    # -4 and 0 are exact exclusive interval starts for [-8, 7] / 4.
    # Repeated complete keys prove stable scatter + stable native sorting.
    table = pa.table({
        "key": [0, -4, None, -5, 7, -4, 0, None, -8, 7],
        "tie": [2, 1, 3, 4, 0, 1, 2, 2, 9, 0],
        "arrival": list(range(10)),
    }, schema=pa.schema([
        ("key", pa.int64()),
        ("tie", pa.int64()),
        ("arrival", pa.int64()),
    ]))
    sort_keys = [("key", direction), ("tie", "ascending")]
    expected = pc.take(
        table,
        pc.sort_indices(
            table,
            sort_keys=sort_keys,
            null_placement=null_placement,
        ),
    )

    def forbidden_legacy_merge(*args, **kwargs):
        raise AssertionError("sealed balanced range unexpectedly used heap merge")

    monkeypatch.setattr(
        spill_module, "_merge_run_batches", forbidden_legacy_merge,
    )
    with SpillSession(tmp_path, budget_bytes=16 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 3),
            schema=table.schema,
            sort_keys=sort_keys,
            session=session,
            memory_budget_bytes=512 * 1024,
            output_batch_rows=2,
            null_placement=null_placement,
            first_key_range=(-8, 7, True),
            input_size_hint_bytes=2 * MIB,
            input_rows_hint=table.num_rows,
            parallelism=4,
        )
        assert session.used_bytes > 0
        actual = stream.collect_table(max_bytes=MIB)
        assert session.used_bytes == 0
    assert actual.equals(expected)


def test_native_range_out_of_stats_values_saturate_without_data_loss(tmp_path):
    table = pa.table({
        "key": [-10_000, -1, 0, 1, 10_000],
        "row": list(range(5)),
    })
    with SpillSession(tmp_path, budget_bytes=8 * MIB, min_free_bytes=0) as session:
        actual = external_sort(
            _batches(table, 2),
            schema=table.schema,
            sort_keys=["key"],
            session=session,
            memory_budget_bytes=512 * 1024,
            first_key_range=(-1, 1),  # deliberately stale/narrow
            input_size_hint_bytes=MIB,
            input_rows_hint=table.num_rows,
            parallelism=2,
        ).collect_table(max_bytes=MIB)
        assert session.used_bytes == 0
    assert actual["key"].to_pylist() == [-10_000, -1, 0, 1, 10_000]
    assert actual["row"].to_pylist() == list(range(5))


def test_native_range_uint64_extrema_and_null_bucket_match_arrow(tmp_path):
    maximum = 2**64 - 1
    table = pa.table({
        "key": pa.array(
            [maximum, 0, None, 2**63, 2**63 - 1, maximum - 1],
            type=pa.uint64(),
        ),
        "row": pa.array(range(6), type=pa.int64()),
    })
    expected = pc.take(
        table,
        pc.sort_indices(
            table,
            sort_keys=[("key", "descending")],
            null_placement="at_end",
        ),
    )
    with SpillSession(tmp_path, budget_bytes=8 * MIB, min_free_bytes=0) as session:
        actual = external_sort(
            _batches(table, 2),
            schema=table.schema,
            sort_keys=[("key", "descending")],
            session=session,
            memory_budget_bytes=512 * 1024,
            null_placement="at_end",
            first_key_range=(0, maximum, True),
            input_size_hint_bytes=MIB,
            input_rows_hint=table.num_rows,
            parallelism=4,
        ).collect_table(max_bytes=MIB)
        assert session.used_bytes == 0
    assert actual.equals(expected)


def test_native_range_empty_input_preserves_schema_without_spill(tmp_path):
    schema = pa.schema([("key", pa.int64()), ("value", pa.binary())])
    with SpillSession(tmp_path, budget_bytes=8 * MIB, min_free_bytes=0) as session:
        result = external_sort(
            [],
            schema=schema,
            sort_keys=["key"],
            session=session,
            memory_budget_bytes=512 * 1024,
            first_key_range=(0, 100),
            input_size_hint_bytes=MIB,
            input_rows_hint=0,
            parallelism=2,
        ).collect_table(max_bytes=0)
        assert result.schema == schema
        assert result.num_rows == 0
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_native_range_parallel_scheduler_bounds_aggregate_workspace(
    tmp_path, monkeypatch,
):
    rows = 120_000
    table = pa.table({
        "key": pa.array(
            [(index * 48_271 + 17) % 1_000_003 for index in range(rows)],
            type=pa.int64(),
        ),
        "row": pa.array(range(rows), type=pa.int64()),
    })
    memory_budget = 8 * MIB
    sort_budget = memory_budget * 7 // 8
    lock = threading.Lock()
    active_workspace = 0
    maximum_workspace = 0
    maximum_workers = 0
    active_workers = 0
    original = spill_module._native_sort_partition

    def observed(partition, **kwargs):
        nonlocal active_workspace, maximum_workspace
        nonlocal active_workers, maximum_workers
        workspace = spill_module._range_sort_workspace_bytes(
            partition.logical_bytes, partition.rows,
        )
        with lock:
            active_workspace += workspace
            active_workers += 1
            maximum_workspace = max(maximum_workspace, active_workspace)
            maximum_workers = max(maximum_workers, active_workers)
        try:
            # Give independently admitted workers an overlap window. The real
            # Arrow kernels below release the GIL too.
            time.sleep(0.02)
            return original(partition, **kwargs)
        finally:
            with lock:
                active_workspace -= workspace
                active_workers -= 1

    monkeypatch.setattr(spill_module, "_native_sort_partition", observed)
    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        actual = external_sort(
            _batches(table, 5000),
            schema=table.schema,
            sort_keys=["key", "row"],
            session=session,
            memory_budget_bytes=memory_budget,
            first_key_range=(0, 1_000_002),
            input_size_hint_bytes=table.nbytes,
            input_rows_hint=table.num_rows,
            parallelism=4,
        ).collect_table(max_bytes=4 * MIB)
        assert session.used_bytes == 0

    assert actual.num_rows == rows
    assert maximum_workers >= 2
    assert maximum_workspace <= sort_budget


def test_native_range_scatter_uses_one_take_per_coalesced_source_block(
    tmp_path, monkeypatch,
):
    rows = 1000
    table = pa.table({
        "key": pa.array(
            [(index * 37) % rows for index in range(rows)], type=pa.int64(),
        ),
        "row": pa.array(range(rows), type=pa.int64()),
    })
    observed_take_rows = []
    original = spill_module.pc.take

    def observed(values, indices, *args, **kwargs):
        observed_take_rows.append((type(values), values.num_rows))
        return original(values, indices, *args, **kwargs)

    monkeypatch.setattr(spill_module.pc, "take", observed)
    with SpillSession(tmp_path, budget_bytes=8 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 1),  # one thousand deliberately tiny source batches
            schema=table.schema,
            sort_keys=["key", "row"],
            session=session,
            memory_budget_bytes=256 * 1024,
            first_key_range=(0, rows - 1),
            input_size_hint_bytes=2 * MIB,
            input_rows_hint=rows,
            parallelism=4,
        )
        # Preparation is eager, while partition sorts begin on first next().
        # All tiny inputs fit one 64-KiB/4096-row coalesced scatter block.
        assert observed_take_rows == [(pa.Table, rows)]
        stream.close()
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_native_range_partial_close_joins_workers_and_reclaims_files(
    tmp_path, monkeypatch,
):
    table = pa.table({
        "key": list(reversed(range(50_000))),
        "row": list(range(50_000)),
    })
    finished = []
    original = spill_module._native_sort_partition

    def observed(partition, **kwargs):
        try:
            return original(partition, **kwargs)
        finally:
            finished.append(partition.bucket)

    monkeypatch.setattr(spill_module, "_native_sort_partition", observed)
    with SpillSession(tmp_path, budget_bytes=16 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 2000),
            schema=table.schema,
            sort_keys=["key"],
            session=session,
            memory_budget_bytes=2 * MIB,
            output_batch_rows=100,
            first_key_range=(0, table.num_rows - 1),
            input_size_hint_bytes=table.nbytes,
            input_rows_hint=table.num_rows,
            parallelism=4,
        )
        next(stream)
        stream.close()
        assert finished
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_native_range_worker_failure_joins_every_future_before_cleanup(
    tmp_path, monkeypatch,
):
    rows = 120_000
    table = pa.table({
        "key": pa.array(
            [(index * 48_271 + 17) % 1_000_003 for index in range(rows)],
            type=pa.int64(),
        ),
        "row": pa.array(range(rows), type=pa.int64()),
    })
    original = spill_module._native_sort_partition
    lock = threading.Lock()
    started = 0
    finished = 0
    active = 0
    inject_failure = [True]

    def observed(partition, **kwargs):
        nonlocal started, finished, active
        with lock:
            started += 1
            active += 1
            fail = inject_failure[0]
            inject_failure[0] = False
        try:
            time.sleep(0.03)
            if fail:
                raise RuntimeError("injected native range failure")
            return original(partition, **kwargs)
        finally:
            with lock:
                active -= 1
                finished += 1

    monkeypatch.setattr(spill_module, "_native_sort_partition", observed)
    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        stream = external_sort(
            _batches(table, 5000),
            schema=table.schema,
            sort_keys=["key", "row"],
            session=session,
            memory_budget_bytes=8 * MIB,
            first_key_range=(0, 1_000_002),
            input_size_hint_bytes=table.nbytes,
            input_rows_hint=table.num_rows,
            parallelism=4,
        )
        with pytest.raises(RuntimeError, match="injected native range failure"):
            stream.collect_table(max_bytes=4 * MIB)
        assert started >= 2
        assert finished == started
        assert active == 0
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_native_range_quota_and_cancel_failures_reclaim_partials(tmp_path):
    table = pa.table({"key": list(reversed(range(20_000)))})
    with SpillSession(tmp_path, budget_bytes=1024, min_free_bytes=0) as session:
        with pytest.raises(SpillBudgetExceeded):
            external_sort(
                _batches(table, 500),
                schema=table.schema,
                sort_keys=["key"],
                session=session,
                memory_budget_bytes=256 * 1024,
                first_key_range=(0, table.num_rows - 1),
                input_size_hint_bytes=table.nbytes,
                input_rows_hint=table.num_rows,
                parallelism=2,
            )
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []

    cancelled = threading.Event()

    def cancelling_batches():
        for index, batch in enumerate(_batches(table, 500)):
            if index == 3:
                cancelled.set()
            yield batch

    with SpillSession(
        tmp_path, budget_bytes=8 * MIB, min_free_bytes=0,
        cancel_event=cancelled,
    ) as session:
        with pytest.raises(SpillCancelled):
            external_sort(
                cancelling_batches(),
                schema=table.schema,
                sort_keys=["key"],
                session=session,
                memory_budget_bytes=256 * 1024,
                first_key_range=(0, table.num_rows - 1),
                input_size_hint_bytes=table.nbytes,
                input_rows_hint=table.num_rows,
                parallelism=2,
            )
        assert session.used_bytes == 0
        assert list(session.directory.iterdir()) == []


def test_native_range_hot_partition_falls_back_to_bounded_merge(
    tmp_path, monkeypatch,
):
    rows = 20_000
    table = pa.table({
        "key": pa.array([7] * rows, type=pa.int64()),
        "tie": pa.array(list(reversed(range(rows))), type=pa.int64()),
    })
    merges = []
    original = spill_module._merge_run_batches

    def observed(paths, **kwargs):
        merges.append(len(paths))
        return original(paths, **kwargs)

    monkeypatch.setattr(spill_module, "_merge_run_batches", observed)
    with SpillSession(tmp_path, budget_bytes=16 * MIB, min_free_bytes=0) as session:
        actual = external_sort(
            _batches(table, 500),
            schema=table.schema,
            sort_keys=["key", "tie"],
            session=session,
            # The hot partition's native workspace still fits 7/8 of 1 MiB,
            # but its retained sorted output exceeds the explicit 1/8 headroom.
            memory_budget_bytes=MIB,
            output_batch_rows=257,
            max_open_runs=2,
            first_key_range=(0, 100),
            input_size_hint_bytes=table.nbytes,
            input_rows_hint=table.num_rows,
            parallelism=2,
        ).collect_table(max_bytes=MIB)
        assert session.used_bytes == 0
    assert merges
    assert actual["tie"].to_pylist() == list(range(rows))


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


def test_low_cardinality_wide_group_reduces_before_spill(tmp_path):
    rows = 40_000
    group_count = 128
    payload_columns = 6
    groups = [index % group_count for index in range(rows)]
    columns = {"group": pa.array(groups, type=pa.int64())}
    for column_index in range(payload_columns):
        columns[f"payload_{column_index}"] = pa.array([
            f"{column_index:02d}-{group:04d}".encode().ljust(32, b"x")
            for group in groups
        ])
    table = pa.table(columns)
    specs = [
        AggregateSpec(f"maximum_{index}", "max", f"payload_{index}")
        for index in range(payload_columns)
    ]

    with SpillSession(tmp_path, budget_bytes=32 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 500),
            schema=table.schema,
            group_keys=["group"],
            aggregates=specs,
            session=session,
            memory_budget_bytes=4 * MIB,
        ).collect_table(max_bytes=4 * MIB)

        # The old implementation wrote and repeatedly merged all wide rows.
        # Native partial aggregation keeps only 128 compact states instead.
        assert session.peak_used_bytes == 0
        assert result.num_rows == group_count
        assert result["group"].to_pylist() == list(range(group_count))


def test_forced_group_spill_writes_only_compact_states(tmp_path, monkeypatch):
    rows = 20_000
    table = pa.table({
        "group": pa.array(range(rows), type=pa.int64()),
        "value": pa.array([index % 17 for index in range(rows)], type=pa.int64()),
        **{
            f"unused_{column}": pa.array([b"z" * 32] * rows)
            for column in range(8)
        },
    })
    written_schemas = []
    original = SpillSession.write_ipc_batches

    def observe_write(self, relative_name, schema, batches):
        written_schemas.append(schema)
        return original(self, relative_name, schema, batches)

    monkeypatch.setattr(SpillSession, "write_ipc_batches", observe_write)
    with SpillSession(tmp_path, budget_bytes=64 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 500),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[
                AggregateSpec("rows", "count_star"),
                AggregateSpec("total", "sum", "value"),
            ],
            session=session,
            memory_budget_bytes=256 * 1024,
            output_batch_rows=257,
            max_open_runs=2,
        ).collect_table(max_bytes=4 * MIB)

        assert session.peak_used_bytes > 0
        assert session.used_bytes == 0
    assert result.num_rows == rows
    assert result["group"].to_pylist() == list(range(rows))
    assert result["rows"].to_pylist() == [1] * rows
    assert [int(value.as_py()) for value in result["total"]] == [
        index % 17 for index in range(rows)
    ]
    assert written_schemas
    assert all(
        not any(name.startswith("unused_") for name in schema.names)
        for schema in written_schemas
    )


def test_external_group_adversarial_nulls_and_duplicate_inputs(tmp_path):
    rng = random.Random(44017)
    rows = 25_000
    groups = [None if index % 97 == 0 else rng.randrange(0, 257) for index in range(rows)]
    values = [None if index % 13 == 0 else rng.randrange(-(2**40), 2**40) for index in range(rows)]
    table = pa.table(
        {"group": groups, "value": values},
        schema=pa.schema([("group", pa.int64()), ("value", pa.int64())]),
    )
    reference = {}
    for group, value in zip(groups, values):
        state = reference.setdefault(
            group,
            {"rows": 0, "count": 0, "sum": None, "minimum": None, "maximum": None},
        )
        state["rows"] += 1
        if value is not None:
            state["count"] += 1
            state["sum"] = value if state["sum"] is None else state["sum"] + value
            state["minimum"] = value if state["minimum"] is None else min(state["minimum"], value)
            state["maximum"] = value if state["maximum"] is None else max(state["maximum"], value)

    with SpillSession(tmp_path, budget_bytes=64 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 333),
            schema=table.schema,
            group_keys=["group"],
            aggregates=[
                AggregateSpec("rows", "count_star"),
                AggregateSpec("count", "count", "value"),
                AggregateSpec("sum_a", "sum", "value"),
                AggregateSpec("sum_b", "sum", "value"),
                AggregateSpec("minimum", "min", "value"),
                AggregateSpec("maximum", "max", "value"),
            ],
            session=session,
            memory_budget_bytes=256 * 1024,
            max_open_runs=2,
        ).collect_table(max_bytes=4 * MIB)

    actual = {row["group"]: row for row in result.to_pylist()}
    assert set(actual) == set(reference)
    for group, expected in reference.items():
        row = actual[group]
        assert row["rows"] == expected["rows"]
        assert row["count"] == expected["count"]
        assert row["sum_a"] == row["sum_b"]
        assert (
            None if row["sum_a"] is None else int(row["sum_a"])
        ) == expected["sum"]
        assert row["minimum"] == expected["minimum"]
        assert row["maximum"] == expected["maximum"]


def test_external_group_empty_input_has_exact_output_schema(tmp_path):
    schema = pa.schema([("group", pa.int64()), ("value", pa.int32())])
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            [],
            schema=schema,
            group_keys=["group"],
            aggregates=[
                AggregateSpec("rows", "count_star"),
                AggregateSpec("total", "sum", "value"),
            ],
            session=session,
            memory_budget_bytes=256 * 1024,
        ).collect_table(max_bytes=MIB)

    assert result.num_rows == 0
    assert result.schema == pa.schema([
        ("group", pa.int64()),
        ("rows", pa.int64()),
        ("total", pa.decimal128(38, 0)),
    ])


def test_external_group_decimal_sum_fails_closed_before_arrow_wraps(tmp_path):
    maximum = Decimal("9" * 38)
    schema = pa.schema([
        ("group", pa.int64()),
        ("value", pa.decimal128(38, 0)),
    ])
    table = pa.table({"group": [1, 1], "value": [maximum, maximum]}, schema=schema)

    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        with pytest.raises(SpillNumericOverflow, match="potentially wrapping"):
            external_group_aggregate(
                _batches(table, 2),
                schema=schema,
                group_keys=["group"],
                aggregates=[AggregateSpec("total", "sum", "value")],
                session=session,
                memory_budget_bytes=256 * 1024,
            )
        assert session.used_bytes == 0
