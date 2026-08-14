from __future__ import annotations

import threading

import pyarrow as pa
import pytest

from supertable.engine.island_spill import (
    AggregateSpec,
    SpillBudgetExceeded,
    SpillCancelled,
    SpillMemoryLimitExceeded,
    SpillSession,
    external_group_aggregate,
)


MIB = 1024 * 1024


def _batches(table: pa.Table, rows: int):
    return table.to_batches(max_chunksize=rows)


def test_multikey_null_groups_remain_exact_through_compact_spill(tmp_path):
    rows = 12_000
    first = [None if index % 101 == 0 else index % 173 for index in range(rows)]
    second = [None if index % 83 == 0 else index % 79 for index in range(rows)]
    values = [None if index % 7 == 0 else index - 6_000 for index in range(rows)]
    table = pa.table(
        {"first": first, "second": second, "value": values},
        schema=pa.schema([
            ("first", pa.int64()),
            ("second", pa.int32()),
            ("value", pa.int64()),
        ]),
    )
    expected = {}
    for key_a, key_b, value in zip(first, second, values):
        state = expected.setdefault(
            (key_a, key_b),
            {"rows": 0, "count": 0, "total": None},
        )
        state["rows"] += 1
        if value is not None:
            state["count"] += 1
            state["total"] = value if state["total"] is None else state["total"] + value

    with SpillSession(tmp_path, budget_bytes=64 * MIB, min_free_bytes=0) as session:
        result = external_group_aggregate(
            _batches(table, 211),
            schema=table.schema,
            group_keys=["first", "second"],
            aggregates=[
                AggregateSpec("rows", "count_star"),
                AggregateSpec("count", "count", "value"),
                AggregateSpec("total", "sum", "value"),
            ],
            session=session,
            memory_budget_bytes=256 * 1024,
            output_batch_rows=97,
            max_open_runs=2,
        ).collect_table(max_bytes=8 * MIB)
        assert session.peak_used_bytes > 0
        assert session.used_bytes == 0

    actual = {
        (row["first"], row["second"]): row
        for row in result.to_pylist()
    }
    assert set(actual) == set(expected)
    for key, state in expected.items():
        assert actual[key]["rows"] == state["rows"]
        assert actual[key]["count"] == state["count"]
        assert (
            None
            if actual[key]["total"] is None
            else int(actual[key]["total"])
        ) == state["total"]


def test_single_oversized_variable_state_fails_before_native_hash(tmp_path):
    table = pa.table({"group": [1], "payload": [b"x" * (200 * 1024)]})
    with SpillSession(tmp_path, budget_bytes=4 * MIB, min_free_bytes=0) as session:
        with pytest.raises(
            SpillMemoryLimitExceeded,
            match="one aggregate row/state",
        ):
            external_group_aggregate(
                _batches(table, 1),
                schema=table.schema,
                group_keys=["group"],
                aggregates=[AggregateSpec("maximum", "max", "payload")],
                session=session,
                memory_budget_bytes=256 * 1024,
            )
        assert session.used_bytes == 0


def test_group_cancellation_stops_scan_and_reclaims_partials(tmp_path):
    table = pa.table({"group": list(range(20_000))})
    cancelled = threading.Event()

    def cancel_during_scan():
        for index, batch in enumerate(_batches(table, 100)):
            if index == 10:
                cancelled.set()
            yield batch

    with SpillSession(
        tmp_path,
        budget_bytes=32 * MIB,
        min_free_bytes=0,
        cancel_event=cancelled,
    ) as session:
        with pytest.raises(SpillCancelled):
            external_group_aggregate(
                cancel_during_scan(),
                schema=table.schema,
                group_keys=["group"],
                aggregates=[],
                session=session,
                memory_budget_bytes=256 * 1024,
            )
        assert session.used_bytes == 0


def test_group_quota_failure_reclaims_every_partial(tmp_path):
    table = pa.table({"group": list(range(20_000))})
    with SpillSession(tmp_path, budget_bytes=1024, min_free_bytes=0) as session:
        with pytest.raises(SpillBudgetExceeded):
            external_group_aggregate(
                _batches(table, 100),
                schema=table.schema,
                group_keys=["group"],
                aggregates=[],
                session=session,
                memory_budget_bytes=256 * 1024,
            )
        assert session.used_bytes == 0
