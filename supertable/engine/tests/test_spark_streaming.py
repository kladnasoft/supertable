# route: supertable.engine.tests.test_spark_streaming
"""Spark streaming: fetchmany instead of fetchall, converted to Arrow.

There is no Spark Thrift server in this environment and PyHive is not
installed, so these drive ``SparkStreamHandle`` with a stub cursor that
implements the DB-API surface it actually uses — ``description``,
``fetchmany``, ``cancel``, ``execute``, ``close``.

That is a real limit worth stating: the conversion, batching, schema stability,
cancellation and teardown logic are covered here, but the Thrift round trip
itself is not. The first run against a live cluster is the real test of that.
"""

from __future__ import annotations

from importlib import import_module

import pyarrow as pa
import pytest

spark = import_module("supertable.engine.spark_thrift")
SparkStreamHandle = spark.SparkStreamHandle


class _StubCursor:
    """Just enough of a PyHive cursor."""

    def __init__(self, rows, description, fail_cancel=False):
        self._rows = list(rows)
        self.description = description
        self.executed = []
        self.closed = False
        self.cancelled = False
        self._fail_cancel = fail_cancel

    def fetchmany(self, size):
        out, self._rows = self._rows[:size], self._rows[size:]
        return out

    def execute(self, sql):
        self.executed.append(sql)

    def cancel(self):
        if self._fail_cancel:
            raise RuntimeError("thrift down")
        self.cancelled = True

    def close(self):
        self.closed = True


class _StubConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


DESC = [("t.a", "BIGINT_TYPE"), ("b", "STRING_TYPE"), ("c", "DOUBLE_TYPE")]
ROWS = [(i, f"s{i}", i * 1.5) for i in range(250)]


def _handle(rows=ROWS, desc=DESC, batch_rows=100, **kw):
    cur = _StubCursor(rows, desc, **kw)
    con = _StubConn()
    return SparkStreamHandle(cursor=cur, connection=con,
                             views=["v1"], tables=["t1"],
                             batch_rows=batch_rows), cur, con


def test_rows_are_pulled_in_batches_not_collected():
    """The whole point: bounded round trips, not one giant fetchall."""
    handle, _, _ = _handle(batch_rows=100)
    sizes = [b.num_rows for b in handle.batches()]
    assert sizes == [100, 100, 50], sizes


def test_values_survive_the_conversion():
    handle, _, _ = _handle(batch_rows=100)
    table = pa.Table.from_batches(list(handle.batches()))
    assert table.num_rows == len(ROWS)
    assert table.column("a").to_pylist() == [r[0] for r in ROWS]
    assert table.column("b").to_pylist() == [r[1] for r in ROWS]
    assert table.column("c").to_pylist() == [r[2] for r in ROWS]


def test_schema_comes_from_the_cursor_description():
    handle, _, _ = _handle()
    assert handle.schema == pa.schema([
        pa.field("a", pa.int64()),          # qualifier stripped
        pa.field("b", pa.string()),
        pa.field("c", pa.float64()),
    ])


def test_schema_is_stable_when_a_batch_is_all_null():
    """Per-batch inference would type this batch as null and refuse to
    concatenate with the next one. The schema is fixed up front instead."""
    rows = [(None, None, None)] * 50 + [(1, "x", 2.0)] * 50
    handle, _, _ = _handle(rows=rows, batch_rows=50)
    batches = list(handle.batches())
    assert len({b.schema for b in batches}) == 1
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 100


def test_unknown_column_type_falls_back_to_inference():
    """An unmappable Hive type must not fail every batch."""
    desc = [("a", "SOMETHING_NEW")]
    assert spark._spark_arrow_schema(desc) is None
    handle, _, _ = _handle(rows=[(1,), (2,)], desc=desc, batch_rows=10)
    batches = list(handle.batches())
    assert batches[0].num_rows == 2
    assert handle.schema is not None, "inferred schema must be frozen for reuse"


def test_close_drops_views_then_releases_the_connection():
    handle, cur, con = _handle()
    list(handle.batches())
    assert [s for s in cur.executed if "DROP VIEW" in s], cur.executed
    assert cur.closed and con.closed


def test_close_is_idempotent():
    handle, cur, con = _handle()
    handle.close()
    n = len(cur.executed)
    handle.close()
    assert len(cur.executed) == n


def test_cancel_reaches_the_server():
    handle, cur, _ = _handle()
    handle.cancel()
    assert cur.cancelled


def test_cancel_failure_is_not_fatal():
    """Cancelling a query that already finished must not raise at the caller."""
    handle, _, _ = _handle(fail_cancel=True)
    handle.cancel()          # must not raise


def test_rows_streamed_and_on_close_feed_monitoring():
    handle, _, _ = _handle(batch_rows=100)
    seen = []
    handle.on_close = lambda rows, cols: seen.append((rows, cols))
    list(handle.batches())
    assert handle.rows_streamed == len(ROWS)
    assert seen == [(len(ROWS), 3)]


def test_empty_result_closes_cleanly():
    handle, cur, con = _handle(rows=[], batch_rows=10)
    assert list(handle.batches()) == []
    assert cur.closed and con.closed


def test_decimal_columns_keep_their_own_type():
    """Hive reports DECIMAL with precision in the name; it must not silently
    become a float, which is how exact sums get corrupted."""
    schema = spark._spark_arrow_schema([("d", "DECIMAL_TYPE")])
    assert pa.types.is_decimal(schema.field("d").type)
