# route: supertable.engine.tests.test_spark_streaming
"""Spark streaming: fetchmany instead of fetchall, converted to Arrow.

PyHive is not a dependency of the SDK, so these drive ``SparkStreamHandle``
with a stub cursor that implements the DB-API surface it actually uses —
``description``, ``fetchmany``, ``cancel``, ``execute``, ``close`` — and hands
back the values PyHive does: ``str`` for a DATE, ``Decimal`` for a DECIMAL,
``datetime`` for a TIMESTAMP, ``str`` for ARRAY, MAP, STRUCT and intervals.

The conversion, batching, schema stability, cancellation and teardown logic
are covered here; the Thrift round trip itself is not.
"""

from __future__ import annotations

import datetime
import sys
from decimal import Decimal
from importlib import import_module
from types import ModuleType, SimpleNamespace

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


def _desc(*cols):
    """A PyHive-shaped description: (name, type, None, None, p, s, True)."""
    out = []
    for col in cols:
        name, type_name, *ps = col
        precision, scale = ps if ps else (None, None)
        out.append((name, type_name, None, None, precision, scale, True))
    return out


def test_every_column_has_a_type_before_the_first_batch():
    """Spark sends ARRAY, MAP, STRUCT and intervals as text, and NULL as
    nulls. The schema must exist before any row does: a consumer writes it
    first (an Arrow IPC export), and Core rejects a stream without one."""
    desc = _desc(("arr", "ARRAY_TYPE"), ("m", "MAP_TYPE"), ("st", "STRUCT_TYPE"),
                 ("iv", "INTERVAL_DAY_TIME_TYPE"), ("ym", "INTERVAL_YEAR_MONTH_TYPE"),
                 ("u", "USER_DEFINED_TYPE"), ("n", "NULL_TYPE"), ("x", "SOMETHING_NEW"))
    handle, _, _ = _handle(rows=[("[1,2]", '{"k":1}', '{"a":1}', "1 00:00:00.000000000",
                                  "1-0", "u", None, "x")], desc=desc, batch_rows=10)
    assert handle.schema == pa.schema(
        [pa.field(n, pa.string()) for n in ("arr", "m", "st", "iv", "ym", "u")]
        + [pa.field("n", pa.null()), pa.field("x", pa.string())])
    table = pa.Table.from_batches(list(handle.batches()), schema=handle.schema)
    assert table.to_pylist() == [{"arr": "[1,2]", "m": '{"k":1}', "st": '{"a":1}',
                                  "iv": "1 00:00:00.000000000", "ym": "1-0",
                                  "u": "u", "n": None, "x": "x"}]


def test_a_text_column_that_starts_all_null_keeps_its_type():
    """Inferring from the first batch typed this column null, and the next
    batch, with a value in it, then failed: ArrowInvalid, Invalid null value."""
    desc = _desc(("s", "STRING_TYPE"), ("a", "ARRAY_TYPE"))
    handle, _, _ = _handle(rows=[(None, None), ("x", "[2]")], desc=desc, batch_rows=1)
    batches = list(handle.batches())
    assert [b.schema for b in batches] == [handle.schema] * 2
    assert pa.Table.from_batches(batches).column("a").to_pylist() == [None, "[2]"]


def test_values_as_pyhive_returns_them():
    """PyHive converts only DECIMAL (to Decimal) and TIMESTAMP (to datetime);
    a DATE comes through as its text. pa.array(['2024-01-01'], date32) raises,
    so every Spark read that returned a date failed."""
    desc = _desc(("d", "DATE_TYPE"), ("ts", "TIMESTAMP_TYPE"), ("x", "DECIMAL_TYPE", 10, 2),
                 ("big", "DECIMAL_TYPE", 38, 0), ("frac", "DECIMAL_TYPE", 38, 20),
                 ("f", "FLOAT_TYPE"), ("ti", "TINYINT_TYPE"), ("b", "BOOLEAN_TYPE"),
                 ("bin", "BINARY_TYPE"))
    rows = [
        ("2024-01-01", datetime.datetime(2024, 1, 1, 10, 0, 0, 123456), Decimal("1.25"),
         Decimal("123456789012345678901"), Decimal("0.12345678901234567890"),
         1.5, 1, True, b"\x01\x02"),
        (None, None, None, None, None, None, None, None, None),
    ]
    handle, _, _ = _handle(rows=rows, desc=desc, batch_rows=10)
    assert handle.schema == pa.schema([
        pa.field("d", pa.date32()), pa.field("ts", pa.timestamp("us")),
        pa.field("x", pa.decimal128(10, 2)), pa.field("big", pa.decimal128(38, 0)),
        pa.field("frac", pa.decimal128(38, 20)), pa.field("f", pa.float32()),
        pa.field("ti", pa.int8()), pa.field("b", pa.bool_()), pa.field("bin", pa.binary()),
    ])
    table = pa.Table.from_batches(list(handle.batches()), schema=handle.schema)
    first, second = table.to_pylist()
    assert first == {
        "d": datetime.date(2024, 1, 1),
        "ts": datetime.datetime(2024, 1, 1, 10, 0, 0, 123456),
        "x": Decimal("1.25"), "big": Decimal("123456789012345678901"),
        "frac": Decimal("0.12345678901234567890"), "f": 1.5, "ti": 1, "b": True,
        "bin": b"\x01\x02",
    }
    assert set(second.values()) == {None}


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
    """A DECIMAL is its own precision and scale. Forcing decimal128(38, 18)
    failed any value with more than 20 integer or 18 fraction digits, and
    turned DECIMAL(10,2) into Decimal(38,18)."""
    schema = spark._spark_arrow_schema(_desc(("d", "DECIMAL_TYPE", 10, 2)))
    assert schema.field("d").type == pa.decimal128(10, 2)


def test_decimal_without_precision_and_scale_is_refused():
    with pytest.raises(RuntimeError, match="no precision and scale for DECIMAL column 'd'"):
        spark._spark_arrow_schema([("d", "DECIMAL_TYPE")])


class _ResultSetMetadata:
    """The GetResultSetMetadata reply PyHive's client returns."""

    def __init__(self, qualifiers):
        def column(quals):
            values = {k: SimpleNamespace(i32Value=v) for k, v in quals.items()}
            entry = SimpleNamespace(typeQualifiers=SimpleNamespace(qualifiers=values)
                                    if quals else None)
            return SimpleNamespace(typeDesc=SimpleNamespace(types=[
                SimpleNamespace(primitiveEntry=entry)]))
        self.status = SimpleNamespace(statusCode=0, errorMessage=None)
        self.schema = SimpleNamespace(columns=[column(q) for q in qualifiers])


def test_precision_and_scale_come_from_the_result_metadata(monkeypatch):
    """PyHive's description has the type name only; the precision and scale
    are in the qualifiers of the GetResultSetMetadata reply, read again."""
    ttypes = ModuleType("TCLIService.ttypes")
    ttypes.TGetResultSetMetadataReq = lambda handle: ("req", handle)
    ttypes.TStatusCode = SimpleNamespace(SUCCESS_STATUS=0)
    package = ModuleType("TCLIService")
    package.ttypes = ttypes
    monkeypatch.setitem(sys.modules, "TCLIService", package)
    monkeypatch.setitem(sys.modules, "TCLIService.ttypes", ttypes)

    requests = []

    class _Client:
        def GetResultSetMetadata(self, req):
            requests.append(req)
            return _ResultSetMetadata([{}, {"precision": 10, "scale": 2}])

    cursor = SimpleNamespace(
        description=[("id", "BIGINT_TYPE", None, None, None, None, True),
                     ("amount", "DECIMAL_TYPE", None, None, None, None, True)],
        _operationHandle="op-1")
    connection = SimpleNamespace(client=_Client())

    description = spark._spark_description(cursor, connection)
    assert requests == [("req", "op-1")]
    assert description[1] == ("amount", "DECIMAL_TYPE", None, None, 10, 2, True)
    assert spark._spark_arrow_schema(description).field("amount").type == pa.decimal128(10, 2)


def test_result_metadata_is_not_read_again_without_a_decimal():
    class _NoCalls:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected call: {name}")

    cursor = SimpleNamespace(description=DESC, _operationHandle="op-1")
    assert spark._spark_description(cursor, SimpleNamespace(client=_NoCalls())) == DESC
