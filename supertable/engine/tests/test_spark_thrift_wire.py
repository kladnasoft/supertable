# route: supertable.engine.tests.test_spark_thrift_wire
"""Spark Thrift over PyHive's real client, against a fake HiveServer2.

The stub-cursor tests cannot show what happens on the wire. PyHive's Thrift
client has no lock, and HiveServer2 answers one connection's requests in
order, so a call made from a second thread waits behind the one in flight.
Here PyHive's own ``TCLIService.Processor`` serves an ephemeral 127.0.0.1
port, one thread per connection taking its requests in turn, over NOSASL and
over the SASL PLAIN framing PyHive uses for auth NONE, the executor's default.
Result values go over the wire as Spark sends them: DATE, DECIMAL and
TIMESTAMP as text, which PyHive converts (or not) on the way in.

Skipped where PyHive is not installed: the SDK does not depend on it, the
service that runs Spark queries does.
"""

from __future__ import annotations

import struct
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from importlib import import_module
from io import BytesIO
from types import SimpleNamespace

import polars as pl
import pyarrow as pa
import pytest

pytest.importorskip("pyhive.hive")
pytest.importorskip("thrift_sasl")

from TCLIService import TCLIService, ttypes as T  # noqa: E402
from thrift.protocol import TBinaryProtocol  # noqa: E402
from thrift.transport import TSocket, TTransport  # noqa: E402

spark = import_module("supertable.engine.spark_thrift")
executor_mod = import_module("supertable.engine.executor")

OK = T.TStatus(statusCode=T.TStatusCode.SUCCESS_STATUS)
USER_SQL = "SELECT v FROM t"

_TYPED_COLUMNS = {
    T.TTypeId.BOOLEAN_TYPE: ("boolVal", T.TBoolColumn, False),
    T.TTypeId.TINYINT_TYPE: ("byteVal", T.TByteColumn, 0),
    T.TTypeId.SMALLINT_TYPE: ("i16Val", T.TI16Column, 0),
    T.TTypeId.INT_TYPE: ("i32Val", T.TI32Column, 0),
    T.TTypeId.BIGINT_TYPE: ("i64Val", T.TI64Column, 0),
    T.TTypeId.FLOAT_TYPE: ("doubleVal", T.TDoubleColumn, 0.0),
    T.TTypeId.DOUBLE_TYPE: ("doubleVal", T.TDoubleColumn, 0.0),
    T.TTypeId.BINARY_TYPE: ("binaryVal", T.TBinaryColumn, b""),
}


def _column(type_id, values):
    """One result column the way Spark encodes it: every type without a typed
    Thrift column goes as text."""
    attr, cls, blank = _TYPED_COLUMNS.get(type_id, ("stringVal", T.TStringColumn, ""))
    nulls = bytearray((len(values) + 7) // 8)
    wire = []
    for i, value in enumerate(values):
        if value is None:
            nulls[i // 8] |= 1 << (i % 8)
        wire.append(blank if value is None else value)
    return T.TColumn(**{attr: cls(values=wire, nulls=bytes(nulls))})


class _Spark:
    """HiveServer2's side of one scripted query.

    ``columns`` are ``(name, TTypeId, qualifiers)``; ``rows`` hold wire values.
    The user's SELECT takes ``execute_delay`` to run and each FetchResults of
    its rows ``fetch_delay`` to answer.
    """

    def __init__(self, columns, rows, execute_delay=0.0, fetch_delay=0.0):
        self.columns, self.rows = columns, rows
        self.execute_delay, self.fetch_delay = execute_delay, fetch_delay
        self.calls = []
        self.fetching = threading.Event()
        self.disconnected = threading.Event()
        self._results = {}
        self._next = 0

    def _operation(self, result):
        self._next += 1
        guid = self._next.to_bytes(16, "big")
        if result is not None:
            self._results[guid] = [*result, 0]
        return T.TOperationHandle(
            operationId=T.THandleIdentifier(guid=guid, secret=b"s" * 16),
            operationType=T.TOperationType.EXECUTE_STATEMENT,
            hasResultSet=result is not None)

    def OpenSession(self, req):
        self.calls.append("OpenSession")
        return T.TOpenSessionResp(
            status=OK, serverProtocolVersion=req.client_protocol,
            sessionHandle=T.TSessionHandle(
                sessionId=T.THandleIdentifier(guid=b"g" * 16, secret=b"s" * 16)))

    def ExecuteStatement(self, req):
        sql = req.statement
        self.calls.append(f"ExecuteStatement {sql}")
        if sql.startswith("EXPLAIN"):
            result = ([("plan", T.TTypeId.STRING_TYPE, None)], [("== Physical Plan ==",)], False)
        elif sql == USER_SQL:
            time.sleep(self.execute_delay)
            result = (self.columns, self.rows, True)
        else:
            result = None
        return T.TExecuteStatementResp(status=OK, operationHandle=self._operation(result))

    def GetResultSetMetadata(self, req):
        self.calls.append("GetResultSetMetadata")
        columns = self._results[req.operationHandle.operationId.guid][0]
        descs = []
        for position, (name, type_id, qualifiers) in enumerate(columns, 1):
            quals = None
            if qualifiers:
                quals = T.TTypeQualifiers(qualifiers={
                    k: T.TTypeQualifierValue(i32Value=v) for k, v in qualifiers.items()})
            descs.append(T.TColumnDesc(
                columnName=name, position=position, typeDesc=T.TTypeDesc(types=[
                    T.TTypeEntry(primitiveEntry=T.TPrimitiveTypeEntry(
                        type=type_id, typeQualifiers=quals))])))
        return T.TGetResultSetMetadataResp(status=OK, schema=T.TTableSchema(columns=descs))

    def FetchResults(self, req):
        self.calls.append("FetchResults")
        result = self._results[req.operationHandle.operationId.guid]
        columns, rows, slow, offset = result
        if slow:
            self.fetching.set()
            time.sleep(self.fetch_delay)
        chunk = rows[offset:offset + req.maxRows]
        result[3] = offset + len(chunk)
        return T.TFetchResultsResp(status=OK, hasMoreRows=False, results=T.TRowSet(
            startRowOffset=offset, rows=[],
            columns=[_column(c[1], [r[i] for r in chunk]) for i, c in enumerate(columns)]))

    def CancelOperation(self, req):
        self.calls.append("CancelOperation")
        return T.TCancelOperationResp(status=OK)

    def CloseOperation(self, req):
        self.calls.append("CloseOperation")
        return T.TCloseOperationResp(status=OK)

    def CloseSession(self, req):
        self.calls.append("CloseSession")
        return T.TCloseSessionResp(status=OK)


class _SaslServerTransport(TTransport.TTransportBase):
    """The server half of thrift_sasl's PLAIN framing: a handshake, then
    length-prefixed frames."""

    def __init__(self, trans):
        self._trans, self._rbuf, self._wbuf, self._ready = trans, BytesIO(b""), BytesIO(), False

    def isOpen(self):
        return self._trans.isOpen()

    def close(self):
        self._trans.close()

    def read(self, sz):
        if not self._ready:
            for _ in range(2):  # START with the mechanism, OK with the credentials
                _status, length = struct.unpack(">BI", self._trans.readAll(5))
                self._trans.readAll(length)
            self._trans.write(struct.pack(">BI", 5, 0))  # COMPLETE
            self._trans.flush()
            self._ready = True
        data = self._rbuf.read(sz)
        if data:
            return data
        (length,) = struct.unpack(">I", self._trans.readAll(4))
        self._rbuf = BytesIO(self._trans.readAll(length))
        return self._rbuf.read(sz)

    def write(self, buf):
        self._wbuf.write(buf)

    def flush(self):
        data, self._wbuf = self._wbuf.getvalue(), BytesIO()
        self._trans.write(struct.pack(">I", len(data)) + data)
        self._trans.flush()


@pytest.fixture()
def hive_server():
    """Start a fake HiveServer2; yields ``start(script, auth) -> cluster``."""
    servers = []

    def start(script, auth):
        listener = TSocket.TServerSocket(host="127.0.0.1", port=0)
        listener.listen()
        processor = TCLIService.Processor(script)

        def serve(client):
            trans = (_SaslServerTransport(client) if auth == "NONE"
                     else TTransport.TBufferedTransport(client))
            protocol = TBinaryProtocol.TBinaryProtocol(trans)
            try:
                while True:
                    processor.process(protocol, protocol)
            except Exception:
                pass
            finally:
                script.disconnected.set()
                trans.close()

        def accept():
            while True:
                try:
                    client = listener.accept()
                except Exception:
                    return
                threading.Thread(target=serve, args=(client,), daemon=True).start()

        threading.Thread(target=accept, daemon=True).start()
        servers.append(listener)
        return {"cluster_id": "fake", "thrift_host": "127.0.0.1",
                "thrift_port": listener.handle.getsockname()[1], "auth": auth}

    yield start
    for listener in servers:
        listener.close()


def _spark_executor(cluster):
    ex = spark.SparkThriftExecutor.__new__(spark.SparkThriftExecutor)
    ex.storage = None
    ex.organization = "org"
    ex.catalog = SimpleNamespace(select_spark_cluster=lambda *_a, **_k: dict(cluster))
    return ex


def _query():
    reflection = SimpleNamespace(reflection_bytes=0, supers=[], rbac_views={},
                                 tombstone_views={})
    parser = SimpleNamespace(get_table_tuples=lambda: [], original_query=USER_SQL,
                             executing_query=None)
    return reflection, parser


def _stream(cluster, batch_rows):
    reflection, parser = _query()
    return _spark_executor(cluster).execute(
        reflection=reflection, parser=parser, query_manager=None,
        timer_capture=lambda _evt: None, stream_batch_rows=batch_rows)


def _in_thread(fn):
    """Run *fn* on a thread of its own, so a call that hangs fails the test
    instead of hanging the run. Returns ``(thread, outcome)``."""
    outcome = {}

    def run():
        try:
            outcome["value"] = fn()
        except Exception as e:
            outcome["error"] = e

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


BIGINTS = [("v", T.TTypeId.BIGINT_TYPE, None)]


@pytest.mark.parametrize("auth", ["NOSASL", "NONE"])
def test_cancel_from_another_thread_ends_a_blocked_fetch(hive_server, auth):
    """The release candidate sent CancelOperation here. Over NOSASL both
    threads then hung for good, holding the teardown lock; over SASL the
    reader hung, or took the cancel's reply as its own."""
    script = _Spark(BIGINTS, [(i,) for i in range(6000)], fetch_delay=0.5)
    handle = _stream(hive_server(script, auth), batch_rows=6000)
    reader, outcome = _in_thread(lambda: sum(b.num_rows for b in handle.batches()))
    assert script.fetching.wait(3)
    time.sleep(0.1)

    canceller, _ = _in_thread(handle.cancel)
    canceller.join(0.5)
    assert not canceller.is_alive(), "cancel() blocked"
    reader.join(2)

    assert not reader.is_alive(), "the blocked fetch was not interrupted"
    assert "interrupted by cancel()" in str(outcome.get("error")), outcome
    assert "CancelOperation" not in script.calls
    assert script.disconnected.wait(3), "the server never saw the connection go"


def test_a_fetch_that_overruns_the_statement_timeout_is_stopped(hive_server, monkeypatch):
    monkeypatch.setattr(spark, "_spark_statement_timeout_seconds", lambda: 0.5)
    script = _Spark(BIGINTS, [(1,)], fetch_delay=3)
    handle = _stream(hive_server(script, "NONE"), batch_rows=100)

    reader, outcome = _in_thread(lambda: list(handle.batches()))
    reader.join(2)
    assert not reader.is_alive(), "the stalled fetch was not stopped"
    assert "fetch timed out after 0.5s" in str(outcome.get("error")), outcome
    # The server notices once its 3 s FetchResults is done.
    assert script.disconnected.wait(5)


@pytest.mark.parametrize("which", ["statement", "query"])
def test_a_timeout_ends_a_running_statement(hive_server, monkeypatch, which):
    """On the release candidate the timers closed the connection, whose
    CloseSession the server answers only after the statement: a 0.5 s
    timeout on a 5 s statement fired after 5 s."""
    monkeypatch.setattr(spark, "_spark_statement_timeout_seconds",
                        (lambda: 0.5) if which == "statement" else (lambda: 60))
    monkeypatch.setattr(spark, "_spark_timeout_seconds",
                        (lambda: 0.5) if which == "query" else (lambda: 60))
    script = _Spark(BIGINTS, [(1,)], execute_delay=5)
    cluster = hive_server(script, "NONE")
    reflection, parser = _query()

    caller, outcome = _in_thread(lambda: _spark_executor(cluster).execute(
        reflection=reflection, parser=parser, query_manager=None,
        timer_capture=lambda _evt: None))
    caller.join(3)
    assert not caller.is_alive(), "the timeout did not end the statement"
    assert "timed out" in str(outcome.get("error")), outcome


TYPED = [
    ("d", T.TTypeId.DATE_TYPE, None),
    ("ts", T.TTypeId.TIMESTAMP_TYPE, None),
    ("x", T.TTypeId.DECIMAL_TYPE, {"precision": 10, "scale": 2}),
    ("big", T.TTypeId.DECIMAL_TYPE, {"precision": 38, "scale": 0}),
    ("frac", T.TTypeId.DECIMAL_TYPE, {"precision": 38, "scale": 20}),
    ("arr", T.TTypeId.ARRAY_TYPE, None),
    ("n", T.TTypeId.NULL_TYPE, None),
    ("i", T.TTypeId.BIGINT_TYPE, None),
]
TYPED_ROWS = [
    ("2024-01-01", "2024-01-01 10:00:00.123456", "1.25", "123456789012345678901",
     "0.12345678901234567890", "[1,2]", None, 7),
    (None, None, None, None, None, None, None, None),
]


def test_result_types_arrive_as_their_own_types(hive_server):
    """A DATE failed every batch it was in, and every DECIMAL was forced to
    decimal128(38, 18), which cannot hold DECIMAL(38,0)'s 21 digits or
    DECIMAL(38,20)'s 20 decimals. ARRAY and NULL left the schema unknown."""
    script = _Spark(TYPED, TYPED_ROWS)
    handle = _stream(hive_server(script, "NONE"), batch_rows=100)

    assert handle.schema == pa.schema([
        ("d", pa.date32()), ("ts", pa.timestamp("us")), ("x", pa.decimal128(10, 2)),
        ("big", pa.decimal128(38, 0)), ("frac", pa.decimal128(38, 20)),
        ("arr", pa.string()), ("n", pa.null()), ("i", pa.int64()),
    ])
    rows = pa.Table.from_batches(list(handle.batches()), schema=handle.schema).to_pylist()
    assert rows[0] == {
        "d": date(2024, 1, 1), "ts": datetime(2024, 1, 1, 10, 0, 0, 123456),
        "x": Decimal("1.25"), "big": Decimal("123456789012345678901"),
        "frac": Decimal("0.12345678901234567890"), "arr": "[1,2]", "n": None, "i": 7,
    }
    assert set(rows[1].values()) == {None}


def test_a_materialized_read_keeps_dates_and_decimals(hive_server, monkeypatch):
    """What Core's interactive query does: Executor.execute(engine=spark_sql)."""
    script = _Spark(TYPED[:3], [r[:3] for r in TYPED_ROWS])
    executor = executor_mod.Executor.__new__(executor_mod.Executor)
    executor.organization = "org"
    executor.spark_exec = _spark_executor(hive_server(script, "NONE"))
    executor._catalog = False
    monkeypatch.setattr(executor_mod, "resolve_engine_configs",
                        lambda *_a, **_k: {"lite": None, "pro": None})
    reflection, parser = _query()

    frame, used = executor.execute(
        engine=executor_mod.Engine.SPARK_SQL, reflection=reflection, parser=parser,
        query_manager=None, timer=SimpleNamespace(capture_and_reset_timing=lambda _e: None),
        plan_stats=SimpleNamespace(add_stat=lambda _s: None), log_prefix="")

    assert used == "spark_sql"
    assert frame.dtypes == [pl.Date, pl.Datetime("us"), pl.Decimal(10, 2)]
    assert frame.row(0) == (date(2024, 1, 1), datetime(2024, 1, 1, 10, 0, 0, 123456),
                            Decimal("1.25"))
    assert frame.row(1) == (None, None, None)
    assert "CloseSession" in script.calls
