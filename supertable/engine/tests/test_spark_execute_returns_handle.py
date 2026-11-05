# route: supertable.engine.tests.test_spark_execute_returns_handle
"""``SparkThriftExecutor.execute`` must hand its stream handle to the caller.

The streaming branch returned a ``SparkStreamHandle`` from inside ``try`` and
the ``finally`` block then ran a bare ``return`` to skip its teardown. A
``return`` in ``finally`` replaces the value already being returned, so every
streamed Spark read came back as ``None``: ``Executor.execute`` materialized
that into an empty DataFrame under ``Status.OK``, and ``Executor.stream``
handed the reader no stream at all. Every query routed to Spark — forced, or
AUTO above the fleet's ``min_bytes`` — was affected.

``test_spark_streaming.py`` covers ``SparkStreamHandle`` on its own and so
could not see this; these tests drive ``execute`` itself, with a stub Thrift
connection standing in for PyHive.
"""

from __future__ import annotations

import socket
import threading
import time
from importlib import import_module
from types import SimpleNamespace

import pyarrow as pa
import pytest

spark = import_module("supertable.engine.spark_thrift")
executor_mod = import_module("supertable.engine.executor")

DESC = [("id", "BIGINT_TYPE"), ("name", "STRING_TYPE")]
ROWS = [(1, "a"), (2, "b"), (3, "c")]
USER_SQL = "SELECT id, name FROM t"


class _Cursor:
    """Enough of a PyHive cursor for one query: EXPLAIN, then the SELECT."""

    def __init__(self):
        self.executed = []
        self.closed = False
        self._rows = []
        self.description = None

    def execute(self, sql):
        self.executed.append(sql)
        if sql.startswith("EXPLAIN"):
            self._rows, self.description = [("== Physical Plan ==",)], [("plan", "STRING_TYPE")]
        elif sql.startswith("SELECT"):
            self._rows, self.description = list(ROWS), DESC

    def fetchall(self):
        out, self._rows = self._rows, []
        return out

    def fetchmany(self, size):
        out, self._rows = self._rows[:size], self._rows[size:]
        return out

    def cancel(self):
        pass

    def close(self):
        self.closed = True


class _TSocket:
    """The part of thrift's TSocket the executor touches: ``handle``, ``close``."""

    def __init__(self, sock):
        self.handle = sock

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class _Conn:
    def __init__(self, tsocket=None):
        self.cur = _Cursor()
        self.closed = False
        self.closed_by = None
        # PyHive's auth NONE wraps the TSocket in a TSaslClientTransport,
        # which keeps it as ``_trans``.
        self._transport = SimpleNamespace(_trans=tsocket)

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True
        self.closed_by = threading.current_thread()


def _spark_executor(monkeypatch, tsocket=None):
    conn = _Conn(tsocket)
    ex = spark.SparkThriftExecutor.__new__(spark.SparkThriftExecutor)
    ex.storage = None
    ex.organization = "org"
    ex.catalog = SimpleNamespace(
        select_spark_cluster=lambda *_a, **_k: {
            "cluster_id": "c1", "thrift_host": "spark.invalid", "thrift_port": 10000,
        },
    )
    monkeypatch.setattr(ex, "_get_connection", lambda _cluster: conn)
    monkeypatch.setattr(spark, "_configure_spark_s3", lambda *_a, **_k: None)
    return ex, conn


def _query():
    reflection = SimpleNamespace(
        reflection_bytes=0, supers=[], rbac_views={}, tombstone_views={},
    )
    parser = SimpleNamespace(
        get_table_tuples=lambda: [], original_query=USER_SQL,
        executing_query=None,
    )
    return reflection, parser


def test_streamed_execute_returns_the_handle(monkeypatch):
    ex, conn = _spark_executor(monkeypatch)
    reflection, parser = _query()

    handle = ex.execute(
        reflection=reflection, parser=parser, query_manager=None,
        timer_capture=lambda _evt: None, stream_batch_rows=2,
    )

    assert isinstance(handle, spark.SparkStreamHandle), (
        f"execute() returned {handle!r} instead of the stream handle"
    )
    # The handle owns the connection: execute() must not have torn it down.
    assert not conn.closed and not conn.cur.closed
    batches = list(handle.batches())
    assert sum(b.num_rows for b in batches) == len(ROWS)
    assert pa.Table.from_batches(batches).column("id").to_pylist() == [1, 2, 3]
    # A drained handle releases the connection itself.
    assert conn.closed


def test_buffered_execute_still_tears_down(monkeypatch):
    ex, conn = _spark_executor(monkeypatch)
    reflection, parser = _query()

    frame = ex.execute(
        reflection=reflection, parser=parser, query_manager=None,
        timer_capture=lambda _evt: None,
    )

    assert frame.height == len(ROWS)
    assert conn.closed and conn.cur.closed


def test_executor_materializes_spark_rows(monkeypatch):
    """The user-visible symptom: a Spark-routed read returned zero rows."""
    ex, _conn = _spark_executor(monkeypatch)
    reflection, parser = _query()

    executor = executor_mod.Executor.__new__(executor_mod.Executor)
    executor.organization = "org"
    executor.spark_exec = ex
    monkeypatch.setattr(executor_mod, "resolve_engine_configs",
                        lambda *_a, **_k: {"lite": None, "pro": None})
    monkeypatch.setattr(executor, "_get_catalog", lambda: None, raising=False)
    plan_stats = SimpleNamespace(add_stat=lambda _s: None)
    timer = SimpleNamespace(capture_and_reset_timing=lambda _e: None)

    df, used = executor.execute(
        engine=executor_mod.Engine.SPARK_SQL, reflection=reflection,
        parser=parser, query_manager=None, timer=timer,
        plan_stats=plan_stats, log_prefix="",
    )

    assert used == "spark_sql"
    assert df.height == len(ROWS), f"Spark read came back with {df.height} rows"


def test_variable_substitution_is_off_before_any_user_sql(monkeypatch):
    """${spark.hadoop.fs.s3a.secret.key} must not expand inside user SQL."""
    ex, conn = _spark_executor(monkeypatch)
    reflection, parser = _query()

    ex.execute(reflection=reflection, parser=parser, query_manager=None,
               timer_capture=lambda _evt: None)

    executed = conn.cur.executed
    off = executed.index("SET spark.sql.variable.substitute=false")
    assert off < executed.index("SELECT id, name FROM t")


def test_no_query_runs_when_substitution_cannot_be_turned_off(monkeypatch):
    ex, conn = _spark_executor(monkeypatch)
    reflection, parser = _query()
    plain = conn.cur.execute

    def refuse(sql):
        if sql.startswith("SET spark.sql.variable.substitute"):
            raise RuntimeError("conf is not modifiable")
        plain(sql)

    conn.cur.execute = refuse

    with pytest.raises(RuntimeError, match="not modifiable"):
        ex.execute(reflection=reflection, parser=parser, query_manager=None,
                   timer_capture=lambda _evt: None)
    assert not any(sql.startswith("SELECT") for sql in conn.cur.executed)


class _MetadataFailsCursor(_Cursor):
    """GetResultSetMetadata fails once the user's SELECT has run."""

    @property
    def description(self):
        if self.executed and self.executed[-1] == USER_SQL:
            raise RuntimeError("GetResultSetMetadata: TTransportException")
        return self._description

    @description.setter
    def description(self, value):
        self._description = value


def test_session_is_torn_down_when_the_handle_cannot_be_built(monkeypatch):
    """Building the handle reads the result's metadata over Thrift. The
    executor marked the session as the handle's before that, so a failure
    there left the session, its cursor and its views open with no owner."""
    ex, conn = _spark_executor(monkeypatch)
    conn.cur = _MetadataFailsCursor()
    reflection, parser = _query()

    with pytest.raises(RuntimeError, match="GetResultSetMetadata"):
        ex.execute(reflection=reflection, parser=parser, query_manager=None,
                   timer_capture=lambda _evt: None, stream_batch_rows=2)
    assert conn.cur.closed and conn.closed


class _BlockingCursor(_Cursor):
    """The user's SELECT waits on the socket, as PyHive's ExecuteStatement
    does for its reply, and fails the way PyHive does once it is shut down."""

    def __init__(self, sock):
        super().__init__()
        self._sock = sock

    def execute(self, sql):
        super().execute(sql)
        if sql == USER_SQL and self._sock.recv(1) == b"":
            raise EOFError("TSocket read 0 bytes")


@pytest.mark.parametrize("which", ["statement", "query"])
def test_a_timeout_interrupts_the_running_statement(monkeypatch, which):
    """Both timers closed the connection, and PyHive's close() sends
    CloseSession over the client the statement is blocked in. The server
    answers that only after the statement, so the timeout fired when the
    statement had finished anyway. Both shut the socket down now."""
    ours, theirs = socket.socketpair()
    tsocket = _TSocket(ours)
    ex, conn = _spark_executor(monkeypatch, tsocket)
    conn.cur = _BlockingCursor(ours)
    short, long_ = (lambda: 0.3), (lambda: 30)
    monkeypatch.setattr(spark, "_spark_statement_timeout_seconds",
                        short if which == "statement" else long_)
    monkeypatch.setattr(spark, "_spark_timeout_seconds",
                        short if which == "query" else long_)
    reflection, parser = _query()
    outcome = {}

    def run():
        try:
            ex.execute(reflection=reflection, parser=parser, query_manager=None,
                       timer_capture=lambda _evt: None)
        except Exception as e:
            outcome["error"] = e

    caller = threading.Thread(target=run, daemon=True)
    started = time.monotonic()
    caller.start()
    caller.join(3)
    try:
        assert not caller.is_alive(), "the timeout did not interrupt the statement"
        assert time.monotonic() - started < 3
        assert "timed out" in str(outcome.get("error")), outcome
        # Only the teardown closes the connection, on the caller's thread.
        assert conn.closed_by is caller
        assert tsocket.handle is None
    finally:
        theirs.close()
