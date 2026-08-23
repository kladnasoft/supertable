from __future__ import annotations

import dataclasses
import importlib
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import supertable
from supertable import data_reader
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine import duckdb_engine
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import _storage_identity
from supertable.engine.islanddb import IslandExecutionTimeout
from supertable.engine.island_resources import (
    ArrowBatchStream,
    ResourceReservationCancelled,
)
from supertable.engine.plan_stats import PlanStats
from supertable.utils.sql_parser import SQLParser


executor_module = importlib.import_module("supertable.engine.executor")


def _reflection(path: str = "s3://bucket/events.parquet") -> Reflection:
    return Reflection(
        "s3",
        10,
        1,
        [SuperSnapshot(
            "shop",
            "events",
            1,
            [path],
            {"id"},
            resource_keys=["org/shop/events.parquet"],
        )],
    )


def test_top_level_exports_bounded_query_stream_and_release_version():
    assert supertable.query_sql_stream is data_reader.query_sql_stream
    assert (
        supertable.query_sql_policy_fingerprint
        is data_reader.query_sql_policy_fingerprint
    )
    assert "query_sql_stream" in supertable.__all__
    assert "query_sql_policy_fingerprint" in supertable.__all__
    assert supertable.__version__ == "2.5.1"


def test_export_computes_one_deadline_and_guards_size_before_parse(monkeypatch):
    reader = MagicMock()
    reader.execute_export_stream.return_value = (
        ArrowBatchStream.from_table(pa.table({"id": [1]})),
        data_reader.Status.OK,
        None,
    )
    monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)
    monkeypatch.setattr(data_reader.time, "monotonic", lambda: 100.0)

    stream = data_reader.query_sql_stream(
        "org",
        "shop",
        "SELECT id FROM events",
        Engine.DUCKDB,
        "reader",
        max_total_rows=10,
        timeout_sec=30,
    )
    stream.close()
    assert (
        reader.execute_export_stream.call_args.kwargs["_deadline_monotonic"]
        == 130.0
    )

    parser_trap = MagicMock(side_effect=AssertionError("parser was called"))
    monkeypatch.setattr(data_reader, "classify_query", parser_trap)
    monkeypatch.setattr(
        data_reader,
        "settings",
        SimpleNamespace(SUPERTABLE_MAX_QUERY_BYTES=8),
    )
    with pytest.raises(ValueError, match="query-size budget"):
        data_reader.query_sql_stream(
            "org",
            "shop",
            "SELECT id FROM events",
            Engine.DUCKDB,
            "reader",
            max_total_rows=10,
            timeout_sec=30,
        )
    parser_trap.assert_not_called()


def test_duckdb_pre_cancel_stops_before_parser_or_connection(monkeypatch):
    engine = duckdb_engine.DuckDB()
    parser_trap = MagicMock(side_effect=AssertionError("parser was called"))
    connection_trap = MagicMock(side_effect=AssertionError("connection opened"))
    monkeypatch.setattr(
        duckdb_engine, "_fresh_validated_duckdb_parser", parser_trap,
    )
    monkeypatch.setattr(engine, "_get_connection", connection_trap)
    cancelled = threading.Event()
    cancelled.set()

    with pytest.raises(ResourceReservationCancelled):
        engine.execute(
            _reflection(),
            MagicMock(),
            SimpleNamespace(temp_dir="/tmp"),
            lambda _event: None,
            cancel_event=cancelled,
            deadline_monotonic=time.monotonic() + 10,
        )
    parser_trap.assert_not_called()
    connection_trap.assert_not_called()


def test_duckdb_absolute_deadline_covers_httpfs_setup(monkeypatch):
    engine = duckdb_engine.DuckDB()
    cursor = MagicMock()
    root = MagicMock()
    root.cursor.return_value = cursor
    monkeypatch.setattr(engine, "_get_connection", lambda **_kwargs: root)

    def slow_setup(*_args, **_kwargs):
        time.sleep(0.03)

    monkeypatch.setattr(engine, "_ensure_httpfs", slow_setup)
    parser = SQLParser("shop", "SELECT id FROM events", "duckdb")

    with pytest.raises(TimeoutError, match="timed out"):
        engine.execute(
            _reflection(),
            parser,
            SimpleNamespace(temp_dir="/tmp"),
            lambda _event: None,
            timeout_sec=60,
            deadline_monotonic=time.monotonic() + 0.01,
        )
    cursor.interrupt.assert_called()
    cursor.close.assert_called()


def test_duckdb_absolute_deadline_interrupts_connection_initialization(
    monkeypatch,
):
    engine = duckdb_engine.DuckDB()
    interrupted = threading.Event()
    connection = MagicMock()
    connection.interrupt.side_effect = interrupted.set
    monkeypatch.setattr(duckdb_engine.duckdb, "connect", lambda: connection)

    def blocked_init(*_args, **_kwargs):
        assert interrupted.wait(1.0), "setup watchdog did not interrupt init"
        raise RuntimeError("interrupted")

    monkeypatch.setattr(duckdb_engine, "init_connection", blocked_init)
    parser = SQLParser("shop", "SELECT id FROM events", "duckdb")

    with pytest.raises(TimeoutError, match="timed out"):
        engine.execute(
            _reflection(),
            parser,
            SimpleNamespace(temp_dir="/tmp"),
            lambda _event: None,
            timeout_sec=60,
            deadline_monotonic=time.monotonic() + 0.03,
        )
    connection.interrupt.assert_called()
    connection.close.assert_called()


def test_provider_credentials_are_stable_but_rotation_isolated():
    class Frozen:
        def __init__(self, token):
            self.access_key = "access"
            self.secret_key = "secret"
            self.token = token

    class Credentials:
        def __init__(self, token):
            self._frozen = Frozen(token)

        def get_frozen_credentials(self):
            return self._frozen

    class Storage:
        bucket_name = "bucket"
        endpoint_url = "https://s3.example"
        region = "eu-west-1"
        base_prefix = "tenant"

        def __init__(self, token):
            signer = SimpleNamespace(_credentials=Credentials(token))
            self.client = SimpleNamespace(_request_signer=signer)

    Storage.__module__ = "supertable.storage.s3_storage"
    assert _storage_identity(Storage("session-a")) == _storage_identity(
        Storage("session-a")
    )
    assert _storage_identity(Storage("session-a")) != _storage_identity(
        Storage("session-b")
    )


def test_duckdb_lru_is_bounded_and_eviction_waits_for_active_use(monkeypatch):
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_DUCKDB_ENGINE_CACHE_MAX_ENTRIES=2,
        ),
    )
    executor_module._duckdb_singleton = None
    executor_module._duckdb_singletons.clear()

    class Storage:
        bucket_name = "bucket"
        endpoint_url = "https://s3.example"
        region = "eu-west-1"
        base_prefix = ""
        _aws_access_key_id = "access"
        _aws_session_token = "session"

        def __init__(self, secret):
            self._aws_secret_access_key = secret

    Storage.__module__ = "supertable.storage.s3_storage"
    first = executor_module._get_duckdb(Storage("one"), "org")
    second = executor_module._get_duckdb(Storage("two"), "org")
    assert executor_module._get_duckdb(Storage("one"), "org") is first
    third = executor_module._get_duckdb(Storage("three"), "org")
    assert list(executor_module._duckdb_singletons.values()) == [first, third]
    assert second.cache_state()["eviction_pending"] is True

    connection = MagicMock()
    active = duckdb_engine.DuckDB()
    active._con = connection
    active._begin_query_use()
    assert active.request_cache_eviction() is False
    connection.close.assert_not_called()
    active._finish_query_use()
    connection.close.assert_called_once()
    assert active.cache_state()["connection_open"] is False

    executor_module._duckdb_singleton = None
    for cached in tuple(executor_module._duckdb_singletons.values()):
        cached.request_cache_eviction()
    executor_module._duckdb_singletons.clear()


def test_azure_delegation_key_is_cached_until_it_no_longer_covers_ttl(
    monkeypatch,
):
    import azure.storage.blob
    from supertable.storage.azure_storage import AzureBlobStorage

    monkeypatch.setattr(
        azure.storage.blob,
        "generate_blob_sas",
        lambda **_kwargs: "signed",
    )
    blob = SimpleNamespace(url="https://account/container/blob")
    container = MagicMock()
    container.get_blob_client.return_value = blob
    svc = MagicMock()
    svc.get_container_client.return_value = container
    svc.credential = object()
    svc.account_name = "account"
    svc.get_user_delegation_key.side_effect = ["key-1", "key-2"]
    storage = AzureBlobStorage("container", svc)

    storage.presign("a", expiry_seconds=60)
    storage.presign("b", expiry_seconds=60)
    assert svc.get_user_delegation_key.call_count == 1

    storage.presign("c", expiry_seconds=1000)
    assert svc.get_user_delegation_key.call_count == 2


def test_idle_duckdb_stream_self_finalizes_at_deadline(monkeypatch):
    engine = duckdb_engine.DuckDB()
    inner = ArrowBatchStream.from_table(pa.table({"id": [1]}))
    monkeypatch.setattr(
        engine,
        "_execute_unleased",
        lambda **_kwargs: inner,
    )

    stream = engine.execute(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        lambda _event: None,
        deadline_monotonic=time.monotonic() + 0.05,
        _streaming=True,
    )
    assert engine.cache_state()["active_queries"] == 1

    wait_until = time.monotonic() + 1.0
    while (
        engine.cache_state()["active_queries"] != 0
        and time.monotonic() < wait_until
    ):
        time.sleep(0.01)

    assert stream.closed is True
    assert inner.closed is True
    assert engine.cache_state()["active_queries"] == 0
    with pytest.raises(TimeoutError, match="timed out"):
        next(stream)


def test_idle_duckdb_stream_self_finalizes_on_external_cancel(monkeypatch):
    engine = duckdb_engine.DuckDB()
    inner = ArrowBatchStream.from_table(pa.table({"id": [1]}))
    monkeypatch.setattr(
        engine,
        "_execute_unleased",
        lambda **_kwargs: inner,
    )
    cancelled = threading.Event()

    stream = engine.execute(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        lambda _event: None,
        cancel_event=cancelled,
        deadline_monotonic=time.monotonic() + 10,
        _streaming=True,
    )
    cancelled.set()

    wait_until = time.monotonic() + 1.0
    while (
        engine.cache_state()["active_queries"] != 0
        and time.monotonic() < wait_until
    ):
        time.sleep(0.01)

    assert stream.closed is True
    assert inner.closed is True
    assert engine.cache_state()["active_queries"] == 0
    with pytest.raises(ResourceReservationCancelled, match="cancelled"):
        next(stream)


@pytest.mark.parametrize(
    ("stop_kind", "expected_error"),
    [
        ("deadline", TimeoutError),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_idle_executor_duckdb_stream_releases_outer_cache_lease(
    monkeypatch, stop_kind, expected_error,
):
    class TrackingCache:
        source_is_local = False

        def __init__(self):
            self.active = 0

        @contextmanager
        def localized(self, reflection, **_kwargs):
            self.active += 1
            try:
                yield reflection, None
            finally:
                self.active -= 1

    cache = TrackingCache()
    duck = duckdb_engine.DuckDB()
    raw = ArrowBatchStream.from_table(pa.table({"id": [1]}))
    monkeypatch.setattr(duck, "_execute_unleased", lambda **_kwargs: raw)
    executor = executor_module.Executor()
    executor.duckdb_exec = duck
    monkeypatch.setattr(executor, "_get_file_cache", lambda: cache)
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args, **_kwargs: ({"duckdb": MagicMock()}, ()),
    )
    cancelled = threading.Event()
    deadline = time.monotonic() + (10.0 if stop_kind == "cancel" else 0.05)

    stream, used = executor.execute_stream(
        Engine.DUCKDB,
        _reflection("/tmp/events.parquet"),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        PlanStats(),
        "test",
        deadline_monotonic=deadline,
        cancel_event=cancelled,
    )
    assert used == "duckdb"
    assert cache.active == 1
    if stop_kind == "cancel":
        cancelled.set()

    wait_until = time.monotonic() + 1.0
    while cache.active and time.monotonic() < wait_until:
        time.sleep(0.01)

    assert cache.active == 0
    assert duck.cache_state()["active_queries"] == 0
    assert stream.closed is True
    assert raw.closed is True
    with pytest.raises(expected_error):
        next(stream)


@pytest.mark.parametrize(
    ("stop_kind", "expected_error"),
    [
        ("deadline", TimeoutError),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_duckdb_stop_defers_cleanup_until_active_next_unwinds(
    monkeypatch, stop_kind, expected_error,
):
    schema = pa.schema([("id", pa.int64())])
    entered = threading.Event()
    release = threading.Event()
    producer_closed = threading.Event()

    class BlockingProducer:
        def __iter__(self):
            return self

        def __next__(self):
            entered.set()
            assert release.wait(1.0)
            return pa.record_batch([pa.array([1])], schema=schema)

        def close(self):
            producer_closed.set()

    engine = duckdb_engine.DuckDB()
    inner = ArrowBatchStream(schema, BlockingProducer())
    monkeypatch.setattr(
        engine,
        "_execute_unleased",
        lambda **_kwargs: inner,
    )
    cancelled = threading.Event()
    stream = engine.execute(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        lambda _event: None,
        deadline_monotonic=time.monotonic() + (
            0.05 if stop_kind == "deadline" else 10.0
        ),
        cancel_event=cancelled,
        _streaming=True,
    )
    outcome = []

    def consume() -> None:
        try:
            next(stream)
        except BaseException as exc:
            outcome.append(exc)

    consumer = threading.Thread(target=consume)
    consumer.start()
    assert entered.wait(1.0)
    if stop_kind == "cancel":
        cancelled.set()
    time.sleep(0.1)

    assert engine.cache_state()["active_queries"] == 1
    assert producer_closed.is_set() is False

    release.set()
    consumer.join(1.0)
    assert consumer.is_alive() is False
    assert len(outcome) == 1
    assert isinstance(outcome[0], expected_error)
    assert producer_closed.is_set() is True
    assert engine.cache_state()["active_queries"] == 0


@pytest.mark.parametrize(
    ("stop_kind", "expected_error"),
    [
        ("deadline", TimeoutError),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_blocked_presign_is_bounded_but_retains_worker_slot(
    monkeypatch, stop_kind, expected_error,
):
    one_slot = threading.BoundedSemaphore(1)
    monkeypatch.setattr(executor_module, "_presign_refresh_slots", one_slot)
    provider_started = threading.Event()
    release_provider = threading.Event()

    class BlockingStorage:
        def presign(self, _key, *, expiry_seconds):
            provider_started.set()
            assert release_provider.wait(2.0)
            return f"https://fresh/object?sig=x&ttl={expiry_seconds}"

    cancelled = threading.Event()
    first_outcome = []
    first_deadline = time.monotonic() + (
        5.0 if stop_kind == "cancel" else 0.05
    )

    def first_refresh() -> None:
        try:
            executor_module._refresh_presigned_reflection(
                BlockingStorage(),
                _reflection(),
                deadline_monotonic=first_deadline,
                cancel_event=cancelled,
            )
        except BaseException as exc:
            first_outcome.append(exc)

    caller = threading.Thread(target=first_refresh)
    caller.start()
    assert provider_started.wait(1.0)
    if stop_kind == "cancel":
        cancelled.set()
    caller.join(1.0)

    assert caller.is_alive() is False
    assert len(first_outcome) == 1
    assert isinstance(first_outcome[0], expected_error)
    assert release_provider.is_set() is False

    waiting_storage = MagicMock()
    waiting_storage.presign.return_value = "https://fresh/waiting?sig=x"
    with pytest.raises(TimeoutError):
        executor_module._refresh_presigned_reflection(
            waiting_storage,
            _reflection(),
            deadline_monotonic=time.monotonic() + 0.05,
        )
    waiting_storage.presign.assert_not_called()

    release_provider.set()
    successful_storage = MagicMock()
    successful_storage.presign.return_value = "https://fresh/ok?sig=x"
    refreshed = executor_module._refresh_presigned_reflection(
        successful_storage,
        _reflection(),
        deadline_monotonic=time.monotonic() + 1.0,
    )
    assert refreshed.supers[0].files == ["https://fresh/ok?sig=x"]


@pytest.mark.parametrize(
    ("stop_kind", "expected_error"),
    [
        ("deadline", TimeoutError),
        ("cancel", ResourceReservationCancelled),
    ],
)
def test_blocked_duckdb_connect_is_bounded_and_late_handle_is_not_cached(
    monkeypatch, tmp_path, stop_kind, expected_error,
):
    one_slot = threading.BoundedSemaphore(1)
    monkeypatch.setattr(duckdb_engine, "_duckdb_connect_slots", one_slot)
    connect_started = threading.Event()
    release_connect = threading.Event()
    late_closed = threading.Event()
    connect_calls = 0

    class FakeConnection:
        def __init__(self, *, late=False):
            self._late = late

        def interrupt(self):
            pass

        def close(self):
            if self._late:
                late_closed.set()

    def native_connect():
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls == 1:
            connect_started.set()
            assert release_connect.wait(2.0)
            return FakeConnection(late=True)
        return FakeConnection()

    monkeypatch.setattr(duckdb_engine.duckdb, "connect", native_connect)
    monkeypatch.setattr(
        duckdb_engine, "init_connection", lambda *_args, **_kwargs: None,
    )

    def get_connection(engine, *, deadline, cancel_event):
        guard = duckdb_engine._DuckDBSetupInterruptGuard(
            deadline_monotonic=deadline,
            timeout_value=max(0.0, deadline - time.monotonic()),
            cancel_event=cancel_event,
        )

        def publish(target):
            guard.set_target(target)
            guard.raise_if_stopped()

        acquired = False
        guard.start()
        try:
            engine._acquire_setup_lock(guard.raise_if_stopped)
            acquired = True
            return engine._get_connection(
                str(tmp_path),
                setup_target_callback=publish,
                setup_check_callback=guard.raise_if_stopped,
            )
        finally:
            if acquired:
                engine._lock.release()
            guard.close()

    first_engine = duckdb_engine.DuckDB()
    cancelled = threading.Event()
    first_outcome = []
    first_deadline = time.monotonic() + (
        5.0 if stop_kind == "cancel" else 0.05
    )

    def first_call():
        try:
            get_connection(
                first_engine,
                deadline=first_deadline,
                cancel_event=cancelled,
            )
        except BaseException as exc:
            first_outcome.append(exc)

    caller = threading.Thread(target=first_call)
    caller.start()
    assert connect_started.wait(1.0)
    if stop_kind == "cancel":
        cancelled.set()
    caller.join(1.0)

    assert caller.is_alive() is False
    assert len(first_outcome) == 1
    assert isinstance(first_outcome[0], expected_error)
    assert first_engine._con is None

    # Evict/shut down this engine while its abandoned native call is still live.
    # The worker owns no engine reference or cache publication authority.
    assert first_engine.request_cache_eviction() is True

    with pytest.raises(TimeoutError):
        get_connection(
            duckdb_engine.DuckDB(),
            deadline=time.monotonic() + 0.05,
            cancel_event=None,
        )
    assert connect_calls == 1

    release_connect.set()
    assert late_closed.wait(1.0)
    assert first_engine._con is None

    final_engine = duckdb_engine.DuckDB()
    connection = get_connection(
        final_engine,
        deadline=time.monotonic() + 1.0,
        cancel_event=None,
    )
    assert connection is final_engine._con
    assert connect_calls == 2
    assert final_engine.request_cache_eviction() is True


@pytest.mark.parametrize(
    "stop_error",
    [
        IslandExecutionTimeout("IslandDB query timed out"),
        ResourceReservationCancelled("IslandDB query was cancelled"),
    ],
)
def test_auto_materialized_stop_is_not_replayed_on_duckdb(
    monkeypatch, stop_error,
):
    executor = executor_module.Executor()
    executor._auto_pick = MagicMock(return_value=Engine.ISLANDDB)
    executor.duckdb_exec = MagicMock()
    executor.island_exec = MagicMock()
    executor.island_exec.prepare_execution.return_value = SimpleNamespace(
        capability=SimpleNamespace(supported=True, reasons=()),
    )
    executor.island_exec.execute.side_effect = stop_error
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args, **_kwargs: ({"duckdb": MagicMock()}, ()),
    )
    warm_metrics = SimpleNamespace(
        coverage_ratio=1.0,
        to_plan_stats=lambda: {},
    )
    warm_cache = SimpleNamespace(
        source_is_local=False,
        localized=lambda *_args, **_kwargs: executor_module.nullcontext(
            (_reflection("/tmp/events.parquet"), warm_metrics)
        ),
    )
    monkeypatch.setattr(executor, "_get_file_cache", lambda: warm_cache)
    plan_stats = PlanStats()

    with pytest.raises(type(stop_error)):
        executor.execute(
            Engine.AUTO,
            _reflection("/tmp/events.parquet"),
            MagicMock(original_query="SELECT id FROM events"),
            SimpleNamespace(
                temp_dir="/tmp",
                query_plan_path="/tmp/query-plan.json",
            ),
            MagicMock(),
            plan_stats,
            "test",
        )

    executor.duckdb_exec.execute.assert_not_called()
    assert not any(
        stat.get("ENGINE_ATTEMPT", {}).get("engine") == Engine.DUCKDB.value
        for stat in plan_stats.stats
    )
