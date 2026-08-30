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

from supertable import data_reader
from supertable.config import settings as settings_module
from supertable.data_classes import (
    Reflection,
    RbacViewDef,
    SuperSnapshot,
    TombstoneDef,
    TombstoneSegmentDef,
)
from supertable.engine import duckdb_engine, islanddb
from supertable.engine.adaptive_router import (
    AdaptiveEngineRouter,
    RoutingAvailability,
    RoutingFeatures,
)
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import (
    Executor,
    _AutoIslandFallbackStream,
    _RetryBeforeFirstBatchStream,
    _refresh_presigned_reflection,
)
from supertable.engine.islanddb import IslandCapability
from supertable.engine.island_resources import (
    ArrowBatchStream,
    IslandResourceError,
    ResourceReservationCancelled,
)
from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer


executor_module = importlib.import_module("supertable.engine.executor")


def _table_stream(values=(1, 2, 3)) -> ArrowBatchStream:
    return ArrowBatchStream.from_table(pa.table({"id": list(values)}))


def test_query_sql_uses_configured_batches_deadline_cancel_and_engine_metadata(
    monkeypatch,
):
    cancel_event = threading.Event()
    reader = MagicMock()
    reader.execute_stream.return_value = (
        _table_stream((1, 2)), data_reader.Status.OK, None,
    )
    reader.query_plan_manager = SimpleNamespace(
        query_id="qid-1", query_hash="hash-1",
    )
    reader.plan_stats = SimpleNamespace(stats=[
        {
            "ENGINE_REQUEST": {
                "requested_engine": "auto",
                "selected_engine": "duckdb",
                "forced": False,
            },
        },
        {
            "AUTO_ROUTING": {
                "selected_engine": "duckdb",
                "features": {"cache_state": "warm"},
            },
        },
        {
            "AUTO_ROUTING_OUTCOME": {
                "selected_engine": "duckdb",
                "actual_engine": "duckdb",
                "fallback": False,
            },
        },
        {"FILE_CACHE_HITS": 2, "FILE_CACHE_COVERAGE_RATIO": 1.0},
        {"ENGINE": "duckdb"},
    ])
    monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)
    monkeypatch.setattr(
        data_reader,
        "settings",
        SimpleNamespace(
            SUPERTABLE_MAX_LIMIT=5000,
            SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES=1024 * 1024,
            SUPERTABLE_RESULT_STREAM_BATCH_ROWS=128,
        ),
    )
    out = {}

    columns, rows, _meta = data_reader.query_sql(
        "org", "shop", "SELECT id FROM events", 100,
        Engine.AUTO, "reader", out=out, timeout_sec=7.5,
        cancel_event=cancel_event,
    )

    assert columns == ["id"]
    assert rows == [[1], [2]]
    kwargs = reader.execute_stream.call_args.kwargs
    assert kwargs["max_batch_rows"] == 128
    assert kwargs["max_batch_bytes"] == 4 * 1024 * 1024
    assert kwargs["timeout_sec"] == 7.5
    assert kwargs["cancel_event"] is cancel_event
    assert out == {
        "query_id": "qid-1",
        "query_hash": "hash-1",
        "requested_engine": "auto",
        "selected_engine": "duckdb",
        "actual_engine": "duckdb",
        "engine_fallback": False,
        "engine_attempts": [],
        "engine_failure": {},
        "routing": {
            "selected_engine": "duckdb",
            "features": {"cache_state": "warm"},
        },
        "cache": {
            "FILE_CACHE_HITS": 2,
            "FILE_CACHE_COVERAGE_RATIO": 1.0,
        },
        "presign_refresh": {},
    }


def test_estimate_query_sql_returns_only_aggregate_snapshot_facts(monkeypatch):
    reflection = Reflection(
        storage_type="s3",
        reflection_bytes=123,
        total_reflections=2,
        supers=[
            SuperSnapshot(
                "shop", "events", 7,
                candidate_rows=11,
                candidate_rows_complete=True,
                candidate_row_groups=3,
                candidate_row_groups_complete=True,
            )
        ],
        source_bytes=456,
        source_bytes_complete=True,
        row_group_scan_bytes=100,
        row_group_scan_bytes_complete=True,
        decoded_bytes=789,
        decoded_bytes_complete=True,
        tombstone_views={"events": TombstoneDef()},
    )
    reader = MagicMock()
    reader.execute.return_value = (None, data_reader.Status.OK, None)
    reader.last_reflection = reflection
    reader.query_plan_manager = SimpleNamespace(
        query_id="qid-estimate", query_hash="hash-estimate",
    )
    monkeypatch.setattr(data_reader, "DataReader", lambda **_kwargs: reader)

    result = data_reader.estimate_query_sql(
        organization="org",
        super_name="shop",
        sql="SELECT id FROM events",
        engine=Engine.AUTO,
        role_name="reader",
        timeout_sec=5,
        expected_role_policy_fingerprint="a" * 64,
    )

    assert result == {
        "version": 1,
        "requested_engine": "auto",
        "recommended_request_engine": "auto",
        "storage_type": "s3",
        "table_count": 1,
        "file_count": 2,
        "estimated_scan_bytes": 123,
        "source_bytes": 456,
        "source_bytes_complete": True,
        "row_group_scan_bytes": 100,
        "row_group_scan_bytes_complete": True,
        "decoded_bytes": 789,
        "decoded_bytes_complete": True,
        "candidate_rows": 11,
        "candidate_rows_complete": True,
        "candidate_row_groups": 3,
        "candidate_row_groups_complete": True,
        "has_active_tombstone": True,
        "query_id": "qid-estimate",
        "query_hash": "hash-estimate",
    }
    assert reader.execute.call_args.kwargs["_policy_fingerprint_only"] is True


def test_query_sql_stream_exceeds_interactive_cap_only_with_explicit_budgets(
    monkeypatch,
):
    created = {}
    reader = MagicMock()
    reader.execute_export_stream.return_value = (
        _table_stream((1, 2, 3)), data_reader.Status.OK, None,
    )
    reader.query_plan_manager = SimpleNamespace(
        query_id="qid-export", query_hash="hash-export",
    )
    reader.plan_stats = SimpleNamespace(stats=[
        {
            "ENGINE_REQUEST": {
                "requested_engine": "duckdb",
                "selected_engine": "duckdb",
                "forced": True,
            },
        },
        {"ENGINE": "duckdb"},
    ])

    def build_reader(**kwargs):
        created.update(kwargs)
        return reader

    monkeypatch.setattr(data_reader, "DataReader", build_reader)
    monkeypatch.setattr(
        data_reader,
        "settings",
        SimpleNamespace(
            SUPERTABLE_MAX_LIMIT=5,
            SUPERTABLE_RESULT_STREAM_BATCH_ROWS=64,
        ),
    )
    cancel_event = threading.Event()
    out = {}

    stream = data_reader.query_sql_stream(
        "org", "shop", "SELECT id FROM events",
        Engine.DUCKDB, "reader",
        max_total_rows=1_000_000,
        timeout_sec=30,
        max_batch_rows=1000,
        cancel_event=cancel_event,
        out=out,
        expected_role_policy_fingerprint="a" * 64,
        expected_effective_policy_fingerprint="b" * 64,
    )
    with stream:
        assert [value.as_py() for batch in stream for value in batch.column(0)] == [
            1, 2, 3,
        ]

    assert created["query"].endswith("LIMIT 1000000")
    kwargs = reader.execute_export_stream.call_args.kwargs
    assert kwargs["role_name"] == "reader"
    assert kwargs["max_total_rows"] == 1_000_000
    assert kwargs["timeout_sec"] == 30
    assert kwargs["max_batch_rows"] == 64
    assert kwargs["max_batch_bytes"] == 4 * 1024 * 1024
    assert kwargs["cancel_event"] is cancel_event
    assert kwargs["expected_role_policy_fingerprint"] == "a" * 64
    assert kwargs["expected_effective_policy_fingerprint"] == "b" * 64
    assert out["requested_engine"] == "duckdb"
    assert out["actual_engine"] == "duckdb"


@pytest.mark.parametrize("rows", [0, -1, True])
def test_query_sql_stream_rejects_non_positive_or_boolean_row_budget(rows):
    with pytest.raises(ValueError, match="max_total_rows"):
        data_reader.query_sql_stream(
            "org", "shop", "SELECT 1", Engine.DUCKDB, "reader",
            max_total_rows=rows,
            timeout_sec=1,
        )


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_query_sql_stream_rejects_invalid_deadline(timeout):
    with pytest.raises(ValueError, match="timeout_sec"):
        data_reader.query_sql_stream(
            "org", "shop", "SELECT 1", Engine.DUCKDB, "reader",
            max_total_rows=10,
            timeout_sec=timeout,
        )


@pytest.mark.parametrize("batch_bytes", [0, -1, True])
def test_query_sql_stream_rejects_invalid_batch_byte_budget(batch_bytes):
    with pytest.raises(ValueError, match="max_batch_bytes"):
        data_reader.query_sql_stream(
            "org", "shop", "SELECT 1", Engine.DUCKDB, "reader",
            max_total_rows=10,
            timeout_sec=1,
            max_batch_bytes=batch_bytes,
        )


@pytest.mark.parametrize(
    "name",
    [
        "SUPERTABLE_RESULT_STREAM_BATCH_BYTES",
        "SUPERTABLE_RESULT_STREAM_VARIABLE_FETCH_ROWS",
    ],
)
@pytest.mark.parametrize("raw", ["0", "-1", "not-an-integer"])
def test_stream_memory_env_knobs_fail_closed(monkeypatch, name, raw):
    monkeypatch.setenv(name, raw)

    with pytest.raises(ValueError, match=name):
        settings_module._build_settings()


def test_export_limit_can_exceed_interactive_limit_without_widening_it(monkeypatch):
    monkeypatch.setattr(
        data_reader, "settings", SimpleNamespace(SUPERTABLE_MAX_LIMIT=5),
    )
    assert data_reader._ensure_sql_limit(
        "SELECT * FROM events", 1_000_000,
    ).endswith("LIMIT 5")
    assert data_reader._ensure_sql_limit(
        "SELECT * FROM events", 1_000_000,
        maximum_limit=1_000_000,
    ).endswith("LIMIT 1000000")


def test_row_budget_stream_slices_backend_overrun_and_cancels():
    class OverrunningStream:
        schema = pa.schema([("id", pa.int64())])

        def __init__(self):
            self.cancelled = False
            self._done = False

        def __iter__(self):
            return self

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return pa.record_batch([pa.array([1, 2, 3, 4, 5])], names=["id"])

        def cancel(self):
            self.cancelled = True

        def close(self):
            self.cancelled = True

    source = OverrunningStream()
    stream = data_reader._RowBudgetResultStream(source, 3)

    assert next(stream).column(0).to_pylist() == [1, 2, 3]
    with pytest.raises(StopIteration):
        next(stream)
    assert source.cancelled is True


@pytest.mark.parametrize("backend_rows", [3, 5])
def test_bounded_completion_is_monitored_as_success_without_n_plus_one(
    backend_rows,
):
    source = _table_stream(range(backend_rows))
    bounded = data_reader._RowBudgetResultStream(source, 3)
    outcomes = []
    stream = data_reader._MonitoredResultStream(
        bounded,
        lambda *args: outcomes.append(args),
    )

    assert next(stream).column(0).to_pylist() == [0, 1, 2]
    # Exporters normally close immediately after the last authorized batch;
    # they must not fetch row N+1 merely to prove successful completion.
    stream.close()

    assert len(outcomes) == 1
    status, message, rows, result_bytes = outcomes[0]
    assert (status, message, rows) == (data_reader.Status.OK.value, None, 3)
    assert result_bytes > 0


def test_row_budget_falls_back_when_inner_declines_terminal_callbacks():
    class Inner:
        schema = pa.schema([("id", pa.int64())])

        def add_terminal_callback(self, _callback):
            return False

        def cancel(self):
            return None

        def close(self):
            return None

    outcomes = []
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *outcome: outcomes.append(outcome),
    )

    stream.cancel()

    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream cancelled",
        0,
        0,
    )]


def test_exact_budget_cleanup_failure_is_not_monitored_as_success():
    batch = pa.record_batch({"id": [1, 2, 3]})

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._done = False
            self._callbacks = []

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return batch

        def close(self):
            for callback in self._callbacks:
                callback("closed")
            raise RuntimeError("cleanup failed")

    outcomes = []
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *outcome: outcomes.append(outcome),
    )

    with pytest.raises(RuntimeError, match="Query result stream failed"):
        next(stream)
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "Query result stream failed",
        0,
        0,
    )]


def test_exact_budget_timeout_during_close_outranks_success():
    batch = pa.record_batch({"id": [1, 2, 3]})

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._done = False
            self._callbacks = []

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return batch

        def close(self):
            for callback in self._callbacks:
                callback("timed_out")

    outcomes = []
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *outcome: outcomes.append(outcome),
    )

    assert next(stream).num_rows == 3
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream timed out",
        3,
        batch.nbytes,
    )]


def test_exact_budget_terminal_surfaces_monitoring_failure():
    batch = pa.record_batch({"id": [1, 2, 3]})

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._done = False
            self._callbacks = []

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return batch

        def close(self):
            for callback in self._callbacks:
                callback("closed")

    monitoring_error = RuntimeError("monitor persistence failed")
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *_outcome: (_ for _ in ()).throw(monitoring_error),
    )

    with pytest.raises(RuntimeError, match="monitor persistence failed") as raised:
        next(stream)
    assert raised.value is monitoring_error


def test_exact_budget_waits_for_concurrent_terminal_monitoring_failure():
    batch = pa.record_batch({"id": [1, 2, 3]})
    after_error_snapshot = threading.Event()
    monitoring_entered = threading.Event()
    release_monitoring = threading.Event()

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._done = False
            self._callbacks = []
            self.terminal_thread = None

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return batch

        def close(self):
            def publish_terminal():
                assert after_error_snapshot.wait(2)
                for callback in self._callbacks:
                    callback("closed")

            self.terminal_thread = threading.Thread(target=publish_terminal)
            self.terminal_thread.start()

    monitoring_error = RuntimeError("monitor persistence failed")

    def finalize(*_outcome):
        monitoring_entered.set()
        assert release_monitoring.wait(2)
        raise monitoring_error

    inner = Inner()
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(inner, 3),
        finalize,
    )
    original_error_snapshot = stream._raise_completed_finalize_error

    def release_terminal_after_snapshot():
        original_error_snapshot()
        after_error_snapshot.set()
        assert monitoring_entered.wait(2)

    stream._raise_completed_finalize_error = release_terminal_after_snapshot
    observed = []
    errors = []

    def consume():
        try:
            observed.append(next(stream))
        except BaseException as exc:
            errors.append(exc)

    consumer = threading.Thread(target=consume)
    consumer.start()
    assert monitoring_entered.wait(1)
    consumer.join(0.05)
    assert consumer.is_alive(), "batch returned before monitoring completed"
    release_monitoring.set()
    consumer.join(2)
    inner.terminal_thread.join(2)

    assert not consumer.is_alive()
    assert observed == []
    assert errors == [monitoring_error]


def test_row_budget_does_not_yield_batch_returned_after_concurrent_cancel():
    batch = pa.record_batch({"id": [1, 2, 3]})
    entered = threading.Event()
    released = threading.Event()

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._callbacks = []

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            entered.set()
            assert released.wait(2), "cancel did not release test producer"
            return batch

        def cancel(self):
            for callback in self._callbacks:
                callback("cancelled")
            released.set()

        def close(self):
            self.cancel()

    outcomes = []
    observed = []
    errors = []
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *outcome: outcomes.append(outcome),
    )

    def consume():
        try:
            observed.append(next(stream))
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(1)
    stream.cancel()
    worker.join(2)

    assert not worker.is_alive()
    assert observed == []
    assert errors
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream cancelled",
        0,
        0,
    )]


def test_row_budget_cancel_wins_while_exact_budget_cleanup_is_blocked():
    batch = pa.record_batch({"id": [1, 2, 3]})
    close_entered = threading.Event()
    release_close = threading.Event()

    class Inner:
        schema = batch.schema

        def __init__(self):
            self._callbacks = []
            self._done = False

        def add_terminal_callback(self, callback):
            self._callbacks.append(callback)

        def __next__(self):
            if self._done:
                raise StopIteration
            self._done = True
            return batch

        def close(self):
            close_entered.set()
            assert release_close.wait(2), "cancel did not release cleanup"
            for callback in self._callbacks:
                callback("closed")

        def cancel(self):
            for callback in self._callbacks:
                callback("cancelled")
            release_close.set()

    outcomes = []
    observed = []
    errors = []
    stream = data_reader._MonitoredResultStream(
        data_reader._RowBudgetResultStream(Inner(), 3),
        lambda *outcome: outcomes.append(outcome),
    )

    def consume():
        try:
            observed.append(next(stream))
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert close_entered.wait(1)
    stream.cancel()
    worker.join(2)

    assert not worker.is_alive()
    assert observed == []
    assert errors
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream cancelled",
        0,
        0,
    )]


class _AutoStreamCache:
    source_is_local = False

    class Metrics:
        def __init__(self, coverage_ratio):
            self.coverage_ratio = coverage_ratio

        def to_plan_stats(self):
            return {"FILE_CACHE_COVERAGE_RATIO": self.coverage_ratio}

    def __init__(self, coverage_ratio):
        self.coverage_ratio = coverage_ratio
        self.calls = []
        self.active = 0
        self.can_populate_calls = 0

    def can_populate_all(self, _reflection):
        self.can_populate_calls += 1
        raise AssertionError("AUTO must not perform cold-cache admission")

    @contextmanager
    def localized(
        self, reflection, *, populate, tolerate_corrupt_hits=False,
    ):
        self.calls.append((populate, tolerate_corrupt_hits))
        self.active += 1
        try:
            yield reflection, self.Metrics(self.coverage_ratio)
        finally:
            self.active -= 1


class _AutoStreamDuck:
    def __init__(self):
        self.calls = []

    def cache_state(self):
        return {
            "connection_open": True,
            "active_queries": 0,
            "eviction_pending": False,
        }

    def execute_stream(self, *, reflection, **kwargs):
        self.calls.append((reflection, kwargs))
        return _table_stream((7, 8))


class _AutoStreamIsland:
    def __init__(self, stream=None):
        self.stream = stream
        self.execute_calls = 0

    def prepare_execution(self, *_args, **_kwargs):
        return SimpleNamespace(capability=IslandCapability(True))

    def execute_stream(self, **_kwargs):
        self.execute_calls += 1
        if self.stream is None:
            raise AssertionError("cold AUTO cache must fall back before Island")
        return self.stream


class _IslandDeliveryFailure:
    schema = pa.schema([("id", pa.int64())])

    def __init__(self, *, first_batch=False, exc=None):
        self._first_batch = first_batch
        self._exc = exc or IslandResourceError("native resource unavailable")
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._first_batch:
            self._first_batch = False
            return pa.record_batch([pa.array([1])], names=["id"])
        raise self._exc

    def close(self):
        self.closed = True

    def cancel(self):
        self.closed = True


class _LifecycleIslandDeliveryFailure(_IslandDeliveryFailure):
    def __init__(self):
        super().__init__()
        self._terminal_callbacks = []

    def add_terminal_callback(self, callback):
        self._terminal_callbacks.append(callback)

    def close(self):
        super().close()
        callbacks, self._terminal_callbacks = self._terminal_callbacks, []
        for callback in callbacks:
            callback("closed")


def _auto_stream_executor(monkeypatch, cache, island):
    # These tests exercise AUTO's cache/fallback state machine, not the
    # separately covered presigned-path mode. Keep ambient .env settings from
    # inserting a provider refresh into the mocked storage=None fixture.
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_DUCKDB_PRESIGNED=False,
        ),
    )
    executor = Executor(storage=None, organization="org")
    duck = _AutoStreamDuck()
    executor.duckdb_exec = duck
    executor.island_exec = island
    executor._file_cache = cache
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor, "_auto_pick", lambda *_args, **_kwargs: Engine.ISLANDDB,
    )
    bundle_calls = []

    def resolve(*_args):
        bundle_calls.append(True)
        return {"duckdb": MagicMock()}, ()

    monkeypatch.setattr(executor_module, "resolve_engine_bundle", resolve)
    reflection = Reflection(
        "s3", 10, 1,
        [SuperSnapshot(
            "shop", "events", 1,
            ["s3://bucket/events.parquet"],
            {"id"},
            resource_keys=["org/shop/events.parquet"],
        )],
    )
    return executor, duck, reflection, bundle_calls


def test_auto_stream_cold_localization_is_hit_only_and_falls_back_before_island(
    monkeypatch,
):
    cache = _AutoStreamCache(coverage_ratio=0.0)
    island = _AutoStreamIsland()
    executor, duck, reflection, bundle_calls = _auto_stream_executor(
        monkeypatch, cache, island,
    )
    stats = PlanStats()

    stream, used = executor.execute_stream(
        Engine.AUTO, reflection, MagicMock(), SimpleNamespace(), Timer(),
        stats, "",
    )
    try:
        assert used == "duckdb"
        assert next(stream).column(0).to_pylist() == [7, 8]
        assert cache.active == 1
    finally:
        stream.close()

    assert cache.active == 0
    assert cache.calls == [(False, True), (False, True)]
    assert cache.can_populate_calls == 0
    assert island.execute_calls == 0
    assert len(duck.calls) == 1
    assert bundle_calls == [True]
    assert [
        item["ENGINE_ATTEMPT"]["engine"]
        for item in stats.stats if "ENGINE_ATTEMPT" in item
    ] == ["islanddb", "duckdb"]
    outcome = next(
        item["AUTO_ROUTING_OUTCOME"]
        for item in stats.stats if "AUTO_ROUTING_OUTCOME" in item
    )
    assert outcome == {
        "selected_engine": "islanddb",
        "actual_engine": "duckdb",
        "fallback": True,
        "reason_code": "IslandResourceError",
        "stage": "setup",
    }


def test_auto_stream_safe_first_batch_failure_releases_island_then_falls_back(
    monkeypatch,
):
    cache = _AutoStreamCache(coverage_ratio=1.0)
    failed = _IslandDeliveryFailure()
    island = _AutoStreamIsland(failed)
    executor, duck, reflection, bundle_calls = _auto_stream_executor(
        monkeypatch, cache, island,
    )
    stats = PlanStats()

    stream, used = executor.execute_stream(
        Engine.AUTO, reflection, MagicMock(), SimpleNamespace(), Timer(),
        stats, "",
    )
    assert used == "islanddb"  # selection is known before deferred delivery
    assert cache.active == 1
    try:
        assert next(stream).column(0).to_pylist() == [7, 8]
        assert failed.closed is True
        # Island's lease was released before DuckDB acquired its own lease.
        assert cache.active == 1
    finally:
        stream.close()

    assert cache.active == 0
    assert cache.calls == [(False, True), (False, True)]
    assert len(duck.calls) == 1
    assert bundle_calls == [True]
    outcome = next(
        item["AUTO_ROUTING_OUTCOME"]
        for item in stats.stats if "AUTO_ROUTING_OUTCOME" in item
    )
    assert outcome["actual_engine"] == "duckdb"
    assert outcome["stage"] == "first_batch"


def test_auto_stream_fallback_forwards_idle_duckdb_timeout(monkeypatch):
    cache = _AutoStreamCache(coverage_ratio=1.0)
    failed = _LifecycleIslandDeliveryFailure()
    island = _AutoStreamIsland(failed)
    executor, _duck, reflection, _bundle_calls = _auto_stream_executor(
        monkeypatch, cache, island,
    )
    stats = PlanStats()
    stream, used = executor.execute_stream(
        Engine.AUTO, reflection, MagicMock(), SimpleNamespace(), Timer(),
        stats, "", deadline_monotonic=time.monotonic() + 0.5,
    )

    assert used == "islanddb"
    assert next(stream).column(0).to_pylist() == [7, 8]
    wait_until = time.monotonic() + 2.0
    while stream.terminal_kind is None and time.monotonic() < wait_until:
        time.sleep(0.01)

    assert stream.terminal_kind == "timed_out"
    assert cache.active == 0
    with pytest.raises(TimeoutError, match="timed out"):
        next(stream)


def test_auto_stream_never_falls_back_after_an_observable_batch(monkeypatch):
    cache = _AutoStreamCache(coverage_ratio=1.0)
    failed = _IslandDeliveryFailure(first_batch=True)
    island = _AutoStreamIsland(failed)
    executor, duck, reflection, _bundle_calls = _auto_stream_executor(
        monkeypatch, cache, island,
    )
    stats = PlanStats()

    stream, _used = executor.execute_stream(
        Engine.AUTO, reflection, MagicMock(), SimpleNamespace(), Timer(),
        stats, "",
    )
    assert next(stream).column(0).to_pylist() == [1]
    with pytest.raises(IslandResourceError, match="resource unavailable"):
        next(stream)

    assert failed.closed is True
    assert cache.active == 0
    assert duck.calls == []
    outcome = next(
        item["AUTO_ROUTING_OUTCOME"]
        for item in stats.stats if "AUTO_ROUTING_OUTCOME" in item
    )
    assert outcome == {
        "selected_engine": "islanddb",
        "actual_engine": "islanddb",
        "fallback": False,
    }


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("deadline expired"),
        ResourceReservationCancelled("client disconnected"),
    ],
)
def test_auto_stream_never_falls_back_on_deadline_or_cancellation(exc):
    failed = _IslandDeliveryFailure(exc=exc)
    fallback = MagicMock(return_value=_table_stream((9,)))
    stream = _AutoIslandFallbackStream(
        failed,
        fallback_factory=fallback,
        island_success=MagicMock(),
    )

    with pytest.raises(type(exc), match=str(exc)):
        next(stream)

    fallback.assert_not_called()


def test_auto_router_rejects_spark_as_a_hard_streaming_capability():
    features = RoutingFeatures(
        reflection_bytes=10_000,
        effective_scan_bytes=10_000,
        decoded_bytes=20_000,
        total_files=1,
        streaming_result=True,
        island_advice="route_spark",
    )
    availability = RoutingAvailability(
        duckdb_available=True,
        island_enabled=True,
        island_supported=True,
        spark_available=True,
        spark_semantics_supported=True,
        fitting_spark_clusters=1,
        spark_min_scan_bytes=0,
    )

    decision = AdaptiveEngineRouter(island_min_bytes=1024).decide(
        features, availability,
    )

    assert decision.engine is Engine.DUCKDB
    spark = next(
        candidate for candidate in decision.candidates
        if candidate.engine is Engine.SPARK_SQL
    )
    assert spark.eligible is False
    assert "streaming Arrow" in " ".join(spark.rejection_reasons)


def test_presign_refresh_rotates_only_paths_and_preserves_pinned_rbac():
    role_view = RbacViewDef(
        allowed_columns=["id"], where_clause='"tenant" = 7',
    )
    reflection = Reflection(
        "s3", 100, 1,
        [SuperSnapshot(
            "shop", "events", 9,
            ["https://old/data.parquet?expired=1"],
            {"id", "tenant"},
            resource_keys=["org/shop/data.parquet"],
        )],
        rbac_views={"events": role_view},
        tombstone_views={
            "events": TombstoneDef(
                tombstone_path="https://old/manifest.json?expired=1",
                cache_key="org/shop/tombstone/manifest.json",
                expected_rows=1,
                segments=(TombstoneSegmentDef(
                    cache_key="org/shop/tombstone/segment.parquet",
                    tombstone_path="https://old/segment.parquet?expired=1",
                    expected_rows=1,
                    file_size=10,
                    tombstone_digest="a" * 64,
                ),),
            ),
        },
    )
    storage = MagicMock()
    storage.presign.side_effect = (
        lambda key, expiry_seconds=3600:
        f"https://fresh/{key}?token=new&ttl={expiry_seconds}"
    )

    refreshed = _refresh_presigned_reflection(storage, reflection)

    assert refreshed is not reflection
    assert refreshed.rbac_views["events"] is role_view
    assert refreshed.supers[0].resource_keys == ["org/shop/data.parquet"]
    assert refreshed.supers[0].files == [
        "https://fresh/org/shop/data.parquet?token=new&ttl=3600",
    ]
    assert refreshed.tombstone_views["events"].segments[0].tombstone_path == (
        "https://fresh/org/shop/tombstone/segment.parquet?token=new&ttl=3600"
    )
    assert storage.presign.call_count == 3


class _FailsWithExpiredCredential:
    schema = pa.schema([("id", pa.int64())])

    def __init__(self, first_batch=None):
        self.first_batch = first_batch
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.first_batch is not None:
            batch, self.first_batch = self.first_batch, None
            return batch
        raise duckdb_engine.DuckDBPresignRefreshRequired(
            "credential refresh required"
        )

    def close(self):
        self.closed = True

    def cancel(self):
        self.closed = True


def test_presign_retry_stream_replans_once_before_first_batch():
    initial = _FailsWithExpiredCredential()
    retry_calls = []

    def retry():
        retry_calls.append(True)
        return _table_stream((7, 8))

    stream = _RetryBeforeFirstBatchStream(initial, retry)

    assert next(stream).column(0).to_pylist() == [7, 8]
    assert retry_calls == [True]
    assert initial.closed is True
    stream.close()


def test_presign_retry_stream_never_replays_after_a_batch_was_emitted():
    first = pa.record_batch([pa.array([1])], names=["id"])
    retry = MagicMock(return_value=_table_stream((2,)))
    stream = _RetryBeforeFirstBatchStream(
        _FailsWithExpiredCredential(first), retry,
    )

    assert next(stream).column(0).to_pylist() == [1]
    with pytest.raises(RuntimeError, match="during result delivery"):
        next(stream)
    retry.assert_not_called()


def test_presign_retry_preserves_typed_cancellation():
    initial = _FailsWithExpiredCredential()

    def cancelled_retry():
        raise ResourceReservationCancelled("client disconnected")

    stream = _RetryBeforeFirstBatchStream(initial, cancelled_retry)
    with pytest.raises(ResourceReservationCancelled, match="disconnected"):
        next(stream)


def test_executor_owns_single_deadline_aware_presign_refresh(monkeypatch):
    class Storage:
        def __init__(self):
            self.calls = []

        def presign(self, key, *, expiry_seconds):
            self.calls.append((key, expiry_seconds))
            return f"https://fresh/{key}?ttl={expiry_seconds}"

    class Duck:
        def __init__(self):
            self.calls = []

        def cache_state(self):
            return {
                "connection_open": True,
                "active_queries": 0,
                "eviction_pending": False,
            }

        def execute_stream(self, *, reflection, **kwargs):
            self.calls.append((reflection, kwargs))
            if len(self.calls) == 1:
                return _FailsWithExpiredCredential()
            return _table_stream((9,))

    storage = Storage()
    executor = Executor(storage=storage, organization="org")
    executor.duckdb_exec = Duck()
    executor._file_cache = False
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_DUCKDB_PRESIGNED=False,
        ),
    )
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    reflection = Reflection(
        "s3", 10, 1,
        [SuperSnapshot(
            "shop", "events", 1,
            ["s3://bucket/events.parquet"],
            {"id"},
            resource_keys=["org/shop/events.parquet"],
        )],
    )
    deadline = __import__("time").monotonic() + 4
    stats = PlanStats()

    stream, used = executor.execute_stream(
        Engine.DUCKDB,
        reflection,
        MagicMock(),
        SimpleNamespace(),
        Timer(),
        stats,
        "",
        deadline_monotonic=deadline,
    )
    try:
        assert next(stream).column(0).to_pylist() == [9]
    finally:
        stream.close()

    assert used == "duckdb"
    assert len(executor.duckdb_exec.calls) == 2
    assert len(storage.calls) == 1
    key, expiry_seconds = storage.calls[0]
    assert key == "org/shop/events.parquet"
    assert 120 <= expiry_seconds <= 130
    assert executor.duckdb_exec.calls[1][0].supers[0].files == [
        f"https://fresh/org/shop/events.parquet?ttl={expiry_seconds}",
    ]
    refresh = next(
        item["DUCKDB_PRESIGN_REFRESH"]
        for item in stats.stats
        if "DUCKDB_PRESIGN_REFRESH" in item
    )
    assert refresh["stage"] == "first_batch"
    assert refresh["succeeded"] is True


@pytest.mark.parametrize(
    "remote_path",
    [
        "s3://bucket/events.parquet",
        "s3a://bucket/events.parquet",
        "gcs://bucket/events.parquet",
        "gs://bucket/events.parquet",
        "azure://container/events.parquet",
        "abfs://container@account/events.parquet",
        "abfss://container@account/events.parquet",
        "https://objects.invalid/events.parquet",
    ],
)
def test_presigned_mode_mints_once_at_materialized_duckdb_setup(
    monkeypatch, remote_path,
):
    class Storage:
        def __init__(self):
            self.calls = []

        def presign(self, key, *, expiry_seconds):
            self.calls.append((key, expiry_seconds))
            return f"https://fresh/{key}?signature=only"

    storage = Storage()
    executor = Executor(storage=storage, organization="org")
    duck = MagicMock()
    duck.cache_state.return_value = {}
    duck.execute.return_value = MagicMock()
    executor.duckdb_exec = duck
    executor._file_cache = False
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_DUCKDB_PRESIGNED=True,
        ),
    )
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    reflection = Reflection(
        "s3", 10, 1,
        [SuperSnapshot(
            "shop", "events", 1,
            [remote_path],
            {"id"},
            resource_keys=["org/shop/events.parquet"],
        )],
    )

    _result, used = executor.execute(
        Engine.DUCKDB,
        reflection,
        MagicMock(),
        SimpleNamespace(),
        Timer(),
        PlanStats(),
        "",
    )

    assert used == "duckdb"
    assert len(storage.calls) == 1
    assert duck.execute.call_count == 1
    assert duck.execute.call_args.kwargs["reflection"].supers[0].files == [
        "https://fresh/org/shop/events.parquet?signature=only",
    ]


@pytest.mark.parametrize(
    "remote_path",
    [
        "s3://bucket/events.parquet",
        "s3a://bucket/events.parquet",
        "gcs://bucket/events.parquet",
        "gs://bucket/events.parquet",
        "azure://container/events.parquet",
        "abfs://container@account/events.parquet",
        "abfss://container@account/events.parquet",
        "https://objects.invalid/events.parquet",
    ],
)
def test_presigned_mode_mints_once_at_streaming_duckdb_setup(
    monkeypatch, remote_path,
):
    class Storage:
        def __init__(self):
            self.calls = []

        def presign(self, key, *, expiry_seconds):
            self.calls.append((key, expiry_seconds))
            return f"https://fresh/{key}?signature=only"

    storage = Storage()
    executor = Executor(storage=storage, organization="org")
    duck = MagicMock()
    duck.cache_state.return_value = {}
    duck.execute_stream.return_value = _table_stream((9,))
    executor.duckdb_exec = duck
    executor._file_cache = False
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "settings",
        dataclasses.replace(
            executor_module.settings,
            SUPERTABLE_DUCKDB_PRESIGNED=True,
        ),
    )
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    reflection = Reflection(
        "s3", 10, 1,
        [SuperSnapshot(
            "shop", "events", 1,
            [remote_path],
            {"id"},
            resource_keys=["org/shop/events.parquet"],
        )],
    )

    stream, used = executor.execute_stream(
        Engine.DUCKDB,
        reflection,
        MagicMock(),
        SimpleNamespace(),
        Timer(),
        PlanStats(),
        "",
        deadline_monotonic=__import__("time").monotonic() + 4,
    )
    try:
        assert next(stream).column(0).to_pylist() == [9]
    finally:
        stream.close()

    assert used == "duckdb"
    assert len(storage.calls) == 1
    assert duck.execute_stream.call_count == 1
    assert duck.execute_stream.call_args.kwargs["reflection"].supers[0].files == [
        "https://fresh/org/shop/events.parquet?signature=only",
    ]


def test_duckdb_iterator_marks_only_pre_first_batch_403_as_refreshable():
    class FailingReader:
        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError(
                "HTTP Error 403 AccessDenied "
                "https://host/object?X-Amz-Signature=SECRET"
            )

        def close(self):
            pass

    iterator = duckdb_engine._DuckDBArrowBatchIterator(
        FailingReader(), MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
    )

    with pytest.raises(
        duckdb_engine.DuckDBPresignRefreshRequired,
        match="authorization expired",
    ) as raised:
        next(iterator)
    assert "SECRET" not in str(raised.value)


def test_duckdb_iterator_honors_external_cancellation_before_fetch():
    cancel_event = threading.Event()
    cancel_event.set()
    reader = MagicMock()
    iterator = duckdb_engine._DuckDBArrowBatchIterator(
        reader, MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
        cancel_event=cancel_event,
    )

    with pytest.raises(Exception, match="cancelled"):
        next(iterator)
    reader.__iter__.return_value.__next__.assert_not_called()


def test_duckdb_iterator_records_actual_fetch_without_affecting_delivery():
    batch = pa.record_batch({"id": [1]})
    durations = []
    iterator = duckdb_engine._DuckDBArrowBatchIterator(
        iter([batch]), MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
        duration_recorder=(
            lambda event, elapsed: durations.append((event, elapsed))
        ),
    )

    assert next(iterator).column(0).to_pylist() == [1]
    assert durations[0][0] == "RESULT_FETCH"
    assert durations[0][1] >= 0

    failing_iterator = duckdb_engine._DuckDBArrowBatchIterator(
        iter([batch]), MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
        duration_recorder=lambda _event, _elapsed: (_ for _ in ()).throw(
            RuntimeError("telemetry unavailable")
        ),
    )
    assert next(failing_iterator).column(0).to_pylist() == [1]


def test_duckdb_cancel_callback_waits_for_fetch_and_cleanup_telemetry():
    batch = pa.record_batch({"id": [1]})
    entered = threading.Event()
    released = threading.Event()
    durations = []

    class BlockingReader:
        schema = batch.schema

        def __iter__(self):
            return self

        def __next__(self):
            entered.set()
            assert released.wait(2), "cancel did not interrupt active fetch"
            return batch

        def close(self):
            return None

    connection = MagicMock()
    connection.interrupt.side_effect = released.set
    producer = duckdb_engine._DuckDBArrowBatchIterator(
        BlockingReader(), connection,
        timed_out=threading.Event(), timeout_value=60,
        duration_recorder=(
            lambda event, elapsed: durations.append((event, elapsed))
        ),
    )
    inner = ArrowBatchStream(
        batch.schema,
        producer,
        close_callback=lambda: durations.append(("CLEANUP", 0.0)),
    )
    lifecycle = duckdb_engine._DuckDBResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=60,
        cancel_event=None,
    )
    callback_snapshots = []
    lifecycle.add_terminal_callback(
        lambda kind: callback_snapshots.append(
            (kind, [event for event, _elapsed in durations])
        )
    )
    worker_errors = []

    def consume():
        try:
            next(lifecycle)
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(1)
    lifecycle.cancel()
    worker.join(2)

    assert not worker.is_alive()
    assert worker_errors
    assert callback_snapshots == [
        ("cancelled", ["RESULT_FETCH", "CLEANUP"]),
    ]


def test_nested_arrow_finalization_orders_engine_cleanup_before_publication():
    batch = pa.record_batch({"id": [1]})
    inner_cleanup_started = threading.Event()
    release_inner_cleanup = threading.Event()
    events = []

    def finish_inner():
        inner_cleanup_started.set()
        assert release_inner_cleanup.wait(2)
        events.append("engine cleanup")

    inner = ArrowBatchStream(
        batch.schema,
        iter([batch]),
        close_callback=finish_inner,
    )
    retry = _RetryBeforeFirstBatchStream(inner)
    guarded = executor_module._FailureTelemetryIterator(
        retry,
        plan_stats=PlanStats(),
        engine=Engine.DUCKDB,
        stage="stream_delivery",
    )
    routed = _AutoIslandFallbackStream(
        guarded,
        fallback_factory=lambda *_args: None,
        island_success=lambda: None,
    )
    outer = ArrowBatchStream(
        batch.schema,
        routed,
        close_callback=lambda: events.append("profile published"),
    )
    outer.add_finalization_callback(lambda: events.append("public terminal"))

    inner_closer = threading.Thread(target=inner.close)
    inner_closer.start()
    assert inner_cleanup_started.wait(1)

    started = time.monotonic()
    outer.close()
    assert time.monotonic() - started < 0.5
    assert events == []
    assert outer.wait_closed(0) is False

    release_inner_cleanup.set()
    inner_closer.join(2)
    assert outer.wait_closed(1)
    assert events == [
        "engine cleanup",
        "profile published",
        "public terminal",
    ]


@pytest.mark.parametrize(
    "lifecycle_factory",
    [
        lambda inner: duckdb_engine._DuckDBResultLifecycleStream(
            inner,
            deadline_monotonic=None,
            timeout_value=60,
            cancel_event=None,
        ),
        lambda inner: islanddb._IslandResultLifecycleStream(
            inner,
            deadline_monotonic=None,
            timeout_value=60,
            cancel_event=None,
        ),
    ],
)
def test_nested_cleanup_failure_is_preserved_as_failed_terminal(
    lifecycle_factory,
):
    batch = pa.record_batch({"id": [1]})
    inner_cleanup_started = threading.Event()
    release_inner_cleanup = threading.Event()
    cleanup_error = RuntimeError("cache lease cleanup failed")

    def finish_inner():
        inner_cleanup_started.set()
        assert release_inner_cleanup.wait(2)

    inner = ArrowBatchStream(
        batch.schema,
        iter([batch]),
        close_callback=finish_inner,
    )
    outer = ArrowBatchStream(
        batch.schema,
        inner,
        close_callback=lambda: (_ for _ in ()).throw(cleanup_error),
    )
    lifecycle = lifecycle_factory(outer)
    terminal = []
    lifecycle.add_terminal_callback(terminal.append)

    inner_closer = threading.Thread(target=inner.close)
    inner_closer.start()
    assert inner_cleanup_started.wait(1)

    lifecycle.close()
    assert terminal == []
    release_inner_cleanup.set()
    inner_closer.join(2)
    assert outer.wait_closed(1)

    assert outer.finalization_error is cleanup_error
    assert terminal == ["failed"]
    with pytest.raises(RuntimeError, match="cache lease cleanup failed") as raised:
        outer.close()
    assert raised.value is cleanup_error


def test_synchronous_nested_cleanup_failure_raises_on_first_close():
    batch = pa.record_batch({"id": [1]})
    cleanup_error = RuntimeError("cache lease cleanup failed")
    inner = ArrowBatchStream(batch.schema, iter([batch]))
    outer = ArrowBatchStream(
        batch.schema,
        inner,
        close_callback=lambda: (_ for _ in ()).throw(cleanup_error),
    )

    with pytest.raises(RuntimeError, match="cache lease cleanup failed") as raised:
        outer.close()

    assert raised.value is cleanup_error
    assert outer.finalization_error is cleanup_error


@pytest.mark.parametrize(
    ("lifecycle_factory", "outcome", "expected_kind"),
    [
        (
            lambda inner: duckdb_engine._DuckDBResultLifecycleStream(
                inner,
                deadline_monotonic=None,
                timeout_value=60,
                cancel_event=None,
            ),
            "completed",
            "completed",
        ),
        (
            lambda inner: duckdb_engine._DuckDBResultLifecycleStream(
                inner,
                deadline_monotonic=None,
                timeout_value=60,
                cancel_event=None,
            ),
            "failed",
            "failed",
        ),
        (
            lambda inner: islanddb._IslandResultLifecycleStream(
                inner,
                deadline_monotonic=None,
                timeout_value=60,
                cancel_event=None,
            ),
            "completed",
            "completed",
        ),
        (
            lambda inner: islanddb._IslandResultLifecycleStream(
                inner,
                deadline_monotonic=None,
                timeout_value=60,
                cancel_event=None,
            ),
            "failed",
            "failed",
        ),
    ],
)
def test_engine_lifecycle_terminal_waits_for_inner_finalization(
    lifecycle_factory, outcome, expected_kind,
):
    batch = pa.record_batch({"id": [1]})

    class DeferredFinalization:
        schema = batch.schema

        def __init__(self):
            self.callbacks = []

        def add_finalization_callback(self, callback):
            self.callbacks.append(callback)
            return True

        def __next__(self):
            if outcome == "completed":
                raise StopIteration
            raise RuntimeError("engine failed")

        def close(self):
            return None

    inner = DeferredFinalization()
    lifecycle = lifecycle_factory(inner)
    terminal = []
    finalized = []
    lifecycle.add_terminal_callback(terminal.append)
    assert lifecycle.add_finalization_callback(
        lambda: finalized.append(True)
    ) is True

    if outcome == "completed":
        with pytest.raises(StopIteration):
            next(lifecycle)
    else:
        with pytest.raises(RuntimeError, match="engine failed"):
            next(lifecycle)

    assert terminal == []
    assert finalized == []
    for callback in list(inner.callbacks):
        callback()
    assert terminal == [expected_kind]
    assert finalized == [True]


@pytest.mark.parametrize(
    "lifecycle_factory",
    [
        lambda inner: duckdb_engine._DuckDBResultLifecycleStream(
            inner,
            deadline_monotonic=None,
            timeout_value=60,
            cancel_event=None,
        ),
        lambda inner: islanddb._IslandResultLifecycleStream(
            inner,
            deadline_monotonic=None,
            timeout_value=60,
            cancel_event=None,
        ),
    ],
)
def test_engine_lifecycle_falls_back_when_finalization_is_declined(
    lifecycle_factory,
):
    batch = pa.record_batch({"id": [1]})

    class UnsupportedFinalization:
        schema = batch.schema

        def add_finalization_callback(self, _callback):
            return False

        def __next__(self):
            raise StopIteration

        def close(self):
            return None

    lifecycle = lifecycle_factory(UnsupportedFinalization())
    terminal = []
    lifecycle.add_terminal_callback(terminal.append)

    with pytest.raises(StopIteration):
        next(lifecycle)

    assert terminal == ["completed"]


def test_duckdb_cancel_does_not_block_on_uncooperative_active_fetch():
    batch = pa.record_batch({"id": [1]})
    entered = threading.Event()
    released = threading.Event()
    durations = []

    class BlockingReader:
        schema = batch.schema

        def __iter__(self):
            return self

        def __next__(self):
            entered.set()
            assert released.wait(2), "test did not release active fetch"
            return batch

        def close(self):
            return None

    producer = duckdb_engine._DuckDBArrowBatchIterator(
        BlockingReader(), MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
        duration_recorder=(
            lambda event, elapsed: durations.append((event, elapsed))
        ),
    )
    inner = ArrowBatchStream(
        batch.schema,
        producer,
        close_callback=lambda: durations.append(("CLEANUP", 0.0)),
    )
    lifecycle = duckdb_engine._DuckDBResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=60,
        cancel_event=None,
    )
    callback_snapshots = []
    lifecycle.add_terminal_callback(
        lambda kind: callback_snapshots.append(
            (kind, [event for event, _elapsed in durations])
        )
    )
    worker_errors = []

    def consume():
        try:
            next(lifecycle)
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(1)

    started = time.monotonic()
    lifecycle.cancel()
    assert time.monotonic() - started < 0.5
    assert callback_snapshots == []

    released.set()
    worker.join(2)
    assert not worker.is_alive()
    assert worker_errors
    assert callback_snapshots == [
        ("cancelled", ["RESULT_FETCH", "CLEANUP"]),
    ]


def test_monitored_row_budget_waits_for_duckdb_cleanup_before_cancel_record():
    batch = pa.record_batch({"id": [1]})
    entered = threading.Event()
    released = threading.Event()
    durations = []

    class BlockingReader:
        schema = batch.schema

        def __iter__(self):
            return self

        def __next__(self):
            entered.set()
            assert released.wait(2), "test did not release active fetch"
            return batch

        def close(self):
            return None

    producer = duckdb_engine._DuckDBArrowBatchIterator(
        BlockingReader(), MagicMock(),
        timed_out=threading.Event(), timeout_value=60,
        duration_recorder=(
            lambda event, elapsed: durations.append((event, elapsed))
        ),
    )
    inner = ArrowBatchStream(
        batch.schema,
        producer,
        close_callback=lambda: durations.append(("CLEANUP", 0.0)),
    )
    lifecycle = duckdb_engine._DuckDBResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=60,
        cancel_event=None,
    )
    bounded = data_reader._RowBudgetResultStream(lifecycle, 10)
    outcomes = []
    monitored = data_reader._MonitoredResultStream(
        bounded,
        lambda status, message, rows, size: outcomes.append(
            (status, message, rows, size, [event for event, _ in durations])
        ),
    )
    worker_errors = []

    def consume():
        try:
            next(monitored)
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(1)

    started = time.monotonic()
    monitored.cancel()
    assert time.monotonic() - started < 0.5
    assert outcomes == []

    released.set()
    worker.join(2)
    assert not worker.is_alive()
    assert worker_errors
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream cancelled",
        0,
        0,
        ["RESULT_FETCH", "CLEANUP"],
    )]


def test_monitored_row_budget_records_idle_backend_cancel_without_consumer():
    batch = pa.record_batch({"id": [1]})
    cancel_event = threading.Event()
    cleanup_complete = threading.Event()
    outcome_complete = threading.Event()
    inner = ArrowBatchStream(
        batch.schema,
        iter([batch]),
        close_callback=cleanup_complete.set,
    )
    lifecycle = duckdb_engine._DuckDBResultLifecycleStream(
        inner,
        deadline_monotonic=None,
        timeout_value=60,
        cancel_event=cancel_event,
    )
    bounded = data_reader._RowBudgetResultStream(lifecycle, 10)
    outcomes = []
    monitored = data_reader._MonitoredResultStream(
        bounded,
        lambda *outcome: (outcomes.append(outcome), outcome_complete.set()),
    )

    cancel_event.set()
    assert outcome_complete.wait(1)
    assert cleanup_complete.is_set()
    assert outcomes == [(
        data_reader.Status.ERROR.value,
        "result stream cancelled",
        0,
        0,
    )]
    assert monitored.closed is True
