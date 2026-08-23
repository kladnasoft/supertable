from __future__ import annotations

import importlib
import threading
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
from supertable.engine import duckdb_engine
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


def _auto_stream_executor(monkeypatch, cache, island):
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
