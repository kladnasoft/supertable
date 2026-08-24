from __future__ import annotations

import importlib
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine import executor as executor_module
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor, _refresh_presigned_reflection
from supertable.engine.plan_stats import PlanStats
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.timer import Timer


def _linked_snapshot(*, expires_ms: int) -> SuperSnapshot:
    return SuperSnapshot(
        "lake",
        "events",
        4,
        ["https://provider.invalid/data.parquet?X-Amz-Signature=secret"],
        {"id"},
        resource_keys=[
            "https://provider.invalid/data.parquet?X-Amz-Signature=secret"
        ],
        share_policy_fingerprint="a" * 64,
        share_allowed_columns=["id"],
        share_credential_expires_ms=expires_ms,
        resource_cache_identities=["share-cache-v1:" + "b" * 64],
    )


def _linked_leaf_payload(
    *, table: str, local_generation: int = 11, link_id: str = "link-1",
) -> dict:
    return {
        "simple_name": table,
        "_linked_share": link_id,
        "_linked_generation": local_generation,
        "_linked_provider_generated_ms": 22,
        "_linked_provider_manifest_digest": "c" * 64,
    }


def _linked_control(
    *,
    tables=("events",),
    local_generation: int = 11,
    link_id: str = "link-1",
) -> dict:
    return {
        "link_id": link_id,
        "alias_prefix": "",
        "publication_generation": local_generation,
        "_linked_provider_generated_ms": 22,
        "_linked_provider_manifest_digest": "c" * 64,
        "cached_manifest": {
            "tables": [{"table": table} for table in tables],
        },
    }


def _authority_estimator(controls):
    estimator = object.__new__(DataEstimator)
    by_link = {control["link_id"]: control for control in controls}
    calls = []

    def exact_control(organization, super_name, link_id):
        calls.append((organization, super_name, link_id))
        return by_link.get(link_id)

    estimator.catalog = SimpleNamespace(
        get_authoritative_linked_share=exact_control,
    )
    estimator._linked_authority_cache = {}
    estimator._authority_lookup_calls = calls
    return estimator


def test_sdk_snapshot_acquisition_rejects_partial_or_orphan_linked_leaf():
    partial = _authority_estimator([_linked_control()])
    with pytest.raises(RuntimeError, match="does not match"):
        partial._validate_linked_snapshot_authority(
            "consumer",
            "lake",
            "events",
            _linked_leaf_payload(table="events", local_generation=12),
        )

    orphan = _authority_estimator([])
    with pytest.raises(RuntimeError, match="no authoritative control"):
        orphan._validate_linked_snapshot_authority(
            "consumer", "lake", "events", _linked_leaf_payload(table="events"),
        )


def test_sdk_aggregate_snapshot_acquisition_validates_every_linked_child():
    estimator = _authority_estimator([
        _linked_control(tables=("events", "users")),
    ])
    estimator._validate_linked_snapshot_authority(
        "consumer", "lake", "events", _linked_leaf_payload(table="events"),
    )
    estimator._validate_linked_snapshot_authority(
        "consumer", "lake", "users", _linked_leaf_payload(table="users"),
    )
    assert estimator._authority_lookup_calls == [
        ("consumer", "lake", "link-1"),
    ]
    with pytest.raises(RuntimeError, match="outside"):
        estimator._validate_linked_snapshot_authority(
            "consumer", "lake", "payments",
            _linked_leaf_payload(table="payments"),
        )
    assert len(estimator._authority_lookup_calls) == 1


def test_sdk_linked_authority_cache_has_unique_link_budget(monkeypatch):
    data_estimator_module = importlib.import_module(
        "supertable.engine.data_estimator"
    )
    monkeypatch.setattr(
        data_estimator_module,
        "_MAX_LINKED_AUTHORITIES_PER_ESTIMATE",
        2,
    )
    estimator = _authority_estimator([
        _linked_control(link_id="link-1"),
        _linked_control(link_id="link-2"),
        _linked_control(link_id="link-3"),
    ])
    for link_id in ("link-1", "link-2"):
        estimator._validate_linked_snapshot_authority(
            "consumer",
            "lake",
            "events",
            _linked_leaf_payload(table="events", link_id=link_id),
        )
    with pytest.raises(RuntimeError, match="exceeds its safety limit"):
        estimator._validate_linked_snapshot_authority(
            "consumer",
            "lake",
            "events",
            _linked_leaf_payload(table="events", link_id="link-3"),
        )
    assert len(estimator._authority_lookup_calls) == 2


class _OneBatchStream:
    schema = pa.schema([("id", pa.int64())])

    def __init__(self):
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        self._done = True
        return pa.record_batch([pa.array([7])], names=["id"])

    def close(self):
        return None

    cancel = close


def test_linked_duckdb_stream_never_uses_consumer_presigner(monkeypatch):
    storage = MagicMock()
    storage.presign.side_effect = AssertionError(
        "provider URL reached consumer presigner"
    )
    executor = Executor(storage=storage, organization="consumer")
    duck = MagicMock()
    duck.cache_state.return_value = {}
    duck.execute_stream.return_value = _OneBatchStream()
    executor.duckdb_exec = duck
    executor._file_cache = False
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    reflection = Reflection(
        "linked", 4096, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 600) * 1000))],
    )

    stream, used = executor.execute_stream(
        Engine.DUCKDB,
        reflection,
        MagicMock(),
        SimpleNamespace(),
        Timer(),
        PlanStats(),
        "",
        deadline_monotonic=time.monotonic() + 10,
    )
    try:
        assert next(stream).column(0).to_pylist() == [7]
    finally:
        stream.close()

    assert used == "duckdb"
    storage.presign.assert_not_called()
    assert duck.execute_stream.call_args.kwargs["reflection"] is reflection


def test_linked_duckdb_stream_rejects_expiry_before_engine_or_presigner(
    monkeypatch,
):
    storage = MagicMock()
    executor = Executor(storage=storage, organization="consumer")
    duck = MagicMock()
    duck.cache_state.return_value = {}
    executor.duckdb_exec = duck
    executor._file_cache = False
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    reflection = Reflection(
        "linked", 4096, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 5) * 1000))],
    )

    with pytest.raises(RuntimeError, match="do not cover the query deadline"):
        executor.execute_stream(
            Engine.DUCKDB,
            reflection,
            MagicMock(),
            SimpleNamespace(),
            Timer(),
            PlanStats(),
            "",
            deadline_monotonic=time.monotonic() + 10,
        )

    storage.presign.assert_not_called()
    duck.execute_stream.assert_not_called()


@pytest.mark.parametrize("requested_engine", [Engine.ISLANDDB, Engine.AUTO])
@pytest.mark.parametrize("streaming", [False, True])
def test_linked_expiry_is_rejected_at_common_entry_before_island_routing_or_cache(
    monkeypatch, requested_engine, streaming,
):
    storage = MagicMock()
    executor = Executor(storage=storage, organization="consumer")
    resolve_bundle = MagicMock(
        side_effect=AssertionError("routing config was read before admission")
    )
    cache_access = MagicMock(
        side_effect=AssertionError("cache was touched before admission")
    )
    island_prepare = MagicMock(
        side_effect=AssertionError("IslandDB was touched before admission")
    )
    monkeypatch.setattr(
        executor_module, "resolve_engine_bundle", resolve_bundle,
    )
    monkeypatch.setattr(executor, "_get_file_cache", cache_access)
    monkeypatch.setattr(
        executor.island_exec, "prepare_execution", island_prepare,
    )
    reflection = Reflection(
        "linked", 4096, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 5) * 1000))],
    )

    with pytest.raises(RuntimeError, match="do not cover the query deadline"):
        if streaming:
            executor.execute_stream(
                requested_engine,
                reflection,
                MagicMock(),
                SimpleNamespace(),
                Timer(),
                PlanStats(),
                "",
                deadline_monotonic=time.monotonic() + 10,
            )
        else:
            executor.execute(
                requested_engine,
                reflection,
                MagicMock(),
                SimpleNamespace(),
                Timer(),
                PlanStats(),
                "",
            )

    resolve_bundle.assert_not_called()
    cache_access.assert_not_called()
    island_prepare.assert_not_called()
    storage.presign.assert_not_called()


def test_materialized_local_island_receives_the_common_absolute_deadline(
    monkeypatch,
):
    executor = Executor(storage=MagicMock(), organization="consumer")
    executor._file_cache = False
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": MagicMock()}, ()),
    )
    prepared = SimpleNamespace(
        capability=SimpleNamespace(supported=True, reasons=()),
    )
    executor.island_exec.prepare_execution = MagicMock(
        return_value=prepared
    )
    executor.island_exec.execute = MagicMock(return_value=MagicMock())
    reflection = Reflection(
        "local", 4096, 1,
        [SuperSnapshot(
            "lake", "events", 1, ["/tmp/events.parquet"], {"id"},
            resource_keys=["events.parquet"],
        )],
    )

    started = time.monotonic()
    _result, used = executor.execute(
        Engine.ISLANDDB,
        reflection,
        MagicMock(),
        SimpleNamespace(),
        Timer(),
        PlanStats(),
        "",
    )

    assert used == "islanddb"
    admitted_deadline = (
        executor.island_exec.execute.call_args.kwargs["deadline_monotonic"]
    )
    assert started < admitted_deadline <= started + 61


@pytest.mark.parametrize("streaming", [False, True])
def test_explicit_island_rejects_linked_bearer_before_config_cache_or_provider(
    monkeypatch, streaming,
):
    storage = MagicMock()
    storage.presign.side_effect = AssertionError("provider work was attempted")
    executor = Executor(storage=storage, organization="consumer")
    executor.island_exec.prepare_execution = MagicMock(
        side_effect=AssertionError("IslandDB preparation was attempted")
    )
    executor.island_exec.execute = MagicMock(
        side_effect=AssertionError("IslandDB materialization was attempted")
    )
    executor.island_exec.execute_stream = MagicMock(
        side_effect=AssertionError("IslandDB streaming was attempted")
    )
    resolve_bundle = MagicMock(
        side_effect=AssertionError("routing config was read before rejection")
    )
    catalog_access = MagicMock(
        side_effect=AssertionError("catalog was read before rejection")
    )
    cache_access = MagicMock(
        side_effect=AssertionError("cache was touched before rejection")
    )
    monkeypatch.setattr(
        executor_module, "resolve_engine_bundle", resolve_bundle,
    )
    monkeypatch.setattr(executor, "_get_catalog", catalog_access)
    monkeypatch.setattr(executor, "_get_file_cache", cache_access)
    reflection = Reflection(
        "linked", 4096, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 600) * 1000))],
    )

    with pytest.raises(RuntimeError, match="provider-linked bearer"):
        if streaming:
            executor.execute_stream(
                Engine.ISLANDDB,
                reflection,
                MagicMock(),
                SimpleNamespace(),
                Timer(),
                PlanStats(),
                "",
                deadline_monotonic=time.monotonic() + 10,
            )
        else:
            executor.execute(
                Engine.ISLANDDB,
                reflection,
                MagicMock(),
                SimpleNamespace(),
                Timer(),
                PlanStats(),
                "",
            )

    resolve_bundle.assert_not_called()
    catalog_access.assert_not_called()
    cache_access.assert_not_called()
    storage.presign.assert_not_called()
    executor.island_exec.prepare_execution.assert_not_called()
    executor.island_exec.execute.assert_not_called()
    executor.island_exec.execute_stream.assert_not_called()


@pytest.mark.parametrize("streaming", [False, True])
def test_auto_linked_bearer_routes_to_duckdb_and_records_island_reason(
    monkeypatch, streaming,
):
    storage = MagicMock()
    storage.presign.side_effect = AssertionError("consumer presign was attempted")
    executor = Executor(storage=storage, organization="consumer")
    executor._file_cache = False
    executor.duckdb_exec = MagicMock()
    executor.duckdb_exec.cache_state.return_value = {}
    executor.duckdb_exec.execute.return_value = MagicMock()
    executor.duckdb_exec.execute_stream.return_value = _OneBatchStream()
    executor.island_exec = MagicMock()
    monkeypatch.setattr(executor, "_get_catalog", lambda: None)
    monkeypatch.setattr(executor, "_active_spark_clusters", lambda: [])
    config = SimpleNamespace(
        engine_freshness_sec=300,
        engine_spark_min_bytes=0,
        engine_island_min_bytes=100 * 1024 * 1024,
    )
    monkeypatch.setattr(
        executor_module,
        "resolve_engine_bundle",
        lambda *_args: ({"duckdb": config}, ()),
    )
    reflection = Reflection(
        "linked", 512 * 1024 * 1024, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 600) * 1000))],
    )
    plan_stats = PlanStats()

    if streaming:
        result, used = executor.execute_stream(
            Engine.AUTO,
            reflection,
            MagicMock(original_query="SELECT id FROM events"),
            SimpleNamespace(),
            Timer(),
            plan_stats,
            "",
            deadline_monotonic=time.monotonic() + 10,
        )
        try:
            assert next(result).column(0).to_pylist() == [7]
        finally:
            result.close()
    else:
        _result, used = executor.execute(
            Engine.AUTO,
            reflection,
            MagicMock(original_query="SELECT id FROM events"),
            SimpleNamespace(),
            Timer(),
            plan_stats,
            "",
        )

    assert used == "duckdb"
    executor.island_exec.can_execute.assert_not_called()
    executor.island_exec.prepare_execution.assert_not_called()
    storage.presign.assert_not_called()
    routing = next(
        stat["AUTO_ROUTING"]
        for stat in plan_stats.stats
        if "AUTO_ROUTING" in stat
    )
    assert routing["selected_engine"] == "duckdb"
    assert routing["availability"]["island_linked_bearer_safe"] is False
    island = next(
        candidate for candidate in routing["candidates"]
        if candidate["engine"] == "islanddb"
    )
    assert not island["eligible"]
    assert any(
        "provider-linked bearer" in reason
        for reason in island["rejection_reasons"]
    )


def test_explicit_spark_rejects_linked_bearer_before_config_or_cluster_setup(
    monkeypatch,
):
    executor = Executor(storage=MagicMock(), organization="consumer")
    resolve_bundle = MagicMock(
        side_effect=AssertionError("Spark config was read before rejection")
    )
    monkeypatch.setattr(
        executor_module, "resolve_engine_bundle", resolve_bundle,
    )
    reflection = Reflection(
        "linked", 4096, 1,
        [_linked_snapshot(expires_ms=int((time.time() + 600) * 1000))],
    )

    with pytest.raises(RuntimeError, match="provider-linked bearer"):
        executor.execute(
            Engine.SPARK_SQL,
            reflection,
            MagicMock(),
            SimpleNamespace(),
            Timer(),
            PlanStats(),
            "",
        )

    resolve_bundle.assert_not_called()
    assert executor.spark_exec is None


def test_mixed_refresh_rotates_only_consumer_owned_remote_paths():
    linked = _linked_snapshot(expires_ms=int((time.time() + 600) * 1000))
    local_remote = SuperSnapshot(
        "lake", "owned", 2,
        ["s3://consumer-bucket/owned.parquet"],
        {"id"},
        resource_keys=["consumer/owned.parquet"],
    )
    reflection = Reflection("mixed", 8192, 2, [linked, local_remote])
    storage = MagicMock()
    storage.presign.side_effect = (
        lambda key, *, expiry_seconds:
        f"https://consumer.invalid/{key}?sig=fresh&ttl={expiry_seconds}"
    )

    refreshed = _refresh_presigned_reflection(storage, reflection)

    assert refreshed.supers[0].files == linked.files
    assert refreshed.supers[1].files[0].startswith(
        "https://consumer.invalid/consumer/owned.parquet"
    )
    storage.presign.assert_called_once()
    assert storage.presign.call_args.args == ("consumer/owned.parquet",)
    assert (
        refreshed.supers[1].resource_credential_generations[0] > 0
    )


@pytest.mark.parametrize(
    "value",
    [
        "https://provider.invalid/object?signature=secret",
        "s3://provider-bucket/object",
        "",
    ],
)
def test_builtin_presign_key_guard_rejects_external_or_empty_values(value):
    with pytest.raises(ValueError, match="storage object key"):
        StorageInterface._require_presign_object_key(value)
