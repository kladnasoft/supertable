# route: supertable.audit.tests.test_logger_security
"""Adversarial bounds, integrity, config, and webhook logger regressions."""
from __future__ import annotations

import importlib
import logging
import os
import queue
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import supertable.audit as audit_pkg
from supertable.audit import crypto
from supertable.audit.chain import GENESIS_HASH, InstanceChain
from supertable.audit.events import Actions, AuditEvent, EventCategory, INSTANCE_ID
from supertable.audit.logger import AuditConfig, AuditLogger


def _bare_logger(*, config: AuditConfig | None = None) -> AuditLogger:
    audit_logger = AuditLogger.__new__(AuditLogger)
    audit_logger._org = "acme"
    audit_logger._owner_pid = os.getpid()
    audit_logger._instance_id = INSTANCE_ID
    audit_logger._config = config or AuditConfig(enabled=True, log_queries=True)
    audit_logger._queue = queue.Queue(maxsize=10_000)
    audit_logger._stop_event = threading.Event()
    audit_logger._worker_done = threading.Event()
    audit_logger._admission_lock = threading.Lock()
    audit_logger._accepting = True
    audit_logger._thread = None
    audit_logger._chain = InstanceChain(instance_id=INSTANCE_ID)
    audit_logger._chain_lock = threading.Lock()
    audit_logger._redis_writer = None
    audit_logger._parquet_writer = None
    audit_logger._stats_lock = threading.Lock()
    audit_logger._stats = {
        "total_emitted": 0,
        "total_written": 0,
        "total_dropped": 0,
        "batches_written": 0,
        "pending_bytes": 0,
        "peak_pending_bytes": 0,
        "webhooks_dropped": 0,
        "redis_failures": 0,
        "parquet_failures": 0,
        "archive_retries": 0,
        "flush_failures": 0,
        "shutdown_failures": 0,
    }
    return audit_logger


def test_constructor_rejects_unsafe_org_before_backend_or_worker_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_redis = MagicMock()
    init_parquet = MagicMock()
    start_worker = MagicMock()
    monkeypatch.setattr(AuditLogger, "_init_redis", init_redis)
    monkeypatch.setattr(AuditLogger, "_init_parquet", init_parquet)
    monkeypatch.setattr(AuditLogger, "_start_worker", start_worker)

    with pytest.raises(ValueError):
        AuditLogger("../escape", AuditConfig(enabled=True))

    init_redis.assert_not_called()
    init_parquet.assert_not_called()
    start_worker.assert_not_called()


def test_hash_mode_rejects_redis_cluster_before_any_redis_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from supertable.audit.durable_journal import (
        AuditJournalConfigurationError,
    )

    calls: list[str] = []

    class RedisCluster:
        def __getattr__(self, name):
            calls.append(name)
            raise AssertionError("cluster backend must not be touched")

    monkeypatch.setitem(
        __import__("sys").modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=RedisCluster()),
    )

    with pytest.raises(AuditJournalConfigurationError):
        AuditLogger(
            "acme",
            AuditConfig(enabled=True, hash_chain=True, log_queries=True),
        )

    assert calls == []


def test_hash_mode_rejects_invalid_close_grace_before_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_redis = MagicMock()
    monkeypatch.setattr(AuditLogger, "_init_redis", init_redis)

    with pytest.raises(ValueError, match="close grace"):
        AuditLogger(
            "acme",
            AuditConfig(hash_chain=True, proof_close_grace_sec=86_401),
        )

    init_redis.assert_not_called()


def test_public_protection_uses_trusted_single_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.config.settings as settings_module

    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=""),
        raising=True,
    )
    crypto._fernet_instance = None
    crypto._fernet_loaded = False
    crypto._fernet_key = None
    audit_logger = _bare_logger()
    monkeypatch.setattr(
        audit_pkg,
        "get_audit_logger",
        lambda _organization, *, action=None: audit_logger,
    )

    audit_pkg.emit(
        organization="acme",
        category=EventCategory.DATA_ACCESS,
        action=Actions.QUERY_EXECUTE,
        detail={"sql": "SELECT private_value"},
    )

    queued = audit_logger._queue.get_nowait()
    assert queued is not None
    assert "SELECT private_value" not in queued.event.detail
    assert "sql_redacted" in queued.event.detail


def test_direct_reserved_output_masquerade_never_queues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_logger = _bare_logger()

    with pytest.raises(crypto.AuditEncryptionError, match="reserved"):
        audit_logger.emit(AuditEvent(
            organization="acme",
            action=Actions.DATA_WRITE,
            detail='{"nested":{"sql_encrypted":"plaintext"}}',
        ))

    assert audit_logger._queue.empty()


def test_reemitting_same_audit_event_gets_distinct_writer_owned_ids() -> None:
    audit_logger = _bare_logger()
    replayed = AuditEvent(
        event_id="caller-controlled-replay-id",
        organization="acme",
        action=Actions.DATA_WRITE,
    )

    assert audit_logger.emit(replayed) is True
    assert audit_logger.emit(replayed) is True

    first = audit_logger._queue.get_nowait()
    second = audit_logger._queue.get_nowait()
    assert first is not None and second is not None
    assert first.event.event_id != "caller-controlled-replay-id"
    assert second.event.event_id != "caller-controlled-replay-id"
    assert first.event.event_id != second.event.event_id


def test_caller_timestamp_is_replaced_with_writer_owned_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    writer_time_ms = 1_800_000_000_123
    monkeypatch.setattr(
        logger_module.time,
        "time_ns",
        lambda: writer_time_ms * 1_000_000,
    )

    assert audit_logger.emit(AuditEvent(
        organization="acme",
        action=Actions.DATA_WRITE,
        timestamp_ms=(1 << 63) - 1,
    )) is True

    queued = audit_logger._queue.get_nowait()
    assert queued is not None
    assert queued.event.timestamp_ms == writer_time_ms


def test_complete_event_field_limit_drops_without_queueing() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger.emit(AuditEvent(
        organization="acme",
        action=Actions.DATA_WRITE,
        actor_username="x" * (logger_module._MAX_AUDIT_EVENT_FIELD_BYTES + 1),
    ))

    assert audit_logger._queue.empty()
    assert audit_logger.stats["total_dropped"] == 1


def test_complete_serialized_event_limit_drops_many_valid_fields() -> None:
    audit_logger = _bare_logger()
    audit_logger.emit(AuditEvent(
        organization="acme",
        action=Actions.DATA_WRITE,
        actor_id="a" * 50_000,
        actor_username="b" * 50_000,
    ))

    assert audit_logger._queue.empty()
    assert audit_logger.stats["total_dropped"] == 1


def test_pending_byte_budget_releases_only_after_durable_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    event = AuditEvent(
        organization="acme",
        action=Actions.DATA_WRITE,
        detail="bounded",
    )
    event_size = logger_module._serialized_event_size(event)
    monkeypatch.setattr(
        logger_module, "_MAX_PENDING_AUDIT_BYTES", event_size + 1,
    )

    audit_logger.emit(event)
    audit_logger.emit(event)
    assert audit_logger.stats["total_emitted"] == 1
    assert audit_logger.stats["total_dropped"] == 1
    assert audit_logger.stats["pending_bytes"] == event_size

    queued = audit_logger._queue.get_nowait()
    assert queued is not None
    assert queued.event.detail == event.detail
    assert queued.event.organization == event.organization
    assert queued.event.event_id != event.event_id
    assert audit_logger.stats["pending_bytes"] == event_size
    audit_logger._complete_queued([queued])
    assert audit_logger.stats["pending_bytes"] == 0
    audit_logger.emit(event)
    assert audit_logger.stats["total_emitted"] == 2


@pytest.mark.parametrize(
    "event",
    [
        AuditEvent(organization="other", action=Actions.DATA_WRITE),
        AuditEvent(
            organization="acme",
            action=Actions.DATA_WRITE,
            chain_hash="forged",
        ),
        AuditEvent(
            organization="acme",
            action=Actions.DATA_WRITE,
            instance_id="foreign-instance",
        ),
    ],
)
def test_direct_event_integrity_binding_is_enforced(event: AuditEvent) -> None:
    audit_logger = _bare_logger()
    audit_logger.emit(event)
    assert audit_logger._queue.empty()
    assert audit_logger.stats["total_dropped"] == 1


def test_stop_cannot_block_on_full_queue() -> None:
    audit_logger = _bare_logger()
    audit_logger._queue = queue.Queue(maxsize=1)
    audit_logger.emit(AuditEvent(
        organization="acme", action=Actions.DATA_WRITE,
    ))
    assert audit_logger._queue.full()
    def drain_after_close() -> None:
        assert audit_logger._stop_event.wait(1.0)
        time.sleep(0.01)
        queued = audit_logger._queue.get(timeout=1.0)
        assert queued is not None
        audit_logger._complete_queued([queued])
        audit_logger._worker_done.set()

    audit_logger._thread = threading.Thread(target=drain_after_close)
    audit_logger._thread.start()

    audit_logger.stop(timeout_s=1.0)

    assert audit_logger._stop_event.is_set()
    assert audit_logger._accepting is False


@pytest.mark.parametrize(
    "changed",
    [
        AuditConfig(enabled=True, log_queries=True, alert_webhook="https://new"),
        AuditConfig(enabled=True, log_queries=True, fernet_key="new-key"),
        AuditConfig(enabled=True, log_queries=False),
    ],
)
def test_effective_config_change_replaces_or_drains_cached_logger(
    monkeypatch: pytest.MonkeyPatch,
    changed: AuditConfig,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")

    class FakeLogger:
        def __init__(self, _organization, config) -> None:
            self._config = config
            self.stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1

    old = FakeLogger("acme", AuditConfig(enabled=True, log_queries=True))
    monkeypatch.setattr(logger_module, "AuditLogger", FakeLogger)
    monkeypatch.setattr(logger_module, "_LOGGERS", {"acme": old})
    monkeypatch.setattr(
        logger_module, "_resolve_config_for", lambda _organization: changed,
    )

    result = logger_module.get_audit_logger(
        "acme", action=Actions.QUERY_EXECUTE,
    )

    assert old.stop_calls == 1
    if changed.log_queries:
        assert isinstance(result, FakeLogger)
        assert result is not old
        assert result._config == changed
    else:
        assert isinstance(result, logger_module.NullAuditLogger)


def test_on_to_off_seen_by_query_stops_cached_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    old = SimpleNamespace(
        _config=AuditConfig(enabled=True, log_queries=True),
        stop=MagicMock(),
    )
    monkeypatch.setattr(logger_module, "_LOGGERS", {"acme": old})
    monkeypatch.setattr(
        logger_module,
        "_resolve_config_for",
        lambda _organization: AuditConfig(enabled=False, log_queries=False),
    )

    result = logger_module.get_audit_logger(
        "acme", action=Actions.QUERY_EXECUTE,
    )

    assert isinstance(result, logger_module.NullAuditLogger)
    old.stop.assert_called_once()


def test_webhook_saturation_drops_without_thread_or_secret_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    secret_url = "https://user:password@example/hook?token=secret"
    audit_logger = _bare_logger(
        config=AuditConfig(alert_webhook=secret_url),
    )
    gate = SimpleNamespace(acquire=lambda **_kwargs: False)
    monkeypatch.setattr(logger_module, "_WEBHOOK_SLOTS", gate)

    class ForbiddenThread:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("saturated webhook created a thread")

    monkeypatch.setattr(logger_module.threading, "Thread", ForbiddenThread)
    with caplog.at_level(logging.WARNING, logger=logger_module.__name__):
        audit_logger._fire_webhook(AuditEvent(
            organization="acme",
            action=Actions.UNUSUAL_ACCESS_PATTERN,
            severity="critical",
        ))

    assert audit_logger.stats["webhooks_dropped"] == 1
    assert "Concurrency limit reached" in caplog.text
    assert secret_url not in caplog.text
    assert "password" not in caplog.text
    assert "secret" not in caplog.text


def test_parquet_failure_cannot_increment_durability_stats(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger._redis_writer = None
    secret = "s3://credential@bucket/private/audit.parquet"

    class FailingParquet:
        def write_batch(self, *_args, **_kwargs):
            raise RuntimeError(secret)

    audit_logger._parquet_writer = FailingParquet()
    with caplog.at_level(logging.ERROR, logger=logger_module.__name__):
        with pytest.raises(RuntimeError, match="credential"):
            audit_logger._write_batch([
                AuditEvent(organization="acme", action=Actions.DATA_WRITE),
            ])

    stats = audit_logger.stats
    assert stats["total_written"] == 0
    assert stats["batches_written"] == 0
    assert stats["total_dropped"] == 0
    assert stats["parquet_failures"] == 1
    assert secret not in caplog.text


def test_archive_success_is_required_before_chain_commit_and_stats() -> None:
    audit_logger = _bare_logger(
        config=AuditConfig(enabled=True, log_queries=True, hash_chain=True),
    )
    calls: list[str] = []

    class Parquet:
        def write_batch(self, _org, events, **kwargs):
            calls.append("parquet")
            assert all(event["chain_hash"] for event in events)
            return {
                "path": "acme/__audit__/batch.parquet",
                "event_count": len(events),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    class Redis:
        def write_batch(self, events):
            calls.append("redis")
            return [f"{index}-0" for index, _event in enumerate(events)]

        def save_chain_head(self, _head, _count):
            calls.append("checkpoint")

    audit_logger._parquet_writer = Parquet()
    audit_logger._redis_writer = Redis()
    event = AuditEvent(organization="acme", action=Actions.DATA_WRITE)

    audit_logger._write_batch([event])

    assert calls == ["parquet", "redis", "checkpoint"]
    assert audit_logger._chain.head != GENESIS_HASH
    assert audit_logger._chain.batch_count == 1
    assert audit_logger.stats["total_written"] == 1
    assert audit_logger.stats["batches_written"] == 1


def test_failed_archive_leaves_tentative_chain_uncommitted() -> None:
    audit_logger = _bare_logger(
        config=AuditConfig(enabled=True, log_queries=True, hash_chain=True),
    )

    class Parquet:
        def write_batch(self, *_args, **_kwargs):
            raise OSError("archive unavailable")

    audit_logger._parquet_writer = Parquet()
    with pytest.raises(OSError):
        audit_logger._write_batch([
            AuditEvent(organization="acme", action=Actions.DATA_WRITE),
        ])

    assert audit_logger._chain.head == GENESIS_HASH
    assert audit_logger._chain.batch_count == 0
    assert audit_logger.stats["total_written"] == 0
    assert audit_logger.stats["batches_written"] == 0


def test_incomplete_receipt_is_rejected_before_chain_commit_and_retries_stably() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger(
        config=AuditConfig(enabled=True, log_queries=True, hash_chain=True),
    )
    publication_ids: list[str] = []

    class Parquet:
        def write_batch(self, _org, events, **kwargs):
            publication_ids.append(kwargs["publication_id"])
            receipt = {
                "path": "acme/__audit__/stable.parquet",
                "event_count": len(events),
                "publication_id": kwargs["publication_id"],
            }
            if len(publication_ids) > 1:
                receipt.update({"bytes_written": 1, "file_hash": "f" * 64})
            return receipt

    audit_logger._parquet_writer = Parquet()
    event = AuditEvent(organization="acme", action=Actions.DATA_WRITE)

    with pytest.raises(logger_module.AuditArchiveUnavailable):
        audit_logger._write_batch([event])
    assert audit_logger._chain.head == GENESIS_HASH
    assert audit_logger._chain.batch_count == 0
    assert audit_logger.stats["total_written"] == 0

    audit_logger._write_batch([event])

    assert publication_ids[0] == publication_ids[1]
    assert audit_logger._chain.batch_count == 1
    assert audit_logger.stats["total_written"] == 1


def test_post_commit_logging_failure_can_never_reclassify_archive_as_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger(
        config=AuditConfig(enabled=True, log_queries=True, hash_chain=True),
    )
    publications: list[str] = []

    class Parquet:
        def write_batch(self, _org, events, **kwargs):
            publications.append(kwargs["publication_id"])
            return {
                "path": "acme/__audit__/stable.parquet",
                "event_count": len(events),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    audit_logger._parquet_writer = Parquet()
    monkeypatch.setattr(
        logger_module.logger,
        "debug",
        MagicMock(side_effect=RuntimeError("hostile logging handler")),
    )
    monkeypatch.setattr(
        logger_module.logger,
        "warning",
        MagicMock(side_effect=RuntimeError("hostile logging handler")),
    )

    audit_logger._write_batch([
        AuditEvent(organization="acme", action=Actions.DATA_WRITE),
    ])

    assert len(publications) == 1
    assert audit_logger._chain.batch_count == 1
    assert audit_logger.stats["total_written"] == 1


def test_failed_redis_checkpoint_cannot_fork_same_boot_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    monkeypatch.setattr(logger_module, "_PROCESS_CHAIN_STATE", {})
    organization = "chain-handoff"
    audit_logger = _bare_logger(
        config=AuditConfig(enabled=True, log_queries=True, hash_chain=True),
    )
    audit_logger._org = organization

    class Parquet:
        def write_batch(self, _org, events, **kwargs):
            return {
                "path": "chain-handoff/__audit__/batch.parquet",
                "event_count": len(events),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    class FailingCheckpointRedis:
        def write_batch(self, events):
            return [f"{index}-0" for index, _event in enumerate(events)]

        def save_chain_head(self, _head, _count):
            raise RuntimeError("checkpoint unavailable")

        def load_chain_head(self):
            return GENESIS_HASH, 0

    audit_logger._parquet_writer = Parquet()
    audit_logger._redis_writer = FailingCheckpointRedis()
    audit_logger._write_batch([
        AuditEvent(organization=organization, action=Actions.DATA_WRITE),
    ])
    committed_head = audit_logger._chain.head

    replacement = _bare_logger(config=audit_logger._config)
    replacement._org = organization
    replacement._redis_writer = FailingCheckpointRedis()
    replacement._restore_chain()

    assert committed_head != GENESIS_HASH
    assert replacement._chain.head == committed_head
    assert replacement._chain.batch_count == 1


def test_repeated_idle_stop_never_strands_a_wakeup_sentinel() -> None:
    for _index in range(50):
        audit_logger = _bare_logger()
        audit_logger._thread = threading.Thread(target=audit_logger._worker_loop)
        audit_logger._thread.start()

        audit_logger.stop(timeout_s=1.0)

        assert audit_logger._worker_done.is_set()
        assert audit_logger._queue.empty()
        assert audit_logger.stats["pending_bytes"] == 0


def test_concurrent_idle_stop_callers_cannot_enqueue_after_exit_decision() -> None:
    for _index in range(25):
        audit_logger = _bare_logger()
        audit_logger._thread = threading.Thread(target=audit_logger._worker_loop)
        audit_logger._thread.start()
        start = threading.Barrier(3)
        errors: list[BaseException] = []

        def stop_logger() -> None:
            start.wait()
            try:
                audit_logger.stop(timeout_s=1.0)
            except BaseException as exc:  # pragma: no cover - assertion payload
                errors.append(exc)

        callers = [threading.Thread(target=stop_logger) for _ in range(2)]
        for caller in callers:
            caller.start()
        start.wait()
        for caller in callers:
            caller.join(timeout=2.0)

        assert errors == []
        assert audit_logger._queue.empty()
        assert audit_logger._worker_done.is_set()


def test_saturated_flush_fails_without_holding_worker_admission_lock() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger._queue = queue.Queue(maxsize=1)
    audit_logger._queue.put_nowait(None)
    audit_logger._thread = MagicMock()
    audit_logger._thread.is_alive.return_value = True

    started = time.monotonic()
    with pytest.raises(logger_module.AuditFlushError):
        audit_logger.flush(timeout_s=1.0)

    assert time.monotonic() - started < 0.2
    assert audit_logger._admission_lock.acquire(blocking=False)
    audit_logger._admission_lock.release()


def test_dead_worker_with_dequeued_pending_bytes_never_reports_flush_success() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger._worker_done.set()
    audit_logger._thread = MagicMock()
    audit_logger._thread.is_alive.return_value = False
    audit_logger._stats["pending_bytes"] = 123

    with pytest.raises(logger_module.AuditFlushError):
        audit_logger.flush(timeout_s=0.1)


def test_saturation_logging_handler_can_read_stats_without_self_deadlock() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger._queue = queue.Queue(maxsize=1)
    first = AuditEvent(organization="acme", action=Actions.DATA_WRITE)
    second = AuditEvent(organization="acme", action=Actions.DATA_WRITE)
    assert audit_logger.emit(first) is True

    observed: list[int] = []

    class StatsReadingHandler(logging.Handler):
        def emit(self, _record: logging.LogRecord) -> None:
            observed.append(audit_logger.stats["total_dropped"])

    handler = StatsReadingHandler(level=logging.WARNING)
    logger_module.logger.addHandler(handler)
    emitter = threading.Thread(target=lambda: audit_logger.emit(second))
    try:
        emitter.start()
        emitter.join(timeout=1.0)
    finally:
        logger_module.logger.removeHandler(handler)

    assert not emitter.is_alive()
    assert observed == [1]


def test_startup_logging_handler_failure_cannot_orphan_uncached_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    monkeypatch.setattr(
        logger_module.logger,
        "info",
        MagicMock(side_effect=RuntimeError("hostile logging handler")),
    )

    audit_logger._start_worker()
    audit_logger.stop(timeout_s=1.0)

    assert audit_logger._worker_done.is_set()
    assert audit_logger._queue.empty()


def test_shutdown_state_rejects_late_worker_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    constructor = MagicMock()
    monkeypatch.setattr(logger_module, "_SHUTTING_DOWN", True)
    monkeypatch.setattr(logger_module, "AuditLogger", constructor)

    resolved = logger_module.get_audit_logger("acme", action=Actions.DATA_WRITE)

    assert isinstance(resolved, logger_module.NullAuditLogger)
    constructor.assert_not_called()


def test_worker_retries_same_stable_publication_before_flush_completes() -> None:
    audit_logger = _bare_logger(
        config=AuditConfig(
            enabled=True,
            log_queries=True,
            hash_chain=True,
            batch_size=1,
            flush_interval_sec=0.01,
        ),
    )
    publication_ids: list[str] = []

    class FlakyParquet:
        def write_batch(self, _org, events, **kwargs):
            publication_ids.append(kwargs["publication_id"])
            if len(publication_ids) == 1:
                raise TimeoutError("ambiguous first attempt")
            return {
                "path": "acme/__audit__/stable.parquet",
                "event_count": len(events),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    audit_logger._parquet_writer = FlakyParquet()
    audit_logger._thread = threading.Thread(target=audit_logger._worker_loop)
    audit_logger._thread.start()
    audit_logger.emit(AuditEvent(
        organization="acme", action=Actions.DATA_WRITE,
    ))

    audit_logger.flush(timeout_s=2.0)
    audit_logger.stop(timeout_s=2.0)

    assert len(publication_ids) == 2
    assert publication_ids[0] == publication_ids[1]
    assert audit_logger._chain.batch_count == 1
    assert audit_logger.stats["archive_retries"] == 1
    assert audit_logger.stats["pending_bytes"] == 0


def test_worker_never_combines_events_from_different_utc_dates() -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger(
        config=AuditConfig(
            enabled=True,
            log_queries=True,
            batch_size=10,
            flush_interval_sec=0.01,
        ),
    )
    day_ms = 86_400_000
    events = [
        AuditEvent(
            event_id="day-one",
            organization="acme",
            action=Actions.DATA_WRITE,
            timestamp_ms=day_ms - 1,
        ),
        AuditEvent(
            event_id="day-two",
            organization="acme",
            action=Actions.DATA_WRITE,
            timestamp_ms=day_ms,
        ),
    ]
    publications: list[tuple[list[int], int]] = []

    class RecordingParquet:
        def write_batch(self, _org, rows, **kwargs):
            publications.append((
                [row["timestamp_ms"] for row in rows],
                kwargs["published_at_ms"],
            ))
            return {
                "path": f"acme/__audit__/batch-{len(publications)}.parquet",
                "event_count": len(rows),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    audit_logger._parquet_writer = RecordingParquet()
    queued = [
        logger_module._QueuedAuditEvent(
            event=event,
            size_bytes=logger_module._serialized_event_size(event),
        )
        for event in events
    ]
    for item in queued:
        audit_logger._queue.put_nowait(item)
    audit_logger._stats["pending_bytes"] = sum(
        item.size_bytes for item in queued
    )
    audit_logger._thread = threading.Thread(target=audit_logger._worker_loop)
    audit_logger._thread.start()

    audit_logger.flush(timeout_s=2.0)
    audit_logger.stop(timeout_s=2.0)

    assert publications == [
        ([day_ms - 1], day_ms - 1),
        ([day_ms], day_ms),
    ]
    assert audit_logger.stats["pending_bytes"] == 0


def test_blocked_sink_never_causes_caller_side_io_or_replacement_race() -> None:
    audit_logger = _bare_logger(
        config=AuditConfig(
            enabled=True,
            log_queries=True,
            batch_size=1,
            flush_interval_sec=0.01,
        ),
    )
    entered = threading.Event()
    release = threading.Event()
    active = 0
    peak_active = 0

    class BlockingParquet:
        def write_batch(self, _org, events, **kwargs):
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            entered.set()
            assert release.wait(2.0)
            active -= 1
            return {
                "path": "acme/__audit__/blocked.parquet",
                "event_count": len(events),
                "bytes_written": 1,
                "file_hash": "f" * 64,
                "publication_id": kwargs["publication_id"],
            }

    audit_logger._parquet_writer = BlockingParquet()
    audit_logger._thread = threading.Thread(target=audit_logger._worker_loop)
    audit_logger._thread.start()
    audit_logger.emit(AuditEvent(
        organization="acme", action=Actions.DATA_WRITE,
    ))
    assert entered.wait(1.0)

    with pytest.raises(importlib.import_module(
        "supertable.audit.logger"
    ).AuditFlushError):
        audit_logger.flush(timeout_s=0.02)
    with pytest.raises(importlib.import_module(
        "supertable.audit.logger"
    ).AuditShutdownError):
        audit_logger.stop(timeout_s=0.02)

    queued_before = audit_logger._queue.qsize()
    audit_logger.emit(AuditEvent(
        organization="acme", action=Actions.DATA_WRITE,
    ))
    assert audit_logger._queue.qsize() == queued_before
    assert peak_active == 1

    release.set()
    assert audit_logger._worker_done.wait(2.0)
    audit_logger.stop(timeout_s=0.1)
    assert audit_logger.stats["total_written"] == 1
    assert audit_logger.stats["pending_bytes"] == 0


def test_emit_that_finishes_validation_after_stop_cannot_strand_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    audit_logger = _bare_logger()
    audit_logger._worker_done.set()
    audit_logger._thread = MagicMock()
    entered = threading.Event()
    release = threading.Event()
    real_size = logger_module._serialized_event_size

    def delayed_size(event):
        entered.set()
        assert release.wait(1.0)
        return real_size(event)

    monkeypatch.setattr(logger_module, "_serialized_event_size", delayed_size)
    emitter = threading.Thread(target=lambda: audit_logger.emit(AuditEvent(
        organization="acme", action=Actions.DATA_WRITE,
    )))
    emitter.start()
    assert entered.wait(1.0)

    audit_logger.stop(timeout_s=0.1)
    release.set()
    emitter.join(timeout=1.0)

    assert not emitter.is_alive()
    assert audit_logger._queue.empty()
    assert audit_logger.stats["total_emitted"] == 0
    assert audit_logger.stats["total_dropped"] == 1


def test_config_replacement_is_quarantined_when_old_worker_cannot_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    old = SimpleNamespace(
        _config=AuditConfig(enabled=True, log_queries=True),
        stop=MagicMock(side_effect=logger_module.AuditShutdownError("blocked")),
    )
    constructor = MagicMock()
    monkeypatch.setattr(logger_module, "AuditLogger", constructor)
    monkeypatch.setattr(logger_module, "_LOGGERS", {"acme": old})
    monkeypatch.setattr(
        logger_module,
        "_resolve_config_for",
        lambda _organization: AuditConfig(
            enabled=True, log_queries=True, alert_webhook="https://new",
        ),
    )

    with pytest.raises(logger_module.AuditShutdownError):
        logger_module.get_audit_logger("acme", action=Actions.DATA_WRITE)

    assert logger_module._LOGGERS["acme"] is old
    constructor.assert_not_called()
