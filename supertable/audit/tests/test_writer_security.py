# route: supertable.audit.tests.test_writer_security
"""Security and durability regressions for the audit storage writers."""
from __future__ import annotations

import hashlib
import logging
import traceback

import pytest

from supertable.audit.chain import MerkleProof
from supertable.audit.writer_parquet import (
    AuditArchiveWriteError,
    AuditReadError,
    ParquetAuditWriter,
)
from supertable.audit.writer_redis import (
    AuditRedisCheckpointError,
    AuditRedisQueryError,
    RedisAuditWriter,
)
from supertable.storage.storage_interface import ObjectMetadata


_SECRET_ERROR = (
    "redis://admin:top-secret@example.test/0 "
    "/srv/customer-acme/token=top-secret.parquet"
)
_SECRET_PATH = "/srv/customer-acme/token=top-secret.parquet"


class SecretBackendError(RuntimeError):
    """Backend failure whose message must never reach application logs."""


def _raise_secret(*_args, **_kwargs):
    raise SecretBackendError(_SECRET_ERROR)


def _minimal_event() -> dict[str, object]:
    return {
        "event_id": "evt-1",
        "timestamp_ms": 1_700_000_000_000,
        "organization": "customer-acme",
        "action": "query_execute",
    }


def _writer_messages(caplog: pytest.LogCaptureFixture, module: str) -> str:
    return "\n".join(
        record.getMessage() for record in caplog.records
        if record.name == module
    )


def _assert_log_is_redacted(messages: str) -> None:
    assert "SecretBackendError" in messages
    assert "top-secret" not in messages
    assert "redis://" not in messages
    assert "/srv/" not in messages


class _FailingParquetStorage:
    write_bytes = _raise_secret
    write_json = _raise_secret
    read_json = _raise_secret
    list_files = _raise_secret


def test_parquet_write_batch_sanitizes_storage_failure_and_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    writer = ParquetAuditWriter(storage=_FailingParquetStorage())
    caplog.set_level(logging.DEBUG, logger="supertable.audit.writer_parquet")

    with pytest.raises(AuditArchiveWriteError) as raised:
        writer.write_batch("customer-acme", [_minimal_event()])

    formatted = "".join(traceback.format_exception(
        raised.type,
        raised.value,
        raised.tb,
    ))
    assert _SECRET_ERROR not in formatted
    assert "During handling of the above exception" not in formatted
    messages = _writer_messages(caplog, "supertable.audit.writer_parquet")
    _assert_log_is_redacted(messages)
    assert "customer-acme/__audit__" not in messages
    assert "write_bytes failed" in messages


def test_parquet_recoverable_failures_log_only_exception_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    writer = ParquetAuditWriter(storage=_FailingParquetStorage())
    caplog.set_level(logging.WARNING, logger="supertable.audit.writer_parquet")
    proof = MerkleProof(date="2026-08-24")
    proof.compute_root()

    assert writer.save_chain_proof(
        "customer-acme", proof,
    ) is False
    with pytest.raises(AuditReadError):
        writer.load_chain_proof("customer-acme", "20260824")
    assert writer.load_chain_proof(
        "customer-acme", "20260824", strict=False,
    ) is None
    with pytest.raises(AuditReadError) as partition_error:
        writer.list_partition_files("customer-acme", 2026, 8, 24)
    with pytest.raises(AuditReadError) as traversal_error:
        writer.list_partitions("customer-acme")

    for caught in (partition_error, traversal_error):
        formatted = "".join(traceback.format_exception(
            caught.type,
            caught.value,
            caught.tb,
        ))
        assert _SECRET_ERROR not in formatted
        assert "During handling of the above exception" not in formatted

    messages = _writer_messages(caplog, "supertable.audit.writer_parquet")
    _assert_log_is_redacted(messages)


class _ParquetReadFailureStorage:
    def list_files(self, *_args, **_kwargs):
        return [
            "customer-acme/__audit__/year=2026/month=08/day=24/"
            "audit_secret.parquet"
        ]

    def stat_object(self, *_args, **_kwargs):
        return ObjectMetadata(size=128, version="v1")

    download_to_file = _raise_secret


def test_parquet_read_failure_does_not_log_source_path_or_exception_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    writer = ParquetAuditWriter(storage=_ParquetReadFailureStorage())
    caplog.set_level(logging.WARNING, logger="supertable.audit.writer_parquet")

    with pytest.raises(AuditReadError):
        writer.read_batch_events("customer-acme", 2026, 8, 24)

    messages = _writer_messages(caplog, "supertable.audit.writer_parquet")
    _assert_log_is_redacted(messages)
    assert _SECRET_PATH not in messages


class _AmbiguousPublicationStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.write_calls = 0

    def stat_object(self, path: str) -> ObjectMetadata:
        try:
            payload = self.objects[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        return ObjectMetadata(
            size=len(payload),
            version="v1",
            checksum_sha256=hashlib.sha256(payload).hexdigest(),
        )

    def read_range(self, path, offset, length, *, expected=None):
        return self.objects[path][offset:offset + length]

    def download_to_file(
        self, path, file_obj, *, expected=None, chunk_size=1024 * 1024,
    ):
        payload = self.objects[path]
        file_obj.write(payload)
        return len(payload)

    def create_bytes_if_absent(self, path: str, payload: bytes) -> bool:
        self.write_calls += 1
        if path in self.objects:
            return False
        self.objects[path] = payload
        raise TimeoutError("provider timed out after commit token=secret")


def test_stable_publication_reconciles_ambiguous_write_exactly_once() -> None:
    storage = _AmbiguousPublicationStorage()
    writer = ParquetAuditWriter(storage=storage)
    publication_id = "a" * 64

    first = writer.write_batch(
        "customer-acme",
        [_minimal_event()],
        publication_id=publication_id,
        published_at_ms=1_700_000_000_000,
    )
    second = writer.write_batch(
        "customer-acme",
        [_minimal_event()],
        publication_id=publication_id,
        published_at_ms=1_700_000_000_000,
    )

    assert first["publication_id"] == publication_id
    assert first["reconciled"] is True
    assert second == first
    # Both attempts use the provider-side create precondition; the second
    # observes the exact existing object and cannot overwrite it.
    assert storage.write_calls == 2
    assert len(storage.objects) == 1


def test_stable_publication_refuses_existing_different_object() -> None:
    storage = _AmbiguousPublicationStorage()
    writer = ParquetAuditWriter(storage=storage)
    publication_id = "b" * 64
    receipt = writer.write_batch(
        "customer-acme",
        [_minimal_event()],
        publication_id=publication_id,
        published_at_ms=1_700_000_000_000,
    )
    storage.objects[receipt["path"]] = b"different-object"

    with pytest.raises(RuntimeError, match="different bytes"):
        writer.write_batch(
            "customer-acme",
            [_minimal_event()],
            publication_id=publication_id,
            published_at_ms=1_700_000_000_000,
        )


class _FailingRedis:
    xgroup_create = _raise_secret
    pipeline = _raise_secret
    hset = _raise_secret
    hgetall = _raise_secret
    xrevrange = _raise_secret
    xinfo_groups = _raise_secret
    xgroup_destroy = _raise_secret


def test_redis_failure_logs_do_not_expose_backend_messages(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="supertable.audit.writer_redis")
    writer = RedisAuditWriter(
        _FailingRedis(), "customer-acme", "instance-1",
    )

    assert writer.write_batch([_minimal_event()]) == []
    with pytest.raises(AuditRedisCheckpointError):
        writer.save_chain_head("a" * 64, 1)
    assert writer.load_chain_head() == ("", 0)
    with pytest.raises(AuditRedisQueryError):
        writer.query()
    assert writer.trim_acknowledged() == 0
    assert writer.create_consumer_group("siem") is False
    assert writer.delete_consumer_group("siem") is False
    assert writer.list_consumer_groups() == []

    messages = _writer_messages(caplog, "supertable.audit.writer_redis")
    _assert_log_is_redacted(messages)


@pytest.mark.parametrize(
    "chain_state",
    [
        {"head": "signed-url-token-DO-NOT-LOG", "batch_count": "1"},
        {"head": "a" * 64, "batch_count": "redis-password-DO-NOT-LOG"},
        {"head": None, "batch_count": "1"},
    ],
)
def test_redis_chain_checkpoint_rejects_poisoned_state_without_logging_content(
    chain_state,
    caplog,
) -> None:
    class PoisonedRedis:
        def xgroup_create(self, *_args, **_kwargs):
            return True

        def hgetall(self, _key):
            return chain_state

    caplog.set_level(logging.WARNING, logger="supertable.audit.writer_redis")
    writer = RedisAuditWriter(PoisonedRedis(), "customer-acme", "instance-1")

    assert writer.load_chain_head() == ("", 0)
    assert "DO-NOT-LOG" not in caplog.text


def _complete_redis_fields() -> dict[str, str]:
    from supertable.audit.events import AuditEvent

    return {
        key: str(value)
        for key, value in AuditEvent(
            event_id="event-1",
            organization="customer-acme",
            instance_id="instance-1",
        ).to_dict().items()
    }


class _QueryRedis:
    def __init__(self, entries):
        self.entries = entries

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def xrevrange(self, *_args, **_kwargs):
        return self.entries


@pytest.mark.parametrize("mutation", ["extra_schema", "oversized_field"])
def test_redis_query_rejects_unbounded_or_unexpected_event_payloads(
    mutation: str,
) -> None:
    fields = _complete_redis_fields()
    if mutation == "extra_schema":
        fields["unexpected"] = "value"
    else:
        fields["detail"] = "x" * (64 * 1024 + 1)
    writer = RedisAuditWriter(
        _QueryRedis([("1-0", fields)]),
        "customer-acme",
        "instance-1",
    )

    with pytest.raises(AuditRedisQueryError):
        writer.query(count=1)


class _ConsumerRedis:
    def __init__(self):
        self.eval_calls = []
        self.groups = []
        self.create_result = 1

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def eval(self, *args):
        self.eval_calls.append(args)
        return self.create_result

    def xinfo_groups(self, *_args, **_kwargs):
        return self.groups


def test_siem_group_and_start_validation_precede_external_group_creation() -> None:
    backend = _ConsumerRedis()
    writer = RedisAuditWriter(
        backend, "customer-acme", "instance-1",
    )

    for group, start in (("../escape", "$"), ("siem", "not-a-stream-id")):
        with pytest.raises(ValueError):
            writer.create_consumer_group(group, start)

    assert backend.eval_calls == []


def test_siem_creation_uses_atomic_bounded_backend_operation() -> None:
    backend = _ConsumerRedis()
    writer = RedisAuditWriter(
        backend, "customer-acme", "instance-1",
    )

    assert writer.create_consumer_group(
        "siem-main", "0-0", max_consumers=7,
    ) is True
    assert len(backend.eval_calls) == 1
    assert backend.eval_calls[0][1:] == (
        1,
        "supertable:customer-acme:system:audit:stream",
        "siem-main",
        "0-0",
        7,
        "__archival__",
    )


def test_siem_group_metadata_has_hard_envelope_and_field_bounds() -> None:
    backend = _ConsumerRedis()
    writer = RedisAuditWriter(
        backend, "customer-acme", "instance-1",
    )
    backend.groups = [{
        "name": f"group-{index}",
        "consumers": 0,
        "pending": 0,
        "last-delivered-id": "0-0",
        "lag": 0,
    } for index in range(102)]

    assert writer.list_consumer_groups() == []

    backend.groups = [{
        b"name": b"siem-main",
        b"consumers": 1,
        b"pending": 2,
        b"last-delivered-id": b"3-0",
        b"lag": 4,
    }]
    assert writer.list_consumer_groups() == [{
        "name": "siem-main",
        "consumers": 1,
        "pending": 2,
        "last_delivered_id": "3-0",
        "lag": 4,
        "is_internal": False,
    }]
