# route: supertable.audit.tests.test_confidentiality
"""Confidentiality regressions for audit control and read surfaces."""
from __future__ import annotations

import json
import logging
import sys
import traceback
from types import SimpleNamespace

import pytest

from supertable.audit import admin, consumers, reader, retention


_SECRET_ERROR = (
    "redis://admin:top-secret@example.test/0 "
    "/srv/customer-acme/token=top-secret.parquet"
)
_SECRET_PREFIX = "s3://admin:top-secret@example.test/private/audit"


class SecretBackendError(RuntimeError):
    """Backend failure whose message must not cross audit boundaries."""


def _raise_secret(*_args, **_kwargs):
    raise SecretBackendError(_SECRET_ERROR)


def _module_messages(
    caplog: pytest.LogCaptureFixture,
    *modules: str,
) -> str:
    return "\n".join(
        record.getMessage() for record in caplog.records
        if record.name in modules
    )


def _assert_secret_absent(value: object) -> None:
    rendered = value if isinstance(value, str) else json.dumps(value)
    assert "top-secret" not in rendered
    assert "redis://" not in rendered
    assert "s3://" not in rendered
    assert "/srv/" not in rendered


def _formatted_exception(caught: pytest.ExceptionInfo[BaseException]) -> str:
    return "".join(traceback.format_exception(
        caught.type,
        caught.value,
        caught.tb,
    ))


class _FailingAdminRedis:
    hgetall = _raise_secret
    hset = _raise_secret


def test_admin_logs_only_backend_exception_type(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(admin, "_redis", lambda: _FailingAdminRedis())
    caplog.set_level(logging.DEBUG, logger="supertable.audit.admin")

    assert admin.get_audit_config("acme")["updated_ms"] == 0
    with pytest.raises(admin.AuditConfigActivationError) as caught:
        admin.set_audit_config("acme", enabled=True)

    messages = _module_messages(caplog, "supertable.audit.admin")
    assert "SecretBackendError" in messages
    _assert_secret_absent(str(caught.value))
    _assert_secret_absent(messages)


def test_consumer_failures_redact_logs_and_results(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import supertable.audit.writer_redis as writer_redis

    class FailingWriter:
        def __init__(self, *_args, **_kwargs):
            _raise_secret()

    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=object()),
    )
    monkeypatch.setattr(
        writer_redis, "RedisAuditWriter", FailingWriter, raising=True,
    )
    caplog.set_level(logging.WARNING, logger="supertable.audit.consumers")

    created = consumers.create_consumer("acme", "siem")
    deleted = consumers.delete_consumer("acme", "siem")
    listed = consumers.list_consumers("acme")

    assert created == {
        "success": False,
        "error": "consumer creation failed",
        "error_type": "SecretBackendError",
    }
    assert deleted == {
        "success": False,
        "error": "consumer deletion failed",
        "error_type": "SecretBackendError",
    }
    assert listed == []
    _assert_secret_absent([created, deleted, listed])
    messages = _module_messages(caplog, "supertable.audit.consumers")
    assert "SecretBackendError" in messages
    _assert_secret_absent(messages)


def test_consumer_creation_honors_siem_policy_and_configured_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_redis as writer_redis
    import supertable.config.settings as settings_module

    calls: list[tuple[str, str, int]] = []

    class FakeWriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def create_consumer_group(
            self, group_name, start_from, *, max_consumers,
        ):
            calls.append((group_name, start_from, max_consumers))
            return True

    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=object()),
    )
    monkeypatch.setattr(writer_redis, "RedisAuditWriter", FakeWriter)
    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS=7),
    )
    monkeypatch.setattr(
        admin,
        "get_audit_config",
        lambda _org, *, strict: {"siem_enabled": False},
    )

    assert consumers.create_consumer("acme", "siem") == {
        "success": False,
        "error": "SIEM audit consumers are disabled",
    }
    assert calls == []

    monkeypatch.setattr(
        admin,
        "get_audit_config",
        lambda _org, *, strict: {"siem_enabled": True},
    )
    assert consumers.create_consumer("acme", "siem", "0-0") == {
        "success": True,
        "group_name": "siem",
        "start_from": "0-0",
    }
    assert calls == [("siem", "0-0", 7)]


def test_retention_uses_paths_internally_but_returns_only_safe_labels(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet
    import supertable.config.settings as settings_module
    import supertable.storage.storage_factory as storage_factory

    first_path = f"{_SECRET_PREFIX}/year=2000/month=01/day=01"
    second_path = f"{_SECRET_PREFIX}/year=2000/month=01/day=02"

    class FakeWriter:
        def list_partitions(self, _organization: str) -> list[str]:
            return [first_path, second_path]

    deleted: list[str] = []

    class FakeStorage:
        def delete(self, path: str) -> None:
            deleted.append(path)
            if path == second_path:
                _raise_secret()

    monkeypatch.setattr(retention, "is_legal_hold_active", lambda _org: False)
    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(SUPERTABLE_AUDIT_RETENTION_DAYS=1),
        raising=True,
    )
    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FakeWriter, raising=True,
    )
    monkeypatch.setattr(
        storage_factory, "get_storage", lambda: FakeStorage(), raising=True,
    )
    import supertable.audit as audit_pkg
    monkeypatch.setattr(audit_pkg, "emit", lambda **_kwargs: None, raising=True)
    caplog.set_level(logging.DEBUG, logger="supertable.audit.retention")

    result = retention.enforce_retention("acme")

    assert deleted == [first_path, second_path]
    assert result["deleted_paths"] == ["year=2000/month=01/day=01"]
    assert result["errors"] == [
        "partition deletion failed (year=2000/month=01/day=02); "
        "error_type=SecretBackendError"
    ]
    _assert_secret_absent(result)
    messages = _module_messages(caplog, "supertable.audit.retention")
    assert "SecretBackendError" in messages
    _assert_secret_absent(messages)


def test_reader_failure_redacts_log_and_result(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class FailingWriter:
        load_day_close_manifest = _raise_secret
        def read_batch_events(self, *_args, **_kwargs):
            _raise_secret()

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FailingWriter, raising=True,
    )
    caplog.set_level(logging.WARNING, logger="supertable.audit.reader")

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["error"] == "Audit integrity inputs could not be read"
    assert result["error_type"] == "SecretBackendError"
    _assert_secret_absent(result)
    messages = _module_messages(caplog, "supertable.audit.reader")
    assert "SecretBackendError" in messages
    _assert_secret_absent(messages)


def test_verified_chain_returns_only_opaque_file_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet
    from supertable.audit.chain import (
        GENESIS_HASH,
        InstanceChain,
        MerkleProof,
        compute_chain_hash,
        compute_event_batch_hash,
    )

    secret_path = f"{_SECRET_PREFIX}/batch.parquet"
    event = {
        "event_id": "event-1",
        "timestamp_ms": 1,
        "organization": "acme",
        "instance_id": "instance-1",
        "detail": "redacted",
        "chain_hash": "",
    }
    head = compute_chain_hash(
        GENESIS_HASH, compute_event_batch_hash([event]),
    )
    event["chain_hash"] = head
    batches = [{
        "instance_id": "instance-1",
        "event_ids": ["event-1"],
        "chain_hash": head,
        "event_count": 1,
        "min_timestamp_ms": 1,
        "file_path": secret_path,
        "events": [event],
    }]
    proof = MerkleProof(date="2026-08-24")
    proof.add_instance(
        InstanceChain("instance-1", head=head, batch_count=1),
        event_count=1,
    )
    proof.compute_root()

    class FakeWriter:
        def read_batch_events(self, *_args, **_kwargs):
            return batches

        def load_chain_proof(self, *_args, **_kwargs):
            date = str(_args[-1])
            return proof if date.replace("-", "") == "20260824" else None

        def load_day_close_manifest(self, *_args, **_kwargs):
            date = str(_args[-1])
            if date.replace("-", "") != "20260824":
                return None
            proof_bytes = json.dumps(
                proof.to_dict(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            return {
                "admitted": 1,
                "batch_ids": ["batch"],
                "cutover_day": 1,
                "day": 1,
                "receipt_count": 1,
                "proof_hash": __import__("hashlib").sha256(
                    proof_bytes,
                ).hexdigest(),
            }

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FakeWriter, raising=True,
    )

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is True
    instance = result["instances"]["instance-1"]
    assert "file_path" not in instance
    assert instance["file_refs"][0].startswith("sha256:")
    _assert_secret_absent(result)


def test_fixed_validation_errors_hide_hostile_causes_from_full_tracebacks(
) -> None:
    import importlib

    audit_logger = importlib.import_module("supertable.audit.logger")
    from supertable.audit import chain, crypto
    from supertable.audit.events import AuditEvent
    from supertable.audit.privileged import (
        PrivilegedActionContext,
        PrivilegedAuditRecord,
    )

    secret = "signed-trace-secret-DO-NOT-RENDER"
    poisoned_text = f"{secret}\ud800"
    operations = [
        (
            lambda: chain.canonical_event_bytes({"detail": poisoned_text}),
            ValueError,
        ),
        (
            lambda: chain.InstanceChain.from_dict({
                "batch_count": secret,
            }),
            ValueError,
        ),
        (
            lambda: chain.MerkleProof.from_dict({
                "total_events": secret,
            }),
            ValueError,
        ),
        (
            lambda: crypto.protect_sensitive_detail(
                {"sql": poisoned_text}, action="query_execute",
            ),
            crypto.AuditEncryptionError,
        ),
        (
            lambda: audit_logger._serialized_event_size(
                AuditEvent(organization="acme", detail=poisoned_text),
            ),
            ValueError,
        ),
        (
            lambda: admin.set_audit_config(
                "acme", enabled=True, updated_by=poisoned_text,
            ),
            ValueError,
        ),
        (
            lambda: PrivilegedAuditRecord.from_json(poisoned_text),
            ValueError,
        ),
        (
            lambda: PrivilegedActionContext(
                actor_type=secret,
                actor_id="actor",
            ),
            ValueError,
        ),
        (
            lambda: reader._query_event_size({"detail": poisoned_text}),
            reader.AuditQueryError,
        ),
    ]

    for operation, expected_type in operations:
        with pytest.raises(expected_type) as caught:
            operation()
        formatted = _formatted_exception(caught)
        assert secret not in formatted
        assert "The above exception was the direct cause" not in formatted
        assert "During handling of the above exception" not in formatted


@pytest.mark.parametrize(
    "payload",
    [
        '{"signed-url-secret-DO-NOT-RENDER":1}',
        '{"signed-url-secret-DO-NOT-RENDER":1,'
        '"signed-url-secret-DO-NOT-RENDER":2}',
    ],
)
def test_poisoned_privileged_json_never_echoes_field_names(
    payload: str,
) -> None:
    from supertable.audit.privileged import PrivilegedAuditRecord

    with pytest.raises(ValueError) as caught:
        PrivilegedAuditRecord.from_json(payload)

    formatted = _formatted_exception(caught)
    assert "signed-url-secret-DO-NOT-RENDER" not in formatted
    assert "During handling of the above exception" not in formatted


def test_unbounded_exception_class_names_are_not_logged_or_returned(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import supertable.audit.writer_redis as writer_redis

    unsafe_name = "SignedUrlSecret-DO-NOT-RENDER"
    unsafe_error = type(unsafe_name, (RuntimeError,), {})

    class FailingWriter:
        def __init__(self, *_args, **_kwargs):
            raise unsafe_error(_SECRET_ERROR)

    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=object()),
    )
    monkeypatch.setattr(writer_redis, "RedisAuditWriter", FailingWriter)
    caplog.set_level(logging.WARNING, logger="supertable.audit.consumers")

    result = consumers.create_consumer("acme", "siem")

    assert result == {
        "success": False,
        "error": "consumer creation failed",
        "error_type": "RuntimeError",
    }
    assert unsafe_name not in caplog.text
    _assert_secret_absent(caplog.text)


def test_valid_identifier_dynamic_type_names_are_never_reflected() -> None:
    from supertable.audit import durable_journal, privileged_outbox, privileged_worker
    from supertable.audit.diagnostics import safe_audit_error_type

    unsafe_name = "SignedUrlSecretDONOTRENDER"
    unsafe_error = type(unsafe_name, (RuntimeError,), {})
    error = unsafe_error(_SECRET_ERROR)

    assert safe_audit_error_type(error) == "RuntimeError"
    assert privileged_outbox._safe_error_type(error) == "RuntimeError"
    assert privileged_worker._safe_error_type(error) == "RuntimeError"
    assert durable_journal._safe_error_type(error) == "RuntimeError"

    unsafe_value = type("CustomerSchemaSecretDONOTRENDER", (), {})()
    assert privileged_outbox._safe_type_name(unsafe_value) == "object"


def test_admin_persistence_failure_has_a_safe_public_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingRedis:
        hset = _raise_secret

    disabled = {
        "enabled": False,
        "log_queries": False,
        "log_reads": False,
        "hash_chain": False,
        "siem_enabled": False,
        "updated_ms": 0,
        "updated_by": "",
    }
    monkeypatch.setattr(admin, "_redis", lambda: FailingRedis())
    monkeypatch.setattr(admin, "get_audit_config", lambda _org: disabled)
    monkeypatch.setattr(
        admin, "_require_runtime_policy", lambda *_args, **_kwargs: object(),
    )

    with pytest.raises(admin.AuditConfigDurabilityError) as caught:
        admin.set_audit_config("acme", enabled=True)

    _assert_secret_absent(_formatted_exception(caught))


def test_invalid_retention_tenant_is_not_reflected_or_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    unsafe_org = "s3://admin:top-secret@example.test/private"
    caplog.set_level(logging.DEBUG, logger="supertable.audit.retention")

    result = retention.enforce_retention(unsafe_org)

    assert result["organization"] == ""
    assert result["errors"] == ["organization is invalid"]
    _assert_secret_absent(result)
    _assert_secret_absent(caplog.text)


def test_poisoned_proof_serialization_stays_inside_integrity_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class PoisonedProof:
        def to_dict(self):
            _raise_secret()

    class FakeWriter:
        def load_day_close_manifest(self, *_args, **_kwargs):
            return {"day": 1, "cutover_day": 1}

        def read_batch_events(self, *_args, **_kwargs):
            return []

        def load_chain_proof(self, *_args, **_kwargs):
            return PoisonedProof()

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FakeWriter, raising=True,
    )

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["status"] == "invalid"
    assert result["error"] == "Audit proof document is invalid"
    assert result["error_type"] == "AuditQueryError"
    _assert_secret_absent(result)
