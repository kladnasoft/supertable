"""Failure, race, and closure regressions for the durable audit journal."""
from __future__ import annotations

import hashlib
import io
import json
import time
import traceback
from dataclasses import replace
from typing import Any

import fakeredis
import pytest

from supertable.audit import durable_journal as journal_module
from supertable.audit.chain import GENESIS_HASH, InstanceChain
from supertable.audit.durable_journal import (
    AuditJournalCollisionError,
    AuditJournalConfigurationError,
    AuditJournalError,
    AuditJournalLimitError,
    DurableAuditArchiver,
    DurableAuditDayCloser,
    JournalArchiveReceipt,
    RedisAuditJournal,
)
from supertable.audit.events import AuditEvent, current_instance_id, new_event_id
from supertable.audit.writer_parquet import ParquetAuditWriter
from supertable.storage.storage_interface import ObjectMetadata


_ORG = "acme"
_ORIGINAL_ADMIT_LUA = journal_module._ADMIT_LUA


def _event(
    event_id: str | None = None,
    *,
    instance_id: str | None = None,
) -> AuditEvent:
    return AuditEvent(
        event_id=event_id or new_event_id(),
        organization=_ORG,
        instance_id=instance_id or current_instance_id(),
        action="data_write",
        detail="bounded",
    )


class _MemoryStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.ambiguous_create = False
        self.durable: list[str] = []

    def write_bytes(self, path: str, payload: bytes) -> None:
        self.objects[path] = payload

    def create_bytes_if_absent(self, path: str, payload: bytes) -> bool:
        if path in self.objects:
            return False
        self.objects[path] = payload
        if self.ambiguous_create:
            raise TimeoutError("provider timed out after exact create")
        return True

    def ensure_bytes_durable(self, path: str) -> None:
        assert path in self.objects
        self.durable.append(path)

    def stat_object(self, path: str) -> ObjectMetadata:
        try:
            payload = self.objects[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        return ObjectMetadata(
            size=len(payload),
            checksum_sha256=hashlib.sha256(payload).hexdigest(),
        )

    def download_to_file(
        self,
        path: str,
        file_obj: Any,
        *,
        expected: ObjectMetadata | None = None,
        chunk_size: int = 1024 * 1024,
    ) -> int:
        payload = self.objects[path]
        for offset in range(0, len(payload), chunk_size):
            file_obj.write(payload[offset:offset + chunk_size])
        return len(payload)

    def read_range(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> bytes:
        return self.objects[path][offset:offset + length]

    def list_files(self, path: str, pattern: str = "*") -> list[str]:
        prefix = path.rstrip("/") + "/"
        suffix = pattern.removeprefix("*")
        return sorted(
            key for key in self.objects
            if key.startswith(prefix) and key.endswith(suffix)
        )


def _redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def _forced_admission_script(timestamp_ms: int) -> str:
    seconds, milliseconds = divmod(timestamp_ms, 1_000)
    replacement = (
        f"local now = {{{seconds!r}, {(milliseconds * 1_000)!r}}}"
    )
    return _ORIGINAL_ADMIT_LUA.replace(
        "local now = redis.call('TIME')", replacement, 1,
    )


def test_admission_is_idempotent_and_redis_time_owns_day_membership() -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    event = _event()

    first = journal.admit(event)
    second = journal.admit(event)

    assert second.journal_id == first.journal_id
    assert second.event.timestamp_ms == first.event.timestamp_ms
    assert second.day == first.day
    assert second.duplicate is True
    assert backend.xlen(journal.base_key + ":events") == 1


def test_same_event_id_with_different_content_is_a_collision() -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    event = _event()
    journal.admit(event)

    with pytest.raises(AuditJournalCollisionError):
        journal.admit(replace(event, detail="different"))

    reservation = journal.claim(count=10)
    assert reservation is not None
    assert reservation.events == (journal.admit(event).event,)


def test_midnight_race_assigns_each_event_to_redis_time_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    day = int(time.time()) // 86_400
    before_ms = (day + 1) * 86_400_000 - 1
    after_ms = before_ms + 1
    before_id = f"{before_ms:012x}-0001-00000001"
    after_id = f"{after_ms:012x}-0001-00000002"

    monkeypatch.setattr(
        journal_module, "_ADMIT_LUA", _forced_admission_script(before_ms),
    )
    before = journal.admit(_event(before_id))
    monkeypatch.setattr(
        journal_module, "_ADMIT_LUA", _forced_admission_script(after_ms),
    )
    after = journal.admit(_event(after_id))

    assert before.day == day
    assert after.day == day + 1
    assert before.event.timestamp_ms == before_ms
    assert after.event.timestamp_ms == after_ms


def test_ambiguous_admission_reconciles_same_event_without_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    event = _event()
    real_eval = journal._eval
    calls = 0

    def ambiguous(operation, script, *args):
        nonlocal calls
        result = real_eval(operation, script, *args)
        calls += 1
        if operation == "admission" and calls == 1:
            raise journal_module.AuditJournalError(
                "simulated timeout after commit"
            )
        return result

    monkeypatch.setattr(journal, "_eval", ambiguous)

    admission = journal.admit(event)

    assert admission.duplicate is True
    assert backend.xlen(journal.base_key + ":events") == 1
    assert journal.claim(count=10).events[0].event_id == event.event_id


def test_ambiguous_admission_rejection_can_only_over_audit_never_lose_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    event = _event()
    real_eval = journal._eval
    real_hget = backend.hget

    def committed_timeout(operation, script, *args):
        result = real_eval(operation, script, *args)
        if operation == "admission":
            raise AuditJournalError("simulated timeout after commit")
        return result

    def reconciliation_outage(*_args, **_kwargs):
        raise ConnectionError("simulated reconciliation outage")

    monkeypatch.setattr(journal, "_eval", committed_timeout)
    monkeypatch.setattr(backend, "hget", reconciliation_outage)
    with pytest.raises(AuditJournalError):
        journal.admit(event)

    assert backend.xlen(journal.base_key + ":events") == 1
    monkeypatch.setattr(backend, "hget", real_hget)
    recovered = RedisAuditJournal(backend, _ORG).claim(count=10)

    assert recovered is not None
    assert [item.event_id for item in recovered.events] == [event.event_id]
    assert len(set(recovered.journal_ids)) == 1


def test_malformed_admission_reconciliation_suppresses_backend_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    event = _event()
    secret = "redis://audit-user:secret@backend/ADMISSION_SENTINEL"
    backend.hset(
        journal.base_key + ":event-index", event.event_id, "malformed-index",
    )

    def failed_admission(*_args, **_kwargs):
        try:
            raise ConnectionError(secret)
        except ConnectionError as exc:
            raise AuditJournalError("durable audit admission failed") from exc

    monkeypatch.setattr(journal, "_eval", failed_admission)

    with pytest.raises(AuditJournalError) as raised:
        journal.admit(event)

    rendered = "".join(traceback.format_exception(raised.value))
    assert str(raised.value) == (
        "durable audit admission reconciliation is invalid"
    )
    assert secret not in rendered


def test_reservation_reclaim_preserves_membership_and_chain_order() -> None:
    backend = _redis()
    first_journal = RedisAuditJournal(backend, _ORG)
    second_journal = RedisAuditJournal(backend, _ORG)
    first_journal.admit(_event())
    first_journal.admit(_event())

    original = first_journal.claim(count=1, lease_ms=1_000)
    assert original is not None
    assert second_journal.claim(count=1, lease_ms=1_000) is None
    backend.hset(
        f"{first_journal.base_key}:reservation:{original.instance_id}",
        "expires_ms",
        "0",
    )
    reclaimed = second_journal.claim(count=1, lease_ms=1_000)

    assert reclaimed is not None
    assert reclaimed.journal_ids == original.journal_ids
    assert reclaimed.previous_head == original.previous_head
    assert reclaimed.previous_batch_count == original.previous_batch_count
    assert reclaimed.token != original.token


def test_day_event_bound_fails_before_second_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(journal_module, "_MAX_DAY_EVENTS", 1)
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    journal.admit(_event())

    with pytest.raises(AuditJournalLimitError):
        journal.admit(_event())

    assert backend.xlen(journal.base_key + ":events") == 1


def test_receipt_backpressure_never_accepts_unclosable_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(journal_module, "_MAX_DAY_RECEIPTS", 1)
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    journal.admit(_event())

    with pytest.raises(AuditJournalLimitError):
        journal.admit(_event())

    assert backend.xlen(journal.base_key + ":events") == 1


def test_cluster_client_is_rejected_before_any_backend_command() -> None:
    calls: list[str] = []

    class RedisCluster:
        def eval(self, *_args):
            calls.append("eval")

    with pytest.raises(AuditJournalConfigurationError):
        RedisAuditJournal(RedisCluster(), _ORG)

    assert calls == []


def test_backend_error_diagnostics_are_bounded_and_confidential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "redis://admin:never-log@example.test/0?access_token=never-log"
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)

    def fail_eval(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError(secret)

    monkeypatch.setattr(backend, "eval", fail_eval)
    with pytest.raises(AuditJournalError) as caught:
        journal._eval("hostile backend", "return 1")

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert "RuntimeError" in str(caught.value)
    assert caught.value.__suppress_context__ is True
    assert secret not in str(caught.value)
    assert secret not in rendered


def test_poisoned_receipt_content_is_not_echoed_in_public_error() -> None:
    secret = "s3://admin:never-log@example.test/private/customer-token"

    with pytest.raises(AuditJournalError) as caught:
        JournalArchiveReceipt.from_json(
            '{"poisoned":"' + secret,
            organization=_ORG,
        )

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert secret not in str(caught.value)
    assert secret not in rendered


def test_format_marker_is_immutable_and_inspection_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    journal = RedisAuditJournal(backend, _ORG)
    backend.hset(journal.base_key + ":meta", "last_closed_day", journal.cutover_day)
    monkeypatch.setattr(
        backend,
        "eval",
        lambda *_args, **_kwargs: pytest.fail("inspection must not run Lua"),
    )

    assert RedisAuditJournal.inspect_day_state(
        backend, _ORG, journal.cutover_day,
    ) == {"status": "closed"}

    backend = _redis()
    base = "supertable:acme:system:audit:stream:durable:v1"
    backend.hset(base + ":meta", mapping={
        "format_version": "2",
        "cutover_day": "1",
        "cutover_ms": str(86_400_000),
    })
    with pytest.raises(AuditJournalConfigurationError):
        RedisAuditJournal(backend, _ORG)


def _archive_one(
    backend: fakeredis.FakeRedis,
    storage: _MemoryStorage,
) -> tuple[RedisAuditJournal, ParquetAuditWriter, JournalArchiveReceipt]:
    journal = RedisAuditJournal(backend, _ORG)
    admission = journal.admit(_event())
    writer = ParquetAuditWriter(storage=storage)
    receipt = DurableAuditArchiver(journal, writer).archive_once(count=10)
    assert receipt is not None
    assert journal.archived_membership([admission.journal_id]) == {
        admission.journal_id: receipt.batch_id,
    }
    return journal, writer, receipt


def _allow_immediate_close(monkeypatch: pytest.MonkeyPatch) -> None:
    script = journal_module._BEGIN_CLOSE_LUA
    script = script.replace(
        "if now_ms < (day + 1) * 86400000 + grace_ms then return {'OPEN'} end",
        "if false then return {'OPEN'} end",
    )
    monkeypatch.setattr(journal_module, "_BEGIN_CLOSE_LUA", script)


def test_archive_commit_is_idempotent_and_advances_exact_checkpoint() -> None:
    backend = _redis()
    storage = _MemoryStorage()
    journal, _writer, receipt = _archive_one(backend, storage)

    head, count = journal.load_checkpoint(receipt.instance_id)
    assert (head, count) == (receipt.chain_head, 1)
    assert journal.claim(count=10) is None
    state = journal.day_state(receipt.day)
    assert state["admitted"] == "1"
    assert state["archived"] == "1"
    assert state["inflight"] == "0"


def test_crash_after_archive_object_reuses_exact_path_and_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    storage = _MemoryStorage()
    journal = RedisAuditJournal(backend, _ORG)
    journal.admit(_event())
    writer = ParquetAuditWriter(storage=storage)
    archiver = DurableAuditArchiver(journal, writer)
    real_complete = journal.complete
    calls = 0

    def crash_once(reservation, receipt):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise AuditJournalError("simulated crash after object publication")
        return real_complete(reservation, receipt)

    monkeypatch.setattr(journal, "complete", crash_once)

    with pytest.raises(AuditJournalError):
        archiver.archive_once(count=10)
    assert len(storage.objects) == 1
    original_path, original_bytes = next(iter(storage.objects.items()))

    receipt = archiver.archive_once(count=10)

    assert receipt is not None
    assert receipt.path == original_path
    assert storage.objects == {original_path: original_bytes}
    assert journal.claim(count=10) is None


def test_close_reconciles_ambiguous_creates_and_cleanup_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    storage = _MemoryStorage()
    journal, writer, receipt = _archive_one(backend, storage)
    _allow_immediate_close(monkeypatch)
    storage.ambiguous_create = True

    result = DurableAuditDayCloser(journal, writer).close_day(
        receipt.day, grace_ms=0,
    )

    assert result is not None
    assert result["events"] == 1
    date = journal_module._day_date(receipt.day).replace("-", "")
    proof_path = f"{_ORG}/__audit__/_chain/chain_{date}.json"
    close_path = f"{_ORG}/__audit__/_chain/closed_{date}.json"
    assert proof_path in storage.objects
    assert close_path in storage.objects
    assert storage.durable == [proof_path, close_path]
    assert journal.cleanup_pending_day(chunk_size=1) == 1
    assert backend.xlen(journal.base_key + ":events") == 0
    assert backend.hlen(journal.base_key + ":event-index") == 0
    assert journal.day_state(receipt.day) == {}
    assert journal.archived_membership(receipt.journal_ids) == {}
    assert journal.archived_membership(
        receipt.journal_ids,
        admission_days={
            journal_id: receipt.day for journal_id in receipt.journal_ids
        },
    ) == {journal_id: GENESIS_HASH for journal_id in receipt.journal_ids}
    marker = writer.load_day_close_manifest(_ORG, date, strict=True)
    assert marker is not None
    assert marker["batch_ids"] == [receipt.batch_id]
    assert marker["cutover_ms"] == journal.cutover_ms


def test_close_refuses_existing_different_proof_without_closed_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    storage = _MemoryStorage()
    journal, writer, receipt = _archive_one(backend, storage)
    _allow_immediate_close(monkeypatch)
    date = journal_module._day_date(receipt.day).replace("-", "")
    proof_path = f"{_ORG}/__audit__/_chain/chain_{date}.json"
    storage.objects[proof_path] = b'{"different":true}'

    with pytest.raises(AuditJournalCollisionError):
        DurableAuditDayCloser(journal, writer).close_day(
            receipt.day, grace_ms=0,
        )

    assert journal.day_state(receipt.day)["status"] == "closing"
    assert (
        f"{_ORG}/__audit__/_chain/closed_{date}.json"
        not in storage.objects
    )


def test_close_resume_uses_one_lease_and_refuses_missing_predecessor_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _redis()
    storage = _MemoryStorage()
    journal = RedisAuditJournal(backend, _ORG)
    _allow_immediate_close(monkeypatch)
    backend.hset(
        journal.base_key + ":meta",
        "last_closed_day",
        journal.cutover_day,
    )
    target = journal.cutover_day + 1

    first = journal.begin_close(target, grace_ms=0)
    second = journal.begin_close(target, grace_ms=0)
    assert first is not None and second is not None
    assert second.token == first.token
    assert second.requested_ms == first.requested_ms

    with pytest.raises(AuditJournalError):
        DurableAuditDayCloser(
            journal, ParquetAuditWriter(storage=storage),
        ).close_day(target, grace_ms=0)

    assert journal.day_state(target)["status"] == "closing"
    assert storage.objects == {}


def test_receipt_parser_rejects_chain_counter_corruption() -> None:
    receipt = JournalArchiveReceipt(
        batch_id="a" * 64,
        day=1,
        instance_id=current_instance_id(),
        journal_ids=("1-0",),
        previous_head=GENESIS_HASH,
        chain_head="b" * 64,
        previous_batch_count=0,
        batch_count=1,
        event_count=1,
        path="acme/__audit__/batch.parquet",
        file_hash="c" * 64,
        bytes_written=1,
        publication_id="a" * 64,
        min_timestamp_ms=1,
    ).to_dict()
    receipt["batch_count"] = 2
    raw = json.dumps(receipt, sort_keys=True, separators=(",", ":"))

    with pytest.raises(journal_module.AuditJournalError):
        JournalArchiveReceipt.from_json(raw, organization=_ORG)
