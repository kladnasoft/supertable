"""Failure- and retry-focused tests for the durable privileged audit outbox."""
from __future__ import annotations

import io
import json
import os
import threading
from pathlib import Path

import fakeredis
import pytest

from supertable.audit.privileged_outbox import (
    ArchiveVerificationError,
    DeliveryPendingError,
    OutboxBackendError,
    OutboxRecordError,
    PrivilegedAuditOutbox,
)


STREAM = "supertable:{acme}:privileged-audit"
LEDGER = "supertable:{acme}:privileged-audit:delivery"


def _event(event_id: str = "evt-1", *, organization: str = "acme"):
    from supertable.audit.privileged import PrivilegedActionContext, build_record

    context = PrivilegedActionContext(
        actor_type="user",
        actor_id="admin-1",
        username="admin",
        ip="192.0.2.1",
        user_agent="test",
        correlation_id="corr-1",
        session_id="session-1",
        server="api",
        reason="test",
    )
    return build_record(
        context=context,
        organization=organization,
        super_name="sales",
        action="role_update",
        resource_type="role",
        resource_id=event_id,
        before_document={"name": "before"},
        after_document={"name": "after"},
        before_version=1,
        after_version=2,
        changed_fields=("name",),
        namespace_version=1,
        ledger_sequence=1,
    )


def _raw(stream_id: str = "1700000000000-0", event=None):
    from supertable.audit.privileged import PrivilegedAuditRecord

    value = _event() if event is None else event
    event_dict = value.to_dict() if hasattr(value, "to_dict") else value
    template_dict = dict(event_dict)
    template_dict.update({
        field: 0 for field in PrivilegedAuditOutbox._COMMIT_FIELDS
    })
    event_json = PrivilegedAuditRecord.from_dict(template_dict).to_json()
    fields = {b"event_json": event_json.encode()}
    for field in PrivilegedAuditOutbox._INDEX_FIELDS:
        fields[field.encode()] = str(event_dict[field]).encode()
    return stream_id.encode(), fields


def _cascade_event(
    *,
    event_id: str = "cascade-event-1",
    ledger_sequence: int = 1,
    affected_count: int = 1,
    assignment_count: int = 2,
):
    from supertable.audit.privileged import build_record

    return build_record(
        organization="acme",
        super_name="sales",
        action="role_delete",
        resource_type="role",
        resource_id="reader-role",
        before_document={"role_id": "reader-role"},
        after_document=None,
        before_version=2,
        after_version=0,
        changed_fields=("user.roles",),
        namespace_version=3,
        affected_count=affected_count,
        cascade_assignment_count=assignment_count,
        user_namespace_version_before=5,
        user_namespace_version_after=5 + int(affected_count > 0),
        ledger_sequence=ledger_sequence,
        event_id=event_id,
        mutation_id=f"mutation-{event_id}",
        timestamp_ms=1_700_000_000_123,
    )


def _install_cascade_manifest(backend, event, *, packed="1|2|2|3|1"):
    from supertable import redis_keys as RK

    key = RK.audit_privileged_cascade(
        event.organization, event.cascade_manifest_id,
    )
    manifest = {
        "schema_version": "1",
        "event_id": event.event_id,
        "mutation_id": event.mutation_id,
        "organization": event.organization,
        "super_name": event.super_name,
        "role_id": event.resource_id,
        "user_count": str(event.affected_count),
        "removed_assignment_count": str(event.cascade_assignment_count),
        "user_namespace_version_before": str(
            event.user_namespace_version_before
        ),
        "user_namespace_version_after": str(
            event.user_namespace_version_after
        ),
        "created_ms": str(event.timestamp_ms),
    }
    if event.affected_count:
        manifest["user:alice-id"] = packed
    backend.hashes[key] = manifest
    return key


class FakeRedis:
    def __init__(self):
        self.entries = []
        self.hashes = {}
        self.groups = []
        self.read_response = []
        self.autoclaim_response = [b"0-0", [], []]
        self.pending_count = 0
        self.failures = {}
        self.acks = []
        self.deleted = []
        self.created_groups = set()
        self.stream_exists = True
        self._eval_lock = threading.RLock()
        self.eval_call_count = 0
        self.eval_failures = {}
        self.read_group_calls = 0

    def fail_once(self, method, exc=None):
        self.failures.setdefault(method, []).append(exc or RuntimeError(f"{method} failed"))

    def fail_eval_call(self, call_number, exc):
        self.eval_failures[call_number] = exc

    def _fail(self, method):
        pending = self.failures.get(method)
        if pending:
            raise pending.pop(0)

    @staticmethod
    def _sid(value):
        if isinstance(value, bytes):
            value = value.decode()
        if value in ("-", "+"):
            return (-1, -1) if value == "-" else (2**63, 2**63)
        first, second = value.split("-", 1)
        return int(first), int(second)

    def _range(self, minimum, maximum, count, reverse=False):
        selected = []
        for item in self.entries:
            try:
                in_range = self._sid(minimum) <= self._sid(item[0]) <= self._sid(maximum)
            except ValueError:
                in_range = True  # let the outbox, not the transport fake, reject it
            if in_range:
                selected.append(item)
        selected.sort(
            key=lambda item: self._sid(item[0]) if item[0] != b"not-a-stream-id" else (0, 0),
            reverse=reverse,
        )
        return selected[:count]

    def xrange(self, key, min="-", max="+", count=None):
        self._fail("xrange")
        return self._range(min, max, count or len(self.entries), reverse=False)

    def xrevrange(self, key, max="+", min="-", count=None):
        self._fail("xrevrange")
        return self._range(min, max, count or len(self.entries), reverse=True)

    def xgroup_create(self, key, group, id="0-0", mkstream=False):
        self._fail("xgroup_create")
        if group in self.created_groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        self.created_groups.add(group)
        return True

    def xreadgroup(self, group, consumer, streams, **kwargs):
        self._fail("xreadgroup")
        self.read_group_calls += 1
        for _, items in self.read_response or []:
            for item in items:
                if item not in self.entries:
                    self.entries.append(item)
        return self.read_response

    def xautoclaim(self, key, group, consumer, min_idle_ms, **kwargs):
        self._fail("xautoclaim")
        if len(self.autoclaim_response) >= 2:
            for item in self.autoclaim_response[1]:
                if item not in self.entries:
                    self.entries.append(item)
        return self.autoclaim_response

    def xack(self, key, group, *ids):
        self._fail("xack")
        self.acks.append((group, ids))
        return len(ids)

    def hget(self, key, field):
        self._fail("hget")
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key):
        self._fail("hgetall")
        return dict(self.hashes.get(key, {}))

    def hlen(self, key):
        self._fail("hlen")
        return len(self.hashes.get(key, {}))

    def hsetnx(self, key, field, value):
        self._fail("hsetnx")
        bucket = self.hashes.setdefault(key, {})
        if field in bucket:
            return 0
        bucket[field] = value
        return 1

    def hset(self, key, mapping):
        self._fail("hset")
        self.hashes.setdefault(key, {}).update(mapping)
        return len(mapping)

    def hmget(self, key, fields):
        self._fail("hmget")
        bucket = self.hashes.get(key, {})
        return [bucket.get(field) for field in fields]

    def ping(self):
        self._fail("ping")
        return True

    def exists(self, *keys):
        self._fail("exists")
        count = 0
        for key in keys:
            if key == STREAM:
                count += int(self.stream_exists)
            elif key in self.hashes:
                count += 1
        return count

    def delete(self, *keys):
        self._fail("delete")
        removed = 0
        for key in keys:
            if key in self.hashes:
                removed += 1
                del self.hashes[key]
        return removed

    def xlen(self, key):
        self._fail("xlen")
        return len(self.entries)

    def xinfo_groups(self, key):
        self._fail("xinfo_groups")
        return self.groups

    def xpending(self, key, group):
        self._fail("xpending")
        return {
            "pending": self.pending_count,
            "min": None,
            "max": None,
            "consumers": [],
        }

    def xdel(self, key, *ids):
        self._fail("xdel")
        id_set = set(ids)
        old_count = len(self.entries)
        self.entries = [item for item in self.entries if item[0].decode() not in id_set]
        removed = old_count - len(self.entries)
        self.deleted.extend(ids)
        return removed

    def eval(self, script, numkeys, *values):
        self.eval_call_count += 1
        scheduled = self.eval_failures.pop(self.eval_call_count, None)
        if scheduled is not None:
            raise scheduled
        self._fail("eval")
        with self._eval_lock:
            keys = values[:numkeys]
            args = values[numkeys:]
            if script == PrivilegedAuditOutbox._CLAIM_ARCHIVE_BATCH_LUA:
                ledger = self.hashes.setdefault(keys[0], {})
                head_field, expected_present, expected_head = args[:3]
                claim_field, batch_id, batch_field, batch_json = args[3:7]
                current_head = ledger.get(head_field)
                if (
                    (expected_present == "1" and current_head != expected_head)
                    or (expected_present == "0" and current_head is not None)
                ):
                    return [-1, current_head or ""]
                current_claim = ledger.get(claim_field)
                current_batch = ledger.get(batch_field)
                if current_claim is not None and current_claim != batch_id:
                    return [-2, current_claim, current_batch or ""]
                if current_batch is not None:
                    if current_claim is None:
                        return [-3, "", current_batch]
                    return [2, current_claim, current_batch]
                if current_claim is not None:
                    return [-3, current_claim, ""]
                ledger[claim_field] = batch_id
                ledger[batch_field] = batch_json
                return [1, batch_id, batch_json]
            if script == PrivilegedAuditOutbox._CAS_ARCHIVE_BATCH_LUA:
                ledger = self.hashes[keys[0]]
                field, expected, replacement = args
                current = ledger.get(field)
                if current == replacement:
                    return [2, current]
                if current != expected:
                    return [0, current or ""]
                ledger[field] = replacement
                return [1, replacement]
            if script == PrivilegedAuditOutbox._FINALIZE_ARCHIVE_BATCH_LUA:
                ledger = self.hashes[keys[0]]
                (
                    batch_field, expected_batch, delivered_batch, head_field,
                    expected_present, expected_head, replacement_head,
                    batch_id, marker_count,
                ) = args[:9]
                current_batch = ledger.get(batch_field)
                if current_batch != expected_batch:
                    return [0, current_batch or "", ledger.get(head_field, "")]
                current_head = ledger.get(head_field)
                if (
                    (expected_present == "1" and current_head != expected_head)
                    or (expected_present == "0" and current_head is not None)
                ):
                    return [-1, current_batch, current_head or ""]
                marker_fields = args[9:9 + int(marker_count)]
                for field in marker_fields:
                    existing = ledger.get(field)
                    if existing is not None and existing != batch_id:
                        return [-2, field, existing]
                ledger[batch_field] = delivered_batch
                ledger[head_field] = replacement_head
                for field in marker_fields:
                    ledger[field] = batch_id
                return [1, delivered_batch, replacement_head]

            assert script == PrivilegedAuditOutbox._TRIM_DELIVERED_LUA
            stream_key, ledger_key, *manifest_keys = keys
            stream_count = int(args[0])
            gc_count = int(args[1])
            stream_ids = args[2:2 + stream_count]
            cursor = 2 + stream_count
            gc_expected = {}
            for _ in range(gc_count):
                field, expected = args[cursor:cursor + 2]
                cursor += 2
                gc_expected[field] = expected
            existing_ids = {item[0].decode() for item in self.entries}
            if any(stream_id not in existing_ids for stream_id in stream_ids):
                raise RuntimeError(
                    "verified privileged stream entry disappeared before trim"
                )
            ledger = self.hashes.get(ledger_key, {})
            if any(ledger.get(field) != expected for field, expected in gc_expected.items()):
                raise RuntimeError(
                    "verified privileged delivery metadata changed before trim"
                )
            for key in manifest_keys:
                field_count = int(args[cursor])
                cursor += 1
                expected_fields = {}
                for _ in range(field_count):
                    field, expected = args[cursor:cursor + 2]
                    cursor += 2
                    expected_fields[field] = expected
                if self.hashes.get(key) != expected_fields:
                    raise RuntimeError(
                        "verified role-delete cascade manifest changed before trim"
                    )
            id_set = set(stream_ids)
            self.entries = [
                item for item in self.entries if item[0].decode() not in id_set
            ]
            self.deleted.extend(stream_ids)
            for key in manifest_keys:
                del self.hashes[key]
            for field in gc_expected:
                del ledger[field]
            return [len(stream_ids), len(manifest_keys), len(gc_expected)]


class FakeStorage:
    def __init__(self):
        self.files = {}
        self.failures = []
        self.write_failures = []
        self.writes = []
        self.durability_checks = []

    def exists(self, path):
        return path in self.files

    def write_bytes(self, path, payload):
        if self.write_failures:
            raise self.write_failures.pop(0)
        self.writes.append(path)
        self.files[path] = payload

    def read_bytes(self, path):
        if self.failures:
            raise self.failures.pop(0)
        return self.files[path]

    def size(self, path):
        return len(self.files[path])

    def ensure_bytes_durable(self, path):
        self.durability_checks.append(path)


class FakeParquetWriter:
    def __init__(self):
        self.storage = FakeStorage()

    def _get_storage(self):
        return self.storage


@pytest.fixture
def backend():
    return FakeRedis()


@pytest.fixture
def outbox(backend):
    return PrivilegedAuditOutbox(backend, stream_key=STREAM, delivery_ledger_key=LEDGER)


def _source_entry(backend, outbox, stream_id="1700000000000-0", event=None):
    raw = _raw(stream_id, event)
    backend.entries.append(raw)
    return outbox._decode_entry(raw)


def test_query_decodes_event_json_and_distinguishes_empty_from_backend_failure(backend, outbox):
    backend.entries = [_raw("1700000000000-0"), _raw("1700000000001-0", _event("evt-2"))]

    result = outbox.query(newest_first=False)
    assert [entry.stream_id for entry in result] == ["1700000000000-0", "1700000000001-0"]
    assert result[1].event["resource_id"] == "evt-2"

    backend.entries = []
    assert outbox.query() == []
    backend.fail_once("xrevrange", ConnectionError("redis unavailable"))
    with pytest.raises(OutboxBackendError, match="redis unavailable"):
        outbox.query()


@pytest.mark.parametrize(
    "raw_entry",
    [
        (b"1-0", {}),
        (b"1-0", {b"event_json": b"not-json"}),
        (b"1-0", {b"event_json": b"[]"}),
        (b"not-a-stream-id", {b"event_json": b"{}"}),
    ],
)
def test_query_rejects_malformed_records(backend, outbox, raw_entry):
    backend.entries = [raw_entry]
    with pytest.raises(OutboxRecordError):
        outbox.query()


def test_query_rejects_stream_index_that_disagrees_with_event_json(backend, outbox):
    raw_id, fields = _raw()
    fields[b"payload_hash"] = b"0" * 64
    backend.entries = [(raw_id, fields)]

    with pytest.raises(OutboxRecordError, match="payload_hash does not match"):
        outbox.query()


def test_query_rejects_uncommitted_extra_stream_fields(backend, outbox):
    raw_id, fields = _raw()
    fields[b"unarchived_secret"] = b"must-not-be-silently-dropped"
    backend.entries = [(raw_id, fields)]

    with pytest.raises(OutboxRecordError, match="unexpected field|too many fields"):
        outbox.query()


def test_query_preserves_valid_noncanonical_json_bytes_from_redis_lua(backend, outbox):
    record = _event()
    noncanonical = json.dumps(record.to_dict(), ensure_ascii=False)
    assert noncanonical != record.to_json()
    fields = {b"event_json": noncanonical.encode()}
    for field in PrivilegedAuditOutbox._INDEX_FIELDS:
        fields[field.encode()] = str(record.to_dict()[field]).encode()
    backend.entries = [(b"1-0", fields)]

    result = outbox.query()
    assert result[0].event_json == noncanonical
    assert result[0].event == record.to_dict()


def test_query_merges_exact_lua_commit_envelope_without_rewriting_template(backend, outbox):
    template = _event().with_ledger_sequence(1)
    template_dict = template.to_dict()
    template_dict.update({
        "ledger_sequence": 0,
        "namespace_version": 0,
        "affected_count": 0,
    })
    from supertable.audit.privileged import PrivilegedAuditRecord

    template = PrivilegedAuditRecord.from_dict(template_dict)
    raw_id, fields = _raw(event=template)
    fields[b"ledger_sequence"] = b"9223372036854775807"
    fields[b"namespace_version"] = b"9007199254740993"
    fields[b"affected_count"] = b"7"
    backend.entries = [(raw_id, fields)]

    entry = outbox.query()[0]
    assert entry.event_json == template.to_json()
    assert entry.event["ledger_sequence"] == 9223372036854775807
    assert entry.event["namespace_version"] == 9007199254740993
    assert entry.event["affected_count"] == 7
    assert entry.committed_event_json != entry.event_json


def test_consumer_group_primitives_preserve_redis_semantics_and_errors(backend, outbox):
    assert outbox.create_group("archive") is True
    assert outbox.create_group("archive") is False
    backend.fail_once("xgroup_create", ConnectionError("down"))
    with pytest.raises(OutboxBackendError):
        outbox.create_group("siem")

    backend.read_response = [(STREAM.encode(), [_raw()])]
    assert [item.stream_id for item in outbox.read_group("archive", "worker-1")] == [
        "1700000000000-0"
    ]
    backend.read_response = None
    assert outbox.read_group("archive", "worker-1", block_ms=1) == []
    backend.fail_once("xreadgroup")
    with pytest.raises(OutboxBackendError):
        outbox.read_group("archive", "worker-1")

    backend.autoclaim_response = [b"1700000000002-0", [_raw()], [b"1699999999999-0"]]
    claimed = outbox.autoclaim("archive", "worker-2", min_idle_ms=60_000)
    assert claimed.next_start_id == "1700000000002-0"
    assert claimed.entries[0].stream_id == "1700000000000-0"
    assert claimed.deleted_ids == ("1699999999999-0",)
    assert outbox.ack("archive", ["1700000000000-0"]) == 1


def test_drain_once_is_a_retryable_bounded_archive_cycle(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    backend.autoclaim_response = [b"0-0", [], []]
    backend.read_response = [(STREAM.encode(), [_raw()])]

    result = outbox.drain_once("acme", consumer="worker-1", count=10)
    assert result is not None
    assert result.acknowledged == 1
    assert backend.acks == [("__privileged_archival__", ("1700000000000-0",))]
    assert len(writer.storage.writes) == 2

    backend.read_response = []
    assert outbox.drain_once("acme", consumer="worker-1", count=10) is None


def test_drain_waits_for_earlier_non_idle_pending_before_reading_fresh(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    raw_one = _raw("1-0", _event("one"))
    raw_two = _raw("2-0", _event("two").with_ledger_sequence(2))
    backend.entries = [raw_one, raw_two]
    backend.pending_count = 1
    backend.read_response = [(STREAM.encode(), [raw_two])]

    with pytest.raises(DeliveryPendingError, match="not idle enough"):
        outbox.drain_once(
            "acme", consumer="stable-worker", reclaim_idle_ms=300_000,
        )
    assert backend.read_group_calls == 0
    assert LEDGER not in backend.hashes

    backend.autoclaim_response = [b"0-0", [raw_one], []]
    delivered = outbox.drain_once(
        "acme", consumer="stable-worker", reclaim_idle_ms=300_000,
    )
    assert delivered is not None
    assert delivered.stream_ids == ("1-0",)
    assert backend.read_group_calls == 0


def test_real_consumer_group_reclaims_crash_before_claim_in_sequence_order():
    redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        redis_client,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    for stream_id, event in (
        ("1-0", _event("one")),
        ("2-0", _event("two").with_ledger_sequence(2)),
    ):
        _, raw_fields = _raw(stream_id, event)
        redis_client.xadd(
            STREAM,
            {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in raw_fields.items()
            },
            id=stream_id,
        )
    outbox.create_group("__privileged_archival__")
    first_delivery = outbox.read_group(
        "__privileged_archival__", "stable-worker", count=1,
    )
    assert tuple(entry.stream_id for entry in first_delivery) == ("1-0",)

    with pytest.raises(DeliveryPendingError, match="not idle enough"):
        outbox.drain_once(
            "acme",
            consumer="stable-worker",
            count=1,
            reclaim_idle_ms=300_000,
        )
    assert redis_client.xpending(STREAM, "__privileged_archival__")["pending"] == 1
    assert redis_client.hlen(LEDGER) == 0

    recovered = outbox.drain_once(
        "acme",
        consumer="stable-worker",
        count=1,
        reclaim_idle_ms=0,
    )
    assert recovered is not None
    assert recovered.stream_ids == ("1-0",)


def test_drain_treats_fresh_sequence_gap_as_retryable_pending_race(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    raw_two = _raw("2-0", _event("two").with_ledger_sequence(2))
    backend.entries = [raw_two]
    backend.read_response = [(STREAM.encode(), [raw_two])]

    with pytest.raises(DeliveryPendingError, match="earlier pending ledger sequence"):
        outbox.drain_once("acme", consumer="worker-racing")
    assert backend.read_group_calls == 1
    assert LEDGER not in backend.hashes


def test_archive_is_verified_marked_and_idempotent_across_ack_retry(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
        clock_ms=lambda: 123,
    )
    entry = _source_entry(backend, outbox)
    backend.fail_once("xack", ConnectionError("ack interrupted"))

    with pytest.raises(OutboxBackendError, match="ack interrupted"):
        outbox.archive_batch("acme", "archive", [entry])
    assert len(writer.storage.writes) == 2
    batch_id = outbox.batch_id("acme", [entry])
    ledger = backend.hashes[LEDGER]
    assert json.loads(ledger[f"batch:{batch_id}"])["status"] == "delivered"
    assert ledger["entry:1700000000000-0"] == batch_id

    retried = outbox.archive_batch("acme", "archive", [entry])
    assert retried.reused is True
    assert retried.acknowledged == 1
    assert len(writer.storage.writes) == 2
    assert retried.archive["schema"] == "privileged-audit-v1"
    restored = outbox.read_archive_batch(batch_id)
    assert len(restored) == 1
    assert restored[0].event == entry.event
    assert restored[0].event_json == entry.event_json

    path = retried.archive["path"]
    writer.storage.files[path] = writer.storage.files[path] + b"tampered"
    with pytest.raises(ArchiveVerificationError, match="size differs|hash differs"):
        outbox.read_archive_batch(batch_id)


def test_role_delete_archive_requires_and_round_trips_exact_cascade_sidecar(
    backend,
):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    cascade_record = _cascade_event()
    manifest_key = _install_cascade_manifest(backend, cascade_record)
    cascade_entry = outbox._decode_entry(_raw("1-0", cascade_record))
    anchor_record = _event("anchor").with_ledger_sequence(2)
    anchor_entry = outbox._decode_entry(_raw("2-0", anchor_record))
    backend.entries = [_raw("1-0", cascade_record), _raw("2-0", anchor_record)]

    result = outbox.archive_batch(
        "acme", "archive", [cascade_entry, anchor_entry],
    )

    assert result.acknowledged == 2
    assert len(writer.storage.writes) == 3
    assert result.archive["cascade"]["manifest_count"] == 1
    assert result.archive["cascade"]["row_count"] == 2
    restored = outbox.read_archive_batch(result.batch_id)
    manifests = outbox.read_archive_cascades(result.batch_id)
    assert [entry.stream_id for entry in restored] == ["1-0", "2-0"]
    assert len(manifests) == 1
    assert manifests[0].event_id == cascade_record.event_id
    assert manifests[0].user_count == 1
    assert manifests[0].removed_assignment_count == 2
    assert manifests[0].rows[0].user_id == "alice-id"
    assert manifests[0].rows[0].removed_occurrences == 2

    backend.groups = [{
        "name": "archive", "pending": 0, "last-delivered-id": "2-0",
    }]
    backend.fail_once("eval", ConnectionError("atomic trim interrupted"))
    with pytest.raises(OutboxBackendError, match="atomic trim interrupted"):
        outbox.trim_delivered("archive", through_id="2-0")
    assert manifest_key in backend.hashes
    assert [entry[0] for entry in backend.entries] == [b"1-0", b"2-0"]

    assert outbox.trim_delivered("archive", through_id="2-0") == 1
    assert manifest_key not in backend.hashes
    assert [entry[0] for entry in backend.entries] == [b"2-0"]


@pytest.mark.parametrize(
    ("tamper", "match"),
    [
        (lambda backend, key: backend.hashes.pop(key), "manifest is missing"),
        (
            lambda backend, key: backend.hashes[key].update(
                {"removed_assignment_count": "3"}
            ),
            "counters do not match",
        ),
        (
            lambda backend, key: backend.hashes[key].update(
                {"user:alice-id": "1|2|1|3|2"}
            ),
            "occurrence sum",
        ),
    ],
)
def test_cascade_manifest_failure_blocks_archive_marker_and_ack(
    backend,
    tamper,
    match,
):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    record = _cascade_event()
    key = _install_cascade_manifest(backend, record)
    tamper(backend, key)
    entry = _source_entry(backend, outbox, "1-0", record)

    with pytest.raises(OutboxRecordError, match=match):
        outbox.archive_batch("acme", "archive", [entry])

    assert writer.storage.writes == []
    assert backend.acks == []
    assert LEDGER not in backend.hashes


def test_cascade_archive_tampering_blocks_readback_and_trim(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    cascade_record = _cascade_event()
    manifest_key = _install_cascade_manifest(backend, cascade_record)
    cascade_entry = outbox._decode_entry(_raw("1-0", cascade_record))
    anchor_record = _event("anchor").with_ledger_sequence(2)
    anchor_entry = outbox._decode_entry(_raw("2-0", anchor_record))
    backend.entries = [_raw("1-0", cascade_record), _raw("2-0", anchor_record)]
    delivered = outbox.archive_batch(
        "acme", "archive", [cascade_entry, anchor_entry],
    )
    cascade_path = delivered.archive["cascade"]["path"]
    writer.storage.files[cascade_path] += b"tampered"

    with pytest.raises(ArchiveVerificationError, match="cascade archive (size|file hash)"):
        outbox.read_archive_batch(delivered.batch_id)
    backend.groups = [{
        "name": "archive", "pending": 0, "last-delivered-id": "2-0",
    }]
    with pytest.raises(ArchiveVerificationError, match="cascade archive (size|file hash)"):
        outbox.trim_delivered("archive", through_id="2-0")
    assert manifest_key in backend.hashes
    assert backend.deleted == []


def test_state_neutral_attempt_at_namespace_zero_archives_exactly(backend):
    from supertable.audit.privileged import build_record

    event = build_record(
        organization="acme",
        super_name="sales",
        action="role_create",
        resource_type="role",
        resource_id="pending-role",
        outcome="denied",
        cause="request_rejected",
        namespace_version=0,
        ledger_sequence=1,
    )
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox, event=event)

    delivered = outbox.archive_batch("acme", "archive", [entry])
    restored = outbox.read_archive_batch(delivered.batch_id)

    assert restored[0].event["outcome"] == "denied"
    assert restored[0].event["namespace_version"] == 0
    assert restored[0].committed_event_json == entry.committed_event_json


def test_archive_rejects_reordered_or_gapped_ledger_entries(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = outbox._decode_entry(_raw("1700000000000-0", _event("one")))
    second_record = _event("two").with_ledger_sequence(3)
    second = outbox._decode_entry(_raw("1700000000001-0", second_record))

    with pytest.raises(OutboxRecordError, match="contiguous"):
        outbox.archive_batch("acme", "archive", [first, second])
    with pytest.raises(OutboxRecordError, match="stream order"):
        outbox.archive_batch("acme", "archive", [second, first])
    with pytest.raises(OutboxRecordError, match="organization"):
        outbox.archive_batch("another-org", "archive", [first])
    assert writer.storage.writes == []


def test_archive_batches_advance_one_global_contiguous_checkpoint(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "1-0", _event("one"))
    second = _source_entry(
        backend, outbox, "2-0", _event("two").with_ledger_sequence(2)
    )
    third = _source_entry(
        backend, outbox, "3-0", _event("three").with_ledger_sequence(3)
    )

    outbox.archive_batch("acme", "archive", [first])
    with pytest.raises(OutboxRecordError, match="expected sequence 2, got 3"):
        outbox.archive_batch("acme", "archive", [third])

    outbox.archive_batch("acme", "archive", [second])
    outbox.archive_batch("acme", "archive", [third])
    head = json.loads(backend.hashes[LEDGER][outbox._ARCHIVE_HEAD_FIELD])
    assert head["first_sequence"] == 3
    assert head["last_sequence"] == 3
    assert head["last_stream_id"] == "3-0"


def test_retry_cannot_rearchive_delivered_entries_in_a_larger_batch(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "1-0", _event("one"))
    second = _source_entry(
        backend, outbox, "2-0", _event("two").with_ledger_sequence(2)
    )
    third = _source_entry(
        backend, outbox, "3-0", _event("three").with_ledger_sequence(3)
    )

    delivered = outbox.archive_batch("acme", "archive", [first, second])
    with pytest.raises(OutboxRecordError, match="expected sequence 3, got 1"):
        outbox.archive_batch("acme", "archive", [first, second, third])
    retried = outbox.archive_batch("acme", "archive", [first, second])

    assert retried.batch_id == delivered.batch_id
    assert retried.reused is True
    assert len(writer.storage.writes) == 2


def test_archive_verification_failure_retries_verification_without_rewriting(backend):
    writer = FakeParquetWriter()
    writer.storage.failures.append(ConnectionError("storage read unavailable"))
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER, parquet_writer=writer
    )
    entry = _source_entry(backend, outbox)

    with pytest.raises(OutboxBackendError, match="storage read unavailable"):
        outbox.archive_batch("acme", "archive", [entry])
    assert len(writer.storage.writes) == 1

    result = outbox.archive_batch("acme", "archive", [entry])
    assert result.reused is True
    assert len(writer.storage.writes) == 2


def test_storage_write_failure_leaves_retryable_deterministic_claim(backend):
    writer = FakeParquetWriter()
    writer.storage.write_failures.append(OSError("storage write failed"))
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER, parquet_writer=writer
    )
    entry = _source_entry(backend, outbox)

    with pytest.raises(OutboxBackendError, match="storage write failed"):
        outbox.archive_batch("acme", "archive", [entry])
    retried = outbox.archive_batch("acme", "archive", [entry])
    assert retried.reused is True
    assert len(writer.storage.writes) == 2


def test_headless_first_batch_claim_is_recoverable_but_not_reported_empty(backend):
    writer = FakeParquetWriter()
    writer.storage.write_failures.append(OSError("storage write failed"))
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)

    with pytest.raises(OutboxBackendError, match="storage write failed"):
        outbox.archive_batch("acme", "archive", [entry])
    assert outbox._ARCHIVE_HEAD_FIELD not in backend.hashes[LEDGER]
    assert set(backend.hashes[LEDGER]) == {
        "sequence-claim:1",
        f"batch:{outbox.batch_id('acme', [entry])}",
    }
    with pytest.raises(DeliveryPendingError, match="durably claimed"):
        outbox.verify_checkpoint_head("acme")

    recovered = outbox.archive_batch("acme", "archive", [entry])
    assert recovered.reused is True
    assert outbox.verify_checkpoint_head("acme") is not None


def test_headless_written_verified_first_batch_is_recoverable(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    # Claim, writing->written_verified CAS, then fail the final head/marker CAS.
    backend.fail_eval_call(3, ConnectionError("finalize interrupted"))

    with pytest.raises(OutboxBackendError, match="finalize interrupted"):
        outbox.archive_batch("acme", "archive", [entry])
    batch_id = outbox.batch_id("acme", [entry])
    assert json.loads(backend.hashes[LEDGER][f"batch:{batch_id}"])[
        "status"
    ] == "written_verified"
    with pytest.raises(DeliveryPendingError, match="durably claimed"):
        outbox.verify_checkpoint_head("acme")

    recovered = outbox.archive_batch("acme", "archive", [entry])
    assert recovered.reused is True
    assert outbox.verify_checkpoint_head("acme") is not None


def test_tampered_headless_writing_artifact_claim_is_integrity_failure(backend):
    writer = FakeParquetWriter()
    writer.storage.write_failures.append(OSError("storage write failed"))
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)

    with pytest.raises(OutboxBackendError, match="storage write failed"):
        outbox.archive_batch("acme", "archive", [entry])
    batch_id = outbox.batch_id("acme", [entry])
    field = f"batch:{batch_id}"
    claim = json.loads(backend.hashes[LEDGER][field])
    claim["artifacts"]["parent"]["file_hash"] = "0" * 64
    backend.hashes[LEDGER][field] = json.dumps(
        claim, sort_keys=True, separators=(",", ":"),
    )

    with pytest.raises(OutboxRecordError, match="checkpoint|artifact"):
        outbox.verify_checkpoint_head("acme")


def test_missing_archive_head_with_delivered_residue_is_integrity_failure(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    outbox.archive_batch("acme", "archive", [entry])
    del backend.hashes[LEDGER][outbox._ARCHIVE_HEAD_FIELD]

    with pytest.raises(OutboxRecordError, match="archive head is absent"):
        outbox.verify_checkpoint_head("acme")
    with pytest.raises(OutboxRecordError, match="archive head is absent"):
        outbox.verify_checkpoint_chain("acme")


def test_orphan_headless_sequence_claim_is_integrity_failure(backend):
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=FakeParquetWriter(),
    )
    backend.hashes[LEDGER] = {"sequence-claim:1": "a" * 64}

    with pytest.raises(OutboxRecordError, match="residual evidence"):
        outbox.verify_checkpoint_head("acme")


def test_redis_claim_failure_prevents_archive_write(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER, parquet_writer=writer
    )
    entry = _source_entry(backend, outbox)
    backend.fail_once("eval", ConnectionError("ledger unavailable"))

    with pytest.raises(OutboxBackendError, match="ledger unavailable"):
        outbox.archive_batch("acme", "archive", [entry])
    assert writer.storage.writes == []


def test_ledger_update_failure_reconciles_same_archive_path_without_duplicate_write(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER, parquet_writer=writer
    )
    entry = _source_entry(backend, outbox)
    backend.fail_eval_call(2, ConnectionError("ledger update interrupted"))

    with pytest.raises(OutboxBackendError, match="ledger update interrupted"):
        outbox.archive_batch("acme", "archive", [entry])
    assert len(writer.storage.writes) == 2

    result = outbox.archive_batch("acme", "archive", [entry])
    assert result.reused is True
    assert len(writer.storage.writes) == 2


def test_existing_deterministic_archive_with_different_bytes_fails_closed(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER, parquet_writer=writer
    )
    entry = _source_entry(backend, outbox)
    batch_id = outbox.batch_id("acme", [entry])
    path = outbox._archive_path("acme", batch_id)
    writer.storage.files[path] = b"tampered"

    with pytest.raises(ArchiveVerificationError, match="immutable (hash/size|claim)"):
        outbox.archive_batch("acme", "archive", [entry])
    assert backend.acks == []


def test_trim_refuses_pending_unmarked_or_overlarge_ranges(backend, outbox):
    backend.entries = [
        _raw("1-0"),
        _raw("2-0", _event("evt-2")),
        _raw("3-0", _event("evt-3")),
    ]
    backend.groups = [{"name": "archive", "pending": 1, "last-delivered-id": "3-0"}]
    with pytest.raises(DeliveryPendingError):
        outbox.trim_delivered("archive", through_id="3-0")
    assert backend.deleted == []

    backend.groups[0]["pending"] = 0
    with pytest.raises(DeliveryPendingError, match="markers are missing"):
        outbox.trim_delivered("archive", through_id="3-0")
    assert backend.deleted == []

    backend.hashes[LEDGER] = {
        "entry:1-0": "batch-1",
        "entry:2-0": "batch-1",
        "entry:3-0": "batch-1",
    }
    with pytest.raises(DeliveryPendingError, match="more than 1"):
        outbox.trim_delivered("archive", through_id="3-0", max_entries=1)
    assert backend.deleted == []

    with pytest.raises(ValueError, match="SHA-256"):
        outbox.trim_delivered("archive", through_id="3-0")
    assert backend.deleted == []
    assert [entry[0] for entry in backend.entries] == [b"1-0", b"2-0", b"3-0"]


def test_trim_backend_failure_is_never_reported_as_zero(backend, outbox):
    backend.groups = [{"name": "archive", "pending": 0, "last-delivered-id": "2-0"}]
    backend.fail_once("xrange", ConnectionError("range unavailable"))
    with pytest.raises(OutboxBackendError, match="range unavailable"):
        outbox.trim_delivered("archive", through_id="2-0")


def test_health_distinguishes_missing_stream_from_backend_failure(backend, outbox):
    backend.stream_exists = False
    health = outbox.health()
    assert health.reachable is True
    assert health.stream_exists is False

    backend.fail_once("ping", ConnectionError("redis unavailable"))
    with pytest.raises(OutboxBackendError, match="redis unavailable"):
        outbox.health()


def test_archive_revalidates_mutated_or_forged_entries_before_claim(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )

    mutated = outbox._decode_entry(_raw())
    mutated.event["ledger_sequence"] = 2
    with pytest.raises(OutboxRecordError, match="mapping differs"):
        outbox.archive_batch("acme", "archive", [mutated])

    valid = outbox._decode_entry(_raw())
    forged = type(valid)(
        stream_id=valid.stream_id,
        event_json=valid.event_json,
        event=dict(valid.event),
        committed_event_json=valid.event_json,
    )
    with pytest.raises(OutboxRecordError, match="ledger_sequence must be positive"):
        outbox.archive_batch("acme", "archive", [forged])

    assert writer.storage.writes == []
    assert LEDGER not in backend.hashes
    assert backend.acks == []


def test_drain_fails_closed_when_autoclaim_reports_deleted_pending_ids(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    backend.autoclaim_response = [b"0-0", [], [b"1700000000000-0"]]

    with pytest.raises(OutboxRecordError, match="deleted pending stream IDs"):
        outbox.drain_once("acme", consumer="worker-1")

    assert writer.storage.writes == []
    assert backend.acks == []


def test_query_rejects_oversized_commit_decimal_as_record_error(backend, outbox):
    raw_id, fields = _raw()
    fields[b"ledger_sequence"] = b"9" * 10_000
    backend.entries = [(raw_id, fields)]

    with pytest.raises(
        OutboxRecordError, match="canonical bounded decimal|4-KiB",
    ):
        outbox.query()


def test_local_atomic_archive_retry_survives_interrupted_replace(
    backend,
    monkeypatch,
    tmp_path: Path,
):
    from supertable.audit.writer_parquet import ParquetAuditWriter
    from supertable.storage.local_storage import LocalStorage

    monkeypatch.chdir(tmp_path)
    storage = LocalStorage()
    writer = ParquetAuditWriter(storage=storage)
    outbox = PrivilegedAuditOutbox(
        backend,
        stream_key=STREAM,
        delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    real_replace = os.replace

    def replace_then_interrupt(source, target):
        real_replace(source, target)
        raise OSError("replace acknowledgement interrupted")

    monkeypatch.setattr(os, "replace", replace_then_interrupt)
    with pytest.raises(OutboxBackendError, match="replace acknowledgement interrupted"):
        outbox.archive_batch("acme", "archive", [entry])

    batch_id = outbox.batch_id("acme", [entry])
    archive_path = tmp_path / outbox._archive_path("acme", batch_id)
    assert archive_path.is_file()
    assert not list(archive_path.parent.glob(".tmp-bytes-*"))

    monkeypatch.setattr(os, "replace", real_replace)
    retried = outbox.archive_batch("acme", "archive", [entry])
    assert retried.reused is True
    assert outbox.read_archive_batch(batch_id)[0].event == entry.event


def test_archive_rejects_entry_absent_from_exact_source_stream(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    forged = outbox._decode_entry(_raw("999-0", _event("invented")))

    with pytest.raises(OutboxRecordError, match="source stream has no exact entry"):
        outbox.archive_batch("acme", "archive", [forged])

    assert LEDGER not in backend.hashes
    assert writer.storage.writes == []
    assert backend.acks == []


def test_cross_batch_stream_ids_must_advance_the_archive_head(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "100-0", _event("one"))
    second = _source_entry(
        backend, outbox, "50-0", _event("two").with_ledger_sequence(2),
    )
    outbox.archive_batch("acme", "archive", [first])

    with pytest.raises(OutboxRecordError, match="do not advance"):
        outbox.archive_batch("acme", "archive", [second])

    head = json.loads(backend.hashes[LEDGER][outbox._ARCHIVE_HEAD_FIELD])
    assert head["last_sequence"] == 1
    assert head["last_stream_id"] == "100-0"


def test_concurrent_same_batch_cannot_regress_archive_head(backend):
    writer = FakeParquetWriter()
    paused = threading.Event()
    release = threading.Event()

    class SlowOutbox(PrivilegedAuditOutbox):
        _paused_once = False

        def _cas_batch_record(self, batch_field, expected_raw, replacement_raw):
            if not self._paused_once:
                self._paused_once = True
                paused.set()
                assert release.wait(5)
            return super()._cas_batch_record(
                batch_field, expected_raw, replacement_raw,
            )

    slow = SlowOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    fast = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, fast, "1-0", _event("one"))
    second = _source_entry(
        backend, fast, "2-0", _event("two").with_ledger_sequence(2),
    )
    errors = []

    def run_slow():
        try:
            slow.archive_batch("acme", "archive", [first])
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=run_slow)
    thread.start()
    assert paused.wait(5)
    fast.archive_batch("acme", "archive", [first])
    fast.archive_batch("acme", "archive", [second])
    head = json.loads(backend.hashes[LEDGER][fast._ARCHIVE_HEAD_FIELD])
    assert head["last_sequence"] == 2
    release.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []
    head = json.loads(backend.hashes[LEDGER][fast._ARCHIVE_HEAD_FIELD])
    assert head["last_sequence"] == 2


def test_ack_failure_recovery_never_merges_delivered_and_fresh_batches(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    raw_one = _raw("1-0", _event("one"))
    backend.entries = [raw_one]
    first = outbox._decode_entry(raw_one)
    backend.fail_once("xack", ConnectionError("ack response lost"))
    with pytest.raises(OutboxBackendError, match="ack response lost"):
        outbox.archive_batch("acme", "archive", [first])

    raw_two = _raw("2-0", _event("two").with_ledger_sequence(2))
    backend.autoclaim_response = [b"0-0", [raw_one], []]
    backend.read_response = [(STREAM.encode(), [raw_two])]
    assert outbox.drain_once("acme", consumer="worker") is None
    assert backend.acks[-1] == ("__privileged_archival__", ("1-0",))

    backend.autoclaim_response = [b"0-0", [], []]
    delivered = outbox.drain_once("acme", consumer="worker")
    assert delivered is not None
    assert delivered.stream_ids == ("2-0",)
    assert json.loads(backend.hashes[LEDGER][outbox._ARCHIVE_HEAD_FIELD])[
        "last_sequence"
    ] == 2


def test_serializer_change_adopts_claimed_existing_semantic_archive(
    backend, monkeypatch,
):
    import pyarrow.parquet as pq

    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    backend.fail_once("xack", ConnectionError("ack response lost"))
    with pytest.raises(OutboxBackendError):
        outbox.archive_batch("acme", "archive", [entry])
    original = outbox._serialize_archive
    original_bytes = original(outbox._archive_rows([entry]))

    def gzip_serializer(rows):
        table = pq.read_table(io.BytesIO(original(rows)))
        target = io.BytesIO()
        pq.write_table(table, target, compression="gzip")
        return target.getvalue()

    assert gzip_serializer(outbox._archive_rows([entry])) != original_bytes
    monkeypatch.setattr(outbox, "_serialize_archive", gzip_serializer)
    retried = outbox.archive_batch("acme", "archive", [entry])

    assert retried.reused is True
    assert retried.acknowledged == 1
    assert writer.storage.durability_checks[-2:] == [
        retried.archive["path"], retried.archive["checkpoint"]["path"],
    ]


def test_cascade_serializer_change_adopts_claimed_existing_sidecar(
    backend, monkeypatch,
):
    import pyarrow.parquet as pq

    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    cascade_record = _cascade_event()
    _install_cascade_manifest(backend, cascade_record)
    cascade_entry = _source_entry(
        backend, outbox, "1-0", cascade_record,
    )
    anchor = _source_entry(
        backend,
        outbox,
        "2-0",
        _event("anchor").with_ledger_sequence(2),
    )
    backend.fail_once("xack", ConnectionError("ack response lost"))
    with pytest.raises(OutboxBackendError):
        outbox.archive_batch("acme", "archive", [cascade_entry, anchor])
    original = outbox._serialize_cascade_archive
    manifests = outbox._load_cascade_manifests([cascade_entry, anchor])

    def gzip_serializer(rows):
        table = pq.read_table(io.BytesIO(original(rows)))
        target = io.BytesIO()
        pq.write_table(table, target, compression="gzip")
        return target.getvalue()

    monkeypatch.setattr(outbox, "_serialize_cascade_archive", gzip_serializer)
    retried = outbox.archive_batch(
        "acme", "archive", [cascade_entry, anchor],
    )

    assert retried.reused is True
    assert retried.acknowledged == 2
    assert gzip_serializer(
        outbox._cascade_archive_rows(manifests)
    ) != original(outbox._cascade_archive_rows(manifests))
    assert writer.storage.durability_checks[-3:] == [
        retried.archive["path"],
        retried.archive["cascade"]["path"],
        retried.archive["checkpoint"]["path"],
    ]


def test_archive_claim_and_membership_are_atomic_on_redis_failure(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    backend.fail_once("eval", ConnectionError("claim interrupted"))

    with pytest.raises(OutboxBackendError, match="claim interrupted"):
        outbox.archive_batch("acme", "archive", [entry])

    assert LEDGER not in backend.hashes
    assert writer.storage.writes == []


def test_checkpoint_paths_are_org_scoped_and_cross_org_isolated():
    writer = FakeParquetWriter()
    acme_backend = FakeRedis()
    beta_backend = FakeRedis()
    acme = PrivilegedAuditOutbox(
        acme_backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    beta = PrivilegedAuditOutbox(
        beta_backend,
        stream_key="supertable:{beta}:privileged-audit",
        delivery_ledger_key="supertable:{beta}:privileged-audit:delivery",
        parquet_writer=writer,
    )
    acme_entry = _source_entry(acme_backend, acme, event=_event("same"))
    beta_entry = _source_entry(
        beta_backend, beta, event=_event("same", organization="beta"),
    )
    acme_result = acme.archive_batch("acme", "archive", [acme_entry])
    beta_result = beta.archive_batch("beta", "archive", [beta_entry])

    acme_path = acme_result.archive["checkpoint"]["path"]
    beta_path = beta_result.archive["checkpoint"]["path"]
    assert acme_path.startswith("acme/__audit__/")
    assert beta_path.startswith("beta/__audit__/")
    assert acme_path != beta_path
    assert acme.verify_checkpoint_head("acme") is not None
    assert beta.verify_checkpoint_head("beta") is not None


def test_full_checkpoint_chain_detects_tampered_grandparent(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    results = []
    for sequence in range(1, 4):
        entry = _source_entry(
            backend,
            outbox,
            f"{sequence}-0",
            _event(f"event-{sequence}").with_ledger_sequence(sequence),
        )
        results.append(outbox.archive_batch("acme", "archive", [entry]))

    assert outbox.verify_checkpoint_head("acme") is not None
    assert outbox.verify_checkpoint_chain("acme")["batch_count"] == 3
    first_path = results[0].archive["checkpoint"]["path"]
    writer.storage.files[first_path] += b"tampered"

    # The routine heartbeat checks only head + immediate predecessor.
    assert outbox.verify_checkpoint_head("acme") is not None
    with pytest.raises(ArchiveVerificationError):
        outbox.verify_checkpoint_chain("acme")


def test_checkpoint_head_rejects_missing_latest_or_predecessor_manifest(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "1-0", _event("one"))
    second = _source_entry(
        backend, outbox, "2-0", _event("two").with_ledger_sequence(2),
    )
    first_result = outbox.archive_batch("acme", "archive", [first])
    second_result = outbox.archive_batch("acme", "archive", [second])
    predecessor_path = first_result.archive["checkpoint"]["path"]
    latest_path = second_result.archive["checkpoint"]["path"]

    predecessor_bytes = writer.storage.files.pop(predecessor_path)
    with pytest.raises(OutboxBackendError, match="checkpoint"):
        outbox.verify_checkpoint_head("acme")
    writer.storage.files[predecessor_path] = predecessor_bytes

    writer.storage.files.pop(latest_path)
    with pytest.raises(OutboxBackendError, match="checkpoint"):
        outbox.verify_checkpoint_head("acme")


def test_trim_reverifies_every_archive_before_source_deletion(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "1-0", _event("one"))
    second = _source_entry(
        backend, outbox, "2-0", _event("two").with_ledger_sequence(2),
    )
    delivered = outbox.archive_batch("acme", "archive", [first, second])
    del writer.storage.files[delivered.archive["path"]]
    backend.groups = [{
        "name": "archive", "pending": 0, "last-delivered-id": "2-0",
    }]

    with pytest.raises(OutboxBackendError):
        outbox.trim_delivered("archive", through_id="2-0")

    assert [item[0] for item in backend.entries] == [b"1-0", b"2-0"]


def test_trim_cas_rejects_delivery_marker_changed_after_verification():
    class RacingRedis(FakeRedis):
        def eval(self, script, numkeys, *values):
            if script == PrivilegedAuditOutbox._TRIM_DELIVERED_LUA:
                self.hashes[LEDGER]["entry:1-0"] = "0" * 64
            return super().eval(script, numkeys, *values)

    backend = RacingRedis()
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    first = _source_entry(backend, outbox, "1-0", _event("one"))
    second = _source_entry(
        backend, outbox, "2-0", _event("two").with_ledger_sequence(2),
    )
    outbox.archive_batch("acme", "archive", [first, second])
    backend.groups = [{
        "name": "archive", "pending": 0, "last-delivered-id": "2-0",
    }]

    with pytest.raises(OutboxBackendError, match="metadata changed"):
        outbox.trim_delivered("archive", through_id="2-0")

    assert [item[0] for item in backend.entries] == [b"1-0", b"2-0"]


def test_many_trimmed_batches_keep_redis_ledger_bounded(backend):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    results = []
    for sequence in range(1, 13):
        entry = _source_entry(
            backend,
            outbox,
            f"{sequence}-0",
            _event(f"event-{sequence}").with_ledger_sequence(sequence),
        )
        results.append(outbox.archive_batch("acme", "archive", [entry]))
        backend.groups = [{
            "name": "archive",
            "pending": 0,
            "last-delivered-id": f"{sequence}-0",
        }]
        outbox.trim_delivered("archive", through_id=f"{sequence}-0")

    assert len(backend.hashes[LEDGER]) == 4
    assert set(backend.hashes[LEDGER]) == {
        outbox._ARCHIVE_HEAD_FIELD,
        "sequence-claim:12",
        f"batch:{results[-1].batch_id}",
        "entry:12-0",
    }
    with pytest.raises(ValueError, match="organization is required"):
        outbox.read_archive_batch(results[0].batch_id)
    assert outbox.read_archive_batch(
        results[0].batch_id, organization="acme",
    )[0].event["ledger_sequence"] == 1
    assert outbox.verify_checkpoint_chain("acme")["batch_count"] == 12


def test_archive_batch_and_ledger_claims_are_bounded(backend, monkeypatch):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = outbox._decode_entry(_raw())
    with pytest.raises(ValueError, match="must not be empty"):
        outbox.archive_batch("acme", "archive", [])
    with pytest.raises(ValueError, match="exceeds 1000"):
        outbox.archive_batch("acme", "archive", [entry] * 1001)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        outbox.query(count=1001)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        outbox.read_group("archive", "worker", count=1001)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        outbox.autoclaim(
            "archive", "worker", min_idle_ms=0, count=1001,
        )
    with pytest.raises(ValueError, match="more than 1000"):
        outbox.ack("archive", ["1-0"] * 1001)

    entry = _source_entry(backend, outbox)
    monkeypatch.setattr(PrivilegedAuditOutbox, "_MAX_LEDGER_RECORD_BYTES", 128)
    with pytest.raises(OutboxRecordError, match="1-MiB limit"):
        outbox.archive_batch("acme", "archive", [entry])
    assert LEDGER not in backend.hashes


def test_oversize_artifact_is_rejected_before_immutable_sequence_claim(
    backend, monkeypatch,
):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    monkeypatch.setattr(PrivilegedAuditOutbox, "_MAX_PARQUET_ARTIFACT_BYTES", 1)

    with pytest.raises(ValueError, match="reduce the archive batch count"):
        outbox.archive_batch("acme", "archive", [entry])
    assert LEDGER not in backend.hashes
    assert writer.storage.writes == []


def test_aggregate_cascade_memory_bound_is_checked_before_sequence_claim(
    backend, monkeypatch,
):
    writer = FakeParquetWriter()
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    event = _cascade_event(affected_count=1)
    _install_cascade_manifest(backend, event)
    entry = _source_entry(backend, outbox, event=event)
    monkeypatch.setattr(PrivilegedAuditOutbox, "_MAX_CASCADE_ARCHIVE_ROWS", 1)

    with pytest.raises(ValueError, match="archive memory bound"):
        outbox.archive_batch("acme", "archive", [entry])
    assert LEDGER not in backend.hashes
    assert writer.storage.writes == []


def test_stream_event_size_is_rejected_before_json_decode(outbox):
    raw = (b"1-0", {b"event_json": b"{" + (b" " * (64 * 1024))})
    with pytest.raises(OutboxRecordError, match="64-KiB"):
        outbox._decode_entry(raw)


def test_local_storage_fsyncs_logical_directory_chain_through_root_on_retry(
    monkeypatch, tmp_path: Path,
):
    from supertable.storage.local_storage import LocalStorage

    monkeypatch.chdir(tmp_path)
    storage = LocalStorage()
    synced = []
    monkeypatch.setattr(
        LocalStorage,
        "_fsync_directory",
        staticmethod(lambda directory: synced.append(os.path.abspath(directory))),
    )
    # Model an ancestor left visible by a failed publication after this
    # storage namespace was opened.  A retry must not assume that merely
    # existing directory entry is durable: it has to anchor the complete
    # logical path through the configured root before acknowledging success.
    (tmp_path / "existing").mkdir()
    storage.write_bytes_atomic("existing/object", b"one")
    assert synced == [
        str(tmp_path / "existing"),
        str(tmp_path),
    ]

    synced.clear()
    storage.write_bytes_atomic("new/a/b/object", b"two")
    assert synced == [
        str(tmp_path / "new/a/b"),
        str(tmp_path / "new/a"),
        str(tmp_path / "new"),
        str(tmp_path),
    ]

    synced.clear()
    storage.ensure_bytes_durable("new/a/b/object")
    assert synced == [
        str(tmp_path / "new/a/b"),
        str(tmp_path / "new/a"),
        str(tmp_path / "new"),
        str(tmp_path),
    ]


def test_existing_local_archive_retry_propagates_directory_fsync_failure(
    backend, monkeypatch, tmp_path: Path,
):
    from supertable.audit.writer_parquet import ParquetAuditWriter
    from supertable.storage.local_storage import LocalStorage

    monkeypatch.chdir(tmp_path)
    storage = LocalStorage()
    writer = ParquetAuditWriter(storage=storage)
    outbox = PrivilegedAuditOutbox(
        backend, stream_key=STREAM, delivery_ledger_key=LEDGER,
        parquet_writer=writer,
    )
    entry = _source_entry(backend, outbox)
    real_fsync_directory = LocalStorage._fsync_directory

    def fail_directory_fsync(_directory):
        raise OSError("directory fsync failed")

    monkeypatch.setattr(
        LocalStorage, "_fsync_directory", staticmethod(fail_directory_fsync),
    )
    with pytest.raises(OutboxBackendError, match="directory fsync failed"):
        outbox.archive_batch("acme", "archive", [entry])
    assert backend.acks == []
    archive_path = tmp_path / outbox._archive_path(
        "acme", outbox.batch_id("acme", [entry]),
    )
    assert archive_path.is_file()

    # The visible existing-object retry must try to establish durability again.
    with pytest.raises(OutboxBackendError, match="directory fsync failed"):
        outbox.archive_batch("acme", "archive", [entry])
    assert backend.acks == []

    monkeypatch.setattr(
        LocalStorage, "_fsync_directory", staticmethod(real_fsync_directory),
    )
    delivered = outbox.archive_batch("acme", "archive", [entry])
    assert delivered.acknowledged == 1
