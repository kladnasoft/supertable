from __future__ import annotations

import fnmatch
import json
from types import SimpleNamespace
from unittest.mock import patch

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.audit import get_privileged_audit_outbox
from supertable.audit.privileged import (
    PrivilegedActionContext,
    PrivilegedAuditRecord,
    build_record,
)
from supertable.audit.privileged_outbox import PrivilegedAuditOutbox
from supertable.recovery.redis_rebuild import (
    RecoveryError,
    build_parser,
    create_catalog_checkpoint,
    main,
    plan_redis_rebuild,
    rebuild_redis,
)
from supertable.redis_catalog import RedisCatalog


ORG = "acme"
SUP = "sales"
TABLE = "orders"
SNAPSHOT_PATH = f"{ORG}/{SUP}/tables/{TABLE}/snapshots/v3.json"
DATA_PATH = f"{ORG}/{SUP}/tables/{TABLE}/data/part.parquet"


class MemoryStorage:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.durable: list[str] = []

    def exists(self, path):
        return path in self.files

    def size(self, path):
        return len(self.files[path])

    def read_bytes(self, path):
        return self.files[path]

    def write_bytes(self, path, payload):
        self.files[path] = bytes(payload)

    def write_bytes_atomic(self, path, payload):
        self.files[path] = bytes(payload)

    def ensure_bytes_durable(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        self.durable.append(path)

    def list_files(self, path, pattern="*"):
        prefix = path.rstrip("/") + "/"
        result = []
        for key in self.files:
            if not key.startswith(prefix):
                continue
            suffix = key[len(prefix):]
            if "/" not in suffix and fnmatch.fnmatch(suffix, pattern):
                result.append(key)
        return sorted(result)


def _redis():
    return fakeredis.FakeRedis(decode_responses=True)


def _seed_catalog(redis_client, storage):
    data_bytes = b"parquet-placeholder"
    snapshot = {
        "simple_name": TABLE,
        "location": f"{ORG}/{SUP}/tables/{TABLE}",
        "snapshot_version": 3,
        "last_updated_ms": 1_700_000_000_000,
        "previous_snapshot": None,
        "schema": [{"id": "long"}],
        "resources": [{"file": DATA_PATH, "file_size": len(data_bytes)}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 42,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.files[SNAPSHOT_PATH] = json.dumps(snapshot).encode()
    storage.files[DATA_PATH] = data_bytes
    redis_client.set(
        RK.meta_root(ORG, SUP),
        json.dumps({"version": 8, "ts": 1_700_000_000_000}),
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUP, TABLE),
        json.dumps({
            "version": 3,
            "ts": 1_700_000_000_000,
            "path": SNAPSHOT_PATH,
            "payload": snapshot,
        }),
    )
    redis_client.sadd(RK.meta_table_names(ORG, SUP), TABLE)
    redis_client.set(RK.schema(ORG, SUP, TABLE), json.dumps({"id": "long"}))
    redis_client.set(RK.meta_rowid_seq(ORG, SUP, TABLE), "42")
    activation = {
        "version": 1,
        "kind": "supertable_privileged_activation_anchor",
        "organization": ORG,
        "activation_id": "recovery-baseline",
        "created_ms": 1_700_000_000_000,
        "state_sha256": "a" * 64,
        "artifact_sha256": "b" * 64,
    }
    redis_client.set(
        RK.audit_privileged_activation(ORG),
        json.dumps(
            activation, sort_keys=True, separators=(",", ":"),
        ),
    )
    # Ephemeral ownership must never be resurrected by a DR checkpoint.
    redis_client.set(RK.lock_leaf(ORG, SUP, TABLE), "dead-owner", ex=30)


def _append_event(source, storage, *, sequence=1, archive=True):
    timestamp_ms = 1_700_000_000_000 + sequence
    context = PrivilegedActionContext(
        actor_type="user",
        actor_id="admin-1",
        username="admin",
        reason="recovery test",
    )
    record = build_record(
        context=context,
        organization=ORG,
        super_name=SUP,
        action="role_update",
        resource_type="role",
        resource_id="reader",
        before_document={"name": "old"},
        after_document={"name": "new"},
        before_version=1,
        after_version=2,
        changed_fields=("name",),
        namespace_version=sequence + 1,
        ledger_sequence=sequence,
        event_id=f"audit-event-{sequence}",
        mutation_id=f"audit-mutation-{sequence}",
        timestamp_ms=timestamp_ms,
    )
    committed = record.to_dict()
    template = dict(committed)
    template.update({field: 0 for field in PrivilegedAuditOutbox._COMMIT_FIELDS})
    fields = {"event_json": PrivilegedAuditRecord.from_dict(template).to_json()}
    fields.update({
        field: str(committed[field])
        for field in PrivilegedAuditOutbox._INDEX_FIELDS
    })
    outbox = get_privileged_audit_outbox(
        ORG, redis_client=source, storage=storage,
    )
    stream_id = f"{timestamp_ms}-0"
    source.xadd(outbox.stream_key, fields, id=stream_id)
    source.hset(
        RK.audit_privileged_meta(ORG),
        mapping={
            "sequence": str(sequence),
            "last_stream_id": stream_id,
            "last_event_id": str(committed["event_id"]),
            "last_payload_hash": str(committed["payload_hash"]),
            "updated_ms": str(timestamp_ms),
        },
    )
    entry = outbox._decode_entry((stream_id, fields))
    if archive:
        result = outbox.drain_once(
            ORG,
            consumer="recovery-test",
            count=100,
            reclaim_idle_ms=0,
        )
        assert result is not None
    return entry


def _checkpoint(source, storage):
    return create_catalog_checkpoint(
        source, storage, ORG, clock_ms=lambda: 1_700_000_000_100,
    )


def test_rebuild_dry_run_apply_and_idempotent_retry():
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    checkpoint = _checkpoint(source, storage)

    dry = rebuild_redis(destination, storage, ORG, dry_run=True)
    assert dry.dry_run is True
    assert dry.applied is False
    assert destination.keys("*") == []

    applied = rebuild_redis(destination, storage, ORG, dry_run=False)
    assert applied.applied is True
    assert destination.get(RK.meta_leaf(ORG, SUP, TABLE)) == source.get(
        RK.meta_leaf(ORG, SUP, TABLE)
    )
    assert not destination.exists(RK.lock_leaf(ORG, SUP, TABLE))

    retry = rebuild_redis(destination, storage, ORG, dry_run=False)
    assert retry.applied is False
    assert retry.already_current is True
    assert retry.checkpoint_sha256 == checkpoint.manifest_sha256


def test_rebuild_refuses_partial_or_unexpected_destination_state():
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _checkpoint(source, storage)
    destination.set(RK.meta_root(ORG, SUP), "stale")

    with pytest.raises(RecoveryError, match="partial, stale, or unexpected"):
        rebuild_redis(destination, storage, ORG, dry_run=False)
    assert destination.get(RK.meta_root(ORG, SUP)) == "stale"


def test_rebuild_fails_closed_without_sealed_checkpoint_and_on_tampering():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    with pytest.raises(RecoveryError, match="no sealed Redis catalog checkpoint"):
        rebuild_redis(_redis(), storage, ORG)

    checkpoint = _checkpoint(source, storage)
    storage.files[SNAPSHOT_PATH] += b" "
    with pytest.raises(RecoveryError, match="snapshot seals differ"):
        plan_redis_rebuild(storage, ORG)
    assert checkpoint.path in storage.files


def test_rebuild_rejects_same_path_data_replacement_and_declared_size_drift():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _checkpoint(source, storage)

    # Same key and same byte length is still a different immutable artifact.
    storage.files[DATA_PATH] = b"tamperx-placeholder"
    assert len(storage.files[DATA_PATH]) == len(b"parquet-placeholder")
    with pytest.raises(RecoveryError, match="snapshot seals differ"):
        plan_redis_rebuild(storage, ORG)

    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    snapshot = json.loads(storage.files[SNAPSHOT_PATH])
    snapshot["resources"][0]["file_size"] += 1
    storage.files[SNAPSHOT_PATH] = json.dumps(snapshot).encode()
    with pytest.raises(RecoveryError, match="size differs from file_size"):
        _checkpoint(source, storage)


def test_checkpoint_rejects_privileged_wal_ahead_of_or_behind_archive():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _append_event(source, storage, archive=False)
    with pytest.raises(RecoveryError, match="no durable archive tip"):
        _checkpoint(source, storage)

    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _append_event(source, storage, sequence=1, archive=True)
    _append_event(source, storage, sequence=2, archive=False)
    with pytest.raises(RecoveryError, match="ahead of or behind"):
        _checkpoint(source, storage)


def test_rebuild_rejects_audit_archive_advanced_beyond_catalog_checkpoint():
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _append_event(source, storage, sequence=1, archive=True)
    _checkpoint(source, storage)

    # Storage may legitimately acquire a later archive after this catalog
    # checkpoint. Recovery must not combine that sequence with older state.
    _append_event(source, storage, sequence=2, archive=True)
    with pytest.raises(RecoveryError, match="advanced beyond"):
        plan_redis_rebuild(storage, ORG)
    with pytest.raises(RecoveryError, match="advanced beyond"):
        rebuild_redis(destination, storage, ORG, dry_run=False)
    assert destination.keys("*") == []


def test_rebuild_restores_verified_privileged_audit_anchor():
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    entry = _append_event(source, storage)
    _checkpoint(source, storage)

    plan = plan_redis_rebuild(storage, ORG)
    assert plan.audit_batch_count == 1
    assert plan.audit_last_sequence == 1
    rebuild_redis(destination, storage, ORG, dry_run=False)

    rows = destination.xrange(RK.audit_privileged_outbox(ORG), "-", "+")
    assert len(rows) == 1
    assert rows[0][0] == entry.stream_id
    assert destination.hget(RK.audit_privileged_meta(ORG), "sequence") == "1"
    groups = destination.xinfo_groups(RK.audit_privileged_outbox(ORG))
    assert groups[0]["name"] == "__privileged_archival__"
    assert groups[0]["last-delivered-id"] == entry.stream_id
    assert destination.get(RK.audit_privileged_activation(ORG)) == source.get(
        RK.audit_privileged_activation(ORG)
    )

    # The restored immutable genesis and ledger head must permit the next
    # audited mutation at exactly sequence N+1.
    with patch(
        "supertable.redis_catalog.RedisConnector",
        return_value=SimpleNamespace(r=destination),
    ):
        catalog = RedisCatalog()
    catalog.rbac_create_role(
        ORG,
        SUP,
        "recovered-reader",
        {
            "role_id": "recovered-reader",
            "role": "reader",
            "role_name": "recovered_reader",
            "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        },
        action_context=PrivilegedActionContext(
            actor_type="system",
            actor_id="recovery-test",
            mutation_id="post-recovery-mutation",
            reason="verify recovered audit continuity",
        ),
    )
    assert destination.hget(RK.audit_privileged_meta(ORG), "sequence") == "2"


def test_rebuild_rejects_corrupt_privileged_audit_checkpoint():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _append_event(source, storage)
    _checkpoint(source, storage)
    manifest_path = next(
        path for path in storage.files
        if "/__audit__/privileged/manifests/" in path
    )
    storage.files[manifest_path] = storage.files[manifest_path].replace(
        b'"last_sequence":1', b'"last_sequence":2', 1,
    )

    with pytest.raises(RecoveryError, match="checkpoint verification failed"):
        plan_redis_rebuild(storage, ORG)


def test_recovery_cli_requires_one_explicit_mode():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--organization", ORG])
    with pytest.raises(SystemExit):
        parser.parse_args(["--organization", ORG, "--dry-run", "--apply"])


def test_recovery_cli_apply_and_followup_dry_run_are_idempotent(
    monkeypatch,
    capsys,
):
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _checkpoint(source, storage)
    monkeypatch.setattr(
        "supertable.redis_connector.RedisConnector",
        lambda: SimpleNamespace(r=destination),
    )
    monkeypatch.setattr(
        "supertable.storage.storage_factory.get_storage",
        lambda: storage,
    )

    assert main(["--organization", ORG, "--apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["ok"] is True
    assert applied["applied"] is True

    assert main(["--organization", ORG, "--dry-run"]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["ok"] is True
    assert verified["already_current"] is True
    assert verified["applied"] is False
