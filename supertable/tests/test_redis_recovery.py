from __future__ import annotations

import fnmatch
import hashlib
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
from supertable.redis_catalog import _canonicalize_role_document
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
)
from supertable.tombstone_manifest_v2 import (
    TombstoneManifestV2,
    TombstoneSegment,
    canonical_tombstone_manifest_v2_bytes,
)


ORG = "acme"
SUP = "sales"
TABLE = "orders"
SNAPSHOT_PATH = f"{ORG}/{SUP}/tables/{TABLE}/snapshots/v3.json"
DATA_PATH = f"{ORG}/{SUP}/tables/{TABLE}/data/part.parquet"
DV_ROOT = f"{ORG}/{SUP}/tables/{TABLE}/tombstone/generation-3/manifest.json"
DV_SEGMENT = (
    f"{ORG}/{SUP}/tables/{TABLE}/tombstone/generation-3/segment.parquet"
)


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

    def stat_object(self, path):
        payload = self.files[path]
        return ObjectMetadata(
            size=len(payload),
            version=hashlib.sha256(payload).hexdigest(),
        )

    def read_range(self, path, offset, length, *, expected=None):
        current = self.stat_object(path)
        if expected is not None and current.identity_token() != expected.identity_token():
            raise ObjectIdentityMismatch(f"object changed: {path}")
        return self.files[path][offset:offset + length]

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
        "_row_filter": None,
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


def _seed_v2_catalog(redis_client, storage) -> TombstoneManifestV2:
    _seed_catalog(redis_client, storage)
    segment_bytes = b"sealed-v2-segment"
    storage.files[DV_SEGMENT] = segment_bytes
    manifest = TombstoneManifestV2(
        organization=ORG,
        super_name=SUP,
        simple_name=TABLE,
        base_snapshot_version=2,
        snapshot_version=3,
        total_rows=1,
        segments=(TombstoneSegment(
            file=DV_SEGMENT,
            rows=1,
            file_size=len(segment_bytes),
            digest="d" * 64,
        ),),
    )
    storage.files[DV_ROOT] = manifest.canonical_bytes()

    snapshot = json.loads(storage.files[SNAPSHOT_PATH])
    snapshot.update({
        "tombstone": DV_ROOT,
        "tombstone_rows": 1,
        "tombstone_digest": manifest.digest(),
        "tombstone_format": 2,
    })
    storage.files[SNAPSHOT_PATH] = json.dumps(snapshot).encode()
    leaf = json.loads(redis_client.get(RK.meta_leaf(ORG, SUP, TABLE)))
    leaf["payload"] = snapshot
    redis_client.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))
    return manifest


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


def _seed_consistent_rbac(source):
    role_id = "reader-role"
    role = _canonicalize_role_document(
        {
            "role": "reader",
            "role_name": "Analyst",
            "tables": {TABLE: {"columns": ["id"], "filters": ["*"]}},
        },
        default_if_empty=False,
    )
    role.update({"role_id": role_id, "doc_version": "1"})
    source.hset(
        RK.rbac_role_doc(ORG, SUP, role_id),
        mapping={
            key: json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            for key, value in role.items()
        },
    )
    source.sadd(RK.rbac_role_index(ORG, SUP), role_id)
    source.sadd(RK.rbac_role_type_index(ORG, SUP, "reader"), role_id)
    source.hset(RK.rbac_rolename_to_id(ORG, SUP), "analyst", role_id)
    source.hset(
        RK.rbac_role_meta(ORG, SUP),
        mapping={
            "version": "1",
            "last_updated_ms": "1700000000000",
            "initialized": "true",
        },
    )
    user_id = "analyst-user"
    source.hset(
        RK.rbac_user_doc(ORG, SUP, user_id),
        mapping={
            "user_id": user_id,
            "username": "analyst",
            "roles": json.dumps([role_id]),
            "doc_version": "1",
        },
    )
    source.sadd(RK.rbac_user_index(ORG, SUP), user_id)
    source.hset(RK.rbac_username_to_id(ORG, SUP), "analyst", user_id)
    source.hset(
        RK.rbac_user_meta(ORG, SUP),
        mapping={
            "version": "1",
            "last_updated_ms": "1700000000000",
            "initialized": "true",
        },
    )
    return role_id, user_id


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


@pytest.mark.parametrize(
    "root_value, message",
    [
        ("not-json", "not valid JSON"),
        ("[]", "not a JSON object"),
        ("{}", "invalid identity fields"),
        ('{"version": 8, "ts": true}', "invalid identity fields"),
        ('{"version": 9007199254740992, "ts": 1}', "invalid identity fields"),
        ('{"version": 8, "ts": 9007199254740992}', "invalid identity fields"),
    ],
)
def test_checkpoint_rejects_malformed_or_non_object_catalog_root(
    root_value, message,
):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_root(ORG, SUP), root_value)

    with pytest.raises(RecoveryError, match=message):
        _checkpoint(source, storage)

    assert not any("/__recovery__/" in path for path in storage.files)


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("version", "3", "no valid version"),
        ("version", True, "no valid version"),
        ("version", -1, "no valid version"),
        ("version", 1 << 53, "no valid version"),
        ("ts", "1700000000000", "no valid timestamp"),
        ("ts", True, "no valid timestamp"),
        ("ts", -1, "no valid timestamp"),
        ("ts", 1 << 53, "no valid timestamp"),
        ("ts", None, "no valid timestamp"),
    ],
)
def test_checkpoint_rejects_leaf_identity_runtime_cannot_commit(
    field, value, message,
):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    key = RK.meta_leaf(ORG, SUP, TABLE)
    leaf = json.loads(source.get(key))
    if value is None:
        leaf.pop(field)
    else:
        leaf[field] = value
    source.set(key, json.dumps(leaf))

    with pytest.raises(RecoveryError, match=message):
        _checkpoint(source, storage)

    assert not any("/__recovery__/" in path for path in storage.files)


@pytest.mark.parametrize(
    "field,value",
    [
        ("cloned_from", "../source"),
        ("replica_tables", "all"),
        ("read_only", 0),
    ],
)
def test_checkpoint_rejects_root_fields_runtime_replica_reads_reject(field, value):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    key = RK.meta_root(ORG, SUP)
    root = json.loads(source.get(key))
    root.update({
        "read_only": True,
        "clone_type": "replica",
        "cloned_from": "source",
        "replica_tables": [TABLE],
    })
    root[field] = value
    source.set(key, json.dumps(root))

    with pytest.raises(RecoveryError, match="runtime contract|live source"):
        _checkpoint(source, storage)


@pytest.mark.parametrize(
    "value",
    ["not-an-integer", "-1", "+1", "01", str(1 << 63)],
)
def test_checkpoint_rejects_noncanonical_or_unsafe_rowid_sequence(value):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_rowid_seq(ORG, SUP, TABLE), value)

    with pytest.raises(RecoveryError, match="rowid sequence"):
        _checkpoint(source, storage)


def test_checkpoint_allows_repairable_rowid_below_snapshot_floor():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_rowid_seq(ORG, SUP, TABLE), "1")

    assert _checkpoint(source, storage).snapshot_count == 1


@pytest.mark.parametrize(
    "document",
    [
        {"max_memory_chunk_size": "1024"},
        {"max_decoded_compaction_bytes": True},
        {"max_overlapping_files": 0},
        {"max_tombstone_rows": -1},
        {"tombstone_compaction_workers": 9},
        {"modified_ms": True},
        {"modified_ms": 1 << 53},
    ],
)
def test_checkpoint_rejects_table_config_runtime_cannot_use(document):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_table_config(ORG, SUP, TABLE), json.dumps(document))

    with pytest.raises(RecoveryError, match="table config"):
        _checkpoint(source, storage)


def test_checkpoint_preserves_unknown_table_config_metadata():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(
        RK.meta_table_config(ORG, SUP, TABLE),
        json.dumps({
            "max_memory_chunk_size": 1024,
            "modified_ms": 1_700_000_000_000,
            "legacy_annotation": {"owner": "analytics"},
        }),
    )

    assert _checkpoint(source, storage).snapshot_count == 1


def test_checkpoint_rejects_orphan_malformed_rbac_role_state():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.hset(
        RK.rbac_role_doc(ORG, SUP, "evil"),
        mapping={
            "role_id": "evil",
            "role": "superadmin",
            "tables": "not-json",
            "doc_version": "1",
        },
    )

    with pytest.raises(RecoveryError, match="RBAC role"):
        _checkpoint(source, storage)


def test_checkpoint_accepts_structurally_consistent_rbac_namespace():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _seed_consistent_rbac(source)

    assert _checkpoint(source, storage).snapshot_count == 1


@pytest.mark.parametrize("fault", ["role_index", "username_map", "revision"])
def test_checkpoint_rejects_inconsistent_rbac_control_indexes(fault):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    _role_id, _user_id = _seed_consistent_rbac(source)
    if fault == "role_index":
        source.delete(RK.rbac_role_index(ORG, SUP))
    elif fault == "username_map":
        source.hset(RK.rbac_username_to_id(ORG, SUP), "analyst", "other-user")
    else:
        source.delete(RK.rbac_user_meta(ORG, SUP))

    with pytest.raises(RecoveryError, match="RBAC"):
        _checkpoint(source, storage)


def test_checkpoint_rejects_schema_that_differs_from_sealed_snapshot():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.schema(ORG, SUP, TABLE), json.dumps({"id": "string"}))

    with pytest.raises(RecoveryError, match="schema differs from snapshot"):
        _checkpoint(source, storage)


@pytest.mark.parametrize(
    "document",
    [
        [],
        {"formats": "DELTA", "ts": 1},
        {"formats": ["DELTA", "DELTA"], "ts": 1},
        {"formats": ["UNKNOWN"], "ts": 1},
        {"formats": ["DELTA"], "ts": True},
    ],
)
def test_checkpoint_rejects_malformed_mirror_control_document(document):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_mirrors(ORG, SUP), json.dumps(document))

    with pytest.raises(RecoveryError, match="mirror configuration"):
        _checkpoint(source, storage)


@pytest.mark.parametrize("scope", ["namespace", "simple", "stage"])
def test_checkpoint_rejects_malformed_deletion_intent_documents(scope):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    malformed = {
        "schema_version": 1,
        "kind": "wrong-kind",
        "organization": ORG,
        "super_name": SUP,
        "intent_id": "delete-1",
        "status": "deleting",
        "created_at_ms": 1,
        "recovery_count": 0,
    }
    if scope == "namespace":
        key = RK.meta_namespace_deletion_intent(ORG, SUP)
    elif scope == "simple":
        key = RK.meta_simple_deletion_intent(ORG, SUP, TABLE)
        source.sadd(RK.meta_simple_deletion_intent_index(ORG, SUP), TABLE)
    else:
        key = RK.meta_stage_deletion_intent(ORG, SUP, "uploads")
        source.sadd(RK.meta_stage_deletion_intent_index(ORG, SUP), "uploads")
    source.set(key, json.dumps(malformed))

    with pytest.raises(RecoveryError, match="deletion intent"):
        _checkpoint(source, storage)


def test_checkpoint_rejects_divergent_complete_leaf_payload():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    # Keep the cached object structurally complete and at the sealed version so
    # normal catalog reads would trust it, but make its table contents differ
    # from the snapshot selected by ``path``.
    leaf["payload"] = dict(leaf["payload"])
    leaf["payload"]["resources"] = []
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    with pytest.raises(
        RecoveryError, match="cached payload differs from snapshot",
    ):
        _checkpoint(source, storage)


def test_checkpoint_normalizes_legacy_heavy_unrestricted_policy_marker():
    """Explicit cache null equals legacy immutable absence semantically."""
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    heavy = json.loads(storage.files[SNAPSHOT_PATH])
    heavy.pop("_row_filter")
    storage.files[SNAPSHOT_PATH] = json.dumps(heavy).encode()

    checkpoint = _checkpoint(source, storage)

    assert checkpoint.snapshot_count == 1


def test_checkpoint_rejects_cache_that_erases_heavy_share_policy():
    """A cache-null policy may not replace a filtered immutable snapshot."""
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    heavy = json.loads(storage.files[SNAPSHOT_PATH])
    heavy["_row_filter"] = "tenant_id = 7"
    storage.files[SNAPSHOT_PATH] = json.dumps(heavy).encode()

    with pytest.raises(
        RecoveryError, match="cached payload differs from snapshot",
    ):
        _checkpoint(source, storage)


def test_checkpoint_rejects_legacy_cache_without_policy_marker():
    """A restored legacy cache must never regain fast-path authority."""
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    leaf["payload"].pop("_row_filter")
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    with pytest.raises(RecoveryError, match="cached payload is incomplete"):
        _checkpoint(source, storage)


def test_checkpoint_rejects_incomplete_leaf_payload():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    # This shape is intentionally incomplete for the canonical snapshot-cache
    # contract, but historical metadata fast paths accepted it merely because
    # ``resources`` was a list.  DR must not seal storage while restoring this
    # divergent cache alongside it.
    leaf["payload"] = {
        "snapshot_version": 3,
        "schema": {"forged": "string"},
        "resources": [],
    }
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    with pytest.raises(
        RecoveryError, match="cached payload is incomplete",
    ):
        _checkpoint(source, storage)


def test_checkpoint_allows_leaf_without_optional_cache():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    leaf.pop("payload")
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    checkpoint = _checkpoint(source, storage)

    assert checkpoint.snapshot_count == 1


def test_checkpoint_allows_complete_nested_share_cache():
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)

    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    snapshot = leaf["payload"]
    leaf["payload"] = {
        "_row_filter": "tenant_id = 7",
        "snapshot": snapshot,
    }
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    checkpoint = _checkpoint(source, storage)

    assert checkpoint.snapshot_count == 1


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


def test_recovery_seals_v2_root_and_every_segment_and_detects_replacement():
    source = _redis()
    storage = MemoryStorage()
    _seed_v2_catalog(source, storage)

    checkpoint = _checkpoint(source, storage)
    checkpoint_document = json.loads(storage.files[checkpoint.path])
    artifacts = {
        item["path"]: item
        for item in checkpoint_document["snapshots"][0]["artifacts"]
    }
    assert artifacts[DV_ROOT]["kind"] == "tombstone_manifest"
    assert artifacts[DV_SEGMENT]["kind"] == "tombstone_segment"
    assert artifacts[DV_ROOT]["sha256"] == hashlib.sha256(
        storage.files[DV_ROOT]
    ).hexdigest()
    assert artifacts[DV_SEGMENT]["sha256"] == hashlib.sha256(
        storage.files[DV_SEGMENT]
    ).hexdigest()

    storage.files[DV_SEGMENT] = b"xxxxxx-v2-segment"
    assert len(storage.files[DV_SEGMENT]) == len(b"sealed-v2-segment")
    with pytest.raises(RecoveryError, match="snapshot seals differ"):
        plan_redis_rebuild(storage, ORG)


def test_checkpoint_rejects_v2_manifest_swap_between_validation_and_seal():
    class SwappingStorage(MemoryStorage):
        manifest_reads = 0
        manifest_stats = 0

        def stat_object(self, path):
            metadata = super().stat_object(path)
            if path == DV_ROOT:
                self.manifest_stats += 1
                # Return the stable post-read observation, then replace the
                # key in the TOCTOU gap before recovery's independent seal.
                if self.manifest_stats == 2:
                    self.files[path] = self.files[path].replace(
                        b'"digest":"dddddddd',
                        b'"digest":"eeeeeeee',
                        1,
                    )
            return metadata

        def read_range(self, path, offset, length, *, expected=None):
            if path == DV_ROOT:
                self.manifest_reads += 1
            current = MemoryStorage.stat_object(self, path)
            if (
                expected is not None
                and current.identity_token() != expected.identity_token()
            ):
                raise ObjectIdentityMismatch(f"object changed: {path}")
            return self.files[path][offset:offset + length]

    source = _redis()
    storage = SwappingStorage()
    _seed_v2_catalog(source, storage)

    with pytest.raises(RecoveryError, match="changed after root validation"):
        _checkpoint(source, storage)
    assert storage.manifest_reads == 2


def test_checkpoint_caps_second_manifest_seal_before_content_read():
    class GrowingStorage(MemoryStorage):
        manifest_reads = 0
        manifest_stats = 0

        def stat_object(self, path):
            metadata = super().stat_object(path)
            if path == DV_ROOT:
                self.manifest_stats += 1
                if self.manifest_stats == 2:
                    self.files[path] = b"x" * (256 * 1024 + 1)
            return metadata

        def read_range(self, path, offset, length, *, expected=None):
            if path == DV_ROOT:
                self.manifest_reads += 1
            current = MemoryStorage.stat_object(self, path)
            if (
                expected is not None
                and current.identity_token() != expected.identity_token()
            ):
                raise ObjectIdentityMismatch(f"object changed: {path}")
            return self.files[path][offset:offset + length]

    source = _redis()
    storage = GrowingStorage()
    _seed_v2_catalog(source, storage)

    with pytest.raises(RecoveryError, match="changed after root validation"):
        _checkpoint(source, storage)
    assert storage.manifest_reads == 1


@pytest.mark.parametrize(
    "mutation, error",
    [
        ("noncanonical", "canonical"),
        ("root_digest", "SHA-256"),
        ("table", "pinned table"),
        ("lineage", "immediate successor"),
        ("count", "total_rows"),
    ],
)
def test_checkpoint_rejects_invalid_v2_manifest_contract(mutation, error):
    source = _redis()
    storage = MemoryStorage()
    manifest = _seed_v2_catalog(source, storage)
    snapshot = json.loads(storage.files[SNAPSHOT_PATH])
    leaf = json.loads(source.get(RK.meta_leaf(ORG, SUP, TABLE)))
    document = manifest.to_dict()

    if mutation == "noncanonical":
        storage.files[DV_ROOT] = json.dumps(document, indent=2).encode()
    elif mutation == "root_digest":
        snapshot["tombstone_digest"] = "0" * 64
    elif mutation == "table":
        document["simple_name"] = "other"
        storage.files[DV_ROOT] = canonical_tombstone_manifest_v2_bytes(document)
        snapshot["tombstone_digest"] = hashlib.sha256(
            storage.files[DV_ROOT]
        ).hexdigest()
    elif mutation == "lineage":
        document["base_snapshot_version"] = 1
        storage.files[DV_ROOT] = canonical_tombstone_manifest_v2_bytes(document)
        snapshot["tombstone_digest"] = hashlib.sha256(
            storage.files[DV_ROOT]
        ).hexdigest()
    else:
        snapshot["tombstone_rows"] = 2

    storage.files[SNAPSHOT_PATH] = json.dumps(snapshot).encode()
    leaf["payload"] = snapshot
    source.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps(leaf))

    with pytest.raises(RecoveryError, match=error):
        _checkpoint(source, storage)


def test_recovery_round_trips_durable_v2_activation_config():
    source = _redis()
    destination = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    config = {
        "deletion_vector_format": 2,
        "dv_v2_reader_fleet_confirmed": True,
        "modified_ms": 1_700_000_000_000,
    }
    source.set(
        RK.meta_table_config(ORG, SUP, TABLE),
        json.dumps(config, sort_keys=True, separators=(",", ":")),
    )

    _checkpoint(source, storage)
    rebuild_redis(destination, storage, ORG, dry_run=False)

    restored = RedisCatalog(redis_client=destination).get_table_config(
        ORG, SUP, TABLE,
    )
    assert restored == config


@pytest.mark.parametrize(
    "config",
    [
        {"deletion_vector_format": 2},
        {"dv_v2_reader_fleet_confirmed": True},
        {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": "true",
        },
    ],
)
def test_checkpoint_rejects_ambiguous_v2_activation_config(config):
    source = _redis()
    storage = MemoryStorage()
    _seed_catalog(source, storage)
    source.set(RK.meta_table_config(ORG, SUP, TABLE), json.dumps(config))

    with pytest.raises(RecoveryError, match="invalid DV-v2 activation"):
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
