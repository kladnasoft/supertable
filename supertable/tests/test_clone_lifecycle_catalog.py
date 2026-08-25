from __future__ import annotations

import json

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable import redis_catalog as redis_catalog_module
from supertable.errors import LockLostError, SnapshotCommitConflictError
from supertable.redis_catalog import DeletionIntentConflictError, RedisCatalog


ORG = "acme"
SOURCE = "source"
TARGET = "target"
TABLE = "orders"


def _catalog() -> tuple[RedisCatalog, fakeredis.FakeStrictRedis]:
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return RedisCatalog(redis_client=client), client


def _payload(version: int) -> dict:
    return {
        "simple_name": TABLE,
        "snapshot_version": version,
        "previous_snapshot": None,
        "last_updated_ms": 10,
        "schema": {"id": "int64"},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }


def _seed_source(client, *, clone_type: str | None = None) -> None:
    root = {"version": 0, "ts": 1}
    if clone_type is not None:
        root.update({
            "read_only": clone_type != "writable",
            "clone_type": clone_type,
            "cloned_from": "upstream",
            "clone_ts": 1,
        })
    client.set(RK.meta_root(ORG, SOURCE), json.dumps(root))


def _seed_clone(client, *, clone_type: str = "readonly") -> None:
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": clone_type != "writable",
            "clone_type": clone_type,
            "cloned_from": SOURCE,
            "clone_ts": 1,
        }),
    )


def _leases(
    client,
    namespace: str = "namespace",
    leaf: str = "leaf",
    source_namespace: str = "source-namespace",
) -> None:
    client.set(RK.lock_namespace(ORG, TARGET), namespace)
    client.set(RK.lock_leaf(ORG, TARGET, TABLE), leaf)
    client.set(RK.lock_namespace(ORG, SOURCE), source_namespace)


def test_writable_clone_root_is_valid_and_explicitly_writable() -> None:
    catalog, client = _catalog()
    _seed_clone(client, clone_type="writable")

    root = catalog.get_root(ORG, TARGET)

    assert root["clone_type"] == "writable"
    assert root["read_only"] is False


def test_clone_root_initialization_is_atomic_and_exactly_lease_fenced() -> None:
    catalog, client = _catalog()
    _seed_source(client)
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    client.set(RK.lock_namespace(ORG, TARGET), "current-owner")
    flags = {
        "read_only": True,
        "clone_type": "readonly",
        "cloned_from": SOURCE,
        "clone_ts": 1,
        "clone_state": "creating",
        "clone_operation_id": "operation",
        "clone_source_owners": [SOURCE],
    }

    catalog.ensure_root(
        ORG,
        TARGET,
        namespace_token="current-owner",
        source_namespace_token="source-owner",
        initial_flags=flags,
    )

    root = catalog.get_root(ORG, TARGET)
    assert {key: root[key] for key in flags} == flags

    other = "other"
    with pytest.raises(LockLostError, match="initialization lock"):
        catalog.ensure_root(
            ORG,
            other,
            namespace_token="expired-owner",
            source_namespace_token="source-owner",
            initial_flags={
                **flags,
                "cloned_from": SOURCE,
                "clone_source_owners": [SOURCE],
            },
        )
    assert not client.exists(RK.meta_root(ORG, other))


def test_clone_root_initialization_rejects_fenced_or_changed_source() -> None:
    catalog, client = _catalog()
    _seed_source(client)
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    client.set(RK.lock_namespace(ORG, TARGET), "target-owner")
    client.set(
        RK.meta_namespace_deletion_intent(ORG, SOURCE),
        json.dumps({"intent_id": "delete-source"}),
    )

    with pytest.raises(DeletionIntentConflictError, match="source is fenced"):
        catalog.ensure_root(
            ORG,
            TARGET,
            namespace_token="target-owner",
            source_namespace_token="source-owner",
            initial_flags={
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": SOURCE,
                "clone_ts": 1,
            },
        )
    assert not client.exists(RK.meta_root(ORG, TARGET))


def test_clone_root_initialization_rejects_different_existing_binding() -> None:
    catalog, client = _catalog()
    _seed_source(client)
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    client.set(RK.lock_namespace(ORG, TARGET), "target-owner")
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 4,
            "ts": 9,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": SOURCE,
            "clone_ts": 1,
            "clone_operation_id": "other-operation",
        }),
    )

    with pytest.raises(SnapshotCommitConflictError, match="binding differs"):
        catalog.ensure_root(
            ORG,
            TARGET,
            namespace_token="target-owner",
            source_namespace_token="source-owner",
            initial_flags={
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": SOURCE,
                "clone_ts": 1,
                "clone_operation_id": "expected-operation",
            },
        )


def test_chained_clone_initialization_atomically_fences_every_owner(
    monkeypatch,
) -> None:
    catalog, client = _catalog()
    middle = "middle"
    other = "other"
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, middle),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": SOURCE,
            "clone_ts": 1,
            "clone_source_owners": [SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    client.set(RK.lock_namespace(ORG, TARGET), "target-owner")
    client.set(RK.lock_namespace(ORG, other), "other-owner")
    flags = {
        "read_only": True,
        "clone_type": "readonly",
        "cloned_from": middle,
        "clone_ts": 1,
        "clone_source_owners": [middle, SOURCE],
    }

    with pytest.raises(LockLostError, match=SOURCE):
        catalog.ensure_root(
            ORG,
            TARGET,
            namespace_token="target-owner",
            source_namespace_token="middle-owner",
            initial_flags=flags,
        )
    assert not client.exists(RK.meta_root(ORG, TARGET))

    script = catalog._root_ensure

    def source_deleter_takes_lease(*, keys, args):
        client.set(RK.lock_namespace(ORG, SOURCE), "source-deleter")
        return script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_root_ensure", source_deleter_takes_lease)
    with pytest.raises(LockLostError, match="source namespace lease"):
        catalog.ensure_root(
            ORG,
            other,
            namespace_token="other-owner",
            source_namespace_token="middle-owner",
            source_namespace_tokens={SOURCE: "source-owner"},
            initial_flags=flags,
        )
    assert not client.exists(RK.meta_root(ORG, other))

    monkeypatch.setattr(catalog, "_root_ensure", script)
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    catalog.ensure_root(
        ORG,
        TARGET,
        namespace_token="target-owner",
        source_namespace_token="middle-owner",
        source_namespace_tokens={SOURCE: "source-owner"},
        initial_flags=flags,
    )
    assert catalog.get_root(ORG, TARGET)["clone_source_owners"] == [
        middle,
        SOURCE,
    ]


def test_clone_initialization_rejects_incoherent_flattened_owner_lineage() -> None:
    catalog, client = _catalog()
    middle = "middle"
    # SOURCE is itself a legacy clone of ``upstream``.  A modern middle root
    # that declares only SOURCE has omitted that indirect owner and therefore
    # cannot safely authorize another clone or its future artifact pointers.
    _seed_source(client, clone_type="readonly")
    client.set(
        RK.meta_root(ORG, middle),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": SOURCE,
            "clone_ts": 1,
            "clone_source_owners": [SOURCE],
        }),
    )

    with pytest.raises(
        SnapshotCommitConflictError,
        match="owner lineage is inconsistent",
    ):
        catalog.ensure_root(
            ORG,
            TARGET,
            namespace_token="target-owner",
            source_namespace_token="middle-owner",
            source_namespace_tokens={SOURCE: "source-owner"},
            initial_flags={
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": middle,
                "clone_ts": 1,
                "clone_source_owners": [middle, SOURCE],
            },
        )

    assert not client.exists(RK.meta_root(ORG, TARGET))


def test_clone_commit_initializes_readonly_leaf_under_both_exact_leases() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    path = f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json"

    assert catalog.commit_clone_snapshot(
        ORG,
        TARGET,
        TABLE,
        _payload(0),
        path,
        source_super=SOURCE,
        expected_version=-1,
        expected_path="",
        namespace_token="namespace",
        source_namespace_token="source-namespace",
        lock_token="leaf",
        now_ms=10,
        commit_id="clone-operation",
    ) == 0

    leaf = catalog._get_leaf_raw(ORG, TARGET, TABLE)
    assert leaf["path"] == path
    assert leaf["payload"]["snapshot_version"] == 0
    assert json.loads(client.get(RK.schema(ORG, TARGET, TABLE))) == {"id": "int64"}
    assert catalog.get_root(ORG, TARGET)["version"] == 1


def test_clone_snapshot_schema_and_identity_exact_boundaries() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    payload = _payload(0)
    empty_schema = json.dumps({"wide": ""}, separators=(",", ":"))
    payload["schema"] = {
        "wide": "x" * (1024 * 1024 - len(empty_schema)),
    }
    prefix = f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/"
    path = prefix + "x" * (4096 - len(prefix) - len(".json")) + ".json"

    assert catalog.commit_clone_snapshot(
        ORG,
        TARGET,
        TABLE,
        payload,
        path,
        source_super=SOURCE,
        expected_version=-1,
        expected_path="",
        namespace_token="namespace",
        source_namespace_token="source-namespace",
        lock_token="leaf",
        commit_id="c" * 4096,
    ) == 0
    assert len(client.get(RK.schema(ORG, TARGET, TABLE)).encode("utf-8")) == (
        1024 * 1024
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema", {"wide": "x" * (1024 * 1024)}, "schema exceeds"),
        ("resources", [None] * 100_001, "resource count"),
    ],
)
def test_clone_snapshot_rejects_metadata_over_limits_without_mutation(
    field: str,
    value: object,
    message: str,
) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    payload = _payload(0)
    payload[field] = value
    before_root = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(ValueError, match=message):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before_root
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))
    assert not client.exists(RK.schema(ORG, TARGET, TABLE))


@pytest.mark.parametrize("injection", ["schema", "resources"])
def test_clone_snapshot_lua_repeats_metadata_guards_without_mutation(
    injection: str,
    monkeypatch,
) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    script = catalog._commit_clone_snapshot

    def inject_oversized_metadata(*, keys, args):
        forwarded = list(args)
        if injection == "schema":
            forwarded[12] = json.dumps({"wide": "x" * (1024 * 1024)})
        else:
            injected_payload = _payload(0)
            injected_payload["resources"] = [None] * 100_001
            forwarded[2] = json.dumps(injected_payload)
        return script(keys=keys, args=forwarded)

    monkeypatch.setattr(
        catalog, "_commit_clone_snapshot", inject_oversized_metadata,
    )
    before_root = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(ValueError, match="byte/count safety limits"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before_root
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


@pytest.mark.parametrize("malformed_owner", ["target", "source"])
@pytest.mark.parametrize(
    "malformed_suffix",
    [',"version":0', ',"padding":NaN'],
)
def test_clone_snapshot_rejects_ambiguous_or_nonfinite_root_without_mutation(
    malformed_owner: str,
    malformed_suffix: str,
) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    key = RK.meta_root(ORG, TARGET if malformed_owner == "target" else SOURCE)
    document = json.loads(client.get(key))
    raw = json.dumps(document, separators=(",", ":"))
    client.set(key, raw[:-1] + malformed_suffix + "}")
    before_target = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before_target
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_snapshot_rejects_root_growth_over_one_mib_without_mutation() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    root = json.loads(client.get(RK.meta_root(ORG, TARGET)))
    root["padding"] = ""
    compact = json.dumps(root, separators=(",", ":"))
    root["padding"] = "x" * (1024 * 1024 - len(compact))
    raw_root = json.dumps(root, separators=(",", ":"))
    assert len(raw_root.encode("utf-8")) == 1024 * 1024
    client.set(RK.meta_root(ORG, TARGET), raw_root)

    with pytest.raises(ValueError, match="Clone root exceeds its size limit"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
            commit_id="root-growth",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == raw_root
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


@pytest.mark.parametrize("predecessor", ["snapshots/forged.json", "missing"])
def test_initial_clone_commit_requires_explicit_null_predecessor(
    predecessor: str,
) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    payload = _payload(0)
    if predecessor == "missing":
        payload.pop("previous_snapshot")
    else:
        payload["previous_snapshot"] = predecessor

    with pytest.raises(ValueError, match="payload is invalid"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "resources",
            [{"file": f"{ORG}/unfenced/tables/{TABLE}/data/part.parquet"}],
        ),
        ("stats_file", f"{ORG}/unfenced/tables/{TABLE}/stats/stats.parquet"),
        ("tombstone", f"{ORG}/unfenced/tables/{TABLE}/tombstone/dv.parquet"),
    ],
)
def test_clone_commit_rejects_artifacts_from_unfenced_namespaces(
    field: str,
    value: object,
) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    payload = _payload(0)
    payload[field] = value
    if field == "tombstone":
        payload["tombstone_rows"] = 1
        payload["tombstone_digest"] = "a" * 64

    with pytest.raises(ValueError, match="unfenced namespace"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_commit_accepts_data_artifact_from_persisted_source_owner() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    payload = _payload(0)
    payload["resources"] = [{
        "file": f"{ORG}/{SOURCE}/tables/{TABLE}/data/part.parquet",
    }]

    assert catalog.commit_clone_snapshot(
        ORG,
        TARGET,
        TABLE,
        payload,
        f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
        source_super=SOURCE,
        expected_version=-1,
        expected_path="",
        namespace_token="namespace",
        source_namespace_token="source-namespace",
        lock_token="leaf",
    ) == 0


def test_root_flag_update_rejects_oversized_documents_without_mutation() -> None:
    catalog, client = _catalog()
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({"version": 0, "ts": 1}),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "namespace")
    before = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(ValueError, match="too large|size limit"):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"operator_note": "x" * (1024 * 1024)},
            namespace_token="namespace",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before


def test_clone_commit_preserves_indirect_owner_binding() -> None:
    catalog, client = _catalog()
    middle = "middle"
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, middle),
        json.dumps({"version": 0, "ts": 1}),
    )
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "namespace")
    client.set(RK.lock_leaf(ORG, TARGET, TABLE), "leaf")
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")

    assert catalog.commit_clone_snapshot(
        ORG,
        TARGET,
        TABLE,
        _payload(0),
        f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
        source_super=middle,
        expected_version=-1,
        expected_path="",
        namespace_token="namespace",
        source_namespace_token="middle-owner",
        source_namespace_tokens={SOURCE: "source-owner"},
        lock_token="leaf",
    ) == 0

    assert catalog.get_root(ORG, TARGET)["clone_source_owners"] == [
        middle,
        SOURCE,
    ]


def test_clone_commit_requires_every_inherited_owner_lease_without_mutation() -> None:
    catalog, client = _catalog()
    middle = "middle"
    _seed_source(client)
    client.set(RK.meta_root(ORG, middle), json.dumps({"version": 0, "ts": 1}))
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "namespace")
    client.set(RK.lock_leaf(ORG, TARGET, TABLE), "leaf")
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    before = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(LockLostError, match=SOURCE):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=middle,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="middle-owner",
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_commit_atomically_rejects_inherited_owner_deletion(
    monkeypatch,
) -> None:
    catalog, client = _catalog()
    middle = "middle"
    _seed_source(client)
    client.set(RK.meta_root(ORG, middle), json.dumps({"version": 0, "ts": 1}))
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "namespace")
    client.set(RK.lock_leaf(ORG, TARGET, TABLE), "leaf")
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    payload = _payload(0)
    payload["resources"] = [{
        "file": f"{ORG}/{SOURCE}/tables/{TABLE}/data/part.parquet",
    }]
    before = client.get(RK.meta_root(ORG, TARGET))
    script = catalog._commit_clone_snapshot

    def begin_inherited_owner_deletion(*, keys, args):
        client.set(
            RK.meta_namespace_deletion_intent(ORG, SOURCE),
            json.dumps({"intent_id": "delete-source"}),
        )
        return script(keys=keys, args=args)

    monkeypatch.setattr(
        catalog,
        "_commit_clone_snapshot",
        begin_inherited_owner_deletion,
    )
    with pytest.raises(DeletionIntentConflictError, match="clone source"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=middle,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="middle-owner",
            source_namespace_tokens={SOURCE: "source-owner"},
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_commit_rejects_stale_namespace_lease_without_mutation() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client, namespace="new-owner")

    with pytest.raises(LockLostError):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="stale-owner",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_successor_requires_exact_leaf_path_and_version() -> None:
    catalog, client = _catalog()
    _seed_clone(client, clone_type="writable")
    _leases(client)
    old_path = f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/old.json"
    client.set(
        RK.meta_leaf(ORG, TARGET, TABLE),
        json.dumps({
            "version": 3,
            "ts": 3,
            "path": old_path,
            "payload": _payload(3),
        }),
    )

    payload = _payload(4)
    payload["previous_snapshot"] = (
        f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/wrong.json"
    )
    with pytest.raises(SnapshotCommitConflictError):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/new.json",
            source_super=SOURCE,
            expected_version=3,
            expected_path=f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/wrong.json",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert json.loads(client.get(RK.meta_leaf(ORG, TARGET, TABLE)))["path"] == old_path


def test_clone_successor_requires_payload_to_name_exact_predecessor() -> None:
    catalog, client = _catalog()
    _seed_clone(client, clone_type="writable")
    _leases(client)
    old_path = f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/old.json"
    client.set(
        RK.meta_leaf(ORG, TARGET, TABLE),
        json.dumps({
            "version": 3,
            "ts": 3,
            "path": old_path,
            "payload": _payload(3),
        }),
    )
    payload = _payload(4)
    payload["previous_snapshot"] = (
        f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/forged.json"
    )

    with pytest.raises(ValueError, match="payload is invalid"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            payload,
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/new.json",
            source_super=SOURCE,
            expected_version=3,
            expected_path=old_path,
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert json.loads(client.get(RK.meta_leaf(ORG, TARGET, TABLE)))["path"] == old_path


def test_clone_commit_rejects_unbound_writable_target() -> None:
    catalog, client = _catalog()
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({"version": 0, "ts": 1}),
    )
    _leases(client)

    with pytest.raises(SnapshotCommitConflictError, match="target binding"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )

    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_commit_rechecks_exact_source_root_inside_lua(monkeypatch) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    script = catalog._commit_clone_snapshot

    def mutate_source_then_commit(*, keys, args):
        client.set(
            RK.meta_root(ORG, SOURCE),
            json.dumps({"version": 1, "ts": 2}),
        )
        return script(keys=keys, args=args)

    monkeypatch.setattr(
        catalog, "_commit_clone_snapshot", mutate_source_then_commit,
    )
    with pytest.raises(SnapshotCommitConflictError, match="source changed"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="source-namespace",
            lock_token="leaf",
        )
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_clone_lifecycle_flags_require_live_unfenced_exact_source() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)

    assert catalog.update_root_flags(
        ORG,
        TARGET,
        {"clone_ts": 2},
        namespace_token="namespace",
        source_namespace_token="source-namespace",
    )
    before = client.get(RK.meta_root(ORG, TARGET))
    client.set(
        RK.meta_namespace_deletion_intent(ORG, SOURCE),
        json.dumps({"intent_id": "delete-source"}),
    )
    with pytest.raises(DeletionIntentConflictError, match="source is fenced"):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"clone_ts": 3},
            namespace_token="namespace",
            source_namespace_token="source-namespace",
        )
    assert client.get(RK.meta_root(ORG, TARGET)) == before


def test_clone_lifecycle_flags_recheck_source_root_inside_lua(monkeypatch) -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    script = catalog._update_root_flags
    before = client.get(RK.meta_root(ORG, TARGET))

    def mutate_source_then_update(*, keys, args):
        client.set(
            RK.meta_root(ORG, SOURCE),
            json.dumps({"version": 1, "ts": 2}),
        )
        return script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_update_root_flags", mutate_source_then_update)
    with pytest.raises(SnapshotCommitConflictError, match="source changed"):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"clone_ts": 2},
            namespace_token="namespace",
            source_namespace_token="source-namespace",
        )
    assert client.get(RK.meta_root(ORG, TARGET)) == before


def test_clone_lifecycle_flags_cannot_replace_source_binding() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    before = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(SnapshotCommitConflictError, match="cannot be replaced"):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"cloned_from": "different-source"},
            namespace_token="namespace",
            source_namespace_token="source-namespace",
        )
    assert client.get(RK.meta_root(ORG, TARGET)) == before


def test_generic_root_flags_cannot_remove_an_indirect_owner() -> None:
    catalog, client = _catalog()
    middle = "middle"
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, middle), json.dumps({"version": 0, "ts": 1}),
    )
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "target-owner")
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    client.set(RK.lock_namespace(ORG, SOURCE), "source-deleter")
    before = client.get(RK.meta_root(ORG, TARGET))
    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="source-deleter",
    ) == [TARGET]

    with pytest.raises(
        SnapshotCommitConflictError,
        match="ownership transitions require",
    ):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"clone_source_owners": [middle]},
            namespace_token="target-owner",
            source_namespace_token="middle-owner",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before
    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="source-deleter",
    ) == [TARGET]


def test_clone_owner_transition_rejects_aggregate_documents_before_lua(
    monkeypatch,
) -> None:
    catalog, client = _catalog()
    source_a = "source-a"
    source_b = "source-b"
    padding = "x" * 700
    for source in (source_a, source_b):
        client.set(
            RK.meta_root(ORG, source),
            json.dumps({"version": 0, "ts": 1, "padding": padding}),
        )
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": source_a,
            "clone_ts": 1,
            "clone_source_owners": [source_a],
        }),
    )
    monkeypatch.setattr(
        redis_catalog_module,
        "_MAX_CLONE_OWNER_DOCUMENT_BYTES",
        1_000,
    )
    monkeypatch.setattr(
        catalog,
        "_transition_clone_owners",
        lambda **_kwargs: pytest.fail("oversized owner documents reached Lua"),
    )

    with pytest.raises(ValueError, match="owner documents are too large"):
        catalog.transition_clone_owners(
            ORG,
            TARGET,
            {
                "cloned_from": source_b,
                "clone_source_owners": [source_b],
            },
            namespace_token="target-owner",
            source_namespace_tokens={
                source_a: "source-a-owner",
                source_b: "source-b-owner",
            },
        )


def test_owner_transition_rechecks_every_indirect_owner_lease(
    monkeypatch,
) -> None:
    catalog, client = _catalog()
    middle = "middle"
    _seed_source(client)
    client.set(
        RK.meta_root(ORG, middle), json.dumps({"version": 0, "ts": 1}),
    )
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 0,
            "ts": 1,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )
    client.set(RK.lock_namespace(ORG, TARGET), "target-owner")
    client.set(RK.lock_namespace(ORG, middle), "middle-owner")
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    flags = {
        "read_only": False,
        "clone_type": None,
        "cloned_from": None,
        "clone_ts": None,
        "clone_source_owners": None,
    }
    before = client.get(RK.meta_root(ORG, TARGET))

    with pytest.raises(LockLostError, match=SOURCE):
        catalog.transition_clone_owners(
            ORG,
            TARGET,
            flags,
            namespace_token="target-owner",
            source_namespace_tokens={middle: "middle-owner"},
        )

    script = catalog._transition_clone_owners

    def source_deleter_takes_lease(*, keys, args):
        client.set(RK.lock_namespace(ORG, SOURCE), "source-deleter")
        return script(keys=keys, args=args)

    monkeypatch.setattr(
        catalog, "_transition_clone_owners", source_deleter_takes_lease,
    )
    with pytest.raises(LockLostError, match="source namespace lease"):
        catalog.transition_clone_owners(
            ORG,
            TARGET,
            flags,
            namespace_token="target-owner",
            source_namespace_tokens={
                middle: "middle-owner",
                SOURCE: "source-owner",
            },
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before

    monkeypatch.setattr(catalog, "_transition_clone_owners", script)
    client.set(RK.lock_namespace(ORG, SOURCE), "source-owner")
    assert catalog.transition_clone_owners(
        ORG,
        TARGET,
        flags,
        namespace_token="target-owner",
        source_namespace_tokens={
            middle: "middle-owner",
            SOURCE: "source-owner",
        },
    )
    root = catalog.get_root(ORG, TARGET)
    assert root["cloned_from"] is None
    assert root["clone_source_owners"] is None


def test_clone_catalog_apis_cannot_hitchhike_current_source_lease() -> None:
    catalog, client = _catalog()
    _seed_clone(client)
    _leases(client)
    before = client.get(RK.meta_root(ORG, TARGET))
    initial_flags = {
        "read_only": True,
        "clone_type": "readonly",
        "cloned_from": SOURCE,
        "clone_ts": 1,
    }

    with pytest.raises(LockLostError, match="Source namespace lease is required"):
        catalog.ensure_root(
            ORG,
            TARGET,
            namespace_token="namespace",
            initial_flags=initial_flags,
        )
    with pytest.raises(LockLostError, match="source lease"):
        catalog.update_root_flags(
            ORG,
            TARGET,
            {"clone_ts": 2},
            namespace_token="namespace",
            source_namespace_token="not-the-owner",
        )
    with pytest.raises(LockLostError, match="source namespace lease"):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(0),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/clone.json",
            source_super=SOURCE,
            expected_version=-1,
            expected_path="",
            namespace_token="namespace",
            source_namespace_token="not-the-owner",
            lock_token="leaf",
        )

    assert client.get(RK.meta_root(ORG, TARGET)) == before
    assert not client.exists(RK.meta_leaf(ORG, TARGET, TABLE))


def test_strict_clone_discovery_fails_closed_and_finds_writable_clone() -> None:
    catalog, client = _catalog()
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    client.set(
        RK.meta_root(ORG, SOURCE), json.dumps({"version": 0, "ts": 1}),
    )
    _seed_clone(client, clone_type="writable")

    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="owner",
    ) == [TARGET]

    client.set(RK.meta_root(ORG, "corrupt"), "not-json")
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.find_clones_strict(ORG, SOURCE, namespace_token="owner")


def test_strict_clone_discovery_retains_indirect_owner_after_parent_detach() -> None:
    catalog, client = _catalog()
    middle = "middle"
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    client.set(
        RK.meta_root(ORG, SOURCE), json.dumps({"version": 0, "ts": 1}),
    )
    # The direct parent is now independent, but TARGET's immutable snapshots
    # can still reference SOURCE from before that detach.
    client.set(
        RK.meta_root(ORG, middle), json.dumps({"version": 1, "ts": 2}),
    )
    client.set(
        RK.meta_root(ORG, TARGET),
        json.dumps({
            "version": 1,
            "ts": 2,
            "read_only": True,
            "clone_type": "readonly",
            "cloned_from": middle,
            "clone_ts": 1,
            "clone_source_owners": [middle, SOURCE],
        }),
    )

    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="owner",
    ) == [TARGET]


def test_strict_clone_discovery_limit_counts_dependencies_not_roots() -> None:
    catalog, client = _catalog()
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    _seed_source(client)
    for index in range(20):
        client.set(
            RK.meta_root(ORG, f"unrelated-{index}"),
            json.dumps({"version": index, "ts": index + 1}),
        )
    _seed_clone(client, clone_type="writable")

    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="owner", maximum=1,
    ) == [TARGET]


def test_strict_clone_discovery_batches_root_reads(monkeypatch) -> None:
    catalog, client = _catalog()
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    _seed_source(client)
    for index in range(600):
        client.set(
            RK.meta_root(ORG, f"unrelated-{index}"),
            json.dumps({"version": index, "ts": index + 1}),
        )
    _seed_clone(client, clone_type="writable")
    original_get = client.get
    original_mget = client.mget
    root_keys = {
        RK.meta_root(ORG, SOURCE),
        RK.meta_root(ORG, TARGET),
        *(RK.meta_root(ORG, f"unrelated-{index}") for index in range(600)),
    }
    sequential_root_reads: list[str] = []
    mget_calls = 0

    def tracked_get(key):
        key_text = key if isinstance(key, str) else key.decode("utf-8")
        if key_text in root_keys:
            sequential_root_reads.append(key_text)
        return original_get(key)

    def tracked_mget(keys, *args):
        nonlocal mget_calls
        mget_calls += 1
        return original_mget(keys, *args)

    monkeypatch.setattr(client, "get", tracked_get)
    monkeypatch.setattr(client, "mget", tracked_mget)

    assert catalog.find_clones_strict(
        ORG, SOURCE, namespace_token="owner",
    ) == [TARGET]
    assert sequential_root_reads == []
    assert 0 < mget_calls < 20


def test_strict_clone_discovery_caps_unrelated_roots(monkeypatch) -> None:
    catalog, client = _catalog()
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    _seed_source(client)
    for index in range(4):
        client.set(
            RK.meta_root(ORG, f"unrelated-{index}"),
            json.dumps({"version": index, "ts": index + 1}),
        )
    monkeypatch.setattr(
        redis_catalog_module,
        "_MAX_CLONE_DISCOVERY_INSPECTED_ROOTS",
        3,
    )

    with pytest.raises(RuntimeError, match="inspected-root bound"):
        catalog.find_clones_strict(
            ORG, SOURCE, namespace_token="owner", maximum=1,
        )


def test_strict_clone_discovery_caps_repeated_scan_pages(monkeypatch) -> None:
    catalog, client = _catalog()
    client.set(RK.lock_namespace(ORG, SOURCE), "owner")
    _seed_source(client)
    calls = 0

    def repeat_source_page(*, cursor, match, count):
        nonlocal calls
        calls += 1
        return 1, [RK.meta_root(ORG, SOURCE)]

    monkeypatch.setattr(client, "scan", repeat_source_page)
    monkeypatch.setattr(
        redis_catalog_module,
        "_MAX_CLONE_DISCOVERY_SCAN_CALLS",
        2,
    )

    with pytest.raises(RuntimeError, match="call bound"):
        catalog.find_clones_strict(
            ORG, SOURCE, namespace_token="owner", maximum=1,
        )
    assert calls == 2
