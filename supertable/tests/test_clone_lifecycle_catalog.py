from __future__ import annotations

import json

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.errors import LockLostError, SnapshotCommitConflictError
from supertable.redis_catalog import RedisCatalog


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


def _seed_clone(client, *, clone_type: str = "readonly") -> None:
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


def _leases(client, namespace: str = "namespace", leaf: str = "leaf") -> None:
    client.set(RK.lock_namespace(ORG, TARGET), namespace)
    client.set(RK.lock_leaf(ORG, TARGET, TABLE), leaf)


def test_writable_clone_root_is_valid_and_explicitly_writable() -> None:
    catalog, client = _catalog()
    _seed_clone(client, clone_type="writable")

    root = catalog.get_root(ORG, TARGET)

    assert root["clone_type"] == "writable"
    assert root["read_only"] is False


def test_clone_root_initialization_is_atomic_and_exactly_lease_fenced() -> None:
    catalog, client = _catalog()
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
            initial_flags={
                **flags,
                "cloned_from": SOURCE,
                "clone_source_owners": [SOURCE],
            },
        )
    assert not client.exists(RK.meta_root(ORG, other))


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
        lock_token="leaf",
        now_ms=10,
        commit_id="clone-operation",
    ) == 0

    leaf = catalog._get_leaf_raw(ORG, TARGET, TABLE)
    assert leaf["path"] == path
    assert leaf["payload"]["snapshot_version"] == 0
    assert json.loads(client.get(RK.schema(ORG, TARGET, TABLE))) == {"id": "int64"}
    assert catalog.get_root(ORG, TARGET)["version"] == 1


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

    with pytest.raises(SnapshotCommitConflictError):
        catalog.commit_clone_snapshot(
            ORG,
            TARGET,
            TABLE,
            _payload(4),
            f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/new.json",
            source_super=SOURCE,
            expected_version=3,
            expected_path=f"{ORG}/{TARGET}/tables/{TABLE}/snapshots/wrong.json",
            namespace_token="namespace",
            lock_token="leaf",
        )

    assert json.loads(client.get(RK.meta_leaf(ORG, TARGET, TABLE)))["path"] == old_path


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
