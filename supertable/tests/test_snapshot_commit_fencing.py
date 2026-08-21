import hashlib
import json
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

from supertable import redis_keys as RK
from supertable.data_writer import DataWriter
from supertable.errors import LockLostError, SnapshotCommitConflictError
from supertable.redis_catalog import (
    DeletionIntentConflictError,
    ReadOnlyCatalogError,
    RedisCatalog,
)
from supertable.tombstone_manifest_v2 import MAX_JSON_EXACT_INTEGER
from supertable.storage.local_storage import LocalStorage


def _catalog():
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    connector = SimpleNamespace(r=fake)
    with patch("supertable.redis_catalog.RedisConnector", return_value=connector):
        catalog = RedisCatalog()
    return catalog, fake


def _seed(fake, *, token="token", version=4, path="snap/4.json"):
    fake.set(
        RK.meta_leaf("org", "lake", "table"),
        json.dumps({
            "version": version,
            "ts": 1,
            "path": path,
            "payload": {"resources": [], "_row_filter": None},
        }),
    )
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), token, ex=30)


def _seed_current_snapshot(
        fake, *, floor=100, token="token", version=4,
):
    _seed(fake, token=token, version=version)
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    leaf["payload"] = {
        "snapshot_version": version,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": floor,
        "_row_filter": None,
    }
    fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))


def _snapshot_payload(**updates):
    payload = {
        "snapshot_version": 5,
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    payload.update(updates)
    return payload


def _explicit_v2_payload(snapshot_version, *, active):
    updates = {
        "snapshot_version": snapshot_version,
        "tombstone_format": 2,
    }
    if active:
        updates.update({
            "tombstone": (
                "org/lake/tables/table/tombstone/generation/root.json"
            ),
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        })
    return _snapshot_payload(**updates)


def test_snapshot_commit_atomically_updates_leaf_and_root():
    catalog, fake = _catalog()
    _seed(fake)
    assert catalog.commit_snapshot(
        "org", "lake", "table",
        _snapshot_payload(resources=[{"file": "f"}]), "snap/5.json",
        expected_version=4, expected_path="snap/4.json", lock_token="token",
        commit_id="commit-5", now_ms=123,
    ) == (5, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    root = json.loads(fake.get(RK.meta_root("org", "lake")))
    assert leaf == {
        "version": 5,
        "ts": 123,
        "path": "snap/5.json",
        "payload": {
            "snapshot_version": 5,
            "resources": [{"file": "f"}],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "_row_filter": None,
        },
        "commit_id": "commit-5",
    }
    assert root["version"] == 10
    assert root["commit_id"] == "commit-5"
    assert root["read_only"] is False


def test_normal_snapshot_commit_does_not_hash_or_persist_payload_digest(
    monkeypatch,
):
    catalog, fake = _catalog()
    _seed(fake)
    monkeypatch.setattr(
        hashlib,
        "sha256",
        MagicMock(side_effect=AssertionError("normal payload was hashed")),
    )

    assert catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
    ) == (5, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert "payload_digest" not in leaf


@pytest.mark.parametrize(
    "changes",
    [
        {"tombstone_rows": 1},
        {"tombstone": "org/lake/tables/table/tombstone/deleted.parquet"},
        {
            "tombstone": "org/lake/tables/table/tombstone/root.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/other/tombstone/root.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/table/tombstone/root.json",
            "tombstone_rows": 10**14,
            "tombstone_digest": "0" * 64,
        },
    ],
)
def test_snapshot_commit_rejects_invalid_tombstone_state_before_mutation(changes):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    before_leaf = fake.get(leaf_key)
    before_root = fake.get(root_key)

    with pytest.raises(
        ValueError, match="deletion-vector state|namespace|row.?count",
    ):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(**changes),
            "snap/5.json", expected_version=4,
            expected_path="snap/4.json", lock_token="token",
        )

    assert fake.get(leaf_key) == before_leaf
    assert fake.get(root_key) == before_root
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(RK.meta_table_names("org", "lake"))


@pytest.mark.parametrize(
    "missing_field", ["tombstone", "tombstone_rows", "tombstone_digest"],
)
def test_snapshot_commit_requires_explicit_tombstone_fields(missing_field):
    catalog, fake = _catalog()
    _seed(fake)
    payload = _snapshot_payload()
    del payload[missing_field]
    before = (
        fake.get(RK.meta_leaf("org", "lake", "table")),
        fake.get(RK.meta_root("org", "lake")),
    )

    with pytest.raises(ValueError, match="explicit deletion-vector state"):
        catalog.commit_snapshot(
            "org", "lake", "table", payload, "snap/5.json",
            expected_version=4, expected_path="snap/4.json",
            lock_token="token",
        )

    assert before == (
        fake.get(RK.meta_leaf("org", "lake", "table")),
        fake.get(RK.meta_root("org", "lake")),
    )


@pytest.mark.parametrize("snapshot_version", [None, True, 4, 5.0, 6])
def test_snapshot_commit_rejects_non_successor_payload_version_without_mutation(
        snapshot_version,
):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    before = (fake.get(leaf_key), fake.get(root_key))

    with pytest.raises(ValueError, match="exact successor"):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _snapshot_payload(snapshot_version=snapshot_version),
            "snap/5.json", expected_version=4,
            expected_path="snap/4.json", lock_token="token",
        )

    assert (fake.get(leaf_key), fake.get(root_key)) == before
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(RK.meta_table_names("org", "lake"))


@pytest.mark.parametrize("fast_path", [False, True])
@pytest.mark.parametrize(
    "lua_rejection", ["foreign-v2", "wrong-version", "oversize-v2"],
)
def test_both_snapshot_commit_lua_paths_repeat_payload_guards(
        monkeypatch, fast_path, lua_rejection,
):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    before = (fake.get(leaf_key), fake.get(root_key))
    payload = _snapshot_payload()
    if lua_rejection == "foreign-v2":
        payload.update({
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/other/tombstone/root.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        })
        # Bypass only the Python tombstone gate to exercise the Lua boundary.
        monkeypatch.setattr(
            "supertable.redis_catalog.validate_snapshot_tombstone_state",
            lambda *_args, **_kwargs: 1,
        )
    elif lua_rejection == "wrong-version":
        # The public argument passes Python's version check; corrupt the cache
        # serialization result to prove Lua independently binds the payload.
        monkeypatch.setattr(
            "supertable.redis_catalog.snapshot_cache_payload",
            lambda value: {**value, "snapshot_version": 99},
        )
    else:
        payload.update({
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/table/tombstone/root.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        })
        monkeypatch.setattr(
            "supertable.redis_catalog.snapshot_cache_payload",
            lambda value: {**value, "tombstone_rows": 10**14},
        )

    kwargs = {
        "expected_mirrors": [],
    }
    if fast_path:
        kwargs["expected_mirror_pin"] = None
    with pytest.raises(ValueError, match="invalid snapshot payload"):
        catalog.commit_snapshot(
            "org", "lake", "table", payload, "snap/5.json",
            expected_version=4, expected_path="snap/4.json",
            lock_token="token", **kwargs,
        )

    assert (fake.get(leaf_key), fake.get(root_key)) == before
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(RK.meta_table_names("org", "lake"))


@pytest.mark.parametrize("fast_path", [False, True])
def test_both_snapshot_commit_paths_accept_table_bound_v2_manifest(fast_path):
    catalog, fake = _catalog()
    _seed(fake)
    payload = _snapshot_payload(
        tombstone_format=2,
        tombstone="org/lake/tables/table/tombstone/generation/root.json",
        tombstone_rows=1,
        tombstone_digest="0" * 64,
    )
    kwargs = {"expected_mirrors": []}
    if fast_path:
        kwargs["expected_mirror_pin"] = None

    assert catalog.commit_snapshot(
        "org", "lake", "table", payload, "snap/5.json",
        expected_version=4, expected_path="snap/4.json",
        lock_token="token", **kwargs,
    ) == (5, 10)


def test_v2_row_count_round_trips_at_redis_cjson_precision_ceiling():
    catalog, fake = _catalog()
    _seed(fake)
    payload = _snapshot_payload(
        tombstone_format=2,
        tombstone="org/lake/tables/table/tombstone/generation/root.json",
        tombstone_rows=MAX_JSON_EXACT_INTEGER,
        tombstone_digest="0" * 64,
    )

    catalog.commit_snapshot(
        "org", "lake", "table", payload, "snap/5.json",
        expected_version=4, expected_path="snap/4.json",
        lock_token="token", expected_mirrors=[],
    )

    stored = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert stored["payload"]["tombstone_rows"] == MAX_JSON_EXACT_INTEGER
    assert type(stored["payload"]["tombstone_rows"]) is int


@pytest.mark.parametrize("active", [False, True])
def test_writer_prepublish_enforces_sticky_v2_snapshot_version_ceiling(active):
    table = SimpleNamespace(simple_dir="org/lake/tables/table")
    DataWriter._validate_snapshot_for_publish(
        _explicit_v2_payload(MAX_JSON_EXACT_INTEGER, active=active),
        simple_table=table,
        expected_version=MAX_JSON_EXACT_INTEGER - 1,
    )
    with pytest.raises(ValueError, match="v2 exact-integer boundary"):
        DataWriter._validate_snapshot_for_publish(
            _explicit_v2_payload(MAX_JSON_EXACT_INTEGER + 1, active=active),
            simple_table=table,
            expected_version=MAX_JSON_EXACT_INTEGER,
        )


@pytest.mark.parametrize("active", [False, True])
def test_catalog_accepts_sticky_v2_successor_at_version_ceiling(active):
    catalog, fake = _catalog()
    base_path = "snap/max-minus-one.json"
    _seed(
        fake,
        version=MAX_JSON_EXACT_INTEGER - 1,
        path=base_path,
    )

    assert catalog.commit_snapshot(
        "org", "lake", "table",
        _explicit_v2_payload(MAX_JSON_EXACT_INTEGER, active=active),
        "snap/max.json",
        expected_version=MAX_JSON_EXACT_INTEGER - 1,
        expected_path=base_path,
        lock_token="token",
        expected_mirrors=[],
    ) == (MAX_JSON_EXACT_INTEGER, 10)

    stored = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert stored["version"] == MAX_JSON_EXACT_INTEGER
    assert stored["payload"]["snapshot_version"] == MAX_JSON_EXACT_INTEGER
    assert stored["payload"]["tombstone_format"] == 2


@pytest.mark.parametrize("active", [False, True])
def test_catalog_rejects_sticky_v2_successor_above_version_ceiling(active):
    catalog, fake = _catalog()
    base_path = "snap/max.json"
    _seed(fake, version=MAX_JSON_EXACT_INTEGER, path=base_path)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    before = (fake.get(leaf_key), fake.get(root_key))

    with pytest.raises(ValueError, match="exact integer range"):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _explicit_v2_payload(MAX_JSON_EXACT_INTEGER + 1, active=active),
            "snap/above-max.json",
            expected_version=MAX_JSON_EXACT_INTEGER,
            expected_path=base_path,
            lock_token="token",
            expected_mirrors=[],
        )

    assert (fake.get(leaf_key), fake.get(root_key)) == before
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(RK.meta_table_names("org", "lake"))


def test_catalog_keeps_v1_successor_behavior_above_v2_ceiling():
    catalog, fake = _catalog()
    base_path = "snap/max.json"
    _seed(fake, version=MAX_JSON_EXACT_INTEGER, path=base_path)

    leaf_version, root_version = catalog.commit_snapshot(
        "org", "lake", "table",
        _snapshot_payload(
            snapshot_version=MAX_JSON_EXACT_INTEGER + 1,
            tombstone_format=1,
        ),
        "snap/v1-above-v2-max.json",
        expected_version=MAX_JSON_EXACT_INTEGER,
        expected_path=base_path,
        lock_token="token",
        expected_mirrors=[],
    )

    assert leaf_version == MAX_JSON_EXACT_INTEGER + 1
    assert root_version == 10


@pytest.mark.parametrize("fast_path", [False, True])
@pytest.mark.parametrize("active", [False, True])
def test_both_lua_commit_paths_reject_v2_version_above_ceiling(
        monkeypatch, fast_path, active,
):
    catalog, fake = _catalog()
    base_path = "snap/max.json"
    _seed(fake, version=MAX_JSON_EXACT_INTEGER, path=base_path)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    before = (fake.get(leaf_key), fake.get(root_key))
    # Bypass only the Python ceiling to exercise the duplicated Lua boundary.
    monkeypatch.setattr(
        "supertable.redis_catalog.MAX_TOMBSTONE_JSON_EXACT_INTEGER",
        MAX_JSON_EXACT_INTEGER + 1,
    )
    kwargs = {"expected_mirrors": []}
    if fast_path:
        kwargs["expected_mirror_pin"] = None

    with pytest.raises(ValueError, match="invalid snapshot payload"):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _explicit_v2_payload(MAX_JSON_EXACT_INTEGER + 1, active=active),
            "snap/above-max.json",
            expected_version=MAX_JSON_EXACT_INTEGER,
            expected_path=base_path,
            lock_token="token",
            **kwargs,
        )

    assert (fake.get(leaf_key), fake.get(root_key)) == before
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(RK.meta_table_names("org", "lake"))


def test_snapshot_zero_cannot_publish_active_v2_manifest_lineage():
    catalog, fake = _catalog()
    _seed(fake)
    fake.delete(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(ValueError, match="invalid version"):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(
                snapshot_version=0,
                tombstone_format=2,
                tombstone="org/lake/tables/table/tombstone/root.json",
                tombstone_rows=1,
                tombstone_digest="0" * 64,
            ),
            "snap/0.json", expected_version=-1, expected_path="",
            lock_token="token",
        )

    assert not fake.exists(RK.meta_leaf("org", "lake", "table"))
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_snapshot_commit_atomically_persists_unresolved_quality_generation():
    catalog, fake = _catalog()
    _seed(fake)
    unresolved_key = catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )

    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        quality_generation="commit-5",
        now_ms=123,
    )

    assert fake.get(unresolved_key) == "commit-5"
    assert fake.ttl(unresolved_key) == -1


def test_expected_absent_commit_publishes_complete_first_snapshot_atomically():
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        "schema": {"id": "Int64"},
        "resources": [{"file": "data/first.parquet", "rows": 3}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 3,
        "_row_filter": None,
    }

    assert catalog.commit_snapshot(
        "org",
        "lake",
        "table",
        payload,
        "snap/first.json",
        expected_version=-1,
        expected_path="",
        lock_token="token",
        commit_id="first-commit",
        expected_mirrors=[],
        expected_mirror_pin=None,
        quality_generation="first-commit",
        now_ms=123,
    ) == (1, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    root = json.loads(fake.get(RK.meta_root("org", "lake")))
    assert leaf["version"] == 1
    assert leaf["path"] == "snap/first.json"
    assert leaf["payload"] == payload
    assert root["version"] == 10
    assert root["commit_id"] == "first-commit"
    assert json.loads(fake.get(RK.schema("org", "lake", "table"))) == {
        "id": "Int64",
    }
    assert fake.smembers(RK.meta_table_names("org", "lake")) == {"table"}
    assert fake.get(catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )) == "first-commit"
    assert fake.get(RK.meta_mirrors("org", "lake")) is None


def test_one_shot_bootstrap_and_successor_keep_pinned_no_mirror_fast_path(
    monkeypatch,
):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    fast_commit = MagicMock(wraps=catalog._snapshot_commit_no_mirrors)
    general_commit = MagicMock(
        side_effect=AssertionError("general mirror commit path used"),
    )
    monkeypatch.setattr(catalog, "_snapshot_commit_no_mirrors", fast_commit)
    monkeypatch.setattr(catalog, "_snapshot_commit", general_commit)
    first_payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        "schema": {"id": "Int64"},
        "resources": [{"file": "data/first.parquet", "rows": 3}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 3,
        "_row_filter": None,
    }

    assert catalog.commit_snapshot(
        "org", "lake", "table", first_payload, "snap/first.json",
        expected_version=-1,
        expected_path="",
        lock_token="token",
        commit_id="first",
        expected_mirrors=[],
        expected_mirror_pin=None,
        now_ms=123,
    ) == (1, 10)

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token",
    )
    assert context["leaf"]["version"] == 1
    assert context["leaf"]["payload"]["snapshot_version"] == 1
    assert context["mirrors"] == []
    assert context["mirror_pin"] is None

    successor = dict(first_payload)
    successor.update({
        "snapshot_version": 2,
        "previous_snapshot": "snap/first.json",
    })
    assert catalog.commit_snapshot(
        "org", "lake", "table", successor, "snap/second.json",
        expected_version=1,
        expected_path="snap/first.json",
        lock_token="token",
        commit_id="second",
        expected_mirrors=context["mirrors"],
        expected_mirror_pin=context["mirror_pin"],
        now_ms=124,
    ) == (2, 11)

    assert fast_commit.call_count == 2
    general_commit.assert_not_called()
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert leaf["version"] == leaf["payload"]["snapshot_version"] == 2


def test_expected_absent_reply_loss_reconciles_compatibility_generation(
    monkeypatch,
):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    real_commit = catalog._snapshot_commit_no_mirrors

    def commit_then_lose_reply(*args, **kwargs):
        real_commit(*args, **kwargs)
        raise redis.TimeoutError("reply lost after commit")

    monkeypatch.setattr(
        catalog, "_snapshot_commit_no_mirrors", commit_then_lose_reply,
    )
    payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        "schema": {"id": "Int64"},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "_row_filter": None,
    }

    assert catalog.commit_snapshot(
        "org", "lake", "table", payload, "snap/first.json",
        expected_version=-1,
        expected_path="",
        lock_token="token",
        commit_id="first",
        expected_mirrors=[],
        expected_mirror_pin=None,
        now_ms=123,
    ) == (1, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert leaf["version"] == leaf["payload"]["snapshot_version"] == 1


@pytest.mark.parametrize("pinned_no_mirrors", [False, True])
def test_reply_loss_reconciliation_uses_exact_pre_lua_payload_digest(
        pinned_no_mirrors, monkeypatch,
):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    script_name = (
        "_snapshot_commit_no_mirrors"
        if pinned_no_mirrors
        else "_snapshot_commit"
    )
    real_commit = getattr(catalog, script_name)

    def commit_then_lose_reply(*args, **kwargs):
        real_commit(*args, **kwargs)
        raise redis.TimeoutError("reply lost after commit")

    monkeypatch.setattr(catalog, script_name, commit_then_lose_reply)
    payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        # Exercise both known Lua cjson identity hazards: empty object shape
        # and an exact Python Int64 above Redis Lua's numeric range.
        "schema": {},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": (1 << 53) + 1,
        "_row_filter": None,
    }
    kwargs = {"expected_mirrors": []}
    if pinned_no_mirrors:
        kwargs["expected_mirror_pin"] = None

    assert catalog.commit_snapshot(
        "org", "lake", "table", payload, "snap/first.json",
        expected_version=-1,
        expected_path="",
        lock_token="token",
        commit_id="first",
        now_ms=123,
        **kwargs,
    ) == (1, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert leaf["payload"] != payload
    assert leaf["payload_digest"] == hashlib.sha256(
        json.dumps(payload).encode("utf-8")
    ).hexdigest()


@pytest.mark.parametrize(
    "expected_version,expected_path,payload_version",
    [(-1, "", 0), (4, "snap/4.json", 6)],
)
@pytest.mark.parametrize("pinned_no_mirrors", [False, True])
def test_snapshot_commit_rejects_payload_generation_mismatch_before_redis(
        expected_version, expected_path, payload_version, pinned_no_mirrors,
        monkeypatch,
):
    catalog, fake = _catalog()
    _seed(fake)
    if expected_version == -1:
        fake.delete(RK.meta_leaf("org", "lake", "table"))
    general = MagicMock(side_effect=AssertionError("Redis should not run"))
    fast = MagicMock(side_effect=AssertionError("Redis should not run"))
    monkeypatch.setattr(catalog, "_snapshot_commit", general)
    monkeypatch.setattr(catalog, "_snapshot_commit_no_mirrors", fast)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))
    kwargs = {"expected_mirrors": []}
    if pinned_no_mirrors:
        kwargs["expected_mirror_pin"] = None

    with pytest.raises(
        ValueError, match="payload generation does not match",
    ):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _snapshot_payload(snapshot_version=payload_version),
            "snap/new.json",
            expected_version=expected_version,
            expected_path=expected_path,
            lock_token="token",
            **kwargs,
        )

    general.assert_not_called()
    fast.assert_not_called()
    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_expected_absent_commit_loses_to_concurrent_creator_without_overwrite():
    catalog, fake = _catalog()
    _seed(fake, version=0, path="snap/concurrent.json")
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(SnapshotCommitConflictError, match="Snapshot base changed"):
        catalog.commit_snapshot(
            "org",
            "lake",
            "table",
            _snapshot_payload(snapshot_version=1),
            "snap/ours.json",
            expected_version=-1,
            expected_path="",
            lock_token="token",
            commit_id="ours",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_expected_absent_commit_is_blocked_by_durable_namespace_deletion():
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    fake.set(
        RK.meta_namespace_deletion_intent("org", "lake"),
        json.dumps({"intent_id": "delete-1"}),
    )

    with pytest.raises(DeletionIntentConflictError):
        catalog.commit_snapshot(
            "org",
            "lake",
            "table",
            _snapshot_payload(snapshot_version=1),
            "snap/first.json",
            expected_version=-1,
            expected_path="",
            lock_token="token",
            commit_id="first",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) is None
    assert not fake.sismember(RK.meta_table_names("org", "lake"), "table")
    assert fake.get(RK.schema("org", "lake", "table")) is None


@pytest.mark.parametrize("pinned_no_mirrors", [False, True])
def test_waiting_first_creator_does_not_wound_current_publisher(
        pinned_no_mirrors,
):
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    connector = SimpleNamespace(r=fake)
    with patch("supertable.redis_catalog.RedisConnector", return_value=connector):
        current = RedisCatalog()
        waiter = RedisCatalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )

    current_namespace = current.acquire_namespace_lock(
        "org", "lake", ttl_s=30, timeout_s=1,
    )
    current_leaf = current.acquire_simple_lock(
        "org", "lake", "table", ttl_s=30, timeout_s=1,
    )
    assert current_namespace and current_leaf
    context = current.begin_table_mutation(
        "org", "lake", "table",
        lock_token=current_leaf,
        namespace_token=current_namespace,
        reserve_count=3,
    )
    assert context["leaf"] is None
    assert current.release_namespace_lock(
        "org", "lake", current_namespace,
    )

    waiter_has_namespace = threading.Event()
    acquired = {}

    def wait_for_leaf():
        namespace_token = waiter.acquire_namespace_lock(
            "org", "lake", ttl_s=30, timeout_s=2,
        )
        acquired["namespace"] = namespace_token
        waiter_has_namespace.set()
        if namespace_token:
            acquired["leaf"] = waiter.acquire_simple_lock(
                "org", "lake", "table", ttl_s=30, timeout_s=2,
            )

    waiting = threading.Thread(target=wait_for_leaf)
    waiting.start()
    assert waiter_has_namespace.wait(timeout=2)
    assert acquired["namespace"]
    assert fake.get(RK.lock_namespace("org", "lake")) == acquired["namespace"]

    payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        "schema": {"id": "Int64"},
        "resources": [{"file": "data/first.parquet", "rows": 3}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 3,
        "_row_filter": None,
    }
    kwargs = {"expected_mirrors": []}
    if pinned_no_mirrors:
        kwargs["expected_mirror_pin"] = None
    try:
        assert current.commit_snapshot(
            "org", "lake", "table", payload, "snap/first.json",
            expected_version=-1,
            expected_path="",
            lock_token=current_leaf,
            commit_id="current",
            now_ms=123,
            **kwargs,
        ) == (1, 10)
    finally:
        current.release_simple_lock(
            "org", "lake", "table", current_leaf,
        )

    waiting.join(timeout=3)
    assert not waiting.is_alive()
    assert acquired.get("leaf")
    try:
        successor_context = waiter.begin_table_mutation(
            "org", "lake", "table",
            lock_token=acquired["leaf"],
            namespace_token=acquired["namespace"],
        )
        assert successor_context["leaf"]["version"] == 1
        assert successor_context["leaf"]["payload"] == payload
    finally:
        waiter.release_simple_lock(
            "org", "lake", "table", acquired["leaf"],
        )
        waiter.release_namespace_lock(
            "org", "lake", acquired["namespace"],
        )


def test_pinned_absent_mirrors_use_small_atomic_commit_path(monkeypatch):
    catalog, fake = _catalog()
    _seed(fake)
    general_commit = MagicMock(
        side_effect=AssertionError("general mirror commit path used"),
    )
    monkeypatch.setattr(catalog, "_snapshot_commit", general_commit)

    assert catalog.commit_snapshot(
        "org",
        "lake",
        "table",
        _snapshot_payload(schema=[{"id": "long"}]),
        "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        expected_mirrors=[],
        expected_mirror_pin=None,
        quality_generation="commit-5",
        now_ms=123,
    ) == (5, 10)

    general_commit.assert_not_called()
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    root = json.loads(fake.get(RK.meta_root("org", "lake")))
    assert leaf["version"] == 5
    assert leaf["path"] == "snap/5.json"
    assert leaf["commit_id"] == "commit-5"
    assert root["version"] == 10
    assert root["commit_id"] == "commit-5"
    assert json.loads(fake.get(RK.schema("org", "lake", "table"))) == {
        "id": "long",
    }
    assert fake.sismember(RK.meta_table_names("org", "lake"), "table")
    assert fake.get(catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )) == "commit-5"


def test_pinned_empty_mirror_document_uses_small_commit_path(monkeypatch):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    fake.set(
        RK.meta_mirrors("org", "lake"),
        json.dumps({"formats": [], "ts": 7}, separators=(",", ":")),
    )
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token",
    )
    general_commit = MagicMock(
        side_effect=AssertionError("general mirror commit path used"),
    )
    monkeypatch.setattr(catalog, "_snapshot_commit", general_commit)

    assert catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        expected_mirrors=context["mirrors"],
        expected_mirror_pin=context["mirror_pin"],
        now_ms=123,
    ) == (5, 10)
    general_commit.assert_not_called()


def test_pinned_no_mirror_commit_rejects_concurrent_enable():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token",
    )
    assert context["mirrors"] == []
    assert context["mirror_pin"] is None
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    catalog.enable_mirror("org", "lake", "DELTA")
    with pytest.raises(
        SnapshotCommitConflictError, match="Mirror configuration changed",
    ):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            expected_mirrors=[],
            expected_mirror_pin=context["mirror_pin"],
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_pinned_no_mirror_commit_rejects_disable_enable_aba():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token",
    )
    catalog.enable_mirror("org", "lake", "DELTA")
    catalog.disable_mirror("org", "lake", "DELTA")
    assert catalog.get_mirrors("org", "lake") == []

    with pytest.raises(
        SnapshotCommitConflictError, match="Mirror configuration changed",
    ):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            expected_mirrors=[],
            expected_mirror_pin=context["mirror_pin"],
        )


def test_pinned_no_mirror_commit_rejects_corrupt_current_key_type():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token",
    )
    fake.hset(RK.meta_mirrors("org", "lake"), "corrupt", "state")
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))

    with pytest.raises(RuntimeError, match="Corrupt mirror configuration"):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            expected_mirrors=[],
            expected_mirror_pin=context["mirror_pin"],
        )
    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf


def test_no_mirror_fast_path_rejects_nonempty_raw_pin():
    catalog, fake = _catalog()
    _seed(fake)
    pin = json.dumps({"formats": ["DELTA"], "ts": 1})

    with pytest.raises(ValueError, match="empty mirror configuration"):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            expected_mirrors=[],
            expected_mirror_pin=pin,
        )


@pytest.mark.parametrize(
    "race,error_type",
    [
        ("stale-base", SnapshotCommitConflictError),
        ("lost-lock", LockLostError),
        ("namespace-lock", RuntimeError),
        ("namespace-delete", DeletionIntentConflictError),
        ("simple-delete", DeletionIntentConflictError),
        ("missing-root", FileNotFoundError),
        ("read-only", PermissionError),
        ("corrupt-leaf", RuntimeError),
    ],
)
def test_no_mirror_fast_path_retains_publication_fences(race, error_type):
    catalog, fake = _catalog()
    _seed(fake)
    expected_version = 4
    lock_token = "token"
    if race == "stale-base":
        expected_version = 3
    elif race == "lost-lock":
        lock_token = "stale-owner"
    elif race == "namespace-lock":
        fake.set(RK.lock_namespace("org", "lake"), "deleter")
    elif race == "namespace-delete":
        fake.set(RK.meta_namespace_deletion_intent("org", "lake"), "pending")
    elif race == "simple-delete":
        fake.set(
            RK.meta_simple_deletion_intent("org", "lake", "table"),
            "pending",
        )
    elif race == "missing-root":
        fake.delete(RK.meta_root("org", "lake"))
    elif race == "read-only":
        fake.set(
            RK.meta_root("org", "lake"),
            json.dumps({
                "version": 9,
                "ts": 1,
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": "source",
            }),
        )
    else:
        fake.set(RK.meta_leaf("org", "lake", "table"), "[]")
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(error_type):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(
                snapshot_version=expected_version + 1, schema=[],
            ),
            "snap/5.json",
            expected_version=expected_version,
            expected_path="snap/4.json",
            lock_token=lock_token,
            commit_id="commit-5",
            expected_mirrors=[],
            expected_mirror_pin=None,
            quality_generation="commit-5",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root
    assert not fake.exists(RK.schema("org", "lake", "table"))
    assert not fake.exists(catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    ))


@pytest.mark.parametrize("failure", ["stale", "lost_lock", "deleting"])
def test_failed_snapshot_commit_never_publishes_quality_generation(failure):
    catalog, fake = _catalog()
    _seed(fake)
    unresolved_key = catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )
    fake.set(unresolved_key, "newer-generation")
    expected_version = 4
    lock_token = "token"
    expected_error = SnapshotCommitConflictError
    if failure == "stale":
        expected_version = 3
    elif failure == "lost_lock":
        lock_token = "old-owner"
        fake.set(RK.lock_leaf("org", "lake", "table"), "new-owner")
        expected_error = LockLostError
    else:
        fake.set(
            RK.meta_simple_deletion_intent("org", "lake", "table"),
            json.dumps({"intent_id": "delete-1"}),
        )
        expected_error = DeletionIntentConflictError

    with pytest.raises(expected_error):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _snapshot_payload(snapshot_version=expected_version + 1),
            "snap/5.json",
            expected_version=expected_version,
            expected_path="snap/4.json",
            lock_token=lock_token,
            commit_id="stale-generation",
            quality_generation="stale-generation",
        )

    assert fake.get(unresolved_key) == "newer-generation"


def test_reply_loss_after_atomic_commit_cannot_lose_quality_generation(
    monkeypatch,
):
    catalog, fake = _catalog()
    _seed(fake)
    unresolved_key = catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )
    original_commit = catalog._snapshot_commit

    def commit_then_lose_reply(*args, **kwargs):
        original_commit(*args, **kwargs)
        raise redis.TimeoutError("reply lost after commit")

    monkeypatch.setattr(catalog, "_snapshot_commit", commit_then_lose_reply)
    reconcile = MagicMock(side_effect=AssertionError("normal commit reconciled"))
    monkeypatch.setattr(catalog, "_reconcile_snapshot_commit", reconcile)
    with pytest.raises(redis.TimeoutError, match="reply lost"):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
            quality_generation="commit-5",
        )

    reconcile.assert_not_called()
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    assert leaf["commit_id"] == "commit-5"
    assert "payload_digest" not in leaf
    assert fake.get(unresolved_key) == "commit-5"


def test_timeout_before_atomic_commit_is_not_false_positive_reconciled(
    monkeypatch,
):
    catalog, fake = _catalog()
    _seed(fake)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))

    def lose_before_commit(*args, **kwargs):
        raise redis.TimeoutError("request never reached Redis")

    monkeypatch.setattr(catalog, "_snapshot_commit", lose_before_commit)
    with pytest.raises(redis.TimeoutError, match="never reached"):
        catalog.commit_snapshot(
            "org",
            "lake",
            "table",
            _snapshot_payload(),
            "snap/5.json",
            expected_version=4,
            expected_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf


def test_disabled_then_enabled_resolution_preserves_committed_generation():
    from supertable.quality import scheduler

    catalog, fake = _catalog()
    _seed(fake)
    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        quality_generation="commit-5",
    )
    admission = scheduler._snapshot_pending_lifecycle_admission(
        fake, "org", "lake", "table",
    )
    assert admission is not None
    scheduler._resolve_unresolved_pending(
        fake, "org", "lake", "table", (), admission,
    )
    unresolved_key = scheduler._unresolved_pending_key(
        "org", "lake", "table",
    )
    assert fake.get(unresolved_key) == "commit-5"

    scheduler._resolve_unresolved_pending(
        fake, "org", "lake", "table", ("quick",), admission,
    )
    assert fake.get(unresolved_key) is None
    assert fake.get(
        scheduler._pending_key("org", "lake", "table", "quick")
    ) == "commit-5"


def test_concurrent_resolver_cannot_consume_newer_committed_generation():
    from supertable.quality import scheduler

    catalog, fake = _catalog()
    _seed(fake)
    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4,
        expected_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        quality_generation="commit-5",
    )
    admission = scheduler._snapshot_pending_lifecycle_admission(
        fake, "org", "lake", "table",
    )
    assert admission is not None

    class CommitBeforeResolverCAS:
        def __init__(self, inner):
            self.inner = inner
            self.committed = False

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def eval(self, script, *args):
            if (
                not self.committed
                and "atomically resolve deferred ingest work" in script
            ):
                self.committed = True
                catalog.commit_snapshot(
                    "org", "lake", "table",
                    _snapshot_payload(snapshot_version=6), "snap/6.json",
                    expected_version=5,
                    expected_path="snap/5.json",
                    lock_token="token",
                    commit_id="commit-6",
                    quality_generation="commit-6",
                )
            return self.inner.eval(script, *args)

    raced = CommitBeforeResolverCAS(fake)
    scheduler._resolve_unresolved_pending(
        raced, "org", "lake", "table", ("quick",), admission,
    )

    assert raced.committed
    assert fake.get(
        scheduler._unresolved_pending_key("org", "lake", "table")
    ) == "commit-6"
    assert fake.get(
        scheduler._pending_key("org", "lake", "table", "quick")
    ) is None


def test_mirror_intent_and_core_commit_transition_are_durable_and_atomic():
    catalog, fake = _catalog()
    _seed(fake)

    prepared = catalog.prepare_mirror_publication(
        "org", "lake", "table",
        commit_id="commit-5", snapshot_path="snap/5.json",
        mirrors=["DELTA", "PARQUET"], lock_token="token", now_ms=120,
    )
    assert prepared["status"] == "prepared"
    assert prepared["core_committed"] is False

    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(resources=[{"file": "f"}]),
        "snap/5.json", expected_version=4, expected_path="snap/4.json",
        lock_token="token", commit_id="commit-5",
        mirror_publication=True, now_ms=123,
    )

    state = catalog.get_mirror_publication("org", "lake", "table")
    assert state["status"] == "core_committed"
    assert state["core_committed"] is True
    assert state["leaf_version"] == 5
    assert state["root_version"] == 10
    assert state["snapshot_path"] == "snap/5.json"
    with pytest.raises(SnapshotCommitConflictError, match="Unresolved mirror"):
        catalog.prepare_mirror_publication(
            "org", "lake", "table", commit_id="commit-6",
            snapshot_path="snap/6.json", mirrors=["DELTA"],
            lock_token="token",
        )


def test_crash_after_prepare_leaves_durable_intent_and_old_core_snapshot():
    catalog, fake = _catalog()
    _seed(fake)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["PARQUET"],
        lock_token="token", now_ms=120,
    )

    # Simulate process death: no core commit or terminal state transition.
    state = catalog.get_mirror_publication("org", "lake", "table")
    assert state["status"] == "prepared"
    assert state["core_committed"] is False
    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    with pytest.raises(SnapshotCommitConflictError, match="Unresolved mirror"):
        catalog.prepare_mirror_publication(
            "org", "lake", "table", commit_id="commit-6",
            snapshot_path="snap/6.json", mirrors=["PARQUET"],
            lock_token="token",
        )


def test_mirror_tracked_commit_without_prepared_intent_changes_nothing():
    catalog, fake = _catalog()
    _seed(fake)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(RuntimeError, match="Missing or mismatched mirror"):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4, expected_path="snap/4.json", lock_token="token",
            commit_id="commit-5", mirror_publication=True,
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_failed_mirror_record_retains_exact_core_commit_and_blocks_overwrite():
    catalog, fake = _catalog()
    _seed(fake)
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["PARQUET"],
        lock_token="token", now_ms=120,
    )
    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4, expected_path="snap/4.json", lock_token="token",
        commit_id="commit-5", mirror_publication=True, now_ms=123,
    )

    failed = catalog.fail_mirror_publication(
        "org", "lake", "table", commit_id="commit-5", lock_token="token",
        failure_stage="mirror:PARQUET", error=OSError("delete denied"),
        now_ms=130,
    )
    assert failed["status"] == "failed"
    assert failed["core_committed"] is True
    assert failed["failure_stage"] == "mirror:PARQUET"
    assert failed["error"] == {"type": "OSError", "message": "delete denied"}

    with pytest.raises(SnapshotCommitConflictError, match="Unresolved mirror"):
        catalog.prepare_mirror_publication(
            "org", "lake", "table", commit_id="commit-6",
            snapshot_path="snap/6.json", mirrors=["PARQUET"],
            lock_token="token",
        )


def test_completed_mirror_record_allows_next_publication():
    catalog, fake = _catalog()
    _seed(fake)
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["DELTA"], lock_token="token",
    )
    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4, expected_path="snap/4.json", lock_token="token",
        commit_id="commit-5", mirror_publication=True,
    )
    complete = catalog.complete_mirror_publication(
        "org", "lake", "table", commit_id="commit-5", lock_token="token",
    )
    assert complete["status"] == "complete"

    next_record = catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-6",
        snapshot_path="snap/6.json", mirrors=["DELTA"], lock_token="token",
    )
    assert next_record["status"] == "prepared"
    assert next_record["commit_id"] == "commit-6"


def test_same_mirror_commit_id_cannot_be_reused_for_another_snapshot():
    catalog, fake = _catalog()
    _seed(fake)
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["DELTA"], lock_token="token",
    )
    with pytest.raises(RuntimeError, match="Invalid mirror publication prepare"):
        catalog.prepare_mirror_publication(
            "org", "lake", "table", commit_id="commit-5",
            snapshot_path="snap/other.json", mirrors=["PARQUET"],
            lock_token="token",
        )


def test_mirror_owner_blocks_stale_a_and_new_c_until_safe_rebind():
    catalog, fake = _catalog()
    _seed(fake, token="publisher-a")
    prepared = catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["PARQUET"],
        lock_token="publisher-a", now_ms=120,
    )
    assert prepared["publication_owner"] == "publisher-a"
    assert prepared["publisher_quiesced"] is False
    assert prepared["owner_generation"] == 0
    catalog.commit_snapshot(
        "org", "lake", "table", _snapshot_payload(), "snap/5.json",
        expected_version=4, expected_path="snap/4.json",
        lock_token="publisher-a", commit_id="commit-5",
        mirror_publication=True, now_ms=123,
    )

    # A's renewable lease is lost while its storage call may still resume.
    # B owns the new Redis lease, but neither B nor a newer C publication may
    # proceed solely because A's lease expired.
    fake.set(RK.lock_leaf("org", "lake", "table"), "publisher-b")
    with pytest.raises(PermissionError, match="previous publisher has stopped"):
        catalog.claim_mirror_publication(
            "org", "lake", "table", commit_id="commit-5",
            expected_previous_owner="publisher-a", lock_token="publisher-b",
            confirm_previous_owner_stopped=False,
        )
    with pytest.raises(SnapshotCommitConflictError, match="intent changed"):
        catalog.claim_mirror_publication(
            "org", "lake", "table", commit_id="wrong-commit",
            expected_previous_owner="publisher-a", lock_token="publisher-b",
            confirm_previous_owner_stopped=True,
        )
    with pytest.raises(SnapshotCommitConflictError, match="owner or status"):
        catalog.claim_mirror_publication(
            "org", "lake", "table", commit_id="commit-5",
            expected_previous_owner="wrong-owner", lock_token="publisher-b",
            confirm_previous_owner_stopped=True,
        )
    with pytest.raises(SnapshotCommitConflictError, match="Unresolved mirror"):
        catalog.prepare_mirror_publication(
            "org", "lake", "table", commit_id="commit-6",
            snapshot_path="snap/6.json", mirrors=["PARQUET"],
            lock_token="publisher-b",
        )

    claimed = catalog.claim_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        expected_previous_owner="publisher-a", lock_token="publisher-b",
        confirm_previous_owner_stopped=True, now_ms=130,
    )
    assert claimed["publication_owner"] == "publisher-b"
    assert claimed["previous_publication_owner"] == "publisher-a"
    assert claimed["publisher_quiesced"] is False
    assert claimed["owner_generation"] == 1

    # Even if stale A somehow presents its old token as a live Redis lock, the
    # durable owner comparison prevents it from closing B's intent.
    fake.set(RK.lock_leaf("org", "lake", "table"), "publisher-a")
    with pytest.raises(SnapshotCommitConflictError, match="another publisher"):
        catalog.complete_mirror_publication(
            "org", "lake", "table", commit_id="commit-5",
            lock_token="publisher-a",
        )

    # A generic mirror error is still storage-ambiguous, so even its
    # exact-owner failure transition does not make C safe automatically.
    fake.set(RK.lock_leaf("org", "lake", "table"), "publisher-b")
    failed = catalog.fail_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        lock_token="publisher-b", failure_stage="mirror:PARQUET",
        error=OSError("copy failed"), now_ms=140,
    )
    assert failed["publisher_quiesced"] is False
    fake.set(RK.lock_leaf("org", "lake", "table"), "publisher-c")
    with pytest.raises(PermissionError, match="previous publisher has stopped"):
        catalog.claim_mirror_publication(
            "org", "lake", "table", commit_id="commit-5",
            expected_previous_owner="publisher-b", lock_token="publisher-c",
            confirm_previous_owner_stopped=False, now_ms=150,
        )
    reclaimed = catalog.claim_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        expected_previous_owner="publisher-b", lock_token="publisher-c",
        confirm_previous_owner_stopped=True, now_ms=150,
    )
    assert reclaimed["publication_owner"] == "publisher-c"
    assert reclaimed["publisher_quiesced"] is False
    assert reclaimed["owner_generation"] == 2
    catalog.complete_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        lock_token="publisher-c", now_ms=160,
    )
    next_record = catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-6",
        snapshot_path="snap/6.json", mirrors=["PARQUET"],
        lock_token="publisher-c", now_ms=170,
    )
    assert next_record["commit_id"] == "commit-6"
    assert next_record["publication_owner"] == "publisher-c"


@pytest.mark.parametrize(
    "safe_stage", ["core_commit", "recovery:core_not_committed", "outbox_complete"],
)
def test_only_provably_non_mirror_io_failures_are_auto_claimable(safe_stage):
    catalog, fake = _catalog()
    _seed(fake, token="publisher-a")
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        snapshot_path="snap/5.json", mirrors=["PARQUET"],
        lock_token="publisher-a",
    )
    if safe_stage == "outbox_complete":
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4, expected_path="snap/4.json",
            lock_token="publisher-a", commit_id="commit-5",
            mirror_publication=True,
        )
    failed = catalog.fail_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        lock_token="publisher-a", failure_stage=safe_stage,
        error=RuntimeError("safe boundary"),
    )
    assert failed["publisher_quiesced"] is True

    fake.set(RK.lock_leaf("org", "lake", "table"), "publisher-b")
    claimed = catalog.claim_mirror_publication(
        "org", "lake", "table", commit_id="commit-5",
        expected_previous_owner="publisher-a", lock_token="publisher-b",
        confirm_previous_owner_stopped=False,
    )
    assert claimed["publication_owner"] == "publisher-b"
    assert claimed["publisher_quiesced"] is False


def test_leaf_initializer_never_overwrites_an_existing_snapshot():
    catalog, fake = _catalog()
    _seed(fake)
    before = fake.get(RK.meta_leaf("org", "lake", "table"))

    with pytest.raises(SnapshotCommitConflictError, match="existing table"):
        catalog.set_leaf_payload_cas(
            "org", "lake", "table",
            {"resources": [], "tombstone": None},
            "snap/bootstrap.json",
            now_ms=456,
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before


def test_leaf_initializer_indexes_empty_table_atomically():
    catalog, fake = _catalog()
    fake.set(RK.meta_root("org", "lake"), json.dumps({"version": 0, "ts": 1}))

    catalog.set_leaf_payload_cas(
        "org", "lake", "empty",
        {"resources": [], "tombstone": None},
        "snap/bootstrap.json",
        now_ms=456,
    )

    assert fake.smembers(RK.meta_table_names("org", "lake")) == {"empty"}


def test_simple_delete_atomically_cleans_index_and_recreation_fences():
    catalog, fake = _catalog()
    _seed(fake)
    fake.sadd(RK.meta_table_names("org", "lake"), "table")
    fake.set(RK.schema("org", "lake", "table"), "{}")
    fake.set(RK.meta_rowid_seq("org", "lake", "table"), "42")
    fake.set(RK.meta_table_config("org", "lake", "table"), "{}")
    catalog.prepare_mirror_publication(
        "org", "lake", "table", commit_id="stale",
        snapshot_path="snap/5.json", mirrors=["DELTA"], lock_token="token",
    )
    root_before = json.loads(fake.get(RK.meta_root("org", "lake")))
    fake.set(RK.lock_namespace("org", "lake"), "namespace-token", ex=30)
    intent = catalog.begin_simple_deletion(
        "org", "lake", "table",
        namespace_token="namespace-token",
        lock_token="token",
        intent_id="delete-1",
    )

    assert catalog.delete_simple_table(
        "org", "lake", "table", lock_token="token",
        namespace_token="namespace-token", intent_id=intent["intent_id"],
    )

    assert fake.get(RK.lock_leaf("org", "lake", "table")) == "token"
    assert not fake.sismember(RK.meta_table_names("org", "lake"), "table")
    for key in (
        RK.meta_leaf("org", "lake", "table"),
        RK.schema("org", "lake", "table"),
        RK.meta_rowid_seq("org", "lake", "table"),
        RK.meta_table_config("org", "lake", "table"),
        RK.meta_mirror_publication("org", "lake", "table"),
    ):
        assert not fake.exists(key)
    terminal = catalog.get_simple_deletion_intent("org", "lake", "table")
    assert terminal["intent_id"] == "delete-1"
    assert terminal["status"] == "deleted"
    root_after = json.loads(fake.get(RK.meta_root("org", "lake")))
    assert root_after["version"] == root_before["version"] + 1

    # The terminal tombstone prevents ordinary recreation or takeover even
    # after both original leases expire.
    fake.delete(RK.lock_leaf("org", "lake", "table"))
    fake.delete(RK.lock_namespace("org", "lake"))
    fake.set(RK.lock_leaf("org", "lake", "table"), "new-token", ex=30)
    fake.set(RK.lock_namespace("org", "lake"), "new-namespace", ex=30)
    with pytest.raises(RuntimeError, match="durable deletion intent"):
        catalog.set_leaf_payload_cas(
            "org", "lake", "table", {}, "new.json",
            namespace_token="new-namespace",
        )
    with pytest.raises(RuntimeError, match="prior deletion intent"):
        catalog.begin_simple_deletion(
            "org", "lake", "table",
            namespace_token="new-namespace",
            lock_token="new-token",
            intent_id="delete-2",
        )

    # Explicit recovery first rebinds and re-finalizes the exact tombstone;
    # only then may the confirmed operator clear it for recreation.
    quality_running = RK.quality_prefix("org", "lake") + "running:table"
    fake.set(quality_running, "stale-quality-owner")
    catalog.recover_simple_deletion(
        "org", "lake", "table",
        expected_intent_id="delete-1",
        namespace_token="new-namespace",
        lock_token="new-token",
        confirm_previous_owner_stopped=True,
    )
    assert not fake.exists(quality_running)
    assert catalog.delete_simple_table(
        "org", "lake", "table", lock_token="new-token",
        namespace_token="new-namespace", intent_id="delete-1",
    )
    catalog.clear_simple_deletion_tombstone(
        "org", "lake", "table",
        expected_intent_id="delete-1",
        namespace_token="new-namespace",
        lock_token="new-token",
        confirm_previous_owner_stopped=True,
    )
    assert not fake.exists(RK.meta_simple_deletion_intent("org", "lake", "table"))


def test_leaf_existence_transport_error_is_unknown_not_absent(monkeypatch):
    catalog, _fake = _catalog()
    monkeypatch.setattr(
        catalog.r, "exists", MagicMock(side_effect=redis.TimeoutError("redis timeout")),
    )

    with pytest.raises(redis.TimeoutError, match="redis timeout"):
        catalog.leaf_exists("org", "lake", "table")


def test_root_transport_error_is_unknown_not_absent(monkeypatch):
    catalog, _fake = _catalog()
    monkeypatch.setattr(
        catalog.r, "get", MagicMock(side_effect=redis.TimeoutError("redis timeout")),
    )
    with pytest.raises(redis.TimeoutError, match="redis timeout"):
        catalog.get_root("org", "lake")


def test_replica_resolution_transport_error_cannot_fall_back_to_local(monkeypatch):
    catalog, _fake = _catalog()
    monkeypatch.setattr(
        catalog.r, "get", MagicMock(side_effect=redis.TimeoutError("redis timeout")),
    )
    with pytest.raises(redis.TimeoutError, match="redis timeout"):
        catalog.get_leaf("org", "replica", "table")


@pytest.mark.parametrize(
    "replica_tables",
    [[], "table", {"table": True}, ["table", 7]],
)
def test_replica_allowlist_never_fails_open(replica_tables):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "source"),
        json.dumps({"version": 1, "ts": 1}),
    )
    fake.set(
        RK.meta_leaf("org", "source", "table"),
        json.dumps({"version": 1, "ts": 1, "path": "snap/1.json"}),
    )
    fake.set(
        RK.meta_root("org", "replica"),
        json.dumps({
            "version": 1,
            "ts": 1,
            "read_only": True,
            "clone_type": "replica",
            "cloned_from": "source",
            "replica_tables": replica_tables,
        }),
    )

    if replica_tables == []:
        assert catalog.get_leaf("org", "replica", "table") is None
        assert catalog.leaf_exists("org", "replica", "table") is False
        assert list(catalog.scan_leaf_keys("org", "replica")) == []
    else:
        with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
            catalog.get_leaf("org", "replica", "table")


@pytest.mark.parametrize("source", [None, "", "replica", "../source"])
def test_replica_invalid_source_never_falls_back_to_local(source):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "replica"),
        json.dumps({
            "version": 1,
            "ts": 1,
            "read_only": True,
            "clone_type": "replica",
            "cloned_from": source,
            "replica_tables": None,
        }),
    )
    # A stale local leaf must remain unreachable when replica metadata is bad.
    fake.set(
        RK.meta_leaf("org", "replica", "table"),
        json.dumps({"version": 1, "ts": 1, "path": "private/1.json"}),
    )

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.get_leaf("org", "replica", "table")


def test_replica_rejects_orphan_source_and_source_deletion_intent():
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "replica"),
        json.dumps({
            "version": 1,
            "ts": 1,
            "read_only": True,
            "clone_type": "replica",
            "cloned_from": "source",
            "replica_tables": None,
        }),
    )
    fake.set(
        RK.meta_leaf("org", "source", "table"),
        json.dumps({"version": 1, "ts": 1, "path": "source/snap.json"}),
    )

    with pytest.raises(RuntimeError, match="missing source namespace"):
        catalog.get_leaf("org", "replica", "table")

    fake.set(
        RK.meta_root("org", "source"),
        json.dumps({"version": 1, "ts": 1}),
    )
    fake.set(
        RK.meta_namespace_deletion_intent("org", "source"),
        json.dumps({"intent_id": "delete-source"}),
    )
    with pytest.raises(DeletionIntentConflictError, match="source is fenced"):
        catalog.get_leaf("org", "replica", "table")


def test_replica_rejects_replica_chain_source():
    catalog, fake = _catalog()
    for name, source in (("replica", "middle"), ("middle", "origin")):
        fake.set(
            RK.meta_root("org", name),
            json.dumps({
                "version": 1,
                "ts": 1,
                "read_only": True,
                "clone_type": "replica",
                "cloned_from": source,
                "replica_tables": None,
            }),
        )
    with pytest.raises(RuntimeError, match="another replica"):
        catalog.get_leaf("org", "replica", "table")


def test_replica_scan_pins_target_and_source_roots(monkeypatch):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "source-a"),
        json.dumps({"version": 1, "ts": 1}),
    )
    fake.set(
        RK.meta_root("org", "source-b"),
        json.dumps({"version": 1, "ts": 1}),
    )
    target_key = RK.meta_root("org", "replica")
    target = {
        "version": 1,
        "ts": 1,
        "read_only": True,
        "clone_type": "replica",
        "cloned_from": "source-a",
        "replica_tables": ["table"],
    }
    fake.set(target_key, json.dumps(target))
    leaf_key = RK.meta_leaf("org", "source-a", "table")
    fake.set(
        leaf_key,
        json.dumps({"version": 1, "ts": 1, "path": "source-a/snap.json"}),
    )

    original_scan = catalog._scan_leaf_keys_raw

    def changing_scan(*args, **kwargs):
        yield from original_scan(*args, **kwargs)
        changed = dict(target)
        changed["cloned_from"] = "source-b"
        fake.set(target_key, json.dumps(changed))

    monkeypatch.setattr(catalog, "_scan_leaf_keys_raw", changing_scan)
    with pytest.raises(SnapshotCommitConflictError, match="Catalog changed"):
        list(catalog.scan_leaf_items("org", "replica", count=1))


def test_get_leaf_transport_and_corruption_fail_closed(monkeypatch):
    catalog, fake = _catalog()
    _seed(fake)
    # Resolve the ordinary root first, then inject a leaf-only transport fault.
    original_get = fake.get

    def failing_get(key):
        if key == RK.meta_leaf("org", "lake", "table"):
            raise redis.TimeoutError("leaf timeout")
        return original_get(key)

    monkeypatch.setattr(fake, "get", failing_get)
    with pytest.raises(redis.TimeoutError, match="leaf timeout"):
        catalog.get_leaf("org", "lake", "table")

    monkeypatch.setattr(fake, "get", original_get)
    fake.set(RK.meta_leaf("org", "lake", "table"), "[]")
    with pytest.raises(RuntimeError, match="Corrupt Redis leaf JSON"):
        catalog.get_leaf("org", "lake", "table")


@pytest.mark.parametrize(
    "leaf",
    [
        {"version": "4", "ts": 1, "path": "snap/4.json"},
        {"version": True, "ts": 1, "path": "snap/4.json"},
        {"version": 4, "ts": "1", "path": "snap/4.json"},
        {"version": 4, "ts": True, "path": "snap/4.json"},
        {"version": 4, "ts": 1, "path": ""},
    ],
)
def test_leaf_reads_and_enumeration_share_strict_identity_contract(leaf):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    fake.set(leaf_key, json.dumps(leaf))

    with pytest.raises(RuntimeError, match="Corrupt Redis leaf JSON"):
        catalog.get_leaf("org", "lake", "table")
    with pytest.raises(RuntimeError, match="Malformed catalog leaf"):
        list(catalog._fetch_batch([leaf_key]))


def test_readonly_guard_transport_error_fails_closed(monkeypatch):
    from supertable.rbac import access_control

    catalog = MagicMock()
    catalog.get_root.side_effect = redis.TimeoutError("redis timeout")
    # The guard imports inside the function, so patch the source constructor.
    monkeypatch.setattr(
        "supertable.redis_catalog.RedisCatalog", MagicMock(return_value=catalog),
    )
    with pytest.raises(redis.TimeoutError, match="redis timeout"):
        access_control._check_readonly_guard("lake", "org", "write")


def test_invalid_mirror_configuration_cannot_be_treated_as_disabled():
    catalog, fake = _catalog()
    fake.set(
        RK.meta_mirrors("org", "lake"),
        json.dumps({"formats": ["PARQUE"], "ts": 1}),
    )
    with pytest.raises(ValueError, match="Unsupported configured mirror"):
        catalog.get_mirrors("org", "lake")


def test_leaf_scan_transport_error_cannot_return_a_partial_table_set(monkeypatch):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")
    calls = iter([(17, [leaf_key]), redis.TimeoutError("page two failed")])

    def scan(**kwargs):
        value = next(calls)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(catalog, "_resolve_replica_info", lambda *a: None)
    monkeypatch.setattr(fake, "scan", scan)

    with pytest.raises(redis.TimeoutError, match="page two failed"):
        list(catalog.scan_leaf_items("org", "lake", count=1))


def test_leaf_scan_rejects_catalog_generation_change(monkeypatch):
    catalog, fake = _catalog()
    _seed(fake)
    leaf_key = RK.meta_leaf("org", "lake", "table")

    def keys(*args, **kwargs):
        yield leaf_key
        fake.set(
            RK.meta_root("org", "lake"),
            json.dumps({"version": 10, "ts": 2, "read_only": False}),
        )

    monkeypatch.setattr(catalog, "_resolve_replica_info", lambda *a: None)
    monkeypatch.setattr(catalog, "_scan_leaf_keys_raw", keys)

    with pytest.raises(SnapshotCommitConflictError, match="Catalog changed"):
        list(catalog.scan_leaf_items("org", "lake", count=1))


def test_snapshot_commit_rejects_stale_base_without_changing_catalog():
    catalog, fake = _catalog()
    _seed(fake)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(SnapshotCommitConflictError):
        catalog.commit_snapshot(
            "org", "lake", "table",
            _snapshot_payload(snapshot_version=4), "snap/stale.json",
            expected_version=3, expected_path="snap/3.json", lock_token="token",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf
    assert fake.get(RK.meta_root("org", "lake")) == before_root


def test_snapshot_commit_rejects_lost_fencing_lock_without_changing_catalog():
    catalog, fake = _catalog()
    _seed(fake, token="new-owner")
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))

    with pytest.raises(LockLostError):
        catalog.commit_snapshot(
            "org", "lake", "table", _snapshot_payload(), "snap/5.json",
            expected_version=4, expected_path="snap/4.json", lock_token="old-owner",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf


def test_ambiguous_atomic_commit_error_is_never_retried_as_path_only():
    events = []

    class Batch:
        def catalog_commit_started(self):
            events.append("commit_started")

        def catalog_commit_succeeded(self):
            events.append("commit_succeeded")

        def catalog_commit_rejected(self):
            events.append("commit_rejected")

    class Catalog:
        def commit_snapshot(self, *args, **kwargs):
            events.append("redis")
            raise TimeoutError("reply lost after Redis commit")

        set_leaf_payload_cas = MagicMock()
        set_leaf_path_cas = MagicMock()
        bump_root = MagicMock()

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="org", super_name="lake")
    writer.catalog = Catalog()
    table = SimpleNamespace(_last_snapshot_leaf={"version": 4, "path": "snap/4.json"})

    with pytest.raises(TimeoutError):
        writer._publish_snapshot(
            simple_table=table,
            simple_name="table",
            payload=_snapshot_payload(schema={}),
            path="snap/5.json",
            base_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
            now_ms=123,
            durability_batch=Batch(),
        )

    assert events == ["commit_started", "redis"]
    writer.catalog.set_leaf_payload_cas.assert_not_called()
    writer.catalog.set_leaf_path_cas.assert_not_called()
    writer.catalog.bump_root.assert_not_called()


@pytest.mark.parametrize(
    "rejection",
    [
        SnapshotCommitConflictError("stale base"),
        LockLostError("lost lock"),
        DeletionIntentConflictError("deletion intent"),
        ReadOnlyCatalogError("read only"),
    ],
)
def test_typed_catalog_rejection_returns_batch_to_safe_orphan_cleanup(
    rejection,
):
    events = []

    class Batch:
        def catalog_commit_started(self):
            events.append("commit_started")

        def catalog_commit_succeeded(self):
            events.append("commit_succeeded")

        def catalog_commit_rejected(self):
            events.append("commit_rejected")

    class Catalog:
        def commit_snapshot(self, *args, **kwargs):
            events.append("redis")
            raise rejection

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="org", super_name="lake")
    writer.catalog = Catalog()
    table = SimpleNamespace(_last_snapshot_leaf={"version": 4, "path": "snap/4.json"})

    with pytest.raises(type(rejection), match=str(rejection)):
        writer._publish_snapshot(
            simple_table=table,
            simple_name="table",
            payload=_snapshot_payload(schema={}),
            path="snap/5.json",
            base_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
            now_ms=123,
            durability_batch=Batch(),
        )

    assert events == ["commit_started", "redis", "commit_rejected"]


def test_durable_delete_rejection_removes_first_write_objects(tmp_path):
    catalog, fake = _catalog()
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), "token", ex=30)
    fake.set(
        RK.meta_namespace_deletion_intent("org", "lake"),
        json.dumps({"intent_id": "delete-1"}),
    )
    storage = LocalStorage(root=tmp_path)
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        organization="org", super_name="lake",
    )
    writer.catalog = catalog
    table = SimpleNamespace(
        _last_snapshot_leaf={"version": -1, "path": ""},
    )
    data_path = "org/lake/tables/table/data/first.parquet"
    snapshot_path = "org/lake/tables/table/snapshots/first.json"
    payload = {
        "snapshot_version": 1,
        "previous_snapshot": None,
        "schema": {"id": "Int64"},
        "resources": [{"file": data_path, "rows": 3}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 3,
        "_row_filter": None,
    }

    with pytest.raises(DeletionIntentConflictError):
        with storage.durability_batch() as batch:
            storage.write_bytes(data_path, b"parquet")
            storage.write_json(snapshot_path, payload)
            batch.barrier()
            writer._publish_snapshot(
                simple_table=table,
                simple_name="table",
                payload=payload,
                path=snapshot_path,
                base_path="",
                lock_token="token",
                commit_id="first",
                now_ms=123,
                mirrors=[],
                mirror_pin_available=True,
                mirror_pin=None,
                durability_batch=batch,
            )

    assert not storage.exists(data_path)
    assert not storage.exists(snapshot_path)
    assert fake.get(RK.meta_leaf("org", "lake", "table")) is None


def test_writer_passes_pinned_empty_mirror_generation_to_capable_catalog():
    class Catalog:
        supports_pinned_no_mirror_commit = True

        def __init__(self):
            self.kwargs = None

        def commit_snapshot(self, *args, **kwargs):
            self.kwargs = kwargs
            return 5, 10

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="org", super_name="lake")
    writer.catalog = Catalog()
    table = SimpleNamespace(
        _last_snapshot_leaf={"version": 4, "path": "snap/4.json"},
    )
    raw_pin = json.dumps({"formats": [], "ts": 7}, separators=(",", ":"))

    writer._publish_snapshot(
        simple_table=table,
        simple_name="table",
        payload=_snapshot_payload(schema={}),
        path="snap/5.json",
        base_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        now_ms=123,
        mirrors=[],
        mirror_pin_available=True,
        mirror_pin=raw_pin,
    )

    assert writer.catalog.kwargs["expected_mirror_pin"] == raw_pin
    assert writer.catalog.kwargs["expected_mirrors"] == []


def test_legacy_catalog_adapter_uses_lightweight_quality_fallback():
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    _seed(fake)

    class LegacyAtomicCatalog:
        def __init__(self):
            self.r = fake

        # Deliberately no quality_generation keyword: the capability flag is
        # absent, so DataWriter must retain adapter compatibility.
        def commit_snapshot(
            self,
            org,
            sup,
            simple,
            payload,
            path,
            *,
            expected_version,
            expected_path,
            lock_token,
            commit_id=None,
            mirror_publication=False,
            expected_mirrors=None,
            now_ms=None,
        ):
            assert expected_version == 4
            assert expected_path == "snap/4.json"
            assert lock_token == "token"
            fake.set(
                RK.meta_leaf(org, sup, simple),
                json.dumps({
                    "version": 5,
                    "ts": now_ms,
                    "path": path,
                    "payload": payload,
                    "commit_id": commit_id,
                }),
            )
            fake.set(
                RK.meta_root(org, sup),
                json.dumps({"version": 10, "ts": now_ms}),
            )
            return 5, 10

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="org", super_name="lake")
    writer.catalog = LegacyAtomicCatalog()
    table = SimpleNamespace(
        _last_snapshot_leaf={"version": 4, "path": "snap/4.json"},
    )

    writer._publish_snapshot(
        simple_table=table,
        simple_name="table",
        payload=_snapshot_payload(schema={}),
        path="snap/5.json",
        base_path="snap/4.json",
        lock_token="token",
        commit_id="commit-5",
        now_ms=123,
        notify_quality=True,
    )

    unresolved = RedisCatalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )
    assert fake.get(unresolved) == "commit-5"


def test_catalog_without_atomic_fenced_commit_is_rejected():
    class LegacyCatalog:
        set_leaf_payload_cas = MagicMock()
        bump_root = MagicMock()

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="org", super_name="lake")
    writer.catalog = LegacyCatalog()
    table = SimpleNamespace(_last_snapshot_leaf={"version": 4, "path": "snap/4.json"})

    with pytest.raises(RuntimeError, match="fenced atomic snapshot"):
        writer._publish_snapshot(
            simple_table=table,
            simple_name="table",
            payload={"resources": []},
            path="snap/5.json",
            base_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
            now_ms=123,
        )

    writer.catalog.set_leaf_payload_cas.assert_not_called()
    writer.catalog.bump_root.assert_not_called()


def test_rowid_reservation_recovers_above_snapshot_high_watermark():
    catalog, fake = _catalog()
    _seed(fake)
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, 2)  # Redis was restored behind immutable table data.

    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=100,
        lock_token="token",
    ) == (101, 103)
    assert int(fake.get(seq_key)) == 103
    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=2, floor=50,
        lock_token="token",
    ) == (104, 105)


def test_begin_table_mutation_pins_context_and_reserves_in_one_boundary():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    fake.set(
        RK.meta_table_config("org", "lake", "table"),
        json.dumps({"primary_keys": ["id"], "modified_ms": 7}),
    )
    fake.set(
        RK.meta_mirrors("org", "lake"),
        json.dumps({"formats": ["DELTA", "PARQUET"], "ts": 8}),
    )
    fake.set(RK.meta_rowid_seq("org", "lake", "table"), 3)

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=4,
    )

    assert context["leaf"]["version"] == 4
    assert context["leaf"]["path"] == "snap/4.json"
    assert context["leaf"]["payload"]["rowid_high_watermark"] == 100
    assert context["table_config"] == {
        "primary_keys": ["id"], "modified_ms": 7,
    }
    assert context["mirrors"] == ["DELTA", "PARQUET"]
    assert json.loads(context["mirror_pin"]) == {
        "formats": ["DELTA", "PARQUET"], "ts": 8,
    }
    assert context["rowid_floor"] == 100
    assert context["rowid_reservation"] == (101, 104)
    assert fake.get(RK.meta_rowid_seq("org", "lake", "table")) == "104"


@pytest.mark.parametrize(
    "config_raw",
    [
        '{"deletion_vector_format":2}',
        '{"dv_v2_reader_fleet_confirmed":true}',
        (
            '{"deletion_vector_format":"2",'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":2.0,'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":2e0,'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":true,'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":2,'
            '"dv_v2_reader_fleet_confirmed":1}'
        ),
        (
            '{"deletion_vector_format":2,'
            '"dv_v2_reader_fleet_confirmed":false}'
        ),
        (
            '{"deletion_vector_format":2,'
            '"dv_v2_reader_fleet_confirmed":true,'
            '"deletion_vector_format":2}'
        ),
        (
            '{"deletion_vector_form\\u0061t":2,'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
    ],
)
def test_begin_rejects_nonexact_activation_before_any_catalog_mutation(config_raw):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    config_key = RK.meta_table_config("org", "lake", "table")
    leaf_key = RK.meta_leaf("org", "lake", "table")
    root_key = RK.meta_root("org", "lake")
    rowid_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(config_key, config_raw)
    before = (fake.get(config_key), fake.get(leaf_key), fake.get(root_key))

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.begin_table_mutation(
            "org", "lake", "table", lock_token="token", reserve_count=4,
        )

    assert (fake.get(config_key), fake.get(leaf_key), fake.get(root_key)) == before
    assert not fake.exists(rowid_key)
    assert not fake.exists(RK.meta_table_names("org", "lake"))


def test_begin_python_decoder_repeats_exact_activation_validation(monkeypatch):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    rowid_key = RK.meta_rowid_seq("org", "lake", "table")
    monkeypatch.setattr(
        catalog,
        "_begin_table_mutation",
        lambda **_kwargs: [
            0,
            "",
            '{"deletion_vector_format":2.0,'
            '"dv_v2_reader_fleet_confirmed":true}',
            "",
            "0",
            "",
            "0",
            "",
            "",
            "0",
        ],
    )

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.begin_table_mutation(
            "org", "lake", "table", lock_token="token", reserve_count=4,
        )
    assert not fake.exists(rowid_key)


def test_begin_accepts_only_exact_durable_v2_activation_pair():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    fake.set(
        RK.meta_table_config("org", "lake", "table"),
        '{"deletion_vector_format":2,'
        '"dv_v2_reader_fleet_confirmed":true}',
    )

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=2,
    )

    assert context["table_config"]["deletion_vector_format"] == 2
    assert context["table_config"]["dv_v2_reader_fleet_confirmed"] is True
    assert context["rowid_reservation"] == (101, 102)


@pytest.mark.parametrize("active", [False, True])
def test_begin_table_mutation_lua_accepts_explicit_v2_snapshot_state(active):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    leaf["payload"]["tombstone_format"] = 2
    if active:
        leaf["payload"].update({
            "tombstone": "org/lake/tables/table/tombstone/manifest.json",
            "tombstone_rows": 3,
            "tombstone_digest": "0" * 64,
        })
    fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=2,
    )

    assert context["validated_snapshot"]["tombstone_format"] == 2
    assert context["rowid_floor"] == 100
    assert context["rowid_reservation"] == (101, 102)


@pytest.mark.parametrize(
    "changes",
    [
        {"tombstone_format": None},
        {"tombstone_format": True},
        {"tombstone_format": 3},
        {
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/table/tombstone/deleted.parquet",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone": "org/lake/tables/table/tombstone/manifest.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": "../tombstone/manifest.json",
            "tombstone_rows": 1,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/table/tombstone/manifest.json",
            "tombstone_rows": 1,
            "tombstone_digest": "A" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": "org/lake/tables/table/tombstone/manifest.json",
            "tombstone_rows": 10**14,
            "tombstone_digest": "0" * 64,
        },
        {
            "tombstone_format": 2,
            "tombstone": None,
            "tombstone_rows": 1,
            "tombstone_digest": None,
        },
    ],
)
def test_begin_table_mutation_lua_rejects_malformed_v2_hybrids(
        monkeypatch, changes,
):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    leaf["payload"].update(changes)
    fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))

    # Isolate the Lua decision.  Even if the post-command Python validator
    # were accidentally permissive, malformed v2 state must not authorize the
    # cached floor or mutate the row-ID allocator.
    monkeypatch.setattr(
        "supertable.redis_catalog.complete_snapshot_payload",
        lambda *_args, **_kwargs: leaf["payload"],
    )
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=2,
    )

    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_prepared_mutation_leaf_reuses_exact_validated_payload(monkeypatch):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None

    # An exact pin must return the object already validated while preparing
    # the leaf, rather than validating the resource tree again.
    monkeypatch.setattr(
        "supertable.redis_catalog.complete_snapshot_payload",
        MagicMock(side_effect=AssertionError("prepared snapshot validated twice")),
    )
    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        reserve_count=4,
        prepared_leaf=pin,
    )

    assert context["leaf"]["path"] == "snap/4.json"
    assert context["validated_snapshot"] is context["leaf"]["payload"]
    assert context["rowid_reservation"] == (101, 104)


def test_prepared_mutation_leaf_falls_back_on_any_raw_leaf_change():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake, floor=100)
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None

    replacement = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    replacement["version"] = 5
    replacement["path"] = "snap/5.json"
    replacement["payload"]["snapshot_version"] = 5
    replacement["payload"]["rowid_high_watermark"] = 500
    # Different whitespace alone is sufficient to defeat the byte pin; the
    # semantic changes prove that the ordinary validator selected live state.
    fake.set(
        RK.meta_leaf("org", "lake", "table"),
        json.dumps(replacement, indent=2, sort_keys=True),
    )

    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        reserve_count=4,
        prepared_leaf=pin,
    )

    assert context["leaf"]["version"] == 5
    assert context["leaf"]["path"] == "snap/5.json"
    assert context["rowid_floor"] == 500
    assert context["rowid_reservation"] == (501, 504)


def test_prepared_mutation_leaf_is_owner_bound_and_one_shot():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    other, _other_fake = _catalog()
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None

    with pytest.raises(ValueError, match="another catalog"):
        other.begin_table_mutation(
            "org", "lake", "table", lock_token="token", prepared_leaf=pin,
        )

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", prepared_leaf=pin,
    )
    assert context["leaf"]["version"] == 4
    with pytest.raises(ValueError, match="already been consumed"):
        catalog.begin_table_mutation(
            "org", "lake", "table", lock_token="token", prepared_leaf=pin,
        )


def test_prepared_mutation_leaf_reply_loss_cannot_be_replayed(monkeypatch):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None
    original = catalog._begin_table_mutation

    def commit_then_lose_reply(*, keys, args):
        original(keys=keys, args=args)
        raise redis.ConnectionError("reply lost after rowid reservation")

    monkeypatch.setattr(
        catalog, "_begin_table_mutation", commit_then_lose_reply,
    )
    with pytest.raises(redis.ConnectionError, match="reply lost"):
        catalog.begin_table_mutation(
            "org",
            "lake",
            "table",
            lock_token="token",
            reserve_count=4,
            prepared_leaf=pin,
        )
    assert fake.get(RK.meta_rowid_seq("org", "lake", "table")) == "104"

    monkeypatch.setattr(catalog, "_begin_table_mutation", original)
    with pytest.raises(ValueError, match="already been consumed"):
        catalog.begin_table_mutation(
            "org",
            "lake",
            "table",
            lock_token="token",
            reserve_count=4,
            prepared_leaf=pin,
        )
    assert fake.get(RK.meta_rowid_seq("org", "lake", "table")) == "104"


def test_prepared_mutation_leaf_keeps_large_int64_floor_on_exact_fallback():
    catalog, fake = _catalog()
    floor = (1 << 53) + 17
    _seed_current_snapshot(fake, floor=floor)
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None

    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        reserve_count=3,
        prepared_leaf=pin,
    )

    assert context["validated_snapshot"]["rowid_high_watermark"] == floor
    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))
    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=floor,
        lock_token="token",
    ) == (floor + 1, floor + 3)


def test_prepared_mutation_leaf_never_trusts_corrupt_cached_floor():
    catalog, fake = _catalog()
    _seed_current_snapshot(fake, floor=100)
    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    leaf["payload"]["tombstone_rows"] = -1
    fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))
    pin = catalog.prepare_table_mutation_leaf("org", "lake", "table")
    assert pin is not None

    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        reserve_count=3,
        prepared_leaf=pin,
    )

    assert "validated_snapshot" not in context
    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_begin_table_mutation_returns_absent_leaf_without_creating_state():
    catalog, fake = _catalog()
    _seed(fake)
    fake.delete(RK.meta_leaf("org", "lake", "table"))

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=4,
    )

    assert context == {
        "leaf": None,
        "table_config": {},
        "mirrors": [],
        "mirror_pin": None,
        "rowid_floor": None,
        "rowid_reservation": None,
    }
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_namespace_fenced_begin_reserves_first_ids_without_creating_leaf():
    catalog, fake = _catalog()
    _seed(fake)
    fake.delete(RK.meta_leaf("org", "lake", "table"))
    fake.set(RK.lock_namespace("org", "lake"), "namespace-token", ex=30)
    fake.set(RK.meta_rowid_seq("org", "lake", "table"), "7")

    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        namespace_token="namespace-token",
        reserve_count=4,
    )

    assert context["leaf"] is None
    assert context["rowid_floor"] == 7
    assert context["rowid_reservation"] == (8, 11)
    assert fake.get(RK.meta_rowid_seq("org", "lake", "table")) == "11"
    assert fake.get(RK.meta_leaf("org", "lake", "table")) is None


def test_namespace_fenced_begin_rejects_lost_creation_lock_before_reserving():
    catalog, fake = _catalog()
    _seed(fake)
    fake.delete(RK.meta_leaf("org", "lake", "table"))
    fake.set(RK.lock_namespace("org", "lake"), "new-owner", ex=30)

    with pytest.raises(LockLostError, match="namespace creation lock"):
        catalog.begin_table_mutation(
            "org",
            "lake",
            "table",
            lock_token="token",
            namespace_token="stale-owner",
            reserve_count=4,
        )

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_crash_after_first_id_reservation_leaves_no_discoverable_table():
    catalog, fake = _catalog()
    _seed(fake)
    fake.delete(RK.meta_leaf("org", "lake", "table"))
    fake.set(RK.lock_namespace("org", "lake"), "namespace-token", ex=30)

    context = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        namespace_token="namespace-token",
        reserve_count=4,
    )
    assert context["rowid_reservation"] == (1, 4)

    # Model process death before any final snapshot commit.  Only an invisible
    # sequence gap may remain; readers/index scans cannot discover the table.
    assert fake.get(RK.meta_rowid_seq("org", "lake", "table")) == "4"
    assert fake.get(RK.meta_leaf("org", "lake", "table")) is None
    assert not fake.sismember(RK.meta_table_names("org", "lake"), "table")
    assert fake.get(RK.schema("org", "lake", "table")) is None
    assert fake.get(catalog._quality_key(
        "org", "lake", "pending_unresolved", "table",
    )) is None

    retry = catalog.begin_table_mutation(
        "org",
        "lake",
        "table",
        lock_token="token",
        namespace_token="namespace-token",
        reserve_count=2,
    )
    assert retry["rowid_floor"] == 4
    assert retry["rowid_reservation"] == (5, 6)


@pytest.mark.parametrize(
    "mutation,error_type",
    [
        ("lost-lock", LockLostError),
        ("namespace-delete", DeletionIntentConflictError),
        ("simple-delete", DeletionIntentConflictError),
        ("missing-root", FileNotFoundError),
        ("corrupt-root", RuntimeError),
        ("read-only", PermissionError),
    ],
)
def test_begin_table_mutation_fails_before_rowid_side_effects(
        mutation, error_type,
):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    if mutation == "lost-lock":
        fake.set(RK.lock_leaf("org", "lake", "table"), "new-owner")
    elif mutation == "namespace-delete":
        fake.set(RK.meta_namespace_deletion_intent("org", "lake"), "pending")
    elif mutation == "simple-delete":
        fake.set(
            RK.meta_simple_deletion_intent("org", "lake", "table"),
            "pending",
        )
    elif mutation == "missing-root":
        fake.delete(RK.meta_root("org", "lake"))
    elif mutation == "corrupt-root":
        fake.set(RK.meta_root("org", "lake"), "[]")
    elif mutation == "read-only":
        fake.set(
            RK.meta_root("org", "lake"),
            json.dumps({
                "version": 9,
                "ts": 1,
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": "source",
            }),
        )
    with pytest.raises(error_type):
        catalog.begin_table_mutation(
            "org", "lake", "table", lock_token="token", reserve_count=4,
        )

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


@pytest.mark.parametrize("corruption", ["leaf", "config", "mirrors", "snapshot"])
def test_begin_table_mutation_rejects_corruption_without_allocating_ids(
        corruption,
):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake)
    if corruption == "leaf":
        fake.set(RK.meta_leaf("org", "lake", "table"), "[]")
    elif corruption == "config":
        fake.set(RK.meta_table_config("org", "lake", "table"), "[]")
    elif corruption == "mirrors":
        fake.set(
            RK.meta_mirrors("org", "lake"),
            json.dumps({"formats": ["DELTA", "DELTA"], "ts": 1}),
        )
    else:
        leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
        leaf["payload"]["tombstone_rows"] = -1
        fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))

    if corruption == "snapshot":
        # The leaf identity remains valid, so the conservative fast path falls
        # back without trusting or mutating from its corrupt cache floor.
        context = catalog.begin_table_mutation(
            "org", "lake", "table", lock_token="token", reserve_count=4,
        )
        assert context["rowid_floor"] is None
        assert context["rowid_reservation"] is None
    else:
        with pytest.raises((RuntimeError, ValueError)):
            catalog.begin_table_mutation(
                "org", "lake", "table", lock_token="token", reserve_count=4,
            )
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_begin_table_mutation_large_int64_floor_uses_exact_fallback():
    catalog, fake = _catalog()
    floor = (1 << 53) + 17
    _seed_current_snapshot(fake, floor=floor)

    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=3,
    )

    assert context["leaf"]["payload"]["rowid_high_watermark"] == floor
    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))
    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=floor,
        lock_token="token",
    ) == (floor + 1, floor + 3)


def test_begin_table_mutation_observes_one_atomic_race_winner(monkeypatch):
    catalog, fake = _catalog()
    _seed_current_snapshot(fake, floor=10)
    original = catalog._begin_table_mutation

    def race_then_begin(*, keys, args):
        leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
        leaf["version"] = 5
        leaf["path"] = "snap/5.json"
        leaf["payload"]["snapshot_version"] = 5
        leaf["payload"]["rowid_high_watermark"] = 20
        fake.set(RK.meta_leaf("org", "lake", "table"), json.dumps(leaf))
        fake.set(
            RK.meta_table_config("org", "lake", "table"),
            json.dumps({"primary_keys": ["new"]}),
        )
        fake.set(
            RK.meta_mirrors("org", "lake"),
            json.dumps({"formats": ["ICEBERG"], "ts": 2}),
        )
        return original(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_begin_table_mutation", race_then_begin)
    context = catalog.begin_table_mutation(
        "org", "lake", "table", lock_token="token", reserve_count=2,
    )

    assert context["leaf"]["version"] == 5
    assert context["leaf"]["path"] == "snap/5.json"
    assert context["table_config"] == {"primary_keys": ["new"]}
    assert context["mirrors"] == ["ICEBERG"]
    assert context["rowid_reservation"] == (21, 22)


def test_rowid_reservation_is_exact_above_double_precision_boundary():
    catalog, fake = _catalog()
    _seed(fake)
    boundary = (1 << 53) + 17

    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=boundary,
        lock_token="token",
    ) == (boundary + 1, boundary + 3)
    assert int(fake.get(RK.meta_rowid_seq("org", "lake", "table"))) == boundary + 3


def test_rowid_reservation_rejects_signed_int64_overflow():
    catalog, fake = _catalog()
    _seed(fake)
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, (1 << 63) - 1)

    with pytest.raises(Exception, match="overflow|increment|range"):
        catalog.reserve_rowids_at_least(
            "org", "lake", "table", count=1, floor=0,
            lock_token="token",
        )


def test_rowid_reservation_rejects_corrupt_negative_counter():
    catalog, fake = _catalog()
    _seed(fake)
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, -1)

    with pytest.raises(Exception, match="non-negative|rowid|sequence"):
        catalog.reserve_rowids_at_least(
            "org", "lake", "table", count=1, floor=0,
            lock_token="token",
        )

    # A corrupt allocator must not be advanced into the valid id namespace.
    assert fake.get(seq_key) == "-1"


@pytest.mark.parametrize(
    "count,floor",
    [(True, 0), (1.0, 0), (1, False), (1, 0.0), (1, "0")],
)
def test_rowid_reservation_rejects_noninteger_arguments(count, floor):
    catalog, fake = _catalog()
    _seed(fake)

    with pytest.raises(TypeError, match="must be integers"):
        catalog.reserve_rowids_at_least(
            "org",
            "lake",
            "table",
            count=count,
            floor=floor,
            lock_token="token",
        )

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


def test_stale_writer_cannot_recreate_rowid_state_after_delete_and_clear():
    catalog, fake = _catalog()
    _seed(fake, token="stale-writer")
    fake.sadd(RK.meta_table_names("org", "lake"), "table")

    # The writer pinned this incarnation, then its lease expired. A deleter
    # acquires both locks and fully finalizes the table lifecycle.
    fake.delete(RK.lock_leaf("org", "lake", "table"))
    fake.set(RK.lock_leaf("org", "lake", "table"), "deleter")
    fake.set(RK.lock_namespace("org", "lake"), "namespace")
    intent = catalog.begin_simple_deletion(
        "org",
        "lake",
        "table",
        namespace_token="namespace",
        lock_token="deleter",
        intent_id="delete-rowids",
    )
    assert catalog.delete_simple_table(
        "org",
        "lake",
        "table",
        namespace_token="namespace",
        lock_token="deleter",
        intent_id=intent["intent_id"],
    )
    catalog.clear_simple_deletion_tombstone(
        "org",
        "lake",
        "table",
        expected_intent_id=intent["intent_id"],
        namespace_token="namespace",
        lock_token="deleter",
        confirm_previous_owner_stopped=True,
    )

    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    assert not fake.exists(seq_key)
    with pytest.raises(LockLostError):
        catalog.reserve_rowids_at_least(
            "org",
            "lake",
            "table",
            count=1,
            floor=100,
            lock_token="stale-writer",
        )
    assert not fake.exists(seq_key)

    # Even if a stale token is reintroduced externally, the missing leaf
    # prevents the allocator from recreating table-scoped state.
    fake.set(RK.lock_leaf("org", "lake", "table"), "stale-writer")
    with pytest.raises(FileNotFoundError, match="SimpleTable does not exist"):
        catalog.reserve_rowids_at_least(
            "org",
            "lake",
            "table",
            count=1,
            floor=100,
            lock_token="stale-writer",
        )
    assert not fake.exists(seq_key)


@pytest.mark.parametrize(
    "intent_key",
    [
        RK.meta_namespace_deletion_intent("org", "lake"),
        RK.meta_simple_deletion_intent("org", "lake", "table"),
    ],
)
def test_rowid_reservation_is_fenced_by_deletion_intents(intent_key):
    catalog, fake = _catalog()
    _seed(fake)
    fake.set(intent_key, "pending")

    with pytest.raises(DeletionIntentConflictError):
        catalog.reserve_rowids_at_least(
            "org",
            "lake",
            "table",
            count=1,
            floor=0,
            lock_token="token",
        )

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


@pytest.mark.parametrize(
    "catalog_state,error_type",
    [
        ("missing-root", FileNotFoundError),
        ("corrupt-root", RuntimeError),
        ("missing-leaf", FileNotFoundError),
        ("corrupt-leaf", RuntimeError),
    ],
)
def test_rowid_reservation_requires_valid_live_catalog(catalog_state, error_type):
    catalog, fake = _catalog()
    _seed(fake)
    if catalog_state == "missing-root":
        fake.delete(RK.meta_root("org", "lake"))
    elif catalog_state == "corrupt-root":
        fake.set(RK.meta_root("org", "lake"), "[]")
    elif catalog_state == "missing-leaf":
        fake.delete(RK.meta_leaf("org", "lake", "table"))
    else:
        fake.set(RK.meta_leaf("org", "lake", "table"), "[]")

    with pytest.raises(error_type):
        catalog.reserve_rowids_at_least(
            "org",
            "lake",
            "table",
            count=1,
            floor=0,
            lock_token="token",
        )

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))


@pytest.mark.parametrize("state", ["absent", "deleting"])
def test_legacy_rowid_allocator_is_retired_without_mutating_catalog(state):
    catalog, fake = _catalog()
    if state == "deleting":
        _seed(fake)
        fake.set(
            RK.meta_simple_deletion_intent("org", "lake", "table"),
            json.dumps({"status": "deleting"}),
        )

    with pytest.raises(RuntimeError, match="reserve_rowids is retired"):
        catalog.reserve_rowids("org", "lake", "table", count=1)

    assert not fake.exists(RK.meta_rowid_seq("org", "lake", "table"))
