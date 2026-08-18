import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

from supertable import redis_keys as RK
from supertable.data_writer import DataWriter
from supertable.errors import LockLostError, SnapshotCommitConflictError
from supertable.redis_catalog import RedisCatalog


def _catalog():
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    connector = SimpleNamespace(r=fake)
    with patch("supertable.redis_catalog.RedisConnector", return_value=connector):
        catalog = RedisCatalog()
    return catalog, fake


def _seed(fake, *, token="token", version=4, path="snap/4.json"):
    fake.set(
        RK.meta_leaf("org", "lake", "table"),
        json.dumps({"version": version, "path": path, "payload": {"resources": []}}),
    )
    fake.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf("org", "lake", "table"), token, ex=30)


def test_snapshot_commit_atomically_updates_leaf_and_root():
    catalog, fake = _catalog()
    _seed(fake)

    assert catalog.commit_snapshot(
        "org", "lake", "table", {"resources": [{"file": "f"}]}, "snap/5.json",
        expected_version=4, expected_path="snap/4.json", lock_token="token",
        commit_id="commit-5", now_ms=123,
    ) == (5, 10)

    leaf = json.loads(fake.get(RK.meta_leaf("org", "lake", "table")))
    root = json.loads(fake.get(RK.meta_root("org", "lake")))
    assert leaf == {
        "version": 5,
        "ts": 123,
        "path": "snap/5.json",
        "payload": {"resources": [{"file": "f"}]},
        "commit_id": "commit-5",
    }
    assert root["version"] == 10
    assert root["commit_id"] == "commit-5"
    assert root["read_only"] is False


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
        "org", "lake", "table", {"resources": [{"file": "f"}]},
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
            "org", "lake", "table", {}, "snap/5.json",
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
        "org", "lake", "table", {}, "snap/5.json",
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
        "org", "lake", "table", {}, "snap/5.json",
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
        "org", "lake", "table", {}, "snap/5.json",
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
            "org", "lake", "table", {}, "snap/5.json",
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
    catalog.recover_simple_deletion(
        "org", "lake", "table",
        expected_intent_id="delete-1",
        namespace_token="new-namespace",
        lock_token="new-token",
        confirm_previous_owner_stopped=True,
    )
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
        json.dumps({"formats": ["PARQUE"]}),
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
    monkeypatch.setattr(catalog, "scan_leaf_keys", keys)

    with pytest.raises(SnapshotCommitConflictError, match="Catalog changed"):
        list(catalog.scan_leaf_items("org", "lake", count=1))


def test_snapshot_commit_rejects_stale_base_without_changing_catalog():
    catalog, fake = _catalog()
    _seed(fake)
    before_leaf = fake.get(RK.meta_leaf("org", "lake", "table"))
    before_root = fake.get(RK.meta_root("org", "lake"))

    with pytest.raises(SnapshotCommitConflictError):
        catalog.commit_snapshot(
            "org", "lake", "table", {}, "snap/stale.json",
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
            "org", "lake", "table", {}, "snap/5.json",
            expected_version=4, expected_path="snap/4.json", lock_token="old-owner",
        )

    assert fake.get(RK.meta_leaf("org", "lake", "table")) == before_leaf


def test_ambiguous_atomic_commit_error_is_never_retried_as_path_only():
    class Catalog:
        def commit_snapshot(self, *args, **kwargs):
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
            payload={"resources": []},
            path="snap/5.json",
            base_path="snap/4.json",
            lock_token="token",
            commit_id="commit-5",
            now_ms=123,
        )

    writer.catalog.set_leaf_payload_cas.assert_not_called()
    writer.catalog.set_leaf_path_cas.assert_not_called()
    writer.catalog.bump_root.assert_not_called()


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
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, 2)  # Redis was restored behind immutable table data.

    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=100
    ) == (101, 103)
    assert int(fake.get(seq_key)) == 103
    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=2, floor=50
    ) == (104, 105)


def test_rowid_reservation_is_exact_above_double_precision_boundary():
    catalog, fake = _catalog()
    boundary = (1 << 53) + 17

    assert catalog.reserve_rowids_at_least(
        "org", "lake", "table", count=3, floor=boundary
    ) == (boundary + 1, boundary + 3)
    assert int(fake.get(RK.meta_rowid_seq("org", "lake", "table"))) == boundary + 3


def test_rowid_reservation_rejects_signed_int64_overflow():
    catalog, fake = _catalog()
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, (1 << 63) - 1)

    with pytest.raises(Exception, match="overflow|increment|range"):
        catalog.reserve_rowids_at_least(
            "org", "lake", "table", count=1, floor=0
        )


def test_rowid_reservation_rejects_corrupt_negative_counter():
    catalog, fake = _catalog()
    seq_key = RK.meta_rowid_seq("org", "lake", "table")
    fake.set(seq_key, -1)

    with pytest.raises(Exception, match="non-negative|rowid|sequence"):
        catalog.reserve_rowids_at_least(
            "org", "lake", "table", count=1, floor=0
        )

    # A corrupt allocator must not be advanced into the valid id namespace.
    assert fake.get(seq_key) == "-1"
