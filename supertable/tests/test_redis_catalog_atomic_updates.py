from __future__ import annotations

import json
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

from supertable import redis_keys as RK
from supertable.data_writer import DataWriter
from supertable.errors import SnapshotCommitConflictError
from supertable.redis_catalog import (
    DeletionIntentConflictError,
    ReadOnlyCatalogError,
    RedisCatalog,
)


class _FirstReadBarrierPipeline:
    """Synchronize the first watched read without changing Redis semantics."""

    def __init__(self, inner, barrier: threading.Barrier, watched_key: str):
        self._inner = inner
        self._barrier = barrier
        self._watched_key = watched_key
        self._waited = False

    def get(self, key):
        value = self._inner.get(key)
        if key == self._watched_key and not self._waited:
            self._waited = True
            self._barrier.wait(timeout=5)
        return value

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _catalog() -> tuple[RedisCatalog, fakeredis.FakeStrictRedis]:
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return RedisCatalog(redis_client=client), client


def _seed_root(client) -> None:
    client.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )


def _coordinate_first_reads(monkeypatch, client, key: str) -> None:
    original_pipeline = client.pipeline
    barrier = threading.Barrier(2)

    def pipeline(*args, **kwargs):
        return _FirstReadBarrierPipeline(
            original_pipeline(*args, **kwargs), barrier, key,
        )

    monkeypatch.setattr(client, "pipeline", pipeline)


def _run_concurrently(*operations):
    results = []
    errors = []

    def invoke(operation):
        try:
            results.append(operation())
        except BaseException as exc:  # surfaced by the assertions below
            errors.append(exc)

    threads = [threading.Thread(target=invoke, args=(operation,)) for operation in operations]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    assert errors == []
    return results


class _SerializedConfigCatalog:
    """Minimal catalog that exposes pre-lock table-config reads as a race."""

    def __init__(self):
        self.config: dict[str, int] = {}
        self._arrivals = threading.Barrier(2)
        self._lease = threading.Lock()
        self._owner: str | None = None

    def acquire_simple_lock(self, org, sup, simple, ttl_s=30, timeout_s=60):
        # Both callers must reach lock acquisition before either proceeds.  An
        # implementation that reads before acquiring therefore deterministically
        # gives both callers the same stale base document.
        self._arrivals.wait(timeout=5)
        assert self._lease.acquire(timeout=5)
        token = threading.current_thread().name
        self._owner = token
        return token

    def release_simple_lock(self, org, sup, simple, token):
        assert self._owner == token
        self._owner = None
        self._lease.release()
        return True

    def get_table_config(self, org, sup, simple):
        return dict(self.config)

    def set_table_config(self, org, sup, simple, config, *, lock_token):
        assert self._owner == lock_token
        self.config = dict(config)
        return True


class _LockAwareCache(dict):
    def __init__(self, catalog: _SerializedConfigCatalog):
        super().__init__()
        self.catalog = catalog
        self.writes_under_owner: list[bool] = []

    def __setitem__(self, key, value):
        self.writes_under_owner.append(
            self.catalog._owner == threading.current_thread().name
        )
        super().__setitem__(key, value)


def _config_writer(catalog) -> DataWriter:
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(organization="acme", super_name="lake")
    writer.catalog = catalog
    writer._table_config_cache = _LockAwareCache(catalog)
    return writer


def test_concurrent_partial_table_config_updates_preserve_both_fields():
    catalog = _SerializedConfigCatalog()
    writer_a = _config_writer(catalog)
    writer_b = _config_writer(catalog)

    with patch("supertable.data_writer.check_write_access"):
        _run_concurrently(
            lambda: writer_a.configure_table(
                "admin", "orders", max_overlapping_files=12,
            ),
            lambda: writer_b.configure_table(
                "admin", "orders", max_tombstone_rows=34,
            ),
        )

    assert catalog.config == {
        "max_overlapping_files": 12,
        "max_tombstone_rows": 34,
    }
    assert writer_a._table_config_cache.writes_under_owner == [True]
    assert writer_b._table_config_cache.writes_under_owner == [True]
    # Each long-lived writer refreshes from the shared authoritative document,
    # rather than retaining the state of its own earlier partial update.
    assert writer_a._get_table_config("orders") == catalog.config
    assert writer_b._get_table_config("orders") == catalog.config


def test_configure_table_linearizes_at_lock_after_same_name_recreation():
    class RecreatedCatalog:
        def __init__(self):
            self.config = {"incarnation": 1, "max_tombstone_rows": 10}
            self._owner = None

        def acquire_simple_lock(self, *_args, **_kwargs):
            # Delete/recreate wins before this call obtains its lease. No state
            # from incarnation 1 may be merged into the replacement.
            self.config = {"incarnation": 2, "max_tombstone_rows": 20}
            self._owner = "new-incarnation-owner"
            return self._owner

        def release_simple_lock(self, *_args):
            self._owner = None
            return True

        def get_table_config(self, *_args):
            return dict(self.config)

        def set_table_config(self, *_args, lock_token, **_kwargs):
            assert lock_token == self._owner
            self.config = dict(_args[-1])
            return True

    catalog = RecreatedCatalog()
    writer = _config_writer(catalog)
    writer._table_config_cache["orders"] = dict(catalog.config)
    with patch("supertable.data_writer.check_write_access"):
        writer.configure_table(
            "admin", "orders", max_overlapping_files=12,
        )

    assert catalog.config == {
        "incarnation": 2,
        "max_tombstone_rows": 20,
        "max_overlapping_files": 12,
    }


@pytest.mark.parametrize("field", ["version", "ts"])
def test_root_identity_above_lua_exact_range_is_rejected_without_mutation(field):
    catalog, client = _catalog()
    document = {"version": 1, "ts": 1}
    document[field] = 1 << 53
    raw = json.dumps(document)
    key = RK.meta_root("acme", "lake")
    client.set(key, raw)

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.bump_root("acme", "lake", now_ms=2)

    assert client.get(key) == raw


def test_root_version_at_lua_exact_ceiling_cannot_be_incremented():
    catalog, client = _catalog()
    key = RK.meta_root("acme", "lake")
    raw = json.dumps({"version": (1 << 53) - 1, "ts": 1})
    client.set(key, raw)

    with pytest.raises(RuntimeError, match="numeric identity is exhausted"):
        catalog.bump_root("acme", "lake", now_ms=2)

    assert client.get(key) == raw


@pytest.mark.parametrize("invalid", [True, -1, 1 << 53])
def test_public_lifecycle_writers_reject_non_lua_safe_timestamps(invalid):
    catalog, client = _catalog()
    _seed_root(client)
    namespace_token = catalog.acquire_namespace_lock(
        "acme", "lake", ttl_s=30, timeout_s=1,
    )
    assert namespace_token

    with pytest.raises(ValueError, match="publication timestamp"):
        catalog.begin_namespace_deletion(
            "acme", "lake",
            namespace_token=namespace_token,
            now_ms=invalid,
        )
    with pytest.raises(ValueError, match="publication timestamp"):
        catalog.set_mirrors("acme", "lake", ["DELTA"], now_ms=invalid)

    assert not client.exists(
        RK.meta_namespace_deletion_intent("acme", "lake"),
    )
    assert not client.exists(RK.meta_mirrors("acme", "lake"))
    catalog.release_namespace_lock(
        "acme", "lake", namespace_token,
    )


@pytest.mark.parametrize("field", ["version", "ts"])
def test_snapshot_commit_rejects_leaf_identity_above_lua_exact_range(field):
    catalog, client = _catalog()
    _seed_root(client)
    leaf_key = RK.meta_leaf("acme", "lake", "orders")
    leaf = {"version": 0, "ts": 1, "path": "snapshots/v0.json"}
    leaf[field] = 1 << 53
    leaf_raw = json.dumps(leaf)
    root_raw = client.get(RK.meta_root("acme", "lake"))
    client.set(leaf_key, leaf_raw)
    client.set(RK.lock_leaf("acme", "lake", "orders"), "owner")

    with pytest.raises(RuntimeError, match="Corrupt Redis catalog JSON"):
        catalog.commit_snapshot(
            "acme", "lake", "orders",
            {"resources": [], "_row_filter": None},
            "snapshots/v1.json",
            expected_version=0,
            expected_path="snapshots/v0.json",
            lock_token="owner",
            now_ms=2,
        )

    assert client.get(leaf_key) == leaf_raw
    assert client.get(RK.meta_root("acme", "lake")) == root_raw


def test_snapshot_commit_cannot_increment_root_at_lua_exact_ceiling():
    catalog, client = _catalog()
    root_key = RK.meta_root("acme", "lake")
    root_raw = json.dumps({"version": (1 << 53) - 1, "ts": 1})
    leaf_key = RK.meta_leaf("acme", "lake", "orders")
    leaf_raw = json.dumps({
        "version": 0,
        "ts": 1,
        "path": "snapshots/v0.json",
    })
    client.set(root_key, root_raw)
    client.set(leaf_key, leaf_raw)
    client.set(RK.lock_leaf("acme", "lake", "orders"), "owner")

    with pytest.raises(RuntimeError, match="numeric identity is exhausted"):
        catalog.commit_snapshot(
            "acme", "lake", "orders",
            {"resources": [], "_row_filter": None},
            "snapshots/v1.json",
            expected_version=0,
            expected_path="snapshots/v0.json",
            lock_token="owner",
            now_ms=2,
        )

    assert client.get(root_key) == root_raw
    assert client.get(leaf_key) == leaf_raw


@pytest.mark.parametrize("operation", ["get_leaf", "leaf_exists"])
def test_replica_leaf_read_atomically_observes_source_deletion_intent(
    monkeypatch, operation,
):
    catalog, client = _catalog()
    client.set(
        RK.meta_root("acme", "source"),
        json.dumps({"version": 1, "ts": 1}),
    )
    client.set(
        RK.meta_leaf("acme", "source", "orders"),
        json.dumps({
            "version": 1,
            "ts": 1,
            "path": "snapshots/source-v1.json",
        }),
    )
    client.set(
        RK.meta_root("acme", "replica"),
        json.dumps({
            "version": 1,
            "ts": 1,
            "read_only": True,
            "clone_type": "replica",
            "cloned_from": "source",
            "replica_tables": ["orders"],
        }),
    )
    original_resolve = catalog._resolve_replica_info

    def begin_delete_after_resolution(org, sup):
        info = original_resolve(org, sup)
        client.set(
            RK.meta_namespace_deletion_intent("acme", "source"),
            "deleting",
        )
        return info

    monkeypatch.setattr(
        catalog, "_resolve_replica_info", begin_delete_after_resolution,
    )
    with pytest.raises(
        DeletionIntentConflictError, match="source is fenced",
    ):
        getattr(catalog, operation)("acme", "replica", "orders")


def test_concurrent_mirror_additions_preserve_both_acknowledged_changes(monkeypatch):
    catalog, client = _catalog()
    _seed_root(client)
    key = RK.meta_mirrors("acme", "lake")
    _coordinate_first_reads(monkeypatch, client, key)

    results = _run_concurrently(
        lambda: catalog.enable_mirror("acme", "lake", "DELTA"),
        lambda: catalog.enable_mirror("acme", "lake", "ICEBERG"),
    )

    assert len(results) == 2
    assert set(catalog.get_mirrors("acme", "lake")) == {"DELTA", "ICEBERG"}


def test_concurrent_mirror_enable_and_disable_preserve_both_mutations(monkeypatch):
    catalog, client = _catalog()
    _seed_root(client)
    key = RK.meta_mirrors("acme", "lake")
    catalog.set_mirrors("acme", "lake", ["DELTA"])
    _coordinate_first_reads(monkeypatch, client, key)

    results = _run_concurrently(
        lambda: catalog.disable_mirror("acme", "lake", "DELTA"),
        lambda: catalog.enable_mirror("acme", "lake", "ICEBERG"),
    )

    assert len(results) == 2
    assert catalog.get_mirrors("acme", "lake") == ["ICEBERG"]


def test_snapshot_commit_rejects_mirror_enable_after_writer_config_read():
    catalog, client = _catalog()
    _seed_root(client)
    leaf_key = RK.meta_leaf("acme", "lake", "orders")
    leaf_raw = json.dumps({
        "version": 0,
        "ts": 1,
        "path": "snapshots/v0.json",
    })
    root_key = RK.meta_root("acme", "lake")
    root_raw = client.get(root_key)
    client.set(leaf_key, leaf_raw)
    client.set(RK.lock_leaf("acme", "lake", "orders"), "owner")

    observed_mirrors = catalog.get_mirrors("acme", "lake")
    assert observed_mirrors == []
    catalog.enable_mirror("acme", "lake", "DELTA")

    with pytest.raises(
        SnapshotCommitConflictError, match="Mirror configuration changed",
    ):
        catalog.commit_snapshot(
            "acme", "lake", "orders",
            {"resources": [], "_row_filter": None},
            "snapshots/v1.json",
            expected_version=0,
            expected_path="snapshots/v0.json",
            lock_token="owner",
            expected_mirrors=observed_mirrors,
            now_ms=2,
        )

    assert client.get(leaf_key) == leaf_raw
    assert client.get(root_key) == root_raw


@pytest.mark.parametrize("operation", ["set", "enable", "disable"])
@pytest.mark.parametrize(
    "root_value, error_type",
    [
        (None, FileNotFoundError),
        ("[]", RuntimeError),
        ("{}", RuntimeError),
        ('{"version": 0, "ts": "invalid"}', RuntimeError),
    ],
)
def test_mirror_mutations_require_valid_live_root(
    operation, root_value, error_type,
):
    catalog, client = _catalog()
    if root_value is not None:
        client.set(RK.meta_root("acme", "lake"), root_value)

    with pytest.raises(error_type):
        if operation == "set":
            catalog.set_mirrors("acme", "lake", ["DELTA"])
        elif operation == "enable":
            catalog.enable_mirror("acme", "lake", "DELTA")
        else:
            catalog.disable_mirror("acme", "lake", "DELTA")

    assert not client.exists(RK.meta_mirrors("acme", "lake"))


def test_mirror_mutation_does_not_replace_malformed_persisted_config():
    catalog, client = _catalog()
    _seed_root(client)
    key = RK.meta_mirrors("acme", "lake")
    client.set(key, "[]")

    with pytest.raises(ValueError, match="Persisted mirror configuration"):
        catalog.enable_mirror("acme", "lake", "DELTA")

    assert client.get(key) == "[]"


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"formats": [], "ts": True},
        {"formats": ["DELTA", "DELTA"], "ts": 1},
    ],
)
def test_mirror_reads_reject_state_runtime_or_dr_cannot_preserve(document):
    catalog, client = _catalog()
    client.set(RK.meta_mirrors("acme", "lake"), json.dumps(document))

    with pytest.raises(ValueError, match="Mirror configuration"):
        catalog.get_mirrors("acme", "lake")


def test_namespace_scoped_mutations_cannot_recreate_state_without_root():
    catalog, client = _catalog()
    lock_key = RK.lock_leaf("acme", "lake", "orders")
    client.set(lock_key, "owner")

    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.check_table_mutation_allowed(
            "acme", "lake", "orders", lock_token="owner",
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.set_leaf_path_cas(
            "acme", "lake", "orders", "snapshots/bootstrap.json",
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.set_leaf_payload_cas(
            "acme", "lake", "orders", {"resources": []},
            "snapshots/bootstrap.json",
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.commit_snapshot(
            "acme", "lake", "orders", {"resources": []},
            "snapshots/v0.json", expected_version=-1, expected_path="",
            lock_token="owner", commit_id="stale-commit",
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.update_root_flags("acme", "lake", {"read_only": True})
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.bump_root("acme", "lake")
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.create_linked_share(
            "acme", "lake", "late", {"id": "late"},
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.update_linked_share(
            "acme", "lake", "late", {"id": "late"},
        )
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.delete_linked_share("acme", "lake", "late")

    assert not client.exists(RK.meta_root("acme", "lake"))
    assert not client.exists(RK.meta_leaf("acme", "lake", "orders"))
    assert not client.exists(RK.meta_table_names("acme", "lake"))
    assert not client.exists(RK.schema("acme", "lake", "orders"))
    assert not client.exists(RK.linked_share_doc("acme", "lake", "late"))
    assert not client.exists(RK.linked_share_index("acme", "lake"))


@pytest.mark.parametrize("root_value", ["[]", "{}"])
def test_leaf_and_linked_share_mutations_reject_invalid_root(root_value):
    catalog, client = _catalog()
    client.set(RK.meta_root("acme", "lake"), root_value)
    client.set(RK.lock_leaf("acme", "lake", "orders"), "owner")

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.check_table_mutation_allowed(
            "acme", "lake", "orders", lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.set_leaf_payload_cas(
            "acme", "lake", "orders", {"resources": []},
            "snapshots/bootstrap.json",
        )
    with pytest.raises(RuntimeError, match="Corrupt Redis catalog JSON"):
        catalog.commit_snapshot(
            "acme", "lake", "orders", {"resources": []},
            "snapshots/v0.json", expected_version=-1, expected_path="",
            lock_token="owner", commit_id="stale-commit",
        )
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.create_linked_share(
            "acme", "lake", "late", {"id": "late"},
        )

    assert client.get(RK.meta_root("acme", "lake")) == root_value
    assert not client.exists(RK.meta_leaf("acme", "lake", "orders"))
    assert not client.exists(RK.linked_share_doc("acme", "lake", "late"))


def test_root_bump_rejects_readonly_root_without_mutation():
    catalog, client = _catalog()
    client.set(
        RK.meta_root("acme", "lake"),
        json.dumps({
            "version": 7,
            "ts": 1,
            "read_only": True,
            "cloned_from": "source",
        }),
    )

    before = client.get(RK.meta_root("acme", "lake"))
    with pytest.raises(ReadOnlyCatalogError):
        catalog.bump_root("acme", "lake", now_ms=2)

    assert client.get(RK.meta_root("acme", "lake")) == before


def test_root_flag_update_cas_compares_exact_prevalidated_document(monkeypatch):
    catalog, client = _catalog()
    _seed_root(client)
    root_key = RK.meta_root("acme", "lake")
    original_script = catalog._update_root_flags

    def race_flags(*, keys, args):
        competing = json.loads(client.get(root_key))
        competing["maintenance_owner"] = "other-admin"
        client.set(root_key, json.dumps(competing, sort_keys=True))
        return original_script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_update_root_flags", race_flags)

    with pytest.raises(SnapshotCommitConflictError, match="root changed"):
        catalog.update_root_flags("acme", "lake", {"read_only": True})

    assert json.loads(client.get(root_key)) == {
        "version": 0,
        "ts": 1,
        "maintenance_owner": "other-admin",
    }


@pytest.mark.parametrize(
    "flags",
    [
        {"version": 0},
        {"ts": 0},
        {"read_only": 0},
        {"read_only": None},
        {"clone_type": "unknown"},
        {"clone_type": "replica", "read_only": False, "cloned_from": "source"},
        {"cloned_from": "../source"},
        {"clone_ts": True},
        {"clone_ts": -1},
        {"replica_tables": "all"},
        {"replica_tables": ["orders", "orders"]},
        {"replica_tables": ["../orders"]},
    ],
)
def test_root_flag_update_rejects_identity_or_lifecycle_corruption(flags):
    catalog, client = _catalog()
    _seed_root(client)
    key = RK.meta_root("acme", "lake")
    original = client.get(key)

    with pytest.raises((TypeError, ValueError)):
        catalog.update_root_flags("acme", "lake", flags)

    assert client.get(key) == original


def test_root_flag_update_accepts_complete_valid_replica_contract():
    catalog, client = _catalog()
    _seed_root(client)

    assert catalog.update_root_flags("acme", "lake", {
        "read_only": True,
        "clone_type": "replica",
        "cloned_from": "source",
        "clone_ts": 7,
        "replica_tables": ["orders", "customers"],
    })

    root = catalog.get_root("acme", "lake")
    assert root["version"] == 0
    assert root["read_only"] is True
    assert root["clone_type"] == "replica"
    assert root["replica_tables"] == ["orders", "customers"]


def test_readonly_transition_atomically_fences_snapshot_and_stage_publication():
    catalog, client = _catalog()
    _seed_root(client)
    client.set(
        RK.meta_leaf("acme", "lake", "orders"),
        json.dumps({
            "version": 0,
            "ts": 1,
            "path": "snapshots/v0.json",
            "payload": {"resources": [], "_row_filter": None},
        }),
    )
    client.set(RK.lock_leaf("acme", "lake", "orders"), "leaf-owner")
    client.set(RK.lock_stage("acme", "lake", "uploads"), "stage-owner")
    stage_key = RK.staging_doc("acme", "lake", "uploads")
    client.set(stage_key, json.dumps({
        "organization": "acme",
        "super_name": "lake",
        "staging_name": "uploads",
        "files": {},
    }))

    # Both writers completed their ordinary Python/Lua preflight before the
    # lifecycle transition. The publication scripts must still observe the
    # current root at their own atomic linearization point.
    catalog.check_table_mutation_allowed(
        "acme", "lake", "orders", lock_token="leaf-owner",
    )
    catalog.check_stage_mutation_allowed(
        "acme", "lake", "uploads", lock_token="stage-owner",
    )
    before_leaf = client.get(RK.meta_leaf("acme", "lake", "orders"))
    before_stage = client.get(stage_key)

    catalog.update_root_flags("acme", "lake", {"read_only": True})

    with pytest.raises(ReadOnlyCatalogError):
        catalog.commit_snapshot(
            "acme", "lake", "orders",
            {"resources": [], "_row_filter": None},
            "snapshots/v1.json",
            expected_version=0,
            expected_path="snapshots/v0.json",
            lock_token="leaf-owner",
        )
    with pytest.raises(ReadOnlyCatalogError):
        catalog.upsert_staging_file_meta(
            "acme", "lake", "uploads", "late.parquet",
            {"rows": 1}, lock_token="stage-owner",
        )

    assert client.get(RK.meta_leaf("acme", "lake", "orders")) == before_leaf
    assert client.get(stage_key) == before_stage


def test_readonly_root_rejects_namespace_child_control_mutations():
    catalog, client = _catalog()
    _seed_root(client)
    client.set(
        RK.meta_leaf("acme", "lake", "orders"),
        json.dumps({"version": 0, "ts": 1, "path": "snapshots/v0.json"}),
    )
    client.set(RK.lock_leaf("acme", "lake", "orders"), "leaf-owner")
    catalog.update_root_flags("acme", "lake", {"read_only": True})

    with pytest.raises(ReadOnlyCatalogError):
        catalog.set_mirrors("acme", "lake", ["DELTA"])
    with pytest.raises(ReadOnlyCatalogError):
        catalog.create_linked_share("acme", "lake", "late", {"id": "late"})
    with pytest.raises(ReadOnlyCatalogError):
        catalog.set_table_config(
            "acme", "lake", "orders", {"max_overlapping_files": 2},
            lock_token="leaf-owner",
        )

    assert not client.exists(RK.meta_mirrors("acme", "lake"))
    assert not client.exists(RK.linked_share_doc("acme", "lake", "late"))
    assert not client.exists(RK.meta_table_config("acme", "lake", "orders"))


@pytest.mark.parametrize("raw", ["[]", "null", "not-json"])
def test_table_config_read_rejects_corrupt_or_nonobject_state(raw):
    catalog, client = _catalog()
    client.set(RK.meta_table_config("acme", "lake", "orders"), raw)

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.get_table_config("acme", "lake", "orders")


def test_dv_v2_activation_config_round_trips_with_exact_json_types():
    catalog, client = _catalog()
    _seed_root(client)
    client.set(
        RK.meta_leaf("acme", "lake", "orders"),
        json.dumps({"version": 0, "ts": 1, "path": "snapshots/v0.json"}),
    )
    client.set(RK.lock_leaf("acme", "lake", "orders"), "owner")

    assert catalog.set_table_config(
        "acme",
        "lake",
        "orders",
        {
            "max_overlapping_files": 4,
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": True,
        },
        lock_token="owner",
    )

    restored = catalog.get_table_config("acme", "lake", "orders")
    assert restored is not None
    assert restored["deletion_vector_format"] == 2
    assert type(restored["deletion_vector_format"]) is int
    assert restored["dv_v2_reader_fleet_confirmed"] is True


@pytest.mark.parametrize(
    "config",
    [
        {"deletion_vector_format": 2},
        {"dv_v2_reader_fleet_confirmed": True},
        {
            "deletion_vector_format": True,
            "dv_v2_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": "2",
            "dv_v2_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": 1,
        },
        {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": False,
        },
    ],
)
def test_table_config_write_rejects_partial_or_coerced_v2_activation(config):
    catalog, client = _catalog()

    with pytest.raises(ValueError, match="DV-v2 activation"):
        catalog.set_table_config(
            "acme", "lake", "orders", config, lock_token="owner",
        )
    assert not client.exists(RK.meta_table_config("acme", "lake", "orders"))


@pytest.mark.parametrize(
    "config",
    [
        {"deletion_vector_format": 2},
        {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": "true",
        },
    ],
)
def test_table_config_read_rejects_ambiguous_v2_activation(config):
    catalog, client = _catalog()
    client.set(
        RK.meta_table_config("acme", "lake", "orders"),
        json.dumps(config),
    )

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.get_table_config("acme", "lake", "orders")


def test_table_config_transport_failure_aborts_writer_and_configure():
    backend = MagicMock()
    backend.get_table_config.side_effect = redis.TimeoutError("redis unavailable")
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        organization="acme", super_name="lake",
    )
    writer.catalog = backend
    writer._table_config_cache = {}

    with pytest.raises(redis.TimeoutError, match="redis unavailable"):
        writer._get_table_config("orders")
    assert "orders" not in writer._table_config_cache

    with (
        patch("supertable.data_writer.check_write_access"),
        pytest.raises(redis.TimeoutError, match="redis unavailable"),
    ):
        writer.configure_table(
            "admin", "orders", max_overlapping_files=2,
        )
    backend.acquire_simple_lock.assert_called_once()
    backend.release_simple_lock.assert_called_once()
    backend.set_table_config.assert_not_called()


@pytest.mark.parametrize(
    "invalid_field",
    [
        {"read_only": 0},
        {"clone_type": "replica", "read_only": False, "cloned_from": "source"},
        {"clone_ts": True},
        {"replica_tables": "all"},
    ],
)
def test_persisted_invalid_root_lifecycle_fields_fail_closed(invalid_field):
    catalog, client = _catalog()
    key = RK.meta_root("acme", "lake")
    document = {"version": 1, "ts": 1, **invalid_field}
    raw = json.dumps(document)
    client.set(key, raw)

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.get_root("acme", "lake")
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.ensure_root("acme", "lake")
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.bump_root("acme", "lake")

    assert client.get(key) == raw


def test_linked_share_delete_is_atomically_fenced_by_namespace_intent():
    catalog, client = _catalog()
    _seed_root(client)
    catalog.create_linked_share(
        "acme", "lake", "linked", {"id": "linked"},
    )
    client.set(RK.meta_namespace_deletion_intent("acme", "lake"), "pending")

    with pytest.raises(DeletionIntentConflictError):
        catalog.delete_linked_share("acme", "lake", "linked")

    assert client.exists(RK.linked_share_doc("acme", "lake", "linked"))
    assert client.sismember(
        RK.linked_share_index("acme", "lake"), "linked",
    )


def test_linked_share_create_and_update_have_atomic_existence_semantics():
    catalog, client = _catalog()
    _seed_root(client)
    catalog.create_linked_share(
        "acme", "lake", "linked", {"id": "linked", "_row_filter": "x=1"},
    )

    with pytest.raises(FileExistsError):
        catalog.create_linked_share(
            "acme", "lake", "linked", {"id": "linked"},
        )
    assert catalog.get_linked_share(
        "acme", "lake", "linked",
    )["_row_filter"] == "x=1"

    assert catalog.update_linked_share(
        "acme", "lake", "missing", {"id": "missing"},
    ) is False
    assert not client.exists(
        RK.linked_share_doc("acme", "lake", "missing"),
    )
    assert not client.sismember(
        RK.linked_share_index("acme", "lake"), "missing",
    )


def test_linked_share_update_rejects_unindexed_orphan_without_mutation():
    catalog, client = _catalog()
    _seed_root(client)
    key = RK.linked_share_doc("acme", "lake", "orphan")
    original = json.dumps({"id": "orphan", "_row_filter": "secret=1"})
    client.set(key, original)

    with pytest.raises(RuntimeError, match="metadata/index"):
        catalog.update_linked_share(
            "acme", "lake", "orphan", {"id": "orphan"},
        )
    assert client.get(key) == original


def test_linked_share_delete_is_absence_aware_and_rejects_index_corruption():
    catalog, client = _catalog()
    _seed_root(client)
    assert catalog.delete_linked_share("acme", "lake", "missing") is False

    key = RK.linked_share_doc("acme", "lake", "orphan")
    original = json.dumps({"id": "orphan", "_row_filter": "secret=1"})
    client.set(key, original)
    with pytest.raises(RuntimeError, match="metadata/index"):
        catalog.delete_linked_share("acme", "lake", "orphan")
    assert client.get(key) == original

    client.delete(key)
    client.sadd(RK.linked_share_index("acme", "lake"), "indexed-only")
    with pytest.raises(RuntimeError, match="metadata/index"):
        catalog.delete_linked_share("acme", "lake", "indexed-only")
    assert client.sismember(
        RK.linked_share_index("acme", "lake"), "indexed-only",
    )


def test_share_mutations_preflight_types_and_use_explicit_cas():
    catalog, client = _catalog()
    index = RK.share_index("acme")
    client.set(index, "wrong-type")

    with pytest.raises(RuntimeError, match="metadata/index"):
        catalog.create_share("acme", "daily", {"id": "daily"})
    assert not client.exists(RK.share_doc("acme", "daily"))

    client.delete(index)
    catalog.create_share("acme", "daily", {"id": "daily", "v": 1})
    with pytest.raises(FileExistsError):
        catalog.create_share("acme", "daily", {"id": "daily", "v": 2})
    assert catalog.get_share("acme", "daily")["v"] == 1
    assert catalog.update_share(
        "acme", "missing", {"id": "missing"},
    ) is False
    assert catalog.update_share(
        "acme", "daily", {"id": "daily", "v": 2},
    ) is True

    client.delete(index)
    client.set(index, "wrong-type")
    with pytest.raises(RuntimeError, match="metadata/index"):
        catalog.delete_share("acme", "daily")
    assert catalog.get_share("acme", "daily")["v"] == 2


def test_control_plane_reads_never_turn_transport_or_corruption_into_empty(monkeypatch):
    catalog, client = _catalog()

    with patch.object(
        client, "smembers", side_effect=redis.TimeoutError("index timeout"),
    ):
        with pytest.raises(redis.TimeoutError, match="index timeout"):
            catalog.list_shares("acme")
        with pytest.raises(redis.TimeoutError, match="index timeout"):
            catalog.list_linked_shares("acme", "lake")
        with pytest.raises(redis.TimeoutError, match="index timeout"):
            catalog.list_stagings("acme", "lake")
        with pytest.raises(redis.TimeoutError, match="index timeout"):
            catalog.list_pipes("acme", "lake", "uploads")

    client.sadd(RK.share_index("acme"), "missing")
    with pytest.raises(RuntimeError, match="missing metadata"):
        catalog.list_shares("acme")
    client.sadd(RK.linked_share_index("acme", "lake"), "missing")
    with pytest.raises(RuntimeError, match="missing metadata"):
        catalog.list_linked_shares("acme", "lake")

    client.set(RK.share_doc("acme", "corrupt"), "[]")
    with pytest.raises(RuntimeError, match="Corrupt share metadata"):
        catalog.get_share("acme", "corrupt")
    client.set(RK.linked_share_doc("acme", "lake", "corrupt"), "[]")
    with pytest.raises(RuntimeError, match="Corrupt linked-share metadata"):
        catalog.get_linked_share("acme", "lake", "corrupt")


@pytest.mark.parametrize("member", ["../escape", b"\xff"])
def test_staging_listing_rejects_unsafe_index_member_instead_of_omitting_it(
    member,
):
    catalog, client = _catalog()
    client.sadd(RK.staging_index("acme", "lake"), member)

    with pytest.raises(RuntimeError, match="Corrupt staging index"):
        catalog.list_stagings("acme", "lake")


def test_rbac_listings_propagate_uncertain_backend_state():
    catalog, client = _catalog()
    with patch.object(
        client, "smembers", side_effect=redis.TimeoutError("rbac timeout"),
    ):
        with pytest.raises(redis.TimeoutError, match="rbac timeout"):
            catalog.get_users("acme", "lake")
        with pytest.raises(redis.TimeoutError, match="rbac timeout"):
            catalog.get_roles("acme", "lake")


def test_concurrent_engine_and_policy_updates_preserve_both_sections(monkeypatch):
    catalog, client = _catalog()
    key = RK.engine_duckdb("acme")
    client.set(
        key,
        json.dumps({
            "duckdb": {"duckdb_threads": 1},
            "auto_policy": [],
        }),
    )
    _coordinate_first_reads(monkeypatch, client, key)

    results = _run_concurrently(
        lambda: catalog.set_engine_config("acme", {"duckdb_threads": 8}),
        lambda: catalog.set_auto_routing_policy("acme", [
            {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
        ]),
    )

    assert results == [True, True]
    stored = catalog.get_engine_config("acme")
    assert stored["duckdb"] == {"duckdb_threads": 8}
    assert stored["auto_policy"] == [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ]


def test_engine_update_does_not_replace_document_after_ambiguous_read(monkeypatch):
    catalog, client = _catalog()
    key = RK.engine_duckdb("acme")
    original = json.dumps({
        "duckdb": {"duckdb_threads": 2},
        "auto_policy": [
            {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
        ],
    })
    client.set(key, original)

    class _TimeoutPipeline:
        def watch(self, *_keys):
            return None

        def get(self, _key):
            raise redis.TimeoutError("ambiguous read")

        def reset(self):
            return None

    monkeypatch.setattr(client, "pipeline", lambda: _TimeoutPipeline())

    assert catalog.set_engine_config("acme", {"duckdb_threads": 8}) is False
    assert client.get(key) == original


def test_malformed_engine_document_is_not_overwritten():
    catalog, client = _catalog()
    key = RK.engine_duckdb("acme")
    client.set(key, "not-json")

    assert catalog.set_auto_routing_policy("acme", []) is False
    assert client.get(key) == "not-json"


@pytest.mark.parametrize(
    "root_value",
    [
        "[]",
        "{}",
        '{"version": true, "ts": 1}',
        '{"version": 0, "ts": false}',
        '{"version": -1, "ts": 1}',
        '{"version": 0.5, "ts": 1}',
    ],
)
def test_ensure_root_rejects_invalid_existing_document(root_value):
    catalog, client = _catalog()
    key = RK.meta_root("acme", "lake")
    client.set(key, root_value)

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.ensure_root("acme", "lake")

    assert client.get(key) == root_value


def test_ensure_root_rejects_wrong_redis_type_without_replacing_it():
    catalog, client = _catalog()
    key = RK.meta_root("acme", "lake")
    client.sadd(key, "corrupt")

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.ensure_root("acme", "lake")

    assert client.type(key) == "set"
    assert client.smembers(key) == {"corrupt"}


def test_ensure_root_preserves_valid_existing_flags():
    catalog, client = _catalog()
    key = RK.meta_root("acme", "lake")
    original = json.dumps({
        "version": 7,
        "ts": 9,
        "read_only": True,
        "cloned_from": "source",
    })
    client.set(key, original)

    catalog.ensure_root("acme", "lake")

    assert client.get(key) == original


def _seed_deletable_table(catalog, client, *, simple="orders"):
    _seed_root(client)
    client.set(
        RK.meta_leaf("acme", "lake", simple),
        json.dumps({
            "version": 0,
            "ts": 1,
            "path": f"snapshots/{simple}.json",
            "payload": {"resources": []},
        }),
    )
    client.sadd(RK.meta_table_names("acme", "lake"), simple)
    client.set(RK.lock_leaf("acme", "lake", simple), "leaf-owner")
    client.set(RK.lock_namespace("acme", "lake"), "namespace-owner")
    return catalog.begin_simple_deletion(
        "acme",
        "lake",
        simple,
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        intent_id=f"delete-{simple}",
    )


def test_table_delete_clears_mutable_quality_state_but_preserves_audit_history():
    catalog, client = _catalog()
    prefix = RK.quality_prefix("acme", "lake")
    target_fixed = catalog._quality_table_mutable_keys(
        "acme", "lake", "orders",
    )
    assert {key.removeprefix(prefix) for key in target_fixed} == {
        "config:orders",
        "schedule:orders",
        "latest:orders",
        "anomalies:orders",
        "pending:orders",
        "pending_unresolved:orders",
        "running:orders",
        "cooldown:orders",
        "pending_mode:orders:quick",
        "pending_mode:orders:deep",
        "pending_mode:orders:custom",
        "cooldown:orders:quick",
        "cooldown:orders:deep",
        "cooldown:orders:custom",
        "retry:orders:quick",
        "retry:orders:deep",
        "retry:orders:custom",
        "cron_state:orders:quick",
        "cron_state:orders:deep",
        "cron_state:orders:custom",
    }
    other_fixed = catalog._quality_table_mutable_keys(
        "acme", "lake", "customers",
    )
    for key in target_fixed:
        client.set(key, "stale-target")
    for key in other_fixed:
        client.set(key, "live-other")
    target_columns = [
        prefix + "latest:orders:id",
        prefix + "latest:orders:total",
    ]
    other_column = prefix + "latest:customers:id"
    for key in target_columns:
        client.set(key, "stale-column")
    client.set(other_column, "live-column")
    history_key = prefix + "history"
    history_rows = [
        json.dumps({"table_name": "orders", "history_id": "old-order"}),
        json.dumps({"table_name": "customers", "history_id": "customer"}),
    ]
    client.rpush(history_key, *history_rows)
    outbox_key = prefix + "history_outbox"
    outbox = {
        "old-order": json.dumps({
            "history_id": "old-order",
            "organization": "acme",
            "super_name": "lake",
            "table_name": "orders",
        }),
        "customer": json.dumps({
            "history_id": "customer",
            "organization": "acme",
            "super_name": "lake",
            "table_name": "customers",
        }),
    }
    client.hset(outbox_key, mapping=outbox)
    cursor_key = prefix + "history_outbox_cursor"
    client.set(cursor_key, "17")
    rules_index = prefix + "rules:index"
    rule_doc = prefix + "rules:doc:orders-policy"
    client.sadd(rules_index, "orders-policy")
    client.set(rule_doc, json.dumps({
        "rule_id": "orders-policy",
        "table_name": "orders",
        "enabled": True,
    }))

    # The running lease is revoked at the exact linearization point where the
    # durable deletion intent first becomes visible.
    running_key = prefix + "running:orders"
    client.set(running_key, "active-quality-token")
    intent = _seed_deletable_table(catalog, client)
    assert not client.exists(running_key)

    assert catalog.delete_simple_table(
        "acme",
        "lake",
        "orders",
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        intent_id=intent["intent_id"],
    )

    assert all(not client.exists(key) for key in target_fixed)
    assert all(not client.exists(key) for key in target_columns)
    assert all(client.get(key) == "live-other" for key in other_fixed)
    assert client.get(other_column) == "live-column"
    assert client.lrange(history_key, 0, -1) == history_rows
    assert client.hgetall(outbox_key) == outbox
    assert client.get(cursor_key) == "17"
    assert client.smembers(rules_index) == {"orders-policy"}
    assert json.loads(client.get(rule_doc))["table_name"] == "orders"

    catalog.clear_simple_deletion_tombstone(
        "acme",
        "lake",
        "orders",
        expected_intent_id=intent["intent_id"],
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        confirm_previous_owner_stopped=True,
    )
    catalog.set_leaf_payload_cas(
        "acme",
        "lake",
        "orders",
        {"resources": []},
        "snapshots/recreated.json",
        namespace_token="namespace-owner",
    )
    assert all(not client.exists(key) for key in target_fixed)
    assert all(not client.exists(key) for key in target_columns)


def test_table_tombstone_cannot_clear_while_dynamic_quality_state_remains():
    catalog, client = _catalog()
    intent = _seed_deletable_table(catalog, client)
    assert catalog.delete_simple_table(
        "acme",
        "lake",
        "orders",
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        intent_id=intent["intent_id"],
    )
    dynamic_key = RK.quality_prefix("acme", "lake") + "latest:orders:late"
    client.set(dynamic_key, "late-state")

    with pytest.raises(RuntimeError, match="Dynamic table quality state remains"):
        catalog.clear_simple_deletion_tombstone(
            "acme",
            "lake",
            "orders",
            expected_intent_id=intent["intent_id"],
            namespace_token="namespace-owner",
            lock_token="leaf-owner",
            confirm_previous_owner_stopped=True,
        )

    assert client.exists(
        RK.meta_simple_deletion_intent("acme", "lake", "orders"),
    )
    assert catalog.delete_simple_table(
        "acme",
        "lake",
        "orders",
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        intent_id=intent["intent_id"],
    )
    assert not client.exists(dynamic_key)


def test_dynamic_quality_cleanup_bound_fails_closed_and_can_resume(monkeypatch):
    catalog, client = _catalog()
    intent = _seed_deletable_table(catalog, client)
    prefix = RK.quality_prefix("acme", "lake") + "latest:orders:"
    client.set(prefix + "first", "one")
    client.set(prefix + "second", "two")
    monkeypatch.setattr(catalog, "_QUALITY_DYNAMIC_KEY_LIMIT", 1)

    with pytest.raises(RuntimeError, match="exceeded its key bound"):
        catalog.delete_simple_table(
            "acme",
            "lake",
            "orders",
            namespace_token="namespace-owner",
            lock_token="leaf-owner",
            intent_id=intent["intent_id"],
        )

    assert client.exists(RK.meta_leaf("acme", "lake", "orders"))
    assert catalog.get_simple_deletion_intent(
        "acme", "lake", "orders",
    )["status"] == "deleting"

    monkeypatch.setattr(catalog, "_QUALITY_DYNAMIC_KEY_LIMIT", 100_000)
    assert catalog.delete_simple_table(
        "acme",
        "lake",
        "orders",
        namespace_token="namespace-owner",
        lock_token="leaf-owner",
        intent_id=intent["intent_id"],
    )
    assert not client.exists(prefix + "first")
    assert not client.exists(prefix + "second")
