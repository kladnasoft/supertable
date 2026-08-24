from __future__ import annotations

import io
import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import redis

from supertable import meta_reader
from supertable import redis_keys as RK
from supertable.errors import LockLostError
from supertable.rbac.permissions import RoleType
from supertable.redis_catalog import DeletionIntentConflictError, RedisCatalog
from supertable.staging_area import Staging
from supertable.storage.local_storage import LocalStorage
from supertable.super_pipe import SuperPipe


def _catalog() -> RedisCatalog:
    return RedisCatalog(
        redis_client=fakeredis.FakeStrictRedis(decode_responses=True),
    )


def _seed_root(catalog: RedisCatalog) -> None:
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )


def _seed_stage(catalog: RedisCatalog, *, lock_token: str) -> None:
    _seed_root(catalog)
    catalog.r.set(RK.lock_stage("acme", "lake", "uploads"), lock_token)
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={"path": "acme/lake/staging/uploads", "files": {}},
        lock_token=lock_token,
    )


def test_row_filtered_metadata_omits_unscoped_snapshot_metrics():
    entry = {
        "columns": ["id"],
        "filters": [{"column": "department", "operator": "=", "value": "oncology"}],
    }
    context = SimpleNamespace(
        role_type=RoleType.READER,
        role_info={"role_id": "oncology-reader"},
        fingerprint="row-filtered",
    )
    reader = meta_reader.MetaReader.__new__(meta_reader.MetaReader)
    reader.super_table = SimpleNamespace(
        super_name="health", organization="acme",
    )
    reader.catalog = MagicMock()
    reader._authorized_meta_targets = lambda *_args: (
        context, [("patients", entry)],
    )

    with patch("supertable.meta_reader.SimpleTable") as table:
        assert reader.get_table_stats("patients", "oncology-reader") == []
        table.assert_not_called()

    reader.catalog.get_root.return_value = {"version": 4, "ts": 1}
    with patch("supertable.meta_reader._super_meta_cache_ttl_s", return_value=0.0):
        assert reader.get_super_meta("oncology-reader") is None
    reader.catalog.r.mget.assert_not_called()


def test_linked_share_filter_omits_stats_even_when_nested_snapshot_hides_it():
    entry = {"columns": ["id"], "filters": ["*"]}
    context = SimpleNamespace(
        role_type=RoleType.READER,
        role_info={"role_id": "share-reader"},
        fingerprint="unrestricted-rbac",
    )
    reader = meta_reader.MetaReader.__new__(meta_reader.MetaReader)
    reader.super_table = SimpleNamespace(super_name="lake", organization="acme")
    reader.catalog = MagicMock()
    reader._authorized_meta_targets = lambda *_args: (
        context, [("orders", entry)],
    )
    snapshot = {
        "schema": {"id": "Int64"},
        "resources": [{
            "file": "provider/private.parquet", "rows": 101,
            "file_size": 9090,
        }],
    }

    with patch("supertable.meta_reader.SimpleTable") as table:
        instance = table.return_value
        instance.get_simple_table_snapshot.return_value = (snapshot, "private")
        instance._last_snapshot_leaf = {
            "payload": {
                "_row_filter": "tenant_id = 'private-tenant'",
                "snapshot": snapshot,
            },
        }
        assert reader.get_table_stats("orders", "share-reader") == []


def test_linked_share_filter_change_cannot_reuse_unfiltered_super_meta_cache():
    entry = {"columns": ["id"], "filters": ["*"]}
    context = SimpleNamespace(
        role_type=RoleType.READER,
        role_info={"role_id": "share-reader"},
        fingerprint="same-rbac-policy",
    )
    reader = meta_reader.MetaReader.__new__(meta_reader.MetaReader)
    reader.super_table = SimpleNamespace(super_name="lake", organization="acme")
    reader.catalog = MagicMock()
    reader.catalog.get_root.return_value = {"version": 7, "ts": 1}
    reader._authorized_meta_targets = lambda *_args: (
        context, [("orders", entry)],
    )
    snapshot = {
        "snapshot_version": 3,
        "_row_filter": None,
        "schema": {"id": "Int64"},
        "resources": [{
            "file": "provider/private.parquet", "rows": 101,
            "file_size": 9090,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    reader.catalog.r.mget.side_effect = [
        [json.dumps({
            "version": 3,
            "ts": 1,
            "path": "snapshots/v3.json",
            "payload": snapshot,
        })],
        [json.dumps({
            "version": 3,
            "ts": 2,
            "path": "snapshots/v3.json",
            "payload": {
                **snapshot,
                "_row_filter": "tenant_id = 'private-tenant'",
            },
        })],
    ]

    with meta_reader._SUPER_META_CACHE_LOCK:
        meta_reader._SUPER_META_CACHE.clear()
    with patch("supertable.meta_reader._super_meta_cache_ttl_s", return_value=60.0):
        first = reader.get_super_meta("share-reader")
        second = reader.get_super_meta("share-reader")

    assert first["super"]["rows"] == 101
    assert first["super"]["tables"][0]["name"] == "orders"
    assert second is None
    assert "private-tenant" not in str(second)


def test_incomplete_leaf_payload_cannot_hide_authoritative_share_filter():
    entry = {"columns": ["id"], "filters": ["*"]}
    context = SimpleNamespace(
        role_type=RoleType.READER,
        role_info={"role_id": "share-reader"},
        fingerprint="unrestricted-rbac",
    )
    reader = meta_reader.MetaReader.__new__(meta_reader.MetaReader)
    reader.super_table = SimpleNamespace(super_name="lake", organization="acme")
    reader.catalog = MagicMock()
    reader.catalog.get_root.return_value = {"version": 7, "ts": 1}
    reader._authorized_meta_targets = lambda *_args: (
        context, [("orders", entry)],
    )

    # Even a tombstone-complete Redis payload is not an authoritative cache
    # unless it explicitly seals the linked-share policy state. The selected
    # immutable document carries the policy this legacy cache omitted.
    partial_snapshot = {
        "snapshot_version": 3,
        "schema": {"id": "Int64"},
        "resources": [{"file": "private.parquet", "rows": 101, "file_size": 9}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    reader.catalog.r.mget.return_value = [json.dumps({
        "version": 3,
        "path": "snapshots/v3.json",
        "payload": partial_snapshot,
    })]
    authoritative_snapshot = {
        **partial_snapshot,
        "_row_filter": "tenant_id = 'private-tenant'",
    }

    with (
        patch("supertable.meta_reader._super_meta_cache_ttl_s", return_value=60.0),
        patch("supertable.meta_reader.SimpleTable") as table,
    ):
        instance = table.return_value
        instance.get_simple_table_snapshot.return_value = (
            authoritative_snapshot, "snapshots/v3.json",
        )
        instance._last_snapshot_leaf = {
            "version": 3,
            "path": "snapshots/v3.json",
            "payload": partial_snapshot,
        }
        assert reader.get_super_meta("share-reader") is None
        instance.get_simple_table_snapshot.assert_called_once_with()


def test_staging_uses_create_write_and_control_gates_and_lists_fenced_file(
    tmp_path,
):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access") as create_access,
        patch("supertable.staging_area.check_write_access") as write_access,
        patch("supertable.staging_area.check_control_access") as control_access,
        patch("supertable.staging_area.check_meta_access"),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        first = stage.save_as_parquet(
            role_name="operator",
            arrow_table=pa.table({"id": [1]}),
            base_file_name="first.parquet",
        )
        # Admission is revalidated after the stage lock, after the object
        # write, and immediately before catalog publication.
        assert create_access.call_count >= 2
        write_access.assert_not_called()
        assert stage.list_files("operator") == [first]
        assert storage.read_json(stage.files_index_path) == []

        stage.save_as_parquet(
            role_name="operator",
            arrow_table=pa.table({"id": [2]}),
            base_file_name="second.parquet",
        )
        assert write_access.call_count >= 2
        assert len(stage.list_files("operator")) == 2

        stage.delete("operator")
        # Deletion revalidates CONTROL after acquiring the fenced stage lock.
        assert control_access.call_count >= 2


def test_stale_staging_file_index_update_is_rejected_without_overwrite():
    catalog = _catalog()
    _seed_root(catalog)
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    catalog.r.set(lock_key, "owner-a")
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={"path": "acme/lake/staging/uploads", "files": {}},
        lock_token="owner-a",
    )
    catalog.upsert_staging_file_meta(
        "acme", "lake", "uploads", "a.parquet",
        meta={"file": "a.parquet", "rows": 1},
        lock_token="owner-a",
    )

    catalog.r.set(lock_key, "owner-b")
    catalog.upsert_staging_file_meta(
        "acme", "lake", "uploads", "b.parquet",
        meta={"file": "b.parquet", "rows": 1},
        lock_token="owner-b",
    )
    with pytest.raises(LockLostError):
        catalog.upsert_staging_file_meta(
            "acme", "lake", "uploads", "stale.parquet",
            meta={"file": "stale.parquet", "rows": 1},
            lock_token="owner-a",
        )

    files = catalog.get_staging_meta("acme", "lake", "uploads")["files"]
    assert set(files) == {"a.parquet", "b.parquet"}
    assert "stale.parquet" not in files


def test_legacy_staging_index_is_migrated_once_and_stale_overwrite_is_ignored(
    tmp_path,
):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_root(catalog)
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    catalog.r.set(lock_key, "bootstrap")
    # Upgrade fixture: old stage documents predate the authoritative Redis
    # file map and rely exclusively on the fixed-path JSON index.
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={"path": "acme/lake/staging/uploads"},
        lock_token="bootstrap",
    )
    catalog.r.delete(lock_key)

    storage.write_bytes("acme/lake/staging/uploads/old.parquet", b"old")
    storage.write_json(
        "acme/lake/staging/uploads_files.json",
        [{"file": "old.parquet", "rows": 1}],
    )

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_meta_access"),
        patch("supertable.staging_area.check_write_access"),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        with (
            patch.object(
                catalog, "acquire_stage_lock",
                wraps=catalog.acquire_stage_lock,
            ) as acquire_lock,
            patch.object(
                catalog, "upsert_staging_meta",
                wraps=catalog.upsert_staging_meta,
            ) as upsert_stage,
        ):
            assert stage.list_files("reader") == ["old.parquet"]
            manager = Staging(organization="acme", super_name="lake")
            assert manager.get_directory_structure(
                "reader",
            )["stages"][0]["files"] == ["old.parquet"]
            acquire_lock.assert_not_called()
            upsert_stage.assert_not_called()
        assert "files" not in catalog.get_staging_meta(
            "acme", "lake", "uploads",
        )

        # The next authorized write performs the one-time migration while its
        # live stage token is checked in the Redis publication boundary.
        generated = stage.save_as_parquet(
            role_name="writer",
            arrow_table=pa.table({"id": [2]}),
            base_file_name="new.parquet",
        )
        assert set(catalog.get_staging_meta(
            "acme", "lake", "uploads",
        )["files"]) == {"old.parquet", generated}

        # A paused old writer resumes after losing its lease.  Its fixed JSON
        # replace is visible in storage, but its Redis fence rejects the stale
        # publication and neither public listing path may resurrect the orphan.
        storage.write_bytes(
            "acme/lake/staging/uploads/orphan.parquet", b"orphan",
        )
        storage.write_json(
            "acme/lake/staging/uploads_files.json",
            [
                {"file": "old.parquet", "rows": 1},
                {"file": "orphan.parquet", "rows": 1},
            ],
        )
        with pytest.raises(LockLostError):
            catalog.upsert_staging_file_meta(
                "acme", "lake", "uploads", "orphan.parquet",
                meta={"file": "orphan.parquet", "rows": 1},
                lock_token="stale-owner",
            )

        expected = sorted(["old.parquet", generated])
        assert stage.list_files("reader") == expected
        manager = Staging(organization="acme", super_name="lake")
        structure = manager.get_directory_structure("reader")
        assert structure["stages"][0]["files"] == expected
        assert structure["stages"][0]["file_count"] == 2


def test_staging_initialization_is_create_only_and_preserves_live_files():
    catalog = _catalog()
    _seed_root(catalog)
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    catalog.r.set(lock_key, "owner")
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={
            "path": "acme/lake/staging/uploads",
            "files": {"existing.parquet": {"file": "existing.parquet"}},
        },
        lock_token="owner",
    )

    with pytest.raises(FileExistsError):
        catalog.upsert_staging_meta(
            "acme", "lake", "uploads",
            meta={"path": "replacement", "files": {}},
            lock_token="owner",
            create_only=True,
        )

    stage_meta = catalog.get_staging_meta("acme", "lake", "uploads")
    assert stage_meta["path"] == "acme/lake/staging/uploads"
    assert set(stage_meta["files"]) == {"existing.parquet"}


def test_staging_save_fails_closed_on_metadata_timeout(tmp_path):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )
    catalog.r.set(RK.lock_stage("acme", "lake", "uploads"), "bootstrap")
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={
            "path": "acme/lake/staging/uploads",
            "files": {"existing.parquet": {"file": "existing.parquet"}},
        },
        lock_token="bootstrap",
    )
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )

    original_get = catalog.r.get
    with (
        patch.object(
            catalog.r, "get", side_effect=redis.TimeoutError("timed out"),
        ),
        patch("supertable.staging_area.check_create_access") as create_access,
        patch("supertable.staging_area.check_write_access") as write_access,
    ):
        with pytest.raises(redis.TimeoutError, match="timed out"):
            stage.save_as_parquet(
                role_name="operator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="new.parquet",
            )
    create_access.assert_not_called()
    write_access.assert_not_called()

    raw = original_get(RK.staging_doc("acme", "lake", "uploads"))
    assert set(json.loads(raw)["files"]) == {"existing.parquet"}
    assert not storage.exists(stage.stage_dir)


def test_staging_save_fails_closed_on_corrupt_metadata(tmp_path):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )
    stage_key = RK.staging_doc("acme", "lake", "uploads")
    catalog.r.set(stage_key, "{not-json")

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )

    with (
        patch("supertable.staging_area.check_create_access") as create_access,
        patch("supertable.staging_area.check_write_access") as write_access,
    ):
        with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
            stage.save_as_parquet(
                role_name="operator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="new.parquet",
            )
    create_access.assert_not_called()
    write_access.assert_not_called()
    assert catalog.r.get(stage_key) == "{not-json"
    assert not storage.exists(stage.stage_dir)


def test_failed_first_parquet_write_does_not_publish_stage_or_change_auth(tmp_path):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_root(catalog)

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access") as create_access,
        patch("supertable.staging_area.check_write_access") as write_access,
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        with patch.object(
            storage, "write_parquet", side_effect=OSError("disk full"),
        ):
            with pytest.raises(OSError, match="disk full"):
                stage.save_as_parquet(
                    role_name="creator",
                    arrow_table=pa.table({"id": [1]}),
                    base_file_name="failed.parquet",
                )

        assert catalog.get_staging_meta("acme", "lake", "uploads") is None
        assert not catalog.r.sismember(
            RK.staging_index("acme", "lake"), "uploads",
        )
        saved = stage.save_as_parquet(
            role_name="creator",
            arrow_table=pa.table({"id": [2]}),
            base_file_name="retry.parquet",
        )

    assert saved in catalog.get_staging_meta(
        "acme", "lake", "uploads",
    )["files"]
    # Both the failed attempt and retry perform fresh authorization checks;
    # the retry also revalidates immediately before publication.
    assert create_access.call_count >= 4
    write_access.assert_not_called()


def test_failed_first_legacy_index_write_does_not_publish_stage(tmp_path):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_root(catalog)

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
        patch.object(
            storage, "write_json", side_effect=OSError("index write failed"),
        ),
        patch.object(storage, "write_parquet") as write_parquet,
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        with pytest.raises(OSError, match="index write failed"):
            stage.save_as_parquet(
                role_name="creator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="failed.parquet",
            )

    assert catalog.get_staging_meta("acme", "lake", "uploads") is None
    assert not catalog.r.sismember(
        RK.staging_index("acme", "lake"), "uploads",
    )
    write_parquet.assert_not_called()


def test_stale_staging_object_cannot_recreate_child_after_root_removal(tmp_path):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_root(catalog)
    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stale = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        catalog.r.delete(RK.meta_root("acme", "lake"))

        with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
            stale.save_as_parquet(
                role_name="operator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="orphan.parquet",
            )

    assert catalog.get_staging_meta("acme", "lake", "uploads") is None
    assert not storage.exists(stale.stage_dir)
    assert not storage.exists(stale.files_index_path)


def test_staging_publication_rechecks_parent_atomically_after_precheck():
    catalog = _catalog()
    _seed_root(catalog)
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    catalog.r.set(lock_key, "owner")
    catalog.check_stage_mutation_allowed(
        "acme", "lake", "uploads", lock_token="owner",
    )

    # Model namespace deletion winning immediately after the caller's initial
    # fence check but before metadata publication.
    catalog.r.delete(RK.meta_root("acme", "lake"))
    with pytest.raises(FileNotFoundError, match="SuperTable does not exist"):
        catalog.upsert_staging_meta(
            "acme", "lake", "uploads",
            meta={"path": "orphan", "files": {}},
            lock_token="owner",
            create_only=True,
        )
    assert catalog.get_staging_meta("acme", "lake", "uploads") is None


@pytest.mark.parametrize(
    "root_value",
    ["[]", "{}", '{"version": 0, "ts": "invalid"}'],
)
def test_stage_mutations_reject_non_object_or_invalid_roots(root_value):
    catalog = _catalog()
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    catalog.r.set(lock_key, "owner")
    catalog.r.set(RK.meta_root("acme", "lake"), root_value)

    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.check_stage_mutation_allowed(
            "acme", "lake", "uploads", lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
        catalog.upsert_staging_meta(
            "acme", "lake", "uploads",
            meta={"path": "orphan", "files": {}},
            lock_token="owner",
        )

    assert not catalog.r.exists(RK.staging_doc("acme", "lake", "uploads"))
    assert not catalog.r.sismember(
        RK.staging_index("acme", "lake"), "uploads",
    )


def test_stage_and_pipe_payload_identities_must_match_catalog_keys():
    catalog = _catalog()
    _seed_root(catalog)
    catalog.r.set(RK.lock_stage("acme", "lake", "uploads"), "owner")

    with pytest.raises(ValueError, match="Staging metadata"):
        catalog.upsert_staging_meta(
            "acme", "lake", "uploads",
            meta={
                "organization": "other",
                "path": "orphan",
                "files": {},
            },
            lock_token="owner",
        )
    assert catalog.get_staging_meta("acme", "lake", "uploads") is None

    catalog.upsert_staging_meta(
        "acme", "lake", "uploads",
        meta={"path": "uploads", "files": {}},
        lock_token="owner",
    )
    with pytest.raises(ValueError, match="Pipe metadata"):
        catalog.upsert_pipe_meta(
            "acme", "lake", "uploads", "daily",
            meta={"staging_name": "other", "simple_name": "events"},
            lock_token="owner",
        )
    assert catalog.get_pipe_meta(
        "acme", "lake", "uploads", "daily",
    ) is None


@pytest.mark.parametrize("stage_value", ["[]", "{}"])
def test_pipe_and_file_mutations_reject_invalid_stage_parent(stage_value):
    catalog = _catalog()
    _seed_stage(catalog, lock_token="owner")
    stage_key = RK.staging_doc("acme", "lake", "uploads")
    pipe_key = RK.pipe_doc("acme", "lake", "uploads", "daily")
    catalog.r.set(stage_key, stage_value)

    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.upsert_staging_file_meta(
            "acme", "lake", "uploads", "part.parquet",
            meta={"rows": 1}, lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.upsert_pipe_meta(
            "acme", "lake", "uploads", "daily",
            meta={"simple_name": "events"}, lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.delete_pipe_meta(
            "acme", "lake", "uploads", "daily", lock_token="owner",
        )

    assert catalog.r.get(stage_key) == stage_value
    assert not catalog.r.exists(pipe_key)


def test_child_mutations_reject_mismatched_stage_parent_identity():
    catalog = _catalog()
    _seed_stage(catalog, lock_token="owner")
    stage_key = RK.staging_doc("acme", "lake", "uploads")
    stage = json.loads(catalog.r.get(stage_key))
    stage["super_name"] = "other"
    corrupt = json.dumps(stage)
    catalog.r.set(stage_key, corrupt)

    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.upsert_staging_file_meta(
            "acme", "lake", "uploads", "part.parquet",
            meta={"rows": 1}, lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.upsert_pipe_meta(
            "acme", "lake", "uploads", "daily",
            meta={"simple_name": "events"}, lock_token="owner",
        )
    with pytest.raises(RuntimeError, match="Corrupt staging metadata"):
        catalog.delete_pipe_meta(
            "acme", "lake", "uploads", "daily", lock_token="owner",
        )

    assert catalog.r.get(stage_key) == corrupt
    assert not catalog.r.exists(
        RK.pipe_doc("acme", "lake", "uploads", "daily"),
    )


def test_pipe_lifecycle_authorizes_actual_target_and_create_is_create_only():
    catalog = _catalog()
    _seed_root(catalog)
    catalog.r.set(RK.lock_stage("acme", "lake", "uploads"), "bootstrap")
    catalog.upsert_staging_meta(
        "acme", "lake", "uploads", meta={"path": "uploads", "files": {}},
        lock_token="bootstrap",
    )
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    with (
        patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
        patch("supertable.super_pipe.check_write_access") as write_access,
        patch("supertable.super_pipe.check_meta_access") as meta_access,
    ):
        pipe = SuperPipe(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        pipe.create(
            role_name="operator", pipe_name="daily", simple_name="secret",
            user_hash="user-1",
        )
        assert write_access.call_args.kwargs["table_name"] == "secret"

        write_access.reset_mock()
        with pytest.raises(FileExistsError):
            pipe.create(
                role_name="operator", pipe_name="daily", simple_name="public",
                user_hash="user-2",
            )
        assert write_access.call_args.kwargs["table_name"] == "secret"
        assert catalog.get_pipe_meta(
            "acme", "lake", "uploads", "daily",
        )["simple_name"] == "secret"

        pipe.set_enabled("daily", False, "operator")
        assert write_access.call_args.kwargs["table_name"] == "secret"
        pipe.read("daily", "operator")
        assert meta_access.call_args.kwargs["table_name"] == "secret"
        pipe.delete("daily", "operator")
        assert write_access.call_args.kwargs["table_name"] == "secret"


def test_stale_pipe_cannot_create_or_update_after_stage_removal():
    catalog = _catalog()
    _seed_stage(catalog, lock_token="bootstrap")
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    with (
        patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
        patch("supertable.super_pipe.check_write_access"),
    ):
        stale = SuperPipe(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        stale.create(
            role_name="operator", pipe_name="existing",
            simple_name="events", user_hash="u",
        )
        original = catalog.get_pipe_meta(
            "acme", "lake", "uploads", "existing",
        )

        catalog.r.delete(RK.staging_doc("acme", "lake", "uploads"))
        catalog.r.srem(RK.staging_index("acme", "lake"), "uploads")

        with pytest.raises(FileNotFoundError, match="Staging does not exist"):
            stale.create(
                role_name="operator", pipe_name="orphan",
                simple_name="other_events", user_hash="u",
            )
        with pytest.raises(FileNotFoundError, match="Staging does not exist"):
            stale.set_enabled("existing", False, "operator")

    assert catalog.get_pipe_meta(
        "acme", "lake", "uploads", "orphan",
    ) is None
    assert catalog.get_pipe_meta(
        "acme", "lake", "uploads", "existing",
    ) == original


def test_pipe_missing_and_forbidden_targets_have_identical_generic_denials():
    catalog = _catalog()
    _seed_stage(catalog, lock_token="bootstrap")
    catalog.upsert_pipe_meta(
        "acme", "lake", "uploads", "known",
        meta={"pipe_name": "known", "simple_name": "secret_orders"},
        lock_token="bootstrap",
    )
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    def deny(**_kwargs):
        raise PermissionError("target-specific secret")

    with (
        patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
        patch("supertable.super_pipe.check_meta_access", side_effect=deny),
        patch("supertable.super_pipe.check_write_access", side_effect=deny),
    ):
        pipe = SuperPipe(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        messages = []
        for name in ("missing", "known"):
            with pytest.raises(PermissionError) as denied:
                pipe.read(name, "scoped-reader")
            messages.append(str(denied.value))
        for name in ("missing", "known"):
            with pytest.raises(PermissionError) as denied:
                pipe.set_enabled(name, False, "scoped-writer")
            messages.append(str(denied.value))

    assert len(set(messages)) == 1
    assert messages[0] == "Pipe is unavailable under the effective access policy."
    assert "secret_orders" not in messages[0]
    assert catalog.get_pipe_meta(
        "acme", "lake", "uploads", "known",
    ).get("enabled") is not False


def test_stale_pipe_read_rechecks_stage_tombstone_after_target_authorization():
    catalog = _catalog()
    _seed_stage(catalog, lock_token="bootstrap")
    catalog.upsert_pipe_meta(
        "acme", "lake", "uploads", "daily",
        meta={"pipe_name": "daily", "simple_name": "secret_orders"},
        lock_token="bootstrap",
    )
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    with (
        patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
        patch("supertable.super_pipe.check_meta_access") as meta_access,
    ):
        pipe = SuperPipe(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        catalog.r.set(
            RK.meta_stage_deletion_intent("acme", "lake", "uploads"),
            json.dumps({"intent_id": "delete-stage"}),
        )
        with pytest.raises(DeletionIntentConflictError):
            pipe.read("daily", "authorized")
        assert any(
            call.kwargs.get("table_name") == "secret_orders"
            for call in meta_access.call_args_list
        )

        meta_access.side_effect = PermissionError("secret target")
        with pytest.raises(
            PermissionError,
            match="Pipe is unavailable under the effective access policy",
        ):
            pipe.read("daily", "scoped")


def test_stale_pipe_lifecycle_error_is_generic_for_target_scoped_reader():
    catalog = _catalog()
    _seed_stage(catalog, lock_token="bootstrap")
    catalog.upsert_pipe_meta(
        "acme", "lake", "uploads", "daily",
        meta={"pipe_name": "daily", "simple_name": "secret_orders"},
        lock_token="bootstrap",
    )
    catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

    def target_only_access(*, table_name, **_kwargs):
        if table_name != "secret_orders":
            raise PermissionError("namespace diagnostics denied")

    with (
        patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
        patch(
            "supertable.super_pipe.check_meta_access",
            side_effect=target_only_access,
        ),
    ):
        pipe = SuperPipe(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        catalog.r.set(
            RK.meta_stage_deletion_intent("acme", "lake", "uploads"),
            json.dumps({"intent_id": "delete-stage"}),
        )
        with pytest.raises(PermissionError) as denied:
            pipe.read("daily", "target-reader")

    assert str(denied.value) == (
        "Pipe is unavailable under the effective access policy."
    )
    assert "delete-stage" not in str(denied.value)


def test_pipe_catalog_create_only_cannot_replace_existing_target():
    catalog = _catalog()
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    _seed_stage(catalog, lock_token="owner")
    catalog.upsert_pipe_meta(
        "acme", "lake", "uploads", "daily",
        meta={"pipe_name": "daily", "simple_name": "secret"},
        lock_token="owner",
    )

    with pytest.raises(FileExistsError):
        catalog.upsert_pipe_meta(
            "acme", "lake", "uploads", "daily",
            meta={"pipe_name": "daily", "simple_name": "public"},
            lock_token="owner",
            create_only=True,
        )

    meta = catalog.get_pipe_meta("acme", "lake", "uploads", "daily")
    assert meta["simple_name"] == "secret"


def test_pipe_metadata_reads_fail_closed_on_timeout_and_corruption():
    catalog = _catalog()
    with patch.object(
        catalog.r, "get", side_effect=redis.TimeoutError("timed out"),
    ):
        with pytest.raises(redis.TimeoutError, match="timed out"):
            catalog.get_pipe_meta("acme", "lake", "uploads", "daily")

    pipe_key = RK.pipe_doc("acme", "lake", "uploads", "daily")
    catalog.r.set(pipe_key, "{not-json")
    with pytest.raises(RuntimeError, match="Corrupt pipe metadata"):
        catalog.get_pipe_meta("acme", "lake", "uploads", "daily")
    assert catalog.r.get(pipe_key) == "{not-json"


def test_pipe_delete_is_atomic_and_rejects_a_stale_stage_token():
    catalog = _catalog()
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    _seed_stage(catalog, lock_token="owner-a")
    catalog.upsert_pipe_meta(
        "acme", "lake", "uploads", "daily",
        meta={"pipe_name": "daily", "simple_name": "events"},
        lock_token="owner-a",
    )
    catalog.r.set(lock_key, "owner-b")

    with pytest.raises(LockLostError):
        catalog.delete_pipe_meta(
            "acme", "lake", "uploads", "daily", lock_token="owner-a",
        )
    assert catalog.r.exists(RK.pipe_doc("acme", "lake", "uploads", "daily"))
    assert catalog.r.sismember(
        RK.pipe_index("acme", "lake", "uploads"), "daily",
    )

    assert catalog.delete_pipe_meta(
        "acme", "lake", "uploads", "daily", lock_token="owner-b",
    ) == 1
    assert not catalog.r.exists(RK.pipe_doc("acme", "lake", "uploads", "daily"))
    assert not catalog.r.sismember(
        RK.pipe_index("acme", "lake", "uploads"), "daily",
    )


def _parquet_bytes() -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"secret": [7, 8], "other": [9, 10]}), buffer)
    return buffer.getvalue()


def _projection_backend(kind: str, tmp_path):
    payload = _parquet_bytes()
    if kind == "local":
        backend = LocalStorage(root=tmp_path)
        backend.write_bytes("data.parquet", payload)
        return backend
    if kind == "s3":
        pytest.importorskip("boto3")
        from supertable.storage.s3_storage import S3Storage

        backend = S3Storage.__new__(S3Storage)
        backend.base_prefix = ""
        backend.bucket_name = "bucket"
        backend._ensure_bucket_region = lambda: None
        backend._get_object_safe = lambda _path: payload
        return backend
    if kind == "minio":
        pytest.importorskip("minio")
        from supertable.storage.minio_storage import MinioStorage

        backend = MinioStorage.__new__(MinioStorage)
        backend.base_prefix = ""
        backend._get_object_safe = lambda _path: payload
        return backend
    if kind == "azure":
        pytest.importorskip("azure.storage.blob")
        from supertable.storage.azure_storage import AzureBlobStorage

        backend = AzureBlobStorage.__new__(AzureBlobStorage)
        backend.base_prefix = ""
        blob = MagicMock()
        blob.download_blob.return_value.readall.return_value = payload
        backend.container = MagicMock()
        backend.container.get_blob_client.return_value = blob
        return backend
    pytest.importorskip("google.cloud.storage")
    from supertable.storage.gcp_storage import GCSStorage

    backend = GCSStorage.__new__(GCSStorage)
    backend.base_prefix = ""
    blob = MagicMock()
    blob.download_as_bytes.return_value = payload
    backend.bucket = MagicMock()
    backend.bucket.get_blob.return_value = blob
    return backend


@pytest.mark.parametrize("kind", ["local", "s3", "minio", "azure", "gcs"])
def test_absent_parquet_projection_returns_zero_columns(kind, tmp_path):
    result = _projection_backend(kind, tmp_path).read_parquet(
        "data.parquet", columns=["does_not_exist"],
    )
    assert result.column_names == []
    assert result.num_rows == 2


@pytest.mark.parametrize("path", ["", "/", "//", "\\\\", ".", "foo/.."])
def test_local_delete_prefix_rejects_root_aliases(path, tmp_path):
    storage = LocalStorage(root=tmp_path)
    storage.write_bytes("victim.bin", b"keep")
    with pytest.raises(ValueError, match="Refusing to delete"):
        storage.delete_prefix(path)
    assert storage.exists("victim.bin")


def test_local_delete_prefix_rejects_absolute_configured_root(tmp_path):
    storage = LocalStorage(root=tmp_path)
    storage.write_bytes("victim.bin", b"keep")
    with pytest.raises(ValueError, match="configured storage root"):
        storage.delete_prefix(str(tmp_path))
    assert storage.exists("victim.bin")


@pytest.mark.parametrize("backend_kind", ["s3", "minio", "azure", "gcs"])
@pytest.mark.parametrize("path", ["folder/..", "//"])
def test_cloud_delete_prefix_rejects_dot_normalized_empty_prefix(
    backend_kind, path,
):
    if backend_kind == "s3":
        pytest.importorskip("boto3")
        from supertable.storage.s3_storage import S3Storage

        backend_type = S3Storage
    elif backend_kind == "minio":
        pytest.importorskip("minio")
        from supertable.storage.minio_storage import MinioStorage

        backend_type = MinioStorage
    elif backend_kind == "azure":
        pytest.importorskip("azure.storage.blob")
        from supertable.storage.azure_storage import AzureBlobStorage

        backend_type = AzureBlobStorage
    else:
        pytest.importorskip("google.cloud.storage")
        from supertable.storage.gcp_storage import GCSStorage

        backend_type = GCSStorage
    backend = backend_type.__new__(backend_type)
    with pytest.raises(ValueError, match="empty storage prefix"):
        backend.delete_prefix(path)


def test_local_json_directory_fsync_failure_propagates(tmp_path):
    storage = LocalStorage(root=tmp_path)
    with (
        patch(
            "supertable.storage.local_storage.os.fsync",
            side_effect=[None, OSError("directory fsync failed")],
        ),
        pytest.raises(OSError, match="directory fsync failed"),
    ):
        storage.write_json("meta.json", {"published": True})


def test_local_json_fsyncs_every_new_ancestor_through_storage_root(tmp_path):
    storage = LocalStorage(root=tmp_path)
    real_fsync = storage._fsync_directory
    synced = []

    def record(directory):
        synced.append(os.path.abspath(directory))
        real_fsync(directory)

    with patch.object(LocalStorage, "_fsync_directory", side_effect=record):
        storage.write_json("new/a/b/meta.json", {"published": True})

    assert synced == [
        str(tmp_path / "new" / "a" / "b"),
        str(tmp_path / "new" / "a"),
        str(tmp_path / "new"),
        str(tmp_path),
    ]
    assert storage.read_json("new/a/b/meta.json") == {"published": True}
