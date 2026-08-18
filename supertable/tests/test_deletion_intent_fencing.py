from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import fakeredis
import pyarrow as pa
import pytest

from supertable import redis_keys as RK
from supertable.redis_catalog import (
    DeletionIntentConflictError,
    RedisCatalog,
)
from supertable.rbac.permissions import RoleType
from supertable.simple_table import SimpleTable
from supertable.staging_area import Staging
from supertable.storage.local_storage import LocalStorage
from supertable.super_pipe import SuperPipe
from supertable.super_table import SuperTable


def _catalog() -> RedisCatalog:
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    connector = SimpleNamespace(r=fake)
    with patch("supertable.redis_catalog.RedisConnector", return_value=connector):
        return RedisCatalog()


def _seed_table(catalog: RedisCatalog) -> None:
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 1, "ts": 1}),
    )
    catalog.r.set(
        RK.meta_leaf("acme", "lake", "events"),
        json.dumps({
            "version": 0,
            "path": "acme/lake/tables/events/snapshots/0.json",
            "payload": {"schema": [], "resources": []},
        }),
    )
    catalog.r.sadd(RK.meta_table_names("acme", "lake"), "events")


def _simple_shell(catalog: RedisCatalog, storage: LocalStorage) -> SimpleTable:
    table = SimpleTable.__new__(SimpleTable)
    table.super_table = SimpleNamespace(
        organization="acme", super_name="lake", storage=storage,
    )
    table.identity = "tables"
    table.simple_name = "events"
    table.storage = storage
    table.catalog = catalog
    table.simple_dir = "acme/lake/tables/events"
    table.data_dir = f"{table.simple_dir}/data"
    table.snapshot_dir = f"{table.simple_dir}/snapshots"
    return table


def test_simple_delete_is_terminal_cleans_mirrors_and_requires_confirmed_recovery(
    tmp_path,
):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_table(catalog)
    for family in ("tables", "delta", "iceberg", "parquet"):
        storage.write_bytes(f"acme/lake/{family}/events/residual.bin", b"old")
    table = _simple_shell(catalog, storage)

    with patch("supertable.simple_table.check_control_access"):
        intent_id = table.delete("admin")

    for family in ("tables", "delta", "iceberg", "parquet"):
        assert storage.list_files(f"acme/lake/{family}/events") == []
    terminal = catalog.get_simple_deletion_intent("acme", "lake", "events")
    assert terminal["intent_id"] == intent_id
    assert terminal["status"] == "deleted"

    # A pre-delete writer resumes after delete returned. Its objects are
    # non-live because the terminal tombstone still blocks catalog publication
    # and normal recreation.
    storage.write_bytes("acme/lake/tables/events/data/stale.parquet", b"stale")
    stale_token = catalog.acquire_simple_lock("acme", "lake", "events")
    assert stale_token
    try:
        with pytest.raises(DeletionIntentConflictError):
            catalog.commit_snapshot(
                "acme", "lake", "events", {}, "stale.json",
                expected_version=-1,
                expected_path="",
                lock_token=stale_token,
            )
        with pytest.raises(DeletionIntentConflictError):
            catalog.set_table_config(
                "acme", "lake", "events", {}, lock_token=stale_token,
            )
    finally:
        catalog.release_simple_lock("acme", "lake", "events", stale_token)

    catalog.r.set(
        RK.meta_leaf("acme", "lake", "events"),
        json.dumps({"version": 99, "path": "stale.json"}),
    )
    parent = table.super_table
    with patch("supertable.simple_table.RedisCatalog", return_value=catalog):
        with pytest.raises(DeletionIntentConflictError):
            SimpleTable(parent, "events")

    catalog.r.delete(RK.meta_root("acme", "lake"))
    with (
        patch("supertable.simple_table.RedisCatalog", return_value=catalog),
        patch("supertable.simple_table.get_storage", return_value=storage),
        patch("supertable.simple_table.check_control_access"),
    ):
        with pytest.raises(PermissionError, match="previous owner has stopped"):
            SimpleTable.recover_pending_delete(
                organization="acme",
                super_name="lake",
                simple_name="events",
                role_name="admin",
                intent_id=intent_id,
            )
        assert SimpleTable.recover_pending_delete(
            organization="acme",
            super_name="lake",
            simple_name="events",
            role_name="admin",
            intent_id=intent_id,
            confirm_previous_owner_stopped=True,
        ) == intent_id

    assert catalog.get_simple_deletion_intent("acme", "lake", "events") is None
    assert not storage.exists("acme/lake/tables/events/data/stale.parquet")
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 1, "ts": 1}),
    )
    with patch("supertable.simple_table.RedisCatalog", return_value=catalog):
        recreated = SimpleTable(parent, "events")
    assert recreated.simple_name == "events"


def test_namespace_terminal_tombstone_preserves_rbac_and_recovers_without_root(
    tmp_path,
):
    catalog = _catalog()
    storage = LocalStorage(root=tmp_path)
    _seed_table(catalog)
    role_key = RK.rbac_role_doc("acme", "lake", "retained")
    catalog.r.hset(role_key, mapping={"role": "reader"})
    storage.write_bytes("acme/lake/tables/events/data/part.parquet", b"old")
    table = SuperTable.__new__(SuperTable)
    table.identity = "super"
    table.organization = "acme"
    table.super_name = "lake"
    table.storage = storage
    table.catalog = catalog
    table.super_dir = "acme/lake/super"
    superadmin = SimpleNamespace(role_type=RoleType.SUPERADMIN)

    with patch(
        "supertable.super_table.resolve_role_access_context",
        return_value=superadmin,
    ):
        intent_id = table.delete("root")

    assert not catalog.r.exists(RK.meta_root("acme", "lake"))
    assert catalog.r.exists(role_key)
    assert catalog.get_namespace_deletion_intent("acme", "lake")["status"] == "deleted"
    with pytest.raises(DeletionIntentConflictError):
        catalog.update_root_flags("acme", "lake", {"read_only": False})
    with pytest.raises(DeletionIntentConflictError):
        catalog.bump_root("acme", "lake")
    with pytest.raises(DeletionIntentConflictError):
        catalog.set_mirrors("acme", "lake", ["DELTA"])
    with pytest.raises(DeletionIntentConflictError):
        catalog.create_linked_share("acme", "lake", "late", {"id": "late"})
    with pytest.raises(DeletionIntentConflictError):
        catalog.update_linked_share("acme", "lake", "late", {"id": "late"})

    # Residual state recreated by a stale pre-delete caller remains fenced and
    # is swept by confirmed recovery before the tombstone is cleared.
    catalog.r.set(RK.schema("acme", "lake", "events"), "{}")
    catalog.r.set(RK.meta_root("acme", "lake"), "{}")
    with (
        patch("supertable.super_table.get_storage", return_value=storage),
        patch("supertable.super_table.RedisCatalog", return_value=catalog),
    ):
        with pytest.raises(DeletionIntentConflictError):
            SuperTable("lake", "acme")
    with (
        patch("supertable.super_table.get_storage", return_value=storage),
        patch("supertable.super_table.RedisCatalog", return_value=catalog),
        patch(
            "supertable.super_table.resolve_role_access_context",
            return_value=superadmin,
        ),
    ):
        assert SuperTable.recover_pending_delete(
            organization="acme",
            super_name="lake",
            role_name="root",
            intent_id=intent_id,
            confirm_previous_owner_stopped=True,
        ) == intent_id

    assert catalog.get_namespace_deletion_intent("acme", "lake") is None
    assert not catalog.r.exists(RK.schema("acme", "lake", "events"))
    assert catalog.r.exists(role_key)
    with (
        patch("supertable.super_table.get_storage", return_value=storage),
        patch("supertable.super_table.RedisCatalog", return_value=catalog),
        patch("supertable.super_table.RoleManager"),
        patch("supertable.super_table.UserManager"),
    ):
        recreated = SuperTable("lake", "acme")
    assert recreated.super_name == "lake"


def test_stale_stage_saver_cannot_succeed_or_enable_recreation_until_recovery(
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
        patch("supertable.staging_area.check_write_access"),
        patch("supertable.staging_area.check_meta_access"),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        stage.save_as_parquet(
            role_name="writer",
            arrow_table=pa.table({"id": [1]}),
            base_file_name="first.parquet",
        )

        # Saver A validated under its old token and then stalls with a cached
        # index. Force lease loss before deleter B enters.
        old_token = "old-saver-token"
        catalog.r.set(RK.lock_stage("acme", "lake", "uploads"), old_token)
        catalog.check_stage_mutation_allowed(
            "acme", "lake", "uploads", lock_token=old_token,
        )
        stale_index = storage.read_json(stage.files_index_path)
        catalog.r.delete(RK.lock_stage("acme", "lake", "uploads"))

        intent_id = stage.delete("writer")
        assert catalog.get_stage_deletion_intent(
            "acme", "lake", "uploads",
        )["status"] == "deleted"

        # A resumes after B returned and recreates fixed-path objects, but its
        # final fence fails and the terminal tombstone prevents saver C.
        storage.write_bytes(f"{stage.stage_dir}/stale.parquet", b"stale")
        storage.write_json(stage.files_index_path, stale_index)
        with pytest.raises(Exception):
            catalog.check_stage_mutation_allowed(
                "acme", "lake", "uploads", lock_token=old_token,
            )
        with pytest.raises(DeletionIntentConflictError):
            stage.save_as_parquet(
                role_name="writer",
                arrow_table=pa.table({"id": [2]}),
                base_file_name="new.parquet",
            )
        with pytest.raises(DeletionIntentConflictError):
            stage.list_files("reader")
        with pytest.raises(DeletionIntentConflictError):
            Staging(
                organization="acme",
                super_name="lake",
                staging_name="uploads",
            )
        with (
            patch("supertable.super_pipe.RedisCatalog", return_value=catalog),
            pytest.raises(DeletionIntentConflictError),
        ):
            SuperPipe(
                organization="acme",
                super_name="lake",
                staging_name="uploads",
            )

        assert stage.recover_delete(
            "writer",
            intent_id=intent_id,
            confirm_previous_owner_stopped=True,
        ) == intent_id
        assert catalog.get_stage_deletion_intent(
            "acme", "lake", "uploads",
        ) is None
        saved = stage.save_as_parquet(
            role_name="writer",
            arrow_table=pa.table({"id": [3]}),
            base_file_name="after-recovery.parquet",
        )
        assert saved.endswith(".parquet")
