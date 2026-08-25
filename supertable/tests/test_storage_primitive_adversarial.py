"""Adversarial contracts for staging, pipe, and restore storage primitives.

These tests deliberately exercise SDK boundaries directly.  Service-layer
validation is not an acceptable substitute because SDK users and background
workers can call these primitives without passing through the HTTP API.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import polars as pl
import pyarrow as pa
import pytest

import supertable.staging_area as staging_module
from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.staging_area import Staging
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_pipe import SuperPipe
from supertable.utils.profiler import Profiler


def _catalog() -> RedisCatalog:
    return RedisCatalog(
        redis_client=fakeredis.FakeStrictRedis(decode_responses=True),
    )


def _seed_root(catalog: RedisCatalog) -> None:
    catalog.r.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )


def _stage_shell(*, events: list[str] | None = None) -> Staging:
    """Build a stage without constructor I/O and with an empty live file map."""

    stage = Staging.__new__(Staging)
    stage.organization = "acme"
    stage.super_name = "lake"
    stage.staging_name = "uploads"
    stage._is_manager = False
    stage.base_staging_dir = "acme/lake/staging"
    stage.stage_dir = "acme/lake/staging/uploads"
    stage.files_index_path = "acme/lake/staging/uploads_files.json"
    stage.storage = MagicMock()
    stage.catalog = MagicMock()
    stage.catalog.get_staging_meta.return_value = {
        "organization": "acme",
        "super_name": "lake",
        "staging_name": "uploads",
        "path": stage.stage_dir,
        "files": {},
    }

    def with_lock(operation):
        if events is not None:
            events.append("lock-acquired")
        return operation("stage-lock-token")

    stage._with_lock = with_lock
    return stage


def _successor_shell() -> SimpleTable:
    table = SimpleTable.__new__(SimpleTable)
    table.super_table = SimpleNamespace(
        organization="acme",
        super_name="lake",
    )
    table.identity = "tables"
    table.simple_name = "events"
    table.simple_dir = "acme/lake/tables/events"
    table.data_dir = f"{table.simple_dir}/data"
    table.snapshot_dir = f"{table.simple_dir}/snapshots"
    table.storage = MagicMock()
    table.storage.exists.return_value = True
    table.catalog = MagicMock()
    table.catalog.acquire_simple_lock.return_value = "table-lock-token"
    table.catalog.get_leaf.return_value = {
        "version": 3,
        "ts": 10,
        "path": f"{table.snapshot_dir}/head.json",
        "payload": {
            "snapshot_version": 3,
            "schema": {"id": "long"},
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "_row_filter": None,
        },
    }
    table.catalog.commit_snapshot.return_value = (4, 8)
    table.catalog.get_mirrors.return_value = []
    return table


def _source_snapshot(resource_path: str) -> dict:
    return {
        "snapshot_version": 1,
        "schema": {"id": "long"},
        "resources": [{
            "file": resource_path,
            "rows": 1,
            "file_size": 128,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }


def _history_snapshot(
    version: int,
    previous: str | None,
    *,
    restore_commit_id: str | None = None,
) -> dict:
    snapshot = {
        "snapshot_version": version,
        "schema": {"id": "long"},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
        "previous_snapshot": previous,
    }
    if restore_commit_id is not None:
        snapshot["_restore_commit_id"] = restore_commit_id
    return snapshot


def _local_successor(tmp_path) -> tuple[SimpleTable, LocalStorage, str, int]:
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    storage.makedirs(table.snapshot_dir)
    resource_path = f"{table.data_dir}/part.parquet"
    storage.write_parquet(pa.table({"id": [1]}), resource_path)
    return table, storage, resource_path, storage.size(resource_path)


def _restore_two_physical_types(
    tmp_path,
    left_type: pa.DataType,
    right_type: pa.DataType,
    left_value: object,
    right_value: object,
) -> tuple[dict, SimpleTable, LocalStorage]:
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    storage.makedirs(table.snapshot_dir)
    paths = [
        f"{table.data_dir}/physical-left.parquet",
        f"{table.data_dir}/physical-right.parquet",
    ]
    for path, dtype, value in zip(
        paths,
        (left_type, right_type),
        (left_value, right_value),
    ):
        storage.write_parquet(
            pa.Table.from_arrays(
                [pa.array([value], type=dtype)], names=["id"],
            ),
            path,
        )
    source = _source_snapshot(paths[0])
    source["resources"] = [
        {
            "file": path,
            "rows": 1,
            "file_size": storage.size(path),
        }
        for path in paths
    ]
    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )
    return result, table, storage


def _pipe_shell() -> SuperPipe:
    pipe = SuperPipe.__new__(SuperPipe)
    pipe.organization = "acme"
    pipe.super_name = "lake"
    pipe.staging_name = "uploads"
    pipe.catalog = MagicMock()
    pipe.catalog.get_pipe_meta.return_value = None
    pipe.catalog.list_pipe_metas.return_value = []
    pipe.catalog.upsert_pipe_meta.return_value = True
    pipe._check_target_access = MagicMock()
    pipe._with_lock = lambda operation: operation("stage-lock-token")
    return pipe


def test_staging_rows_require_read_permission_not_meta_permission(monkeypatch):
    stage = _stage_shell()
    meta_gate = MagicMock()
    read_gate = MagicMock(side_effect=PermissionError("READ denied"))
    monkeypatch.setattr(staging_module, "check_meta_access", meta_gate)
    # ``check_read_access`` is the missing safe primitive this contract asks
    # the SDK to expose and use. ``raising=False`` keeps the failure behavioural
    # while production is still missing it.
    monkeypatch.setattr(
        staging_module, "check_read_access", read_gate, raising=False,
    )

    with pytest.raises(PermissionError, match="READ denied"):
        stage.read_parquet_files("meta-only-role")

    read_gate.assert_called()
    meta_gate.assert_not_called()


def test_legacy_staging_index_rejection_never_logs_poisoned_file_name(
    monkeypatch,
    caplog,
):
    secret = "../../https://host/capability/TOP-SECRET"
    stage = _stage_shell()
    stage.storage.exists.return_value = True
    monkeypatch.setattr(
        staging_module,
        "_read_sealed_json_object",
        lambda *_args, **_kwargs: [{"file": secret}],
    )

    result = stage._read_legacy_file_map(
        files_index_path=stage.files_index_path,
    )

    assert result == {}
    rendered = "\n".join(record.getMessage() for record in caplog.records)
    assert secret not in rendered
    assert "unsafe legacy file entry 0" in rendered


def test_staging_read_reauthorizes_after_waiting_for_stage_lock(monkeypatch):
    events: list[str] = []
    stage = _stage_shell(events=events)

    def fresh_role() -> str:
        events.append("fresh-authorization")
        return "live-role"

    read_gate = MagicMock()
    monkeypatch.setattr(
        staging_module, "check_read_access", read_gate, raising=False,
    )
    monkeypatch.setattr(staging_module, "check_meta_access", MagicMock())

    assert stage.read_parquet_files(
        "stale-role",
        authorization_callback=fresh_role,
    ) == []
    assert events == ["lock-acquired", "fresh-authorization"]
    assert read_gate.call_args_list[-1].kwargs["role_name"] == "live-role"


def test_staging_read_uses_bounded_metadata_by_default(monkeypatch):
    stage = _stage_shell()
    file_name = "stage_1_deadbeef.parquet"
    stage.catalog.get_staging_meta.return_value = {
        "organization": "acme",
        "super_name": "lake",
        "staging_name": "uploads",
        "path": stage.stage_dir,
        "files": {
            file_name: {
                "file": file_name,
                "rows": 1,
                "file_size": 1,
                "memory_bytes": 2048,
                "column_count": 1,
                "physical_column_count": 1,
                "schema_bytes": 16,
                "object_identity": "size=1|version=immutable",
                "written_at_ns": 1,
            },
        },
    }
    monkeypatch.setattr(staging_module, "check_read_access", MagicMock())

    with pytest.raises(ValueError, match="declared safety limits"):
        stage.read_parquet_files(
            "reader",
            file_names=[file_name],
            max_bytes=1024,
        )

    stage.storage.read_parquet.assert_not_called()
    stage.storage.download_to_file.assert_not_called()


def test_staging_read_cannot_disable_sealed_bounded_lane(monkeypatch):
    stage = _stage_shell()
    monkeypatch.setattr(staging_module, "check_read_access", MagicMock())

    with pytest.raises(ValueError, match="Bounded staging metadata is required"):
        stage.read_parquet_files(
            "reader",
            require_bounded_metadata=False,
        )

    stage.catalog.get_staging_meta.assert_not_called()
    stage.storage.size.assert_not_called()
    stage.storage.stat_object.assert_not_called()
    stage.storage.download_to_file.assert_not_called()
    stage.storage.read_parquet.assert_not_called()


@pytest.mark.parametrize(
    ("limit_name", "oversized"),
    [
        ("max_files", 257),
        ("max_rows", 5_000_001),
        ("max_bytes", 512 * 1024 * 1024 + 1),
        ("max_columns", 4097),
        ("max_schema_bytes", 1024 * 1024 + 1),
    ],
)
def test_staging_read_limits_can_only_tighten_hard_ceilings(
    monkeypatch,
    limit_name,
    oversized,
):
    stage = _stage_shell()
    monkeypatch.setattr(staging_module, "check_read_access", MagicMock())

    with pytest.raises(ValueError, match="only tighten the hard ceiling"):
        stage.read_parquet_files("reader", **{limit_name: oversized})

    stage.catalog.get_staging_meta.assert_not_called()
    stage.storage.size.assert_not_called()
    stage.storage.stat_object.assert_not_called()
    stage.storage.download_to_file.assert_not_called()
    stage.storage.read_parquet.assert_not_called()


def test_staging_read_rejects_file_fanout_before_normalizing_or_locking(
    monkeypatch,
):
    events: list[str] = []
    stage = _stage_shell(events=events)
    monkeypatch.setattr(staging_module, "check_read_access", MagicMock())

    # The invalid final element proves the length guard runs before the
    # per-name normalization loop, while ``events`` proves no lock/catalog or
    # object operation was reached.
    names = ["stage_1_deadbeef.parquet"] * 256 + ["../escape.parquet"]
    with pytest.raises(ValueError, match="fan-out exceeds"):
        stage.read_parquet_files("reader", file_names=names)

    assert events == []
    stage.catalog.get_staging_meta.assert_not_called()
    stage.storage.size.assert_not_called()
    stage.storage.stat_object.assert_not_called()
    stage.storage.download_to_file.assert_not_called()
    stage.storage.read_parquet.assert_not_called()


def test_staging_bounded_read_allows_parquet_representation_drift(tmp_path):
    catalog = _catalog()
    _seed_root(catalog)
    storage = LocalStorage(root=tmp_path)
    dictionary = pa.array(["short", "a substantially longer value"])
    source = pa.table({
        # Parquet does not preserve Arrow chunk boundaries. Two equivalent
        # dictionary chunks therefore force a representation-size change on
        # every supported PyArrow 23 build without changing the decoded data.
        "kind": pa.chunked_array([
            pa.DictionaryArray.from_arrays(
                pa.array([0, 1], type=pa.int8()), dictionary,
            ),
            pa.DictionaryArray.from_arrays(
                pa.array([0, 1], type=pa.int8()), dictionary,
            ),
        ]),
    })

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stage = Staging(
            organization="acme",
            super_name="lake",
            staging_name="uploads",
        )
        file_name = stage.save_as_parquet(
            role_name="creator",
            arrow_table=source,
            base_file_name="dictionary.parquet",
        )

    with patch("supertable.staging_area.check_read_access"):
        restored = stage.read_parquet_files(
            "reader",
            file_names=[file_name],
        )

    assert len(restored) == 1
    assert restored[0].to_pydict() == source.to_pydict()
    # Bounds apply independently to the declared, encoded, expanded and
    # decoded sizes; byte-for-byte equality is not an integrity invariant.
    assert restored[0].nbytes != source.nbytes


def test_staging_bounded_read_seals_nested_physical_leaf_count(tmp_path):
    catalog = _catalog()
    _seed_root(catalog)
    storage = LocalStorage(root=tmp_path)
    source = pa.table({
        "payload": pa.array(
            [{"id": 7, "label": "nested"}],
            type=pa.struct([
                pa.field("id", pa.int64()),
                pa.field("label", pa.string()),
            ]),
        ),
    })

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stage = Staging(
            organization="acme",
            super_name="lake",
            staging_name="uploads",
        )
        file_name = stage.save_as_parquet(
            role_name="creator",
            arrow_table=source,
            base_file_name="nested.parquet",
        )

    metadata = catalog.get_staging_meta("acme", "lake", "uploads")
    assert metadata["files"][file_name]["column_count"] == 1
    assert metadata["files"][file_name]["physical_column_count"] == 2
    with patch("supertable.staging_area.check_read_access"):
        restored = stage.read_parquet_files(
            "reader",
            file_names=[file_name],
        )
    assert restored[0].to_pydict() == source.to_pydict()


def test_staging_publication_atomically_rejects_rbac_revocation(
    tmp_path, monkeypatch,
):
    catalog = _catalog()
    _seed_root(catalog)
    storage = LocalStorage(root=tmp_path)

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stage = Staging(
            organization="acme",
            super_name="lake",
            staging_name="uploads",
        )
        original = catalog._upsert_staging_meta

        def revoke_at_publication(*, keys, args):
            catalog.r.hset(
                RK.rbac_role_meta("acme", "lake"),
                mapping={"version": "1", "initialized": "true"},
            )
            return original(keys=keys, args=args)

        monkeypatch.setattr(
            catalog, "_upsert_staging_meta", revoke_at_publication,
        )
        with pytest.raises(PermissionError, match="authority changed"):
            stage.save_as_parquet(
                role_name="creator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="revoked.parquet",
            )

    assert catalog.get_staging_meta("acme", "lake", "uploads") is None
    assert storage.list_files(stage.stage_dir, "*.parquet") == []


def test_staging_footer_expansion_uses_remaining_aggregate_budget(monkeypatch):
    stage = _stage_shell()
    files = {}
    for index in (1, 2):
        name = f"stage_{index}_deadbeef.parquet"
        files[name] = {
            "file": name,
            "rows": 1,
            "file_size": 1,
            "memory_bytes": 1,
            "column_count": 1,
            "physical_column_count": 1,
            "schema_bytes": 16,
            "object_identity": "size=1|version=immutable",
            "written_at_ns": index,
        }
    stage.catalog.get_staging_meta.return_value = {
        "organization": "acme",
        "super_name": "lake",
        "staging_name": "uploads",
        "path": stage.stage_dir,
        "files": files,
    }
    stage.storage.size.return_value = 1
    stage.storage.stat_object.return_value = ObjectMetadata(
        size=1,
        version="immutable",
    )

    def download(_path, sink, **_kwargs):
        sink.write(b"x")
        return 1

    stage.storage.download_to_file.side_effect = download
    group = SimpleNamespace(
        num_columns=1,
        column=lambda _index: SimpleNamespace(total_uncompressed_size=6),
    )
    metadata = SimpleNamespace(
        num_rows=1,
        num_row_groups=1,
        num_columns=1,
        row_group=lambda _index: group,
    )
    first_table = pa.table({"id": [1]})
    monkeypatch.setattr(staging_module, "check_read_access", MagicMock())

    with (
        patch("pyarrow.parquet.ParquetFile", return_value=SimpleNamespace(
            metadata=metadata,
        )),
        patch("pyarrow.parquet.read_table", return_value=first_table) as read,
    ):
        with pytest.raises(ValueError, match="expansion.*memory limit"):
            stage.read_parquet_files("reader", max_bytes=10)

    # The second file is rejected from its bounded footer before decoding it.
    assert read.call_count == 1


def test_upload_reconciliation_waits_for_a_paused_writer_then_removes_orphan(
    tmp_path,
):
    stage = _stage_shell()
    stage.storage = LocalStorage(root=tmp_path)
    stage.storage.makedirs(stage.stage_dir)
    target = "stage_1_deadbeef.parquet"
    journal = stage._write_reconcile_journal([target], operation="upload")

    # Absence alone is not proof: an expired writer may still be paused just
    # before its object write, so automatic recovery retains the journal.
    assert stage._reconcile_pending_objects(live_files={}) == 0
    assert stage.storage.exists(journal)

    target_path = os.path.join(stage.stage_dir, target)
    stage.storage.write_bytes(target_path, b"orphan")
    assert stage._reconcile_pending_objects(live_files={}) == 1
    assert not stage.storage.exists(target_path)
    assert not stage.storage.exists(journal)


def test_delete_reconciliation_is_durable_after_unpublication(tmp_path):
    stage = _stage_shell()
    stage.storage = LocalStorage(root=tmp_path)
    stage.storage.makedirs(stage.stage_dir)
    target = "stage_2_deadbeef.parquet"
    target_path = os.path.join(stage.stage_dir, target)
    stage.storage.write_bytes(target_path, b"pending-delete")
    journal = stage._write_reconcile_journal([target], operation="delete")

    assert stage._reconcile_pending_objects(live_files={}) == 1
    assert not stage.storage.exists(target_path)
    assert not stage.storage.exists(journal)


@pytest.mark.parametrize("stat_mode", ["error", "identity-less"])
def test_staging_upload_does_not_publish_or_orphan_without_stable_identity(
    tmp_path,
    stat_mode,
):
    catalog = _catalog()
    _seed_root(catalog)
    storage = LocalStorage(root=tmp_path)

    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
    ):
        stage = Staging(
            organization="acme",
            super_name="lake",
            staging_name="uploads",
        )

    if stat_mode == "error":
        stat_result = OSError("stat unavailable")
    else:
        stat_result = ObjectMetadata(size=1)

    with (
        patch("supertable.staging_area.check_create_access"),
        patch.object(storage, "stat_object", side_effect=[stat_result]),
    ):
        with pytest.raises((OSError, RuntimeError), match="stat|identity"):
            stage.save_as_parquet(
                role_name="creator",
                arrow_table=pa.table({"id": [1]}),
                base_file_name="upload.parquet",
            )

    assert catalog.get_staging_meta("acme", "lake", "uploads") is None
    physical_stage = tmp_path / stage.stage_dir
    assert not physical_stage.exists() or not list(
        physical_stage.glob("*.parquet")
    )


@pytest.mark.parametrize(
    "resource_path",
    [
        pytest.param(
            os.path.abspath(
                "acme/lake/tables/events/data/part.parquet",
            ),
            id="absolute-logical-key",
        ),
        pytest.param(
            "acme/lake/tables/events/data/./part.parquet",
            id="dot-component",
        ),
        pytest.param(
            "acme/lake/tables/events/data/nested/../part.parquet",
            id="parent-component-even-when-normalized-inside",
        ),
    ],
)
def test_successor_rejects_noncanonical_resource_paths_before_storage_write(
    resource_path,
):
    table = _successor_shell()

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="path|canonical|absolute"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=_source_snapshot(resource_path),
            )

    table.storage.write_json.assert_not_called()
    table.catalog.commit_snapshot.assert_not_called()


def test_successor_requires_mirror_workflow_before_writing_snapshot():
    table = _successor_shell()
    table.catalog.get_mirrors.return_value = ["PARQUET"]
    table.catalog.commit_snapshot.side_effect = RuntimeError(
        "Corrupt mirror configuration during snapshot publication",
    )

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(RuntimeError, match="mirror"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=_source_snapshot(
                    "acme/lake/tables/events/data/part.parquet",
                ),
            )

    # A caller may eventually provide a complete mirror-publication workflow.
    # Until then, fail before leaving an immutable orphan in storage.
    table.catalog.get_mirrors.assert_called_once_with("acme", "lake")
    table.storage.write_json.assert_not_called()
    table.catalog.commit_snapshot.assert_not_called()


def test_successor_seals_and_validates_real_parquet_before_publication(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    resource = result["snapshot"]["resources"][0]
    assert resource["rows"] == 1
    assert resource["file_size"] == file_size
    assert resource["object_seal"]["size"] == file_size
    assert resource["object_seal"]["version"]
    assert storage.exists(result["snapshot_path"])
    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []
    table.catalog.commit_snapshot.assert_called_once()


def test_successor_rejects_declared_size_before_snapshot_write(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size + 1

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(RuntimeError, match="size or identity"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []
    table.catalog.commit_snapshot.assert_not_called()


def test_successor_rejects_local_symlink_into_another_table(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    other_dir = "acme/lake/tables/other/data"
    storage.makedirs(other_dir)
    other_path = f"{other_dir}/part.parquet"
    storage.write_parquet(pa.table({"id": [1]}), other_path)
    linked_path = f"{table.data_dir}/part.parquet"
    os.symlink(
        storage.to_duckdb_path(other_path),
        storage.to_duckdb_path(linked_path),
    )
    source = _source_snapshot(linked_path)
    source["resources"][0]["file_size"] = storage.size(linked_path)

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="physical table namespace"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()


def test_successor_cleans_snapshot_when_final_authorization_is_revoked(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    roles = iter(["controller", "controller", "revoked"])

    def authorize(*_args, **kwargs):
        if kwargs.get("role_name") == "revoked":
            raise PermissionError("revoked")

    with patch("supertable.simple_table.check_control_access", side_effect=authorize):
        with pytest.raises(PermissionError, match="revoked"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
                authorization_callback=lambda: next(roles),
            )

    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []
    assert storage.list_files(table.snapshot_dir, "*.json") == []
    table.catalog.commit_snapshot.assert_not_called()


def test_successor_atomic_commit_rejects_revoke_after_final_check(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    base_leaf = dict(table.catalog.get_leaf.return_value)

    class RevokingCatalog:
        def __init__(self):
            self.generation = (1, 1, 3, 10)
            self.commit_generation = None
            self.commit_calls = 0

        def acquire_simple_lock(self, *_args, **_kwargs):
            return "table-lock-token"

        def release_simple_lock(self, *_args, **_kwargs):
            return True

        def check_deletion_intent_absent(self, *_args, **_kwargs):
            return None

        def get_leaf(self, *_args, **_kwargs):
            return dict(base_leaf)

        def get_mirrors(self, *_args, **_kwargs):
            return []

        def sample_write_authority_generation(self, *_args):
            return self.generation

        def validate_write_authority_generation(self, *_args):
            return tuple(_args[-1]) == self.generation

        def commit_snapshot(
            self,
            *_args,
            expected_write_authority_generation=None,
            **_kwargs,
        ):
            self.commit_calls += 1
            self.commit_generation = tuple(
                expected_write_authority_generation or (),
            )
            # Model a revoke in the atomic Redis script's commit window: the
            # final full policy check was valid, but its generation is stale by
            # the instant publication is attempted.
            self.generation = (2, 1, 3, 10)
            if self.commit_generation != self.generation:
                raise PermissionError(
                    "Write authority changed before snapshot publication"
                )
            return 4, 8

    catalog = RevokingCatalog()
    table.catalog = catalog

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(PermissionError, match="Write authority changed"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    assert catalog.commit_calls == 1
    assert catalog.commit_generation == (1, 1, 3, 10)
    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []
    assert storage.list_files(table.snapshot_dir, "*.json") == []


def test_restore_reconciliation_preserves_committed_historical_ancestor(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.snapshot_dir)
    candidate = f"{table.snapshot_dir}/candidate-v3.json"
    base = f"{table.snapshot_dir}/version-v2.json"
    version_four = f"{table.snapshot_dir}/version-v4.json"
    current = f"{table.snapshot_dir}/version-v5.json"
    storage.write_json(candidate, _history_snapshot(
        3, base, restore_commit_id="restore-v3",
    ))
    storage.write_json(version_four, _history_snapshot(4, candidate))
    storage.write_json(current, _history_snapshot(5, version_four))
    journal = table._write_restore_journal(
        snapshot_path=candidate,
        commit_id="restore-v3",
        snapshot_version=3,
        base_path=base,
    )

    assert table._reconcile_restore_journals({
        "version": 5,
        "path": current,
        "commit_id": "newer-commit",
        "payload": _history_snapshot(5, version_four),
    }) == 1
    assert storage.exists(candidate)
    assert not storage.exists(journal)


def test_restore_reconciliation_deletes_only_proven_unpublished_candidate(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.snapshot_dir)
    candidate = f"{table.snapshot_dir}/orphan-v3.json"
    base = f"{table.snapshot_dir}/version-v2.json"
    other_version_three = f"{table.snapshot_dir}/published-v3.json"
    version_four = f"{table.snapshot_dir}/version-v4.json"
    current = f"{table.snapshot_dir}/version-v5.json"
    storage.write_json(candidate, _history_snapshot(
        3, base, restore_commit_id="failed-restore-v3",
    ))
    storage.write_json(version_four, _history_snapshot(4, other_version_three))
    storage.write_json(current, _history_snapshot(5, version_four))
    journal = table._write_restore_journal(
        snapshot_path=candidate,
        commit_id="failed-restore-v3",
        snapshot_version=3,
        base_path=base,
    )

    assert table._reconcile_restore_journals({
        "version": 5,
        "path": current,
        "commit_id": "newer-commit",
        "payload": _history_snapshot(5, version_four),
    }) == 1
    assert not storage.exists(candidate)
    assert not storage.exists(journal)


def test_restore_absent_intent_requires_explicit_inactive_owner_proof(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.snapshot_dir)
    candidate = f"{table.snapshot_dir}/not-written-v4.json"
    journal = table._write_restore_journal(
        snapshot_path=candidate,
        commit_id="paused-restore-v4",
        snapshot_version=4,
        base_path=table.catalog.get_leaf.return_value["path"],
    )

    with patch("supertable.simple_table.check_control_access"):
        assert table.recover_pending_restore_objects("controller") == 0
        assert storage.exists(journal)
        assert table.recover_pending_restore_objects(
            "controller",
            confirm_previous_owner_stopped=True,
        ) == 1

    assert not storage.exists(journal)
    assert table.catalog.release_simple_lock.call_count == 2


def test_restore_commit_timeout_recognizes_candidate_under_newer_successor(
    tmp_path,
):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    initial_leaf = table.catalog.get_leaf.return_value
    candidate: dict[str, str] = {}
    leaf_reads = 0

    def get_leaf(*_args):
        nonlocal leaf_reads
        leaf_reads += 1
        if leaf_reads == 1:
            return initial_leaf
        return {
            "version": 5,
            "path": f"{table.snapshot_dir}/successor-v5.json",
            "commit_id": "successor-commit",
            "payload": _history_snapshot(5, candidate["path"]),
        }

    def ambiguous_commit(*args, **_kwargs):
        candidate["path"] = args[4]
        raise TimeoutError("reply lost after commit")

    table.catalog.get_leaf.side_effect = get_leaf
    table.catalog.commit_snapshot.side_effect = ambiguous_commit
    table.catalog.get_root.return_value = {"version": 9}

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    assert result["snapshot_path"] == candidate["path"]
    assert result["leaf_version"] == 4
    assert storage.exists(candidate["path"])
    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []


def test_successor_semantically_validates_legacy_deletion_vector(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    tombstone_dir = f"{table.simple_dir}/tombstone"
    storage.makedirs(tombstone_dir)
    tombstone_path = f"{tombstone_dir}/malformed.parquet"
    storage.write_parquet(pa.table({"untrusted": [1]}), tombstone_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    source.update({
        "tombstone": tombstone_path,
        "tombstone_rows": 1,
        "tombstone_digest": "0" * 64,
    })

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises((RuntimeError, ValueError), match="deletion|tombstone"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()
    assert storage.list_files(
        table.snapshot_dir, "supertable_restore_pending_*.json",
    ) == []


def test_successor_strips_unproven_rowid_and_stats_authority_then_migrates(
    tmp_path,
):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    storage.makedirs(table.snapshot_dir)
    resource_path = f"{table.data_dir}/rowid-100.parquet"
    storage.write_parquet(pa.table({
        "id": [1],
        "__rowid__": pa.array([100], type=pa.int64()),
    }), resource_path)
    source = _source_snapshot(resource_path)
    source["resources"][0].update({
        "file_size": storage.size(resource_path),
        "rowid_integrity": {
            "version": 1,
            "rows": 1,
            "nonnull": 1,
            "unique": 1,
            "minimum": 1,
            "maximum": 1,
            "digest": "0" * 64,
            "footer_sha256": "0" * 64,
        },
        "column_max_value_bytes": {"id": 0},
        "provider_cache_identity": "attacker-selected",
    })
    source.update({
        "rowid_high_watermark": 0,
        "stats_file": f"{table.simple_dir}/stats/forged.parquet",
        "stats_rows": 1,
        "_linked_share": {"provider": "forged"},
    })

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    restored = result["snapshot"]
    assert "rowid_high_watermark" not in restored
    assert restored["stats_file"] is None
    assert restored["stats_rows"] == 0
    assert "_linked_share" not in restored
    assert "rowid_integrity" not in restored["resources"][0]
    assert "column_max_value_bytes" not in restored["resources"][0]
    assert "provider_cache_identity" not in restored["resources"][0]

    from supertable.data_writer import DataWriter

    class FloorCatalog:
        def reserve_rowids_at_least(
            self, _org, _sup, _simple, count, floor, *, lock_token,
        ):
            assert lock_token == "table-lock-token"
            return floor + 1, floor + count

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        organization="acme",
        super_name="lake",
        storage=storage,
    )
    writer.catalog = FloorCatalog()
    with patch(
        "supertable.data_writer._read_parquet_safe",
        return_value=pl.DataFrame({"__rowid__": [100]}),
    ):
        start, high = writer._reserve_snapshot_rowids(
            snapshot=restored,
            simple_name="events",
            count=1,
            profiler=Profiler(),
            lock_token="table-lock-token",
        )

    assert (start, high) == (101, 101)


def test_successor_rejects_forged_resource_stats_seal(tmp_path):
    table, storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0].update({
        "file_size": file_size,
        "footer_sha256": "0" * 64,
        "stats_rows": 1,
        "stats_digest": "0" * 64,
    })

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(RuntimeError, match="statistics.*footer"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()
    assert storage.list_files(table.snapshot_dir, "*.json") == []


def test_successor_rejects_schema_not_present_in_physical_resources(tmp_path):
    table, _storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    source["schema"] = {"forged": "string"}

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="schema.*resources"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()


def test_successor_derives_schema_type_from_physical_footer(tmp_path):
    table, _storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    source["schema"] = {"id": "attacker-controlled-binary"}

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    assert result["snapshot"]["schema"] == {"id": "Int64"}


def test_successor_widens_schema_across_every_physical_resource(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    storage.makedirs(table.snapshot_dir)
    integer_path = f"{table.data_dir}/integer.parquet"
    float_path = f"{table.data_dir}/float.parquet"
    storage.write_parquet(
        pa.table({"id": pa.array([1], type=pa.int32())}), integer_path,
    )
    storage.write_parquet(
        pa.table({"id": pa.array([1.5], type=pa.float64())}), float_path,
    )
    source = _source_snapshot(integer_path)
    source["resources"] = [
        {
            "file": integer_path,
            "rows": 1,
            "file_size": storage.size(integer_path),
        },
        {
            "file": float_path,
            "rows": 1,
            "file_size": storage.size(float_path),
        },
    ]

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    assert result["snapshot"]["schema"] == {"id": "Float64"}


@pytest.mark.parametrize(
    ("left_type", "right_type", "left_value", "right_value", "expected"),
    [
        pytest.param(
            pa.int16(), pa.uint16(), -1, 65_535, "Int32",
            id="signed-unsigned-integer",
        ),
        pytest.param(
            pa.int64(), pa.uint64(), -1, (1 << 64) - 1,
            "Decimal(precision=20, scale=0)",
            id="full-width-signed-unsigned-decimal",
        ),
        pytest.param(
            pa.decimal128(12, 2), pa.decimal128(12, 4),
            Decimal("1234567890.12"), Decimal("12345678.1234"),
            "Decimal(precision=14, scale=4)",
            id="decimal-integer-and-fractional-digits",
        ),
        pytest.param(
            pa.timestamp("us", tz="UTC"), pa.timestamp("ns", tz="UTC"),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            "Datetime(time_unit='ns', time_zone='UTC')",
            id="datetime-unit-widening",
        ),
        pytest.param(
            pa.duration("ms"), pa.duration("us"),
            timedelta(milliseconds=1), timedelta(microseconds=1),
            "Duration(time_unit='us')",
            id="duration-unit-widening",
        ),
    ],
)
def test_successor_uses_lossless_physical_type_lattice(
    tmp_path,
    left_type,
    right_type,
    left_value,
    right_value,
    expected,
):
    result, _table, _storage = _restore_two_physical_types(
        tmp_path,
        left_type,
        right_type,
        left_value,
        right_value,
    )
    assert result["snapshot"]["schema"] == {"id": expected}


@pytest.mark.parametrize(
    ("left_type", "right_type", "left_value", "right_value"),
    [
        pytest.param(
            pa.int64(), pa.float64(), 9_007_199_254_740_993, 1.5,
            id="int64-float64",
        ),
        pytest.param(
            pa.date32(), pa.timestamp("ms"),
            date(2024, 1, 1), datetime(2024, 1, 1),
            id="date-datetime",
        ),
        pytest.param(
            pa.timestamp("us", tz="UTC"),
            pa.timestamp("us", tz="Europe/Budapest"),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            id="datetime-timezones",
        ),
    ],
)
def test_successor_rejects_incompatible_physical_type_pairs(
    tmp_path,
    left_type,
    right_type,
    left_value,
    right_value,
):
    with pytest.raises(ValueError, match="incompatible|lossless"):
        _restore_two_physical_types(
            tmp_path,
            left_type,
            right_type,
            left_value,
            right_value,
        )


def test_successor_rejects_duplicate_physical_top_level_names(tmp_path):
    table = _successor_shell()
    storage = LocalStorage(root=tmp_path)
    table.storage = storage
    storage.makedirs(table.data_dir)
    storage.makedirs(table.snapshot_dir)
    resource_path = f"{table.data_dir}/duplicate.parquet"
    storage.write_parquet(
        pa.Table.from_arrays(
            [pa.array([1]), pa.array([2])], names=["id", "id"],
        ),
        resource_path,
    )
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = storage.size(resource_path)

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="repeats a column"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()


def test_successor_bounded_decoder_accepts_valid_legacy_tombstone(tmp_path):
    from supertable.processing import TOMBSTONE_SCHEMA, tombstone_digest

    table, storage, resource_path, file_size = _local_successor(tmp_path)
    tombstone_dir = f"{table.simple_dir}/tombstone"
    storage.makedirs(tombstone_dir)
    tombstone_path = f"{tombstone_dir}/valid.parquet"
    frame = pl.DataFrame(
        {"__file__": [resource_path], "__rowid__": [1]},
        schema=TOMBSTONE_SCHEMA,
    )
    storage.write_parquet(frame.to_arrow(), tombstone_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    source.update({
        "tombstone": tombstone_path,
        "tombstone_rows": 1,
        "tombstone_digest": tombstone_digest(frame),
    })

    with patch("supertable.simple_table.check_control_access"):
        result = table.publish_restored_successor(
            role_name="controller",
            source_snapshot=source,
        )

    assert result["snapshot"]["tombstone"] == tombstone_path
    assert result["snapshot"]["tombstone_rows"] == 1


def test_successor_rejects_tombstones_beyond_physical_row_count(tmp_path):
    table, _storage, resource_path, file_size = _local_successor(tmp_path)
    source = _source_snapshot(resource_path)
    source["resources"][0]["file_size"] = file_size
    source.update({
        "tombstone": f"{table.simple_dir}/tombstone/oversized.parquet",
        "tombstone_rows": 2,
        "tombstone_digest": "0" * 64,
    })

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(ValueError, match="row count exceeds"):
            table.publish_restored_successor(
                role_name="controller",
                source_snapshot=source,
            )

    table.catalog.commit_snapshot.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"overwrite_columns": [f"column_{i}" for i in range(4097)]},
            id="column-fanout",
        ),
        pytest.param(
            {"overwrite_columns": ["x" * 1025]},
            id="oversized-column-name",
        ),
        pytest.param(
            {"user_hash": "u" * 4097},
            id="oversized-user-identity",
        ),
        pytest.param(
            {"enabled": "true"},
            id="non-boolean-enabled",
        ),
    ],
)
def test_pipe_payload_bounds_are_enforced_before_catalog_mutation(overrides):
    pipe = _pipe_shell()
    arguments = {
        "role_name": "writer",
        "pipe_name": "daily",
        "simple_name": "events",
        "user_hash": "user-1",
        "overwrite_columns": ["id"],
        "enabled": True,
    }
    arguments.update(overrides)

    with pytest.raises((TypeError, ValueError)):
        pipe.create(**arguments)

    pipe.catalog.upsert_pipe_meta.assert_not_called()


def test_pipe_fallback_scan_clamps_attacker_controlled_count(monkeypatch):
    catalog = _catalog()
    observed_counts: list[int] = []

    def bounded_scan(*, cursor, match, count):
        observed_counts.append(count)
        return 0, []

    monkeypatch.setattr(catalog.r, "scan", bounded_scan)
    assert catalog.list_pipes(
        "acme", "lake", "uploads", count=10**9,
    ) == []
    assert observed_counts
    assert max(observed_counts) <= 1000


def test_staging_index_cannot_reference_missing_metadata():
    catalog = _catalog()
    catalog.r.sadd(RK.staging_index("acme", "lake"), "uploads")

    with pytest.raises(RuntimeError, match="missing|disagree|corrupt"):
        catalog.list_stagings("acme", "lake")


def test_pipe_index_cannot_reference_missing_metadata():
    catalog = _catalog()
    catalog.r.sadd(
        RK.pipe_index("acme", "lake", "uploads"), "daily",
    )

    with pytest.raises(RuntimeError, match="missing|disagree|corrupt"):
        catalog.list_pipes("acme", "lake", "uploads")
