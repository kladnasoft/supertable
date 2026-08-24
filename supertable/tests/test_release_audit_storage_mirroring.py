from __future__ import annotations

import json
import os
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pyarrow as pa
import pytest

from supertable import redis_keys as RK
from supertable.errors import LockLostError
from supertable.mirroring.mirror_delta import (
    verify_delta_table,
    write_delta_table,
)
from supertable.mirroring.mirror_formats import (
    MirrorFormats,
    MirrorRecoveryConfirmationRequired,
)
from supertable.mirroring.mirror_iceberg import (
    verify_iceberg_table,
    write_iceberg_table,
)
from supertable.mirroring.mirror_parquet import (
    verify_parquet_table,
    write_parquet_table,
)
from supertable.rbac.permissions import RoleType
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.staging_area import Staging
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable


class GsUriLocalStorage(LocalStorage):
    """Local object persistence with a GCS-style canonical URI namespace."""

    def __init__(self, root, *, base_prefix: str) -> None:
        super().__init__(root=root)
        self.base_prefix = base_prefix.strip("/")

    def canonical_uri(self, path: str) -> str:
        return f"gs://mirror-bucket/{self._with_base(path)}"


class CopyFailingStorage(GsUriLocalStorage):
    def copy(self, src_path: str, dst_path: str) -> None:
        raise OSError("server-side copy failed")

    def read_bytes(self, path: str) -> bytes:
        raise OSError("fallback download failed")


class SilentFirstCopyStorage(GsUriLocalStorage):
    def __init__(self, root, *, base_prefix: str) -> None:
        super().__init__(root, base_prefix=base_prefix)
        self.copy_attempts = 0

    def copy(self, src_path: str, dst_path: str) -> None:
        self.copy_attempts += 1
        if self.copy_attempts == 1:
            return
        super().copy(src_path, dst_path)


class CorruptFirstDeltaLogStorage(LocalStorage):
    def __init__(self, root) -> None:
        super().__init__(root=root)
        self.corrupt_log = True

    def write_bytes(self, path: str, data: bytes) -> None:
        if self.corrupt_log and path.endswith(".json") and "/_delta_log/" in path:
            self.corrupt_log = False
            return super().write_bytes(path, b"BROKEN")
        return super().write_bytes(path, data)


class CorruptIcebergAvroStorage(LocalStorage):
    def write_bytes(self, path: str, data: bytes) -> None:
        if path.endswith(".avro"):
            return super().write_bytes(path, b"BROKEN")
        return super().write_bytes(path, data)


def _table(storage):
    return SimpleNamespace(
        organization="acme",
        super_name="lake",
        storage=storage,
    )


def _snapshot(*, commit_id: str = "commit-41"):
    return {
        "simple_name": "events",
        "snapshot_version": 41,
        "schema": [{"name": "id", "type": "Int64"}],
        "resources": [
            {
                "file": "acme/lake/simple/events/data/part.parquet",
                "file_size": 4,
                "rows": 1,
            }
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
        "_mirror_commit_id": commit_id,
        "_mirror_snapshot_path": "acme/lake/simple/events/snapshots/41.json",
    }


def test_delta_bootstrap_always_starts_at_version_zero(tmp_path):
    storage = LocalStorage(root=tmp_path)
    storage.write_bytes("acme/lake/simple/events/data/part.parquet", b"PAR1")
    snapshot = _snapshot()

    write_delta_table(_table(storage), "events", snapshot)

    log_dir = "acme/lake/delta/events/_delta_log"
    assert storage.list_files(log_dir, "*.json") == [
        f"{log_dir}/00000000000000000000.json"
    ]
    actions = [
        json.loads(line)
        for line in storage.read_text(
            f"{log_dir}/00000000000000000000.json"
        ).splitlines()
    ]
    assert any("protocol" in action for action in actions)
    assert any("metaData" in action for action in actions)
    assert any(
        action.get("commitInfo", {}).get("txnId") == "commit-41"
        for action in actions
    )
    verify_delta_table(
        _table(storage), "events", snapshot, commit_id="commit-41",
    )


def test_delta_silent_log_corruption_is_rejected_and_retry_repairs_v0(tmp_path):
    storage = CorruptFirstDeltaLogStorage(tmp_path)
    storage.write_bytes("acme/lake/simple/events/data/part.parquet", b"PAR1")
    snapshot = _snapshot()
    table = _table(storage)

    with pytest.raises(RuntimeError, match="read-after-write verification"):
        write_delta_table(table, "events", snapshot)

    write_delta_table(table, "events", snapshot)
    verify_delta_table(table, "events", snapshot, commit_id="commit-41")
    assert storage.list_files(
        "acme/lake/delta/events/_delta_log", "*.json",
    ) == ["acme/lake/delta/events/_delta_log/00000000000000000000.json"]


@pytest.mark.integration
def test_delta_output_opens_with_delta_rs_when_installed(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    storage = LocalStorage(root=tmp_path)
    source = "acme/lake/simple/events/data/part.parquet"
    storage.write_parquet(pa.table({"id": pa.array([1], type=pa.int64())}), source)
    snapshot = _snapshot()
    snapshot["resources"][0]["file_size"] = storage.size(source)

    write_delta_table(_table(storage), "events", snapshot)

    delta = deltalake.DeltaTable(
        str(tmp_path / "acme/lake/delta/events")
    )
    assert delta.version() == 0
    assert delta.to_pyarrow_table().column("id").to_pylist() == [1]


def test_iceberg_uses_backend_canonical_uri_with_full_base_prefix(tmp_path):
    storage = GsUriLocalStorage(tmp_path, base_prefix="tenant/root")
    storage.write_bytes("acme/lake/simple/events/data/part.parquet", b"PAR1")
    snapshot = _snapshot()

    write_iceberg_table(_table(storage), "events", snapshot)

    metadata_dir = "acme/lake/iceberg/events/metadata"
    metadata = storage.read_json(f"{metadata_dir}/v1.metadata.json")
    uri_prefix = "gs://mirror-bucket/tenant/root/acme/lake/iceberg/events"
    assert metadata["location"] == uri_prefix
    assert metadata["snapshots"][-1]["manifest-list"].startswith(
        f"{uri_prefix}/metadata/"
    )
    avro_payloads = [
        storage.read_bytes(path)
        for path in storage.list_files(metadata_dir, "*.avro")
    ]
    assert any(
        f"{uri_prefix}/data/".encode("utf-8") in payload
        for payload in avro_payloads
    )
    verify_iceberg_table(
        _table(storage), "events", snapshot, commit_id="commit-41",
    )


@pytest.mark.integration
def test_iceberg_output_opens_with_pyiceberg_when_installed(tmp_path):
    pyiceberg_table = pytest.importorskip("pyiceberg.table")
    storage = LocalStorage(root=tmp_path)
    source = "acme/lake/simple/events/data/part.parquet"
    storage.write_parquet(pa.table({"id": pa.array([1], type=pa.int64())}), source)
    snapshot = _snapshot()
    snapshot["resources"][0]["file_size"] = storage.size(source)

    write_iceberg_table(_table(storage), "events", snapshot)

    metadata_uri = (
        tmp_path / "acme/lake/iceberg/events/metadata/v1.metadata.json"
    ).as_uri()
    iceberg = pyiceberg_table.StaticTable.from_metadata(metadata_uri)
    assert iceberg.scan().to_arrow().column("id").to_pylist() == [1]


def test_iceberg_copy_failure_never_publishes_a_source_path_fallback(tmp_path):
    storage = CopyFailingStorage(tmp_path, base_prefix="tenant/root")
    snapshot = _snapshot()

    with pytest.raises(RuntimeError, match="Failed to copy data file"):
        write_iceberg_table(_table(storage), "events", snapshot)

    assert not storage.exists(
        "acme/lake/iceberg/events/metadata/version-hint.text"
    )
    assert not storage.exists("acme/lake/iceberg/events/latest.json")


def test_iceberg_silent_avro_corruption_is_never_published(tmp_path):
    storage = CorruptIcebergAvroStorage(tmp_path)
    storage.write_bytes("acme/lake/simple/events/data/part.parquet", b"PAR1")

    with pytest.raises(RuntimeError, match="read-after-write verification"):
        write_iceberg_table(_table(storage), "events", _snapshot())

    assert not storage.exists(
        "acme/lake/iceberg/events/metadata/version-hint.text"
    )
    assert not storage.exists("acme/lake/iceberg/events/latest.json")


def test_iceberg_same_commit_retry_repairs_a_silent_copy_failure(tmp_path):
    storage = SilentFirstCopyStorage(tmp_path, base_prefix="tenant/root")
    storage.write_bytes("acme/lake/simple/events/data/part.parquet", b"PAR1")
    snapshot = _snapshot()
    table = _table(storage)

    with pytest.raises(RuntimeError, match="content-sealed"):
        write_iceberg_table(table, "events", snapshot)

    # No metadata pointer was published. Retrying the exact immutable commit
    # repairs the copy and safely creates the first generation.
    write_iceberg_table(table, "events", snapshot)
    verify_iceberg_table(
        table, "events", snapshot, commit_id="commit-41",
    )
    assert storage.read_text(
        "acme/lake/iceberg/events/metadata/version-hint.text"
    ) == "1"
    assert storage.copy_attempts == 2


def test_parquet_verifier_rejects_corruption_and_writer_repairs_it(tmp_path):
    storage = LocalStorage(root=tmp_path)
    source = "acme/lake/simple/events/data/part.parquet"
    storage.write_bytes(source, b"PAR1")
    snapshot = _snapshot()
    table = _table(storage)

    write_parquet_table(table, "events", snapshot)
    destination = (
        "acme/lake/parquet/events/files/58ffee7a_part.parquet"
    )
    storage.write_bytes(destination, b"BROKEN")
    with pytest.raises(RuntimeError, match="content seal"):
        verify_parquet_table(table, "events", snapshot)

    write_parquet_table(table, "events", snapshot)
    verify_parquet_table(table, "events", snapshot)
    assert storage.read_bytes(destination) == b"PAR1"


def test_failed_mirror_intent_reconciles_exact_snapshot_idempotently(tmp_path):
    storage = LocalStorage(root=tmp_path)
    snapshot_path = "acme/lake/simple/events/snapshots/41.json"
    snapshot = _snapshot()
    storage.write_json(snapshot_path, snapshot)
    table = _table(storage)
    failed = {
        "status": "failed",
        "commit_id": "commit-41",
        "snapshot_path": snapshot_path,
        "core_committed": True,
        "mirrors": ["DELTA", "ICEBERG"],
        "publication_owner": "writer-owner",
        "publisher_quiesced": True,
        "failure_stage": "outbox_complete",
    }
    claimed = {
        **failed,
        "publication_owner": "lease-token",
        "publisher_quiesced": False,
    }
    complete = {**claimed, "status": "complete"}
    catalog = MagicMock(spec=RedisCatalog)
    catalog.acquire_simple_lock.return_value = "lease-token"
    catalog.get_mirror_publication.side_effect = [failed, complete]
    catalog.get_leaf.return_value = {
        "commit_id": "commit-41",
        "path": snapshot_path,
        "version": 41,
        "payload": snapshot,
    }
    catalog.claim_mirror_publication.return_value = claimed
    catalog.complete_mirror_publication.return_value = complete

    with (
        patch.object(MirrorFormats, "_catalog", return_value=catalog),
        patch.object(MirrorFormats, "mirror_if_enabled") as mirror,
    ):
        results = [
            MirrorFormats.reconcile_publication(table, "events"),
            MirrorFormats.reconcile_publication(table, "events"),
        ]

    assert results == [complete, complete]

    mirror.assert_called_once_with(
        table,
        "events",
        snapshot,
        mirrors=["DELTA", "ICEBERG"],
        commit_id="commit-41",
        snapshot_path=snapshot_path,
        verify=True,
    )
    catalog.complete_mirror_publication.assert_called_once_with(
        "acme",
        "lake",
        "events",
        commit_id="commit-41",
        lock_token="lease-token",
    )
    catalog.claim_mirror_publication.assert_called_once_with(
        "acme",
        "lake",
        "events",
        commit_id="commit-41",
        expected_previous_owner="writer-owner",
        lock_token="lease-token",
        confirm_previous_owner_stopped=False,
    )
    assert catalog.release_simple_lock.call_count == 2


def test_mirror_recovery_never_takes_over_on_lease_expiry_alone(tmp_path):
    storage = LocalStorage(root=tmp_path)
    snapshot_path = "acme/lake/simple/events/snapshots/41.json"
    snapshot = _snapshot()
    storage.write_json(snapshot_path, snapshot)
    table = _table(storage)
    state = {
        "status": "core_committed",
        "commit_id": "commit-41",
        "snapshot_path": snapshot_path,
        "core_committed": True,
        "mirrors": ["PARQUET"],
        "publication_owner": "stale-owner-a",
        "publisher_quiesced": False,
    }
    catalog = MagicMock(spec=RedisCatalog)
    catalog.acquire_simple_lock.return_value = "contender-b"
    catalog.get_mirror_publication.return_value = state
    catalog.get_leaf.return_value = {
        "commit_id": "commit-41",
        "path": snapshot_path,
        "version": 41,
        "payload": snapshot,
    }

    with (
        patch.object(MirrorFormats, "_catalog", return_value=catalog),
        patch.object(MirrorFormats, "mirror_if_enabled") as mirror,
    ):
        with pytest.raises(
            MirrorRecoveryConfirmationRequired,
            match="previous publisher has stopped",
        ):
            MirrorFormats.reconcile_publication(table, "events")

    catalog.claim_mirror_publication.assert_not_called()
    mirror.assert_not_called()
    catalog.complete_mirror_publication.assert_not_called()


def test_mirror_recovery_rejects_same_path_snapshot_replacement(tmp_path):
    storage = LocalStorage(root=tmp_path)
    snapshot_path = "acme/lake/simple/events/snapshots/41.json"
    committed_snapshot = _snapshot()
    tampered_snapshot = json.loads(json.dumps(committed_snapshot))
    tampered_snapshot["resources"][0]["file"] = (
        "acme/lake/simple/events/data/replacement.parquet"
    )
    storage.write_json(snapshot_path, tampered_snapshot)
    table = _table(storage)
    failed = {
        "status": "failed",
        "commit_id": "commit-41",
        "snapshot_path": snapshot_path,
        "core_committed": True,
        "mirrors": ["DELTA"],
    }
    catalog = MagicMock()
    catalog.acquire_simple_lock.return_value = "lease-token"
    catalog.get_mirror_publication.return_value = failed
    catalog.get_leaf.return_value = {
        "commit_id": "commit-41",
        "path": snapshot_path,
        "version": 41,
        "payload": committed_snapshot,
    }

    with (
        patch.object(MirrorFormats, "_catalog", return_value=catalog),
        patch.object(MirrorFormats, "mirror_if_enabled") as mirror,
    ):
        with pytest.raises(RuntimeError, match="authoritative catalog leaf"):
            MirrorFormats.reconcile_publication(table, "events")

    mirror.assert_not_called()
    catalog.complete_mirror_publication.assert_not_called()
    catalog.release_simple_lock.assert_called_once_with(
        "acme", "lake", "events", "lease-token",
    )


@pytest.mark.parametrize(
    "leaf_payload",
    [
        None,
        {
            "snapshot_version": 41,
            "schema": [],
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            # A missing digest is not an authoritative no-tombstone state.
        },
    ],
    ids=["missing", "partial"],
)
def test_mirror_recovery_requires_complete_leaf_payload(tmp_path, leaf_payload):
    storage = LocalStorage(root=tmp_path)
    snapshot_path = "acme/lake/simple/events/snapshots/41.json"
    storage.write_json(snapshot_path, _snapshot())
    table = _table(storage)
    failed = {
        "status": "failed",
        "commit_id": "commit-41",
        "snapshot_path": snapshot_path,
        "core_committed": True,
        "mirrors": ["PARQUET"],
    }
    catalog = MagicMock()
    catalog.acquire_simple_lock.return_value = "lease-token"
    catalog.get_mirror_publication.return_value = failed
    catalog.get_leaf.return_value = {
        "commit_id": "commit-41",
        "path": snapshot_path,
        "version": 41,
        "payload": leaf_payload,
    }

    with (
        patch.object(MirrorFormats, "_catalog", return_value=catalog),
        patch.object(MirrorFormats, "mirror_if_enabled") as mirror,
    ):
        with pytest.raises(RuntimeError, match="no complete mirror snapshot"):
            MirrorFormats.reconcile_publication(table, "events")

    mirror.assert_not_called()
    catalog.complete_mirror_publication.assert_not_called()


def _staging_dependencies():
    storage = MagicMock()
    storage.size.return_value = 128
    storage.stat_object.return_value.identity_token.return_value = (
        "etag:test-staging-object"
    )
    catalog = MagicMock()
    catalog.root_exists.return_value = True
    catalog.acquire_stage_lock.return_value = "stage-lock-token"
    catalog.begin_stage_deletion.return_value = {
        "intent_id": "stage-delete-intent",
    }
    catalog.get_staging_meta.return_value = None
    return storage, catalog


def test_staging_rejects_path_components_and_generates_upload_key():
    storage, catalog = _staging_dependencies()
    storage.exists.return_value = False
    storage.read_json.return_value = []
    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_create_access"),
        patch("supertable.staging_area.check_meta_access"),
        patch("supertable.staging_area.check_write_access"),
    ):
        with pytest.raises(ValueError, match="Invalid staging_name"):
            Staging(
                organization="acme", super_name="lake",
                staging_name="../escape",
            )
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        generated = stage.save_as_parquet(
            role_name="writer",
            arrow_table=pa.table({"id": [1]}),
            base_file_name="../../outside.parquet",
        )
        published = catalog.upsert_staging_meta.call_args.kwargs["meta"]
        file_meta = published["files"][generated]
        catalog.get_staging_meta.return_value = {
            "files": {generated: file_meta},
        }
        listed = stage.list_files("reader")

    assert os.path.dirname(storage.write_parquet.call_args.args[1]) == (
        "acme/lake/staging/uploads"
    )
    assert generated.startswith("stage_") and generated.endswith(".parquet")
    assert "/" not in generated and "\\" not in generated
    assert file_meta["original_name"] == "outside.parquet"
    assert listed == [generated]


def test_staging_deletion_is_resumable_and_metadata_is_removed_last():
    storage, catalog = _staging_dependencies()
    stage_events = []
    storage.delete_prefix.side_effect = lambda path: stage_events.append(
        ("data", path)
    )
    storage.delete.side_effect = lambda path: stage_events.append(("index", path))
    storage.exists.return_value = False
    def delete_staging_meta(*args, **_kwargs):
        stage_events.append(("catalog", args[-1]))
        return True

    catalog.delete_staging_meta.side_effect = delete_staging_meta
    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_control_access"),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        stage.delete("writer")

    assert [event[0] for event in stage_events] == ["data", "index", "catalog"]
    catalog.release_stage_lock.assert_called_once_with(
        "acme", "lake", "uploads", "stage-lock-token",
    )

    storage.reset_mock()
    catalog.delete_staging_meta.reset_mock()
    storage.delete_prefix.side_effect = OSError("cloud delete failed")
    with patch("supertable.staging_area.check_control_access"):
        with pytest.raises(OSError, match="cloud delete failed"):
            stage.delete("writer")
    catalog.delete_staging_meta.assert_not_called()


def test_staging_deletion_fails_when_catalog_cleanup_is_incomplete():
    storage, catalog = _staging_dependencies()
    storage.exists.return_value = False
    catalog.delete_staging_meta.return_value = False
    with (
        patch("supertable.staging_area.get_storage", return_value=storage),
        patch("supertable.staging_area.RedisCatalog", return_value=catalog),
        patch("supertable.staging_area.check_control_access"),
    ):
        stage = Staging(
            organization="acme", super_name="lake", staging_name="uploads",
        )
        with pytest.raises(RuntimeError, match="metadata removal was incomplete"):
            stage.delete("writer")

    catalog.delete_staging_meta.assert_called_once_with(
        "acme", "lake", "uploads", lock_token="stage-lock-token",
        intent_id="stage-delete-intent",
    )
    catalog.release_stage_lock.assert_called_once_with(
        "acme", "lake", "uploads", "stage-lock-token",
    )


def test_staging_catalog_deletion_is_lock_fenced_and_atomic():
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog._staging_delete_children = catalog.r.register_script(
        catalog._LUA_STAGING_DELETE_CHILDREN
    )
    catalog._staging_delete_meta = catalog.r.register_script(
        catalog._LUA_STAGING_DELETE_META
    )
    index_key = RK.staging_index("acme", "lake")
    meta_key = RK.staging_doc("acme", "lake", "uploads")
    pipe_index_key = RK.pipe_index("acme", "lake", "uploads")
    pipe_key = RK.pipe_doc("acme", "lake", "uploads", "ingest")
    lock_key = RK.lock_stage("acme", "lake", "uploads")
    intent_key = RK.meta_stage_deletion_intent("acme", "lake", "uploads")
    intent_index = RK.meta_stage_deletion_intent_index("acme", "lake")
    catalog.r.sadd(index_key, "uploads")
    catalog.r.set(meta_key, "{}")
    catalog.r.sadd(pipe_index_key, "ingest")
    catalog.r.set(pipe_key, "{}")
    catalog.r.set(lock_key, "owner-token")
    catalog.r.set(intent_key, json.dumps({
        "intent_id": "stage-intent",
        "lock_token": "owner-token",
        "status": "deleting",
    }))
    catalog.r.sadd(intent_index, "uploads")

    with pytest.raises(LockLostError, match="Lost staging lock"):
        catalog.delete_staging_meta(
            "acme", "lake", "uploads", lock_token="wrong-token",
            intent_id="stage-intent",
        )

    # A stale caller cannot remove either the children or the discovery state.
    assert catalog.r.exists(meta_key, pipe_index_key, pipe_key) == 3
    assert catalog.r.sismember(index_key, "uploads")

    assert catalog.delete_staging_meta(
        "acme", "lake", "uploads", lock_token="owner-token",
        intent_id="stage-intent",
    ) is True
    assert catalog.r.exists(meta_key, pipe_index_key, pipe_key) == 0
    assert not catalog.r.sismember(index_key, "uploads")
    assert json.loads(catalog.r.get(intent_key))["status"] == "deleted"


def test_simple_table_prefix_failure_keeps_catalog_metadata():
    simple = SimpleTable.__new__(SimpleTable)
    simple.super_table = SimpleNamespace(organization="acme", super_name="lake")
    simple.simple_name = "events"
    simple.identity = "tables"
    simple.storage = MagicMock()
    simple.catalog = MagicMock()
    simple.catalog.acquire_namespace_lock.return_value = "namespace-token"
    simple.catalog.acquire_simple_lock.return_value = "delete-token"
    simple.catalog.begin_simple_deletion.return_value = {
        "intent_id": "simple-delete-intent",
    }
    simple.storage.delete_prefix.side_effect = OSError("prefix not empty")

    with patch("supertable.simple_table.check_control_access"):
        with pytest.raises(OSError, match="prefix not empty"):
            simple.delete("admin")

    simple.storage.delete_prefix.assert_called_once_with(
        "acme/lake/tables/events"
    )
    simple.catalog.delete_simple_table.assert_not_called()


def test_simple_table_delete_holds_writer_lock_through_catalog_removal():
    entered_delete = threading.Event()
    finish_delete = threading.Event()
    errors = []

    class Catalog:
        def __init__(self):
            self.namespace = threading.Lock()
            self.mutex = threading.Lock()
            self.metadata_removed = False

        def acquire_namespace_lock(self, *_args, **kwargs):
            timeout = 0.05 if kwargs.get("timeout_s") == 1 else 1.0
            return (
                "namespace-token"
                if self.namespace.acquire(timeout=timeout) else None
            )

        def release_namespace_lock(self, *_args):
            self.namespace.release()
            return True

        def acquire_simple_lock(self, *_args, **kwargs):
            timeout = 0.05 if kwargs.get("timeout_s") == 1 else 1.0
            return "delete-token" if self.mutex.acquire(timeout=timeout) else None

        def release_simple_lock(self, *_args):
            self.mutex.release()
            return True

        def begin_simple_deletion(self, *_args, **_kwargs):
            return {"intent_id": "simple-delete-intent"}

        def delete_simple_table(self, *_args, **_kwargs):
            assert self.mutex.locked()
            self.metadata_removed = True
            return True

    catalog = Catalog()
    storage = MagicMock()

    def blocked_prefix_delete(_path):
        entered_delete.set()
        assert finish_delete.wait(timeout=2)

    storage.delete_prefix.side_effect = blocked_prefix_delete
    simple = SimpleTable.__new__(SimpleTable)
    simple.super_table = SimpleNamespace(organization="acme", super_name="lake")
    simple.simple_name = "events"
    simple.identity = "tables"
    simple.storage = storage
    simple.catalog = catalog

    def run_delete():
        try:
            with patch("supertable.simple_table.check_control_access"):
                simple.delete("admin")
        except Exception as exc:  # pragma: no cover - reported by assertion
            errors.append(exc)

    worker = threading.Thread(target=run_delete)
    worker.start()
    assert entered_delete.wait(timeout=2)
    assert catalog.acquire_simple_lock(
        "acme", "lake", "events", ttl_s=30, timeout_s=1,
    ) is None
    assert catalog.acquire_namespace_lock(
        "acme", "lake", ttl_s=30, timeout_s=1,
    ) is None
    finish_delete.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert errors == []
    assert catalog.metadata_removed is True


def test_simple_table_delete_namespace_fence_blocks_concurrent_initializer():
    prefix_delete_entered = threading.Event()
    initializer_waiting = threading.Event()
    finish_delete = threading.Event()
    errors = []

    class Catalog:
        def __init__(self):
            self.namespace = threading.Lock()
            self.leaf = threading.Lock()
            self.leaf_exists_now = True
            self.delete_intent = False

        def acquire_namespace_lock(self, *_args, **_kwargs):
            if not self.namespace.acquire(blocking=False):
                initializer_waiting.set()
                if not self.namespace.acquire(timeout=2):
                    return None
            return f"namespace-{threading.get_ident()}"

        def release_namespace_lock(self, *_args):
            self.namespace.release()
            return True

        def acquire_simple_lock(self, *_args, **_kwargs):
            return "leaf-token" if self.leaf.acquire(timeout=2) else None

        def release_simple_lock(self, *_args):
            self.leaf.release()
            return True

        def root_exists(self, *_args):
            return True

        def leaf_exists(self, *_args):
            return self.leaf_exists_now

        def begin_simple_deletion(self, *_args, **_kwargs):
            self.delete_intent = True
            return {"intent_id": "simple-delete-intent"}

        def check_initialization_allowed(self, *_args, **_kwargs):
            if self.delete_intent:
                raise RuntimeError("durable deletion intent")

        def delete_simple_table(self, *_args, **_kwargs):
            self.leaf_exists_now = False
            return True

        def set_leaf_payload_cas(self, *_args, **_kwargs):
            assert self.namespace.locked()
            self.leaf_exists_now = True
            return 0

    catalog = Catalog()
    storage = MagicMock()
    storage.exists.return_value = True

    deleting = SimpleTable.__new__(SimpleTable)
    deleting.super_table = SimpleNamespace(organization="acme", super_name="lake")
    deleting.simple_name = "events"
    deleting.identity = "tables"
    deleting.storage = storage
    deleting.catalog = catalog

    creating = SimpleTable.__new__(SimpleTable)
    creating.super_table = deleting.super_table
    creating.simple_name = "events"
    creating.identity = "tables"
    creating.storage = storage
    creating.catalog = catalog
    creating.simple_dir = "acme/lake/tables/events"
    creating.data_dir = "acme/lake/tables/events/data"
    creating.snapshot_dir = "acme/lake/tables/events/snapshots"

    def delete_prefix(_path):
        prefix_delete_entered.set()
        assert finish_delete.wait(timeout=2)

    storage.delete_prefix.side_effect = delete_prefix

    def run_delete():
        try:
            with patch("supertable.simple_table.check_control_access"):
                deleting.delete("admin")
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    def run_init():
        try:
            creating.init_simple_table()
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    delete_thread = threading.Thread(target=run_delete)
    delete_thread.start()
    assert prefix_delete_entered.wait(timeout=2)
    init_thread = threading.Thread(target=run_init)
    init_thread.start()
    assert initializer_waiting.wait(timeout=2)
    storage.write_json.assert_not_called()

    finish_delete.set()
    delete_thread.join(timeout=2)
    init_thread.join(timeout=2)
    assert not delete_thread.is_alive()
    assert not init_thread.is_alive()
    assert len(errors) == 1
    assert "durable deletion intent" in str(errors[0])
    storage.write_json.assert_not_called()


def test_super_table_prefix_failure_keeps_catalog_metadata():
    lake = SuperTable.__new__(SuperTable)
    lake.organization = "acme"
    lake.super_name = "lake"
    lake.storage = MagicMock()
    lake.catalog = MagicMock()
    lake.catalog.acquire_namespace_lock.return_value = "namespace-token"
    lake.catalog.begin_namespace_deletion.return_value = {
        "intent_id": "namespace-delete-intent",
    }
    lake.catalog.find_clones_strict.return_value = []
    lake.catalog.scan_leaf_keys.return_value = iter(())
    lake.storage.delete_prefix.side_effect = OSError("prefix not empty")

    with patch(
        "supertable.super_table.resolve_role_access_context",
        return_value=SimpleNamespace(role_type=RoleType.SUPERADMIN),
    ):
        with pytest.raises(OSError, match="prefix not empty"):
            lake.delete("root")

    lake.storage.delete_prefix.assert_called_once_with("acme/lake")
    lake.catalog.delete_super_table.assert_not_called()


def test_super_table_delete_fences_new_child_before_any_storage_write():
    prefix_verified = threading.Event()
    finish_delete = threading.Event()
    errors = []

    class Catalog:
        def __init__(self):
            self.namespace = threading.Lock()
            self.root = True

        def acquire_namespace_lock(self, *_args, **_kwargs):
            return (
                f"token-{threading.get_ident()}"
                if self.namespace.acquire(timeout=2) else None
            )

        def release_namespace_lock(self, *_args):
            self.namespace.release()
            return True

        def scan_leaf_keys(self, *_args, **_kwargs):
            return iter(())

        def find_clones_strict(self, *_args, **_kwargs):
            return []

        def scan_leaf_lock_names(self, *_args):
            return []

        def scan_stage_lock_names(self, *_args):
            return []

        def begin_namespace_deletion(self, *_args, **_kwargs):
            return {"intent_id": "namespace-delete-intent"}

        def delete_super_table(self, *_args, **_kwargs):
            self.root = False
            return 1

        def root_exists(self, *_args):
            return self.root

        def leaf_exists(self, *_args):
            return False

    catalog = Catalog()
    storage = MagicMock()

    def delete_prefix(_path):
        prefix_verified.set()
        assert finish_delete.wait(timeout=2)

    storage.delete_prefix.side_effect = delete_prefix
    lake = SuperTable.__new__(SuperTable)
    lake.organization = "acme"
    lake.super_name = "lake"
    lake.storage = storage
    lake.catalog = catalog

    def run_delete():
        try:
            with patch(
                "supertable.super_table.resolve_role_access_context",
                return_value=SimpleNamespace(role_type=RoleType.SUPERADMIN),
            ):
                lake.delete("root")
        except Exception as exc:  # pragma: no cover - reported by assertion
            errors.append(exc)

    deletion = threading.Thread(target=run_delete)
    deletion.start()
    assert prefix_verified.wait(timeout=2)

    creation_errors = []

    def create_child():
        try:
            with patch(
                "supertable.simple_table.RedisCatalog", return_value=catalog,
            ):
                SimpleTable(lake, "new_child")
        except Exception as exc:
            creation_errors.append(exc)

    creation = threading.Thread(target=create_child)
    creation.start()
    time.sleep(0.1)
    storage.write_json.assert_not_called()

    finish_delete.set()
    deletion.join(timeout=2)
    creation.join(timeout=2)

    assert not deletion.is_alive() and not creation.is_alive()
    assert errors == []
    assert creation_errors
    storage.write_json.assert_not_called()
