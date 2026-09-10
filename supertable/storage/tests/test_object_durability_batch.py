# route: supertable.storage.tests.test_object_durability_batch
"""Contract tests for the generic object-store durability/rollback batch.

Before this batch existed, ``durability_batch`` was implemented only by
``LocalStorage``.  ``DataWriter`` probes for it duck-typed, so on S3, MinIO,
Azure and GCS the whole commit state machine — ``barrier()``,
``catalog_commit_*`` and crucially ``abort()`` — was skipped, and every
rejected commit permanently orphaned its data file, tombstone, stats parquet
and snapshot JSON.  There is no orphan sweeper, so those objects leaked
forever.

Each backend is driven through a tiny in-memory fake of its provider client so
the real adapter code (including ``_with_base`` key translation) is exercised.
"""

from __future__ import annotations

import os
import threading
from contextvars import copy_context
from types import SimpleNamespace

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa

from supertable.storage.storage_interface import (
    ObjectStoreDurabilityBatch,
    StorageInterface,
    _ACTIVE_OBJECT_DURABILITY_BATCH,
)


# ---------------------------------------------------------------------------
# Shared in-memory object namespace
# ---------------------------------------------------------------------------
class _ObjectStore:
    """Flat key -> bytes namespace with an audit trail of delete attempts."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deletes: list[str] = []
        self.puts: list[str] = []

    def put(self, key: str, payload: bytes) -> None:
        self.puts.append(key)
        self.objects[key] = bytes(payload)

    def drop(self, key: str) -> bool:
        self.deletes.append(key)
        return self.objects.pop(key, None) is not None


class _FakeBody:
    """Minimal streaming-body stand-in shared by the S3 and MinIO fakes."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self, *_args) -> bytes:
        return self._payload

    def readall(self) -> bytes:
        return self._payload

    def close(self) -> None:
        return None

    def release_conn(self) -> None:
        return None


def _drain(body) -> bytes:
    if body is None:
        return b""
    if isinstance(body, (bytes, bytearray, memoryview)):
        return bytes(body)
    if hasattr(body, "read"):
        return body.read()
    return b"".join(body)


TABLE = pa.table({"id": [1, 2, 3]})


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------
def _make_s3(store: _ObjectStore, base_prefix: str = ""):
    boto3 = pytest.importorskip("boto3")  # noqa: F841
    from botocore.exceptions import ClientError

    from supertable.storage.s3_storage import S3Storage

    class FakeS3Client:
        meta = SimpleNamespace(
            endpoint_url="https://s3.amazonaws.com", region_name="us-east-1",
        )

        def head_bucket(self, **kwargs):
            return {}

        def head_object(self, **kwargs):
            key = kwargs["Key"]
            if key not in store.objects:
                raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
            return {"ContentLength": len(store.objects[key])}

        def get_object(self, **kwargs):
            key = kwargs["Key"]
            if key not in store.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": _FakeBody(store.objects[key])}

        def put_object(self, **kwargs):
            key = kwargs["Key"]
            if kwargs.get("IfNoneMatch") == "*" and key in store.objects:
                raise ClientError(
                    {
                        "Error": {"Code": "PreconditionFailed"},
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    },
                    "PutObject",
                )
            store.put(key, _drain(kwargs.get("Body")))
            return {"ETag": '"etag"'}

        def delete_object(self, **kwargs):
            store.drop(kwargs["Key"])
            return {}

        def copy_object(self, **kwargs):
            source = kwargs["CopySource"]["Key"]
            store.put(kwargs["Key"], store.objects.get(source, b"copied"))
            return {}

    storage = S3Storage(
        "bucket", client=FakeS3Client(), region="us-east-1",
        base_prefix=base_prefix,
    )
    storage._bucket_region_checked = True
    return storage


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
def _make_minio(store: _ObjectStore, base_prefix: str = ""):
    pytest.importorskip("minio")

    # Resolve the exception class from the adapter module rather than from
    # ``minio.error``: ``test_storage`` installs a lightweight ``minio`` stub
    # when the SDK has not been imported yet, and only the class the adapter
    # actually catches is guaranteed to match its ``except`` clauses.
    from supertable.storage import minio_storage as minio_module
    from supertable.storage.minio_storage import MinioStorage

    def _s3_error(code: str, status: int, name: str):
        try:
            return minio_module.S3Error(
                SimpleNamespace(status=status), code, "error", name, "req", "host",
            )
        except TypeError:  # stubbed S3Error(code, message)
            error = minio_module.S3Error(code, "error")
            error.response = SimpleNamespace(status=status)
            return error

    class FakeMinioClient:
        _base_url = SimpleNamespace(_url=SimpleNamespace(hostname="minio", port=9000))

        def put_object(self, bucket, name, data=None, length=None, **kwargs):
            store.put(name, _drain(data))

        def remove_object(self, bucket, name):
            store.drop(name)

        def get_object(self, bucket, name):
            if name not in store.objects:
                raise _s3_error("NoSuchKey", 404, name)
            return _FakeBody(store.objects[name])

        def stat_object(self, bucket, name):
            if name not in store.objects:
                raise _s3_error("NoSuchKey", 404, name)
            return SimpleNamespace(size=len(store.objects[name]))

        def copy_object(self, bucket_name=None, object_name=None, source=None):
            # The stubbed CopySource carries no attributes, so the destination
            # bytes are synthetic; only the destination key matters here.
            store.put(object_name, b"copied")

        def _execute(self, method, bucket, name, body=None, headers=None, **kwargs):
            if (headers or {}).get("If-None-Match") == "*" and name in store.objects:
                raise _s3_error("PreconditionFailed", 412, name)
            store.put(name, bytes(body))

    return MinioStorage("bucket", FakeMinioClient(), base_prefix=base_prefix)


# ---------------------------------------------------------------------------
# Azure
# ---------------------------------------------------------------------------
def _make_azure(store: _ObjectStore, base_prefix: str = ""):
    pytest.importorskip("azure.storage.blob")
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

    from supertable.storage.azure_storage import AzureBlobStorage

    class FakeBlob:
        def __init__(self, name: str) -> None:
            self.name = name

        def upload_blob(self, data, overwrite=False, **kwargs):
            if not overwrite and self.name in store.objects:
                raise ResourceExistsError("blob already exists")
            store.put(self.name, _drain(data))

        def get_blob_properties(self):
            if self.name not in store.objects:
                raise ResourceNotFoundError("missing")
            return SimpleNamespace(size=len(store.objects[self.name]))

        def download_blob(self):
            if self.name not in store.objects:
                raise ResourceNotFoundError("missing")
            return _FakeBody(store.objects[self.name])

    class FakeContainer:
        def get_blob_client(self, name):
            return FakeBlob(name)

        def delete_blob(self, name):
            if not store.drop(name):
                raise ResourceNotFoundError("missing")

    class FakeService:
        url = "https://account.blob.core.windows.net"
        account_name = "account"

        def get_container_client(self, name):
            return FakeContainer()

    return AzureBlobStorage("container", FakeService(), base_prefix=base_prefix)


# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------
def _make_gcs(store: _ObjectStore, base_prefix: str = ""):
    pytest.importorskip("google.cloud.storage")
    from google.api_core.exceptions import NotFound, PreconditionFailed

    from supertable.storage.gcp_storage import GCSStorage

    class FakeBlob:
        def __init__(self, name: str) -> None:
            self.name = name

        def upload_from_string(self, data, content_type=None, if_generation_match=None):
            if if_generation_match == 0 and self.name in store.objects:
                raise PreconditionFailed("generation conflict")
            store.put(self.name, _drain(data))

        def delete(self):
            if not store.drop(self.name):
                raise NotFound("missing")

        def exists(self, client=None):
            return self.name in store.objects

        def download_as_bytes(self):
            if self.name not in store.objects:
                raise NotFound("missing")
            return store.objects[self.name]

        @property
        def size(self):
            return len(store.objects[self.name])

    class FakeBucket:
        def blob(self, name):
            return FakeBlob(name)

        def get_blob(self, name):
            return FakeBlob(name) if name in store.objects else None

        def copy_blob(self, src_blob, bucket, new_name=None):
            store.put(new_name, store.objects[src_blob.name])

    class FakeClient:
        project = "proj"

        def bucket(self, name):
            return FakeBucket()

    return GCSStorage("bucket", client=FakeClient(), base_prefix=base_prefix)


BACKENDS = {
    "s3": _make_s3,
    "minio": _make_minio,
    "azure": _make_azure,
    "gcs": _make_gcs,
}


@pytest.fixture(params=sorted(BACKENDS))
def backend(request):
    """Yield ``(storage, store)`` for every built-in object-store backend."""
    store = _ObjectStore()
    yield BACKENDS[request.param](store), store
    # No batch may survive a test and poison the next one's context.
    assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None


def _write_three(storage) -> list[str]:
    """Write one object through each enrolled entry point."""
    storage.write_parquet(TABLE, "org/lake/t/data/1_a_data.parquet")
    storage.write_bytes("org/lake/t/tombstone/1_b_tombstone.parquet", b"dv")
    storage.write_json("org/lake/t/snapshots/1_c_snap.json", {"version": 1})
    return [
        "org/lake/t/data/1_a_data.parquet",
        "org/lake/t/tombstone/1_b_tombstone.parquet",
        "org/lake/t/snapshots/1_c_snap.json",
    ]


# ---------------------------------------------------------------------------
# (1) written then aborted -> deleted
# ---------------------------------------------------------------------------
def test_abort_deletes_objects_created_inside_the_batch(backend):
    storage, store = backend
    with pytest.raises(RuntimeError, match="simulated failure"):
        with storage.durability_batch():
            paths = _write_three(storage)
            assert all(storage.exists(p) for p in paths)
            raise RuntimeError("simulated failure")

    assert store.objects == {}
    assert len(store.deletes) == 3


def test_abort_before_the_barrier_deletes_objects(backend):
    """A failure between the first write and the barrier still rolls back."""
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        paths = _write_three(storage)
        batch.abort()
    finally:
        batch.close()

    assert store.objects == {}
    assert len(paths) == 3


# ---------------------------------------------------------------------------
# (2) commit started + ambiguous failure -> RETAINED
# ---------------------------------------------------------------------------
def test_ambiguous_failure_after_commit_started_retains_objects(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        paths = _write_three(storage)
        batch.barrier()
        batch.catalog_commit_started()
        # A transport timeout here cannot prove the catalog did not commit.
        batch.abort()
    finally:
        batch.close()

    assert sorted(store.objects) == sorted(storage._with_base(p) for p in paths)
    assert store.deletes == []
    assert batch._state == "commit_started"


# ---------------------------------------------------------------------------
# (3) typed rejection -> deleted
# ---------------------------------------------------------------------------
def test_typed_commit_rejection_deletes_objects(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        _write_three(storage)
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_rejected()
        batch.abort()
    finally:
        batch.close()

    assert store.objects == {}
    assert len(store.deletes) == 3
    assert batch._state == "aborted"


# ---------------------------------------------------------------------------
# (4) committed -> retained
# ---------------------------------------------------------------------------
def test_committed_batch_retains_objects(backend):
    storage, store = backend
    with storage.durability_batch() as batch:
        paths = _write_three(storage)
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_succeeded()

    assert sorted(store.objects) == sorted(storage._with_base(p) for p in paths)
    assert store.deletes == []


def test_post_commit_writes_are_not_enrolled(backend):
    """Mirror artifacts written after the commit must never be rolled back."""
    storage, store = backend
    with storage.durability_batch() as batch:
        _write_three(storage)
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_succeeded()
        # Deterministically-named mirror commit file, written post-commit.
        storage.write_bytes("org/lake/t/_delta_log/00000000000000000001.json", b"{}")
        assert batch.recorded_objects() == [
            "org/lake/t/data/1_a_data.parquet",
            "org/lake/t/tombstone/1_b_tombstone.parquet",
            "org/lake/t/snapshots/1_c_snap.json",
        ]

    assert storage.exists("org/lake/t/_delta_log/00000000000000000001.json")
    assert store.deletes == []


# ---------------------------------------------------------------------------
# (5) concurrent batches are isolated
# ---------------------------------------------------------------------------
def test_concurrent_batches_do_not_delete_each_others_objects(backend):
    storage, store = backend
    gate = threading.Barrier(2, timeout=30)
    errors: list[BaseException] = []

    def committer():
        try:
            with storage.durability_batch() as batch:
                storage.write_bytes("t/keep/keep_a.parquet", b"keep")
                gate.wait()
                gate.wait()
                batch.barrier()
                batch.catalog_commit_started()
                batch.catalog_commit_succeeded()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    def aborter():
        try:
            batch = storage.durability_batch()
            batch.__enter__()
            try:
                storage.write_bytes("t/drop/drop_a.parquet", b"drop")
                gate.wait()
                # Both batches are simultaneously open on one storage object.
                batch.abort()
            finally:
                batch.close()
                gate.wait()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=committer), threading.Thread(target=aborter)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert errors == []
    assert storage.exists("t/keep/keep_a.parquet")
    assert not storage.exists("t/drop/drop_a.parquet")
    assert store.deletes == [storage._with_base("t/drop/drop_a.parquet")]


# ---------------------------------------------------------------------------
# (6) nested batches rejected
# ---------------------------------------------------------------------------
def test_nested_batch_is_rejected(backend):
    storage, _ = backend
    outer = storage.durability_batch()
    outer.__enter__()
    try:
        inner = storage.durability_batch()
        with pytest.raises(RuntimeError, match="nested"):
            inner.__enter__()
        assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is outer
    finally:
        outer.abort()
        outer.close()
    # The failed inner batch never stole or detached the outer scope.
    assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None


def test_batch_cannot_be_re_entered(backend):
    storage, _ = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        with pytest.raises(RuntimeError, match="re-entered"):
            batch.__enter__()
    finally:
        batch.abort()
        batch.close()


# ---------------------------------------------------------------------------
# (7) already-absent key tolerated
# ---------------------------------------------------------------------------
def test_abort_tolerates_an_already_deleted_key(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_bytes("t/data/gone.parquet", b"x")
        storage.write_bytes("t/data/present.parquet", b"y")
        # Something else removed one object first (retry, sweeper, operator).
        store.objects.pop(storage._with_base("t/data/gone.parquet"))
        batch.abort()
    finally:
        batch.close()

    assert store.objects == {}
    assert len(store.deletes) == 2


# ---------------------------------------------------------------------------
# Enrollment coverage per entry point
# ---------------------------------------------------------------------------
def test_write_text_and_copy_are_enrolled(backend):
    storage, store = backend
    storage.write_bytes("t/source/src.bin", b"payload")
    store.deletes.clear()

    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_text("t/data/text_a.txt", "hello")
        storage.copy("t/source/src.bin", "t/data/copy_a.bin")
        assert set(batch.recorded_objects()) == {
            "t/data/text_a.txt", "t/data/copy_a.bin",
        }
        batch.abort()
    finally:
        batch.close()

    # The pre-existing source object is untouched; only new objects vanish.
    assert storage.exists("t/source/src.bin")
    assert not storage.exists("t/data/text_a.txt")
    assert not storage.exists("t/data/copy_a.bin")


def test_create_bytes_if_absent_enrolls_only_a_successful_create(backend):
    storage, _ = backend
    # A pre-existing object owned by whoever won the race.
    assert storage.create_bytes_if_absent("t/lease/winner.json", b"first") is True

    batch = storage.durability_batch()
    batch.__enter__()
    try:
        assert storage.create_bytes_if_absent("t/lease/winner.json", b"second") is False
        assert storage.create_bytes_if_absent("t/lease/mine.json", b"mine") is True
        # A lost race must never enroll another writer's object for deletion.
        assert batch.recorded_objects() == ["t/lease/mine.json"]
        batch.abort()
    finally:
        batch.close()

    assert storage.read_bytes("t/lease/winner.json") == b"first"
    assert not storage.exists("t/lease/mine.json")


def test_enrollment_is_deduplicated(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        # write_text delegates to write_bytes: one logical object, one entry.
        storage.write_text("t/data/dedup.txt", "a")
        storage.write_text("/t/data/dedup.txt", "b")
        assert batch.recorded_objects() == ["t/data/dedup.txt"]
        batch.abort()
    finally:
        batch.close()

    assert len(store.deletes) == 1


def test_writes_outside_a_batch_are_never_enrolled(backend):
    storage, store = backend
    _write_three(storage)
    assert len(store.objects) == 3
    assert store.deletes == []


# ---------------------------------------------------------------------------
# Rollback must never widen into a prefix delete
# ---------------------------------------------------------------------------
def test_rollback_never_widens_into_a_prefix_delete(backend):
    storage, store = backend
    storage.write_bytes("t/data/sibling/inner.parquet", b"live")
    store.deletes.clear()

    batch = storage.durability_batch()
    batch.__enter__()
    try:
        # Enrol a key that is also the prefix of a live object and make it
        # absent, which is exactly what would trip a recursive ``delete``.
        batch.record_new_object("t/data/sibling")
        batch.abort()
    finally:
        batch.close()

    assert storage.exists("t/data/sibling/inner.parquet")


def test_base_prefix_rollback_targets_the_physical_key():
    store = _ObjectStore()
    storage = _make_s3(store, base_prefix="tenant/zone")
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_bytes("org/lake/t/data/1_a.parquet", b"x")
        assert "tenant/zone/org/lake/t/data/1_a.parquet" in store.objects
        batch.abort()
    finally:
        batch.close()

    assert store.deletes == ["tenant/zone/org/lake/t/data/1_a.parquet"]
    assert store.objects == {}


# ---------------------------------------------------------------------------
# Fork safety and context propagation
# ---------------------------------------------------------------------------
def test_batch_created_in_a_parent_does_not_act_in_a_child(backend):
    """Every mutating entry point is inert once the owning pid no longer matches.

    The batch object is built directly rather than entered so the assertions
    stay on the pid guards themselves and never touch the process-wide
    ContextVar, which a real ``fork()`` clears through the registered hook
    (covered separately by ``test_fork_hook_detaches_an_inherited_batch``).
    """
    storage, store = backend
    batch = ObjectStoreDurabilityBatch(storage)
    batch._state = "open"  # as if __enter__ had run in the parent
    storage.write_bytes("t/data/parent.parquet", b"parent")
    batch.record_new_object("t/data/parent.parquet")

    batch.pid = os.getpid() + 1_000_000  # now observed from a forked child

    assert batch.accepts(storage) is False
    batch.record_new_object("t/data/child.parquet")
    batch.abort()
    batch.close()

    assert batch.recorded_objects() == ["t/data/parent.parquet"]
    assert store.deletes == []
    assert storage.exists("t/data/parent.parquet")
    for transition in (
        batch.barrier,
        batch.catalog_commit_started,
        batch.catalog_commit_succeeded,
        batch.catalog_commit_rejected,
    ):
        with pytest.raises(RuntimeError, match="fork boundary"):
            transition()


def test_fork_hook_detaches_an_inherited_batch():
    from supertable.storage import storage_interface

    store = _ObjectStore()
    storage = _make_s3(store)
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is batch
        storage_interface._reset_object_durability_batch_after_fork()
        assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None
        # A child may now open its own batch instead of failing as "nested".
        child = storage.durability_batch()
        child.__enter__()
        child.abort()
        child.close()
    finally:
        batch.abort()
        batch.close()
    assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None


def test_worker_threads_enroll_only_through_copy_context(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        propagated = threading.Thread(
            target=copy_context().run,
            args=(storage.write_bytes, "t/data/branch.parquet", b"branch"),
        )
        propagated.start()
        propagated.join(timeout=30)

        detached = threading.Thread(
            target=storage.write_bytes,
            args=("t/data/unrelated.parquet", b"unrelated"),
        )
        detached.start()
        detached.join(timeout=30)

        assert batch.recorded_objects() == ["t/data/branch.parquet"]
        batch.abort()
    finally:
        batch.close()

    assert not storage.exists("t/data/branch.parquet")
    assert storage.exists("t/data/unrelated.parquet")


# ---------------------------------------------------------------------------
# State machine guards
# ---------------------------------------------------------------------------
def test_barrier_runs_exactly_once(backend):
    storage, _ = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        batch.barrier()
        with pytest.raises(RuntimeError, match="exactly once"):
            batch.barrier()
    finally:
        batch.abort()
        batch.close()


def test_commit_requires_a_completed_barrier(backend):
    storage, _ = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        with pytest.raises(RuntimeError, match="durability barrier"):
            batch.catalog_commit_started()
        with pytest.raises(RuntimeError, match="not started"):
            batch.catalog_commit_succeeded()
        with pytest.raises(RuntimeError, match="not started"):
            batch.catalog_commit_rejected()
    finally:
        batch.abort()
        batch.close()


def test_clean_exit_without_a_commit_is_an_error(backend):
    storage, _ = backend
    with pytest.raises(RuntimeError, match="without a catalog commit"):
        with storage.durability_batch():
            storage.write_bytes("t/data/pending.parquet", b"x")
    assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None


def test_abort_is_idempotent_and_writes_after_abort_are_ignored(backend):
    storage, store = backend
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_bytes("t/data/one.parquet", b"1")
        batch.abort()
        batch.abort()
        storage.write_bytes("t/data/two.parquet", b"2")
    finally:
        batch.close()

    assert len(store.deletes) == 1
    assert storage.exists("t/data/two.parquet")


# ---------------------------------------------------------------------------
# Defensive delete-failure handling
# ---------------------------------------------------------------------------
def test_abort_reports_a_delete_failure_after_deleting_everything_else(backend, caplog):
    storage, store = backend
    original = storage._delete_object_for_rollback

    def flaky(path: str) -> None:
        if path.endswith("stuck.parquet"):
            raise OSError("provider refused the delete")
        original(path)

    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_bytes("t/data/a.parquet", b"a")
        storage.write_bytes("t/data/stuck.parquet", b"b")
        storage.write_bytes("t/data/c.parquet", b"c")
        storage._delete_object_for_rollback = flaky
        with caplog.at_level("ERROR"):
            with pytest.raises(OSError, match="provider refused"):
                batch.abort()
    finally:
        storage._delete_object_for_rollback = original
        batch.close()

    # One failure never stops the remaining rollback.
    assert not storage.exists("t/data/a.parquet")
    assert not storage.exists("t/data/c.parquet")
    assert storage.exists("t/data/stuck.parquet")
    assert batch._state == "aborted"
    assert any("durability rollback" in record.message for record in caplog.records)
    # Diagnostics must never render tenant-bearing object keys.
    assert not any("stuck.parquet" in record.getMessage() for record in caplog.records)


# ---------------------------------------------------------------------------
# The gap itself, plus third-party compatibility
# ---------------------------------------------------------------------------
def test_every_backend_exposes_a_durability_batch():
    """The regression under test: only LocalStorage used to implement this."""
    from supertable.storage.azure_storage import AzureBlobStorage
    from supertable.storage.gcp_storage import GCSStorage
    from supertable.storage.local_storage import LocalStorage
    from supertable.storage.minio_storage import MinioStorage
    from supertable.storage.s3_storage import S3Storage

    for cls in (
        StorageInterface, LocalStorage, S3Storage, MinioStorage,
        AzureBlobStorage, GCSStorage,
    ):
        assert callable(getattr(cls, "durability_batch", None)), cls.__name__


def _third_party_storage():
    class ThirdParty(StorageInterface):
        """An adapter written before this contract existed."""

        def read_json(self, path): ...
        def write_json(self, path, data): ...
        def exists(self, path): return False
        def size(self, path): return 0
        def makedirs(self, path): ...
        def list_files(self, path, pattern="*"): return []
        def delete(self, path): raise FileNotFoundError(path)
        def get_directory_structure(self, path): return {}
        def write_parquet(self, table, path): ...
        def read_parquet(self, path, columns=None): ...
        def write_bytes(self, path, data): ...
        def read_bytes(self, path): return b""
        def write_text(self, path, text, encoding="utf-8"): ...
        def read_text(self, path, encoding="utf-8"): return ""
        def copy(self, src_path, dst_path): ...

    return ThirdParty()


def test_third_party_adapter_gets_a_working_no_op_batch():
    storage = _third_party_storage()
    with storage.durability_batch() as batch:
        assert isinstance(batch, ObjectStoreDurabilityBatch)
        storage.write_bytes("anything", b"x")
        assert batch.recorded_objects() == []
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_succeeded()
    assert _ACTIVE_OBJECT_DURABILITY_BATCH.get() is None


def test_third_party_abort_is_a_no_op():
    storage = _third_party_storage()
    batch = storage.durability_batch()
    batch.__enter__()
    try:
        storage.write_bytes("anything", b"x")
        batch.abort()
    finally:
        batch.close()
    assert batch._state == "aborted"


def test_compat_rollback_delete_absorbs_a_missing_object():
    storage = _third_party_storage()
    # ``delete`` raises FileNotFoundError; rollback must swallow exactly that.
    storage._delete_object_for_rollback("missing/object.parquet")


def test_batches_on_equivalent_adapters_share_one_scope():
    """LocalStorage scopes by root; object stores scope by cache namespace."""
    store = _ObjectStore()
    first = _make_s3(store)
    second = _make_s3(store)
    batch = first.durability_batch()
    batch.__enter__()
    try:
        second.write_bytes("t/data/shared.parquet", b"x")
        assert batch.recorded_objects() == ["t/data/shared.parquet"]
        batch.abort()
    finally:
        batch.close()
    assert store.objects == {}


def test_batches_on_different_namespaces_are_isolated():
    store = _ObjectStore()
    first = _make_s3(store)
    other = _make_s3(store, base_prefix="other-tenant")
    batch = first.durability_batch()
    batch.__enter__()
    try:
        other.write_bytes("t/data/foreign.parquet", b"x")
        assert batch.recorded_objects() == []
        batch.abort()
    finally:
        batch.close()
    assert "other-tenant/t/data/foreign.parquet" in store.objects
