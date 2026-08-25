import base64
import duckdb
import hashlib
import io
import os
import traceback
from dataclasses import FrozenInstanceError
import traceback
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    PARQUET_DECODE_MAX_PENDING_BATCHES,
    PARQUET_DECODE_MAX_PENDING_ROWS,
    _iter_bounded_parquet_batch_groups,
    normalize_sha256_checksum,
)


class RecordingSink(io.BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.write_sizes = []

    def write(self, data) -> int:
        self.write_sizes.append(len(data))
        return super().write(data)


def test_object_metadata_is_frozen_and_has_stable_identity():
    metadata = ObjectMetadata(
        size=12,
        version="v1",
        etag="abc",
        last_modified_ns=42,
        checksum_sha256="digest",
    )

    assert metadata.identity_token() == (
        "size=12|version=v1|etag=abc|mtime_ns=42|sha256=digest"
    )
    with pytest.raises(FrozenInstanceError):
        metadata.size = 13


def test_size_alone_is_not_an_object_identity():
    assert ObjectMetadata(size=12).identity_token() is None


def test_sha256_checksum_normalization_accepts_hex_and_base64_only():
    raw = hashlib.sha256(b"payload").digest()
    expected = raw.hex()

    assert normalize_sha256_checksum(expected.upper()) == expected
    assert normalize_sha256_checksum(base64.b64encode(raw).decode("ascii")) == expected
    assert normalize_sha256_checksum("not-a-sha256") == ""


def test_local_download_streams_and_honours_expected_metadata(tmp_path):
    path = tmp_path / "source.parquet"
    payload = b"0123456789" * 5
    path.write_bytes(payload)
    storage = LocalStorage(str(tmp_path))
    expected = storage.stat_object(str(path))
    sink = RecordingSink()

    written = storage.download_to_file(
        str(path), sink, expected=expected, chunk_size=7
    )

    assert written == len(payload)
    assert sink.getvalue() == payload
    assert max(sink.write_sizes) <= 7
    assert storage.cache_namespace() == {"provider": "local"}
    assert storage.is_local_storage() is True


def test_local_relative_paths_are_rooted_without_changing_process_cwd(tmp_path):
    root = tmp_path / "storage-root"
    elsewhere = tmp_path / "caller-cwd"
    elsewhere.mkdir()
    before = os.getcwd()
    try:
        os.chdir(elsewhere)
        storage = LocalStorage(root=root)
        storage.write_bytes("tenant/table/data.bin", b"payload")

        assert os.getcwd() == str(elsewhere)
        assert (root / "tenant/table/data.bin").read_bytes() == b"payload"
        assert storage.list_files("tenant/table", "*.bin") == [
            "tenant/table/data.bin"
        ]
        assert storage.canonical_uri("tenant/table/data.bin") == (
            root / "tenant/table/data.bin"
        ).resolve().as_uri()
    finally:
        os.chdir(before)


def test_local_duckdb_path_resolves_logical_hive_key_without_uri_escaping(tmp_path):
    root = tmp_path / "storage-root"
    storage = LocalStorage(root=root)
    logical = "tenant/table/year=2026/month=08/data.parquet"
    storage.write_parquet(pa.table({"id": [1, 2]}), logical)

    resolved = storage.to_duckdb_path(logical)

    assert resolved == str((root / logical).resolve())
    assert os.path.isabs(resolved)
    assert "%3D" not in resolved
    connection = duckdb.connect()
    try:
        assert connection.from_parquet(resolved).fetchall() == [(1,), (2,)]
    finally:
        connection.close()


def test_local_relative_paths_cannot_escape_configured_root(tmp_path):
    storage = LocalStorage(root=tmp_path / "storage-root")

    with pytest.raises(ValueError, match="escapes configured root"):
        storage.write_bytes("../outside.bin", b"forbidden")


def test_local_download_rejects_stale_metadata(tmp_path):
    path = tmp_path / "source.parquet"
    path.write_bytes(b"before")
    storage = LocalStorage(str(tmp_path))
    stale = storage.stat_object(str(path))
    path.write_bytes(b"after-but-different")

    with pytest.raises(OSError, match="changed before download"):
        storage.download_to_file(str(path), io.BytesIO(), expected=stale)


def test_local_download_rejects_invalid_chunk_size(tmp_path):
    path = tmp_path / "source.parquet"
    path.write_bytes(b"data")

    with pytest.raises(ValueError, match="chunk_size must be positive"):
        LocalStorage(str(tmp_path)).download_to_file(str(path), io.BytesIO(), chunk_size=0)


def test_parquet_stream_splits_clustered_wide_rows_to_decoded_budget(
        tmp_path, monkeypatch,
):
    from supertable.storage import storage_interface

    storage = LocalStorage(root=tmp_path)
    values = (["x" * 100_000] * 100) + (["x"] * 9_900)
    storage.write_parquet(pa.table({"value": values}), "skew.parquet")
    budget = 1 * 1024 * 1024
    real_parquet_file = storage_interface.pq.ParquetFile
    decoder_batch_sizes = []

    class RecordingParquetFile:
        def __init__(self, path):
            self._inner = real_parquet_file(path)

        def iter_batches(self, **kwargs):
            decoder_batch_sizes.append(kwargs["batch_size"])
            yield from self._inner.iter_batches(**kwargs)

    monkeypatch.setattr(
        storage_interface.pq, "ParquetFile", RecordingParquetFile,
    )

    batches = list(storage.iter_parquet_batches(
        "skew.parquet", max_decoded_bytes=budget,
    ))

    assert sum(batch.num_rows for batch in batches) == len(values)
    assert all(
        batch.num_rows == 1 or batch.nbytes <= budget
        for batch in batches
    )
    assert max(batch.nbytes for batch in batches) <= budget
    assert max(batch.num_rows for batch in batches) <= (
        PARQUET_DECODE_MAX_PENDING_ROWS
    )
    assert decoder_batch_sizes == [1]


def test_parquet_batch_group_metadata_is_bounded_for_very_narrow_rows():
    """A byte-only budget must not retain one wrapper for every tiny row."""
    row_count = PARQUET_DECODE_MAX_PENDING_BATCHES * 3 + 17
    narrow_batches = (
        SimpleNamespace(nbytes=1, num_rows=1)
        for _ in range(row_count)
    )

    groups = list(_iter_bounded_parquet_batch_groups(
        narrow_batches,
        max_decoded_bytes=row_count * 2,
    ))

    assert sum(len(group) for group in groups) == row_count
    assert max(len(group) for group in groups) == (
        PARQUET_DECODE_MAX_PENDING_BATCHES
    )
    assert all(
        sum(batch.num_rows for batch in group)
        <= PARQUET_DECODE_MAX_PENDING_ROWS
        for group in groups
    )

    row_capped = list(_iter_bounded_parquet_batch_groups(
        (SimpleNamespace(nbytes=1, num_rows=10) for _ in range(7)),
        max_decoded_bytes=1_000,
        max_pending_batches=100,
        max_pending_rows=25,
    ))
    assert [sum(batch.num_rows for batch in group) for group in row_capped] == [
        20, 20, 20, 10,
    ]

    with pytest.raises(RuntimeError, match="larger than the pending-row cap"):
        list(_iter_bounded_parquet_batch_groups(
            [SimpleNamespace(nbytes=1, num_rows=26)],
            max_decoded_bytes=1_000,
            max_pending_rows=25,
        ))


def test_local_range_reads_only_requested_bytes_and_rejects_stale_seal(tmp_path):
    path = tmp_path / "source.parquet"
    payload = bytes(range(251)) * 10
    path.write_bytes(payload)
    storage = LocalStorage(str(tmp_path))
    expected = storage.stat_object(str(path))

    assert storage.read_range(str(path), 127, 31, expected=expected) == payload[127:158]
    path.write_bytes(b"x" * len(payload))
    with pytest.raises(ObjectIdentityMismatch, match="changed"):
        storage.read_range(str(path), 127, 31, expected=expected)


class ChunkBody:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.offset = 0
        self.closed = False
        self.released = False
        self.read_sizes = []

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        chunk = self.payload[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


def test_s3_streams_with_version_and_etag_preconditions():
    pytest.importorskip("boto3")
    from supertable.storage.s3_storage import S3Storage

    payload = b"abcdefghijk"
    body = ChunkBody(payload)
    client = MagicMock()
    client.meta.endpoint_url = "https://s3.amazonaws.com"
    client.meta.region_name = "us-east-1"
    client.get_object.return_value = {
        "Body": body,
        "ContentLength": len(payload),
        "VersionId": "v7",
        "ETag": '"etag-7"',
    }
    storage = S3Storage("bucket", client=client, region="us-east-1")
    storage._bucket_region_checked = True
    expected = ObjectMetadata(size=len(payload), version="v7", etag="etag-7")
    sink = RecordingSink()

    written = storage.download_to_file("raw/key", sink, expected=expected, chunk_size=3)

    assert written == len(payload)
    assert sink.getvalue() == payload
    assert max(body.read_sizes) == 3
    assert body.closed
    client.get_object.assert_called_once_with(
        Bucket="bucket", Key="raw/key", VersionId="v7", IfMatch='"etag-7"',
    )


def test_s3_installed_sdk_models_if_none_match_for_put_object():
    pytest.importorskip("botocore")
    import botocore.session

    model = (
        botocore.session.get_session()
        .get_service_model("s3")
        .operation_model("PutObject")
        .input_shape
    )

    assert "IfNoneMatch" in model.members


def test_s3_range_uses_conditional_bounded_get():
    pytest.importorskip("boto3")
    from supertable.storage.s3_storage import S3Storage

    payload = b"defg"
    body = ChunkBody(payload)
    client = MagicMock()
    client.meta.endpoint_url = "https://s3.amazonaws.com"
    client.meta.region_name = "us-east-1"
    client.get_object.return_value = {
        "Body": body,
        "ContentLength": len(payload),
        "VersionId": "v7",
        "ETag": '"etag-7"',
    }
    storage = S3Storage("bucket", client=client, region="us-east-1")
    storage._bucket_region_checked = True
    expected = ObjectMetadata(size=11, version="v7", etag="etag-7")

    assert storage.read_range("raw/key", 3, 4, expected=expected) == payload
    client.get_object.assert_called_once_with(
        Bucket="bucket",
        Key="raw/key",
        Range="bytes=3-6",
        VersionId="v7",
        IfMatch='"etag-7"',
    )
    assert body.read_sizes == [4, 1]
    assert body.closed


def test_s3_range_rejects_unsealed_or_ignored_range_response():
    pytest.importorskip("boto3")
    from supertable.storage.s3_storage import S3Storage

    client = MagicMock()
    client.meta.endpoint_url = "https://s3.amazonaws.com"
    client.meta.region_name = "us-east-1"
    storage = S3Storage("bucket", client=client, region="us-east-1")
    storage._bucket_region_checked = True

    with pytest.raises(ValueError, match="version or ETag"):
        storage.read_range("raw/key", 3, 4)
    assert not client.get_object.called

    client.get_object.return_value = {
        "Body": ChunkBody(b"defg-ignored-rest-of-object"),
        "ContentLength": 4,
        "VersionId": "v7",
        "ETag": '"etag-7"',
    }
    expected = ObjectMetadata(size=100, version="v7", etag="etag-7")
    with pytest.raises(ObjectIdentityMismatch, match="length"):
        storage.read_range("raw/key", 3, 4, expected=expected)


def test_minio_streams_and_releases_connection_with_version_fence():
    pytest.importorskip("minio")
    from supertable.storage.minio_storage import MinioStorage

    payload = b"abcdefghijk"
    response = ChunkBody(payload)
    client = MagicMock()
    client.get_object.return_value = response
    storage = MinioStorage("bucket", client)
    expected = ObjectMetadata(size=len(payload), version="v7", etag="etag-7")
    sink = RecordingSink()

    written = storage.download_to_file("raw/key", sink, expected=expected, chunk_size=4)

    assert written == len(payload)
    assert sink.getvalue() == payload
    assert response.closed and response.released
    client.get_object.assert_called_once_with(
        "bucket",
        "raw/key",
        version_id="v7",
        request_headers={"If-Match": '"etag-7"'},
    )


def test_minio_range_sets_offset_length_and_version_fence():
    pytest.importorskip("minio")
    from supertable.storage.minio_storage import MinioStorage

    response = ChunkBody(b"cdefg")
    client = MagicMock()
    client.get_object.return_value = response
    storage = MinioStorage("bucket", client)
    expected = ObjectMetadata(size=11, version="v7", etag="etag-7")

    assert storage.read_range("raw/key", 2, 5, expected=expected) == b"cdefg"
    client.get_object.assert_called_once_with(
        "bucket",
        "raw/key",
        offset=2,
        length=5,
        version_id="v7",
        request_headers={"If-Match": '"etag-7"'},
    )
    assert response.read_sizes == [5, 1]
    assert response.closed and response.released


def test_minio_conditional_create_suppresses_binary_body_trace():
    pytest.importorskip("minio")
    from minio import Minio

    from supertable.storage.minio_storage import MinioStorage

    http = MagicMock()
    http.urlopen.return_value = SimpleNamespace(
        data=b"",
        headers={},
        status=200,
    )
    client = Minio(
        "localhost:9000",
        access_key="access-key",
        secret_key="secret-key",
        secure=False,
        region="us-east-1",
    )
    client._http = http
    trace = io.StringIO()
    client.trace_on(trace)
    storage = MinioStorage("bucket", client)
    payload = b"confidential-proof-\xff\x00"

    assert storage.create_bytes_if_absent("proof.json", payload) is True
    assert "confidential-proof" not in trace.getvalue()
    assert http.urlopen.call_args.kwargs["body"] == payload


def test_gcs_streams_to_sink_with_generation_precondition():
    pytest.importorskip("google.cloud.storage")
    from supertable.storage.gcp_storage import GCSStorage

    payload = b"abcdefghijk"
    client = MagicMock()
    bucket = client.bucket.return_value
    blob = bucket.blob.return_value
    blob.download_to_file.side_effect = lambda sink, **_kwargs: sink.write(payload)
    storage = GCSStorage("bucket", client=client, base_prefix="prefix")
    expected = ObjectMetadata(size=len(payload), version="123", etag="etag")
    sink = RecordingSink()

    written = storage.download_to_file("raw/key", sink, expected=expected, chunk_size=256 * 1024)

    assert written == len(payload)
    assert sink.getvalue() == payload
    assert blob.chunk_size == 256 * 1024
    bucket.blob.assert_called_once_with("prefix/raw/key")
    assert len(blob.download_to_file.call_args.args) == 1
    assert blob.download_to_file.call_args.kwargs == {"if_generation_match": 123}


def test_gcs_range_uses_inclusive_end_and_generation_fence():
    pytest.importorskip("google.cloud.storage")
    from supertable.storage.gcp_storage import GCSStorage

    client = MagicMock()
    blob = client.bucket.return_value.blob.return_value
    blob.download_as_bytes.return_value = b"defg"
    storage = GCSStorage("bucket", client=client, base_prefix="prefix")
    expected = ObjectMetadata(size=11, version="123", etag="etag")

    assert storage.read_range("raw/key", 3, 4, expected=expected) == b"defg"
    blob.download_as_bytes.assert_called_once_with(
        start=3, end=6, if_generation_match=123,
    )


def test_gcs_create_bytes_if_absent_uses_generation_zero_and_preserves_errors():
    pytest.importorskip("google.cloud.storage")
    from google.api_core.exceptions import PreconditionFailed
    from supertable.storage.gcp_storage import GCSStorage

    client = MagicMock()
    blob = client.bucket.return_value.blob.return_value
    storage = GCSStorage("bucket", client=client, base_prefix="prefix")

    assert storage.create_bytes_if_absent("proof.json", b"proof") is True
    blob.upload_from_string.assert_called_once_with(
        b"proof", if_generation_match=0,
    )

    blob.upload_from_string.side_effect = PreconditionFailed("exists")
    assert storage.create_bytes_if_absent("proof.json", b"proof") is False
    blob.upload_from_string.side_effect = OSError("ambiguous")
    with pytest.raises(OSError, match="ambiguous"):
        storage.create_bytes_if_absent("proof.json", b"proof")


def test_gcs_parquet_failure_redacts_path_and_backend_message(monkeypatch):
    pytest.importorskip("google.cloud.storage")
    from supertable.storage import gcp_storage
    from supertable.storage.gcp_storage import GCSStorage

    secret = "signed-path-token-DO-NOT-LOG"
    client = MagicMock()
    blob = client.bucket.return_value.get_blob.return_value
    blob.download_as_bytes.return_value = b"not parquet"
    storage = GCSStorage("bucket", client=client, base_prefix="prefix")
    monkeypatch.setattr(
        gcp_storage.pq,
        "read_table",
        MagicMock(side_effect=RuntimeError(f"backend-secret-{secret}")),
    )

    with pytest.raises(
        RuntimeError,
        match=r"^Failed to read Parquet; error_type=RuntimeError$",
    ) as caught:
        storage.read_parquet(f"tenant/{secret}.parquet")

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert secret not in rendered


def test_azure_streams_bounded_sink_writes_with_version_and_etag():
    pytest.importorskip("azure.storage.blob")
    from supertable.storage.azure_storage import AzureBlobStorage

    payload = b"abcdefghijk"
    service = MagicMock()
    container = service.get_container_client.return_value
    blob = container.get_blob_client.return_value
    blob.download_blob.return_value.chunks.return_value = [payload]
    storage = AzureBlobStorage("container", service, base_prefix="prefix")
    expected = ObjectMetadata(size=len(payload), version="v7", etag='"etag-7"')
    sink = RecordingSink()

    written = storage.download_to_file("raw/key", sink, expected=expected, chunk_size=4)

    assert written == len(payload)
    assert sink.getvalue() == payload
    assert max(sink.write_sizes) <= 4
    container.get_blob_client.assert_called_once_with(
        "prefix/raw/key", version_id="v7",
    )
    assert blob.download_blob.call_args.kwargs["etag"] == '"etag-7"'


def test_azure_range_uses_offset_length_and_version_fence():
    pytest.importorskip("azure.storage.blob")
    from azure.core import MatchConditions
    from supertable.storage.azure_storage import AzureBlobStorage

    service = MagicMock()
    container = service.get_container_client.return_value
    blob = container.get_blob_client.return_value
    blob.download_blob.return_value.readall.return_value = b"defg"
    storage = AzureBlobStorage("container", service, base_prefix="prefix")
    expected = ObjectMetadata(size=11, version="v7", etag='"etag-7"')

    assert storage.read_range("raw/key", 3, 4, expected=expected) == b"defg"
    container.get_blob_client.assert_called_once_with(
        "prefix/raw/key", version_id="v7",
    )
    blob.download_blob.assert_called_once_with(
        offset=3,
        length=4,
        etag='"etag-7"',
        match_condition=MatchConditions.IfNotModified,
    )


def test_azure_create_bytes_if_absent_never_enables_overwrite():
    pytest.importorskip("azure.storage.blob")
    from azure.core.exceptions import ResourceExistsError
    from supertable.storage.azure_storage import AzureBlobStorage

    service = MagicMock()
    blob = service.get_container_client.return_value.get_blob_client.return_value
    storage = AzureBlobStorage("container", service, base_prefix="prefix")

    assert storage.create_bytes_if_absent("proof.json", b"proof") is True
    assert blob.upload_blob.call_args.args == (b"proof",)
    assert blob.upload_blob.call_args.kwargs["overwrite"] is False

    blob.upload_blob.side_effect = ResourceExistsError("exists")
    assert storage.create_bytes_if_absent("proof.json", b"proof") is False
    blob.upload_blob.side_effect = OSError("ambiguous")
    with pytest.raises(OSError, match="ambiguous"):
        storage.create_bytes_if_absent("proof.json", b"proof")


def test_azure_parquet_failure_redacts_path_and_backend_message(monkeypatch):
    pytest.importorskip("azure.storage.blob")
    from supertable.storage import azure_storage
    from supertable.storage.azure_storage import AzureBlobStorage

    secret = "signed-path-token-DO-NOT-LOG"
    service = MagicMock()
    blob = service.get_container_client.return_value.get_blob_client.return_value
    blob.download_blob.return_value.readall.return_value = b"not parquet"
    storage = AzureBlobStorage("container", service, base_prefix="prefix")
    monkeypatch.setattr(
        azure_storage.pq,
        "read_table",
        MagicMock(side_effect=RuntimeError(f"backend-secret-{secret}")),
    )

    with pytest.raises(
        RuntimeError,
        match=r"^Failed to read Parquet; error_type=RuntimeError$",
    ) as caught:
        storage.read_parquet(f"tenant/{secret}.parquet")

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert secret not in rendered


def test_gcs_delete_prefix_drains_all_batches_and_retries_partial_failure():
    pytest.importorskip("google.cloud.storage")
    from supertable.storage.gcp_storage import GCSStorage

    remaining = {f"base/table/file-{index:04d}" for index in range(3505)}
    failed_once = {"value": False}

    class Blob:
        def __init__(self, name):
            self.name = name

        def delete(self):
            if self.name.endswith("file-0000") and not failed_once["value"]:
                failed_once["value"] = True
                raise OSError("transient GCS delete error")
            remaining.discard(self.name)

    client = MagicMock()
    bucket = client.bucket.return_value
    bucket.get_blob.return_value = None
    client.list_blobs.side_effect = lambda *_args, **_kwargs: [
        Blob(name) for name in sorted(remaining)
    ]
    storage = GCSStorage("bucket", client=client, base_prefix="base")

    storage.delete_prefix("table")

    assert remaining == set()
    assert failed_once["value"] is True
    assert client.list_blobs.call_count >= 5
    assert all(
        call.kwargs["prefix"] == "base/table/"
        for call in client.list_blobs.call_args_list
    )


def test_azure_delete_prefix_drains_all_batches_and_retries_partial_failure():
    pytest.importorskip("azure.storage.blob")
    from supertable.storage.azure_storage import AzureBlobStorage

    remaining = {f"base/table/file-{index:04d}" for index in range(3505)}
    failed_once = {"value": False}
    service = MagicMock()
    container = service.get_container_client.return_value
    container.list_blobs.side_effect = lambda **_kwargs: [
        SimpleNamespace(name=name) for name in sorted(remaining)
    ]

    def delete_blob(name):
        if name.endswith("file-0000") and not failed_once["value"]:
            failed_once["value"] = True
            raise OSError("transient Azure delete error")
        remaining.discard(name)

    container.delete_blob.side_effect = delete_blob
    storage = AzureBlobStorage("container", service, base_prefix="base")
    storage._blob_exists = lambda _name: False

    storage.delete_prefix("table")

    assert remaining == set()
    assert failed_once["value"] is True
    assert container.list_blobs.call_count >= 5
    assert all(
        call.kwargs["name_starts_with"] == "base/table/"
        for call in container.list_blobs.call_args_list
    )


def test_gcs_delete_prefix_failure_suppresses_path_and_provider_text():
    pytest.importorskip("google.cloud.storage")
    from supertable.storage.gcp_storage import GCSStorage

    secret = "https://gcs.invalid/private?signature=DELETE_SECRET"

    class Blob:
        name = "base/table/PRIVATE_OBJECT_SECRET"

        def delete(self):
            raise OSError(secret)

    listings = iter([[Blob()], []])
    client = MagicMock()
    bucket = client.bucket.return_value
    bucket.get_blob.return_value = None
    client.list_blobs.side_effect = lambda *_args, **_kwargs: next(listings)
    storage = GCSStorage("bucket", client=client, base_prefix="base")
    path_secret = "table/PRIVATE_PATH_SECRET"

    with pytest.raises(OSError) as caught:
        storage.delete_prefix(path_secret)

    rendered = "".join(traceback.format_exception(caught.value))
    for forbidden in (secret, "DELETE_SECRET", "PRIVATE_OBJECT_SECRET", "PRIVATE_PATH_SECRET"):
        assert forbidden not in rendered
    assert caught.value.__cause__ is None


def test_azure_delete_prefix_failure_suppresses_path_and_provider_text():
    pytest.importorskip("azure.storage.blob")
    from supertable.storage.azure_storage import AzureBlobStorage

    secret = "https://azure.invalid/private?signature=DELETE_SECRET"
    service = MagicMock()
    container = service.get_container_client.return_value
    listings = iter([
        [SimpleNamespace(name="base/table/PRIVATE_OBJECT_SECRET")],
        [],
    ])
    container.list_blobs.side_effect = lambda **_kwargs: next(listings)
    container.delete_blob.side_effect = OSError(secret)
    storage = AzureBlobStorage("container", service, base_prefix="base")
    storage._blob_exists = lambda _name: False
    path_secret = "table/PRIVATE_PATH_SECRET"

    with pytest.raises(OSError) as caught:
        storage.delete_prefix(path_secret)

    rendered = "".join(traceback.format_exception(caught.value))
    for forbidden in (secret, "DELETE_SECRET", "PRIVATE_OBJECT_SECRET", "PRIVATE_PATH_SECRET"):
        assert forbidden not in rendered
    assert caught.value.__cause__ is None
