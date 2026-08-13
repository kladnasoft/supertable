import base64
import hashlib
import io
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
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
    storage = LocalStorage()
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


def test_local_download_rejects_stale_metadata(tmp_path):
    path = tmp_path / "source.parquet"
    path.write_bytes(b"before")
    storage = LocalStorage()
    stale = storage.stat_object(str(path))
    path.write_bytes(b"after-but-different")

    with pytest.raises(OSError, match="changed before download"):
        storage.download_to_file(str(path), io.BytesIO(), expected=stale)


def test_local_download_rejects_invalid_chunk_size(tmp_path):
    path = tmp_path / "source.parquet"
    path.write_bytes(b"data")

    with pytest.raises(ValueError, match="chunk_size must be positive"):
        LocalStorage().download_to_file(str(path), io.BytesIO(), chunk_size=0)


def test_local_range_reads_only_requested_bytes_and_rejects_stale_seal(tmp_path):
    path = tmp_path / "source.parquet"
    payload = bytes(range(251)) * 10
    path.write_bytes(payload)
    storage = LocalStorage()
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
