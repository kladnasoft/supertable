# route: supertable.tests.test_s3_storage_retry
"""
Regression tests for payload integrity across :meth:`S3Storage._call` retries.

``_call`` transparently retries a request once after correcting the client for
a redirect (``PermanentRedirect`` and friends).  botocore consumes a file-like
``Body`` while sending the first attempt, so retrying with the *same* stream
object uploaded only the unconsumed remainder — in practice zero bytes — and
returned success.  That produced silently empty parquet objects.
"""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest

pytest.importorskip("boto3")
pytest.importorskip("pyarrow")

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from supertable.storage.s3_storage import S3Storage


REDIRECT_ERROR = {
    "Error": {
        "Code": "PermanentRedirect",
        "Region": "eu-central-1",
        "Message": "The bucket is in this region: eu-central-1",
    },
    "ResponseMetadata": {"HTTPStatusCode": 301, "HTTPHeaders": {}},
}


class RecordingPutClient:
    """boto3 stand-in that consumes the request body exactly as botocore does."""

    def __init__(self, fail_first: bool = True):
        self.fail_first = fail_first
        self.uploads: list[bytes] = []
        self.meta = SimpleNamespace(
            endpoint_url="https://s3.amazonaws.com", region_name="us-east-1",
        )

    @staticmethod
    def _drain(body) -> bytes:
        if body is None:
            return b""
        if isinstance(body, (bytes, bytearray, memoryview)):
            return bytes(body)
        if hasattr(body, "read"):
            return body.read()
        return b"".join(body)

    def put_object(self, **kwargs):
        self.uploads.append(self._drain(kwargs.get("Body")))
        if self.fail_first and len(self.uploads) == 1:
            raise ClientError(REDIRECT_ERROR, "PutObject")
        return {"ETag": '"deadbeef"'}


@pytest.fixture
def storage(monkeypatch):
    """S3Storage wired to a recording client, with client rebuilds neutralised.

    ``_rebuild_client`` would otherwise swap in a real boto3 client mid-retry
    and discard the recorder.
    """
    def _make(fail_first: bool = True) -> tuple[S3Storage, RecordingPutClient]:
        client = RecordingPutClient(fail_first=fail_first)
        monkeypatch.setattr(S3Storage, "_rebuild_client", lambda self: None)
        s3 = S3Storage("bucket", client=client, region="us-east-1")
        s3._bucket_region_checked = True
        return s3, client

    return _make


def test_write_parquet_retry_after_redirect_uploads_full_payload(storage):
    """A retried parquet PUT must carry the whole file, not a drained stream."""
    s3, client = storage()
    table = pa.table({"a": list(range(500)), "b": ["x" * 20] * 500})

    s3.write_parquet(table, "data/t.parquet")

    assert len(client.uploads) == 2, "expected exactly one redirect retry"
    first, second = client.uploads
    assert len(first) > 0
    # The bug: the retry uploaded 0 bytes and reported success.
    assert len(second) == len(first) > 0
    assert second == first
    assert pq.read_table(io.BytesIO(second)).equals(table)


def test_write_bytes_retry_after_redirect_uploads_full_payload(storage):
    s3, client = storage()

    s3.write_bytes("data/blob.bin", b"payload-bytes" * 100)

    assert client.uploads == [b"payload-bytes" * 100] * 2


def test_call_rewinds_seekable_body_between_attempts(storage):
    """Defence in depth: _call itself restores a rewindable stream per attempt."""
    s3, client = storage()
    payload = b"stream-payload" * 100
    buf = io.BytesIO(payload)

    s3._call("put_object", Bucket="bucket", Key="k", Body=buf)

    assert client.uploads == [payload, payload]


def test_call_preserves_non_zero_start_offset_on_rewind(storage):
    s3, client = storage()
    buf = io.BytesIO(b"HEADER" + b"tail-payload")
    buf.seek(6)

    s3._call("put_object", Bucket="bucket", Key="k", Body=buf)

    assert client.uploads == [b"tail-payload", b"tail-payload"]


def test_call_refuses_to_retry_an_unrewindable_body(storage):
    """A consumed generator cannot be replayed, so the error must propagate."""
    s3, client = storage()
    body = (chunk for chunk in (b"chunk-a", b"chunk-b"))

    with pytest.raises(ClientError):
        s3._call("put_object", Bucket="bucket", Key="k", Body=body)

    assert client.uploads == [b"chunk-achunk-b"], "must not retry a drained stream"


def test_call_still_retries_bodyless_operations(storage):
    """The rewind guard must not disable the existing redirect retry."""
    s3, client = storage()

    s3._call("put_object", Bucket="bucket", Key="k")

    assert client.uploads == [b"", b""]
