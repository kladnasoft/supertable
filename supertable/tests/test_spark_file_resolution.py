"""Spark file-path resolution: direct s3a:// by default, presigned on opt-in.

``sup.files`` are resolved once by the estimator using the *DuckDB* presign
setting (``SUPERTABLE_DUCKDB_PRESIGNED``) and shared across every engine.  Spark
must not inherit that choice: a cluster that can reach the object store with its
own ``fs.s3a.*`` credentials should scan direct ``s3a://`` paths, while a cluster
that cannot should be able to opt into presigned URLs — independently of how the
files were shaped for DuckDB.

``_resolve_spark_file`` is the decoupling point, governed solely by
``SUPERTABLE_SPARK_PRESIGNED``:

  * default (False) → ``_to_s3a_path`` normalises s3://, presigned http(s):// and
    s3a:// inputs to a direct ``s3a://bucket/key`` and never touches ``presign``;
  * opt-in (True)   → the object key is recovered (base_prefix stripped, as the
    storage layer re-applies it) and a fresh presigned URL is minted, even when
    the shared list held bare ``s3://`` paths.

These are pure helpers, so the tests drive them directly with a fake storage —
no Spark/Thrift cluster involved.
"""
from __future__ import annotations

import dataclasses

import pytest

from supertable.config.settings import settings
from supertable.engine import spark_thrift
from supertable.engine.spark_thrift import _resolve_spark_file, _to_s3a_path


class _FakeStorage:
    """Minimal storage stub exposing only what the resolver reads."""

    def __init__(self, base_prefix: str = "", *, result=None, raises: bool = False):
        self.base_prefix = base_prefix
        self._result = result
        self._raises = raises
        self.presign_calls: list[str] = []

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        self.presign_calls.append(key)
        if self._raises:
            raise RuntimeError("presign boom")
        return self._result if self._result is not None else f"https://signed.example/{key}?sig=x"


class _NoPresignStorage:
    base_prefix = ""


def _set_presigned(monkeypatch, enabled: bool) -> None:
    # Mirror the module-level ``settings`` swap used by the other setting-gated
    # suites (settings is a frozen dataclass, replaced not mutated).
    monkeypatch.setattr(
        spark_thrift, "settings",
        dataclasses.replace(settings, SUPERTABLE_SPARK_PRESIGNED=enabled),
    )


# A realistic DuckDB-minted presigned URL: path-style endpoint, a Hive
# partition dir percent-escaped by boto3, and the presign query string.
_PRESIGNED_IN = (
    "https://minio:9000/bucket/base/year%3D2026/f.parquet"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef"
)


class TestDefaultDirectS3a:
    """SUPERTABLE_SPARK_PRESIGNED=False → direct s3a://, presign untouched."""

    @pytest.fixture(autouse=True)
    def _off(self, monkeypatch):
        _set_presigned(monkeypatch, False)

    def test_s3_scheme_becomes_s3a(self):
        assert _resolve_spark_file(_FakeStorage(), "s3://bucket/a/b.parquet") == "s3a://bucket/a/b.parquet"

    def test_presigned_http_stripped_to_s3a(self):
        # Query dropped, %3D decoded back to '=' — Spark's S3A re-encodes itself.
        assert _resolve_spark_file(_FakeStorage(), _PRESIGNED_IN) == "s3a://bucket/base/year=2026/f.parquet"

    def test_s3a_unchanged(self):
        assert _resolve_spark_file(_FakeStorage(), "s3a://bucket/k") == "s3a://bucket/k"

    def test_local_path_unchanged(self):
        assert _resolve_spark_file(_FakeStorage(), "/tmp/x.parquet") == "/tmp/x.parquet"

    def test_presign_never_called(self):
        # Even with a presign-capable storage, the default path must not call it.
        st = _FakeStorage()
        _resolve_spark_file(st, "s3://bucket/a/b.parquet")
        assert st.presign_calls == []

    def test_matches_bare_to_s3a(self):
        # Default resolution is exactly _to_s3a_path (the historical behaviour).
        for f in ("s3://bucket/k", _PRESIGNED_IN, "s3a://bucket/k", "/tmp/x.parquet"):
            assert _resolve_spark_file(_FakeStorage(), f) == _to_s3a_path(f)


class TestPresignedOptIn:
    """SUPERTABLE_SPARK_PRESIGNED=True → freshly minted presigned URLs."""

    @pytest.fixture(autouse=True)
    def _on(self, monkeypatch):
        _set_presigned(monkeypatch, True)

    def test_s3_no_base_prefix_presigns_rel_key(self):
        st = _FakeStorage(base_prefix="", result="https://signed/a/b.parquet?sig=1")
        out = _resolve_spark_file(st, "s3://bucket/a/b.parquet")
        assert out == "https://signed/a/b.parquet?sig=1"
        assert st.presign_calls == ["a/b.parquet"]

    def test_base_prefix_stripped_before_presign(self):
        # storage.presign() re-applies base_prefix, so the resolver must strip it.
        st = _FakeStorage(base_prefix="base")
        _resolve_spark_file(st, "s3://bucket/base/a/b.parquet")
        assert st.presign_calls == ["a/b.parquet"]

    def test_represigns_a_duckdb_presigned_url(self):
        # A URL already presigned for DuckDB is re-resolved to the raw key and
        # presigned afresh — so Spark's access is independent of DuckDB's.
        st = _FakeStorage(base_prefix="base", result="https://fresh/f.parquet?sig=new")
        out = _resolve_spark_file(st, _PRESIGNED_IN)
        assert out == "https://fresh/f.parquet?sig=new"
        assert st.presign_calls == ["year=2026/f.parquet"]

    def test_local_path_not_presigned(self):
        st = _FakeStorage()
        assert _resolve_spark_file(st, "/tmp/x.parquet") == "/tmp/x.parquet"
        assert st.presign_calls == []

    def test_storage_without_presign_falls_back_to_s3a(self):
        assert _resolve_spark_file(_NoPresignStorage(), "s3://bucket/k") == "s3a://bucket/k"

    def test_presign_exception_falls_back_to_s3a(self):
        st = _FakeStorage(raises=True)
        assert _resolve_spark_file(st, "s3://bucket/a/b.parquet") == "s3a://bucket/a/b.parquet"

    def test_presign_none_result_falls_back_to_s3a(self):
        st = _FakeStorage(result=None)
        # result=None makes the stub return its default string, so force empty:
        st._result = ""
        assert _resolve_spark_file(st, "s3://bucket/k") == "s3a://bucket/k"

    def test_none_storage_falls_back_to_s3a(self):
        assert _resolve_spark_file(None, "s3://bucket/k") == "s3a://bucket/k"
