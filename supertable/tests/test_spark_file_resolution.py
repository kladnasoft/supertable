"""Spark file-path resolution never exposes bearer URLs to user SQL.

``sup.files`` are resolved once by the estimator using the *DuckDB* presign
setting (``SUPERTABLE_DUCKDB_PRESIGNED``) and shared across every engine.  Spark
must not inherit that choice.  A cluster must scan direct backend paths using
cluster-side workload identity or a Hadoop credential provider.

``_resolve_spark_file`` is the decoupling point, governed solely by
``SUPERTABLE_SPARK_PRESIGNED``:

  * ``False`` → direct provider URIs stay credential-less; a custom HTTP
    endpoint is resolved only through ``storage.canonical_uri(raw_key)``;
  * ``True`` → fail closed because a Spark source URL is visible to the same
    session that runs user SQL and is therefore a reusable bearer credential.

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

    def __init__(
        self,
        base_prefix: str = "",
        *,
        result=None,
        raises: bool = False,
        canonical_result=None,
        canonical_raises: bool = False,
    ):
        self.base_prefix = base_prefix
        self._result = result
        self._raises = raises
        self._canonical_result = canonical_result
        self._canonical_raises = canonical_raises
        self.presign_calls: list[str] = []
        self.canonical_calls: list[str] = []

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        self.presign_calls.append(key)
        if self._raises:
            raise RuntimeError("presign boom")
        return self._result if self._result is not None else f"https://signed.example/{key}?sig=x"

    def canonical_uri(self, key: str) -> str:
        self.canonical_calls.append(key)
        if self._canonical_raises:
            raise RuntimeError("backend SECRET must not escape")
        if self._canonical_result is None:
            raise RuntimeError("canonical URI unavailable")
        return self._canonical_result


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

    def test_custom_presigned_http_uses_pinned_backend_uri(self):
        storage = _FakeStorage(
            canonical_result="s3://trusted-bucket/base/year%3D2026/f.parquet"
        )
        assert _resolve_spark_file(
            storage,
            _PRESIGNED_IN,
            raw_key="year=2026/f.parquet",
        ) == "s3a://trusted-bucket/base/year%3D2026/f.parquet"
        assert storage.canonical_calls == ["year=2026/f.parquet"]

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
        for f in ("s3://bucket/k", "s3a://bucket/k", "/tmp/x.parquet"):
            assert _resolve_spark_file(_FakeStorage(), f) == _to_s3a_path(f)

    @pytest.mark.parametrize(
        "signed_url",
        [
            "https://objects.internal/attacker-bucket/wrong.parquet?sig=SECRET",
            "https://attacker-bucket.objects.internal/wrong.parquet?sig=SECRET",
        ],
    )
    def test_custom_endpoint_shape_never_controls_bucket_or_key(self, signed_url):
        storage = _FakeStorage(
            canonical_result="s3://trusted-bucket/prefix/right.parquet"
        )

        resolved = _resolve_spark_file(
            storage,
            signed_url,
            raw_key="right.parquet",
        )

        assert resolved == "s3a://trusted-bucket/prefix/right.parquet"
        assert "attacker-bucket" not in resolved

    @pytest.mark.parametrize(
        "url",
        [
            _PRESIGNED_IN,
            "https://bucket.objects.internal/key.parquet",
        ],
    )
    def test_unknown_custom_http_without_pinned_backend_uri_fails(self, url):
        with pytest.raises(RuntimeError, match="bearer credential|cannot be mapped"):
            _resolve_spark_file(_FakeStorage(), url)

    def test_backend_uri_failure_has_credential_safe_error(self):
        storage = _FakeStorage(canonical_raises=True)

        with pytest.raises(RuntimeError) as captured:
            _resolve_spark_file(storage, _PRESIGNED_IN, raw_key="right.parquet")

        assert "SECRET" not in str(captured.value)

    def test_ambiguous_signed_url_fails_closed(self):
        signed_adls = (
            "https://acct.dfs.core.windows.net/container/key.parquet"
            "?sig=SECRET&sp=r"
        )
        with pytest.raises(RuntimeError, match="bearer credential"):
            _resolve_spark_file(_FakeStorage(), signed_adls)


class TestPresignedModeDisabled:
    """SUPERTABLE_SPARK_PRESIGNED=True is a release-blocking config error."""

    @pytest.fixture(autouse=True)
    def _on(self, monkeypatch):
        _set_presigned(monkeypatch, True)

    def test_remote_path_rejected_before_presign(self):
        st = _FakeStorage()
        with pytest.raises(RuntimeError, match="SPARK_PRESIGNED is disabled"):
            _resolve_spark_file(st, "s3://bucket/a/b.parquet")
        assert st.presign_calls == []

    def test_local_path_also_rejected_until_mode_is_removed(self):
        with pytest.raises(RuntimeError, match="SPARK_PRESIGNED is disabled"):
            _resolve_spark_file(_NoPresignStorage(), "/tmp/x.parquet")
