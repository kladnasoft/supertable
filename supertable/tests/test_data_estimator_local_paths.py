"""LOCAL storage must never be described with an object-store URL.

The endpoint/bucket settings can be populated for a backend that is not the one
currently in use — a process configured for MinIO may still be asked to read
LOCAL storage (benchmarks, tests, a local export). ``_to_duckdb_path`` used to
consult those settings unconditionally, which produced two failure modes:

* it referenced an undefined name and raised ``NameError`` on every read, and
* once that was fixed, it happily built ``s3://bucket/key`` for a local file,
  so DuckDB went to the object store for data that was on disk and got 404.

These tests pin the rule: the settings fallbacks apply only when a bucket
backend is actually selected. Object-store behaviour is pinned alongside it so
the gate cannot be widened into a regression for real S3/MinIO users.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from supertable.engine import data_estimator as de


class _AttrlessStorage:
    """Stand-in for LocalStorage: exposes no endpoint, bucket or URL helper."""


class _BucketStorage:
    """Stand-in for a storage object that knows its own bucket."""
    bucket = "from-storage"


def _estimator(storage) -> de.DataEstimator:
    # The path helpers depend only on ``self.storage``, so bypass the full
    # constructor rather than standing up a catalog for a pure path question.
    estimator = object.__new__(de.DataEstimator)
    estimator.storage = storage
    return estimator


@pytest.fixture
def with_settings(monkeypatch):
    """Swap in object-store settings, as a real deployment carries them.

    ``Settings`` is a frozen dataclass, so the whole object is replaced rather
    than mutated field by field.
    """
    def apply(storage_type: str):
        monkeypatch.setattr(de, "settings", SimpleNamespace(
            STORAGE_TYPE=storage_type,
            STORAGE_ENDPOINT_URL="http://minio:9000",
            STORAGE_BUCKET="configured-bucket",
            STORAGE_USE_SSL=False,
            SUPERTABLE_DUCKDB_USE_HTTPFS=False,
            SUPERTABLE_DUCKDB_PRESIGNED=False,
        ))
    return apply


def test_local_storage_does_not_become_an_s3_url(with_settings):
    """The regression: a local file addressed as s3:// resolves to nothing."""
    with_settings("LOCAL")
    resolved = _estimator(_AttrlessStorage())._to_duckdb_path("tables/t/x.parquet")
    assert not resolved.startswith("s3://")
    assert resolved == "tables/t/x.parquet"


def test_local_storage_detects_no_endpoint_or_bucket(with_settings):
    with_settings("LOCAL")
    estimator = _estimator(_AttrlessStorage())
    assert estimator._detect_endpoint() is None
    assert estimator._detect_bucket() is None


def test_endpoint_detection_does_not_raise_name_error(with_settings):
    """Guards the original defect: this branch referenced an undefined name."""
    with_settings("MINIO")
    assert _estimator(_AttrlessStorage())._detect_endpoint() == "minio:9000"


@pytest.mark.parametrize("storage_type", ["MINIO", "S3"])
def test_object_store_still_builds_a_bucket_url(with_settings, storage_type):
    """The gate must not take the settings fallback away from real buckets."""
    with_settings(storage_type)
    resolved = _estimator(_AttrlessStorage())._to_duckdb_path("tables/t/x.parquet")
    assert resolved == "s3://configured-bucket/tables/t/x.parquet"


def test_storage_owned_bucket_wins_over_settings(with_settings):
    """A storage object that knows its own bucket is authoritative."""
    with_settings("MINIO")
    assert _estimator(_BucketStorage())._detect_bucket() == "from-storage"
