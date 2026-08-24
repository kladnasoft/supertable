"""Shared classification for object-store paths used by query engines."""
from __future__ import annotations


# Schemes emitted by the built-in S3/MinIO, GCS, and Azure adapters, plus
# their common Hadoop spellings and the HTTP form used for signed URLs.
REMOTE_SCAN_PREFIXES = (
    "s3://",
    "s3a://",
    "http://",
    "https://",
    "gcs://",
    "gs://",
    "azure://",
    "abfs://",
    "abfss://",
)


def is_remote_scan_path(path: object) -> bool:
    """Return whether *path* names a supported non-local scan resource."""
    return str(path or "").strip().casefold().startswith(
        REMOTE_SCAN_PREFIXES
    )
