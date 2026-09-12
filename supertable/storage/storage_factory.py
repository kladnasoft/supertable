# route: supertable.storage.storage_factory
"""
Dynamic storage factory with lazy imports and optional cloud dependencies.

- Default backend: LOCAL (no extra packages required)
- Optional backends (install on demand):
    pip install 'supertable[minio]'  -> MinIO
    pip install 'supertable[s3]'     -> AWS S3
    pip install 'supertable[azure]'  -> Azure Blob
    pip install 'supertable[all]'    -> all of the above
"""

from typing import Any, Dict, Optional, Tuple
import importlib
import os
import threading

from supertable.config.defaults import default
from supertable.config.settings import settings
from supertable.storage.storage_interface import StorageInterface


def _require(module: str, extra: str) -> None:
    """
    Ensure a module is importable. If not, raise a friendly hint to install the right extra.
    """
    if importlib.util.find_spec(module) is None:
        raise RuntimeError(
            f"Missing dependency '{module}'. Install it with: pip install 'supertable[{extra}]'"
        )


# =========================================================
# Backend cache
# =========================================================
#
# A backend is a handle over a cloud SDK client, and building one is not free:
# ``MinioStorage.from_env`` ends with a live ``bucket_exists`` (HeadBucket) and
# ``S3Storage`` pays ~13 ms just to construct ``boto3.client("s3")``, then a
# ``head_bucket`` on first use.  ``get_storage()`` is called once per
# ``DataReader``, once per ``SuperTable`` and once per table inside the
# estimator's loop — measured at 2 calls for a single-table query and 4 for a
# three-table join, i.e. N+1 rebuilds and N+1 round-trips for a configuration
# that cannot change mid-process.
#
# THREADING.  The cache is process-wide, NOT per-thread, and that is a
# deliberate choice:
#   * ``minio.Minio`` documents itself as "thread safe when using the Python
#     threading library" (it is only unsafe to share across *processes*);
#     botocore's low-level clients are likewise documented thread-safe, while
#     client *construction* is not — which is exactly what the lock below
#     serialises.
#   * The backend wrappers hold immutable configuration after construction.
#     The one mutation either can perform is the self-healing client rebuild on
#     a region-mismatch error, an atomic rebind to an equivalent client that
#     converges to the same value from every thread.
#   * A per-thread cache would multiply the HeadBucket round-trips by the
#     worker-thread count and give most of the cost straight back — the read
#     path is threaded.
# This is the opposite of ``engine/duckdb.py``, which keys its connection per
# thread: a DuckDB connection genuinely is not thread-safe, an SDK client is.
#
# FORKING.  A client is NOT safe to share across processes, and a pre-fork
# ``get_storage()`` would otherwise leak one into every child, so the cache is
# dropped in the child at fork (see ``os.register_at_fork`` below).
_CACHE_LOCK = threading.Lock()
_STORAGE_CACHE: Dict[Tuple[Any, ...], StorageInterface] = {}


def _cache_key(storage_type: str) -> Tuple[Any, ...]:
    """Everything a backend's ``from_env()`` reads, so a config change misses.

    ``settings`` is looked up as a module global on every call on purpose:
    tests swap that binding to exercise alternative configurations, and the key
    has to follow them or a cached backend would answer for the wrong settings.
    """
    s = settings
    return (
        storage_type,
        # shared / S3 / MinIO
        s.STORAGE_BUCKET, s.STORAGE_REGION, s.STORAGE_ENDPOINT_URL,
        s.STORAGE_ACCESS_KEY, s.STORAGE_SECRET_KEY, s.STORAGE_SESSION_TOKEN,
        s.STORAGE_FORCE_PATH_STYLE, s.STORAGE_USE_SSL, s.SUPERTABLE_PREFIX,
        # Azure
        s.AZURE_STORAGE_ACCOUNT, s.AZURE_CONTAINER, s.AZURE_BLOB_ENDPOINT,
        s.AZURE_STORAGE_CONNECTION_STRING, s.AZURE_STORAGE_KEY, s.AZURE_SAS_TOKEN,
        # GCP
        s.GCS_BUCKET, s.GOOGLE_APPLICATION_CREDENTIALS, s.GCP_SA_JSON, s.GCP_PROJECT,
    )


def reset_storage_cache() -> None:
    """Forget every cached backend.

    Call this after changing storage configuration inside a live process —
    which in practice means tests, and the child side of a fork.
    """
    with _CACHE_LOCK:
        _STORAGE_CACHE.clear()


if hasattr(os, "register_at_fork"):  # pragma: no branch - always true on POSIX
    os.register_at_fork(after_in_child=reset_storage_cache)


def get_storage(kind: Optional[str] = None, **kwargs: Any) -> StorageInterface:
    """
    Returns a StorageInterface instance for the selected backend.

    Selection order:
      1) explicit `kind` argument if provided
      2) settings.STORAGE_TYPE (read from the environment at import)
      3) default.STORAGE_TYPE (e.g., 'LOCAL', 'S3', 'MINIO', 'AZURE')
      4) fallback to 'LOCAL'

    For AZURE and MINIO: if no args are provided, construct from environment.

    The env-constructed backends are memoized per process, keyed on the
    resolved storage type plus every setting their ``from_env()`` reads, so a
    configuration change still produces a new backend.  Callers that pass
    explicit ``**kwargs`` are never cached: those arguments can carry a live
    client object whose identity the caller chose, and the result must stay
    exactly the object that call constructed.
    """
    storage_type = (
        (kind or "").upper()
        or settings.STORAGE_TYPE
        or (getattr(default, "STORAGE_TYPE", None) or "LOCAL").upper()
    )

    if not kwargs:
        key = _cache_key(storage_type)
        cached = _STORAGE_CACHE.get(key)
        if cached is not None:
            return cached
        with _CACHE_LOCK:
            # Re-check: another thread may have built it while we waited.  The
            # lock is held across construction because SDK client creation is
            # the part that is documented as not thread-safe.
            cached = _STORAGE_CACHE.get(key)
            if cached is None:
                cached = _build_storage(storage_type, kwargs)
                _STORAGE_CACHE[key] = cached
            return cached

    return _build_storage(storage_type, kwargs)


def _build_storage(storage_type: str, kwargs: Dict[str, Any]) -> StorageInterface:
    """Construct a backend, uncached.  The original body of ``get_storage``."""
    if storage_type == "LOCAL":
        mod = importlib.import_module("supertable.storage.local_storage")
        return getattr(mod, "LocalStorage")(**kwargs)

    if storage_type == "S3":
        _require("boto3", "s3")
        mod = importlib.import_module("supertable.storage.s3_storage")
        S3Storage = getattr(mod, "S3Storage")
        if kwargs:
            return S3Storage(**kwargs)
        return S3Storage.from_env()

    if storage_type == "MINIO":
        _require("minio", "minio")
        mod = importlib.import_module("supertable.storage.minio_storage")
        MinioStorage = getattr(mod, "MinioStorage")
        if kwargs:
            # Backward compatibility: explicit parameters provided by caller
            return MinioStorage(**kwargs)
        # From environment (endpoint, creds, bucket)
        return MinioStorage.from_env()

    if storage_type == "AZURE":
        _require("azure.storage.blob", "azure")
        mod = importlib.import_module("supertable.storage.azure_storage")
        AzureBlobStorage = getattr(mod, "AzureBlobStorage")
        if kwargs:
            # Backward compatibility: explicit parameters provided by caller
            return AzureBlobStorage(**kwargs)
        # From environment (supports managed identity & abfss SUPERTABLE_HOME)
        return AzureBlobStorage.from_env()

    if storage_type in ("GCS", "GCP"):
        _require("google.cloud.storage", "gcp")
        mod = importlib.import_module("supertable.storage.gcp_storage")
        GCSStorage = getattr(mod, "GCSStorage")
        if kwargs:
            return GCSStorage(**kwargs)
        return GCSStorage.from_env()

    raise ValueError(f"Unknown storage type: {storage_type}")