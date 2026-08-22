"""Application-owned local cache for immutable Parquet objects.

The cache sits below query engines: DuckDB and IslandDB can receive the same
ordinary local filename while the catalog continues to use stable raw object
keys for identity.  Presigned/resolved URLs are deliberately never parsed or
stored here.

Only complete Parquet objects are cached.  A backend must expose both
``stat_object`` (including a stable version/etag seal) and a streaming
``download_to_file`` implementation.  Cache failures are an optimisation miss
and leave the original reflection path in place; a downloaded object whose
identity, size, checksum, or Parquet footer is wrong raises
``FileCacheIntegrityError`` and fails closed.
"""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

import pyarrow.parquet as pq

from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.storage.storage_interface import StorageInterface
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V2,
    TOMBSTONE_FORMAT_V3,
)


_CACHE_FORMAT_VERSION = 1
_CACHE_DIR_NAME = "objects-v1"
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_DEFAULT_MAX_BYTES = 5 * 1024 * 1024 * 1024
_DEFAULT_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024
_STALE_RESERVATION_SECONDS = 24 * 60 * 60


class FileCacheError(RuntimeError):
    """Base class for file-cache errors."""


class FileCacheUnavailable(FileCacheError):
    """The cache could not serve an object; callers may use the remote path."""


class FileCacheIntegrityError(FileCacheError):
    """An object failed an identity or integrity check and must not be queried."""


@dataclass
class CacheMetrics:
    """Per-operation or cumulative shared-cache counters.

    ``requested_files`` counts unique physical raw object keys in the supplied
    reflection.  ``localized_files`` includes both cache hits/fills and local
    storage no-copy paths.  ``coverage_ratio`` is therefore the fraction of
    physical objects that an engine can open locally.
    """

    requested_files: int = 0
    localized_files: int = 0
    localized_bytes: int = 0
    fallback_files: int = 0
    hits: int = 0
    misses: int = 0
    downloads: int = 0
    downloaded_bytes: int = 0
    bypasses: int = 0
    local_no_copy: int = 0
    evictions: int = 0
    evicted_bytes: int = 0
    integrity_failures: int = 0
    errors: int = 0

    @property
    def coverage_ratio(self) -> float:
        if self.requested_files <= 0:
            return 1.0
        return self.localized_files / self.requested_files

    def merge(self, other: "CacheMetrics") -> "CacheMetrics":
        for item in fields(self):
            setattr(self, item.name, getattr(self, item.name) + getattr(other, item.name))
        return self

    def copy(self) -> "CacheMetrics":
        return CacheMetrics(**{item.name: getattr(self, item.name) for item in fields(self)})

    def as_dict(self) -> Dict[str, Any]:
        result = {item.name: getattr(self, item.name) for item in fields(self)}
        result["coverage_ratio"] = self.coverage_ratio
        return result

    def to_plan_stats(self) -> Dict[str, Any]:
        """Return stable uppercase keys suitable for ``PlanStats.add_stat``."""
        return {
            "FILE_CACHE_REQUESTED_FILES": self.requested_files,
            "FILE_CACHE_LOCALIZED_FILES": self.localized_files,
            "FILE_CACHE_LOCALIZED_BYTES": self.localized_bytes,
            "FILE_CACHE_FALLBACK_FILES": self.fallback_files,
            "FILE_CACHE_HITS": self.hits,
            "FILE_CACHE_MISSES": self.misses,
            "FILE_CACHE_DOWNLOADS": self.downloads,
            "FILE_CACHE_DOWNLOADED_BYTES": self.downloaded_bytes,
            "FILE_CACHE_BYPASSES": self.bypasses,
            "FILE_CACHE_LOCAL_NO_COPY": self.local_no_copy,
            "FILE_CACHE_EVICTIONS": self.evictions,
            "FILE_CACHE_EVICTED_BYTES": self.evicted_bytes,
            "FILE_CACHE_INTEGRITY_FAILURES": self.integrity_failures,
            "FILE_CACHE_ERRORS": self.errors,
            "FILE_CACHE_COVERAGE_RATIO": self.coverage_ratio,
        }


class _HashingWriter:
    """Transparent binary sink that counts and hashes streamed writes."""

    def __init__(self, raw: BinaryIO, max_bytes: int):
        self.raw = raw
        self.max_bytes = int(max_bytes)
        self.bytes_written = 0
        self.digest = hashlib.sha256()

    def write(self, data) -> int:
        view = memoryview(data)
        if self.bytes_written + len(view) > self.max_bytes:
            raise FileCacheIntegrityError(
                "streaming download exceeded its sealed object size"
            )
        total = 0
        while total < len(view):
            written = self.raw.write(view[total:])
            if written is None:
                written = len(view) - total
            if written <= 0:
                raise OSError("cache sink made no progress while writing")
            self.digest.update(view[total:total + written])
            self.bytes_written += int(written)
            total += int(written)
        return total

    def __getattr__(self, name: str):
        return getattr(self.raw, name)


class _Lease:
    """A shared flock preventing eviction while an engine scans a cache file."""

    __slots__ = ("path", "_fd", "_closed")

    def __init__(self, path: str, fd: int):
        self.path = path
        self._fd = fd
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)

    def __enter__(self) -> str:
        return self.path

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


@dataclass
class _Resolution:
    path: Optional[str]
    lease: Optional[_Lease]
    metrics: CacheMetrics


@dataclass(frozen=True)
class _EntryPaths:
    directory: str
    data: str
    manifest: str
    access: str
    lock: str
    identity_hash: str


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _secret_fingerprint(values: List[object]) -> str:
    """Hash length-framed authorization values without retaining a secret."""
    digest = hashlib.sha256()
    for value in values:
        raw = b"" if value is None else str(value).encode("utf-8")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _metadata_size(metadata: object) -> int:
    try:
        size = int(getattr(metadata, "size"))
    except Exception as exc:
        raise FileCacheUnavailable("storage metadata has no valid object size") from exc
    if size < 0:
        raise FileCacheUnavailable("storage metadata reported a negative object size")
    return size


def _metadata_identity_token(metadata: object) -> Optional[str]:
    method = getattr(metadata, "identity_token", None)
    if callable(method):
        token = method()
        if token:
            return str(token)

    version = str(getattr(metadata, "version", "") or "")
    etag = str(getattr(metadata, "etag", "") or "")
    if version:
        return _canonical_json({"version": version})
    if etag:
        return _canonical_json({"etag": etag})
    return None


def _metadata_checksum(metadata: object) -> str:
    checksum = str(getattr(metadata, "checksum_sha256", "") or "").lower()
    return checksum if _HEX_SHA256.fullmatch(checksum) else ""


class FileCache:
    """Shared, bounded local cache of complete immutable Parquet objects.

    Parameters are explicit so tests and benchmarks can isolate each run.
    When ``root`` is omitted, the existing DuckDB external-cache location is
    reused (or ``SUPERTABLE_HOME/duckdb_cache`` when that setting is empty).

    Public API::

        cache = FileCache(storage, organization, root=..., max_bytes=..., ttl=...)
        localized, metrics = cache.localize_reflection(reflection, populate=True)
        metrics = cache.coverage(reflection)
        with cache.localized(reflection, populate=True) as (localized, metrics):
            run_query(localized)  # eviction leases remain held through the query
    """

    def __init__(
        self,
        storage,
        organization: str,
        root: Optional[str] = None,
        max_bytes: int = _DEFAULT_MAX_BYTES,
        ttl: float = _DEFAULT_TTL_SECONDS,
        workers: Optional[int] = None,
        *,
        chunk_bytes: int = _DEFAULT_CHUNK_BYTES,
    ) -> None:
        self.storage = storage
        self.organization = str(organization or "")
        configured_root = root or settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
        if not configured_root:
            configured_root = os.path.join(get_app_home(), "duckdb_cache")
        self.root = os.path.realpath(
            os.path.abspath(os.path.expanduser(str(configured_root)))
        )
        self.cache_root = os.path.join(self.root, _CACHE_DIR_NAME)
        self.max_bytes = max(0, int(max_bytes))
        self.ttl = max(0.0, float(ttl))
        self.workers = max(1, int(workers or 4))
        self.chunk_bytes = max(64 * 1024, int(chunk_bytes))
        self._metrics = CacheMetrics()
        self._metrics_lock = threading.Lock()

        namespace = self._storage_namespace()
        self._organization_hash = _sha256_text(self.organization)
        self._storage_hash = _sha256_text(_canonical_json(namespace))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def metrics(self) -> CacheMetrics:
        """Return a thread-safe snapshot of cumulative counters."""
        with self._metrics_lock:
            return self._metrics.copy()

    def reset_metrics(self) -> None:
        with self._metrics_lock:
            self._metrics = CacheMetrics()

    def localize_reflection(
        self, reflection, populate: bool = True,
    ) -> Tuple[Any, CacheMetrics]:
        """Return a deep-cloned reflection whose available objects are local.

        This convenience method releases cache leases before returning.  Engine
        execution should prefer :meth:`localized`, which holds leases until the
        query and result fetch complete.
        """
        cloned, metrics, leases = self._localize(reflection, populate=populate)
        for lease in leases:
            lease.close()
        return cloned, metrics

    @contextmanager
    def localized(
        self,
        reflection,
        populate: bool = True,
        *,
        tolerate_corrupt_hits: bool = False,
    ) -> Iterator[Tuple[Any, CacheMetrics]]:
        """Localize a reflection and hold eviction leases for the context."""
        cloned, metrics, leases = self._localize(
            reflection,
            populate=populate,
            tolerate_corrupt_hits=tolerate_corrupt_hits,
        )
        try:
            yield cloned, metrics
        finally:
            for lease in leases:
                lease.close()

    def coverage(self, reflection) -> CacheMetrics:
        """Report hit/no-copy coverage without downloading or cloning data."""
        _cloned, metrics, leases = self._localize(
            reflection, populate=False, clone_reflection=False,
        )
        for lease in leases:
            lease.close()
        return metrics

    def can_populate_all(self, reflection) -> bool:
        """Cheap conservative admission check for an explicit Island query.

        It uses snapshot-declared object sizes and the hard per-cache byte cap.
        The real reservation/lease logic remains authoritative; this preflight
        prevents predictably partial multi-object fills such as two 8 GiB files
        under a 10 GiB cap.
        """
        if self._is_local_storage():
            return True
        sizes: Dict[str, int] = {}
        for snapshot in getattr(reflection, "supers", ()) or ():
            keys = list(getattr(snapshot, "resource_keys", ()) or ())
            declared = list(getattr(snapshot, "resource_sizes", ()) or ())
            if len(keys) != len(declared):
                return False
            for key, size in zip(keys, declared):
                try:
                    parsed = int(size)
                except (TypeError, ValueError):
                    return False
                if parsed <= 0:
                    return False
                sizes[str(key)] = max(sizes.get(str(key), 0), parsed)
        # Tombstone sizes are not yet snapshot-pinned. They are bounded by the
        # writer threshold, but an active remote DV still needs actual reserve
        # admission during localization.
        return sum(sizes.values()) <= self.max_bytes

    @property
    def source_is_local(self) -> bool:
        """Whether localization would only restat already-local source files."""
        return self._is_local_storage()

    def prune(self) -> CacheMetrics:
        """Apply idle-TTL and byte-cap eviction immediately."""
        metrics = CacheMetrics()
        if not os.path.isdir(self.cache_root):
            return metrics
        try:
            self._ensure_cache_root()
            with self._global_lock():
                metrics.merge(self._reclaim_orphans())
                self._clean_stale_reservations()
                entries = self._scan_entries()
                now_ns = time.time_ns()
                total = sum(item[1] for item in entries)
                ordered = sorted(entries, key=lambda item: item[2])
                for manifest, size, accessed_ns in ordered:
                    expired = (
                        self.ttl > 0
                        and now_ns - accessed_ns > int(self.ttl * 1_000_000_000)
                    )
                    over = total > self.max_bytes
                    if not expired and not over:
                        continue
                    if self._evict_manifest(manifest):
                        total -= size
                        metrics.evictions += 1
                        metrics.evicted_bytes += size
        except (OSError, ValueError, FileCacheUnavailable):
            metrics.errors += 1
        self._merge_cumulative(metrics)
        return metrics

    # ------------------------------------------------------------------
    # Reflection localization
    # ------------------------------------------------------------------

    def _localize(
        self,
        reflection,
        *,
        populate: bool,
        clone_reflection: bool = True,
        tolerate_corrupt_hits: bool = False,
    ) -> Tuple[Any, CacheMetrics, List[_Lease]]:
        # A cold hit-only DuckDB lookup should be nearly free.  Delay the deep
        # clone until at least one path can actually be replaced; otherwise a
        # miss across thousands of files would copy the entire Reflection for
        # no behavioral change.
        target = reflection
        metrics = CacheMetrics()
        leases: List[_Lease] = []

        raw_keys: Dict[str, None] = {}
        declared_sizes: Dict[str, Optional[int]] = {}
        declared_checksums: Dict[str, Optional[str]] = {}
        invalid_keys = set()
        malformed = 0
        for sup in getattr(reflection, "supers", ()) or ():
            files = list(getattr(sup, "files", ()) or ())
            keys = list(getattr(sup, "resource_keys", ()) or ())
            sizes = list(getattr(sup, "resource_sizes", ()) or ())
            if len(files) != len(keys) or (sizes and len(sizes) != len(keys)):
                # No URL reverse parsing: an unpaired path is ineligible.
                malformed += max(len(files), len(keys))
                continue
            for index, key in enumerate(keys):
                normalized = str(key)
                declared: Optional[int] = None
                if sizes:
                    try:
                        declared = int(sizes[index])
                    except (TypeError, ValueError):
                        malformed += 1
                        invalid_keys.add(normalized)
                        raw_keys.pop(normalized, None)
                        declared_sizes.pop(normalized, None)
                        declared_checksums.pop(normalized, None)
                        continue
                    if declared < 0:
                        malformed += 1
                        invalid_keys.add(normalized)
                        raw_keys.pop(normalized, None)
                        declared_sizes.pop(normalized, None)
                        declared_checksums.pop(normalized, None)
                        continue
                    if declared == 0:
                        declared = None
                if normalized in invalid_keys:
                    continue
                prior = declared_sizes.get(normalized)
                if prior is not None and declared is not None and prior != declared:
                    malformed += 1
                    invalid_keys.add(normalized)
                    raw_keys.pop(normalized, None)
                    declared_sizes.pop(normalized, None)
                    declared_checksums.pop(normalized, None)
                    continue
                raw_keys.setdefault(normalized, None)
                declared_sizes[normalized] = declared if prior is None else prior

        for tombstone in (getattr(reflection, "tombstone_views", {}) or {}).values():
            tombstone_format = getattr(tombstone, "tombstone_format", None)
            if tombstone_format == TOMBSTONE_FORMAT_V2:
                # The JSON root is small metadata already loaded and validated
                # through the storage SDK. Only immutable Parquet segments
                # belong in the shared whole-file cache.
                segments = getattr(tombstone, "segments", ())
                if not isinstance(segments, tuple) or not segments:
                    malformed += 1
                    continue
                for segment in segments:
                    path = getattr(segment, "tombstone_path", None)
                    key = getattr(segment, "cache_key", None)
                    declared = getattr(segment, "file_size", None)
                    if (
                        not path or not key
                        or not isinstance(declared, int)
                        or isinstance(declared, bool)
                        or declared <= 0
                    ):
                        malformed += 1
                        continue
                    normalized = str(key)
                    if normalized in invalid_keys:
                        continue
                    prior = declared_sizes.get(normalized)
                    if prior is not None and prior != declared:
                        malformed += 1
                        invalid_keys.add(normalized)
                        raw_keys.pop(normalized, None)
                        declared_sizes.pop(normalized, None)
                        declared_checksums.pop(normalized, None)
                        continue
                    raw_keys.setdefault(normalized, None)
                    declared_sizes[normalized] = int(declared)
                continue

            path = getattr(tombstone, "tombstone_path", None)
            key = getattr(tombstone, "cache_key", None)
            if tombstone_format == TOMBSTONE_FORMAT_V3 and (
                getattr(tombstone, "segments", ()) != ()
                or not isinstance(key, str)
                or not key.endswith(".parquet")
            ):
                malformed += 1
                continue
            if path and key:
                # Keep the logical key as the cache/artifact identity. On a
                # hit ``tombstone_path`` is replaced below with a local file,
                # while v3 readers still receive this key as ``artifact_key``.
                normalized = str(key)
                if normalized not in invalid_keys:
                    if tombstone_format == TOMBSTONE_FORMAT_V3:
                        checksum = getattr(tombstone, "tombstone_digest", None)
                        if (
                            not isinstance(checksum, str)
                            or _HEX_SHA256.fullmatch(checksum) is None
                        ):
                            malformed += 1
                            invalid_keys.add(normalized)
                            raw_keys.pop(normalized, None)
                            declared_sizes.pop(normalized, None)
                            declared_checksums.pop(normalized, None)
                            continue
                        prior_checksum = declared_checksums.get(normalized)
                        if (
                            prior_checksum is not None
                            and prior_checksum != checksum
                        ):
                            malformed += 1
                            invalid_keys.add(normalized)
                            raw_keys.pop(normalized, None)
                            declared_sizes.pop(normalized, None)
                            declared_checksums.pop(normalized, None)
                            continue
                        declared_checksums[normalized] = checksum
                    raw_keys.setdefault(normalized, None)
            elif path:
                malformed += 1

        metrics.requested_files = len(raw_keys) + malformed
        if malformed:
            metrics.fallback_files += malformed
            metrics.bypasses += malformed

        resolutions: Dict[str, _Resolution] = {}
        try:
            integrity_error: Optional[FileCacheIntegrityError] = None
            if raw_keys:
                with ThreadPoolExecutor(max_workers=min(self.workers, len(raw_keys))) as pool:
                    futures = {
                        pool.submit(
                            self._resolve_one,
                            raw_key,
                            populate,
                            tolerate_corrupt_hits,
                            declared_sizes.get(raw_key),
                            declared_checksums.get(raw_key),
                        ): raw_key
                        for raw_key in raw_keys
                    }
                    for future in as_completed(futures):
                        raw_key = futures[future]
                        try:
                            resolutions[raw_key] = future.result()
                        except FileCacheIntegrityError as exc:
                            # Drain every future so any leases acquired by sibling
                            # workers are visible and can be released below.
                            integrity_error = integrity_error or exc
                        except Exception:
                            unavailable = CacheMetrics(
                                errors=1, bypasses=1, fallback_files=1,
                            )
                            resolutions[raw_key] = _Resolution(
                                None, None, unavailable,
                            )
            if integrity_error is not None:
                raise integrity_error
        except FileCacheIntegrityError:
            metrics.integrity_failures += 1
            for resolution in resolutions.values():
                if resolution.lease:
                    resolution.lease.close()
            self._merge_cumulative(metrics)
            raise
        except Exception:
            # A worker/runtime failure is a cache availability problem.  Keep
            # every original remote path instead of making reads depend on cache.
            for resolution in resolutions.values():
                if resolution.lease:
                    resolution.lease.close()
            resolutions.clear()
            metrics.errors += len(raw_keys)
            metrics.bypasses += len(raw_keys)
            metrics.fallback_files += len(raw_keys)

        for resolution in resolutions.values():
            metrics.merge(resolution.metrics)
            if resolution.lease:
                leases.append(resolution.lease)

        if clone_reflection and any(
            resolution.path for resolution in resolutions.values()
        ):
            target = copy.deepcopy(reflection)
            for sup in getattr(target, "supers", ()) or ():
                files = list(getattr(sup, "files", ()) or ())
                keys = list(getattr(sup, "resource_keys", ()) or ())
                if len(files) != len(keys):
                    continue
                sup.files = [
                    resolutions.get(str(key), _Resolution(None, None, CacheMetrics())).path
                    or original
                    for original, key in zip(files, keys)
                ]

            for tombstone in (getattr(target, "tombstone_views", {}) or {}).values():
                if (
                    getattr(tombstone, "tombstone_format", None)
                    == TOMBSTONE_FORMAT_V2
                ):
                    localized_segments = []
                    for segment in getattr(tombstone, "segments", ()) or ():
                        resolution = resolutions.get(str(segment.cache_key))
                        localized_segments.append(
                            replace(
                                segment,
                                tombstone_path=(
                                    resolution.path
                                    if resolution and resolution.path
                                    else segment.tombstone_path
                                ),
                            )
                        )
                    tombstone.segments = tuple(localized_segments)
                    continue
                key = getattr(tombstone, "cache_key", None)
                if key:
                    resolution = resolutions.get(str(key))
                    if resolution and resolution.path:
                        tombstone.tombstone_path = resolution.path

        self._merge_cumulative(metrics)
        return target, metrics, leases

    def _resolve_one(
        self,
        raw_key: str,
        populate: bool,
        tolerate_corrupt_hits: bool = False,
        declared_size: Optional[int] = None,
        declared_checksum: Optional[str] = None,
    ) -> _Resolution:
        metrics = CacheMetrics()
        if not raw_key or "://" in raw_key:
            metrics.bypasses = metrics.fallback_files = 1
            return _Resolution(None, None, metrics)

        if self._is_local_storage():
            try:
                # Resolve the raw catalog key, never a resolved/file URL.
                path = os.path.realpath(os.path.abspath(raw_key))
                info = os.stat(path, follow_symlinks=True)
                if not stat.S_ISREG(info.st_mode):
                    raise OSError("local parquet path is not a regular file")
                if declared_size is not None and (
                    not isinstance(declared_size, int)
                    or isinstance(declared_size, bool)
                    or int(info.st_size) != declared_size
                ):
                    raise FileCacheIntegrityError(
                        "snapshot-declared object size does not match the "
                        "local immutable object"
                    )
                metrics.localized_files = metrics.local_no_copy = 1
                metrics.localized_bytes = int(info.st_size)
                return _Resolution(path, None, metrics)
            except OSError:
                metrics.errors = metrics.bypasses = metrics.fallback_files = 1
                return _Resolution(None, None, metrics)

        try:
            # Hit-only DuckDB/cache-coverage probes must not turn every cold
            # object into a remote HEAD.  The key directory is independent of
            # the provider version seal, so its absence proves there cannot be
            # a hit.  Once it exists, stat_object remains mandatory to select
            # and verify the current version-specific entry.
            if not populate and not self._cache_key_directory_exists(raw_key):
                metrics.misses = metrics.bypasses = metrics.fallback_files = 1
                return _Resolution(None, None, metrics)
            stat_object = getattr(self.storage, "stat_object", None)
            if not callable(stat_object):
                raise FileCacheUnavailable("storage has no stat_object implementation")
            metadata = stat_object(raw_key)
            if declared_size is not None and _metadata_size(metadata) != declared_size:
                raise FileCacheIntegrityError(
                    "snapshot-declared object size does not match storage metadata"
                )
            paths = self._entry_paths(raw_key, metadata)
            try:
                resolution = self._acquire(
                    paths,
                    raw_key,
                    metadata,
                    populate=populate,
                    declared_checksum=declared_checksum,
                )
            except FileCacheIntegrityError:
                if tolerate_corrupt_hits and not populate:
                    self._discard_corrupt(paths)
                    metrics.integrity_failures = 1
                    metrics.bypasses = 1
                    metrics.fallback_files = 1
                    return _Resolution(None, None, metrics)
                raise
            metrics.merge(resolution.metrics)
            return _Resolution(resolution.path, resolution.lease, metrics)
        except FileCacheIntegrityError:
            raise
        except Exception:
            metrics.errors = metrics.bypasses = metrics.fallback_files = 1
            return _Resolution(None, None, metrics)

    # ------------------------------------------------------------------
    # Entry acquire/fill
    # ------------------------------------------------------------------

    def _acquire(
        self,
        paths: _EntryPaths,
        raw_key: str,
        metadata: object,
        *,
        populate: bool,
        declared_checksum: Optional[str] = None,
    ) -> _Resolution:
        metrics = CacheMetrics()
        size = _metadata_size(metadata)

        self._validate_existing_directory_chain(paths.directory)
        hit = self._open_committed(
            paths, metadata, declared_checksum=declared_checksum,
        )
        if hit is not None:
            metrics.hits = metrics.localized_files = 1
            metrics.localized_bytes = size
            self._touch(paths.access)
            return _Resolution(paths.data, hit, metrics)

        metrics.misses = 1
        if not populate or self.max_bytes <= 0 or size > self.max_bytes:
            metrics.bypasses = metrics.fallback_files = 1
            return _Resolution(None, None, metrics)

        self._ensure_entry_dir(paths.directory)
        fd = self._open_lock_fd(paths.lock, create=True)
        fd_transferred = False
        reservation: Optional[str] = None
        temp_path: Optional[str] = None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            if self._is_committed(paths):
                self._validate_committed(
                    paths, metadata, declared_checksum=declared_checksum,
                )
                self._touch(paths.access)
                fcntl.flock(fd, fcntl.LOCK_SH)
                metrics.hits = metrics.localized_files = 1
                metrics.localized_bytes = size
                lease = _Lease(paths.data, fd)
                fd_transferred = True
                return _Resolution(paths.data, lease, metrics)

            # A process may have died after replacing the data but before the
            # manifest commit marker, or while writing a uniquely named part.
            # Under the per-entry exclusive lock none of these can belong to a
            # live downloader, so reclaim them before reserving capacity.
            self._remove_incomplete_entry(paths)

            reservation, evicted = self._reserve(size, exclude_directory=paths.directory)
            metrics.merge(evicted)

            tmp_fd, temp_path = tempfile.mkstemp(
                prefix=".download-", suffix=".part", dir=paths.directory,
            )
            os.fchmod(tmp_fd, 0o600)
            hashing: Optional[_HashingWriter] = None
            try:
                with os.fdopen(tmp_fd, "wb", closefd=True) as raw:
                    hashing = _HashingWriter(raw, size)
                    download = getattr(self.storage, "download_to_file", None)
                    if not callable(download):
                        raise FileCacheUnavailable(
                            "storage has no streaming download_to_file implementation"
                        )
                    if (
                        isinstance(self.storage, StorageInterface)
                        and type(self.storage).download_to_file
                        is StorageInterface.download_to_file
                    ):
                        # The compatibility method is intentionally not a
                        # whole-object read_bytes fallback.  Only explicitly
                        # streaming backends are admitted to this cache.
                        raise FileCacheUnavailable(
                            "storage does not override streaming download_to_file"
                        )
                    download(
                        raw_key,
                        hashing,
                        expected=metadata,
                        chunk_size=self.chunk_bytes,
                    )
                    raw.flush()
                    os.fsync(raw.fileno())
            except FileCacheIntegrityError:
                raise
            except Exception as exc:
                # Conditional GETs commonly report an SDK-specific exception.
                # Re-stat after any failed transfer: a changed/disappeared
                # version is a snapshot-integrity failure, while a transient
                # download error against the same sealed object is merely cache
                # unavailability and may safely use the original engine path.
                try:
                    current = self.storage.stat_object(raw_key)
                except FileNotFoundError as missing:
                    raise FileCacheIntegrityError(
                        "remote object disappeared during cache download"
                    ) from missing
                except Exception:
                    raise FileCacheUnavailable(
                        "streaming cache download failed"
                    ) from exc
                if (
                    _metadata_size(current) != size
                    or _metadata_identity_token(current)
                    != _metadata_identity_token(metadata)
                ):
                    raise FileCacheIntegrityError(
                        "remote object version changed during cache download"
                    ) from exc
                raise FileCacheUnavailable(
                    "streaming cache download failed"
                ) from exc

            assert hashing is not None
            actual_size = hashing.bytes_written
            actual_digest = hashing.digest.hexdigest()
            if actual_size != size:
                raise FileCacheIntegrityError(
                    f"downloaded Parquet size mismatch: expected {size}, got {actual_size}"
                )
            expected_checksum = _metadata_checksum(metadata)
            if declared_checksum and actual_digest != declared_checksum:
                raise FileCacheIntegrityError(
                    "downloaded Parquet does not match its snapshot SHA-256"
                )
            if expected_checksum and actual_digest != expected_checksum:
                raise FileCacheIntegrityError("downloaded Parquet SHA-256 mismatch")

            try:
                pq.read_metadata(temp_path)
            except Exception as exc:
                raise FileCacheIntegrityError(
                    "downloaded object has an invalid Parquet footer"
                ) from exc

            # Fence providers that do not support a conditional streaming GET,
            # and defend against implementations that silently ignored it.
            fresh = self.storage.stat_object(raw_key)
            if (
                _metadata_size(fresh) != size
                or _metadata_identity_token(fresh) != _metadata_identity_token(metadata)
            ):
                raise FileCacheIntegrityError(
                    "remote object version changed during cache download"
                )

            os.replace(temp_path, paths.data)
            temp_path = None
            self._fsync_directory(paths.directory)
            data_stat = os.stat(paths.data, follow_symlinks=False)
            manifest = {
                "format_version": _CACHE_FORMAT_VERSION,
                "identity_hash": paths.identity_hash,
                "size": size,
                "sha256": actual_digest,
                # A warm hit should not hash the entire Parquet object again.
                # This local stat seal cheaply detects replacement/in-place
                # mutation; the remote identity and streamed SHA-256 seal the
                # original fill, and PyArrow validates the footer on every hit.
                "data_dev": int(data_stat.st_dev),
                "data_ino": int(data_stat.st_ino),
                "data_mtime_ns": int(data_stat.st_mtime_ns),
                "data_ctime_ns": int(data_stat.st_ctime_ns),
                "created_ns": time.time_ns(),
            }
            self._atomic_json(paths.manifest, manifest)
            self._atomic_touch(paths.access)
            self._fsync_directory(paths.directory)

            metrics.downloads = 1
            metrics.downloaded_bytes = size
            metrics.localized_files = 1
            metrics.localized_bytes = size
            fcntl.flock(fd, fcntl.LOCK_SH)
            lease = _Lease(paths.data, fd)
            fd_transferred = True
            return _Resolution(paths.data, lease, metrics)
        except FileCacheIntegrityError:
            metrics.integrity_failures += 1
            raise
        finally:
            if temp_path:
                self._safe_unlink(temp_path)
            if reservation:
                self._safe_unlink(reservation)
            if not fd_transferred:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)

        # Unreachable; retained for type checkers.
        raise FileCacheUnavailable("cache acquire did not complete")

    def _open_committed(
        self,
        paths: _EntryPaths,
        metadata: object,
        *,
        declared_checksum: Optional[str] = None,
    ) -> Optional[_Lease]:
        if not self._is_committed(paths) or not os.path.exists(paths.lock):
            return None
        fd = self._open_lock_fd(paths.lock, create=False)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH)
            if not self._is_committed(paths):
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                return None
            self._validate_committed(
                paths, metadata, declared_checksum=declared_checksum,
            )
            return _Lease(paths.data, fd)
        except Exception:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            raise

    def _discard_corrupt(self, paths: _EntryPaths) -> None:
        """Quarantine an unlocked corrupt hit without ever returning its bytes."""
        if not os.path.isfile(paths.lock) or os.path.islink(paths.lock):
            return
        try:
            fd = self._open_lock_fd(paths.lock, create=False)
        except OSError:
            return
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            for name in ("manifest.json", "data.parquet", "access"):
                path = os.path.join(paths.directory, name)
                if os.path.islink(path):
                    continue
                self._safe_unlink(path)
            self._fsync_directory(paths.directory)
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    # ------------------------------------------------------------------
    # Identity and validation
    # ------------------------------------------------------------------

    def _storage_namespace(self) -> Dict[str, str]:
        namespace: Dict[str, str]
        method = getattr(self.storage, "cache_namespace", None)
        inherited_default = (
            isinstance(self.storage, StorageInterface)
            and type(self.storage).cache_namespace
            is StorageInterface.cache_namespace
        )
        if callable(method) and not inherited_default:
            value = method()
            if isinstance(value, dict) and value:
                namespace = {str(k): str(v) for k, v in value.items()}
            else:
                namespace = {}
        else:
            namespace = {}

        if not namespace:
            # Opaque/custom backends get a process-private namespace.  This
            # permits safe tests/adapters without allowing another credential
            # context or a future process to reuse their files.
            cls = self.storage.__class__
            namespace = {
                "backend": f"{cls.__module__}.{cls.__qualname__}",
            }

        if self._is_local_storage():
            return namespace

        # Route/bucket identity is insufficient as an authorization boundary:
        # two principals can address the same key while only one may read it.
        # Reusing the first principal's local bytes would bypass the second
        # principal's storage authorization check. Stable explicit credentials
        # are fingerprinted; opaque refreshable SDK clients are deliberately
        # isolated by object identity (less reuse, never cross-principal data).
        credential_values: List[object] = []
        for name in (
            "_aws_access_key_id", "_aws_secret_access_key",
            "_aws_session_token", "_access_key", "_secret_key",
        ):
            try:
                credential_values.append(getattr(self.storage, name, None))
            except Exception:
                credential_values.append(None)
        if not any(value not in (None, "", b"") for value in credential_values):
            try:
                client = getattr(self.storage, "client")
                credentials = getattr(
                    getattr(client, "_request_signer", None), "_credentials", None,
                )
                credential_values = [
                    getattr(credentials, "access_key", None),
                    getattr(credentials, "secret_key", None),
                    getattr(credentials, "token", None),
                ]
            except Exception:
                credential_values = []
        if any(value not in (None, "", b"") for value in credential_values):
            namespace["auth_scope"] = _secret_fingerprint(credential_values)
            return namespace

        for name in ("client", "svc"):
            try:
                client = getattr(self.storage, name)
            except Exception:
                continue
            if client is not None:
                namespace["auth_scope"] = _sha256_text(
                    f"opaque-client:{id(client)}"
                )
                return namespace
        namespace["auth_scope"] = _sha256_text(
            f"opaque-storage:{id(self.storage)}"
        )
        return namespace

    def _is_local_storage(self) -> bool:
        method = getattr(self.storage, "is_local_storage", None)
        if callable(method):
            try:
                return bool(method())
            except Exception:
                return False
        cls = self.storage.__class__
        return (
            cls.__module__ == "supertable.storage.local_storage"
            and cls.__qualname__ == "LocalStorage"
        )

    def _entry_paths(self, raw_key: str, metadata: object) -> _EntryPaths:
        token = _metadata_identity_token(metadata)
        if not token:
            raise FileCacheUnavailable(
                "storage metadata has no immutable version or etag seal"
            )
        size = _metadata_size(metadata)
        identity = _canonical_json({
            "format": _CACHE_FORMAT_VERSION,
            "organization": self.organization,
            "storage": self._storage_hash,
            "raw_key": raw_key,
            "object_version": token,
            "size": size,
        })
        identity_hash = _sha256_text(identity)
        key_hash = _sha256_text(raw_key)
        version_hash = _sha256_text(token + "\0" + str(size))
        directory = os.path.join(
            self.cache_root,
            self._organization_hash,
            self._storage_hash,
            key_hash,
            version_hash,
        )
        return _EntryPaths(
            directory=directory,
            data=os.path.join(directory, "data.parquet"),
            manifest=os.path.join(directory, "manifest.json"),
            access=os.path.join(directory, "access"),
            lock=os.path.join(directory, "entry.lock"),
            identity_hash=identity_hash,
        )

    def _cache_key_directory(self, raw_key: str) -> str:
        return os.path.join(
            self.cache_root,
            self._organization_hash,
            self._storage_hash,
            _sha256_text(raw_key),
        )

    def _cache_key_directory_exists(self, raw_key: str) -> bool:
        directory = self._cache_key_directory(raw_key)
        self._validate_existing_directory_chain(directory)
        if not os.path.isdir(directory):
            return False
        # Eviction intentionally leaves lock inodes in place so a thread that
        # already opened one cannot race a replacement lock.  Only a committed
        # manifest proves useful cache history and justifies a provider HEAD.
        try:
            with os.scandir(directory) as versions:
                for version in versions:
                    if not (
                        _HEX_SHA256.fullmatch(version.name)
                        and version.is_dir(follow_symlinks=False)
                    ):
                        continue
                    try:
                        manifest = os.stat(
                            os.path.join(version.path, "manifest.json"),
                            follow_symlinks=False,
                        )
                    except OSError:
                        continue
                    if stat.S_ISREG(manifest.st_mode):
                        return True
            return False
        except OSError:
            return False

    def _validate_existing_directory_chain(self, directory: str) -> None:
        """Reject symlink/non-directory cache ancestors without creating them."""
        root = Path(self.cache_root)
        try:
            relative = Path(directory).relative_to(root)
        except ValueError as exc:
            raise FileCacheUnavailable("cache entry escaped its configured root") from exc

        current = root
        for component in ("", *relative.parts):
            if component:
                current = current / component
            try:
                info = os.lstat(current)
            except FileNotFoundError:
                return
            if stat.S_ISLNK(info.st_mode):
                raise FileCacheUnavailable("cache namespace contains a symbolic link")
            if not stat.S_ISDIR(info.st_mode):
                raise FileCacheUnavailable("cache namespace component is not a directory")

    @staticmethod
    def _is_committed(paths: _EntryPaths) -> bool:
        return os.path.isfile(paths.data) and os.path.isfile(paths.manifest)

    def _validate_committed(
        self,
        paths: _EntryPaths,
        metadata: object,
        *,
        declared_checksum: Optional[str] = None,
    ) -> None:
        try:
            if os.path.islink(paths.data) or os.path.islink(paths.manifest):
                raise FileCacheIntegrityError("cache entry contains a symbolic link")
            with open(paths.manifest, "r", encoding="utf-8") as source:
                manifest = json.load(source)
            expected_size = _metadata_size(metadata)
            data_stat = os.stat(paths.data, follow_symlinks=False)
            if (
                manifest.get("format_version") != _CACHE_FORMAT_VERSION
                or manifest.get("identity_hash") != paths.identity_hash
                or manifest.get("size") != expected_size
                or int(data_stat.st_size) != expected_size
                or manifest.get("data_dev") != int(data_stat.st_dev)
                or manifest.get("data_ino") != int(data_stat.st_ino)
                or manifest.get("data_mtime_ns") != int(data_stat.st_mtime_ns)
                or manifest.get("data_ctime_ns") != int(data_stat.st_ctime_ns)
            ):
                raise FileCacheIntegrityError("committed cache entry seal mismatch")
            if (
                declared_checksum is not None
                and manifest.get("sha256") != declared_checksum
            ):
                raise FileCacheIntegrityError(
                    "committed cache entry does not match its snapshot SHA-256"
                )
            pq.read_metadata(paths.data)
        except FileCacheIntegrityError:
            raise
        except Exception as exc:
            raise FileCacheIntegrityError("committed cache entry is corrupt") from exc

    # ------------------------------------------------------------------
    # Capacity, TTL, and LRU
    # ------------------------------------------------------------------

    def _reserve(
        self, incoming_size: int, *, exclude_directory: str,
    ) -> Tuple[str, CacheMetrics]:
        if incoming_size > self.max_bytes:
            raise FileCacheUnavailable("object exceeds the configured cache byte cap")
        self._ensure_cache_root()
        metrics = CacheMetrics()
        with self._global_lock():
            self._clean_stale_reservations()
            metrics.merge(self._reclaim_orphans())
            entries = self._scan_entries()
            reservations = self._reservation_bytes()
            total = sum(item[1] for item in entries)
            now_ns = time.time_ns()

            ordered = sorted(entries, key=lambda item: item[2])
            for manifest, size, accessed_ns in ordered:
                directory = os.path.dirname(manifest)
                if os.path.realpath(directory) == os.path.realpath(exclude_directory):
                    continue
                expired = (
                    self.ttl > 0
                    and now_ns - accessed_ns > int(self.ttl * 1_000_000_000)
                )
                needs_space = total + reservations + incoming_size > self.max_bytes
                if not expired and not needs_space:
                    continue
                if self._evict_manifest(manifest):
                    total -= size
                    metrics.evictions += 1
                    metrics.evicted_bytes += size

            if total + reservations + incoming_size > self.max_bytes:
                raise FileCacheUnavailable(
                    "cache byte cap is occupied by active query leases or downloads"
                )

            reservation_dir = os.path.join(self.cache_root, ".reservations")
            self._ensure_private_dir(reservation_dir)
            reservation = os.path.join(
                reservation_dir,
                f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}.json",
            )
            self._atomic_json(reservation, {
                "size": incoming_size,
                "created_ns": time.time_ns(),
                "pid": os.getpid(),
            })
            return reservation, metrics

    def _version_directories(self) -> List[str]:
        """Enumerate the fixed hash layout without following symlinks."""
        current = [self.cache_root]
        for _depth in range(4):
            following: List[str] = []
            for parent in current:
                try:
                    with os.scandir(parent) as entries:
                        for entry in entries:
                            if (
                                _HEX_SHA256.fullmatch(entry.name)
                                and entry.is_dir(follow_symlinks=False)
                            ):
                                following.append(entry.path)
                except OSError:
                    continue
            current = following
        return current

    def _scan_entries(self) -> List[Tuple[str, int, int]]:
        result: List[Tuple[str, int, int]] = []
        if not os.path.isdir(self.cache_root):
            return result
        for directory in self._version_directories():
            manifest_path = Path(directory) / "manifest.json"
            try:
                if manifest_path.is_symlink():
                    continue
                with manifest_path.open("r", encoding="utf-8") as source:
                    manifest = json.load(source)
                size = int(manifest.get("size", -1))
                data = manifest_path.with_name("data.parquet")
                if size < 0 or not data.is_file() or data.stat().st_size != size:
                    continue
                access = manifest_path.with_name("access")
                accessed_ns = (
                    access.stat().st_mtime_ns
                    if access.exists()
                    else manifest_path.stat().st_mtime_ns
                )
                result.append((str(manifest_path), size, accessed_ns))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
        return result

    def _reclaim_orphans(self) -> CacheMetrics:
        """Remove unlocked partial/corrupt entries left by crashed writers."""
        metrics = CacheMetrics()
        for directory in self._version_directories():
            lock_path = os.path.join(directory, "entry.lock")
            if not os.path.isfile(lock_path) or os.path.islink(lock_path):
                continue
            try:
                fd = self._open_lock_fd(lock_path, create=False)
            except OSError:
                continue
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue

                data_path = os.path.join(directory, "data.parquet")
                manifest_path = os.path.join(directory, "manifest.json")
                committed = False
                try:
                    if (
                        os.path.isfile(data_path)
                        and not os.path.islink(data_path)
                        and os.path.isfile(manifest_path)
                        and not os.path.islink(manifest_path)
                    ):
                        with open(manifest_path, "r", encoding="utf-8") as source:
                            manifest = json.load(source)
                        info = os.stat(data_path, follow_symlinks=False)
                        committed = (
                            manifest.get("format_version") == _CACHE_FORMAT_VERSION
                            and manifest.get("size") == int(info.st_size)
                            and manifest.get("data_dev") == int(info.st_dev)
                            and manifest.get("data_ino") == int(info.st_ino)
                            and manifest.get("data_mtime_ns") == int(info.st_mtime_ns)
                            and manifest.get("data_ctime_ns") == int(info.st_ctime_ns)
                        )
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    committed = False

                targets = list(Path(directory).glob(".download-*.part"))
                if not committed:
                    targets.extend(Path(directory) / name for name in (
                        "data.parquet", "manifest.json", "access",
                    ))
                reclaimed = 0
                removed = False
                for target in targets:
                    try:
                        info = os.lstat(target)
                        if stat.S_ISREG(info.st_mode):
                            reclaimed += int(info.st_size)
                        os.unlink(target)
                        removed = True
                    except FileNotFoundError:
                        continue
                    except OSError:
                        metrics.errors += 1
                if removed:
                    metrics.evictions += 1
                    metrics.evicted_bytes += reclaimed
                    self._fsync_directory(directory)
            finally:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
        return metrics

    def _evict_manifest(self, manifest: str) -> bool:
        directory = os.path.dirname(manifest)
        lock_path = os.path.join(directory, "entry.lock")
        if not os.path.isfile(lock_path) or os.path.islink(lock_path):
            return False
        try:
            fd = self._open_lock_fd(lock_path, create=False)
        except OSError:
            return False
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            for name in ("manifest.json", "data.parquet", "access"):
                path = os.path.join(directory, name)
                if os.path.islink(path):
                    return False
            try:
                self._unlink_if_exists(os.path.join(directory, "data.parquet"))
                self._unlink_if_exists(os.path.join(directory, "access"))
                self._unlink_if_exists(os.path.join(directory, "manifest.json"))
            except OSError:
                return False
            self._fsync_directory(directory)
            return True
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _reservation_bytes(self) -> int:
        directory = os.path.join(self.cache_root, ".reservations")
        total = 0
        if not os.path.isdir(directory):
            return total
        for path in Path(directory).glob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as source:
                    value = json.load(source)
                total += max(0, int(value.get("size", 0)))
            except Exception:
                continue
        return total

    def _clean_stale_reservations(self) -> None:
        directory = os.path.join(self.cache_root, ".reservations")
        if not os.path.isdir(directory):
            return
        cutoff = time.time() - _STALE_RESERVATION_SECONDS
        for path in Path(directory).glob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as source:
                    value = json.load(source)
                pid = int(value.get("pid", 0) or 0)
                alive = False
                if pid > 0:
                    try:
                        os.kill(pid, 0)
                        alive = True
                    except PermissionError:
                        alive = True
                    except ProcessLookupError:
                        pass
                if (pid > 0 and not alive) or (pid <= 0 and path.stat().st_mtime < cutoff):
                    path.unlink()
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue

    # ------------------------------------------------------------------
    # Filesystem primitives
    # ------------------------------------------------------------------

    def _ensure_cache_root(self) -> None:
        if os.path.lexists(self.cache_root) and os.path.islink(self.cache_root):
            raise FileCacheUnavailable("cache root must not be a symbolic link")
        self._ensure_private_dir(self.cache_root)

    def _ensure_entry_dir(self, directory: str) -> None:
        self._ensure_cache_root()
        current = Path(self.cache_root)
        try:
            relative = Path(directory).relative_to(current)
        except ValueError as exc:
            raise FileCacheUnavailable("cache entry escaped its configured root") from exc
        for component in relative.parts:
            current = current / component
            if os.path.lexists(current) and os.path.islink(current):
                raise FileCacheUnavailable(
                    "cache namespace contains a symbolic link"
                )
            self._ensure_private_dir(str(current))

    @staticmethod
    def _ensure_private_dir(directory: str) -> None:
        try:
            existing = os.lstat(directory)
            if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
                raise FileCacheUnavailable(
                    "cache namespace component is not a private directory"
                )
        except FileNotFoundError:
            pass
        os.makedirs(directory, mode=0o700, exist_ok=True)
        try:
            os.chmod(directory, 0o700)
        except OSError as exc:
            try:
                info = os.stat(directory, follow_symlinks=False)
            except OSError:
                raise FileCacheUnavailable(
                    "cache directory permissions could not be secured"
                ) from exc
            if stat.S_IMODE(info.st_mode) & 0o077:
                raise FileCacheUnavailable(
                    "cache directory permissions could not be secured"
                ) from exc
        info = os.stat(directory, follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
            raise FileCacheUnavailable("cache directory is not private")
        if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
            raise FileCacheUnavailable("cache directory is owned by another user")

    @staticmethod
    def _open_lock_fd(path: str, *, create: bool) -> int:
        flags = os.O_RDWR
        if create:
            flags |= os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise FileCacheUnavailable("cache lock is not a regular file")
            os.fchmod(fd, 0o600)
            return fd
        except Exception:
            os.close(fd)
            raise

    @contextmanager
    def _global_lock(self):
        lock_path = os.path.join(self.cache_root, ".cache.lock")
        fd = self._open_lock_fd(lock_path, create=True)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    @staticmethod
    def _atomic_json(path: str, value: Dict[str, Any]) -> None:
        directory = os.path.dirname(path)
        fd, temp_path = tempfile.mkstemp(prefix=".manifest-", dir=directory)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as target:
                json.dump(value, target, sort_keys=True, separators=(",", ":"))
                target.flush()
                os.fsync(target.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    @staticmethod
    def _atomic_touch(path: str) -> None:
        directory = os.path.dirname(path)
        fd, temp_path = tempfile.mkstemp(prefix=".access-", dir=directory)
        try:
            os.fchmod(fd, 0o600)
            os.fsync(fd)
            os.close(fd)
            fd = -1
            os.replace(temp_path, path)
        finally:
            if fd >= 0:
                os.close(fd)
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    @staticmethod
    def _touch(path: str) -> None:
        try:
            os.utime(path, None, follow_symlinks=False)
        except (FileNotFoundError, NotImplementedError):
            pass

    @staticmethod
    def _fsync_directory(directory: str) -> None:
        try:
            fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except OSError:
            pass

    @staticmethod
    def _unlink_if_exists(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _remove_incomplete_entry(self, paths: _EntryPaths) -> None:
        if not self._is_committed(paths):
            self._safe_unlink(paths.data)
            self._safe_unlink(paths.manifest)
            self._safe_unlink(paths.access)
        try:
            partials = Path(paths.directory).glob(".download-*.part")
            for partial in partials:
                self._safe_unlink(str(partial))
        except OSError:
            pass

    def _merge_cumulative(self, metrics: CacheMetrics) -> None:
        with self._metrics_lock:
            self._metrics.merge(metrics)


__all__ = [
    "CacheMetrics",
    "FileCache",
    "FileCacheError",
    "FileCacheIntegrityError",
    "FileCacheUnavailable",
]
