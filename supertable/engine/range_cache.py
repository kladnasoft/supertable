"""Version-sealed persistent cache for exact remote object byte ranges.

Unlike :mod:`supertable.engine.file_cache`, this cache never materializes a
complete object merely to satisfy a narrow read.  It stores the exact missing
intervals requested by a seekable reader and reuses overlapping intervals on
later reads.  This is intended for Parquet footer, row-group and column-chunk
I/O through ``pyarrow``.

Correctness rules are deliberately strict:

* raw catalog object keys, never signed URLs, form cache identity;
* every entry is sealed by organization, storage/auth scope and immutable
  object metadata;
* provider reads carry a version/ETag precondition and must return exactly the
  requested number of bytes;
* a source identity failure never falls back to an unconditioned/current read;
* local cache availability failures may bypass to the same bounded,
  conditional provider read.
"""

from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.engine.file_cache import (
    FileCache,
    _canonical_json,
    _metadata_identity_token,
    _metadata_size,
    _sha256_text,
)
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    StorageInterface,
    validate_range_request,
)


_FORMAT_VERSION = 1
_RANGE_DIR = "ranges-v1"
_CHUNK_NAME = re.compile(r"^[0-9a-f]{16}-[0-9a-f]{16}$")
_DEFAULT_MAX_BYTES = 5 * 1024 * 1024 * 1024
_DEFAULT_TTL_SECONDS = 24 * 60 * 60
_STALE_RESERVATION_SECONDS = 24 * 60 * 60


class RangeCacheError(RuntimeError):
    """Base error for conditional range caching."""


class RangeCacheUnavailable(RangeCacheError):
    """The cache/provider could not serve a range without weakening safety."""


class RangeCacheIntegrityError(RangeCacheError):
    """The addressed source object no longer matches its snapshot seal."""


class _CachedIntervalGone(RangeCacheUnavailable):
    """An interval catalog entry was evicted/corrupt before lease acquisition."""


@dataclass
class RangeCacheMetrics:
    logical_requests: int = 0
    requested_bytes: int = 0
    served_bytes: int = 0
    cache_hit_chunks: int = 0
    cache_hit_bytes: int = 0
    cache_miss_chunks: int = 0
    remote_requests: int = 0
    remote_bytes: int = 0
    fills: int = 0
    bypass_requests: int = 0
    bypass_bytes: int = 0
    corruption_repairs: int = 0
    evictions: int = 0
    evicted_bytes: int = 0
    errors: int = 0

    def merge(self, other: "RangeCacheMetrics") -> "RangeCacheMetrics":
        for item in fields(self):
            setattr(self, item.name, getattr(self, item.name) + getattr(other, item.name))
        return self

    def copy(self) -> "RangeCacheMetrics":
        return RangeCacheMetrics(
            **{item.name: getattr(self, item.name) for item in fields(self)}
        )

    def as_dict(self) -> Dict[str, int]:
        return {item.name: getattr(self, item.name) for item in fields(self)}


@dataclass(frozen=True)
class _ObjectPaths:
    directory: str
    chunks: str
    identity_hash: str


@dataclass(frozen=True)
class _ChunkPaths:
    directory: str
    data: str
    manifest: str
    access: str
    lock: str
    identity_hash: str
    start: int
    length: int


@dataclass(frozen=True)
class _Interval:
    start: int
    end: int
    paths: _ChunkPaths


class CachedRandomAccessFile(io.RawIOBase):
    """Seekable Python file object backed by exact cached object ranges."""

    def __init__(
        self,
        cache: "RangeCache",
        raw_key: str,
        metadata: ObjectMetadata,
        object_paths: _ObjectPaths,
    ) -> None:
        super().__init__()
        self._cache = cache
        self.raw_key = raw_key
        self.metadata = metadata
        self._object_paths = object_paths
        self._position = 0
        self._position_lock = threading.Lock()
        self._metrics = RangeCacheMetrics()

    @property
    def size(self) -> int:
        return int(self.metadata.size)

    @property
    def metrics(self) -> RangeCacheMetrics:
        return self._metrics.copy()

    def readable(self) -> bool:
        return not self.closed

    def seekable(self) -> bool:
        return not self.closed

    def tell(self) -> int:
        self._checkClosed()
        with self._position_lock:
            return self._position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self._checkClosed()
        with self._position_lock:
            if whence == os.SEEK_SET:
                target = int(offset)
            elif whence == os.SEEK_CUR:
                target = self._position + int(offset)
            elif whence == os.SEEK_END:
                target = self.size + int(offset)
            else:
                raise ValueError("invalid whence")
            if target < 0:
                raise ValueError("negative seek position")
            self._position = target
            return target

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        with self._position_lock:
            if self._position >= self.size:
                return b""
            if size is None or int(size) < 0:
                length = self.size - self._position
            else:
                length = min(int(size), self.size - self._position)
            if length <= 0:
                return b""
            payload, metrics = self._cache._read(
                self.raw_key,
                self.metadata,
                self._object_paths,
                self._position,
                length,
            )
            self._position += len(payload)
            self._metrics.merge(metrics)
            return payload

    def readinto(self, target) -> int:
        payload = self.read(len(target))
        target[:len(payload)] = payload
        return len(payload)

    def read_at(self, nbytes: int, offset: int) -> bytes:
        """Read without changing the shared stream position (Arrow-style API)."""
        self._checkClosed()
        offset, nbytes = validate_range_request(offset, nbytes, self.metadata)
        payload, metrics = self._cache._read(
            self.raw_key, self.metadata, self._object_paths, offset, nbytes,
        )
        with self._position_lock:
            self._metrics.merge(metrics)
        return payload


class RangeCache:
    """Persistent, bounded cache of exact versioned object byte intervals."""

    def __init__(
        self,
        storage,
        organization: str,
        root: Optional[str] = None,
        max_bytes: int = _DEFAULT_MAX_BYTES,
        ttl: float = _DEFAULT_TTL_SECONDS,
        *,
        allow_cache_bypass: bool = True,
    ) -> None:
        self.storage = storage
        self.organization = str(organization or "")
        configured_root = root or settings.SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
        if not configured_root:
            configured_root = os.path.join(get_app_home(), "duckdb_cache")
        self.root = os.path.realpath(
            os.path.abspath(os.path.expanduser(str(configured_root)))
        )
        self.cache_root = os.path.join(self.root, _RANGE_DIR)
        self.max_bytes = max(0, int(max_bytes))
        self.ttl = max(0.0, float(ttl))
        self.allow_cache_bypass = bool(allow_cache_bypass)

        # Reuse the whole-file cache's carefully isolated organization,
        # provider and credential/auth namespace calculation. Construction has
        # no filesystem side effect.
        identity = FileCache(
            storage, self.organization, root=self.root, max_bytes=0, workers=1,
        )
        self._organization_hash = identity._organization_hash
        self._storage_hash = identity._storage_hash
        self._source_is_local = identity.source_is_local

        self._metrics = RangeCacheMetrics()
        self._metrics_lock = threading.Lock()
        self._catalog_lock = threading.Lock()
        self._interval_catalog: Dict[str, List[_Interval]] = {}

    def metrics(self) -> RangeCacheMetrics:
        with self._metrics_lock:
            return self._metrics.copy()

    def reset_metrics(self) -> None:
        with self._metrics_lock:
            self._metrics = RangeCacheMetrics()

    def open(
        self,
        raw_key: str,
        *,
        expected: ObjectMetadata | None = None,
    ) -> CachedRandomAccessFile:
        """Open a snapshot-sealed, seekable object without downloading it."""
        raw_key = str(raw_key)
        if not raw_key or "://" in raw_key:
            raise ValueError("range cache requires a raw catalog object key")
        if expected is None:
            try:
                expected = self.storage.stat_object(raw_key)
            except Exception as exc:
                raise RangeCacheUnavailable("could not stat ranged object") from exc
        if not isinstance(expected, ObjectMetadata):
            try:
                expected = ObjectMetadata(
                    size=_metadata_size(expected),
                    version=str(getattr(expected, "version", "") or ""),
                    etag=str(getattr(expected, "etag", "") or ""),
                    last_modified_ns=int(
                        getattr(expected, "last_modified_ns", 0) or 0
                    ),
                    checksum_sha256=str(
                        getattr(expected, "checksum_sha256", "") or ""
                    ),
                )
            except Exception as exc:
                raise RangeCacheUnavailable("invalid ranged object metadata") from exc
        if expected.size < 0:
            raise RangeCacheUnavailable("ranged object has a negative size")
        if expected.size and not expected.identity_token():
            raise RangeCacheUnavailable(
                "ranged object metadata has no stable version/etag seal"
            )
        paths = self._object_paths(raw_key, expected)
        return CachedRandomAccessFile(self, raw_key, expected, paths)

    def read(
        self,
        raw_key: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> Tuple[bytes, RangeCacheMetrics]:
        """Convenience exact-range API returning bytes and operation metrics."""
        reader = self.open(raw_key, expected=expected)
        try:
            payload = reader.read_at(length, offset)
            return payload, reader.metrics
        finally:
            reader.close()

    def prefetch(
        self,
        raw_key: str,
        ranges: Iterator[Tuple[int, int]],
        *,
        expected: ObjectMetadata | None = None,
        workers: int = 4,
        coalesce_gap_bytes: int = 0,
    ) -> RangeCacheMetrics:
        """Populate selected intervals concurrently without materializing them.

        ``ranges`` contains ``(offset, length)`` half-open spans obtained from
        the Parquet footer. Overlaps/adjacent spans are merged. A positive
        ``coalesce_gap_bytes`` is an explicit latency-vs-transfer tradeoff;
        zero (the default) never fetches bytes between disjoint chunks.
        """
        reader = self.open(raw_key, expected=expected)
        try:
            gap = max(0, int(coalesce_gap_bytes))
            normalized: List[Tuple[int, int]] = []
            for offset, length in ranges:
                offset, length = validate_range_request(
                    offset, length, reader.metadata,
                )
                if length:
                    normalized.append((offset, offset + length))
            if not normalized:
                return RangeCacheMetrics()
            normalized.sort()
            merged: List[Tuple[int, int]] = []
            for start, end in normalized:
                if merged and start <= merged[-1][1] + gap:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], end))
                else:
                    merged.append((start, end))

            result = RangeCacheMetrics()
            batch_reservation: Optional[str] = None
            capacity_reserved = False
            if not self._source_is_local and self.max_bytes > 0:
                missing_bytes = self._missing_bytes(
                    reader._object_paths, merged, reader.metadata.size,
                )
                if missing_bytes:
                    try:
                        # Reserve the full requested union, not only currently
                        # missing bytes: admission may evict an otherwise
                        # useful requested interval before worker leases it.
                        # A full reservation guarantees that refilling every
                        # such interval still cannot exceed the hard cap.
                        reserve_bytes = sum(end - start for start, end in merged)
                        batch_reservation, evictions = self._reserve(
                            reserve_bytes, exclude_directory="",
                        )
                        result.merge(evictions)
                        capacity_reserved = True
                    except RangeCacheUnavailable:
                        # Individual reads retain their normal safe bypass and
                        # admission behavior when a conservative batch reserve
                        # cannot be made (for example because most bytes are
                        # held by active leases).
                        batch_reservation = None
            count = min(max(1, int(workers)), len(merged))
            try:
                with ThreadPoolExecutor(max_workers=count) as pool:
                    futures = [
                        pool.submit(
                            self._prefetch_one,
                            reader.raw_key,
                            reader.metadata,
                            reader._object_paths,
                            start,
                            end - start,
                            capacity_reserved,
                        )
                        for start, end in merged
                    ]
                    for future in as_completed(futures):
                        result.merge(future.result())
            finally:
                if batch_reservation:
                    self._safe_unlink(batch_reservation)
            return result
        finally:
            reader.close()

    def _prefetch_one(
        self,
        raw_key: str,
        metadata: ObjectMetadata,
        object_paths: _ObjectPaths,
        offset: int,
        length: int,
        capacity_reserved: bool,
    ) -> RangeCacheMetrics:
        # Do not store byte results in Future objects: a large row-group
        # prefetch should use disk capacity, not retain all compressed chunks
        # in query memory until the executor shuts down.
        _payload, metrics = self._read(
            raw_key,
            metadata,
            object_paths,
            offset,
            length,
            capacity_reserved,
        )
        return metrics

    def prune(self) -> RangeCacheMetrics:
        """Apply idle TTL and byte-cap eviction to unlocked cached intervals."""
        metrics = RangeCacheMetrics()
        if not os.path.isdir(self.cache_root):
            return metrics
        try:
            self._ensure_root()
            with self._global_lock():
                self._clean_stale_reservations()
                entries = sorted(self._scan_entries(), key=lambda item: item[2])
                total = sum(item[1] for item in entries)
                now_ns = time.time_ns()
                for paths, size, accessed_ns in entries:
                    expired = (
                        self.ttl > 0
                        and now_ns - accessed_ns > int(self.ttl * 1_000_000_000)
                    )
                    if not expired and total <= self.max_bytes:
                        continue
                    if self._evict(paths):
                        total -= size
                        metrics.evictions += 1
                        metrics.evicted_bytes += size
        except Exception:
            metrics.errors += 1
        self._merge_metrics(metrics)
        return metrics

    # ------------------------------------------------------------------
    # Read planning and exact missing-range fills
    # ------------------------------------------------------------------

    def _read(
        self,
        raw_key: str,
        metadata: ObjectMetadata,
        object_paths: _ObjectPaths,
        offset: int,
        length: int,
        capacity_reserved: bool = False,
    ) -> Tuple[bytes, RangeCacheMetrics]:
        offset, length = validate_range_request(offset, length, metadata)
        metrics = RangeCacheMetrics(
            logical_requests=1,
            requested_bytes=length,
        )
        if length == 0:
            self._merge_metrics(metrics)
            return b"", metrics

        if self._source_is_local or self.max_bytes <= 0:
            payload = self._direct_read(raw_key, metadata, offset, length, metrics)
            metrics.served_bytes = len(payload)
            self._merge_metrics(metrics)
            return payload, metrics

        try:
            intervals = [
                item for item in self._intervals_for(object_paths)
                if 0 <= item.start < item.end <= metadata.size
            ]
            pieces: List[bytes] = []
            cursor = offset
            end = offset + length
            while cursor < end:
                covering = [item for item in intervals if item.start <= cursor < item.end]
                if covering:
                    chosen = max(covering, key=lambda item: item.end)
                    try:
                        chunk, was_hit = self._acquire_chunk(
                            raw_key,
                            metadata,
                            chosen.paths,
                            metrics,
                            populate_if_missing=False,
                            capacity_reserved=capacity_reserved,
                        )
                    except _CachedIntervalGone:
                        intervals.remove(chosen)
                        self._forget_interval(object_paths, chosen)
                        continue
                    consume_end = min(end, chosen.end)
                    begin = cursor - chosen.start
                    pieces.append(chunk[begin:begin + consume_end - cursor])
                    if was_hit:
                        metrics.cache_hit_chunks += 1
                        metrics.cache_hit_bytes += consume_end - cursor
                    else:
                        metrics.cache_miss_chunks += 1
                    cursor = consume_end
                    continue

                following = [item.start for item in intervals if item.start > cursor]
                gap_end = min(end, min(following) if following else end)
                paths = self._chunk_paths(
                    object_paths, cursor, gap_end - cursor,
                )
                chunk, was_hit = self._acquire_chunk(
                    raw_key, metadata, paths, metrics,
                    capacity_reserved=capacity_reserved,
                )
                pieces.append(chunk)
                if was_hit:
                    metrics.cache_hit_chunks += 1
                    metrics.cache_hit_bytes += len(chunk)
                else:
                    metrics.cache_miss_chunks += 1
                intervals.append(_Interval(cursor, gap_end, paths))
                self._remember_interval(
                    object_paths, _Interval(cursor, gap_end, paths),
                )
                cursor = gap_end

            payload = b"".join(pieces)
            if len(payload) != length:
                raise RangeCacheIntegrityError("cached range assembly was incomplete")
        except RangeCacheIntegrityError:
            self._merge_metrics(metrics)
            raise
        except Exception as exc:
            if not self.allow_cache_bypass:
                metrics.errors += 1
                self._merge_metrics(metrics)
                if isinstance(exc, RangeCacheError):
                    raise
                raise RangeCacheUnavailable("range cache could not serve bytes") from exc
            # Cache filesystem/capacity problems may bypass, but the fallback
            # remains the exact same conditional provider range.
            metrics.errors += 1
            payload = self._direct_read(raw_key, metadata, offset, length, metrics)

        metrics.served_bytes = len(payload)
        self._merge_metrics(metrics)
        return payload, metrics

    def _direct_read(
        self,
        raw_key: str,
        metadata: ObjectMetadata,
        offset: int,
        length: int,
        metrics: RangeCacheMetrics,
    ) -> bytes:
        payload = self._provider_read(raw_key, metadata, offset, length)
        metrics.remote_requests += 1
        metrics.remote_bytes += length
        metrics.bypass_requests += 1
        metrics.bypass_bytes += length
        return payload

    def _provider_read(
        self,
        raw_key: str,
        metadata: ObjectMetadata,
        offset: int,
        length: int,
    ) -> bytes:
        method = getattr(self.storage, "read_range", None)
        if not callable(method):
            raise RangeCacheUnavailable("storage does not implement bounded range reads")
        if (
            isinstance(self.storage, StorageInterface)
            and type(self.storage).read_range is StorageInterface.read_range
        ):
            raise RangeCacheUnavailable("storage does not override bounded read_range")
        try:
            payload = method(raw_key, offset, length, expected=metadata)
        except ObjectIdentityMismatch as exc:
            raise RangeCacheIntegrityError(
                "source object identity changed during conditional range read"
            ) from exc
        except (ValueError, FileNotFoundError) as exc:
            # A disappeared/shrunk object cannot be replaced with whatever now
            # occupies the key; this snapshot is no longer safely readable.
            raise RangeCacheIntegrityError(
                "source object no longer satisfies its snapshot range"
            ) from exc
        except NotImplementedError as exc:
            raise RangeCacheUnavailable("bounded provider read unavailable") from exc
        except Exception as exc:
            # Separate a version race from transient provider availability.
            try:
                current = self.storage.stat_object(raw_key)
            except FileNotFoundError as missing:
                raise RangeCacheIntegrityError("source object disappeared") from missing
            except Exception:
                raise RangeCacheUnavailable("bounded provider read failed") from exc
            if (
                _metadata_size(current) != metadata.size
                or _metadata_identity_token(current)
                != _metadata_identity_token(metadata)
            ):
                raise RangeCacheIntegrityError("source object identity changed") from exc
            raise RangeCacheUnavailable("bounded provider read failed") from exc
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise RangeCacheUnavailable("bounded provider read returned non-bytes")
        payload = bytes(payload)
        if len(payload) != length:
            raise RangeCacheIntegrityError(
                f"conditional range length mismatch: expected {length}, got {len(payload)}"
            )
        return payload

    # ------------------------------------------------------------------
    # Chunk acquisition, publication and validation
    # ------------------------------------------------------------------

    def _acquire_chunk(
        self,
        raw_key: str,
        metadata: ObjectMetadata,
        paths: _ChunkPaths,
        metrics: RangeCacheMetrics,
        *,
        populate_if_missing: bool = True,
        capacity_reserved: bool = False,
    ) -> Tuple[bytes, bool]:
        self._validate_existing_chain(paths.directory)
        if os.path.isfile(paths.lock) and not os.path.islink(paths.lock):
            fd = self._open_lock(paths.lock, create=False)
            try:
                fcntl.flock(fd, fcntl.LOCK_SH)
                try:
                    payload = self._read_committed(paths)
                except RangeCacheIntegrityError:
                    payload = None
                if payload is not None:
                    self._touch(paths.access)
                    return payload, True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

        self._ensure_private_chain(paths.directory)
        fd = self._open_lock(paths.lock, create=True)
        reservation: Optional[str] = None
        temp_path: Optional[str] = None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                payload = self._read_committed(paths)
            except RangeCacheIntegrityError:
                metrics.corruption_repairs += 1
                self._remove_chunk_files(paths)
                payload = None
            if payload is not None:
                self._touch(paths.access)
                return payload, True

            if not populate_if_missing:
                raise _CachedIntervalGone("cached interval disappeared")

            self._remove_chunk_files(paths)
            if not capacity_reserved:
                reservation, evictions = self._reserve(
                    paths.length, exclude_directory=paths.directory,
                )
                metrics.merge(evictions)
            payload = self._provider_read(
                raw_key, metadata, paths.start, paths.length,
            )
            metrics.remote_requests += 1
            metrics.remote_bytes += paths.length

            temp_fd, temp_path = tempfile.mkstemp(
                prefix=".range-", suffix=".part", dir=paths.directory,
            )
            try:
                os.fchmod(temp_fd, 0o600)
                with os.fdopen(temp_fd, "wb", closefd=True) as target:
                    target.write(payload)
                    target.flush()
                    os.fsync(target.fileno())
                os.replace(temp_path, paths.data)
                temp_path = None
                info = os.stat(paths.data, follow_symlinks=False)
                manifest = {
                    "format_version": _FORMAT_VERSION,
                    "identity_hash": paths.identity_hash,
                    "start": paths.start,
                    "length": paths.length,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "data_dev": int(info.st_dev),
                    "data_ino": int(info.st_ino),
                    "data_mtime_ns": int(info.st_mtime_ns),
                    "data_ctime_ns": int(info.st_ctime_ns),
                    "created_ns": time.time_ns(),
                }
                self._atomic_json(paths.manifest, manifest)
                self._atomic_touch(paths.access)
                self._fsync_dir(paths.directory)
            finally:
                if temp_path:
                    self._safe_unlink(temp_path)
            metrics.fills += 1
            return payload, False
        finally:
            if reservation:
                self._safe_unlink(reservation)
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @staticmethod
    def _read_committed(paths: _ChunkPaths) -> Optional[bytes]:
        if not os.path.isfile(paths.data) or not os.path.isfile(paths.manifest):
            return None
        try:
            if os.path.islink(paths.data) or os.path.islink(paths.manifest):
                raise RangeCacheIntegrityError("range entry contains a symbolic link")
            with open(paths.manifest, "r", encoding="utf-8") as source:
                manifest = json.load(source)
            info = os.stat(paths.data, follow_symlinks=False)
            if (
                manifest.get("format_version") != _FORMAT_VERSION
                or manifest.get("identity_hash") != paths.identity_hash
                or manifest.get("start") != paths.start
                or manifest.get("length") != paths.length
                or int(info.st_size) != paths.length
                or manifest.get("data_dev") != int(info.st_dev)
                or manifest.get("data_ino") != int(info.st_ino)
                or manifest.get("data_mtime_ns") != int(info.st_mtime_ns)
                or manifest.get("data_ctime_ns") != int(info.st_ctime_ns)
            ):
                raise RangeCacheIntegrityError("range entry seal mismatch")
            with open(paths.data, "rb") as source:
                payload = source.read()
            if len(payload) != paths.length:
                raise RangeCacheIntegrityError("range entry is short")
            if hashlib.sha256(payload).hexdigest() != manifest.get("sha256"):
                raise RangeCacheIntegrityError("range entry checksum mismatch")
            return payload
        except RangeCacheIntegrityError:
            raise
        except Exception as exc:
            raise RangeCacheIntegrityError("range entry is corrupt") from exc

    def _committed_intervals(self, obj: _ObjectPaths) -> List[_Interval]:
        self._validate_existing_chain(obj.chunks)
        if not os.path.isdir(obj.chunks):
            return []
        result: List[_Interval] = []
        try:
            with os.scandir(obj.chunks) as entries:
                for entry in entries:
                    if not _CHUNK_NAME.fullmatch(entry.name):
                        continue
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    start_hex, length_hex = entry.name.split("-", 1)
                    start, length = int(start_hex, 16), int(length_hex, 16)
                    paths = self._chunk_paths(obj, start, length)
                    if os.path.isfile(paths.data) and os.path.isfile(paths.manifest):
                        result.append(_Interval(start, start + length, paths))
        except OSError as exc:
            raise RangeCacheUnavailable("could not inspect cached ranges") from exc
        return result

    def _missing_bytes(
        self,
        obj: _ObjectPaths,
        requested: List[Tuple[int, int]],
        object_size: int,
    ) -> int:
        """Return the union of requested bytes not covered by committed ranges."""
        cached = [
            item for item in self._intervals_for(obj)
            if 0 <= item.start < item.end <= object_size
        ]
        missing = 0
        for start, end in requested:
            cursor = start
            overlaps = sorted(
                (
                    (max(start, item.start), min(end, item.end))
                    for item in cached
                    if item.end > start and item.start < end
                ),
                key=lambda item: item[0],
            )
            for covered_start, covered_end in overlaps:
                if covered_end <= cursor:
                    continue
                if covered_start > cursor:
                    missing += covered_start - cursor
                cursor = max(cursor, covered_end)
                if cursor >= end:
                    break
            if cursor < end:
                missing += end - cursor
        return missing

    def _intervals_for(self, obj: _ObjectPaths) -> List[_Interval]:
        """Load an object's persistent interval catalog once per process."""
        with self._catalog_lock:
            existing = self._interval_catalog.get(obj.identity_hash)
            if existing is not None:
                existing = [
                    item for item in existing
                    if os.path.isfile(item.paths.data)
                    and os.path.isfile(item.paths.manifest)
                ]
                self._interval_catalog[obj.identity_hash] = existing
                return list(existing)
        discovered = self._committed_intervals(obj)
        with self._catalog_lock:
            current = self._interval_catalog.setdefault(obj.identity_hash, [])
            known = {(item.start, item.end) for item in current}
            for item in discovered:
                if (item.start, item.end) not in known:
                    current.append(item)
            return list(current)

    def _remember_interval(self, obj: _ObjectPaths, interval: _Interval) -> None:
        with self._catalog_lock:
            current = self._interval_catalog.setdefault(obj.identity_hash, [])
            if not any(
                item.start == interval.start and item.end == interval.end
                for item in current
            ):
                current.append(interval)

    def _forget_interval(self, obj: _ObjectPaths, interval: _Interval) -> None:
        with self._catalog_lock:
            current = self._interval_catalog.get(obj.identity_hash, [])
            self._interval_catalog[obj.identity_hash] = [
                item for item in current
                if not (
                    item.start == interval.start
                    and item.end == interval.end
                    and item.paths.directory == interval.paths.directory
                )
            ]

    # ------------------------------------------------------------------
    # Identity paths and capacity management
    # ------------------------------------------------------------------

    def _object_paths(
        self, raw_key: str, metadata: ObjectMetadata,
    ) -> _ObjectPaths:
        token = _metadata_identity_token(metadata)
        if metadata.size and not token:
            raise RangeCacheUnavailable("object metadata has no immutable identity")
        token = token or "empty-object"
        identity = _canonical_json({
            "format": _FORMAT_VERSION,
            "organization": self.organization,
            "storage": self._storage_hash,
            "raw_key": raw_key,
            "object_version": token,
            "size": metadata.size,
        })
        identity_hash = _sha256_text(identity)
        directory = os.path.join(
            self.cache_root,
            self._organization_hash,
            self._storage_hash,
            _sha256_text(raw_key),
            _sha256_text(token + "\0" + str(metadata.size)),
        )
        return _ObjectPaths(
            directory=directory,
            chunks=os.path.join(directory, "chunks"),
            identity_hash=identity_hash,
        )

    @staticmethod
    def _chunk_paths(obj: _ObjectPaths, start: int, length: int) -> _ChunkPaths:
        if start < 0 or length <= 0:
            raise ValueError("cached chunk ranges must be positive")
        name = f"{start:016x}-{length:016x}"
        directory = os.path.join(obj.chunks, name)
        identity_hash = _sha256_text(
            f"{obj.identity_hash}\0{start}\0{length}"
        )
        return _ChunkPaths(
            directory=directory,
            data=os.path.join(directory, "data.bin"),
            manifest=os.path.join(directory, "manifest.json"),
            access=os.path.join(directory, "access"),
            lock=os.path.join(directory, "entry.lock"),
            identity_hash=identity_hash,
            start=start,
            length=length,
        )

    def _reserve(
        self, incoming: int, *, exclude_directory: str,
    ) -> Tuple[str, RangeCacheMetrics]:
        if incoming > self.max_bytes:
            raise RangeCacheUnavailable("range exceeds configured cache byte cap")
        self._ensure_root()
        metrics = RangeCacheMetrics()
        with self._global_lock():
            self._clean_stale_reservations()
            entries = sorted(self._scan_entries(), key=lambda item: item[2])
            reservations = self._reservation_bytes()
            total = sum(item[1] for item in entries)
            now_ns = time.time_ns()
            for paths, size, accessed_ns in entries:
                if os.path.realpath(paths.directory) == os.path.realpath(exclude_directory):
                    continue
                expired = (
                    self.ttl > 0
                    and now_ns - accessed_ns > int(self.ttl * 1_000_000_000)
                )
                needs_space = total + reservations + incoming > self.max_bytes
                if not expired and not needs_space:
                    continue
                if self._evict(paths):
                    total -= size
                    metrics.evictions += 1
                    metrics.evicted_bytes += size
            if total + reservations + incoming > self.max_bytes:
                raise RangeCacheUnavailable(
                    "range cache cap is occupied by active readers/downloads"
                )
            reservation_dir = os.path.join(self.cache_root, ".reservations")
            self._ensure_private_chain(reservation_dir)
            reservation = os.path.join(
                reservation_dir,
                f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}.json",
            )
            self._atomic_json(reservation, {
                "size": incoming,
                "created_ns": time.time_ns(),
                "pid": os.getpid(),
            })
            return reservation, metrics

    def _scan_entries(self) -> List[Tuple[_ChunkPaths, int, int]]:
        result: List[Tuple[_ChunkPaths, int, int]] = []
        if not os.path.isdir(self.cache_root):
            return result
        for root, directories, files in os.walk(self.cache_root, followlinks=False):
            directories[:] = [
                name for name in directories
                if not os.path.islink(os.path.join(root, name))
            ]
            if os.path.basename(root) == ".reservations":
                directories[:] = []
                continue
            if "manifest.json" not in files:
                continue
            name = os.path.basename(root)
            if not _CHUNK_NAME.fullmatch(name):
                continue
            try:
                start_hex, length_hex = name.split("-", 1)
                start, length = int(start_hex, 16), int(length_hex, 16)
                with open(os.path.join(root, "manifest.json"), "r", encoding="utf-8") as src:
                    manifest = json.load(src)
                paths = _ChunkPaths(
                    directory=root,
                    data=os.path.join(root, "data.bin"),
                    manifest=os.path.join(root, "manifest.json"),
                    access=os.path.join(root, "access"),
                    lock=os.path.join(root, "entry.lock"),
                    identity_hash=str(manifest.get("identity_hash") or ""),
                    start=start,
                    length=length,
                )
                info = os.stat(paths.data, follow_symlinks=False)
                accessed = (
                    os.stat(paths.access, follow_symlinks=False).st_mtime_ns
                    if os.path.isfile(paths.access)
                    else os.stat(paths.manifest, follow_symlinks=False).st_mtime_ns
                )
                if int(info.st_size) == length:
                    result.append((paths, length, accessed))
            except Exception:
                continue
        return result

    def _evict(self, paths: _ChunkPaths) -> bool:
        if not os.path.isfile(paths.lock) or os.path.islink(paths.lock):
            return False
        try:
            fd = self._open_lock(paths.lock, create=False)
        except OSError:
            return False
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            for target in (paths.data, paths.access, paths.manifest):
                if os.path.islink(target):
                    return False
            for target in (paths.data, paths.access, paths.manifest):
                self._safe_unlink(target)
            self._fsync_dir(paths.directory)
            return True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _reservation_bytes(self) -> int:
        directory = os.path.join(self.cache_root, ".reservations")
        total = 0
        if not os.path.isdir(directory):
            return total
        for path in Path(directory).glob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as source:
                    total += max(0, int(json.load(source).get("size", 0)))
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
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Safe filesystem primitives
    # ------------------------------------------------------------------

    def _ensure_root(self) -> None:
        self._ensure_private_chain(self.cache_root)

    def _validate_existing_chain(self, directory: str) -> None:
        root = Path(self.cache_root)
        try:
            relative = Path(directory).relative_to(root)
        except ValueError as exc:
            raise RangeCacheUnavailable("range cache path escaped its root") from exc
        current = root
        for component in ("", *relative.parts):
            if component:
                current /= component
            try:
                info = os.lstat(current)
            except FileNotFoundError:
                return
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RangeCacheUnavailable("unsafe range cache directory chain")

    def _ensure_private_chain(self, directory: str) -> None:
        root = Path(self.cache_root)
        try:
            relative = Path(directory).relative_to(root)
        except ValueError as exc:
            raise RangeCacheUnavailable("range cache path escaped its root") from exc
        current = root
        for component in ("", *relative.parts):
            if component:
                current /= component
            try:
                info = os.lstat(current)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise RangeCacheUnavailable("unsafe range cache directory chain")
                created = False
            except FileNotFoundError:
                created = True
                try:
                    if current == root:
                        os.makedirs(current, mode=0o700, exist_ok=True)
                    else:
                        os.mkdir(current, mode=0o700)
                except FileExistsError:
                    # Another range reader may be creating the same immutable
                    # namespace concurrently. Validate its result below.
                    pass
                info = os.lstat(current)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise RangeCacheUnavailable("unsafe range cache directory chain")
            if created:
                os.chmod(current, 0o700)
            info = os.stat(current, follow_symlinks=False)
            if stat.S_IMODE(info.st_mode) & 0o077:
                raise RangeCacheUnavailable("range cache directory is not private")
            if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
                raise RangeCacheUnavailable("range cache directory has another owner")

    @staticmethod
    def _open_lock(path: str, *, create: bool) -> int:
        flags = os.O_RDWR | (os.O_CREAT if create else 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise RangeCacheUnavailable("range lock is not a regular file")
        os.fchmod(fd, 0o600)
        return fd

    def _global_lock(self):
        class _Lock:
            def __init__(inner, outer: "RangeCache"):
                inner.outer = outer
                inner.fd = -1

            def __enter__(inner):
                inner.fd = inner.outer._open_lock(
                    os.path.join(inner.outer.cache_root, ".cache.lock"), create=True,
                )
                fcntl.flock(inner.fd, fcntl.LOCK_EX)

            def __exit__(inner, exc_type, exc, tb):
                fcntl.flock(inner.fd, fcntl.LOCK_UN)
                os.close(inner.fd)

        return _Lock(self)

    @staticmethod
    def _atomic_json(path: str, value: Dict[str, Any]) -> None:
        directory = os.path.dirname(path)
        fd, temporary = tempfile.mkstemp(prefix=".manifest-", dir=directory)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as target:
                json.dump(value, target, sort_keys=True, separators=(",", ":"))
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @staticmethod
    def _atomic_touch(path: str) -> None:
        flags = os.O_WRONLY | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        os.utime(path, None, follow_symlinks=False)

    @staticmethod
    def _touch(path: str) -> None:
        try:
            os.utime(path, None, follow_symlinks=False)
        except (FileNotFoundError, NotImplementedError):
            pass

    @staticmethod
    def _fsync_dir(directory: str) -> None:
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

    def _remove_chunk_files(self, paths: _ChunkPaths) -> None:
        for target in (paths.data, paths.manifest, paths.access):
            self._safe_unlink(target)
        try:
            for partial in Path(paths.directory).glob(".range-*.part"):
                self._safe_unlink(str(partial))
        except OSError:
            pass

    def _merge_metrics(self, metrics: RangeCacheMetrics) -> None:
        with self._metrics_lock:
            self._metrics.merge(metrics)


__all__ = [
    "CachedRandomAccessFile",
    "RangeCache",
    "RangeCacheError",
    "RangeCacheIntegrityError",
    "RangeCacheMetrics",
    "RangeCacheUnavailable",
]
