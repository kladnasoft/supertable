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
import struct
import tempfile
import threading
import time
import uuid
import zlib
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.engine.file_cache import (
    FileCache,
    RANGE_CACHE_SUBDIR,
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
_RANGE_DIR = RANGE_CACHE_SUBDIR
_CHUNK_NAME = re.compile(r"^[0-9a-f]{16}-[0-9a-f]{16}$")
_DEFAULT_MAX_BYTES = 5 * 1024 * 1024 * 1024
# Data objects addressed by this cache are immutable and version-sealed.  Keep
# them until capacity pressure or corruption requires rotation.  A caller that
# explicitly supplies a positive TTL retains the previous idle-expiry policy.
_DEFAULT_TTL_SECONDS = 0
_STALE_RESERVATION_SECONDS = 24 * 60 * 60
_ACCESS_MAGIC = b"STRG"
_ACCESS_VERSION = 1
_ACCESS_BODY = struct.Struct(">4sB3xIQQQ")
_ACCESS_CRC = struct.Struct(">I")
_ACCESS_RECORD_BYTES = _ACCESS_BODY.size + _ACCESS_CRC.size
_FREQUENCY_HALF_LIFE_NS = 60 * 60 * 1_000_000_000
_MAX_FREQUENCY = 65_535
_MIN_REFILL_COST_NS = 1_000_000
_MAX_REFILL_COST_NS = 5 * 60 * 1_000_000_000
_MAX_VALIDATION_PROOFS = 16_384
_MAX_INTERVAL_CATALOG_OBJECTS = 4_096
_MAX_INTERVAL_CATALOG_INTERVALS = 65_536
_MAX_MANIFEST_BYTES = 16 * 1024
_RESERVATION_ACCOUNTING_VERSION = 2
# A new object can require organization/storage/key/version/chunks/chunk
# directories plus its entry lock.  Two additional allocation units cover
# directory growth and filesystem metadata not exposed through ``st_blocks``.
_NEW_ENTRY_NAMESPACE_UNITS = 9


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
    validated_hits: int = 0
    hash_validation_skips: int = 0
    hash_validations: int = 0
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


@dataclass(frozen=True)
class _ValidationProof:
    """One process-local checksum proof for an unchanged committed inode."""

    stat_identity: Tuple[int, int, int, int, int]
    sha256: str


@dataclass(frozen=True)
class _AccessRecord:
    """Fixed-size advisory eviction metadata; never an integrity authority."""

    frequency: int
    decay_epoch_ns: int
    last_access_ns: int
    refill_cost_ns: int


@dataclass(frozen=True)
class _ScannedEntry:
    paths: _ChunkPaths
    # Accounted filesystem bytes reclaimed by removing this chunk directory,
    # including its data, manifest, access record, lock and directory blocks.
    size: int
    payload_size: int
    accessed_ns: int
    access: Optional[_AccessRecord]


class CachedRandomAccessFile(io.RawIOBase):
    """Seekable Python file object backed by exact cached object ranges."""

    def __init__(
        self,
        cache: "RangeCache",
        raw_key: str,
        metadata: ObjectMetadata,
        object_paths: _ObjectPaths,
        metrics_sink: Optional[Callable[[RangeCacheMetrics], None]] = None,
    ) -> None:
        super().__init__()
        self._cache = cache
        self.raw_key = raw_key
        self.metadata = metadata
        self._object_paths = object_paths
        self._position = 0
        self._position_lock = threading.Lock()
        self._metrics = RangeCacheMetrics()
        self._metrics_sink = metrics_sink

    def _record_metrics(self, metrics: RangeCacheMetrics) -> None:
        self._metrics.merge(metrics)
        if self._metrics_sink is not None:
            # Query telemetry is diagnostic and must never make a valid range
            # read fail. The internal sink is lock-protected and non-blocking;
            # this boundary also keeps custom test/adaptor sinks harmless.
            try:
                self._metrics_sink(metrics.copy())
            except Exception:
                pass

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
            self._record_metrics(metrics)
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
            self._record_metrics(metrics)
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
        self._interval_catalog: "OrderedDict[str, List[_Interval]]" = OrderedDict()
        self._interval_catalog_count = 0
        # Persistent manifests remain authoritative.  This process-local index
        # only avoids repeating SHA-256 after a successful fill/validation while
        # the complete data-file stat identity is unchanged.  It intentionally
        # starts empty after every process restart.
        self._validation_lock = threading.Lock()
        self._validation_proofs: "OrderedDict[str, _ValidationProof]" = OrderedDict()

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
        metrics_sink: Optional[Callable[[RangeCacheMetrics], None]] = None,
    ) -> CachedRandomAccessFile:
        """Open a snapshot-sealed, seekable object without downloading it."""
        raw_key = str(raw_key)
        if not raw_key or "://" in raw_key:
            raise ValueError("range cache requires a raw catalog object key")
        if expected is None:
            try:
                expected = self.storage.stat_object(raw_key)
            except Exception:
                raise RangeCacheUnavailable("could not stat ranged object") from None
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
            except Exception:
                raise RangeCacheUnavailable("invalid ranged object metadata") from None
        if expected.size < 0:
            raise RangeCacheUnavailable("ranged object has a negative size")
        if expected.size and not expected.identity_token():
            raise RangeCacheUnavailable(
                "ranged object metadata has no stable version/etag seal"
            )
        paths = self._object_paths(raw_key, expected)
        return CachedRandomAccessFile(
            self, raw_key, expected, paths, metrics_sink=metrics_sink,
        )

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
                            reserve_bytes,
                            exclude_directory="",
                            entry_count=len(merged),
                            include_entry_shell=True,
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
                    result.merge(
                        self._finish_batch_reservation(batch_reservation)
                    )
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
                self._clean_orphans_locked()
                entries, total = self._scan_cache()
                reservations = self._reservation_bytes()
                now_ns = time.time_ns()
                evicted_directories = set()

                # A positive TTL remains an explicit instruction to discard
                # every idle entry, independently from capacity pressure.
                if self.ttl > 0:
                    cutoff_ns = now_ns - int(self.ttl * 1_000_000_000)
                    for entry in sorted(entries, key=lambda item: item.accessed_ns):
                        if entry.accessed_ns >= cutoff_ns:
                            break
                        if self._evict(entry.paths):
                            total -= entry.size
                            evicted_directories.add(entry.paths.directory)
                            metrics.evictions += 1
                            metrics.evicted_bytes += entry.payload_size

                required = max(0, total + reservations - self.max_bytes)
                if required > 0:
                    candidates = (
                        entry for entry in entries
                        if entry.paths.directory not in evicted_directories
                    )
                    for entry in sorted(
                        candidates,
                        key=lambda item: self._eviction_key(item, now_ns),
                    ):
                        if required <= 0:
                            break
                        if self._evict(entry.paths):
                            total -= entry.size
                            required -= entry.size
                            metrics.evictions += 1
                            metrics.evicted_bytes += entry.payload_size
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
                raise RangeCacheUnavailable("range cache could not serve bytes") from None
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
        except ObjectIdentityMismatch:
            raise RangeCacheIntegrityError(
                "source object identity changed during conditional range read"
            ) from None
        except (ValueError, FileNotFoundError):
            # A disappeared/shrunk object cannot be replaced with whatever now
            # occupies the key; this snapshot is no longer safely readable.
            raise RangeCacheIntegrityError(
                "source object no longer satisfies its snapshot range"
            ) from None
        except NotImplementedError:
            raise RangeCacheUnavailable("bounded provider read unavailable") from None
        except Exception:
            # Separate a version race from transient provider availability.
            try:
                current = self.storage.stat_object(raw_key)
            except FileNotFoundError:
                raise RangeCacheIntegrityError("source object disappeared") from None
            except Exception:
                raise RangeCacheUnavailable("bounded provider read failed") from None
            if (
                _metadata_size(current) != metadata.size
                or _metadata_identity_token(current)
                != _metadata_identity_token(metadata)
            ):
                raise RangeCacheIntegrityError("source object identity changed") from None
            raise RangeCacheUnavailable("bounded provider read failed") from None
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
                    payload = self._read_committed(paths, metrics)
                except RangeCacheIntegrityError:
                    payload = None
                if payload is not None:
                    self._record_access(paths)
                    return payload, True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

        if (
            populate_if_missing
            and not capacity_reserved
            and not os.path.exists(self.cache_root)
        ):
            # Avoid creating an over-cap namespace merely to discover that its
            # first entry cannot fit. Two additional units cover the cache root,
            # global lock and reservation namespace after the shell estimate's
            # deliberately conservative directory-growth allowance.
            minimum = self._estimate_publication_bytes(
                paths.length,
                entry_count=1,
                include_entry_shell=True,
            ) + 2 * self._allocation_unit()
            if minimum > self.max_bytes:
                raise RangeCacheUnavailable(
                    "range plus cache metadata exceed configured byte cap"
                )

        self._ensure_private_chain(paths.directory)
        fd = self._open_lock(paths.lock, create=True)
        reservation: Optional[str] = None
        temp_path: Optional[str] = None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                payload = self._read_committed(paths, metrics)
            except RangeCacheIntegrityError:
                metrics.corruption_repairs += 1
                self._remove_chunk_files(paths)
                payload = None
            if payload is not None:
                self._record_access(paths)
                return payload, True

            if not populate_if_missing:
                raise _CachedIntervalGone("cached interval disappeared")

            self._remove_chunk_files(paths)
            if not capacity_reserved:
                reservation, evictions = self._reserve(
                    paths.length,
                    exclude_directory=paths.directory,
                    entry_count=1,
                    include_entry_shell=False,
                )
                metrics.merge(evictions)
            refill_started_ns = time.perf_counter_ns()
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
                payload_sha256 = hashlib.sha256(payload).hexdigest()
                manifest = {
                    "format_version": _FORMAT_VERSION,
                    "identity_hash": paths.identity_hash,
                    "start": paths.start,
                    "length": paths.length,
                    "sha256": payload_sha256,
                    "data_dev": int(info.st_dev),
                    "data_ino": int(info.st_ino),
                    "data_mtime_ns": int(info.st_mtime_ns),
                    "data_ctime_ns": int(info.st_ctime_ns),
                    "created_ns": time.time_ns(),
                }
                self._atomic_json(paths.manifest, manifest)
                self._fsync_dir(paths.directory)
                committed_info = os.stat(paths.data, follow_symlinks=False)
                if self._stat_identity(committed_info) == self._stat_identity(info):
                    self._remember_validation(
                        paths, committed_info, payload_sha256,
                    )
                self._record_access(
                    paths,
                    refill_cost_ns=time.perf_counter_ns() - refill_started_ns,
                )
                admitted = True
                if reservation:
                    admitted = self._finish_reservation(
                        reservation, paths, metrics,
                    )
            finally:
                if temp_path:
                    self._safe_unlink(temp_path)
            if admitted:
                metrics.fills += 1
            else:
                metrics.bypass_requests += 1
                metrics.bypass_bytes += paths.length
            return payload, False
        finally:
            if reservation:
                self._safe_unlink(reservation)
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            if not (
                os.path.isfile(paths.data)
                and os.path.isfile(paths.manifest)
            ):
                self._reclaim_chunk_if_orphan(paths)

    @staticmethod
    def _stat_identity(info: os.stat_result) -> Tuple[int, int, int, int, int]:
        return (
            int(info.st_dev),
            int(info.st_ino),
            int(info.st_size),
            int(info.st_mtime_ns),
            int(info.st_ctime_ns),
        )

    def _remember_validation(
        self,
        paths: _ChunkPaths,
        info: os.stat_result,
        sha256: str,
    ) -> None:
        # The digest is useful as a proof only for the exact committed inode
        # observed by the caller; malformed values must never enter the fast
        # path even through an internal programming error.
        if not (
            isinstance(sha256, str)
            and len(sha256) == 64
            and all(ch in "0123456789abcdef" for ch in sha256)
        ):
            return
        proof = _ValidationProof(self._stat_identity(info), str(sha256))
        with self._validation_lock:
            self._validation_proofs.pop(paths.data, None)
            self._validation_proofs[paths.data] = proof
            while len(self._validation_proofs) > _MAX_VALIDATION_PROOFS:
                self._validation_proofs.popitem(last=False)

    def _forget_validation(self, paths: _ChunkPaths) -> None:
        with self._validation_lock:
            self._validation_proofs.pop(paths.data, None)

    def _read_committed(
        self,
        paths: _ChunkPaths,
        metrics: RangeCacheMetrics,
    ) -> Optional[bytes]:
        if not os.path.isfile(paths.data) or not os.path.isfile(paths.manifest):
            self._forget_validation(paths)
            return None
        data_fd = -1
        try:
            if os.path.islink(paths.data) or os.path.islink(paths.manifest):
                raise RangeCacheIntegrityError("range entry contains a symbolic link")
            manifest = self._load_manifest_bounded(paths.manifest)
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            data_fd = os.open(paths.data, flags)
            info = os.fstat(data_fd)
            if not stat.S_ISREG(info.st_mode):
                raise RangeCacheIntegrityError("range entry data is not a regular file")
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
            expected_sha256 = manifest.get("sha256")
            if not (
                isinstance(expected_sha256, str)
                and len(expected_sha256) == 64
                and all(ch in "0123456789abcdef" for ch in expected_sha256)
            ):
                raise RangeCacheIntegrityError("range entry checksum seal is invalid")

            with os.fdopen(data_fd, "rb", closefd=False) as source:
                payload = source.read()
            if len(payload) != paths.length:
                raise RangeCacheIntegrityError("range entry is short")

            # A mutation concurrent with this read must not be returned merely
            # because the inode matched at open time.  Recheck the complete stat
            # identity on the same open file descriptor after consuming it.
            finished_info = os.fstat(data_fd)
            stat_identity = self._stat_identity(info)
            if self._stat_identity(finished_info) != stat_identity:
                self._forget_validation(paths)
                raise RangeCacheIntegrityError("range entry changed while being read")

            with self._validation_lock:
                proof = self._validation_proofs.get(paths.data)
                if proof is not None:
                    if (
                        proof.stat_identity == stat_identity
                        and proof.sha256 == expected_sha256
                    ):
                        self._validation_proofs.move_to_end(paths.data)
                    else:
                        # A changed inode/seal invalidates the old proof and it
                        # must not consume the bounded memo indefinitely.
                        self._validation_proofs.pop(paths.data, None)
            if (
                proof is not None
                and proof.stat_identity == stat_identity
                and proof.sha256 == expected_sha256
            ):
                metrics.hash_validation_skips += 1
            else:
                metrics.hash_validations += 1
                actual_sha256 = hashlib.sha256(payload).hexdigest()
                if actual_sha256 != expected_sha256:
                    self._forget_validation(paths)
                    raise RangeCacheIntegrityError("range entry checksum mismatch")
                self._remember_validation(paths, finished_info, actual_sha256)
            metrics.validated_hits += 1
            return payload
        except RangeCacheIntegrityError:
            raise
        except Exception:
            raise RangeCacheIntegrityError("range entry is corrupt") from None
        finally:
            if data_fd >= 0:
                os.close(data_fd)

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
        except OSError:
            raise RangeCacheUnavailable("could not inspect cached ranges") from None
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
        """Load an object's persistent intervals into a bounded LRU memo."""
        with self._catalog_lock:
            existing = self._interval_catalog.get(obj.identity_hash)
            if existing is not None:
                live = [
                    item for item in existing
                    if os.path.isfile(item.paths.data)
                    and os.path.isfile(item.paths.manifest)
                ]
                self._interval_catalog_count += len(live) - len(existing)
                if live:
                    self._interval_catalog[obj.identity_hash] = live
                    self._interval_catalog.move_to_end(obj.identity_hash)
                else:
                    self._interval_catalog.pop(obj.identity_hash, None)
                result = list(live)
                self._trim_interval_catalog_locked()
                return result
        discovered = self._committed_intervals(obj)
        with self._catalog_lock:
            current = self._interval_catalog.get(obj.identity_hash)
            if current is None:
                current = []
                self._interval_catalog[obj.identity_hash] = current
            known = {(item.start, item.end) for item in current}
            for item in discovered:
                if (item.start, item.end) not in known:
                    current.append(item)
                    self._interval_catalog_count += 1
            self._interval_catalog.move_to_end(obj.identity_hash)
            result = list(current)
            self._trim_interval_catalog_locked()
            return result

    def _remember_interval(self, obj: _ObjectPaths, interval: _Interval) -> None:
        with self._catalog_lock:
            current = self._interval_catalog.get(obj.identity_hash)
            if current is None:
                current = []
                self._interval_catalog[obj.identity_hash] = current
            if not any(
                item.start == interval.start and item.end == interval.end
                for item in current
            ):
                current.append(interval)
                self._interval_catalog_count += 1
            self._interval_catalog.move_to_end(obj.identity_hash)
            self._trim_interval_catalog_locked()

    def _forget_interval(self, obj: _ObjectPaths, interval: _Interval) -> None:
        with self._catalog_lock:
            current = self._interval_catalog.get(obj.identity_hash)
            if current is None:
                return
            remaining = [
                item for item in current
                if not (
                    item.start == interval.start
                    and item.end == interval.end
                    and item.paths.directory == interval.paths.directory
                )
            ]
            self._interval_catalog_count -= len(current) - len(remaining)
            if remaining:
                self._interval_catalog[obj.identity_hash] = remaining
            else:
                self._interval_catalog.pop(obj.identity_hash, None)

    def _forget_catalog_directory(self, directory: str) -> None:
        """Drop every memoized interval backed by an evicted directory."""
        with self._catalog_lock:
            for identity_hash, current in list(self._interval_catalog.items()):
                remaining = [
                    item for item in current
                    if item.paths.directory != directory
                ]
                self._interval_catalog_count -= len(current) - len(remaining)
                if remaining:
                    self._interval_catalog[identity_hash] = remaining
                else:
                    self._interval_catalog.pop(identity_hash, None)

    def _trim_interval_catalog_locked(self) -> None:
        """Enforce both object-identity and aggregate-interval LRU limits."""
        object_limit = max(0, int(_MAX_INTERVAL_CATALOG_OBJECTS))
        interval_limit = max(0, int(_MAX_INTERVAL_CATALOG_INTERVALS))
        while self._interval_catalog and (
            len(self._interval_catalog) > object_limit
            or self._interval_catalog_count > interval_limit
        ):
            _identity_hash, intervals = self._interval_catalog.popitem(last=False)
            self._interval_catalog_count -= len(intervals)
        # Defensive normalization keeps the bound trustworthy even if a caller
        # or future refactor supplies malformed internal state.
        self._interval_catalog_count = max(0, self._interval_catalog_count)

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
        self,
        incoming: int,
        *,
        exclude_directory: str,
        entry_count: int = 1,
        include_entry_shell: bool = False,
    ) -> Tuple[str, RangeCacheMetrics]:
        incoming = max(0, int(incoming))
        entry_count = max(1, int(entry_count))
        future_bytes = self._estimate_publication_bytes(
            incoming,
            entry_count=entry_count,
            include_entry_shell=include_entry_shell,
        )
        if future_bytes > self.max_bytes:
            raise RangeCacheUnavailable("range exceeds configured cache byte cap")
        self._ensure_root()
        metrics = RangeCacheMetrics()
        reservation_dir = os.path.join(self.cache_root, ".reservations")
        self._ensure_private_chain(reservation_dir)
        reservation = os.path.join(
            reservation_dir,
            f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}.json",
        )
        # Publish the reservation before examining capacity. Other processes
        # can now account both its actual filesystem allocation and the
        # conservatively estimated bytes that this fill has not created yet.
        self._atomic_json(reservation, {
            "accounting_version": _RESERVATION_ACCOUNTING_VERSION,
            "size": incoming,
            "future_bytes": future_bytes,
            "entry_count": entry_count,
            "created_ns": time.time_ns(),
            "pid": os.getpid(),
        })
        try:
            with self._global_lock():
                self._clean_stale_reservations()
                self._clean_orphans_locked()
                entries, total = self._scan_cache()
                reservations = self._reservation_bytes()
                now_ns = time.time_ns()
                excluded = (
                    os.path.realpath(exclude_directory)
                    if exclude_directory else None
                )
                candidates = [
                    entry for entry in entries
                    if excluded is None
                    or os.path.realpath(entry.paths.directory) != excluded
                ]
                evicted_directories = set()

                # Keep explicit positive-TTL behavior separate from admission:
                # every expired, unlocked entry is eligible even when the new
                # range already fits.
                if self.ttl > 0:
                    cutoff_ns = now_ns - int(self.ttl * 1_000_000_000)
                    for entry in sorted(
                        candidates, key=lambda item: item.accessed_ns,
                    ):
                        if entry.accessed_ns >= cutoff_ns:
                            break
                        if self._evict(entry.paths):
                            total -= entry.size
                            evicted_directories.add(entry.paths.directory)
                            metrics.evictions += 1
                            metrics.evicted_bytes += entry.payload_size

                # Entries are indivisible. Stop immediately after enough
                # accounted bytes are reclaimed, even if the final entry frees
                # more than strictly required.
                required = max(
                    0, total + reservations - self.max_bytes,
                )
                if required > 0:
                    remaining = (
                        entry for entry in candidates
                        if entry.paths.directory not in evicted_directories
                    )
                    for entry in sorted(
                        remaining,
                        key=lambda item: self._eviction_key(item, now_ns),
                    ):
                        if required <= 0:
                            break
                        if self._evict(entry.paths):
                            total -= entry.size
                            required -= entry.size
                            metrics.evictions += 1
                            metrics.evicted_bytes += entry.payload_size

                # The scan was performed under the global lock, and `_evict`
                # reports success only after its complete accounted directory
                # was purged. Existing in-flight fills remain covered by their
                # conservative reservations, so subtraction is authoritative
                # without a second full cache walk.
                if total + reservations > self.max_bytes:
                    raise RangeCacheUnavailable(
                        "range cache cap is occupied by active readers/downloads"
                    )
                return reservation, metrics
        except Exception:
            self._safe_unlink(reservation)
            raise

    def _finish_reservation(
        self,
        reservation: str,
        current: _ChunkPaths,
        metrics: RangeCacheMetrics,
    ) -> bool:
        """Publish only if the authoritative post-write footprint fits."""
        with self._global_lock():
            self._safe_unlink(reservation)
            self._clean_stale_reservations()
            entries, total = self._scan_cache()
            reservations = self._reservation_bytes()
            required = max(0, total + reservations - self.max_bytes)
            if required > 0:
                now_ns = time.time_ns()
                current_real = os.path.realpath(current.directory)
                candidates = [
                    entry for entry in entries
                    if os.path.realpath(entry.paths.directory) != current_real
                ]
                for entry in sorted(
                    candidates,
                    key=lambda item: self._eviction_key(item, now_ns),
                ):
                    if required <= 0:
                        break
                    if self._evict(entry.paths):
                        required -= entry.size
                        total -= entry.size
                        metrics.evictions += 1
                        metrics.evicted_bytes += entry.payload_size
            if total + reservations <= self.max_bytes:
                return True

            # The filesystem consumed more than the conservative estimate and
            # no unlocked older entry can make room. Keep the already fetched
            # payload for this caller, but do not admit an over-cap cache entry.
            self._remove_chunk_files(current)
            return False

    def _finish_batch_reservation(
        self, reservation: str,
    ) -> RangeCacheMetrics:
        """Release a prefetch reservation and enforce the measured final cap."""
        metrics = RangeCacheMetrics()
        try:
            with self._global_lock():
                self._safe_unlink(reservation)
                self._clean_stale_reservations()
                entries, total = self._scan_cache()
                reservations = self._reservation_bytes()
                required = max(0, total + reservations - self.max_bytes)
                now_ns = time.time_ns()
                for entry in sorted(
                    entries,
                    key=lambda item: self._eviction_key(item, now_ns),
                ):
                    if required <= 0:
                        break
                    if self._evict(entry.paths):
                        required -= entry.size
                        total -= entry.size
                        metrics.evictions += 1
                        metrics.evicted_bytes += entry.payload_size
                if total + reservations > self.max_bytes:
                    metrics.errors += 1
        except Exception:
            self._safe_unlink(reservation)
            metrics.errors += 1
        return metrics

    def _scan_entries(self) -> List[_ScannedEntry]:
        """Compatibility helper returning committed entries only."""
        return self._scan_cache()[0]

    def _scan_cache(self) -> Tuple[List[_ScannedEntry], int]:
        """Return committed entries and conservatively allocated cache bytes."""
        result: List[_ScannedEntry] = []
        if not os.path.isdir(self.cache_root):
            return result, 0
        total = 0
        allocation_unit = self._allocation_unit()
        for root, directories, files in os.walk(self.cache_root, followlinks=False):
            try:
                total += self._accounted_stat_bytes(
                    os.lstat(root), allocation_unit,
                )
            except OSError:
                continue
            safe_directories = []
            for name in directories:
                path = os.path.join(root, name)
                try:
                    info = os.lstat(path)
                except OSError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    total += self._accounted_stat_bytes(info, allocation_unit)
                elif stat.S_ISDIR(info.st_mode):
                    safe_directories.append(name)
            directories[:] = safe_directories

            file_bytes: Dict[str, int] = {}
            for name in files:
                path = os.path.join(root, name)
                try:
                    accounted = self._accounted_stat_bytes(
                        os.lstat(path), allocation_unit,
                    )
                except OSError:
                    continue
                file_bytes[name] = accounted
                total += accounted

            if "manifest.json" not in files:
                continue
            name = os.path.basename(root)
            if not _CHUNK_NAME.fullmatch(name):
                continue
            try:
                start_hex, length_hex = name.split("-", 1)
                start, length = int(start_hex, 16), int(length_hex, 16)
                manifest = self._load_manifest_bounded(
                    os.path.join(root, "manifest.json")
                )
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
                access = self._read_access_record(paths.access)
                if access is None:
                    # Missing, legacy, torn or corrupt advisory state is cold
                    # and immediately TTL-eligible.  It never affects bytes.
                    accessed = 0
                else:
                    access_mtime_ns = os.stat(
                        paths.access, follow_symlinks=False,
                    ).st_mtime_ns
                    accessed = min(
                        int(access.last_access_ns),
                        int(access_mtime_ns),
                        time.time_ns(),
                    )
                if int(info.st_size) == length:
                    chunk_size = self._accounted_stat_bytes(
                        os.lstat(root), allocation_unit,
                    ) + sum(file_bytes.values())
                    result.append(_ScannedEntry(
                        paths=paths,
                        size=chunk_size,
                        payload_size=length,
                        accessed_ns=int(accessed),
                        access=access,
                    ))
            except Exception:
                continue
        return result, total

    def _allocation_unit(self) -> int:
        """Return the filesystem's smallest useful conservative allocation."""
        try:
            probe = self.cache_root
            while not os.path.exists(probe):
                parent = os.path.dirname(probe)
                if parent == probe:
                    break
                probe = parent
            info = os.statvfs(probe)
            return max(
                512,
                int(getattr(info, "f_frsize", 0) or 0),
                int(getattr(info, "f_bsize", 0) or 0),
            )
        except OSError:
            # The common 4-KiB unit is deliberately conservative for filesystems
            # that cannot expose allocation geometry at admission time.
            return 4096

    @staticmethod
    def _accounted_stat_bytes(info: os.stat_result, allocation_unit: int) -> int:
        """Conservatively account blocks plus otherwise invisible inode cost."""
        unit = max(512, int(allocation_unit))
        logical = max(0, int(getattr(info, "st_size", 0) or 0))
        rounded_logical = ((logical + unit - 1) // unit) * unit
        allocated = max(0, int(getattr(info, "st_blocks", 0) or 0)) * 512
        # Empty locks and directories still consume inode/directory metadata.
        # At least one allocation unit per namespace node prevents tiny ranges
        # from escaping the cap through metadata amplification.
        return max(unit, rounded_logical, allocated)

    def _estimate_publication_bytes(
        self,
        incoming: int,
        *,
        entry_count: int,
        include_entry_shell: bool,
    ) -> int:
        """Bound bytes a reservation can still add before it is released."""
        unit = self._allocation_unit()
        count = max(1, int(entry_count))
        incoming = max(0, int(incoming))
        # Rounding each independent data interval can cost almost one full unit
        # beyond rounding their union. There is no extra padding for one entry,
        # which preserves exact-cap replacement of an equal-footprint chunk.
        data = (
            ((incoming + unit - 1) // unit) * unit
            + max(0, count - 1) * unit
        )
        # Every committed chunk adds one bounded manifest and one fixed access
        # record. Individual fills have already created their directory/lock;
        # batch reservations must also cover the complete namespace shell.
        metadata = count * 2 * unit
        if include_entry_shell:
            metadata += count * _NEW_ENTRY_NAMESPACE_UNITS * unit
        return data + metadata

    @staticmethod
    def _load_manifest_bounded(path: str) -> Dict[str, Any]:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_MANIFEST_BYTES:
                raise RangeCacheIntegrityError("range manifest is not bounded")
            parts: List[bytes] = []
            remaining = _MAX_MANIFEST_BYTES + 1
            while remaining > 0:
                part = os.read(fd, remaining)
                if not part:
                    break
                parts.append(part)
                remaining -= len(part)
            payload = b"".join(parts)
            if len(payload) > _MAX_MANIFEST_BYTES:
                raise RangeCacheIntegrityError("range manifest is too large")
            finished = os.fstat(fd)
            before_identity = (
                int(info.st_dev), int(info.st_ino), int(info.st_size),
                int(info.st_mtime_ns), int(info.st_ctime_ns),
            )
            after_identity = (
                int(finished.st_dev), int(finished.st_ino), int(finished.st_size),
                int(finished.st_mtime_ns), int(finished.st_ctime_ns),
            )
            if before_identity != after_identity:
                raise RangeCacheIntegrityError(
                    "range manifest changed while being read"
                )
            value = json.loads(payload.decode("utf-8"))
            if not isinstance(value, dict):
                raise RangeCacheIntegrityError("range manifest is not an object")
            return value
        except RangeCacheIntegrityError:
            raise
        except Exception:
            raise RangeCacheIntegrityError("range manifest is corrupt") from None
        finally:
            os.close(fd)

    @staticmethod
    def _aged_frequency(record: _AccessRecord, now_ns: int) -> int:
        frequency = min(_MAX_FREQUENCY, max(0, int(record.frequency)))
        elapsed_ns = max(0, int(now_ns) - int(record.decay_epoch_ns))
        half_lives = elapsed_ns // max(1, _FREQUENCY_HALF_LIFE_NS)
        # The bounded counter is zero after at most 16 shifts.  Avoid shifts
        # derived from a corrupt/far-future timestamp even though decode also
        # validates the persistent record.
        if half_lives >= _MAX_FREQUENCY.bit_length():
            return 0
        return frequency >> int(half_lives)

    @classmethod
    def _eviction_key(
        cls,
        entry: _ScannedEntry,
        now_ns: int,
    ) -> Tuple[int, int, int, str]:
        record = entry.access
        if record is None:
            # Access state is advisory: an absent/torn/corrupt record makes the
            # entry cold, but cannot invalidate or influence its cached bytes.
            return (0, 0, entry.accessed_ns, entry.paths.directory)
        frequency = cls._aged_frequency(record, now_ns)
        # Greedy-Dual-style retention: repeated use and expensive refills raise
        # priority, while a larger byte footprint lowers it.  Integer math keeps
        # ordering deterministic and avoids floating-point corner cases.
        retention = (
            frequency
            * max(_MIN_REFILL_COST_NS, min(_MAX_REFILL_COST_NS, record.refill_cost_ns))
            * 1_000_000
            // max(1, entry.size)
        )
        return (1, retention, record.last_access_ns, entry.paths.directory)

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
            quarantine = self._quarantine_chunk_locked(paths)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        return bool(quarantine and self._purge_quarantine(quarantine))

    def _quarantine_chunk_locked(self, paths: _ChunkPaths) -> Optional[str]:
        """Atomically detach a chunk while its entry lock is exclusively held."""
        parent = os.path.dirname(paths.directory)
        quarantine = os.path.join(
            parent,
            f".{os.path.basename(paths.directory)}.reclaim-{uuid.uuid4().hex}",
        )
        try:
            os.rename(paths.directory, quarantine)
        except (FileNotFoundError, OSError):
            return None
        self._forget_validation(paths)
        self._forget_catalog_directory(paths.directory)
        self._fsync_dir(parent)
        return quarantine

    @staticmethod
    def _purge_quarantine(directory: str) -> bool:
        """Remove one already-detached private chunk tree without following links."""
        try:
            for root, directories, files in os.walk(
                directory, topdown=False, followlinks=False,
            ):
                for name in files:
                    try:
                        os.unlink(os.path.join(root, name))
                    except FileNotFoundError:
                        pass
                for name in directories:
                    path = os.path.join(root, name)
                    try:
                        if os.path.islink(path):
                            os.unlink(path)
                        else:
                            os.rmdir(path)
                    except FileNotFoundError:
                        pass
                os.rmdir(root)
            return not os.path.lexists(directory)
        except OSError:
            return False

    def _entry_is_structurally_committed(self, paths: _ChunkPaths) -> bool:
        """Validate the bounded publication seal without rehashing payload data."""
        try:
            for target in (paths.data, paths.manifest):
                info = os.lstat(target)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                    return False
            manifest = self._load_manifest_bounded(paths.manifest)
            info = os.stat(paths.data, follow_symlinks=False)
            identity_hash = manifest.get("identity_hash")
            checksum = manifest.get("sha256")
            return bool(
                paths.length > 0
                and manifest.get("format_version") == _FORMAT_VERSION
                and isinstance(identity_hash, str)
                and len(identity_hash) == 64
                and all(ch in "0123456789abcdef" for ch in identity_hash)
                and manifest.get("start") == paths.start
                and manifest.get("length") == paths.length
                and int(info.st_size) == paths.length
                and manifest.get("data_dev") == int(info.st_dev)
                and manifest.get("data_ino") == int(info.st_ino)
                and manifest.get("data_mtime_ns") == int(info.st_mtime_ns)
                and manifest.get("data_ctime_ns") == int(info.st_ctime_ns)
                and isinstance(checksum, str)
                and len(checksum) == 64
                and all(ch in "0123456789abcdef" for ch in checksum)
            )
        except Exception:
            return False

    def _clean_chunk_orphan_locked(self, paths: _ChunkPaths) -> None:
        """Clean one chunk directory; caller must hold the global cache lock."""
        try:
            if not os.path.isdir(paths.directory):
                return
            self._validate_existing_chain(paths.directory)
            self._ensure_private_chain(paths.directory)
            had_lock = os.path.isfile(paths.lock) and not os.path.islink(paths.lock)
            if os.path.islink(paths.lock):
                self._safe_unlink(paths.lock)
            fd = self._open_lock(paths.lock, create=True)
        except (OSError, RangeCacheError):
            return
        quarantine: Optional[str] = None
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            if not had_lock:
                # Every normal writer creates the lock before any data. A
                # lockless directory is therefore either foreign or torn; the
                # temporary lock above only serializes its safe quarantine.
                quarantine = self._quarantine_chunk_locked(paths)
            elif self._entry_is_structurally_committed(paths):
                # Crash leftovers beside a valid atomic commit are never part
                # of the entry and can be reclaimed under the same entry lock.
                try:
                    names = os.listdir(paths.directory)
                except OSError:
                    return
                unexpected = [
                    name for name in names
                    if name not in {
                        "data.bin", "manifest.json", "access", "entry.lock",
                    }
                    and not (
                        (name.startswith(".range-") and name.endswith(".part"))
                        or name.startswith(".manifest-")
                    )
                ]
                if unexpected:
                    quarantine = self._quarantine_chunk_locked(paths)
                    # Continue after releasing the entry lock so the detached
                    # tree (including an unexpected directory) can be purged.
                else:
                    for name in names:
                        if (
                            (name.startswith(".range-") and name.endswith(".part"))
                            or name.startswith(".manifest-")
                        ):
                            self._safe_unlink(os.path.join(paths.directory, name))
                    return
            else:
                quarantine = self._quarantine_chunk_locked(paths)
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        if quarantine:
            self._purge_quarantine(quarantine)

    def _clean_orphans_locked(self) -> None:
        """Reclaim crash/torn chunk state without disturbing active leases."""
        if not os.path.isdir(self.cache_root):
            return
        chunks: List[_ChunkPaths] = []
        quarantines: List[str] = []
        for root, directories, _files in os.walk(
            self.cache_root, followlinks=False,
        ):
            directories[:] = [
                name for name in directories
                if not os.path.islink(os.path.join(root, name))
            ]
            if os.path.basename(os.path.dirname(root)) == "chunks":
                name = os.path.basename(root)
                if _CHUNK_NAME.fullmatch(name):
                    try:
                        start_hex, length_hex = name.split("-", 1)
                        start, length = int(start_hex, 16), int(length_hex, 16)
                        manifest_path = os.path.join(root, "manifest.json")
                        identity_hash = ""
                        try:
                            identity_hash = str(
                                self._load_manifest_bounded(manifest_path).get(
                                    "identity_hash", ""
                                )
                            )
                        except Exception:
                            pass
                        chunks.append(_ChunkPaths(
                            directory=root,
                            data=os.path.join(root, "data.bin"),
                            manifest=manifest_path,
                            access=os.path.join(root, "access"),
                            lock=os.path.join(root, "entry.lock"),
                            identity_hash=identity_hash,
                            start=start,
                            length=length,
                        ))
                    except (TypeError, ValueError):
                        pass
                elif ".reclaim-" in name and name.startswith("."):
                    quarantines.append(root)
                directories[:] = []
        for paths in chunks:
            self._clean_chunk_orphan_locked(paths)
        for directory in quarantines:
            self._purge_quarantine(directory)

    def _reclaim_chunk_if_orphan(self, paths: _ChunkPaths) -> None:
        """Best-effort removal of an empty/torn shell after a failed fill."""
        try:
            if not os.path.isdir(self.cache_root):
                return
            with self._global_lock():
                self._clean_chunk_orphan_locked(paths)
        except Exception:
            pass

    def _reservation_bytes(self) -> int:
        directory = os.path.join(self.cache_root, ".reservations")
        total = 0
        if not os.path.isdir(directory):
            return total
        for path in Path(directory).glob("*.json"):
            try:
                value = self._load_manifest_bounded(str(path))
                if (
                    value.get("accounting_version")
                    == _RESERVATION_ACCOUNTING_VERSION
                ):
                    total += max(0, int(value.get("future_bytes", 0)))
                else:
                    # A live reservation from an older process only recorded
                    # payload bytes. Upgrade it conservatively in memory so a
                    # rolling deployment cannot over-admit metadata.
                    total += self._estimate_publication_bytes(
                        max(0, int(value.get("size", 0))),
                        entry_count=max(1, int(value.get("entry_count", 1))),
                        include_entry_shell=True,
                    )
            except Exception:
                # A malformed reservation is already counted by actual disk
                # usage. Treat its unknown future as the entire cap until stale
                # cleanup can prove it abandoned.
                total += self.max_bytes
        return total

    def _clean_stale_reservations(self) -> None:
        directory = os.path.join(self.cache_root, ".reservations")
        if not os.path.isdir(directory):
            return
        cutoff = time.time() - _STALE_RESERVATION_SECONDS
        for path in Path(directory).glob("*.json"):
            try:
                value = self._load_manifest_bounded(str(path))
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
                # Final reservation names are published atomically; malformed
                # JSON therefore cannot be an in-progress writer.
                try:
                    if not path.is_symlink() and path.is_file():
                        path.unlink()
                except Exception:
                    pass
        # Atomic reservation manifests abandoned before rename carry no PID.
        # Age-gating avoids racing a live writer that has not published yet.
        for path in Path(directory).glob(".manifest-*"):
            try:
                if path.stat().st_mtime < cutoff:
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
        except ValueError:
            raise RangeCacheUnavailable("range cache path escaped its root") from None
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
        except ValueError:
            raise RangeCacheUnavailable("range cache path escaped its root") from None
        current = root
        for component in ("", *relative.parts):
            if component:
                current /= component
            try:
                info = os.lstat(current)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise RangeCacheUnavailable(
                        "unsafe range cache directory chain"
                    ) from None
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
                    raise RangeCacheUnavailable(
                        "unsafe range cache directory chain"
                    ) from None
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

    @staticmethod
    def _open_access(path: str, *, create: bool) -> int:
        flags = os.O_RDWR | (os.O_CREAT if create else 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise RangeCacheUnavailable(
                    "range access record is not a regular file"
                )
            os.fchmod(fd, 0o600)
            return fd
        except Exception:
            os.close(fd)
            raise

    @staticmethod
    def _encode_access_record(record: _AccessRecord) -> bytes:
        body = _ACCESS_BODY.pack(
            _ACCESS_MAGIC,
            _ACCESS_VERSION,
            min(_MAX_FREQUENCY, max(0, int(record.frequency))),
            max(0, int(record.decay_epoch_ns)),
            max(0, int(record.last_access_ns)),
            max(
                _MIN_REFILL_COST_NS,
                min(_MAX_REFILL_COST_NS, int(record.refill_cost_ns)),
            ),
        )
        return body + _ACCESS_CRC.pack(zlib.crc32(body) & 0xFFFFFFFF)

    @staticmethod
    def _decode_access_record(payload: bytes) -> Optional[_AccessRecord]:
        if len(payload) != _ACCESS_RECORD_BYTES:
            return None
        try:
            body = payload[:-_ACCESS_CRC.size]
            (expected_crc,) = _ACCESS_CRC.unpack(payload[-_ACCESS_CRC.size:])
            if zlib.crc32(body) & 0xFFFFFFFF != expected_crc:
                return None
            magic, version, frequency, epoch, last_access, refill_cost = (
                _ACCESS_BODY.unpack(body)
            )
            if (
                magic != _ACCESS_MAGIC
                or version != _ACCESS_VERSION
                or frequency > _MAX_FREQUENCY
                or epoch <= 0
                or last_access <= 0
                or refill_cost < _MIN_REFILL_COST_NS
                or refill_cost > _MAX_REFILL_COST_NS
            ):
                return None
            return _AccessRecord(
                frequency=int(frequency),
                decay_epoch_ns=int(epoch),
                last_access_ns=int(last_access),
                refill_cost_ns=int(refill_cost),
            )
        except (OverflowError, struct.error, ValueError):
            return None

    def _read_access_record(self, path: str) -> Optional[_AccessRecord]:
        """Read bounded advisory state; malformed state is always cold."""
        fd = -1
        try:
            fd = self._open_access(path, create=False)
            fcntl.flock(fd, fcntl.LOCK_SH)
            os.lseek(fd, 0, os.SEEK_SET)
            # One extra byte makes oversized/unbounded records invalid without
            # ever allocating based on their on-disk length.
            payload = os.read(fd, _ACCESS_RECORD_BYTES + 1)
            return self._decode_access_record(payload)
        except (OSError, RangeCacheError):
            return None
        finally:
            if fd >= 0:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)

    def _record_access(
        self,
        paths: _ChunkPaths,
        *,
        refill_cost_ns: Optional[int] = None,
    ) -> bool:
        """Persist one bounded, cross-process-safe advisory access update."""
        fd = -1
        try:
            # A warm hit must never grow the cache outside admission. New fills
            # reserve this fixed record before passing a refill cost; legacy or
            # torn entries with no record simply remain cold until rotation.
            fd = self._open_access(
                paths.access, create=refill_cost_ns is not None,
            )
            fcntl.flock(fd, fcntl.LOCK_EX)
            now_ns = time.time_ns()
            if refill_cost_ns is None:
                os.lseek(fd, 0, os.SEEK_SET)
                current = self._decode_access_record(
                    os.read(fd, _ACCESS_RECORD_BYTES + 1)
                )
                if current is None:
                    frequency = 1
                    refill_cost = _MIN_REFILL_COST_NS
                else:
                    frequency = min(
                        _MAX_FREQUENCY,
                        self._aged_frequency(current, now_ns) + 1,
                    )
                    refill_cost = current.refill_cost_ns
            else:
                frequency = 1
                refill_cost = max(
                    _MIN_REFILL_COST_NS,
                    min(_MAX_REFILL_COST_NS, int(refill_cost_ns)),
                )
            payload = self._encode_access_record(_AccessRecord(
                frequency=frequency,
                decay_epoch_ns=now_ns,
                last_access_ns=now_ns,
                refill_cost_ns=refill_cost,
            ))
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            written = 0
            while written < len(payload):
                count = os.write(fd, payload[written:])
                if count <= 0:
                    raise OSError("short range access-record write")
                written += count
            os.ftruncate(fd, len(payload))
            return True
        except (OSError, OverflowError, RangeCacheError, struct.error, ValueError):
            # The record never authorizes or validates cached bytes.  Losing an
            # update only makes the entry colder at the next admission scan.
            return False
        finally:
            if fd >= 0:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)

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
        self._forget_validation(paths)
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
