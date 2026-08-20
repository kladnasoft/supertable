# processing.py

import decimal
import hashlib
import json
import logging
import os
import io
import stat
import struct
import time
import threading
import uuid
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from contextvars import copy_context
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Any, Callable, Dict, FrozenSet, Iterable, Iterator, List, Set, Tuple, Optional, cast

import polars
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from supertable.utils.helper import generate_filename, hourly_partition_subpath
from supertable.config.defaults import default
from supertable.config.settings import settings
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import ObjectMetadata, StorageInterface
from supertable.tombstone_manifest_v2 import (
    MAX_JSON_EXACT_INTEGER,
    MAX_TOMBSTONE_MANIFEST_V2_BYTES,
    MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS,
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V2,
    TombstoneManifestV2,
    TombstoneManifestV2Error,
    TombstoneSegment,
    load_tombstone_manifest_v2,
    validate_logical_storage_path,
    validate_tombstone_segment_observation,
)
from supertable.utils.profiler import Profiler, get_null_profiler
from supertable.utils.snapshot import read_bounded_tombstone_manifest_bytes
from supertable.data_classes import (
    IntegerDomainBound,
    PredInterval,
    ResourceObjectSeal,
    ResourceStatsSeal,
    RowGroupSelection,
    TombstoneSegmentDef,
)

# Target row-group size for all Parquet writes.
# 122 880 rows ≈ 120 K — sits comfortably in the recommended 100 K–1 M range.
# Smaller groups mean tighter min/max statistics so DuckDB can skip more groups;
# larger groups reduce metadata overhead.  120 K is a good balance for the
# incremental-merge pattern used here.
_PARQUET_ROW_GROUP_SIZE = 122_880
_STATS_CACHE_IDENTITY_PREFIX = "__supertable_stats_cache__/"
_TOMBSTONE_CACHE_IDENTITY_PREFIX = "__supertable_tombstone_cache__/"


@dataclass(frozen=True)
class _LocalArtifactCacheIdentityState:
    """Inputs that make one built-in local-storage cache scope reusable."""

    process_id: int
    organization: str
    storage_id: int
    storage_root: str


def _local_artifact_cache_identity_state(
        organization: str,
        storage: object,
) -> Optional[_LocalArtifactCacheIdentityState]:
    """Return a cacheable state only for the exact built-in LocalStorage.

    Remote and third-party adapters may rotate credentials behind a stable
    Python object.  They deliberately stay on FileCache's live namespace/auth
    sampling path below.  LocalStorage has no credential scope, exposes a
    canonical absolute root, and has a frozen ``{"provider": "local"}``
    namespace, so its identity inputs can safely be retained by one writer.
    """
    try:
        from supertable.storage.local_storage import LocalStorage

        if type(storage) is not LocalStorage:
            return None
        if storage.cache_namespace() != {"provider": "local"}:
            return None
        if storage.is_local_storage() is not True:
            return None
        root = storage.root
        if type(root) is not str or not root or not os.path.isabs(root):
            return None
    except Exception:
        return None
    return _LocalArtifactCacheIdentityState(
        process_id=os.getpid(),
        organization=str(organization or ""),
        storage_id=id(storage),
        storage_root=root,
    )


def _local_artifact_cache_scope(
        state: _LocalArtifactCacheIdentityState,
) -> str:
    """Hash the non-secret, process-local LocalStorage cache boundary."""
    payload = {
        "organization": state.organization,
        "process_id": state.process_id,
        "storage_namespace": {"provider": "local"},
        "storage_root": state.storage_root,
    }
    return hashlib.sha256(json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")).hexdigest()


def _artifact_cache_identity(
        artifact_path: str,
        *,
        prefix: str,
        organization: str = "",
        storage: Optional[object] = None,
        identity_scope: Optional[str] = None,
) -> str:
    """Return an organization/storage/auth-scoped immutable artifact key."""
    value = str(artifact_path or "")
    if value.startswith(prefix):
        return value
    active_storage = storage
    if active_storage is None:
        try:
            active_storage = _get_storage()
        except Exception:
            active_storage = None
    scope = identity_scope
    if scope is None:
        local_state = _local_artifact_cache_identity_state(
            organization, active_storage,
        )
        if local_state is not None:
            scope = _local_artifact_cache_scope(local_state)
        else:
            try:
                # Remote/custom adapters stay byte-for-byte on FileCache's
                # hardened namespace contract, including live credential
                # fingerprints and opaque-client isolation.  Do not retain this
                # result: refreshable authorization may change between calls.
                from supertable.engine.file_cache import FileCache
                namespace = FileCache(
                    active_storage, organization, max_bytes=0, workers=1,
                )
                scope = f"{namespace._organization_hash}{namespace._storage_hash}"
            except Exception:
                fallback = (
                    f"{organization}\0{type(active_storage).__module__}."
                    f"{type(active_storage).__qualname__}\0{id(active_storage)}"
                )
                scope = hashlib.sha256(fallback.encode("utf-8")).hexdigest()
    table_key = _PathKeyedFrameCache._key(value) if value else ""
    table_hash = hashlib.sha256(table_key.encode("utf-8")).hexdigest()
    version_hash = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return (
        f"{prefix}{scope}/"
        f"{table_hash}/{version_hash}"
    )


def stats_cache_identity(
        stats_path: str,
        *,
        organization: str = "",
        storage: Optional[object] = None,
) -> str:
    """Return an organization/storage/auth-scoped immutable stats cache key."""
    return _artifact_cache_identity(
        stats_path,
        prefix=_STATS_CACHE_IDENTITY_PREFIX,
        organization=organization,
        storage=storage,
    )


def tombstone_cache_identity(
        tombstone_path: str,
        *,
        organization: str = "",
        storage: Optional[object] = None,
) -> str:
    """Return an organization/storage/auth-scoped deletion-vector cache key."""
    return _artifact_cache_identity(
        tombstone_path,
        prefix=_TOMBSTONE_CACHE_IDENTITY_PREFIX,
        organization=organization,
        storage=storage,
    )


class _WriterArtifactCacheIdentities:
    """Reuse only the immutable built-in LocalStorage identity per writer."""

    def __init__(self) -> None:
        self._process_id = os.getpid()
        self._state: Optional[_LocalArtifactCacheIdentityState] = None
        self._scope: Optional[str] = None
        self._lock = threading.Lock()

    def _local_scope(
            self,
            organization: str,
            storage: object,
    ) -> Optional[str]:
        process_id = os.getpid()
        if process_id != self._process_id:
            # A lock inherited while another parent thread held it can never be
            # released in the child.  Replace both synchronization and cached
            # identity before examining the forked writer.
            self._lock = threading.Lock()
            self._process_id = process_id
            self._state = None
            self._scope = None

        state = _local_artifact_cache_identity_state(organization, storage)
        if state is None:
            return None
        with self._lock:
            if state != self._state:
                self._state = state
                self._scope = _local_artifact_cache_scope(state)
            return self._scope

    def stats(
            self,
            path: str,
            *,
            organization: str,
            storage: object,
    ) -> str:
        value = str(path or "")
        if value.startswith(_STATS_CACHE_IDENTITY_PREFIX):
            return value
        scope = self._local_scope(organization, storage)
        if scope is None:
            return stats_cache_identity(
                value, organization=organization, storage=storage,
            )
        return _artifact_cache_identity(
            value,
            prefix=_STATS_CACHE_IDENTITY_PREFIX,
            organization=organization,
            storage=storage,
            identity_scope=scope,
        )

    def tombstone(
            self,
            path: str,
            *,
            organization: str,
            storage: object,
    ) -> str:
        value = str(path or "")
        if value.startswith(_TOMBSTONE_CACHE_IDENTITY_PREFIX):
            return value
        scope = self._local_scope(organization, storage)
        if scope is None:
            return tombstone_cache_identity(
                value, organization=organization, storage=storage,
            )
        return _artifact_cache_identity(
            value,
            prefix=_TOMBSTONE_CACHE_IDENTITY_PREFIX,
            organization=organization,
            storage=storage,
            identity_scope=scope,
        )


def _resolve_limits(table_config: Optional[dict]) -> Tuple[int, int]:
    """Return (max_mem_bytes, max_files) for the given table config.

    Resolution order:
      1. Per-table value stored in Redis (table_config dict)
      2. Global default (from environment / defaults.py)
    """
    cfg = table_config or {}
    max_mem = int(cfg.get("max_memory_chunk_size") or getattr(default, "MAX_MEMORY_CHUNK_SIZE", 16 * 1024 * 1024))
    max_files = int(cfg.get("max_overlapping_files") or getattr(default, "MAX_OVERLAPPING_FILES", 100))
    return max_mem, max_files

# Lazy storage accessor to avoid import-time initialization failures
_storage = None


def _get_storage():
    global _storage
    if _storage is None:
        _storage = get_storage()
    return _storage

# =========================
# Schema helpers (robust, minimal)
# =========================

_NUMERIC_INTS = {
    polars.Int8, polars.Int16, polars.Int32, polars.Int64,
    polars.UInt8, polars.UInt16, polars.UInt32, polars.UInt64,
}
_NUMERIC_FLOATS = {polars.Float32, polars.Float64}


def _native_polars_parquet_eligibility(
        write_df: polars.DataFrame,
) -> Tuple[bool, Optional[str]]:
    """Return whether the native Polars Parquet codec is stats-compatible.

    Polars' Rust writer is substantially faster than the PyArrow writer, but a
    few footer-statistics behaviours differ in ways that can weaken or change
    the row-group bounds consumed by SuperTable.  Keep those frames on the
    established PyArrow path:

      * float columns containing NaN (Polars omits their min/max),
      * top-level strings wider than 64 UTF-8 bytes (Polars truncates bounds),
      * categorical/enum columns, whose dictionary/statistics representation is
        deliberately left to the compatibility writer for now,
      * nested list/array/struct columns, until the same checks can be proven
        recursively for every Parquet leaf.

    The check itself fails closed.  A new/third-party dtype or expression error
    therefore costs performance for that file, never statistics correctness.
    The optional reason is a fixed token suitable for a telemetry counter.
    """
    checks: List[polars.Expr] = []
    check_kinds: List[str] = []

    try:
        for index, (name, dtype) in enumerate(write_df.schema.items()):
            base_type = dtype.base_type()
            if base_type == polars.Categorical:
                return False, "categorical"
            if base_type == polars.Enum:
                return False, "enum"
            if base_type in (polars.List, polars.Array, polars.Struct):
                return False, "nested"
            if dtype in _NUMERIC_FLOATS:
                checks.append(
                    polars.col(name)
                    .is_nan()
                    .any()
                    .fill_null(False)
                    .alias(f"__supertable_codec_check_{index}")
                )
                check_kinds.append("nan")
            elif dtype == polars.String:
                checks.append(
                    polars.col(name)
                    .str.len_bytes()
                    .max()
                    .fill_null(0)
                    .alias(f"__supertable_codec_check_{index}")
                )
                check_kinds.append("long_string")

        if checks:
            observed = write_df.select(checks).row(0)
            for kind, value in zip(check_kinds, observed):
                if kind == "nan" and bool(value):
                    return False, kind
                if kind == "long_string" and int(value or 0) > 64:
                    return False, kind
    except Exception:
        return False, "stats_check_error"
    return True, None


def _encode_parquet_polars(
        write_df: polars.DataFrame,
        compression_level: int,
) -> bytes:
    """Encode with Polars' native Rust writer using the table invariants."""
    buf = io.BytesIO()
    write_df.write_parquet(
        buf,
        compression="zstd",
        compression_level=int(compression_level),
        # Match PyArrow's established min/max/null-count contract without the
        # extra distinct-count pass performed by ``statistics="full"``.
        statistics={
            "min": True,
            "max": True,
            "null_count": True,
            "distinct_count": False,
        },
        row_group_size=_PARQUET_ROW_GROUP_SIZE,
    )
    return buf.getvalue()


def _encode_parquet_pyarrow(
        arrow_tbl: pa.Table,
        compression_level: int,
) -> bytes:
    """Encode with the compatibility PyArrow writer using existing settings."""
    buf = io.BytesIO()
    pq.write_table(
        arrow_tbl,
        buf,
        compression="zstd",
        compression_level=int(compression_level),
        use_dictionary=True,
        write_statistics=True,
        row_group_size=_PARQUET_ROW_GROUP_SIZE,
    )
    return buf.getvalue()


def _encode_system_parquet_polars(
        write_df: polars.DataFrame,
        compression_level: int,
) -> bytes:
    """Encode an internal metadata frame without unused footer statistics.

    Deletion vectors and the external statistics artifact are consumed as
    complete objects.  No read or write decision consults *their own* Parquet
    min/max statistics, so the data-file compatibility gate for NaNs, long
    strings, and nested values does not apply.  In particular, system frames
    contain long immutable object keys by construction; scanning every key
    only to route the frame through PyArrow was pure overhead.
    """
    buf = io.BytesIO()
    write_df.write_parquet(
        buf,
        compression="zstd",
        compression_level=int(compression_level),
        statistics=False,
        row_group_size=_PARQUET_ROW_GROUP_SIZE,
    )
    return buf.getvalue()


def _encode_system_parquet_pyarrow(
        arrow_tbl: pa.Table,
        compression_level: int,
) -> bytes:
    """Compatibility encoder for internal metadata objects."""
    buf = io.BytesIO()
    pq.write_table(
        arrow_tbl,
        buf,
        compression="zstd",
        compression_level=int(compression_level),
        use_dictionary=True,
        write_statistics=False,
        row_group_size=_PARQUET_ROW_GROUP_SIZE,
    )
    return buf.getvalue()


def _record_parquet_codec(
        profiler: Profiler,
        codec: str,
        fallback_reason: Optional[str] = None,
) -> None:
    """Record the selected encoder and, for PyArrow, its fixed gate reason."""
    profiler.add(f"parquet_codec_{codec}", 1)
    if codec == "pyarrow" and fallback_reason:
        profiler.add(f"parquet_codec_pyarrow_{fallback_reason}", 1)


def _resolve_unified_dtype(dtypes: Set[polars.DataType]) -> polars.DataType:
    if not dtypes:
        return polars.Utf8
    if len(dtypes) == 1:
        return next(iter(dtypes))
    if polars.Utf8 in dtypes:
        return polars.Utf8
    ints = any(dt in _NUMERIC_INTS for dt in dtypes)
    floats = any(dt in _NUMERIC_FLOATS for dt in dtypes)
    if polars.Datetime in dtypes:
        return polars.Datetime("us", None)
    if polars.Date in dtypes:
        return polars.Date
    if floats or (ints and floats):
        return polars.Float64
    if ints:
        return polars.Int64
    return polars.Utf8


def _union_schema_many(frames: List[polars.DataFrame]) -> Dict[str, polars.DataType]:
    """Build a unified column-name → dtype mapping across N dataframes.

    The output dict preserves first-appearance order: a column that first
    appears in frame *i* takes position determined by frame *i*'s own order
    relative to columns that appeared earlier.  Dtypes are widened via
    ``_resolve_unified_dtype`` over the set of dtypes the column carries
    across all frames that contain it.
    """
    seen: Set[str] = set()
    cols: List[str] = []
    for f in frames:
        for c in f.columns:
            if c not in seen:
                seen.add(c)
                cols.append(c)
    target: Dict[str, polars.DataType] = {}
    for c in cols:
        types: Set[polars.DataType] = set()
        for f in frames:
            if c in f.columns:
                types.add(f[c].dtype)
        target[c] = _resolve_unified_dtype(types)
    return target


def _union_schema(a: polars.DataFrame, b: polars.DataFrame) -> Dict[str, polars.DataType]:
    return _union_schema_many([a, b])


def _align_to_schema(df: polars.DataFrame, target_schema: Dict[str, polars.DataType]) -> polars.DataFrame:
    """Project *df* into *target_schema*: same column names, same order, same dtypes.

    For every column in *target_schema*:
      - present in *df* with the target dtype → keep the existing series
      - present in *df* with a different dtype → lossless/representable cast
      - absent in *df*                          → fill with a typed null literal

    The resulting frame's column order is **exactly** ``list(target_schema.keys())``.
    This is the contract callers like :func:`concat_with_union` rely on:
    ``polars.concat(..., how="vertical_relaxed")`` aligns frames *positionally*,
    so it requires identical names at identical positions.

    Implementation note: ``df.select(exprs)`` is used (not ``with_columns``).
    ``with_columns`` preserves the input frame's column order and appends new
    columns at the end, which silently breaks the positional-concat contract
    when *df*'s order disagrees with *target_schema*'s order.
    """
    if not target_schema:
        return df
    # Zero-row defence: ``df.select([pl.lit(None), ...])`` on an empty frame
    # broadcasts the literal to a single null row, which would silently turn
    # a 0-row input into a 1-row output.  Materialise an explicit empty frame
    # with the target schema instead.
    if df.height == 0:
        return polars.DataFrame(schema=target_schema)
    exprs: List[polars.Expr] = []
    for col, dtype in target_schema.items():
        if col in df.columns:
            if df.schema[col] != dtype:
                # Compaction is a physical rewrite, so replacing values that do
                # not fit the chosen union dtype with NULL would be permanent
                # data loss.  Strict casting catches invalid representations;
                # an exact round-trip also catches valid-but-lossy conversions
                # such as Int64(2**53+1) -> Float64.
                source = df.get_column(col)
                casted = source.cast(dtype, strict=True).alias(col)
                roundtrip = casted.cast(source.dtype, strict=True).alias(col)
                if not source.equals(roundtrip, check_dtypes=True):
                    raise ValueError(
                        f"Compaction would change values in column {col!r} "
                        f"while casting {source.dtype!r} to {dtype!r}"
                    )
                exprs.append(casted)
            else:
                exprs.append(polars.col(col))
        else:
            exprs.append(polars.lit(None, dtype=dtype).alias(col))
    return df.select(exprs)


def concat_with_union(a: polars.DataFrame, b: polars.DataFrame) -> polars.DataFrame:
    """Vertically concatenate two frames with a unified schema.

    Computes the union of *a*'s and *b*'s schemas, aligns both frames to it
    (filling missing columns with nulls and widening conflicting dtypes), and
    then concatenates positionally.  After the union both frames have
    identical columns in identical positions, so the concat cannot fail with
    ``schema names differ``.
    """
    if a.height == 0:
        return b
    if b.height == 0:
        return a
    target = _union_schema_many([a, b])
    return polars.concat(
        [_align_to_schema(a, target), _align_to_schema(b, target)],
        how="vertical_relaxed",
    )


def concat_many_with_union(frames: List[polars.DataFrame]) -> polars.DataFrame:
    """Vertically concatenate N frames with a single unified schema.

    Equivalent to repeated :func:`concat_with_union` but computes the union
    schema once across all inputs (rather than re-deriving it pairwise), and
    issues a single ``polars.concat``.  Use this when merging an arbitrary
    set of parquet files with potentially different / dynamic column sets
    (e.g. GA4-style ``param_*`` dynamic columns where each batch contains a
    different subset of keys).

    Semantics:
      - Empty frames are skipped.
      - If all frames are empty, an empty frame with the union schema is returned.
      - If no frames are given, an empty zero-column frame is returned.

    Note on memory: this materialises every input frame in memory at once.
    Chunked callers reduce simultaneous frame retention but are not a hard
    bound while each Parquet source is still decoded whole.
    """
    if not frames:
        return polars.DataFrame()
    non_empty = [f for f in frames if f.height > 0]
    if not non_empty:
        # All inputs are empty — return an empty frame carrying the union schema
        target = _union_schema_many(frames)
        return polars.DataFrame(schema=target)
    if len(non_empty) == 1:
        # Still project to its own schema explicitly so the output dtype map is
        # the same shape as the multi-frame path (callers can rely on it).
        target = _union_schema_many(non_empty)
        return _align_to_schema(non_empty[0], target)
    target = _union_schema_many(non_empty)
    aligned = [_align_to_schema(f, target) for f in non_empty]
    return polars.concat(aligned, how="vertical_relaxed")


# =========================
# Safe storage I/O helpers
# =========================

def _safe_exists(
        path: str,
        profiler: Optional[Profiler] = None,
        strict: bool = False,
        storage: Optional[object] = None,
) -> bool:
    p = profiler or get_null_profiler()
    try:
        with p.span("io.exists"):
            active_storage = cast(
                StorageInterface,
                storage if storage is not None else _get_storage(),
            )
            return active_storage.exists(path)
    except Exception:
        # A failed existence probe is normally treated as "absent" (lenient).
        # *strict* callers (carry-forward reads) must not mistake a backend
        # error for a genuine absence, so re-raise instead.
        if strict:
            raise
        return False


def _read_parquet_safe(
        path: str,
        profiler: Optional[Profiler] = None,
        file_size: int = 0,
        columns: Optional[List[str]] = None,
        required: bool = False,
        storage: Optional[object] = None,
) -> Optional[polars.DataFrame]:
    """Read a parquet object into polars, or ``None`` when it is absent.

    When *required* is True, both absence and a genuine read failure are raised.
    Callers use that mode only for objects referenced by the locked current
    snapshot, where treating NotFound as an empty artifact/file would truncate a
    deletion decision and can resurrect rows.  Optional/race-tolerant callers use
    the default mode and continue to receive ``None`` for absence/failure.
    """
    p = profiler or get_null_profiler()
    if storage is not None:
        # Keep the caller-pinned backend through both EXISTS and GET. Besides
        # preventing a probe fallback from switching storage namespaces, this
        # also prevents a factory/config change between the two operations.
        exists = _safe_exists(
            path,
            profiler=profiler,
            strict=required,
            storage=storage,
        )
    else:
        exists = _safe_exists(path, profiler=p, strict=required)
    if not exists:
        logging.info(f"[race] file already sunset by another writer: {path}")
        if required:
            raise FileNotFoundError(f"Required parquet object is missing: {path}")
        return None
    try:
        active_storage = cast(
            StorageInterface,
            storage if storage is not None else _get_storage(),
        )
        with p.span("io.read_parquet"):
            # Project to *columns* when given so only those column chunks are
            # read (memory-bound fallback); gated so storages/test doubles that
            # only accept ``path`` keep working on the unprojected paths.
            tbl = (
                active_storage.read_parquet(path, columns=columns)
                if columns else active_storage.read_parquet(path)
            )  # -> pyarrow.Table
        with p.span("io.arrow_to_polars"):
            df = polars.from_arrow(tbl)
        p.add("files_read", 1)
        p.add("bytes_read", int(file_size))
        p.add("rows_read", int(df.height))
        return df
    except FileNotFoundError:
        logging.info(f"[race] file vanished before read: {path}")
        if required:
            raise
        return None
    except Exception as e:
        logging.warning(f"[read] failed to read parquet at {path}: {e}")
        if required:
            raise
        return None


_ORIGINAL_READ_PARQUET_SAFE = _read_parquet_safe


# Besides decoded bytes, cap the Python/Polars metadata retained before the
# once-per-output concat.  Very narrow rows otherwise permit millions of tiny
# frames to fit under a byte-only budget.
_MAX_COMPACTION_CHUNK_FRAMES = 128
_MAX_COMPACTION_CHUNK_ROWS = 1_048_576

# ``max_memory_chunk_size`` is an encoded-output target, while the packer
# retains decoded Polars frames.  A one-to-one default makes a 16 MiB target
# impossible to reach for ordinary compressible data, especially after the
# total budget is apportioned across pending encoders and the coordinator.
# The audited corpus reaches 11.64 decoded bytes per encoded byte. Reserve
# twelve decoded bytes per target byte by default, but keep the implicit
# process-wide frame budget finite. Workloads outside this envelope can set
# ``max_decoded_compaction_bytes`` explicitly; that value remains authoritative.
_DEFAULT_COMPACTION_DECODED_EXPANSION = 12
_DEFAULT_COMPACTION_DECODED_BUDGET_CAP = 1024 * 1024 * 1024
_DEFAULT_COMPACTION_MEMORY_FRACTION_DENOMINATOR = 4
_DEFAULT_COMPACTION_UNKNOWN_MEMORY_CAP = 128 * 1024 * 1024


def _host_physical_memory_bytes() -> Optional[int]:
    """Return installed host RAM using only bounded stdlib observations."""
    try:
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        total = pages * page_size
        return total if total > 0 else None
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _detect_compaction_hard_memory_bytes(
        *,
        proc_cgroup_path: str = "/proc/self/cgroup",
        cgroup_root: str = "/sys/fs/cgroup",
) -> Optional[int]:
    """Return the smaller positive host/cgroup-v2 hard-memory boundary.

    The cgroup path is resolved beneath the configured mount root and rejected
    if it escapes.  Missing, malformed, or unlimited cgroup state never invents
    a boundary; host RAM remains the fallback observation.
    """
    host_total = _host_physical_memory_bytes()
    cgroup_limit: Optional[int] = None
    try:
        with open(proc_cgroup_path, "r", encoding="utf-8") as handle:
            cgroup_lines = handle.read().splitlines()
    except (OSError, UnicodeError):
        cgroup_lines = []

    relative = None
    for line in cgroup_lines:
        fields = line.split(":", 2)
        if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
            relative = fields[2].lstrip("/")
            break

    root = os.path.realpath(os.path.abspath(cgroup_root))
    candidates = []
    if relative is not None:
        directory = os.path.realpath(os.path.join(root, relative))
        try:
            contained = os.path.commonpath((root, directory)) == root
        except ValueError:
            contained = False
        if contained:
            while True:
                candidates.append(directory)
                if directory == root:
                    break
                parent = os.path.dirname(directory)
                if parent == directory:
                    break
                directory = parent
    if root not in candidates:
        candidates.append(root)
    seen = set()
    for directory in candidates:
        if directory in seen:
            continue
        seen.add(directory)
        try:
            if os.path.commonpath((root, directory)) != root:
                continue
        except ValueError:
            continue
        try:
            with open(
                os.path.join(directory, "memory.max"),
                "r",
                encoding="ascii",
            ) as handle:
                raw = handle.read().strip()
        except (OSError, UnicodeError):
            continue
        if raw == "max":
            continue
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            cgroup_limit = (
                parsed if cgroup_limit is None else min(cgroup_limit, parsed)
            )

    observed = [
        value for value in (host_total, cgroup_limit)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    ]
    return min(observed) if observed else None


def _resolve_compaction_decoded_budget(
        table_config: Optional[dict],
        *,
        encoded_target_bytes: int,
        retained_frame_slots: int,
) -> Tuple[int, bool, bool]:
    """Return ``(budget, defaulted, capped)`` for decoded compaction frames.

    An explicit per-table budget is used exactly (apart from the historical
    one-byte lower bound).  When it is absent or malformed, derive a bounded
    budget that gives every retained frame slot enough room for a typically
    12:1-compressed target-sized output instead of dividing the encoded target
    itself among all slots.
    """
    cfg = table_config or {}
    configured = cfg.get("max_decoded_compaction_bytes")
    if configured is not None:
        try:
            return max(1, int(configured)), False, False
        except (TypeError, ValueError):
            # Direct processing callers may bypass DataWriter's configuration
            # validation.  Preserve the established safe-fallback behaviour.
            pass

    target = max(1, int(encoded_target_bytes))
    slots = max(1, int(retained_frame_slots))
    desired = target * _DEFAULT_COMPACTION_DECODED_EXPANSION * slots
    hard_memory = _detect_compaction_hard_memory_bytes()
    memory_cap = (
        max(1, hard_memory // _DEFAULT_COMPACTION_MEMORY_FRACTION_DENOMINATOR)
        if hard_memory is not None
        else _DEFAULT_COMPACTION_UNKNOWN_MEMORY_CAP
    )
    implicit_cap = min(_DEFAULT_COMPACTION_DECODED_BUDGET_CAP, memory_cap)
    budget = min(desired, implicit_cap)
    return max(1, budget), True, desired > budget


class _OptionalParquetStreamUnavailable(Exception):
    """An optional maintenance source could not be opened before any rows."""


def _iter_parquet_frames_safe(
        path: str,
        *,
        max_decoded_bytes: int,
        profiler: Optional[Profiler] = None,
        file_size: int = 0,
        columns: Optional[List[str]] = None,
        required: bool = False,
) -> Optional[Iterator[polars.DataFrame]]:
    """Stream one Parquet source through a bounded spill/batch reader.

    Built-in storage adapters use ``StorageInterface.iter_parquet_batches``.
    Compatibility test doubles and third-party adapters retain the whole-file
    reader, but their decoded frames are sliced before entering the compaction
    packer.  Once a streamed source yields any rows, a later read failure is
    fatal even for best-effort maintenance: keeping the original alongside a
    partial successor would duplicate data.
    """
    p = profiler or get_null_profiler()
    # Respect injected/custom readers.  Besides keeping third-party adapters
    # compatible, this avoids silently bypassing a caller's integrity wrapper
    # merely because the built-in storage object also supports streaming.
    if _read_parquet_safe is not _ORIGINAL_READ_PARQUET_SAFE:
        frame = _read_parquet_safe(
            path,
            profiler=p,
            file_size=file_size,
            columns=columns,
            required=required,
        )
        return None if frame is None else iter((frame,))
    if not _safe_exists(path, profiler=p, strict=required):
        if required:
            raise FileNotFoundError(f"Required parquet object is missing: {path}")
        return None
    storage = _get_storage()

    stream_capable = False
    if isinstance(storage, StorageInterface):
        storage_type = type(storage)
        stream_capable = (
            storage_type.iter_parquet_batches
            is not StorageInterface.iter_parquet_batches
            or (
                storage_type.stat_object is not StorageInterface.stat_object
                and storage_type.download_to_file
                is not StorageInterface.download_to_file
            )
        )
    if not stream_capable:
        frame = _read_parquet_safe(
            path,
            profiler=p,
            file_size=file_size,
            columns=columns,
            required=required,
        )
        return None if frame is None else iter((frame,))

    def _generate() -> Iterator[polars.DataFrame]:
        yielded = False
        rows = 0
        try:
            with p.span("io.iter_parquet"):
                for batch in storage.iter_parquet_batches(
                    path,
                    max_decoded_bytes=max(1, int(max_decoded_bytes)),
                    columns=columns,
                ):
                    frame = polars.from_arrow(batch)
                    rows += frame.height
                    yielded = yielded or frame.height > 0
                    if frame.height:
                        yield frame
            p.add("files_read", 1)
            p.add("bytes_read", int(file_size))
            p.add("rows_read", rows)
        except FileNotFoundError:
            if required or yielded:
                raise
            logging.info(f"[race] file vanished before streaming read: {path}")
            raise _OptionalParquetStreamUnavailable(path)
        except Exception as exc:
            if required or yielded:
                raise
            logging.warning(f"[read] failed to stream parquet at {path}: {exc}")
            raise _OptionalParquetStreamUnavailable(path) from exc

    return _generate()


# =========================
# Original-style merge threshold logic
# =========================

def is_file_in_overlapping_files(file: str, overlapping_files: Set[Tuple[str, bool, int]]) -> bool:
    for f, _, _ in overlapping_files:
        if f == file:
            return True
    return False


def prune_not_overlapping_files_by_threshold(
        overlapping_files: Set[Tuple[str, bool, int]],
        table_config: Optional[dict] = None,
) -> Set[Tuple[str, bool, int]]:
    """
    Policy:
      - Always include entries with has_overlap=True
      - For has_overlap=False small files, include them only if either:
          total_size_of_all_candidates > MAX_MEMORY_CHUNK_SIZE
          OR count_of_false_items >= MAX_OVERLAPPING_FILES
        When the gate opens, ALL false items are included (downstream
        compaction handles chunked flushing at memory boundaries, so we
        must not drop files here).

    Limits are resolved per-table (table_config) with fallback to global default.
    """
    max_mem, max_files = _resolve_limits(table_config)

    total_size = sum(item[2] for item in overlapping_files)
    total_false = len([item for item in overlapping_files if item[1] is False])

    # Always keep all True (overlapping) items
    result: Set[Tuple[str, bool, int]] = set([item for item in overlapping_files if item[1] is True])

    # Gate: only pull in False items if thresholds hit
    if total_size > max_mem or total_false >= max_files:
        # Include ALL false items — downstream handles chunked flushing
        for item in overlapping_files:
            if item[1] is False:
                result.add(item)

    return result


def should_compact_small_files(
        resources: List[Dict],
        table_config: Optional[dict] = None,
) -> bool:
    """Return True when accumulated small files trip the auto-compaction gate.

    Mirrors the threshold in ``prune_not_overlapping_files_by_threshold``: a
    file is "small" when its ``file_size`` is strictly smaller than
    ``max_memory_chunk_size``.  The gate opens when EITHER the small-file count
    reaches ``max_overlapping_files`` OR the combined small-file size exceeds
    ``max_memory_chunk_size``.  Files already at/above the chunk size are big
    enough on their own and are never counted.

    ``resources`` is a snapshot's resource list (dicts with ``file`` /
    ``file_size``).  Limits resolve per-table via ``_resolve_limits``.
    """
    max_mem, max_files = _resolve_limits(table_config)
    small_sizes = [
        int(r.get("file_size") or 0)
        for r in (resources or [])
        if r.get("file") and int(r.get("file_size") or 0) < max_mem
    ]
    if not small_sizes:
        return False
    return len(small_sizes) >= max_files or sum(small_sizes) > max_mem


# =========================
# Public API: Overlap selection (with compaction triggers)
# =========================

def find_overlapping_files(  # keep name/signature for compatibility
        last_simple_table: dict,
        df: polars.DataFrame,
        overwrite_columns: List[str],
        locking: object = None,  # deprecated: kept for signature compatibility
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
) -> Set[Tuple[str, bool, int]]:
    """
    Builds the candidate set:
      - has_overlap=True for every existing file when overwrite_columns are given
        (snapshots carry no per-file key statistics, so non-overlap can't be proven)
      - has_overlap=False for small files in the pure-compaction path (< MAX_MEMORY_CHUNK_SIZE)
    Then applies prune_not_overlapping_files_by_threshold to decide the final merge set.

    Limits are resolved per-table (table_config) with fallback to global default.

    NOTE:
      - No per-file locking here (consistent with new locking model).
      - Return: set of tuples (file_path, has_overlap: bool, file_size)
    """
    p = profiler or get_null_profiler()
    resources = last_simple_table.get("resources", {}) or {}
    overlapping_files: Set[Tuple[str, bool, int]] = set()
    p.add("resources_total", len(resources))

    if overwrite_columns:
        # Snapshots carry no per-file key statistics, so a file cannot be proven
        # free of the incoming keys.  Every existing file is therefore a
        # delete/overwrite candidate and must be scanned for matching rowids.
        t0 = time.perf_counter()
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            overlapping_files.add((file, True, file_size))
        p.mark("overlap.scan_resources", t0)

    else:
        # No overwrite columns → pure compaction path for small files
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            _max_mem, _ = _resolve_limits(table_config)
            if file_size < _max_mem:
                overlapping_files.add((file, False, file_size))

    # Apply pruning logic to trigger compaction when many/large small files accumulate
    with p.span("overlap.prune"):
        overlapping_files = prune_not_overlapping_files_by_threshold(overlapping_files, table_config=table_config)

    p.add("overlap_files_true", sum(1 for _, ov, _ in overlapping_files if ov))
    p.add("overlap_files_false", sum(1 for _, ov, _ in overlapping_files if not ov))
    p.add("overlap_files_total_bytes", sum(sz for _, _, sz in overlapping_files))

    # Per-file locks removed intentionally; higher-level simple/table lock handles concurrency
    return overlapping_files


# =========================
# Public API: standalone compaction (no incoming data)
# =========================

def _validate_compaction_source_rowids(
        frame: polars.DataFrame,
        file_path: str,
        *,
        required: bool,
) -> Optional[polars.Series]:
    """Validate a source rowid lane before it can be repacked.

    Rowid-less legacy files remain supported for ordinary compaction.  A file
    named by a deletion vector, however, must carry the exact canonical lane;
    otherwise the vector cannot be consumed safely.
    """
    folded = [
        column for column in frame.columns
        if str(column).casefold() == ROWID_COL.casefold()
    ]
    if not folded:
        if required:
            raise ValueError(
                f"Cannot drain deletion-vector: {file_path!r} lacks "
                f"{ROWID_COL!r}"
            )
        return None
    if folded != [ROWID_COL]:
        raise ValueError(
            f"Compaction encountered an ambiguous reserved rowid column in "
            f"{file_path!r}"
        )
    rowids = frame.get_column(ROWID_COL)
    if rowids.dtype != polars.Int64:
        raise ValueError(
            f"Compaction requires canonical {ROWID_COL} Int64 in {file_path!r}"
        )
    if rowids.null_count() > 0:
        raise ValueError(
            f"Compaction cannot consume NULL rowids in {file_path!r}"
        )
    minimum = rowids.min()
    if minimum is None or minimum <= 0:
        raise ValueError(
            f"Compaction requires positive rowids in {file_path!r}"
        )
    if rowids.n_unique() != frame.height:
        raise ValueError(
            f"Compaction found duplicate rowids in {file_path!r}"
        )
    return rowids


def _prove_safe_compacted_rowids(frame: polars.DataFrame) -> None:
    """Prove that one final output cannot make a future DV over-delete."""
    try:
        _validate_compaction_source_rowids(
            frame, "compaction output", required=False,
        )
    except ValueError as exc:
        # Keep the long-standing compaction diagnostic: callers and runbooks
        # need to distinguish unsafe cross-file identity collapse from a
        # malformed rowid lane in one source file.
        if "duplicate rowids" in str(exc):
            raise ValueError(
                "Compaction would merge duplicate __rowid__ values into one "
                "file and make future tombstones over-delete live rows"
            ) from exc
        raise


def _cleanup_compaction_outputs(resources: List[Dict]) -> None:
    """Best-effort removal of only objects minted by one compaction attempt.

    This helper is called while preserving an earlier mutation failure, so no
    storage lookup, malformed resource entry, delete, or diagnostic logging
    failure may escape and replace that original error.
    """
    if not resources:
        return
    try:
        storage = _get_storage()
    except BaseException as cleanup_error:
        try:
            logging.warning(
                "[compaction] could not resolve storage for output cleanup: %s",
                cleanup_error,
            )
        except BaseException:
            pass
        return
    seen: Set[str] = set()
    for resource in resources:
        try:
            generated = (
                resource.get("file") if isinstance(resource, dict) else None
            )
        except BaseException:
            continue
        if not isinstance(generated, str) or not generated or generated in seen:
            continue
        seen.add(generated)
        _cleanup_unpublished_parquet_path(storage, generated)


def _compact_resources_with_tombstones(
        *,
        snapshot: dict,
        tombstone_df: polars.DataFrame,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict],
        small_only: bool,
        required_reads: bool,
        profiler: Profiler,
        footer_md_out: Optional[Dict],
) -> Tuple[int, int, List[Dict], Set[str], polars.DataFrame]:
    """Fuse DV draining and small-file packing into one physical pass.

    A vector-referenced source is decoded at most once and its survivors are
    fed directly into the final target-sized packer.  No per-source successor
    is uploaded and then downloaded by a second phase.  Tombstone identity is
    all-or-nothing per source: an unprovable group keeps both its original file
    and the complete group in the residual vector.

    The byte target is a proportional estimate based on immutable source file
    sizes.  It controls output packing, not the decoded-memory footprint; a
    single highly-compressed or skewed row can still exceed the target.
    """
    p = profiler

    def _profile_span(name: str):
        span = getattr(p, "span", None)
        return span(name) if callable(span) else nullcontext()

    def _merge_profile(other: Profiler) -> None:
        merge = getattr(p, "merge", None)
        if callable(merge):
            merge(other)
            return
        # ``compact_resources`` historically accepted lightweight profiler
        # duck types that expose only ``add``. Preserve that contract after
        # routing ordinary compaction through the fused bounded packer.
        for name, value in other.emit_counts().items():
            p.add(name, value)
    tombstone_df = validate_tombstone_frame(
        tombstone_df, source="deletion-vector passed to fused compaction",
    )
    resources = snapshot.get("resources") or []

    by_path: Dict[str, Dict] = {}
    ordered_resources: List[Dict] = []
    for resource in resources:
        path = resource.get("file") if isinstance(resource, dict) else None
        if not path:
            continue
        if path in by_path:
            raise ValueError(
                f"Compaction snapshot contains duplicate resource path {path!r}"
            )
        by_path[path] = resource
        ordered_resources.append(resource)

    grouped_raw = tombstone_df.partition_by(
        TOMBSTONE_FILE_COL, as_dict=True, maintain_order=False,
    )
    grouped: Dict[str, polars.DataFrame] = {}
    for key, group in grouped_raw.items():
        path = key[0] if isinstance(key, tuple) else key
        grouped[str(path)] = group
    p.add("tombstone_files_total", len(grouped))
    p.add("compact_fused", 1)

    residual_parts: List[polars.DataFrame] = [
        group for path, group in grouped.items() if path not in by_path
    ]
    max_bytes, _max_files = _resolve_limits(table_config)
    max_bytes = max(1, int(max_bytes))

    candidates: List[Tuple[Dict, bool]] = []
    small_candidate_paths: Set[str] = set()
    large_tombstone_candidate_paths: Set[str] = set()
    for resource in ordered_resources:
        path = str(resource["file"])
        file_size = max(0, int(resource.get("file_size") or 0))
        has_tombstones = path in grouped
        if has_tombstones or not small_only or file_size < max_bytes:
            candidates.append((resource, has_tombstones))
            if file_size < max_bytes:
                small_candidate_paths.add(path)
            elif has_tombstones:
                # A DV-referenced source is mandatory even when it is larger
                # than the small-file target. Keep it out of the small-file
                # telemetry lane so operators can distinguish the two costs.
                large_tombstone_candidate_paths.add(path)
    p.add("compact_candidates_total", len(candidates))
    p.add("compact_small_candidates", len(small_candidate_paths))
    p.add(
        "compact_large_tombstone_candidates",
        len(large_tombstone_candidate_paths),
    )

    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()
    total_rows = 0
    removed_rows = 0
    local_footer_cache: Dict = {}
    chunk_parts: List[polars.DataFrame] = []
    chunk_rows = 0
    chunk_estimated_bytes = 0
    chunk_decoded_bytes = 0
    peak_decoded_buffer_bytes = 0
    packing_limit = max_bytes
    observed_expansion = 1.0
    packing_calibrated = False

    cfg = table_config or {}
    try:
        configured_workers = int(
            cfg.get("tombstone_compaction_workers")
            or getattr(default, "TOMBSTONE_COMPACTION_WORKERS", 2)
        )
    except (TypeError, ValueError):
        configured_workers = 2
    # Keep one core available for the coordinator's read/union/anti-join work.
    # On the supported 4-CPU envelope, three PyArrow encoders were both faster
    # and lower-RSS than four; larger pools only add allocator contention.
    encode_workers = max(1, min(configured_workers, 3))
    executor = (
        ThreadPoolExecutor(max_workers=encode_workers)
        if encode_workers > 1 else None
    )
    # Pending encoders retain their input frames.  The coordinator can briefly
    # hold both the streamed source buffers and the once-concatenated output,
    # so reserve two decoded-frame slots in addition to every encoder slot.
    # An explicit decoded budget remains a hard shared frame bound.  The
    # derived default, however, is based on the encoded target *per slot*;
    # dividing the encoded target itself here was what produced hundreds of
    # sub-megabyte files from an otherwise 16-MiB packing campaign.
    retained_frame_slots = (encode_workers + 2) if executor else 2
    (
        max_decoded_budget,
        decoded_budget_defaulted,
        decoded_budget_capped,
    ) = _resolve_compaction_decoded_budget(
        cfg,
        encoded_target_bytes=max_bytes,
        retained_frame_slots=retained_frame_slots,
    )
    decoded_chunk_limit = max(
        1,
        max_decoded_budget // retained_frame_slots,
    )
    pending_writes: List[Any] = []
    p.add("compact_encode_worker_capacity", encode_workers if executor else 1)
    p.add("compact_decoded_budget_bytes", max_decoded_budget)
    p.add("compact_decoded_chunk_limit_bytes", decoded_chunk_limit)
    p.add("compact_decoded_budget_defaulted", int(decoded_budget_defaulted))
    p.add("compact_decoded_budget_capped", int(decoded_budget_capped))
    encode_state_lock = threading.Lock()
    active_encodes = 0
    max_active_encodes = 0
    encode_calls = 0

    def _write_final_chunk(
            merged: polars.DataFrame,
            estimated: int,
    ) -> Tuple[List[Dict], Dict, Profiler, int, int]:
        """Encode one independent final chunk with worker-local telemetry."""
        nonlocal active_encodes, max_active_encodes, encode_calls
        with encode_state_lock:
            active_encodes += 1
            encode_calls += 1
            max_active_encodes = max(max_active_encodes, active_encodes)
        sub = Profiler()
        resources: List[Dict] = []
        footer_cache: Dict = {}
        try:
            write_parquet_and_collect_resources(
                write_df=merged,
                overwrite_columns=[],
                data_dir=data_dir,
                new_resources=resources,
                compression_level=compression_level,
                profiler=sub,
                footer_md_out=footer_cache,
            )
            return resources, footer_cache, sub, merged.height, int(estimated)
        finally:
            with encode_state_lock:
                active_encodes -= 1

    def _accept_write(result: Tuple[List[Dict], Dict, Profiler, int, int]) -> None:
        nonlocal total_rows, packing_limit, observed_expansion
        resources, footer_cache, sub, rows, estimated = result
        _merge_profile(sub)
        new_resources.extend(resources)
        local_footer_cache.update(footer_cache)
        total_rows += rows
        p.add("compact_estimated_output_bytes", estimated)
        actual_total = sum(
            max(0, int(resource.get("file_size") or 0))
            for resource in resources
        )
        if estimated > 0 and actual_total > 0:
            expansion = actual_total / estimated
            if expansion > observed_expansion:
                observed_expansion = expansion
                # The source-byte estimate can understate a unioned output
                # because missing columns and new row-group boundaries add
                # encoded overhead.  Calibrate from actual uploaded bytes; a
                # 1% guard absorbs integer row slicing without chronically
                # underfilling schema-stable files.
                packing_limit = max(
                    1,
                    int(max_bytes / (observed_expansion * 1.01)),
                )
                p.add("compact_packing_calibrations", 1)
        for resource in resources:
            actual = max(0, int(resource.get("file_size") or 0))
            if actual > max_bytes:
                p.add("compact_output_oversize_bytes", actual - max_bytes)
                p.add("compact_output_oversize_files", 1)

    def _harvest_one() -> None:
        future = pending_writes.pop(0)
        _accept_write(future.result())

    def _harvest_all() -> None:
        while pending_writes:
            _harvest_one()

    def _flush_chunk() -> None:
        nonlocal chunk_parts, chunk_rows
        nonlocal chunk_estimated_bytes, chunk_decoded_bytes
        nonlocal packing_calibrated
        if not chunk_parts:
            return
        with _profile_span("compact.concat"):
            merged = concat_many_with_union(chunk_parts)
        chunk_parts = []
        chunk_rows = 0
        estimated = chunk_estimated_bytes
        chunk_estimated_bytes = 0
        decoded = chunk_decoded_bytes
        chunk_decoded_bytes = 0
        if merged.height == 0:
            return
        p.add("compact_decoded_bytes_flushed", decoded)
        _prove_safe_compacted_rowids(merged)
        # The first output is an intentional synchronous calibration sample.
        # Subsequent PyArrow-compatible chunks use bounded outer parallelism;
        # native Polars chunks already use the global Rust worker pool and stay
        # synchronous to avoid oversubscription.
        native_eligible, _reason = _native_polars_parquet_eligibility(merged)
        if not packing_calibrated:
            _accept_write(_write_final_chunk(merged, estimated))
            packing_calibrated = True
        elif executor is not None and not native_eligible:
            if len(pending_writes) >= encode_workers:
                _harvest_one()
            pending_writes.append(
                executor.submit(
                    copy_context().run,
                    _write_final_chunk,
                    merged,
                    estimated,
                )
            )
        else:
            # Do not overlap a native/global-pool encode with queued PyArrow
            # work. This keeps the CPU and memory cap deterministic.
            _harvest_all()
            _accept_write(_write_final_chunk(merged, estimated))

    def _pack(frame: polars.DataFrame, estimated_bytes: int) -> None:
        """Pack slices under both encoded-output and decoded-RSS budgets."""
        nonlocal chunk_rows, chunk_estimated_bytes, chunk_decoded_bytes
        nonlocal peak_decoded_buffer_bytes
        if frame.height == 0:
            return
        remaining_offset = 0
        remaining_rows = frame.height
        remaining_estimate = max(1, int(estimated_bytes))
        remaining_decoded = max(1, int(frame.estimated_size()))
        while remaining_rows > 0:
            if (
                len(chunk_parts) >= _MAX_COMPACTION_CHUNK_FRAMES
                or chunk_rows >= _MAX_COMPACTION_CHUNK_ROWS
            ):
                _flush_chunk()
            encoded_capacity = packing_limit - chunk_estimated_bytes
            decoded_capacity = decoded_chunk_limit - chunk_decoded_bytes
            if encoded_capacity <= 0 or decoded_capacity <= 0:
                _flush_chunk()
                encoded_capacity = packing_limit
                decoded_capacity = decoded_chunk_limit

            take_rows = remaining_rows
            take_rows = min(
                take_rows,
                _MAX_COMPACTION_CHUNK_ROWS - chunk_rows,
            )
            if remaining_estimate > encoded_capacity:
                take_rows = min(
                    take_rows,
                    (remaining_rows * encoded_capacity) // remaining_estimate,
                )
            if remaining_decoded > decoded_capacity:
                take_rows = min(
                    take_rows,
                    (remaining_rows * decoded_capacity) // remaining_decoded,
                )
            if take_rows <= 0:
                if chunk_parts:
                    _flush_chunk()
                    continue
                # One physical row is the indivisible lower bound.
                take_rows = 1
            take_rows = min(remaining_rows, max(1, take_rows))
            take_estimate = max(
                1, (remaining_estimate * take_rows) // remaining_rows,
            )
            piece = frame.slice(remaining_offset, take_rows)
            piece_decoded = max(1, int(piece.estimated_size()))
            if chunk_parts and (
                piece_decoded > decoded_capacity
                or take_estimate > encoded_capacity
            ):
                _flush_chunk()
                continue
            if piece_decoded > decoded_chunk_limit:
                p.add("compact_decoded_oversize_rows", take_rows)

            chunk_parts.append(piece)
            chunk_rows += take_rows
            chunk_estimated_bytes += take_estimate
            chunk_decoded_bytes += piece_decoded
            p.add(
                "compact_decoded_peak_buffer_bytes",
                max(0, chunk_decoded_bytes - peak_decoded_buffer_bytes),
            )
            peak_decoded_buffer_bytes = max(
                peak_decoded_buffer_bytes, chunk_decoded_bytes,
            )
            remaining_offset += take_rows
            remaining_rows -= take_rows
            remaining_estimate = max(
                0, remaining_estimate - take_estimate,
            )
            remaining_decoded = max(
                0, remaining_decoded - piece_decoded,
            )
            if (
                chunk_estimated_bytes >= packing_limit
                or chunk_decoded_bytes >= decoded_chunk_limit
                or len(chunk_parts) >= _MAX_COMPACTION_CHUNK_FRAMES
                or chunk_rows >= _MAX_COMPACTION_CHUNK_ROWS
            ):
                _flush_chunk()

    try:
        for resource, has_tombstones in candidates:
            file_path = str(resource["file"])
            file_size = max(0, int(resource.get("file_size") or 0))
            file_tombstones = grouped.get(file_path)

            # Exact fully-dead shortcut: the row-count seal only admits the
            # projection lane; identity equality is still proved from rowids.
            if has_tombstones and file_tombstones is not None:
                try:
                    declared_rows = int(resource.get("rows"))
                except (TypeError, ValueError):
                    declared_rows = -1
                if declared_rows == file_tombstones.height:
                    projected = _read_parquet_safe(
                        file_path,
                        profiler=p,
                        file_size=file_size,
                        columns=[ROWID_COL],
                        required=True,
                    )
                    _validate_compaction_source_rowids(
                        projected, file_path, required=True,
                    )
                    physical = projected.select(ROWID_COL)
                    dead_ids = file_tombstones.select(ROWID_COL)
                    if (
                        physical.join(dead_ids, on=ROWID_COL, how="anti").height == 0
                        and dead_ids.join(
                            physical, on=ROWID_COL, how="anti"
                        ).height == 0
                    ):
                        sunset_files.add(file_path)
                        removed_rows += file_tombstones.height
                        p.add("tombstone_fully_dead_fast_path", 1)
                        p.add("tombstone_files_touched", 1)
                        continue
                    residual_parts.append(file_tombstones)
                    p.add("tombstone_groups_residual", 1)
                    continue

            if not has_tombstones:
                # Clean candidates do not need a table-wide anti-join. Stream
                # them through a version-sealed spill file so compressed size
                # cannot dictate decoded RSS. Each frame enters the same fused
                # packer used by DV compaction and is concatenated only once at
                # flush time.
                frames = _iter_parquet_frames_safe(
                    file_path,
                    max_decoded_bytes=decoded_chunk_limit,
                    profiler=p,
                    file_size=file_size,
                    required=required_reads,
                )
                if frames is None:
                    continue
                try:
                    declared_rows = int(resource.get("rows") or 0)
                except (TypeError, ValueError):
                    declared_rows = 0
                streamed_rows = 0
                try:
                    for frame in frames:
                        _validate_compaction_source_rowids(
                            frame, file_path, required=False,
                        )
                        streamed_rows += frame.height
                        estimated = (
                            max(
                                1,
                                (file_size * frame.height + declared_rows - 1)
                                // declared_rows,
                            )
                            if file_size > 0 and declared_rows > 0
                            else max(1, int(frame.estimated_size()))
                        )
                        _pack(frame, estimated)
                except _OptionalParquetStreamUnavailable:
                    # Best-effort maintenance keeps an unreadable source live.
                    # No frame was yielded, so no successor contains its rows.
                    continue
                if declared_rows and streamed_rows != declared_rows:
                    raise RuntimeError(
                        f"Parquet row count changed while compacting {file_path!r}: "
                        f"expected {declared_rows}, streamed {streamed_rows}"
                    )
                sunset_files.add(file_path)
                continue

            existing_df = _read_parquet_safe(
                file_path,
                profiler=p,
                file_size=file_size,
                # A current snapshot file named by the DV is mandatory.  Clean
                # maintenance candidates preserve the legacy best-effort read.
                required=bool(has_tombstones or required_reads),
            )
            if existing_df is None:
                continue

            _validate_compaction_source_rowids(
                existing_df, file_path, required=has_tombstones,
            )
            original_rows = existing_df.height
            kept_df = existing_df
            if has_tombstones and file_tombstones is not None:
                dead_ids = file_tombstones.select(ROWID_COL)
                unmatched = dead_ids.join(
                    existing_df.select(ROWID_COL), on=ROWID_COL, how="anti",
                )
                if unmatched.height > 0:
                    # This path is blacklisted from the ordinary small pack: a
                    # residual must keep naming its original live resource.
                    residual_parts.append(file_tombstones)
                    p.add("tombstone_groups_residual", 1)
                    continue
                with _profile_span("tombstone.anti_join"):
                    kept_df = existing_df.join(
                        dead_ids, on=ROWID_COL, how="anti",
                    )
                removed = original_rows - kept_df.height
                if removed != file_tombstones.height:
                    raise RuntimeError(
                        f"Deletion-vector cardinality proof failed for "
                        f"{file_path!r}"
                    )
                removed_rows += removed
                p.add("tombstone_files_touched", 1)
                if kept_df.height:
                    p.add("tombstone_files_with_survivors", 1)
                    p.add("compact_intermediate_files_eliminated", 1)

            sunset_files.add(file_path)
            if kept_df.height == 0:
                continue
            if file_size > 0 and original_rows > 0:
                estimated = max(
                    1,
                    (file_size * kept_df.height + original_rows - 1)
                    // original_rows,
                )
            else:
                estimated = max(1, int(kept_df.estimated_size()))
            _pack(kept_df, estimated)

        _flush_chunk()
        _harvest_all()
    except BaseException:
        # A queued worker may already have uploaded a successful final chunk.
        # Join every worker and collect its resource identity so cleanup cannot
        # miss an unpublished object. Preserve the original exception.
        while pending_writes:
            future = pending_writes.pop(0)
            try:
                _accept_write(future.result())
            except BaseException:
                pass
        _cleanup_compaction_outputs(new_resources)
        raise
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        # Report observed concurrency separately from configured capacity.
        # Native Polars encodes are intentionally synchronous, so an all-native
        # run correctly reports one worker even when a larger pool was allowed.
        p.add("compact_encode_calls", encode_calls)
        p.add("compact_encode_workers", max_active_encodes)

    residual = (
        polars.concat(residual_parts, how="vertical")
        if residual_parts else _empty_tombstone_df()
    )
    residual = validate_tombstone_frame(
        residual, source="residual deletion-vector after fused compaction",
    )
    if removed_rows != tombstone_df.height - residual.height:
        _cleanup_compaction_outputs(new_resources)
        raise RuntimeError("Fused compaction tombstone accounting mismatch")
    try:
        p.add("compact_files_consumed_total", len(sunset_files))
        p.add(
            "compact_small_files_consumed",
            len(sunset_files.intersection(small_candidate_paths)),
        )
        p.add(
            "compact_large_tombstone_files_consumed",
            len(sunset_files.intersection(large_tombstone_candidate_paths)),
        )
        if footer_md_out is not None:
            footer_md_out.update(local_footer_cache)
    except BaseException:
        # Telemetry/cache publication is still pre-snapshot work. If a custom
        # profiler or mapping rejects it, none of this invocation's encoded
        # outputs may be left behind as unreferenced objects.
        _cleanup_compaction_outputs(new_resources)
        raise
    return (
        len(sunset_files), total_rows, new_resources, sunset_files, residual,
    )

def compact_resources(
        snapshot: dict,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        small_only: bool = True,
        dead_rowids: Optional[Set[int]] = None,
        dead_rowids_by_file: Optional[Dict[str, Set[int]]] = None,
        required_reads: bool = False,
        profiler: Optional[Profiler] = None,
        footer_md_out: Optional[Dict] = None,
        tombstone_df: Optional[polars.DataFrame] = None,
        return_residual: bool = False,
) -> Tuple:
    """Compact small parquet files in a snapshot's resources list.

    Reads files and rewrites them into target-sized chunks, **without needing
    incoming data**. Used by ``DataWriter.compact()`` and
    ``SimpleTable.export_to()``. The target is based on compressed source
    bytes; it is not a hard decoded-memory limit.

    Args:
        snapshot: the current snapshot dict (read by the caller — must
            contain a ``resources`` list of resource dicts with at least
            ``file`` and ``file_size``).
        data_dir: where to write the new compacted parquet files.
        compression_level: zstd compression level.
        table_config: per-table config dict (or None for global defaults);
            used by ``_resolve_limits`` to pick up
            ``max_memory_chunk_size`` and ``max_overlapping_files``.
        small_only: when True (default), only files **strictly smaller**
            than ``max_memory_chunk_size`` are considered for
            compaction — large files are left untouched. When False,
            every file is rewritten regardless of size.
        dead_rowids: optional set of ``__rowid__`` values to physically
            drop from the output. When provided, each source file is
            anti-joined against this set before buffering, so the written
            files contain no logically-deleted rows. Used by ``export_to``
            to bake the deletion-vector into a standalone copy. ``None``
            (default) preserves every row.
        dead_rowids_by_file: canonical deletion-vector identity mapping from
            source file key to its dead rowids. Prefer this over ``dead_rowids``:
            the same legacy/corrupt rowid in another file remains live.
        required_reads: when True, every snapshot-referenced source must be
            readable.  Logical materialisations such as export use this mode so
            a transient NotFound/backend error cannot produce a successful but
            incomplete copy.  Best-effort maintenance compaction keeps the
            default and conservatively leaves unreadable resources live.
        profiler: optional :class:`Profiler`.  When supplied, the reads of
            the small candidate files and the writes of the merged chunks are
            counted into the shared ``files_read``/``bytes_read``/
            ``files_written``/``bytes_written`` counters and ``io.*`` spans, so
            the auto-compaction I/O is attributable per write (the write path
            passes the live profiler; ``None`` -> no instrumentation).
        tombstone_df: optional canonical deletion vector. When supplied, DV
            draining and small-file packing are fused so each original source
            is decoded once and only final outputs are encoded. It cannot be
            combined with the legacy ``dead_rowids`` inputs.
        return_residual: required with ``tombstone_df``. Adds the exact
            unconsumed deletion vector as the fifth return value.

    Returns:
        Ordinarily a 4-tuple
        ``(considered, total_rows, new_resources, sunset_files)``:

          - ``considered`` — number of files that qualified for compaction
            (i.e. that would be sunset if at least one new file was written).
          - ``total_rows`` — total rows written into the new files.
          - ``new_resources`` — list of resource dicts for the freshly
            written files (matches the shape used by
            ``simple_table.update``).
          - ``sunset_files`` — set of file paths that were merged into
            the new files. The caller passes this set to
            ``simple_table.update`` so the resource list is correctly
            replaced.

        Fused DV mode returns the same four values plus ``residual_dv``. A
        vector group is consumed only after its complete rowid identity is
        proved; otherwise its original source remains live and its full group
        remains in that residual.

    Value-preservation properties enforced here:

      - Each source file is read **exactly once** via
        ``_read_parquet_safe`` (which returns ``None`` for races where
        another writer already sunset the file).
      - The merge is a row-preserving ``concat_with_union`` — no
        deduplication or implicit row drops. In fused mode only rowids proved
        by the canonical deletion vector are anti-joined before packing.
      - All columns from every source file are preserved: missing
        columns in any input are filled with ``null`` via
        ``concat_with_union``, never silently dropped.
      - Source files are added to ``sunset_files`` **only after** their
        rows have been successfully buffered into ``merged_df``. If a
        read fails (``_read_parquet_safe`` returns ``None``), the file
        is left in the snapshot — the next compaction retries it.
    """
    p = profiler or get_null_profiler()
    if tombstone_df is not None:
        if not return_residual:
            raise ValueError(
                "Fused tombstone compaction requires return_residual=True"
            )
        if dead_rowids is not None or dead_rowids_by_file is not None:
            raise ValueError(
                "tombstone_df cannot be combined with legacy dead_rowids inputs"
            )
        return _compact_resources_with_tombstones(
            snapshot=snapshot,
            tombstone_df=tombstone_df,
            data_dir=data_dir,
            compression_level=compression_level,
            table_config=table_config,
            small_only=small_only,
            required_reads=required_reads,
            profiler=p,
            footer_md_out=footer_md_out,
        )
    if dead_rowids is None and dead_rowids_by_file is None:
        # Ordinary legacy compaction shares the fused slice packer with DV
        # compaction.  An empty canonical vector selects only its clean-file
        # streaming lane, eliminating pairwise concatenation and whole-object
        # decode while retaining the historical four-field return shape.
        compacted = _compact_resources_with_tombstones(
            snapshot=snapshot,
            tombstone_df=_empty_tombstone_df(),
            data_dir=data_dir,
            compression_level=compression_level,
            table_config=table_config,
            small_only=small_only,
            required_reads=required_reads,
            profiler=p,
            footer_md_out=footer_md_out,
        )
        return compacted[:4]
    resources = snapshot.get("resources") or []
    if not resources:
        return 0, 0, [], set()

    max_mem, _max_files = _resolve_limits(table_config)

    # Classify candidates. Per ``small_only``, a file is a compaction
    # candidate when its ``file_size`` is < max_mem (small files create
    # the small-file accumulation problem this method exists to fix).
    # When ``small_only=False`` every file is a candidate.
    candidates: List[Tuple[str, int]] = []
    for resource in resources:
        file_path = resource.get("file")
        if not file_path:
            continue
        file_size = int(resource.get("file_size") or 0)
        if small_only and file_size >= max_mem:
            continue
        candidates.append((file_path, file_size))

    if not candidates:
        return 0, 0, [], set()

    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()
    total_rows = 0
    chunk_size_bytes = 0
    chunk_df: Optional[polars.DataFrame] = None

    p.add("compact_small_candidates", len(candidates))

    def _prove_safe_compacted_rowids(frame: polars.DataFrame) -> None:
        """Prevent composite file identity collapsing duplicate legacy IDs.

        A deletion vector distinguishes ``(source file, rowid)``. Once rows
        from several files are merged, the successor file becomes their shared
        identity; duplicate rowids inside it would make one future tombstone
        delete every colliding live row. Legacy-only chunks without any rowid
        column remain readable, but a modern/mixed chunk must prove the exact
        canonical invariant before it is written.
        """
        folded_rowids = [
            column for column in frame.columns
            if str(column).casefold() == ROWID_COL.casefold()
        ]
        if not folded_rowids:
            return
        if folded_rowids != [ROWID_COL]:
            raise ValueError(
                "Compaction encountered an ambiguous reserved rowid column"
            )
        rowids = frame.get_column(ROWID_COL)
        if rowids.dtype != polars.Int64:
            raise ValueError("Compaction requires canonical __rowid__ Int64")
        if rowids.null_count() > 0:
            raise ValueError(
                "Compaction cannot merge modern and rowid-less legacy rows"
            )
        minimum = rowids.min()
        if minimum is None or minimum <= 0:
            raise ValueError("Compaction requires positive __rowid__ values")
        if rowids.n_unique() != frame.height:
            raise ValueError(
                "Compaction would merge duplicate __rowid__ values into one "
                "file and make future tombstones over-delete live rows"
            )

    try:
        for file_path, file_size in candidates:
            existing_df = _read_parquet_safe(
                file_path,
                profiler=p,
                file_size=file_size,
                required=required_reads,
            )
            if existing_df is None:
                # Race: another writer already sunset this file. Skip and
                # leave it out of sunset_files — the snapshot still
                # references it; the next compaction will retry.
                continue

            # Physically drop logically-deleted rows when a deletion-vector
            # is supplied (export bakes the vector into the copy).
            has_deletion_vector = (
                dead_rowids_by_file is not None or dead_rowids is not None
            )
            if has_deletion_vector:
                if ROWID_COL not in existing_df.columns:
                    raise ValueError(
                        f"Cannot apply deletion-vector while materialising "
                        f"{file_path!r}: missing canonical {ROWID_COL!r} column"
                    )
                if existing_df.get_column(ROWID_COL).null_count() > 0:
                    raise ValueError(
                        f"Cannot apply deletion-vector while materialising "
                        f"{file_path!r}: NULL rowids are not allowed"
                    )
                file_dead_rowids = (
                    set(dead_rowids_by_file.get(file_path, set()))
                    if dead_rowids_by_file is not None else set(dead_rowids or set())
                )
                if file_dead_rowids:
                    existing_df = existing_df.filter(
                        ~polars.col(ROWID_COL).is_in(list(file_dead_rowids))
                    )

            if chunk_df is None or chunk_df.height == 0:
                chunk_df = (
                    existing_df if chunk_df is None
                    else concat_with_union(chunk_df, existing_df)
                )
            else:
                chunk_df = concat_with_union(chunk_df, existing_df)

            sunset_files.add(file_path)
            chunk_size_bytes += int(file_size or 0)

            if chunk_size_bytes >= max_mem:
                _prove_safe_compacted_rowids(chunk_df)
                total_rows += chunk_df.shape[0]
                write_parquet_and_collect_resources(
                    write_df=chunk_df,
                    overwrite_columns=[],
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                    footer_md_out=footer_md_out,
                )
                chunk_df = None
                chunk_size_bytes = 0

        # Final flush — if anything remains in the buffer, write it out.
        if chunk_df is not None and chunk_df.height > 0:
            _prove_safe_compacted_rowids(chunk_df)
            total_rows += chunk_df.shape[0]
            write_parquet_and_collect_resources(
                write_df=chunk_df,
                overwrite_columns=[],
                data_dir=data_dir,
                new_resources=new_resources,
                compression_level=compression_level,
                profiler=p,
                footer_md_out=footer_md_out,
            )
    except BaseException:
        # No successful output from a failed invocation can be referenced by a
        # snapshot. Roll back every exact path collected by this call for both
        # strict materialisation and best-effort maintenance compaction; source
        # and pre-existing target objects never enter ``new_resources``.
        _cleanup_compaction_outputs(new_resources)
        raise

    try:
        p.add("compact_files_consumed_total", len(sunset_files))
        p.add(
            "compact_small_files_consumed",
            len(
                sunset_files.intersection(
                    {path for path, size in candidates if size < max_mem}
                )
            ),
        )
    except BaseException:
        _cleanup_compaction_outputs(new_resources)
        raise
    return len(sunset_files), total_rows, new_resources, sunset_files


# =========================
# Write helpers
# =========================

def write_parquet_and_collect_resources(
        write_df, overwrite_columns, data_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
        footer_md_out: Optional[Dict] = None,
):
    """Write a DataFrame as a single Parquet file and append a resource dict.

    Sharding strategy:
      - If ``__timestamp__`` is present, the file is written into a Hive-style
        subdirectory ``data_dir/year=YYYY/month=MM/day=DD/`` whose date is the
        CURRENT write time — a single bucket for the whole frame, NOT a per-row
        split.  The folder only bounds how many files accumulate per directory;
        it is never read back as data.
      - If ``__timestamp__`` is absent (table has no dedup-on-read), the frame
        is written to the flat ``data_dir/`` as before.

    The folder is sharding metadata only: its date is derived from the path, the
    partition keys are NOT stored as columns inside the Parquet file, and the
    read side passes ``partitioning=None`` so they are never inferred either —
    the resource ``file`` path is the only thing the read side needs.  Crucially
    the bucket is NOT derived from each row's ``__timestamp__``: a compaction
    chunk whose rows span many days is still written as one file (it would
    otherwise be shredded into one tiny file per day, defeating compaction).
    ``__timestamp__`` itself stays in the body untouched.
    """
    if write_df.height == 0:
        return

    # --- Sharded write path: one current-time bucket, one file ----------
    # When ``__timestamp__`` is present the file is placed under a Hive-style
    # ``year=YYYY/month=MM/day=DD/`` folder.  That folder is PURELY a sharding
    # device to bound how many files pile up in a single directory — it is NOT
    # semantic: the read side passes ``partitioning=None`` (local_storage.py) and
    # locates every file by its resource path, never by the folder.  The bucket
    # is therefore derived from the CURRENT write time, NOT from each row's
    # ``__timestamp__``.
    #
    # Deriving it per-row (the old behaviour) shredded a memory-bounded compaction
    # chunk back into one tiny file per distinct row-day: merging small files whose
    # rows span N days emitted N small files instead of one ~16 MB file, so
    # compaction could never actually consolidate and the small-file gate stayed
    # permanently tripped.  Writing the whole chunk into a single current-time
    # bucket keeps it one ~chunk-sized file regardless of how many days its rows
    # span.  ``__timestamp__`` is untouched in the body — it stays the dedup
    # ORDER BY key and is hidden from query output by the read view's EXCLUDE
    # projection; only the *external folder* stops tracking it.
    if "__timestamp__" in write_df.columns:
        now = datetime.now(timezone.utc)
        partition_dir = os.path.join(
            data_dir,
            f"year={now.year}",
            f"month={now.month:02d}",
            f"day={now.day:02d}",
        )
        _write_single_parquet_file(
            write_df, overwrite_columns, partition_dir, new_resources, compression_level,
            profiler=profiler, footer_md_out=footer_md_out,
        )
    else:
        # --- Flat write path (no __timestamp__) — backward compatible ---
        _write_single_parquet_file(write_df, overwrite_columns, data_dir, new_resources, compression_level, profiler=profiler, footer_md_out=footer_md_out)


def _cleanup_unpublished_parquet_path(storage: object, path: str) -> None:
    """Best-effort release of one exact path after upload may have started.

    A remote PUT can persist an object and then lose its acknowledgement.  The
    UUID path is therefore considered potentially live as soon as it is handed
    to the pinned backend instance.  Cleanup must use that same instance (a
    storage factory may rotate clients) and must never replace the publication
    error that caused this rollback attempt.
    """
    try:
        delete = getattr(storage, "delete", None)
        if callable(delete):
            delete(path)
    except BaseException as cleanup_error:
        try:
            logging.warning(
                "[write] failed to clean unpublished parquet %s: %s",
                path,
                cleanup_error,
            )
        except BaseException:
            # Logging is diagnostic only. Even a hostile exception formatter
            # must not obscure the original upload/publication failure.
            pass


def _write_single_parquet_file(
        write_df, overwrite_columns, target_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
        footer_md_out: Optional[Dict] = None,
):
    """Write and publish one resource, cleaning any released orphan on error."""
    publication_state: Dict[str, Any] = {}
    try:
        return _write_single_parquet_file_attempt(
            write_df,
            overwrite_columns,
            target_dir,
            new_resources,
            compression_level,
            profiler=profiler,
            footer_md_out=footer_md_out,
            publication_state=publication_state,
        )
    except BaseException:
        if (
            publication_state.get("released") is True
            and publication_state.get("published") is not True
        ):
            storage = publication_state.get("storage")
            path = publication_state.get("path")
            if storage is not None and isinstance(path, str):
                _cleanup_unpublished_parquet_path(storage, path)
        raise


def _write_single_parquet_file_attempt(
        write_df, overwrite_columns, target_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
        footer_md_out: Optional[Dict] = None,
        *,
        publication_state: Dict[str, Any],
):
    """Write a single Parquet file into *target_dir* and append a resource entry.

    This is the low-level writer extracted from the original
    ``write_parquet_and_collect_resources``.  All Parquet encoding settings
    (zstd, dictionary, row-group size, statistics) are unchanged.
    """
    p = profiler or get_null_profiler()
    rows = write_df.shape[0]
    columns = write_df.shape[1]

    # Resolve the backend once. Apart from avoiding repeated factory/config
    # work, this is a correctness boundary: upload and metadata must describe
    # the same backend instance if a custom storage factory rotates clients.
    storage = _get_storage()
    state = publication_state
    state["storage"] = storage

    # Ensure the target directory exists.  makedirs is idempotent on local
    # storage and a no-op on object storage; calling it directly avoids a
    # pointless prefix HEAD (which always 404s) on object stores.
    with p.span("write.ensure_dir"):
        try:
            storage.makedirs(target_dir)
        except Exception:
            pass

    new_parquet_file = generate_filename("data", "parquet")
    new_parquet_path = os.path.join(target_dir, new_parquet_file)
    state["path"] = new_parquet_path

    # Sort before writing so each row group covers a tight min/max range.
    # DuckDB uses these zonemaps to skip entire row groups during filtered scans.
    sort_cols = [
        c for c in (overwrite_columns or [])
        if c in write_df.columns and c != "__timestamp__"
    ]
    if "__timestamp__" in write_df.columns and write_df.height > 1:
        timestamp = write_df.get_column("__timestamp__")
        # Ordinary writes inject one timestamp literal for the entire batch, so
        # sorting it cannot improve row-group zonemaps and only allocates/sorts
        # a full frame. Compaction can combine batches with different timestamps;
        # retain the sort there because it materially tightens row-group ranges.
        if timestamp.min() != timestamp.max():
            sort_cols.insert(0, "__timestamp__")
    if sort_cols and write_df.height > 0:
        with p.span("write.sort"):
            write_df = write_df.sort(sort_cols)

    write_bytes = getattr(storage, "write_bytes", None)
    write_parquet = getattr(storage, "write_parquet", None)

    # Write to the active storage backend. Encoding and upload are deliberately
    # separate: a failed object-store PUT must abort the mutation, never fall
    # back to writing the bare object key on the writer's local filesystem.
    # Native Polars is only selected when the exact encoded bytes can be PUT.
    # Compatibility backends receive the same Arrow table as before and may
    # apply their own encoding in ``write_parquet``.
    arrow_tbl: Optional[pa.Table] = None
    data: Optional[bytes] = None
    fallback_reason: Optional[str] = "backend"
    native_eligible = False
    if callable(write_bytes):
        with p.span("write.parquet_codec_check"):
            native_eligible, fallback_reason = (
                _native_polars_parquet_eligibility(write_df)
            )
    if native_eligible:
        try:
            with p.span("write.parquet_encode"):
                data = _encode_parquet_polars(write_df, compression_level)
            # This branch has no fallback reason, so recording the counter
            # directly is equivalent to _record_parquet_codec and accepts the
            # no-op profiler returned when instrumentation is disabled.
            p.add("parquet_codec_polars", 1)
        except Exception as exc:
            # No bytes have been uploaded yet. Falling back here is atomic and
            # preserves support for a dtype newly rejected by Polars' writer.
            fallback_reason = "encode_error"
            p.add("parquet_codec_polars_encode_error", 1)
            logging.warning(
                "[write] native Polars parquet encode failed; using PyArrow: %s",
                exc,
            )
    if data is None:
        with p.span("write.to_arrow"):
            arrow_tbl = write_df.to_arrow()
        with p.span("write.parquet_encode"):
            data = _encode_parquet_pyarrow(arrow_tbl, compression_level)
        _record_parquet_codec(p, "pyarrow", fallback_reason)

    footer_md = None
    object_seal = None
    local_write_identity = None
    if callable(write_bytes):
        with p.span("write.upload_bytes"):
            state["released"] = True
            write_with_identity = (
                getattr(storage, "write_bytes_with_identity", None)
                if _is_exact_local_storage(storage) else None
            )
            if callable(write_with_identity):
                local_write_identity = write_with_identity(
                    new_parquet_path, data,
                )
            else:
                write_bytes(new_parquet_path, data)
        # These are the exact bytes submitted to the backend, so size does not
        # require another lookup. A remote metadata observation below exists
        # solely to obtain the provider's conditional-read identity.
        file_size = len(data)
        # Pin the remote provider identity once, at the write boundary.  This
        # replaces N future HEADs (one per IslandDB query) with one observation
        # of the just-uploaded, UUID-named immutable object. Local files keep
        # their stronger descriptor/stat validation and skip this work.
        with p.span("write.object_seal"):
            object_seal = _uploaded_resource_object_seal(
                storage, new_parquet_path, file_size,
            )
        # The uploaded bytes ARE ``data`` here, so this metadata is the exact
        # immutable footer identity.  Seal it before a snapshot can reference
        # the resource; an unparseable encoder result may leave an orphan object
        # but must never publish an ambiguously identified resource.
        try:
            footer_md = pq.read_metadata(io.BytesIO(data))
        except Exception as exc:
            raise RuntimeError(
                f"Could not parse freshly encoded Parquet footer for "
                f"{new_parquet_path!r}"
            ) from exc
    elif callable(write_parquet):
        if arrow_tbl is None:  # pragma: no cover - guarded by codec selection
            raise RuntimeError("Compatibility parquet backend requires Arrow")
        with p.span("write.upload_parquet"):
            state["released"] = True
            write_parquet(arrow_tbl, new_parquet_path)
        # This compatibility branch may re-encode the Arrow table, so only the
        # backend can report the resulting object size.  Do not silently record
        # zero or an unrelated local path when that mandatory metadata lookup
        # fails: the snapshot's resource metadata must remain trustworthy.
        with p.span("write.size_lookup"):
            file_size = storage.size(new_parquet_path)
        with p.span("write.object_seal"):
            object_seal = _uploaded_resource_object_seal(
                storage, new_parquet_path, int(file_size),
            )
        # A compatibility backend may re-encode the Arrow table.  Only bytes
        # read back from that backend can describe its exact footer.  Third-party
        # implementations without byte reads remain safely unsealed: they are
        # readable but cannot participate in absence pruning.
        read_bytes = getattr(storage, "read_bytes", None)
        if callable(read_bytes):
            try:
                footer_md = pq.read_metadata(io.BytesIO(read_bytes(new_parquet_path)))
            except Exception:
                footer_md = None
    else:
        raise RuntimeError("Configured storage provides no parquet write method")

    p.add("files_written", 1)
    p.add("rows_written", int(rows))
    p.add("bytes_written", int(file_size))

    resource = {
        "file": new_parquet_path,
        "file_size": int(file_size),
        "rows": rows,
        "columns": columns,
    }
    binary_value_bounds: Dict[str, int] = {}
    if arrow_tbl is not None:
        # Preserve the established Arrow calculation on compatibility writes.
        binary_columns = (
            (name, column)
            for name, column in zip(arrow_tbl.schema.names, arrow_tbl.columns)
            if (
                pa.types.is_binary(column.type)
                or pa.types.is_large_binary(column.type)
                or pa.types.is_fixed_size_binary(column.type)
            )
        )
        for name, column in binary_columns:
            try:
                lengths = pc.binary_length(column)
                maximum = pc.max(lengths).as_py()
            except Exception as exc:
                raise RuntimeError(
                    f"Could not compute Binary value-width seal for {name!r}"
                ) from exc
            binary_value_bounds[str(name)] = max(0, int(maximum or 0))
    else:
        # Do not materialise a full Arrow table solely for this seal on the fast
        # path. Polars reports the same byte width (not character count).
        for name, dtype in write_df.schema.items():
            if dtype != polars.Binary:
                continue
            try:
                maximum = write_df.get_column(name).bin.size().max()
            except Exception as exc:
                # This seal is an execution-memory boundary. A writer capable
                # of publishing Binary data must never omit or guess it.
                raise RuntimeError(
                    f"Could not compute Binary value-width seal for {name!r}"
                ) from exc
            binary_value_bounds[str(name)] = max(0, int(maximum or 0))
    if binary_value_bounds:
        resource["column_max_value_bytes"] = binary_value_bounds
    if object_seal is not None:
        resource["object_seal"] = {
            "size": object_seal.size,
            "version": object_seal.version,
            "etag": object_seal.etag,
            "last_modified_ns": object_seal.last_modified_ns,
            "checksum_sha256": object_seal.checksum_sha256,
        }
    if footer_md is not None:
        footer_sha256 = parquet_footer_sha256(footer_md)
        exact_stats_rows = _stats_rows_for_metadata(
            new_parquet_path,
            footer_md,
            footer_sha256=footer_sha256,
        )
        seal = stats_seal_for_metadata(
            new_parquet_path,
            footer_md,
            rows=exact_stats_rows,
            footer_sha256=footer_sha256,
        )
        resource.update({
            "footer_sha256": seal.footer_sha256,
            "stats_rows": seal.stats_rows,
            "stats_digest": seal.stats_digest,
        })
        if footer_md_out is not None:
            # Reuse both products of the one footer traversal in the subsequent
            # combined stats-artifact build. Logical schema publication is
            # independent of physical compaction, so no output body/schema
            # reread is required here.
            footer_md_out[new_parquet_path] = _FooterStatsCacheEntry(
                metadata=footer_md,
                rows=exact_stats_rows,
            )
    # Exact LocalStorage publication is a trusted, durable exact-byte boundary.
    # Retain only the physical schema and projected-byte routing map already
    # available in the encoded footer. Complete rowid integrity deliberately
    # remains lazy: decoding that column here taxes every append, including
    # files that are never mutation candidates. Compatibility backends may
    # re-encode ``data`` and therefore cannot seed from the submitted bytes.
    if (
        callable(write_bytes)
        and footer_md is not None
        and data is not None
        and local_write_identity is not None
        and _is_exact_local_storage(storage)
    ):
        with p.span("write.probe_metadata_seed"):
            _seed_local_write_probe_metadata(
                file_key=new_parquet_path,
                published_identity=local_write_identity,
                published_size=len(data),
                encoded_metadata=footer_md,
                encoded_footer_sha256=footer_sha256,
                profiler=p,
            )
    new_resources.append(resource)
    state["published"] = True


# =========================
# Newer-than filtering (idempotency / conflict resolution)
# =========================

def filter_stale_incoming_rows(
        incoming_df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        newer_than_col: str,
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        profiler: Optional[Profiler] = None,
        read_columns: Optional[List[str]] = None,
        required: bool = False,
        dead_rowids_by_file: Optional[Dict[str, polars.Series]] = None,
        storage: Optional[object] = None,
) -> polars.DataFrame:
    """
    Remove rows from *incoming_df* that are stale or already present in existing data.

    For each incoming row (keyed by *overwrite_columns*), we find the maximum value of
    *newer_than_col* across all overlapping existing files.  If the existing max is >=
    the incoming value, the incoming row is dropped (it is either a replay or out-of-order).

    Edge cases:
      - Key not found in existing data            → keep incoming row (new key).
      - Existing file lacks the newer_than column → keep incoming row (legacy data).
      - incoming newer_than > existing max        → keep incoming row (genuine update).

    If file_cache dict is provided, read DataFrames are stored in it keyed by file path
    so downstream processing can reuse them without re-reading from storage.

    Returns the filtered incoming DataFrame (potentially empty).
    """
    p = profiler or get_null_profiler()
    if not overwrite_columns or not newer_than_col:
        return incoming_df

    # Collect only has_overlap=True files — those are the ones sharing keys with incoming data
    overlap_true_files = [(f, sz) for f, has_overlap, sz in overlapping_files if has_overlap]
    if not overlap_true_files:
        # No overlapping files → all incoming rows are new
        return incoming_df

    # Columns we need from existing files: overwrite keys + the newer_than column
    needed_cols = list(dict.fromkeys(overwrite_columns + [newer_than_col]))

    # Read and collect relevant rows from overlapping files
    existing_parts: List[polars.DataFrame] = []
    for file_path, file_size in overlap_true_files:
        if storage is not None:
            part = _read_parquet_safe(
                file_path,
                profiler=profiler,
                file_size=file_size,
                columns=read_columns,
                required=required,
                storage=storage,
            )
        else:
            part = _read_parquet_safe(
                file_path,
                profiler=p,
                file_size=file_size,
                columns=read_columns,
                required=required,
            )
        if part is None:
            continue
        # Validate the complete projected source before excluding already-dead
        # rows.  Validating the filtered frame could hide a duplicate whose
        # other occurrence is already in the deletion vector.
        if required or ROWID_COL in part.columns:
            _validate_mutation_source_rowids(part, file_path)
        dead_ids = (dead_rowids_by_file or {}).get(file_path)
        if dead_ids is not None and len(dead_ids) > 0 and ROWID_COL in part.columns:
            part = part.join(
                dead_ids.alias(ROWID_COL).to_frame(),
                on=ROWID_COL,
                how="anti",
            )
        # Cache the full DataFrame for downstream reuse (avoids double-read)
        if file_cache is not None:
            file_cache[file_path] = part
        # If the file doesn't have the newer_than column, skip it (legacy data → allow overwrite)
        if newer_than_col not in part.columns:
            continue
        # Select only the columns we need, filtering to matching keys
        available_cols = [c for c in needed_cols if c in part.columns]
        if not all(c in available_cols for c in overwrite_columns):
            if required:
                missing = [c for c in overwrite_columns if c not in available_cols]
                raise ValueError(
                    f"Mutation candidate {file_path!r} lacks overwrite column(s) {missing!r}"
                )
            continue
        existing_parts.append(part.select(available_cols))

    if not existing_parts:
        # No existing data with the newer_than column → all incoming rows proceed
        return incoming_df

    with p.span("newer_than.concat"):
        existing_combined = polars.concat(existing_parts, how="vertical_relaxed")

    # Get max(newer_than_col) per key group from existing data
    with p.span("newer_than.group_agg"):
        existing_max = existing_combined.group_by(overwrite_columns).agg(
            polars.col(newer_than_col).max().alias("__existing_max__")
        )

    # Left join incoming against existing max.  nulls_equal=True so a NULL key
    # compares against the existing NULL group's max, consistent with the
    # null-safe delete semi-join — otherwise an older NULL-keyed row would skip
    # the stale filter yet still tombstone the newer existing NULL-keyed row.
    with p.span("newer_than.join_filter"):
        joined = incoming_df.join(
            existing_max, on=overwrite_columns, how="left", nulls_equal=True
        )

        # Keep rows where:
        #   - no existing data for this key (null max → new key)
        #   - incoming value > existing max   (genuine update)
        filtered = joined.filter(
            polars.col("__existing_max__").is_null()
            | (polars.col(newer_than_col) > polars.col("__existing_max__"))
        ).drop("__existing_max__")

    return filtered


# =========================
# Tombstone (rowid deletion-vector) helpers
# =========================
#
# Deletes and upserts no longer rewrite data files in the common case.
# Instead, the ``__rowid__`` of every logically-removed row is recorded in a
# per-table deletion-vector parquet (columns ``__file__`` + ``__rowid__``).
# The read path anti-joins live data against that vector on ``__rowid__``.
# Physical removal happens lazily, only when the vector grows past
# ``max_tombstone_rows`` (see ``compact_tombstones``).

ROWID_COL = "__rowid__"
TOMBSTONE_FILE_COL = "__file__"
TOMBSTONE_SCHEMA: Dict[str, polars.DataType] = {
    TOMBSTONE_FILE_COL: polars.Utf8,
    ROWID_COL: polars.Int64,
}

@dataclass(frozen=True)
class LoadedTombstoneState:
    """Validated physical representation behind one snapshot DV pointer.

    ``frame`` is the logical union consumed by existing writer/compaction APIs.
    For v2, ``segments`` and ``root_digest`` retain the independently sealed
    manifest state so a later writer can append one segment without rereading
    or rewriting the previous union.  A v1 state has exactly one synthetic
    segment descriptor when a caller requests migration metadata.  Explicit
    empty v2 state has no pointer, digest, or segments.
    """

    frame: polars.DataFrame
    tombstone_format: int
    tombstone_path: Optional[str]
    root_digest: Optional[str]
    segments: Tuple[TombstoneSegment, ...] = ()


@dataclass(frozen=True)
class _TombstoneCacheSeal:
    """Immutable validation metadata stored beside one cached DV frame."""

    rows: int
    digest: str
    referenced_files: FrozenSet[str]
    state: Optional[LoadedTombstoneState] = None


def _empty_tombstone_df() -> polars.DataFrame:
    """Return an empty deletion-vector frame with the sealed schema."""
    return polars.DataFrame(schema=TOMBSTONE_SCHEMA)


def _validate_mutation_source_rowids(
        frame: polars.DataFrame,
        file_path: str,
) -> None:
    """Prove that one immutable source file is safe to tombstone by row id.

    A deletion vector identifies a physical row by ``(file, __rowid__)``.  If
    one file contains the same row id twice, a predicate matching only one of
    those rows would still hide both.  This validation therefore runs over the
    *complete projected row-id column* before any live/dead filtering or key
    matching.  Snapshot high-watermarks prevent future allocation reuse but do
    not prove that a legacy or previously compacted file is collision-free.
    """
    if ROWID_COL not in frame.columns:
        raise ValueError(
            f"Mutation candidate {file_path!r} has no required {ROWID_COL!r} column"
        )
    rowids = frame.get_column(ROWID_COL)
    if rowids.dtype != polars.Int64:
        raise ValueError(
            f"Mutation candidate {file_path!r} has non-Int64 rowids"
        )
    if rowids.null_count() > 0:
        raise ValueError(
            f"Mutation candidate {file_path!r} contains NULL rowids"
        )
    if frame.height and (
        rowids.min() is None
        or int(rowids.min()) <= 0
        or rowids.n_unique() != frame.height
    ):
        raise ValueError(
            f"Mutation candidate {file_path!r} contains non-positive or "
            "duplicate rowids"
        )


def _checked_tombstone_expected_rows(
        expected_rows: Optional[int], *, source: str,
) -> Optional[int]:
    if expected_rows is None:
        return None
    if isinstance(expected_rows, bool):
        raise ValueError(f"{source} has invalid expected row count")
    try:
        expected = int(expected_rows)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{source} has invalid expected row count") from exc
    if expected < 0:
        raise ValueError(f"{source} has invalid expected row count")
    return expected


def _checked_tombstone_expected_digest(
        expected_digest: Optional[str], *, source: str,
) -> Optional[str]:
    if expected_digest is None:
        return None
    if (
        not isinstance(expected_digest, str)
        or len(expected_digest) != 64
        or expected_digest != expected_digest.lower()
        or any(ch not in "0123456789abcdef" for ch in expected_digest)
    ):
        raise ValueError(f"{source} has an invalid expected digest")
    return expected_digest


def validate_tombstone_frame(
        df: polars.DataFrame,
        *,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        allowed_files: Optional[Set[str]] = None,
        source: str = "deletion-vector",
) -> polars.DataFrame:
    """Validate the persisted deletion-vector before it can affect data.

    Tombstones are correctness metadata, not an optional optimisation.  A
    malformed or truncated vector must therefore abort the operation instead
    of being interpreted as an empty/partial set (which would resurrect rows).

    ``__rowid__`` is additionally required to be globally unique as a writer,
    allocation, and legacy-migration integrity invariant.  This stronger
    requirement also proves composite ``(__file__``, ``__rowid__``)
    uniqueness without a second cardinality scan.
    """
    if not isinstance(df, polars.DataFrame):
        raise ValueError(f"{source} is not a Polars DataFrame")
    if list(df.columns) != list(TOMBSTONE_SCHEMA):
        raise ValueError(
            f"{source} has invalid columns {df.columns!r}; "
            f"expected {list(TOMBSTONE_SCHEMA)!r}"
        )
    if df.schema != TOMBSTONE_SCHEMA:
        raise ValueError(
            f"{source} has invalid schema {df.schema!r}; "
            f"expected {TOMBSTONE_SCHEMA!r}"
        )
    expected_digest = _checked_tombstone_expected_digest(
        expected_digest, source=source,
    )
    expected = _checked_tombstone_expected_rows(expected_rows, source=source)
    if expected is not None:
        if df.height != expected:
            raise ValueError(
                f"{source} row-count mismatch: expected {expected}, got {df.height}"
            )
    if df.height == 0:
        if expected_digest is not None:
            actual_digest = _tombstone_digest_validated(df)
            if actual_digest != expected_digest:
                raise ValueError(
                    f"{source} digest mismatch: expected {expected_digest}, "
                    f"got {actual_digest}"
                )
        return df
    integrity = df.select([
        polars.col(TOMBSTONE_FILE_COL).null_count().alias("null_files"),
        polars.col(ROWID_COL).null_count().alias("null_rowids"),
        (polars.col(TOMBSTONE_FILE_COL).str.len_chars() == 0)
        .sum().alias("empty_files"),
        polars.col(ROWID_COL).n_unique().alias("rowid_unique"),
        polars.col(ROWID_COL).min().alias("min_rowid"),
    ]).row(0, named=True)
    if integrity["null_files"] or integrity["null_rowids"]:
        raise ValueError(f"{source} contains NULL file/rowid values")
    if integrity["empty_files"]:
        raise ValueError(f"{source} contains an empty file key")
    if integrity["rowid_unique"] != df.height:
        raise ValueError(
            f"{source} contains duplicate (file, rowid) entries or reuses a "
            "rowid across files; writer integrity requires table-global rowid "
            "uniqueness"
        )
    if integrity["min_rowid"] <= 0:
        raise ValueError(f"{source} contains non-positive rowids")
    if allowed_files is not None:
        referenced = set(df.get_column(TOMBSTONE_FILE_COL).unique().to_list())
        foreign = referenced.difference(set(allowed_files))
        if foreign:
            raise ValueError(
                f"{source} references file(s) outside the current snapshot: "
                f"{sorted(foreign)!r}"
            )
    if expected_digest is not None:
        actual_digest = _tombstone_digest_validated(df)
        if actual_digest != expected_digest:
            raise ValueError(
                f"{source} digest mismatch: expected {expected_digest}, "
                f"got {actual_digest}"
            )
    return df


def _tombstone_digest_validated(df: polars.DataFrame) -> str:
    """Hash a frame already validated against :data:`TOMBSTONE_SCHEMA`."""
    # Keep sorting in Polars' columnar engine instead of allocating a Python
    # tuple for every record. Feed the digest incrementally so a threshold-size
    # vector never creates a second monolithic body bytes object.
    ordered = (
        df.select([
            polars.col(TOMBSTONE_FILE_COL).str.encode("base64").alias("__b64file__"),
            polars.col(ROWID_COL),
        ])
        .sort(["__b64file__", ROWID_COL])
    )
    digest = hashlib.sha256(b"supertable-tombstone-v1\n")
    separator = ""
    for encoded_file, rowid in ordered.iter_rows():
        # One bounded record allocation and one OpenSSL/Python boundary per
        # row.  The bytes remain exactly ``[newline]base64(file):hex(rowid)``.
        digest.update(
            f"{separator}{encoded_file}:{int(rowid):016x}".encode("ascii")
        )
        separator = "\n"
    return digest.hexdigest()


def tombstone_digest(
    df: polars.DataFrame, *, assume_valid: bool = False,
) -> str:
    """Return the canonical ``st-dv-v1`` logical deletion-vector digest."""
    validated = (
        df if assume_valid else
        validate_tombstone_frame(df, source="deletion-vector to digest")
    )
    return _tombstone_digest_validated(validated)


def _tombstone_cache_seal(
        df: polars.DataFrame,
        *,
        known_digest: Optional[str] = None,
        state: Optional[LoadedTombstoneState] = None,
        source: str,
) -> _TombstoneCacheSeal:
    """Build immutable cache metadata for an already validated frame."""
    digest = _checked_tombstone_expected_digest(known_digest, source=source)
    if digest is None:
        digest = _tombstone_digest_validated(df)
    referenced = frozenset(
        df.get_column(TOMBSTONE_FILE_COL).unique().to_list()
    ) if df.height else frozenset()
    return _TombstoneCacheSeal(
        rows=int(df.height),
        digest=digest,
        referenced_files=referenced,
        state=state,
    )


def _validate_cached_tombstone_seal(
        seal: _TombstoneCacheSeal,
        *,
        expected_rows: Optional[int],
        expected_digest: Optional[str],
        allowed_files: Optional[Set[str]],
        source: str,
) -> None:
    """Validate a pinned snapshot against cached immutable metadata only."""
    if isinstance(seal, tuple):
        # Compatibility with entries seeded by a pre-v2 process in the same
        # interpreter during rolling tests/upgrades.
        rows, digest, referenced = seal[:3]
    else:
        rows = seal.rows
        digest = seal.digest
        referenced = seal.referenced_files
    expected = _checked_tombstone_expected_rows(expected_rows, source=source)
    wanted_digest = _checked_tombstone_expected_digest(
        expected_digest, source=source,
    )
    if expected is not None and rows != expected:
        raise ValueError(
            f"{source} row-count mismatch: expected {expected}, got {rows}"
        )
    if wanted_digest is not None and digest != wanted_digest:
        raise ValueError(
            f"{source} digest mismatch: expected {wanted_digest}, got {digest}"
        )
    if allowed_files is not None:
        foreign = referenced.difference(frozenset(allowed_files))
        if foreign:
            raise ValueError(
                f"{source} references file(s) outside the current snapshot: "
                f"{sorted(foreign)!r}"
            )


def _max_tombstone_rows(table_config: Optional[dict]) -> int:
    """Return the deletion-vector row count that triggers physical compaction.

    Per-table ``max_tombstone_rows`` override falls back to the global
    ``MAX_TOMBSTONE_ROWS`` default (env-configurable, like
    ``MAX_MEMORY_CHUNK_SIZE`` / ``MAX_OVERLAPPING_FILES``).
    """
    cfg = table_config or {}
    return int(cfg.get("max_tombstone_rows") or getattr(default, "MAX_TOMBSTONE_ROWS", 1_000_000))


def _write_df_parquet(
        write_df: polars.DataFrame,
        path: str,
        compression_level: int = 1,
        profiler: Optional[Profiler] = None,
        storage: Optional[object] = None,
) -> int:
    """Write a Polars DataFrame to a single parquet file on the active storage.

    Minimal writer for system files (the tombstone deletion-vector) that need
    no column statistics or Hive partitioning. Returns the file size in bytes.
    """
    p = profiler or get_null_profiler()
    active_storage = storage if storage is not None else _get_storage()
    write_bytes = getattr(active_storage, "write_bytes", None)
    write_parquet = getattr(active_storage, "write_parquet", None)

    data: Optional[bytes] = None
    arrow_tbl: Optional[pa.Table] = None
    wrote_exact_bytes = False
    with p.span("tombstone.encode"):
        if callable(write_bytes):
            try:
                # System artifacts are read as complete objects and never use
                # their own footer min/max statistics.  Bypass the data-file
                # compatibility scan (long object keys otherwise force every
                # stats/DV write through PyArrow) and attempt native Polars
                # directly.  An encode failure is still safely recoverable
                # because no bytes have been released to storage yet.
                data = _encode_system_parquet_polars(
                    write_df, compression_level,
                )
                _record_parquet_codec(p, "polars")
            except Exception as exc:
                p.add("parquet_codec_polars_encode_error", 1)
                logging.warning(
                    "[write] native Polars system parquet encode failed; "
                    "using PyArrow: %s",
                    exc,
                )
                arrow_tbl = write_df.to_arrow()
                data = _encode_system_parquet_pyarrow(
                    arrow_tbl, compression_level,
                )
                _record_parquet_codec(p, "pyarrow", "encode_error")
        elif callable(write_parquet):
            # The compatibility backend owns the final encoding.  Building a
            # throwaway PyArrow byte buffer here would encode the same frame
            # twice before one object is published.
            arrow_tbl = write_df.to_arrow()
            _record_parquet_codec(p, "pyarrow", "backend")

    if callable(write_bytes):
        # Upload failures are authoritative.  Never reinterpret a failed remote
        # PUT as a local relative-path write: that can publish a snapshot whose
        # object key was created only on the writer's local filesystem.
        write_bytes(path, data)
        wrote_exact_bytes = True
    elif callable(write_parquet):
        if arrow_tbl is None:  # pragma: no cover - guarded above
            raise RuntimeError("Compatibility parquet backend requires Arrow")
        write_parquet(arrow_tbl, path)
    else:
        raise RuntimeError("Configured storage provides no parquet write method")
    # Fast path: the write_bytes backend (MinIO/S3/local) stored precisely
    # `data`, so return its length and skip the extra HEAD.  Only the
    # write_parquet / fallback branches (which may re-encode) consult size().
    if wrote_exact_bytes and data is not None:
        return len(data)
    try:
        return int(active_storage.size(path))
    except Exception:
        try:
            return os.path.getsize(path)
        except Exception:
            return len(data) if data is not None else 0


def identify_deleted_rowids(
        df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        profiler: Optional[Profiler] = None,
        read_columns: Optional[List[str]] = None,
        required: bool = False,
        dead_rowids_by_file: Optional[Dict[str, polars.Series]] = None,
        storage: Optional[object] = None,
) -> List[Tuple[str, int]]:
    """Find the ``(file, __rowid__)`` pairs of existing rows matching a delete predicate.

    For every overlapping data file, semi-joins the file against the unique
    *overwrite_columns* key tuples present in *df* and collects the
    ``__rowid__`` of each matched row plus the file it lives in. These pairs
    are appended to the tombstone deletion-vector by the caller.

    Files lacking a ``__rowid__`` column (legacy data written before rowids
    existed) cannot be tombstoned by id and are skipped.
    """
    p = profiler or get_null_profiler()
    pairs: List[Tuple[str, int]] = []
    if not overwrite_columns:
        return pairs

    key_cols = [c for c in overwrite_columns if c in df.columns]
    if key_cols != list(overwrite_columns):
        # Not all predicate columns present in the incoming df — nothing to match.
        return pairs
    with p.span("delete.incoming_keys"):
        incoming_keys = df.select(overwrite_columns).unique()

    for file, has_overlap, file_size in overlapping_files:
        if not has_overlap:
            continue
        p.add("delete_files_seen", 1)

        if file_cache is not None and file in file_cache:
            existing_df = file_cache.get(file)
        else:
            if storage is not None:
                existing_df = _read_parquet_safe(
                    file,
                    profiler=profiler,
                    file_size=file_size,
                    columns=read_columns,
                    required=required,
                    storage=storage,
                )
            else:
                existing_df = _read_parquet_safe(
                    file,
                    profiler=p,
                    file_size=file_size,
                    columns=read_columns,
                    required=required,
                )
        if existing_df is None:
            continue
        if ROWID_COL not in existing_df.columns:
            if required:
                raise ValueError(
                    f"Mutation candidate {file!r} has no required {ROWID_COL!r} column"
                )
            continue
        if not all(c in existing_df.columns for c in overwrite_columns):
            if required:
                missing = [c for c in overwrite_columns if c not in existing_df.columns]
                raise ValueError(
                    f"Mutation candidate {file!r} lacks overwrite column(s) {missing!r}"
                )
            continue
        # This frame contains every projected row from the source file (or was
        # cached only after the same validation in stale filtering).  Never
        # emit a deletion-vector pair unless one row id identifies exactly one
        # physical row in that file.
        _validate_mutation_source_rowids(existing_df, file)
        dead_ids = (dead_rowids_by_file or {}).get(file)
        if dead_ids is not None and len(dead_ids) > 0:
            existing_df = existing_df.join(
                dead_ids.alias(ROWID_COL).to_frame(),
                on=ROWID_COL,
                how="anti",
            )

        with p.span("delete.semi_join"):
            # nulls_equal=True so a NULL in an overwrite key matches an existing
            # NULL (null-safe overwrite/delete), unlike SQL's NULL != NULL.
            matched = existing_df.join(
                incoming_keys, on=overwrite_columns, how="semi", nulls_equal=True
            )
        if matched.height == 0:
            continue

        rowids = matched.get_column(ROWID_COL).drop_nulls().to_list()
        pairs.extend((file, int(rid)) for rid in rowids)
        p.add("delete_rows_matched", len(rowids))

    return pairs


def identify_all_rowids(
        resources: list,
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        profiler: Optional[Profiler] = None,
        required: bool = False,
) -> List[Tuple[str, int]]:
    """Collect every ``(file, __rowid__)`` pair across all data files.

    This is the delete-all tombstone set used by ``delete_only`` writes that
    pass no *overwrite_columns*: the whole table is logically emptied by
    tombstoning every live ``__rowid__``. Files lacking a ``__rowid__`` column
    (legacy data written before rowids existed) cannot be tombstoned by id and
    are skipped.
    """
    p = profiler or get_null_profiler()
    pairs: List[Tuple[str, int]] = []
    for resource in resources or []:
        if not isinstance(resource, dict):
            continue
        file = resource.get("file")
        if not file:
            continue
        file_size = int(resource.get("file_size") or 0)
        if file_cache is not None and file in file_cache:
            existing_df = file_cache.get(file)
        else:
            # Only __rowid__ is consumed below, so read just that column chunk.
            # A delete-all can touch every file; a full-width read would pull all
            # columns of every file into memory for nothing.
            existing_df = _read_parquet_safe(
                file,
                profiler=p,
                file_size=file_size,
                columns=[ROWID_COL],
                required=required,
            )
        if existing_df is None:
            continue
        if ROWID_COL not in existing_df.columns:
            if required:
                raise ValueError(
                    f"Mutation candidate {file!r} has no required {ROWID_COL!r} column"
                )
            continue
        if existing_df.get_column(ROWID_COL).null_count() > 0:
            if required:
                raise ValueError(f"Mutation candidate {file!r} contains NULL rowids")
            existing_df = existing_df.filter(polars.col(ROWID_COL).is_not_null())
        rowids = existing_df.get_column(ROWID_COL).drop_nulls().to_list()
        pairs.extend((file, int(rid)) for rid in rowids)
        p.add("delete_rows_matched", len(rowids))

    return pairs


# =========================
# Pushdown overwrite resolution (Island-native local / DuckDB remote probe,
# strict Polars fallback)
# =========================
#
# The legacy path (``filter_stale_incoming_rows`` + ``identify_deleted_rowids``)
# reads EVERY overlapping data file FULLY (all columns, all rows) into polars,
# then group/join over the whole table — cost O(table size), independent of how
# few rows are actually written.  ``resolve_overwrite_writes`` replaces both with
# ONE column-projected native ``scan_parquet`` that reads only the key /
# ``__rowid__`` / newer-than columns and returns only rows whose key matches an
# incoming key (null-safe SEMI JOIN), then derives both results in-memory from
# that small matched set. DuckDB is retained only for the explicit remote
# compatibility lane. The two legacy functions are the exact semantic oracle
# and fallback for any environment/schema the accelerators cannot handle.


def _storage_duckdb_path(storage, key: str, force_presign: bool = False) -> str:
    """Resolve a storage key to a path string DuckDB can read directly.

    Mirrors the read path's ``DataEstimator._to_duckdb_path``: DuckDB cannot read
    a private object-store file unless the URL is **presigned**, so when
    ``SUPERTABLE_DUCKDB_PRESIGNED`` is set (or *force_presign* is True — used by
    the probe's reactive retry after an HTTP/auth error) the bare object key is
    signed via ``storage.presign(key)``.  Local storage always resolves through
    ``to_duckdb_path`` first and never presigns; object stores use either their
    direct DuckDB URL or a signed URL. Anything already a URL passes through
    untouched (presign takes a key, never a URL).
    """
    if not key:
        return key
    if "://" in key:
        return key

    # A backend that identifies itself as local must always resolve through its
    # filesystem path hook. Presigning is both unnecessary and unsafe for the
    # local auto lane: the base StorageInterface method raises, while a hybrid
    # adapter could return a remote URL and silently pull httpfs/network work
    # into what is deliberately a local-only optimization.
    is_local = getattr(storage, "is_local_storage", None)
    try:
        local = callable(is_local) and is_local() is True
    except Exception:
        local = False
    if local:
        fn = getattr(storage, "to_duckdb_path", None)
        if callable(fn):
            try:
                path = fn(key)
                if isinstance(path, str) and path:
                    return path
            except NotImplementedError:
                pass
            except Exception as e:
                logging.debug(
                    f"[write-probe] local to_duckdb_path failed for {key}: {e}"
                )
        raise ValueError(
            f"local storage could not resolve a DuckDB filesystem path: {key!r}"
        )

    # Proactive (or forced) presign — sign the bare object key, never a URL.
    if force_presign or settings.SUPERTABLE_DUCKDB_PRESIGNED:
        presign_fn = getattr(storage, "presign", None)
        if callable(presign_fn):
            try:
                url = presign_fn(key)
                if isinstance(url, str) and url:
                    return url
            except Exception as e:
                logging.debug(f"[write-probe] presign failed for {key}: {e}")

    fn = getattr(storage, "to_duckdb_path", None)
    if callable(fn):
        try:
            url = fn(key)
            if isinstance(url, str) and url:
                return url
        except NotImplementedError:
            pass
        except Exception as e:
            logging.debug(f"[write-probe] to_duckdb_path failed for {key}: {e}")
    return key


# Error substrings that signal an unsigned/expired object-store read which a
# presigned URL can fix.  Mirrors create_reflection_view_with_presign_retry so
# the write-side probe retries the same way the read path does.
_PRESIGN_RETRY_TOKENS = (
    "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
    "AccessDenied", "SignatureDoesNotMatch", "403", "400",
)
_LOCAL_WRITE_PROBE_MIN_FILES = 8
_LOCAL_WRITE_PROBE_MIN_BYTES = 128 * 1024
_LOCAL_ROWID_INTEGRITY_CACHE_MAX_ENTRIES = 4096

_LocalProbeFileIdentity = Tuple[str, int, int, int, int, int]


@dataclass
class _PinnedLocalProbeFile:
    """An immutable LocalStorage candidate held open for one write probe."""

    key: str
    path: str
    scan_path: str
    fd: int
    identity: _LocalProbeFileIdentity

    def close(self) -> None:
        fd, self.fd = self.fd, -1
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass


def _is_exact_local_storage(storage: object) -> bool:
    try:
        from supertable.storage.local_storage import LocalStorage
    except Exception:
        return False
    return type(storage) is LocalStorage


def _local_probe_file_identity(
        canonical_path: str,
        state: os.stat_result,
) -> _LocalProbeFileIdentity:
    return (
        canonical_path,
        int(state.st_dev),
        int(state.st_ino),
        int(state.st_size),
        int(state.st_mtime_ns),
        int(state.st_ctime_ns),
    )


def _pin_local_probe_files(
        storage: object,
        file_keys: List[str],
        resolved_paths: List[str],
) -> Optional[List[_PinnedLocalProbeFile]]:
    """Pin exact LocalStorage files and expose their open fds to a scanner.

    Cache eligibility is deliberately narrower than probe eligibility. Only the
    built-in, exact ``LocalStorage`` type may use cached integrity results;
    subclasses, duck-typed adapters, URLs, non-regular files, symlinks, and
    platforms without a readable ``/proc/self/fd`` view keep doing the complete
    rowid scan on every probe.
    """
    if not _is_exact_local_storage(storage) or len(file_keys) != len(resolved_paths):
        return None

    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    pins: List[_PinnedLocalProbeFile] = []
    canonical_keys: Dict[str, str] = {}
    try:
        for key, raw_path in zip(file_keys, resolved_paths):
            if not isinstance(raw_path, str) or not raw_path or "://" in raw_path:
                raise ValueError("local probe path is not an unambiguous filesystem path")
            path = os.path.abspath(raw_path)
            fd = os.open(path, flags)
            try:
                opened = os.fstat(fd)
                if not stat.S_ISREG(opened.st_mode):
                    raise ValueError("local probe candidate is not a regular file")
                current = os.stat(path, follow_symlinks=True)
                canonical = os.path.realpath(path)
                identity = _local_probe_file_identity(canonical, opened)
                if _local_probe_file_identity(canonical, current) != identity:
                    raise ValueError("local probe candidate changed while it was opened")
                prior_key = canonical_keys.get(canonical)
                if prior_key is not None and prior_key != key:
                    raise ValueError("local probe paths alias the same canonical file")
                canonical_keys[canonical] = key

                scan_path = f"/proc/self/fd/{fd}"
                proc_state = os.stat(scan_path, follow_symlinks=True)
                if (
                    not stat.S_ISREG(proc_state.st_mode)
                    or int(proc_state.st_dev) != int(opened.st_dev)
                    or int(proc_state.st_ino) != int(opened.st_ino)
                ):
                    raise ValueError("open local file cannot be pinned through /proc")
                pins.append(_PinnedLocalProbeFile(
                    key=key,
                    path=path,
                    scan_path=scan_path,
                    fd=fd,
                    identity=identity,
                ))
            except BaseException:
                os.close(fd)
                raise
        return pins
    except BaseException as exc:
        for pin in pins:
            pin.close()
        logging.debug(f"[write-probe] local integrity cache unavailable: {exc}")
        return None


def _local_probe_pins_unchanged(pins: Iterable[_PinnedLocalProbeFile]) -> bool:
    """Verify both each open inode and its pathname still have one identity."""
    try:
        for pin in pins:
            opened = os.fstat(pin.fd)
            current = os.stat(pin.path, follow_symlinks=True)
            if (
                _local_probe_file_identity(pin.identity[0], opened) != pin.identity
                or _local_probe_file_identity(pin.identity[0], current) != pin.identity
            ):
                return False
        return True
    except (OSError, ValueError):
        return False


class _LocalRowidIntegrityCache:
    """Bounded positive-result cache with per-identity in-flight coalescing."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: OrderedDict[_LocalProbeFileIdentity, None] = OrderedDict()
        self._by_path: Dict[str, _LocalProbeFileIdentity] = {}
        self._inflight: Dict[_LocalProbeFileIdentity, threading.Event] = {}

    def reserve(
            self,
            identities: Iterable[_LocalProbeFileIdentity],
    ) -> Tuple[List[_LocalProbeFileIdentity], List[_LocalProbeFileIdentity]]:
        """Return identities this caller must scan and identities already valid.

        Concurrent callers for the same cold identity wait for its owner. If
        that owner's probe fails, exactly one waiter claims and rescans it;
        failures are never cached.
        """
        pending = list(dict.fromkeys(identities))
        owned: List[_LocalProbeFileIdentity] = []
        hits: List[_LocalProbeFileIdentity] = []
        while pending:
            waiters: List[Tuple[_LocalProbeFileIdentity, threading.Event]] = []
            with self._lock:
                for identity in pending:
                    path = identity[0]
                    prior = self._by_path.get(path)
                    if prior is not None and prior != identity:
                        self._entries.pop(prior, None)
                        if self._by_path.get(path) == prior:
                            self._by_path.pop(path, None)
                    if identity in self._entries:
                        self._entries.move_to_end(identity)
                        hits.append(identity)
                        continue
                    event = self._inflight.get(identity)
                    if event is None:
                        self._inflight[identity] = threading.Event()
                        owned.append(identity)
                    else:
                        waiters.append((identity, event))
            if not waiters:
                break
            for _identity, event in waiters:
                event.wait()
            pending = [identity for identity, _event in waiters]
        return owned, hits

    def finish(
            self,
            owned: Iterable[_LocalProbeFileIdentity],
            successful: Iterable[_LocalProbeFileIdentity] = (),
    ) -> None:
        successful_set = set(successful)
        signals: List[threading.Event] = []
        with self._lock:
            for identity in owned:
                if identity in successful_set:
                    path = identity[0]
                    prior = self._by_path.get(path)
                    if prior is not None and prior != identity:
                        self._entries.pop(prior, None)
                    self._entries[identity] = None
                    self._entries.move_to_end(identity)
                    self._by_path[path] = identity
                event = self._inflight.pop(identity, None)
                if event is not None:
                    signals.append(event)
            limit = max(0, int(_LOCAL_ROWID_INTEGRITY_CACHE_MAX_ENTRIES))
            while len(self._entries) > limit:
                evicted, _none = self._entries.popitem(last=False)
                if self._by_path.get(evicted[0]) == evicted:
                    self._by_path.pop(evicted[0], None)
            for event in signals:
                event.set()

    def clear(self) -> None:
        """Clear process-local state (also used for isolated contract tests)."""
        with self._lock:
            signals = list(self._inflight.values())
            self._entries.clear()
            self._by_path.clear()
            self._inflight.clear()
            for event in signals:
                event.set()


_LOCAL_ROWID_INTEGRITY_CACHE = _LocalRowidIntegrityCache()


class _LocalProbeSchemaProof:
    """Physical footer facts for one exact immutable local identity.

    Writer publication retains the already-parsed footer object without doing
    more work. Schema conversion and compressed-byte aggregation are computed
    only if a later mutation probe actually consumes them. Cold/legacy probes
    may instead supply an already-read Arrow schema and no footer metadata.
    """

    def __init__(
            self,
            *,
            arrow_schema: Optional[pa.Schema],
            rows: int,
            footer_sha256: str,
            compressed_column_bytes: Optional[Tuple[Tuple[str, int], ...]] = None,
            encoded_metadata: Optional[pq.FileMetaData] = None,
    ) -> None:
        self._arrow_schema = arrow_schema
        self.rows = rows
        self.footer_sha256 = footer_sha256
        self._compressed_column_bytes = compressed_column_bytes
        self._encoded_metadata = encoded_metadata
        self._materialize_lock = threading.Lock()

    @property
    def arrow_schema(self) -> pa.Schema:
        with self._materialize_lock:
            if self._arrow_schema is None:
                if self._encoded_metadata is None:
                    raise ValueError("cached local probe schema is unavailable")
                self._arrow_schema = (
                    self._encoded_metadata.schema.to_arrow_schema()
                )
            return self._arrow_schema

    @property
    def compressed_column_bytes(
            self,
    ) -> Optional[Tuple[Tuple[str, int], ...]]:
        with self._materialize_lock:
            if (
                self._compressed_column_bytes is None
                and self._encoded_metadata is not None
            ):
                self._compressed_column_bytes = (
                    _parquet_compressed_column_bytes(self._encoded_metadata)
                )
            return self._compressed_column_bytes


class _LocalProbeSchemaCache:
    """Bounded physical-schema cache keyed by the complete local identity."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: OrderedDict[
            _LocalProbeFileIdentity, _LocalProbeSchemaProof,
        ] = OrderedDict()
        self._by_path: Dict[str, _LocalProbeFileIdentity] = {}

    def get(
            self, identity: _LocalProbeFileIdentity,
    ) -> Optional[_LocalProbeSchemaProof]:
        with self._lock:
            path = identity[0]
            prior = self._by_path.get(path)
            if prior is not None and prior != identity:
                self._entries.pop(prior, None)
                if self._by_path.get(path) == prior:
                    self._by_path.pop(path, None)
            proof = self._entries.get(identity)
            if proof is not None:
                self._entries.move_to_end(identity)
            return proof

    def publish(
            self,
            identity: _LocalProbeFileIdentity,
            proof: _LocalProbeSchemaProof,
    ) -> None:
        with self._lock:
            path = identity[0]
            prior = self._by_path.get(path)
            if prior is not None and prior != identity:
                self._entries.pop(prior, None)
            self._entries[identity] = proof
            self._entries.move_to_end(identity)
            self._by_path[path] = identity
            limit = max(0, int(_LOCAL_ROWID_INTEGRITY_CACHE_MAX_ENTRIES))
            while len(self._entries) > limit:
                evicted, _proof = self._entries.popitem(last=False)
                if self._by_path.get(evicted[0]) == evicted:
                    self._by_path.pop(evicted[0], None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._by_path.clear()


_LOCAL_PROBE_SCHEMA_CACHE = _LocalProbeSchemaCache()


def _parquet_compressed_column_bytes(
        metadata: pq.FileMetaData,
) -> Tuple[Tuple[str, int], ...]:
    """Return exact compressed bytes per physical Parquet column path."""
    totals: Dict[str, int] = defaultdict(int)
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            totals[str(column.path_in_schema)] += max(
                0, int(column.total_compressed_size or 0),
            )
    return tuple(sorted(totals.items()))


def _seed_local_write_probe_metadata(
        *,
        file_key: str,
        published_identity: object,
        published_size: int,
        encoded_metadata: pq.FileMetaData,
        encoded_footer_sha256: str,
        profiler: Optional[Profiler] = None,
) -> None:
    """Seed identity-bound local probe metadata without reading published data.

    The exact built-in ``LocalStorage`` writer has just durably published the
    same byte string whose footer produced ``encoded_metadata``. Bind its
    physical schema and compressed-column map to the live path/inode identity,
    but never infer or publish rowid integrity from the input frame. The first
    mutation probe still scans and validates every encoded rowid from the pinned
    file. Any identity ambiguity merely leaves the metadata cache cold.
    """
    p = profiler or get_null_profiler()
    try:
        from supertable.storage.local_storage import LocalWriteIdentity

        if not isinstance(published_identity, LocalWriteIdentity):
            raise ValueError("local publication returned no trusted identity")
        identity: _LocalProbeFileIdentity = (
            published_identity.canonical_path,
            int(published_identity.device),
            int(published_identity.inode),
            int(published_identity.size),
            int(published_identity.mtime_ns),
            int(published_identity.ctime_ns),
        )
        if not os.path.isabs(identity[0]) or "://" in identity[0]:
            raise ValueError("local publication identity has an invalid path")
        if identity[3] != int(published_size):
            raise ValueError("published local file size differs from encoded bytes")

        if (
            not isinstance(encoded_footer_sha256, str)
            or not encoded_footer_sha256
        ):
            raise ValueError("encoded Parquet footer seal is missing")
        schema_proof = _LocalProbeSchemaProof(
            arrow_schema=None,
            rows=int(encoded_metadata.num_rows),
            footer_sha256=encoded_footer_sha256,
            encoded_metadata=encoded_metadata,
        )
        _LOCAL_PROBE_SCHEMA_CACHE.publish(identity, schema_proof)
        p.add("write_probe_published_schema_metadata", 1)
    except Exception as exc:
        p.add("write_probe_publication_metadata_failures", 1)
        logging.debug(
            "[write-probe] local publication metadata not cached for %r: %s",
            file_key,
            exc,
        )


def _local_projected_parquet_bytes(
        storage: object,
        overlap_true_files: List[Tuple[str, int]],
        projected_columns: Iterable[str],
) -> Optional[int]:
    """Return compressed local Parquet bytes for the probe's projection.

    Candidate metadata reports whole-object sizes, which can be dominated by
    payload columns neither overwrite-resolution path reads. Trusted writer
    publications retain this footer-derived map under the immutable identity;
    legacy/cold files read only their local footer. Any ambiguous path/footer
    fails closed to Polars.
    """
    wanted = set(projected_columns)
    if not wanted:
        return 0
    total = 0
    try:
        for key, _whole_file_size in overlap_true_files:
            path = _storage_duckdb_path(storage, key)
            if not isinstance(path, str) or not path or "://" in path:
                return None
            # Exact writer publications retain their footer-bound projected
            # byte map under the same complete identity used by the probe.
            # A stale observation can only influence optional route selection:
            # the selected probe still opens/pins/revalidates the live file
            # before returning a mutation decision.
            if _is_exact_local_storage(storage):
                state = os.stat(path, follow_symlinks=True)
                if not stat.S_ISREG(state.st_mode):
                    return None
                identity = _local_probe_file_identity(
                    os.path.realpath(os.path.abspath(path)), state,
                )
                proof = _LOCAL_PROBE_SCHEMA_CACHE.get(identity)
                if (
                    proof is not None
                    and proof.compressed_column_bytes is not None
                ):
                    compressed = dict(proof.compressed_column_bytes)
                    total += sum(
                        compressed.get(column, 0) for column in wanted
                    )
                    continue
            metadata = pq.read_metadata(path)
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    column = row_group.column(column_index)
                    if column.path_in_schema in wanted:
                        compressed = int(column.total_compressed_size or 0)
                        if compressed > 0:
                            total += compressed
    except Exception as e:
        logging.debug(
            f"[write-probe] projected local cost unavailable; using polars: {e}"
        )
        return None
    return total


def _storage_reports_local(storage: Optional[object]) -> bool:
    if storage is None:
        return False
    is_local = getattr(storage, "is_local_storage", None)
    if not callable(is_local):
        return False
    try:
        return is_local() is True
    except Exception:
        return False


def _write_probe_selected(
        storage: Optional[object],
        overlap_true_files: List[Tuple[str, int]],
        projected_columns: Iterable[str],
) -> Tuple[bool, bool]:
    """Return ``(selected, auto_local)`` for overwrite resolution.

    ``SUPERTABLE_DUCKDB_WRITE_PROBE`` remains the explicit cross-backend opt-in
    for compatibility. Local selections run through the Island-native scanner;
    DuckDB is used only when an explicitly selected backend is non-local.
    Larger many-file local candidate sets are safe to accelerate automatically
    because the native scanner reads the same pinned immutable files directly
    and cannot stall on httpfs installation or remote authentication. The
    dedicated local-auto switch keeps an explicit operational escape hatch;
    any probe/schema failure still returns to the strict projected Polars
    oracle.
    """
    if settings.SUPERTABLE_DUCKDB_WRITE_PROBE:
        return True, False
    if not getattr(settings, "SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO", True):
        return False, False
    # Direct processing callers historically needed no storage construction to
    # reach the strict fallback. Only the owning DataWriter may opt into local
    # auto-selection by supplying its already-resolved backend instance.
    if storage is None:
        return False, False
    if not _storage_reports_local(storage):
        # Storage classification is optional acceleration metadata. An
        # ambiguous/failed answer must retain the storage-SDK oracle path.
        return False, False

    # A multi-file probe pays fixed schema/integrity costs and is slower than
    # the projected strict oracle for a handful of tiny local files. Targeted
    # crossover profiling puts the stable win at eight non-trivial candidates.
    # Gate the byte side on the exact compressed key/rowid/version chunks, not
    # unrelated payload columns that neither path reads.
    if len(overlap_true_files) < _LOCAL_WRITE_PROBE_MIN_FILES:
        return False, False
    candidate_bytes = _local_projected_parquet_bytes(
        storage, overlap_true_files, projected_columns,
    )
    selected = (
        candidate_bytes is not None
        and candidate_bytes >= _LOCAL_WRITE_PROBE_MIN_BYTES
    )
    return selected, selected


_ISLAND_PROBE_SOURCE_COL = "__supertable_write_probe_source__"
_ISLAND_PROBE_EXACT_TYPES = frozenset({
    polars.Boolean,
    polars.Int8,
    polars.Int16,
    polars.Int32,
    polars.Int64,
    polars.UInt8,
    polars.UInt16,
    polars.UInt32,
    polars.UInt64,
    polars.Utf8,
})


def _island_probe_scan(
        paths: List[str],
        projected_schema: Dict[str, polars.DataType],
) -> polars.LazyFrame:
    """Return IslandDB's native local Parquet scan for a write probe.

    This intentionally uses the same Polars lazy scanner as IslandDB without
    constructing the public SQL/parser/planner facade.  The caller has already
    pinned every path to an open immutable inode and proven each physical
    schema.  ``missing_columns='insert'`` is therefore used only for the
    optional newer-than column; rowid and overwrite keys were required in every
    file before this scan was built.
    """
    return polars.scan_parquet(
        paths,
        schema=polars.Schema(projected_schema),
        include_file_paths=_ISLAND_PROBE_SOURCE_COL,
        hive_partitioning=False,
        use_statistics=True,
        parallel="auto",
        missing_columns="insert",
        extra_columns="ignore",
    )


def _island_probe_overlap_matches(
        overlap_true_files: List[Tuple[str, int]],
        overwrite_columns: List[str],
        newer_than_col: Optional[str],
        incoming_keys: polars.DataFrame,
        incoming_schema: Optional[Dict[str, polars.DataType]] = None,
        profiler: Optional[Profiler] = None,
        storage: Optional[object] = None,
) -> Optional[polars.DataFrame]:
    """Resolve local overwrite matches with IslandDB's native Polars scan.

    The public IslandDB SQL facade deliberately hides source identity and rowid,
    so the write path uses its lower-level execution primitive instead.  Every
    candidate is opened with ``O_NOFOLLOW`` and scanned through ``/proc/self/fd``;
    path and inode identity are fenced again after collection.  Physical schema
    and complete rowid integrity are proven before a ``(file, rowid)`` pair can
    be returned.  Any unsupported or ambiguous state returns ``None`` so the
    required strict Polars/storage oracle remains authoritative.
    """
    p = profiler or get_null_profiler()
    if not overlap_true_files or not overwrite_columns:
        return None

    source_schema = incoming_schema or dict(incoming_keys.schema)
    projected_schema: Dict[str, polars.DataType] = {ROWID_COL: polars.Int64}
    for column in overwrite_columns:
        dtype = source_schema.get(column)
        if dtype not in _ISLAND_PROBE_EXACT_TYPES:
            logging.info(
                f"[write-probe] unsupported exact native key type "
                f"{column}={dtype}; using strict polars path"
            )
            return None
        projected_schema[column] = dtype
    if newer_than_col:
        dtype = source_schema.get(newer_than_col)
        if dtype not in _ISLAND_PROBE_EXACT_TYPES:
            logging.info(
                f"[write-probe] unsupported exact native version type "
                f"{newer_than_col}={dtype}; using strict polars path"
            )
            return None
        projected_schema[newer_than_col] = dtype

    if storage is None:
        try:
            storage = _get_storage()
        except Exception as exc:
            logging.info(
                f"[write-probe] local storage unavailable, using polars path: {exc}"
            )
            return None
    # Open-fd pinning and its proof cache are intentionally restricted to the
    # exact built-in LocalStorage implementation.  A subclass can change path
    # resolution/immutability semantics, so it keeps the storage-SDK oracle.
    if not _is_exact_local_storage(storage):
        return None

    file_keys = [key for key, _size in overlap_true_files]
    if len(set(file_keys)) != len(file_keys):
        logging.info(
            "[write-probe] duplicate mutation resource key; using polars path"
        )
        return None
    try:
        resolved_paths = [
            _storage_duckdb_path(storage, key) for key in file_keys
        ]
    except Exception as exc:
        logging.info(
            f"[write-probe] local path resolution failed, using polars path: {exc}"
        )
        return None

    pins = _pin_local_probe_files(storage, file_keys, resolved_paths)
    if pins is None:
        return None

    cache_owned: List[_LocalProbeFileIdentity] = []
    cache_scanned: Set[_LocalProbeFileIdentity] = set()
    cache_finished = False
    schema_cache_pending: Dict[
        _LocalProbeFileIdentity, _LocalProbeSchemaProof,
    ] = {}
    try:
        # Required mutation columns must physically exist with their exact
        # canonical spelling/type in EVERY file.  Allowing schema-union to
        # synthesize a NULL key could make an incoming NULL tombstone an
        # unrelated legacy row.  A missing newer-than column remains valid
        # legacy state and is materialized as typed NULL by the native scan.
        schema_cache_hits = 0
        schema_cache_misses = 0
        with p.span("io.island_probe_schema"):
            for pin in pins:
                schema_proof = _LOCAL_PROBE_SCHEMA_CACHE.get(pin.identity)
                if schema_proof is None:
                    schema_cache_misses += 1
                    arrow_schema = pq.read_schema(pin.scan_path)
                    schema_proof = _LocalProbeSchemaProof(
                        arrow_schema=arrow_schema,
                        rows=-1,
                        footer_sha256="",
                    )
                    schema_cache_pending[pin.identity] = schema_proof
                else:
                    schema_cache_hits += 1
                    arrow_schema = schema_proof.arrow_schema
                names = [str(field.name) for field in arrow_schema]
                folded: Dict[str, int] = {}
                for name in names:
                    key = name.casefold()
                    folded[key] = folded.get(key, 0) + 1
                if folded.get(_ISLAND_PROBE_SOURCE_COL.casefold(), 0):
                    raise ValueError(
                        "mutation candidate contains the reserved probe source column"
                    )
                physical = polars.Schema(arrow_schema)
                required = [ROWID_COL, *overwrite_columns]
                for column in required:
                    expected = projected_schema[column]
                    if (
                        column not in physical
                        or physical[column] != expected
                        or folded.get(column.casefold(), 0) != 1
                    ):
                        raise ValueError(
                            f"mutation candidate {pin.key!r} has missing, ambiguous, "
                            f"or non-{expected} column {column!r}"
                        )
                if newer_than_col and newer_than_col in physical:
                    if (
                        physical[newer_than_col]
                        != projected_schema[newer_than_col]
                    ):
                        raise ValueError(
                            f"mutation candidate {pin.key!r} has incompatible "
                            f"newer-than column {newer_than_col!r}"
                        )
        p.add("probe_schema_cache_hits", schema_cache_hits)
        p.add("probe_schema_cache_misses", schema_cache_misses)

        cache_owned, cache_hits = _LOCAL_ROWID_INTEGRITY_CACHE.reserve(
            pin.identity for pin in pins
        )
        p.add("probe_rowid_integrity_cache_hits", len(cache_hits))
        p.add("probe_rowid_integrity_cache_misses", len(cache_owned))
        owned = set(cache_owned)
        integrity_pins = [pin for pin in pins if pin.identity in owned]
        if integrity_pins:
            integrity_scan = _island_probe_scan(
                [pin.scan_path for pin in integrity_pins],
                {ROWID_COL: polars.Int64},
            )
            with p.span("io.island_probe_rowid_integrity"):
                integrity = (
                    integrity_scan
                    .group_by(_ISLAND_PROBE_SOURCE_COL)
                    .agg([
                        polars.len().alias("__rows__"),
                        polars.col(ROWID_COL).count().alias("__nonnull__"),
                        polars.col(ROWID_COL).n_unique().alias("__unique__"),
                        polars.col(ROWID_COL).min().alias("__minimum__"),
                    ])
                    .collect(engine="streaming")
                )
            expected_paths = {pin.scan_path for pin in integrity_pins}
            observed_paths = set(
                integrity.get_column(_ISLAND_PROBE_SOURCE_COL).to_list()
            ) if integrity.height else set()
            if not observed_paths.issubset(expected_paths):
                raise ValueError(
                    "native mutation probe returned an unknown source file"
                )
            invalid = integrity.filter(
                (polars.col("__rows__") != polars.col("__nonnull__"))
                | (polars.col("__rows__") != polars.col("__unique__"))
                | (polars.col("__minimum__") <= 0)
            )
            if invalid.height:
                raise ValueError(
                    "mutation candidate contains NULL, non-positive, or "
                    "duplicate rowids"
                )
            # Empty Parquet files do not appear in the grouped result and are
            # valid.  Publish their proof only after the main query and final
            # path/inode fence also succeed.
            cache_scanned.update(cache_owned)
            p.add("probe_rowid_integrity_scanned_files", len(integrity_pins))

        relation = _island_probe_scan(
            [pin.scan_path for pin in pins], projected_schema,
        )
        with p.span("io.island_probe"):
            matched = (
                relation
                .select([
                    _ISLAND_PROBE_SOURCE_COL,
                    ROWID_COL,
                    *overwrite_columns,
                    *([newer_than_col] if newer_than_col else []),
                ])
                .join(
                    incoming_keys.lazy(),
                    on=overwrite_columns,
                    how="semi",
                    nulls_equal=True,
                )
                .collect(engine="streaming")
            )
        if not _local_probe_pins_unchanged(pins):
            raise ValueError("local mutation candidate changed during probe")

        for pin in pins:
            proof = schema_cache_pending.get(pin.identity)
            if proof is not None:
                _LOCAL_PROBE_SCHEMA_CACHE.publish(pin.identity, proof)
        _LOCAL_ROWID_INTEGRITY_CACHE.finish(cache_owned, cache_scanned)
        cache_finished = True

        source_map = polars.DataFrame({
            _ISLAND_PROBE_SOURCE_COL: [pin.scan_path for pin in pins],
            TOMBSTONE_FILE_COL: [pin.key for pin in pins],
        })
        matched = matched.join(
            source_map, on=_ISLAND_PROBE_SOURCE_COL, how="left",
        ).drop(_ISLAND_PROBE_SOURCE_COL)
        if (
            TOMBSTONE_FILE_COL not in matched.columns
            or matched.get_column(TOMBSTONE_FILE_COL).null_count() > 0
            or ROWID_COL not in matched.columns
            or matched.get_column(ROWID_COL).null_count() > 0
        ):
            raise ValueError("native mutation probe returned ambiguous identity")
        p.add("probe_files", len(pins))
        p.add("probe_rows_matched", int(matched.height))
        logging.debug(
            f"[write-probe] island-native scan matched {matched.height} "
            f"existing row(s) across {len(pins)} file(s)"
        )
        return matched
    except Exception as exc:
        logging.info(
            f"[write-probe] island-native probe failed, using polars path: {exc}"
        )
        return None
    finally:
        if not cache_finished and cache_owned:
            _LOCAL_ROWID_INTEGRITY_CACHE.finish(cache_owned)
        for pin in pins:
            pin.close()


def _duckdb_probe_overlap_matches(
        overlap_true_files: List[Tuple[str, int]],
        overwrite_columns: List[str],
        newer_than_col: Optional[str],
        incoming_keys: polars.DataFrame,
        incoming_schema: Optional[Dict[str, polars.DataType]] = None,
        profiler: Optional[Profiler] = None,
        storage: Optional[object] = None,
) -> Optional[polars.DataFrame]:
    """Column-projected pushdown probe over the overlapping data files.

    Runs one ``parquet_scan`` (union_by_name, ranged GETs, row-group skipping)
    null-safe ``SEMI JOIN``-ed against the unique *incoming_keys*, projecting only
    ``__rowid__`` + the overwrite columns (+ *newer_than_col* when given) plus the
    source ``filename``.  Returns a polars frame with columns ``__file__`` (the
    original storage key), ``__rowid__``, the overwrite columns and the
    newer-than column — i.e. every existing row whose key matches an incoming
    key.  Returns ``None`` on any failure or unsupported schema (e.g. a referenced
    column absent from EVERY candidate file → DuckDB binder error), signalling the
    caller to fall back to the polars full-read path.
    """
    p = profiler or get_null_profiler()
    if not overlap_true_files or not overwrite_columns:
        return None

    exact_duckdb_types = {
        polars.Boolean: "BOOLEAN",
        polars.Int8: "TINYINT",
        polars.Int16: "SMALLINT",
        polars.Int32: "INTEGER",
        polars.Int64: "BIGINT",
        polars.UInt8: "UTINYINT",
        polars.UInt16: "USMALLINT",
        polars.UInt32: "UINTEGER",
        polars.UInt64: "UBIGINT",
        polars.Utf8: "VARCHAR",
    }
    expected_types: Dict[str, str] = {}
    source_schema = incoming_schema or dict(incoming_keys.schema)
    typed_columns = list(overwrite_columns)
    if newer_than_col and (
        incoming_schema is not None or newer_than_col in source_schema
    ):
        typed_columns.append(newer_than_col)
    for column in typed_columns:
        dtype = source_schema.get(column)
        expected = exact_duckdb_types.get(dtype)
        if expected is None:
            # DuckDB may coerce temporal, floating, decimal, or nested values
            # differently from Polars' strict mutation oracle. Acceleration is
            # optional; use the required=True path for exact semantics.
            logging.info(
                f"[write-probe] unsupported exact key type {column}={dtype}; "
                "using strict polars path"
            )
            return None
        expected_types[column] = expected

    try:
        import duckdb  # noqa: F401  (imported for availability check / errors)
        from supertable.engine.engine_common import (
            get_pooled_duckdb_connection,
            configure_httpfs_and_s3,
            escape_parquet_path,
            quote_if_needed,
        )
    except Exception as e:
        logging.info(f"[write-probe] duckdb unavailable, using polars path: {e}")
        return None

    if storage is None:
        storage = _get_storage()
    file_keys = [fk for fk, _sz in overlap_true_files]

    def _resolve(force_presign: bool):
        """Resolve keys → (duck_paths, {duck_path: original_key}).

        When *force_presign* (or SUPERTABLE_DUCKDB_PRESIGNED) is set, the paths
        are presigned URLs; the map still keys on the exact string handed to
        DuckDB so its returned ``filename`` resolves back to the storage key.
        """
        d2k: Dict[str, str] = {}
        paths: List[str] = []
        seen_keys: Set[str] = set()
        for k in file_keys:
            if k in seen_keys:
                raise ValueError(
                    "duplicate mutation resource key makes tombstone identity ambiguous"
                )
            seen_keys.add(k)
            dp = _storage_duckdb_path(storage, k, force_presign=force_presign)
            prior = d2k.get(dp)
            if prior is not None and prior != k:
                raise ValueError(
                    "resolved mutation path maps to multiple resource keys; "
                    "tombstone identity is ambiguous"
                )
            d2k[dp] = k
            paths.append(dp)
        return paths, d2k

    # Proactive: honours SUPERTABLE_DUCKDB_PRESIGNED for object stores exactly
    # like the read path. Local storage always stays on its filesystem path.
    # Identity ambiguity disables only the optional accelerator; the strict
    # storage-SDK fallback below still reads each raw resource key separately.
    try:
        duck_paths, duck_to_key = _resolve(force_presign=False)
    except Exception as e:
        logging.info(
            f"[write-probe] unsafe path identity, using polars path: {e}"
        )
        return None

    local_pins = _pin_local_probe_files(storage, file_keys, duck_paths)
    if local_pins is not None:
        # DuckDB reads the already-open inode, while path + open-fd identities
        # are checked again after both queries. This closes replacement races
        # between a cache lookup, integrity validation, and the key probe.
        duck_paths = [pin.scan_path for pin in local_pins]
        duck_to_key = {pin.scan_path: pin.key for pin in local_pins}
    elif _is_exact_local_storage(storage):
        p.add("probe_rowid_integrity_cache_identity_fallback", 1)

    cache_owned: List[_LocalProbeFileIdentity] = []
    cache_scanned: Set[_LocalProbeFileIdentity] = set()
    cache_finished = False

    select_cols = ["filename", quote_if_needed(ROWID_COL)]
    select_cols += [quote_if_needed(c) for c in overwrite_columns]
    if newer_than_col:
        select_cols.append(quote_if_needed(newer_than_col))
    def _join_operand(alias: str, column: str) -> str:
        operand = f"{alias}.{quote_if_needed(column)}"
        if expected_types.get(column) == "VARCHAR":
            # Pooled connections use default_collation=nocase for SELECT. Polars
            # overwrite equality is case-sensitive; force binary/C collation so
            # enabling the probe cannot turn A and a into the same mutation key.
            return f"({operand} COLLATE c)"
        return operand

    join_cond = " AND ".join(
        f"{_join_operand('src', c)} IS NOT DISTINCT FROM {_join_operand('k', c)}"
        for c in overwrite_columns
    )
    ik_name = f"__st_ik_{uuid.uuid4().hex}"

    def _run(paths):
        files_sql = ", ".join(f"'{escape_parquet_path(dp)}'" for dp in paths)
        # ``union_by_name`` materialises columns absent from one file as NULL.
        # That is unsafe for mutation keys: an incoming NULL can then match a
        # synthetic NULL and tombstone an unrelated legacy row; a synthetic
        # NULL rowid can also make a partial mutation appear successful. Inspect
        # each footer first and accept pushdown only when every physical file
        # has the canonical rowid plus every overwrite key. Any uncertainty
        # falls through to the required=True oracle, which aborts on absence.
        schema_sql = (
            "SELECT file_name, name, duckdb_type FROM "
            f"parquet_schema([{files_sql}]) WHERE column_id > 0"
        )
        with p.span("io.duckdb_probe_schema"):
            schema_rows = con.execute(schema_sql).fetchall()
        columns_by_file: Dict[str, Dict[str, str]] = {path: {} for path in paths}
        folded_by_file: Dict[str, Dict[str, int]] = {path: {} for path in paths}
        for schema_file, column_name, duckdb_type in schema_rows:
            if schema_file in columns_by_file:
                columns_by_file[schema_file][column_name] = str(duckdb_type or "").upper()
                folded = column_name.casefold()
                folded_by_file[schema_file][folded] = (
                    folded_by_file[schema_file].get(folded, 0) + 1
                )
        required_columns = {ROWID_COL, *overwrite_columns}
        incomplete = {}
        for path, columns in columns_by_file.items():
            problems = []
            for column in required_columns:
                if column not in columns:
                    problems.append(f"missing {column}")
                    continue
                expected = "BIGINT" if column == ROWID_COL else expected_types[column]
                if columns[column] != expected:
                    problems.append(
                        f"{column} type {columns[column]} != {expected}"
                    )
                if folded_by_file[path].get(column.casefold(), 0) != 1:
                    problems.append(f"ambiguous case-folded column {column}")
            if (
                newer_than_col
                and newer_than_col in columns
                and newer_than_col in expected_types
            ):
                expected = expected_types[newer_than_col]
                if columns[newer_than_col] != expected:
                    problems.append(
                        f"{newer_than_col} type {columns[newer_than_col]} != {expected}"
                    )
            if problems:
                incomplete[path] = problems
        if incomplete:
            raise ValueError(
                f"mutation candidate schema is incomplete: {incomplete!r}"
            )

        # Footer type checks are not enough: a legacy/previously compacted
        # file can contain duplicate positive BIGINT rowids.  A key-filtered
        # probe might see only one occurrence and then publish a tombstone that
        # hides an unrelated duplicate.  Scan only the rowid column, but scan
        # it completely for every candidate before the accelerator may emit a
        # pair.  Binary collation keeps case-distinct source paths separate
        # even though pooled SELECT connections default to nocase.
        integrity_paths = paths
        owned_for_run: List[_LocalProbeFileIdentity] = []
        if local_pins is not None:
            owned_for_run, cache_hits = _LOCAL_ROWID_INTEGRITY_CACHE.reserve(
                pin.identity for pin in local_pins
            )
            cache_owned.extend(owned_for_run)
            p.add("probe_rowid_integrity_cache_hits", len(cache_hits))
            p.add("probe_rowid_integrity_cache_misses", len(owned_for_run))
            owned_set = set(owned_for_run)
            integrity_paths = [
                pin.scan_path for pin in local_pins if pin.identity in owned_set
            ]

        integrity_rows = []
        if integrity_paths:
            integrity_files_sql = ", ".join(
                f"'{escape_parquet_path(path)}'" for path in integrity_paths
            )
            integrity_sql = (
                "SELECT filename COLLATE \"binary\" AS __st_file__, "
                "count(*) AS __rows__, "
                f"count({quote_if_needed(ROWID_COL)}) AS __nonnull__, "
                f"count(DISTINCT {quote_if_needed(ROWID_COL)}) AS __unique__, "
                f"min({quote_if_needed(ROWID_COL)}) AS __min__ "
                f"FROM parquet_scan([{integrity_files_sql}], union_by_name=TRUE, "
                "filename=TRUE, hive_partitioning=FALSE) "
                "GROUP BY filename COLLATE \"binary\""
            )
            with p.span("io.duckdb_probe_rowid_integrity"):
                integrity_rows = con.execute(integrity_sql).fetchall()
            p.add("probe_rowid_integrity_scanned_files", len(integrity_paths))
        checked_paths = set()
        for source_file, total, nonnull, unique, minimum in integrity_rows:
            source_file = str(source_file)
            checked_paths.add(source_file)
            if (
                int(total) != int(nonnull)
                or int(total) != int(unique)
                or (int(total) > 0 and (minimum is None or int(minimum) <= 0))
            ):
                raise ValueError(
                    f"mutation candidate {source_file!r} contains NULL, "
                    "non-positive, or duplicate rowids"
                )
        # Non-empty inputs must appear exactly once in the binary-collated
        # aggregate. Empty parquet files cannot match a key and are harmless.
        unexpected = checked_paths.difference(integrity_paths)
        if unexpected:
            raise ValueError(
                f"mutation probe returned unrecognized source file(s): {unexpected!r}"
            )
        # Empty Parquet inputs do not appear in the grouped result, but have no
        # rowids and are therefore valid. Publish only after the main query and
        # the post-query identity fence also succeed.
        cache_scanned.update(owned_for_run)
        sql = (
            f"SELECT {', '.join(select_cols)} "
            f"FROM parquet_scan([{files_sql}], union_by_name=TRUE, "
            f"filename=TRUE, hive_partitioning=FALSE) AS src "
            f"SEMI JOIN {ik_name} AS k ON {join_cond}"
        )
        logging.debug(
            f"[write-probe] duckdb scan: {len(paths)} file(s), "
            f"project={select_cols}, semi-join on {incoming_keys.height} key(s)"
        )
        with p.span("io.duckdb_probe"):
            return con.execute(sql).pl()

    con = None
    try:
        # Reuse this thread's pooled connection (cold-built exactly like the
        # read path: same pragmas, pinned home_directory so the probe never
        # falls back to the OS home, which is absent under a restricted service
        # user).  The pool re-applies httpfs/S3 for remote paths, so a warm
        # connection is configured for the current probe's object store.
        con = get_pooled_duckdb_connection(temp_dir="write_probe", for_paths=duck_paths)
        con.register(ik_name, incoming_keys.to_arrow())
        try:
            matched = _run(duck_paths)
        except Exception as e:
            # Reactive presign fallback — mirrors the read path's
            # create_reflection_view_with_presign_retry: a private object store
            # rejects an unsigned/expired read (403 / AccessDenied /
            # SignatureDoesNotMatch / HTTP …); presign the keys and retry once.
            msg = str(e)
            if local_pins is None and getattr(storage, "presign", None) and any(
                tok in msg for tok in _PRESIGN_RETRY_TOKENS
            ):
                logging.warning(f"[write-probe] presign fallback after: {msg}")
                duck_paths, duck_to_key = _resolve(force_presign=True)
                configure_httpfs_and_s3(con, duck_paths)
                matched = _run(duck_paths)
            else:
                raise
        if local_pins is not None:
            if not _local_probe_pins_unchanged(local_pins):
                raise ValueError("local mutation candidate changed during probe")
            _LOCAL_ROWID_INTEGRITY_CACHE.finish(cache_owned, cache_scanned)
            cache_finished = True
    except Exception as e:
        logging.info(f"[write-probe] probe failed, using polars path: {e}")
        return None
    finally:
        if not cache_finished and cache_owned:
            _LOCAL_ROWID_INTEGRITY_CACHE.finish(cache_owned)
        if con is not None:
            # Return the connection to the thread-local pool (do NOT close it);
            # only drop the per-probe registered relation so the uuid-named
            # keys table can't accumulate across reuses.
            try:
                con.unregister(ik_name)
            except Exception:
                pass
        for pin in local_pins or ():
            pin.close()

    if matched is None or "filename" not in matched.columns:
        return None
    # Restore the original storage key (DuckDB's ``filename`` is the path we
    # passed in) as __file__ via a join so the tombstone stores keys, not URLs.
    map_df = polars.DataFrame(
        {"filename": list(duck_to_key.keys()),
         TOMBSTONE_FILE_COL: list(duck_to_key.values())}
    )
    matched = matched.join(map_df, on="filename", how="left").drop("filename")
    if matched.get_column(TOMBSTONE_FILE_COL).null_count() > 0:
        # A returned filename did not map back — refuse to emit ambiguous
        # tombstones; let the caller fall back to the polars path.
        logging.info("[write-probe] unmapped filename in probe result; using polars path")
        return None
    if ROWID_COL not in matched.columns or matched.get_column(ROWID_COL).null_count() > 0:
        logging.info("[write-probe] missing/NULL rowid in probe result; using strict polars path")
        return None
    p.add("probe_files", len(duck_paths))
    p.add("probe_rows_matched", int(matched.height))
    logging.debug(
        f"[write-probe] duckdb scan matched {matched.height} existing row(s) "
        f"across {len(duck_paths)} file(s) (only key/__rowid__ columns read, "
        f"row groups skipped by footer min/max)"
    )
    return matched


def _align_keys_to_incoming(
        matched: polars.DataFrame,
        incoming_df: polars.DataFrame,
        overwrite_columns: List[str],
        newer_than_col: Optional[str],
) -> polars.DataFrame:
    """Cast probe-result key / newer-than columns to the incoming df's dtypes.

    DuckDB → Arrow → polars round-trips can yield a different (if compatible)
    dtype than the in-memory incoming frame; polars joins/comparisons want
    matching dtypes.  Casts are best-effort; an unrepresentable cast raises and
    the caller falls back to the polars path.
    """
    casts = []
    for c in overwrite_columns:
        if c in matched.columns and c in incoming_df.columns:
            if matched.schema[c] != incoming_df.schema[c]:
                casts.append(polars.col(c).cast(incoming_df.schema[c]))
    if newer_than_col and newer_than_col in matched.columns and newer_than_col in incoming_df.columns:
        if matched.schema[newer_than_col] != incoming_df.schema[newer_than_col]:
            casts.append(polars.col(newer_than_col).cast(incoming_df.schema[newer_than_col]))
    return matched.with_columns(casts) if casts else matched


def _derive_stale_and_deletes(
        incoming_df: polars.DataFrame,
        matched: polars.DataFrame,
        overwrite_columns: List[str],
        newer_than_col: Optional[str],
        profiler: Optional[Profiler] = None,
) -> Tuple[polars.DataFrame, List[Tuple[str, int]]]:
    """Derive (filtered incoming df, delete pairs) from the probe's matched rows.

    Mirrors the legacy two-function semantics exactly:
      * stale filter — drop incoming rows whose newer-than value is <= the max
        existing value for that key (null existing max ⇒ new/legacy key ⇒ keep);
        skipped entirely when *newer_than_col* is falsy;
      * delete pairs — ``(file, __rowid__)`` of existing rows matched by the
        SURVIVING incoming keys (null-safe), so stale rows tombstone nothing and
        rows without a ``__rowid__`` (legacy files) are dropped.
    """
    p = profiler or get_null_profiler()
    matched = _align_keys_to_incoming(matched, incoming_df, overwrite_columns, newer_than_col)

    if newer_than_col and newer_than_col in matched.columns:
        with p.span("newer_than.group_agg"):
            existing_max = matched.group_by(overwrite_columns).agg(
                polars.col(newer_than_col).max().alias("__existing_max__")
            )
        with p.span("newer_than.join_filter"):
            # nulls_equal=True keeps this consistent with the null-safe delete
            # semi-join below and the polars fallback oracle.
            joined = incoming_df.join(
                existing_max, on=overwrite_columns, how="left", nulls_equal=True
            )
            filtered = joined.filter(
                polars.col("__existing_max__").is_null()
                | (polars.col(newer_than_col) > polars.col("__existing_max__"))
            ).drop("__existing_max__")
    else:
        filtered = incoming_df

    pairs: List[Tuple[str, int]] = []
    if ROWID_COL in matched.columns:
        surviving_keys = filtered.select(overwrite_columns).unique()
        with p.span("delete.semi_join"):
            matched_surviving = matched.join(
                surviving_keys, on=overwrite_columns, how="semi", nulls_equal=True
            )
        dv = matched_surviving.select([TOMBSTONE_FILE_COL, ROWID_COL]).drop_nulls()
        pairs = [(file, int(rid)) for file, rid in dv.iter_rows()]
        p.add("delete_rows_matched", len(pairs))
    return filtered, pairs


def resolve_overwrite_writes(
        incoming_df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        newer_than_col: Optional[str] = None,
        profiler: Optional[Profiler] = None,
        required: bool = True,
        existing_tombstones: Optional[polars.DataFrame] = None,
        storage: Optional[object] = None,
) -> Tuple[polars.DataFrame, List[Tuple[str, int]]]:
    """Single-pass overwrite resolution: stale filtering + delete-vector pairs.

    Returns ``(filtered_incoming_df, delete_pairs)``. A sufficiently large local
    candidate set uses the Island-native Polars scanner; an explicitly enabled
    non-local compatibility lane uses DuckDB. Both compute the result from one
    projected match probe over the overlapping files. It falls back to the
    strict projected Polars path (``filter_stale_incoming_rows`` plus
    ``identify_deleted_rowids``) when acceleration is disabled/unavailable, a
    probe fails, or a file schema cannot be proven; semantics are identical on
    every path.

    *newer_than_col* falsy ⇒ no stale filtering (delete/upsert without conflict
    resolution); the incoming df is returned unchanged and every overlapping row
    matched by an incoming key is tombstoned.

    ``storage`` is the caller's already-pinned backend instance. ``None`` keeps
    local auto-selection off (while the explicit cross-backend flag still works),
    so direct processing callers never construct or contact a backend merely to
    choose an optional accelerator.
    """
    p = profiler or get_null_profiler()
    overlap_true = [(f, sz) for f, has_overlap, sz in overlapping_files if has_overlap]
    if not overlap_true or not overwrite_columns:
        return incoming_df, []

    key_cols = [c for c in overwrite_columns if c in incoming_df.columns]
    if key_cols != list(overwrite_columns):
        # Incoming df lacks a key column → no existing row can match (mirrors the
        # polars path, which returns no pairs and filters nothing).
        return incoming_df, []

    incoming_keys = incoming_df.select(overwrite_columns).unique()
    logging.debug(
        f"[write-probe] resolve: {len(overlap_true)} overlapping file(s), "
        f"{incoming_keys.height} unique incoming key(s) on {overwrite_columns}, "
        f"newer_than={newer_than_col}"
    )
    # Remote/object storage remains opt-in because probing it can require httpfs
    # and credentials. LocalStorage is auto-selected into the Island-native
    # open-fd scanner; every unsupported/error case still falls through to the
    # required=True projected Polars oracle below.
    matched = None
    probe_columns = list(dict.fromkeys(
        [ROWID_COL]
        + list(overwrite_columns)
        + ([newer_than_col] if newer_than_col else [])
    ))
    probe_selected, auto_local = _write_probe_selected(
        storage, overlap_true, probe_columns,
    )
    if auto_local:
        p.add("overwrite_resolve_probe_auto_local", 1)
    if probe_selected:
        probe_storage = storage
        if probe_storage is None:
            try:
                probe_storage = _get_storage()
            except Exception:
                probe_storage = None
        if _storage_reports_local(probe_storage):
            p.add("overwrite_resolve_probe_island_native", 1)
            matched = _island_probe_overlap_matches(
                overlap_true,
                overwrite_columns,
                newer_than_col,
                incoming_keys,
                incoming_schema=dict(incoming_df.schema),
                profiler=p,
                storage=probe_storage,
            )
        else:
            # Compatibility only: remote probing is never automatic and still
            # requires the explicit SUPERTABLE_DUCKDB_WRITE_PROBE opt-in.
            p.add("overwrite_resolve_probe_duckdb_remote", 1)
            matched = _duckdb_probe_overlap_matches(
                overlap_true,
                overwrite_columns,
                newer_than_col,
                incoming_keys,
                incoming_schema=dict(incoming_df.schema),
                profiler=p,
                storage=probe_storage,
            )
    if matched is not None:
        try:
            if existing_tombstones is not None and existing_tombstones.height:
                matched = matched.join(
                    existing_tombstones.select(
                        [TOMBSTONE_FILE_COL, ROWID_COL]
                    ),
                    on=[TOMBSTONE_FILE_COL, ROWID_COL],
                    how="anti",
                )
            return _derive_stale_and_deletes(
                incoming_df, matched, overwrite_columns, newer_than_col, profiler=p,
            )
        except Exception as e:
            logging.warning(f"[write-probe] derive failed, using polars path: {e}")

    # ---- Fallback: original polars full-read path (semantics oracle) ----
    p.add("overwrite_resolve_fallback", 1)
    # Project reads to only the columns the fallback consumes — overwrite keys
    # (+ newer-than for stale filtering) + __rowid__ (for the delete vector) —
    # so wide tables are not fully materialised into memory.  The shared
    # file_cache holds this projected union; each consumer selects its subset.
    read_columns = list(dict.fromkeys(
        list(overwrite_columns)
        + ([newer_than_col] if newer_than_col else [])
        + [ROWID_COL]
    ))
    logging.debug(
        f"[write-probe] polars full-read fallback over {len(overlap_true)} file(s), "
        f"reading only {read_columns}"
    )
    file_cache: Dict[str, polars.DataFrame] = {}
    dead_rowids_by_file: Dict[str, polars.Series] = {}
    if existing_tombstones is not None and existing_tombstones.height:
        # A mutation normally overlaps only a small fraction of a table.  Keep
        # the million-row vector columnar and partition only the file groups the
        # fallback will actually read; never materialise every rowid as a Python
        # ``int``/``set`` under the write lock.
        overlap_paths = [file_path for file_path, _size in overlap_true]
        relevant_tombstones = existing_tombstones.filter(
            polars.col(TOMBSTONE_FILE_COL).is_in(overlap_paths)
        )
        for key, group in relevant_tombstones.partition_by(
            TOMBSTONE_FILE_COL, as_dict=True, maintain_order=False
        ).items():
            file_path = key[0] if isinstance(key, tuple) else key
            dead_rowids_by_file[file_path] = group.get_column(ROWID_COL)
    if newer_than_col:
        filtered = filter_stale_incoming_rows(
            incoming_df=incoming_df,
            overlapping_files=overlapping_files,
            overwrite_columns=overwrite_columns,
            newer_than_col=newer_than_col,
            file_cache=file_cache,
            profiler=p,
            read_columns=read_columns,
            required=required,
            dead_rowids_by_file=dead_rowids_by_file,
            storage=storage,
        )
    else:
        filtered = incoming_df
    pairs = identify_deleted_rowids(
        filtered, overlapping_files, overwrite_columns,
        file_cache=file_cache, profiler=p, read_columns=read_columns,
        required=required,
        dead_rowids_by_file=dead_rowids_by_file,
        storage=storage,
    )
    return filtered, pairs


def _partitioned_new_artifact_path(
        base_dir: str,
        alias: str,
        extension: str,
        *,
        storage: Optional[object] = None,
) -> str:
    """Return a fresh versioned artifact path under an hour-partitioned subdir.

    Spreads immutable tombstone/stats artifacts across
    ``<base_dir>/year=YYYY/month=MM/day=DD/hour=HH/`` (UTC) so no single folder
    accumulates hundreds of thousands of files under heavy writes — which slows
    directory listing and per-file creation on object stores and real
    filesystems alike.  The partition subdir is created idempotently (a no-op
    prefix on object stores); the returned path is stored verbatim in the
    snapshot metadata, so reads are unaffected and pre-existing flat-layout
    files need no migration.
    """
    part_dir = os.path.join(base_dir, hourly_partition_subpath())
    try:
        # Direct makedirs (idempotent local, no-op object) — avoids a 404 prefix HEAD.
        active_storage = storage if storage is not None else _get_storage()
        active_storage.makedirs(part_dir)
    except Exception:
        pass
    return os.path.join(part_dir, generate_filename(alias, extension))


def _partitioned_new_path(base_dir: str, alias: str) -> str:
    """Backward-compatible Parquet spelling for existing metadata writers."""
    return _partitioned_new_artifact_path(base_dir, alias, "parquet")


def build_tombstone_file(
        tombstone_dir: str,
        prev_tombstone_path: Optional[str],
        new_pairs: List[Tuple[str, int]],
        compression_level: int,
        profiler: Optional[Profiler] = None,
        prev_df: Optional[polars.DataFrame] = None,
        persist: bool = True,
        prev_df_validated: bool = False,
        validation_out: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[polars.DataFrame]]:
    """Carry forward the previous deletion-vector and append newly deleted rows.

    The tombstone parquet has two columns: ``__file__`` (the data file that
    holds the row) and ``__rowid__``. Each delete writes a NEW immutable
    tombstone file = previous rows ∪ new rows (deduplicated on ``__rowid__``).

    *prev_df* lets the caller hand in the already-loaded previous deletion-vector
    (the writer reads it to exclude already-tombstoned rows) so it is not read
    from storage twice.  When ``None`` it is read from *prev_tombstone_path*.

    Returns ``(tombstone_path, combined_df)``:
      - no new pairs → ``(prev_tombstone_path, None)`` — pure carry-forward,
        the new snapshot reuses the previous file, no rewrite.
      - new pairs → ``(new_path, combined_df)`` where ``combined_df`` is the
        full deletion-vector (so the caller can run threshold compaction
        without re-reading the file).
    """
    p = profiler or get_null_profiler()
    if not new_pairs:
        return prev_tombstone_path, None

    new_df = polars.DataFrame(
        {
            TOMBSTONE_FILE_COL: [f for f, _ in new_pairs],
            ROWID_COL: [int(r) for _, r in new_pairs],
        }
    )

    if prev_df is None and prev_tombstone_path:
        # required=True: refuse to build a truncated deletion-vector if the
        # previous one exists but cannot be read (would resurrect dead rows).
        prev_df = _read_parquet_safe(prev_tombstone_path, profiler=p, required=True)
    if prev_df is not None and not prev_df_validated:
        prev_df = validate_tombstone_frame(
            prev_df, source=f"deletion-vector {prev_tombstone_path or '<memory>'}"
        )
    if prev_df is not None and prev_df.height > 0:
        combined = polars.concat(
            [prev_df.select([TOMBSTONE_FILE_COL, ROWID_COL]), new_df],
            how="vertical",
        )
    else:
        combined = new_df

    combined = combined.unique(
        subset=[TOMBSTONE_FILE_COL, ROWID_COL], keep="first", maintain_order=True
    )
    combined = combined.select(
        polars.col(TOMBSTONE_FILE_COL).cast(polars.Utf8),
        polars.col(ROWID_COL).cast(polars.Int64, strict=True),
    )
    validate_tombstone_frame(combined, source="new deletion-vector")
    if validation_out is not None:
        validation_out["frame"] = combined
        validation_out["digest"] = tombstone_digest(
            combined, assume_valid=True,
        )

    if not persist:
        # Threshold drains can consume this in-memory frame in the same locked
        # mutation.  Deferring the upload avoids creating a large immutable
        # artifact that no committed snapshot can ever reference.
        return None, combined
    new_path = _partitioned_new_path(tombstone_dir, "deleted")
    _write_df_parquet(combined, new_path, compression_level, profiler=p)
    return new_path, combined


def persist_tombstone_frame(
        tombstone_dir: str,
        frame: polars.DataFrame,
        compression_level: int,
        profiler: Optional[Profiler] = None,
) -> Tuple[str, polars.DataFrame]:
    """Persist an already materialised DV without Python row expansion."""
    validated = validate_tombstone_frame(
        frame, source="deletion-vector frame to persist"
    )
    if validated.height == 0:
        raise ValueError("Cannot persist an empty deletion-vector")
    new_path = _partitioned_new_path(tombstone_dir, "deleted")
    _write_df_parquet(validated, new_path, compression_level, profiler=profiler)
    return new_path, validated


def persist_tombstone_segment_v2(
        tombstone_dir: str,
        frame: polars.DataFrame,
        compression_level: int,
        profiler: Optional[Profiler] = None,
        *,
        known_digest: Optional[str] = None,
        storage: Optional[object] = None,
) -> TombstoneSegment:
    """Persist one non-empty immutable v2 segment and return all its seals."""
    p = profiler or get_null_profiler()
    with p.span("tombstone_v2.segment_validate"):
        validated = validate_tombstone_frame(
            frame, source="deletion-vector v2 segment to persist"
        )
    if validated.height == 0:
        raise ValueError("Cannot persist an empty deletion-vector segment")
    digest = _checked_tombstone_expected_digest(
        known_digest, source="deletion-vector v2 segment"
    )
    if digest is None:
        with p.span("tombstone_v2.segment_digest"):
            digest = tombstone_digest(validated, assume_valid=True)
    path = _partitioned_new_artifact_path(
        tombstone_dir, "segment", "parquet", storage=storage,
    )
    with p.span("tombstone_v2.segment_encode_write"):
        file_size = _write_df_parquet(
            validated,
            path,
            compression_level,
            profiler=p,
            storage=storage,
        )
    p.add("tombstone_v2_segment_rows", validated.height)
    p.add("tombstone_v2_segment_bytes", file_size)
    p.add("tombstone_v2_segments_written", 1)
    return TombstoneSegment(
        file=path,
        rows=int(validated.height),
        file_size=int(file_size),
        digest=digest,
    )


def persist_tombstone_manifest_v2(
        tombstone_dir: str,
        *,
        organization: str,
        super_name: str,
        simple_name: str,
        base_snapshot_version: int,
        snapshot_version: int,
        segments: Tuple[TombstoneSegment, ...],
        profiler: Optional[Profiler] = None,
        storage: Optional[object] = None,
) -> Tuple[str, TombstoneManifestV2]:
    """Persist one canonical standalone v2 manifest.

    Segment descriptors are sorted by immutable logical path before the strict
    core constructor validates the hard format cap and total cardinality.  The
    canonical bytes are written directly so every backend stores the same root
    representation; the snapshot pins ``manifest.digest()``.
    """
    p = profiler or get_null_profiler()
    with p.span("tombstone_v2.manifest_encode"):
        ordered = tuple(sorted(segments, key=lambda segment: segment.file))
        manifest = TombstoneManifestV2(
            organization=organization,
            super_name=super_name,
            simple_name=simple_name,
            base_snapshot_version=base_snapshot_version,
            snapshot_version=snapshot_version,
            total_rows=sum(segment.rows for segment in ordered),
            segments=ordered,
        )
        canonical = manifest.canonical_bytes()
    path = _partitioned_new_artifact_path(
        tombstone_dir, "manifest", "json", storage=storage,
    )
    active_storage = storage if storage is not None else _get_storage()
    write_bytes = getattr(active_storage, "write_bytes", None)
    if not callable(write_bytes):
        raise RuntimeError(
            "Configured storage provides no exact-byte manifest write method"
        )
    with p.span("tombstone_v2.manifest_write"):
        write_bytes(path, canonical)
    p.add("tombstone_v2_manifest_bytes", len(canonical))
    p.add("tombstone_v2_manifests_written", 1)
    p.add("tombstone_v2_segment_count", len(ordered))
    return path, manifest


def persist_tombstone_v2_frame(
        tombstone_dir: str,
        frame: polars.DataFrame,
        compression_level: int,
        *,
        organization: str,
        super_name: str,
        simple_name: str,
        base_snapshot_version: int,
        snapshot_version: int,
        profiler: Optional[Profiler] = None,
        storage: Optional[object] = None,
) -> Tuple[Optional[str], polars.DataFrame, LoadedTombstoneState]:
    """Persist a materialised successor as one segment plus one v2 root.

    This is used after reclamation/physical compaction changes an arbitrary set
    of old segments.  Exact empty v2 state writes no object at all.
    """
    p = profiler or get_null_profiler()
    with p.span("tombstone_v2.union_integrity"):
        validated = validate_tombstone_frame(
            frame, source="deletion-vector v2 successor to persist"
        )
    p.add("tombstone_v2_union_rows", validated.height)
    if validated.height == 0:
        return None, validated, LoadedTombstoneState(
            frame=validated,
            tombstone_format=TOMBSTONE_FORMAT_V2,
            tombstone_path=None,
            root_digest=None,
            segments=(),
        )
    segment = persist_tombstone_segment_v2(
        tombstone_dir,
        validated,
        compression_level,
        profiler=p,
        storage=storage,
    )
    path, manifest = persist_tombstone_manifest_v2(
        tombstone_dir,
        organization=organization,
        super_name=super_name,
        simple_name=simple_name,
        base_snapshot_version=base_snapshot_version,
        snapshot_version=snapshot_version,
        segments=(segment,),
        profiler=p,
        storage=storage,
    )
    return path, validated, LoadedTombstoneState(
        frame=validated,
        tombstone_format=TOMBSTONE_FORMAT_V2,
        tombstone_path=path,
        root_digest=manifest.digest(),
        segments=manifest.segments,
    )


def build_tombstone_v2(
        tombstone_dir: str,
        previous_state: Optional[LoadedTombstoneState],
        new_pairs: List[Tuple[str, int]],
        compression_level: int,
        *,
        organization: str,
        super_name: str,
        simple_name: str,
        base_snapshot_version: int,
        snapshot_version: int,
        profiler: Optional[Profiler] = None,
        persist: bool = True,
        validation_out: Optional[Dict[str, Any]] = None,
        storage: Optional[object] = None,
) -> Tuple[Optional[str], Optional[polars.DataFrame], Optional[LoadedTombstoneState]]:
    """Append one logical delta segment and publish a new standalone root.

    The prior union stays immutable.  At the hard 32-segment format cap the
    complete validated union is consolidated into one replacement segment.
    ``persist=False`` materialises only the union for an immediately following
    physical drain and emits neither a segment nor a manifest.
    """
    if not new_pairs:
        return (
            previous_state.tombstone_path if previous_state is not None else None,
            None,
            previous_state,
        )

    p = profiler or get_null_profiler()
    with p.span("tombstone_v2.delta_validate"):
        new_df = polars.DataFrame(
            {
                TOMBSTONE_FILE_COL: [file for file, _rowid in new_pairs],
                ROWID_COL: [int(rowid) for _file, rowid in new_pairs],
            },
            schema=TOMBSTONE_SCHEMA,
        )
        new_df = validate_tombstone_frame(
            new_df, source="new deletion-vector v2 segment"
        )
    p.add("tombstone_v2_delta_rows", new_df.height)
    previous_frame = (
        previous_state.frame
        if previous_state is not None else _empty_tombstone_df()
    )
    prior_segments = (
        previous_state.segments if previous_state is not None else ()
    )
    if previous_state is not None:
        _normalized_tombstone_format(previous_state.tombstone_format)
        if len(prior_segments) > MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
            raise ValueError("Previous deletion-vector exceeds the segment cap")
        if sum(segment.rows for segment in prior_segments) != previous_frame.height:
            raise ValueError(
                "Previous deletion-vector segment rows do not match its union"
            )
        if previous_frame.height and not prior_segments:
            raise ValueError("Previous deletion-vector state has no segment seal")
    delta = new_df
    with p.span("tombstone_v2.union_integrity"):
        combined = polars.concat(
            [previous_frame, delta], how="vertical",
        ) if previous_frame.height else delta
        combined = validate_tombstone_frame(
            combined, source="new deletion-vector v2 union"
        )
    p.add("tombstone_v2_union_rows", combined.height)
    if validation_out is not None:
        validation_out["frame"] = combined
    if not persist:
        return None, combined, None

    if len(prior_segments) >= MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
        p.add("tombstone_v2_consolidations", 1)
        segments = (
            persist_tombstone_segment_v2(
                tombstone_dir,
                combined,
                compression_level,
                profiler=profiler,
                storage=storage,
            ),
        )
    else:
        delta_segment = persist_tombstone_segment_v2(
            tombstone_dir,
            delta,
            compression_level,
            profiler=profiler,
            storage=storage,
        )
        segments = prior_segments + (delta_segment,)
    path, manifest = persist_tombstone_manifest_v2(
        tombstone_dir,
        organization=organization,
        super_name=super_name,
        simple_name=simple_name,
        base_snapshot_version=base_snapshot_version,
        snapshot_version=snapshot_version,
        segments=segments,
        profiler=p,
        storage=storage,
    )
    state = LoadedTombstoneState(
        frame=combined,
        tombstone_format=TOMBSTONE_FORMAT_V2,
        tombstone_path=path,
        root_digest=manifest.digest(),
        segments=manifest.segments,
    )
    if validation_out is not None:
        validation_out["state"] = state
        validation_out["digest"] = state.root_digest
    return path, combined, state


def reclaim_fully_dead_files(
        resources: List[Dict],
        combined_dv: polars.DataFrame,
        tombstone_dir: str,
        compression_level: int,
        profiler: Optional[Profiler] = None,
        persist: bool = True,
        assume_valid: bool = False,
) -> Tuple[Set[str], Optional[str], Optional[polars.DataFrame]]:
    """Drop data files whose every physical row is in the deletion-vector.

    Merge-on-read defers physical deletes to the compaction threshold, so a
    file whose rows are *all* tombstoned otherwise lingers in the snapshot —
    re-scanned by every later overwrite probe and inflating the resource list.
    Such a file is 100% dead and can be removed for free: no rewrite, just drop
    it from ``resources`` and drop its ``__rowid__``s from the vector.

    A file is fully dead when the count of its rowids in *combined_dv* equals
    its physical ``rows`` count.  (rowids are table-unique and each lives in one
    file, so the vector never holds more rowids for a file than the file has.)

    Returns ``(sunset_files, new_tombstone_path, new_dv)``:
      - nothing fully dead → ``(set(), None, None)`` — caller keeps its current
        tombstone pointer / frame unchanged;
      - some fully dead → the reclaimed file keys, plus a freshly written
        tombstone holding only the surviving rowids, or
        ``(sunset_files, None, None)`` when the vector is emptied entirely.
    """
    p = profiler or get_null_profiler()
    if combined_dv is None or combined_dv.height == 0 or not resources:
        return set(), None, None

    if not assume_valid:
        combined_dv = validate_tombstone_frame(
            combined_dv, source="deletion-vector used for eager reclamation"
        )
    tombstone_groups = combined_dv.partition_by(
        TOMBSTONE_FILE_COL, as_dict=True, maintain_order=False
    )
    dead_ids_by_file = {
        (key[0] if isinstance(key, tuple) else key):
        group.get_column(ROWID_COL)
        for key, group in tombstone_groups.items()
    }

    fully_dead: Set[str] = set()
    for r in resources:
        if not isinstance(r, dict):
            continue
        f = r.get("file")
        rows = int(r.get("rows") or 0)
        dead_ids = dead_ids_by_file.get(f)
        if not (f and rows > 0 and dead_ids is not None and len(dead_ids) >= rows):
            continue

        # Resource row counts are metadata and may be stale/corrupt.  They are
        # only a cheap candidate filter; prove coverage against the physical
        # rowid column before metadata-sunsetting a file.  Failure to prove is a
        # conservative retain, because eager reclaim is only an optimisation.
        physical = _read_parquet_safe(
            f,
            profiler=p,
            file_size=int(r.get("file_size") or 0),
            columns=[ROWID_COL],
            required=False,
        )
        if physical is None or ROWID_COL not in physical.columns:
            continue
        physical_ids = physical.get_column(ROWID_COL)
        if (
            physical_ids.dtype != polars.Int64
            or physical_ids.null_count() > 0
            or physical_ids.min() is None
            or physical_ids.min() <= 0
            or physical_ids.n_unique() != physical.height
        ):
            continue
        # Exact equality proves that every DV entry for this file names one
        # physical row and every physical row is dead.  A mere subset proof
        # would discard ghost DV entries while sunsetting the file.
        physical_frame = physical.select(ROWID_COL)
        dead_frame = polars.DataFrame({ROWID_COL: dead_ids})
        physical_missing = physical_frame.join(
            dead_frame, on=ROWID_COL, how="anti"
        ).height
        dead_missing = dead_frame.join(
            physical_frame, on=ROWID_COL, how="anti"
        ).height
        if physical_missing == 0 and dead_missing == 0:
            fully_dead.add(f)

    if not fully_dead:
        return set(), None, None

    p.add("reclaimed_dead_files", len(fully_dead))

    survivors = combined_dv.filter(
        ~polars.col(TOMBSTONE_FILE_COL).is_in(list(fully_dead))
    )
    if survivors.height == 0:
        return fully_dead, None, None

    if not persist:
        return fully_dead, None, survivors

    new_path = _partitioned_new_path(tombstone_dir, "deleted")
    _write_df_parquet(survivors, new_path, compression_level, profiler=p)
    return fully_dead, new_path, survivors


# =========================
# Column-statistics artifact (external "stats parquet")
# =========================
#
# A per-table, immutable, versioned parquet built by reading the FOOTERS of the
# data parquet files (no data scan).  One row per (file × row_group × column).
# It mirrors the tombstone deletion-vector exactly: never overwritten, carried
# forward on each write (minus rows for sunset files, plus rows for new files),
# and referenced by the snapshot via ``stats_file`` / ``stats_rows``.  The two
# consumers (write-path overwrite/delete pruning and read-path SELECT pruning)
# use it to skip data files whose row-group min/max ranges cannot overlap a
# predicate.  ``stats_available=False`` rows are always retained by both
# consumers (decimal / unsupported / no footer stats → never used to exclude).

TIMESTAMP_COL = "__timestamp__"

# Schema (column order is significant — keep it stable, the artifact is sealed).
STATS_SCHEMA: Dict[str, polars.DataType] = {
    "file_path": polars.Utf8,
    # Strong binding between every stats row and the exact Parquet footer from
    # which it was extracted.  A row is usable for absence pruning only when the
    # owning snapshot resource also pins this footer plus its complete canonical
    # stats-row digest. Legacy NULL/unpinned seals disable all absence pruning.
    "footer_sha256": polars.Utf8,
    "row_group_id": polars.Int64,
    "column_name": polars.Utf8,
    "physical_type": polars.Utf8,
    "logical_type": polars.Utf8,
    "min_bigint": polars.Int64,
    "max_bigint": polars.Int64,
    "min_double": polars.Float64,
    "max_double": polars.Float64,
    "min_timestamp": polars.Datetime("us"),
    "max_timestamp": polars.Datetime("us"),
    "min_string": polars.Utf8,
    "max_string": polars.Utf8,
    "null_count": polars.Int64,
    "row_group_rows": polars.Int64,
    # On-disk (compressed) size of this column-chunk, from the parquet footer.
    # Drives projection-aware read estimation: a query that selects N of M
    # columns scans only the selected columns' chunks, not the whole file.
    # Nullable so a stats file written before this column carries forward.
    "compressed_bytes": polars.Int64,
    # Parquet's decompressed *encoded-page* size for this column chunk. RLE and
    # dictionary pages can be orders of magnitude smaller than decoded Arrow
    # buffers, so this metric is never used alone as a memory upper bound.
    "uncompressed_bytes": polars.Int64,
    "stats_available": polars.Boolean,
    "min_is_exact": polars.Boolean,
    "max_is_exact": polars.Boolean,
}

STATS_FILE_PATH_COL = "file_path"


@dataclass(frozen=True)
class _FooterStatsCacheEntry:
    """Writer-local reuse of one parsed footer and its canonical stats rows."""

    metadata: Any
    rows: List[dict]

# Domain-separate the per-resource digest from every other SHA-256 used by the
# storage format.  Bumping this marker is mandatory if the canonical encoding
# below ever changes.  The schema declaration itself is included once per
# digest, so a column addition/reorder cannot accidentally validate an older
# resource seal.
_STATS_RESOURCE_DIGEST_DOMAIN = b"supertable.stats-resource.v1\x00"
_STATS_EPOCH = datetime(1970, 1, 1)


def _digest_length_prefixed(buffer: bytearray, payload: bytes) -> None:
    buffer.extend(struct.pack(">Q", len(payload)))
    buffer.extend(payload)


def _canonical_stats_row_bytes(row: Tuple[object, ...]) -> bytes:
    """Encode one exact ``STATS_SCHEMA`` row without JSON/locale ambiguity."""
    if len(row) != len(STATS_SCHEMA):
        raise ValueError("stats row width does not match STATS_SCHEMA")
    out = bytearray()
    for value, dtype in zip(row, STATS_SCHEMA.values()):
        if value is None:
            out.append(0)
            continue
        out.append(1)
        if dtype == polars.Utf8:
            if not isinstance(value, str):
                raise ValueError("stats string lane contains a non-string value")
            _digest_length_prefixed(out, value.encode("utf-8"))
        elif dtype == polars.Int64:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError("stats integer lane contains a non-integer value")
            out.extend(struct.pack(">q", value))
        elif dtype == polars.Float64:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("stats float lane contains a non-numeric value")
            out.extend(struct.pack(">d", float(value)))
        elif isinstance(dtype, polars.Datetime):
            if not isinstance(value, datetime) or value.tzinfo is not None:
                raise ValueError("stats timestamp lane is not a naive datetime")
            delta = value - _STATS_EPOCH
            micros = (
                (delta.days * 86_400 + delta.seconds) * 1_000_000
                + delta.microseconds
            )
            out.extend(struct.pack(">q", micros))
        elif dtype == polars.Boolean:
            if not isinstance(value, bool):
                raise ValueError("stats boolean lane contains a non-boolean value")
            out.append(1 if value else 0)
        else:  # pragma: no cover - guarded by the frozen STATS_SCHEMA test
            raise ValueError(f"unsupported canonical stats dtype: {dtype!r}")
    return bytes(out)


def _new_stats_resource_hasher(file_path: str):
    if not isinstance(file_path, str) or not file_path:
        raise ValueError("stats resource path must be a non-empty string")
    digest = hashlib.sha256()
    digest.update(_STATS_RESOURCE_DIGEST_DOMAIN)
    schema_marker = "\x1f".join(
        f"{name}:{dtype!s}" for name, dtype in STATS_SCHEMA.items()
    ).encode("utf-8")
    digest.update(struct.pack(">Q", len(schema_marker)))
    digest.update(schema_marker)
    encoded_path = file_path.encode("utf-8")
    digest.update(struct.pack(">Q", len(encoded_path)))
    digest.update(encoded_path)
    return digest


def stats_resource_seals(
        stats_df: Optional[polars.DataFrame],
) -> Optional[Dict[str, ResourceStatsSeal]]:
    """Return deterministic seals for every complete per-file row stream.

    Rows are ordered by their canonical identity before hashing, so Parquet row
    order and Polars chunking cannot affect the result.  A file with NULL or
    conflicting footer hashes is omitted from the returned mapping.  Callers
    treat omission as unknown and retain that physical resource.
    """
    if stats_df is None or not isinstance(stats_df, polars.DataFrame):
        return None
    if list(stats_df.columns) != list(STATS_SCHEMA) or stats_df.schema != STATS_SCHEMA:
        return None
    if stats_df.height == 0:
        return {}
    try:
        ordered = stats_df.sort(
            ["file_path", "row_group_id", "column_name"],
            maintain_order=True,
        )
        file_index = list(STATS_SCHEMA).index("file_path")
        footer_index = list(STATS_SCHEMA).index("footer_sha256")
        result: Dict[str, ResourceStatsSeal] = {}
        current_path: Optional[str] = None
        current_digest = None
        current_rows = 0
        current_footer: Optional[str] = None
        footer_valid = True

        def finish() -> None:
            if current_path is None or current_digest is None or not footer_valid:
                return
            try:
                result[current_path] = ResourceStatsSeal(
                    footer_sha256=current_footer,
                    stats_rows=current_rows,
                    stats_digest=current_digest.hexdigest(),
                )
            except (TypeError, ValueError):
                # Invalid footer strings are deliberately unsealed/fail-open.
                return

        for row in ordered.iter_rows(buffer_size=1024):
            file_path = row[file_index]
            if not isinstance(file_path, str) or not file_path:
                return None
            if file_path != current_path:
                finish()
                current_path = file_path
                current_digest = _new_stats_resource_hasher(file_path)
                current_rows = 0
                current_footer = None
                footer_valid = True
            footer = row[footer_index]
            if current_rows == 0:
                current_footer = footer if isinstance(footer, str) else None
            elif footer != current_footer:
                footer_valid = False
            if not isinstance(footer, str):
                footer_valid = False
            current_digest.update(_canonical_stats_row_bytes(row))
            current_rows += 1
        finish()
        return result
    except Exception:
        # Statistics are optional.  Any malformed canonical value disables their
        # use instead of surfacing an error or narrowing a scan.
        return None


def resource_stats_seal(resource: object) -> Optional[ResourceStatsSeal]:
    """Parse one resource dict's three-field seal, returning ``None`` for legacy."""
    if not isinstance(resource, dict):
        return None
    try:
        return ResourceStatsSeal(
            footer_sha256=resource.get("footer_sha256"),
            stats_rows=resource.get("stats_rows"),
            stats_digest=resource.get("stats_digest"),
        )
    except (TypeError, ValueError):
        return None


def resource_object_seal(resource: object) -> Optional[ResourceObjectSeal]:
    """Parse a snapshot resource's optional provider identity seal.

    The duplicated size is intentional: it binds the provider metadata to the
    same bytes described by ``file_size``. Any legacy, malformed, or disagreeing
    value disables only the HEAD-elision optimization.
    """
    if not isinstance(resource, dict):
        return None
    raw = resource.get("object_seal")
    if not isinstance(raw, dict):
        return None
    try:
        seal = ResourceObjectSeal(
            size=raw.get("size"),
            version=raw.get("version", ""),
            etag=raw.get("etag", ""),
            last_modified_ns=raw.get("last_modified_ns", 0),
            checksum_sha256=raw.get("checksum_sha256", ""),
        )
    except (TypeError, ValueError):
        return None
    declared_size = resource.get("file_size")
    if (
        not isinstance(declared_size, int)
        or isinstance(declared_size, bool)
        or declared_size < 0
        or seal.size != declared_size
    ):
        return None
    return seal


def _uploaded_resource_object_seal(
    storage: object,
    path: str,
    expected_size: int,
) -> Optional[ResourceObjectSeal]:
    """Observe one newly uploaded immutable object without making it required.

    Object identity is a performance seal, never a commit prerequisite. A
    backend that cannot expose stable conditional-read metadata stays readable
    through IslandDB's existing per-query ``stat_object`` path.
    """
    is_local = getattr(storage, "is_local_storage", None)
    if callable(is_local):
        try:
            if is_local() is True:
                return None
        except Exception:
            # Unknown storage locality is treated as remote; the stat attempt
            # below remains optional and safely fails open.
            pass
    stat_object = getattr(storage, "stat_object", None)
    if not callable(stat_object):
        return None
    try:
        metadata = stat_object(path)
    except Exception as exc:
        logging.warning(
            "[write.object_seal] immutable object metadata unavailable for %r: %s",
            path,
            exc,
        )
        return None
    # StorageInterface's contract is deliberately strict here. Duck-typing an
    # arbitrary SDK response could serialize a repr or an unstable timestamp
    # and later use it as a conditional-read authority.
    if not isinstance(metadata, ObjectMetadata):
        return None
    if metadata.size != expected_size:
        logging.warning(
            "[write.object_seal] uploaded object size mismatch for %r: "
            "encoded=%d observed=%d; HEAD elision disabled",
            path,
            expected_size,
            metadata.size,
        )
        return None
    try:
        return ResourceObjectSeal(
            size=metadata.size,
            version=metadata.version,
            etag=metadata.etag,
            last_modified_ns=metadata.last_modified_ns,
            checksum_sha256=metadata.checksum_sha256,
        )
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class _StatsFrameValidation:
    """Cold validation result memoised for one immutable stats artifact."""

    resource_seals: Optional[Dict[str, ResourceStatsSeal]]
    complete_resource_rows: Dict[str, int]


def _validate_stats_frame_once(stats_df: polars.DataFrame) -> _StatsFrameValidation:
    """Compute digest and structural completeness indexes in one cold pass."""
    seals = stats_resource_seals(stats_df)
    if seals is None or stats_df.height == 0:
        return _StatsFrameValidation(seals, {})
    required = ["file_path", "row_group_id", "column_name", "row_group_rows"]
    try:
        groups = (
            stats_df.select(required)
            .group_by(["file_path", "row_group_id"])
            .agg([
                polars.len().alias("__slots"),
                polars.col("column_name").n_unique().alias("__unique_slots"),
                polars.col("column_name").unique().sort().alias("__columns"),
                polars.col("row_group_rows").n_unique().alias("__row_counts"),
                polars.col("row_group_rows").first().alias("__rows"),
                polars.col("column_name").is_not_null().all().alias("__names_valid"),
                (
                    polars.col("row_group_rows").is_not_null()
                    & (polars.col("row_group_rows") >= 0)
                ).all().alias("__rows_valid"),
            ])
        )
        complete = (
            groups.group_by("file_path")
            .agg([
                polars.len().alias("__group_count"),
                polars.col("row_group_id").min().alias("__min_group"),
                polars.col("row_group_id").max().alias("__max_group"),
                polars.col("row_group_id").is_not_null().all().alias("__ids_valid"),
                (polars.col("row_group_id") >= 0).all().alias("__ids_nonnegative"),
                polars.col("__rows").sum().alias("__total_rows"),
                (polars.col("__slots") == polars.col("__unique_slots"))
                .all().alias("__slots_valid"),
                (polars.col("__row_counts") == 1).all().alias("__counts_valid"),
                polars.col("__names_valid").all().alias("__all_names_valid"),
                polars.col("__rows_valid").all().alias("__all_rows_valid"),
                polars.col("__columns").n_unique().alias("__column_sets"),
            ])
            .filter(
                polars.col("__ids_valid")
                & polars.col("__ids_nonnegative")
                & polars.col("__slots_valid")
                & polars.col("__counts_valid")
                & polars.col("__all_names_valid")
                & polars.col("__all_rows_valid")
                & (polars.col("__min_group") == 0)
                & (polars.col("__max_group") == polars.col("__group_count") - 1)
                & (polars.col("__column_sets") == 1)
            )
            .select(["file_path", "__total_rows"])
        )
        complete_rows = {
            file_path: int(rows)
            for file_path, rows in complete.iter_rows()
            if isinstance(file_path, str)
            and isinstance(rows, int)
            and not isinstance(rows, bool)
            and rows >= 0
            and file_path in seals
        }
        return _StatsFrameValidation(seals, complete_rows)
    except Exception:
        return _StatsFrameValidation(None, {})

# Narrow SELECT-pruning projections do not materialize the rejected
# double/string bounds into Python.  They carry this boolean instead so a
# malformed row with both a trusted lane and any second populated lane cannot
# evade _stored_lane's exactly-one-lane invariant.
_SELECT_OTHER_LANE_COL = "__select_other_lane_populated"

# Internal system columns never emitted into the stats artifact (they must not
# leak, same as everywhere else).
_STATS_SYSTEM_COLUMNS = {ROWID_COL, TIMESTAMP_COL}


def _logical_type_name(stat) -> str:
    """Return a lossless-enough parquet logical type marker for pruning.

    PyArrow exposes both TIMESTAMP and TIMESTAMPTZ as ``type ==
    "TIMESTAMP"``.  The distinction lives in ``isAdjustedToUTC`` in the
    logical type JSON.  Losing it is unsafe for range propagation: DuckDB
    compares a naïve timestamp/date to a zoned timestamp in the session
    timezone, not by comparing their raw footer integers.  New stats artifacts
    therefore persist both semantics and resolution, for example
    ``TIMESTAMP_NTZ_MICROS`` or ``TIMESTAMP_TZ_MILLIS``.  Historical rows
    containing the old ambiguous ``TIMESTAMP`` marker (or a marker without a
    resolution) are deliberately treated as unavailable by select pruning.
    """
    try:
        lt = stat.logical_type
        if lt is None:
            return ""
        name = getattr(lt, "type", None)
        if name is None or str(name).upper() == "NONE":
            return ""
        name = str(name).upper()
        if name == "TIMESTAMP":
            try:
                payload = json.loads(lt.to_json())
                adjusted = payload.get("isAdjustedToUTC")
                unit = {
                    "MILLISECONDS": "MILLIS",
                    "MICROSECONDS": "MICROS",
                    "NANOSECONDS": "NANOS",
                }.get(str(payload.get("timeUnit") or "").upper())
                if adjusted is True and unit:
                    return f"TIMESTAMP_TZ_{unit}"
                if adjusted is False and unit:
                    return f"TIMESTAMP_NTZ_{unit}"
            except Exception:
                # An ambiguous marker is retained in the artifact for
                # observability, but select pruning will not trust it.
                pass
        return name
    except Exception:
        return ""


def _to_us_datetime(v) -> Optional[datetime]:
    """Normalise a footer min/max into a tz-naive microsecond ``datetime``.

    ``datetime`` (tz-aware → converted to UTC wall time, tz dropped) and ``date``
    (→ midnight) are supported; anything else returns ``None`` (→ unsupported,
    stats_available stays False so the column never prunes).
    """
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v
    # NOTE: ``datetime`` is a subclass of ``date`` — handled above first.
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    return None


def _route_stats(stat) -> Tuple[Optional[str], object, object]:
    """Route a footer ``Statistics`` to a typed column.

    Returns ``(category, min_val, max_val)`` where ``category`` is one of
    ``bigint`` / ``double`` / ``timestamp`` / ``string`` (with normalised
    values), or ``(None, None, None)`` when the type is unsupported for pruning
    (decimal — lossy as double, binary, time, etc.).  Conservative: an
    unsupported type yields no usable range, so the column is never used to
    exclude a file.
    """
    mn, mx = stat.min, stat.max
    # Decimal is intentionally unsupported: routing through double is lossy and
    # could cause false negatives. Detected via logical type or decoded value.
    if _logical_type_name(stat).upper() == "DECIMAL":
        return None, None, None
    if isinstance(mn, decimal.Decimal) or isinstance(mx, decimal.Decimal):
        return None, None, None
    # date / timestamp → micros (datetime is a date subclass; both routed here)
    if isinstance(mn, date):
        a = _to_us_datetime(mn)
        b = _to_us_datetime(mx)
        if a is None or b is None:
            return None, None, None
        return "timestamp", a, b
    # bool(0/1) and all integer widths → bigint (bool is an int subclass)
    if isinstance(mn, bool):
        return "bigint", int(mn), int(mx)
    if isinstance(mn, int):
        # STATS_SCHEMA stores signed Int64.  PyArrow decodes UInt64 footer
        # values as Python ints too; attempting to append values above 2**63-1
        # raises while building the stats frame and can abort an otherwise
        # valid write.  Out-of-lane integers are simply unsupported: every
        # pruning consumer then retains the file.
        lo, hi = int(mn), int(mx)
        if not (-(2**63) <= lo <= 2**63 - 1) or not (
            -(2**63) <= hi <= 2**63 - 1
        ):
            return None, None, None
        return "bigint", lo, hi
    if isinstance(mn, float):
        return "double", float(mn), float(mx)
    if isinstance(mn, str):
        return "string", str(mn), str(mx)
    # bytes / binary / anything else → unsupported
    return None, None, None


def _read_footer_metadata(path: str, profiler: Optional[Profiler] = None):
    """Read just the parquet footer (FileMetaData) for *path*, or ``None``.

    Reads the file's bytes from the active storage backend and parses only the
    footer via ``pq.read_metadata`` — no data pages are decoded.  Returns
    ``None`` on a race (file already sunset) or any read/parse error.
    """
    p = profiler or get_null_profiler()
    if not _safe_exists(path, profiler=p):
        logging.info(f"[stats] file already sunset before footer read: {path}")
        return None
    try:
        with p.span("stats.read_footer"):
            data = _get_storage().read_bytes(path)
            return pq.read_metadata(io.BytesIO(data))
    except FileNotFoundError:
        logging.info(f"[stats] file vanished before footer read: {path}")
        return None
    except Exception as e:
        logging.warning(f"[stats] failed to read footer at {path}: {e}")
        return None


def parquet_footer_sha256(md) -> str:
    """Return a stable SHA-256 seal for one PyArrow Parquet footer."""
    payload = io.BytesIO()
    md.write_metadata_file(payload)
    return hashlib.sha256(payload.getvalue()).hexdigest()


def _stats_rows_for_metadata(
        file_path: str,
        md,
        *,
        footer_sha256: Optional[str] = None,
) -> List[dict]:
    """Build the per-(row_group × column) stats rows for one file's footer."""
    rows: List[dict] = []
    if footer_sha256 is None:
        try:
            footer_sha256 = parquet_footer_sha256(md)
        except Exception:
            # Stats remain usable for conservative file pruning, but no executor may
            # trust row-group IDs that are not bound to an exact live footer.
            footer_sha256 = None
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        rg_rows = int(g.num_rows)
        for c in range(g.num_columns):
            col = g.column(c)
            name = col.path_in_schema
            if name in _STATS_SYSTEM_COLUMNS:
                continue
            try:
                stat = col.statistics if col.is_stats_set else None
            except Exception:
                stat = None
            row = {k: None for k in STATS_SCHEMA}
            row["file_path"] = file_path
            row["footer_sha256"] = footer_sha256
            row["row_group_id"] = int(rg)
            row["column_name"] = name
            row["physical_type"] = str(col.physical_type or "")
            row["logical_type"] = _logical_type_name(stat) if stat is not None else ""
            try:
                row["null_count"] = (
                    int(stat.null_count)
                    if stat is not None and stat.null_count is not None
                    else None
                )
            except Exception:
                row["null_count"] = None
            row["row_group_rows"] = rg_rows
            # On-disk (compressed) bytes of this column-chunk — the bytes a
            # projection-pushdown scan actually fetches for this column.
            try:
                tcs = col.total_compressed_size
                row["compressed_bytes"] = int(tcs) if tcs is not None else None
            except Exception:
                row["compressed_bytes"] = None
            try:
                tus = col.total_uncompressed_size
                row["uncompressed_bytes"] = int(tus) if tus is not None else None
            except Exception:
                row["uncompressed_bytes"] = None
            # Writers may truncate long string stats (polars caps them at 64
            # bytes: prefix min, byte-incremented max). Truncated values are
            # still valid BOUNDS (min <= all values <= max) — all any pruning
            # consumer relies on — but not necessarily exact. pyarrow's
            # Statistics does not expose the exactness bit, so these flags are
            # optimistic placeholders; nothing may treat them as authoritative.
            row["min_is_exact"] = True
            row["max_is_exact"] = True
            row["stats_available"] = False

            try:
                routed = (
                    _route_stats(stat)
                    if stat is not None and stat.has_min_max
                    else (None, None, None)
                )
            except Exception:
                routed = (None, None, None)
            category, mn, mx = routed
            if category is not None:
                if category == "bigint":
                    row["min_bigint"], row["max_bigint"] = mn, mx
                    row["stats_available"] = True
                elif category == "double":
                    row["min_double"], row["max_double"] = mn, mx
                    row["stats_available"] = True
                elif category == "timestamp":
                    row["min_timestamp"], row["max_timestamp"] = mn, mx
                    row["stats_available"] = True
                elif category == "string":
                    row["min_string"], row["max_string"] = mn, mx
                    row["stats_available"] = True
                # else: unsupported type → stats_available stays False
            rows.append(row)
    return rows


def stats_seal_for_metadata(
        file_path: str,
        md,
        *,
        rows: Optional[List[dict]] = None,
        footer_sha256: Optional[str] = None,
) -> ResourceStatsSeal:
    """Build the exact footer + canonical stats-row seal for a new resource."""
    exact_footer_sha256 = footer_sha256 or parquet_footer_sha256(md)
    exact_rows = (
        _stats_rows_for_metadata(
            file_path,
            md,
            footer_sha256=exact_footer_sha256,
        )
        if rows is None else rows
    )
    if not exact_rows:
        # A system-column-only file has no STATS_SCHEMA rows.  Seal that exact
        # empty stream anyway, although absence pruning will remain unavailable
        # because there are no user-column ranges to prove anything.
        return ResourceStatsSeal(
            footer_sha256=exact_footer_sha256,
            stats_rows=0,
            stats_digest=_new_stats_resource_hasher(file_path).hexdigest(),
        )
    # These rows were generated from one freshly parsed footer.  Hash their
    # canonical stream directly instead of constructing a second DataFrame,
    # sorting it, iterating it, and serialising the same footer again.  The
    # ordering and row encoder are exactly those used by stats_resource_seals.
    try:
        ordered = sorted(
            exact_rows,
            key=lambda row: (row["row_group_id"], row["column_name"]),
        )
        digest = _new_stats_resource_hasher(file_path)
        for row in ordered:
            if (
                row.get("file_path") != file_path
                or row.get("footer_sha256") != exact_footer_sha256
            ):
                raise ValueError("stats rows do not match their footer resource")
            values = tuple(row.get(name) for name in STATS_SCHEMA)
            digest.update(_canonical_stats_row_bytes(values))
        return ResourceStatsSeal(
            footer_sha256=exact_footer_sha256,
            stats_rows=len(ordered),
            stats_digest=digest.hexdigest(),
        )
    except Exception as exc:
        raise ValueError(
            f"could not seal statistics for resource {file_path!r}"
        ) from exc


def _empty_stats_df() -> polars.DataFrame:
    return polars.DataFrame(schema=STATS_SCHEMA)


def _conform_stats_schema(df: polars.DataFrame) -> polars.DataFrame:
    """Safely project a legacy/corrupt frame onto ``STATS_SCHEMA``.

    Absent columns are typed NULL, wrong-typed columns are conservatively cast
    (bad values become NULL), and rows lacking their file/column/row-group
    identity are discarded.  Consumers treat all resulting NULL stats as
    unknown, so carrying a damaged artifact forward can only reduce pruning.
    """
    expressions = []
    for name, dtype in STATS_SCHEMA.items():
        if name not in df.columns:
            expressions.append(polars.lit(None).cast(dtype).alias(name))
            continue
        col = polars.col(name)
        if df.schema[name] != dtype:
            # Legacy/corrupt artifacts may carry an existing column with the
            # wrong dtype.  Coerce parseable values and turn anything malformed
            # into NULL; every pruning consumer treats NULL stats as unknown.
            # Text-to-boolean is unsupported by Polars, so only accept the
            # unambiguous legacy 0/1 integer representation for booleans.
            source_dtype = df.schema[name]
            if dtype == polars.Boolean and source_dtype.is_integer():
                col = (
                    polars.when(col == 1).then(True)
                    .when(col == 0).then(False)
                    .otherwise(None)
                )
            elif dtype == polars.Boolean:
                col = polars.lit(None).cast(dtype)
            elif dtype == polars.Int64 and not source_dtype.is_integer():
                col = polars.lit(None).cast(dtype)
            elif dtype == polars.Float64 and not source_dtype.is_float():
                col = polars.lit(None).cast(dtype)
            elif dtype == polars.Utf8 and source_dtype != polars.Utf8:
                col = polars.lit(None).cast(dtype)
            elif isinstance(dtype, polars.Datetime):
                safe_temporal_source = (
                    source_dtype == polars.Date
                    or (
                        isinstance(source_dtype, polars.Datetime)
                        and source_dtype.time_zone is None
                        and source_dtype.time_unit in ("ms", "us")
                    )
                )
                if not safe_temporal_source:
                    # Unitless integers/strings do not encode a trustworthy
                    # epoch unit; nanoseconds are lossy in STATS_SCHEMA's us
                    # lane; zoned legacy values lack a normalisation contract.
                    col = polars.lit(None).cast(dtype)
            else:
                col = col.cast(dtype, strict=False)
        expressions.append(col.alias(name))
    conformed = df.select(expressions)
    # Rows without a usable identity cannot match a data file/column and only
    # add work.  Drop them from the successor stats artifact; actual data files
    # remain referenced by the snapshot and therefore scan conservatively.
    return conformed.filter(
        polars.col("file_path").is_not_null()
        & polars.col("column_name").is_not_null()
        & polars.col("row_group_id").is_not_null()
    )


def extract_stats_rows(
        file_paths: List[str],
        profiler: Optional[Profiler] = None,
        footer_md_cache: Optional[Dict] = None,
) -> polars.DataFrame:
    """Read the footers of *file_paths* and return their stats rows.

    One row per (file × row_group × column), excluding the internal
    ``__rowid__`` / ``__timestamp__`` columns.  Files whose footer cannot be
    read (race / corruption) are skipped.  Returns a frame with ``STATS_SCHEMA``
    (possibly empty).

    *footer_md_cache* (optional) maps a file path to a parquet ``FileMetaData``
    already parsed in memory at write time (from the exact bytes that were
    uploaded).  When a path is present its footer is reused directly, skipping a
    full-file re-download; otherwise the footer is read back from storage.
    """
    p = profiler or get_null_profiler()
    cache = footer_md_cache or {}
    all_rows: List[dict] = []
    for path in file_paths:
        if not path:
            continue
        cached_entry = cache.get(path)
        cached_rows = (
            cached_entry.rows
            if isinstance(cached_entry, _FooterStatsCacheEntry)
            else None
        )
        md = (
            cached_entry.metadata
            if isinstance(cached_entry, _FooterStatsCacheEntry)
            else cached_entry
        )
        if md is None:
            md = _read_footer_metadata(path, profiler=p)
        else:
            p.add("stats_footer_cache_hit", 1)
        if md is None:
            continue
        all_rows.extend(
            cached_rows
            if cached_rows is not None
            else _stats_rows_for_metadata(path, md)
        )
    if not all_rows:
        return _empty_stats_df()
    p.add("stats_rows_extracted", len(all_rows))
    return polars.DataFrame(all_rows, schema=STATS_SCHEMA)


def build_stats_file(
        stats_dir: str,
        prev_stats_path: Optional[str],
        new_rows: Optional[polars.DataFrame],
        removed_files: Optional[Set[str]],
        compression_level: int,
        profiler: Optional[Profiler] = None,
        prev_cache_identity: Optional[str] = None,
        validation_out: Optional[Dict[str, _StatsFrameValidation]] = None,
) -> Tuple[Optional[str], Optional[polars.DataFrame]]:
    """Carry forward the previous stats parquet and apply this write's delta.

    The new stats parquet = (previous rows, MINUS any row whose ``file_path`` is
    in *removed_files*) + *new_rows*.  Mirrors :func:`build_tombstone_file`: each
    change writes a NEW immutable, versioned file — an existing artifact is never
    mutated.

    Returns ``(stats_path, combined_df)``:
      - no new rows AND nothing removed → ``(prev_stats_path, None)`` — pure
        carry-forward; the new snapshot reuses the previous file, no rewrite.
      - otherwise → ``(new_path, combined_df)`` where ``combined_df`` is the
        full stats artifact (so the caller can record ``stats_rows`` without
        re-reading the file).
    """
    p = profiler or get_null_profiler()
    removed = set(removed_files or set())
    new_df = new_rows if new_rows is not None else _empty_stats_df()
    has_new = new_df.height > 0

    if not has_new and not removed:
        return prev_stats_path, None

    # Serve the previous stats from the in-process cache: the prune phase of
    # THIS same write already loaded this exact (latest) version via
    # load_stats(allow_cache=True), so on the warm path this is a memory hit
    # with no storage round-trip.  prev_stats_path is the table's CURRENT latest
    # version here, so allow_cache=True is correct; a genuine miss falls through
    # to a fresh read (identical to the former _read_parquet_safe behaviour).
    prev_df = (
        load_stats(
            prev_stats_path,
            allow_cache=True,
            profiler=p,
            cache_identity=prev_cache_identity,
        )
        if prev_stats_path else None
    )
    if prev_df is not None and prev_df.height > 0 and STATS_FILE_PATH_COL in prev_df.columns:
        # Conform (not a bare select): a stats file written before a schema
        # addition lacks the new column, so add it as NULL rather than raise.
        kept_prev = _conform_stats_schema(prev_df)
        if removed:
            kept_prev = kept_prev.filter(
                ~polars.col(STATS_FILE_PATH_COL).is_in(list(removed))
            )
        if has_new:
            combined = polars.concat([kept_prev, new_df], how="vertical_relaxed")
        else:
            combined = kept_prev
    else:
        combined = new_df

    if combined.height == 0:
        # Clearing all live resources/stats needs no empty immutable object.
        # A null pointer is exact, cheaper, and cannot suffer a remote upload
        # failure after the data rewrite has already completed.
        if validation_out is not None:
            validation_out["validation"] = _StatsFrameValidation({}, {})
        return None, combined

    # The previous immutable stats frame was already validated before it was
    # allowed to prune this write (and its validation is cached beside that
    # exact frame).  Preserve those per-resource proofs, remove sunset paths,
    # and validate only the newly extracted footer rows.  Without this handoff
    # every successful write seeds the next cache entry without metadata, so
    # the next mutation sorts and hashes the complete, ever-growing stats
    # history again while holding the table lock.
    #
    # This is deliberately conservative: any malformed validation, duplicate
    # path across the retained/new partitions, or unavailable proof falls back
    # to the established full-frame validator.  The optimization can therefore
    # only reduce work; it cannot make an untrusted stats row eligible for
    # pruning.
    combined_validation: Optional[_StatsFrameValidation] = None
    try:
        prev_validation = (
            _stats_validation_for_frame(
                prev_df,
                stats_path=prev_cache_identity or prev_stats_path,
            )
            if prev_df is not None and prev_df.height > 0
            else _StatsFrameValidation({}, {})
        )
        new_validation = (
            _validate_stats_frame_once(new_df)
            if has_new
            else _StatsFrameValidation({}, {})
        )
        if (
            prev_validation.resource_seals is not None
            and new_validation.resource_seals is not None
        ):
            retained_seals = {
                path: seal
                for path, seal in prev_validation.resource_seals.items()
                if path not in removed
            }
            retained_rows = {
                path: rows
                for path, rows in prev_validation.complete_resource_rows.items()
                if path not in removed
            }
            new_paths = set(new_validation.resource_seals)
            if not new_paths.intersection(retained_seals):
                combined_validation = _StatsFrameValidation(
                    {
                        **retained_seals,
                        **new_validation.resource_seals,
                    },
                    {
                        **retained_rows,
                        **new_validation.complete_resource_rows,
                    },
                )
    except Exception:
        combined_validation = None
    if combined_validation is None:
        combined_validation = _validate_stats_frame_once(combined)

    new_path = _partitioned_new_path(stats_dir, "stats")
    _write_df_parquet(combined, new_path, compression_level, profiler=p)
    p.add("stats_rows_total", int(combined.height))
    if validation_out is not None:
        validation_out["validation"] = combined_validation
    return new_path, combined


# ===========================================================================
# Consumer 5a: stats-driven file pruning for overwrite / delete
# ---------------------------------------------------------------------------
# Given the incoming dataframe's per-key-column range ("probe") and the stored
# external stats artifact, drop candidate files that *provably* contain none of
# the incoming keys.  The contract is one-directional: pruning may only remove
# files with zero matching keys.  Every uncertainty (missing file/row-group/
# column stat, unsupported type, lane mismatch, NULL keys) resolves to RETAIN,
# never to drop — so the tombstone output is bit-identical with or without
# pruning.  Pruning is a pure performance optimisation, never a correctness one.
# ===========================================================================

# polars dtypes that route into the bigint lane (signed ints + the unsigned
# widths that fit losslessly in Int64).  UInt64 is deliberately excluded: its
# range can exceed Int64, so we never prune on it (→ retain).
_PROBE_BIGINT_DTYPES = {
    polars.Int8, polars.Int16, polars.Int32, polars.Int64,
    polars.UInt8, polars.UInt16, polars.UInt32,
}
_PROBE_FLOAT_DTYPES = {polars.Float32, polars.Float64}


def _intervals_overlap(a_lo, a_hi, b_lo, b_hi) -> bool:
    """Closed-interval overlap test: ``[a_lo,a_hi]`` ∩ ``[b_lo,b_hi]`` ≠ ∅.

    The robust, type-agnostic primitive behind all pruning: two ranges overlap
    iff ``a_lo <= b_hi and b_lo <= a_hi``.  Both endpoints are inclusive
    (footer min/max are inclusive bounds).  Only ever called with non-None,
    same-lane values.
    """
    return a_lo <= b_hi and b_lo <= a_hi


def _probe_lane_for_dtype(dtype) -> Optional[str]:
    """Map an incoming polars key-column dtype to a stored stats lane.

    Returns ``bigint`` / ``double`` / ``timestamp`` / ``string`` to match
    :func:`_route_stats`, or ``None`` for any type we never prune on (decimal,
    UInt64, binary, …).  ``None`` ⇒ that column contributes no constraint, so
    no file can be excluded by it.
    """
    if dtype == polars.Boolean:
        return "bigint"
    if dtype in _PROBE_BIGINT_DTYPES:
        return "bigint"
    if dtype in _PROBE_FLOAT_DTYPES:
        return "double"
    if dtype == polars.Date or isinstance(dtype, polars.Datetime):
        # Write pruning must preserve Polars' strict key-type semantics. Stored
        # temporal footer values share one normalised timestamp slot and cannot
        # prove Date vs naïve/tz-aware Datetime compatibility, so retain files.
        return None
    if dtype == polars.Utf8:
        return "string"
    return None


def _probe_type_signature(dtype) -> Optional[str]:
    """Return a footer-verifiable exact mutation-key dtype marker."""
    if dtype == polars.Boolean:
        return "boolean"
    if dtype == polars.Int32:
        return "int32"
    if dtype == polars.Int64:
        return "int64"
    if dtype == polars.Float32:
        return "float32"
    if dtype == polars.Float64:
        return "float64"
    if dtype == polars.Utf8:
        return "string"
    if dtype == polars.Date:
        return "date"
    if isinstance(dtype, polars.Datetime) and dtype.time_zone is None:
        if dtype.time_unit in ("ms", "us"):
            return f"timestamp_ntz_{dtype.time_unit}"
    # Integer annotations narrower/unsigned than Int32, nanosecond timestamps,
    # and timezone IDs cannot be reconstructed exactly from the stats schema.
    return None


def _normalise_probe_bounds(lane: str, lo, hi):
    """Coerce a column's min/max into the same normalised form the stored lane
    uses, so probe and stored values are directly comparable.  Returns
    ``(lo, hi)`` or ``None`` if the values can't be normalised."""
    if lane == "bigint":
        return int(lo), int(hi)
    if lane == "double":
        return float(lo), float(hi)
    if lane == "string":
        return str(lo), str(hi)
    if lane == "timestamp":
        a, b = _to_us_datetime(lo), _to_us_datetime(hi)
        if a is None or b is None:
            return None
        return a, b
    return None


def probe_ranges_from_df(
        df: polars.DataFrame,
        key_cols: List[str],
) -> Dict[str, Optional[Tuple[str, object, object]]]:
    """Derive the incoming dataframe's per-key-column range ("probe").

    For each column in *key_cols* returns ``(lane, lo, hi)`` — the closed range
    of that column's values, normalised to the stored lane — or ``None`` when
    the column must not be used to prune:

      - any NULL present (footer min/max exclude NULLs, but overwrite equality
        uses ``nulls_equal=True``: a NULL key could match a file whose range
        doesn't cover it → must retain);
      - any NaN present in a floating column (Parquet footer min/max commonly
        omit NaNs while DuckDB/Polars equality can match NaN to NaN);
      - unsupported dtype (decimal / UInt64 / binary → :func:`_probe_lane_for_dtype`
        returns None);
      - empty column (min/max are None).

    A column mapped to ``None`` simply drops out of the constraint set, so it
    can never exclude a file.  This is the df-probe: because the file we just
    wrote carries identical footer min/max, comparing the in-memory df range
    against the stored stats is mathematically equivalent to comparing footers,
    without opening a single file.
    """
    out: Dict[str, Optional[Tuple[str, object, object]]] = {}
    for name in key_cols:
        if name not in df.columns:
            out[name] = None
            continue
        col = df[name]
        exact_type = _probe_type_signature(col.dtype)
        lane = (
            "timestamp"
            if exact_type in {"date", "timestamp_ntz_ms", "timestamp_ntz_us"}
            else _probe_lane_for_dtype(col.dtype)
        )
        if lane is None or exact_type is None:
            out[name] = None
            continue
        if col.null_count() > 0:
            out[name] = None
            continue
        if lane == "double" and bool(col.is_nan().any()):
            out[name] = None
            continue
        lo, hi = col.min(), col.max()
        if lo is None or hi is None:
            out[name] = None
            continue
        bounds = _normalise_probe_bounds(lane, lo, hi)
        if bounds is None:
            out[name] = None
            continue
        out[name] = (lane, bounds[0], bounds[1], exact_type)
    return out


def _stored_write_type_compatible(row: dict, expected: Optional[str]) -> bool:
    """Whether footer metadata proves exact Polars mutation-key compatibility."""
    if expected is None:
        return True  # compatibility for legacy/manual three-field probes
    physical = str(row.get("physical_type") or "").upper()
    logical = str(row.get("logical_type") or "").upper()
    exact = {
        "boolean": ("BOOLEAN", ""),
        "int32": ("INT32", ""),
        "int64": ("INT64", ""),
        "float32": ("FLOAT", ""),
        "float64": ("DOUBLE", ""),
        "string": ("BYTE_ARRAY", "STRING"),
        "date": ("INT32", "DATE"),
        "timestamp_ntz_ms": ("INT64", "TIMESTAMP_NTZ_MILLIS"),
        "timestamp_ntz_us": ("INT64", "TIMESTAMP_NTZ_MICROS"),
    }.get(expected)
    return exact is not None and (physical, logical) == exact


def _stored_lane(row: dict) -> Optional[Tuple[str, object, object]]:
    """Read a stored stats row's typed range as ``(lane, min, max)``.

    Returns ``None`` when the row carries no usable range (``stats_available``
    False, or no lane populated) — meaning the file/row-group can't be excluded
    on that column.
    """
    if row.get("stats_available") is not True:
        return None
    lane_specs = (
        (
            "bigint", "min_bigint", "max_bigint",
            lambda value: isinstance(value, int) and not isinstance(value, bool),
        ),
        (
            "double", "min_double", "max_double",
            lambda value: isinstance(value, float),
        ),
        (
            "timestamp", "min_timestamp", "max_timestamp",
            lambda value: isinstance(value, datetime),
        ),
        (
            "string", "min_string", "max_string",
            lambda value: isinstance(value, str),
        ),
    )
    populated = []
    for lane, min_name, max_name, valid_type in lane_specs:
        lo, hi = row.get(min_name), row.get(max_name)
        if lo is None and hi is None:
            continue
        # A half-populated lane or a value in the wrong physical Python type is
        # a corrupt/legacy row, not a range that may exclude data.
        if lo is None or hi is None or not valid_type(lo) or not valid_type(hi):
            return None
        populated.append((lane, lo, hi))
    # Exactly one typed lane must describe the column.  Multiple populated
    # lanes can arise after a damaged schema migration and are ambiguous.
    if len(populated) != 1:
        return None
    stored = populated[0]

    # Footer ranges are usable only when they form a valid closed interval.
    # Corrupt/legacy artifacts can contain reversed, NaN/NaT, or incomparable
    # endpoints.  Letting such a range reach an overlap test can produce a false
    # "disjoint" result and drop a contributing file.  Every malformed case is
    # therefore unknown and retains the file.
    _lane, lo, hi = stored
    try:
        if lo != lo or hi != hi:  # NaN / NaT / other non-reflexive sentinel
            return None
        if not bool(lo <= hi):
            return None
    except Exception:
        return None
    return stored


def _stored_select_lane_values(
        stats_available: object,
        physical_type: object,
        logical_type: object,
        min_bigint: object,
        max_bigint: object,
        min_timestamp: object,
        max_timestamp: object,
        other_lane_populated: object = False,
) -> Optional[Tuple[str, object, object]]:
    """Tuple-oriented SELECT lane decoder used by stats hot loops.

    ``DataFrame.iter_rows(named=True)`` allocates a dictionary for every stats
    row.  Read pruning commonly examines tens of thousands of rows, so its
    callers project these values in the exact argument order above and iterate
    ordinary tuples instead.  Keep every validation here equivalent to the
    mapping-based :func:`_stored_select_lane`: malformed, ambiguous, lossy, or
    executor-incompatible ranges remain unknown and therefore retain files.
    """
    if other_lane_populated is True or stats_available is not True:
        return None

    bigint_populated = min_bigint is not None or max_bigint is not None
    timestamp_populated = (
        min_timestamp is not None or max_timestamp is not None
    )
    # Zero populated lanes means absent stats; two means corrupt/ambiguous
    # stats.  Avoid building a short-lived list on this per-row hot path.
    if bigint_populated == timestamp_populated:
        return None
    if bigint_populated:
        if (
            min_bigint is None
            or max_bigint is None
            or not isinstance(min_bigint, int)
            or isinstance(min_bigint, bool)
            or not isinstance(max_bigint, int)
            or isinstance(max_bigint, bool)
        ):
            return None
        lane, lo, hi = "bigint", min_bigint, max_bigint
    else:
        if (
            min_timestamp is None
            or max_timestamp is None
            or not isinstance(min_timestamp, datetime)
            or not isinstance(max_timestamp, datetime)
        ):
            return None
        lane, lo, hi = "timestamp", min_timestamp, max_timestamp
    try:
        if lo != lo or hi != hi:
            return None
        if not bool(lo <= hi):
            return None
    except Exception:
        return None

    physical = str(physical_type or "").upper()
    logical = str(logical_type or "").upper()
    if lane == "bigint":
        if physical not in {"BOOLEAN", "INT32", "INT64"}:
            return None
        if logical not in {"", "INT"}:
            return None
        return lane, lo, hi
    if logical == "DATE" and physical == "INT32":
        return "date", lo, hi
    if (
        logical in ("TIMESTAMP_NTZ_MILLIS", "TIMESTAMP_NTZ_MICROS")
        and physical == "INT64"
    ):
        return "timestamp", lo, hi
    if (
        logical in ("TIMESTAMP_TZ_MILLIS", "TIMESTAMP_TZ_MICROS")
        and physical == "INT64"
    ):
        return "timestamptz", lo, hi
    return None


def _stored_select_lane(row: dict) -> Optional[Tuple[str, object, object]]:
    """Return a footer range only when SELECT semantics can trust its order.

    This is intentionally stricter than :func:`_stored_lane`, which is also
    used by the write-path equality probe under Polars' type-stable semantics.
    A read may execute in DuckDB or Spark and may involve implicit casts and
    connection collation, so a footer lane is eligible only when the range
    ordering is identical to the executor's equality/filter semantics:

    * signed bigint ranges are exact;
    * DATE, naïve TIMESTAMP and TIMESTAMPTZ are distinct lanes, and only new
      unambiguous millisecond/microsecond markers are accepted (the stats
      artifact stores microseconds, so nanosecond bounds would be lossy);
    * doubles are unavailable because Parquet footer min/max can omit NaNs and
      mixed bigint/double equality can round beyond 2**53;
    * strings are unavailable because DuckDB connections use the ``nocase``
      collation while Parquet footer bounds use binary UTF-8 order.

    Returning ``None`` never drops data: callers treat it as unknown and retain
    the row group/file.
    """
    # Direct callers still pass the full stats mapping.  Fold the rejected
    # typed slots into the same poison bit used by narrow DataFrame projections
    # so a corrupt row with multiple lanes cannot become trusted.
    other_lane_populated = (
        row.get(_SELECT_OTHER_LANE_COL) is True
        or any(
            row.get(name) is not None
            for name in (
                "min_double", "max_double", "min_string", "max_string",
            )
        )
    )
    return _stored_select_lane_values(
        row.get("stats_available"),
        row.get("physical_type"),
        row.get("logical_type"),
        row.get("min_bigint"),
        row.get("max_bigint"),
        row.get("min_timestamp"),
        row.get("max_timestamp"),
        other_lane_populated,
    )


def stats_for_complete_files(
        stats_df: Optional[polars.DataFrame],
        resource_rows: Dict[str, Optional[int]],
        resource_seals: Optional[Dict[str, Optional[ResourceStatsSeal]]] = None,
        *,
        stats_path: Optional[str] = None,
) -> Optional[polars.DataFrame]:
    """Retain only snapshot-bound, complete per-file statistics manifests.

    A table-level artifact row count cannot detect a missing row-group slot that
    was replaced by a duplicate row elsewhere, nor a same-height artifact copied
    from another table. Both SELECT and mutation pruning may use a file's stats
    only when row-group ids, physical row counts and exact column slots agree AND
    every canonical row matches the resource's snapshot-pinned count, footer
    SHA-256 and cryptographic digest. Legacy/unsealed resources are deliberately
    absent from the result and therefore scan conservatively.
    """
    if stats_df is None or not isinstance(stats_df, polars.DataFrame):
        return None
    if stats_df.height == 0:
        return stats_df
    required = [
        "file_path", "footer_sha256", "row_group_id", "column_name",
        "row_group_rows",
    ]
    if not set(required).issubset(stats_df.columns):
        return None
    seals = resource_seals or {}
    manifest = {
        file_path: (rows, seals.get(file_path))
        for file_path, rows in resource_rows.items()
        if isinstance(file_path, str)
        and isinstance(rows, int)
        and not isinstance(rows, bool)
        and rows >= 0
        and isinstance(seals.get(file_path), ResourceStatsSeal)
    }
    if not manifest:
        return stats_df.head(0)
    try:
        validation = _stats_validation_for_frame(
            stats_df, stats_path=stats_path,
        )
        if validation.resource_seals is None:
            return None
        trusted = [
            file_path
            for file_path, (expected_rows, expected_seal) in manifest.items()
            if validation.complete_resource_rows.get(file_path) == expected_rows
            and validation.resource_seals.get(file_path) == expected_seal
        ]
        if not trusted:
            return stats_df.head(0)
        manifested = stats_df.filter(polars.col("file_path").is_in(list(manifest)))
        if (
            manifested.height == stats_df.height
            and len(trusted) == len(manifest)
        ):
            return stats_df
        return manifested.filter(polars.col("file_path").is_in(trusted))
    except Exception:
        return None


def integer_domains_from_complete_stats(
    stats_df: Optional[polars.DataFrame],
    file_keys: Iterable[str],
    selections: Optional[Dict[str, RowGroupSelection]] = None,
    column_names: Optional[Iterable[str]] = None,
) -> Dict[str, IntegerDomainBound]:
    """Derive complete integer domains for the selected row groups.

    ``stats_df`` must already be the output of
    :func:`stats_for_complete_files` for the pinned snapshot.  This second
    boundary intentionally revalidates every selected (file, row-group,
    column) slot it uses: a missing/duplicate slot, bad NULL count, ambiguous
    lane, inexact extremum, or absent selected group omits that column's proof.
    ``column_names`` narrows this metadata work to planner-relevant integer
    candidates; it never relaxes the all-groups/all-files completeness check.
    The result is an optional planning optimisation only, so every exception
    fails closed to an empty mapping.

    Integer footer extrema are exact for Parquet INT32/INT64 columns.  A range
    can contain holes, therefore the consumer may use ``max - min + 1`` only as
    an upper bound on distinct non-NULL keys.  SQL NULL contributes at most one
    additional GROUP BY key.
    """
    keys = list(file_keys)
    if not keys:
        return {}
    required = {
        "file_path", "row_group_id", "column_name", "physical_type",
        "row_group_rows", "null_count", "stats_available",
        "min_bigint", "max_bigint", "min_double", "max_double",
        "min_timestamp", "max_timestamp", "min_string", "max_string",
        "min_is_exact", "max_is_exact",
    }
    if (
        stats_df is None
        or not isinstance(stats_df, polars.DataFrame)
        or stats_df.height == 0
        or not required.issubset(stats_df.columns)
        or len(set(keys)) != len(keys)
        or any(not isinstance(key, str) or not key for key in keys)
    ):
        return {}
    try:
        requested_names = (
            {
                str(name).casefold()
                for name in column_names
                if isinstance(name, str) and name
            }
            if column_names is not None
            else None
        )
        if requested_names == set():
            return {}
        key_set = set(keys)
        scoped = stats_df.filter(polars.col("file_path").is_in(keys))
        present_files = set(
            scoped.get_column("file_path").drop_nulls().unique().to_list()
        )
        if present_files != key_set:
            return {}

        available_pairs: Dict[str, Set[int]] = {key: set() for key in keys}
        for file_path, group_id in scoped.select(
            ["file_path", "row_group_id"]
        ).unique().iter_rows():
            if (
                file_path not in key_set
                or not isinstance(group_id, int)
                or isinstance(group_id, bool)
                or group_id < 0
            ):
                return {}
            available_pairs[file_path].add(group_id)

        selections = selections or {}
        expected_pairs: Set[Tuple[str, int]] = set()
        for key in keys:
            selection = selections.get(key)
            if selection is None:
                group_ids = available_pairs[key]
            elif isinstance(selection, RowGroupSelection):
                group_ids = set(selection.selected_ids)
                if not group_ids.issubset(available_pairs[key]):
                    return {}
            else:
                return {}
            expected_pairs.update((key, group_id) for group_id in group_ids)
        if not expected_pairs:
            return {}

        minima: Dict[str, int] = {}
        maxima: Dict[str, int] = {}
        has_null: Dict[str, bool] = defaultdict(bool)
        seen: Dict[str, Set[Tuple[str, int]]] = defaultdict(set)
        spellings: Dict[str, Set[str]] = defaultdict(set)
        invalid: Set[str] = set()
        columns = [
            "file_path", "row_group_id", "column_name", "physical_type",
            "row_group_rows", "null_count", "stats_available",
            "min_bigint", "max_bigint", "min_double", "max_double",
            "min_timestamp", "max_timestamp", "min_string", "max_string",
            "min_is_exact", "max_is_exact",
        ]
        candidate_stats = scoped
        if requested_names is not None:
            candidate_stats = candidate_stats.filter(
                polars.col("column_name")
                .str.to_lowercase()
                .is_in(sorted(requested_names))
            )
        for row in candidate_stats.select(columns).iter_rows(named=True):
            pair = (row["file_path"], row["row_group_id"])
            if pair not in expected_pairs:
                continue
            raw_name = row["column_name"]
            if not isinstance(raw_name, str) or not raw_name:
                continue
            name = raw_name.casefold()
            spellings[name].add(raw_name)
            if pair in seen[name]:
                invalid.add(name)
                continue
            seen[name].add(pair)

            rows = row["row_group_rows"]
            nulls = row["null_count"]
            if (
                not isinstance(rows, int)
                or isinstance(rows, bool)
                or rows < 0
                or not isinstance(nulls, int)
                or isinstance(nulls, bool)
                or nulls < 0
                or nulls > rows
                or str(row["physical_type"] or "").upper()
                not in {"INT32", "INT64"}
            ):
                invalid.add(name)
                continue
            if nulls:
                has_null[name] = True

            minimum = row["min_bigint"]
            maximum = row["max_bigint"]
            other_lane = any(
                row[lane] is not None
                for lane in (
                    "min_double", "max_double", "min_timestamp",
                    "max_timestamp", "min_string", "max_string",
                )
            )
            non_null_rows = rows - nulls
            if non_null_rows == 0:
                if (
                    bool(row["stats_available"])
                    or minimum is not None
                    or maximum is not None
                    or other_lane
                ):
                    invalid.add(name)
                continue
            if (
                row["stats_available"] is not True
                or row["min_is_exact"] is not True
                or row["max_is_exact"] is not True
                or other_lane
                or not isinstance(minimum, int)
                or isinstance(minimum, bool)
                or not isinstance(maximum, int)
                or isinstance(maximum, bool)
                or minimum > maximum
            ):
                invalid.add(name)
                continue
            minima[name] = min(minima.get(name, minimum), minimum)
            maxima[name] = max(maxima.get(name, maximum), maximum)

        result: Dict[str, IntegerDomainBound] = {}
        for name, pairs in seen.items():
            if (
                name in invalid
                or pairs != expected_pairs
                or len(spellings[name]) != 1
            ):
                continue
            minimum = minima.get(name)
            maximum = maxima.get(name)
            if (minimum is None) != (maximum is None):
                continue
            result[name] = IntegerDomainBound(
                minimum=minimum,
                maximum=maximum,
                has_null=bool(has_null[name]),
            )
        return result
    except Exception:
        return {}


def prune_overlapping_files_by_stats(
        overlapping_files: Set[Tuple[str, bool, int]],
        stored_stats_df: Optional[polars.DataFrame],
        probe_ranges: Dict[str, Optional[Tuple[str, object, object]]],
        profiler: Optional[Profiler] = None,
) -> Set[Tuple[str, bool, int]]:
    """Narrow the overwrite/delete candidate set using the stored stats.

    A file is dropped **only** when, for every row group, at least one probed
    key column's range provably does NOT overlap the stored range (so that row
    group cannot hold any incoming key) — i.e. no row group can match.  The
    decision is AND-within-row-group (every constrained column must overlap),
    OR-across-row-groups (one matching row group keeps the file).

    Every uncertainty retains the file:
      - no usable probe constraints → return the input unchanged;
      - no stored stats → return the input unchanged;
      - file absent from the stats → retained;
      - a (row-group, column) stat missing / ``stats_available`` False / lane
        mismatch → that column can't exclude that row group → treated as a
        potential match.

    ``has_overlap=False`` entries (pure-compaction candidates) are passed
    through untouched — pruning only applies to overwrite/delete candidates.
    """
    p = profiler or get_null_profiler()
    constraints = {c: v for c, v in (probe_ranges or {}).items() if v is not None}
    if not constraints:
        return overlapping_files
    if stored_stats_df is None or stored_stats_df.height == 0:
        return overlapping_files

    constrained_cols = list(constraints.keys())
    needed = stored_stats_df.filter(polars.col("column_name").is_in(constrained_cols))
    # index: file -> row group -> column -> (range, physical/logical row)
    index: Dict[str, Dict[int, Dict[str, Tuple[object, dict]]]] = {}
    for row in needed.iter_rows(named=True):
        fp = row["file_path"]
        rg = row["row_group_id"]
        col = row["column_name"]
        index.setdefault(fp, {}).setdefault(rg, {})[col] = (_stored_lane(row), row)

    kept: Set[Tuple[str, bool, int]] = set()
    pruned = 0
    for entry in overlapping_files:
        file_path, has_overlap, _file_size = entry
        if not has_overlap:
            kept.add(entry)
            continue
        rgs = index.get(file_path)
        if not rgs:
            kept.add(entry)  # no stats for this file → cannot prove absence
            continue
        file_can_match = False
        for _rg_id, cols in rgs.items():
            rg_matches = True
            for col, constraint in constraints.items():
                lane, lo, hi = constraint[:3]
                expected_type = constraint[3] if len(constraint) > 3 else None
                stored_entry = cols.get(col)
                if stored_entry is None:
                    continue  # missing / unavailable stat → can't exclude
                stored, stored_row = stored_entry
                if stored is None or not _stored_write_type_compatible(
                    stored_row, expected_type
                ):
                    continue
                s_lane, s_min, s_max = stored
                if s_lane != lane:
                    continue  # lane mismatch → can't compare → assume overlap
                if not _intervals_overlap(lo, hi, s_min, s_max):
                    rg_matches = False
                    break
            if rg_matches:
                file_can_match = True
                break
        if file_can_match:
            kept.add(entry)
        else:
            pruned += 1
    p.add("stats_pruned_files", pruned)
    return kept


# ===========================================================================
# Read-path pruning (consumer 5b) — prune files by SQL WHERE predicates
# ---------------------------------------------------------------------------
# The write path probes the *incoming dataframe's* range against the stored
# stats.  The read path instead derives an *allowed range* per column from the
# query's WHERE predicate (a :class:`PredInterval`) and drops any file whose
# every row group provably cannot satisfy it.  Same conservative contract:
# a file is dropped ONLY when no row group can match; every uncertainty (no
# stats, missing/​unavailable stat, lane it can't compare) retains the file.
# ===========================================================================


def _pred_overlaps_stored(pred: PredInterval, stored: Tuple[str, object, object]) -> bool:
    """True if a value in the stored row-group range ``[s_min, s_max]`` could
    satisfy the predicate interval *pred*.

    Returns ``True`` (assume overlap → retain) whenever the predicate lane and
    the stored lane can't be compared — never a false "no overlap", so pruning
    stays sound.
    """
    p_lane = pred.lane
    s_lane, s_min, s_max = stored
    if p_lane in ("numeric", "numeric_cast") and s_lane == "bigint":
        # Integer literals/ranges compare exactly.  A floating predicate bound
        # against BIGINT is deliberately incomparable: DuckDB may coerce the
        # bigint to double, where values past 2**53 collapse together.
        if any(
            isinstance(v, float)
            for v in (pred.lo, pred.hi)
            if v is not None
        ):
            return True
        smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi
    elif p_lane == "date" and s_lane == "date":
        smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi
    elif p_lane == "timestamp" and s_lane == "timestamp":
        smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi
    elif p_lane == "timestamptz" and s_lane == "timestamptz":
        smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi
    else:
        return True  # incomparable lanes → cannot exclude

    # Effective lower bound = the greater of [smin (inclusive), plo].
    if plo is None or smin > plo:
        low, low_incl = smin, True
    elif smin < plo:
        low, low_incl = plo, pred.lo_incl
    else:
        low, low_incl = smin, pred.lo_incl
    # Effective upper bound = the lesser of [smax (inclusive), phi].
    if phi is None or smax < phi:
        high, high_incl = smax, True
    elif smax > phi:
        high, high_incl = phi, pred.hi_incl
    else:
        high, high_incl = smax, pred.hi_incl

    if low < high:
        return True
    if low == high:
        return low_incl and high_incl
    return False


def _occurrence_excludes_file(
        occ: Dict[str, PredInterval],
        rgs: Dict[int, Dict[str, Optional[Tuple[str, object, object]]]],
) -> bool:
    """True if *every* row group of a file fails at least one of this
    occurrence's column predicates (so the file cannot contribute any row).

    AND-within-row-group (all constrained columns must be able to overlap),
    OR-across-row-groups (one possibly-matching row group keeps the file).
    """
    for _rg_id, cols in rgs.items():
        rg_matches = True
        for col, pred in occ.items():
            stored = cols.get(col)
            if stored is None:
                continue  # missing / unavailable stat → can't exclude
            if not _pred_overlaps_stored(pred, stored):
                rg_matches = False
                break
        if rg_matches:
            return False
    return True


def prune_files_by_predicates(
        file_keys: List[str],
        stored_stats_df: Optional[polars.DataFrame],
        occurrences: List[Dict[str, PredInterval]],
        profiler: Optional[Profiler] = None,
) -> List[str]:
    """Return the subset of *file_keys* that could satisfy the query predicates.

    *occurrences* is one constraint dict per place the physical table is scanned
    (alias / subquery scope).  Because the executor scans a table once and reuses
    it for every occurrence, the surviving set is the **union** of what each
    occurrence needs: a file is dropped only when **every** occurrence excludes
    it.  Conservative guards (all retain the full list):

      - no occurrences, or any occurrence carries no usable constraint;
      - no stored stats;
      - pruning that would empty the list (keeps the estimator's "≥1 file"
        invariant and lets the executor return the correct empty result).

    Files absent from the stats are always retained.  ``file_keys`` are matched
    against the stats ``file_path`` column, so callers must pass the raw storage
    keys (not resolved/presigned URLs).
    """
    p = profiler or get_null_profiler()
    if not occurrences or any(not occ for occ in occurrences):
        return file_keys
    if stored_stats_df is None or stored_stats_df.height == 0:
        return file_keys

    # SQL identifiers are case-insensitive in the supported engines.  Normalize
    # both sides before indexing, but fail open if one occurrence itself has a
    # collision (choosing either constraint could exclude the wrong column).
    normalized_occurrences: List[Dict[str, PredInterval]] = []
    for occurrence in occurrences:
        normalized: Dict[str, PredInterval] = {}
        for column, predicate in occurrence.items():
            if not isinstance(column, str):
                return file_keys
            column_lower = column.lower()
            if column_lower in normalized:
                return file_keys
            normalized[column_lower] = predicate
        normalized_occurrences.append(normalized)

    constrained_cols = sorted({
        column for occurrence in normalized_occurrences for column in occurrence
    })
    if not {"file_path", "row_group_id", "column_name"}.issubset(
        stored_stats_df.columns
    ):
        return file_keys
    # Only signed integers and explicitly typed temporal ranges are eligible
    # for SELECT pruning.  Avoid converting the rejected double/string lanes
    # (and unrelated stats metadata) into Python dictionaries.
    lane_columns = (
        "file_path", "row_group_id", "stats_available",
        "physical_type", "logical_type",
        "min_bigint", "max_bigint", "min_timestamp", "max_timestamp",
    )
    expressions = [
        polars.col("column_name").str.to_lowercase().alias("__column_lower")
    ] + [
        (
            polars.col(column)
            if column in stored_stats_df.columns
            else polars.lit(None).alias(column)
        )
        for column in lane_columns
    ]
    rejected_lane_columns = [
        column for column in (
            "min_double", "max_double", "min_string", "max_string",
        )
        if column in stored_stats_df.columns
    ]
    expressions.append(
        (
            polars.any_horizontal(
                [polars.col(column).is_not_null()
                 for column in rejected_lane_columns]
            )
            if rejected_lane_columns else polars.lit(False)
        ).alias(_SELECT_OTHER_LANE_COL)
    )
    needed = (
        stored_stats_df
        .filter(
            polars.col("column_name").str.to_lowercase().is_in(constrained_cols)
        )
        .select(expressions)
    )
    index: Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]] = {}
    for (
        col, fp, rg, stats_available, physical_type, logical_type,
        min_bigint, max_bigint, min_timestamp, max_timestamp,
        other_lane_populated,
    ) in needed.iter_rows():
        rg_cols = index.setdefault(fp, {}).setdefault(rg, {})
        if col in rg_cols:
            # Never let iteration order choose between physical footer columns
            # that collide after case folding (e.g. externally written ID/id).
            rg_cols[col] = None
        else:
            rg_cols[col] = _stored_select_lane_values(
                stats_available,
                physical_type,
                logical_type,
                min_bigint,
                max_bigint,
                min_timestamp,
                max_timestamp,
                other_lane_populated,
            )

    kept: List[str] = []
    pruned = 0
    for fk in file_keys:
        rgs = index.get(fk)
        if not rgs:
            kept.append(fk)  # no stats for this file → cannot prove absence
            continue
        if all(
            _occurrence_excludes_file(occurrence, rgs)
            for occurrence in normalized_occurrences
        ):
            pruned += 1
        else:
            kept.append(fk)

    # Never empty a table's file list — pruning is an optimisation, and the
    # estimator treats zero files as an error.  Retain all if we pruned all.
    if not kept:
        return file_keys
    if pruned == 0:
        return file_keys
    p.add("read_pruned_files", pruned)
    return kept


def select_row_groups_by_predicates(
        file_keys: List[str],
        stored_stats_df: Optional[polars.DataFrame],
        occurrences: List[Dict[str, PredInterval]],
) -> Dict[str, RowGroupSelection]:
    """Return conservative literal-WHERE row-group hints keyed by raw object key.

    An absent mapping entry means scan every row group. Missing/colliding slots
    and unavailable comparison lanes retain the affected group; they do not
    erase independent disjoint proof from another conjunct. Malformed group
    identities, incomplete file manifests, an unfiltered physical occurrence,
    or a selection containing every group all produce the absent/ALL form.
    Candidates are unioned across physical-table occurrences because the
    executor builds one shared scan for all aliases/subqueries.

    Empty selections cannot be represented.  When the WHERE ranges disqualify
    every row group of every file, the complete table plan is rolled back to
    ALL groups.  This mirrors ``prune_files_by_predicates(... allow_empty=False)``
    and guarantees that a hint can never turn the estimator's retained fallback
    files into an accidental empty scan.

    Callers must first pass the artifact through the estimator's complete-file
    manifest validation.  The checks here are defence in depth and fail open.
    Join-derived constraints are intentionally not accepted by this API.
    """
    if not file_keys or not occurrences:
        return {}
    try:
        if (
            any(not isinstance(file_key, str) for file_key in file_keys)
            or len(file_keys) != len(set(file_keys))
        ):
            return {}
    except Exception:
        return {}
    if (
        any(not occurrence for occurrence in occurrences)
        or stored_stats_df is None
        or not isinstance(stored_stats_df, polars.DataFrame)
        or stored_stats_df.height == 0
    ):
        return {}

    required = {
        "file_path", "row_group_id", "column_name", "stats_available",
        "physical_type", "logical_type", "min_bigint", "max_bigint",
        "min_timestamp", "max_timestamp", "footer_sha256",
    }
    if not required.issubset(stored_stats_df.columns):
        return {}

    normalized_occurrences: List[Dict[str, PredInterval]] = []
    try:
        for occurrence in occurrences:
            normalized: Dict[str, PredInterval] = {}
            for column, predicate in occurrence.items():
                if not isinstance(column, str) or not isinstance(
                    predicate, PredInterval
                ):
                    return {}
                lower = column.lower()
                if lower in normalized:
                    return {}
                normalized[lower] = predicate
            normalized_occurrences.append(normalized)
    except Exception:
        return {}

    constrained_cols = sorted({
        column
        for occurrence in normalized_occurrences
        for column in occurrence
    })
    if not constrained_cols:
        return {}

    rejected_lane_columns = [
        column for column in (
            "min_double", "max_double", "min_string", "max_string",
        )
        if column in stored_stats_df.columns
    ]
    try:
        scoped_stats = (
            stored_stats_df
            .filter(polars.col("file_path").is_in(file_keys))
        )
        all_groups_frame = (
            scoped_stats
            .select(["file_path", "row_group_id"])
            .unique()
        )
        groups_by_file: Dict[str, List[int]] = defaultdict(list)
        for file_path, group_id in all_groups_frame.iter_rows():
            groups_by_file[file_path].append(group_id)

        # Validate footer seals once per file in one columnar pass. Filtering
        # the complete stats artifact separately for every file is quadratic
        # at the 100+ resource scale this optimization is intended to serve.
        seals_by_file: Dict[str, str] = {}
        seal_frame = scoped_stats.group_by("file_path").agg([
            polars.len().alias("__seal_rows"),
            polars.col("footer_sha256").null_count().alias("__seal_nulls"),
            polars.col("footer_sha256").n_unique().alias("__seal_unique"),
            polars.col("footer_sha256").first().alias("__seal"),
        ])
        for file_path, rows, nulls, unique, seal in seal_frame.iter_rows():
            if rows > 0 and nulls == 0 and unique == 1:
                seals_by_file[file_path] = seal

        expressions = [
            polars.col("column_name").str.to_lowercase().alias("__column_lower"),
            polars.col("file_path"),
            polars.col("row_group_id"),
            polars.col("stats_available"),
            polars.col("physical_type"),
            polars.col("logical_type"),
            polars.col("min_bigint"),
            polars.col("max_bigint"),
            polars.col("min_timestamp"),
            polars.col("max_timestamp"),
            (
                polars.any_horizontal([
                    polars.col(column).is_not_null()
                    for column in rejected_lane_columns
                ])
                if rejected_lane_columns else polars.lit(False)
            ).alias(_SELECT_OTHER_LANE_COL),
        ]
        needed = (
            stored_stats_df
            .filter(
                polars.col("file_path").is_in(file_keys)
                & polars.col("column_name").str.to_lowercase().is_in(
                    constrained_cols
                )
            )
            .select(expressions)
        )
    except Exception:
        return {}

    # file -> group -> lower-column -> trusted lane, with None representing a
    # duplicate/colliding or unavailable footer slot.
    index: Dict[
        str,
        Dict[int, Dict[str, Optional[Tuple[str, object, object]]]],
    ] = {}
    try:
        for (
            column, file_path, group_id, stats_available, physical_type,
            logical_type, min_bigint, max_bigint, min_timestamp,
            max_timestamp, other_lane_populated,
        ) in needed.iter_rows():
            group_columns = index.setdefault(file_path, {}).setdefault(
                group_id, {}
            )
            if column in group_columns:
                group_columns[column] = None
            else:
                group_columns[column] = _stored_select_lane_values(
                    stats_available,
                    physical_type,
                    logical_type,
                    min_bigint,
                    max_bigint,
                    min_timestamp,
                    max_timestamp,
                    other_lane_populated,
                )
    except Exception:
        return {}

    selections: Dict[str, RowGroupSelection] = {}
    any_possible_group = False
    for file_key in file_keys:
        raw_group_ids = groups_by_file.get(file_key)
        if not raw_group_ids:
            # This object is absent from the validated artifact: ALL.
            any_possible_group = True
            continue
        if any(
            not isinstance(group_id, int)
            or isinstance(group_id, bool)
            or group_id < 0
            for group_id in raw_group_ids
        ):
            any_possible_group = True
            continue
        group_ids = sorted(set(raw_group_ids))
        expected_count = len(group_ids)
        if group_ids != list(range(expected_count)) or expected_count <= 0:
            any_possible_group = True
            continue

        # Every stats slot must be bound to the same footer. A partially legacy
        # or corrupt file cannot borrow the valid seal carried by its other rows.
        footer_sha256 = seals_by_file.get(file_key)
        if (
            not isinstance(footer_sha256, str)
            or len(footer_sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in footer_sha256)
        ):
            any_possible_group = True
            continue

        file_index = index.get(file_key, {})
        selected: Set[int] = set()
        for group_id in group_ids:
            columns = file_index.get(group_id, {})
            # A group is retained when ANY physical SQL occurrence can still
            # match. Within one occurrence (an AND conjunction), one trusted
            # disjoint predicate is enough to prove it cannot match; missing or
            # unsupported predicates merely stay "possible" for that group.
            # This preserves the superset contract while allowing an integer
            # predicate to prune groups even when a sibling string predicate
            # has no safe NOCASE stats lane.
            for occurrence in normalized_occurrences:
                occurrence_possible = True
                for column, predicate in occurrence.items():
                    stored = columns.get(column)
                    if stored is None:
                        continue  # unknown is possible, never disjoint proof
                    try:
                        overlaps = _pred_overlaps_stored(predicate, stored)
                    except Exception:
                        continue
                    if not overlaps:
                        occurrence_possible = False
                        break
                if occurrence_possible:
                    selected.add(group_id)
                    break
        if not selected:
            # The containing file should normally be removed by literal file
            # pruning.  Keep no empty hint; the table-wide rollback below makes
            # retained fallback files scan ALL.
            continue
        any_possible_group = True
        selected_ids = tuple(sorted(selected))
        if len(selected_ids) == expected_count:
            continue  # absence is the canonical ALL representation
        try:
            selections[file_key] = RowGroupSelection(
                expected_row_group_count=expected_count,
                selected_ids=selected_ids,
                footer_sha256=footer_sha256,
            )
        except (TypeError, ValueError):
            # A validation failure for one resource is local and fail-open.
            selections.pop(file_key, None)

    if not any_possible_group:
        # allow_empty=False: predicates excluded the entire table, so the file
        # pruner retains all files and row-group hints must do the same.
        return {}
    return selections


# ===========================================================================
# Stats + tombstone artifact accessors + in-process caches
# ---------------------------------------------------------------------------
# The stats parquet is read on every overwrite/delete (write-path pruning) and
# on every filtered read (query estimation); the tombstone (deletion-vector)
# parquet is read on every overwrite/delete to carry the vector forward.
# Re-reading either from object storage each time would defeat the point of
# having one consolidated artifact, so we keep the *latest* version of each
# table's stats — and, symmetrically, its deletion-vector — in memory.
#
# Cache semantics (deliberately minimal):
#   * Keyed by the table's stats directory; each entry holds exactly one
#     ``(path, DataFrame)`` — the most recent version seen for that table.
#   * A versioned stats filename is immutable, so a cache hit (cached path ==
#     requested path) can never be stale: a new write produces a NEW path, which
#     misses and reads fresh.
#   * Historical (time-travel) reads pass ``allow_cache=False`` so they read the
#     old version fresh WITHOUT evicting the table's cached latest.
#   * Writers call :func:`cache_stats` with the just-built frame, so the next
#     read is served from memory with no storage round-trip at all.
# ===========================================================================


def _cache_metadata_estimated_bytes(metadata: Any) -> int:
    """Conservative admission size for immutable validation metadata."""
    if metadata is None:
        return 0
    validation = getattr(metadata, "validation", None)
    if validation is not None:
        seals = getattr(validation, "resource_seals", None) or {}
        complete = getattr(validation, "complete_resource_rows", None) or {}
        # Dict/object/key overhead varies by interpreter. These intentionally
        # high constants keep the configured cap conservative without walking
        # object graphs or materialising reprs on the query hot path.
        return (
            256
            + sum(512 + len(str(key).encode("utf-8")) for key in seals)
            + sum(128 + len(str(key).encode("utf-8")) for key in complete)
        )
    if isinstance(metadata, tuple):
        total = 128
        for value in metadata:
            if isinstance(value, (set, frozenset)):
                total += sum(
                    96 + len(str(item).encode("utf-8")) for item in value
                )
            elif isinstance(value, str):
                total += 64 + len(value.encode("utf-8"))
            else:
                total += 64
        return total
    if isinstance(metadata, _TombstoneCacheSeal):
        state = metadata.state
        return (
            256
            + sum(
                256 + len(segment.file.encode("utf-8"))
                for segment in (state.segments if state is not None else ())
            )
            + sum(
                96 + len(item.encode("utf-8"))
                for item in metadata.referenced_files
            )
        )
    return 1024


class _PathKeyedFrameCache:
    """Process-wide LRU of each table's latest artifact frame (one per table).

    Keyed by the artifact's *directory*; each entry holds exactly one
    ``(path, DataFrame)`` — the most recent version seen for that table.  A
    versioned artifact filename is immutable, so a cache hit (cached path ==
    requested path) can never be stale: a new write produces a NEW path, which
    misses and reads fresh.  The cap is resolved dynamically per call via
    *cap_getter* (so a settings change / test patch takes effect immediately);
    ``<= 0`` disables the cache.  Backs both the stats and tombstone caches.
    """

    __slots__ = ("_lock", "_entries", "_cap_getter", "_byte_cap_getter", "_bytes")

    def __init__(
            self,
            cap_getter: Callable[[], int],
            byte_cap_getter: Optional[Callable[[], int]] = None,
    ) -> None:
        self._lock = threading.Lock()
        # table_key -> (path, DataFrame, estimated bytes, immutable metadata).
        # Stats entries use ``None`` metadata.  Tombstone entries carry a
        # validation seal so an immutable-path cache hit never rescans or
        # re-hashes a million-row deletion vector while the write lock is held.
        self._entries: "OrderedDict[str, Tuple[str, polars.DataFrame, int, Any]]" = OrderedDict()
        self._cap_getter = cap_getter
        self._byte_cap_getter = byte_cap_getter
        self._bytes = 0

    @staticmethod
    def _key(path: str) -> str:
        # New artifact versions are hour-partitioned.  Collapse the canonical
        # year/month/day/hour suffix so every immutable version of one table's
        # stats or deletion-vector shares a single LRU entry.  Legacy flat paths
        # naturally retain their immediate parent as the key.
        directory = os.path.dirname(path)
        for prefix in ("hour=", "day=", "month=", "year="):
            if os.path.basename(directory).startswith(prefix):
                directory = os.path.dirname(directory)
            else:
                break
        return directory

    def _cap(self) -> int:
        try:
            return int(self._cap_getter())
        except Exception:
            return 64

    def _byte_cap(self) -> Optional[int]:
        if self._byte_cap_getter is None:
            return None
        try:
            return int(self._byte_cap_getter())
        except Exception:
            return 256 * 1024 * 1024

    def _trim_locked(self, cap: int, byte_cap: Optional[int]) -> None:
        while self._entries and (
            len(self._entries) > cap
            or (byte_cap is not None and self._bytes > byte_cap)
        ):
            _key, (_path, _df, size, _metadata) = self._entries.popitem(last=False)
            self._bytes -= size

    def get(self, path: str) -> Optional[polars.DataFrame]:
        """Return the cached frame iff the cached path matches *exactly*."""
        entry = self.get_entry(path)
        return entry[0] if entry is not None else None

    def get_entry(self, path: str) -> Optional[Tuple[polars.DataFrame, Any]]:
        """Return ``(frame, metadata)`` for an exact immutable-path hit."""
        cap = self._cap()
        byte_cap = self._byte_cap()
        if cap <= 0 or (byte_cap is not None and byte_cap <= 0):
            self.clear()
            return None
        key = self._key(path)
        with self._lock:
            self._trim_locked(cap, byte_cap)
            entry = self._entries.get(key)
            if entry is not None and entry[0] == path:
                self._entries.move_to_end(key)
                return entry[1], entry[3]
        return None

    def put(self, path: str, df: polars.DataFrame, metadata: Any = None) -> None:
        cap = self._cap()
        byte_cap = self._byte_cap()
        if cap <= 0 or (byte_cap is not None and byte_cap <= 0):
            self.clear()
            return
        try:
            frame_bytes = max(0, int(df.estimated_size()))
        except Exception:
            # An unmeasurable frame cannot be admitted to a byte-bounded cache.
            if byte_cap is not None:
                return
            frame_bytes = 0
        entry_bytes = frame_bytes + _cache_metadata_estimated_bytes(metadata)
        key = self._key(path)
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                self._bytes -= old[2]
            if byte_cap is not None and entry_bytes > byte_cap:
                return
            self._entries[key] = (path, df, entry_bytes, metadata)
            self._bytes += entry_bytes
            self._entries.move_to_end(key)
            self._trim_locked(cap, byte_cap)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes = 0

    def discard(self, path: str) -> None:
        """Evict *path* only if it is the exact cached immutable version."""
        key = self._key(path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry[0] == path:
                removed = self._entries.pop(key, None)
                if removed is not None:
                    self._bytes -= removed[2]


def _stats_cache_cap() -> int:
    return int(settings.SUPERTABLE_STATS_CACHE_MAX_TABLES)


def _stats_cache_byte_cap() -> int:
    configured = getattr(settings, "SUPERTABLE_STATS_CACHE_MAX_BYTES", None)
    if not isinstance(configured, (int, str)) or isinstance(configured, bool):
        configured = os.environ.get(
            "SUPERTABLE_STATS_CACHE_MAX_BYTES", str(256 * 1024 * 1024)
        )
    return int(configured)


def _tombstone_cache_cap() -> int:
    return int(settings.SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES)


def _tombstone_cache_byte_cap() -> int:
    configured = getattr(settings, "SUPERTABLE_TOMBSTONE_CACHE_MAX_BYTES", None)
    if not isinstance(configured, (int, str)) or isinstance(configured, bool):
        configured = os.environ.get(
            "SUPERTABLE_TOMBSTONE_CACHE_MAX_BYTES", str(256 * 1024 * 1024)
        )
    return int(configured)


_STATS_CACHE = _PathKeyedFrameCache(_stats_cache_cap, _stats_cache_byte_cap)
_TOMBSTONE_CACHE = _PathKeyedFrameCache(
    _tombstone_cache_cap, _tombstone_cache_byte_cap
)


@dataclass(frozen=True)
class _StatsCacheMetadata:
    """Validation work cached beside one immutable stats-frame version."""

    validation: _StatsFrameValidation


def _stats_validation_for_frame(
        stats_df: polars.DataFrame,
        *,
        stats_path: Optional[str] = None,
) -> _StatsFrameValidation:
    """Compute digest/group indexes once per immutable cached stats frame."""
    cache_key = stats_cache_identity(stats_path) if stats_path else None
    cache_entry = _STATS_CACHE.get_entry(cache_key) if cache_key else None
    if cache_entry is not None:
        cached_frame, metadata = cache_entry
        if cached_frame is stats_df and isinstance(metadata, _StatsCacheMetadata):
            return metadata.validation

    calculated = _validate_stats_frame_once(stats_df)
    # Only attach metadata when this exact frame is already an admitted entry.
    # Historical allow_cache=False reads must not evict the current version.
    if cache_entry is not None and cache_entry[0] is stats_df:
        _STATS_CACHE.put(
            cache_key, stats_df, _StatsCacheMetadata(calculated),
        )
    return calculated


def load_stats(
        stats_path: Optional[str],
        *,
        allow_cache: bool = True,
        cache_identity: Optional[str] = None,
        profiler: Optional[Profiler] = None,
) -> Optional[polars.DataFrame]:
    """Load a table's stats parquet, serving the latest version from memory.

    *allow_cache* must be ``True`` only when *stats_path* is the table's CURRENT
    (latest) stats version — that version is what gets memoised.  Time-travel
    reads of an older version must pass ``allow_cache=False`` so they read fresh
    without disturbing the cached latest.
    """
    if not stats_path:
        return None
    p = profiler or get_null_profiler()
    identity = cache_identity or stats_cache_identity(stats_path)
    cached = _STATS_CACHE.get(identity)
    if cached is not None:
        p.add("stats_cache_hit", 1)
        return cached
    p.add("stats_cache_miss", 1)
    df = _read_parquet_safe(stats_path, profiler=p)
    if df is not None and df.schema != STATS_SCHEMA:
        try:
            df = _conform_stats_schema(df)
        except Exception as e:
            # Stats are an optional optimisation.  An artifact whose legacy or
            # corrupt schema cannot be made conservative must behave as absent,
            # never abort a SELECT/write or feed untyped values into pruning.
            logging.warning(
                f"[stats] incompatible stats schema at {stats_path}: {e}"
            )
            return None
    if df is not None and allow_cache:
        _STATS_CACHE.put(identity, df)
    return df


def cache_stats(
        stats_path: Optional[str],
        df: Optional[polars.DataFrame],
        *,
        cache_identity: Optional[str] = None,
        validation: Optional[_StatsFrameValidation] = None,
) -> None:
    """Seed the cache with a freshly built latest-version stats frame.

    Called by writers right after :func:`build_stats_file` so the very next
    read (this process's next overwrite/delete or query) needs no storage read.
    """
    if stats_path and df is not None:
        identity = cache_identity or stats_cache_identity(stats_path)
        metadata = (
            _StatsCacheMetadata(validation)
            if isinstance(validation, _StatsFrameValidation)
            else None
        )
        _STATS_CACHE.put(identity, df, metadata)


def _normalized_tombstone_format(value: Optional[int]) -> int:
    if value is None:
        return TOMBSTONE_FORMAT_V1
    if type(value) is int and value == TOMBSTONE_FORMAT_V1:
        return TOMBSTONE_FORMAT_V1
    if type(value) is int and value == TOMBSTONE_FORMAT_V2:
        return TOMBSTONE_FORMAT_V2
    raise TombstoneManifestV2Error("tombstone_format must be integer 1 or 2")


def _v1_loaded_tombstone_state(
        *,
        path: str,
        frame: polars.DataFrame,
        digest: Optional[str],
        storage: Optional[object],
        expected_segment_prefix: Optional[str],
) -> LoadedTombstoneState:
    logical_digest = _checked_tombstone_expected_digest(
        digest, source=f"legacy deletion-vector {path}"
    ) or tombstone_digest(frame, assume_valid=True)
    active_storage = storage if storage is not None else _get_storage()
    try:
        file_size = int(active_storage.size(path))
    except Exception as exc:
        raise TombstoneManifestV2Error(
            f"Unable to seal legacy deletion-vector size at {path!r}"
        ) from exc
    if expected_segment_prefix is not None:
        prefix = validate_logical_storage_path(
            expected_segment_prefix.rstrip("/"),
            field_name="expected_segment_prefix",
        ) + "/"
        logical_path = validate_logical_storage_path(
            path,
            field_name="legacy deletion-vector path",
            required_suffix=".parquet",
        )
        if not logical_path.startswith(prefix):
            raise TombstoneManifestV2Error(
                "legacy deletion-vector escapes the expected table prefix"
            )
    segment = TombstoneSegment(
        file=path,
        rows=int(frame.height),
        file_size=file_size,
        digest=logical_digest,
    )
    return LoadedTombstoneState(
        frame=frame,
        tombstone_format=TOMBSTONE_FORMAT_V1,
        tombstone_path=path,
        root_digest=logical_digest,
        segments=(segment,),
    )


def _read_tombstone_manifest_v2(
        path: str,
        *,
        storage: Optional[object],
        loader: Optional[Callable[[], Any]],
        required: bool,
) -> Optional[Any]:
    try:
        if loader is not None:
            raw = loader()
            if isinstance(raw, str):
                raw_size = len(raw.encode("utf-8"))
            elif isinstance(raw, (bytes, bytearray, memoryview)):
                raw_size = len(raw)
            else:
                # The strict canonical boundary below rejects decoded mappings.
                # Do not attempt to serialize an arbitrary loader result here.
                return raw
            if not 1 <= raw_size <= MAX_TOMBSTONE_MANIFEST_V2_BYTES:
                raise TombstoneManifestV2Error(
                    "manifest JSON size is outside the supported bound"
                )
            return raw
        active_storage = storage if storage is not None else _get_storage()
        stat_object = getattr(active_storage, "stat_object", None)
        read_range = getattr(active_storage, "read_range", None)
        if not callable(stat_object) or not callable(read_range):
            raise TombstoneManifestV2Error(
                "Configured storage provides no bounded manifest read"
            )
        before = stat_object(path)
        if not isinstance(before, ObjectMetadata):
            raise TombstoneManifestV2Error(
                "Configured storage returned invalid manifest metadata"
            )
        if before.identity_token() is None:
            raise TombstoneManifestV2Error(
                "Manifest metadata has no immutable provider identity"
            )
        manifest_size = before.size
        if (
            type(manifest_size) is not int
            or not 1 <= manifest_size <= MAX_TOMBSTONE_MANIFEST_V2_BYTES
        ):
            raise TombstoneManifestV2Error(
                "manifest JSON size is outside the supported bound"
            )
        raw = read_range(path, 0, manifest_size, expected=before)
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise TombstoneManifestV2Error(
                "bounded manifest read did not return bytes"
            )
        raw = bytes(raw)
        if len(raw) != manifest_size:
            raise TombstoneManifestV2Error(
                "bounded manifest read returned a short or oversized payload"
            )
        after = stat_object(path)
        if not isinstance(after, ObjectMetadata) or after != before:
            raise TombstoneManifestV2Error(
                "manifest object changed during the bounded read"
            )
        return raw
    except Exception:
        if required:
            raise
        logging.warning("[read] failed to read tombstone manifest at %s", path)
        return None


def _load_tombstone_manifest_v2_state(
        tombstone_path: str,
        *,
        storage: Optional[object],
        manifest_loader: Optional[Callable[[], Any]],
        segment_loader: Optional[Callable[[TombstoneSegment], Any]],
        required: bool,
        expected_rows: Optional[int],
        expected_digest: Optional[str],
        allowed_files: Optional[Set[str]],
        expected_organization: Optional[str],
        expected_super_name: Optional[str],
        expected_simple_name: Optional[str],
        pinned_snapshot_version: Optional[int],
        expected_segment_prefix: Optional[str],
        profiler: Profiler,
) -> Optional[LoadedTombstoneState]:
    with profiler.span("tombstone_v2.manifest_read"):
        raw = _read_tombstone_manifest_v2(
            tombstone_path,
            storage=storage,
            loader=manifest_loader,
            required=required,
        )
    if raw is None:
        return None
    with profiler.span("tombstone_v2.manifest_validate"):
        manifest = load_tombstone_manifest_v2(
            raw,
            expected_organization=expected_organization,
            expected_super_name=expected_super_name,
            expected_simple_name=expected_simple_name,
            pinned_snapshot_version=pinned_snapshot_version,
            expected_total_rows=expected_rows,
            expected_digest=expected_digest,
            expected_segment_prefix=expected_segment_prefix,
            require_canonical_json=True,
        )
    profiler.add("tombstone_v2_segments_loaded", len(manifest.segments))
    profiler.add("tombstone_v2_manifest_rows", manifest.total_rows)
    active_storage = storage if storage is not None else _get_storage()
    stat_object = getattr(active_storage, "stat_object", None)
    if not callable(stat_object):
        raise TombstoneManifestV2Error(
            "Configured storage provides no segment identity metadata"
        )
    frames: List[polars.DataFrame] = []
    for segment in manifest.segments:
        observed_size: Optional[int] = None
        try:
            before = stat_object(segment.file)
            if not isinstance(before, ObjectMetadata):
                raise TombstoneManifestV2Error(
                    "Configured storage returned invalid segment metadata"
                )
            if before.identity_token() is None:
                raise TombstoneManifestV2Error(
                    "Segment metadata has no immutable provider identity"
                )
            if (
                type(before.size) is not int
                or before.size != segment.file_size
            ):
                raise TombstoneManifestV2Error(
                    f"segment {segment.file!r} file_size does not match "
                    "the manifest"
                )
            observed_size = before.size
            loaded = (
                segment_loader(segment)
                if segment_loader is not None else None
            )
            if isinstance(loaded, tuple) and len(loaded) == 2:
                segment_frame, observed_size = loaded
            elif loaded is not None:
                segment_frame = loaded
            else:
                segment_frame = _read_parquet_safe(
                    segment.file,
                    profiler=profiler,
                    file_size=segment.file_size,
                    required=True,
                    storage=active_storage,
                )
            if segment_frame is None:
                raise FileNotFoundError(
                    f"Required deletion-vector segment is missing: {segment.file}"
                )
            with profiler.span("tombstone_v2.segment_validate"):
                segment_frame = validate_tombstone_frame(
                    segment_frame,
                    expected_rows=segment.rows,
                    expected_digest=segment.digest,
                    allowed_files=allowed_files,
                    source=f"deletion-vector segment {segment.file}",
                )
            validate_tombstone_segment_observation(
                segment,
                file_size=observed_size,
                rows=segment_frame.height,
                digest=segment.digest,
            )
            after = stat_object(segment.file)
            if not isinstance(after, ObjectMetadata):
                raise TombstoneManifestV2Error(
                    "Configured storage returned invalid segment metadata"
                )
            if (
                type(after.size) is not int
                or after.size != segment.file_size
                or after != before
            ):
                raise TombstoneManifestV2Error(
                    f"segment {segment.file!r} changed during read"
                )
            frames.append(segment_frame)
        except Exception:
            if required:
                raise
            logging.warning(
                "[read] failed to validate tombstone segment at %s",
                segment.file,
            )
            return None
    with profiler.span("tombstone_v2.union_integrity"):
        combined = polars.concat(frames, how="vertical")
        combined = validate_tombstone_frame(
            combined,
            expected_rows=manifest.total_rows,
            allowed_files=allowed_files,
            source=f"deletion-vector manifest union {tombstone_path}",
        )
    return LoadedTombstoneState(
        frame=combined,
        tombstone_format=TOMBSTONE_FORMAT_V2,
        tombstone_path=tombstone_path,
        root_digest=manifest.digest(),
        segments=manifest.segments,
    )


def load_tombstone(
        tombstone_path: Optional[str],
        *,
        cache_identity: Optional[str] = None,
        loader: Optional[Callable[[], Optional[polars.DataFrame]]] = None,
        manifest_loader: Optional[Callable[[], Any]] = None,
        segment_loader: Optional[Callable[[TombstoneSegment], Any]] = None,
        allow_cache: bool = True,
        required: bool = False,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        allowed_files: Optional[Set[str]] = None,
        profiler: Optional[Profiler] = None,
        tombstone_format: Optional[int] = None,
        state_out: Optional[Dict[str, LoadedTombstoneState]] = None,
        storage: Optional[object] = None,
        expected_organization: Optional[str] = None,
        expected_super_name: Optional[str] = None,
        expected_simple_name: Optional[str] = None,
        pinned_snapshot_version: Optional[int] = None,
        expected_segment_prefix: Optional[str] = None,
) -> Optional[polars.DataFrame]:
    """Load a v1 Parquet DV or v2 manifest union, preserving old return type.

    ``cache_identity`` may provide a stable, authorization-scoped raw object
    identity when ``tombstone_path`` is a rotating presigned URL. It controls
    only the cache key; misses are still read from ``tombstone_path``.
    ``loader`` lets an engine use its bounded storage-SDK read on a miss while
    retaining the same sealed cache and validation path.

    Symmetric to :func:`load_stats`.  *allow_cache* must be ``True`` only when
    *tombstone_path* is the table's CURRENT (latest) deletion-vector version —
    that version is what gets memoised; time-travel/older reads must pass
    ``allow_cache=False``.

    *required* is forwarded to the reader: when ``True``, both absence and a
    genuine read failure re-raise rather than being swallowed to ``None`` (a
    truncated carried-forward deletion-vector would resurrect deleted rows).
    ``expected_rows`` seals the immutable snapshot pointer to its declared row
    count.  A cache hit is validated against the same schema/count contract.
    """
    normalized_format = _normalized_tombstone_format(tombstone_format)
    if not tombstone_path:
        return None
    identity = str(cache_identity or tombstone_cache_identity(tombstone_path))
    p = profiler or get_null_profiler()
    cached_entry = _TOMBSTONE_CACHE.get_entry(identity) if allow_cache else None
    if cached_entry is not None:
        cached, metadata = cached_entry
        source = f"cached deletion-vector {identity}"
        metadata_state = (
            metadata.state
            if isinstance(metadata, _TombstoneCacheSeal) else None
        )
        # A legacy three-tuple cannot prove a v2 root/segment manifest. Treat it
        # as a miss rather than comparing the root digest to the union digest.
        if normalized_format == TOMBSTONE_FORMAT_V2 and (
            metadata_state is None
            or metadata_state.tombstone_format != TOMBSTONE_FORMAT_V2
        ):
            cached_entry = None
        else:
            p.add("tombstone_cache_hit", 1)
        if cached_entry is not None and not (
            isinstance(metadata, _TombstoneCacheSeal)
            or (
                isinstance(metadata, tuple)
                and len(metadata) >= 3
                and isinstance(metadata[0], int)
                and isinstance(metadata[1], str)
                and isinstance(metadata[2], frozenset)
            )
        ):
            # Backward-compatible repair for an entry inserted through the
            # generic frame-cache API.  Validate/hash once, then all later hits
            # use the immutable seal below.
            cached = validate_tombstone_frame(
                cached,
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                allowed_files=allowed_files,
                source=source,
            )
            metadata = _tombstone_cache_seal(
                cached, known_digest=expected_digest, source=source,
            )
            _TOMBSTONE_CACHE.put(identity, cached, metadata)
        if cached_entry is not None:
            _validate_cached_tombstone_seal(
                metadata,
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                allowed_files=allowed_files,
                source=source,
            )
            if state_out is not None:
                state = (
                    metadata.state
                    if isinstance(metadata, _TombstoneCacheSeal) else None
                )
                if state is None:
                    state = _v1_loaded_tombstone_state(
                        path=tombstone_path,
                        frame=cached,
                        digest=expected_digest,
                        storage=storage,
                        expected_segment_prefix=expected_segment_prefix,
                    )
                    metadata = _tombstone_cache_seal(
                        cached,
                        known_digest=state.root_digest,
                        state=state,
                        source=source,
                    )
                    _TOMBSTONE_CACHE.put(identity, cached, metadata)
                state_out["state"] = state
            return cached
    p.add("tombstone_cache_miss", 1)
    loaded_state: Optional[LoadedTombstoneState] = None
    if normalized_format == TOMBSTONE_FORMAT_V2:
        loaded_state = _load_tombstone_manifest_v2_state(
            tombstone_path,
            storage=storage,
            manifest_loader=manifest_loader,
            segment_loader=segment_loader,
            required=required,
            expected_rows=expected_rows,
            expected_digest=expected_digest,
            allowed_files=allowed_files,
            expected_organization=expected_organization,
            expected_super_name=expected_super_name,
            expected_simple_name=expected_simple_name,
            pinned_snapshot_version=pinned_snapshot_version,
            expected_segment_prefix=expected_segment_prefix,
            profiler=p,
        )
        df = loaded_state.frame if loaded_state is not None else None
    else:
        df = (
            loader()
            if loader is not None
            else _read_parquet_safe(
                tombstone_path,
                profiler=p,
                required=required,
                storage=storage,
            )
        )
        if df is not None:
            df = validate_tombstone_frame(
                df,
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                allowed_files=allowed_files,
                source=f"deletion-vector {tombstone_path}",
            )
            if state_out is not None:
                loaded_state = _v1_loaded_tombstone_state(
                    path=tombstone_path,
                    frame=df,
                    digest=expected_digest,
                    storage=storage,
                    expected_segment_prefix=expected_segment_prefix,
                )
    if loaded_state is not None and state_out is not None:
        state_out["state"] = loaded_state
    if df is not None and allow_cache:
        seal = _tombstone_cache_seal(
            df,
            known_digest=(
                loaded_state.root_digest
                if loaded_state is not None else expected_digest
            ),
            state=loaded_state,
            source=f"deletion-vector cache entry {tombstone_path}",
        )
        _TOMBSTONE_CACHE.put(identity, df, seal)
    return df


def load_tombstone_manifest_from_storage(
        storage: StorageInterface,
        manifest_key: str,
        *,
        expected_organization: Optional[str] = None,
        expected_super_name: Optional[str] = None,
        expected_simple_name: Optional[str] = None,
        pinned_snapshot_version: Optional[int] = None,
        expected_total_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        expected_segment_prefix: Optional[str] = None,
) -> TombstoneManifestV2:
    """Read one canonical v2 manifest through a bounded conditional range.

    The shared reader seals the object with ``stat_object``, issues one
    conditional ``read_range`` for no more than 256 KiB, and reseals metadata
    afterward. Backends without that bounded identity contract are rejected.
    The body must itself be the canonical JSON representation sealed by the
    snapshot root digest.
    """
    if storage is None:
        raise TombstoneManifestV2Error(
            "v2 tombstone manifest requires a storage backend"
        )
    try:
        key = validate_logical_storage_path(
            manifest_key,
            field_name="tombstone manifest pointer",
            required_suffix=".json",
        )
    except TombstoneManifestV2Error:
        raise
    except (TypeError, ValueError) as exc:
        # ``urllib.parse.urlsplit`` can raise a plain ValueError for malformed
        # bracketed authorities. Keep provider/path parser details behind the
        # manifest integrity boundary rather than leaking an alternate error
        # type to callers.
        raise TombstoneManifestV2Error(
            "tombstone manifest pointer is not a valid logical storage path"
        ) from exc
    exact_body = read_bounded_tombstone_manifest_bytes(storage, key)
    try:
        return load_tombstone_manifest_v2(
            bytes(exact_body),
            expected_organization=expected_organization,
            expected_super_name=expected_super_name,
            expected_simple_name=expected_simple_name,
            pinned_snapshot_version=pinned_snapshot_version,
            expected_total_rows=expected_total_rows,
            expected_digest=expected_digest,
            expected_segment_prefix=expected_segment_prefix,
            require_canonical_json=True,
        )
    except TombstoneManifestV2Error:
        raise
    except (TypeError, ValueError) as exc:
        raise TombstoneManifestV2Error(
            "tombstone manifest contains an invalid logical storage path"
        ) from exc


def _load_one_tombstone_segment(
        segment: TombstoneSegmentDef,
        *,
        storage: StorageInterface,
        allowed_files: Optional[Set[str]],
) -> polars.DataFrame:
    """Read and validate one manifest-sealed Parquet segment."""
    if not isinstance(segment, TombstoneSegmentDef):
        raise ValueError("v2 deletion-vector segments are malformed")
    try:
        observed_size = storage.size(segment.cache_key)
    except Exception as exc:
        raise ValueError(
            "Unable to observe required deletion-vector segment size"
        ) from exc
    if (
        not isinstance(observed_size, int)
        or isinstance(observed_size, bool)
        or observed_size <= 0
        or observed_size != segment.file_size
    ):
        raise ValueError(
            "Deletion-vector segment size does not match the manifest"
        )

    resolved = str(segment.tombstone_path or "")
    if not resolved:
        raise ValueError("Required deletion-vector segment has no resolved path")
    try:
        if "://" not in resolved and os.path.isfile(resolved):
            local_size = os.path.getsize(resolved)
            if local_size != segment.file_size:
                raise ValueError(
                    "Localized deletion-vector segment size does not match "
                    "the manifest"
                )
            frame = polars.read_parquet(resolved, hive_partitioning=False)
        else:
            frame = polars.from_arrow(storage.read_parquet(segment.cache_key))
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(
            "Unable to read required deletion-vector segment"
        ) from exc
    return validate_tombstone_frame(
        frame,
        expected_rows=segment.expected_rows,
        expected_digest=segment.tombstone_digest,
        allowed_files=allowed_files,
        source=f"deletion-vector segment {segment.cache_key}",
    )


def load_tombstone_segments(
        segments: Tuple[TombstoneSegmentDef, ...],
        *,
        storage: StorageInterface,
        cache_identity: str,
        expected_rows: int,
        allowed_files: Optional[Set[str]] = None,
        allow_cache: bool = True,
        profiler: Optional[Profiler] = None,
) -> polars.DataFrame:
    """Load, seal, and union all segments of one v2 deletion vector.

    ``cache_identity`` must bind the stable manifest key and its snapshot-root
    digest.  Segment digests are the logical ``st-dv-v1`` values and are
    checked independently.  The snapshot root is deliberately never compared
    with the union's logical digest; it seals the canonical JSON manifest.
    After concatenation the ordinary frame validator proves table-global rowid
    uniqueness and snapshot-file membership across segment boundaries.
    """
    if not isinstance(segments, tuple) or not segments:
        raise ValueError("Active v2 deletion vector has no segments")
    if len(segments) > MAX_TOMBSTONE_MANIFEST_V2_SEGMENTS:
        raise ValueError("v2 deletion vector contains too many segments")
    if any(not isinstance(segment, TombstoneSegmentDef) for segment in segments):
        raise ValueError("v2 deletion-vector segments are malformed")
    segment_keys: List[str] = []
    segment_rows = 0
    for segment in segments:
        if (
            not isinstance(segment.cache_key, str)
            or not segment.cache_key
            or not isinstance(segment.tombstone_path, str)
            or not segment.tombstone_path
            or not isinstance(segment.expected_rows, int)
            or isinstance(segment.expected_rows, bool)
            or not isinstance(segment.file_size, int)
            or isinstance(segment.file_size, bool)
            or segment.file_size <= 0
            or segment.file_size > MAX_JSON_EXACT_INTEGER
        ):
            raise ValueError("v2 deletion-vector segment path/size is malformed")
        try:
            validate_logical_storage_path(
                segment.cache_key,
                field_name="segment cache key",
                required_suffix=".parquet",
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "v2 deletion-vector segment cache key is malformed"
            ) from exc
        checked_rows = _checked_tombstone_expected_rows(
            segment.expected_rows,
            source="v2 deletion-vector segment",
        )
        if (
            checked_rows is None
            or checked_rows <= 0
            or checked_rows > MAX_JSON_EXACT_INTEGER
        ):
            raise ValueError("v2 deletion-vector segment row count is malformed")
        _checked_tombstone_expected_digest(
            segment.tombstone_digest,
            source="v2 deletion-vector segment",
        )
        segment_keys.append(segment.cache_key)
        segment_rows += checked_rows
    if segment_keys != sorted(segment_keys) or len(segment_keys) != len(
        set(segment_keys)
    ):
        raise ValueError(
            "v2 deletion-vector segments are not uniquely and canonically ordered"
        )
    if not isinstance(cache_identity, str) or not cache_identity:
        raise ValueError("v2 deletion-vector cache identity is missing")
    if not isinstance(expected_rows, int) or isinstance(expected_rows, bool):
        raise ValueError("v2 deletion-vector union has invalid expected row count")
    expected = _checked_tombstone_expected_rows(
        expected_rows, source="v2 deletion-vector union",
    )
    if (
        expected is None
        or expected <= 0
        or expected > MAX_JSON_EXACT_INTEGER
    ):
        raise ValueError("v2 deletion-vector union has invalid expected row count")
    if segment_rows != expected:
        raise ValueError(
            "v2 deletion-vector segment rows do not match the manifest total"
        )

    # Keep the v2 union namespace disjoint from legacy single-file cache keys.
    # The caller supplies ``manifest-key + root``; hashing prevents path/URL
    # material from becoming a filesystem-like cache identity elsewhere.
    descriptor_digest = hashlib.sha256(b"supertable-dv-v2-segments\n")
    for segment in segments:
        for value in (
            segment.cache_key,
            str(segment.expected_rows),
            str(segment.file_size),
            segment.tombstone_digest,
        ):
            encoded = str(value).encode("utf-8")
            descriptor_digest.update(len(encoded).to_bytes(8, "big"))
            descriptor_digest.update(encoded)
    identity = (
        "__supertable_tombstone_v2_union__/"
        + hashlib.sha256(
            cache_identity.encode("utf-8")
            + b"\0"
            + descriptor_digest.digest()
        ).hexdigest()
    )

    def _loader() -> polars.DataFrame:
        frames = [
            _load_one_tombstone_segment(
                segment,
                storage=storage,
                allowed_files=allowed_files,
            )
            for segment in segments
        ]
        return polars.concat(frames, how="vertical", rechunk=False)

    frame = load_tombstone(
        identity,
        cache_identity=identity,
        loader=_loader,
        allow_cache=allow_cache,
        required=True,
        expected_rows=expected,
        # A v2 root digest seals canonical manifest JSON, not DV logical rows.
        expected_digest=None,
        allowed_files=allowed_files,
        profiler=profiler,
    )
    if frame is None:  # required loader and active segments make this defensive.
        raise ValueError("Required v2 deletion vector was unavailable")
    return frame


def load_tombstone_state(
        tombstone_path: Optional[str], **kwargs: Any,
) -> Optional[LoadedTombstoneState]:
    """State-returning companion to :func:`load_tombstone`."""
    if not tombstone_path:
        return None
    state_out: Dict[str, LoadedTombstoneState] = {}
    kwargs["state_out"] = state_out
    frame = load_tombstone(tombstone_path, **kwargs)
    if frame is None:
        return None
    state = state_out.get("state")
    if state is None:  # pragma: no cover - every successful path sets it
        raise RuntimeError("Deletion-vector load produced no physical state")
    return state


def cache_tombstone(
        tombstone_path: Optional[str],
        df: Optional[polars.DataFrame],
        *,
        cache_identity: Optional[str] = None,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        assume_valid: bool = False,
        loaded_state: Optional[LoadedTombstoneState] = None,
        tombstone_format: Optional[int] = None,
) -> None:
    """Seed the cache with a freshly built latest-version deletion-vector frame.

    Called by writers right after the tombstone pointer is finalised so the next
    write's carry-forward read (this process's next overwrite/delete) needs no
    storage round-trip.  No-op when the vector was fully consumed this write
    (``tombstone_path`` is ``None``) or unchanged with no frame in hand.
    """
    if tombstone_path and df is not None:
        source = f"deletion-vector cache seed {tombstone_path}"
        normalized_format = _normalized_tombstone_format(
            loaded_state.tombstone_format
            if loaded_state is not None else tombstone_format
        )
        if assume_valid:
            # Internal writer fast path: the exact frame was validated and
            # hashed immediately before this call.  Retain cheap structural and
            # seal checks, but do not repeat the O(rows) integrity scan/hash.
            if not isinstance(df, polars.DataFrame) or df.schema != TOMBSTONE_SCHEMA:
                raise ValueError(f"{source} has invalid schema")
            checked_rows = _checked_tombstone_expected_rows(
                expected_rows, source=source,
            )
            checked_digest = _checked_tombstone_expected_digest(
                expected_digest, source=source,
            )
            if checked_rows is None or checked_digest is None:
                raise ValueError(
                    "assume_valid tombstone cache seeds require row and digest seals"
                )
            if df.height != checked_rows:
                raise ValueError(
                    f"{source} row-count mismatch: expected {checked_rows}, "
                    f"got {df.height}"
                )
            validated = df
        else:
            if normalized_format == TOMBSTONE_FORMAT_V2:
                validated = validate_tombstone_frame(
                    df,
                    expected_rows=expected_rows,
                    source=source,
                )
                checked_digest = _checked_tombstone_expected_digest(
                    expected_digest, source=source,
                )
            else:
                validated = validate_tombstone_frame(
                    df,
                    expected_rows=expected_rows,
                    expected_digest=expected_digest,
                    source=source,
                )
                checked_digest = expected_digest
        if normalized_format == TOMBSTONE_FORMAT_V2:
            if (
                loaded_state is None
                or loaded_state.tombstone_format != TOMBSTONE_FORMAT_V2
                or loaded_state.frame is not df
                or loaded_state.tombstone_path != tombstone_path
                or loaded_state.root_digest != checked_digest
            ):
                raise ValueError(
                    "format-2 tombstone cache seeds require the exact loaded state"
                )
        seal = _tombstone_cache_seal(
            validated,
            known_digest=checked_digest,
            state=loaded_state,
            source=source,
        )
        identity = str(cache_identity or tombstone_cache_identity(tombstone_path))
        _TOMBSTONE_CACHE.put(identity, validated, seal)


def evict_tombstone(
        tombstone_path: Optional[str],
        *,
        cache_identity: Optional[str] = None,
) -> None:
    """Remove a superseded/currently-drained deletion-vector cache entry."""
    if tombstone_path:
        identity = str(cache_identity or tombstone_cache_identity(tombstone_path))
        _TOMBSTONE_CACHE.discard(identity)


def compact_tombstones(
        snapshot: dict,
        tombstone_df: polars.DataFrame,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
        return_residual: bool = False,
        footer_md_out: Optional[Dict] = None,
) -> Tuple:
    """Physically drop tombstoned rows from the data files that hold them.

    *tombstone_df* is the deletion-vector (columns ``__file__`` + ``__rowid__``).
    Only the data files named in ``__file__`` are read and rewritten — a
    targeted compaction. For each, rows whose ``__rowid__`` is in the vector
    are anti-joined out and the survivors written to a new file; the original
    is sunset. Survivors keep their original ``__rowid__`` (no remapping).

    Successfully processed groups no longer need tombstones.  Callers that can
    mutate metadata must request and publish the residual frame before clearing
    or replacing the old pointer.

    Returns ``(removed_rows, new_resources, sunset_files)`` for backward
    compatibility.  With ``return_residual=True`` a fourth item is returned:
    the validated tombstone entries that could not be safely consumed.

    A file is rewritten only when *every* rowid recorded for it is present in
    that physical file.  Missing resources are retained as residual entries;
    missing/unreadable current files raise.  This makes it impossible for a
    caller to clear an entry merely because one part of a drain was skipped.
    """
    p = profiler or get_null_profiler()
    if tombstone_df is None or tombstone_df.height == 0:
        empty = _empty_tombstone_df()
        return (0, [], set(), empty) if return_residual else (0, [], set())

    tombstone_df = validate_tombstone_frame(
        tombstone_df, source="deletion-vector passed to compaction"
    )

    resources = snapshot.get("resources") or []
    by_path = {r.get("file"): r for r in resources if r.get("file")}

    removed = 0
    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()
    residual_parts: List[polars.DataFrame] = []

    # Partition once.  The old implementation filtered all D rows once for
    # every one of F files (O(F*D)); this builds all file groups in O(D).
    grouped = tombstone_df.partition_by(
        TOMBSTONE_FILE_COL, as_dict=True, maintain_order=False
    )
    p.add("tombstone_files_total", len(grouped))

    def _validate_physical(frame: polars.DataFrame, file_path: str):
        if ROWID_COL not in frame.columns:
            raise ValueError(
                f"Cannot drain deletion-vector: {file_path!r} lacks {ROWID_COL!r}"
            )
        physical_ids = frame.get_column(ROWID_COL)
        if physical_ids.dtype != polars.Int64:
            raise ValueError(
                f"Cannot drain deletion-vector: {file_path!r} rowids are not Int64"
            )
        if physical_ids.null_count() > 0:
            raise ValueError(
                f"Cannot drain deletion-vector: {file_path!r} contains NULL rowids"
            )
        if (
            physical_ids.min() is None
            or physical_ids.min() <= 0
            or physical_ids.n_unique() != frame.height
        ):
            raise ValueError(
                f"Cannot drain deletion-vector: {file_path!r} contains "
                "non-positive or duplicate rowids"
            )
        return physical_ids

    def _drain_group(item):
        group_key, file_tombstones = item
        file_path = group_key[0] if isinstance(group_key, tuple) else group_key
        local_footer_md = {}
        resource = by_path.get(file_path)
        if not resource:
            # Do not discard a ghost entry: without its referenced physical file
            # we cannot prove that the deletion was consumed.  Keeping the whole
            # group residual is the only conservative action available here.
            return 0, [], None, file_tombstones, local_footer_md, Profiler()
        sub = Profiler()
        file_size = int(resource.get("file_size") or 0)
        dead_ids = file_tombstones.select(ROWID_COL)

        # Fully-dead fast path: when independent resource metadata agrees with
        # the DV cardinality, read only the rowid column and prove exact set
        # equality. A match needs no full decode and no successor parquet write.
        try:
            declared_rows = int(resource.get("rows"))
        except (TypeError, ValueError):
            declared_rows = -1
        if declared_rows == file_tombstones.height:
            projected = _read_parquet_safe(
                file_path,
                profiler=sub,
                file_size=file_size,
                columns=[ROWID_COL],
                required=True,
            )
            _validate_physical(projected, file_path)
            projected_ids = projected.select(ROWID_COL)
            if (
                projected_ids.join(dead_ids, on=ROWID_COL, how="anti").height == 0
                and dead_ids.join(
                    projected_ids, on=ROWID_COL, how="anti"
                ).height == 0
            ):
                sub.add("tombstone_fully_dead_fast_path", 1)
                sub.add("tombstone_files_touched", 1)
                return projected.height, [], file_path, None, local_footer_md, sub
            # Count agreed but identities did not: metadata/DV corruption. Do
            # not rewrite or discard any part of this group.
            return 0, [], None, file_tombstones, local_footer_md, sub

        # required=True: this is the only physical drain.  A transient backend
        # error or NotFound must abort with the prior snapshot + vector intact;
        # treating either as an empty source would permit pointer clearing and
        # row resurrection.
        existing_df = _read_parquet_safe(
            file_path, profiler=sub, file_size=file_size, required=True
        )
        if existing_df is None:  # defensive; required=True normally raises
            return 0, [], None, file_tombstones, local_footer_md, sub
        _validate_physical(existing_df, file_path)
        # A partial match cannot be safely rewritten while retaining only the
        # unmatched entries: doing so would move survivors to a new file while
        # the residual still names the sunset source.  Retain the whole group
        # and retry after the metadata inconsistency is repaired.
        unmatched = dead_ids.join(
            existing_df.select(ROWID_COL), on=ROWID_COL, how="anti"
        )
        if unmatched.height > 0:
            return 0, [], None, file_tombstones, local_footer_md, sub
        with sub.span("tombstone.anti_join"):
            kept_df = existing_df.join(dead_ids, on=ROWID_COL, how="anti")
        difference = existing_df.height - kept_df.height
        if difference == 0:
            return 0, [], None, file_tombstones, local_footer_md, sub

        local_resources = []
        sub.add("tombstone_files_touched", 1)

        if kept_df.height > 0:
            sub.add("tombstone_files_with_survivors", 1)
            with sub.span("tombstone.write_kept"):
                write_parquet_and_collect_resources(
                    write_df=kept_df,
                    overwrite_columns=[],
                    data_dir=data_dir,
                    new_resources=local_resources,
                    compression_level=compression_level,
                    profiler=sub,
                    footer_md_out=local_footer_md,
                )
        return difference, local_resources, file_path, None, local_footer_md, sub

    items = list(grouped.items())
    cfg = table_config or {}
    try:
        configured_workers = int(
            cfg.get("tombstone_compaction_workers")
            or getattr(default, "TOMBSTONE_COMPACTION_WORKERS", 2)
        )
    except (TypeError, ValueError):
        configured_workers = 2
    # Keep the executor bounded: every worker may hold one decoded data file
    # plus its successor frame.  The environment/per-table knob lets an
    # operator match object-store bandwidth without turning a threshold drain
    # into an unbounded memory fan-out.
    workers = max(1, min(configured_workers, 8, len(items)))
    collected_footer_md: Dict = {}
    first_failure: Optional[BaseException] = None
    first_traceback = None

    def _remember_failure(error: BaseException) -> None:
        nonlocal first_failure, first_traceback
        if first_failure is None:
            first_failure = error
            first_traceback = error.__traceback__

    def _accept_result(result) -> None:
        nonlocal removed
        (
            difference,
            local_resources,
            sunset,
            residual_part,
            local_footer_md,
            sub,
        ) = result
        # Track minted paths before telemetry/cache merging. If either of those
        # later operations fails, every successful worker upload is still known
        # to the rollback path.
        new_resources.extend(local_resources)
        p.merge(sub)
        if local_footer_md:
            collected_footer_md.update(local_footer_md)
        removed += difference
        if sunset:
            sunset_files.add(sunset)
        if residual_part is not None:
            residual_parts.append(residual_part)

    if workers == 1:
        for item in items:
            try:
                result = _drain_group(item)
                _accept_result(result)
            except BaseException as error:
                _remember_failure(error)
                break
    else:
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = []
        try:
            for item in items:
                try:
                    futures.append(
                        executor.submit(copy_context().run, _drain_group, item)
                    )
                except BaseException as error:
                    _remember_failure(error)
                    break
            # Consume every submitted future even after an earlier ordered
            # result fails. A later worker may already have uploaded a valid
            # successor; dropping its result here would make that object
            # invisible to invocation rollback.
            for future in futures:
                try:
                    result = future.result()
                except BaseException as error:
                    _remember_failure(error)
                    continue
                try:
                    _accept_result(result)
                except BaseException as error:
                    _remember_failure(error)
        finally:
            try:
                executor.shutdown(wait=True, cancel_futures=False)
            except BaseException as error:
                _remember_failure(error)

    if first_failure is not None:
        _cleanup_compaction_outputs(new_resources)
        raise first_failure.with_traceback(first_traceback)

    try:
        residual = (
            polars.concat(residual_parts, how="vertical")
            if residual_parts else _empty_tombstone_df()
        )
        residual = validate_tombstone_frame(
            residual, source="residual deletion-vector after compaction"
        )
    except BaseException:
        _cleanup_compaction_outputs(new_resources)
        raise
    if footer_md_out is not None and collected_footer_md:
        try:
            footer_md_out.update(collected_footer_md)
        except BaseException:
            _cleanup_compaction_outputs(new_resources)
            raise
    if return_residual:
        return removed, new_resources, sunset_files, residual
    return removed, new_resources, sunset_files
