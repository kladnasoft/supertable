# processing.py

import decimal
import hashlib
import json
import logging
import os
import io
import time
import threading
import uuid
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timezone
from typing import Any, Callable, Dict, FrozenSet, List, Set, Tuple, Optional

import polars
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.utils.helper import generate_filename, hourly_partition_subpath
from supertable.config.defaults import default
from supertable.config.settings import settings
from supertable.storage.storage_factory import get_storage
from supertable.utils.profiler import Profiler, get_null_profiler
from supertable.data_classes import PredInterval, RowGroupSelection

# Target row-group size for all Parquet writes.
# 122 880 rows ≈ 120 K — sits comfortably in the recommended 100 K–1 M range.
# Smaller groups mean tighter min/max statistics so DuckDB can skip more groups;
# larger groups reduce metadata overhead.  120 K is a good balance for the
# incremental-merge pattern used here.
_PARQUET_ROW_GROUP_SIZE = 122_880


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
    For memory-bounded streaming compaction, callers should still iterate
    with chunked flushes via :func:`concat_with_union` — this helper is for
    callers that already have all frames in memory.
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

def _safe_exists(path: str, profiler: Optional[Profiler] = None, strict: bool = False) -> bool:
    p = profiler or get_null_profiler()
    try:
        with p.span("io.exists"):
            return _get_storage().exists(path)
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
) -> Optional[polars.DataFrame]:
    """Read a parquet object into polars, or ``None`` when it is absent.

    When *required* is True, both absence and a genuine read failure are raised.
    Callers use that mode only for objects referenced by the locked current
    snapshot, where treating NotFound as an empty artifact/file would truncate a
    deletion decision and can resurrect rows.  Optional/race-tolerant callers use
    the default mode and continue to receive ``None`` for absence/failure.
    """
    p = profiler or get_null_profiler()
    if not _safe_exists(path, profiler=p, strict=required):
        logging.info(f"[race] file already sunset by another writer: {path}")
        if required:
            raise FileNotFoundError(f"Required parquet object is missing: {path}")
        return None
    try:
        with p.span("io.read_parquet"):
            # Project to *columns* when given so only those column chunks are
            # read (memory-bound fallback); gated so storages/test doubles that
            # only accept ``path`` keep working on the unprojected paths.
            tbl = (
                _get_storage().read_parquet(path, columns=columns)
                if columns else _get_storage().read_parquet(path)
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
) -> Tuple[int, int, List[Dict], Set[str]]:
    """Compact small parquet files in a snapshot's resources list.

    Reads files and rewrites them in memory-bounded chunks, **without
    needing any incoming data**. Used by ``DataWriter.compact()`` and
    ``SimpleTable.export_to()``.

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

    Returns:
        A 4-tuple ``(considered, total_rows, new_resources, sunset_files)``:

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

    Value-preservation properties enforced here:

      - Each source file is read **exactly once** via
        ``_read_parquet_safe`` (which returns ``None`` for races where
        another writer already sunset the file).
      - The merge is a row-preserving ``concat_with_union`` — no
        deduplication, no row drops. Tombstone-driven row removal is a
        **separate** pre-step performed by ``compact_tombstones`` in
        the caller (so this function only sees the post-tombstone
        survivors when ``DataWriter.compact()`` invokes it).
      - All columns from every source file are preserved: missing
        columns in any input are filled with ``null`` via
        ``concat_with_union``, never silently dropped.
      - Source files are added to ``sunset_files`` **only after** their
        rows have been successfully buffered into ``merged_df``. If a
        read fails (``_read_parquet_safe`` returns ``None``), the file
        is left in the snapshot — the next compaction retries it.
    """
    p = profiler or get_null_profiler()
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
            )
    except Exception:
        if required_reads and new_resources:
            # A logical materialisation must be all-or-nothing from the caller's
            # perspective. Delete only keys minted by this invocation; never
            # enumerate or touch pre-existing target contents.
            delete = getattr(_get_storage(), "delete", None)
            if callable(delete):
                for resource in new_resources:
                    generated = resource.get("file") if isinstance(resource, dict) else None
                    if not generated:
                        continue
                    try:
                        delete(generated)
                    except Exception as cleanup_error:
                        logging.warning(
                            f"[materialize] failed to clean orphan {generated}: "
                            f"{cleanup_error}"
                        )
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


def _write_single_parquet_file(
        write_df, overwrite_columns, target_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
        footer_md_out: Optional[Dict] = None,
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

    # Write to the active storage backend.  Encoding and upload are deliberately
    # separate: a failed object-store PUT must abort the mutation, never fall back
    # to writing the bare object key on the writer's local filesystem.
    with p.span("write.to_arrow"):
        arrow_tbl: pa.Table = write_df.to_arrow()
    with p.span("write.parquet_encode"):
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
        data = buf.getvalue()

    write_bytes = getattr(storage, "write_bytes", None)
    write_parquet = getattr(storage, "write_parquet", None)
    if callable(write_bytes):
        with p.span("write.upload_bytes"):
            write_bytes(new_parquet_path, data)
        # These are the exact bytes accepted by the backend.  A follow-up
        # size()/HEAD is both redundant and racy (the object could be replaced
        # between PUT and HEAD), and adds a full network round-trip remotely.
        file_size = len(data)
        # The uploaded bytes ARE ``data`` here, so parse the footer in memory
        # (footer-only, no decode, no network round-trip) for stats reuse.
        if footer_md_out is not None:
            try:
                footer_md_out[new_parquet_path] = pq.read_metadata(io.BytesIO(data))
            except Exception:
                pass
    elif callable(write_parquet):
        with p.span("write.upload_parquet"):
            write_parquet(arrow_tbl, new_parquet_path)
        # This compatibility branch may re-encode the Arrow table, so only the
        # backend can report the resulting object size.  Do not silently record
        # zero or an unrelated local path when that mandatory metadata lookup
        # fails: the snapshot's resource metadata must remain trustworthy.
        with p.span("write.size_lookup"):
            file_size = storage.size(new_parquet_path)
    else:
        raise RuntimeError("Configured storage provides no parquet write method")

    p.add("files_written", 1)
    p.add("rows_written", int(rows))
    p.add("bytes_written", int(file_size))

    new_resources.append(
        {
            "file": new_parquet_path,
            "file_size": int(file_size),
            "rows": rows,
            "columns": columns,
        }
    )


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

# (logical row count, canonical digest, referenced immutable data-file keys).
# Stored next to an immutable cached frame so a cache hit can re-check the
# pinned snapshot contract without scanning the deletion vector again.
_TombstoneCacheSeal = Tuple[int, str, FrozenSet[str]]


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
        source: str,
) -> _TombstoneCacheSeal:
    """Build immutable cache metadata for an already validated frame."""
    digest = _checked_tombstone_expected_digest(known_digest, source=source)
    if digest is None:
        digest = _tombstone_digest_validated(df)
    referenced = frozenset(
        df.get_column(TOMBSTONE_FILE_COL).unique().to_list()
    ) if df.height else frozenset()
    return int(df.height), digest, referenced


def _validate_cached_tombstone_seal(
        seal: _TombstoneCacheSeal,
        *,
        expected_rows: Optional[int],
        expected_digest: Optional[str],
        allowed_files: Optional[Set[str]],
        source: str,
) -> None:
    """Validate a pinned snapshot against cached immutable metadata only."""
    rows, digest, referenced = seal
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
) -> int:
    """Write a Polars DataFrame to a single parquet file on the active storage.

    Minimal writer for system files (the tombstone deletion-vector) that need
    no column statistics or Hive partitioning. Returns the file size in bytes.
    """
    p = profiler or get_null_profiler()
    data = None
    wrote_exact_bytes = False
    with p.span("tombstone.encode"):
        arrow_tbl = write_df.to_arrow()
        buf = io.BytesIO()
        pq.write_table(
            arrow_tbl, buf,
            compression="zstd",
            compression_level=int(compression_level),
            use_dictionary=True,
            write_statistics=True,
            row_group_size=_PARQUET_ROW_GROUP_SIZE,
        )
        data = buf.getvalue()

    storage = _get_storage()
    write_bytes = getattr(storage, "write_bytes", None)
    write_parquet = getattr(storage, "write_parquet", None)
    if callable(write_bytes):
        # Upload failures are authoritative.  Never reinterpret a failed remote
        # PUT as a local relative-path write: that can publish a snapshot whose
        # object key was created only on the writer's local filesystem.
        write_bytes(path, data)
        wrote_exact_bytes = True
    elif callable(write_parquet):
        write_parquet(arrow_tbl, path)
    else:
        raise RuntimeError("Configured storage provides no parquet write method")
    # Fast path: the write_bytes backend (MinIO/S3/local) stored precisely
    # `data`, so return its length and skip the extra HEAD.  Only the
    # write_parquet / fallback branches (which may re-encode) consult size().
    if wrote_exact_bytes and data is not None:
        return len(data)
    try:
        return int(_get_storage().size(path))
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
# Pushdown overwrite resolution (DuckDB probe, polars fallback)
# =========================
#
# The legacy path (``filter_stale_incoming_rows`` + ``identify_deleted_rowids``)
# reads EVERY overlapping data file FULLY (all columns, all rows) into polars,
# then group/join over the whole table — cost O(table size), independent of how
# few rows are actually written.  ``resolve_overwrite_writes`` replaces both with
# ONE column-projected DuckDB ``parquet_scan`` that reads only the key /
# ``__rowid__`` / newer-than columns and only the rows whose key matches an
# incoming key (null-safe SEMI JOIN), then derives both results in-memory from
# that small matched set.  The two legacy functions are retained as the exact
# semantic oracle and the fallback for any environment/schema the probe can't
# handle.


def _storage_duckdb_path(storage, key: str, force_presign: bool = False) -> str:
    """Resolve a storage key to a path string DuckDB can read directly.

    Mirrors the read path's ``DataEstimator._to_duckdb_path``: DuckDB cannot read
    a private object-store file unless the URL is **presigned**, so when
    ``SUPERTABLE_DUCKDB_PRESIGNED`` is set (or *force_presign* is True — used by
    the probe's reactive retry after an HTTP/auth error) the bare object key is
    signed via ``storage.presign(key)``.  Otherwise object stores expose
    ``to_duckdb_path`` (→ ``s3://``/``http(s)://``); local storage has none, so
    the on-disk path is already DuckDB-readable and returned unchanged.  Anything
    already a URL passes through untouched (presign takes a key, never a URL).
    """
    if not key:
        return key

    # Proactive (or forced) presign — sign the bare object key, never a URL.
    if (force_presign or settings.SUPERTABLE_DUCKDB_PRESIGNED) and "://" not in key:
        presign_fn = getattr(storage, "presign", None)
        if callable(presign_fn):
            try:
                url = presign_fn(key)
                if isinstance(url, str) and url:
                    return url
            except Exception as e:
                logging.debug(f"[write-probe] presign failed for {key}: {e}")

    if "://" in key:
        return key
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


def _duckdb_probe_overlap_matches(
        overlap_true_files: List[Tuple[str, int]],
        overwrite_columns: List[str],
        newer_than_col: Optional[str],
        incoming_keys: polars.DataFrame,
        incoming_schema: Optional[Dict[str, polars.DataType]] = None,
        profiler: Optional[Profiler] = None,
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

    # Proactive: honours SUPERTABLE_DUCKDB_PRESIGNED exactly like the read path.
    # Identity ambiguity disables only the optional accelerator; the strict
    # storage-SDK fallback below still reads each raw resource key separately.
    try:
        duck_paths, duck_to_key = _resolve(force_presign=False)
    except Exception as e:
        logging.info(
            f"[write-probe] unsafe path identity, using polars path: {e}"
        )
        return None

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
        integrity_sql = (
            "SELECT filename COLLATE \"binary\" AS __st_file__, "
            "count(*) AS __rows__, "
            f"count({quote_if_needed(ROWID_COL)}) AS __nonnull__, "
            f"count(DISTINCT {quote_if_needed(ROWID_COL)}) AS __unique__, "
            f"min({quote_if_needed(ROWID_COL)}) AS __min__ "
            f"FROM parquet_scan([{files_sql}], union_by_name=TRUE, "
            "filename=TRUE, hive_partitioning=FALSE) "
            "GROUP BY filename COLLATE \"binary\""
        )
        with p.span("io.duckdb_probe_rowid_integrity"):
            integrity_rows = con.execute(integrity_sql).fetchall()
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
        unexpected = checked_paths.difference(paths)
        if unexpected:
            raise ValueError(
                f"mutation probe returned unrecognized source file(s): {unexpected!r}"
            )
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
            if getattr(storage, "presign", None) and any(
                tok in msg for tok in _PRESIGN_RETRY_TOKENS
            ):
                logging.warning(f"[write-probe] presign fallback after: {msg}")
                duck_paths, duck_to_key = _resolve(force_presign=True)
                configure_httpfs_and_s3(con, duck_paths)
                matched = _run(duck_paths)
            else:
                raise
    except Exception as e:
        logging.info(f"[write-probe] probe failed, using polars path: {e}")
        return None
    finally:
        if con is not None:
            # Return the connection to the thread-local pool (do NOT close it);
            # only drop the per-probe registered relation so the uuid-named
            # keys table can't accumulate across reuses.
            try:
                con.unregister(ik_name)
            except Exception:
                pass

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
) -> Tuple[polars.DataFrame, List[Tuple[str, int]]]:
    """Single-pass overwrite resolution: stale filtering + delete-vector pairs.

    Returns ``(filtered_incoming_df, delete_pairs)``.  When the DuckDB pushdown
    probe is enabled (``SUPERTABLE_DUCKDB_WRITE_PROBE``) it is computed from ONE
    probe over the overlapping files.  Falls back to the original polars
    full-read path (``filter_stale_incoming_rows`` + ``identify_deleted_rowids``)
    when the probe is disabled (the default), DuckDB is unavailable, the probe
    fails, or the file schema can't be probed — semantics are identical on both
    paths.

    *newer_than_col* falsy ⇒ no stale filtering (delete/upsert without conflict
    resolution); the incoming df is returned unchanged and every overlapping row
    matched by an incoming key is tombstoned.
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
    # The DuckDB pushdown probe is opt-in (SUPERTABLE_DUCKDB_WRITE_PROBE).  When
    # disabled (the default), skip it entirely and use the polars fallback below,
    # which reads only the projected key columns via the storage SDK and needs no
    # httpfs extension — the safe path for environments without one.
    matched = None
    if settings.SUPERTABLE_DUCKDB_WRITE_PROBE:
        matched = _duckdb_probe_overlap_matches(
            overlap_true,
            overwrite_columns,
            newer_than_col,
            incoming_keys,
            incoming_schema=dict(incoming_df.schema),
            profiler=p,
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
        )
    else:
        filtered = incoming_df
    pairs = identify_deleted_rowids(
        filtered, overlapping_files, overwrite_columns,
        file_cache=file_cache, profiler=p, read_columns=read_columns,
        required=required,
        dead_rowids_by_file=dead_rowids_by_file,
    )
    return filtered, pairs


def _partitioned_new_path(base_dir: str, alias: str) -> str:
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
        _get_storage().makedirs(part_dir)
    except Exception:
        pass
    return os.path.join(part_dir, generate_filename(alias, "parquet"))


def build_tombstone_file(
        tombstone_dir: str,
        prev_tombstone_path: Optional[str],
        new_pairs: List[Tuple[str, int]],
        compression_level: int,
        profiler: Optional[Profiler] = None,
        prev_df: Optional[polars.DataFrame] = None,
        persist: bool = True,
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
    if prev_df is not None:
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


def reclaim_fully_dead_files(
        resources: List[Dict],
        combined_dv: polars.DataFrame,
        tombstone_dir: str,
        compression_level: int,
        profiler: Optional[Profiler] = None,
        persist: bool = True,
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

    survivors = combined_dv.filter(
        ~polars.col(TOMBSTONE_FILE_COL).is_in(list(fully_dead))
    )
    if survivors.height == 0:
        return fully_dead, None, None

    if not persist:
        return fully_dead, None, survivors

    new_path = _partitioned_new_path(tombstone_dir, "deleted")
    _write_df_parquet(survivors, new_path, compression_level, profiler=p)
    p.add("reclaimed_dead_files", len(fully_dead))
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
    # Strong binding between every row-group hint and the exact Parquet footer
    # from which these rows were extracted. Legacy NULL seals disable only
    # row-group hints; file-level conservative pruning remains available.
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


def _stats_rows_for_metadata(file_path: str, md) -> List[dict]:
    """Build the per-(row_group × column) stats rows for one file's footer."""
    rows: List[dict] = []
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
        md = cache.get(path)
        if md is None:
            md = _read_footer_metadata(path, profiler=p)
        else:
            p.add("stats_footer_cache_hit", 1)
        if md is None:
            continue
        all_rows.extend(_stats_rows_for_metadata(path, md))
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
    prev_df = load_stats(prev_stats_path, allow_cache=True, profiler=p) if prev_stats_path else None
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
        return None, combined

    new_path = _partitioned_new_path(stats_dir, "stats")
    _write_df_parquet(combined, new_path, compression_level, profiler=p)
    p.add("stats_rows_total", int(combined.height))
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
) -> Optional[polars.DataFrame]:
    """Retain stats only where the per-file row-group manifest is provably whole.

    A table-level artifact row count cannot detect a missing row-group slot that
    was replaced by a duplicate row elsewhere.  Both SELECT and mutation pruning
    may use a file's stats only when row-group ids, row counts, and exact column
    slot sets agree with the independently pinned snapshot resource row count.
    Invalid/absent files disappear from the returned stats; pruning consumers
    interpret that as unknown and retain the physical file.
    """
    if stats_df is None or not isinstance(stats_df, polars.DataFrame):
        return None
    if stats_df.height == 0:
        return stats_df
    required = {"file_path", "row_group_id", "column_name", "row_group_rows"}
    if not required.issubset(stats_df.columns):
        return None
    manifest = {
        file_path: rows
        for file_path, rows in resource_rows.items()
        if isinstance(file_path, str)
        and isinstance(rows, int)
        and not isinstance(rows, bool)
        and rows >= 0
    }
    if not manifest:
        return stats_df.head(0)
    try:
        manifested = stats_df.filter(polars.col("file_path").is_in(list(manifest)))
        groups = (
            manifested.select(list(required))
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
        expected = polars.DataFrame(
            {
                "file_path": list(manifest),
                "__expected_rows": list(manifest.values()),
            },
            schema={"file_path": polars.Utf8, "__expected_rows": polars.Int64},
        )
        trusted_frame = (
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
            .join(expected, on="file_path", how="inner")
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
                & (polars.col("__total_rows") == polars.col("__expected_rows"))
            )
            .select("file_path")
        )
        if trusted_frame.height == 0:
            return stats_df.head(0)
        trusted = trusted_frame.get_column("file_path").to_list()
        if (
            manifested.height == stats_df.height
            and len(trusted) == groups.get_column("file_path").n_unique()
        ):
            return stats_df
        return manifested.filter(polars.col("file_path").is_in(trusted))
    except Exception:
        return None


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
        key = self._key(path)
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                self._bytes -= old[2]
            if byte_cap is not None and frame_bytes > byte_cap:
                return
            self._entries[key] = (path, df, frame_bytes, metadata)
            self._bytes += frame_bytes
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


def _tombstone_cache_cap() -> int:
    return int(settings.SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES)


def _tombstone_cache_byte_cap() -> int:
    configured = getattr(settings, "SUPERTABLE_TOMBSTONE_CACHE_MAX_BYTES", None)
    if not isinstance(configured, (int, str)) or isinstance(configured, bool):
        configured = os.environ.get(
            "SUPERTABLE_TOMBSTONE_CACHE_MAX_BYTES", str(256 * 1024 * 1024)
        )
    return int(configured)


_STATS_CACHE = _PathKeyedFrameCache(_stats_cache_cap)
_TOMBSTONE_CACHE = _PathKeyedFrameCache(
    _tombstone_cache_cap, _tombstone_cache_byte_cap
)


def load_stats(
        stats_path: Optional[str],
        *,
        allow_cache: bool = True,
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
    cached = _STATS_CACHE.get(stats_path)
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
        _STATS_CACHE.put(stats_path, df)
    return df


def cache_stats(stats_path: Optional[str], df: Optional[polars.DataFrame]) -> None:
    """Seed the cache with a freshly built latest-version stats frame.

    Called by writers right after :func:`build_stats_file` so the very next
    read (this process's next overwrite/delete or query) needs no storage read.
    """
    if stats_path and df is not None:
        _STATS_CACHE.put(stats_path, df)


def load_tombstone(
        tombstone_path: Optional[str],
        *,
        allow_cache: bool = True,
        required: bool = False,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        allowed_files: Optional[Set[str]] = None,
        profiler: Optional[Profiler] = None,
) -> Optional[polars.DataFrame]:
    """Load a table's deletion-vector parquet, serving the latest from memory.

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
    if not tombstone_path:
        return None
    p = profiler or get_null_profiler()
    cached_entry = _TOMBSTONE_CACHE.get_entry(tombstone_path)
    if cached_entry is not None:
        p.add("tombstone_cache_hit", 1)
        cached, metadata = cached_entry
        source = f"cached deletion-vector {tombstone_path}"
        if not (
            isinstance(metadata, tuple)
            and len(metadata) == 3
            and isinstance(metadata[0], int)
            and isinstance(metadata[1], str)
            and isinstance(metadata[2], frozenset)
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
            _TOMBSTONE_CACHE.put(tombstone_path, cached, metadata)
        _validate_cached_tombstone_seal(
            metadata,
            expected_rows=expected_rows,
            expected_digest=expected_digest,
            allowed_files=allowed_files,
            source=source,
        )
        return cached
    p.add("tombstone_cache_miss", 1)
    df = _read_parquet_safe(tombstone_path, profiler=p, required=required)
    if df is not None:
        df = validate_tombstone_frame(
            df,
            expected_rows=expected_rows,
            expected_digest=expected_digest,
            allowed_files=allowed_files,
            source=f"deletion-vector {tombstone_path}",
        )
    if df is not None and allow_cache:
        seal = _tombstone_cache_seal(
            df,
            known_digest=expected_digest,
            source=f"deletion-vector cache entry {tombstone_path}",
        )
        _TOMBSTONE_CACHE.put(tombstone_path, df, seal)
    return df


def cache_tombstone(
        tombstone_path: Optional[str],
        df: Optional[polars.DataFrame],
        *,
        expected_rows: Optional[int] = None,
        expected_digest: Optional[str] = None,
        assume_valid: bool = False,
) -> None:
    """Seed the cache with a freshly built latest-version deletion-vector frame.

    Called by writers right after the tombstone pointer is finalised so the next
    write's carry-forward read (this process's next overwrite/delete) needs no
    storage round-trip.  No-op when the vector was fully consumed this write
    (``tombstone_path`` is ``None``) or unchanged with no frame in hand.
    """
    if tombstone_path and df is not None:
        source = f"deletion-vector cache seed {tombstone_path}"
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
            validated = validate_tombstone_frame(
                df,
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                source=source,
            )
            checked_digest = expected_digest
        seal = _tombstone_cache_seal(
            validated, known_digest=checked_digest, source=source,
        )
        _TOMBSTONE_CACHE.put(tombstone_path, validated, seal)


def evict_tombstone(tombstone_path: Optional[str]) -> None:
    """Remove a superseded/currently-drained deletion-vector cache entry."""
    if tombstone_path:
        _TOMBSTONE_CACHE.discard(tombstone_path)


def compact_tombstones(
        snapshot: dict,
        tombstone_df: polars.DataFrame,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
        return_residual: bool = False,
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
        resource = by_path.get(file_path)
        if not resource:
            # Do not discard a ghost entry: without its referenced physical file
            # we cannot prove that the deletion was consumed.  Keeping the whole
            # group residual is the only conservative action available here.
            return 0, [], None, file_tombstones, Profiler()
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
                return projected.height, [], file_path, None, sub
            # Count agreed but identities did not: metadata/DV corruption. Do
            # not rewrite or discard any part of this group.
            return 0, [], None, file_tombstones, sub

        # required=True: this is the only physical drain.  A transient backend
        # error or NotFound must abort with the prior snapshot + vector intact;
        # treating either as an empty source would permit pointer clearing and
        # row resurrection.
        existing_df = _read_parquet_safe(
            file_path, profiler=sub, file_size=file_size, required=True
        )
        if existing_df is None:  # defensive; required=True normally raises
            return 0, [], None, file_tombstones, sub
        _validate_physical(existing_df, file_path)
        # A partial match cannot be safely rewritten while retaining only the
        # unmatched entries: doing so would move survivors to a new file while
        # the residual still names the sunset source.  Retain the whole group
        # and retry after the metadata inconsistency is repaired.
        unmatched = dead_ids.join(
            existing_df.select(ROWID_COL), on=ROWID_COL, how="anti"
        )
        if unmatched.height > 0:
            return 0, [], None, file_tombstones, sub
        with sub.span("tombstone.anti_join"):
            kept_df = existing_df.join(dead_ids, on=ROWID_COL, how="anti")
        difference = existing_df.height - kept_df.height
        if difference == 0:
            return 0, [], None, file_tombstones, sub

        local_resources = []
        sub.add("tombstone_files_touched", 1)

        if kept_df.height > 0:
            with sub.span("tombstone.write_kept"):
                write_parquet_and_collect_resources(
                    write_df=kept_df,
                    overwrite_columns=[],
                    data_dir=data_dir,
                    new_resources=local_resources,
                    compression_level=compression_level,
                    profiler=sub,
                )
        return difference, local_resources, file_path, None, sub

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
    if workers == 1:
        results = map(_drain_group, items)
    else:
        executor = ThreadPoolExecutor(max_workers=workers)
        results = executor.map(_drain_group, items)
    try:
        for difference, local_resources, sunset, residual_part, sub in results:
            p.merge(sub)
            removed += difference
            new_resources.extend(local_resources)
            if sunset:
                sunset_files.add(sunset)
            if residual_part is not None:
                residual_parts.append(residual_part)
    finally:
        if workers > 1:
            executor.shutdown(wait=True, cancel_futures=True)

    residual = (
        polars.concat(residual_parts, how="vertical")
        if residual_parts else _empty_tombstone_df()
    )
    residual = validate_tombstone_frame(
        residual, source="residual deletion-vector after compaction"
    )
    if return_residual:
        return removed, new_resources, sunset_files, residual
    return removed, new_resources, sunset_files
