# processing.py

import decimal
import logging
import os
import io
import time
import threading
import uuid
from collections import OrderedDict
from datetime import datetime, date, timezone
from typing import Dict, List, Set, Tuple, Optional

import polars
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.utils.helper import generate_filename
from supertable.config.defaults import default
from supertable.config.settings import settings
from supertable.storage.storage_factory import get_storage
from supertable.utils.profiler import Profiler, get_null_profiler
from supertable.data_classes import PredInterval

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
      - present in *df* with a different dtype → cast (strict=False, so unconvertible values become null)
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
                exprs.append(polars.col(col).cast(dtype, strict=False))
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

    When *required* is True a genuine read failure — the object exists but cannot
    be read (corrupt body, transient/persistent backend error) — is re-raised
    instead of being swallowed to ``None``.  Absence still returns ``None`` even
    when required (a missing object, or one sunset by a concurrent writer, is a
    legitimate "no previous artifact" signal).  Carry-forward callers that would
    otherwise silently drop a still-referenced artifact — the deletion-vector —
    must pass ``required=True`` so a failed read aborts the write rather than
    persisting a truncated successor (which would resurrect deleted rows).
    """
    p = profiler or get_null_profiler()
    if not _safe_exists(path, profiler=p, strict=required):
        logging.info(f"[race] file already sunset by another writer: {path}")
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

    for file_path, file_size in candidates:
        existing_df = _read_parquet_safe(file_path)
        if existing_df is None:
            # Race: another writer already sunset this file. Skip and
            # leave it out of sunset_files — the snapshot still
            # references it; the next compaction will retry.
            continue

        # Physically drop logically-deleted rows when a deletion-vector
        # is supplied (export bakes the vector into the copy).
        if dead_rowids and ROWID_COL in existing_df.columns:
            existing_df = existing_df.filter(
                ~polars.col(ROWID_COL).is_in(list(dead_rowids))
            )

        if chunk_df is None or chunk_df.height == 0:
            # Seed the buffer with the first survivor. Using
            # ``concat_with_union`` even for the seed keeps the schema
            # behaviour identical to the merge path (no column drop on
            # the first file).
            chunk_df = (
                existing_df if chunk_df is None
                else concat_with_union(chunk_df, existing_df)
            )
        else:
            chunk_df = concat_with_union(chunk_df, existing_df)

        sunset_files.add(file_path)
        chunk_size_bytes += int(file_size or 0)

        # Flush when the buffered chunk exceeds the per-table memory cap.
        if chunk_size_bytes >= max_mem:
            total_rows += chunk_df.shape[0]
            # No overwrite columns for plain compaction — we don't need to pin
            # a specific key for the resulting file.
            write_parquet_and_collect_resources(
                write_df=chunk_df,
                overwrite_columns=[],
                data_dir=data_dir,
                new_resources=new_resources,
                compression_level=compression_level,
            )
            chunk_df = None
            chunk_size_bytes = 0

    # Final flush — if anything remains in the buffer, write it out.
    if chunk_df is not None and chunk_df.height > 0:
        total_rows += chunk_df.shape[0]
        write_parquet_and_collect_resources(
            write_df=chunk_df,
            overwrite_columns=[],
            data_dir=data_dir,
            new_resources=new_resources,
            compression_level=compression_level,
        )

    return len(sunset_files), total_rows, new_resources, sunset_files


# =========================
# Write helpers
# =========================

def write_parquet_and_collect_resources(
        write_df, overwrite_columns, data_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
):
    """Write a DataFrame as one or more Parquet files and append resource dicts.

    Partitioning strategy:
      - If ``__timestamp__`` is present and non-null, rows are split by
        year/month/day and each partition is written into a Hive-style
        subdirectory: ``data_dir/year=YYYY/month=MM/day=DD/``.
      - Rows where ``__timestamp__`` is null (if any) are written to the
        flat ``data_dir/`` for safety.
      - If ``__timestamp__`` is absent (table has no dedup-on-read), the
        entire DataFrame is written to the flat ``data_dir/`` as before.

    The partition columns are derived from the path only — they are NOT
    stored as extra columns inside the Parquet file.  This keeps full
    backward compatibility: the resource ``file`` path is still the only
    thing the read side needs.
    """
    if write_df.height == 0:
        return

    # --- Partitioned write path: split by __timestamp__ day ------------
    if "__timestamp__" in write_df.columns:
        # Add temporary partition-key columns derived from __timestamp__
        partitioned = write_df.with_columns([
            polars.col("__timestamp__").dt.year().alias("__p_year__"),
            polars.col("__timestamp__").dt.month().alias("__p_month__"),
            polars.col("__timestamp__").dt.day().alias("__p_day__"),
        ])

        # Separate rows with null timestamps (defensive — shouldn't happen
        # in normal flow because data_writer always injects a non-null value)
        null_mask = (
            polars.col("__p_year__").is_null()
            | polars.col("__p_month__").is_null()
            | polars.col("__p_day__").is_null()
        )
        has_nulls = partitioned.filter(null_mask).height > 0

        if has_nulls:
            null_df = partitioned.filter(null_mask).drop(["__p_year__", "__p_month__", "__p_day__"])
            _write_single_parquet_file(null_df, overwrite_columns, data_dir, new_resources, compression_level, profiler=profiler)
            partitioned = partitioned.filter(~null_mask)

        if partitioned.height > 0:
            # partition_by returns a list of DataFrames, one per unique (year, month, day)
            groups = partitioned.partition_by(["__p_year__", "__p_month__", "__p_day__"], maintain_order=False)
            for group_df in groups:
                year = int(group_df["__p_year__"][0])
                month = int(group_df["__p_month__"][0])
                day = int(group_df["__p_day__"][0])
                group_df = group_df.drop(["__p_year__", "__p_month__", "__p_day__"])

                partition_dir = os.path.join(
                    data_dir,
                    f"year={year}",
                    f"month={month:02d}",
                    f"day={day:02d}",
                )
                _write_single_parquet_file(
                    group_df, overwrite_columns, partition_dir, new_resources, compression_level,
                    profiler=profiler,
                )
    else:
        # --- Flat write path (no __timestamp__) — backward compatible ---
        _write_single_parquet_file(write_df, overwrite_columns, data_dir, new_resources, compression_level, profiler=profiler)


def _write_single_parquet_file(
        write_df, overwrite_columns, target_dir, new_resources, compression_level=10,
        profiler: Optional[Profiler] = None,
):
    """Write a single Parquet file into *target_dir* and append a resource entry.

    This is the low-level writer extracted from the original
    ``write_parquet_and_collect_resources``.  All Parquet encoding settings
    (zstd, dictionary, row-group size, statistics) are unchanged.
    """
    p = profiler or get_null_profiler()
    rows = write_df.shape[0]
    columns = write_df.shape[1]

    # Ensure the target directory exists.  makedirs is idempotent on local
    # storage and a no-op on object storage; calling it directly avoids a
    # pointless prefix HEAD (which always 404s) on object stores.
    with p.span("write.ensure_dir"):
        try:
            _get_storage().makedirs(target_dir)
        except Exception:
            pass

    new_parquet_file = generate_filename("data", "parquet")
    new_parquet_path = os.path.join(target_dir, new_parquet_file)

    # Sort before writing so each row group covers a tight min/max range.
    # DuckDB uses these zonemaps to skip entire row groups during filtered scans.
    sort_cols = (
        (["__timestamp__"] if "__timestamp__" in write_df.columns else [])
        + [c for c in (overwrite_columns or []) if c in write_df.columns and c != "__timestamp__"]
    )
    if sort_cols and write_df.height > 0:
        with p.span("write.sort"):
            write_df = write_df.sort(sort_cols)

    # Write to the active storage backend
    try:
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

        if hasattr(_get_storage(), "write_bytes"):
            with p.span("write.upload_bytes"):
                _get_storage().write_bytes(new_parquet_path, data)
        elif hasattr(_get_storage(), "write_parquet"):
            with p.span("write.upload_parquet"):
                _get_storage().write_parquet(arrow_tbl, new_parquet_path)
        else:
            with p.span("write.local_fallback"):
                write_df.write_parquet(
                    file=new_parquet_path,
                    compression="zstd",
                    compression_level=int(compression_level),
                    statistics=True,
                    row_group_size=_PARQUET_ROW_GROUP_SIZE,
                )
    except Exception:
        with p.span("write.local_fallback"):
            write_df.write_parquet(
                file=new_parquet_path,
                compression="zstd",
                compression_level=int(compression_level),
                statistics=True,
                row_group_size=_PARQUET_ROW_GROUP_SIZE,
            )

    # Determine file size
    try:
        with p.span("write.size_lookup"):
            file_size = _get_storage().size(new_parquet_path)
    except Exception:
        try:
            file_size = os.path.getsize(new_parquet_path)
        except Exception:
            try:
                file_size = len(data)  # type: ignore[name-defined]
            except Exception:
                file_size = 0

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
        part = _read_parquet_safe(file_path, profiler=p, file_size=file_size, columns=read_columns)
        if part is None:
            continue
        # Cache the full DataFrame for downstream reuse (avoids double-read)
        if file_cache is not None:
            file_cache[file_path] = part
        # If the file doesn't have the newer_than column, skip it (legacy data → allow overwrite)
        if newer_than_col not in part.columns:
            continue
        # Select only the columns we need, filtering to matching keys
        available_cols = [c for c in needed_cols if c in part.columns]
        if not all(c in available_cols for c in overwrite_columns):
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

    # Left join incoming against existing max
    with p.span("newer_than.join_filter"):
        joined = incoming_df.join(existing_max, on=overwrite_columns, how="left")

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
    try:
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
        if hasattr(_get_storage(), "write_bytes"):
            _get_storage().write_bytes(path, data)
        elif hasattr(_get_storage(), "write_parquet"):
            _get_storage().write_parquet(arrow_tbl, path)
        else:
            write_df.write_parquet(file=path, compression="zstd", compression_level=int(compression_level), statistics=True, row_group_size=_PARQUET_ROW_GROUP_SIZE)
    except Exception:
        write_df.write_parquet(file=path, compression="zstd", compression_level=int(compression_level), statistics=True, row_group_size=_PARQUET_ROW_GROUP_SIZE)
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
            existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size, columns=read_columns)
        if existing_df is None:
            continue
        if ROWID_COL not in existing_df.columns:
            continue
        if not all(c in existing_df.columns for c in overwrite_columns):
            continue

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
                file, profiler=p, file_size=file_size, columns=[ROWID_COL]
            )
        if existing_df is None or ROWID_COL not in existing_df.columns:
            continue
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

    try:
        import duckdb  # noqa: F401  (imported for availability check / errors)
        from supertable.engine.engine_common import (
            new_duckdb_connection,
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
        for k in file_keys:
            dp = _storage_duckdb_path(storage, k, force_presign=force_presign)
            d2k[dp] = k
            paths.append(dp)
        return paths, d2k

    # Proactive: honours SUPERTABLE_DUCKDB_PRESIGNED exactly like the read path.
    duck_paths, duck_to_key = _resolve(force_presign=False)

    select_cols = ["filename", quote_if_needed(ROWID_COL)]
    select_cols += [quote_if_needed(c) for c in overwrite_columns]
    if newer_than_col:
        select_cols.append(quote_if_needed(newer_than_col))
    join_cond = " AND ".join(
        f"src.{quote_if_needed(c)} IS NOT DISTINCT FROM k.{quote_if_needed(c)}"
        for c in overwrite_columns
    )
    ik_name = f"__st_ik_{uuid.uuid4().hex}"

    def _run(paths):
        files_sql = ", ".join(f"'{escape_parquet_path(dp)}'" for dp in paths)
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
        # Build the connection exactly like the read path (same pragmas, and a
        # pinned home_directory) so the probe never falls back to the OS home —
        # which is absent under a restricted service user.  httpfs/S3 is loaded
        # by the helper only when duck_paths contain a remote URL.
        con = new_duckdb_connection(temp_dir="write_probe", for_paths=duck_paths)
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
            try:
                con.unregister(ik_name)
            except Exception:
                pass
            try:
                con.close()
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
            joined = incoming_df.join(existing_max, on=overwrite_columns, how="left")
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
) -> Tuple[polars.DataFrame, List[Tuple[str, int]]]:
    """Single-pass overwrite resolution: stale filtering + delete-vector pairs.

    Returns ``(filtered_incoming_df, delete_pairs)`` computed from ONE DuckDB
    pushdown probe over the overlapping files.  Falls back to the original polars
    full-read path (``filter_stale_incoming_rows`` + ``identify_deleted_rowids``)
    when DuckDB is unavailable, the probe fails, or the file schema can't be
    probed — semantics are identical on both paths.

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
    matched = _duckdb_probe_overlap_matches(
        overlap_true, overwrite_columns, newer_than_col, incoming_keys, profiler=p,
    )
    if matched is not None:
        try:
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
    if newer_than_col:
        filtered = filter_stale_incoming_rows(
            incoming_df=incoming_df,
            overlapping_files=overlapping_files,
            overwrite_columns=overwrite_columns,
            newer_than_col=newer_than_col,
            file_cache=file_cache,
            profiler=p,
            read_columns=read_columns,
        )
    else:
        filtered = incoming_df
    pairs = identify_deleted_rowids(
        filtered, overlapping_files, overwrite_columns,
        file_cache=file_cache, profiler=p, read_columns=read_columns,
    )
    return filtered, pairs


def build_tombstone_file(
        tombstone_dir: str,
        prev_tombstone_path: Optional[str],
        new_pairs: List[Tuple[str, int]],
        compression_level: int,
        profiler: Optional[Profiler] = None,
        prev_df: Optional[polars.DataFrame] = None,
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
    if prev_df is not None and prev_df.height > 0 and ROWID_COL in prev_df.columns:
        combined = polars.concat(
            [prev_df.select([TOMBSTONE_FILE_COL, ROWID_COL]), new_df],
            how="vertical",
        )
    else:
        combined = new_df

    combined = combined.unique(subset=[ROWID_COL], keep="first")

    try:
        # Direct makedirs (idempotent local, no-op object) — avoids a 404 prefix HEAD.
        _get_storage().makedirs(tombstone_dir)
    except Exception:
        pass

    new_path = os.path.join(tombstone_dir, generate_filename("deleted", "parquet"))
    _write_df_parquet(combined, new_path, compression_level, profiler=p)
    return new_path, combined


def reclaim_fully_dead_files(
        resources: List[Dict],
        combined_dv: polars.DataFrame,
        tombstone_dir: str,
        compression_level: int,
        profiler: Optional[Profiler] = None,
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

    dead_counts = (
        combined_dv.group_by(TOMBSTONE_FILE_COL).agg(polars.len().alias("__dead__"))
    )
    dead_map = {f: int(n) for f, n in dead_counts.iter_rows()}

    fully_dead: Set[str] = set()
    for r in resources:
        if not isinstance(r, dict):
            continue
        f = r.get("file")
        rows = int(r.get("rows") or 0)
        if f and rows > 0 and dead_map.get(f, 0) >= rows:
            fully_dead.add(f)

    if not fully_dead:
        return set(), None, None

    survivors = combined_dv.filter(
        ~polars.col(TOMBSTONE_FILE_COL).is_in(list(fully_dead))
    )
    if survivors.height == 0:
        return fully_dead, None, None

    try:
        # Direct makedirs (idempotent local, no-op object) — avoids a 404 prefix HEAD.
        _get_storage().makedirs(tombstone_dir)
    except Exception:
        pass

    new_path = os.path.join(tombstone_dir, generate_filename("deleted", "parquet"))
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
    "stats_available": polars.Boolean,
    "min_is_exact": polars.Boolean,
    "max_is_exact": polars.Boolean,
}

STATS_FILE_PATH_COL = "file_path"

# Internal system columns never emitted into the stats artifact (they must not
# leak, same as everywhere else).
_STATS_SYSTEM_COLUMNS = {ROWID_COL, TIMESTAMP_COL}


def _logical_type_name(stat) -> str:
    """Return the parquet logical type name (e.g. ``TIMESTAMP``/``STRING``) or ``""``."""
    try:
        lt = stat.logical_type
        if lt is None:
            return ""
        name = getattr(lt, "type", None)
        if name is None or str(name).upper() == "NONE":
            return ""
        return str(name)
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
        return "bigint", int(mn), int(mx)
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


def _stats_rows_for_metadata(file_path: str, md) -> List[dict]:
    """Build the per-(row_group × column) stats rows for one file's footer."""
    rows: List[dict] = []
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        rg_rows = int(g.num_rows)
        for c in range(g.num_columns):
            col = g.column(c)
            name = col.path_in_schema
            if name in _STATS_SYSTEM_COLUMNS:
                continue
            stat = col.statistics if col.is_stats_set else None
            row = {k: None for k in STATS_SCHEMA}
            row["file_path"] = file_path
            row["row_group_id"] = int(rg)
            row["column_name"] = name
            row["physical_type"] = str(col.physical_type or "")
            row["logical_type"] = _logical_type_name(stat) if stat is not None else ""
            row["null_count"] = (
                int(stat.null_count)
                if stat is not None and stat.null_count is not None
                else 0
            )
            row["row_group_rows"] = rg_rows
            # We never truncate footer stats, so min/max are always exact.
            row["min_is_exact"] = True
            row["max_is_exact"] = True
            row["stats_available"] = False

            if stat is not None and stat.has_min_max:
                category, mn, mx = _route_stats(stat)
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


def extract_stats_rows(
        file_paths: List[str],
        profiler: Optional[Profiler] = None,
) -> polars.DataFrame:
    """Read the footers of *file_paths* and return their stats rows.

    One row per (file × row_group × column), excluding the internal
    ``__rowid__`` / ``__timestamp__`` columns.  Files whose footer cannot be
    read (race / corruption) are skipped.  Returns a frame with ``STATS_SCHEMA``
    (possibly empty).
    """
    p = profiler or get_null_profiler()
    all_rows: List[dict] = []
    for path in file_paths:
        if not path:
            continue
        md = _read_footer_metadata(path, profiler=p)
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

    prev_df = _read_parquet_safe(prev_stats_path, profiler=p) if prev_stats_path else None
    if prev_df is not None and prev_df.height > 0 and STATS_FILE_PATH_COL in prev_df.columns:
        kept_prev = prev_df.select(list(STATS_SCHEMA.keys()))
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

    try:
        # Direct makedirs (idempotent local, no-op object) — avoids a 404 prefix HEAD.
        _get_storage().makedirs(stats_dir)
    except Exception:
        pass

    new_path = os.path.join(stats_dir, generate_filename("stats", "parquet"))
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
        return "timestamp"
    if dtype == polars.Utf8:
        return "string"
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
        lane = _probe_lane_for_dtype(col.dtype)
        if lane is None:
            out[name] = None
            continue
        if col.null_count() > 0:
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
        out[name] = (lane, bounds[0], bounds[1])
    return out


def _stored_lane(row: dict) -> Optional[Tuple[str, object, object]]:
    """Read a stored stats row's typed range as ``(lane, min, max)``.

    Returns ``None`` when the row carries no usable range (``stats_available``
    False, or no lane populated) — meaning the file/row-group can't be excluded
    on that column.
    """
    if not row.get("stats_available"):
        return None
    if row.get("min_bigint") is not None and row.get("max_bigint") is not None:
        return "bigint", row["min_bigint"], row["max_bigint"]
    if row.get("min_double") is not None and row.get("max_double") is not None:
        return "double", row["min_double"], row["max_double"]
    if row.get("min_timestamp") is not None and row.get("max_timestamp") is not None:
        return "timestamp", row["min_timestamp"], row["max_timestamp"]
    if row.get("min_string") is not None and row.get("max_string") is not None:
        return "string", row["min_string"], row["max_string"]
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
    # index: file_path -> row_group_id -> column_name -> (lane,min,max)|None
    index: Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]] = {}
    for row in needed.iter_rows(named=True):
        fp = row["file_path"]
        rg = row["row_group_id"]
        col = row["column_name"]
        index.setdefault(fp, {}).setdefault(rg, {})[col] = _stored_lane(row)

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
            for col, (lane, lo, hi) in constraints.items():
                stored = cols.get(col)
                if stored is None:
                    continue  # missing / unavailable stat → can't exclude
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
    if p_lane == "numeric" and s_lane in ("bigint", "double"):
        smin, smax = float(s_min), float(s_max)
        plo = None if pred.lo is None else float(pred.lo)
        phi = None if pred.hi is None else float(pred.hi)
    elif p_lane == "timestamp" and s_lane == "timestamp":
        smin, smax, plo, phi = s_min, s_max, pred.lo, pred.hi
    elif p_lane == "string" and s_lane == "string":
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

    constrained_cols = sorted({c for occ in occurrences for c in occ})
    needed = stored_stats_df.filter(polars.col("column_name").is_in(constrained_cols))
    index: Dict[str, Dict[int, Dict[str, Optional[Tuple[str, object, object]]]]] = {}
    for row in needed.iter_rows(named=True):
        fp = row["file_path"]
        rg = row["row_group_id"]
        col = row["column_name"]
        index.setdefault(fp, {}).setdefault(rg, {})[col] = _stored_lane(row)

    kept: List[str] = []
    pruned = 0
    for fk in file_keys:
        rgs = index.get(fk)
        if not rgs:
            kept.append(fk)  # no stats for this file → cannot prove absence
            continue
        if all(_occurrence_excludes_file(occ, rgs) for occ in occurrences):
            pruned += 1
        else:
            kept.append(fk)

    # Never empty a table's file list — pruning is an optimisation, and the
    # estimator treats zero files as an error.  Retain all if we pruned all.
    if not kept:
        return file_keys
    p.add("read_pruned_files", pruned)
    return kept


# ===========================================================================
# Stats artifact accessor + in-process cache
# ---------------------------------------------------------------------------
# The stats parquet is read on every overwrite/delete (write-path pruning) and
# on every filtered read (query estimation).  Re-reading it from object storage
# each time would defeat the point of having one consolidated artifact, so we
# keep the *latest* version of each table's stats in memory.
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

class _StatsCache:
    """Process-wide LRU of each table's latest stats frame (one per table)."""

    __slots__ = ("_lock", "_entries")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # table_key -> (stats_path, DataFrame)
        self._entries: "OrderedDict[str, Tuple[str, polars.DataFrame]]" = OrderedDict()

    @staticmethod
    def _key(stats_path: str) -> str:
        # All versions of a table's stats live in the same stats/ directory.
        return os.path.dirname(stats_path)

    @staticmethod
    def _cap() -> int:
        try:
            return int(settings.SUPERTABLE_STATS_CACHE_MAX_TABLES)
        except Exception:
            return 64

    def get(self, stats_path: str) -> Optional[polars.DataFrame]:
        """Return the cached frame iff the cached path matches *exactly*."""
        if self._cap() <= 0:
            return None
        key = self._key(stats_path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry[0] == stats_path:
                self._entries.move_to_end(key)
                return entry[1]
        return None

    def put(self, stats_path: str, df: polars.DataFrame) -> None:
        cap = self._cap()
        if cap <= 0:
            return
        key = self._key(stats_path)
        with self._lock:
            self._entries[key] = (stats_path, df)
            self._entries.move_to_end(key)
            while len(self._entries) > cap:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_STATS_CACHE = _StatsCache()


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


def compact_tombstones(
        snapshot: dict,
        tombstone_df: polars.DataFrame,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
) -> Tuple[int, List[Dict], Set[str]]:
    """Physically drop tombstoned rows from the data files that hold them.

    *tombstone_df* is the deletion-vector (columns ``__file__`` + ``__rowid__``).
    Only the data files named in ``__file__`` are read and rewritten — a
    targeted compaction. For each, rows whose ``__rowid__`` is in the vector
    are anti-joined out and the survivors written to a new file; the original
    is sunset. Survivors keep their original ``__rowid__`` (no remapping).

    After this call the caller clears the tombstone pointer: the dead rows no
    longer exist on disk, so the deletion-vector is fully consumed.

    Returns ``(removed_rows, new_resources, sunset_files)``.
    """
    p = profiler or get_null_profiler()
    if tombstone_df is None or tombstone_df.height == 0:
        return 0, [], set()

    resources = snapshot.get("resources") or []
    by_path = {r.get("file"): r for r in resources if r.get("file")}

    removed = 0
    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()

    files_with_deletes = (
        tombstone_df.select(TOMBSTONE_FILE_COL).unique().get_column(TOMBSTONE_FILE_COL).to_list()
    )
    p.add("tombstone_files_total", len(files_with_deletes))

    for file_path in files_with_deletes:
        resource = by_path.get(file_path)
        if not resource:
            # File already sunset by an earlier compaction — skip.
            continue
        file_size = int(resource.get("file_size") or 0)
        # required=True: this is the ONLY physical drain (Phase B is row-preserving
        # and never re-drops these rows), and the callers clear the deletion-vector
        # pointer unconditionally once the vector was non-empty.  If a transient
        # backend error here were swallowed to None, this file's tombstoned rows
        # would be silently skipped yet the pointer cleared -> the rows RESURRECT on
        # read.  Failing loud aborts the write/compact with the prior snapshot +
        # vector intact for retry (matches the carry-forward DV-pointer reads).  A
        # genuine absence still returns None (file already sunset/raced -> its rows
        # are gone, so skipping it is correct).
        existing_df = _read_parquet_safe(
            file_path, profiler=p, file_size=file_size, required=True
        )
        if existing_df is None or ROWID_COL not in existing_df.columns:
            continue

        dead_ids = (
            tombstone_df.filter(polars.col(TOMBSTONE_FILE_COL) == file_path)
            .select(ROWID_COL)
            .unique()
        )
        with p.span("tombstone.anti_join"):
            kept_df = existing_df.join(dead_ids, on=ROWID_COL, how="anti")
        difference = existing_df.height - kept_df.height
        if difference == 0:
            continue

        removed += difference
        sunset_files.add(file_path)
        p.add("tombstone_files_touched", 1)

        if kept_df.height > 0:
            with p.span("tombstone.write_kept"):
                write_parquet_and_collect_resources(
                    write_df=kept_df,
                    overwrite_columns=[],
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                )

    return removed, new_resources, sunset_files