# processing.py

import logging
import os
import io
import time
from datetime import datetime, date
from typing import Dict, List, Set, Tuple, Optional

import polars
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.utils.helper import generate_filename, collect_schema
from supertable.config.defaults import default
from supertable.storage.storage_factory import get_storage
from supertable.utils.profiler import Profiler, get_null_profiler

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

def _safe_exists(path: str, profiler: Optional[Profiler] = None) -> bool:
    p = profiler or get_null_profiler()
    try:
        with p.span("io.exists"):
            return _get_storage().exists(path)
    except Exception:
        return False


def _read_parquet_safe(
        path: str,
        profiler: Optional[Profiler] = None,
        file_size: int = 0,
) -> Optional[polars.DataFrame]:
    p = profiler or get_null_profiler()
    if not _safe_exists(path, profiler=p):
        logging.info(f"[race] file already sunset by another writer: {path}")
        return None
    try:
        with p.span("io.read_parquet"):
            tbl = _get_storage().read_parquet(path)  # -> pyarrow.Table
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
        return None


# =========================
# Idempotency: filter stale incoming rows
# =========================


def filter_stale_incoming_rows(
        incoming_df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        idempotent_check_col: str,
) -> polars.DataFrame:
    """
    Remove incoming rows whose key already exists in storage with an equal or
    newer value in the ``idempotent_check_col`` column.

    Only files marked ``has_overlap=True`` are inspected (they are the only ones
    that can contain matching keys).

    Rules:
      - existing >= incoming  →  drop the incoming row  (stale / replay)
      - existing < incoming   →  keep the incoming row   (genuine update)
      - key not found         →  keep the incoming row   (new data)
      - idempotent_check_col missing in existing file →  keep the incoming row
    """
    # Collect existing rows from overlapping files, only the columns we need
    needed_cols = overwrite_columns + [idempotent_check_col]
    existing_parts: List[polars.DataFrame] = []

    for file, has_overlap, _ in overlapping_files:
        if not has_overlap:
            continue
        df_existing = _read_parquet_safe(file)
        if df_existing is None:
            continue
        # If the idempotent check column is missing from this file, skip it
        # (rule: missing column → allow incoming rows through)
        if idempotent_check_col not in df_existing.columns:
            continue
        # Select only columns we need that exist in the file
        select_cols = [c for c in needed_cols if c in df_existing.columns]
        existing_parts.append(df_existing.select(select_cols))

    if not existing_parts:
        # No existing data with the idempotent column → everything is new
        return incoming_df

    existing_keys_df = polars.concat(existing_parts, how="vertical_relaxed")

    # For each key combination, find the max idempotent_check value in existing data
    existing_max = (
        existing_keys_df
        .group_by(overwrite_columns)
        .agg(polars.col(idempotent_check_col).max().alias("__existing_max__"))
    )

    # Left join incoming against existing max values
    incoming_with_max = incoming_df.join(
        existing_max,
        on=overwrite_columns,
        how="left",
    )

    # Keep rows where:
    #   - no existing max found (new key) → __existing_max__ is null
    #   - incoming value > existing max   (genuine update)
    filtered = incoming_with_max.filter(
        polars.col("__existing_max__").is_null()
        | (polars.col(idempotent_check_col) > polars.col("__existing_max__"))
    ).drop("__existing_max__")

    return filtered


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
        When the gate opens, ALL false items are included (the downstream
        process_files_without_overlap handles chunked flushing at memory
        boundaries, so we must not drop files here).

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
      - has_overlap=True for files whose stats indicate key overlap (or missing stats)
      - has_overlap=False for small, non-overlapping files (< MAX_MEMORY_CHUNK_SIZE)
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
        # ORIGINAL APPROACH - much faster
        with p.span("overlap.schema"):
            new_schema = collect_schema(df)
        new_data_columns: Dict[str, List] = {}
        with p.span("overlap.unique_values"):
            for col in overwrite_columns:
                if col in df.columns:
                    # Use original unique values approach - it's actually faster for most cases
                    unique_values = df[col].unique().to_list()
                    new_data_columns[col] = unique_values

        t0 = time.perf_counter()
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            stats = resource.get("stats")

            if stats:
                # Check overlap per overwrite column
                overlapped = False
                for col in overwrite_columns:
                    if col not in stats:
                        overlapped = True
                        break

                    col_stats = stats[col]
                    min_val = col_stats.get("min")
                    max_val = col_stats.get("max")
                    new_vals = new_data_columns.get(col, [])

                    if min_val is None or max_val is None:
                        overlapped = True
                        break

                    # Normalize to types if needed
                    if col in new_schema and new_schema[col] == "Date":
                        if isinstance(min_val, str):
                            min_val = datetime.fromisoformat(min_val).date()
                        if isinstance(max_val, str):
                            max_val = datetime.fromisoformat(max_val).date()
                    elif col in new_schema and new_schema[col] == "DateTime":
                        if isinstance(min_val, str):
                            min_val = datetime.fromisoformat(min_val)
                        if isinstance(max_val, str):
                            max_val = datetime.fromisoformat(max_val)

                    if any(val is None for val in new_vals):
                        overlapped = True
                        break

                    if any(min_val <= val <= max_val for val in new_vals if val is not None):
                        overlapped = True
                        break

                if overlapped:
                    overlapping_files.add((file, True, file_size))
                else:
                    # non-overlapping small files can be considered for compaction
                    _max_mem, _ = _resolve_limits(table_config)
                    if (file_size < _max_mem) and not is_file_in_overlapping_files(file,
                                                                                   overlapping_files):
                        overlapping_files.add((file, False, file_size))
            else:
                # Missing stats → treat as overlapping (be conservative)
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
# Public API: Processing (merge & rewrite)
# =========================

def process_overlapping_files(
        df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        data_dir: str,
        compression_level: int,
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
):
    """
    Merge implementation:
      - For has_overlap=False entries, batch-read & append (compaction)
      - For has_overlap=True entries, read existing file, drop rows being overwritten, append the remainder
      - Periodically flush chunks if they get too big
      - Write any remainder at the end

    Limits are resolved per-table (table_config) with fallback to global default.
    """
    p = profiler or get_null_profiler()
    inserted = df.shape[0]
    deleted = 0
    total_columns = df.shape[1]
    total_rows = 0

    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()

    # Base schema/empty chunk
    schema = df.schema
    empty_df = polars.DataFrame(schema=schema)

    # Phase 1: pull in non-overlapping (False) as compaction chunks
    with p.span("process.phase1_compact"):
        chunk_df = process_files_without_overlap(
            empty_df=empty_df,
            data_dir=data_dir,
            new_resources=new_resources,
            overlapping_files=overlapping_files,
            overwrite_columns=overwrite_columns,
            sunset_files=sunset_files,
            compression_level=compression_level,
            table_config=table_config,
            profiler=p,
        )

    # Start merged with compaction chunk + incoming df
    with p.span("process.initial_concat"):
        merged_df = concat_with_union(chunk_df, df)

    # Phase 2: process overlapping=True files (pull-forward non-overwritten rows)
    with p.span("process.phase2"):
        deleted, merged_df, total_rows, skipped_files = process_files_with_overlap(
            data_dir=data_dir,
            deleted=deleted,
            df=df,
            empty_df=empty_df,
            merged_df=merged_df,
            new_resources=new_resources,
            overlapping_files=overlapping_files,
            overwrite_columns=overwrite_columns,
            sunset_files=sunset_files,
            total_rows=total_rows,
            compression_level=compression_level,
            file_cache=file_cache,
            table_config=table_config,
            profiler=p,
        )
    p.add("phase2_skipped_files", len(skipped_files))

    # Phase 3: compact skipped small files when file count exceeds threshold.
    #
    # After the OR→AND fix, has_overlap=True files with zero actual composite
    # key matches are correctly left untouched in Phase 2.  Without compaction
    # these accumulate as many small files over successive writes.
    #
    # Policy: when the number of skipped (untouched) small files reaches
    # MAX_OVERLAPPING_FILES, merge ALL of them into the output buffer.
    # process_files_without_overlap already handles this pattern for
    # has_overlap=False files; this does the same for the skipped True files.
    max_mem, max_files = _resolve_limits(table_config)

    if len(skipped_files) >= max_files:
        p.add("phase3_compaction_triggered", 1)
        with p.span("process.phase3_skipped_compact"):
            compact_size = 0
            for file, file_size in skipped_files:
                existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size)
                if existing_df is None:
                    continue

                with p.span("process.phase3.concat"):
                    merged_df = concat_with_union(merged_df, existing_df)
                sunset_files.add(file)
                compact_size += int(file_size or 0)

                # Flush if chunk exceeds memory limit
                if compact_size >= max_mem:
                    total_rows += merged_df.shape[0]
                    with p.span("process.phase3.flush"):
                        write_parquet_and_collect_resources(
                            write_df=merged_df,
                            overwrite_columns=overwrite_columns,
                            data_dir=data_dir,
                            new_resources=new_resources,
                            compression_level=compression_level,
                            profiler=p,
                        )
                    merged_df = empty_df.clone()
                    compact_size = 0

    # Final flush if anything remains
    if merged_df.shape[0] > 0:
        total_rows += merged_df.shape[0]
        with p.span("process.final_flush"):
            write_parquet_and_collect_resources(
                write_df=merged_df,
                overwrite_columns=overwrite_columns,
                data_dir=data_dir,
                new_resources=new_resources,
                compression_level=compression_level,
                profiler=p,
            )

    return inserted, deleted, total_rows, total_columns, new_resources, sunset_files


def process_files_with_overlap(
        data_dir,
        deleted,
        df,
        empty_df,
        merged_df,
        new_resources,
        overlapping_files,
        overwrite_columns,
        sunset_files,
        total_rows,
        compression_level,
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
):
    p = profiler or get_null_profiler()
    # Pre-compute unique values for overwrite columns (ORIGINAL FAST APPROACH)
    unique_values_map = {}
    if overwrite_columns:
        with p.span("process.phase2.unique_values"):
            for col in overwrite_columns:
                if col in df.columns:
                    unique_values_map[col] = df[col].unique(maintain_order=False)

    # Track has_overlap=True files that had zero actual row matches (skipped).
    # These are candidates for small-file compaction when the total file count
    # exceeds MAX_OVERLAPPING_FILES.
    skipped_files: List[Tuple[str, int]] = []

    # Iterate only files where has_overlap is True
    for file, file_size in ((file, file_size) for file, has_overlap, file_size in overlapping_files if has_overlap):
        p.add("phase2_files_seen", 1)
        # Use cached read if available (populated by filter_stale_incoming_rows)
        if file_cache is not None and file in file_cache:
            existing_df = file_cache.pop(file)
            p.add("phase2_cache_hits", 1)
        else:
            existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size)
        if existing_df is None:
            continue

        filtered_df = empty_df.clone()

        if overwrite_columns:
            # Use anti-join on composite key to correctly match all columns (AND),
            # not OR'd is_in per column which over-deletes on partial key matches.
            key_cols_in_existing = [c for c in overwrite_columns if c in existing_df.columns]
            key_cols_in_incoming = [c for c in overwrite_columns if c in unique_values_map]

            if key_cols_in_existing == list(overwrite_columns) and key_cols_in_incoming == list(overwrite_columns):
                # Build a DataFrame of unique incoming key tuples for the join
                with p.span("process.phase2.incoming_keys"):
                    incoming_keys = df.select(overwrite_columns).unique()
                with p.span("process.phase2.anti_join"):
                    kept = existing_df.join(incoming_keys, on=overwrite_columns, how="anti")
                difference = existing_df.shape[0] - kept.shape[0]
                deleted += difference
                filtered_df = kept
            else:
                filtered_df = existing_df
        else:
            filtered_df = existing_df

        # If nothing changed, skip re-writing that file
        if filtered_df.shape[0] == existing_df.shape[0] and overwrite_columns:
            # No rows deleted → keep original; record for potential compaction
            skipped_files.append((file, file_size))
            p.add("phase2_files_skipped", 1)
            continue

        with p.span("process.phase2.concat"):
            merged_df = concat_with_union(merged_df, filtered_df)
        sunset_files.add(file)
        p.add("phase2_files_touched", 1)

        # Spill chunk if too large (ORIGINAL APPROACH - 2x memory chunk)
        _max_mem, _ = _resolve_limits(table_config)
        if merged_df.estimated_size() > _max_mem * 2:
            total_rows += merged_df.shape[0]
            with p.span("process.phase2.flush"):
                write_parquet_and_collect_resources(
                    write_df=merged_df,
                    overwrite_columns=overwrite_columns,
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                )
            merged_df = empty_df.clone()

    return deleted, merged_df, total_rows, skipped_files


# =========================
# Public API: standalone compaction (no incoming data)
# =========================

def compact_resources(
        snapshot: dict,
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        small_only: bool = True,
) -> Tuple[int, int, List[Dict], Set[str]]:
    """Compact small parquet files in a snapshot's resources list.

    This is the standalone counterpart to ``process_overlapping_files``:
    it reads files and rewrites them in memory-bounded chunks, **without
    needing any incoming data**. Used by ``DataWriter.compact()``.

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
        # The same threshold ``process_files_without_overlap`` uses.
        if chunk_size_bytes >= max_mem:
            total_rows += chunk_df.shape[0]
            # No overwrite columns for plain compaction — the resource
            # stats will be re-collected by
            # ``collect_column_statistics`` inside the writer; we don't
            # need to pin a specific key for the resulting file.
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


def process_files_without_overlap(
        empty_df,
        data_dir,
        new_resources,
        overlapping_files,
        overwrite_columns,
        sunset_files,
        compression_level,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
):
    p = profiler or get_null_profiler()
    # ORIGINAL COMPACTION APPROACH - simple and fast
    chunk_size = 0
    chunk_df = empty_df.clone()
    max_mem, _ = _resolve_limits(table_config)

    # Pull in has_overlap=False files (selected by threshold pruning) for compaction
    for file, file_size in ((file, file_size) for file, has_overlap, file_size in overlapping_files if not has_overlap):
        existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size)
        if existing_df is None:
            continue

        with p.span("process.phase1.concat"):
            chunk_df = concat_with_union(chunk_df, existing_df)
        sunset_files.add(file)
        chunk_size += int(file_size or 0)
        p.add("phase1_files", 1)

        # If the chunk size exceeds the max memory chunk size, write it out
        if chunk_size >= max_mem:
            with p.span("process.phase1.flush"):
                write_parquet_and_collect_resources(
                    write_df=chunk_df,
                    overwrite_columns=overwrite_columns,
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                )
            chunk_size = 0
            chunk_df = empty_df.clone()

    return chunk_df


# =========================
# Delete-only processing (per-file in-place rewrite)
# =========================

def process_delete_only(
        df: polars.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        data_dir: str,
        compression_level: int,
        file_cache: Optional[Dict[str, polars.DataFrame]] = None,
        profiler: Optional[Profiler] = None,
):
    """
    Delete-only path: for each overlapping file, remove rows matching the
    overwrite_columns keys present in *df*, then rewrite the remainder as a
    standalone file.

    Semantics:
      - Only has_overlap=True files are touched.
      - has_overlap=False files are ignored (no compaction during delete).
      - Each file is rewritten independently — no union/merge across files.
      - If all rows are deleted from a file, only sunset occurs (no new file).
      - If no rows match, the file is left untouched.

    Returns the same 6-tuple as process_overlapping_files:
        (inserted, deleted, total_rows, total_columns, new_resources, sunset_files)
    where inserted is always 0.
    """
    p = profiler or get_null_profiler()
    deleted = 0
    total_rows = 0
    total_columns = 0

    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()

    # Pre-compute unique key values for efficient is_in() filtering
    unique_values_map: Dict[str, polars.Series] = {}
    with p.span("delete.unique_values"):
        for col in overwrite_columns:
            if col in df.columns:
                unique_values_map[col] = df[col].unique(maintain_order=False)

    for file, has_overlap, file_size in overlapping_files:
        if not has_overlap:
            continue
        p.add("delete_files_seen", 1)

        # Use cached read if available (populated by filter_stale_incoming_rows)
        if file_cache is not None and file in file_cache:
            existing_df = file_cache.pop(file)
            p.add("delete_cache_hits", 1)
        else:
            existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size)
        if existing_df is None:
            continue

        # Build the match condition: anti-join on composite key (AND semantics)
        key_cols_in_existing = [c for c in overwrite_columns if c in existing_df.columns]
        key_cols_in_incoming = [c for c in overwrite_columns if c in unique_values_map]

        if key_cols_in_existing != list(overwrite_columns) or key_cols_in_incoming != list(overwrite_columns):
            # No key columns overlap with this file — skip
            continue

        # Build a DataFrame of unique incoming key tuples for the join
        with p.span("delete.incoming_keys"):
            incoming_keys = df.select(overwrite_columns).unique()
        with p.span("delete.anti_join"):
            kept_df = existing_df.join(incoming_keys, on=overwrite_columns, how="anti")
        difference = existing_df.shape[0] - kept_df.shape[0]

        if difference == 0:
            # Nothing deleted from this file — leave it untouched
            p.add("delete_files_skipped", 1)
            continue

        deleted += difference

        # Sunset the original file
        sunset_files.add(file)
        p.add("delete_files_touched", 1)

        # Track total_columns from existing files (they have the full schema)
        if total_columns == 0:
            total_columns = existing_df.shape[1]

        # If there are remaining rows, write them as a standalone file
        if kept_df.shape[0] > 0:
            total_rows += kept_df.shape[0]
            with p.span("delete.write_kept"):
                write_parquet_and_collect_resources(
                    write_df=kept_df,
                    overwrite_columns=overwrite_columns,
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                )

    # Fallback: if no files were touched, derive total_columns from the delete df
    if total_columns == 0:
        total_columns = df.shape[1]

    return 0, deleted, total_rows, total_columns, new_resources, sunset_files


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

    with p.span("write.collect_stats"):
        stats = collect_column_statistics(write_df, overwrite_columns)

    # Ensure target directory exists (no-op on object storage)
    with p.span("write.ensure_dir"):
        try:
            if not _get_storage().exists(target_dir):
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
            "stats": stats,
        }
    )


def collect_column_statistics(write_df, overwrite_columns: List[str]):
    """
    Collect min/max for EVERY column in a single vectorized pass (fast).
    Function signature stays the same to preserve call sites and internal logic.
    """
    stats: Dict[str, dict] = {}

    if write_df.is_empty():
        return stats

    cols = write_df.columns

    # Build vectorized aggregations: for each column compute min/max.
    agg_exprs: List[polars.Expr] = []
    for c in cols:
        agg_exprs.append(polars.col(c).min().alias(f"__min__{c}"))
        agg_exprs.append(polars.col(c).max().alias(f"__max__{c}"))

    agg = write_df.select(agg_exprs)
    # Single-row result; convert once to Python dict for cheap lookups
    row = agg.to_dicts()[0]

    for c in cols:
        min_val = row.get(f"__min__{c}")
        max_val = row.get(f"__max__{c}")

        # Normalize temporal types to ISO strings (matches original behavior)
        if isinstance(min_val, (date, datetime)):
            min_val = min_val.isoformat()
        if isinstance(max_val, (date, datetime)):
            max_val = max_val.isoformat()

        stats[c] = {"min": min_val, "max": max_val}

    return stats


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
        part = _read_parquet_safe(file_path, profiler=p, file_size=file_size)
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
# Tombstone (soft-delete) helpers
# =========================

# Default threshold: number of tombstoned keys before compaction is triggered.
_DEFAULT_TOMBSTONE_COMPACT_TOTAL = 1000


def _tombstone_threshold(table_config: Optional[dict]) -> int:
    """Return the tombstone compaction threshold for the given table config."""
    cfg = table_config or {}
    return int(cfg.get("tombstone_compact_total") or _DEFAULT_TOMBSTONE_COMPACT_TOTAL)


def extract_key_tuples(
        df: polars.DataFrame,
        primary_keys: List[str],
) -> List[Tuple]:
    """Extract unique composite-key tuples from a DataFrame.

    Each element is a tuple of Python scalars ordered by *primary_keys*.
    Suitable for JSON-serialisable tombstone storage.
    """
    available = [c for c in primary_keys if c in df.columns]
    if available != list(primary_keys):
        return []
    key_df = df.select(primary_keys).unique()
    return key_df.rows()  # list of tuples


def reconcile_tombstones(
        tombstone_keys: List,
        incoming_keys: List,
) -> List:
    """Remove from *tombstone_keys* any key tuple also present in *incoming_keys*.

    Both inputs are lists of tuples/lists (one entry per composite key).
    Returns the pruned tombstone list.
    """
    if not tombstone_keys or not incoming_keys:
        return list(tombstone_keys or [])
    incoming_set = {tuple(k) for k in incoming_keys}
    return [k for k in tombstone_keys if tuple(k) not in incoming_set]


def compact_tombstones(
        snapshot: dict,
        primary_keys: List[str],
        data_dir: str,
        compression_level: int,
        table_config: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
) -> Tuple[int, List[Dict], Set[str]]:
    """Physically remove tombstoned rows from parquet files.

    Reads the ``tombstones`` block from *snapshot*, iterates all resources
    whose stats *might* overlap with the tombstoned keys, anti-joins the
    rows out, and rewrites the file.

    Returns:
        (compacted_rows, new_resources, sunset_files)
    where compacted_rows is the total number of rows physically removed.
    After this call the caller should clear the tombstone list.
    """
    p = profiler or get_null_profiler()
    tombstone_block = snapshot.get("tombstones") or {}
    deleted_keys = tombstone_block.get("deleted_keys") or []
    if not deleted_keys or not primary_keys:
        return 0, [], set()

    # Build a Polars DataFrame of the tombstone keys for anti-join
    with p.span("tombstone.build_df"):
        try:
            tombstone_df = polars.DataFrame(
                {pk: [row[i] for row in deleted_keys] for i, pk in enumerate(primary_keys)}
            )
        except Exception:
            return 0, [], set()

    resources = snapshot.get("resources") or []
    p.add("tombstone_keys", len(deleted_keys))
    p.add("tombstone_resources_total", len(resources))
    compacted = 0
    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()

    for resource in resources:
        file_path = resource.get("file")
        if not file_path:
            continue
        file_size = int(resource.get("file_size") or 0)

        # Use stats to skip files that cannot contain any tombstoned key
        stats = resource.get("stats")
        if stats:
            with p.span("tombstone.stats_check"):
                overlaps = _tombstone_overlaps_stats(tombstone_df, primary_keys, stats)
            if not overlaps:
                p.add("tombstone_files_pruned", 1)
                continue

        existing_df = _read_parquet_safe(file_path, profiler=p, file_size=file_size)
        if existing_df is None:
            continue

        # Check that all primary key columns exist in the file
        if not all(c in existing_df.columns for c in primary_keys):
            continue

        with p.span("tombstone.anti_join"):
            kept_df = existing_df.join(tombstone_df, on=primary_keys, how="anti")
        difference = existing_df.shape[0] - kept_df.shape[0]

        if difference == 0:
            p.add("tombstone_files_skipped", 1)
            continue

        compacted += difference
        sunset_files.add(file_path)
        p.add("tombstone_files_touched", 1)

        if kept_df.shape[0] > 0:
            with p.span("tombstone.write_kept"):
                write_parquet_and_collect_resources(
                    write_df=kept_df,
                    overwrite_columns=primary_keys,
                    data_dir=data_dir,
                    new_resources=new_resources,
                    compression_level=compression_level,
                    profiler=p,
                )

    return compacted, new_resources, sunset_files


def _tombstone_overlaps_stats(
        tombstone_df: polars.DataFrame,
        primary_keys: List[str],
        stats: Dict,
) -> bool:
    """Check if any tombstone key *might* overlap with a file's column stats.

    Conservative: returns True (might overlap) when stats are missing or
    inconclusive.  Only returns False when we can **prove** no overlap —
    i.e. when at least one primary-key column has *all* tombstone values
    falling outside the file's [min, max] range for that column.
    """
    for col in primary_keys:
        if col not in stats:
            return True  # missing stats → assume overlap
        col_stats = stats[col]
        min_val = col_stats.get("min")
        max_val = col_stats.get("max")
        if min_val is None or max_val is None:
            return True

        try:
            tomb_vals = tombstone_df[col].unique().to_list()
        except Exception:
            return True

        # If NO tombstone value falls in [min, max] for this column,
        # the file cannot contain any of the tombstoned keys → skip.
        if not any(v is not None and min_val <= v <= max_val for v in tomb_vals):
            return False

    # Every column has at least one tombstone value in range → must check file
    return True