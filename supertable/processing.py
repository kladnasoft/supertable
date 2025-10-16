# processing.py

import logging
import os
import io
from datetime import datetime, date
from typing import Dict, List, Set, Tuple, Optional

import polars
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.locking import Locking
from supertable.utils.helper import generate_filename, collect_schema
from supertable.config.defaults import default
from supertable.storage.storage_factory import get_storage

# Single storage instance
_storage = get_storage()

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


def _union_schema(a: polars.DataFrame, b: polars.DataFrame) -> Dict[str, polars.DataType]:
    cols: List[str] = list(dict.fromkeys(a.columns + b.columns))
    target: Dict[str, polars.DataType] = {}
    for c in cols:
        types: Set[polars.DataType] = set()
        if c in a.columns:
            types.add(a[c].dtype)
        if c in b.columns:
            types.add(b[c].dtype)
        target[c] = _resolve_unified_dtype(types)
    return target


def _align_to_schema(df: polars.DataFrame, target_schema: Dict[str, polars.DataType]) -> polars.DataFrame:
    exprs = []
    for col, dtype in target_schema.items():
        if col in df.columns:
            if df[col].dtype != dtype:
                exprs.append(polars.col(col).cast(dtype, strict=False))
        else:
            exprs.append(polars.lit(None, dtype=dtype).alias(col))
    return df.with_columns(exprs) if exprs else df


def concat_with_union(a: polars.DataFrame, b: polars.DataFrame) -> polars.DataFrame:
    if a.height == 0:
        return b
    if b.height == 0:
        return a
    target = _union_schema(a, b)
    return polars.concat([_align_to_schema(a, target), _align_to_schema(b, target)], how="vertical_relaxed")


# =========================
# Safe storage I/O helpers
# =========================

def _safe_exists(path: str) -> bool:
    try:
        return _storage.exists(path)
    except Exception:
        return False


def _read_parquet_safe(path: str) -> Optional[polars.DataFrame]:
    if not _safe_exists(path):
        logging.info(f"[race] file already sunset by another writer: {path}")
        return None
    try:
        tbl = _storage.read_parquet(path)  # -> pyarrow.Table
        return polars.from_arrow(tbl)
    except FileNotFoundError:
        logging.info(f"[race] file vanished before read: {path}")
        return None
    except Exception as e:
        logging.warning(f"[read] failed to read parquet at {path}: {e}")
        return None


# =========================
# Original-style merge threshold logic
# =========================

def is_file_in_overlapping_files(file: str, overlapping_files: Set[Tuple[str, bool, int]]) -> bool:
    for f, _, _ in overlapping_files:
        if f == file:
            return True
    return False


def prune_not_overlapping_files_by_threshold(overlapping_files: Set[Tuple[str, bool, int]]) -> Set[
    Tuple[str, bool, int]]:
    """
    Policy:
      - Always include entries with has_overlap=True
      - For has_overlap=False small files, include them only if either:
          total_size_of_all_candidates > MAX_MEMORY_CHUNK_SIZE
          OR count_of_false_items >= MAX_OVERLAPPING_FILES
        and then add false items until hitting MAX_MEMORY_CHUNK_SIZE
    """
    max_mem = int(getattr(default, "MAX_MEMORY_CHUNK_SIZE", 512 * 1024 * 1024))
    max_files = int(getattr(default, "MAX_OVERLAPPING_FILES", 100))

    total_size = sum(item[2] for item in overlapping_files)
    total_false = len([item for item in overlapping_files if item[1] is False])

    # Always keep all True (overlapping) items
    result: Set[Tuple[str, bool, int]] = set([item for item in overlapping_files if item[1] is True])

    # Gate: only pull in False items if thresholds hit
    if total_size > max_mem or total_false >= max_files:
        running_total = sum(item[2] for item in result)
        false_items = [item for item in overlapping_files if item[1] is False]

        for item in false_items:
            if running_total > max_mem:
                break
            result.add(item)
            running_total += item[2]

    return result


# =========================
# Public API: Overlap selection (with compaction triggers)
# =========================

def find_and_lock_overlapping_files(  # keep name/signature for compatibility
        last_simple_table: dict,
        df: polars.DataFrame,
        overwrite_columns: List[str],
        locking: Locking,  # not used anymore for per-file locks; higher-level lock covers us
) -> Set[Tuple[str, bool, int]]:
    """
    Builds the candidate set:
      - has_overlap=True for files whose stats indicate key overlap (or missing stats)
      - has_overlap=False for small, non-overlapping files (< MAX_MEMORY_CHUNK_SIZE)
    Then applies prune_not_overlapping_files_by_threshold to decide the final merge set.

    NOTE:
      - No per-file locking here (consistent with new locking model).
      - Return: set of tuples (file_path, has_overlap: bool, file_size)
    """
    resources = last_simple_table.get("resources", {}) or {}
    overlapping_files: Set[Tuple[str, bool, int]] = set()

    if overwrite_columns:
        # ORIGINAL APPROACH - much faster
        new_schema = collect_schema(df)
        new_data_columns: Dict[str, List] = {}
        for col in overwrite_columns:
            if col in df.columns:
                # Use original unique values approach - it's actually faster for most cases
                unique_values = df[col].unique().to_list()
                new_data_columns[col] = unique_values

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
                    if (file_size < int(getattr(default, "MAX_MEMORY_CHUNK_SIZE",
                                                512 * 1024 * 1024))) and not is_file_in_overlapping_files(file,
                                                                                                          overlapping_files):
                        overlapping_files.add((file, False, file_size))
            else:
                # Missing stats → treat as overlapping (be conservative)
                overlapping_files.add((file, True, file_size))

    else:
        # No overwrite columns → pure compaction path for small files
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            if file_size < int(getattr(default, "MAX_MEMORY_CHUNK_SIZE", 512 * 1024 * 1024)):
                overlapping_files.add((file, False, file_size))

    # Apply pruning logic to trigger compaction when many/large small files accumulate
    overlapping_files = prune_not_overlapping_files_by_threshold(overlapping_files)

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
):
    """
    Merge implementation:
      - For has_overlap=False entries, batch-read & append (compaction)
      - For has_overlap=True entries, read existing file, drop rows being overwritten, append the remainder
      - Periodically flush chunks if they get too big
      - Write any remainder at the end
    """
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
    chunk_df = process_files_without_overlap(
        empty_df=empty_df,
        data_dir=data_dir,
        new_resources=new_resources,
        overlapping_files=overlapping_files,
        overwrite_columns=overwrite_columns,
        sunset_files=sunset_files,
        compression_level=compression_level,
    )

    # Start merged with compaction chunk + incoming df
    merged_df = concat_with_union(chunk_df, df)

    # Phase 2: process overlapping=True files (pull-forward non-overwritten rows)
    deleted, merged_df, total_rows = process_files_with_overlap(
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
    )

    # Final flush if anything remains
    if merged_df.shape[0] > 0:
        total_rows += merged_df.shape[0]
        write_parquet_and_collect_resources(
            write_df=merged_df,
            overwrite_columns=overwrite_columns,
            data_dir=data_dir,
            new_resources=new_resources,
            compression_level=compression_level,
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
):
    # Pre-compute unique values for overwrite columns (ORIGINAL FAST APPROACH)
    unique_values_map = {}
    if overwrite_columns:
        for col in overwrite_columns:
            if col in df.columns:
                unique_values_map[col] = df[col].unique(maintain_order=False)

    # Iterate only files where has_overlap is True
    for file, file_size in ((file, file_size) for file, has_overlap, file_size in overlapping_files if has_overlap):
        existing_df = _read_parquet_safe(file)
        if existing_df is None:
            continue

        filtered_df = empty_df.clone()

        if overwrite_columns:
            # ORIGINAL EFFICIENT APPROACH - multiple is_in() calls
            cond = polars.lit(False)  # Start with False, OR with each column
            any_pred = False
            for col in overwrite_columns:
                if col in existing_df.columns and col in unique_values_map:
                    any_pred = True
                    unique_vals = unique_values_map[col]
                    cond = cond | polars.col(col).is_in(unique_vals)

            if any_pred:
                kept = existing_df.filter(~cond)
                difference = existing_df.shape[0] - kept.shape[0]
                deleted += difference
                filtered_df = kept
            else:
                filtered_df = existing_df
        else:
            filtered_df = existing_df

        # If nothing changed, skip re-writing that file
        if filtered_df.shape[0] == existing_df.shape[0] and overwrite_columns:
            # No rows deleted → keep original; no need to sunset
            continue

        merged_df = concat_with_union(merged_df, filtered_df)
        sunset_files.add(file)

        # Spill chunk if too large (ORIGINAL APPROACH - 2x memory chunk)
        if merged_df.estimated_size() > int(getattr(default, "MAX_MEMORY_CHUNK_SIZE", 512 * 1024 * 1024)) * 2:
            total_rows += merged_df.shape[0]
            write_parquet_and_collect_resources(
                write_df=merged_df,
                overwrite_columns=overwrite_columns,
                data_dir=data_dir,
                new_resources=new_resources,
                compression_level=compression_level,
            )
            merged_df = empty_df.clone()

    return deleted, merged_df, total_rows


def process_files_without_overlap(
        empty_df,
        data_dir,
        new_resources,
        overlapping_files,
        overwrite_columns,
        sunset_files,
        compression_level,
):
    # ORIGINAL COMPACTION APPROACH - simple and fast
    chunk_size = 0
    chunk_df = empty_df.clone()
    max_mem = int(getattr(default, "MAX_MEMORY_CHUNK_SIZE", 512 * 1024 * 1024))

    # Pull in has_overlap=False files (selected by threshold pruning) for compaction
    for file, file_size in ((file, file_size) for file, has_overlap, file_size in overlapping_files if not has_overlap):
        existing_df = _read_parquet_safe(file)
        if existing_df is None:
            continue

        chunk_df = concat_with_union(chunk_df, existing_df)
        sunset_files.add(file)
        chunk_size += int(file_size or 0)

        # If the chunk size exceeds the max memory chunk size, write it out
        if chunk_size >= max_mem:
            write_parquet_and_collect_resources(
                write_df=chunk_df,
                overwrite_columns=overwrite_columns,
                data_dir=data_dir,
                new_resources=new_resources,
                compression_level=compression_level,
            )
            chunk_size = 0
            chunk_df = empty_df.clone()

    return chunk_df


# =========================
# Write helpers
# =========================

def write_parquet_and_collect_resources(
        write_df, overwrite_columns, data_dir, new_resources, compression_level=10
):
    rows = write_df.shape[0]
    columns = write_df.shape[1]

    # UPDATED: collect stats for EVERY column (still min/max only; perf-friendly vectorization)
    stats = collect_column_statistics(write_df, overwrite_columns)

    # Ensure target "directory" exists in the active storage (creates a marker or no-op)
    try:
        if not _storage.exists(data_dir):
            _storage.makedirs(data_dir)
    except Exception:
        # Best-effort: some backends may not require explicit directory creation
        pass

    new_parquet_file = generate_filename("data", "parquet")
    new_parquet_path = os.path.join(data_dir, new_parquet_file)

    # Write to the active storage backend (MinIO/local/etc.) WITHOUT changing behavior:
    # we keep zstd + compression_level + statistics exactly like before by generating
    # the Parquet bytes ourselves and uploading them.
    try:
        # Create parquet bytes with the same settings the original code used
        arrow_tbl: pa.Table = write_df.to_arrow()
        buf = io.BytesIO()
        pq.write_table(
            arrow_tbl,
            buf,
            compression="zstd",
            compression_level=int(compression_level),
            use_dictionary=True,
            write_statistics=True,
        )
        data = buf.getvalue()

        if hasattr(_storage, "write_bytes"):
            _storage.write_bytes(new_parquet_path, data)
        elif hasattr(_storage, "write_parquet"):
            # Fallback: some storage backends may only expose write_parquet(table, path)
            # (may ignore compression level); we prefer bytes path above to preserve behavior.
            _storage.write_parquet(arrow_tbl, new_parquet_path)
        else:
            # Last-resort local write identical to original behavior
            write_df.write_parquet(
                file=new_parquet_path,
                compression="zstd",
                compression_level=int(compression_level),
                statistics=True,
            )
    except Exception:
        # As a safety net, do a local write if the storage upload path fails
        write_df.write_parquet(
            file=new_parquet_path,
            compression="zstd",
            compression_level=int(compression_level),
            statistics=True,
        )

    # size via storage if available
    try:
        file_size = _storage.size(new_parquet_path)
    except Exception:
        try:
            file_size = os.path.getsize(new_parquet_path)
        except Exception:
            # if we uploaded via bytes and path is virtual, fall back to len(data) if defined
            try:
                file_size = len(data)  # type: ignore[name-defined]
            except Exception:
                file_size = 0

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
