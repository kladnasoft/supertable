import logging
import os
from datetime import datetime, date
from typing import Dict, List, Set, Tuple, Optional

import polars

from supertable.locking import Locking
from supertable.utils.helper import generate_filename, collect_schema
from supertable.config.defaults import default


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
    ints   = any(dt in _NUMERIC_INTS   for dt in dtypes)
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
        if c in a.columns: types.add(a[c].dtype)
        if c in b.columns: types.add(b[c].dtype)
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
    if a.height == 0: return b
    if b.height == 0: return a
    target = _union_schema(a, b)
    return polars.concat([_align_to_schema(a, target), _align_to_schema(b, target)], how="vertical_relaxed")


# =========================
# Safe file I/O helpers
# =========================

def _safe_exists(path: str) -> bool:
    try:
        return os.path.exists(path)
    except Exception:
        return False

def _read_parquet_safe(path: str) -> Optional[polars.DataFrame]:
    if not _safe_exists(path):
        logging.info(f"[race] file already sunset by another writer: {path}")
        return None
    try:
        return polars.read_parquet(path)
    except FileNotFoundError:
        logging.info(f"[race] file vanished before read: {path}")
        return None


# =========================
# Public API: Overlap selection (NO thresholds)
# =========================

def find_and_lock_overlapping_files(
    last_simple_table: dict,
    df: polars.DataFrame,
    overwrite_columns: List[str],
    locking: Locking,
) -> Set[Tuple[str, bool, int]]:
    """
    Build the set of truly overlapping files *only if* overwrite_columns are specified.
    No table-wide compaction, no thresholds. We lock only the overlapping files.

    Returns a set of tuples: (file_path, has_overlap=True, file_size)
    """
    resources = last_simple_table.get("resources", {}) or {}
    overlapping_files: Set[Tuple[str, bool, int]] = set()

    if not overwrite_columns:
        # Append-only write: do not compact or touch existing files.
        return set()

    # Prepare quick overlap probes from incoming data
    new_schema = collect_schema(df)
    new_data_columns: Dict[str, List] = {}
    for col in overwrite_columns:
        if col in df.columns:
            new_data_columns[col] = df[col].unique().to_list()

    for resource in resources:
        file = resource["file"]
        file_size = int(resource.get("file_size") or 0)
        stats = resource.get("stats")

        has_overlap = False
        if stats:
            for col in overwrite_columns:
                # If stats missing for a key column, be conservative: treat as overlap
                if col not in stats:
                    has_overlap = True
                    break

                min_val = stats[col].get("min")
                max_val = stats[col].get("max")
                if min_val is None or max_val is None:
                    has_overlap = True
                    break

                # Normalize types if needed
                if col in new_schema and new_schema[col] == "Date":
                    if isinstance(min_val, str): min_val = datetime.fromisoformat(min_val).date()
                    if isinstance(max_val, str): max_val = datetime.fromisoformat(max_val).date()
                elif col in new_schema and new_schema[col] == "DateTime":
                    if isinstance(min_val, str): min_val = datetime.fromisoformat(min_val)
                    if isinstance(max_val, str): max_val = datetime.fromisoformat(max_val)

                vals = new_data_columns.get(col, [])
                if any(v is None for v in vals):
                    has_overlap = True
                    break
                if any((min_val <= v <= max_val) for v in vals if v is not None):
                    has_overlap = True
                    break
        else:
            # No stats: we cannot prove it's disjoint; mark as overlap to be safe
            has_overlap = True

        if has_overlap:
            overlapping_files.add((file, True, file_size))

    # Acquire per-file locks on the overlapping files (by basename → lock id stays short)
    if False: #overlapping_files:
        lock_list = [os.path.basename(f) for (f, _, _) in overlapping_files]
        got = locking.lock_resources(
            resources=lock_list,
            timeout_seconds=int(getattr(default, "DEFAULT_TIMEOUT_SEC", 30)),
            lock_duration_seconds=int(getattr(default, "DEFAULT_LOCK_DURATION_SEC", 120)),
        )
        if not got:
            logging.debug("[overlap] per-file locks busy; skipping compaction this round")
            return set()

    return overlapping_files


# =========================
# Public API: Processing (NO thresholds)
# =========================

def process_overlapping_files(
    df: polars.DataFrame,
    overlapping_files: Set[Tuple[str, bool, int]],
    overwrite_columns: List[str],
    data_dir: str,
    compression_level: int,
):
    """
    Always write a NEW file containing:
      - the incoming df, plus
      - for each overlapping file: its NON-overwritten rows ("pull forward"),
    then mark those overlapping files as sunset.

    Non-overlapping files are left untouched. No background/append-only compaction.
    """
    inserted = 0
    deleted = 0
    total_rows = 0
    total_columns = len(df.columns)
    new_resources: List[Dict] = []
    sunset_files: Set[str] = set()

    # Start with incoming data
    merged_df = df

    if overwrite_columns and overlapping_files:
        # Build quick lookup for incoming overwrite keys
        overwrite_uniques: Dict[str, polars.Series] = {}
        for col in overwrite_columns:
            if col in df.columns:
                overwrite_uniques[col] = df[col].unique()

        # For each overlapping file: read safely, drop rows that are being overwritten, and pull forward the rest
        for file, _, _ in overlapping_files:
            existing_df = _read_parquet_safe(file)
            if existing_df is None:
                continue

            filtered_df = existing_df
            # Remove rows whose key(s) are present in the incoming df
            if overwrite_uniques:
                cond = polars.lit(True)
                any_pred = False
                for col in overwrite_columns:
                    if col in existing_df.columns and (col in overwrite_uniques):
                        any_pred = True
                        cond &= polars.col(col).is_in(overwrite_uniques[col])
                if any_pred:
                    kept = existing_df.filter(~cond)
                    deleted += (existing_df.shape[0] - kept.shape[0])
                    filtered_df = kept

            # Union schemas and append
            merged_df = concat_with_union(merged_df, filtered_df)
            sunset_files.add(file)

    # Write out merged_df as ONE (or a few) new file(s), depending on size
    # (We keep a simple size-aware chunker to avoid huge single fragments.)
    max_bytes = int(getattr(default, "MAX_MEMORY_CHUNK_SIZE", 512 * 1024 * 1024))
    est_size = max(1, int(merged_df.estimated_size()))
    approx_row_size = max(1, est_size // max(1, merged_df.shape[0]))
    target_rows = max(1, max_bytes // approx_row_size)

    start = 0
    n = merged_df.shape[0]
    while start < n:
        end = min(n, start + target_rows)
        chunk = merged_df.slice(start, end - start)
        if chunk.shape[0] == 0:
            break
        _append_resource(chunk, overwrite_columns, data_dir, new_resources, compression_level)
        total_rows += int(chunk.shape[0])
        start = end

    inserted = total_rows
    return inserted, deleted, total_rows, total_columns, new_resources, sunset_files


# =========================
# Write helpers
# =========================

def _append_resource(
    df: polars.DataFrame,
    overwrite_columns: List[str],
    data_dir: str,
    new_resources: List[Dict],
    compression_level: int,
):
    os.makedirs(data_dir, exist_ok=True)
    stats = _collect_column_statistics(df, overwrite_columns)
    fname = generate_filename("data", "parquet")
    path  = os.path.join(data_dir, fname)
    df.write_parquet(
        file=path,
        compression="zstd",
        compression_level=int(compression_level),
        statistics=True,
    )
    file_size = os.path.getsize(path)
    new_resources.append(
        {
            "file": path,
            "file_size": file_size,
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "stats": stats,
            "overwrite_keys": overwrite_columns or [],
        }
    )

def _collect_column_statistics(df: polars.DataFrame, overwrite_columns: List[str]) -> Dict[str, dict]:
    """
    Compute column stats for ALL columns (parity with LocalStorage expectations, better diagnostics):
      - min/max for sortable types (numeric, boolean, string, date/datetime)
      - None for unorderable types
      - nulls counts NULLs + NaNs (for float dtypes)
    NOTE: Overlap detection still only uses stats for `overwrite_columns`, so behavior is unchanged there.
    """
    stats: Dict[str, dict] = {}

    for col in df.columns:
        s = df[col]
        dtype = s.dtype

        # Nulls: include NaNs for float columns
        nulls = int(s.null_count())
        if dtype in _NUMERIC_FLOATS:
            try:
                nan_count = int(s.is_nan().sum())
                nulls += nan_count
            except Exception:
                # is_nan not available or failed; ignore
                pass

        min_val = None
        max_val = None

        # Only try min/max on orderable types
        try:
            # For float columns, ignore NaNs when computing bounds
            series_no_nulls = s.drop_nulls()
            if dtype in _NUMERIC_FLOATS:
                try:
                    series_no_nulls = series_no_nulls.filter(~series_no_nulls.is_nan())
                except Exception:
                    # if is_nan/filter not available, proceed with existing series
                    pass

            if len(series_no_nulls) > 0:
                min_val = series_no_nulls.min()
                max_val = series_no_nulls.max()
        except Exception:
            # leave min/max as None if dtype is not orderable or any failure occurs
            min_val = None
            max_val = None

        if isinstance(min_val, (date, datetime)): min_val = min_val.isoformat()
        if isinstance(max_val, (date, datetime)): max_val = max_val.isoformat()

        stats[col] = {"min": min_val, "max": max_val, "nulls": nulls}

    return stats
