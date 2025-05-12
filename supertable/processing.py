import logging
import os
import polars
from datetime import datetime, date

from supertable.locking import Locking
from supertable.utils.helper import generate_filename, collect_schema
from supertable.config.defaults import default


def is_file_is_in_overlapping_files(file, overlapping_files):
    for f, _, _ in overlapping_files:
        if f == file:
            return True
    return False

def prune_not_overlapping_files_by_threshold(overlapping_files):
    # Step 1: Get all items and calculate their total size
    total_size = sum(item[2] for item in overlapping_files)
    total_files = len([item for item in overlapping_files if item[1] is False])

    # Step 2: Add all True items, maybe nothing else is needed
    result = set([item for item in overlapping_files if item[1] is True])

    # we have to unite
    if total_size > default.MAX_MEMORY_CHUNK_SIZE or total_files >= default.MAX_OVERLAPPING_FILES:
        running_total = sum(item[2] for item in overlapping_files if item[1] is True)

        # Step 3: Decide whether we can add False items or not
        false_items = [item for item in overlapping_files if item[1] is False]

        for item in false_items:
            if running_total > default.MAX_MEMORY_CHUNK_SIZE:
                break
            result.add(item)
            running_total += item[2]

    return result



def find_and_lock_overlapping_files(
    last_simple_table: dict,
    df: polars.DataFrame,
    overwrite_columns: list,
    locking: Locking,
):
    resources = last_simple_table.get("resources", {})
    new_schema = collect_schema(df)

    new_data_columns = {}
    overlapping_files = set()

    if overwrite_columns:
        for col in overwrite_columns:
            if col in df.columns:
                unique_values = df[col].unique().to_list()
                new_data_columns[col] = unique_values

        for resource in resources:
            file = resource["file"]
            file_size = resource["file_size"]
            stats = resource.get("stats")

            if stats:
                for col in overwrite_columns:
                    if col not in stats:
                        overlapping_files.add((file, True, file_size))
                        break
                    col_stats = stats[col]
                    min_val = col_stats["min"]
                    max_val = col_stats["max"]
                    new_col_values = new_data_columns[col]

                    # Only convert min_val and max_val to date/datetime if the column type is Date or DateTime
                    if new_schema[col] == "Date":
                        if isinstance(min_val, str):
                            min_val = datetime.fromisoformat(min_val).date()
                        if isinstance(max_val, str):
                            max_val = datetime.fromisoformat(max_val).date()
                    elif new_schema[col] == "DateTime":
                        if isinstance(min_val, str):
                            min_val = datetime.fromisoformat(min_val)
                        if isinstance(max_val, str):
                            max_val = datetime.fromisoformat(max_val)

                    if any(val is None for val in new_col_values):
                        overlapping_files.add((file, True, file_size))
                        break
                    if any(
                        min_val <= val <= max_val
                        for val in new_col_values
                        if val is not None
                    ):
                        overlapping_files.add((file, True, file_size))
                        break
            else:
                overlapping_files.add((file, False, file_size))

            if (
                file_size < default.MAX_MEMORY_CHUNK_SIZE
            ) and not is_file_is_in_overlapping_files(file, overlapping_files):
                overlapping_files.add((file, False, file_size))
    else:
        for resource in resources:
            if resource["file_size"] < default.MAX_MEMORY_CHUNK_SIZE:
                file = resource["file"]
                file_size = resource["file_size"]
                overlapping_files.add((file, False, file_size))


    overlapping_files = prune_not_overlapping_files_by_threshold(overlapping_files)

    # Lock Matching Resources
    resources_to_lock = [os.path.basename(file) for file, _, _ in overlapping_files]
    if resources_to_lock:
        if not locking.lock_resources(
            resources=resources_to_lock,
            timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
            lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC,
        ):
            logging.DEBUG(f"Resources cannot be locked: {resources_to_lock}")
            raise RuntimeError("Failed to acquire locks for overlapping resources")

    return overlapping_files


def process_overlapping_files(
    df: polars.DataFrame,
    overlapping_files: set,
    overwrite_columns: list,
    data_dir: str,
    compression_level: int,
):
    inserted = df.shape[0]
    deleted = 0
    total_columns = df.shape[1]
    total_rows = 0

    new_resources = []
    sunset_files = set()
    # Get the schema of the existing DataFrame
    schema = df.schema
    empty_df = polars.DataFrame(schema=schema)

    chunk_df = process_files_without_overlap(
        empty_df,
        data_dir,
        new_resources,
        overlapping_files,
        overwrite_columns,
        sunset_files,
        compression_level,
    )

    merged_df = polars.concat([chunk_df, df])

    deleted, merged_df, total_rows = process_files_with_overlap(
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
    )

    # Write remaining data if any
    if merged_df.shape[0] > 0:
        total_rows += merged_df.shape[0]
        write_parquet_and_collect_resources(
            merged_df, overwrite_columns, data_dir, new_resources, compression_level
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
    # Iterate only on values where has_overlap is True using a generator expression
    for file, file_size in (
        (file, file_size)
        for file, has_overlap, file_size in overlapping_files
        if has_overlap
    ):
        existing_df = polars.read_parquet(file)
        filtered_df = empty_df.clone()

        if overwrite_columns:
            # Filter out the rows where the overwrite_columns are in the new data
            filter_condition = polars.lit(True)
            for col in overwrite_columns:
                filter_condition &= polars.col(col).is_in(df[col].unique())
            filtered_df = existing_df.filter(~filter_condition)

        difference = existing_df.shape[0] - filtered_df.shape[0]
        deleted += difference

        # we need the full original file, whí to write itt out again?
        if difference == 0:
            continue

        merged_df = polars.concat([merged_df, filtered_df])
        sunset_files.add(file)

        if merged_df.estimated_size() > default.MAX_MEMORY_CHUNK_SIZE * 2:
            total_rows += merged_df.shape[0]
            write_parquet_and_collect_resources(
                merged_df, overwrite_columns, data_dir, new_resources, compression_level
            )
            merged_df = empty_df.clone()  # Reset the chunk DataFrame
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

    # Initialize the chunk DataFrame
    chunk_size = 0
    chunk_df = empty_df.clone()

    # Summarize file_size where has_overlap is False
    total_chunks_file_size = sum(
        file_size
        for file, has_overlap, file_size in overlapping_files
        if not has_overlap
    )
    #if total_chunks_file_size >= default.MAX_MEMORY_CHUNK_SIZE:
    for file, file_size in (
        (file, file_size)
        for file, has_overlap, file_size in overlapping_files
        if not has_overlap
    ):
        # Read the parquet file and concatenate it to the chunk DataFrame
        chunk_df = polars.concat([chunk_df, polars.read_parquet(file)])
        sunset_files.add(file)  # Track the file being processed
        chunk_size += file_size

        # If the chunk size exceeds the max memory chunk size, write it out
        if chunk_size >= default.MAX_MEMORY_CHUNK_SIZE:
            write_parquet_and_collect_resources(
                chunk_df,
                overwrite_columns,
                data_dir,
                new_resources,
                compression_level,
            )
            chunk_size = 0  # Reset the chunk size
            chunk_df = empty_df.clone()  # Reset the chunk DataFrame
    return chunk_df


def write_parquet_and_collect_resources(
    write_df, overwrite_columns, data_dir, new_resources, compression_level=10
):
    rows = write_df.shape[0]
    columns = write_df.shape[1]

    # Collect statistics and schema
    stats = collect_column_statistics(write_df, overwrite_columns)

    new_parquet_file = generate_filename("data", "parquet")
    new_parquet_path = os.path.join(data_dir, new_parquet_file)
    write_df.write_parquet(
        file=new_parquet_path,
        compression="zstd",
        compression_level=compression_level,
        statistics=True,
    )
    file_size = os.path.getsize(new_parquet_path)

    new_resources.append(
        {
            "file": new_parquet_path,
            "file_size": file_size,
            "rows": rows,
            "columns": columns,
            "stats": stats,
        }
    )


def collect_column_statistics(write_df, overwrite_columns: list):
    stats = {}
    rows = len(write_df)

    for col in overwrite_columns:
        if col in write_df.columns:
            column_data = write_df[col]
            if column_data.null_count() == rows:  # Check if all values are null
                stats[col] = {
                    # "type": str ( column_data.dtype ) ,
                    "min": None,
                    "max": None,
                    # "nulls": column_data.null_count () ,
                    # "unique": None ,
                }
            else:
                min_val = column_data.min()
                max_val = column_data.max()

                # Check if the column is of date or datetime type
                if isinstance(min_val, (date, datetime)):
                    min_val = min_val.isoformat()
                    max_val = max_val.isoformat()

                stats[col] = {
                    # "type": str ( column_data.dtype ) ,
                    "min": min_val,
                    "max": max_val,
                    # "nulls": column_data.null_count () ,
                    # "unique": column_data.n_unique () ,
                }

    return stats
