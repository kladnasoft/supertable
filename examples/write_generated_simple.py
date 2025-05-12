import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
import time
from datetime import timedelta

from supertable.data_writer import DataWriter
from supertable.utils.helper import format_size
from examples.defaults import organization, super_name, user_hash, overwrite_columns, generated_data_dir

# Initialize DataWriter (placeholder, this will be handled in the main loop)
data_writer = None

# Enhanced statistics tracking
stats = {
    'total_files': 0,
    'total_rows': 0,
    'total_inserted': 0,
    'total_deleted': 0,
    'total_duration': 0.0,
    'batch_stats': defaultdict(lambda: {
        'count': 0,
        'inserted': 0,
        'deleted': 0,
        'duration': 0.0
    })
}


def format_duration(seconds):
    """Convert seconds to human-readable HH:MM:SS.ss format"""
    return str(timedelta(seconds=seconds))


def print_stats(batch_number):
    batch = stats['batch_stats'][batch_number]
    print("\n" + "=" * 100)
    print(f"BATCH STATISTICS (Every 1000 files) - BATCH {batch_number}")
    print(f"Files processed: {batch['count']}")
    print(f"Total duration: {format_duration(batch['duration'])}")
    print(f"Rows inserted: {batch['inserted']}")
    print(f"Rows deleted: {batch['deleted']}")

    if batch['count'] > 0:
        avg_duration = batch['duration'] / batch['count']
        print(f"Avg duration per file: {format_duration(avg_duration)}")
        print(f"Avg inserted per file: {batch['inserted'] / batch['count']:.2f}")
        print(f"Avg deleted per file: {batch['deleted'] / batch['count']:.2f}")
        print(f"Files per second: {batch['count'] / batch['duration']:.2f}")
    print("=" * 100 + "\n")


def read_file_as_table(file_path):
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, low_memory=False)
        df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
        return pa.Table.from_pandas(df)
    elif file_path.endswith(".json"):
        try:
            df = pd.read_json(file_path, lines=True)
        except ValueError:
            with open(file_path, "r") as f:
                try:
                    data = json.load(f)
                    df = pd.DataFrame(data)
                except json.JSONDecodeError:
                    f.seek(0)
                    data = [json.loads(line) for line in f]
                    df = pd.DataFrame(data)
        df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
        return pa.Table.from_pandas(df)
    elif file_path.endswith(".parquet"):
        return pq.read_table(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def get_data(generated_data_dir=generated_data_dir):
    directories = os.listdir(generated_data_dir)

    for directory in sorted(directories):
        dir_path = os.path.join(generated_data_dir, directory)
        if os.path.isdir(dir_path):
            #print(f"Directory: {directory}")
            #print("-" * 100)
            files = os.listdir(dir_path)
            for file in sorted(files):
                relative_path = os.path.join(dir_path, file)
                hyper_name = relative_path.split("/")[1]
                #print(f"File: {relative_path}")
                table = read_file_as_table(relative_path)
                file_size = os.path.getsize(relative_path)

                #print( f"Rows: {table.num_rows}, Columns: {table.num_columns}, Size: {format_size(file_size)}" )

                yield hyper_name, table


# Main Execution
if __name__ == "__main__":
    # Initialize DataWriter with the super name
    data_writer = DataWriter(super_name, organization)
    start_time = time.time()
    batch_start_time = start_time

    for i, (hyper_name, data) in enumerate(get_data(generated_data_dir), 1):
        file_start_time = time.time()
        batch_number = (i - 1) // 1000 + 1

        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=data,
            overwrite_columns=overwrite_columns,
        )

        file_duration = time.time() - file_start_time

        # Update statistics
        stats['total_files'] += 1
        stats['total_rows'] += rows
        stats['total_inserted'] += inserted
        stats['total_deleted'] += deleted
        stats['total_duration'] += file_duration

        current_batch = stats['batch_stats'][batch_number]
        current_batch['count'] += 1
        current_batch['inserted'] += inserted
        current_batch['deleted'] += deleted
        current_batch['duration'] += file_duration

        print(
            f"Response: [columns: {columns}, rows: {rows}, "
            f"inserted: {inserted}, deleted: {deleted}, "
            f"duration: {format_duration(file_duration)}]"
        )
        print("-" * 100)

        # Print batch statistics every 1000 files
        if i % 1000 == 0:
            print_stats(batch_number)
            batch_start_time = time.time()

    # Print final statistics
    total_duration = time.time() - start_time
    print("\n" + "=" * 100)
    print("FINAL STATISTICS")
    print(f"Total files processed: {stats['total_files']}")
    print(f"Total rows processed: {stats['total_rows']}")
    print(f"Total duration: {format_duration(total_duration)}")
    print(f"Total inserted: {stats['total_inserted']}")
    print(f"Total deleted: {stats['total_deleted']}")

    if stats['total_files'] > 0:
        print(f"\nAverages:")
        print(f"Duration per file: {format_duration(total_duration / stats['total_files'])}")
        print(f"Inserted per file: {stats['total_inserted'] / stats['total_files']:.2f}")
        print(f"Deleted per file: {stats['total_deleted'] / stats['total_files']:.2f}")
        print(f"Files per second: {stats['total_files'] / total_duration:.2f}")
    print("=" * 100)