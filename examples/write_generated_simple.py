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

# --- Logging suppression (quiet the library, keep our prints) ---
import logging

def quiet_logs():
    """
    Silence noisy third-party loggers without affecting our own print output.
    Tweak LEVEL to WARNING/ERROR/CRITICAL as you prefer.
    """
    LEVEL = logging.ERROR

    targets = [
        "supertable",
        "supertable.data_writer",
        "supertable.locking",
        "supertable.utils",
        # Optional extras you might want quieter in this script:
        "py4j", "py4j.java_gateway", "pyspark",
        "urllib3", "botocore",
    ]

    for name in targets:
        lg = logging.getLogger(name)
        lg.setLevel(LEVEL)
        lg.propagate = False
        if not lg.handlers:
            lg.addHandler(logging.NullHandler())

# Apply log silencing early
quiet_logs()

# Initialize DataWriter (placeholder, this will be handled in the main loop)
data_writer = None

# Enhanced statistics tracking
stats = {
    'total_files': 0,
    'total_rows': 0,
    'total_inserted': 0,
    'total_deleted': 0,
    'total_duration': 0.0,
    'total_bytes': 0,
    'batch_stats': defaultdict(lambda: {
        'count': 0,
        'inserted': 0,
        'deleted': 0,
        'duration': 0.0
    })
}

def format_duration(seconds):
    """Convert seconds to human-readable HH:MM:SS.ss format"""
    seconds = max(0.0, float(seconds))
    return str(timedelta(seconds=seconds))



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

def list_all_files(generated_data_dir=generated_data_dir):
    """Pre-scan all files so we can show totals & ETA."""
    for directory in sorted(os.listdir(generated_data_dir)):
        dir_path = os.path.join(generated_data_dir, directory)
        if os.path.isdir(dir_path):
            for file in sorted(os.listdir(dir_path)):
                abs_path = os.path.join(dir_path, file)
                if not os.path.isfile(abs_path):
                    continue
                parts = abs_path.split(os.sep)
                hyper_name = parts[1] if len(parts) > 1 else directory
                try:
                    size = os.path.getsize(abs_path)
                except OSError:
                    size = 0
                yield hyper_name, abs_path, size

def get_data(file_list):
    """Read data tables for the previously listed files."""
    for hyper_name, abs_path, size in file_list:
        table = read_file_as_table(abs_path)
        yield hyper_name, table, size

# Main Execution
if __name__ == "__main__":
    # Pre-scan files for accurate progress & ETA
    all_files = list(list_all_files(generated_data_dir))
    total_files = len(all_files)

    print("=" * 100)
    print(f"Discovered {total_files} files in: {generated_data_dir}")
    print("=" * 100)

    # Initialize DataWriter with the super name
    data_writer = DataWriter(super_name, organization)
    start_time = time.time()
    batch_start_time = start_time

    # Stream the data
    for i, (hyper_name, data, file_size) in enumerate(get_data(all_files), 1):
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
        stats['total_bytes'] += int(file_size or 0)

        current_batch = stats['batch_stats'][batch_number]
        current_batch['count'] += 1
        current_batch['inserted'] += inserted
        current_batch['deleted'] += deleted
        current_batch['duration'] += file_duration

        # Merge progress + response into one line
        elapsed = time.time() - start_time
        percent = (i / total_files) * 100 if total_files else 100.0
        avg_time_per_file = elapsed / i if i > 0 else 0
        eta = avg_time_per_file * (total_files - i)
        files_per_sec = i / elapsed if elapsed > 0 else 0

        line = (
            f"[{i}/{total_files}] {percent:6.2f}% | "
            f"elapsed {format_duration(elapsed)} | "
            f"ETA {format_duration(eta)} | {files_per_sec:.2f} files/s | "
            f"rows {stats['total_rows']:,} | bytes {format_size(stats['total_bytes'])} | "
            f"Response: [columns: {columns}, rows: {rows}, "
            f"inserted: {inserted}, deleted: {deleted}, "
            f"duration: {format_duration(file_duration)}]"
        )
        print(line)


    # Final stats
    total_duration = time.time() - start_time
    print("\n" + "=" * 100)
    print("FINAL STATISTICS")
    print(f"Total files processed: {stats['total_files']}")
    print(f"Total rows processed: {stats['total_rows']}")
    print(f"Total data size: {format_size(stats['total_bytes'])}")
    print(f"Total duration: {format_duration(total_duration)}")
    print(f"Total inserted: {stats['total_inserted']}")
    print(f"Total deleted: {stats['total_deleted']}")
    if stats['total_files'] > 0 and total_duration > 0:
        print(f"\nAverages:")
        print(f"Duration per file: {format_duration(total_duration / stats['total_files'])}")
        print(f"Inserted per file: {stats['total_inserted'] / stats['total_files']:.2f}")
        print(f"Deleted per file: {stats['total_deleted'] / stats['total_files']:.2f}")
        print(f"Files per second: {stats['total_files'] / total_duration:.2f}")
        print(f"Rows per second: {stats['total_rows'] / total_duration:.2f}")
        if stats['total_bytes'] > 0:
            print(f"Throughput: {format_size(stats['total_bytes'] / total_duration)}/s")
    print("=" * 100)
