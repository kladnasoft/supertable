#!/usr/bin/env python3
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse

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
    LEVEL = logging.INFO
    targets = [
        "supertable",
        "supertable.data_writer",
        "supertable.locking",
        "supertable.utils",
        # Optional extras:
        "py4j", "py4j.java_gateway", "pyspark",
        "urllib3", "botocore",
    ]
    for name in targets:
        lg = logging.getLogger(name)
        lg.setLevel(LEVEL)
        lg.propagate = False
        if not lg.handlers:
            lg.addHandler(logging.NullHandler())

#quiet_logs()

# -------------------------
# Config
# -------------------------
DEFAULT_THREADS = 6
# If DataWriter.write is NOT thread-safe, set this to True to serialize writes:
SERIALIZE_WRITES = False

# -------------------------
# Shared stats (thread-safe)
# -------------------------
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
stats_lock = threading.Lock()
print_lock = threading.Lock()       # avoid interleaved lines in console
write_lock = threading.Lock()       # optional: serialize DataWriter.write if needed
counter = 0                         # processed files counter (global)
counter_lock = threading.Lock()

# Compact thread id mapping (ident -> T1, T2, ...)
thread_id_map = {}
thread_id_lock = threading.Lock()
next_thread_id = 1

def get_compact_thread_id() -> str:
    global next_thread_id
    ident = threading.current_thread().ident
    with thread_id_lock:
        tid = thread_id_map.get(ident)
        if tid is None:
            tid = f"T{next_thread_id}"
            thread_id_map[ident] = tid
            next_thread_id += 1
    return tid


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


# -------------------------
# Folder-aware scanning & partitioning
# -------------------------
def list_top_level_folders(base_dir):
    """Return sorted list of subfolder names (top-level only) under base_dir."""
    return sorted(
        [name for name in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, name))]
    )

def list_files_in_folders(base_dir, folders_subset):
    """
    Yield (hyper_name, abs_path, size) for files inside the specified top-level folders.
    hyper_name == folder name.
    """
    for folder in folders_subset:
        dir_path = os.path.join(base_dir, folder)
        if not os.path.isdir(dir_path):
            continue
        for file in sorted(os.listdir(dir_path)):
            abs_path = os.path.join(dir_path, file)
            if not os.path.isfile(abs_path):
                continue
            try:
                size = os.path.getsize(abs_path)
            except OSError:
                size = 0
            yield folder, abs_path, size

def build_folder_file_index(base_dir, folders):
    """Return dict: folder -> list[(hyper_name, abs_path, size)] and overall total file count."""
    index = {}
    total = 0
    for folder in folders:
        files = list(list_files_in_folders(base_dir, [folder]))
        index[folder] = files
        total += len(files)
    return index, total

def partition_folders(folders, num_parts):
    """
    Split folder list into num_parts slices with nearly equal size.
    Example: 10 folders, 2 parts -> 5 each; 10 folders, 3 parts -> 4,3,3
    """
    n = len(folders)
    if num_parts <= 0:
        return [folders]
    size = max(1, (n + num_parts - 1) // num_parts)  # ceil division
    parts = [folders[i:i + size] for i in range(0, n, size)]
    # Ensure exactly num_parts partitions (some may be empty)
    while len(parts) < num_parts:
        parts.append([])
    return parts[:num_parts]


# -------------------------
# Worker functions
# -------------------------
def process_file_core(data_writer, file_tuple, start_time, total_files):
    """
    Read one file → write via DataWriter → update stats → print progress.
    """
    global counter
    hyper_name, abs_path, file_size = file_tuple

    file_start_time = time.time()
    table = read_file_as_table(abs_path)

    # DataWriter.write (optionally serialized)
    if SERIALIZE_WRITES:
        with write_lock:
            columns, rows, inserted, deleted = data_writer.write(
                user_hash=user_hash,
                simple_name=hyper_name,
                data=table,
                overwrite_columns=overwrite_columns,
            )
    else:
        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=table,
            overwrite_columns=overwrite_columns,
        )

    file_duration = time.time() - file_start_time

    # Update shared stats
    with stats_lock:
        stats['total_files'] += 1
        stats['total_rows'] += rows
        stats['total_inserted'] += inserted
        stats['total_deleted'] += deleted
        stats['total_duration'] += file_duration
        stats['total_bytes'] += int(file_size or 0)

    # Bump global counter to compute progress
    with counter_lock:
        counter += 1
        i = counter

    # Progress computation
    elapsed = time.time() - start_time
    percent = (i / total_files) * 100 if total_files else 100.0
    avg_time_per_file = elapsed / i if i > 0 else 0
    eta = avg_time_per_file * (total_files - i)
    files_per_sec = i / elapsed if elapsed > 0 else 0

    # Thread label
    tid = get_compact_thread_id()

    # Merge progress + response into one line
    with print_lock:
        line = (
            f"[{i}/{total_files}] {percent:6.2f}% | "
            f"elapsed {format_duration(elapsed)} | "
            f"ETA {format_duration(eta)} | {files_per_sec:.2f} files/s | "
            f"rows {stats['total_rows']:,} | bytes {format_size(stats['total_bytes'])} | "
            f"Response: [columns: {columns}, rows: {rows}, "
            f"inserted: {inserted}, deleted: {deleted}, "
            f"duration: {format_duration(file_duration)}] "
            f"({tid}, folder={hyper_name})"
        )
        print(line)

def worker_thread(data_writer, files_subset, start_time, total_files):
    """
    Process a pre-assigned list of files (all from specific folders) sequentially within this thread.
    """
    for ft in files_subset:
        process_file_core(data_writer, ft, start_time, total_files)


def parse_args():
    ap = argparse.ArgumentParser(description="Multithreaded SuperTable loader (folder-partitioned)")
    ap.add_argument("--threads", "-t", type=int, default=DEFAULT_THREADS, help="Number of worker threads")
    return ap.parse_args()


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    args = parse_args()
    num_threads = max(1, args.threads or DEFAULT_THREADS)

    # Top-level folders and indexing
    folders = list_top_level_folders(generated_data_dir)
    folder_index, total_files = build_folder_file_index(generated_data_dir, folders)
    folder_parts = partition_folders(folders, num_threads)

    print("=" * 100)
    print(f"Discovered {total_files} files in {len(folders)} folders under: {generated_data_dir}")
    print(f"Using {num_threads} threads")
    # Show assignment summary
    for idx, part in enumerate(folder_parts, 1):
        print(f"  Thread {idx} → {len(part)} folder(s): {', '.join(part) if part else '-'}")
    print("=" * 100)

    # Prepare per-thread file lists (concatenate files of assigned folders)
    per_thread_files = []
    for part in folder_parts:
        files = []
        for folder in part:
            files.extend(folder_index.get(folder, []))
        per_thread_files.append(files)

    # Initialize DataWriter (shared across threads)
    data_writer = DataWriter(super_name, organization)
    start_time = time.time()

    # Launch one worker per thread with its folder subset
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for files_subset in per_thread_files:
            futures.append(executor.submit(worker_thread, data_writer, files_subset, start_time, total_files))
        for f in as_completed(futures):
            # Surface exceptions
            f.result()

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
