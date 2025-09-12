import json
import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import concurrent.futures
import logging
import traceback
import time
from typing import List, Dict, Tuple
from datetime import datetime

from supertable.data_writer import DataWriter
from supertable.utils.helper import format_size
from examples.defaults import organization, super_name, user_hash, overwrite_columns, generated_data_dir

data_writer = DataWriter(super_name, organization)

logging.getLogger("file_lock").setLevel(logging.DEBUG)
logging.getLogger("locking").setLevel(logging.DEBUG)
logging.getLogger("redis_lock").setLevel(logging.DEBUG)


def read_file_as_table(file_path):
    try:
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
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        traceback.print_exc()
        raise


def process_file(file_info):
    """Process a single file - this function will be called by multiple threads"""
    file_path, hyper_name, thread_id = file_info
    start_time = time.time()
    thread_start_time = datetime.now().strftime("%H:%M:%S")

    try:
        print(f"[Thread {thread_id} - {thread_start_time}] Processing file: {file_path}")

        table = read_file_as_table(file_path)
        file_size = os.path.getsize(file_path)
        read_time = time.time() - start_time

        print(
            f"[Thread {thread_id}] Rows: {table.num_rows}, Columns: {table.num_columns}, "
            f"Size: {format_size(file_size)}, Read time: {read_time:.2f}s")

        write_start_time = time.time()
        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=table,
            overwrite_columns=overwrite_columns,
        )
        write_time = time.time() - write_start_time
        total_time = time.time() - start_time

        print(
            f"[Thread {thread_id}] Response: [columns: {columns}, rows: {rows}, "
            f"inserted: {inserted}, deleted: {deleted}]")
        print(
            f"[Thread {thread_id}] Timing: [read: {read_time:.2f}s, write: {write_time:.2f}s, "
            f"total: {total_time:.2f}s]")
        print("-" * 100)

        return {"success": True, "thread_id": thread_id, "processing_time": total_time, "file_path": file_path}

    except Exception as e:
        error_time = time.time() - start_time
        error_timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[Thread {thread_id} - {error_timestamp}] ERROR processing file {file_path}: {str(e)}")
        print(f"[Thread {thread_id}] Error occurred after {error_time:.2f}s")
        traceback.print_exc()
        return {"success": False, "thread_id": thread_id, "processing_time": error_time, "file_path": file_path,
                "error": str(e)}


def get_all_files_from_directory(directory_path: str) -> List[tuple]:
    """Get all files from a directory with their metadata"""
    files_info = []
    if os.path.isdir(directory_path):
        files = os.listdir(directory_path)
        for file in sorted(files):
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):  # Only process files, not subdirectories
                hyper_name = directory_path.split("/")[-1]  # Get directory name as hyper_name
                files_info.append((file_path, hyper_name))
    return files_info


def main():
    try:
        start_time = time.time()
        print(f"Starting parallel file processing at {datetime.now().strftime('%H:%M:%S')}...")

        # Get the target directory
        target_directory = os.path.join(generated_data_dir, "system_but_clearly")

        if not os.path.isdir(target_directory):
            print(f"Directory not found: {target_directory}")
            return

        # Get all files from the directory
        files_info = get_all_files_from_directory(target_directory)
        print(f"Found {len(files_info)} files in directory: {target_directory}")

        if not files_info:
            print("No files found to process!")
            return

        # Prepare file info with thread assignment (round-robin)
        files_with_threads = []
        for i, (file_path, hyper_name) in enumerate(files_info):
            thread_id = (i % 4) + 1  # Assign files to threads 1-4 in round-robin fashion
            files_with_threads.append((file_path, hyper_name, thread_id))

        # Use ThreadPoolExecutor to process files in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            print(f"Starting 4 threads to process {len(files_with_threads)} files...")

            # Submit all files for processing and store mapping of future to file info
            future_to_file_info = {}
            futures = []

            for file_info in files_with_threads:
                future = executor.submit(process_file, file_info)
                futures.append(future)
                future_to_file_info[future] = file_info  # Store mapping

            # Wait for all tasks to complete and collect results
            successful = 0
            failed = 0
            thread_times = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0}
            thread_counts = {1: 0, 2: 0, 3: 0, 4: 0}
            failed_files = []

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    thread_id = result["thread_id"]
                    processing_time = result["processing_time"]
                    file_path = result["file_path"]

                    if result["success"]:
                        successful += 1
                        thread_times[thread_id] += processing_time
                        thread_counts[thread_id] += 1
                    else:
                        failed += 1
                        thread_times[thread_id] += processing_time
                        thread_counts[thread_id] += 1
                        failed_files.append((file_path, result.get("error", "Unknown error")))

                except Exception as e:
                    failed += 1
                    print(f"Unexpected error processing future: {str(e)}")
                    traceback.print_exc()

        total_time = time.time() - start_time

        print(f"\nProcessing completed at {datetime.now().strftime('%H:%M:%S')}!")
        print(f"Total time: {total_time:.2f}s")
        print(f"Results: Successful: {successful}, Failed: {failed}")

        # Print thread statistics
        print("\nThread Statistics:")
        for thread_id in sorted(thread_times.keys()):
            count = thread_counts[thread_id]
            if count > 0:
                avg_time = thread_times[thread_id] / count
                print(f"Thread {thread_id}: {count} files, "
                      f"Total time: {thread_times[thread_id]:.2f}s, "
                      f"Avg time per file: {avg_time:.2f}s")
            else:
                print(f"Thread {thread_id}: 0 files processed")

        # Print failed files if any
        if failed_files:
            print(f"\nFailed Files ({len(failed_files)}):")
            for file_path, error in failed_files:
                print(f"  {file_path}: {error}")

    except Exception as e:
        error_time = time.time() - start_time if 'start_time' in locals() else 0
        print(f"Main function error after {error_time:.2f}s: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()