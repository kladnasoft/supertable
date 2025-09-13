import os
import sys
import json
import time
import traceback
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Tuple
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_writer import DataWriter
from supertable.utils.helper import format_size
from examples.defaults import (
    organization,
    super_name,
    user_hash,
    overwrite_columns,
    generated_data_dir,
)

from supertable.config.defaults import logger, logging

# ---------------------------- Tunables ---------------------------------
MAX_WORKERS = int(os.getenv("WRITER_MAX_WORKERS", "4"))
SUBMIT_STAGGER_SEC = float(os.getenv("WRITER_SUBMIT_STAGGER_SEC", "1"))
FUTURE_TIMEOUT_SEC = float(os.getenv("WRITER_FUTURE_TIMEOUT_SEC", "600"))  # per-file timeout
READ_CHUNK_LOG = bool(os.getenv("WRITER_READ_CHUNK_LOG", "0") == "1")
LOG_LEVEL = os.getenv("WRITER_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
# -----------------------------------------------------------------------


data_writer = DataWriter(super_name, organization)


def _lp(job_id: str, thread_id: int | None = None) -> str:
    """Log prefix for correlation."""
    t = f"[tid={thread_id}] " if thread_id is not None else ""
    return f"[job={job_id}] {t}"


def read_file_as_table(file_path: str, job_id: str, thread_id: int) -> pa.Table:
    try:
        start = time.perf_counter()
        _, ext = os.path.splitext(file_path.lower())

        logger.debug(_lp(job_id, thread_id) + f"Reading file: {file_path}")
        if ext == ".csv":
            # You can extend this with dtype hints for speed if needed
            df = pd.read_csv(file_path, low_memory=False)
            if READ_CHUNK_LOG:
                logger.debug(_lp(job_id, thread_id) + f"CSV loaded: shape={df.shape}")
            df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
            table = pa.Table.from_pandas(df)
        elif ext == ".json":
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
            if READ_CHUNK_LOG:
                logger.debug(_lp(job_id, thread_id) + f"JSON loaded: shape={df.shape}")
            df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
            table = pa.Table.from_pandas(df)
        elif ext == ".parquet":
            table = pq.read_table(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path}")

        dur = time.perf_counter() - start
        logger.debug(
            _lp(job_id, thread_id)
            + f"Read done: rows={table.num_rows}, cols={table.num_columns}, took={dur:.3f}s"
        )
        return table
    except Exception as e:
        logger.exception(_lp(job_id, thread_id) + f"Error reading file {file_path}: {e}")
        raise


def process_file(file_info: Tuple[str, str, int]) -> Dict[str, object]:
    """
    Process a single file - this function will be called by multiple threads.
    Returns a dict with success flag, timing, and error if any.
    """
    file_path, hyper_name, thread_id = file_info
    job_id = str(uuid.uuid4())
    start = time.perf_counter()
    started_at = datetime.now().strftime("%H:%M:%S")

    try:
        logger.info(_lp(job_id, thread_id) + f"▶ Processing file: {file_path} -> table={hyper_name} (start={started_at})")

        table = read_file_as_table(file_path, job_id, thread_id)
        file_size = os.path.getsize(file_path)
        read_time = time.perf_counter() - start

        logger.info(
            _lp(job_id, thread_id)
            + f"Read stats: rows={table.num_rows}, cols={table.num_columns}, "
              f"size={format_size(file_size)}, read_time={read_time:.2f}s"
        )

        write_start = time.perf_counter()
        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=table,
            overwrite_columns=overwrite_columns,
        )
        write_time = time.perf_counter() - write_start
        total_time = time.perf_counter() - start

        logger.info(
            _lp(job_id, thread_id)
            + f"Write stats: columns={columns}, rows={rows}, inserted={inserted}, deleted={deleted}, "
              f"write_time={write_time:.2f}s, total={total_time:.2f}s"
        )

        return {
            "success": True,
            "job_id": job_id,
            "thread_id": thread_id,
            "processing_time": total_time,
            "file_path": file_path,
            "table": hyper_name,
            "rows": rows,
            "columns": columns,
            "inserted": inserted,
            "deleted": deleted,
        }

    except Exception as e:
        error_time = time.perf_counter() - start
        logger.exception(_lp(job_id, thread_id) + f"❌ ERROR processing {file_path}: {e} (after {error_time:.2f}s)")
        return {
            "success": False,
            "job_id": job_id,
            "thread_id": thread_id,
            "processing_time": error_time,
            "file_path": file_path,
            "table": hyper_name,
            "error": str(e),
        }


def get_all_files_from_directory(directory_path: str) -> List[Tuple[str, str]]:
    """Get all files from a directory with their metadata"""
    files_info: List[Tuple[str, str]] = []
    if os.path.isdir(directory_path):
        files = sorted(os.listdir(directory_path))
        for file in files:
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):  # Only process files, not subdirectories
                hyper_name = os.path.basename(directory_path)  # dir name as hyper_name
                files_info.append((file_path, hyper_name))
    return files_info


def main():
    total_start = time.perf_counter()
    logger.info(f"Starting parallel file processing at {datetime.now().strftime('%H:%M:%S')}…")
    logger.info(f"Config: workers={MAX_WORKERS}, submit_stagger={SUBMIT_STAGGER_SEC:.1f}s, future_timeout={FUTURE_TIMEOUT_SEC:.0f}s")

    try:
        # Get the target directory
        target_directory = os.path.join(generated_data_dir, "system_but_clearly")
        if not os.path.isdir(target_directory):
            logger.error(f"Directory not found: {target_directory}")
            return

        # Get all files from the directory
        files_info = get_all_files_from_directory(target_directory)
        logger.info(f"Discovered {len(files_info)} files under {target_directory}")
        if not files_info:
            logger.warning("No files found to process.")
            return

        # Prepare file info with thread assignment (round-robin)
        files_with_threads: List[Tuple[str, str, int]] = []
        for i, (file_path, hyper_name) in enumerate(files_info):
            thread_id = (i % MAX_WORKERS) + 1  # 1..MAX_WORKERS
            files_with_threads.append((file_path, hyper_name, thread_id))

        # Use ThreadPoolExecutor to process files in parallel
        successful = 0
        failed = 0
        thread_times: Dict[int, float] = {i + 1: 0.0 for i in range(MAX_WORKERS)}
        thread_counts: Dict[int, int] = {i + 1: 0 for i in range(MAX_WORKERS)}
        failed_files: List[Tuple[str, str]] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="writer") as executor:
            logger.info(f"Starting {MAX_WORKERS} workers to process {len(files_with_threads)} files…")

            future_to_info: Dict[concurrent.futures.Future, Tuple[str, str, int]] = {}
            futures: List[concurrent.futures.Future] = []

            # Submit work with stagger (helps reduce contention at start)
            for file_info in files_with_threads:
                file_path, hyper_name, thread_id = file_info
                logger.debug(_lp("submit", thread_id) + f"Queueing file: {file_path} -> table={hyper_name}")
                future = executor.submit(process_file, file_info)
                futures.append(future)
                future_to_info[future] = file_info
                if SUBMIT_STAGGER_SEC > 0:
                    time.sleep(SUBMIT_STAGGER_SEC)

            # Collect results with per-future timeout to avoid getting stuck forever
            for future in concurrent.futures.as_completed(futures, timeout=None):
                file_path, hyper_name, thread_id = future_to_info[future]
                try:
                    result = future.result(timeout=FUTURE_TIMEOUT_SEC)
                    if result.get("success"):
                        successful += 1
                        t = float(result.get("processing_time", 0.0))
                        thread_times[thread_id] += t
                        thread_counts[thread_id] += 1
                        logger.debug(_lp(result.get("job_id", "?"), thread_id) + f"Completed: {file_path} in {t:.2f}s")
                    else:
                        failed += 1
                        t = float(result.get("processing_time", 0.0))
                        thread_times[thread_id] += t
                        thread_counts[thread_id] += 1
                        err = result.get("error", "Unknown error")
                        failed_files.append((file_path, err))
                        logger.error(_lp(result.get("job_id", "?"), thread_id) + f"Failed: {file_path} after {t:.2f}s — {err}")

                except concurrent.futures.TimeoutError:
                    failed += 1
                    failed_files.append((file_path, f"Timeout after {FUTURE_TIMEOUT_SEC:.0f}s"))
                    logger.error(_lp("timeout", thread_id) + f"Timeout waiting for {file_path} (> {FUTURE_TIMEOUT_SEC:.0f}s). Attempting to cancel…")
                    future.cancel()

                except Exception as e:
                    failed += 1
                    logger.exception(_lp("collect", thread_id) + f"Unexpected error collecting result for {file_path}: {e}")

    except KeyboardInterrupt:
        logger.warning("⏹️ KeyboardInterrupt: cancelling remaining tasks and shutting down executor…")
        # Note: ThreadPoolExecutor context manager will wait for running tasks.
        failed = -1  # mark interrupted
        successful = -1
        failed_files = []

    except Exception as e:
        logger.exception(f"Main error: {e}")
        failed = -1
        successful = -1
        failed_files = []

    finally:
        total_time = time.perf_counter() - total_start
        # Colorize the totals for quick glancing (blue number, dark green rest)
        total_str = f"\033[94m{total_time:.3f}\033[32m"
        logger.info(
            f"\033[0mParallel processing summary: "
            f"total={total_str}s\033[0m | workers={MAX_WORKERS} | files={len(files_info) if 'files_info' in locals() else 0} | "
            f"successful={successful} | failed={failed}"
        )

        # Thread stats
        logger.info("Thread statistics:")
        for tid in sorted(thread_times.keys()):
            count = thread_counts[tid]
            total_t = thread_times[tid]
            if count > 0:
                avg = total_t / count if count else 0.0
                logger.info(f"  tid={tid}: files={count}, total={total_t:.2f}s, avg={avg:.2f}s")
            else:
                logger.info(f"  tid={tid}: files=0")

        # Failed files (if any)
        if failed_files:
            logger.error(f"Failed Files ({len(failed_files)}):")
            for fp, err in failed_files:
                logger.error(f"  {fp}: {err}")


if __name__ == "__main__":
    main()
