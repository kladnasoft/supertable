import os
import sys
import json
import time
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Tuple
import uuid
import threading
import logging as pylogging

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
MAX_WORKERS            = int(os.getenv("WRITER_MAX_WORKERS", "10"))
SUBMIT_STAGGER_SEC     = float(os.getenv("WRITER_SUBMIT_STAGGER_SEC", "1"))   # spacing for *initial* starts only
START_STAGGER_COUNT    = int(os.getenv("WRITER_START_STAGGER_COUNT", str(MAX_WORKERS)))
FUTURE_TIMEOUT_SEC     = float(os.getenv("WRITER_FUTURE_TIMEOUT_SEC", "600"))
READ_CHUNK_LOG         = bool(os.getenv("WRITER_READ_CHUNK_LOG", "0") == "1")
LOG_LEVEL              = os.getenv("WRITER_LOG_LEVEL", "INFO").upper()
PROGRESS_EVERY         = int(os.getenv("WRITER_PROGRESS_EVERY", "100"))
IO_PAD_MS              = int(os.getenv("WRITER_IO_PAD_MS", "0"))              # optional test knob
# -----------------------------------------------------------------------

# Logger: ms + thread name for visible interleaving
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
if not logger.handlers:
    sh = pylogging.StreamHandler(stream=sys.stdout)
    sh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    sh.setFormatter(pylogging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-7s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S"
    ))
    logger.addHandler(sh)
else:
    for h in logger.handlers:
        try:
            h.setFormatter(pylogging.Formatter(
                fmt="%(asctime)s.%(msecs)03d %(levelname)-7s [%(threadName)s] %(message)s",
                datefmt="%H:%M:%S"
            ))
        except Exception:
            pass

data_writer = DataWriter(super_name, organization)

# ---- Concurrency & progress trackers ----------------------------------
_active_lock = threading.Lock()
_active_now = 0
_peak_active = 0

_progress_lock = threading.Lock()
_processed = 0
_total_files = 0

def _inc_active():
    global _active_now, _peak_active
    with _active_lock:
        _active_now += 1
        if _active_now > _peak_active:
            _peak_active = _active_now
        return _active_now, _peak_active

def _dec_active():
    global _active_now
    with _active_lock:
        _active_now -= 1
        return _active_now

def _bump_progress():
    global _processed
    with _progress_lock:
        _processed += 1
        done = _processed
        total = _total_files
        pct = (done / total * 100.0) if total else 0.0
        return done, total, pct
# -----------------------------------------------------------------------

def _lp(job_id: str, thread_id: int | None = None) -> str:
    tname = threading.current_thread().name
    t = f"[tid={thread_id}] " if thread_id is not None else ""
    return f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] [job={job_id}] {t}[tname={tname}] "

def read_file_as_table(file_path: str, job_id: str, thread_id: int) -> pa.Table:
    start = time.perf_counter()
    _, ext = os.path.splitext(file_path.lower())
    logger.debug(_lp(job_id, thread_id) + f"Reading file: {file_path}")

    if ext == ".csv":
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

    if IO_PAD_MS > 0:
        time.sleep(IO_PAD_MS / 1000.0)  # optional: test knob to make overlap obvious

    dur = time.perf_counter() - start
    logger.debug(_lp(job_id, thread_id) + f"Read done: rows={table.num_rows}, cols={table.num_columns}, took={dur:.3f}s")
    return table

def process_file(file_info: Tuple[str, str, int]) -> Dict[str, object]:
    file_path, hyper_name, thread_id = file_info
    job_id = str(uuid.uuid4())
    started_at = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    t0 = time.perf_counter()
    cur, peak = _inc_active()
    logger.info(_lp(job_id, thread_id) + f"▶ START {file_path} -> table={hyper_name} (start={started_at}) active={cur} peak={peak}")

    try:
        table = read_file_as_table(file_path, job_id, thread_id)
        file_size = os.path.getsize(file_path)
        t_read = time.perf_counter() - t0

        logger.info(_lp(job_id, thread_id) + f"Read stats: rows={table.num_rows}, cols={table.num_columns}, size={format_size(file_size)}, read_time={t_read:.2f}s")

        t_pre = time.perf_counter()
        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=table,
            overwrite_columns=overwrite_columns,
        )
        t_post = time.perf_counter()

        write_wall = t_post - t_pre
        total_time = t_post - t0

        logger.info(_lp(job_id, thread_id) + f"Write stats: columns={columns}, rows={rows}, inserted={inserted}, deleted={deleted}, write_wall={write_wall:.2f}s, total={total_time:.2f}s")
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
        err_time = time.perf_counter() - t0
        logger.exception(_lp(job_id, thread_id) + f"❌ ERROR {file_path}: {e} (after {err_time:.2f}s)")
        return {
            "success": False,
            "job_id": job_id,
            "thread_id": thread_id,
            "processing_time": err_time,
            "file_path": file_path,
            "table": hyper_name,
            "error": str(e),
        }
    finally:
        cur = _dec_active()
        logger.debug(_lp(job_id, thread_id) + f"⬅ END {file_path} active_now={cur}")

def get_all_files_from_directory(directory_path: str) -> List[Tuple[str, str]]:
    files_info: List[Tuple[str, str]] = []
    if os.path.isdir(directory_path):
        for file in sorted(os.listdir(directory_path)):
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):
                hyper_name = os.path.basename(directory_path)
                files_info.append((file_path, hyper_name))
    return files_info

def main():
    global _total_files
    total_start = time.perf_counter()
    logger.info(f"Starting parallel file processing at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}…")
    logger.info(f"Config: workers={MAX_WORKERS}, start_stagger_count={START_STAGGER_COUNT}, start_stagger={SUBMIT_STAGGER_SEC:.2f}s, future_timeout={FUTURE_TIMEOUT_SEC:.0f}s")

    try:
        target_directory = os.path.join(generated_data_dir, "system_but_clearly")
        if not os.path.isdir(target_directory):
            logger.error(f"Directory not found: {target_directory}")
            return

        files_info = get_all_files_from_directory(target_directory)
        _total_files = len(files_info)
        logger.info(f"Discovered {_total_files} files under {target_directory}")
        if not files_info:
            logger.warning("No files found to process.")
            return

        files_with_threads: List[Tuple[str, str, int]] = []
        for i, (file_path, hyper_name) in enumerate(files_info):
            thread_id = (i % MAX_WORKERS) + 1
            files_with_threads.append((file_path, hyper_name, thread_id))

        successful = 0
        failed = 0
        thread_times: Dict[int, float] = {i + 1: 0.0 for i in range(MAX_WORKERS)}
        thread_counts: Dict[int, int] = {i + 1: 0 for i in range(MAX_WORKERS)}
        failed_files: List[Tuple[str, str]] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="writer") as executor:
            logger.info(f"Starting {MAX_WORKERS} workers…")

            futures: List[concurrent.futures.Future] = []
            future_to_info: Dict[concurrent.futures.Future, Tuple[str, str, int]] = {}

            # ---- Stagger only the *first* START_STAGGER_COUNT submissions
            warmup_n = max(0, min(START_STAGGER_COUNT, len(files_with_threads)))
            for i in range(warmup_n):
                fi = files_with_threads[i]
                f = executor.submit(process_file, fi)
                futures.append(f)
                future_to_info[f] = fi
                if i < warmup_n - 1 and SUBMIT_STAGGER_SEC > 0:
                    time.sleep(SUBMIT_STAGGER_SEC)

            # ---- After warmup, submit the REST immediately (no sleeps)
            for fi in files_with_threads[warmup_n:]:
                f = executor.submit(process_file, fi)
                futures.append(f)
                future_to_info[f] = fi
            # ----------------------------------------------------------------

            # Collect results with progress reporting
            for future in concurrent.futures.as_completed(futures, timeout=None):
                file_path, hyper_name, thread_id = future_to_info[future]
                try:
                    result = future.result(timeout=FUTURE_TIMEOUT_SEC)

                    done, total, pct = _bump_progress()
                    if done == total or done % PROGRESS_EVERY == 0:
                        logger.info(f"📈 Progress: processed {done} / {total} ({pct:.1f}%) — peak_active={_peak_active}")

                    if result.get("success"):
                        successful += 1
                        t = float(result.get("processing_time", 0.0))
                        thread_times[thread_id] += t
                        thread_counts[thread_id] += 1
                    else:
                        failed += 1
                        t = float(result.get("processing_time", 0.0))
                        thread_times[thread_id] += t
                        thread_counts[thread_id] += 1
                        err = result.get("error", "Unknown error")
                        failed_files.append((file_path, err))
                        logger.error(_lp(result.get("job_id", "?"), thread_id) + f"Failed: {file_path} after {t:.2f}s — {err}")

                except concurrent.futures.TimeoutError:
                    done, total, pct = _bump_progress()
                    failed += 1
                    failed_files.append((file_path, f"Timeout after {FUTURE_TIMEOUT_SEC:.0f}s"))
                    logger.error(_lp("timeout", thread_id) + f"Timeout waiting for {file_path} (> {FUTURE_TIMEOUT_SEC:.0f}s). Attempting to cancel…")
                    future.cancel()
                    if done == total or done % PROGRESS_EVERY == 0:
                        logger.info(f"📈 Progress: processed {done} / {total} ({pct:.1f}%) — peak_active={_peak_active}")

                except Exception as e:
                    done, total, pct = _bump_progress()
                    failed += 1
                    logger.exception(_lp("collect", thread_id) + f"Unexpected error collecting result for {file_path}: {e}")
                    if done == total or done % PROGRESS_EVERY == 0:
                        logger.info(f"📈 Progress: processed {done} / {total} ({pct:.1f}%) — peak_active={_peak_active}")

    except KeyboardInterrupt:
        logger.warning("⏹️ KeyboardInterrupt: cancelling remaining tasks and shutting down executor…")
        successful = -1
        failed = -1
        failed_files = []

    except Exception as e:
        logger.exception(f"Main error: {e}")
        successful = -1
        failed = -1
        failed_files = []

    finally:
        total_time = time.perf_counter() - total_start
        logger.info(f"Parallel processing summary: total={total_time:.3f}s | workers={MAX_WORKERS} | files={_total_files} | successful={successful} | failed={failed} | peak_active={_peak_active}")

        logger.info("Thread statistics:")
        for tid in sorted(thread_times.keys()):
            count = thread_counts[tid]
            total_t = thread_times[tid]
            if count > 0:
                avg = total_t / count
                logger.info(f"  tid={tid}: files={count}, total={total_t:.2f}s, avg={avg:.2f}s")
            else:
                logger.info(f"  tid={tid}: files=0")

        if failed_files:
            logger.error(f"Failed Files ({len(failed_files)}):")
            for fp, err in failed_files:
                logger.error(f"  {fp}: {err}")

if __name__ == "__main__":
    main()
