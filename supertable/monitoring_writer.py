# supertable/monitoring_writer.py
import time
import threading
import queue
import atexit
import uuid
import os
import io
from typing import Dict, List, Any, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime, timezone

from supertable.config.defaults import logger
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import RedisCatalog

# ---------------- Singleton registry ----------------

_MONITOR_INSTANCES: Dict[Tuple[str, str, str], "MonitoringWriter"] = {}

def get_monitoring_logger(super_name: str, organization: str, monitor_type: str) -> "MonitoringWriter":
    """
    Global singleton per (organization, super_name, monitor_type).
    Ensures a single background writer thread per stream.
    """
    key = (organization, super_name, monitor_type)
    inst = _MONITOR_INSTANCES.get(key)
    if inst is None:
        inst = MonitoringWriter(super_name=super_name, organization=organization, monitor_type=monitor_type)
        _MONITOR_INSTANCES[key] = inst
    return inst

def _shutdown_all_monitors():
    for inst in list(_MONITOR_INSTANCES.values()):
        try:
            inst.close()
        except Exception:
            pass
    _MONITOR_INSTANCES.clear()

atexit.register(_shutdown_all_monitors)


class MonitoringWriter:
    """
    Minimal-lock, queue-based monitoring (minute-cadence batching):

      • Producers call log_metric(record) → enqueues only (no IO, no locks).
      • A single background thread (per (org,super,type)) aggregates for ~1 minute,
        then writes a Parquet file per table_name for that cycle.
      • Files are written under:
           <org>/<super>/monitoring/<type>/data/table_name=<name>/<ts>_<id>_data.parquet
      • After each write we update _stats.json under a short Redis lock:
           - Append {"path","rows","table_name"} for each file
           - Maintain "file_count" and "row_count" aggregates
      • Debug logs include:
           - Thread start/stop
           - Cycle BEGIN/END with pre/post file & row counts
           - Lock acquisition time and stats update time
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        monitor_type: str,
        max_rows_per_file: int = 1_000_000,   # effectively "unlimited" per minute per table
        flush_interval: float = 60.0,         # strict minute cadence
        compression: str = "zstd",
        compression_level: int = 1,
        idle_stop_after: float = 90.0,        # stop thread if idle this long after last activity
    ):
        self.identity = "monitoring"
        self.super_name = super_name
        self.organization = organization
        self.monitor_type = monitor_type

        self.max_rows_per_file = int(max_rows_per_file)
        self.flush_interval = float(flush_interval)
        self.compression = compression
        self.compression_level = int(compression_level)
        self.idle_stop_after = float(idle_stop_after)

        self.storage = get_storage()
        self.catalog = RedisCatalog()

        # Paths
        self.base_dir = os.path.join(self.organization, self.super_name, self.identity, self.monitor_type)
        self.data_dir = os.path.join(self.base_dir, "data")
        self.stats_path = os.path.join(self.organization, self.super_name, "_stats.json")

        # State
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.current_batch: List[Dict[str, Any]] = []
        self.batch_lock = threading.Lock()

        self.stop_event = threading.Event()

        # Stats about the queue/thread
        self.queue_stats = {
            "total_received": 0,
            "total_processed": 0,
            "current_size": 0,
            "max_size": 0,
            "last_flush_time": 0.0,
            "flush_durations": [],
            "last_flush_size": 0,
            "start_time": time.time(),
        }
        self.queue_stats_lock = threading.Lock()

        # Ensure directories exist
        self.storage.makedirs(self.base_dir)
        self.storage.makedirs(self.data_dir)

        # Start background writer (auto-restart on demand)
        self._writer_thread: Optional[threading.Thread] = None
        self._ensure_thread()

    # ---------------- Utilities ----------------

    def _ensure_thread(self):
        if self._writer_thread is None or not self._writer_thread.is_alive():
            self.stop_event.clear()
            self._writer_thread = threading.Thread(
                target=self._write_loop,
                name=f"MonitoringWriter-{self.organization}-{self.super_name}-{self.monitor_type}",
                daemon=True,
            )
            self._writer_thread.start()
            logger.info(
                f"[monitor] started writer thread for {self.organization}/{self.super_name}/{self.monitor_type}"
            )

    def _generate_filename(self, prefix: str) -> str:
        timestamp = int(time.time() * 1000)
        unique_hash = uuid.uuid4().hex[:16]
        return f"{timestamp}_{unique_hash}_{prefix}"

    # ---------------- IO helpers ----------------

    @staticmethod
    def _ensure_execution_time(record: Dict[str, Any]) -> Dict[str, Any]:
        if "execution_time" not in record:
            record["execution_time"] = int(datetime.now(timezone.utc).timestamp() * 1_000)
        return record

    def _dir_for_table(self, table_name: str) -> str:
        safe = str(table_name)
        return os.path.join(self.data_dir, f"table_name={safe}")

    def _write_parquet_file(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
    ) -> Dict[str, Any]:
        if not data:
            return {"file": "", "file_size": 0, "rows": 0, "columns": 0, "table_name": table_name}

        data = [self._ensure_execution_time(r) for r in data]
        df = pl.from_dicts(data)

        table_dir = self._dir_for_table(table_name)
        self.storage.makedirs(table_dir)

        new_filename = self._generate_filename("data.parquet")
        new_path = os.path.join(table_dir, new_filename)

        table: pa.Table = df.to_arrow()
        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=self.compression,
            compression_level=self.compression_level,
            use_dictionary=True,
            write_statistics=True,
        )
        payload = buf.getvalue()
        if hasattr(self.storage, "write_bytes"):
            self.storage.write_bytes(new_path, payload)
        else:
            with open(new_path, "wb") as f:
                f.write(payload)

        try:
            size = self.storage.size(new_path)
        except Exception:
            try:
                size = os.path.getsize(new_path)
            except Exception:
                size = 0

        return {
            "file": new_path,
            "file_size": int(size),
            "rows": int(df.height),
            "columns": len(df.columns),
            "table_name": table_name,
        }

    # ---------------- Stats JSON (locked) ----------------

    def _read_stats(self) -> Dict[str, Any]:
        """
        Structure:
        {
          "file_count": int,
          "row_count": int,
          "files": [ {"path": "...parquet", "rows": int, "table_name": "..." } ],
          "updated_ms": int
        }
        """
        if self.storage.exists(self.stats_path):
            try:
                obj = self.storage.read_json(self.stats_path)
                if isinstance(obj, dict):
                    obj.setdefault("files", [])
                    obj.setdefault("file_count", len(obj.get("files", [])))
                    obj.setdefault("row_count", 0)
                    obj.setdefault("updated_ms", 0)
                    # Back-compat: old list[str]
                    if obj["files"] and isinstance(obj["files"][0], str):
                        obj["files"] = [{"path": p, "rows": 0, "table_name": ""} for p in obj["files"]]
                    obj["file_count"] = int(obj.get("file_count", len(obj["files"])))
                    obj["row_count"] = int(obj.get("row_count", 0))
                    return obj
            except Exception:
                pass
        return {"files": [], "file_count": 0, "row_count": 0, "updated_ms": 0}

    def _write_stats(self, stats: Dict[str, Any]) -> None:
        stats["updated_ms"] = int(time.time() * 1000)
        self.storage.write_json(self.stats_path, stats)

    def _append_files_to_stats_locked(self, new_file_entries: List[Dict[str, Any]]) -> None:
        """
        Acquire stat lock, read _stats.json, append new refs, write back, release.
        Logs lock acquisition time and update duration.
        Each entry: {"path": str, "rows": int, "table_name": str}
        """
        if not new_file_entries:
            return

        org = self.organization
        sup = self.super_name

        acquire_start = time.time()
        token = self.catalog.acquire_stat_lock(org, sup, ttl_s=10, timeout_s=10)
        lock_wait = time.time() - acquire_start
        if not token:
            logger.warning(
                f"[monitor] stats lock ACQUIRE FAILED for {org}/{sup} "
                f"(waited {lock_wait:.4f}s), skipping stats update"
            )
            return

        update_start = time.time()
        try:
            stats = self._read_stats()
            before_cnt = int(stats.get("file_count", 0))
            before_rows = int(stats.get("row_count", 0))
            logger.debug(
                f"[monitor] stats update BEGIN for {org}/{sup}: "
                f"file_count(before)={before_cnt}, row_count(before)={before_rows}, "
                f"new_files={len(new_file_entries)}, lock_wait={lock_wait:.4f}s"
            )

            files = stats.get("files", [])
            files.extend(
                {"path": e.get("file") or e.get("path"), "rows": int(e.get("rows", 0)), "table_name": e.get("table_name", "")}
                for e in new_file_entries
                if e.get("file") or e.get("path")
            )
            stats["files"] = files
            stats["file_count"] = len(files)
            stats["row_count"] = sum(int(f.get("rows", 0)) for f in files)

            self._write_stats(stats)

            after_cnt = int(stats["file_count"])
            after_rows = int(stats["row_count"])
            update_dur = time.time() - update_start
            logger.debug(
                f"[monitor] stats update END for {org}/{sup}: "
                f"file_count(after)={after_cnt}, row_count(after)={after_rows}, "
                f"lock_wait={lock_wait:.4f}s, update_time={update_dur:.4f}s"
            )
        finally:
            try:
                self.catalog.release_stat_lock(org, sup, token)
            except Exception:
                pass

    # ---------------- Flush logic ----------------

    def _flush_batch(self):
        with self.batch_lock:
            if not self.current_batch:
                return
            batch = self.current_batch
            self.current_batch = []

        start = time.time()

        # Group by table_name (required)
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for rec in (self._ensure_execution_time(r) for r in batch):
            table_name = rec.get("table_name") or "unknown_table"
            grouped.setdefault(table_name, []).append(rec)

        # Write one (or few if huge) file per table_name for this minute-cycle
        written_entries: List[Dict[str, Any]] = []
        for table_name, rows in grouped.items():
            idx = 0
            n = len(rows)
            while idx < n:
                end = min(idx + self.max_rows_per_file, n)
                res = self._write_parquet_file(rows[idx:end], table_name)
                if res.get("file"):
                    written_entries.append({"path": res["file"], "rows": res["rows"], "table_name": table_name})
                idx = end

        # Update _stats.json under lock
        if written_entries:
            self._append_files_to_stats_locked(written_entries)

        # Update queue stats
        with self.queue_stats_lock:
            self.queue_stats["total_processed"] += len(batch)
            self.queue_stats["last_flush_size"] = len(batch)
            self.queue_stats["last_flush_time"] = time.time()
            self.queue_stats["flush_durations"].append(self.queue_stats["last_flush_time"] - start)
            if len(self.queue_stats["flush_durations"]) > 100:
                self.queue_stats["flush_durations"].pop(0)

    # ---------------- Public API ----------------

    def log_metric(self, metric_data: Dict[str, Any]):
        """Enqueue only; ensures background thread is running."""
        self._ensure_thread()
        self.queue.put(metric_data)
        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1
            current_size = self.queue.qsize()
            self.queue_stats["current_size"] = current_size
            self.queue_stats["max_size"] = max(self.queue_stats["max_size"], current_size)

    # ---------------- Thread lifecycle (minute cadence) ----------------

    def _drain_queue_once(self) -> int:
        drained = 0
        while True:
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                break
            with self.batch_lock:
                self.current_batch.append(item)
            drained += 1
        return drained

    def _write_loop(self):
        logger.info(
            f"[monitor] dequeue thread running for {self.organization}/{self.super_name}/{self.monitor_type}"
        )
        last_activity = time.time()
        last_flush = time.time()

        while not self.stop_event.is_set():
            # 1) Periodically poll the queue, but do not flush more often than flush_interval
            #    We use a small sleep to avoid busy looping while still being responsive.
            polled = 0
            try:
                # Block briefly to accumulate data without spinning
                item = self.queue.get(timeout=0.5)
                with self.batch_lock:
                    self.current_batch.append(item)
                polled = 1
            except queue.Empty:
                polled = 0

            if polled:
                last_activity = time.time()
                # Drain the rest quickly
                polled += self._drain_queue_once()

            # 2) If a full minute has elapsed since the last flush, flush whatever we've collected
            now = time.time()
            due = (now - last_flush) >= self.flush_interval

            if due and self.current_batch:
                # Debug: before-cycle stats snapshot
                stats_before = self._read_stats()
                before_cnt = int(stats_before.get("file_count", len(stats_before.get("files", []))))
                before_rows = int(stats_before.get("row_count", 0))
                with self.queue_stats_lock:
                    qsz = self.queue.qsize()
                    cur_batch = len(self.current_batch)
                logger.info(
                    f"[monitor] cycle BEGIN for {self.organization}/{self.super_name}/{self.monitor_type}: "
                    f"queued={qsz}, current_batch={cur_batch}, "
                    f"file_count(before)={before_cnt}, row_count(before)={before_rows}"
                )

                self._flush_batch()
                last_flush = time.time()

                stats_after = self._read_stats()
                after_cnt = int(stats_after.get("file_count", len(stats_after.get("files", []))))
                after_rows = int(stats_after.get("row_count", 0))
                with self.queue_stats_lock:
                    flushed = self.queue_stats["last_flush_size"]
                logger.info(
                    f"[monitor] cycle END for {self.organization}/{self.super_name}/{self.monitor_type}: "
                    f"flushed_records={flushed}, file_count(after)={after_cnt}, row_count(after)={after_rows}"
                )

            # 3) Auto-stop if idle (no queue items and no current batch) for idle_stop_after
            if self.queue.empty() and not self.current_batch and (time.time() - last_activity) >= self.idle_stop_after:
                logger.info(
                    f"[monitor] idle timeout reached, stopping dequeue thread for "
                    f"{self.organization}/{self.super_name}/{self.monitor_type}"
                )
                break

            # 4) Small sleep to respect cadence and reduce CPU
            time.sleep(0.1)

        # --- Shutdown path: drain anything left and flush once (prevents missing rows) ---
        try:
            remaining = self._drain_queue_once()
            if remaining or self.current_batch:
                logger.info(
                    f"[monitor] shutdown flush for {self.organization}/{self.super_name}/{self.monitor_type}: "
                    f"drained_remaining={remaining}, batch_size={len(self.current_batch)}"
                )
                self._flush_batch()
        except Exception as e:
            logger.error(f"[monitor] error during shutdown flush: {e}")

        logger.info(
            f"[monitor] dequeue thread exited for {self.organization}/{self.super_name}/{self.monitor_type}"
        )

    def close(self):
        """Signal stop; thread is daemon and will exit. We trigger a final flush in _write_loop."""
        self.stop_event.set()
