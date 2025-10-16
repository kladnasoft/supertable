# supertable/monitoring_writer.py
import time
import threading
import queue
import atexit
import uuid
import os
import io
from typing import Dict, List, Any, Optional, Tuple
import weakref

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime, timezone

from supertable.config.defaults import logger
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import RedisCatalog

# ---------------- Singleton registry ----------------

_MONITOR_INSTANCES: Dict[Tuple[str, str, str], "MonitoringWriter"] = {}
_MONITOR_INSTANCES_WEAK = weakref.WeakValueDictionary()
_MONITOR_INSTANCES_LOCK = threading.Lock()


def get_monitoring_logger(super_name: str, organization: str, monitor_type: str) -> "MonitoringWriter":
    """
    Global singleton per (organization, super_name, monitor_type).
    Ensures a single background writer thread per stream.
    Uses weak references to allow garbage collection.
    """
    key = (organization, super_name, monitor_type)

    with _MONITOR_INSTANCES_LOCK:
        inst = _MONITOR_INSTANCES_WEAK.get(key)
        if inst is None or not inst.is_alive():
            inst = MonitoringWriter(super_name=super_name, organization=organization, monitor_type=monitor_type)
            _MONITOR_INSTANCES_WEAK[key] = inst
            _MONITOR_INSTANCES[key] = inst  # Strong reference to prevent premature GC
        return inst


def _shutdown_all_monitors():
    """Shutdown all active monitor instances."""
    with _MONITOR_INSTANCES_LOCK:
        keys_to_remove = []
        for key, inst in _MONITOR_INSTANCES.items():
            try:
                if inst.is_alive():
                    inst.close(force_flush=True)
                keys_to_remove.append(key)
            except Exception as e:
                logger.error(f"[monitor] Error during shutdown of {key}: {e}")

        # Remove all keys after iteration to avoid modification during iteration
        for key in keys_to_remove:
            try:
                del _MONITOR_INSTANCES[key]
            except KeyError:
                pass  # Key might have been removed already


atexit.register(_shutdown_all_monitors)


class MonitoringWriter:
    """
    Robust queue-based monitoring with minute-cadence batching and guaranteed final flush.
    """

    def __init__(
            self,
            super_name: str,
            organization: str,
            monitor_type: str,
            *,
            max_rows_per_file: int = 1_000_000,
            flush_interval: float = 60.0,
            compression: str = "zstd",
            compression_level: int = 1,
            idle_stop_after: float = 10.0,  # Auto-stop after 10 seconds of inactivity
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

        # State - use RLock for better reentrancy
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.current_batch: List[Dict[str, Any]] = []
        self.pending_stat_entries: List[Dict[str, Any]] = []
        self.batch_lock = threading.RLock()

        # Thread control
        self.stop_event = threading.Event()
        self.flush_requested = threading.Event()
        self.last_activity = time.time()
        self.activity_lock = threading.Lock()
        self._shutting_down = False

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
            "flush_requests": 0,
        }
        self.queue_stats_lock = threading.Lock()

        # Ensure directories exist
        try:
            self.storage.makedirs(self.base_dir)
            self.storage.makedirs(self.data_dir)
        except Exception as e:
            logger.error(f"[monitor] Directory creation failed: {e}")

        # Start background writer
        self._writer_thread: Optional[threading.Thread] = None
        self._ensure_thread()

    def is_alive(self) -> bool:
        """Check if writer thread is alive."""
        return self._writer_thread is not None and self._writer_thread.is_alive()

    def _update_activity(self):
        """Update last activity timestamp."""
        with self.activity_lock:
            self.last_activity = time.time()

    def _get_idle_time(self) -> float:
        """Get time since last activity."""
        with self.activity_lock:
            return time.time() - self.last_activity

    # ---------------- Utilities ----------------

    def _ensure_thread(self):
        """Start writer thread if not running."""
        if not self.is_alive() and not self._shutting_down:
            self.stop_event.clear()
            self.flush_requested.clear()
            self._update_activity()  # Reset activity timer
            self._writer_thread = threading.Thread(
                target=self._write_loop,
                name=f"MonitoringWriter-{self.organization}-{self.super_name}-{self.monitor_type}",
                daemon=False,  # Non-daemon to ensure proper shutdown
            )
            self._writer_thread.start()
            logger.info(
                f"[monitor] started dequeue thread for {self.organization}/{self.super_name}/{self.monitor_type}"
            )

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _generate_filename(self, prefix: str) -> str:
        timestamp = self._now_ms()
        unique_hash = uuid.uuid4().hex[:16]
        return f"{timestamp}_{unique_hash}_{prefix}"

    # ---------------- IO helpers ----------------

    @staticmethod
    def _ensure_execution_time(record: Dict[str, Any]) -> Dict[str, Any]:
        if "execution_time" not in record:
            record["execution_time"] = int(datetime.now(timezone.utc).timestamp() * 1_000)
        return record

    def _dir_for_table(self, table_name: str) -> str:
        safe = str(table_name).replace('/', '_').replace('\\', '_')
        return os.path.join(self.data_dir, f"table_name={safe}")

    def _write_parquet_file(
            self,
            data: List[Dict[str, Any]],
            table_name: str,
    ) -> Dict[str, Any]:
        if not data:
            return {"file": "", "file_size": 0, "rows": 0, "columns": 0, "table_name": table_name}

        try:
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
                size = len(payload)  # Fallback to buffer size

            return {
                "file": new_path,
                "file_size": int(size),
                "rows": int(df.height),
                "columns": len(df.columns),
                "table_name": table_name,
            }
        except Exception as e:
            logger.error(f"[monitor] Parquet write failed for table {table_name}: {e}")
            return {"file": "", "file_size": 0, "rows": 0, "columns": 0, "table_name": table_name}

    # ---------------- Stats JSON (locked) ----------------

    def _read_stats(self) -> Dict[str, Any]:
        """Read and parse stats JSON with proper error handling."""
        if self.storage.exists(self.stats_path):
            try:
                obj = self.storage.read_json(self.stats_path)
                if isinstance(obj, dict):
                    obj.setdefault("files", [])
                    # Back-compat: old list[str]
                    if obj["files"] and isinstance(obj["files"][0], str):
                        obj["files"] = [{"path": p, "rows": 0, "table_name": ""} for p in obj["files"]]
                    obj["file_count"] = len(obj["files"])
                    obj["row_count"] = sum(int(f.get("rows", 0)) for f in obj["files"])
                    obj.setdefault("updated_ms", 0)
                    return obj
            except Exception as e:
                logger.error(f"[monitor] Error reading stats: {e}")
        return {"files": [], "file_count": 0, "row_count": 0, "updated_ms": 0}

    def _write_stats(self, stats: Dict[str, Any]) -> None:
        """Write stats JSON with timestamp."""
        stats["updated_ms"] = self._now_ms()
        try:
            self.storage.write_json(self.stats_path, stats)
        except Exception as e:
            logger.error(f"[monitor] Error writing stats: {e}")
            raise

    def _commit_stats_with_retry(self, new_entries: List[Dict[str, Any]]) -> bool:
        """
        Try to append entries to _stats.json under Redis lock.
        Returns True on success, False on failure (entries will be retried).
        """
        if not new_entries:
            return True

        org = self.organization
        sup = self.super_name

        acquire_start = time.time()
        token = self.catalog.acquire_stat_lock(org, sup, ttl_s=10, timeout_s=10)
        lock_wait = time.time() - acquire_start

        if not token:
            logger.warning(
                f"[monitor] stats lock ACQUIRE FAILED for {org}/{sup} "
                f"(waited {lock_wait:.4f}s); will retry next cycle"
            )
            return False

        try:
            update_start = time.time()
            stats = self._read_stats()

            before_cnt = stats["file_count"]
            before_rows = stats["row_count"]

            logger.debug(
                f"[monitor] stats update BEGIN for {org}/{sup}: "
                f"file_count(before)={before_cnt}, row_count(before)={before_rows}, "
                f"new_files={len(new_entries)}, lock_wait={lock_wait:.4f}s"
            )

            # Add new entries
            for entry in new_entries:
                if entry.get("file") or entry.get("path"):
                    stats["files"].append({
                        "path": entry.get("file") or entry.get("path"),
                        "rows": int(entry.get("rows", 0)),
                        "table_name": entry.get("table_name", "")
                    })

            # Update counts
            stats["file_count"] = len(stats["files"])
            stats["row_count"] = sum(int(f.get("rows", 0)) for f in stats["files"])

            self._write_stats(stats)

            after_cnt = stats["file_count"]
            after_rows = stats["row_count"]
            update_dur = time.time() - update_start

            logger.debug(
                f"[monitor] stats update END for {org}/{sup}: "
                f"file_count(after)={after_cnt}, row_count(after)={after_rows}, "
                f"lock_wait={lock_wait:.4f}s, update_time={update_dur:.4f}s"
            )
            return True

        except Exception as e:
            logger.error(f"[monitor] Error during stats update: {e}")
            return False
        finally:
            try:
                self.catalog.release_stat_lock(org, sup, token)
            except Exception:
                pass

    # ---------------- Flush logic ----------------

    def _drain_queue_to_batch(self) -> int:
        """Drain all available items from queue to current batch."""
        drained = 0
        try:
            while True:
                try:
                    item = self.queue.get_nowait()
                    with self.batch_lock:
                        self.current_batch.append(item)
                    drained += 1
                except queue.Empty:
                    break
        except Exception as e:
            logger.error(f"[monitor] Error draining queue: {e}")
        return drained

    def _flush_batch(self):
        """
        Process current batch: write parquet files and update stats.
        Retries failed stats updates in next cycle.
        """
        # Get batch data quickly with minimal locking
        with self.batch_lock:
            if not self.current_batch:
                return
            batch = self.current_batch.copy()
            self.current_batch.clear()

        start = time.time()
        written_entries: List[Dict[str, Any]] = []

        try:
            # Group by table_name
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for rec in batch:
                table_name = rec.get("table_name") or "unknown_table"
                grouped.setdefault(table_name, []).append(self._ensure_execution_time(rec))

            # Write files for each table
            for table_name, rows in grouped.items():
                idx = 0
                n = len(rows)
                while idx < n:
                    end = min(idx + self.max_rows_per_file, n)
                    chunk = rows[idx:end]
                    res = self._write_parquet_file(chunk, table_name)
                    if res.get("file") and res["rows"] > 0:
                        written_entries.append(res)
                    idx = end

            # Add pending entries from previous failures
            if self.pending_stat_entries:
                written_entries = self.pending_stat_entries + written_entries
                self.pending_stat_entries = []

            # Update stats under Redis lock
            if written_entries:
                ok = self._commit_stats_with_retry(written_entries)
                if not ok:
                    # Keep for next retry
                    self.pending_stat_entries.extend(written_entries)

            # Update performance stats
            with self.queue_stats_lock:
                self.queue_stats["total_processed"] += len(batch)
                self.queue_stats["last_flush_size"] = len(batch)
                self.queue_stats["last_flush_time"] = time.time()
                self.queue_stats["flush_durations"].append(time.time() - start)
                if len(self.queue_stats["flush_durations"]) > 100:
                    self.queue_stats["flush_durations"].pop(0)

        except Exception as e:
            logger.error(f"[monitor] Flush batch failed: {e}")
            # Restore batch on failure to prevent data loss
            with self.batch_lock:
                self.current_batch.extend(batch)

    # ---------------- Public API ----------------

    def log_metric(self, metric_data: Dict[str, Any]):
        """
        Enqueue metric data. Never blocks on writer.
        Ensures background thread is running.
        """
        if self._shutting_down:
            logger.warning("[monitor] Writer is shutting down, ignoring new metric")
            return

        self._ensure_thread()

        # Update activity timestamp and enqueue
        self._update_activity()
        self.queue.put(metric_data)

        # Update stats
        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1
            current_size = self.queue.qsize()
            self.queue_stats["current_size"] = current_size
            self.queue_stats["max_size"] = max(self.queue_stats["max_size"], current_size)

    def request_flush(self):
        """Request immediate flush (for testing or shutdown)."""
        self.flush_requested.set()

    # ---------------- Thread lifecycle ----------------

    def _write_loop(self):
        """
        Main writer loop with minute cadence and auto-stop after 10 seconds of inactivity.
        Always performs final flush before stopping.
        """
        logger.debug(
            f"[monitor] dequeue thread RUNNING for {self.organization}/{self.super_name}/{self.monitor_type}"
        )

        last_flush = time.time()

        try:
            while not self.stop_event.is_set():
                now = time.time()
                idle_time = self._get_idle_time()

                # Check if we should flush (time-based, request-based, or shutdown)
                time_due = (now - last_flush) >= self.flush_interval
                should_flush = self.flush_requested.is_set() or time_due or idle_time >= self.idle_stop_after

                # Always drain queue first
                drained = self._drain_queue_to_batch()
                if drained > 0:
                    self._update_activity()  # Reset idle timer

                # Process batch if we have data and should flush
                if self.current_batch and should_flush:
                    self._flush_batch()
                    last_flush = now
                    self.flush_requested.clear()

                # Check if we should stop due to inactivity
                if idle_time >= self.idle_stop_after:
                    # One final drain and flush to catch any last items
                    final_drained = self._drain_queue_to_batch()
                    if final_drained > 0:
                        self._update_activity()
                        # If we got new items, flush them immediately
                        if self.current_batch:
                            self._flush_batch()

                    # Only stop if queue is completely empty after final drain
                    if self.queue.empty() and not self.current_batch and not self.pending_stat_entries:
                        logger.info(
                            f"[monitor] Auto-stopping after {idle_time:.1f}s inactivity for "
                            f"{self.organization}/{self.super_name}/{self.monitor_type}"
                        )
                        break

                # Sleep to reduce CPU usage (shorter sleep when nearing idle timeout)
                sleep_time = 0.1 if idle_time < self.idle_stop_after - 1 else 0.01
                time.sleep(sleep_time)

        except Exception as e:
            logger.error(f"[monitor] Unexpected error in write loop: {e}")
        finally:
            # GUARANTEED FINAL FLUSH: Ensure all data is processed before exit
            self._perform_final_flush()

    def _perform_final_flush(self):
        """Perform final flush ensuring all data is written before thread exit."""
        try:
            logger.info(
                f"[monitor] Performing final flush for {self.organization}/{self.super_name}/{self.monitor_type}"
            )

            # Drain any remaining items from queue
            remaining_in_queue = self._drain_queue_to_batch()

            # Final flush if we have any data
            if self.current_batch or self.pending_stat_entries:
                logger.info(
                    f"[monitor] Final flush processing: "
                    f"batch_size={len(self.current_batch)}, pending_stats={len(self.pending_stat_entries)}"
                )
                self._flush_batch()

            logger.info(
                f"[monitor] Final flush completed for {self.organization}/{self.super_name}/{self.monitor_type}: "
                f"processed_final_items={remaining_in_queue}"
            )

        except Exception as e:
            logger.error(f"[monitor] Error during final flush: {e}")
        finally:
            logger.debug(
                f"[monitor] dequeue thread EXITED for {self.organization}/{self.super_name}/{self.monitor_type}"
            )
            # Clean up singleton reference when thread exits naturally
            self._cleanup_singleton()

    def _cleanup_singleton(self):
        """Clean up singleton reference safely."""
        with _MONITOR_INSTANCES_LOCK:
            key = (self.organization, self.super_name, self.monitor_type)
            try:
                if key in _MONITOR_INSTANCES:
                    del _MONITOR_INSTANCES[key]
            except KeyError:
                pass  # Key might have been removed already

    def close(self, force_flush: bool = False):
        """
        Graceful shutdown with optional forced flush.
        """
        self._shutting_down = True

        if force_flush:
            self.request_flush()
            # Give it a moment to process
            time.sleep(0.5)

        self.stop_event.set()

        if self._writer_thread and self._writer_thread.is_alive():
            self._writer_thread.join(timeout=5.0)  # Wait up to 5 seconds for clean shutdown

        # Clean up singleton reference
        self._cleanup_singleton()