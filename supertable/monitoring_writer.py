# supertable/monitoring_writer.py
import json
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
from supertable.super_table import SuperTable  # kept (even if unused elsewhere)
from supertable.storage.storage_factory import get_storage

# ---------------- Singleton registry so producers don't block or fight threads ----------------

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
    Lock-free, queue-based monitoring:
      * Producers call log_metric(record) (non-blocking except queue.put)
      * A background thread drains the queue on interval and writes Parquet
      * Files are written into Hive-style minute partitions:
            .../monitoring/<type>/data/year=YYYY/month=MM/day=DD/hour=HH/min=mm/<ts>_<id>_data.parquet
      * A lightweight snapshot + compact catalog JSON are maintained.
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        monitor_type: str,
        max_rows_per_file: int = 5000,
        flush_interval: float = 1.0,
        compression: str = "zstd",
        compression_level: int = 1,
    ):
        self.identity = "monitoring"
        self.super_name = super_name
        self.organization = organization
        self.monitor_type = monitor_type

        self.max_rows_per_file = max_rows_per_file
        self.flush_interval = flush_interval
        self.compression = compression
        self.compression_level = compression_level
        self.storage = get_storage()

        # Initialize paths
        self.base_dir = os.path.join(self.organization, self.super_name, self.identity, self.monitor_type)
        self.data_dir = os.path.join(self.base_dir, "data")
        self.snapshots_dir = os.path.join(self.base_dir, "snapshots")
        self.catalog_path = os.path.join(self.organization, self.super_name, f"_{self.monitor_type}.json")

        # Initialize state
        self.queue = queue.Queue()
        self.current_batch: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.has_data_event = threading.Event()

        # Queue monitoring stats
        self.queue_stats = {
            "total_received": 0,
            "total_processed": 0,
            "current_size": 0,
            "max_size": 0,
            "last_flush_time": 0,
            "flush_durations": [],
            "last_flush_size": 0,
            "start_time": time.time(),
        }
        self.queue_stats_lock = threading.Lock()

        # Ensure directories exist in the configured storage (MinIO, local, etc.)
        self.storage.makedirs(self.base_dir)
        self.storage.makedirs(self.data_dir)
        self.storage.makedirs(self.snapshots_dir)

        # Load or initialize catalog
        self.catalog = self._load_or_init_catalog()

        # Start background writer (daemon → won't block process exit)
        self.writer_thread = threading.Thread(
            target=self._write_loop,
            name=f"MonitoringWriter-{monitor_type}",
            daemon=True,
        )
        self.writer_thread.start()

    # ---------------- Catalog helpers ----------------

    def _load_or_init_catalog(self) -> Dict[str, Any]:
        if self.storage.exists(self.catalog_path):
            return self.storage.read_json(self.catalog_path)
        return {
            "current": None,
            "previous": None,
            "last_updated_ms": 0,
            "file_count": 0,
            "total_rows": 0,
            "total_file_size": 0,
            "version": 1,
        }

    def _save_catalog(self):
        self.storage.write_json(self.catalog_path, self.catalog)

    def _generate_filename(self, prefix: str) -> str:
        timestamp = int(time.time() * 1000)
        unique_hash = uuid.uuid4().hex[:16]
        return f"{timestamp}_{unique_hash}_{prefix}"

    def _create_snapshot(self, resources: List[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
        snapshot_id = self._generate_filename(f"{self.monitor_type}.json")
        snapshot_path = os.path.join(self.snapshots_dir, snapshot_id)

        snapshot = {
            "snapshot_version": self.catalog["version"] + 1,
            "last_updated_ms": int(time.time() * 1000),
            "resources": resources,
        }

        self.storage.write_json(snapshot_path, snapshot)
        return snapshot_path, snapshot

    # ---------------- Partitioning helpers ----------------

    @staticmethod
    def _ensure_execution_time(record: Dict[str, Any]) -> Dict[str, Any]:
        if "execution_time" not in record:
            record["execution_time"] = int(datetime.now(timezone.utc).timestamp() * 1_000)
        return record

    @staticmethod
    def _partition_from_ts(ts_ms: int) -> Dict[str, str]:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return {
            "year": f"{dt.year:04d}",
            "month": f"{dt.month:02d}",
            "day": f"{dt.day:02d}",
            "hour": f"{dt.hour:02d}",
            "min": f"{dt.minute:02d}",
        }

    def _partition_dir_for_ts(self, ts_ms: int) -> str:
        parts = self._partition_from_ts(ts_ms)
        return os.path.join(
            self.data_dir,
            f"year={parts['year']}",
            f"month={parts['month']}",
            f"day={parts['day']}",
            f"hour={parts['hour']}",
            f"min={parts['min']}",
        )

    # ---------------- IO helpers ----------------

    def _read_existing_parquet_to_df(self, path: str) -> Optional[pl.DataFrame]:
        try:
            if not self.storage.exists(path):
                return None
            if hasattr(self.storage, "read_parquet"):
                table = self.storage.read_parquet(path)
                if isinstance(table, pa.Table):
                    return pl.from_arrow(table)
            if hasattr(self.storage, "read_bytes"):
                data = self.storage.read_bytes(path)
                table = pq.read_table(io.BytesIO(data))
                return pl.from_arrow(table)
            return pl.read_parquet(path)
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def _write_parquet_bytes_via_storage(self, table: pa.Table, path: str):
        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=self.compression,
            compression_level=int(self.compression_level),
            use_dictionary=True,
            write_statistics=True,
        )
        data = buf.getvalue()
        if hasattr(self.storage, "write_bytes"):
            self.storage.write_bytes(path, data)
        elif hasattr(self.storage, "write_parquet"):
            self.storage.write_parquet(table, path)
        else:
            pq.write_table(table, path, compression=self.compression, compression_level=int(self.compression_level))

    def _write_parquet_file(
        self,
        data: List[Dict[str, Any]],
        partition_dir: str,
    ) -> Dict[str, Any]:
        if not data:
            return {"file": "", "file_size": 0, "rows": 0, "columns": 0, "stats": {}}

        data = [self._ensure_execution_time(r) for r in data]
        df = pl.from_dicts(data)

        # Ensure partition directory exists on storage
        self.storage.makedirs(partition_dir)

        new_filename = self._generate_filename("data.parquet")
        new_path = os.path.join(partition_dir, new_filename)

        table = df.to_arrow()
        try:
            self._write_parquet_bytes_via_storage(table, new_path)
        except Exception:
            pq.write_table(table, new_path, compression=self.compression, compression_level=int(self.compression_level))

        try:
            size = self.storage.size(new_path)
        except Exception:
            try:
                size = os.path.getsize(new_path)
            except Exception:
                size = 0

        # compute min/max execution_time for pruning
        stats = {}
        if "execution_time" in df.columns:
            try:
                ts_col = df["execution_time"].cast(pl.Int64)
                stats["execution_time"] = {"min": int(ts_col.min()), "max": int(ts_col.max())}
            except Exception:
                stats["execution_time"] = {"min": "unknown", "max": "unknown"}

        return {
            "file": new_path,
            "file_size": int(size),
            "rows": int(df.height),
            "columns": len(df.columns),
            "stats": stats,
        }

    # ---------------- Flush logic ----------------

    def _flush_batch(self, force: bool = False):
        if not self.current_batch and not force:
            return

        with self.lock:
            if not self.current_batch and not force:
                return

            # Start from current snapshot resources
            current_resources: List[Dict[str, Any]] = []
            current_snapshot_path = self.catalog.get("current")
            if current_snapshot_path and self.storage.exists(current_snapshot_path):
                try:
                    current_snapshot = self.storage.read_json(current_snapshot_path)
                    current_resources = current_snapshot.get("resources", [])
                except Exception:
                    pass

            # Take the batch
            batch = self.current_batch
            self.current_batch = []

            # Group records by minute-level partition
            # NOTE: if a record lacks execution_time, we add it (UTC now) so partitioning is deterministic
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for rec in (self._ensure_execution_time(r) for r in batch):
                pdir = self._partition_dir_for_ts(int(rec["execution_time"]))
                grouped.setdefault(pdir, []).append(rec)

            # Write each partition's chunk in new files (simple, no cross-file merging)
            new_resources: List[Dict[str, Any]] = []
            for pdir, rows in grouped.items():
                # write in chunks of max_rows_per_file
                start = 0
                n = len(rows)
                while start < n:
                    end = min(start + self.max_rows_per_file, n)
                    new_resources.append(self._write_parquet_file(rows[start:end], pdir))
                    start = end

            # Keep old resources + append new
            resources = (current_resources or []) + new_resources

            # New snapshot & catalog
            snapshot_path, snap = self._create_snapshot(resources)
            self._update_catalog(snapshot_path, current_snapshot_path, snap)

            with self.queue_stats_lock:
                self.queue_stats["total_processed"] += len(batch)
                self.queue_stats["last_flush_size"] = len(batch)
                self.queue_stats["last_flush_time"] = time.time()

    def _update_catalog(self, snapshot_path: str, current_snapshot_path: Optional[str], new_snapshot: Dict[str, Any]):
        resources = new_snapshot.get("resources", [])
        self.catalog.update(
            {
                "current": snapshot_path,
                "previous": current_snapshot_path,
                "last_updated_ms": new_snapshot.get("last_updated_ms", 0),
                "file_count": len(resources),
                "total_rows": sum(int(r.get("rows", 0)) for r in resources),
                "total_file_size": sum(int(r.get("file_size", 0)) for r in resources),
                "version": int(new_snapshot.get("snapshot_version", 0)),
            }
        )
        self._save_catalog()

    # ---------------- Public API ----------------

    def log_metric(self, metric_data: Dict[str, Any]):
        self.queue.put(metric_data)
        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1
            current_size = self.queue.qsize()
            self.queue_stats["current_size"] = current_size
            self.queue_stats["max_size"] = max(self.queue_stats["max_size"], current_size)
        self.has_data_event.set()

    def get_queue_stats(self) -> Dict[str, Any]:
        with self.queue_stats_lock:
            stats = self.queue_stats.copy()
            stats["current_size"] = self.queue.qsize()
            if stats["flush_durations"]:
                stats["avg_flush_duration"] = sum(stats["flush_durations"]) / len(stats["flush_durations"])
            else:
                stats["avg_flush_duration"] = 0
            uptime = time.time() - stats["start_time"]
            stats["processing_rate"] = stats["total_processed"] / uptime if uptime > 0 else 0
            return stats

    def get_queue_health(self) -> Dict[str, Any]:
        stats = self.get_queue_stats()
        return {
            "status": "healthy" if stats["current_size"] < self.max_rows_per_file else "backlogged",
            "backlog": stats["current_size"],
            "processing_rate": stats["processing_rate"],
            "estimated_time_to_clear": (
                stats["current_size"] / stats["processing_rate"] if stats["processing_rate"] > 0 else float("inf")
            ),
            "last_flush": {
                "time": stats["last_flush_time"],
                "duration": stats["flush_durations"][-1] if stats["flush_durations"] else 0,
                "items_processed": stats["last_flush_size"],
            },
            "totals": {
                "received": stats["total_received"],
                "processed": stats["total_processed"],
                "uptime_seconds": time.time() - stats["start_time"],
            },
        }

    # ---------------- Thread lifecycle ----------------

    def _write_loop(self):
        while not self.stop_event.is_set():
            self.has_data_event.wait(timeout=self.flush_interval)

            start_flush = time.time()
            drained = 0

            # Drain queue quickly (no storage IO under this loop)
            while True:
                try:
                    item = self.queue.get_nowait()
                    self.current_batch.append(item)
                    drained += 1
                except queue.Empty:
                    break

            if self.current_batch:
                self._flush_batch()
                self.has_data_event.clear()
                with self.queue_stats_lock:
                    flush_duration = time.time() - start_flush
                    self.queue_stats["flush_durations"].append(flush_duration)
                    if len(self.queue_stats["flush_durations"]) > 100:
                        self.queue_stats["flush_durations"].pop(0)
            else:
                self.has_data_event.clear()

        # Final forced flush on shutdown request
        try:
            self._flush_batch(force=True)
        except Exception:
            pass

    def close(self):
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.has_data_event.set()
            # Daemon thread; no strict join needed
