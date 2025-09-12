import json
import time
import threading
import queue
import atexit
import uuid
import os
from typing import Dict, List, Any, Optional
import pyarrow as pa
import polars as pl
from datetime import datetime, timezone
from supertable.super_table import SuperTable
from supertable.storage.storage_factory import get_storage


class MonitoringLogger:
    def __init__(
            self,
            super_name: str,
            organization: str,
            monitor_type: str,
            max_rows_per_file: int = 5000,
            flush_interval: float = 1.0,
            compression: str = "zstd",
            compression_level: int = 1
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
            'total_received': 0,
            'total_processed': 0,
            'current_size': 0,
            'max_size': 0,
            'last_flush_time': 0,
            'flush_durations': [],
            'last_flush_size': 0,
            'start_time': time.time()
        }
        self.queue_stats_lock = threading.Lock()

        # Ensure directories exist
        self.storage.makedirs(self.base_dir)
        self.storage.makedirs(self.data_dir)
        self.storage.makedirs(self.snapshots_dir)

        # Load or initialize catalog
        self.catalog = self._load_or_init_catalog()

        # Start background writer (daemon → won't block process exit)
        self.writer_thread = threading.Thread(
            target=self._write_loop,
            name=f"MonitoringWriter-{monitor_type}",
            daemon=True
        )
        self.writer_thread.start()

        # Ensure graceful shutdown without relying on open() during teardown
        atexit.register(self._safe_close)

    def _safe_close(self):
        try:
            self.close()
        except Exception:
            # Avoid noisy errors during interpreter shutdown
            pass

    def _load_or_init_catalog(self) -> Dict[str, Any]:
        """Load existing catalog or initialize a new one."""
        if self.storage.exists(self.catalog_path):
            return self.storage.read_json(self.catalog_path)
        return {
            "current": None,
            "previous": None,
            "last_updated_ms": 0,
            "file_count": 0,
            "total_rows": 0,
            "total_file_size": 0,
            "version": 1
        }

    def _save_catalog(self):
        """Save the catalog with file locking."""
        self.storage.write_json(self.catalog_path, self.catalog)

    def _generate_filename(self, prefix: str) -> str:
        """Generate a unique filename with timestamp and hash."""
        timestamp = int(time.time() * 1000)
        unique_hash = uuid.uuid4().hex[:16]
        return f"{timestamp}_{unique_hash}_{prefix}"

    def _create_snapshot(self, resources: List[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
        """Create a new snapshot metadata file."""
        snapshot_id = self._generate_filename(f"{self.monitor_type}.json")
        snapshot_path = os.path.join(self.snapshots_dir, snapshot_id)

        snapshot = {
            "snapshot_version": self.catalog["version"] + 1,
            "last_updated_ms": int(time.time() * 1000),
            "resources": resources
        }

        self.storage.write_json(snapshot_path, snapshot)
        return snapshot_path, snapshot

    def _write_parquet_file(self, data: List[Dict[str, Any]], existing_path: Optional[str] = None) -> Dict[str, Any]:
        """Write data to a new or existing Parquet file."""
        if not data:
            # Return a no-op resource to keep caller logic simple
            return {
                "file": existing_path or "",
                "file_size": 0,
                "rows": 0,
                "columns": 0,
                "stats": {}
            }

        data = [self._ensure_execution_time(record) for record in data]
        df = pl.from_dicts(data)

        if existing_path and self.storage.exists(existing_path):
            try:
                existing_df = pl.read_parquet(existing_path)
                df = pl.concat([existing_df, df], how="vertical_relaxed")
                self.storage.delete(existing_path)
            except Exception as e:
                print(f"Warning: Failed to merge with existing file {existing_path}: {str(e)}")

        new_filename = self._generate_filename("data.parquet")
        new_path = os.path.join(self.data_dir, new_filename)

        # Convert to pyarrow table and write using storage interface
        table = df.to_arrow()
        # (Storage interface handles details; compression may be ignored if unsupported)
        try:
            self.storage.write_parquet(table, new_path)
        except TypeError:
            # Fallback if storage doesn't accept pyarrow Table
            # Write via pyarrow directly to a local path, then upload if needed
            import pyarrow.parquet as pq
            pq.write_table(table, new_path, compression="zstd")

        return {
            "file": new_path,
            "file_size": self.storage.size(new_path),
            "rows": len(df),
            "columns": len(df.columns),
            "stats": self._calculate_stats(df)
        }

    def _calculate_stats(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calculate min/max of execution_time as integer ms timestamps."""
        stats: Dict[str, Any] = {}
        if "execution_time" in df.columns:
            try:
                ts_col = df["execution_time"].cast(pl.Int64)
                min_ms = int(ts_col.min())
                max_ms = int(ts_col.max())
                stats["execution_time"] = {"min": min_ms, "max": max_ms}
            except Exception:
                stats["execution_time"] = {"min": "unknown", "max": "unknown"}
        return stats

    def _flush_batch(self, force: bool = False):
        if not self.current_batch and not force:
            return

        with self.lock:
            if not self.current_batch and not force:
                return

            # Initialize resources from current snapshot
            current_resources: List[Dict[str, Any]] = []
            current_snapshot = None
            current_snapshot_path = self.catalog["current"]
            if current_snapshot_path and self.storage.exists(current_snapshot_path):
                try:
                    current_snapshot = self.storage.read_json(current_snapshot_path)
                    current_resources = current_snapshot.get("resources", [])
                except Exception as e:
                    print(f"Warning: Failed to load current snapshot: {str(e)}")

            processed_data = self.current_batch
            n = len(processed_data)
            self.current_batch = []
            new_resources: List[Dict[str, Any]] = []

            # Fill partially-full files first
            for resource in current_resources:
                if processed_data and resource.get("rows", 0) < self.max_rows_per_file:
                    remaining = self.max_rows_per_file - int(resource.get("rows", 0))
                    if remaining > 0:
                        chunk = processed_data[:remaining]
                        processed_data = processed_data[remaining:]
                        merged_resource = self._write_parquet_file(chunk, resource.get("file"))
                        new_resources.append(merged_resource)
                        continue
                # keep as-is (full or not touched)
                new_resources.append(resource)

            # Create new files for remaining data
            while processed_data:
                chunk_size = min(len(processed_data), self.max_rows_per_file)
                chunk = processed_data[:chunk_size]
                processed_data = processed_data[chunk_size:]
                new_resources.append(self._write_parquet_file(chunk))

            # Create new snapshot
            snapshot_path, new_snapshot = self._create_snapshot(new_resources)
            self._update_catalog(snapshot_path, current_snapshot_path, new_snapshot)

            # Update queue stats after successful flush
            with self.queue_stats_lock:
                self.queue_stats["total_processed"] += n
                self.queue_stats["last_flush_size"] = n
                self.queue_stats['last_flush_time'] = time.time()

    def _update_catalog(self, snapshot_path: str, current_snapshot_path: str, new_snapshot: Dict[str, Any]):
        """Update the catalog with new snapshot information."""
        resources = new_snapshot.get("resources", [])
        self.catalog.update({
            "current": snapshot_path,
            "previous": current_snapshot_path,
            "last_updated_ms": new_snapshot.get("last_updated_ms", 0),
            "file_count": len(resources),
            "total_rows": sum(int(r.get("rows", 0)) for r in resources),
            "total_file_size": sum(int(r.get("file_size", 0)) for r in resources),
            "version": int(new_snapshot.get("snapshot_version", 0))
        })
        self._save_catalog()

    def _ensure_execution_time(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add UTC timestamp in milliseconds if execution_time is missing."""
        if "execution_time" not in record:
            record["execution_time"] = int(datetime.now(timezone.utc).timestamp() * 1_000)
        return record

    def log_metric(self, metric_data: Dict[str, Any]):
        """Add metric data to the queue and update stats."""
        self.queue.put(metric_data)
        with self.queue_stats_lock:
            self.queue_stats['total_received'] += 1
            current_size = self.queue.qsize()
            self.queue_stats['current_size'] = current_size
            self.queue_stats['max_size'] = max(self.queue_stats['max_size'], current_size)
        self.has_data_event.set()

    def _write_loop(self):
        """Main writer loop that processes the queue."""
        while not self.stop_event.is_set():
            # Wait up to flush_interval or until data arrives
            self.has_data_event.wait(timeout=self.flush_interval)

            start_flush = time.time()
            drained = 0

            # Drain queue quickly without holding the write lock
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
                    self.queue_stats['flush_durations'].append(flush_duration)
                    if len(self.queue_stats['flush_durations']) > 100:
                        self.queue_stats['flush_durations'].pop(0)
            else:
                # Nothing to do, reset event
                self.has_data_event.clear()

        # Final forced flush on shutdown request
        try:
            self._flush_batch(force=True)
        except Exception:
            # Don't crash on shutdown
            pass

    def get_queue_stats(self) -> Dict[str, Any]:
        """Return current queue statistics."""
        with self.queue_stats_lock:
            stats = self.queue_stats.copy()
            stats['current_size'] = self.queue.qsize()

            # Calculate averages
            if stats['flush_durations']:
                stats['avg_flush_duration'] = sum(stats['flush_durations']) / len(stats['flush_durations'])
            else:
                stats['avg_flush_duration'] = 0

            uptime = time.time() - stats['start_time']
            stats['processing_rate'] = (
                stats['total_processed'] / uptime if uptime > 0 else 0
            )

            return stats

    def get_queue_health(self) -> Dict[str, Any]:
        """Return a health assessment of the queue."""
        stats = self.get_queue_stats()
        return {
            'status': 'healthy' if stats['current_size'] < self.max_rows_per_file else 'backlogged',
            'backlog': stats['current_size'],
            'processing_rate': stats['processing_rate'],
            'estimated_time_to_clear': (
                stats['current_size'] / stats['processing_rate']
                if stats['processing_rate'] > 0
                else float('inf')
            ),
            'last_flush': {
                'time': stats['last_flush_time'],
                'duration': stats['flush_durations'][-1] if stats['flush_durations'] else 0,
                'items_processed': stats['last_flush_size']
            },
            'totals': {
                'received': stats['total_received'],
                'processed': stats['total_processed'],
                'uptime_seconds': time.time() - stats['start_time']
            }
        }

    def _emergency_flush(self):
        """(Kept for backward compat; prefer _safe_close/close)"""
        try:
            if not self.stop_event.is_set():
                self._flush_batch(force=True)
        except Exception:
            pass

    def close(self):
        """Stop the writer and flush remaining data."""
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.has_data_event.set()
            # Join briefly; thread is daemon so process can still exit if it lingers
            self.writer_thread.join(timeout=2.0)
            if self.writer_thread.is_alive():
                # Best-effort final flush
                try:
                    self._flush_batch(force=True)
                except Exception:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
