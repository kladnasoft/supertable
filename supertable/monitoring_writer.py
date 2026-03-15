# supertable/monitoring_writer.py
"""
Monitoring writer/logger (enqueue-only).

Backward compatibility
----------------------
Your codebase uses multiple access patterns:

1) Class import:
    from supertable.monitoring_writer import MonitoringWriter

2) Function:
    from supertable.monitoring_writer import get_monitoring_logger

3) Examples expect extra attributes on the returned logger, e.g.:
    monitor.queue_stats
    monitor.queue_stats_lock
    monitor.queue
    monitor.current_batch
    monitor.request_flush()

See examples/2.4.2. write_monitoring_parallel.py. fileciteturn2file0

Fixes included
--------------
- MonitoringWriter is a proper context manager (__enter__/__exit__)
- get_monitoring_logger(...) ALWAYS returns an object that:
    - supports context manager
    - has log_metric(payload)
    - has request_flush(...)
    - exposes queue/current_batch/queue_stats/queue_stats_lock for debug/monitor scripts
- When monitoring is disabled/misconfigured, we return NullMonitoringLogger (no-op)
  so monitoring never breaks query execution.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

from supertable.config.defaults import logger

try:
    # Optional dependency. If Redis isn't available/configured, we fall back to log-only.
    from supertable.redis_connector import RedisConnector
except Exception:  # pragma: no cover
    RedisConnector = None  # type: ignore


class MonitoringLogger(Protocol):
    # Methods expected everywhere
    def log_metric(self, payload: Dict[str, Any]) -> None: ...
    def request_flush(self, timeout_s: float = 2.0) -> None: ...
    def __enter__(self) -> "MonitoringLogger": ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...

    # Debug attributes expected by some examples/scripts
    queue: "queue.Queue[Dict[str, Any]]"
    current_batch: list
    queue_stats: Dict[str, int]
    queue_stats_lock: threading.Lock


class NullMonitoringLogger:
    """No-op logger that is also a context manager (and exposes debug fields)."""

    def __init__(self) -> None:
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.current_batch: list = []
        self.queue_stats: Dict[str, int] = {
            "total_received": 0,
            "total_processed": 0,
            "total_dropped": 0,
            "current_size": 0,
        }
        self.queue_stats_lock = threading.Lock()

    def log_metric(self, payload: Dict[str, Any]) -> None:
        # Still track "received" so debug scripts can run without crashing.
        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1

    def request_flush(self, timeout_s: float = 2.0) -> None:
        return None

    def __enter__(self) -> "NullMonitoringLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


@dataclass(frozen=True)
class _MonitorKey:
    organization: str
    super_name: str
    monitor_type: str

    @property
    def path_key(self) -> str:
        # Matches your log style: "org/super/monitor_type"
        return f"{self.organization}/{self.super_name}/{self.monitor_type}"

    @property
    def redis_list_key(self) -> str:
        return f"monitor:{self.organization}:{self.super_name}:{self.monitor_type}"


class _AsyncMonitoringLogger:
    """
    Enqueue-only monitoring logger.

    - log_metric(payload) -> puts payload into a bounded queue (never blocks)
    - background thread drains and ships payloads to Redis list if available; else logs debug
    - request_flush() drains any queued items synchronously (best-effort)
    - exposes debug stats/locks/queue fields used by examples
    """

    def __init__(
        self,
        key: _MonitorKey,
        *,
        redis_connector: Optional["RedisConnector"] = None,
        max_queue: int = 10_000,
        ship_to_redis: bool = True,
        batch_max: int = 200,
        batch_wait_s: float = 0.05,
    ):
        self._key = key

        # public debug fields expected by examples
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=max_queue)
        self.current_batch: list = []
        self.queue_stats: Dict[str, int] = {
            "total_received": 0,
            "total_processed": 0,
            "total_dropped": 0,
            "current_size": 0,
        }
        self.queue_stats_lock = threading.Lock()

        # internal controls
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ship_lock = threading.Lock()  # prevents request_flush() shipping concurrently with worker
        self._batch_max = int(batch_max)
        self._batch_wait_s = float(batch_wait_s)

        self._ship_to_redis = ship_to_redis
        self._redis = redis_connector
        if self._redis is None and RedisConnector is not None:
            try:
                self._redis = RedisConnector()
            except Exception as e:
                logger.debug(f"[monitor] redis unavailable for {self._key.path_key}: {e}")
                self._redis = None

        self._start_worker()

    # --- context manager support ---
    def __enter__(self) -> "_AsyncMonitoringLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Best-effort flush so metrics enqueued inside the `with` block
        # are shipped to Redis before the scope closes.  This is critical
        # for short-lived processes (CLI, Lambda) where the daemon thread
        # would be killed on exit before it gets to drain the queue.
        #
        # We use _try_flush (non-blocking lock acquire) rather than
        # request_flush to avoid blocking the caller for up to 50ms when
        # the worker thread is in its batch-formation window holding
        # _ship_lock.  If the lock is busy, the worker is actively
        # shipping — the item will be delivered without our help.
        try:
            self._try_flush()
        except Exception:
            pass
        return False

    # --- public API ---
    def log_metric(self, payload: Dict[str, Any]) -> None:
        """
        Enqueue a metric. Never blocks the caller; if queue is full, drop the metric.
        """
        if "recorded_at" not in payload:
            payload = dict(payload)
            payload["recorded_at"] = time.time()

        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1

        try:
            self.queue.put_nowait(payload)
        except queue.Full:
            with self.queue_stats_lock:
                self.queue_stats["total_dropped"] += 1
                self.queue_stats["current_size"] = self.queue.qsize()
            logger.warning(f"[monitor] queue full; dropping metric for {self._key.path_key}")
            return

        with self.queue_stats_lock:
            self.queue_stats["current_size"] = self.queue.qsize()

    def request_flush(self, timeout_s: float = 2.0) -> None:
        """
        Best-effort synchronous flush.

        Drains the current queue and ships items in the caller thread.

        It does NOT guarantee flushing an item already dequeued by the background worker,
        but the ship lock ensures we don't double-ship.
        """
        deadline = time.time() + max(0.0, float(timeout_s))

        # Block worker shipping while we drain & ship queued items.
        with self._ship_lock:
            batch = self._drain_batch(deadline=deadline, allow_wait=False)
            if not batch:
                return
            self._set_current_batch(batch)
            try:
                self._ship_batch(batch)
            finally:
                self._clear_current_batch()

        with self.queue_stats_lock:
            self.queue_stats["current_size"] = self.queue.qsize()

    def _try_flush(self) -> None:
        """
        Low-contention flush used by __exit__.

        Attempts a non-blocking lock acquire first.  If the worker is
        currently holding _ship_lock, falls back to a short blocking
        wait (100ms) — just long enough for the worker to finish its
        current batch without adding meaningful latency to the caller.

        This avoids the two failure modes:
          - Blocking request_flush(2s) injected 50ms+ latency every call.
          - Pure non-blocking skip lost metrics in short-lived processes
            (CLI, Lambda) when the worker held the lock at exit time.
        """
        acquired = self._ship_lock.acquire(blocking=True, timeout=0.1)
        if not acquired:
            # Worker is mid-ship and slower than 100ms (e.g., Redis latency).
            # In a long-lived process the daemon will deliver it.
            return
        try:
            batch = self._drain_batch(deadline=time.time() + 0.5, allow_wait=False)
            if not batch:
                return
            self._set_current_batch(batch)
            try:
                self._ship_batch(batch)
            finally:
                self._clear_current_batch()
        finally:
            self._ship_lock.release()

        with self.queue_stats_lock:
            self.queue_stats["current_size"] = self.queue.qsize()

    # --- internals ---
    def _start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._worker,
            name=f"monitor:{self._key.path_key}",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"[monitor] started dequeue thread for {self._key.path_key}")

    def _worker(self) -> None:
        backoff = 0.1
        while not self._stop.is_set():
            try:
                # Block until we have at least 1 item.
                first = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                with self._ship_lock:
                    batch = [first]
                    batch.extend(self._drain_more(max_items=self._batch_max - 1, wait_s=self._batch_wait_s))
                    self._set_current_batch(batch)
                    try:
                        self._ship_batch(batch)
                    finally:
                        self._clear_current_batch()
                backoff = 0.1
            except Exception as e:
                logger.warning(f"[monitor] ship failed (non-fatal) for {self._key.path_key}: {e}")
                time.sleep(backoff)
                backoff = min(5.0, backoff * 2.0)
            finally:
                with self.queue_stats_lock:
                    self.queue_stats["current_size"] = self.queue.qsize()

    def _drain_more(self, *, max_items: int, wait_s: float) -> list:
        """
        Drain additional items (best-effort) up to max_items, waiting briefly to form a batch.
        """
        items = []
        if max_items <= 0:
            return items

        # Small wait: allows parallel writers to contribute to the same batch.
        if wait_s > 0:
            end = time.time() + wait_s
            while time.time() < end and len(items) < max_items:
                try:
                    items.append(self.queue.get_nowait())
                except queue.Empty:
                    time.sleep(0.001)

        while len(items) < max_items:
            try:
                items.append(self.queue.get_nowait())
            except queue.Empty:
                break

        return items

    def _drain_batch(self, *, deadline: float, allow_wait: bool) -> list:
        batch = []
        while time.time() <= deadline:
            try:
                batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
            if len(batch) >= self._batch_max:
                break
            if allow_wait:
                time.sleep(0.001)
        return batch

    def _set_current_batch(self, batch: list) -> None:
        with self.queue_stats_lock:
            self.current_batch = list(batch)

    def _clear_current_batch(self) -> None:
        with self.queue_stats_lock:
            self.current_batch = []

    def _ship_batch(self, batch: list) -> None:
        """
        Ship a batch.  Only counts items as processed after successful delivery.
        On total failure (Redis down, fallback also fails), items are counted as
        dropped so stats remain accurate.
        """
        shipped = 0
        if self._ship_to_redis and self._redis is not None:
            try:
                pipe = self._redis.r.pipeline()
                for payload in batch:
                    s = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                    pipe.rpush(self._key.redis_list_key, s)
                pipe.execute()
                shipped = len(batch)
            except Exception:
                # Pipeline failed — fall back to per-item shipping.
                for payload in batch:
                    try:
                        self._ship_one(payload)
                        shipped += 1
                    except Exception:
                        pass
        else:
            for payload in batch:
                try:
                    self._ship_one(payload)
                    shipped += 1
                except Exception:
                    pass

        failed = len(batch) - shipped
        with self.queue_stats_lock:
            self.queue_stats["total_processed"] += shipped
            if failed > 0:
                self.queue_stats["total_dropped"] += failed

    def _ship_one(self, payload: Dict[str, Any]) -> None:
        if not self._ship_to_redis or self._redis is None:
            logger.debug(f"[monitor] {self._key.path_key} metric: {payload}")
            return

        s = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        # RedisConnector in your code exposes "r" as the redis client.
        self._redis.r.rpush(self._key.redis_list_key, s)


# ---- singleton cache (one worker per key) ----
# Bounded to prevent unbounded thread/Redis-connection growth in multi-tenant
# environments.  When the cache is full, the least-recently-used entry is
# evicted (its daemon thread stops on the next loop iteration via _stop event).
_MONITORS: Dict[str, MonitoringLogger] = {}
_MONITORS_LOCK = threading.Lock()
_MONITORS_MAX = int(os.getenv("SUPERTABLE_MONITOR_CACHE_MAX", "256"))


def _evict_oldest_monitor() -> None:
    """
    Evict one entry from _MONITORS.  Must be called with _MONITORS_LOCK held.

    Strategy: evict the first key (oldest insertion in dict-order, Python 3.7+).
    Signal the evicted logger's worker thread to stop so it doesn't leak.
    """
    if not _MONITORS:
        return
    oldest_key = next(iter(_MONITORS))
    evicted = _MONITORS.pop(oldest_key)
    # Signal the background worker to exit its loop gracefully.
    stop_event = getattr(evicted, "_stop", None)
    if stop_event is not None:
        stop_event.set()
    logger.debug(f"[monitor] evicted cached logger for {oldest_key}")


def _monitoring_enabled() -> bool:
    raw = os.getenv("SUPERTABLE_MONITORING_ENABLED", "true").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def get_monitoring_logger(
    *,
    super_name: str,
    organization: str,
    monitor_type: str = "plans",
    redis_connector: Optional["RedisConnector"] = None,
) -> MonitoringLogger:
    """
    Return a monitoring logger (context manager + log_metric + request_flush + debug fields).

    This function never raises; on any failure it returns a NullMonitoringLogger.
    """
    if not _monitoring_enabled():
        return NullMonitoringLogger()

    key = _MonitorKey(organization=organization, super_name=super_name, monitor_type=monitor_type)
    cache_key = key.path_key

    try:
        with _MONITORS_LOCK:
            existing = _MONITORS.get(cache_key)
            if existing is not None:
                return existing
            # Evict oldest entry if cache is at capacity.
            if len(_MONITORS) >= _MONITORS_MAX:
                _evict_oldest_monitor()
            mon = _AsyncMonitoringLogger(key, redis_connector=redis_connector)
            _MONITORS[cache_key] = mon
            return mon
    except Exception as e:
        # Keep this string, since you already log/grep for it.
        logger.warning(f"Monitoring logging failed (non-fatal): {e}")
        return NullMonitoringLogger()


class MonitoringWriter:
    """
    Backwards-compatible facade.

    Supported usage patterns:
        mon = MonitoringWriter(...)
        mon.log_metric({...})
        mon.request_flush()

    And context-manager style:
        with MonitoringWriter(...) as mon:
            mon.log_metric({...})

    It also forwards debug attributes used in examples (queue_stats, etc.).
    """

    def __init__(
        self,
        *,
        super_name: str,
        organization: str,
        monitor_type: str = "plans",
        redis_connector: Optional["RedisConnector"] = None,
    ):
        self.super_name = super_name
        self.organization = organization
        self.monitor_type = monitor_type
        self._logger: MonitoringLogger = get_monitoring_logger(
            super_name=super_name,
            organization=organization,
            monitor_type=monitor_type,
            redis_connector=redis_connector,
        )

    def __enter__(self) -> "MonitoringWriter":
        try:
            self._logger.__enter__()
        except Exception:
            # non-fatal by design
            pass
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            return bool(self._logger.__exit__(exc_type, exc, tb))
        except Exception:
            return False

    def log_metric(self, payload: Dict[str, Any]) -> None:
        self._logger.log_metric(payload)

    def request_flush(self, timeout_s: float = 2.0) -> None:
        self._logger.request_flush(timeout_s=timeout_s)

    # Common aliases that might exist in older code
    def write(self, payload: Dict[str, Any]) -> None:
        self.log_metric(payload)

    def enqueue(self, payload: Dict[str, Any]) -> None:
        self.log_metric(payload)

    # Forward debug fields for scripts
    def __getattr__(self, item: str):
        return getattr(self._logger, item)

    @property
    def logger(self) -> MonitoringLogger:
        return self._logger
