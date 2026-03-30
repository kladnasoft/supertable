# route: supertable.audit.logger
"""
AuditLogger — thread-safe, non-blocking audit event emitter.

Follows the same architectural pattern as monitoring_writer.py:
  - emit() enqueues and returns immediately (< 50μs)
  - Background worker thread drains the queue in batches
  - Batches are written to Redis Streams (hot) and Parquet (warm)
  - One logger instance per organization (singleton cache)

Thread-safety: emit() is safe to call from any thread. The background
worker serializes all I/O.

Compliance: DORA Art. 6(5), 10; SOC 2 CC7.1.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from supertable.audit.events import AuditEvent, INSTANCE_ID
from supertable.audit.chain import InstanceChain, GENESIS_HASH

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration (loaded lazily from settings)
# ---------------------------------------------------------------------------

@dataclass
class AuditConfig:
    enabled: bool = True
    batch_size: int = 1000
    flush_interval_sec: int = 60
    redis_stream_ttl_hours: int = 24
    redis_stream_maxlen: int = 100_000
    hash_chain: bool = True
    log_queries: bool = True
    log_reads: bool = True
    alert_webhook: str = ""
    fernet_key: str = ""
    siem_enabled: bool = True
    siem_max_consumers: int = 10

    @classmethod
    def from_settings(cls) -> "AuditConfig":
        try:
            from supertable.config.settings import settings as _cfg
            return cls(
                enabled=getattr(_cfg, "SUPERTABLE_AUDIT_ENABLED", True),
                batch_size=getattr(_cfg, "SUPERTABLE_AUDIT_BATCH_SIZE", 1000),
                flush_interval_sec=getattr(_cfg, "SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC", 60),
                redis_stream_ttl_hours=getattr(_cfg, "SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS", 24),
                redis_stream_maxlen=getattr(_cfg, "SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN", 100_000),
                hash_chain=getattr(_cfg, "SUPERTABLE_AUDIT_HASH_CHAIN", True),
                log_queries=getattr(_cfg, "SUPERTABLE_AUDIT_LOG_QUERIES", True),
                log_reads=getattr(_cfg, "SUPERTABLE_AUDIT_LOG_READS", True),
                alert_webhook=getattr(_cfg, "SUPERTABLE_AUDIT_ALERT_WEBHOOK", ""),
                fernet_key=getattr(_cfg, "SUPERTABLE_AUDIT_FERNET_KEY", ""),
                siem_enabled=getattr(_cfg, "SUPERTABLE_AUDIT_SIEM_ENABLED", True),
                siem_max_consumers=getattr(_cfg, "SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS", 10),
            )
        except Exception:
            return cls()


# ---------------------------------------------------------------------------
# NullAuditLogger (no-op when auditing is disabled)
# ---------------------------------------------------------------------------

class NullAuditLogger:
    """No-op logger returned when auditing is disabled."""

    def emit(self, event: AuditEvent) -> None:
        pass

    def flush(self, timeout_s: float = 2.0) -> None:
        pass

    def stop(self) -> None:
        pass


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------

_QUEUE_MAX = 10_000  # Bounded queue — backpressure at this depth


class AuditLogger:
    """Production audit logger with background worker.

    Lifecycle:
      1. get_audit_logger(org) → returns cached AuditLogger
      2. emit(event) → enqueues, returns immediately
      3. Background worker drains queue → writes to Redis + (future) Parquet
      4. stop() → signals worker to drain and exit
    """

    def __init__(self, organization: str, config: Optional[AuditConfig] = None):
        self._org = organization
        self._config = config or AuditConfig.from_settings()
        self._queue: queue.Queue[Optional[AuditEvent]] = queue.Queue(maxsize=_QUEUE_MAX)
        self._stop_event = threading.Event()
        self._chain = InstanceChain(instance_id=INSTANCE_ID)
        self._chain_lock = threading.Lock()
        self._redis_writer = None
        self._parquet_writer = None
        self._thread: Optional[threading.Thread] = None
        self._stats_lock = threading.Lock()
        self._stats = {
            "total_emitted": 0,
            "total_written": 0,
            "total_dropped": 0,
            "batches_written": 0,
        }

        self._init_redis()
        self._init_parquet()
        self._restore_chain()
        self._start_worker()

    # ── Initialization ─────────────────────────────────────

    def _init_redis(self) -> None:
        """Initialize Redis Stream writer."""
        try:
            from supertable.server_common import redis_client
            from supertable.audit.writer_redis import RedisAuditWriter
            self._redis_writer = RedisAuditWriter(
                redis_client=redis_client,
                org=self._org,
                instance_id=INSTANCE_ID,
                maxlen=self._config.redis_stream_maxlen,
            )
        except Exception as e:
            logger.error("[audit] Failed to initialize Redis writer: %s — events will be logged only", e)

    def _init_parquet(self) -> None:
        """Initialize Parquet writer (warm tier)."""
        try:
            from supertable.audit.writer_parquet import ParquetAuditWriter
            self._parquet_writer = ParquetAuditWriter()
        except Exception as e:
            logger.error("[audit] Failed to initialize Parquet writer: %s", e)

    def _restore_chain(self) -> None:
        """Restore chain state from Redis on startup."""
        if not self._config.hash_chain or not self._redis_writer:
            return
        try:
            head, count = self._redis_writer.load_chain_head()
            if head:
                self._chain.head = head
                self._chain.batch_count = count
                logger.info(
                    "[audit] Restored chain for %s/%s: head=%s, batches=%d",
                    self._org, INSTANCE_ID, head[:16] + "...", count,
                )
        except Exception as e:
            logger.warning("[audit] Chain restore failed: %s — starting fresh", e)

    # ── Public API ─────────────────────────────────────────

    def emit(self, event: AuditEvent) -> None:
        """Enqueue an audit event. Non-blocking, returns immediately.

        If the queue is full, the event is dropped and a warning is logged.
        This should never happen under normal load — the queue holds 10,000
        events and the worker drains in sub-second batches.
        """
        try:
            self._queue.put_nowait(event)
            with self._stats_lock:
                self._stats["total_emitted"] += 1
        except queue.Full:
            with self._stats_lock:
                self._stats["total_dropped"] += 1
            logger.warning(
                "[audit] Queue full for %s — event dropped: %s/%s",
                self._org, event.category, event.action,
            )

    def flush(self, timeout_s: float = 2.0) -> None:
        """Drain the queue synchronously. Called on shutdown."""
        deadline = time.time() + timeout_s
        events = self._drain_queue(max_items=_QUEUE_MAX, deadline=deadline)
        if events:
            self._write_batch(events)

    def stop(self) -> None:
        """Signal the worker to stop and drain remaining events."""
        self._stop_event.set()
        self._queue.put(None)  # Sentinel to wake the worker
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self.flush(timeout_s=2.0)

    @property
    def stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    # ── Background worker ──────────────────────────────────

    def _start_worker(self) -> None:
        self._thread = threading.Thread(
            target=self._worker_loop,
            name=f"audit:{self._org}",
            daemon=True,
        )
        self._thread.start()
        logger.info("[audit] Started worker for org=%s instance=%s", self._org, INSTANCE_ID)

    def _worker_loop(self) -> None:
        batch_wait = min(self._config.flush_interval_sec, 5.0)
        backoff = 0.1

        while not self._stop_event.is_set():
            try:
                first = self._queue.get(timeout=batch_wait)
                if first is None:
                    continue  # Sentinel — check stop flag

                events = [first]
                events.extend(self._drain_queue(
                    max_items=self._config.batch_size - 1,
                    deadline=time.time() + 0.05,
                ))
                self._write_batch(events)
                backoff = 0.1
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("[audit] Worker error for %s: %s", self._org, e)
                time.sleep(backoff)
                backoff = min(5.0, backoff * 2.0)

    def _drain_queue(self, max_items: int, deadline: float) -> List[AuditEvent]:
        items: List[AuditEvent] = []
        while len(items) < max_items and time.time() < deadline:
            try:
                item = self._queue.get_nowait()
                if item is not None:
                    items.append(item)
            except queue.Empty:
                break
        return items

    # ── Batch writing ──────────────────────────────────────

    def _write_batch(self, events: List[AuditEvent]) -> None:
        """Write a batch to Redis Streams and advance the chain."""
        if not events:
            return

        # Compute chain hash for this batch
        event_dicts = []
        event_ids = []
        for event in events:
            d = event.to_dict()
            event_ids.append(event.event_id)
            event_dicts.append(d)

        chain_hash = ""
        if self._config.hash_chain:
            with self._chain_lock:
                chain_hash = self._chain.advance(event_ids)

            # Stamp chain_hash onto every event dict in the batch
            for d in event_dicts:
                d["chain_hash"] = chain_hash

        # Write to Redis Stream (hot tier)
        written = 0
        if self._redis_writer:
            try:
                stream_ids = self._redis_writer.write_batch(event_dicts)
                written = len(stream_ids)
            except Exception as e:
                logger.error("[audit] Redis write failed for %s: %s", self._org, e)

        # Persist chain head to Redis
        if self._config.hash_chain and self._redis_writer:
            try:
                self._redis_writer.save_chain_head(self._chain.head, self._chain.batch_count)
            except Exception as e:
                logger.warning("[audit] Chain head persist failed: %s", e)

        # Write to Parquet (warm tier)
        if self._parquet_writer:
            try:
                result = self._parquet_writer.write_batch(self._org, event_dicts)
                if result.get("path"):
                    logger.debug(
                        "[audit] Parquet batch: %s (%d events, %d bytes)",
                        result["path"], result["event_count"], result["bytes_written"],
                    )
            except Exception as e:
                logger.error("[audit] Parquet write failed for %s: %s", self._org, e)

        # Update stats
        with self._stats_lock:
            self._stats["total_written"] += written
            self._stats["batches_written"] += 1
            dropped = len(events) - written
            if dropped > 0:
                self._stats["total_dropped"] += dropped

        # Log if critical events were in this batch
        for event in events:
            if event.severity == "critical":
                logger.warning(
                    "[audit] CRITICAL event: org=%s action=%s actor=%s resource=%s",
                    self._org, event.action, event.actor_username or event.actor_id,
                    event.resource_id,
                )

                if self._config.alert_webhook:
                    self._fire_webhook(event)

    # ── Alert webhook ─────────────────────────────────────

    def _fire_webhook(self, event: AuditEvent) -> None:
        """POST a critical event to the configured alert webhook.

        Fire-and-forget: runs in a daemon thread so it never blocks
        the batch writer.  Timeouts are aggressive (5s connect, 10s total)
        to prevent webhook latency from stalling the audit pipeline.
        """
        url = self._config.alert_webhook
        if not url:
            return

        payload = {
            "event_id": event.event_id,
            "timestamp_ms": event.timestamp_ms,
            "organization": self._org,
            "category": event.category,
            "action": event.action,
            "severity": event.severity,
            "actor_id": event.actor_id,
            "actor_username": event.actor_username,
            "resource_type": event.resource_type,
            "resource_id": event.resource_id,
            "outcome": event.outcome,
            "detail": event.detail,
            "instance_id": event.instance_id,
        }

        def _send():
            try:
                import httpx
                with httpx.Client(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                    resp = client.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json",
                                 "User-Agent": "SuperTable-Audit/1.0"},
                    )
                    if resp.status_code >= 400:
                        logger.warning(
                            "[audit-webhook] POST %s returned %d",
                            url, resp.status_code,
                        )
            except ImportError:
                logger.warning("[audit-webhook] httpx not installed — webhook disabled")
            except Exception as e:
                logger.warning("[audit-webhook] POST %s failed: %s", url, e)

        try:
            t = threading.Thread(target=_send, daemon=True, name="audit-webhook")
            t.start()
        except Exception as e:
            logger.warning("[audit-webhook] Thread start failed: %s", e)


# ---------------------------------------------------------------------------
# Singleton cache (one logger per organization)
# ---------------------------------------------------------------------------

_LOGGERS: Dict[str, AuditLogger] = {}
_LOGGERS_LOCK = threading.Lock()
_CONFIG: Optional[AuditConfig] = None


def _get_config() -> AuditConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = AuditConfig.from_settings()
    return _CONFIG


def get_audit_logger(organization: str) -> "AuditLogger | NullAuditLogger":
    """Return a cached AuditLogger for the organization.

    Thread-safe. Returns NullAuditLogger if auditing is disabled.
    """
    config = _get_config()
    if not config.enabled:
        return NullAuditLogger()

    with _LOGGERS_LOCK:
        existing = _LOGGERS.get(organization)
        if existing is not None:
            return existing

        audit_logger = AuditLogger(organization, config)
        _LOGGERS[organization] = audit_logger
        return audit_logger


def shutdown_all() -> None:
    """Stop all audit loggers. Called on application shutdown."""
    with _LOGGERS_LOCK:
        for org, audit_logger in _LOGGERS.items():
            try:
                audit_logger.stop()
            except Exception as e:
                logger.warning("[audit] Shutdown error for %s: %s", org, e)
        _LOGGERS.clear()
    logger.info("[audit] All audit loggers stopped")
