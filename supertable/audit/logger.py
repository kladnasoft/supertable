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
import os
import queue
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Mapping, Optional

from supertable.audit.diagnostics import safe_audit_error_type
from supertable.audit.events import (
    Actions,
    AuditEvent,
    current_instance_id,
    new_event_id,
)
from supertable.audit.chain import (
    InstanceChain,
    compute_event_batch_hash,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration (loaded lazily from settings)
# ---------------------------------------------------------------------------

@dataclass
class AuditConfig:
    # Default to OFF.  Real values come from env (from_settings) and are
    # overridden per-organization via the Redis-backed admin layer.
    enabled: bool = False
    batch_size: int = 1000
    flush_interval_sec: int = 60
    redis_stream_ttl_hours: int = 24
    redis_stream_maxlen: int = 100_000
    hash_chain: bool = False
    proof_close_grace_sec: int = 300
    log_queries: bool = False
    log_reads: bool = False
    alert_webhook: str = ""
    fernet_key: str = ""
    siem_enabled: bool = False
    siem_max_consumers: int = 10

    @classmethod
    def from_settings(cls) -> "AuditConfig":
        try:
            from supertable.config.settings import settings as _cfg
        except Exception:
            return cls()
        config = cls(
            enabled=getattr(_cfg, "SUPERTABLE_AUDIT_ENABLED", False),
            batch_size=getattr(_cfg, "SUPERTABLE_AUDIT_BATCH_SIZE", 1000),
            flush_interval_sec=getattr(_cfg, "SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC", 60),
            redis_stream_ttl_hours=getattr(_cfg, "SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS", 24),
            redis_stream_maxlen=getattr(_cfg, "SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN", 100_000),
            hash_chain=getattr(_cfg, "SUPERTABLE_AUDIT_HASH_CHAIN", False),
            proof_close_grace_sec=getattr(
                _cfg, "SUPERTABLE_AUDIT_PROOF_CLOSE_GRACE_SEC", 300,
            ),
            log_queries=getattr(_cfg, "SUPERTABLE_AUDIT_LOG_QUERIES", False),
            log_reads=getattr(_cfg, "SUPERTABLE_AUDIT_LOG_READS", False),
            alert_webhook=getattr(_cfg, "SUPERTABLE_AUDIT_ALERT_WEBHOOK", ""),
            fernet_key=getattr(_cfg, "SUPERTABLE_AUDIT_FERNET_KEY", ""),
            siem_enabled=getattr(_cfg, "SUPERTABLE_AUDIT_SIEM_ENABLED", False),
            siem_max_consumers=getattr(_cfg, "SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS", 10),
        )
        return config

    def with_overrides(self, overrides: Dict[str, Any]) -> "AuditConfig":
        """Return a copy with select fields replaced from a dict (e.g., from
        the Redis-backed audit:config HASH).  Unknown keys are ignored."""
        from dataclasses import replace
        kw: Dict[str, Any] = {}
        for k in (
            "enabled",
            "hash_chain",
            "log_queries",
            "log_reads",
            "siem_enabled",
        ):
            if k in overrides and overrides[k] is not None:
                kw[k] = bool(overrides[k])
        return replace(self, **kw) if kw else self


# ---------------------------------------------------------------------------
# NullAuditLogger (no-op when auditing is disabled)
# ---------------------------------------------------------------------------

class NullAuditLogger:
    """No-op logger returned when auditing is disabled."""

    def emit(self, event: AuditEvent) -> bool:
        return False

    def flush(self, timeout_s: float = 2.0) -> None:
        pass

    def stop(self) -> None:
        pass


_NULL_AUDIT_LOGGER = NullAuditLogger()


class AuditFlushError(RuntimeError):
    """The worker could not durably reach a flush barrier in time."""


class AuditShutdownError(RuntimeError):
    """The audit worker could not quiesce and drain within the deadline."""


class AuditArchiveUnavailable(RuntimeError):
    """No exact durable Parquet publication was confirmed for a batch."""


class AuditConfigUnavailable(RuntimeError):
    """No authoritative or last-known audit policy is available."""


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------

_QUEUE_MAX = 10_000  # Bounded queue — backpressure at this depth
_MAX_AUDIT_EVENT_BYTES = 96 * 1_024
_MAX_AUDIT_EVENT_FIELD_BYTES = 64 * 1_024
_MAX_PENDING_AUDIT_BYTES = 16 * 1_024 * 1_024
_PROTECTED_EVENT_CAPABILITY = object()
_WEBHOOK_MAX_IN_FLIGHT = 4
_WEBHOOK_SLOTS = threading.BoundedSemaphore(_WEBHOOK_MAX_IN_FLIGHT)
_PROCESS_CHAIN_LOCK = threading.Lock()
# The instance ID is a random per-process identity.  This process-local checkpoint is
# therefore the authoritative hand-off between same-boot logger replacements;
# Redis remains a restart aid, not a prerequisite for continuity.
_PROCESS_CHAIN_STATE: Dict[tuple[str, str], tuple[str, int]] = {}


@dataclass(frozen=True)
class _QueuedAuditEvent:
    event: AuditEvent
    size_bytes: int


@dataclass(frozen=True)
class _FlushBarrier:
    done: threading.Event


def _serialized_event_size(event: AuditEvent) -> int:
    """Return the canonical event size after validating scalar field bounds."""
    total_field_bytes = 0
    for field_name in event.__dataclass_fields__:
        value = getattr(event, field_name)
        if field_name == "timestamp_ms":
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                or value > (1 << 63) - 1
            ):
                raise ValueError("audit event timestamp must be an integer")
            continue
        if not isinstance(value, str):
            raise ValueError("audit event fields must be strings")
        try:
            field_size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            raise ValueError("audit event field is not valid UTF-8") from None
        if field_size > _MAX_AUDIT_EVENT_FIELD_BYTES:
            raise ValueError("audit event field exceeds the byte limit")
        total_field_bytes += field_size
        if total_field_bytes > _MAX_AUDIT_EVENT_BYTES:
            raise ValueError("audit event exceeds the aggregate field limit")
    try:
        serialized = event.to_json().encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("audit event is not canonical JSON") from None
    if len(serialized) > _MAX_AUDIT_EVENT_BYTES:
        raise ValueError("audit event exceeds the serialized byte limit")
    return len(serialized)


class AuditLogger:
    """Production audit logger with background worker.

    Lifecycle:
      1. get_audit_logger(org) → returns cached AuditLogger
      2. emit(event) → enqueues, returns immediately
      3. Background worker drains queue → writes to Redis + (future) Parquet
      4. stop() → signals worker to drain and exit
    """

    def __init__(self, organization: str, config: Optional[AuditConfig] = None):
        from supertable import redis_keys as RK

        # Reject an unsafe tenant before Redis initialization, storage
        # construction, or worker start.  Deferring this to a sink would leave
        # a live worker retrying a permanently invalid archive path.
        RK.audit_stream(organization)
        self._org = organization
        self._owner_pid = os.getpid()
        self._instance_id = current_instance_id()
        self._config = config or AuditConfig.from_settings()
        if (
            self._config.hash_chain
            and (
                type(self._config.proof_close_grace_sec) is not int
                or not 0 <= self._config.proof_close_grace_sec <= 86_400
            )
        ):
            raise ValueError("audit proof close grace must be 0..86400 seconds")
        if self._config.fernet_key:
            from supertable.audit.crypto import (
                ensure_configured_encryption_ready,
            )
            ensure_configured_encryption_ready()
        self._queue: queue.Queue[
            Optional[_QueuedAuditEvent | _FlushBarrier]
        ] = queue.Queue(
            maxsize=_QUEUE_MAX,
        )
        self._stop_event = threading.Event()
        self._worker_done = threading.Event()
        self._admission_lock = threading.Lock()
        self._accepting = True
        self._chain = InstanceChain(instance_id=self._instance_id)
        self._chain_lock = threading.Lock()
        self._redis_writer: Any = None
        self._parquet_writer: Any = None
        self._journal: Any = None
        self._journal_archiver: Any = None
        self._journal_closer: Any = None
        self._journal_wakeup = threading.Event()
        self._journal_progress = threading.Event()
        self._journal_pending: Dict[str, tuple[int, int]] = {}
        self._thread: Optional[threading.Thread] = None
        self._stats_lock = threading.Lock()
        self._stats = {
            "total_emitted": 0,
            "total_written": 0,
            "total_dropped": 0,
            "batches_written": 0,
            "pending_bytes": 0,
            "peak_pending_bytes": 0,
            "webhooks_dropped": 0,
            "redis_failures": 0,
            "parquet_failures": 0,
            "archive_retries": 0,
            "flush_failures": 0,
            "shutdown_failures": 0,
        }

        self._init_redis()
        self._init_parquet()
        self._restore_chain()
        self._start_worker()

    # ── Initialization ─────────────────────────────────────

    def _init_redis(self) -> None:
        """Initialize Redis Stream writer."""
        try:
            from supertable.redis_infra import redis_client
            from supertable.audit.writer_redis import RedisAuditWriter
            if self._config.hash_chain:
                from supertable.audit.durable_journal import RedisAuditJournal

                # Validate/activate the atomic journal before the best-effort
                # hot stream attempts any command. Redis Cluster must fail here
                # with a controlled topology error, never later with CROSSSLOT.
                self._journal = RedisAuditJournal(redis_client, self._org)
            self._redis_writer = RedisAuditWriter(
                redis_client=redis_client,
                org=self._org,
                instance_id=self._instance_id,
                maxlen=self._config.redis_stream_maxlen,
            )
        except Exception as e:
            logger.error(
                "[audit] Redis writer initialization failed: %s",
                safe_audit_error_type(e),
            )
            if self._config.hash_chain:
                from supertable.audit.durable_journal import (
                    AuditJournalConfigurationError,
                )
                if isinstance(e, AuditJournalConfigurationError):
                    raise AuditJournalConfigurationError(
                        "audit journal configuration is invalid"
                    ) from None

    def _init_parquet(self) -> None:
        """Initialize Parquet writer (warm tier)."""
        try:
            from supertable.audit.writer_parquet import ParquetAuditWriter
            self._parquet_writer = ParquetAuditWriter()
        except Exception as e:
            logger.error(
                "[audit] Parquet writer initialization failed: %s",
                safe_audit_error_type(e),
            )

    def _restore_chain(self) -> None:
        """Restore chain state from Redis on startup."""
        if not self._config.hash_chain:
            return
        with _PROCESS_CHAIN_LOCK:
            process_state = _PROCESS_CHAIN_STATE.get(
                (self._org, self._instance_id),
            )
        if process_state is not None:
            self._chain.head, self._chain.batch_count = process_state
            return

        head = ""
        count = 0
        if self._journal is not None:
            try:
                head, count = self._journal.load_checkpoint(self._instance_id)
            except Exception as e:
                logger.warning(
                    "[audit] Durable chain restore failed: %s",
                    safe_audit_error_type(e),
                )
        elif self._redis_writer:
            try:
                head, count = self._redis_writer.load_chain_head()
            except Exception as e:
                logger.warning(
                    "[audit] Chain restore failed: %s — starting fresh",
                    safe_audit_error_type(e),
                )
        if head:
            if (
                not isinstance(head, str)
                or len(head) != 64
                or any(ch not in "0123456789abcdef" for ch in head)
                or type(count) is not int
                or count < 0
            ):
                raise ValueError("persisted audit chain checkpoint is invalid")
            self._chain.head = head
            self._chain.batch_count = count
            logger.info(
                "[audit] Restored chain for %s/%s: head=%s, batches=%d",
                self._org, self._instance_id, head[:16] + "...", count,
            )
        with _PROCESS_CHAIN_LOCK:
            _PROCESS_CHAIN_STATE[(self._org, self._instance_id)] = (
                self._chain.head, self._chain.batch_count,
            )

    # ── Public API ─────────────────────────────────────────

    def emit(self, event: AuditEvent) -> bool:
        """Enqueue an audit event. Non-blocking, returns immediately.

        If the queue is full, the event is dropped and a warning is logged.
        This should never happen under normal load — the queue holds 10,000
        events and the worker drains in sub-second batches.
        """
        config = getattr(self, "_config", None)
        if (
            event.action == Actions.QUERY_EXECUTE
            and config is not None
            and not config.log_queries
        ):
            return False

        from supertable.audit.crypto import protect_sensitive_detail

        protected_detail = protect_sensitive_detail(
            event.detail, action=event.action,
        )
        if isinstance(protected_detail, (dict, list)):
            protected_detail = json.dumps(
                protected_detail,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        elif not isinstance(protected_detail, str):
            protected_detail = str(protected_detail)
        if protected_detail != event.detail:
            event = replace(event, detail=protected_detail)
        return self._enqueue_protected(
            event,
            capability=_PROTECTED_EVENT_CAPABILITY,
        )

    def _enqueue_protected(
        self,
        event: AuditEvent,
        *,
        capability: object,
    ) -> bool:
        """Enqueue an already-protected event from the trusted public path."""
        if capability is not _PROTECTED_EVENT_CAPABILITY:
            raise PermissionError("protected audit enqueue capability required")
        # Event identity and archive time are writer-owned integrity metadata,
        # like instance_id/chain_hash.  Never admit a replayable ID or a caller
        # timestamp that could redirect the batch to an arbitrary partition (or
        # overflow datetime conversion and permanently block the sole worker).
        event = replace(
            event,
            event_id=new_event_id(),
            timestamp_ms=time.time_ns() // 1_000_000,
        )
        instance_id = getattr(self, "_instance_id", current_instance_id())
        if (
            event.organization != self._org
            or bool(event.chain_hash)
            or event.instance_id != instance_id
        ):
            with self._stats_lock:
                self._stats["total_dropped"] += 1
            logger.warning(
                "[audit] Event integrity binding rejected for org=%s",
                self._org,
            )
            return False
        try:
            event_size = _serialized_event_size(event)
        except ValueError:
            with self._stats_lock:
                self._stats["total_dropped"] += 1
            logger.warning(
                "[audit] Oversized or invalid event dropped for org=%s",
                self._org,
            )
            return False

        # Admission close and queue publication are one linearized operation.
        # A producer that finishes protection after stop() began observes the
        # closed flag and cannot strand work behind a terminated worker.
        rejection: Optional[str] = None
        admitted = False
        with self._admission_lock:
            if not self._accepting:
                with self._stats_lock:
                    self._stats["total_dropped"] += 1
                rejection = "closed"
            else:
                with self._stats_lock:
                    pending = self._stats.get("pending_bytes", 0)
                if pending + event_size > _MAX_PENDING_AUDIT_BYTES:
                    with self._stats_lock:
                        self._stats["total_dropped"] += 1
                    rejection = "bytes"
                elif (
                    getattr(getattr(self, "_config", None), "hash_chain", False)
                    and hasattr(self, "_journal")
                ):
                    if self._journal is None:
                        with self._stats_lock:
                            self._stats["total_dropped"] += 1
                        rejection = "backend"
                    elif len(self._journal_pending) >= _QUEUE_MAX:
                        with self._stats_lock:
                            self._stats["total_dropped"] += 1
                        rejection = "items"
                    else:
                        try:
                            admission = self._journal.admit(event)
                        except Exception:
                            with self._stats_lock:
                                self._stats["total_dropped"] += 1
                            rejection = "backend"
                        else:
                            self._journal_pending[
                                admission.journal_id
                            ] = (event_size, admission.day)
                            pending += event_size
                            with self._stats_lock:
                                self._stats["pending_bytes"] = pending
                                self._stats["peak_pending_bytes"] = max(
                                    self._stats.get("peak_pending_bytes", 0),
                                    pending,
                                )
                                self._stats["total_emitted"] += 1
                            admitted = True
                            self._journal_wakeup.set()
                else:
                    queued = _QueuedAuditEvent(
                        event=event, size_bytes=event_size,
                    )
                    try:
                        self._queue.put_nowait(queued)
                    except queue.Full:
                        with self._stats_lock:
                            self._stats["total_dropped"] += 1
                        rejection = "items"
                    else:
                        pending += event_size
                        with self._stats_lock:
                            self._stats["pending_bytes"] = pending
                            self._stats["peak_pending_bytes"] = max(
                                self._stats.get("peak_pending_bytes", 0),
                                pending,
                            )
                            self._stats["total_emitted"] += 1
                        admitted = True

        # No user-configurable logging handler runs under lifecycle/stat locks.
        # A handler is observability only and cannot deadlock admission.
        if rejection is not None:
            messages = {
                "closed": "Event rejected after admission closed",
                "bytes": "Pending-byte budget full; event dropped",
                "items": "Queue item budget full; event dropped",
                "backend": "Durable journal unavailable; event rejected",
            }
            try:
                logger.warning("[audit] %s for org=%s", messages[rejection], self._org)
            except Exception:
                pass
        return admitted

    def _complete_queued(self, queued: List[_QueuedAuditEvent]) -> None:
        completed_bytes = sum(item.size_bytes for item in queued)
        with self._stats_lock:
            self._stats["pending_bytes"] = max(
                0,
                self._stats.get("pending_bytes", 0) - completed_bytes,
            )

    def flush(self, timeout_s: float = 2.0) -> None:
        """Wait until the sole writer reaches all work admitted before now."""
        timeout_s = max(0.0, float(timeout_s))
        deadline = time.monotonic() + timeout_s
        thread = self._thread
        if self._config.hash_chain and hasattr(self, "_journal"):
            with self._admission_lock:
                target_ids = frozenset(self._journal_pending)
            if not target_ids:
                return
            if thread is None or not thread.is_alive():
                with self._stats_lock:
                    self._stats["flush_failures"] += 1
                raise AuditFlushError(
                    "durable audit journal worker is unavailable for flush"
                )
            self._journal_wakeup.set()
            while True:
                with self._admission_lock:
                    remaining_ids = target_ids.intersection(
                        self._journal_pending,
                    )
                if not remaining_ids:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    with self._stats_lock:
                        self._stats["flush_failures"] += 1
                    raise AuditFlushError("durable audit journal flush timed out")
                self._journal_progress.wait(min(remaining, 0.25))
                self._journal_progress.clear()

        if thread is None or not thread.is_alive():
            if (
                self._queue.empty()
                and self._worker_done.is_set()
                and self.stats.get("pending_bytes", 0) == 0
            ):
                return
            with self._stats_lock:
                self._stats["flush_failures"] += 1
            raise AuditFlushError("audit worker is unavailable for flush")

        barrier = _FlushBarrier(done=threading.Event())
        with self._admission_lock:
            if not self._accepting:
                wait_for_worker = True
            else:
                wait_for_worker = False
                try:
                    # Never block while holding the admission lock: the worker
                    # now takes this lock for its exit decision and must remain
                    # able to dequeue a saturated queue.
                    self._queue.put_nowait(barrier)
                except queue.Full:
                    with self._stats_lock:
                        self._stats["flush_failures"] += 1
                    raise AuditFlushError(
                        "audit flush barrier queue timed out"
                    ) from None

        remaining = max(0.0, deadline - time.monotonic())
        completed = (
            self._worker_done.wait(remaining)
            if wait_for_worker
            else barrier.done.wait(remaining)
        )
        if not completed:
            with self._stats_lock:
                self._stats["flush_failures"] += 1
            raise AuditFlushError("audit flush timed out")
        if wait_for_worker and (
            not self._queue.empty() or self.stats.get("pending_bytes", 0)
        ):
            with self._stats_lock:
                self._stats["flush_failures"] += 1
            raise AuditFlushError("audit worker exited before durable flush")

    def stop(self, timeout_s: float = 7.0) -> None:
        """Close admission and wait for the worker to durably drain itself."""
        with self._admission_lock:
            first_close = self._accepting
            self._accepting = False
            self._stop_event.set()
            if hasattr(self, "_journal_wakeup"):
                self._journal_wakeup.set()
            if first_close and not self._worker_done.is_set():
                try:
                    self._queue.put_nowait(None)  # Wake an idle worker.
                except queue.Full:
                    # A full queue has already made the worker runnable.
                    pass
        thread = self._thread
        if thread is None:
            if self._queue.empty():
                self._worker_done.set()
                return
            with self._stats_lock:
                self._stats["shutdown_failures"] += 1
            raise AuditShutdownError("audit worker is unavailable during stop")
        if thread is threading.current_thread():
            raise AuditShutdownError("audit worker cannot stop itself")
        if not self._worker_done.wait(max(0.0, float(timeout_s))):
            with self._stats_lock:
                self._stats["shutdown_failures"] += 1
            # The logger stays closed and cached/quarantined.  Callers must not
            # construct a same-org replacement while this worker can still
            # touch the non-thread-safe writers or instance chain.
            raise AuditShutdownError("audit worker did not quiesce before timeout")
        thread.join(timeout=0)
        if not self._queue.empty() or self.stats.get("pending_bytes", 0):
            with self._stats_lock:
                self._stats["shutdown_failures"] += 1
            raise AuditShutdownError("audit worker exited before durable drain")

    @property
    def stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    # ── Background worker ──────────────────────────────────

    def _start_worker(self) -> None:
        target = (
            self._journal_worker_loop
            if self._config.hash_chain and hasattr(self, "_journal")
            else self._worker_loop
        )
        self._thread = threading.Thread(
            target=target,
            name=f"audit:{self._org}",
            daemon=True,
        )
        self._thread.start()
        try:
            logger.info(
                "[audit] Started worker for org=%s instance=%s",
                self._org,
                self._instance_id,
            )
        except Exception:
            # A custom logging handler cannot orphan an otherwise valid worker
            # by making construction fail after Thread.start().
            pass

    def _complete_journal_ids(self, archived: Mapping[str, str]) -> int:
        completed_bytes = 0
        completed_events = 0
        with self._admission_lock:
            for journal_id in archived:
                pending = self._journal_pending.pop(journal_id, None)
                if pending is not None:
                    completed_bytes += pending[0]
                    completed_events += 1
        if completed_events:
            with self._stats_lock:
                self._stats["pending_bytes"] = max(
                    0,
                    self._stats.get("pending_bytes", 0) - completed_bytes,
                )
                self._stats["total_written"] += completed_events
            self._journal_progress.set()
        return completed_events

    def _journal_worker_loop(self) -> None:
        """Archive and close the durable hash/proof journal off-thread."""
        backoff = 0.1
        next_close_check = 0.0
        try:
            journal_batch_size = max(
                1, min(int(self._config.batch_size), 1_000),
            )
        except (TypeError, ValueError, OverflowError):
            journal_batch_size = 1_000
        try:
            if self._journal is None or self._parquet_writer is None:
                raise AuditArchiveUnavailable(
                    "durable audit journal or Parquet writer is unavailable"
                )
            from supertable.audit.durable_journal import (
                DurableAuditArchiver,
                DurableAuditDayCloser,
            )

            self._journal_archiver = DurableAuditArchiver(
                self._journal,
                self._parquet_writer,
                redis_writer=self._redis_writer,
            )
            self._journal_closer = DurableAuditDayCloser(
                self._journal,
                self._parquet_writer,
            )
            while True:
                try:
                    with self._admission_lock:
                        pending_admissions = dict(self._journal_pending)
                    if pending_admissions:
                        archived = self._journal.archived_membership(
                            tuple(pending_admissions),
                            admission_days={
                                journal_id: pending[1]
                                for journal_id, pending in pending_admissions.items()
                            },
                        )
                        self._complete_journal_ids(archived)

                    with self._admission_lock:
                        should_stop = (
                            self._stop_event.is_set()
                            and not self._journal_pending
                        )
                    if should_stop:
                        break

                    receipt = self._journal_archiver.archive_once(
                        count=journal_batch_size,
                    )
                    if receipt is not None:
                        self._complete_journal_ids({
                            journal_id: receipt.batch_id
                            for journal_id in receipt.journal_ids
                        })
                        if receipt.instance_id == self._instance_id:
                            with self._chain_lock:
                                self._chain.head = receipt.chain_head
                                self._chain.batch_count = receipt.batch_count
                                with _PROCESS_CHAIN_LOCK:
                                    _PROCESS_CHAIN_STATE[(
                                        self._org, self._instance_id,
                                    )] = (
                                        receipt.chain_head,
                                        receipt.batch_count,
                                    )
                        with self._stats_lock:
                            self._stats["batches_written"] += 1
                        backoff = 0.1
                        continue

                    now = time.monotonic()
                    if now >= next_close_check:
                        next_close_check = now + 5.0
                        try:
                            self._journal.cleanup_pending_day()
                            self._journal_closer.close_day(
                                self._journal.next_close_day(),
                                grace_ms=(
                                    self._config.proof_close_grace_sec * 1_000
                                ),
                            )
                        except Exception as exc:
                            logger.warning(
                                "[audit] Durable day close deferred: %s",
                                safe_audit_error_type(exc),
                            )
                    self._journal_wakeup.wait(0.25)
                    self._journal_wakeup.clear()
                    backoff = 0.1
                except Exception as exc:
                    with self._stats_lock:
                        self._stats["archive_retries"] = (
                            self._stats.get("archive_retries", 0) + 1
                        )
                    logger.error(
                        "[audit] Durable journal worker retry for org=%s: %s",
                        self._org,
                        safe_audit_error_type(exc),
                    )
                    self._journal_wakeup.wait(backoff)
                    self._journal_wakeup.clear()
                    backoff = min(5.0, backoff * 2.0)
        except Exception as exc:
            logger.error(
                "[audit] Durable journal worker stopped: %s",
                safe_audit_error_type(exc),
            )
        finally:
            self._worker_done.set()
            self._journal_progress.set()

    def _worker_loop(self) -> None:
        try:
            batch_wait = min(
                max(0.01, float(self._config.flush_interval_sec)), 5.0,
            )
        except (TypeError, ValueError):
            batch_wait = 5.0
        try:
            batch_size = min(
                _QUEUE_MAX, max(1, int(self._config.batch_size)),
            )
        except (TypeError, ValueError):
            batch_size = 1_000
        backoff = 0.1
        pending: List[_QueuedAuditEvent] = []
        barriers_after_pending: List[_FlushBarrier] = []
        deferred: Optional[_QueuedAuditEvent] = None

        try:
            while True:
                # Linearize the empty-stop decision with admission close and
                # its wakeup sentinel so stop() cannot strand a sentinel after
                # the worker has already decided to exit.
                with self._admission_lock:
                    should_stop = (
                        self._stop_event.is_set()
                        and not pending
                        and deferred is None
                        and self._queue.empty()
                    )
                    if should_stop:
                        # Publish completion under the same lock used by every
                        # stop caller before exposing the exit decision.
                        self._worker_done.set()
                if should_stop:
                    break

                if not pending:
                    first: Optional[_QueuedAuditEvent | _FlushBarrier]
                    if deferred is not None:
                        first = deferred
                        deferred = None
                    else:
                        try:
                            first = self._queue.get(timeout=batch_wait)
                        except queue.Empty:
                            continue
                    if first is None:
                        continue
                    if isinstance(first, _FlushBarrier):
                        first.done.set()
                        continue
                    pending.append(first)
                    partition_day = first.event.timestamp_ms // 86_400_000

                    gather_deadline = time.monotonic() + 0.05
                    while (
                        len(pending) < batch_size
                        and time.monotonic() < gather_deadline
                    ):
                        try:
                            item = self._queue.get_nowait()
                        except queue.Empty:
                            break
                        if item is None:
                            continue
                        if isinstance(item, _FlushBarrier):
                            barriers_after_pending.append(item)
                            break
                        if item.event.timestamp_ms // 86_400_000 != partition_day:
                            # Keep queue order while ensuring every durable file
                            # belongs to exactly one event UTC date.  Holding one
                            # deferred item avoids a put-back race and, on an
                            # archive retry, never replays an already committed
                            # date subgroup.
                            deferred = item
                            break
                        pending.append(item)

                try:
                    self._write_batch([item.event for item in pending])
                except Exception as exc:
                    with self._stats_lock:
                        self._stats["archive_retries"] = (
                            self._stats.get("archive_retries", 0) + 1
                        )
                    logger.error(
                        "[audit] Durable archive retry for org=%s: %s",
                        self._org,
                        safe_audit_error_type(exc),
                    )
                    time.sleep(backoff)
                    backoff = min(5.0, backoff * 2.0)
                    continue

                self._complete_queued(pending)
                pending = []
                backoff = 0.1
                for barrier in barriers_after_pending:
                    barrier.done.set()
                barriers_after_pending = []
        finally:
            self._worker_done.set()

    # ── Batch writing ──────────────────────────────────────

    def _write_batch(self, events: List[AuditEvent]) -> None:
        """Durably archive a batch, then commit its chain and secondary copy."""
        if not events:
            return

        event_dicts = [event.to_dict() for event in events]
        content_hash = compute_event_batch_hash(event_dicts)

        chain_hash = ""
        previous_head = ""
        if self._config.hash_chain:
            with self._chain_lock:
                previous_head = self._chain.head
                chain_hash = self._chain.next_for_events(event_dicts)

            for d in event_dicts:
                d["chain_hash"] = chain_hash

        # Publication identity and partition time remain stable across retries.
        publication_id = chain_hash or content_hash
        published_at_ms = max(event.timestamp_ms for event in events)
        if self._parquet_writer is None:
            with self._stats_lock:
                self._stats["parquet_failures"] = (
                    self._stats.get("parquet_failures", 0) + 1
                )
            raise AuditArchiveUnavailable("Parquet audit writer is unavailable")
        try:
            result = self._parquet_writer.write_batch(
                self._org,
                event_dicts,
                publication_id=publication_id,
                published_at_ms=published_at_ms,
            )
        except Exception:
            with self._stats_lock:
                self._stats["parquet_failures"] = (
                    self._stats.get("parquet_failures", 0) + 1
                )
            raise
        if (
            not isinstance(result, dict)
            or not isinstance(result.get("path"), str)
            or not result["path"].startswith(f"{self._org}/__audit__/")
            or len(result["path"].encode("utf-8")) > 2_048
            or result.get("event_count") != len(events)
            or result.get("publication_id") != publication_id
            or type(result.get("bytes_written")) is not int
            or result["bytes_written"] <= 0
            or not isinstance(result.get("file_hash"), str)
            or len(result["file_hash"]) != 64
            or any(
                ch not in "0123456789abcdef" for ch in result["file_hash"]
            )
        ):
            with self._stats_lock:
                self._stats["parquet_failures"] = (
                    self._stats.get("parquet_failures", 0) + 1
                )
            raise AuditArchiveUnavailable(
                "Parquet writer returned an incomplete publication receipt"
            )

        # Only an exact durable publication may advance the integrity state.
        if self._config.hash_chain:
            with self._chain_lock:
                self._chain.commit(previous_head, chain_hash)
                with _PROCESS_CHAIN_LOCK:
                    _PROCESS_CHAIN_STATE[(
                        self._org, self._instance_id,
                    )] = (
                        self._chain.head, self._chain.batch_count,
                    )

        # From this point on the exact batch is committed. No ancillary Redis,
        # logging, or alert failure may escape to the worker's archive-retry
        # loop and append the same events from an already-advanced head.
        with self._stats_lock:
            self._stats["total_written"] += len(events)
            self._stats["batches_written"] += 1

        try:
            # Redis is a secondary hot copy.  Its failure cannot turn a durable
            # archive into a false drop or advance the chain ahead of Parquet.
            if self._redis_writer:
                try:
                    stream_ids = self._redis_writer.write_batch(event_dicts)
                    if len(stream_ids) != len(events):
                        raise RuntimeError("Redis audit write was incomplete")
                except Exception as exc:
                    with self._stats_lock:
                        self._stats["redis_failures"] = (
                            self._stats.get("redis_failures", 0) + 1
                        )
                    logger.error(
                        "[audit] Redis secondary write failed for org=%s: %s",
                        self._org,
                        safe_audit_error_type(exc),
                    )

                if self._config.hash_chain:
                    try:
                        self._redis_writer.save_chain_head(
                            self._chain.head, self._chain.batch_count,
                        )
                    except Exception as exc:
                        with self._stats_lock:
                            self._stats["redis_failures"] = (
                                self._stats.get("redis_failures", 0) + 1
                            )
                        logger.warning(
                            "[audit] Redis chain checkpoint failed: %s",
                            safe_audit_error_type(exc),
                        )

            logger.debug(
                "[audit] Durable Parquet batch persisted (%d events, %d bytes)",
                result["event_count"], result["bytes_written"],
            )

            # Log if critical events were in this batch
            for event in events:
                if event.severity == "critical":
                    logger.warning(
                        "[audit] CRITICAL event received for org=%s",
                        self._org,
                    )

                    if self._config.alert_webhook:
                        self._fire_webhook(event)
        except Exception as exc:
            # Even a deliberately hostile logging handler must not make a
            # committed archive look retryable. Diagnostics are best-effort.
            try:
                logger.warning(
                    "[audit] Post-archive secondary handling failed: %s",
                    safe_audit_error_type(exc),
                )
            except Exception:
                pass

    # ── Alert webhook ─────────────────────────────────────

    def _fire_webhook(self, event: AuditEvent) -> None:
        """POST a critical event to the configured alert webhook.

        Fire-and-forget under a process-wide bounded concurrency gate. HTTP
        response bodies are never buffered. Timeouts are aggressive (5s
        connect, 10s total) to prevent webhook latency from stalling the audit
        pipeline.
        """
        url = self._config.alert_webhook
        if not url:
            return
        if not _WEBHOOK_SLOTS.acquire(blocking=False):
            with self._stats_lock:
                self._stats["webhooks_dropped"] = (
                    self._stats.get("webhooks_dropped", 0) + 1
                )
            logger.warning(
                "[audit-webhook] Concurrency limit reached; alert dropped"
            )
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
                    with client.stream(
                        "POST",
                        url,
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "User-Agent": "SuperTable-Audit/1.0",
                        },
                    ) as resp:
                        if resp.status_code >= 400:
                            logger.warning(
                                "[audit-webhook] POST returned %d",
                                resp.status_code,
                            )
            except Exception as e:
                # Webhook URLs commonly contain basic-auth, path, or query
                # credentials. Exception messages from HTTP clients can echo
                # the request URL too, so log only the failure class.
                logger.warning(
                    "[audit-webhook] POST failed: %s",
                    safe_audit_error_type(e),
                )
            finally:
                _WEBHOOK_SLOTS.release()

        try:
            t = threading.Thread(target=_send, daemon=True, name="audit-webhook")
            t.start()
        except Exception as e:
            _WEBHOOK_SLOTS.release()
            logger.warning(
                "[audit-webhook] Thread start failed: %s",
                safe_audit_error_type(e),
            )


# ---------------------------------------------------------------------------
# Singleton cache (one logger per organization)
# ---------------------------------------------------------------------------

_LOGGERS: Dict[str, "AuditLogger | NullAuditLogger"] = {}
_SHUTTING_DOWN = False
# One state lock linearizes config resolution/cache invalidation with logger
# replacement.  In particular, an old Redis read cannot finish after an admin
# invalidation and install a stale worker.  RLock keeps the internal resolver
# safe both when called directly and from get_audit_logger().
_LOGGERS_LOCK = threading.RLock()

# Per-org config cache: org → (config, expires_at_seconds).
# Resolved against env defaults + Redis override (supertable:{org}:system:audit:config).
_ORG_CFG_CACHE: Dict[str, "tuple[AuditConfig, float]"] = {}
_ORG_CFG_TTL_S: float = 0.0  # policy is authoritative on every resolution


def _reset_logger_state_after_fork() -> None:
    """Discard parent threads, queues, locks, and chain state in a child."""
    global _LOGGERS, _LOGGERS_LOCK, _ORG_CFG_CACHE, _PROCESS_CHAIN_LOCK
    global _PROCESS_CHAIN_STATE, _SHUTTING_DOWN, _WEBHOOK_SLOTS

    _LOGGERS = {}
    _ORG_CFG_CACHE = {}
    _PROCESS_CHAIN_STATE = {}
    _LOGGERS_LOCK = threading.RLock()
    _PROCESS_CHAIN_LOCK = threading.Lock()
    _WEBHOOK_SLOTS = threading.BoundedSemaphore(_WEBHOOK_MAX_IN_FLIGHT)
    _SHUTTING_DOWN = False


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_logger_state_after_fork)


def _resolve_config_for(organization: str) -> AuditConfig:
    """Resolve the effective AuditConfig for *organization*.

    Merges the Redis override at ``supertable:{org}:system:audit:config`` over the
    env-var defaults.  Cached for _ORG_CFG_TTL_S seconds.
    """
    with _LOGGERS_LOCK:
        now = time.time()
        cached = _ORG_CFG_CACHE.get(organization)
        if cached is not None and cached[1] > now:
            return cached[0]

        base = AuditConfig.from_settings()
        try:
            from supertable.audit.admin import get_audit_config as _get_redis_cfg
            overrides = _get_redis_cfg(organization, strict=True)
        except Exception as exc:
            if cached is not None:
                # A transient control-plane read failure must never flip an
                # already-running policy to environment defaults. Preserve the
                # last known good policy and retry soon.
                _ORG_CFG_CACHE[organization] = (cached[0], now + 5.0)
                logger.warning(
                    "[audit] Config refresh failed; preserving last-known "
                    "policy; error_type=%s",
                    safe_audit_error_type(exc),
                )
                return cached[0]
            raise AuditConfigUnavailable(
                "authoritative audit policy is unavailable"
            ) from None

        cfg = base.with_overrides(overrides) if overrides else base
        _ORG_CFG_CACHE[organization] = (cfg, now + _ORG_CFG_TTL_S)
        return cfg


def invalidate_audit_config_cache(organization: Optional[str] = None) -> None:
    """Drop the cached config for *organization* (or all orgs).  Called by the
    admin endpoint after a toggle so the change takes effect immediately."""
    with _LOGGERS_LOCK:
        if organization is None:
            _ORG_CFG_CACHE.clear()
        else:
            _ORG_CFG_CACHE.pop(organization, None)


def get_audit_logger(
    organization: str,
    *,
    action: Optional[str] = None,
) -> "AuditLogger | NullAuditLogger":
    """Return a cached AuditLogger for the organization.

    Thread-safe.  When auditing is disabled (env default OFF, or per-org
    override set to ``enabled=false``), returns a NullAuditLogger.  When the
    toggle is flipped from ON→OFF, the previously-running real logger is
    stopped and replaced with a NullAuditLogger on the next call.
    """
    with _LOGGERS_LOCK:
        if _SHUTTING_DOWN:
            return _NULL_AUDIT_LOGGER
        # Resolve under the same lock used by invalidation and replacement.
        # Otherwise a pre-invalidation Redis read can acquire this lock later
        # and overwrite the newly activated policy.
        config = _resolve_config_for(organization)
        existing = _LOGGERS.get(organization)
        if (
            existing is not None
            and not isinstance(existing, NullAuditLogger)
            and getattr(existing, "_owner_pid", os.getpid()) != os.getpid()
        ):
            # On platforms without register_at_fork, never reuse a parent
            # process's vanished worker thread or its chain identity.
            _LOGGERS.pop(organization, None)
            existing = None

        if not config.enabled:
            # If we previously had a real logger, drain & stop it.
            if existing is not None and not isinstance(existing, NullAuditLogger):
                # Keep a timed-out worker cached and closed.  Replacing or
                # forgetting it would allow two workers with the same
                # org/instance identity to touch the chain concurrently.
                existing.stop()
                _LOGGERS[organization] = NullAuditLogger()
            elif existing is None:
                _LOGGERS[organization] = _NULL_AUDIT_LOGGER
            return _LOGGERS[organization]

        # Apply effective config changes before action-specific admission. If
        # this event is suppressed, drain a stale worker and leave a Null in
        # the cache; the next admitted category constructs the replacement and
        # validates its encryption policy.
        if existing is not None and not isinstance(existing, NullAuditLogger):
            if existing._config != config:
                if action == Actions.QUERY_EXECUTE and not config.log_queries:
                    existing.stop()
                    _LOGGERS[organization] = _NULL_AUDIT_LOGGER
                    return _NULL_AUDIT_LOGGER

                existing.stop()
                replacement = AuditLogger(organization, config)
                _LOGGERS[organization] = replacement
                return replacement
            if action == Actions.QUERY_EXECUTE and not config.log_queries:
                return _NULL_AUDIT_LOGGER
            return existing

        if action == Actions.QUERY_EXECUTE and not config.log_queries:
            _LOGGERS[organization] = _NULL_AUDIT_LOGGER
            return _NULL_AUDIT_LOGGER

        # Either no logger or the cached one was a Null — create a real one.
        audit_logger = AuditLogger(organization, config)
        _LOGGERS[organization] = audit_logger
        return audit_logger


def shutdown_all() -> None:
    """Stop all audit loggers. Called on application shutdown."""
    global _SHUTTING_DOWN
    with _LOGGERS_LOCK:
        _SHUTTING_DOWN = True
        failed: Dict[str, str] = {}
        for org, audit_logger in list(_LOGGERS.items()):
            try:
                audit_logger.stop()
            except Exception as e:
                failed[org] = safe_audit_error_type(e)
                logger.warning(
                    "[audit] Shutdown error for org=%s: %s",
                    org,
                    safe_audit_error_type(e),
                )
            else:
                _LOGGERS.pop(org, None)
        if failed:
            raise AuditShutdownError(
                "one or more audit workers did not quiesce"
            )
    logger.info("[audit] All audit loggers stopped")
