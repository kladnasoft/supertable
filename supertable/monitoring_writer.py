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

Delivery contract
-----------------
- MonitoringWriter is a proper context manager (__enter__/__exit__)
- get_monitoring_logger(...) ALWAYS returns an object that:
    - supports context manager
    - has log_metric(payload)
    - has request_flush(...)
    - exposes queue/current_batch/queue_stats/queue_stats_lock for debug/monitor scripts
- An explicit ``SUPERTABLE_MONITORING_ENABLED=false`` returns a no-op logger.
  When monitoring is enabled, construction/cache/delivery failures are
  surfaced as ``MonitoringDeliveryError``; an outage is never disguised as a
  successful no-op.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import socket
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Literal, Optional, Protocol

try:  # POSIX interprocess durability boundary; fail clear at logger creation.
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from supertable import redis_keys as RK
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.utils.diagnostic_redaction import safe_exception_type

# ---------------------------------------------------------------------------
# Instance identity — stable for the lifetime of this process.
# Injected into every monitoring payload so the monitoring page can show
# per-instance breakdowns when multiple API instances run concurrently.
# ---------------------------------------------------------------------------
_MONITORING_INSTANCE_ID: str = f"{socket.gethostname()}:{os.getpid()}"

try:
    # If Redis is unavailable, durable local spooling remains operational.
    from supertable.redis_connector import RedisConnector
except Exception:  # pragma: no cover
    RedisConnector = None


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


class MonitoringDeliveryError(RuntimeError):
    """The enabled monitoring delivery contract was not fully satisfied."""


class MonitoringDurabilityError(MonitoringDeliveryError):
    """Monitoring could not establish or preserve a durable local record."""


class MonitoringBackpressureError(MonitoringDurabilityError):
    """The bounded monitoring spool cannot durably accept another record."""


class MonitoringExpiredRecordError(MonitoringDurabilityError):
    """A retained record outlived its immutable Redis partition window."""


def monitoring_error_type(error: BaseException) -> str:
    """Return bounded exception metadata without rendering backend text."""

    return safe_exception_type(error)


def _safe_durability_error(
    context: str,
    error: BaseException,
) -> MonitoringDurabilityError:
    return MonitoringDurabilityError(
        f"{context}; error_type={monitoring_error_type(error)}"
    )


class MonitoringPostCommitError(MonitoringDurabilityError):
    """Core data committed, but its monitoring event was not durable.

    ``core_committed`` tells callers not to blindly retry the mutation.  The
    original core result is attached so an API can report the committed
    outcome while directing an operator to restore spool capacity/durability.
    """

    def __init__(
        self,
        *,
        organization: str,
        super_name: str,
        table_name: str,
        operation: str,
        core_result: Any,
        cause: MonitoringDurabilityError,
    ) -> None:
        self.organization = organization
        self.super_name = super_name
        self.table_name = table_name
        self.operation = operation
        self.core_result = core_result
        self.core_committed = True
        self.cause_type = monitoring_error_type(cause)
        self.cause = MonitoringDurabilityError(
            "monitoring durability failure; "
            f"error_type={self.cause_type}"
        )
        self.mirror_error: Exception | None = None
        super().__init__(
            "Core data committed, but the monitoring record could not be "
            "durably retained; "
            f"error_type={self.cause_type}. "
            "Do not blindly retry the mutation."
        )


class MonitoringPostExecutionError(MonitoringDurabilityError):
    """A query completed, but its enabled monitoring event was not durable."""

    def __init__(
        self,
        *,
        organization: str,
        super_name: str,
        query_id: str,
        status: str,
        cause: MonitoringDurabilityError,
    ) -> None:
        self.organization = organization
        self.super_name = super_name
        self.query_id = query_id
        self.status = status
        self.execution_completed = True
        self.cause_type = monitoring_error_type(cause)
        self.cause = MonitoringDurabilityError(
            "monitoring durability failure; "
            f"error_type={self.cause_type}"
        )
        super().__init__(
            "Query execution completed, but its monitoring record could not "
            "be durably retained; "
            f"error_type={self.cause_type}."
        )


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

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False


def _today_utc_date() -> str:
    """UTC ISO-8601 calendar date (``YYYY-MM-DD``).

    Computed fresh on every call — never cached. Cheap (microseconds)
    and avoids the edge case where a writer thread keeps writing to
    yesterday's partition after midnight because the date was cached
    at thread start.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# Hard retention cap for every monitoring partition. The external
# orchestrator (``supertable.monitoring.partitions``) is the primary
# cleanup — it drains older partitions into SDK sink tables and deletes
# them. This TTL is the backstop: any partition that is never drained
# self-destructs, so un-flushed monitoring data is lost after at most
# this many days rather than accumulating in Redis forever.
_MONITOR_TTL_DAYS: int = 7


def _partition_expire_at(date: str) -> int:
    """Absolute EXPIREAT epoch (seconds) for a monitoring partition.

    Anchored to midnight UTC of the partition's *own* calendar date plus
    ``_MONITOR_TTL_DAYS`` days — not to the time of the last write. So a
    record written on date D is gone by the start of D+7 no matter when
    within the day it landed, giving a strict ≤7-day retention rather
    than a sliding window that a steadily-written partition could renew
    indefinitely. Recomputing it for the same date yields the same value,
    so re-applying EXPIREAT on every batch is idempotent.
    """
    midnight = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int((midnight + timedelta(days=_MONITOR_TTL_DAYS)).timestamp())


@dataclass(frozen=True)
class _MonitorKey:
    organization: str
    monitor_type: str

    @property
    def path_key(self) -> str:
        # Matches the log style: "org/monitor_type". Used as the cache
        # key for the singleton ``_MONITORS`` registry. The partition date is
        # deliberately NOT part of this key; each WAL envelope independently
        # seals its original date before delivery.
        return f"{self.organization}/{self.monitor_type}"

    def redis_list_key_today(self) -> str:
        """Resolve the Redis key for today's partition.

        Layout: ``supertable:{org}:monitor:{type}:doc:{YYYY-MM-DD}``.
        Each batch ship computes this fresh so a writer that runs
        across midnight naturally rolls into the new day's partition.
        Yesterday's partition is left untouched for the external
        orchestrator (``supertable.monitoring.partitions``) to drain.
        """
        return RK.monitor_partition(
            self.organization, self.monitor_type, _today_utc_date(),
        )

    def redis_partition_today(self) -> tuple[str, int]:
        """Today's partition key paired with its absolute EXPIREAT epoch.

        The UTC date is resolved exactly once here so the key and its
        expiry can never disagree across a midnight boundary (computing
        them separately could pin the TTL to a different day than the key
        it guards). The expiry enforces the ≤7-day retention backstop —
        see :func:`_partition_expire_at`.
        """
        date = _today_utc_date()
        key = RK.monitor_partition(self.organization, self.monitor_type, date)
        return key, _partition_expire_at(date)


_SPOOL_VERSION = 1
_PRODUCER_RECEIPT_GRACE_DAYS = 1

# KEYS[1] = monitoring LIST; KEYS[2] = producer receipt HASH.
# ARGV = delivery id, payload digest, canonical payload JSON, partition expiry,
# receipt expiry.  A reused delivery id is accepted only for the exact payload.
_IDEMPOTENT_ENQUEUE_LUA = r"""
local now = redis.call('TIME')
if tonumber(now[1]) >= tonumber(ARGV[4]) then
  return -1
end
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  if existing ~= ARGV[2] then
    return redis.error_reply('monitoring delivery id collision')
  end
  redis.call('EXPIREAT', KEYS[2], ARGV[5])
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('RPUSH', KEYS[1], ARGV[3])
redis.call('EXPIREAT', KEYS[1], ARGV[4])
redis.call('EXPIREAT', KEYS[2], ARGV[5])
return 1
"""
_DELIVERY_ID_COLLISION_ERROR = "monitoring delivery id collision"


def _is_delivery_id_collision(error: BaseException) -> bool:
    """Recognize our exact Lua error without rendering backend exception text."""

    args = getattr(error, "args", ())
    if not isinstance(args, tuple) or len(args) != 1:
        return False
    message = args[0]
    return (
        isinstance(message, str)
        and len(message) == len(_DELIVERY_ID_COLLISION_ERROR)
        and message.casefold() == _DELIVERY_ID_COLLISION_ERROR
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _monitor_spool_dir() -> str:
    configured = str(settings.SUPERTABLE_MONITOR_SPOOL_DIR or "").strip()
    if configured:
        expanded = os.path.expanduser(configured)
        if not os.path.isabs(expanded):
            raise MonitoringDurabilityError(
                "SUPERTABLE_MONITOR_SPOOL_DIR must be an absolute path"
            )
        return os.path.abspath(expanded)

    # Resolve/create the application home only when monitoring is actually
    # instantiated; importing ``supertable`` remains side-effect free.
    from supertable.config.homedir import get_app_home

    return os.path.join(get_app_home(), "monitoring-spool")


@dataclass(frozen=True)
class _SpoolRecord:
    path: str
    envelope: Dict[str, Any]
    size: int


class _DurableMonitoringSpool:
    """Bounded, crash-durable, process-safe local monitoring WAL.

    One immutable JSON file is published per event.  A process lock protects
    capacity accounting and filesystem transitions, while Redis delivery is
    intentionally outside that lock: concurrent retry is harmless because the
    Redis Lua boundary is idempotent.  No failed event is retained in memory.
    """

    def __init__(
        self,
        root: str,
        *,
        max_bytes: int,
        max_records: int,
    ) -> None:
        expanded_root = os.path.expanduser(str(root))
        if not os.path.isabs(expanded_root):
            raise MonitoringDurabilityError("monitoring spool path must be absolute")
        root = os.path.abspath(expanded_root)
        self.root = root
        self.max_bytes = int(max_bytes)
        self.max_records = int(max_records)
        if self.max_bytes <= 0 or self.max_records <= 0:
            raise MonitoringDurabilityError(
                "monitoring spool byte and record limits must be positive"
            )
        self._thread_lock = threading.RLock()
        self._lock_path = os.path.join(self.root, ".spool.lock")
        self._ensure_secure_root()
        with self._locked():
            self._recover_temps_locked()

    @staticmethod
    def _fsync_directory(path: str) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        fd = os.open(path, flags)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _ensure_secure_root(self) -> None:
        if fcntl is None:
            raise MonitoringDurabilityError(
                "durable monitoring spool requires POSIX fcntl locking on this "
                "release; disable monitoring explicitly on unsupported hosts"
            )
        failure: Optional[MonitoringDurabilityError] = None
        try:
            # A newly-created directory is not crash-durable until the parent
            # containing its directory entry has also been fsynced.  Remember
            # the complete missing chain before makedirs() so nested configured
            # paths get the same guarantee, all the way back to an ancestor
            # that already existed.
            missing_directories: list[str] = []
            candidate = self.root
            while not os.path.lexists(candidate):
                missing_directories.append(candidate)
                parent = os.path.dirname(candidate)
                if parent == candidate:  # pragma: no cover - absolute root exists
                    break
                candidate = parent

            os.makedirs(self.root, mode=0o700, exist_ok=True)
            root_stat = os.lstat(self.root)
            if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
                raise MonitoringDurabilityError(
                    "monitoring spool root is not a real directory"
                )
            if hasattr(os, "geteuid") and root_stat.st_uid != os.geteuid():
                raise MonitoringDurabilityError(
                    "monitoring spool root is not owned by this process user"
                )
            os.chmod(self.root, 0o700)
            self._fsync_directory(self.root)
            # Always persist the root's directory entry as well.  This also
            # repairs a pre-existing root created by an older process that did
            # not fsync its parent.  For a newly-created nested path, continue
            # deepest to shallowest until every new ancestor is reachable.
            parent_directories = [os.path.dirname(self.root)]
            parent_directories.extend(
                os.path.dirname(created_directory)
                for created_directory in missing_directories[1:]
            )
            for parent_directory in dict.fromkeys(parent_directories):
                self._fsync_directory(parent_directory)
        except MonitoringDurabilityError:
            raise
        except OSError as exc:
            failure = _safe_durability_error(
                "could not initialize monitoring spool", exc,
            )
        if failure is not None:
            raise failure

    @contextmanager
    def _locked(self) -> Iterator[None]:
        lock_module = fcntl
        if lock_module is None:  # pragma: no cover - guarded during init
            raise MonitoringDurabilityError(
                "durable monitoring spool requires POSIX fcntl locking"
            )
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        with self._thread_lock:
            open_failure: Optional[MonitoringDurabilityError] = None
            fd: Optional[int] = None
            try:
                fd = os.open(self._lock_path, flags, 0o600)
            except OSError as exc:
                open_failure = _safe_durability_error(
                    "could not open monitoring spool lock", exc,
                )
            if open_failure is not None:
                raise open_failure
            assert fd is not None
            operation_failure: Optional[MonitoringDurabilityError] = None
            try:
                lock_stat = os.fstat(fd)
                if not stat.S_ISREG(lock_stat.st_mode):
                    raise MonitoringDurabilityError(
                        "monitoring spool lock is not a regular file"
                    )
                os.fchmod(fd, 0o600)
                lock_module.flock(fd, lock_module.LOCK_EX)
                yield
            except MonitoringDeliveryError:
                raise
            except OSError as exc:
                operation_failure = _safe_durability_error(
                    "monitoring spool lock operation failed", exc,
                )
            finally:
                try:
                    lock_module.flock(fd, lock_module.LOCK_UN)
                finally:
                    os.close(fd)
            if operation_failure is not None:
                raise operation_failure

    @staticmethod
    def _key_hash(key: _MonitorKey) -> str:
        return hashlib.sha256(key.path_key.encode("utf-8")).hexdigest()[:24]

    @classmethod
    def _final_name(cls, envelope: Dict[str, Any]) -> str:
        key = _MonitorKey(
            organization=str(envelope["organization"]),
            monitor_type=str(envelope["monitor_type"]),
        )
        created_ns = int(envelope["created_ns"])
        delivery_id = str(envelope["delivery_id"])
        if (
            len(delivery_id) != 32
            or any(ch not in "0123456789abcdef" for ch in delivery_id)
        ):
            raise MonitoringDurabilityError("invalid monitoring delivery id in spool")
        return (
            f"{cls._key_hash(key)}-{created_ns:020d}-{delivery_id}.monitor.json"
        )

    @staticmethod
    def _is_record_name(name: str) -> bool:
        return name.endswith(".monitor.json") and not name.startswith(".")

    @staticmethod
    def _is_temp_name(name: str) -> bool:
        return name.startswith(".") and name.endswith(".monitor.json.tmp")

    def _safe_path(self, name: str) -> str:
        if not name or name != os.path.basename(name):
            raise MonitoringDurabilityError("invalid monitoring spool filename")
        path = os.path.join(self.root, name)
        if os.path.commonpath((self.root, path)) != self.root:
            raise MonitoringDurabilityError("monitoring spool path escaped its root")
        return path

    def _read_bytes(self, path: str) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
            try:
                file_stat = os.fstat(fd)
                if not stat.S_ISREG(file_stat.st_mode):
                    raise MonitoringDurabilityError(
                        "monitoring spool entry is not a regular file"
                    )
                if file_stat.st_size <= 0 or file_stat.st_size > self.max_bytes:
                    raise MonitoringDurabilityError(
                        "monitoring spool entry has invalid size"
                    )
                chunks: list[bytes] = []
                remaining = file_stat.st_size
                while remaining:
                    chunk = os.read(fd, min(remaining, 1024 * 1024))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                data = b"".join(chunks)
                if len(data) != file_stat.st_size:
                    raise MonitoringDurabilityError(
                        "monitoring spool entry changed while reading"
                    )
                return data
            finally:
                os.close(fd)
        except MonitoringDeliveryError:
            raise
        except OSError as exc:
            failure = _safe_durability_error(
                "could not read monitoring spool entry", exc,
            )
        raise failure

    @staticmethod
    def _decode_envelope(data: bytes) -> Dict[str, Any]:
        decode_failed = False
        try:
            envelope = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            decode_failed = True
        if decode_failed:
            raise MonitoringDurabilityError(
                "monitoring spool contains a corrupt record"
            )
        if not isinstance(envelope, dict) or envelope.get("version") != _SPOOL_VERSION:
            raise MonitoringDurabilityError(
                "monitoring spool contains an unsupported record"
            )
        required = {
            "delivery_id", "created_ns", "organization", "monitor_type",
            "partition_date", "partition_key", "partition_expires_at",
            "receipt_key", "receipt_expires_at", "payload_json",
            "payload_sha256",
        }
        if not required.issubset(envelope):
            raise MonitoringDurabilityError("monitoring spool record is incomplete")
        coordinates_invalid = False
        try:
            key = _MonitorKey(
                organization=str(envelope["organization"]),
                monitor_type=str(envelope["monitor_type"]),
            )
            date = str(envelope["partition_date"])
            expected_partition = RK.monitor_partition(
                key.organization, key.monitor_type, date,
            )
            expected_receipt = RK.monitor_partition_producer_receipts(
                key.organization, key.monitor_type, date,
            )
            expires_at = int(envelope["partition_expires_at"])
            receipt_expires_at = int(envelope["receipt_expires_at"])
            created_ns = int(envelope["created_ns"])
        except (TypeError, ValueError, KeyError):
            coordinates_invalid = True
        if coordinates_invalid:
            raise MonitoringDurabilityError(
                "monitoring spool record coordinates are invalid"
            )
        if (
            envelope["partition_key"] != expected_partition
            or envelope["receipt_key"] != expected_receipt
            or expires_at != _partition_expire_at(date)
            or receipt_expires_at
            != expires_at + (_PRODUCER_RECEIPT_GRACE_DAYS * 86400)
            or created_ns <= 0
        ):
            raise MonitoringDurabilityError(
                "monitoring spool record coordinates failed validation"
            )
        payload_json = envelope["payload_json"]
        digest = envelope["payload_sha256"]
        if not isinstance(payload_json, str) or not isinstance(digest, str):
            raise MonitoringDurabilityError("monitoring spool payload is invalid")
        actual_digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        if digest != actual_digest:
            raise MonitoringDurabilityError(
                "monitoring spool payload digest failed validation"
            )
        payload_corrupt = False
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload_corrupt = True
        if payload_corrupt:
            raise MonitoringDurabilityError(
                "monitoring spool payload JSON is corrupt"
            )
        if not isinstance(payload, dict):
            raise MonitoringDurabilityError(
                "monitoring spool payload must be a JSON object"
            )
        delivery_id = str(envelope["delivery_id"])
        if (
            len(delivery_id) != 32
            or any(ch not in "0123456789abcdef" for ch in delivery_id)
        ):
            raise MonitoringDurabilityError("invalid monitoring delivery id")
        envelope["payload"] = payload
        envelope["partition_expires_at"] = expires_at
        envelope["receipt_expires_at"] = receipt_expires_at
        envelope["created_ns"] = created_ns
        return envelope

    def _recover_temps_locked(self) -> None:
        changed = False
        for entry in os.scandir(self.root):
            if not self._is_temp_name(entry.name):
                continue
            path = self._safe_path(entry.name)
            entry_stat = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(entry_stat.st_mode):
                raise MonitoringDurabilityError(
                    "unsafe monitoring spool temporary entry"
                )
            # A crash before the temporary file was completely written/fsynced
            # leaves an unacknowledged fragment.  It was never published and
            # log_metric never returned, so discard that fragment instead of
            # permanently bricking recovery.  A valid complete temp is the
            # opposite crash point (after fsync, before replace) and is promoted.
            if entry_stat.st_size == 0:
                os.unlink(path)
                changed = True
                continue
            data = self._read_bytes(path)
            try:
                envelope = self._decode_envelope(data)
            except MonitoringDurabilityError:
                os.unlink(path)
                changed = True
                logger.warning(
                    "[monitor] discarded incomplete unacknowledged spool temp"
                )
                continue
            final_path = self._safe_path(self._final_name(envelope))
            if os.path.lexists(final_path):
                existing = self._read_bytes(final_path)
                if existing != data:
                    raise MonitoringDurabilityError(
                        "monitoring spool temp/final record conflict"
                    )
                os.unlink(path)
            else:
                os.replace(path, final_path)
            changed = True
        if changed:
            self._fsync_directory(self.root)

    def _usage_locked(self) -> tuple[int, int]:
        records = 0
        total_bytes = 0
        for entry in os.scandir(self.root):
            if not (self._is_record_name(entry.name) or self._is_temp_name(entry.name)):
                continue
            entry_stat = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(entry_stat.st_mode):
                raise MonitoringDurabilityError(
                    "unsafe monitoring spool entry"
                )
            records += 1
            total_bytes += int(entry_stat.st_size)
        return records, total_bytes

    def enqueue(self, key: _MonitorKey, payload: Dict[str, Any]) -> _SpoolRecord:
        canonical_failure: Optional[MonitoringDurabilityError] = None
        try:
            date = _today_utc_date()
            partition_key = RK.monitor_partition(
                key.organization, key.monitor_type, date,
            )
            receipt_key = RK.monitor_partition_producer_receipts(
                key.organization, key.monitor_type, date,
            )
            expires_at = _partition_expire_at(date)
            payload_json = _canonical_json(payload)
            envelope: Dict[str, Any] = {
                "version": _SPOOL_VERSION,
                "delivery_id": uuid.uuid4().hex,
                "created_ns": time.time_ns(),
                "organization": key.organization,
                "monitor_type": key.monitor_type,
                "partition_date": date,
                "partition_key": partition_key,
                "partition_expires_at": expires_at,
                "receipt_key": receipt_key,
                "receipt_expires_at": (
                    expires_at + (_PRODUCER_RECEIPT_GRACE_DAYS * 86400)
                ),
                "payload_json": payload_json,
                "payload_sha256": hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest(),
            }
            serialized = _canonical_json(envelope).encode("utf-8")
        except MonitoringDeliveryError:
            raise
        except (TypeError, ValueError, OverflowError) as exc:
            canonical_failure = _safe_durability_error(
                "monitoring payload is not canonical JSON", exc,
            )
        if canonical_failure is not None:
            raise canonical_failure
        if len(serialized) > self.max_bytes:
            raise MonitoringBackpressureError(
                "one monitoring record exceeds SUPERTABLE_MONITOR_SPOOL_MAX_BYTES"
            )

        final_name = self._final_name(envelope)
        temp_name = f".{final_name}.tmp"
        final_path = self._safe_path(final_name)
        temp_path = self._safe_path(temp_name)
        with self._locked():
            self._recover_temps_locked()
            record_count, byte_count = self._usage_locked()
            if record_count + 1 > self.max_records:
                raise MonitoringBackpressureError(
                    "monitoring spool record cap reached; restore Redis delivery "
                    "or increase SUPERTABLE_MONITOR_SPOOL_MAX_RECORDS"
                )
            if byte_count + len(serialized) > self.max_bytes:
                raise MonitoringBackpressureError(
                    "monitoring spool byte cap reached; restore Redis delivery "
                    "or increase SUPERTABLE_MONITOR_SPOOL_MAX_BYTES"
                )
            flags = (
                os.O_WRONLY | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0)
            )
            publish_failure: Optional[MonitoringDurabilityError] = None
            try:
                fd = os.open(temp_path, flags, 0o600)
                try:
                    with os.fdopen(fd, "wb", closefd=False) as handle:
                        handle.write(serialized)
                        handle.flush()
                        os.fsync(handle.fileno())
                finally:
                    os.close(fd)
                os.replace(temp_path, final_path)
                self._fsync_directory(self.root)
            except OSError as exc:
                publish_failure = _safe_durability_error(
                    "could not durably publish monitoring spool record", exc,
                )
            if publish_failure is not None:
                raise publish_failure
        decoded = self._decode_envelope(serialized)
        return _SpoolRecord(final_path, decoded, len(serialized))

    def pending(
        self,
        key: Optional[_MonitorKey] = None,
        *,
        limit: Optional[int] = None,
    ) -> list[_SpoolRecord]:
        prefix = self._key_hash(key) + "-" if key is not None else ""
        with self._locked():
            self._recover_temps_locked()
            names = sorted(
                entry.name
                for entry in os.scandir(self.root)
                if self._is_record_name(entry.name)
                and (not prefix or entry.name.startswith(prefix))
            )
            if limit is not None:
                names = names[:max(0, int(limit))]
            records: list[_SpoolRecord] = []
            for name in names:
                path = self._safe_path(name)
                data = self._read_bytes(path)
                envelope = self._decode_envelope(data)
                if self._final_name(envelope) != name:
                    raise MonitoringDurabilityError(
                        "monitoring spool filename does not match its record"
                    )
                if key is not None and (
                    envelope["organization"] != key.organization
                    or envelope["monitor_type"] != key.monitor_type
                ):
                    raise MonitoringDurabilityError(
                        "monitoring spool key-hash collision"
                    )
                records.append(_SpoolRecord(path, envelope, len(data)))
            return records

    def count(self, key: Optional[_MonitorKey] = None) -> int:
        return len(self.pending(key))

    def delete(self, record: _SpoolRecord) -> None:
        path = os.path.abspath(record.path)
        if os.path.dirname(path) != self.root:
            raise MonitoringDurabilityError("monitoring spool delete escaped its root")
        with self._locked():
            if not os.path.lexists(path):
                # Another process delivered and durably removed this same
                # idempotent record.
                return
            data = self._read_bytes(path)
            current = self._decode_envelope(data)
            if current["delivery_id"] != record.envelope["delivery_id"]:
                raise MonitoringDurabilityError(
                    "monitoring spool record changed before acknowledgement"
                )
            delete_failure: Optional[MonitoringDurabilityError] = None
            try:
                os.unlink(path)
                self._fsync_directory(self.root)
            except OSError as exc:
                delete_failure = _safe_durability_error(
                    "could not durably remove delivered monitoring record", exc,
                )
            if delete_failure is not None:
                raise delete_failure


def _deliver_spool_record(
    record: _SpoolRecord,
    redis_connector: Optional["RedisConnector"],
    *,
    ship_to_redis: bool,
) -> bool:
    """Return ``True`` only after Redis's idempotent Lua boundary replies."""
    envelope = record.envelope
    if not ship_to_redis:
        if time.time() >= int(envelope["partition_expires_at"]):
            raise MonitoringExpiredRecordError(
                "monitoring record outlived its original partition retention "
                f"window ({envelope['partition_date']}); it remains in the "
                "spool and requires explicit operator disposition"
            )
        logger.debug(
            "[monitor] metric accepted",
        )
        return True
    if redis_connector is None:
        return False
    receipt_collision = False
    try:
        result = redis_connector.r.eval(
            _IDEMPOTENT_ENQUEUE_LUA,
            2,
            envelope["partition_key"],
            envelope["receipt_key"],
            envelope["delivery_id"],
            envelope["payload_sha256"],
            envelope["payload_json"],
            int(envelope["partition_expires_at"]),
            int(envelope["receipt_expires_at"]),
        )
    except Exception as exc:
        if _is_delivery_id_collision(exc):
            receipt_collision = True
        else:
            return False
    if receipt_collision:
        raise MonitoringDurabilityError(
            "Redis producer receipt conflicts with the spooled payload"
        )
    invalid_receipt = False
    try:
        receipt = int(result)
    except (TypeError, ValueError, OverflowError):
        invalid_receipt = True
        receipt = 0
    if invalid_receipt:
        raise MonitoringDurabilityError(
            "Redis returned an invalid monitoring receipt"
        )
    if receipt not in (0, 1):
        if receipt == -1:
            raise MonitoringExpiredRecordError(
                "Redis refused a monitoring record at/after its immutable "
                f"partition expiry ({envelope['partition_date']}); the record "
                "remains in the local spool"
            )
        raise MonitoringDurabilityError(
            "Redis returned an invalid monitoring receipt"
        )
    return True


def drain_monitoring_spool(
    *,
    redis_connector: Optional["RedisConnector"] = None,
    spool_dir: Optional[str] = None,
    timeout_s: float = 30.0,
    batch_max: int = 500,
) -> int:
    """Replay every durable monitoring WAL record after process restart.

    This explicit recovery entry point is intentionally independent of the
    per-tenant logger cache: an application startup/health worker can drain old
    tenants even when they emit no new metric in the new process.  It returns
    the number durably acknowledged by Redis and raises when any record remains
    (network outage) or is corrupt/expired.  No record is deleted on failure.
    """
    spool = _DurableMonitoringSpool(
        spool_dir or _monitor_spool_dir(),
        max_bytes=settings.SUPERTABLE_MONITOR_SPOOL_MAX_BYTES,
        max_records=settings.SUPERTABLE_MONITOR_SPOOL_MAX_RECORDS,
    )
    connector = redis_connector
    if connector is None and RedisConnector is not None:
        try:
            connector = RedisConnector()
        except Exception:
            connector = None
    deadline = time.time() + max(0.0, float(timeout_s))
    delivered = 0
    limit = max(1, int(batch_max))
    while time.time() <= deadline:
        records = spool.pending(limit=limit)
        if not records:
            return delivered
        progressed = 0
        for record in records:
            if not _deliver_spool_record(
                record, connector, ship_to_redis=True,
            ):
                break
            spool.delete(record)
            delivered += 1
            progressed += 1
        if progressed == 0:
            break
    pending = spool.count()
    if pending:
        raise MonitoringDeliveryError(
            f"{pending} monitoring metric(s) remain durable in "
            "the local spool but unaccepted by Redis"
        )
    return delivered


class _AsyncMonitoringLogger:
    """
    Redis-delivering monitoring logger backed by a bounded durable spool.

    - ``log_metric`` returns after durable local acceptance (and opportunistic
      Redis delivery), never after a volatile in-memory enqueue.
    - the background thread retries spool records oldest-first.
    - Redis delivery is idempotent by stable delivery ID + payload digest.
    - spool-capacity/fsync failures are explicit backpressure, not drops.
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
        spool_dir: Optional[str] = None,
        spool_max_bytes: Optional[int] = None,
        spool_max_records: Optional[int] = None,
        start_worker: bool = True,
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
        if self._batch_max <= 0:
            raise MonitoringDurabilityError("monitoring batch_max must be positive")
        self._spool = _DurableMonitoringSpool(
            spool_dir or _monitor_spool_dir(),
            max_bytes=(
                settings.SUPERTABLE_MONITOR_SPOOL_MAX_BYTES
                if spool_max_bytes is None else int(spool_max_bytes)
            ),
            max_records=(
                settings.SUPERTABLE_MONITOR_SPOOL_MAX_RECORDS
                if spool_max_records is None else int(spool_max_records)
            ),
        )

        self._ship_to_redis = ship_to_redis
        self._redis = redis_connector
        if self._redis is None and RedisConnector is not None:
            try:
                self._redis = RedisConnector()
            except Exception as e:
                logger.debug(
                    "[monitor] redis unavailable; error_type=%s",
                    monitoring_error_type(e),
                )
                self._redis = None

        if start_worker:
            self._start_worker()

    # --- context manager support ---
    def __enter__(self) -> "_AsyncMonitoringLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        # Opportunistically ship the already-fsynced WAL before a short-lived
        # scope exits. Skipping this retry cannot lose the metric.
        try:
            self._try_flush()
        except Exception:
            pass
        return False

    # --- public API ---
    def log_metric(self, payload: Dict[str, Any]) -> None:
        """
        Durably enqueue a metric.

        Returning means the exact record is durable either in Redis or in the
        bounded local spool.  Redis outage is therefore safe and does not make
        a committed data write fail.  If the spool cannot fsync/publish the
        record or its hard cap is reached, a ``MonitoringDurabilityError`` is
        raised and mutation callers must surface a post-commit outcome.
        """
        if not isinstance(payload, dict):
            raise MonitoringDurabilityError("monitoring payload must be a dict")
        if "recorded_at" not in payload or "instance_id" not in payload:
            payload = dict(payload)
            if "recorded_at" not in payload:
                payload["recorded_at"] = time.time()
            payload.setdefault("instance_id", _MONITORING_INSTANCE_ID)

        with self.queue_stats_lock:
            self.queue_stats["total_received"] += 1

        # Publish the WAL entry before any Redis I/O.  This closes the crash
        # window where Redis could accept a write but its reply is lost, and it
        # makes retry safe across process restart.
        with self._ship_lock:
            enqueue_failure: Optional[MonitoringDurabilityError] = None
            try:
                self._spool.enqueue(self._key, payload)
                # Oldest-first: a new live event never leaps over an existing
                # outage backlog.  Network failure simply leaves the WAL intact.
                self._deliver_pending(max_items=self._batch_max)
                self._refresh_size()
            except MonitoringDeliveryError:
                raise
            except Exception as exc:
                # No enabled-monitoring call site may mistake a pre-WAL runtime
                # failure for optional telemetry.  If publication succeeded,
                # the record remains durable; if it did not, this explicit
                # error prevents an acknowledged gap.
                enqueue_failure = _safe_durability_error(
                    "monitoring durable enqueue failed", exc,
                )
            if enqueue_failure is not None:
                raise enqueue_failure

    def request_flush(self, timeout_s: float = 2.0) -> None:
        """
        Synchronously retry this logger's durable backlog.

        Raises ``MonitoringDeliveryError`` when Redis remains unavailable;
        records stay fsynced in the spool.  Durability/corruption/expiry errors
        are stronger failures and are never converted to best-effort success.
        """
        deadline = time.time() + max(0.0, float(timeout_s))
        with self._ship_lock:
            while time.time() <= deadline:
                before = self._spool.count(self._key)
                if before == 0:
                    self._refresh_size()
                    return
                delivered = self._deliver_pending(max_items=self._batch_max)
                if delivered == 0:
                    break
            pending = self._spool.count(self._key)
            self._refresh_size(pending=pending)
        if pending:
            raise MonitoringDeliveryError(
                f"{pending} monitoring metric(s) "
                "remain unaccepted by Redis but durable locally"
            )

    def close_if_idle(self, timeout_s: float = 0.5) -> bool:
        """Stop this worker only after proving it owns no undelivered data."""
        acquired = self._ship_lock.acquire(
            blocking=True, timeout=max(0.0, float(timeout_s)),
        )
        if not acquired:
            return False
        try:
            idle = (
                self._spool.count(self._key) == 0
                and not self.current_batch
                and self.queue.empty()
            )
            if not idle:
                return False
            self._stop.set()
        finally:
            self._ship_lock.release()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, float(timeout_s)))
        return thread is None or not thread.is_alive()

    def close_for_eviction(self, timeout_s: float = 0.5) -> bool:
        """Stop a cache worker while leaving all pending WAL files durable."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, float(timeout_s)))
        return thread is None or not thread.is_alive()

    def _try_flush(self) -> None:
        """Low-contention opportunistic retry used by ``__exit__``."""
        acquired = self._ship_lock.acquire(blocking=True, timeout=0.1)
        if not acquired:
            # Unlike the old in-memory queue, skipping here cannot lose an
            # event: ``log_metric`` already fsynced it before returning.
            return
        try:
            self._deliver_pending(max_items=self._batch_max)
            self._refresh_size()
        finally:
            self._ship_lock.release()

    # --- internals ---
    def _start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._worker,
            name="supertable-monitor",
            daemon=True,
        )
        self._thread.start()
        logger.info("[monitor] started dequeue thread")

    def _worker(self) -> None:
        backoff = 0.1
        while not self._stop.is_set():
            try:
                with self._ship_lock:
                    pending = self._spool.count(self._key)
                    delivered = (
                        self._deliver_pending(max_items=self._batch_max)
                        if pending else 0
                    )
                    self._refresh_size()
                if not pending:
                    self._stop.wait(0.1)
                elif delivered == 0:
                    logger.warning(
                        "[monitor] %d metric(s) durable in local spool",
                        pending,
                    )
                    self._stop.wait(backoff)
                    backoff = min(5.0, backoff * 2.0)
                else:
                    backoff = 0.1
            except MonitoringExpiredRecordError as exc:
                # Never recreate/re-date an old partition after its producer
                # receipts expire.  The immutable file stays for explicit
                # operator disposition and naturally applies backpressure.
                logger.error(
                    "[monitor] fail-closed spool record; error_type=%s",
                    monitoring_error_type(exc),
                )
                self._stop.wait(5.0)
            except Exception as e:
                self._clear_current_batch()
                logger.warning(
                    "[monitor] spool retry failed; error_type=%s",
                    monitoring_error_type(e),
                )
                self._stop.wait(backoff)
                backoff = min(5.0, backoff * 2.0)
            finally:
                try:
                    self._refresh_size()
                except MonitoringDeliveryError:
                    pass

    @property
    def _retry_batch(self) -> list[Dict[str, Any]]:
        """Compatibility debug view backed by disk, never by volatile memory."""
        return [record.envelope["payload"] for record in self._spool.pending(self._key)]

    def _refresh_size(self, *, pending: Optional[int] = None) -> None:
        if pending is None:
            pending = self._spool.count(self._key)
        with self.queue_stats_lock:
            self.queue_stats["current_size"] = self.queue.qsize() + pending

    def _deliver_pending(self, *, max_items: int) -> int:
        records = self._spool.pending(self._key, limit=max_items)
        if not records:
            return 0
        self._set_current_batch([
            record.envelope["payload"] for record in records
        ])
        delivered = 0
        try:
            for record in records:
                accepted = _deliver_spool_record(
                    record,
                    self._redis,
                    ship_to_redis=self._ship_to_redis,
                )
                if not accepted:
                    # Preserve global order: later records cannot leapfrog the
                    # first Redis-unaccepted record.
                    break
                self._spool.delete(record)
                delivered += 1
        finally:
            self._clear_current_batch()
        if delivered:
            with self.queue_stats_lock:
                self.queue_stats["total_processed"] += delivered
        return delivered

    def _set_current_batch(self, batch: list) -> None:
        with self.queue_stats_lock:
            self.current_batch = list(batch)

    def _clear_current_batch(self) -> None:
        with self.queue_stats_lock:
            self.current_batch = []

# ---- singleton cache (one worker per key) ----
# Bounded to prevent unbounded thread/Redis-connection growth in multi-tenant
# environments.  When the cache is full, the least-recently-used entry is
# evicted (its daemon thread stops on the next loop iteration via _stop event).
_MONITORS: Dict[str, MonitoringLogger] = {}
_MONITORS_LOCK = threading.Lock()
_MONITORS_MAX = settings.SUPERTABLE_MONITOR_CACHE_MAX


def _evict_oldest_monitor() -> bool:
    """
    Evict one entry from _MONITORS.  Must be called with _MONITORS_LOCK held.

    Strategy: evict the first key (oldest insertion in dict-order, Python 3.7+).
    Signal the evicted logger's worker thread to stop so it doesn't leak.

    Note: we intentionally do NOT close the evicted logger's Redis client.
    Since supertable.redis_connector.create_redis_client caches one
    redis.Redis (and one ConnectionPool) per effective RedisOptions, that
    client is shared with every other monitor / catalog / writer in this
    process.  Disconnecting it here would tear down their connections too.
    Call supertable.redis_connector.close_all_redis_clients() at process
    shutdown if a clean teardown is required.
    """
    for oldest_key, candidate in tuple(_MONITORS.items()):
        close_if_idle = getattr(candidate, "close_if_idle", None)
        if callable(close_if_idle) and bool(close_if_idle(timeout_s=0.5)):
            _MONITORS.pop(oldest_key, None)
            logger.debug("[monitor] evicted idle cached logger")
            return True
        # Pending records are no longer owned by volatile worker memory: they
        # are immutable files in the shared spool.  It is therefore safe to
        # stop an old tenant worker under cache pressure; the global recovery
        # API or a future logger for that key will resume the WAL.
        close_for_eviction = getattr(candidate, "close_for_eviction", None)
        if callable(close_for_eviction) and bool(
            close_for_eviction(timeout_s=0.5)
        ):
            _MONITORS.pop(oldest_key, None)
            logger.debug(
                "[monitor] evicted cached logger with durable WAL"
            )
            return True
    return False


def _monitoring_enabled() -> bool:
    raw = str(settings.SUPERTABLE_MONITORING_ENABLED).lower()
    return raw in ("1", "true", "yes", "y", "on")


def get_monitoring_logger(
    *,
    organization: str,
    monitor_type: str = "plans",
    redis_connector: Optional["RedisConnector"] = None,
) -> MonitoringLogger:
    """
    Return a monitoring logger (context manager + log_metric + request_flush + debug fields).

    Disabled monitoring returns a ``NullMonitoringLogger``. With monitoring
    enabled, invalid configuration, spool durability/capacity failures and an
    un-stoppable full worker cache raise ``MonitoringDeliveryError``.

    Per-supertable attribution is encoded **in the payload**, not in
    the Redis key — callers include a ``supertables: [str]`` field in
    each payload passed to ``log_metric``. The org-level shape lets
    cross-supertable queries record a single canonical entry.
    """
    if not _monitoring_enabled():
        return NullMonitoringLogger()

    key = _MonitorKey(organization=organization, monitor_type=monitor_type)
    cache_key = key.path_key

    logger_failure: Optional[MonitoringDurabilityError] = None
    try:
        # Validate tenant/type before allocating a thread or touching the cache.
        key.redis_partition_today()
        with _MONITORS_LOCK:
            existing = _MONITORS.get(cache_key)
            if existing is not None:
                # Dict order is the bounded cache's LRU order.
                _MONITORS.pop(cache_key)
                _MONITORS[cache_key] = existing
                return existing
            if _MONITORS_MAX <= 0:
                raise MonitoringBackpressureError(
                    "SUPERTABLE_MONITOR_CACHE_MAX must be positive when monitoring is enabled"
                )
            if len(_MONITORS) >= _MONITORS_MAX and not _evict_oldest_monitor():
                raise MonitoringBackpressureError(
                    "monitoring logger cache is full and every candidate owns "
                    "a worker that could not be stopped; the new metric was not spooled"
                )
            mon = _AsyncMonitoringLogger(key, redis_connector=redis_connector)
            _MONITORS[cache_key] = mon
            return mon
    except MonitoringDeliveryError:
        raise
    except Exception as e:
        logger.warning(
            "Monitoring logger creation failed; error_type=%s",
            monitoring_error_type(e),
        )
        logger_failure = _safe_durability_error(
            "could not create monitoring logger", e,
        )
    assert logger_failure is not None
    raise logger_failure


class MonitoringWriter:
    """
    Monitoring façade — org-level writer (SDK 2.2.0+).

    Usage:
        mon = MonitoringWriter(organization=org, monitor_type="plans")
        mon.log_metric({"supertables": ["sales", "customers"], ...})
        mon.request_flush()

    Context-manager style:
        with MonitoringWriter(organization=org, monitor_type="writes") as mon:
            mon.log_metric({"supertables": ["sales"], ...})

    Per-supertable attribution: the Redis key is org-level
    (``supertable:{org}:monitor:{monitor_type}``). Callers include a
    ``supertables: [str]`` field in each payload — the full list of
    supertables the event touches. Cross-supertable queries (JOINs,
    multi-target writes) pass the full list. Events not tied to any
    supertable pass ``supertables: []`` (or omit it — the writer
    will default to an empty list).

    Also forwards debug attributes used in examples (queue_stats, etc.).
    """

    def __init__(
        self,
        *,
        organization: str,
        monitor_type: str = "plans",
        redis_connector: Optional["RedisConnector"] = None,
    ):
        self.organization = organization
        self.monitor_type = monitor_type
        self._logger: MonitoringLogger = get_monitoring_logger(
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
