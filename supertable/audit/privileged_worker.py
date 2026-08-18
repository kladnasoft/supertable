"""Supervised worker for the mandatory privileged RBAC audit outbox.

The worker deliberately owns no delivery semantics.  It only schedules the
public :class:`~supertable.audit.privileged_outbox.PrivilegedAuditOutbox`
operations, so archive verification and acknowledgement remain in one place.
Run this module as a separately supervised process rather than a background
thread in an API server.
"""

from __future__ import annotations

import argparse
import logging
import math
import random
import signal
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Mapping, Optional, Sequence

from supertable.audit.privileged_outbox import (
    ArchiveVerificationError,
    DeliveryPendingError,
    OutboxBackendError,
    OutboxRecordError,
    PrivilegedOutboxError,
)


logger = logging.getLogger(__name__)

DEFAULT_GROUP = "__privileged_archival__"
MAX_BATCH_COUNT = 1_000
MAX_TRIM_ENTRIES = 1_000
DEFAULT_VERIFY_MAX_BATCHES = 10_000
MAX_VERIFY_BATCHES = 1_000_000


class WorkerExitCode(IntEnum):
    """Stable process exit codes for service supervisors and probes."""

    OK = 0
    CONFIG = 2
    INTEGRITY = 3
    RETRY_EXHAUSTED = 4
    UNEXPECTED = 5


class RedisDurabilityError(RuntimeError):
    """The strict Redis deployment preflight could not prove durability."""


@dataclass(frozen=True)
class RedisDurabilityReport:
    """Non-secret Redis settings proven by the strict startup preflight."""

    stream_type: str
    delivery_type: str
    appendonly: str
    appendfsync: str
    maxmemory_policy: str
    configured_min_replicas: int = 0
    connected_replicas: int = 0


def _validate_identity(name: str, value: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(
            f"{name} must be non-empty text without surrounding whitespace"
        )
    if len(value) > 128:
        raise ValueError(f"{name} must not exceed 128 characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} must not contain control characters")


@dataclass(frozen=True)
class WorkerConfig:
    """Validated runtime settings for one organization worker."""

    organization: str
    consumer: str
    group: str = DEFAULT_GROUP
    count: int = 100
    reclaim_idle_ms: int = 300_000
    trim: bool = False
    trim_max_entries: int = 1_000
    once: bool = False
    health_check: bool = False
    verify_chain: bool = False
    verify_max_batches: int = DEFAULT_VERIFY_MAX_BATCHES
    poll_seconds: float = 1.0
    heartbeat_seconds: float = 60.0
    max_retries: int = 8
    retry_initial_seconds: float = 1.0
    retry_max_seconds: float = 30.0
    retry_jitter: float = 0.2
    require_durable_redis: bool = False
    allow_everysec: bool = False
    min_replicas_to_write: int = 0
    min_connected_replicas: int = 0

    def __post_init__(self) -> None:
        _validate_identity("organization", self.organization)
        _validate_identity("consumer", self.consumer)
        _validate_identity("group", self.group)
        for field_name in (
            "trim",
            "once",
            "health_check",
            "verify_chain",
            "require_durable_redis",
            "allow_everysec",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise ValueError(f"{field_name} must be a boolean")
        if (
            isinstance(self.count, bool)
            or not isinstance(self.count, int)
            or not 1 <= self.count <= MAX_BATCH_COUNT
        ):
            raise ValueError(f"count must be between 1 and {MAX_BATCH_COUNT}")
        if (
            isinstance(self.reclaim_idle_ms, bool)
            or not isinstance(self.reclaim_idle_ms, int)
            or self.reclaim_idle_ms < 0
        ):
            raise ValueError("reclaim_idle_ms must be non-negative")
        if (
            isinstance(self.trim_max_entries, bool)
            or not isinstance(self.trim_max_entries, int)
            or not 1 <= self.trim_max_entries <= MAX_TRIM_ENTRIES
        ):
            raise ValueError(
                f"trim_max_entries must be between 1 and {MAX_TRIM_ENTRIES}"
            )
        if (
            isinstance(self.verify_max_batches, bool)
            or not isinstance(self.verify_max_batches, int)
            or not 1 <= self.verify_max_batches <= MAX_VERIFY_BATCHES
        ):
            raise ValueError(
                "verify_max_batches must be between 1 and "
                f"{MAX_VERIFY_BATCHES}"
            )
        if sum((self.once, self.health_check, self.verify_chain)) > 1:
            raise ValueError(
                "once, health_check, and verify_chain are mutually exclusive"
            )
        if (self.health_check or self.verify_chain) and self.trim:
            raise ValueError("read-only worker modes cannot be combined with trim")
        for field_name in (
            "poll_seconds",
            "heartbeat_seconds",
            "retry_initial_seconds",
            "retry_max_seconds",
            "retry_jitter",
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{field_name} must be a finite number")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or not 0 <= self.max_retries <= 100
        ):
            raise ValueError("max_retries must be between 0 and 100")
        if self.retry_initial_seconds <= 0:
            raise ValueError("retry_initial_seconds must be positive")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must be greater than or equal to retry_initial_seconds"
            )
        if not 0 <= self.retry_jitter <= 1:
            raise ValueError("retry_jitter must be between 0 and 1")
        for field_name in ("min_replicas_to_write", "min_connected_replicas"):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 100
            ):
                raise ValueError(f"{field_name} must be between 0 and 100")
        if self.allow_everysec and not self.require_durable_redis:
            raise ValueError("allow_everysec requires require_durable_redis")
        if (
            self.min_replicas_to_write or self.min_connected_replicas
        ) and not self.require_durable_redis:
            raise ValueError("replica thresholds require require_durable_redis")


@dataclass
class WorkerStats:
    cycles: int = 0
    empty_cycles: int = 0
    archived_batches: int = 0
    archived_events: int = 0
    acknowledged_events: int = 0
    trim_attempts: int = 0
    trimmed_entries: int = 0
    transient_failures: int = 0
    checkpoint_verifications: int = 0
    chain_verifications: int = 0


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _mapping_text(mapping: Mapping[Any, Any]) -> dict[str, str]:
    return {_text(key): _text(value) for key, value in mapping.items()}


def _config_value(redis_client: Any, name: str) -> str:
    try:
        raw = redis_client.config_get(name)
    except Exception as exc:
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} is unavailable; strict durability "
            "preflight requires CONFIG GET ACL permission"
        ) from exc
    if not isinstance(raw, Mapping):
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} returned an invalid response"
        )
    try:
        decoded = _mapping_text(raw)
    except (TypeError, UnicodeError, ValueError) as exc:
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} returned undecodable data"
        ) from exc
    value = decoded.get(name)
    if value is None:
        raise RedisDurabilityError(f"Redis CONFIG GET {name!r} returned no setting")
    return value.strip().lower()


def _redis_key_type(redis_client: Any, key: str, *, label: str) -> str:
    try:
        value = _text(redis_client.type(key)).strip().lower()
    except Exception as exc:
        raise RedisDurabilityError(
            f"Redis TYPE preflight failed for the privileged {label} key"
        ) from exc
    return value


def verify_redis_durability(
    outbox: Any,
    *,
    allow_everysec: bool = False,
    min_replicas_to_write: int = 0,
    min_connected_replicas: int = 0,
) -> RedisDurabilityReport:
    """Fail unless Redis exposes the deployment durability contract.

    This check is intentionally opt-in at the CLI because managed Redis
    services frequently deny ``CONFIG GET``.  It verifies deployment state; it
    does not configure Redis and is not a replacement for tested backups,
    replicas, or storage-level WORM retention.
    """

    if not isinstance(allow_everysec, bool):
        raise RedisDurabilityError("allow_everysec must be a boolean")
    for name, value in (
        ("min_replicas_to_write", min_replicas_to_write),
        ("min_connected_replicas", min_connected_replicas),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= 100
        ):
            raise RedisDurabilityError(f"{name} must be an integer between 0 and 100")

    redis_client = getattr(outbox, "_redis", None)
    stream_key = getattr(outbox, "stream_key", None)
    delivery_key = getattr(outbox, "delivery_ledger_key", None)
    if (
        redis_client is None
        or not isinstance(stream_key, str)
        or not isinstance(delivery_key, str)
    ):
        raise RedisDurabilityError(
            "outbox does not expose the Redis client and privileged key identities "
            "required by strict durability preflight"
        )

    stream_type = _redis_key_type(redis_client, stream_key, label="stream")
    delivery_type = _redis_key_type(redis_client, delivery_key, label="delivery")
    if stream_type not in {"none", "stream"}:
        raise RedisDurabilityError(
            f"privileged stream key has Redis type {stream_type!r}, expected 'stream' or 'none'"
        )
    if delivery_type not in {"none", "hash"}:
        raise RedisDurabilityError(
            f"privileged delivery key has Redis type {delivery_type!r}, expected 'hash' or 'none'"
        )

    try:
        health = outbox.health()
    except Exception as exc:
        raise RedisDurabilityError(
            f"privileged outbox Redis health preflight failed: {exc}"
        ) from exc
    if not bool(getattr(health, "reachable", False)):
        raise RedisDurabilityError("privileged outbox Redis health is not reachable")

    appendonly = _config_value(redis_client, "appendonly")
    appendfsync = _config_value(redis_client, "appendfsync")
    maxmemory_policy = _config_value(redis_client, "maxmemory-policy")
    failures: list[str] = []
    if appendonly != "yes":
        failures.append("appendonly must be yes")
    accepted_appendfsync = {"always", "everysec"} if allow_everysec else {"always"}
    if appendfsync not in accepted_appendfsync:
        if appendfsync == "everysec":
            failures.append(
                "appendfsync must be always unless the explicit everysec risk opt-in is set"
            )
        else:
            failures.append(
                "appendfsync must be always"
                + (" or everysec" if allow_everysec else "")
            )
    if maxmemory_policy != "noeviction":
        failures.append("maxmemory-policy must be noeviction")

    configured_min_replicas = 0
    if min_replicas_to_write:
        raw_min_replicas = _config_value(redis_client, "min-replicas-to-write")
        try:
            configured_min_replicas = int(raw_min_replicas)
        except ValueError as exc:
            raise RedisDurabilityError(
                "Redis min-replicas-to-write is not an integer"
            ) from exc
        if configured_min_replicas < min_replicas_to_write:
            failures.append(
                "min-replicas-to-write must be at least "
                f"{min_replicas_to_write}, got {configured_min_replicas}"
            )
        try:
            replica_lag = int(_config_value(redis_client, "min-replicas-max-lag"))
        except ValueError as exc:
            raise RedisDurabilityError(
                "Redis min-replicas-max-lag is not an integer"
            ) from exc
        if replica_lag <= 0:
            failures.append("min-replicas-max-lag must be positive")

    connected_replicas = 0
    if min_connected_replicas:
        try:
            raw_replication = redis_client.info("replication")
        except Exception as exc:
            raise RedisDurabilityError(
                "Redis INFO replication is unavailable; the configured connected-replica "
                "threshold cannot be verified"
            ) from exc
        if not isinstance(raw_replication, Mapping):
            raise RedisDurabilityError(
                "Redis INFO replication returned an invalid response"
            )
        replication = _mapping_text(raw_replication)
        raw_connected = replication.get(
            "connected_slaves", replication.get("connected_replicas", "0")
        )
        try:
            connected_replicas = int(raw_connected)
        except ValueError as exc:
            raise RedisDurabilityError(
                "Redis INFO replication connected-replica count is invalid"
            ) from exc
        if connected_replicas < min_connected_replicas:
            failures.append(
                "connected replicas must be at least "
                f"{min_connected_replicas}, got {connected_replicas}"
            )

    if failures:
        raise RedisDurabilityError(
            "Redis durability preflight rejected the deployment: " + "; ".join(failures)
        )
    return RedisDurabilityReport(
        stream_type=stream_type,
        delivery_type=delivery_type,
        appendonly=appendonly,
        appendfsync=appendfsync,
        maxmemory_policy=maxmemory_policy,
        configured_min_replicas=configured_min_replicas,
        connected_replicas=connected_replicas,
    )


class PrivilegedArchiveWorker:
    """Run bounded archive cycles until stopped or an error policy exits."""

    def __init__(
        self,
        outbox: Any,
        config: WorkerConfig,
        *,
        stop_event: Optional[threading.Event] = None,
        monotonic: Callable[[], float] = time.monotonic,
        wait_for_stop: Optional[Callable[[float], bool]] = None,
        random_uniform: Callable[[float, float], float] = random.uniform,
        worker_logger: logging.Logger = logger,
    ) -> None:
        self.outbox = outbox
        self.config = config
        self.stop_event = stop_event or threading.Event()
        self._monotonic = monotonic
        self._wait_for_stop = wait_for_stop or self.stop_event.wait
        self._random_uniform = random_uniform
        self._logger = worker_logger
        self._stop_signal: Optional[int] = None
        self._pending_trim_id: Optional[str] = None
        self.stats = WorkerStats()

    def request_stop(self, signum: Optional[int] = None) -> None:
        """Request cooperative shutdown after the current outbox call returns."""

        self._stop_signal = signum
        self.stop_event.set()

    def _retry_delay(self, attempt: int) -> float:
        exponent = min(max(0, attempt - 1), 30)
        ceiling = min(
            self.config.retry_max_seconds,
            self.config.retry_initial_seconds * (2**exponent),
        )
        lower = ceiling * (1.0 - self.config.retry_jitter)
        return max(0.0, min(ceiling, self._random_uniform(lower, ceiling)))

    def _log_health(self) -> None:
        checkpoint_pending = False
        try:
            checkpoint = self.outbox.verify_checkpoint_head(
                self.config.organization
            )
        except DeliveryPendingError:
            # The only headless state accepted by the outbox is an exact
            # recoverable first-batch claim. Keep the active worker alive so
            # drain_once can finish it; all malformed/deleted-head residue is
            # still an OutboxRecordError and terminates with integrity status.
            checkpoint = None
            checkpoint_pending = True
        if checkpoint is not None and not isinstance(checkpoint, Mapping):
            raise OutboxRecordError(
                "archive checkpoint verification returned an invalid result"
            )
        self.stats.checkpoint_verifications += 1
        health = self.outbox.health()
        self._logger.info(
            "privileged_audit_worker_heartbeat organization=%s consumer=%s "
            "checkpoint_present=%s checkpoint_pending=%s checkpoint_batch_id=%s "
            "checkpoint_last_sequence=%s stream_exists=%s stream_length=%s "
            "group_count=%s cycles=%s checkpoint_verifications=%s "
            "archived_batches=%s archived_events=%s acknowledged_events=%s "
            "trimmed_entries=%s transient_failures=%s",
            self.config.organization,
            self.config.consumer,
            checkpoint is not None,
            checkpoint_pending,
            checkpoint.get("batch_id", "-") if checkpoint is not None else "-",
            checkpoint.get("last_sequence", 0) if checkpoint is not None else 0,
            bool(getattr(health, "stream_exists", False)),
            int(getattr(health, "stream_length", 0)),
            int(getattr(health, "group_count", 0)),
            self.stats.cycles,
            self.stats.checkpoint_verifications,
            self.stats.archived_batches,
            self.stats.archived_events,
            self.stats.acknowledged_events,
            self.stats.trimmed_entries,
            self.stats.transient_failures,
        )

    def _verify_chain(self) -> None:
        try:
            result = self.outbox.verify_checkpoint_chain(
                self.config.organization,
                max_batches=self.config.verify_max_batches,
            )
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "archive checkpoint chain rejected stored evidence"
            ) from exc
        if not isinstance(result, Mapping):
            raise OutboxRecordError(
                "archive checkpoint chain verification returned an invalid result"
            )
        organization = result.get("organization")
        batch_count = result.get("batch_count")
        first_sequence = result.get("first_sequence")
        last_sequence = result.get("last_sequence")
        latest_batch_id = result.get("latest_batch_id")
        if organization != self.config.organization or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (batch_count, first_sequence, last_sequence)
        ):
            raise OutboxRecordError(
                "archive checkpoint chain verification returned invalid bounds"
            )
        if batch_count == 0:
            if first_sequence != 0 or last_sequence != 0 or latest_batch_id is not None:
                raise OutboxRecordError(
                    "empty archive checkpoint chain returned inconsistent bounds"
                )
        elif (
            first_sequence != 1
            or last_sequence < 1
            or not isinstance(latest_batch_id, str)
            or len(latest_batch_id) != 64
            or any(character not in "0123456789abcdef" for character in latest_batch_id)
        ):
            raise OutboxRecordError(
                "archive checkpoint chain verification returned an invalid head"
            )
        self.stats.chain_verifications += 1
        self._logger.info(
            "privileged_audit_worker_chain_verified organization=%s consumer=%s "
            "batch_count=%s first_sequence=%s last_sequence=%s latest_batch_id=%s",
            self.config.organization,
            self.config.consumer,
            batch_count,
            first_sequence,
            last_sequence,
            latest_batch_id or "-",
        )

    def _trim_pending(self) -> None:
        through_id = self._pending_trim_id
        if through_id is None:
            return
        self.stats.trim_attempts += 1
        removed = self.outbox.trim_delivered(
            self.config.group,
            through_id=through_id,
            max_entries=self.config.trim_max_entries,
        )
        self.stats.trimmed_entries += int(removed)
        self._logger.info(
            "privileged_audit_worker_trim organization=%s consumer=%s "
            "through_id=%s removed_entries=%s",
            self.config.organization,
            self.config.consumer,
            through_id,
            int(removed),
        )
        self._pending_trim_id = None

    def _run_one_unit(self) -> bool:
        # A failed trim remains attached to the verified drain result.  Retrying
        # it before reading more work avoids silently abandoning the watermark.
        if self._pending_trim_id is not None:
            self._trim_pending()
            return True

        result = self.outbox.drain_once(
            self.config.organization,
            group=self.config.group,
            consumer=self.config.consumer,
            count=self.config.count,
            reclaim_idle_ms=self.config.reclaim_idle_ms,
        )
        self.stats.cycles += 1
        if result is None:
            self.stats.empty_cycles += 1
            return False

        stream_ids = tuple(getattr(result, "stream_ids", ()))
        if not stream_ids:
            raise OutboxRecordError(
                "archive worker received a batch with no stream IDs"
            )
        acknowledged = int(getattr(result, "acknowledged", 0))
        self.stats.archived_batches += 1
        self.stats.archived_events += len(stream_ids)
        self.stats.acknowledged_events += acknowledged
        self._logger.info(
            "privileged_audit_worker_delivery organization=%s consumer=%s "
            "batch_id=%s first_stream_id=%s last_stream_id=%s event_count=%s "
            "acknowledged=%s reused=%s",
            self.config.organization,
            self.config.consumer,
            getattr(result, "batch_id", ""),
            stream_ids[0],
            stream_ids[-1],
            len(stream_ids),
            acknowledged,
            bool(getattr(result, "reused", False)),
        )
        if self.config.trim:
            # Only a verified result returned by drain_once can establish a trim
            # watermark.  The worker never invokes XACK/XDEL directly.
            self._pending_trim_id = stream_ids[-1]
            self._trim_pending()
        return True

    def run(self) -> int:
        """Run according to ``WorkerConfig`` and return a stable process code."""

        self._logger.info(
            "privileged_audit_worker_start organization=%s consumer=%s group=%s "
            "mode=%s count=%s reclaim_idle_ms=%s trim=%s max_retries=%s",
            self.config.organization,
            self.config.consumer,
            self.config.group,
            (
                "verify-chain"
                if self.config.verify_chain
                else (
                    "health-check"
                    if self.config.health_check
                    else "once" if self.config.once else "continuous"
                )
            ),
            self.config.count,
            self.config.reclaim_idle_ms,
            self.config.trim,
            self.config.max_retries,
        )

        exit_code = WorkerExitCode.OK
        reason = "stopped"
        consecutive_failures = 0
        next_heartbeat = 0.0
        while not self.stop_event.is_set():
            try:
                if self.config.verify_chain:
                    self._verify_chain()
                    reason = "checkpoint chain verification passed"
                    break
                now = self._monotonic()
                if now >= next_heartbeat:
                    self._log_health()
                    next_heartbeat = now + self.config.heartbeat_seconds
                if self.config.health_check:
                    reason = "health check passed"
                    break

                did_work = self._run_one_unit()
                consecutive_failures = 0
                if self.config.once:
                    reason = "one archive cycle completed"
                    break
                if not did_work:
                    until_heartbeat = max(0.0, next_heartbeat - self._monotonic())
                    wait_seconds = min(self.config.poll_seconds, until_heartbeat)
                    if self._wait_for_stop(wait_seconds):
                        reason = "shutdown requested"
                        break
            except (OutboxBackendError, DeliveryPendingError) as exc:
                consecutive_failures += 1
                self.stats.transient_failures += 1
                if consecutive_failures > self.config.max_retries:
                    self._logger.critical(
                        "privileged_audit_worker_retry_exhausted organization=%s "
                        "consumer=%s failures=%s error=%s",
                        self.config.organization,
                        self.config.consumer,
                        consecutive_failures,
                        exc,
                        exc_info=True,
                    )
                    exit_code = WorkerExitCode.RETRY_EXHAUSTED
                    reason = "transient retry budget exhausted"
                    break
                delay = self._retry_delay(consecutive_failures)
                self._logger.warning(
                    "privileged_audit_worker_transient_failure organization=%s "
                    "consumer=%s attempt=%s max_retries=%s retry_seconds=%.3f error=%s",
                    self.config.organization,
                    self.config.consumer,
                    consecutive_failures,
                    self.config.max_retries,
                    delay,
                    exc,
                    exc_info=True,
                )
                if self._wait_for_stop(delay):
                    reason = "shutdown requested during retry backoff"
                    break
            except (ArchiveVerificationError, OutboxRecordError) as exc:
                self._logger.critical(
                    "privileged_audit_worker_integrity_failure organization=%s "
                    "consumer=%s error=%s",
                    self.config.organization,
                    self.config.consumer,
                    exc,
                    exc_info=True,
                )
                exit_code = WorkerExitCode.INTEGRITY
                reason = "unrecoverable archive or ledger integrity failure"
                break
            except (TypeError, ValueError) as exc:
                self._logger.critical(
                    "privileged_audit_worker_runtime_config_failure organization=%s "
                    "consumer=%s error=%s",
                    self.config.organization,
                    self.config.consumer,
                    exc,
                    exc_info=True,
                )
                exit_code = WorkerExitCode.CONFIG
                reason = "runtime configuration rejected"
                break
            except PrivilegedOutboxError as exc:
                self._logger.critical(
                    "privileged_audit_worker_unclassified_outbox_failure organization=%s "
                    "consumer=%s error=%s",
                    self.config.organization,
                    self.config.consumer,
                    exc,
                    exc_info=True,
                )
                exit_code = WorkerExitCode.INTEGRITY
                reason = "unclassified privileged outbox failure"
                break
            except KeyboardInterrupt:
                self.request_stop(signal.SIGINT)
                reason = "keyboard interrupt"
                break
            except Exception as exc:
                self._logger.critical(
                    "privileged_audit_worker_unexpected_failure organization=%s "
                    "consumer=%s error=%s",
                    self.config.organization,
                    self.config.consumer,
                    exc,
                    exc_info=True,
                )
                exit_code = WorkerExitCode.UNEXPECTED
                reason = "unexpected worker failure"
                break

        self._logger.info(
            "privileged_audit_worker_stop organization=%s consumer=%s exit_code=%s "
            "reason=%s signal=%s cycles=%s archived_batches=%s archived_events=%s "
            "acknowledged_events=%s trimmed_entries=%s transient_failures=%s",
            self.config.organization,
            self.config.consumer,
            int(exit_code),
            reason,
            self._stop_signal,
            self.stats.cycles,
            self.stats.archived_batches,
            self.stats.archived_events,
            self.stats.acknowledged_events,
            self.stats.trimmed_entries,
            self.stats.transient_failures,
        )
        return int(exit_code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="supertable-privileged-audit-worker",
        description=(
            "Archive and acknowledge the mandatory privileged control-plane Redis outbox "
            "using a separately supervised worker."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--organization", required=True)
    parser.add_argument("--consumer", required=True)
    parser.add_argument("--group", default=DEFAULT_GROUP)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--reclaim-idle-ms", type=int, default=300_000)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=60.0)
    parser.add_argument(
        "--trim",
        action="store_true",
        help=(
            "After a verified drain result, invoke the outbox's conservative trim. "
            "Disabled by default because this deletes delivered Redis stream entries."
        ),
    )
    parser.add_argument("--trim-max-entries", type=int, default=1_000)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        action="store_true",
        help="Run one bounded drain/optional-trim unit and exit.",
    )
    mode.add_argument(
        "--health-check",
        action="store_true",
        help="Run only the Redis/outbox health check and exit without draining or trimming.",
    )
    mode.add_argument(
        "--verify-chain",
        action="store_true",
        help=(
            "Read and verify the full immutable checkpoint/artifact chain through "
            "genesis, then exit without draining or trimming."
        ),
    )
    parser.add_argument(
        "--verify-max-batches",
        type=int,
        default=DEFAULT_VERIFY_MAX_BATCHES,
        help=(
            "Maximum checkpoint batches a full --verify-chain run may traverse "
            f"(hard maximum {MAX_VERIFY_BATCHES})."
        ),
    )
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-initial-seconds", type=float, default=1.0)
    parser.add_argument("--retry-max-seconds", type=float, default=30.0)
    parser.add_argument("--retry-jitter", type=float, default=0.2)
    parser.add_argument(
        "--require-durable-redis",
        action="store_true",
        help=(
            "Fail startup unless CONFIG proves AOF, appendfsync=always, "
            "and noeviction; requires CONFIG GET ACL permission."
        ),
    )
    parser.add_argument(
        "--allow-everysec",
        action="store_true",
        help=(
            "With strict Redis preflight, accept appendfsync=everysec and its "
            "documented loss window instead of requiring appendfsync=always."
        ),
    )
    parser.add_argument("--min-replicas-to-write", type=int, default=0)
    parser.add_argument("--min-connected-replicas", type=int, default=0)
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> WorkerConfig:
    return WorkerConfig(
        organization=args.organization,
        consumer=args.consumer,
        group=args.group,
        count=args.count,
        reclaim_idle_ms=args.reclaim_idle_ms,
        trim=args.trim,
        trim_max_entries=args.trim_max_entries,
        once=args.once,
        health_check=args.health_check,
        verify_chain=args.verify_chain,
        verify_max_batches=args.verify_max_batches,
        poll_seconds=args.poll_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
        max_retries=args.max_retries,
        retry_initial_seconds=args.retry_initial_seconds,
        retry_max_seconds=args.retry_max_seconds,
        retry_jitter=args.retry_jitter,
        require_durable_redis=args.require_durable_redis,
        allow_everysec=args.allow_everysec,
        min_replicas_to_write=args.min_replicas_to_write,
        min_connected_replicas=args.min_connected_replicas,
    )


def _default_outbox_factory(organization: str) -> Any:
    from supertable.audit import get_privileged_audit_outbox

    return get_privileged_audit_outbox(organization)


def _install_signal_handlers(worker: PrivilegedArchiveWorker) -> dict[int, Any]:
    previous: dict[int, Any] = {}

    def request_shutdown(signum: int, _frame: Any) -> None:
        worker.request_stop(signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, request_shutdown)
    return previous


def _restore_signal_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    outbox_factory: Optional[Callable[[str], Any]] = None,
    durability_checker: Callable[..., RedisDurabilityReport] = verify_redis_durability,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = _config_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    factory = outbox_factory or _default_outbox_factory
    try:
        outbox = factory(config.organization)
    except Exception as exc:
        logger.critical(
            "privileged_audit_worker_factory_failure organization=%s error=%s",
            config.organization,
            exc,
            exc_info=True,
        )
        return int(WorkerExitCode.CONFIG)

    if config.require_durable_redis:
        try:
            report = durability_checker(
                outbox,
                allow_everysec=config.allow_everysec,
                min_replicas_to_write=config.min_replicas_to_write,
                min_connected_replicas=config.min_connected_replicas,
            )
        except Exception as exc:
            logger.critical(
                "privileged_audit_worker_redis_preflight_failure organization=%s error=%s",
                config.organization,
                exc,
                exc_info=True,
            )
            return int(WorkerExitCode.CONFIG)
        logger.info(
            "privileged_audit_worker_redis_preflight_ok organization=%s "
            "stream_type=%s delivery_type=%s appendonly=%s appendfsync=%s "
            "maxmemory_policy=%s configured_min_replicas=%s connected_replicas=%s",
            config.organization,
            report.stream_type,
            report.delivery_type,
            report.appendonly,
            report.appendfsync,
            report.maxmemory_policy,
            report.configured_min_replicas,
            report.connected_replicas,
        )

    worker = PrivilegedArchiveWorker(outbox, config)
    previous_handlers: dict[int, Any] = {}
    try:
        previous_handlers = _install_signal_handlers(worker)
        return worker.run()
    except (ValueError, OSError) as exc:
        logger.critical(
            "privileged_audit_worker_signal_setup_failure error=%s", exc, exc_info=True
        )
        return int(WorkerExitCode.CONFIG)
    finally:
        if previous_handlers:
            _restore_signal_handlers(previous_handlers)


if __name__ == "__main__":  # pragma: no cover - exercised through console script
    raise SystemExit(main())


__all__ = [
    "PrivilegedArchiveWorker",
    "RedisDurabilityError",
    "RedisDurabilityReport",
    "WorkerConfig",
    "WorkerExitCode",
    "WorkerStats",
    "build_parser",
    "main",
    "verify_redis_durability",
]
