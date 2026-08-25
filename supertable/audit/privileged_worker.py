"""Supervised worker for the mandatory privileged RBAC audit outbox.

The worker deliberately owns no delivery semantics.  It only schedules the
public :class:`~supertable.audit.privileged_outbox.PrivilegedAuditOutbox`
operations, so archive verification and acknowledgement remain in one place.
Run this module as a separately supervised process rather than a background
thread in an API server.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import math
import os
import random
import re
import signal
import stat
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
from supertable.audit.diagnostics import safe_audit_error_type


logger = logging.getLogger(__name__)

DEFAULT_GROUP = "__privileged_archival__"
MAX_BATCH_COUNT = 1_000
MAX_TRIM_ENTRIES = 1_000
DEFAULT_VERIFY_MAX_BATCHES = 10_000
MAX_VERIFY_BATCHES = 1_000_000
MAX_ACTIVATION_STATE_KEYS = 100_000
MAX_ACTIVATION_STATE_ENTRIES = 2_000_000
MAX_ACTIVATION_STATE_BYTES = 256 * 1024 * 1024
_MAX_EXCEPTION_ARG_BYTES = 4096
_SAFE_STREAM_ID_RE = re.compile(
    r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})"
)


def _safe_error_type(exc: BaseException) -> str:
    """Return bounded exception-type metadata without rendering a failure."""

    return safe_audit_error_type(exc)


def _exception_has_token(exc: BaseException, token: str) -> bool:
    try:
        arguments = exc.args
    except Exception:
        return False
    if not isinstance(arguments, tuple):
        return False
    for value in arguments[:4]:
        if isinstance(value, bytes):
            if len(value) > _MAX_EXCEPTION_ARG_BYTES:
                continue
            text = value.decode("ascii", errors="ignore")
        elif isinstance(value, str):
            if len(value.encode("utf-8", errors="ignore")) > _MAX_EXCEPTION_ARG_BYTES:
                continue
            text = value
        else:
            continue
        if token in text:
            return True
    return False


def _identity_ref(value: str) -> str:
    """Create a stable operational correlation value without logging identity text."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _is_safe_stream_id(value: Any) -> bool:
    if not isinstance(value, str) or not _SAFE_STREAM_ID_RE.fullmatch(value):
        return False
    milliseconds, sequence = value.split("-", 1)
    return all(
        int(component) <= 18_446_744_073_709_551_615
        for component in (milliseconds, sequence)
    )


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_durability_report(report: Any) -> None:
    try:
        values = (
            report.stream_type,
            report.delivery_type,
            report.appendonly,
            report.appendfsync,
            report.maxmemory_policy,
            report.configured_min_replicas,
            report.connected_replicas,
        )
    except Exception:
        raise RedisDurabilityError(
            "Redis durability checker returned an invalid report"
        ) from None
    if (
        values[0] not in {"none", "stream"}
        or values[1] not in {"none", "hash"}
        or values[2] != "yes"
        or values[3] not in {"always", "everysec"}
        or values[4] != "noeviction"
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= 1_000_000
            for value in values[5:]
        )
    ):
        raise RedisDurabilityError(
            "Redis durability checker returned an invalid report"
        )


class WorkerExitCode(IntEnum):
    """Stable process exit codes for service supervisors and probes."""

    OK = 0
    CONFIG = 2
    INTEGRITY = 3
    RETRY_EXHAUSTED = 4
    UNEXPECTED = 5


class RedisDurabilityError(RuntimeError):
    """The strict Redis deployment preflight could not prove durability."""


class ActivationBaselineError(RuntimeError):
    """The pinned existing-estate activation baseline is absent or changed."""


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


@dataclass(frozen=True)
class ActivationBaselineReport:
    """Identity of one deployment-pinned canonical activation baseline."""

    organization: str
    activation_id: str
    created_ms: int
    state_sha256: str
    artifact_sha256: str


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
    require_durable_redis: bool = True
    allow_everysec: bool = False
    min_replicas_to_write: int = 0
    min_connected_replicas: int = 0
    activation_baseline_path: str = ""
    activation_baseline_sha256: str = ""

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
        baseline_path = self.activation_baseline_path
        baseline_hash = self.activation_baseline_sha256
        if bool(baseline_path) != bool(baseline_hash):
            raise ValueError(
                "activation_baseline_path and activation_baseline_sha256 "
                "must be supplied together"
            )
        if baseline_hash and (
            len(baseline_hash) != 64
            or any(character not in "0123456789abcdef" for character in baseline_hash)
        ):
            raise ValueError(
                "activation_baseline_sha256 must be lowercase SHA-256 hex"
            )


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
    redis_attestations: int = 0
    baseline_attestations: int = 0


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    if type(value) is int:
        return str(value)
    raise TypeError("Redis response value must be bytes or text")


def _mapping_text(mapping: Mapping[Any, Any]) -> dict[str, str]:
    return {_text(key): _text(value) for key, value in mapping.items()}


def _config_value(redis_client: Any, name: str) -> str:
    try:
        raw = redis_client.config_get(name)
    except Exception as exc:
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} is unavailable; strict durability "
            "preflight requires CONFIG GET ACL permission"
        ) from None
    if not isinstance(raw, Mapping):
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} returned an invalid response"
        )
    try:
        decoded = _mapping_text(raw)
    except (TypeError, UnicodeError, ValueError) as exc:
        raise RedisDurabilityError(
            f"Redis CONFIG GET {name!r} returned undecodable data"
        ) from None
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
        ) from None
    return value


def verify_redis_durability(
    outbox: Any,
    *,
    allow_everysec: bool = False,
    min_replicas_to_write: int = 0,
    min_connected_replicas: int = 0,
) -> RedisDurabilityReport:
    """Fail unless Redis exposes the deployment durability contract.

    The operational CLI enables this check by default and requires an explicit
    unsafe opt-out when a managed service denies ``CONFIG GET``. It verifies
    deployment state; it does not configure Redis and is not a replacement for
    tested backups, replicas, or storage-level WORM retention.
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
            "privileged stream key has an unexpected Redis type"
        )
    if delivery_type not in {"none", "hash"}:
        raise RedisDurabilityError(
            "privileged delivery key has an unexpected Redis type"
        )

    try:
        health = outbox.health()
    except Exception as exc:
        raise RedisDurabilityError(
            "privileged outbox Redis health preflight failed; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    try:
        reachable = health.reachable
    except Exception as exc:
        raise RedisDurabilityError(
            "privileged outbox Redis health response is invalid; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    if not isinstance(reachable, bool):
        raise RedisDurabilityError(
            "privileged outbox Redis health response is invalid"
        )
    if not reachable:
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
            ) from None
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
            ) from None
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
            ) from None
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
            ) from None
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


def verify_activation_baseline(
    path: str,
    *,
    expected_sha256: str,
    organization: str,
) -> ActivationBaselineReport:
    """Verify a canonical activation artifact against a deployment pin.

    The SHA-256 is deliberately supplied out-of-band (service unit, secret
    manager, or signed deployment manifest). Trusting a mutable hash stored
    beside the baseline would let replacement of both files silently move the
    audit genesis point.
    """

    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
    ):
        raise ActivationBaselineError(
            "activation baseline pin must be lowercase SHA-256 hex"
        )
    if not isinstance(path, str) or not path:
        raise ActivationBaselineError("activation baseline path is required")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ActivationBaselineError(
            "cannot open pinned activation baseline; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ActivationBaselineError(
                "activation baseline must be a regular file"
            )
        if metadata.st_size < 1 or metadata.st_size > 1024 * 1024:
            raise ActivationBaselineError(
                "activation baseline must be between 1 byte and 1 MiB"
            )
        chunks: list[bytes] = []
        remaining = int(metadata.st_size)
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise ActivationBaselineError(
                    "activation baseline changed during read"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ActivationBaselineError(
                "activation baseline grew during read"
            )
        payload = b"".join(chunks)
    finally:
        os.close(descriptor)
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ActivationBaselineError(
            "activation baseline differs from its deployment-pinned SHA-256"
        )
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise ActivationBaselineError(
            "activation baseline is not valid JSON"
        ) from None
    if not isinstance(document, dict):
        raise ActivationBaselineError("activation baseline must be a JSON object")
    required_fields = {
        "version",
        "kind",
        "organization",
        "activation_id",
        "created_ms",
        "state_sha256",
    }
    if set(document) != required_fields:
        raise ActivationBaselineError(
            "activation baseline must contain exactly the version, kind, "
            "organization, activation_id, created_ms, and state_sha256 fields"
        )
    canonical = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    if payload != canonical:
        raise ActivationBaselineError(
            "activation baseline must use canonical JSON encoding"
        )
    if (
        document.get("version") != 1
        or document.get("kind") != "supertable_privileged_activation_baseline"
        or document.get("organization") != organization
    ):
        raise ActivationBaselineError(
            "activation baseline identity does not match this worker"
        )
    activation_id = document.get("activation_id")
    state_sha256 = document.get("state_sha256")
    created_ms = document.get("created_ms")
    if (
        not isinstance(activation_id, str)
        or not activation_id
        or len(activation_id) > 256
        or isinstance(created_ms, bool)
        or not isinstance(created_ms, int)
        or created_ms < 1
        or not isinstance(state_sha256, str)
        or len(state_sha256) != 64
        or any(character not in "0123456789abcdef" for character in state_sha256)
    ):
        raise ActivationBaselineError(
            "activation baseline contains invalid bounded identity fields"
        )
    return ActivationBaselineReport(
        organization=organization,
        activation_id=activation_id,
        created_ms=created_ms,
        state_sha256=state_sha256,
        artifact_sha256=actual_sha256,
    )


def _activation_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ActivationBaselineError(
        f"privileged-state {label} must be Redis bytes or text"
    )


def _activation_digest_frame(digest: Any, value: bytes) -> None:
    """Hash one unambiguous length-delimited byte string."""
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def compute_privileged_state_sha256(outbox: Any, organization: str) -> str:
    """Hash the exact live RBAC and auth-token estate canonically.

    The format is versioned and hashes raw Redis bytes. Keys, HASH fields, and
    SET members are sorted, so Redis iteration order and response decoding do
    not affect the result. The export is deliberately bounded; an estate over
    a bound must be partitioned/migrated deliberately rather than silently
    producing a partial activation baseline.
    """
    from supertable import redis_keys as RK

    redis_client = getattr(outbox, "_redis", None)
    stream_key = getattr(outbox, "stream_key", None)
    expected_stream_key = RK.audit_privileged_outbox(organization)
    if redis_client is None or stream_key != expected_stream_key:
        raise ActivationBaselineError(
            "outbox does not expose the expected privileged Redis identity"
        )

    discovered: dict[bytes, Any] = {}
    try:
        pattern = RK.rbac_pattern_for_org(organization)
        for raw_key in redis_client.scan_iter(match=pattern, count=1_000):
            encoded_key = _activation_bytes(raw_key, label="key")
            discovered.setdefault(encoded_key, raw_key)
            if len(discovered) > MAX_ACTIVATION_STATE_KEYS:
                raise ActivationBaselineError(
                    "privileged-state key count exceeds the activation bound"
                )

        # Organization token material and its monotonic namespace revision are
        # privileged state but live outside each SuperTable's RBAC namespace.
        for key in (RK.auth_tokens(organization), f"{RK.auth_tokens(organization)}:audit_meta"):
            key_type = _text(redis_client.type(key)).strip().lower()
            if key_type != "none":
                discovered.setdefault(key.encode("utf-8"), key)
    except ActivationBaselineError:
        raise
    except Exception as exc:
        raise ActivationBaselineError(
            "cannot enumerate live privileged Redis state; "
            f"error_type={_safe_error_type(exc)}"
        ) from None

    digest = hashlib.sha256()
    digest.update(b"supertable-privileged-state-v1\x00")
    _activation_digest_frame(digest, organization.encode("utf-8"))
    entry_count = 0
    byte_count = 0
    try:
        for encoded_key in sorted(discovered):
            raw_key = discovered[encoded_key]
            key_type = _text(redis_client.type(raw_key)).strip().lower()
            if key_type == "none":
                raise ActivationBaselineError(
                    "privileged-state key disappeared during activation export"
                )
            if key_type == "hash":
                raw_mapping = redis_client.hgetall(raw_key)
                if not isinstance(raw_mapping, Mapping):
                    raise ActivationBaselineError(
                        "privileged-state HASH returned an invalid response"
                    )
                items = sorted(
                    (
                        _activation_bytes(field, label="HASH field"),
                        _activation_bytes(value, label="HASH value"),
                    )
                    for field, value in raw_mapping.items()
                )
            elif key_type == "set":
                raw_members = redis_client.smembers(raw_key)
                if isinstance(raw_members, (str, bytes)):
                    raise ActivationBaselineError(
                        "privileged-state SET returned an invalid response"
                    )
                items = [
                    (_activation_bytes(member, label="SET member"), b"")
                    for member in sorted(
                        raw_members,
                        key=lambda member: _activation_bytes(member, label="SET member"),
                    )
                ]
            else:
                raise ActivationBaselineError(
                    "privileged-state key has an unsupported Redis type"
                )

            entry_count += len(items)
            byte_count += len(encoded_key) + sum(
                len(field) + len(value) for field, value in items
            )
            if entry_count > MAX_ACTIVATION_STATE_ENTRIES:
                raise ActivationBaselineError(
                    "privileged-state entry count exceeds the activation bound"
                )
            if byte_count > MAX_ACTIVATION_STATE_BYTES:
                raise ActivationBaselineError(
                    "privileged-state bytes exceed the activation bound"
                )

            _activation_digest_frame(digest, b"key")
            _activation_digest_frame(digest, encoded_key)
            _activation_digest_frame(digest, key_type.encode("ascii"))
            _activation_digest_frame(digest, str(len(items)).encode("ascii"))
            for field, value in items:
                _activation_digest_frame(digest, field)
                _activation_digest_frame(digest, value)
    except ActivationBaselineError:
        raise
    except Exception as exc:
        raise ActivationBaselineError(
            "cannot read live privileged Redis state; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    return digest.hexdigest()


_LUA_ATTEST_ACTIVATION_BASELINE = """
local activation_key = KEYS[1]
local outbox_key = KEYS[2]
local meta_key = KEYS[3]
local delivery_key = KEYS[4]
local expected = ARGV[1]

local function normalized_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then value = value['ok'] end
    return tostring(value)
end

local activation_type = normalized_type(activation_key)
if activation_type ~= 'none' and activation_type ~= 'string' then
    return redis.error_reply('privileged activation anchor has wrong Redis type')
end
local outbox_type = normalized_type(outbox_key)
if outbox_type ~= 'none' and outbox_type ~= 'stream' then
    return redis.error_reply('privileged audit outbox has wrong Redis type')
end
local meta_type = normalized_type(meta_key)
if meta_type ~= 'none' and meta_type ~= 'hash' then
    return redis.error_reply('privileged audit meta has wrong Redis type')
end
local delivery_type = normalized_type(delivery_key)
if delivery_type ~= 'none' and delivery_type ~= 'hash' then
    return redis.error_reply('privileged audit delivery has wrong Redis type')
end

local current = redis.call('GET', activation_key)
if current ~= false then
    if current ~= expected then
        return redis.error_reply('privileged activation anchor differs from baseline')
    end
    return 1
end
if (outbox_type == 'stream' and redis.call('XLEN', outbox_key) > 0)
    or (meta_type == 'hash' and redis.call('HLEN', meta_key) > 0)
    or (delivery_type == 'hash' and redis.call('HLEN', delivery_key) > 0) then
    return redis.error_reply(
        'privileged ledger exists without its immutable activation anchor'
    )
end
redis.call('SET', activation_key, expected)
return 2
"""


def _activation_anchor_payload(report: ActivationBaselineReport) -> str:
    return json.dumps(
        {
            "version": 1,
            "kind": "supertable_privileged_activation_anchor",
            "organization": report.organization,
            "activation_id": report.activation_id,
            "created_ms": report.created_ms,
            "state_sha256": report.state_sha256,
            "artifact_sha256": report.artifact_sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def attest_activation_baseline(
    outbox: Any,
    report: ActivationBaselineReport,
) -> bool:
    """Match/anchor the pinned baseline before any privileged ledger use.

    On first activation, new audited mutations are still fenced because their
    Lua preamble requires this anchor. Only an empty ledger may be anchored,
    and only after the live state digest matches the independently pinned
    artifact. Later calls compare the exact immutable anchor; normal audited
    state evolution therefore does not invalidate the activation genesis.
    Returns ``True`` only when this call installs the first anchor.
    """
    from supertable import redis_keys as RK

    redis_client = getattr(outbox, "_redis", None)
    if redis_client is None:
        raise ActivationBaselineError(
            "outbox does not expose Redis for activation attestation"
        )
    payload = _activation_anchor_payload(report)
    activation_key = RK.audit_privileged_activation(report.organization)
    try:
        current = redis_client.get(activation_key)
    except Exception as exc:
        raise ActivationBaselineError(
            "cannot read privileged activation anchor; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    if current is None:
        actual_state = compute_privileged_state_sha256(outbox, report.organization)
        if not hmac.compare_digest(actual_state, report.state_sha256):
            raise ActivationBaselineError(
                "activation baseline state_sha256 does not match live privileged state"
            )
    try:
        result = int(redis_client.eval(
            _LUA_ATTEST_ACTIVATION_BASELINE,
            4,
            activation_key,
            RK.audit_privileged_outbox(report.organization),
            RK.audit_privileged_meta(report.organization),
            RK.audit_privileged_delivery(report.organization),
            payload,
        ))
    except Exception as exc:
        controlled_failures = (
            (
                "privileged activation anchor differs from baseline",
                "privileged activation anchor differs from baseline",
            ),
            (
                "privileged ledger exists without its immutable activation anchor",
                "privileged ledger exists without its immutable activation anchor",
            ),
            (
                "privileged activation anchor has wrong Redis type",
                "privileged activation anchor has an invalid Redis type",
            ),
            (
                "privileged audit outbox has wrong Redis type",
                "privileged audit outbox has an invalid Redis type",
            ),
            (
                "privileged audit meta has wrong Redis type",
                "privileged audit meta has an invalid Redis type",
            ),
            (
                "privileged audit delivery has wrong Redis type",
                "privileged audit delivery has an invalid Redis type",
            ),
        )
        for token, safe_message in controlled_failures:
            if _exception_has_token(exc, token):
                raise ActivationBaselineError(safe_message) from None
        raise ActivationBaselineError(
            "privileged activation anchor attestation failed; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    if result not in {1, 2}:
        raise ActivationBaselineError(
            "privileged activation anchor returned an invalid result"
        )
    return result == 2


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
        durability_checker: Optional[Callable[..., RedisDurabilityReport]] = None,
        baseline_checker: Optional[Callable[..., ActivationBaselineReport]] = None,
        activation_checker: Optional[Callable[..., bool]] = None,
    ) -> None:
        self.outbox = outbox
        self.config = config
        self.stop_event = stop_event or threading.Event()
        self._monotonic = monotonic
        self._wait_for_stop = wait_for_stop or self.stop_event.wait
        self._random_uniform = random_uniform
        self._logger = worker_logger
        self._durability_checker = durability_checker
        self._baseline_checker = baseline_checker
        self._activation_checker = activation_checker
        self._stop_signal: Optional[int] = None
        self._pending_trim_id: Optional[str] = None
        self.stats = WorkerStats()

    def _attest_redis(self) -> None:
        """Re-prove durability against the connection serving this cycle.

        Redis clients transparently reconnect and Sentinel clients transparently
        follow a new master. A startup-only CONFIG check therefore becomes stale
        exactly when it matters most. Strict workers run the same fail-closed
        attestation before every health/drain unit.
        """

        if not self.config.require_durable_redis:
            return
        checker = self._durability_checker or verify_redis_durability
        checker(
            self.outbox,
            allow_everysec=self.config.allow_everysec,
            min_replicas_to_write=self.config.min_replicas_to_write,
            min_connected_replicas=self.config.min_connected_replicas,
        )
        self.stats.redis_attestations += 1

    def _attest_baseline(self) -> None:
        """Re-read the externally pinned activation baseline every unit."""
        if not self.config.activation_baseline_path:
            return
        checker = self._baseline_checker or verify_activation_baseline
        report = checker(
            self.config.activation_baseline_path,
            expected_sha256=self.config.activation_baseline_sha256,
            organization=self.config.organization,
        )
        if (
            getattr(report, "organization", None) != self.config.organization
            or getattr(report, "artifact_sha256", None)
            != self.config.activation_baseline_sha256
        ):
            raise ActivationBaselineError(
                "activation baseline attestation report does not match deployment pin"
            )
        activation_checker = self._activation_checker or attest_activation_baseline
        activation_checker(self.outbox, report)
        self.stats.baseline_attestations += 1

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
        checkpoint_batch_id = "-"
        checkpoint_last_sequence = 0
        if checkpoint is not None:
            raw_batch_id = checkpoint.get("batch_id")
            raw_last_sequence = checkpoint.get("last_sequence")
            if (
                not isinstance(raw_batch_id, str)
                or len(raw_batch_id) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in raw_batch_id
                )
                or isinstance(raw_last_sequence, bool)
                or not isinstance(raw_last_sequence, int)
                or raw_last_sequence < 1
            ):
                raise OutboxRecordError(
                    "archive checkpoint verification returned invalid metadata"
                )
            checkpoint_batch_id = raw_batch_id
            checkpoint_last_sequence = raw_last_sequence
        self.stats.checkpoint_verifications += 1
        health = self.outbox.health()
        stream_exists = getattr(health, "stream_exists", None)
        stream_length = getattr(health, "stream_length", None)
        group_count = getattr(health, "group_count", None)
        if (
            not isinstance(stream_exists, bool)
            or isinstance(stream_length, bool)
            or not isinstance(stream_length, int)
            or stream_length < 0
            or isinstance(group_count, bool)
            or not isinstance(group_count, int)
            or group_count < 0
        ):
            raise OutboxRecordError(
                "privileged outbox health returned invalid metadata"
            )
        self._logger.info(
            "privileged_audit_worker_heartbeat organization_ref=%s consumer_ref=%s "
            "checkpoint_present=%s checkpoint_pending=%s checkpoint_batch_id=%s "
            "checkpoint_last_sequence=%s stream_exists=%s stream_length=%s "
            "group_count=%s cycles=%s checkpoint_verifications=%s "
            "archived_batches=%s archived_events=%s acknowledged_events=%s "
            "trimmed_entries=%s transient_failures=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
            checkpoint is not None,
            checkpoint_pending,
            checkpoint_batch_id,
            checkpoint_last_sequence,
            stream_exists,
            stream_length,
            group_count,
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
            ) from None
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
            "privileged_audit_worker_chain_verified organization_ref=%s "
            "consumer_ref=%s "
            "batch_count=%s first_sequence=%s last_sequence=%s latest_batch_id=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
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
            "privileged_audit_worker_trim organization_ref=%s consumer_ref=%s "
            "through_id=%s removed_entries=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
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

        raw_stream_ids = getattr(result, "stream_ids", None)
        if not isinstance(raw_stream_ids, (tuple, list)):
            raise OutboxRecordError(
                "archive worker received invalid stream-ID metadata"
            )
        stream_ids = tuple(raw_stream_ids)
        if (
            not stream_ids
            or len(stream_ids) > self.config.count
            or any(not _is_safe_stream_id(value) for value in stream_ids)
        ):
            raise OutboxRecordError(
                "archive worker received invalid stream-ID metadata"
            )
        batch_id = getattr(result, "batch_id", None)
        acknowledged = getattr(result, "acknowledged", None)
        reused = getattr(result, "reused", None)
        if (
            not isinstance(batch_id, str)
            or len(batch_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in batch_id
            )
            or isinstance(acknowledged, bool)
            or not isinstance(acknowledged, int)
            or not 0 <= acknowledged <= len(stream_ids)
            or not isinstance(reused, bool)
        ):
            raise OutboxRecordError(
                "archive worker received invalid delivery metadata"
            )
        self.stats.archived_batches += 1
        self.stats.archived_events += len(stream_ids)
        self.stats.acknowledged_events += acknowledged
        self._logger.info(
            "privileged_audit_worker_delivery organization_ref=%s consumer_ref=%s "
            "batch_id=%s first_stream_id=%s last_stream_id=%s event_count=%s "
            "acknowledged=%s reused=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
            batch_id,
            stream_ids[0],
            stream_ids[-1],
            len(stream_ids),
            acknowledged,
            reused,
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
            "privileged_audit_worker_start organization_ref=%s consumer_ref=%s "
            "group_ref=%s "
            "mode=%s count=%s reclaim_idle_ms=%s trim=%s max_retries=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
            _identity_ref(self.config.group),
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
                # This is intentionally per unit, not just startup/heartbeat:
                # redis-py and Sentinel may reconnect between any two calls.
                self._attest_redis()
                self._attest_baseline()
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
                        "privileged_audit_worker_retry_exhausted "
                        "organization_ref=%s consumer_ref=%s failures=%s "
                        "error_type=%s",
                        _identity_ref(self.config.organization),
                        _identity_ref(self.config.consumer),
                        consecutive_failures,
                        _safe_error_type(exc),
                    )
                    exit_code = WorkerExitCode.RETRY_EXHAUSTED
                    reason = "transient retry budget exhausted"
                    break
                delay = self._retry_delay(consecutive_failures)
                self._logger.warning(
                    "privileged_audit_worker_transient_failure "
                    "organization_ref=%s consumer_ref=%s attempt=%s max_retries=%s "
                    "retry_seconds=%.3f error_type=%s",
                    _identity_ref(self.config.organization),
                    _identity_ref(self.config.consumer),
                    consecutive_failures,
                    self.config.max_retries,
                    delay,
                    _safe_error_type(exc),
                )
                if self._wait_for_stop(delay):
                    reason = "shutdown requested during retry backoff"
                    break
            except (ArchiveVerificationError, OutboxRecordError) as exc:
                self._logger.critical(
                    "privileged_audit_worker_integrity_failure "
                    "organization_ref=%s consumer_ref=%s error_type=%s",
                    _identity_ref(self.config.organization),
                    _identity_ref(self.config.consumer),
                    _safe_error_type(exc),
                )
                exit_code = WorkerExitCode.INTEGRITY
                reason = "unrecoverable archive or ledger integrity failure"
                break
            except (
                TypeError,
                ValueError,
                RedisDurabilityError,
                ActivationBaselineError,
            ) as exc:
                self._logger.critical(
                    "privileged_audit_worker_runtime_config_failure "
                    "organization_ref=%s consumer_ref=%s error_type=%s",
                    _identity_ref(self.config.organization),
                    _identity_ref(self.config.consumer),
                    _safe_error_type(exc),
                )
                exit_code = WorkerExitCode.CONFIG
                reason = "runtime configuration rejected"
                break
            except PrivilegedOutboxError as exc:
                self._logger.critical(
                    "privileged_audit_worker_unclassified_outbox_failure "
                    "organization_ref=%s consumer_ref=%s error_type=%s",
                    _identity_ref(self.config.organization),
                    _identity_ref(self.config.consumer),
                    _safe_error_type(exc),
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
                    "privileged_audit_worker_unexpected_failure "
                    "organization_ref=%s consumer_ref=%s error_type=%s",
                    _identity_ref(self.config.organization),
                    _identity_ref(self.config.consumer),
                    _safe_error_type(exc),
                )
                exit_code = WorkerExitCode.UNEXPECTED
                reason = "unexpected worker failure"
                break

        self._logger.info(
            "privileged_audit_worker_stop organization_ref=%s consumer_ref=%s "
            "exit_code=%s "
            "reason=%s signal=%s cycles=%s archived_batches=%s archived_events=%s "
            "acknowledged_events=%s trimmed_entries=%s transient_failures=%s",
            _identity_ref(self.config.organization),
            _identity_ref(self.config.consumer),
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
    parser.add_argument(
        "--activation-baseline",
        help=(
            "Canonical existing-estate/greenfield activation baseline file. "
            "Operational worker startup fails closed when it is omitted."
        ),
    )
    parser.add_argument(
        "--activation-baseline-sha256",
        help=(
            "Lowercase SHA-256 pin supplied independently from the baseline "
            "storage location."
        ),
    )
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
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Require CONFIG to prove AOF, appendfsync=always, and noeviction "
            "at startup and before every worker unit. The unsafe --no-require-"
            "durable-redis opt-out must be an explicit deployment decision."
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
        activation_baseline_path=args.activation_baseline or "",
        activation_baseline_sha256=args.activation_baseline_sha256 or "",
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
    baseline_checker: Callable[..., ActivationBaselineReport] = verify_activation_baseline,
    activation_checker: Callable[..., bool] = attest_activation_baseline,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = _config_from_args(args)
    except ValueError as exc:
        parser.error("invalid worker configuration")

    if not config.activation_baseline_path:
        logger.critical(
            "privileged_audit_worker_baseline_missing organization_ref=%s; "
            "--activation-baseline and --activation-baseline-sha256 are mandatory",
            _identity_ref(config.organization),
        )
        return int(WorkerExitCode.CONFIG)
    try:
        baseline = baseline_checker(
            config.activation_baseline_path,
            expected_sha256=config.activation_baseline_sha256,
            organization=config.organization,
        )
        if (
            not isinstance(baseline, ActivationBaselineReport)
            and not all(
                hasattr(baseline, field_name)
                for field_name in (
                    "organization",
                    "activation_id",
                    "created_ms",
                    "state_sha256",
                    "artifact_sha256",
                )
            )
        ):
            raise ActivationBaselineError(
                "activation baseline checker returned an invalid report"
            )
        if (
            baseline.organization != config.organization
            or baseline.artifact_sha256 != config.activation_baseline_sha256
            or not isinstance(baseline.activation_id, str)
            or not baseline.activation_id
            or len(baseline.activation_id) > 256
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in baseline.activation_id
            )
            or isinstance(baseline.created_ms, bool)
            or not isinstance(baseline.created_ms, int)
            or baseline.created_ms < 1
            or not _is_sha256(baseline.state_sha256)
            or not _is_sha256(baseline.artifact_sha256)
        ):
            raise ActivationBaselineError(
                "activation baseline report does not match the deployment pin"
            )
    except Exception as exc:
        logger.critical(
            "privileged_audit_worker_baseline_failure organization_ref=%s "
            "error_type=%s",
            _identity_ref(config.organization),
            _safe_error_type(exc),
        )
        return int(WorkerExitCode.CONFIG)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info(
        "privileged_audit_worker_baseline_ok organization_ref=%s "
        "created_ms=%s state_sha256=%s artifact_sha256=%s",
        _identity_ref(baseline.organization),
        baseline.created_ms,
        baseline.state_sha256,
        baseline.artifact_sha256,
    )
    factory = outbox_factory or _default_outbox_factory
    try:
        outbox = factory(config.organization)
    except Exception as exc:
        logger.critical(
            "privileged_audit_worker_factory_failure organization_ref=%s "
            "error_type=%s",
            _identity_ref(config.organization),
            _safe_error_type(exc),
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
            _validate_durability_report(report)
        except Exception as exc:
            logger.critical(
                "privileged_audit_worker_redis_preflight_failure "
                "organization_ref=%s error_type=%s",
                _identity_ref(config.organization),
                _safe_error_type(exc),
            )
            return int(WorkerExitCode.CONFIG)
        logger.info(
            "privileged_audit_worker_redis_preflight_ok organization_ref=%s "
            "stream_type=%s delivery_type=%s appendonly=%s appendfsync=%s "
            "maxmemory_policy=%s configured_min_replicas=%s connected_replicas=%s",
            _identity_ref(config.organization),
            report.stream_type,
            report.delivery_type,
            report.appendonly,
            report.appendfsync,
            report.maxmemory_policy,
            report.configured_min_replicas,
            report.connected_replicas,
        )

    worker = PrivilegedArchiveWorker(
        outbox,
        config,
        durability_checker=(durability_checker if config.require_durable_redis else None),
        baseline_checker=baseline_checker,
        activation_checker=activation_checker,
    )
    previous_handlers: dict[int, Any] = {}
    try:
        previous_handlers = _install_signal_handlers(worker)
        return worker.run()
    except (ValueError, OSError) as exc:
        logger.critical(
            "privileged_audit_worker_signal_setup_failure error_type=%s",
            _safe_error_type(exc),
        )
        return int(WorkerExitCode.CONFIG)
    finally:
        if previous_handlers:
            _restore_signal_handlers(previous_handlers)


if __name__ == "__main__":  # pragma: no cover - exercised through console script
    raise SystemExit(main())


__all__ = [
    "ActivationBaselineError",
    "ActivationBaselineReport",
    "PrivilegedArchiveWorker",
    "RedisDurabilityError",
    "RedisDurabilityReport",
    "WorkerConfig",
    "WorkerExitCode",
    "WorkerStats",
    "build_parser",
    "compute_privileged_state_sha256",
    "main",
    "attest_activation_baseline",
    "verify_redis_durability",
    "verify_activation_baseline",
]
