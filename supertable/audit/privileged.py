"""Validated records for durable privileged-action auditing.

This module is deliberately independent of the audit logger and Redis catalog.
It defines the value objects that a future transactional outbox can persist at
the same boundary as an RBAC mutation.  Records contain only bounded metadata
and cryptographic digests of security documents; role policies and row-filter
literals are never embedded in the record.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import time
import uuid
from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union


PRIVILEGED_AUDIT_SCHEMA_VERSION = 1
MAX_PRIVILEGED_RECORD_BYTES = 64 * 1024
MAX_SECURITY_DOCUMENT_BYTES = 1024 * 1024
MAX_ROLE_DELTA_PREVIEW = 16

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_DECIMAL_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")
_SAFE_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\-\[\]*]{0,127}$")
_VALID_ACTOR_TYPES = frozenset(
    {"user", "superuser", "api_token", "system", "mcp"}
)
_VALID_OUTCOMES = frozenset({"success", "failure", "denied", "no_change"})
_VALID_SEVERITIES = frozenset({"info", "warning", "critical"})

_CONTEXT_LIMITS = {
    "actor_type": 32,
    "actor_id": 256,
    "username": 256,
    "ip": 64,
    "user_agent": 512,
    "correlation_id": 256,
    "session_id": 256,
    "server": 128,
    "reason": 2048,
    "ticket_id": 256,
    "mutation_id": 256,
    "cause": 512,
}

_RECORD_LIMITS = {
    "event_id": 256,
    "mutation_id": 256,
    "organization": 256,
    "super_name": 256,
    "action": 128,
    "actor_type": 32,
    "actor_id": 256,
    "actor_username": 256,
    "actor_ip": 64,
    "actor_user_agent": 512,
    "correlation_id": 256,
    "session_id": 256,
    "server": 128,
    "resource_type": 128,
    "resource_id": 512,
    "cause": 512,
    "reason": 2048,
    "ticket_id": 256,
    "cascade_manifest_id": 64,
}


def _new_id() -> str:
    return uuid.uuid4().hex


def _enum_text(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _bounded_text(
    value: Any,
    *,
    field_name: str,
    max_length: int,
    required: bool = False,
) -> str:
    value = _enum_text(value)
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    value = value.strip()
    if required and not value:
        raise ValueError(f"{field_name} is required")
    if len(value) > max_length:
        raise ValueError(
            f"{field_name} exceeds the {max_length}-character limit"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field_name} contains control characters")
    return value


def _bounded_int(
    value: Any,
    *,
    field_name: str,
    minimum: int = 0,
    maximum: int = 9_223_372_036_854_775_807,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(
            f"{field_name} must be between {minimum} and {maximum}"
        )
    return value


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        rendered = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("value is not canonical JSON data") from exc
    return rendered.encode("utf-8")


def _validate_security_value(value: Any, *, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("security document nesting exceeds 64 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("security documents cannot contain NaN or infinity")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError("security document object keys must be strings")
            _validate_security_value(child, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            _validate_security_value(child, depth=depth + 1)
        return
    raise TypeError(
        "security documents may contain only JSON objects, arrays, and scalars"
    )


def canonical_security_sha256(document: Any) -> str:
    """Return the SHA-256 of a bounded canonical security document.

    The document is used transiently for hashing and is never retained.  Object
    keys are sorted and JSON is encoded as UTF-8 without insignificant spaces.
    """

    _validate_security_value(document)
    encoded = _canonical_json_bytes(document)
    if len(encoded) > MAX_SECURITY_DOCUMENT_BYTES:
        raise ValueError(
            "security document exceeds the 1-MiB canonical encoding limit"
        )
    return hashlib.sha256(encoded).hexdigest()


# Short aliases make the helper convenient without creating alternate
# implementations with subtly different canonicalisation rules.
canonical_sha256 = canonical_security_sha256
canonical_security_document_sha256 = canonical_security_sha256


@dataclass(frozen=True)
class PrivilegedActionContext:
    """Trusted, bounded actor and request context for a privileged mutation."""

    actor_type: str
    actor_id: str
    username: str = ""
    ip: str = ""
    user_agent: str = ""
    correlation_id: str = ""
    session_id: str = ""
    server: str = ""
    reason: str = ""
    ticket_id: str = ""
    mutation_id: str = ""
    cause: str = ""
    context_missing: bool = False

    def __post_init__(self) -> None:
        for name, limit in _CONTEXT_LIMITS.items():
            value = getattr(self, name)
            required = name in {"actor_type", "actor_id"}
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    value,
                    field_name=name,
                    max_length=limit,
                    required=required,
                ),
            )
        if self.actor_type not in _VALID_ACTOR_TYPES:
            raise ValueError(f"unsupported actor_type: {self.actor_type!r}")
        if not isinstance(self.context_missing, bool):
            raise TypeError("context_missing must be a boolean")
        if self.context_missing and self.actor_type != "system":
            raise ValueError("a missing context must use actor_type='system'")

    @classmethod
    def coerce(
        cls,
        value: Optional[Union["PrivilegedActionContext", Mapping[str, Any]]],
    ) -> "PrivilegedActionContext":
        """Coerce an instance, strict mapping, or legacy missing context.

        ``None`` is represented explicitly rather than silently pretending that
        an authenticated system actor was present.  Callers that intentionally
        run as the system must use :meth:`system`.
        """

        if isinstance(value, cls):
            return value
        if value is None:
            return cls(
                actor_type="system",
                actor_id="legacy-unattributed",
                cause="missing_context",
                context_missing=True,
            )
        if not isinstance(value, Mapping):
            raise TypeError(
                "privileged action context must be None, a mapping, or an instance"
            )

        aliases = {
            "actor_username": "username",
            "actor_ip": "ip",
            "actor_user_agent": "user_agent",
        }
        allowed = {field.name for field in fields(cls)}
        normalized: Dict[str, Any] = {}
        for raw_key, item in value.items():
            if not isinstance(raw_key, str):
                raise TypeError("context field names must be strings")
            key = aliases.get(raw_key, raw_key)
            if key not in allowed:
                raise ValueError(f"unknown privileged context field: {raw_key!r}")
            if key in normalized:
                raise ValueError(f"duplicate privileged context field: {key!r}")
            normalized[key] = item

        missing_marker = normalized.get("context_missing", False)
        if not isinstance(missing_marker, bool):
            raise TypeError("context_missing must be a boolean")
        if missing_marker:
            normalized.setdefault("actor_type", "system")
            normalized.setdefault("actor_id", "legacy-unattributed")
        if "actor_type" not in normalized or "actor_id" not in normalized:
            raise ValueError("actor_type and actor_id are required")
        return cls(**normalized)

    @classmethod
    def system(
        cls,
        reason: str,
        *,
        actor_id: str = "system",
        username: str = "",
        ip: str = "",
        user_agent: str = "",
        correlation_id: str = "",
        session_id: str = "",
        server: str = "",
        ticket_id: str = "",
        mutation_id: str = "",
        cause: str = "system",
    ) -> "PrivilegedActionContext":
        """Construct an explicit system actor; ``reason`` is mandatory."""

        checked_reason = _bounded_text(
            reason,
            field_name="reason",
            max_length=_CONTEXT_LIMITS["reason"],
            required=True,
        )
        return cls(
            actor_type="system",
            actor_id=actor_id,
            username=username,
            ip=ip,
            user_agent=user_agent,
            correlation_id=correlation_id,
            session_id=session_id,
            server=server,
            reason=checked_reason,
            ticket_id=ticket_id,
            mutation_id=mutation_id,
            cause=cause,
            context_missing=False,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
        }


def _canonical_string_tuple(
    values: Any,
    *,
    field_name: str,
    max_items: int,
    item_limit: int,
    field_names_only: bool = False,
) -> Tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(
        values, Iterable
    ):
        raise TypeError(f"{field_name} must be an iterable of strings")
    checked = []
    for value in values:
        item = _bounded_text(
            value,
            field_name=f"{field_name} item",
            max_length=item_limit,
            required=True,
        )
        if field_names_only and not _SAFE_FIELD_RE.fullmatch(item):
            raise ValueError(f"invalid changed field name: {item!r}")
        checked.append(item)
        if len(checked) > max_items:
            raise ValueError(f"{field_name} exceeds the {max_items}-item limit")
    return tuple(sorted(set(checked)))


def _checked_sha256(value: Any, *, field_name: str) -> str:
    value = _bounded_text(
        value,
        field_name=field_name,
        max_length=64,
        required=False,
    ).lower()
    if value and not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be an empty string or SHA-256 hex")
    return value


@dataclass(frozen=True)
class PrivilegedAuditRecord:
    """Versioned, digest-only evidence for one privileged state transition."""

    schema_version: int = PRIVILEGED_AUDIT_SCHEMA_VERSION
    event_id: str = ""
    mutation_id: str = ""
    timestamp_ms: int = 0
    organization: str = ""
    super_name: str = ""
    action: str = ""
    outcome: str = "success"
    severity: str = "warning"
    actor_type: str = "system"
    actor_id: str = "legacy-unattributed"
    actor_username: str = ""
    actor_ip: str = ""
    actor_user_agent: str = ""
    correlation_id: str = ""
    session_id: str = ""
    server: str = ""
    context_missing: bool = True
    resource_type: str = ""
    resource_id: str = ""
    before_version: int = 0
    after_version: int = 0
    before_sha256: str = ""
    after_sha256: str = ""
    changed_fields: Tuple[str, ...] = ()
    role_ids_added: Tuple[str, ...] = ()
    role_ids_removed: Tuple[str, ...] = ()
    role_ids_added_count: int = 0
    role_ids_removed_count: int = 0
    role_ids_added_sha256: str = ""
    role_ids_removed_sha256: str = ""
    cause: str = ""
    reason: str = ""
    ticket_id: str = ""
    namespace_version: int = 0
    affected_count: int = 0
    cascade_manifest_id: str = ""
    cascade_assignment_count: int = 0
    user_namespace_version_before: int = 0
    user_namespace_version_after: int = 0
    ledger_sequence: int = 0
    payload_hash: str = ""

    def __post_init__(self) -> None:
        schema_version = _bounded_int(
            self.schema_version,
            field_name="schema_version",
            minimum=1,
            maximum=1,
        )
        object.__setattr__(self, "schema_version", schema_version)

        required_text = {
            "event_id",
            "mutation_id",
            "organization",
            "action",
            "actor_type",
            "actor_id",
            "resource_type",
            "resource_id",
        }
        for name, limit in _RECORD_LIMITS.items():
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    field_name=name,
                    max_length=limit,
                    required=name in required_text,
                ),
            )

        object.__setattr__(
            self,
            "timestamp_ms",
            _bounded_int(self.timestamp_ms, field_name="timestamp_ms", minimum=1),
        )
        for name in (
            "before_version",
            "after_version",
            "namespace_version",
            "affected_count",
            "cascade_assignment_count",
            "user_namespace_version_before",
            "user_namespace_version_after",
            "ledger_sequence",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), field_name=name),
            )

        if self.actor_type not in _VALID_ACTOR_TYPES:
            raise ValueError(f"unsupported actor_type: {self.actor_type!r}")
        outcome = _enum_text(self.outcome)
        severity = _enum_text(self.severity)
        if not isinstance(outcome, str) or outcome not in _VALID_OUTCOMES:
            raise ValueError(f"unsupported outcome: {outcome!r}")
        if not isinstance(severity, str) or severity not in _VALID_SEVERITIES:
            raise ValueError(f"unsupported severity: {severity!r}")
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "severity", severity)

        # Successful role deletion always owns an exact affected-user
        # sidecar.  Its identifier is the event ID, making the relationship
        # immutable without introducing another independently generated ID.
        # Non-success attempts and all other actions must not claim a sidecar.
        manifest_id = self.cascade_manifest_id
        if self.action == "role_delete" and outcome == "success":
            manifest_id = manifest_id or self.event_id
            if manifest_id != self.event_id:
                raise ValueError("cascade_manifest_id must equal event_id")
            manifest_id = _bounded_text(
                manifest_id,
                field_name="cascade_manifest_id",
                max_length=_RECORD_LIMITS["cascade_manifest_id"],
                required=True,
            )
        elif manifest_id:
            raise ValueError(
                "cascade manifests are valid only for successful role_delete events"
            )
        object.__setattr__(self, "cascade_manifest_id", manifest_id)

        if not isinstance(self.context_missing, bool):
            raise TypeError("context_missing must be a boolean")
        if self.context_missing and self.actor_type != "system":
            raise ValueError("a missing context must use actor_type='system'")

        object.__setattr__(
            self,
            "before_sha256",
            _checked_sha256(self.before_sha256, field_name="before_sha256"),
        )
        object.__setattr__(
            self,
            "after_sha256",
            _checked_sha256(self.after_sha256, field_name="after_sha256"),
        )
        object.__setattr__(
            self,
            "changed_fields",
            _canonical_string_tuple(
                self.changed_fields,
                field_name="changed_fields",
                max_items=128,
                item_limit=128,
                field_names_only=True,
            ),
        )
        object.__setattr__(
            self,
            "role_ids_added",
            _canonical_string_tuple(
                self.role_ids_added,
                field_name="role_ids_added",
                max_items=MAX_ROLE_DELTA_PREVIEW,
                item_limit=256,
            ),
        )
        object.__setattr__(
            self,
            "role_ids_removed",
            _canonical_string_tuple(
                self.role_ids_removed,
                field_name="role_ids_removed",
                max_items=MAX_ROLE_DELTA_PREVIEW,
                item_limit=256,
            ),
        )
        for prefix in ("role_ids_added", "role_ids_removed"):
            preview = getattr(self, prefix)
            count_name = f"{prefix}_count"
            hash_name = f"{prefix}_sha256"
            count = _bounded_int(getattr(self, count_name), field_name=count_name)
            if count == 0:
                count = len(preview)
            if count < len(preview):
                raise ValueError(f"{count_name} cannot be smaller than its preview")
            supplied_digest = _checked_sha256(
                getattr(self, hash_name), field_name=hash_name,
            )
            if count == len(preview):
                computed_digest = (
                    canonical_security_sha256(list(preview)) if count else ""
                )
                if supplied_digest and not hmac.compare_digest(
                    supplied_digest, computed_digest,
                ):
                    raise ValueError(f"{hash_name} does not match its role IDs")
                supplied_digest = supplied_digest or computed_digest
            elif not supplied_digest:
                raise ValueError(
                    f"{hash_name} is required when the role-ID delta is truncated"
                )
            object.__setattr__(self, count_name, count)
            object.__setattr__(self, hash_name, supplied_digest)

        computed_hash = self.compute_payload_hash()
        supplied_hash = _checked_sha256(
            self.payload_hash, field_name="payload_hash"
        )
        if supplied_hash and not hmac.compare_digest(supplied_hash, computed_hash):
            raise ValueError("payload_hash does not match the canonical record")
        object.__setattr__(self, "payload_hash", supplied_hash or computed_hash)

        if not manifest_id and any((
            self.cascade_assignment_count,
            self.user_namespace_version_before,
            self.user_namespace_version_after,
        )):
            raise ValueError("cascade commit fields require a cascade manifest")
        if manifest_id:
            if self.cascade_assignment_count < self.affected_count:
                raise ValueError(
                    "cascade_assignment_count cannot be smaller than affected_count"
                )
            if self.affected_count == 0:
                if self.cascade_assignment_count != 0:
                    raise ValueError(
                        "an empty cascade cannot remove role assignments"
                    )
                if (
                    self.user_namespace_version_before
                    != self.user_namespace_version_after
                ):
                    raise ValueError(
                        "an empty cascade cannot advance the user namespace"
                    )
            elif (
                self.user_namespace_version_after
                != self.user_namespace_version_before + 1
            ):
                raise ValueError(
                    "a non-empty cascade must advance the user namespace once"
                )

        if len(_canonical_json_bytes(self.to_dict())) > MAX_PRIVILEGED_RECORD_BYTES:
            raise ValueError("privileged audit record exceeds the 64-KiB limit")

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for field in fields(self):
            value = getattr(self, field.name)
            result[field.name] = list(value) if isinstance(value, tuple) else value
        return result

    def to_json(self) -> str:
        encoded = _canonical_json_bytes(self.to_dict())
        if len(encoded) > MAX_PRIVILEGED_RECORD_BYTES:
            raise ValueError("privileged audit record exceeds the 64-KiB limit")
        return encoded.decode("utf-8")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PrivilegedAuditRecord":
        if not isinstance(value, Mapping):
            raise TypeError("privileged audit record must be an object")
        known = {field.name for field in fields(cls)}
        supplied = set(value)
        unknown = supplied - known
        missing = known - supplied
        if unknown:
            raise ValueError(
                f"unknown privileged audit fields: {sorted(str(x) for x in unknown)}"
            )
        if missing:
            raise ValueError(
                f"missing privileged audit fields: {sorted(missing)}"
            )
        normalized = dict(value)
        if len(_canonical_json_bytes(normalized)) > MAX_PRIVILEGED_RECORD_BYTES:
            raise ValueError("privileged audit record exceeds the 64-KiB limit")
        for name in ("changed_fields", "role_ids_added", "role_ids_removed"):
            if not isinstance(normalized[name], list):
                raise TypeError(f"{name} must be a JSON array")
        # Redis Lua emits commit-assigned counters as exact decimal strings so
        # cjson cannot round values above JavaScript's 2**53 precision limit.
        # No other integer field receives string coercion.
        for name in (
            "ledger_sequence",
            "namespace_version",
            "affected_count",
            "cascade_assignment_count",
            "user_namespace_version_before",
            "user_namespace_version_after",
        ):
            raw = normalized[name]
            if isinstance(raw, str):
                if (
                    len(raw) > 19
                    or not _CANONICAL_DECIMAL_RE.fullmatch(raw)
                ):
                    raise ValueError(
                        f"{name} must be a canonical unsigned decimal string"
                    )
                normalized[name] = int(raw)
        return cls(**normalized)

    @classmethod
    def from_json(cls, value: Union[str, bytes, bytearray]) -> "PrivilegedAuditRecord":
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            encoded = bytes(value)
        else:
            raise TypeError("privileged audit JSON must be text or bytes")
        if len(encoded) > MAX_PRIVILEGED_RECORD_BYTES:
            raise ValueError("privileged audit record exceeds the 64-KiB limit")
        try:
            text = encoded.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("privileged audit JSON is not valid UTF-8") from exc

        def no_duplicate_keys(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
            result: Dict[str, Any] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON field: {key!r}")
                result[key] = item
            return result

        try:
            parsed = json.loads(
                text,
                object_pairs_hook=no_duplicate_keys,
                parse_constant=lambda token: (_ for _ in ()).throw(
                    ValueError(f"invalid JSON number: {token}")
                ),
            )
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("invalid privileged audit JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("privileged audit JSON must contain an object")
        return cls.from_dict(parsed)

    def compute_payload_hash(self) -> str:
        payload = self.to_dict()
        # These fields are assigned atomically by the mutation/outbox commit
        # script.  They authenticate the ledger position and mutation result,
        # but cannot participate in the pre-commit payload fingerprint.
        payload.pop("ledger_sequence", None)
        payload.pop("namespace_version", None)
        payload.pop("affected_count", None)
        payload.pop("cascade_assignment_count", None)
        payload.pop("user_namespace_version_before", None)
        payload.pop("user_namespace_version_after", None)
        payload.pop("payload_hash", None)
        return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()

    def with_ledger_sequence(self, sequence: int) -> "PrivilegedAuditRecord":
        """Return a sequenced copy without changing the immutable payload hash."""

        checked = _bounded_int(
            sequence, field_name="ledger_sequence", minimum=1
        )
        return replace(self, ledger_sequence=checked)

    def to_audit_event(self):
        """Convert to the existing flat :class:`AuditEvent` archival schema."""

        from supertable.audit.events import AuditEvent, EventCategory

        safe_detail = {
            "schema_version": self.schema_version,
            "mutation_id": self.mutation_id,
            "context_missing": self.context_missing,
            "before_version": self.before_version,
            "after_version": self.after_version,
            "before_sha256": self.before_sha256,
            "after_sha256": self.after_sha256,
            "changed_fields": list(self.changed_fields),
            "role_ids_added": list(self.role_ids_added),
            "role_ids_removed": list(self.role_ids_removed),
            "role_ids_added_count": self.role_ids_added_count,
            "role_ids_removed_count": self.role_ids_removed_count,
            "role_ids_added_sha256": self.role_ids_added_sha256,
            "role_ids_removed_sha256": self.role_ids_removed_sha256,
            "cause": self.cause,
            "ticket_id": self.ticket_id,
            "namespace_version": self.namespace_version,
            "affected_count": self.affected_count,
            "cascade_manifest_id": self.cascade_manifest_id,
            "cascade_assignment_count": self.cascade_assignment_count,
            "user_namespace_version_before": self.user_namespace_version_before,
            "user_namespace_version_after": self.user_namespace_version_after,
            "ledger_sequence": self.ledger_sequence,
            "payload_hash": self.payload_hash,
        }
        return AuditEvent(
            event_id=self.event_id,
            timestamp_ms=self.timestamp_ms,
            category=EventCategory.RBAC_CHANGE.value,
            action=self.action,
            severity=self.severity,
            actor_type=self.actor_type,
            actor_id=self.actor_id,
            actor_username=self.actor_username,
            actor_ip=self.actor_ip,
            actor_user_agent=self.actor_user_agent,
            organization=self.organization,
            super_name=self.super_name,
            correlation_id=self.correlation_id,
            session_id=self.session_id,
            server=self.server,
            resource_type=self.resource_type,
            resource_id=self.resource_id,
            detail=_canonical_json_bytes(safe_detail).decode("utf-8"),
            outcome=self.outcome,
            reason=self.reason,
        )

    # Readable alias for callers that prefer conversion verbs over ``to_*``.
    as_audit_event = to_audit_event


def build_record(
    *,
    context: Optional[
        Union[PrivilegedActionContext, Mapping[str, Any]]
    ] = None,
    organization: str,
    super_name: str,
    action: str,
    resource_type: str,
    resource_id: str,
    before_document: Any = None,
    after_document: Any = None,
    before_version: int = 0,
    after_version: int = 0,
    changed_fields: Iterable[str] = (),
    role_ids_added: Iterable[str] = (),
    role_ids_removed: Iterable[str] = (),
    outcome: Any = "success",
    severity: Any = "warning",
    namespace_version: int = 0,
    affected_count: int = 0,
    cascade_manifest_id: str = "",
    cascade_assignment_count: int = 0,
    user_namespace_version_before: int = 0,
    user_namespace_version_after: int = 0,
    ledger_sequence: int = 0,
    event_id: Optional[str] = None,
    mutation_id: Optional[str] = None,
    timestamp_ms: Optional[int] = None,
    cause: Optional[str] = None,
    reason: Optional[str] = None,
    ticket_id: Optional[str] = None,
) -> PrivilegedAuditRecord:
    """Build a digest-only privileged record from pre/post security documents."""

    action_context = PrivilegedActionContext.coerce(context)
    added_full = _canonical_string_tuple(
        role_ids_added,
        field_name="role_ids_added",
        max_items=32_768,
        item_limit=256,
    )
    removed_full = _canonical_string_tuple(
        role_ids_removed,
        field_name="role_ids_removed",
        max_items=32_768,
        item_limit=256,
    )
    return PrivilegedAuditRecord(
        schema_version=PRIVILEGED_AUDIT_SCHEMA_VERSION,
        event_id=event_id or _new_id(),
        mutation_id=(mutation_id or action_context.mutation_id or _new_id()),
        timestamp_ms=(
            timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        ),
        organization=organization,
        super_name=super_name,
        action=action,
        outcome=_enum_text(outcome),
        severity=_enum_text(severity),
        actor_type=action_context.actor_type,
        actor_id=action_context.actor_id,
        actor_username=action_context.username,
        actor_ip=action_context.ip,
        actor_user_agent=action_context.user_agent,
        correlation_id=action_context.correlation_id,
        session_id=action_context.session_id,
        server=action_context.server,
        context_missing=action_context.context_missing,
        resource_type=resource_type,
        resource_id=resource_id,
        before_version=before_version,
        after_version=after_version,
        before_sha256=(
            "" if before_document is None
            else canonical_security_sha256(before_document)
        ),
        after_sha256=(
            "" if after_document is None
            else canonical_security_sha256(after_document)
        ),
        changed_fields=changed_fields,
        role_ids_added=added_full[:MAX_ROLE_DELTA_PREVIEW],
        role_ids_removed=removed_full[:MAX_ROLE_DELTA_PREVIEW],
        role_ids_added_count=len(added_full),
        role_ids_removed_count=len(removed_full),
        role_ids_added_sha256=(
            canonical_security_sha256(list(added_full)) if added_full else ""
        ),
        role_ids_removed_sha256=(
            canonical_security_sha256(list(removed_full)) if removed_full else ""
        ),
        cause=action_context.cause if cause is None else cause,
        reason=action_context.reason if reason is None else reason,
        ticket_id=(
            action_context.ticket_id if ticket_id is None else ticket_id
        ),
        namespace_version=namespace_version,
        affected_count=affected_count,
        cascade_manifest_id=cascade_manifest_id,
        cascade_assignment_count=cascade_assignment_count,
        user_namespace_version_before=user_namespace_version_before,
        user_namespace_version_after=user_namespace_version_after,
        ledger_sequence=ledger_sequence,
    )


RecordInput = Union[
    PrivilegedAuditRecord,
    Mapping[str, Any],
    str,
    bytes,
    bytearray,
]


def verify_records(
    records: Iterable[RecordInput],
    *,
    expected_start: Optional[int] = None,
) -> bool:
    """Verify payload hashes and a contiguous, positive ledger sequence.

    Records are checked in the supplied order.  A non-empty sequence may start
    at any positive number unless ``expected_start`` is supplied.
    """

    if expected_start is not None:
        try:
            expected_start = _bounded_int(
                expected_start,
                field_name="expected_start",
                minimum=1,
            )
        except (TypeError, ValueError):
            return False

    previous: Optional[int] = None
    seen_event_ids = set()
    try:
        for item in records:
            if isinstance(item, PrivilegedAuditRecord):
                record = item
            elif isinstance(item, Mapping):
                record = PrivilegedAuditRecord.from_dict(item)
            elif isinstance(item, (str, bytes, bytearray)):
                record = PrivilegedAuditRecord.from_json(item)
            else:
                return False

            if record.ledger_sequence <= 0:
                return False
            if previous is None:
                if expected_start is not None and record.ledger_sequence != expected_start:
                    return False
            elif record.ledger_sequence != previous + 1:
                return False
            if record.event_id in seen_event_ids:
                return False
            seen_event_ids.add(record.event_id)
            if not hmac.compare_digest(
                record.payload_hash, record.compute_payload_hash()
            ):
                return False
            previous = record.ledger_sequence
    except (TypeError, ValueError, OverflowError):
        return False
    return True


__all__ = [
    "MAX_PRIVILEGED_RECORD_BYTES",
    "PRIVILEGED_AUDIT_SCHEMA_VERSION",
    "PrivilegedActionContext",
    "PrivilegedAuditRecord",
    "build_record",
    "canonical_security_sha256",
    "canonical_security_document_sha256",
    "canonical_sha256",
    "verify_records",
]
