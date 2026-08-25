# route: supertable.audit.privileged_outbox
"""Durable Redis outbox access for privileged RBAC audit events.

The normal audit logger is deliberately best-effort.  This module is not: a
Redis error is surfaced to the caller, malformed stream records are rejected,
and stream entries are only removable after an archive delivery marker exists.

Keys are supplied by the caller so this module can be introduced independently
of the catalog and key-registry changes which produce the stream.
"""
from __future__ import annotations

import hashlib
import importlib
import io
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from supertable.audit.chain import compute_file_hash
from supertable.audit.diagnostics import safe_audit_error_type
from supertable.audit.writer_parquet import (
    AuditReadError,
    AuditReadLimitError,
    ParquetAuditWriter,
    _BoundedSpillWriter,
    _validated_object_metadata,
)
from supertable.utils.diagnostic_redaction import safe_value_type

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - pyarrow is a required project dependency
    pa = None  # type: ignore
    pq = None  # type: ignore


_MAX_EXCEPTION_ARG_BYTES = 4096


def _safe_error_type(exc: BaseException) -> str:
    """Return a bounded, non-message diagnostic for an untrusted failure."""

    return safe_audit_error_type(exc)


def _safe_type_name(value: Any) -> str:
    return safe_value_type(value)


def _exception_has_token(exc: BaseException, token: str) -> bool:
    """Recognize one transport status token without rendering the exception."""

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
        if token.casefold() in text.casefold():
            return True
    return False


class PrivilegedOutboxError(RuntimeError):
    """Base class for durable privileged-audit errors."""


class OutboxBackendError(PrivilegedOutboxError):
    """Redis or archive storage failed; an empty result must not be inferred."""

    def __init__(self, operation: str, cause: BaseException):
        cause_type = _safe_error_type(cause)
        super().__init__(
            f"privileged outbox {operation} failed; error_type={cause_type}"
        )
        self.operation = operation
        self.cause_type = cause_type
        self.cause = RuntimeError(
            f"privileged outbox backend failure; error_type={cause_type}"
        )


class OutboxRecordError(PrivilegedOutboxError):
    """A Redis stream entry does not contain a valid ``event_json`` record."""


class ArchiveVerificationError(PrivilegedOutboxError):
    """The Parquet writer result could not be verified in durable storage."""


class DeliveryPendingError(PrivilegedOutboxError):
    """Trimming was requested before every selected entry was delivered."""


@dataclass(frozen=True)
class OutboxEntry:
    """One decoded privileged-audit stream entry."""

    stream_id: str
    # Exact pre-commit template bytes stored by the Lua producer.
    event_json: str
    # Validated event after exact commit-envelope counters are applied.
    event: Mapping[str, Any]
    committed_event_json: str


@dataclass(frozen=True)
class CascadeEvidenceRow:
    """One exact user assignment transition caused by a role deletion."""

    user_id: str
    before_doc_version: int
    after_doc_version: int
    removed_occurrences: int
    before_role_count: int
    after_role_count: int


@dataclass(frozen=True)
class CascadeManifest:
    """Bounded-row forensic evidence linked to one role-delete event."""

    schema_version: int
    event_id: str
    mutation_id: str
    organization: str
    super_name: str
    role_id: str
    user_count: int
    removed_assignment_count: int
    user_namespace_version_before: int
    user_namespace_version_after: int
    created_ms: int
    rows: Tuple[CascadeEvidenceRow, ...]


@dataclass(frozen=True)
class AutoClaimResult:
    """Result of Redis ``XAUTOCLAIM``."""

    next_start_id: str
    entries: Tuple[OutboxEntry, ...]
    deleted_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveBatchResult:
    """A verified Parquet delivery, identified independently of retries."""

    batch_id: str
    stream_ids: Tuple[str, ...]
    archive: Mapping[str, Any]
    acknowledged: int
    reused: bool = False


@dataclass(frozen=True)
class OutboxHealth:
    """A successful end-to-end Redis health snapshot."""

    reachable: bool
    stream_exists: bool
    stream_length: int
    group_count: int


ArchiveVerifier = Callable[[Mapping[str, Any]], bool]

_SAFE_CASCADE_USER_ID = re.compile(
    r"^(?:__[a-z0-9][a-z0-9_-]{0,59}__|[a-z0-9][a-z0-9_-]{0,63})$"
)
_CASCADE_META_FIELDS = frozenset({
    "schema_version",
    "event_id",
    "mutation_id",
    "organization",
    "super_name",
    "role_id",
    "user_count",
    "removed_assignment_count",
    "user_namespace_version_before",
    "user_namespace_version_after",
    "created_ms",
})


def _bounded_decimal(
    value: Any,
    *,
    field: str,
    minimum: int = 0,
) -> int:
    text_value = _text(value, field=field)
    if (
        not text_value.isascii()
        or not text_value.isdigit()
        or len(text_value) > 19
        or (len(text_value) > 1 and text_value.startswith("0"))
    ):
        raise OutboxRecordError(f"{field} is not a canonical bounded decimal")
    number = int(text_value)
    if number < minimum or number > 9_223_372_036_854_775_807:
        raise OutboxRecordError(f"{field} is outside the signed 64-bit range")
    return number


def _text(value: Any, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OutboxRecordError(f"{field} is not valid UTF-8") from None
    if isinstance(value, str):
        return value
    raise OutboxRecordError(
        f"{field} must be bytes or str; value_type={_safe_type_name(value)}"
    )


def _stream_id_tuple(stream_id: str) -> Tuple[int, int]:
    try:
        milliseconds, sequence = stream_id.split("-", 1)
        if (
            not milliseconds.isascii()
            or not sequence.isascii()
            or not milliseconds.isdigit()
            or not sequence.isdigit()
            or len(milliseconds) > 20
            or len(sequence) > 20
            or (len(milliseconds) > 1 and milliseconds.startswith("0"))
            or (len(sequence) > 1 and sequence.startswith("0"))
        ):
            raise ValueError("non-canonical Redis stream ID")
        parsed = int(milliseconds), int(sequence)
        if any(value > 18_446_744_073_709_551_615 for value in parsed):
            raise ValueError("Redis stream ID component exceeds uint64")
        return parsed
    except (AttributeError, TypeError, ValueError) as exc:
        raise OutboxRecordError("invalid Redis stream id") from None


def _safe_archive_organization(value: Any) -> str:
    """Apply the canonical Redis organization-segment policy to paths too."""

    from supertable import redis_keys as RK

    try:
        RK.audit_privileged_outbox(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("organization is not safe for audit storage") from None
    return value


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _load_shared_record(payload: Dict[str, Any], event_json: str) -> Mapping[str, Any]:
    """Use the shared privileged record validator when that module is present.

    The producer module is being introduced independently.  Its public
    ``PrivilegedAuditRecord`` contract is detected lazily so process startup
    ordering does not decide whether records are validated.  JSON-native
    parsing is preferred when supplied by the shared record type.  The exact
    input JSON remains authoritative for archival because Redis Lua may
    reserialize a semantically valid record in a different key order.
    """
    try:
        module = importlib.import_module("supertable.audit.privileged")
    except ModuleNotFoundError as exc:
        if exc.name == "supertable.audit.privileged":
            raise OutboxRecordError(
                "supertable.audit.privileged is unavailable; privileged records cannot be verified"
            ) from None
        raise

    record_type = getattr(module, "PrivilegedAuditRecord", None)
    if record_type is None:
        raise OutboxRecordError("supertable.audit.privileged has no PrivilegedAuditRecord validator")
    try:
        from_json = getattr(record_type, "from_json", None)
        record = from_json(event_json) if callable(from_json) else record_type.from_dict(payload)
    except Exception as exc:
        raise OutboxRecordError(
            "event_json failed privileged record validation; "
            f"error_type={_safe_error_type(exc)}"
        ) from None
    if hasattr(record, "to_dict"):
        value = record.to_dict()
        if isinstance(value, Mapping):
            return dict(value)
    if isinstance(record, Mapping):
        return dict(record)
    raise OutboxRecordError("PrivilegedAuditRecord.from_dict() did not return a mapping-compatible record")


class PrivilegedAuditOutbox:
    """Read, consume, archive, and conservatively trim a privileged event stream."""

    _LEDGER_VERSION = 1
    _MAX_ARCHIVE_BATCH_ENTRIES = 1000
    _MAX_CASCADE_MANIFEST_USERS = 10_000
    _MAX_CASCADE_ARCHIVE_ROWS = 100_000
    _MAX_LEDGER_RECORD_BYTES = 1024 * 1024
    _MAX_PARQUET_ARTIFACT_BYTES = 256 * 1024 * 1024
    _ARCHIVE_HEAD_FIELD = "archive:head"
    _CLAIM_ARCHIVE_BATCH_LUA = """
local ledger_key = KEYS[1]
local type_result = redis.call('TYPE', ledger_key)
local ledger_type = type(type_result) == 'table' and type_result['ok'] or type_result
if ledger_type ~= 'none' and ledger_type ~= 'hash' then
    return redis.error_reply('privileged delivery ledger has wrong Redis type')
end

local head_field = ARGV[1]
local expected_head_present = ARGV[2]
local expected_head = ARGV[3]
local claim_field = ARGV[4]
local batch_id = ARGV[5]
local batch_field = ARGV[6]
local batch_json = ARGV[7]

local current_head = redis.call('HGET', ledger_key, head_field)
if expected_head_present == '1' then
    if current_head ~= expected_head then
        return {-1, current_head or ''}
    end
elseif current_head then
    return {-1, current_head}
end

local current_claim = redis.call('HGET', ledger_key, claim_field)
local current_batch = redis.call('HGET', ledger_key, batch_field)
if current_claim and current_claim ~= batch_id then
    return {-2, current_claim, current_batch or ''}
end
if current_batch then
    if not current_claim then
        return {-3, '', current_batch}
    end
    return {2, current_claim, current_batch}
end
if current_claim then
    return {-3, current_claim, ''}
end

redis.call(
    'HSET', ledger_key,
    claim_field, batch_id,
    batch_field, batch_json
)
return {1, batch_id, batch_json}
"""
    _CAS_ARCHIVE_BATCH_LUA = """
local ledger_key = KEYS[1]
local type_result = redis.call('TYPE', ledger_key)
local ledger_type = type(type_result) == 'table' and type_result['ok'] or type_result
if ledger_type ~= 'hash' then
    return redis.error_reply('privileged delivery ledger has wrong Redis type')
end
local field = ARGV[1]
local expected = ARGV[2]
local replacement = ARGV[3]
local current = redis.call('HGET', ledger_key, field)
if current == replacement then return {2, current} end
if current ~= expected then return {0, current or ''} end
redis.call('HSET', ledger_key, field, replacement)
return {1, replacement}
"""
    _FINALIZE_ARCHIVE_BATCH_LUA = """
local ledger_key = KEYS[1]
local type_result = redis.call('TYPE', ledger_key)
local ledger_type = type(type_result) == 'table' and type_result['ok'] or type_result
if ledger_type ~= 'hash' then
    return redis.error_reply('privileged delivery ledger has wrong Redis type')
end

local batch_field = ARGV[1]
local expected_batch = ARGV[2]
local delivered_batch = ARGV[3]
local head_field = ARGV[4]
local expected_head_present = ARGV[5]
local expected_head = ARGV[6]
local replacement_head = ARGV[7]
local batch_id = ARGV[8]
local marker_count = tonumber(ARGV[9])

local current_batch = redis.call('HGET', ledger_key, batch_field)
if current_batch ~= expected_batch then
    return {0, current_batch or '', redis.call('HGET', ledger_key, head_field) or ''}
end
local current_head = redis.call('HGET', ledger_key, head_field)
if expected_head_present == '1' then
    if current_head ~= expected_head then
        return {-1, current_batch, current_head or ''}
    end
elseif current_head then
    return {-1, current_batch, current_head}
end

for index = 1, marker_count do
    local field = ARGV[9 + index]
    local existing = redis.call('HGET', ledger_key, field)
    if existing and existing ~= batch_id then
        return {-2, field, existing}
    end
end

local updates = {
    batch_field, delivered_batch,
    head_field, replacement_head
}
for index = 1, marker_count do
    updates[#updates + 1] = ARGV[9 + index]
    updates[#updates + 1] = batch_id
end
redis.call('HSET', ledger_key, unpack(updates))
return {1, delivered_batch, replacement_head}
"""
    _TRIM_DELIVERED_LUA = """
local stream_key = KEYS[1]
local ledger_key = KEYS[2]
local stream_count = tonumber(ARGV[1])
local gc_count = tonumber(ARGV[2])
local cursor = 3
local ledger_type_result = redis.call('TYPE', ledger_key)
local ledger_type = type(ledger_type_result) == 'table'
    and ledger_type_result['ok'] or ledger_type_result
if ledger_type ~= 'hash' then
    return redis.error_reply('privileged delivery ledger has wrong Redis type')
end
for index = 1, stream_count do
    local stream_id = ARGV[cursor]
    cursor = cursor + 1
    local present = redis.call(
        'XRANGE', stream_key, stream_id, stream_id, 'COUNT', 1
    )
    if #present ~= 1 then
        return redis.error_reply(
            'verified privileged stream entry disappeared before trim'
        )
    end
end
local gc_fields = {}
for index = 1, gc_count do
    local field = ARGV[cursor]
    local expected = ARGV[cursor + 1]
    cursor = cursor + 2
    local actual = redis.call('HGET', ledger_key, field)
    if actual ~= expected then
        return redis.error_reply(
            'verified privileged delivery metadata changed before trim'
        )
    end
    gc_fields[#gc_fields + 1] = field
end
for key_index = 3, #KEYS do
    local manifest_key = KEYS[key_index]
    local field_count = tonumber(ARGV[cursor])
    cursor = cursor + 1
    if redis.call('HLEN', manifest_key) ~= field_count then
        return redis.error_reply(
            'verified role-delete cascade manifest changed before trim'
        )
    end
    for field_index = 1, field_count do
        local field = ARGV[cursor]
        local expected = ARGV[cursor + 1]
        cursor = cursor + 2
        if redis.call('HGET', manifest_key, field) ~= expected then
            return redis.error_reply(
                'verified role-delete cascade manifest changed before trim'
            )
        end
    end
end
local stream_ids = {}
for index = 1, stream_count do
    stream_ids[#stream_ids + 1] = ARGV[2 + index]
end
local removed_entries = redis.call('XDEL', stream_key, unpack(stream_ids))
local removed_manifests = 0
if #KEYS > 2 then
    local manifest_keys = {}
    for index = 3, #KEYS do
        manifest_keys[#manifest_keys + 1] = KEYS[index]
    end
    removed_manifests = redis.call('DEL', unpack(manifest_keys))
end
local removed_metadata = 0
if #gc_fields > 0 then
    removed_metadata = redis.call('HDEL', ledger_key, unpack(gc_fields))
end
if removed_entries ~= stream_count
    or removed_manifests ~= (#KEYS - 2)
    or removed_metadata ~= gc_count then
    return redis.error_reply('verified privileged trim was not exact')
end
return {removed_entries, removed_manifests, removed_metadata}
"""
    _INDEX_FIELDS = (
        "event_id",
        "mutation_id",
        "action",
        "resource_type",
        "resource_id",
        "organization",
        "super_name",
        "ledger_sequence",
        "namespace_version",
        "affected_count",
        "cascade_manifest_id",
        "cascade_assignment_count",
        "user_namespace_version_before",
        "user_namespace_version_after",
        "payload_hash",
    )
    _COMMIT_FIELDS = (
        "ledger_sequence",
        "namespace_version",
        "affected_count",
        "cascade_assignment_count",
        "user_namespace_version_before",
        "user_namespace_version_after",
    )

    def __init__(
        self,
        redis_client: Any,
        *,
        stream_key: str,
        delivery_ledger_key: str,
        parquet_writer: Optional[ParquetAuditWriter] = None,
        archive_verifier: Optional[ArchiveVerifier] = None,
        clock_ms: Optional[Callable[[], int]] = None,
    ) -> None:
        if not stream_key or not delivery_ledger_key:
            raise ValueError("stream_key and delivery_ledger_key are required")
        if stream_key == delivery_ledger_key:
            raise ValueError("stream_key and delivery_ledger_key must be different Redis keys")
        self._redis = redis_client
        self.stream_key = stream_key
        self.delivery_ledger_key = delivery_ledger_key
        self._writer = parquet_writer
        self._archive_verifier = archive_verifier
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))

    # -- Redis boundary -------------------------------------------------

    def _redis_call(self, operation: str, method: str, *args: Any, **kwargs: Any) -> Any:
        try:
            return getattr(self._redis, method)(*args, **kwargs)
        except Exception as exc:
            raise OutboxBackendError(operation, exc) from None

    @staticmethod
    def _sealed_read_bytes(
        storage: Any,
        path: str,
        *,
        maximum_size: int,
        expected_size: Optional[int] = None,
        artifact: str = "privileged archive",
    ) -> bytes:
        """Read one exact object version through a hard byte ceiling."""
        stat_object = getattr(storage, "stat_object", None)
        download_to_file = getattr(storage, "download_to_file", None)
        if not callable(stat_object) or not callable(download_to_file):
            raise ArchiveVerificationError(
                "privileged archive storage lacks sealed streaming reads"
            )
        try:
            observed = _validated_object_metadata(
                stat_object(path), minimum_size=1, maximum_size=maximum_size,
            )
            if expected_size is not None and observed.size != expected_size:
                raise ArchiveVerificationError(
                    f"{artifact} size differs from its immutable claim"
                )
            buffer = io.BytesIO()
            sink = _BoundedSpillWriter(buffer, observed.size)
            downloaded = download_to_file(
                path,
                sink,
                expected=observed,
                chunk_size=min(1024 * 1024, maximum_size),
            )
            if (
                type(downloaded) is not int
                or downloaded != observed.size
                or sink.written != observed.size
                or buffer.tell() != observed.size
            ):
                raise ArchiveVerificationError(
                    f"{artifact} download is incomplete"
                )
            resealed = _validated_object_metadata(
                stat_object(path), minimum_size=1, maximum_size=maximum_size,
            )
            if resealed != observed:
                raise ArchiveVerificationError(
                    f"{artifact} identity changed during download"
                )
            return buffer.getvalue()
        except ArchiveVerificationError:
            raise
        except AuditReadLimitError as exc:
            raise ArchiveVerificationError(
                f"{artifact} exceeded its reader safety limit"
            ) from None
        except AuditReadError as exc:
            raise ArchiveVerificationError(
                f"{artifact} identity seal is invalid"
            ) from None

    # -- decoding and query --------------------------------------------

    @staticmethod
    def _decode_entry(raw: Any) -> OutboxEntry:
        if not isinstance(raw, (tuple, list)) or len(raw) != 2:
            raise OutboxRecordError("Redis stream entry must be a (stream_id, fields) pair")
        raw_id, raw_fields = raw
        stream_id = _text(raw_id, field="stream_id")
        _stream_id_tuple(stream_id)
        if not isinstance(raw_fields, Mapping):
            raise OutboxRecordError("Redis stream entry fields must be a mapping")
        allowed_fields = {
            "event_json", *PrivilegedAuditOutbox._INDEX_FIELDS,
            *PrivilegedAuditOutbox._COMMIT_FIELDS,
        }
        if len(raw_fields) > len(allowed_fields):
            raise OutboxRecordError(
                f"stream {stream_id} contains too many fields"
            )
        fields: Dict[str, Any] = {}
        for raw_name, raw_value in raw_fields.items():
            if isinstance(raw_name, bytes) and len(raw_name) > 64:
                raise OutboxRecordError(
                    f"stream {stream_id} contains an oversized field name"
                )
            name = _text(raw_name, field="field name")
            if name not in allowed_fields:
                raise OutboxRecordError(
                    "Redis stream entry contains an unexpected field"
                )
            if name in fields:
                raise OutboxRecordError(
                    f"stream {stream_id} contains duplicate field {name!r}"
                )
            if isinstance(raw_value, bytes):
                value_size = len(raw_value)
            elif isinstance(raw_value, str):
                value_size = len(raw_value.encode("utf-8"))
            else:
                raise OutboxRecordError(
                    f"stream {stream_id} field {name} must be bytes or str"
                )
            maximum = 64 * 1024 if name == "event_json" else 4096
            if value_size > maximum:
                label = "64-KiB" if name == "event_json" else "4-KiB"
                raise OutboxRecordError(
                    f"stream {stream_id} field {name} exceeds the {label} limit"
                )
            fields[name] = raw_value
        if "event_json" not in fields:
            raise OutboxRecordError(f"stream {stream_id} is missing event_json")
        event_json = _text(fields["event_json"], field="event_json")
        if len(event_json.encode("utf-8")) > 64 * 1024:
            raise OutboxRecordError(
                f"stream {stream_id} event_json exceeds the 64-KiB limit"
            )
        try:
            event = json.loads(event_json)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "Redis stream entry has invalid event_json"
            ) from None
        if not isinstance(event, dict):
            raise OutboxRecordError(f"stream {stream_id} event_json must decode to an object")
        validated = dict(_load_shared_record(event, event_json))
        # Redis Lua deliberately preserves event_json exactly and publishes
        # counters separately as decimal strings.  This avoids Redis cjson's
        # empty-array coercion and >2^53 number rounding.  Merge and revalidate
        # the complete committed record before exposing or archiving it.
        for field in PrivilegedAuditOutbox._COMMIT_FIELDS:
            if field not in fields:
                raise OutboxRecordError(
                    f"stream {stream_id} is missing commit field {field}"
                )
            decimal = _text(fields[field], field=field)
            if (
                not decimal.isascii()
                or not decimal.isdigit()
                or len(decimal) > 19
                or (len(decimal) > 1 and decimal.startswith("0"))
            ):
                raise OutboxRecordError(
                    f"stream {stream_id} {field} is not a canonical bounded decimal"
                )
            requires_positive = field == "ledger_sequence" or (
                field == "namespace_version" and event.get("outcome") == "success"
            )
            if requires_positive and int(decimal) < 1:
                raise OutboxRecordError(
                    f"stream {stream_id} {field} must be positive"
                )
            validated[field] = decimal
        try:
            module = importlib.import_module("supertable.audit.privileged")
            record_type = getattr(module, "PrivilegedAuditRecord")
            committed_record = record_type.from_dict(validated)
            validated = committed_record.to_dict()
            committed_event_json = committed_record.to_json()
        except Exception as exc:
            raise OutboxRecordError(
                "Redis stream entry commit envelope is invalid; "
                f"error_type={_safe_error_type(exc)}"
            ) from None
        for field in PrivilegedAuditOutbox._INDEX_FIELDS:
            if field in fields:
                duplicate = _text(fields[field], field=field)
                if duplicate != str(validated.get(field, "")):
                    raise OutboxRecordError(
                        f"stream {stream_id} duplicated {field} does not match event_json"
                    )
        return OutboxEntry(
            stream_id,
            event_json,
            validated,
            committed_event_json,
        )

    def query(
        self,
        *,
        start_id: str = "-",
        end_id: str = "+",
        count: int = 500,
        newest_first: bool = True,
    ) -> List[OutboxEntry]:
        """Read an exact stream range; Redis failures raise, while ``[]`` means empty."""
        if count <= 0 or count > self._MAX_ARCHIVE_BATCH_ENTRIES:
            raise ValueError(
                f"count must be between 1 and {self._MAX_ARCHIVE_BATCH_ENTRIES}"
            )
        if newest_first:
            raw = self._redis_call(
                "query", "xrevrange", self.stream_key, max=end_id, min=start_id, count=count
            )
        else:
            raw = self._redis_call(
                "query", "xrange", self.stream_key, min=start_id, max=end_id, count=count
            )
        if raw is None:
            raise OutboxRecordError("Redis range command returned None instead of an entry list")
        if len(raw) > count:
            raise OutboxRecordError("Redis range command exceeded the requested bound")
        return [self._decode_entry(item) for item in raw]

    def get(self, stream_id: str) -> Optional[OutboxEntry]:
        """Return one exact entry, or ``None`` only when Redis confirms it is absent."""
        _stream_id_tuple(stream_id)
        entries = self.query(start_id=stream_id, end_id=stream_id, count=1, newest_first=False)
        return entries[0] if entries else None

    @staticmethod
    def _cascade_key(organization: str, manifest_id: str) -> str:
        from supertable import redis_keys as RK

        try:
            return RK.audit_privileged_cascade(organization, manifest_id)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "cascade manifest identity is not a safe Redis key segment"
            ) from None

    @staticmethod
    def _parse_cascade_manifest(
        parent: Mapping[str, Any],
        raw_manifest: Any,
    ) -> CascadeManifest:
        if not isinstance(raw_manifest, Mapping) or not raw_manifest:
            raise OutboxRecordError("role-delete cascade manifest is missing")
        if len(raw_manifest) > (
            len(_CASCADE_META_FIELDS)
            + PrivilegedAuditOutbox._MAX_CASCADE_MANIFEST_USERS
        ):
            raise OutboxRecordError("role-delete cascade manifest exceeds its user bound")
        decoded: Dict[str, str] = {}
        for raw_key, raw_value in raw_manifest.items():
            if isinstance(raw_key, bytes) and len(raw_key) > 128:
                raise OutboxRecordError("cascade manifest field name is too large")
            if isinstance(raw_value, bytes):
                raw_value_size = len(raw_value)
            elif isinstance(raw_value, str):
                raw_value_size = len(raw_value.encode("utf-8"))
            else:
                raise OutboxRecordError(
                    "cascade manifest field value must be bytes or text"
                )
            if raw_value_size > 4096:
                raise OutboxRecordError("cascade manifest field value is too large")
            key = _text(raw_key, field="cascade manifest field")
            if key in decoded:
                raise OutboxRecordError("cascade manifest contains a duplicate field")
            decoded[key] = _text(raw_value, field="cascade manifest value")

        missing = _CASCADE_META_FIELDS - set(decoded)
        unknown = {
            field
            for field in decoded
            if field not in _CASCADE_META_FIELDS and not field.startswith("user:")
        }
        if missing or unknown:
            raise OutboxRecordError("cascade manifest field set is invalid")

        schema_version = _bounded_decimal(
            decoded["schema_version"], field="cascade schema_version", minimum=1,
        )
        if schema_version != 1:
            raise OutboxRecordError("unsupported cascade manifest schema version")
        user_count = _bounded_decimal(
            decoded["user_count"], field="cascade user_count",
        )
        if user_count > PrivilegedAuditOutbox._MAX_CASCADE_MANIFEST_USERS:
            raise OutboxRecordError("cascade manifest user count exceeds its bound")
        removed_count = _bounded_decimal(
            decoded["removed_assignment_count"],
            field="cascade removed_assignment_count",
        )
        user_namespace_before = _bounded_decimal(
            decoded["user_namespace_version_before"],
            field="cascade user_namespace_version_before",
        )
        user_namespace_after = _bounded_decimal(
            decoded["user_namespace_version_after"],
            field="cascade user_namespace_version_after",
        )
        created_ms = _bounded_decimal(
            decoded["created_ms"], field="cascade created_ms", minimum=1,
        )

        exact_parent = {
            "event_id": parent.get("event_id"),
            "mutation_id": parent.get("mutation_id"),
            "organization": parent.get("organization"),
            "super_name": parent.get("super_name"),
            "role_id": parent.get("resource_id"),
        }
        for field, expected in exact_parent.items():
            if decoded[field] != expected:
                raise OutboxRecordError(
                    f"cascade manifest {field} does not match its parent event"
                )
        if (
            parent.get("action") != "role_delete"
            or parent.get("outcome") != "success"
            or parent.get("resource_type") != "role"
            or parent.get("cascade_manifest_id") != parent.get("event_id")
        ):
            raise OutboxRecordError(
                "cascade manifest parent is not a successful role deletion"
            )
        try:
            expected_user_count = int(parent.get("affected_count", -1))
            expected_removed_count = int(
                parent.get("cascade_assignment_count", -1)
            )
            expected_user_namespace_before = int(
                parent.get("user_namespace_version_before", -1)
            )
            expected_user_namespace_after = int(
                parent.get("user_namespace_version_after", -1)
            )
            expected_created_ms = int(parent.get("timestamp_ms", -1))
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "cascade parent contains invalid numeric fields"
            ) from None
        if (
            user_count != expected_user_count
            or removed_count != expected_removed_count
            or user_namespace_before != expected_user_namespace_before
            or user_namespace_after != expected_user_namespace_after
            or created_ms != expected_created_ms
        ):
            raise OutboxRecordError(
                "cascade manifest counters do not match its parent event"
            )

        rows: List[CascadeEvidenceRow] = []
        for field, packed in decoded.items():
            if not field.startswith("user:"):
                continue
            user_id = field[5:]
            if not _SAFE_CASCADE_USER_ID.fullmatch(user_id):
                raise OutboxRecordError("cascade manifest has an unsafe user ID")
            if len(packed) > 128:
                raise OutboxRecordError("cascade evidence row exceeds 128 bytes")
            parts = packed.split("|")
            if len(parts) != 5:
                raise OutboxRecordError("cascade evidence row is malformed")
            before_doc, after_doc, occurrences, before_roles, after_roles = (
                _bounded_decimal(
                    part,
                    field="cascade row counter",
                    minimum=minimum,
                )
                for part, name, minimum in zip(
                    parts,
                    (
                        "before_doc_version",
                        "after_doc_version",
                        "removed_occurrences",
                        "before_role_count",
                        "after_role_count",
                    ),
                    (0, 1, 1, 1, 0),
                )
            )
            if after_doc != before_doc + 1:
                raise OutboxRecordError("cascade evidence row has a version gap")
            if occurrences > before_roles or after_roles != before_roles - occurrences:
                raise OutboxRecordError(
                    "cascade evidence row has inconsistent role counts"
                )
            rows.append(CascadeEvidenceRow(
                user_id=user_id,
                before_doc_version=before_doc,
                after_doc_version=after_doc,
                removed_occurrences=occurrences,
                before_role_count=before_roles,
                after_role_count=after_roles,
            ))
        rows.sort(key=lambda row: row.user_id)
        if len(rows) != user_count:
            raise OutboxRecordError(
                "cascade manifest user-row count does not match its header"
            )
        if sum(row.removed_occurrences for row in rows) != removed_count:
            raise OutboxRecordError(
                "cascade manifest occurrence sum does not match its header"
            )
        if user_count == 0:
            if removed_count != 0 or user_namespace_before != user_namespace_after:
                raise OutboxRecordError("empty cascade manifest has non-empty effects")
        elif user_namespace_after != user_namespace_before + 1:
            raise OutboxRecordError(
                "non-empty cascade manifest did not advance the user namespace"
            )
        return CascadeManifest(
            schema_version=schema_version,
            event_id=decoded["event_id"],
            mutation_id=decoded["mutation_id"],
            organization=decoded["organization"],
            super_name=decoded["super_name"],
            role_id=decoded["role_id"],
            user_count=user_count,
            removed_assignment_count=removed_count,
            user_namespace_version_before=user_namespace_before,
            user_namespace_version_after=user_namespace_after,
            created_ms=created_ms,
            rows=tuple(rows),
        )

    def get_cascade_manifest(
        self,
        entry_or_event: OutboxEntry | Mapping[str, Any],
    ) -> Optional[CascadeManifest]:
        """Load and validate exact live cascade evidence for one event."""

        event = (
            entry_or_event.event
            if isinstance(entry_or_event, OutboxEntry)
            else entry_or_event
        )
        if not isinstance(event, Mapping):
            raise TypeError("entry_or_event must be an OutboxEntry or mapping")
        manifest_id = event.get("cascade_manifest_id")
        if not manifest_id:
            return None
        if not isinstance(manifest_id, str):
            raise OutboxRecordError("cascade_manifest_id must be text")
        organization = event.get("organization")
        if not isinstance(organization, str):
            raise OutboxRecordError("cascade parent organization must be text")
        key = self._cascade_key(organization, manifest_id)
        raw = self._redis_call(
            "read role-delete cascade manifest", "hgetall", key,
        )
        return self._parse_cascade_manifest(event, raw)

    def _load_cascade_manifests(
        self,
        entries: Sequence[OutboxEntry],
    ) -> Tuple[CascadeManifest, ...]:
        manifests: List[CascadeManifest] = []
        seen: set[str] = set()
        archive_row_count = 0
        for entry in entries:
            manifest = self.get_cascade_manifest(entry)
            if manifest is None:
                continue
            if manifest.event_id in seen:
                raise OutboxRecordError(
                    "archive batch references a duplicate cascade manifest"
                )
            seen.add(manifest.event_id)
            archive_row_count += 1 + len(manifest.rows)
            if archive_row_count > self._MAX_CASCADE_ARCHIVE_ROWS:
                raise ValueError(
                    "role-delete cascade evidence exceeds the "
                    f"{self._MAX_CASCADE_ARCHIVE_ROWS}-row archive memory bound; "
                    "reduce the archive batch count"
                )
            manifests.append(manifest)
        return tuple(manifests)

    @staticmethod
    def _cascade_manifest_fields(
        manifest: CascadeManifest,
    ) -> Dict[str, str]:
        """Return the exact canonical Redis hash represented by a manifest."""

        fields = {
            "schema_version": str(manifest.schema_version),
            "event_id": manifest.event_id,
            "mutation_id": manifest.mutation_id,
            "organization": manifest.organization,
            "super_name": manifest.super_name,
            "role_id": manifest.role_id,
            "user_count": str(manifest.user_count),
            "removed_assignment_count": str(
                manifest.removed_assignment_count
            ),
            "user_namespace_version_before": str(
                manifest.user_namespace_version_before
            ),
            "user_namespace_version_after": str(
                manifest.user_namespace_version_after
            ),
            "created_ms": str(manifest.created_ms),
        }
        for row in manifest.rows:
            fields[f"user:{row.user_id}"] = "|".join(str(value) for value in (
                row.before_doc_version,
                row.after_doc_version,
                row.removed_occurrences,
                row.before_role_count,
                row.after_role_count,
            ))
        return fields

    # -- consumer groups ----------------------------------------------

    def create_group(self, group: str, *, start_id: str = "0-0") -> bool:
        """Create a group and stream; return ``False`` only for BUSYGROUP."""
        if not group:
            raise ValueError("group is required")
        try:
            self._redis.xgroup_create(self.stream_key, group, id=start_id, mkstream=True)
            return True
        except Exception as exc:
            if _exception_has_token(exc, "BUSYGROUP"):
                return False
            raise OutboxBackendError("create consumer group", exc) from None

    def read_group(
        self,
        group: str,
        consumer: str,
        *,
        count: int = 100,
        block_ms: Optional[int] = None,
        read_id: str = ">",
    ) -> List[OutboxEntry]:
        """Read group entries. ``[]`` means a successful read with no messages."""
        if not group or not consumer:
            raise ValueError("group and consumer are required")
        if (
            count <= 0
            or count > self._MAX_ARCHIVE_BATCH_ENTRIES
            or (block_ms is not None and block_ms < 0)
        ):
            raise ValueError(
                f"count must be between 1 and {self._MAX_ARCHIVE_BATCH_ENTRIES} "
                "and block_ms must be non-negative"
            )
        kwargs: Dict[str, Any] = {"count": count}
        if block_ms is not None:
            kwargs["block"] = block_ms
        raw = self._redis_call(
            "read consumer group",
            "xreadgroup",
            group,
            consumer,
            {self.stream_key: read_id},
            **kwargs,
        )
        if raw is None:
            return []  # redis-py uses None for a successful blocking timeout
        entries: List[OutboxEntry] = []
        for stream_result in raw:
            if not isinstance(stream_result, (tuple, list)) or len(stream_result) != 2:
                raise OutboxRecordError("XREADGROUP returned a malformed stream result")
            returned_key = _text(stream_result[0], field="stream key")
            if returned_key != self.stream_key:
                raise OutboxRecordError("XREADGROUP returned an unexpected stream")
            entries.extend(self._decode_entry(item) for item in stream_result[1])
            if len(entries) > count:
                raise OutboxRecordError(
                    "XREADGROUP returned more entries than requested"
                )
        return entries

    def autoclaim(
        self,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
    ) -> AutoClaimResult:
        """Claim abandoned pending messages using Redis ``XAUTOCLAIM``."""
        if not group or not consumer:
            raise ValueError("group and consumer are required")
        if (
            min_idle_ms < 0
            or count <= 0
            or count > self._MAX_ARCHIVE_BATCH_ENTRIES
        ):
            raise ValueError(
                "min_idle_ms must be non-negative and count must be between "
                f"1 and {self._MAX_ARCHIVE_BATCH_ENTRIES}"
            )
        raw = self._redis_call(
            "autoclaim",
            "xautoclaim",
            self.stream_key,
            group,
            consumer,
            min_idle_ms,
            start_id=start_id,
            count=count,
        )
        if not isinstance(raw, (tuple, list)) or len(raw) not in (2, 3):
            raise OutboxRecordError("XAUTOCLAIM returned an unsupported response")
        next_id = _text(raw[0], field="next start id")
        if len(raw[1]) > count:
            raise OutboxRecordError("XAUTOCLAIM returned more entries than requested")
        entries = tuple(self._decode_entry(item) for item in raw[1])
        deleted_ids = tuple(_text(item, field="deleted id") for item in raw[2]) if len(raw) == 3 else ()
        return AutoClaimResult(next_id, entries, deleted_ids)

    def ack(self, group: str, stream_ids: Iterable[str]) -> int:
        """Acknowledge explicit stream IDs, raising on backend failure."""
        ids = tuple(stream_ids)
        if not group:
            raise ValueError("group is required")
        if not ids:
            return 0
        if len(ids) > self._MAX_ARCHIVE_BATCH_ENTRIES:
            raise ValueError(
                f"cannot acknowledge more than {self._MAX_ARCHIVE_BATCH_ENTRIES} entries"
            )
        for stream_id in ids:
            _stream_id_tuple(stream_id)
        result = self._redis_call("acknowledge", "xack", self.stream_key, group, *ids)
        try:
            return int(result)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("XACK returned a non-integer result") from None

    # -- durable archive ledger ---------------------------------------

    @staticmethod
    def batch_id(org: str, entries: Sequence[OutboxEntry]) -> str:
        """Derive a stable batch identity from organization, IDs, and exact bytes."""
        digest = hashlib.sha256()
        digest.update(org.encode("utf-8"))
        digest.update(b"\0")
        for entry in entries:
            digest.update(entry.stream_id.encode("ascii"))
            digest.update(b"\0")
            digest.update(entry.event_json.encode("utf-8"))
            digest.update(b"\0")
            digest.update(entry.committed_event_json.encode("utf-8"))
            digest.update(b"\0")
        return digest.hexdigest()

    @staticmethod
    def _revalidate_archive_entry(entry: OutboxEntry) -> OutboxEntry:
        """Rebuild an entry from its exact evidence before delivery.

        The dataclass is frozen, but a caller could still mutate its mapping or
        manually construct an instance.  Archival therefore trusts neither:
        both JSON documents are parsed through the shared validator and the
        mutable mapping must match the canonical committed document exactly.
        """
        if not isinstance(entry, OutboxEntry):
            raise OutboxRecordError("archive entries must be OutboxEntry instances")
        _stream_id_tuple(entry.stream_id)
        for field, value in (
            ("event_json", entry.event_json),
            ("committed_event_json", entry.committed_event_json),
        ):
            if not isinstance(value, str):
                raise OutboxRecordError(f"archive entry {field} must be text")
            if len(value.encode("utf-8")) > 64 * 1024:
                raise OutboxRecordError(
                    f"archive entry {field} exceeds the 64-KiB limit"
                )
        try:
            template_payload = json.loads(entry.event_json)
            committed_payload = json.loads(entry.committed_event_json)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("archive entry contains invalid JSON") from None
        if not isinstance(template_payload, dict) or not isinstance(committed_payload, dict):
            raise OutboxRecordError("archive entry JSON must contain objects")

        template = dict(_load_shared_record(template_payload, entry.event_json))
        committed = dict(
            _load_shared_record(committed_payload, entry.committed_event_json)
        )
        try:
            module = importlib.import_module("supertable.audit.privileged")
            record_type = getattr(module, "PrivilegedAuditRecord")
            committed_record = record_type.from_dict(committed)
            committed = committed_record.to_dict()
            canonical_committed_json = committed_record.to_json()
        except Exception as exc:
            raise OutboxRecordError(
                "archive entry committed record is invalid; "
                f"error_type={_safe_error_type(exc)}"
            ) from None
        if canonical_committed_json != entry.committed_event_json:
            raise OutboxRecordError("archive entry committed JSON is not canonical")

        for field, template_value in template.items():
            if field in PrivilegedAuditOutbox._COMMIT_FIELDS:
                continue
            if committed.get(field) != template_value:
                raise OutboxRecordError(
                    f"archive entry committed {field} differs from its template"
                )
        try:
            if any(
                int(template.get(field, 0)) != 0
                for field in PrivilegedAuditOutbox._COMMIT_FIELDS
            ):
                raise OutboxRecordError(
                    "archive entry template contains assigned commit fields"
                )
            if int(committed.get("ledger_sequence", 0)) < 1:
                raise OutboxRecordError(
                    "archive entry ledger_sequence must be positive"
                )
            if (
                committed.get("outcome") == "success"
                and int(committed.get("namespace_version", 0)) < 1
            ):
                raise OutboxRecordError(
                    "successful archive entry namespace_version must be positive"
                )
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "archive entry commit fields are invalid"
            ) from None
        if dict(entry.event) != committed:
            raise OutboxRecordError(
                "archive entry mapping differs from its exact committed JSON"
            )
        return OutboxEntry(
            stream_id=entry.stream_id,
            event_json=entry.event_json,
            event=committed,
            committed_event_json=canonical_committed_json,
        )

    @staticmethod
    def _batch_field(batch_id: str) -> str:
        return f"batch:{batch_id}"

    @staticmethod
    def _entry_field(stream_id: str) -> str:
        return f"entry:{stream_id}"

    @staticmethod
    def _sequence_claim_field(first_sequence: int) -> str:
        return f"sequence-claim:{first_sequence}"

    def _ledger_get(self, field: str) -> Optional[str]:
        value = self._redis_call("read delivery ledger", "hget", self.delivery_ledger_key, field)
        if value is None:
            return None
        return _text(value, field="delivery ledger value")

    @staticmethod
    def _decode_ledger(value: str) -> Dict[str, Any]:
        if len(value.encode("utf-8")) > PrivilegedAuditOutbox._MAX_LEDGER_RECORD_BYTES:
            raise OutboxRecordError("delivery ledger record exceeds the 1-MiB limit")
        try:
            record = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("delivery ledger contains invalid JSON") from None
        if not isinstance(record, dict) or record.get("version") != PrivilegedAuditOutbox._LEDGER_VERSION:
            raise OutboxRecordError("delivery ledger contains an unsupported record")
        return record

    def _ledger_json(self, record: Mapping[str, Any]) -> str:
        value = json.dumps(
            record, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        )
        if len(value.encode("utf-8")) > self._MAX_LEDGER_RECORD_BYTES:
            raise OutboxRecordError("delivery ledger record exceeds the 1-MiB limit")
        return value

    @staticmethod
    def _same_entry(left: OutboxEntry, right: OutboxEntry) -> bool:
        return (
            left.stream_id == right.stream_id
            and left.event_json == right.event_json
            and left.committed_event_json == right.committed_event_json
            and dict(left.event) == dict(right.event)
        )

    def _source_entry(self, expected: OutboxEntry) -> OutboxEntry:
        raw = self._redis_call(
            "verify privileged source stream entry",
            "xrange",
            self.stream_key,
            min=expected.stream_id,
            max=expected.stream_id,
            count=2,
        )
        if raw is None or len(raw) != 1:
            raise OutboxRecordError(
                "privileged source stream has no exact entry"
            )
        actual = self._decode_entry(raw[0])
        if not self._same_entry(actual, expected):
            raise OutboxRecordError(
                "privileged source stream entry differs from the archive input"
            )
        return actual

    def _source_entries(self, stream_ids: Sequence[str]) -> Tuple[OutboxEntry, ...]:
        entries: List[OutboxEntry] = []
        for stream_id in stream_ids:
            raw = self._redis_call(
                "read claimed privileged source stream entry",
                "xrange",
                self.stream_key,
                min=stream_id,
                max=stream_id,
                count=2,
            )
            if raw is None or len(raw) != 1:
                raise OutboxRecordError(
                    f"claimed privileged source stream entry {stream_id} is missing"
                )
            entries.append(self._decode_entry(raw[0]))
        return tuple(entries)

    def _validated_archive_head(
        self,
        raw_head: Optional[str],
        *,
        organization: str,
    ) -> Tuple[int, Optional[str]]:
        if raw_head is None:
            return 0, None
        head = self._decode_ledger(raw_head)
        if head.get("kind") != "archive_head":
            raise OutboxRecordError("delivery ledger archive head is malformed")
        if head.get("organization") != organization:
            raise OutboxRecordError(
                "delivery ledger archive head has the wrong organization"
            )
        try:
            sequence = int(head["last_sequence"])
            first_sequence = int(head["first_sequence"])
        except (KeyError, TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "delivery ledger archive head has an invalid sequence"
            ) from None
        if first_sequence < 1 or sequence < first_sequence:
            raise OutboxRecordError(
                "delivery ledger archive head sequence is invalid"
            )
        first_stream_id = str(head.get("first_stream_id", ""))
        stream_id = str(head.get("last_stream_id", ""))
        if _stream_id_tuple(first_stream_id) > _stream_id_tuple(stream_id):
            raise OutboxRecordError(
                "delivery ledger archive head stream range is invalid"
            )
        batch_id = head.get("batch_id")
        checkpoint_hash = head.get("checkpoint_sha256")
        checkpoint_path = head.get("checkpoint_path")
        if (
            not _is_sha256(batch_id)
            or not _is_sha256(checkpoint_hash)
            or checkpoint_path != self._checkpoint_path(organization, batch_id)
        ):
            raise OutboxRecordError(
                "delivery ledger archive head checkpoint identity is invalid"
            )
        return sequence, stream_id

    def _delivery_ledger_field_count(self) -> int:
        raw_count = self._redis_call(
            "count delivery ledger fields",
            "hlen",
            self.delivery_ledger_key,
        )
        if isinstance(raw_count, bool):
            raise OutboxRecordError("delivery ledger HLEN returned a boolean")
        try:
            count = int(raw_count)
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "delivery ledger HLEN returned a non-integer"
            ) from None
        if count < 0:
            raise OutboxRecordError("delivery ledger HLEN returned a negative count")
        return count

    def _validate_recoverable_headless_claim(self, organization: str) -> None:
        """Validate the sole legitimate no-head state after a first-batch crash.

        A sequence-one batch is claimed before its artifacts are published and
        the archive head is finalized.  That bounded two-field state must be
        recoverable, but every delivered marker, additional claim, orphan, or
        malformed artifact claim is evidence that a previously durable head
        disappeared and must fail closed.
        """

        if self._delivery_ledger_field_count() != 2:
            raise OutboxRecordError(
                "archive head is absent while the delivery ledger contains residual evidence"
            )
        batch_id = self._ledger_get(self._sequence_claim_field(1))
        if not _is_sha256(batch_id):
            raise OutboxRecordError(
                "headless delivery ledger has no valid sequence-one claim"
            )
        raw_batch = self._ledger_get(self._batch_field(batch_id))
        if raw_batch is None:
            raise OutboxRecordError(
                "headless delivery ledger sequence claim is orphaned"
            )
        record = self._decode_ledger(raw_batch)
        stream_ids_value = record.get("stream_ids")
        if (
            not isinstance(stream_ids_value, list)
            or not stream_ids_value
            or len(stream_ids_value) > self._MAX_ARCHIVE_BATCH_ENTRIES
            or any(not isinstance(stream_id, str) for stream_id in stream_ids_value)
        ):
            raise OutboxRecordError(
                "headless archive claim has invalid stream membership"
            )
        stream_ids = tuple(stream_ids_value)
        stream_order = tuple(_stream_id_tuple(stream_id) for stream_id in stream_ids)
        if (
            len(set(stream_ids)) != len(stream_ids)
            or stream_order != tuple(sorted(stream_order))
        ):
            raise OutboxRecordError(
                "headless archive claim stream membership is not ordered and unique"
            )

        entries = self._source_entries(stream_ids)
        sequences = tuple(int(entry.event["ledger_sequence"]) for entry in entries)
        if (
            sequences != tuple(range(1, len(entries) + 1))
            or any(entry.event.get("organization") != organization for entry in entries)
            or self.batch_id(organization, entries) != batch_id
        ):
            raise OutboxRecordError(
                "headless archive claim differs from its source evidence"
            )
        self._validate_batch_record(
            record,
            organization=organization,
            batch_id=batch_id,
            stream_ids=stream_ids,
            sequences=sequences,
        )
        status = record.get("status")
        if status not in {"writing", "written_verified"}:
            raise OutboxRecordError(
                "headless archive claim is not an incomplete recoverable batch"
            )
        expected_fields = {
            "version",
            "kind",
            "status",
            "batch_id",
            "organization",
            "stream_ids",
            "first_sequence",
            "last_sequence",
            "first_stream_id",
            "last_stream_id",
            "artifacts",
            "claimed_ms",
            "predecessor_head",
        }
        if status == "written_verified":
            expected_fields.update({"archive", "written_ms"})
        if set(record) != expected_fields or record.get("predecessor_head") is not None:
            raise OutboxRecordError(
                "headless archive claim contains unexpected delivery state"
            )
        timestamp_fields = (
            ("claimed_ms", "written_ms")
            if status == "written_verified"
            else ("claimed_ms",)
        )
        for timestamp_field in timestamp_fields:
            timestamp = record.get(timestamp_field)
            if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
                raise OutboxRecordError(
                    "headless archive claim timestamp is invalid"
                )

        artifacts = record["artifacts"]
        parent_value = artifacts.get("parent")
        parent = self._validated_artifact_spec(
            parent_value,
            expected_path=self._archive_path(organization, batch_id),
            expected_schema="privileged-audit-v1",
        )
        if (
            not isinstance(parent_value, Mapping)
            or set(parent_value) != {
                "path", "file_hash", "bytes_written", "schema", "codec",
                "event_count",
            }
            or parent_value.get("event_count") != len(entries)
        ):
            raise OutboxRecordError(
                "headless archive parent artifact claim is inconsistent"
            )
        cascade_value = artifacts.get("cascade")
        if cascade_value is not None:
            cascade = self._validated_artifact_spec(
                cascade_value,
                expected_path=self._cascade_archive_path(organization, batch_id),
                expected_schema="privileged-role-delete-cascade-v1",
            )
            if (
                not isinstance(cascade_value, Mapping)
                or set(cascade_value) != {
                    "path", "file_hash", "bytes_written", "schema", "codec",
                    "manifest_count", "row_count",
                }
                or isinstance(cascade_value.get("manifest_count"), bool)
                or not isinstance(cascade_value.get("manifest_count"), int)
                or not 1 <= cascade_value["manifest_count"] <= len(entries)
                or isinstance(cascade_value.get("row_count"), bool)
                or not isinstance(cascade_value.get("row_count"), int)
                or not cascade_value["manifest_count"] <= cascade_value["row_count"]
            ):
                raise OutboxRecordError(
                    "headless archive cascade artifact claim is inconsistent"
                )
        else:
            cascade = None
        if set(artifacts) != {"parent", "cascade", "checkpoint"}:
            raise OutboxRecordError(
                "headless archive artifact set is invalid"
            )
        checkpoint_value = artifacts.get("checkpoint")
        checkpoint = self._validated_checkpoint_spec(
            checkpoint_value,
            batch_id=batch_id,
            organization=organization,
        )
        manifest = checkpoint["manifest"]
        if (
            set(manifest) != {
                "version", "kind", "organization", "batch_id",
                "first_sequence", "last_sequence", "first_stream_id",
                "last_stream_id", "event_count", "parent", "cascade",
                "previous", "manifest_sha256",
            }
            or manifest.get("first_sequence") != 1
            or manifest.get("last_sequence") != len(entries)
            or manifest.get("first_stream_id") != stream_ids[0]
            or manifest.get("last_stream_id") != stream_ids[-1]
            or manifest.get("event_count") != len(entries)
            or manifest.get("parent") != dict(parent_value)
            or manifest.get("cascade") != (
                dict(cascade_value) if isinstance(cascade_value, Mapping) else None
            )
            or manifest.get("previous") is not None
        ):
            raise OutboxRecordError(
                "headless archive checkpoint does not match its batch claim"
            )

        if status == "written_verified":
            expected_archive: Dict[str, Any] = {
                "path": parent["path"],
                "file_hash": parent["file_hash"],
                "event_count": len(entries),
                "bytes_written": parent["bytes_written"],
                "schema": "privileged-audit-v1",
                "cascade": None,
                "checkpoint": {
                    "path": checkpoint["path"],
                    "file_hash": checkpoint["file_hash"],
                    "bytes_written": checkpoint["bytes_written"],
                    "schema": checkpoint["schema"],
                    "manifest_sha256": checkpoint["manifest_sha256"],
                },
            }
            if cascade is not None:
                expected_archive["cascade"] = {
                    "path": cascade["path"],
                    "file_hash": cascade["file_hash"],
                    "manifest_count": cascade_value["manifest_count"],
                    "row_count": cascade_value["row_count"],
                    "bytes_written": cascade["bytes_written"],
                    "schema": "privileged-role-delete-cascade-v1",
                }
            if record.get("archive") != expected_archive:
                raise OutboxRecordError(
                    "headless written archive metadata differs from its claims"
                )

    def _archive_head_for_verification(
        self,
        organization: str,
    ) -> Optional[str]:
        """Read a head, distinguish true genesis, and reject deleted-head residue."""

        raw_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        if raw_head is not None:
            return raw_head
        if self._delivery_ledger_field_count() == 0:
            # HLEN is the linearization point confirming an actually empty
            # ledger. A concurrently published head is safe to verify now too.
            return self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        try:
            self._validate_recoverable_headless_claim(organization)
        except (OutboxRecordError, OutboxBackendError):
            raced_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
            if raced_head is not None:
                return raced_head
            raise
        raced_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        if raced_head is not None:
            return raced_head
        raise DeliveryPendingError(
            "the first archive batch is durably claimed but its head is not finalized"
        )

    def _claim_archive_sequence(
        self,
        *,
        organization: str,
        batch_id: str,
        batch_field: str,
        batch_record: Mapping[str, Any],
        first_sequence: int,
        last_sequence: int,
        first_stream_id: str,
        last_stream_id: str,
    ) -> str:
        """Atomically claim the next sequence and its immutable batch record."""
        raw_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        head_sequence, head_stream_id = self._validated_archive_head(
            raw_head, organization=organization,
        )

        if first_sequence != head_sequence + 1:
            raise OutboxRecordError(
                "archive batch does not continue the durable privileged ledger: "
                f"expected sequence {head_sequence + 1}, got {first_sequence}"
            )
        if last_sequence < first_sequence:
            raise OutboxRecordError("archive batch sequence range is invalid")
        if _stream_id_tuple(first_stream_id) > _stream_id_tuple(last_stream_id):
            raise OutboxRecordError("archive batch stream range is invalid")
        if (
            head_stream_id is not None
            and _stream_id_tuple(first_stream_id) <= _stream_id_tuple(head_stream_id)
        ):
            raise OutboxRecordError(
                "archive batch stream IDs do not advance the durable archive head"
            )

        claimed_record = dict(batch_record)
        claimed_record["predecessor_head"] = raw_head
        claimed_record["artifacts"] = self._with_checkpoint_claim(
            organization,
            batch_id,
            claimed_record,
            raw_head,
        )
        batch_json = self._ledger_json(claimed_record)

        claim_field = self._sequence_claim_field(first_sequence)
        result = self._redis_call(
            "atomically claim archive batch",
            "eval",
            self._CLAIM_ARCHIVE_BATCH_LUA,
            1,
            self.delivery_ledger_key,
            self._ARCHIVE_HEAD_FIELD,
            "1" if raw_head is not None else "0",
            raw_head or "",
            claim_field,
            batch_id,
            batch_field,
            batch_json,
        )
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            raise OutboxRecordError("archive claim Lua returned an invalid result")
        try:
            code = int(result[0])
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "archive claim Lua returned a non-integer status"
            ) from None
        if code == -1:
            raise DeliveryPendingError(
                "privileged archive head advanced concurrently; retry the batch"
            )
        if code == -2:
            raise DeliveryPendingError(
                f"privileged ledger sequence {first_sequence} is already claimed "
                "by a different archive batch"
            )
        if code == -3:
            raise OutboxRecordError(
                "delivery ledger has an orphan sequence claim or batch record"
            )
        if code not in {1, 2} or len(result) < 3:
            raise OutboxRecordError(
                f"archive claim Lua returned unsupported status {code}"
            )
        return _text(result[2], field="archive batch ledger record")

    def _cas_batch_record(
        self,
        batch_field: str,
        expected_raw: str,
        replacement_raw: str,
    ) -> Tuple[bool, str]:
        result = self._redis_call(
            "compare-and-set archive batch status",
            "eval",
            self._CAS_ARCHIVE_BATCH_LUA,
            1,
            self.delivery_ledger_key,
            batch_field,
            expected_raw,
            replacement_raw,
        )
        if not isinstance(result, (tuple, list)) or len(result) != 2:
            raise OutboxRecordError("archive batch CAS returned an invalid result")
        try:
            code = int(result[0])
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("archive batch CAS status is invalid") from None
        current = _text(result[1], field="archive batch CAS record")
        if code in {1, 2}:
            return True, current
        if code == 0 and current:
            return False, current
        raise OutboxRecordError("archive batch disappeared during CAS")

    @staticmethod
    def _archive_rows(entries: Sequence[OutboxEntry]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for entry in entries:
            row: Dict[str, Any] = {
                "stream_id": entry.stream_id,
                "event_json": entry.committed_event_json,
                "template_event_json": entry.event_json,
            }
            for field in PrivilegedAuditOutbox._INDEX_FIELDS:
                value = entry.event.get(field)
                row[field] = (
                    int(value or 0)
                    if field in PrivilegedAuditOutbox._COMMIT_FIELDS
                    else str(value or "")
                )
            rows.append(row)
        return rows

    @staticmethod
    def _serialize_archive(rows: Sequence[Mapping[str, Any]]) -> bytes:
        if pa is None or pq is None:
            raise ArchiveVerificationError("pyarrow is required for privileged audit archival")
        schema = pa.schema([
            ("stream_id", pa.string()),
            ("event_json", pa.string()),
            ("template_event_json", pa.string()),
            ("event_id", pa.string()),
            ("mutation_id", pa.string()),
            ("action", pa.string()),
            ("resource_type", pa.string()),
            ("resource_id", pa.string()),
            ("organization", pa.string()),
            ("super_name", pa.string()),
            ("ledger_sequence", pa.int64()),
            ("namespace_version", pa.int64()),
            ("affected_count", pa.int64()),
            ("cascade_manifest_id", pa.string()),
            ("cascade_assignment_count", pa.int64()),
            ("user_namespace_version_before", pa.int64()),
            ("user_namespace_version_after", pa.int64()),
            ("payload_hash", pa.string()),
        ])
        table = pa.Table.from_pylist(list(rows), schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy", write_statistics=True)
        return buffer.getvalue()

    @staticmethod
    def _cascade_archive_rows(
        manifests: Sequence[CascadeManifest],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for manifest in manifests:
            common = {
                "schema_version": manifest.schema_version,
                "event_id": manifest.event_id,
                "mutation_id": manifest.mutation_id,
                "organization": manifest.organization,
                "super_name": manifest.super_name,
                "role_id": manifest.role_id,
                "user_count": manifest.user_count,
                "removed_assignment_count": manifest.removed_assignment_count,
                "user_namespace_version_before": (
                    manifest.user_namespace_version_before
                ),
                "user_namespace_version_after": (
                    manifest.user_namespace_version_after
                ),
                "created_ms": manifest.created_ms,
            }
            rows.append({
                **common,
                "row_kind": "manifest",
                "user_id": "",
                "before_doc_version": 0,
                "after_doc_version": 0,
                "removed_occurrences": 0,
                "before_role_count": 0,
                "after_role_count": 0,
            })
            for evidence in manifest.rows:
                rows.append({
                    **common,
                    "row_kind": "user",
                    "user_id": evidence.user_id,
                    "before_doc_version": evidence.before_doc_version,
                    "after_doc_version": evidence.after_doc_version,
                    "removed_occurrences": evidence.removed_occurrences,
                    "before_role_count": evidence.before_role_count,
                    "after_role_count": evidence.after_role_count,
                })
        return rows

    @staticmethod
    def _serialize_cascade_archive(rows: Sequence[Mapping[str, Any]]) -> bytes:
        if pa is None or pq is None:
            raise ArchiveVerificationError(
                "pyarrow is required for cascade evidence archival"
            )
        schema = pa.schema([
            ("row_kind", pa.string()),
            ("schema_version", pa.int64()),
            ("event_id", pa.string()),
            ("mutation_id", pa.string()),
            ("organization", pa.string()),
            ("super_name", pa.string()),
            ("role_id", pa.string()),
            ("user_count", pa.int64()),
            ("removed_assignment_count", pa.int64()),
            ("user_namespace_version_before", pa.int64()),
            ("user_namespace_version_after", pa.int64()),
            ("created_ms", pa.int64()),
            ("user_id", pa.string()),
            ("before_doc_version", pa.int64()),
            ("after_doc_version", pa.int64()),
            ("removed_occurrences", pa.int64()),
            ("before_role_count", pa.int64()),
            ("after_role_count", pa.int64()),
        ])
        table = pa.Table.from_pylist(list(rows), schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy", write_statistics=True)
        return buffer.getvalue()

    @staticmethod
    def _archive_path(org: str, batch_id: str) -> str:
        _safe_archive_organization(org)
        return f"{org}/__audit__/privileged/batch_{batch_id}.parquet"

    @staticmethod
    def _cascade_archive_path(org: str, batch_id: str) -> str:
        _safe_archive_organization(org)
        return f"{org}/__audit__/privileged/batch_{batch_id}_cascade.parquet"

    @staticmethod
    def _codec_claim(schema: str) -> Dict[str, str]:
        return {
            "format": "parquet",
            "schema": schema,
            "compression": "snappy",
            "writer": "pyarrow",
            "writer_version": str(getattr(pa, "__version__", "unknown")),
        }

    @staticmethod
    def _checkpoint_path(organization: str, batch_id: str) -> str:
        _safe_archive_organization(organization)
        return (
            f"{organization}/__audit__/privileged/manifests/"
            f"batch_{batch_id}.json"
        )

    def _with_checkpoint_claim(
        self,
        organization: str,
        batch_id: str,
        batch_record: Mapping[str, Any],
        raw_head: Optional[str],
    ) -> Dict[str, Any]:
        artifacts = batch_record.get("artifacts")
        if not isinstance(artifacts, Mapping):
            raise OutboxRecordError("archive batch has no artifact claims")
        claimed_artifacts = dict(artifacts)
        previous: Optional[Dict[str, Any]] = None
        if raw_head is not None:
            head = self._decode_ledger(raw_head)
            previous_hash = head.get("checkpoint_sha256")
            previous_path = head.get("checkpoint_path")
            previous_batch_id = head.get("batch_id")
            if (
                not _is_sha256(previous_hash)
                or not _is_sha256(previous_batch_id)
                or previous_path
                != self._checkpoint_path(organization, previous_batch_id)
            ):
                raise OutboxRecordError(
                    "archive head has no immutable checkpoint anchor"
                )
            previous = {
                "batch_id": previous_batch_id,
                "last_sequence": int(head["last_sequence"]),
                "last_stream_id": str(head["last_stream_id"]),
                "manifest_sha256": previous_hash,
                "path": previous_path,
            }
        parent = claimed_artifacts.get("parent")
        cascade = claimed_artifacts.get("cascade")
        if not isinstance(parent, Mapping):
            raise OutboxRecordError("archive parent artifact claim is missing")
        manifest_without_hash: Dict[str, Any] = {
            "version": 1,
            "kind": "privileged_archive_checkpoint",
            "organization": organization,
            "batch_id": batch_id,
            "first_sequence": int(batch_record["first_sequence"]),
            "last_sequence": int(batch_record["last_sequence"]),
            "first_stream_id": str(batch_record["first_stream_id"]),
            "last_stream_id": str(batch_record["last_stream_id"]),
            "event_count": len(tuple(batch_record["stream_ids"])),
            "parent": dict(parent),
            "cascade": dict(cascade) if isinstance(cascade, Mapping) else None,
            "previous": previous,
        }
        canonical = self._ledger_json(manifest_without_hash).encode("utf-8")
        manifest_sha256 = hashlib.sha256(canonical).hexdigest()
        manifest = dict(manifest_without_hash)
        manifest["manifest_sha256"] = manifest_sha256
        payload = self._ledger_json(manifest).encode("utf-8")
        claimed_artifacts["checkpoint"] = {
            "path": self._checkpoint_path(organization, batch_id),
            "file_hash": compute_file_hash(payload),
            "bytes_written": len(payload),
            "schema": "privileged-archive-checkpoint-v1",
            "codec": {
                "format": "canonical-json",
                "schema": "privileged-archive-checkpoint-v1",
                "version": "1",
            },
            "manifest_sha256": manifest_sha256,
            "manifest": manifest,
        }
        return claimed_artifacts

    def _new_artifact_claim(
        self,
        org: str,
        batch_id: str,
        entries: Sequence[OutboxEntry],
        manifests: Sequence[CascadeManifest],
    ) -> Dict[str, Any]:
        parent_bytes = self._serialize_archive(self._archive_rows(entries))
        self._validate_new_artifact_size(
            len(parent_bytes), artifact_name="privileged parent Parquet",
        )
        parent_schema = "privileged-audit-v1"
        claim: Dict[str, Any] = {
            "parent": {
                "path": self._archive_path(org, batch_id),
                "file_hash": compute_file_hash(parent_bytes),
                "bytes_written": len(parent_bytes),
                "schema": parent_schema,
                "codec": self._codec_claim(parent_schema),
                "event_count": len(entries),
            },
            "cascade": None,
        }
        if manifests:
            cascade_rows = self._cascade_archive_rows(manifests)
            cascade_bytes = self._serialize_cascade_archive(cascade_rows)
            self._validate_new_artifact_size(
                len(cascade_bytes), artifact_name="role-delete cascade Parquet",
            )
            cascade_schema = "privileged-role-delete-cascade-v1"
            claim["cascade"] = {
                "path": self._cascade_archive_path(org, batch_id),
                "file_hash": compute_file_hash(cascade_bytes),
                "bytes_written": len(cascade_bytes),
                "schema": cascade_schema,
                "codec": self._codec_claim(cascade_schema),
                "manifest_count": len(manifests),
                "row_count": len(cascade_rows),
            }
        return claim

    @classmethod
    def _validate_new_artifact_size(
        cls,
        byte_count: int,
        *,
        artifact_name: str,
    ) -> None:
        # This check deliberately happens before the immutable Redis sequence
        # claim. An operator can safely retry with a smaller drain count instead
        # of stranding a permanently unwriteable claimed partition.
        if byte_count <= 0 or byte_count > cls._MAX_PARQUET_ARTIFACT_BYTES:
            raise ValueError(
                f"{artifact_name} exceeds the "
                f"{cls._MAX_PARQUET_ARTIFACT_BYTES}-byte archive limit; "
                "reduce the archive batch count"
            )

    @staticmethod
    def _validated_artifact_spec(
        value: Any,
        *,
        expected_path: str,
        expected_schema: str,
    ) -> Dict[str, Any]:
        if not isinstance(value, Mapping):
            raise OutboxRecordError("archive batch has no immutable artifact claim")
        path = value.get("path")
        file_hash = value.get("file_hash")
        schema = value.get("schema")
        codec = value.get("codec")
        try:
            byte_count = int(value.get("bytes_written", -1))
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("archive artifact byte count is invalid") from None
        if path != expected_path or schema != expected_schema:
            raise OutboxRecordError("archive artifact path/schema claim is invalid")
        if (
            not _is_sha256(file_hash)
            or byte_count <= 0
            or byte_count > PrivilegedAuditOutbox._MAX_PARQUET_ARTIFACT_BYTES
        ):
            raise OutboxRecordError("archive artifact hash/size claim is invalid")
        if not isinstance(codec, Mapping) or dict(codec).get("schema") != schema:
            raise OutboxRecordError("archive artifact codec claim is invalid")
        codec = dict(codec)
        if expected_schema == "privileged-archive-checkpoint-v1":
            if codec != {
                "format": "canonical-json",
                "schema": expected_schema,
                "version": "1",
            }:
                raise OutboxRecordError(
                    "archive checkpoint codec claim is invalid"
                )
        elif (
            set(codec)
            != {"format", "schema", "compression", "writer", "writer_version"}
            or codec.get("format") != "parquet"
            or codec.get("compression") != "snappy"
            or codec.get("writer") != "pyarrow"
            or not isinstance(codec.get("writer_version"), str)
            or not codec["writer_version"]
            or len(codec["writer_version"]) > 64
        ):
            raise OutboxRecordError("archive Parquet codec claim is invalid")
        return {
            "path": path,
            "file_hash": file_hash,
            "bytes_written": byte_count,
            "schema": schema,
            "codec": codec,
        }

    def _validated_checkpoint_spec(
        self,
        value: Any,
        *,
        batch_id: str,
        organization: str,
    ) -> Dict[str, Any]:
        spec = self._validated_artifact_spec(
            value,
            expected_path=self._checkpoint_path(organization, batch_id),
            expected_schema="privileged-archive-checkpoint-v1",
        )
        manifest = value.get("manifest") if isinstance(value, Mapping) else None
        if not isinstance(manifest, Mapping):
            raise OutboxRecordError("archive checkpoint manifest is missing")
        manifest = dict(manifest)
        supplied_hash = manifest.get("manifest_sha256")
        unsigned = dict(manifest)
        unsigned.pop("manifest_sha256", None)
        computed_hash = hashlib.sha256(
            self._ledger_json(unsigned).encode("utf-8")
        ).hexdigest()
        if (
            supplied_hash != computed_hash
            or value.get("manifest_sha256") != computed_hash
            or manifest.get("batch_id") != batch_id
            or manifest.get("organization") != organization
            or manifest.get("kind") != "privileged_archive_checkpoint"
            or manifest.get("version") != 1
        ):
            raise OutboxRecordError("archive checkpoint manifest claim is invalid")
        payload = self._ledger_json(manifest).encode("utf-8")
        if (
            len(payload) != int(spec["bytes_written"])
            or compute_file_hash(payload) != spec["file_hash"]
        ):
            raise OutboxRecordError("archive checkpoint bytes do not match its claim")
        spec["manifest"] = manifest
        spec["manifest_sha256"] = computed_hash
        return spec

    def _write_and_verify_checkpoint(
        self,
        storage: Any,
        spec: Mapping[str, Any],
    ) -> bytes:
        path = str(spec["path"])
        expected = self._ledger_json(spec["manifest"]).encode("utf-8")
        try:
            exists = bool(storage.exists(path))
            if exists:
                ensure_durable = getattr(storage, "ensure_bytes_durable", None)
                if callable(ensure_durable):
                    ensure_durable(path)
                size = getattr(storage, "size", None)
                if callable(size) and int(size(path)) != len(expected):
                    raise ArchiveVerificationError(
                        "archive checkpoint size differs from its claim"
                    )
            else:
                atomic_write = getattr(storage, "write_bytes_atomic", None)
                if callable(atomic_write):
                    atomic_write(path, expected)
                else:
                    storage.write_bytes(path, expected)
            actual = self._sealed_read_bytes(
                storage,
                path,
                maximum_size=self._MAX_LEDGER_RECORD_BYTES,
                expected_size=len(expected),
                artifact="archive checkpoint",
            )
        except PrivilegedOutboxError:
            raise
        except Exception as exc:
            raise OutboxBackendError(
                "write/read immutable archive checkpoint", exc,
            ) from None
        if (
            actual != expected
            or compute_file_hash(actual) != spec["file_hash"]
        ):
            raise ArchiveVerificationError(
                "immutable archive checkpoint differs from its claim"
            )
        try:
            parsed = json.loads(actual)
        except (TypeError, ValueError) as exc:
            raise ArchiveVerificationError(
                "immutable archive checkpoint is not valid JSON"
            ) from None
        if parsed != spec["manifest"]:
            raise ArchiveVerificationError(
                "immutable archive checkpoint content is not canonical"
            )
        return actual

    def _write_and_verify_one_artifact(
        self,
        *,
        storage: Any,
        spec: Mapping[str, Any],
        rows: Sequence[Mapping[str, Any]],
        serializer: Callable[[Sequence[Mapping[str, Any]]], bytes],
        operation: str,
    ) -> bytes:
        path = str(spec["path"])
        try:
            exists = bool(storage.exists(path))
            if exists:
                ensure_durable = getattr(storage, "ensure_bytes_durable", None)
                if callable(ensure_durable):
                    ensure_durable(path)
                size = getattr(storage, "size", None)
                if callable(size) and int(size(path)) != int(spec["bytes_written"]):
                    raise ArchiveVerificationError(
                        f"{operation} size differs from its immutable claim"
                    )
            else:
                candidate = serializer(rows)
                if (
                    len(candidate) != int(spec["bytes_written"])
                    or compute_file_hash(candidate) != spec["file_hash"]
                    or dict(spec["codec"])
                    != self._codec_claim(str(spec["schema"]))
                ):
                    raise ArchiveVerificationError(
                        f"{operation} cannot be reproduced by this codec version"
                    )
                atomic_write = getattr(storage, "write_bytes_atomic", None)
                if callable(atomic_write):
                    atomic_write(path, candidate)
                else:
                    storage.write_bytes(path, candidate)
            actual = self._sealed_read_bytes(
                storage,
                path,
                maximum_size=self._MAX_PARQUET_ARTIFACT_BYTES,
                expected_size=int(spec["bytes_written"]),
                artifact=operation,
            )
        except PrivilegedOutboxError:
            raise
        except Exception as exc:
            raise OutboxBackendError(operation, exc) from None

        if (
            len(actual) != int(spec["bytes_written"])
            or compute_file_hash(actual) != spec["file_hash"]
        ):
            raise ArchiveVerificationError(
                f"{operation} differs from its immutable hash/size claim"
            )
        try:
            actual_rows = pq.read_table(io.BytesIO(actual)).to_pylist()
        except Exception as exc:
            raise ArchiveVerificationError(
                f"{operation} cannot be read; "
                f"error_type={_safe_error_type(exc)}"
            ) from None
        if actual_rows != list(rows):
            raise ArchiveVerificationError(f"{operation} rows differ from source evidence")
        return actual

    def _write_and_verify_archive(
        self,
        org: str,
        batch_id: str,
        entries: Sequence[OutboxEntry],
        manifests: Sequence[CascadeManifest],
        artifact_claim: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Publish or re-verify artifacts against the immutable batch claim."""
        if self._writer is None:
            raise ArchiveVerificationError("no ParquetAuditWriter is configured")
        rows = self._archive_rows(entries)
        try:
            storage = self._writer._get_storage()
        except Exception as exc:
            raise OutboxBackendError(
                "open privileged archive storage", exc
            ) from None
        parent_spec = self._validated_artifact_spec(
            artifact_claim.get("parent") if isinstance(artifact_claim, Mapping) else None,
            expected_path=self._archive_path(org, batch_id),
            expected_schema="privileged-audit-v1",
        )
        self._write_and_verify_one_artifact(
            storage=storage,
            spec=parent_spec,
            rows=rows,
            serializer=self._serialize_archive,
            operation="write/read privileged Parquet archive",
        )

        archive = {
            "path": parent_spec["path"],
            "file_hash": parent_spec["file_hash"],
            "event_count": len(entries),
            "bytes_written": parent_spec["bytes_written"],
            "schema": "privileged-audit-v1",
        }
        if manifests:
            cascade_rows = self._cascade_archive_rows(manifests)
            cascade_spec = self._validated_artifact_spec(
                artifact_claim.get("cascade"),
                expected_path=self._cascade_archive_path(org, batch_id),
                expected_schema="privileged-role-delete-cascade-v1",
            )
            self._write_and_verify_one_artifact(
                storage=storage,
                spec=cascade_spec,
                rows=cascade_rows,
                serializer=self._serialize_cascade_archive,
                operation="write/read role-delete cascade Parquet archive",
            )
            archive["cascade"] = {
                "path": cascade_spec["path"],
                "file_hash": cascade_spec["file_hash"],
                "manifest_count": len(manifests),
                "row_count": len(cascade_rows),
                "bytes_written": cascade_spec["bytes_written"],
                "schema": "privileged-role-delete-cascade-v1",
            }
        else:
            if artifact_claim.get("cascade") is not None:
                raise OutboxRecordError(
                    "archive batch claims cascade evidence without a parent event"
                )
            archive["cascade"] = None
        checkpoint_spec = self._validated_checkpoint_spec(
            artifact_claim.get("checkpoint"),
            batch_id=batch_id,
            organization=org,
        )
        self._write_and_verify_checkpoint(storage, checkpoint_spec)
        archive["checkpoint"] = {
            "path": checkpoint_spec["path"],
            "file_hash": checkpoint_spec["file_hash"],
            "bytes_written": checkpoint_spec["bytes_written"],
            "schema": checkpoint_spec["schema"],
            "manifest_sha256": checkpoint_spec["manifest_sha256"],
        }
        if self._archive_verifier is not None:
            try:
                extra_verified = self._archive_verifier(archive)
            except Exception as exc:
                raise OutboxBackendError(
                    "run additional archive verifier", exc
                ) from None
            if not extra_verified:
                raise ArchiveVerificationError("additional privileged archive verifier rejected delivery")
        return archive

    def _validate_batch_record(
        self,
        record: Mapping[str, Any],
        *,
        organization: str,
        batch_id: str,
        stream_ids: Sequence[str],
        sequences: Sequence[int],
    ) -> Mapping[str, Any]:
        if (
            record.get("kind") != "archive_batch"
            or record.get("batch_id") != batch_id
            or record.get("organization") != organization
            or tuple(record.get("stream_ids", ())) != tuple(stream_ids)
            or record.get("first_sequence") != sequences[0]
            or record.get("last_sequence") != sequences[-1]
            or record.get("first_stream_id") != stream_ids[0]
            or record.get("last_stream_id") != stream_ids[-1]
            or record.get("status")
            not in {"writing", "written_verified", "delivered"}
            or not isinstance(record.get("artifacts"), Mapping)
            or "predecessor_head" not in record
        ):
            raise OutboxRecordError(
                "delivery ledger batch identity/claim is malformed"
            )
        predecessor = record.get("predecessor_head")
        if predecessor is not None and not isinstance(predecessor, str):
            raise OutboxRecordError("archive batch predecessor head is invalid")
        return record

    def _finalize_batch_record(
        self,
        *,
        batch_field: str,
        batch_id: str,
        expected_raw: str,
        delivered_raw: str,
        predecessor_head: Optional[str],
        replacement_head: str,
        stream_ids: Sequence[str],
    ) -> Tuple[bool, str]:
        result = self._redis_call(
            "atomically finalize archive batch",
            "eval",
            self._FINALIZE_ARCHIVE_BATCH_LUA,
            1,
            self.delivery_ledger_key,
            batch_field,
            expected_raw,
            delivered_raw,
            self._ARCHIVE_HEAD_FIELD,
            "1" if predecessor_head is not None else "0",
            predecessor_head or "",
            replacement_head,
            batch_id,
            str(len(stream_ids)),
            *(self._entry_field(stream_id) for stream_id in stream_ids),
        )
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            raise OutboxRecordError("archive finalize Lua returned an invalid result")
        try:
            code = int(result[0])
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError("archive finalize status is invalid") from None
        current = _text(result[1], field="archive finalize batch record")
        if code == 1:
            return True, current
        if code == 0 and current:
            return False, current
        if code == -1:
            return False, current
        if code == -2:
            raise OutboxRecordError(
                "archive entry marker belongs to a different batch"
            )
        raise OutboxRecordError(
            f"archive finalize Lua returned unsupported status {code}"
        )

    def archive_batch(
        self,
        org: str,
        group: str,
        entries: Sequence[OutboxEntry],
        *,
        acknowledge: bool = True,
    ) -> ArchiveBatchResult:
        """Archive a batch once, verify it, mark delivery, then optionally ACK.

        The batch ID is also its Parquet filename, making a retry after any crash
        converge on the same bytes rather than creating a second archive object.
        """
        if not org or not group:
            raise ValueError("org and group are required")
        _safe_archive_organization(org)
        if not entries:
            raise ValueError("entries must not be empty")
        if len(entries) > self._MAX_ARCHIVE_BATCH_ENTRIES:
            raise ValueError(
                f"archive batch exceeds {self._MAX_ARCHIVE_BATCH_ENTRIES} entries"
            )
        if self._writer is None:
            raise ValueError("parquet_writer is required for archive_batch")
        entries = tuple(self._revalidate_archive_entry(entry) for entry in entries)
        stream_ids = tuple(entry.stream_id for entry in entries)
        if len(set(stream_ids)) != len(stream_ids):
            raise ValueError("entries contain duplicate stream IDs")
        if any(entry.event.get("organization") != org for entry in entries):
            raise OutboxRecordError(
                "archive organization does not match its privileged events"
            )
        stream_order = tuple(_stream_id_tuple(stream_id) for stream_id in stream_ids)
        if stream_order != tuple(sorted(stream_order)):
            raise OutboxRecordError("archive entries are not in Redis stream order")
        sequences = tuple(int(entry.event["ledger_sequence"]) for entry in entries)
        if any(
            current != previous + 1
            for previous, current in zip(sequences, sequences[1:])
        ):
            raise OutboxRecordError(
                "archive entries do not have a contiguous privileged ledger sequence"
            )
        batch_id = self.batch_id(org, entries)
        batch_field = self._batch_field(batch_id)
        existing_raw = self._ledger_get(batch_field)
        reused = existing_raw is not None
        existing_status: Optional[str] = None
        if existing_raw is not None:
            existing_ledger = self._decode_ledger(existing_raw)
            self._validate_batch_record(
                existing_ledger,
                organization=org,
                batch_id=batch_id,
                stream_ids=stream_ids,
                sequences=sequences,
            )
            existing_status = str(existing_ledger["status"])

        # Delivered retries may run after conservative source trimming. Every
        # first delivery and incomplete retry must still prove exact provenance.
        if existing_status != "delivered":
            entries = tuple(self._source_entry(entry) for entry in entries)

        # Resolve role-delete sidecars before claiming/delivery. A missing or
        # altered manifest cannot produce an archive marker or acknowledgement.
        manifests = self._load_cascade_manifests(entries)

        if existing_status != "delivered":
            if existing_raw is None:
                artifacts = self._new_artifact_claim(
                    org, batch_id, entries, manifests,
                )
            else:
                artifacts = self._decode_ledger(existing_raw).get("artifacts")
            claim = {
                "version": self._LEDGER_VERSION,
                "kind": "archive_batch",
                "status": "writing",
                "batch_id": batch_id,
                "organization": org,
                "stream_ids": list(stream_ids),
                "first_sequence": sequences[0],
                "last_sequence": sequences[-1],
                "first_stream_id": stream_ids[0],
                "last_stream_id": stream_ids[-1],
                "artifacts": artifacts,
                "claimed_ms": self._clock_ms(),
            }
            try:
                existing_raw = self._claim_archive_sequence(
                    organization=org,
                    batch_id=batch_id,
                    batch_field=batch_field,
                    batch_record=claim,
                    first_sequence=sequences[0],
                    last_sequence=sequences[-1],
                    first_stream_id=stream_ids[0],
                    last_stream_id=stream_ids[-1],
                )
            except DeliveryPendingError:
                raced_raw = self._ledger_get(batch_field)
                if raced_raw is None:
                    raise
                raced = self._decode_ledger(raced_raw)
                self._validate_batch_record(
                    raced,
                    organization=org,
                    batch_id=batch_id,
                    stream_ids=stream_ids,
                    sequences=sequences,
                )
                if raced.get("status") != "delivered":
                    raise
                existing_raw = raced_raw
                reused = True

        if existing_raw is None:  # pragma: no cover - defensive invariant
            raise OutboxRecordError("archive batch has no durable claim")

        ledger_raw = existing_raw
        for _ in range(8):
            ledger = self._decode_ledger(ledger_raw)
            self._validate_batch_record(
                ledger,
                organization=org,
                batch_id=batch_id,
                stream_ids=stream_ids,
                sequences=sequences,
            )
            status = ledger["status"]
            artifacts = ledger["artifacts"]
            if status == "writing":
                archive = self._write_and_verify_archive(
                    org, batch_id, entries, manifests, artifacts,
                )
                replacement = dict(ledger)
                replacement.update({
                    "status": "written_verified",
                    "archive": archive,
                    "written_ms": self._clock_ms(),
                })
                replacement_raw = self._ledger_json(replacement)
                _, ledger_raw = self._cas_batch_record(
                    batch_field, ledger_raw, replacement_raw,
                )
                continue

            archive = ledger.get("archive")
            if not isinstance(archive, Mapping):
                raise OutboxRecordError(
                    "written delivery ledger record has no archive metadata"
                )
            reconciled = self._write_and_verify_archive(
                org, batch_id, entries, manifests, artifacts,
            )
            if dict(reconciled) != dict(archive):
                raise ArchiveVerificationError(
                    "ledger archive metadata differs from verified archive"
                )

            if status == "written_verified":
                delivered = dict(ledger)
                delivered.update({
                    "status": "delivered",
                    "delivered_ms": self._clock_ms(),
                })
                delivered_raw = self._ledger_json(delivered)
                checkpoint = archive.get("checkpoint")
                if not isinstance(checkpoint, Mapping):
                    raise OutboxRecordError(
                        "archive has no immutable checkpoint metadata"
                    )
                head_raw = self._ledger_json({
                    "version": self._LEDGER_VERSION,
                    "kind": "archive_head",
                    "organization": org,
                    "batch_id": batch_id,
                    "first_sequence": sequences[0],
                    "last_sequence": sequences[-1],
                    "first_stream_id": stream_ids[0],
                    "last_stream_id": stream_ids[-1],
                    "checkpoint_path": checkpoint["path"],
                    "checkpoint_sha256": checkpoint["manifest_sha256"],
                    "delivered_ms": self._clock_ms(),
                })
                _, ledger_raw = self._finalize_batch_record(
                    batch_field=batch_field,
                    batch_id=batch_id,
                    expected_raw=ledger_raw,
                    delivered_raw=delivered_raw,
                    predecessor_head=ledger.get("predecessor_head"),
                    replacement_head=head_raw,
                    stream_ids=stream_ids,
                )
                continue

            if status != "delivered":
                raise OutboxRecordError("delivery ledger has an unsupported status")
            raw_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
            if raw_head is None:
                raise OutboxRecordError(
                    "delivered archive has no durable sequence head"
                )
            archived_through, archived_stream_id = self._validated_archive_head(
                raw_head, organization=org,
            )
            if (
                archived_through < sequences[-1]
                or archived_stream_id is None
                or _stream_id_tuple(archived_stream_id)
                < _stream_id_tuple(stream_ids[-1])
            ):
                raise OutboxRecordError(
                    "durable archive head does not cover this batch"
                )
            markers = self._redis_call(
                "verify archive entry markers",
                "hmget",
                self.delivery_ledger_key,
                [self._entry_field(stream_id) for stream_id in stream_ids],
            )
            if markers is None or any(
                marker is None
                or _text(marker, field="archive entry marker") != batch_id
                for marker in markers
            ):
                raise OutboxRecordError(
                    "delivered archive entry markers are missing or inconsistent"
                )
            acknowledged = self.ack(group, stream_ids) if acknowledge else 0
            return ArchiveBatchResult(
                batch_id,
                stream_ids,
                dict(archive),
                acknowledged,
                reused=reused,
            )
        raise DeliveryPendingError(
            "archive batch changed concurrently too many times; retry"
        )

    def drain_once(
        self,
        organization: str,
        *,
        group: str = "__privileged_archival__",
        consumer: str,
        count: int = 100,
        reclaim_idle_ms: int = 300_000,
    ) -> Optional[ArchiveBatchResult]:
        """Recover abandoned entries, consume one batch, and archive it.

        This is the bounded unit a separately supervised worker should call.
        Failures deliberately propagate: entries remain pending and the next
        invocation reclaims them, while a verified deterministic archive is
        reused rather than duplicated.
        """
        if not organization or not consumer:
            raise ValueError("organization and consumer are required")
        _safe_archive_organization(organization)
        if count <= 0 or reclaim_idle_ms < 0:
            raise ValueError("count must be positive and reclaim_idle_ms non-negative")
        if count > self._MAX_ARCHIVE_BATCH_ENTRIES:
            raise ValueError(
                f"count must not exceed {self._MAX_ARCHIVE_BATCH_ENTRIES}"
            )
        self.create_group(group, start_id="0-0")

        # A prior crash may have fixed an immutable partition before publishing
        # or ACKing it. Resume that exact membership before selecting any new
        # partition; never guess from the current worker's count.
        raw_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        head_sequence, _ = self._validated_archive_head(
            raw_head, organization=organization,
        )
        claimed_batch_id = self._ledger_get(
            self._sequence_claim_field(head_sequence + 1)
        )
        if claimed_batch_id is not None:
            claimed_raw = self._ledger_get(self._batch_field(claimed_batch_id))
            if claimed_raw is None:
                raise OutboxRecordError(
                    "next archive sequence has an orphan batch claim"
                )
            claimed = self._decode_ledger(claimed_raw)
            stream_ids = claimed.get("stream_ids")
            if (
                claimed.get("batch_id") != claimed_batch_id
                or claimed.get("organization") != organization
                or not isinstance(stream_ids, list)
                or not stream_ids
                or len(stream_ids) > self._MAX_ARCHIVE_BATCH_ENTRIES
            ):
                raise OutboxRecordError("claimed archive batch membership is invalid")
            return self.archive_batch(
                organization,
                group,
                self._source_entries(tuple(stream_ids)),
                acknowledge=True,
            )

        claim_result = self.autoclaim(
            group,
            consumer,
            min_idle_ms=reclaim_idle_ms,
            start_id="0-0",
            count=count,
        )
        if claim_result.deleted_ids:
            raise OutboxRecordError(
                "privileged audit consumer group contains deleted pending stream IDs: "
                + ", ".join(claim_result.deleted_ids)
            )
        reclaimed = tuple(sorted(
            claim_result.entries, key=lambda entry: _stream_id_tuple(entry.stream_id),
        ))
        if reclaimed:
            marker_fields = [self._entry_field(entry.stream_id) for entry in reclaimed]
            markers = self._redis_call(
                "read reclaimed delivery markers",
                "hmget",
                self.delivery_ledger_key,
                marker_fields,
            )
            if markers is None or len(markers) != len(reclaimed):
                raise OutboxRecordError(
                    "delivery ledger returned invalid reclaimed markers"
                )
            delivered_ids: List[str] = []
            pending_entries: List[OutboxEntry] = []
            verified: Dict[str, Tuple[OutboxEntry, ...]] = {}
            for entry, marker in zip(reclaimed, markers):
                if marker is None:
                    pending_entries.append(entry)
                    continue
                delivered_batch_id = _text(
                    marker, field="reclaimed archive delivery marker",
                )
                archived = verified.get(delivered_batch_id)
                if archived is None:
                    archived = self.read_archive_batch(delivered_batch_id)
                    verified[delivered_batch_id] = archived
                if not any(
                    self._same_entry(entry, candidate) for candidate in archived
                ):
                    raise OutboxRecordError(
                        "reclaimed entry marker does not reference exact archive evidence"
                    )
                delivered_ids.append(entry.stream_id)
            if delivered_ids:
                self.ack(group, delivered_ids)
            if not pending_entries:
                return None
            ordered = tuple(pending_entries)
        else:
            pending_summary = self._redis_call(
                "inspect pending privileged deliveries",
                "xpending",
                self.stream_key,
                group,
            )
            if not isinstance(pending_summary, Mapping):
                raise OutboxRecordError("XPENDING returned an invalid summary")
            pending_value = None
            for raw_key, raw_value in pending_summary.items():
                if _text(raw_key, field="XPENDING summary key") == "pending":
                    pending_value = raw_value
                    break
            if isinstance(pending_value, bool):
                raise OutboxRecordError("XPENDING returned a boolean pending count")
            try:
                pending_count = int(pending_value)
            except (TypeError, ValueError) as exc:
                raise OutboxRecordError(
                    "XPENDING returned an invalid pending count"
                ) from None
            if pending_count < 0:
                raise OutboxRecordError("XPENDING returned a negative pending count")
            if pending_count:
                raise DeliveryPendingError(
                    "an earlier privileged delivery is pending but not idle enough "
                    "to reclaim; wait before reading fresh entries"
                )
            fresh = self.read_group(group, consumer, count=count)
            if not fresh:
                return None
            ordered = tuple(sorted(
                fresh, key=lambda entry: _stream_id_tuple(entry.stream_id),
            ))
        latest_head = self._ledger_get(self._ARCHIVE_HEAD_FIELD)
        archived_through, _ = self._validated_archive_head(
            latest_head, organization=organization,
        )
        first_sequence = int(ordered[0].event["ledger_sequence"])
        if first_sequence > archived_through + 1:
            raise DeliveryPendingError(
                "the selected privileged delivery starts after an earlier pending "
                "ledger sequence; reclaim the earlier sequence first"
            )
        return self.archive_batch(
            organization,
            group,
            ordered,
            acknowledge=True,
        )

    def _read_checkpoint_manifest(
        self,
        batch_id: str,
        *,
        organization: str,
        expected_file_hash: Optional[str] = None,
        expected_manifest_hash: Optional[str] = None,
        expected_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._writer is None:
            raise ArchiveVerificationError("no ParquetAuditWriter is configured")
        if not organization:
            raise ValueError("organization is required to read an archive checkpoint")
        path = expected_path or self._checkpoint_path(organization, batch_id)
        if path != self._checkpoint_path(organization, batch_id):
            raise ArchiveVerificationError("archive checkpoint path is not deterministic")
        try:
            storage = self._writer._get_storage()
            payload = self._sealed_read_bytes(
                storage,
                path,
                maximum_size=self._MAX_LEDGER_RECORD_BYTES,
                artifact="archive checkpoint",
            )
        except PrivilegedOutboxError:
            raise
        except Exception as exc:
            raise OutboxBackendError(
                "read immutable archive checkpoint", exc
            ) from None
        if len(payload) > self._MAX_LEDGER_RECORD_BYTES:
            raise ArchiveVerificationError("archive checkpoint exceeds the 1-MiB limit")
        if (
            expected_file_hash is not None
            and compute_file_hash(payload) != expected_file_hash
        ):
            raise ArchiveVerificationError(
                "archive checkpoint file hash differs from its claim"
            )
        try:
            manifest = json.loads(payload)
        except (TypeError, ValueError) as exc:
            raise ArchiveVerificationError(
                "archive checkpoint is invalid JSON"
            ) from None
        if not isinstance(manifest, dict):
            raise ArchiveVerificationError("archive checkpoint must be an object")
        supplied_hash = manifest.get("manifest_sha256")
        unsigned = dict(manifest)
        unsigned.pop("manifest_sha256", None)
        computed_hash = hashlib.sha256(
            self._ledger_json(unsigned).encode("utf-8")
        ).hexdigest()
        if supplied_hash != computed_hash or (
            expected_manifest_hash is not None
            and supplied_hash != expected_manifest_hash
        ):
            raise ArchiveVerificationError(
                "archive checkpoint canonical hash is invalid"
            )
        if (
            manifest.get("version") != 1
            or manifest.get("kind") != "privileged_archive_checkpoint"
            or manifest.get("batch_id") != batch_id
            or manifest.get("organization") != organization
        ):
            raise ArchiveVerificationError("archive checkpoint identity is invalid")
        try:
            first_sequence = int(manifest["first_sequence"])
            last_sequence = int(manifest["last_sequence"])
            event_count = int(manifest["event_count"])
            first_stream_id = str(manifest["first_stream_id"])
            last_stream_id = str(manifest["last_stream_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ArchiveVerificationError(
                "archive checkpoint range is invalid"
            ) from None
        if (
            first_sequence < 1
            or last_sequence < first_sequence
            or event_count != last_sequence - first_sequence + 1
            or event_count < 1
            or event_count > self._MAX_ARCHIVE_BATCH_ENTRIES
            or _stream_id_tuple(first_stream_id) > _stream_id_tuple(last_stream_id)
        ):
            raise ArchiveVerificationError("archive checkpoint range is inconsistent")
        previous = manifest.get("previous")
        if first_sequence == 1:
            if previous is not None:
                raise ArchiveVerificationError(
                    "genesis archive checkpoint has a predecessor"
                )
        elif (
            not isinstance(previous, Mapping)
            or previous.get("last_sequence") != first_sequence - 1
            or not _is_sha256(previous.get("batch_id"))
            or not isinstance(previous.get("last_stream_id"), str)
            or not _is_sha256(previous.get("manifest_sha256"))
            or not isinstance(previous.get("path"), str)
        ):
            raise ArchiveVerificationError(
                "archive checkpoint predecessor metadata is invalid"
            )
        if previous is not None:
            if previous["path"] != self._checkpoint_path(
                organization, previous["batch_id"],
            ):
                raise ArchiveVerificationError(
                    "archive checkpoint predecessor path is invalid"
                )
            try:
                previous_stream_id = _stream_id_tuple(previous["last_stream_id"])
            except OutboxRecordError as exc:
                raise ArchiveVerificationError(
                    "archive checkpoint predecessor stream ID is invalid"
                ) from None
            if previous_stream_id >= _stream_id_tuple(first_stream_id):
                raise ArchiveVerificationError(
                    "archive checkpoint stream range does not advance its predecessor"
                )
        organization = manifest.get("organization")
        if not isinstance(organization, str) or not organization:
            raise ArchiveVerificationError("archive checkpoint organization is invalid")
        self._validated_artifact_spec(
            manifest.get("parent"),
            expected_path=self._archive_path(organization, batch_id),
            expected_schema="privileged-audit-v1",
        )
        cascade = manifest.get("cascade")
        if cascade is not None:
            self._validated_artifact_spec(
                cascade,
                expected_path=self._cascade_archive_path(organization, batch_id),
                expected_schema="privileged-role-delete-cascade-v1",
            )
        if payload != self._ledger_json(manifest).encode("utf-8"):
            raise ArchiveVerificationError("archive checkpoint JSON is not canonical")
        return manifest

    def _archive_metadata_from_manifest(
        self,
        manifest: Mapping[str, Any],
    ) -> Dict[str, Any]:
        parent = manifest["parent"]
        cascade = manifest.get("cascade")
        archive: Dict[str, Any] = {
            "path": parent["path"],
            "file_hash": parent["file_hash"],
            "event_count": manifest["event_count"],
            "bytes_written": parent["bytes_written"],
            "schema": parent["schema"],
            "checkpoint": {
                "path": self._checkpoint_path(
                    str(manifest["organization"]), str(manifest["batch_id"]),
                ),
                "file_hash": "",
                "bytes_written": len(self._ledger_json(manifest).encode("utf-8")),
                "schema": "privileged-archive-checkpoint-v1",
                "manifest_sha256": manifest["manifest_sha256"],
            },
        }
        if cascade is None:
            archive["cascade"] = None
        else:
            archive["cascade"] = {
                "path": cascade["path"],
                "file_hash": cascade["file_hash"],
                "manifest_count": cascade.get("manifest_count", -1),
                "row_count": cascade.get("row_count", -1),
                "bytes_written": cascade["bytes_written"],
                "schema": cascade["schema"],
            }
        return archive

    def read_archive_batch(
        self,
        batch_id: str,
        *,
        organization: Optional[str] = None,
    ) -> Tuple[OutboxEntry, ...]:
        """Read and revalidate one delivered immutable archive batch.

        The delivery ledger is used only as an index.  File bytes, SHA-256,
        row count, exact template JSON, commit envelope, and canonical final
        event are all checked again before any record is returned.
        """
        if not _is_sha256(batch_id):
            raise ValueError("batch_id must be lowercase SHA-256 hex")
        raw_ledger = self._ledger_get(self._batch_field(batch_id))
        ledger: Optional[Mapping[str, Any]] = None
        if raw_ledger is not None:
            ledger = self._decode_ledger(raw_ledger)
            if ledger.get("status") != "delivered":
                raise DeliveryPendingError("archive batch is not delivered")
            ledger_organization = ledger.get("organization")
            if not isinstance(ledger_organization, str) or not ledger_organization:
                raise OutboxRecordError("delivered batch organization is invalid")
            if organization is not None and organization != ledger_organization:
                raise ArchiveVerificationError(
                    "requested organization differs from delivered archive batch"
                )
            organization = ledger_organization
            archive = ledger.get("archive")
            artifacts = ledger.get("artifacts")
            checkpoint_claim = (
                artifacts.get("checkpoint")
                if isinstance(artifacts, Mapping)
                else None
            )
            if not isinstance(archive, Mapping) or not isinstance(
                checkpoint_claim, Mapping,
            ):
                raise OutboxRecordError(
                    "delivered batch has no archive/checkpoint metadata"
                )
            manifest = self._read_checkpoint_manifest(
                batch_id,
                organization=organization,
                expected_file_hash=str(checkpoint_claim.get("file_hash", "")),
                expected_manifest_hash=str(
                    checkpoint_claim.get("manifest_sha256", "")
                ),
                expected_path=str(checkpoint_claim.get("path", "")),
            )
            checkpoint_archive = self._archive_metadata_from_manifest(manifest)
            for field in ("path", "file_hash", "event_count", "bytes_written", "schema"):
                if archive.get(field) != checkpoint_archive.get(field):
                    raise ArchiveVerificationError(
                        "delivery ledger archive differs from immutable checkpoint"
                    )
            if archive.get("cascade") != checkpoint_archive.get("cascade"):
                raise ArchiveVerificationError(
                    "delivery ledger cascade archive differs from immutable checkpoint"
                )
        else:
            if not organization:
                raise ValueError(
                    "organization is required after archive delivery metadata is trimmed"
                )
            manifest = self._read_checkpoint_manifest(
                batch_id, organization=organization,
            )
            archive = self._archive_metadata_from_manifest(manifest)
        if self._writer is None:
            raise ArchiveVerificationError("no ParquetAuditWriter is configured")
        path = archive.get("path")
        if not isinstance(path, str) or not path:
            raise OutboxRecordError("archive metadata has no valid path")
        try:
            storage = self._writer._get_storage()
            payload = self._sealed_read_bytes(
                storage,
                path,
                maximum_size=self._MAX_PARQUET_ARTIFACT_BYTES,
                expected_size=int(archive.get("bytes_written", -1)),
                artifact="privileged archive",
            )
        except PrivilegedOutboxError:
            raise
        except Exception as exc:
            raise OutboxBackendError(
                "read privileged Parquet archive", exc
            ) from None
        if len(payload) != int(archive.get("bytes_written", -1)):
            raise ArchiveVerificationError(
                "privileged archive size differs from checkpoint"
            )
        if compute_file_hash(payload) != archive.get("file_hash"):
            raise ArchiveVerificationError("privileged archive file hash differs from ledger")
        try:
            rows = pq.read_table(io.BytesIO(payload)).to_pylist()
        except Exception as exc:
            raise ArchiveVerificationError(
                "privileged archive cannot be decoded; "
                f"error_type={_safe_error_type(exc)}"
            ) from None
        if len(rows) != int(archive.get("event_count", -1)):
            raise ArchiveVerificationError("privileged archive row count differs from ledger")

        entries: List[OutboxEntry] = []
        expected_columns = {
            "stream_id", "event_json", "template_event_json",
            *self._INDEX_FIELDS,
        }
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != expected_columns:
                raise ArchiveVerificationError(
                    "privileged archive contains an invalid row schema"
                )
            fields = {"event_json": row.get("template_event_json")}
            fields.update({
                field: str(row.get(field, ""))
                for field in self._INDEX_FIELDS
            })
            entry = self._decode_entry((row.get("stream_id"), fields))
            if entry.committed_event_json != row.get("event_json"):
                raise ArchiveVerificationError(
                    "privileged archive committed event differs from its exact envelope"
                )
            entries.append(entry)
        stream_ids = tuple(entry.stream_id for entry in entries)
        if ledger is not None:
            if stream_ids != tuple(ledger.get("stream_ids", ())):
                raise ArchiveVerificationError(
                    "privileged archive stream IDs differ from ledger"
                )
        elif (
            len(entries) != int(manifest["event_count"])
            or not entries
            or stream_ids[0] != manifest["first_stream_id"]
            or stream_ids[-1] != manifest["last_stream_id"]
            or int(entries[0].event["ledger_sequence"])
            != int(manifest["first_sequence"])
            or int(entries[-1].event["ledger_sequence"])
            != int(manifest["last_sequence"])
            or self.batch_id(str(manifest["organization"]), entries) != batch_id
        ):
            raise ArchiveVerificationError(
                "privileged archive does not match its immutable checkpoint"
            )
        # A parent batch is not readable as verified evidence unless every
        # linked cascade artifact still passes its own hash/row/parent checks.
        self._read_verified_cascade_archive(archive, entries)
        return tuple(entries)

    def _read_verified_cascade_archive(
        self,
        archive: Mapping[str, Any],
        entries: Sequence[OutboxEntry],
    ) -> Tuple[CascadeManifest, ...]:
        parents = {
            str(entry.event["event_id"]): entry.event
            for entry in entries
            if entry.event.get("cascade_manifest_id")
        }
        cascade_meta = archive.get("cascade")
        if not parents:
            if cascade_meta is not None:
                raise ArchiveVerificationError(
                    "archive has cascade evidence without a parent event"
                )
            return ()
        if not isinstance(cascade_meta, Mapping):
            raise ArchiveVerificationError(
                "role-delete archive is missing cascade artifact metadata"
            )
        if cascade_meta.get("schema") != "privileged-role-delete-cascade-v1":
            raise ArchiveVerificationError("cascade archive schema is invalid")
        path = cascade_meta.get("path")
        if not isinstance(path, str) or not path:
            raise ArchiveVerificationError("cascade archive path is invalid")
        if self._writer is None:
            raise ArchiveVerificationError("no ParquetAuditWriter is configured")
        try:
            storage = self._writer._get_storage()
            payload = self._sealed_read_bytes(
                storage,
                path,
                maximum_size=self._MAX_PARQUET_ARTIFACT_BYTES,
                expected_size=int(cascade_meta.get("bytes_written", -1)),
                artifact="cascade archive",
            )
        except PrivilegedOutboxError:
            raise
        except Exception as exc:
            raise OutboxBackendError(
                "read role-delete cascade archive", exc
            ) from None
        if len(payload) != int(cascade_meta.get("bytes_written", -1)):
            raise ArchiveVerificationError(
                "cascade archive size differs from checkpoint"
            )
        if compute_file_hash(payload) != cascade_meta.get("file_hash"):
            raise ArchiveVerificationError(
                "cascade archive file hash differs from ledger"
            )
        try:
            rows = pq.read_table(io.BytesIO(payload)).to_pylist()
        except Exception as exc:
            raise ArchiveVerificationError(
                "cascade archive cannot be decoded; "
                f"error_type={_safe_error_type(exc)}"
            ) from None
        try:
            expected_row_count = int(cascade_meta.get("row_count", -1))
            expected_manifest_count = int(
                cascade_meta.get("manifest_count", -1)
            )
        except (TypeError, ValueError) as exc:
            raise ArchiveVerificationError(
                "cascade archive metadata counters are invalid"
            ) from None
        if len(rows) != expected_row_count:
            raise ArchiveVerificationError(
                "cascade archive row count differs from ledger"
            )
        if len(parents) != expected_manifest_count:
            raise ArchiveVerificationError(
                "cascade archive manifest count differs from its parent batch"
            )

        expected_columns = {
            "row_kind",
            "schema_version",
            "event_id",
            "mutation_id",
            "organization",
            "super_name",
            "role_id",
            "user_count",
            "removed_assignment_count",
            "user_namespace_version_before",
            "user_namespace_version_after",
            "created_ms",
            "user_id",
            "before_doc_version",
            "after_doc_version",
            "removed_occurrences",
            "before_role_count",
            "after_role_count",
        }
        raw_by_event: Dict[str, Dict[str, str]] = {
            event_id: {} for event_id in parents
        }
        header_seen: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != expected_columns:
                raise ArchiveVerificationError(
                    "cascade archive contains an invalid row schema"
                )
            event_id = row.get("event_id")
            if not isinstance(event_id, str) or event_id not in parents:
                raise ArchiveVerificationError(
                    "cascade archive row has no matching parent event"
                )
            raw = raw_by_event[event_id]
            common = {
                "schema_version": str(row["schema_version"]),
                "event_id": event_id,
                "mutation_id": str(row["mutation_id"]),
                "organization": str(row["organization"]),
                "super_name": str(row["super_name"]),
                "role_id": str(row["role_id"]),
                "user_count": str(row["user_count"]),
                "removed_assignment_count": str(
                    row["removed_assignment_count"]
                ),
                "user_namespace_version_before": str(
                    row["user_namespace_version_before"]
                ),
                "user_namespace_version_after": str(
                    row["user_namespace_version_after"]
                ),
                "created_ms": str(row["created_ms"]),
            }
            for field, value in common.items():
                previous = raw.setdefault(field, value)
                if previous != value:
                    raise ArchiveVerificationError(
                        "cascade archive repeats inconsistent manifest metadata"
                    )
            row_kind = row.get("row_kind")
            if row_kind == "manifest":
                if event_id in header_seen:
                    raise ArchiveVerificationError(
                        "cascade archive contains duplicate manifest headers"
                    )
                header_seen.add(event_id)
                if row.get("user_id") != "" or any(
                    int(row[field]) != 0
                    for field in (
                        "before_doc_version",
                        "after_doc_version",
                        "removed_occurrences",
                        "before_role_count",
                        "after_role_count",
                    )
                ):
                    raise ArchiveVerificationError(
                        "cascade manifest header contains user evidence"
                    )
            elif row_kind == "user":
                user_id = row.get("user_id")
                if not isinstance(user_id, str) or not user_id:
                    raise ArchiveVerificationError(
                        "cascade user row has no user identity"
                    )
                user_field = f"user:{user_id}"
                if user_field in raw:
                    raise ArchiveVerificationError(
                        "cascade archive contains a duplicate user row"
                    )
                raw[user_field] = "|".join(str(row[field]) for field in (
                    "before_doc_version",
                    "after_doc_version",
                    "removed_occurrences",
                    "before_role_count",
                    "after_role_count",
                ))
            else:
                raise ArchiveVerificationError(
                    "cascade archive contains an unknown row kind"
                )
        if header_seen != set(parents):
            raise ArchiveVerificationError(
                "cascade archive is missing a manifest header"
            )
        manifests = tuple(
            self._parse_cascade_manifest(parent, raw_by_event[event_id])
            for event_id, parent in parents.items()
        )
        if rows != self._cascade_archive_rows(manifests):
            raise ArchiveVerificationError(
                "cascade archive rows are not in canonical parent/user order"
            )
        return manifests

    def read_archive_cascades(
        self,
        batch_id: str,
        *,
        organization: Optional[str] = None,
    ) -> Tuple[CascadeManifest, ...]:
        """Return exact cascade manifests from a delivered archive batch."""

        entries = self.read_archive_batch(batch_id, organization=organization)
        raw_ledger = self._ledger_get(self._batch_field(batch_id))
        if raw_ledger is None:
            if not organization:
                raise ValueError(
                    "organization is required after archive delivery metadata is trimmed"
                )
            archive = self._archive_metadata_from_manifest(
                self._read_checkpoint_manifest(
                    batch_id, organization=organization,
                )
            )
        else:
            ledger = self._decode_ledger(raw_ledger)
            archive = ledger.get("archive")
            if not isinstance(archive, Mapping):
                raise OutboxRecordError("delivered batch has no archive metadata")
        return self._read_verified_cascade_archive(archive, entries)

    # -- health and conservative retention ----------------------------

    def verify_checkpoint_head(
        self,
        organization: str,
    ) -> Optional[Mapping[str, Any]]:
        """Fast-check the Redis head, its predecessor, and latest artifacts.

        ``None`` is returned only when Redis confirms that no archive head
        exists. Backend and integrity failures always raise. Use
        :meth:`verify_checkpoint_chain` for a bounded walk to genesis.
        """

        if not organization:
            raise ValueError("organization is required")
        _safe_archive_organization(organization)
        raw_head = self._archive_head_for_verification(organization)
        if raw_head is None:
            return None
        last_sequence, last_stream_id = self._validated_archive_head(
            raw_head, organization=organization,
        )
        head = self._decode_ledger(raw_head)
        batch_id = head.get("batch_id")
        checkpoint_path = head.get("checkpoint_path")
        checkpoint_hash = head.get("checkpoint_sha256")
        if not _is_sha256(batch_id):
            raise OutboxRecordError("archive head batch ID is invalid")
        manifest = self._read_checkpoint_manifest(
            batch_id,
            organization=organization,
            expected_manifest_hash=(
                checkpoint_hash if isinstance(checkpoint_hash, str) else ""
            ),
            expected_path=(
                checkpoint_path if isinstance(checkpoint_path, str) else ""
            ),
        )
        if (
            int(manifest["last_sequence"]) != last_sequence
            or manifest["last_stream_id"] != last_stream_id
        ):
            raise ArchiveVerificationError(
                "archive head range differs from immutable checkpoint"
            )
        previous = manifest.get("previous")
        if previous is not None:
            predecessor = self._read_checkpoint_manifest(
                str(previous["batch_id"]),
                organization=organization,
                expected_manifest_hash=str(previous["manifest_sha256"]),
                expected_path=str(previous["path"]),
            )
            if (
                int(predecessor["last_sequence"])
                != int(previous["last_sequence"])
                or predecessor["last_stream_id"]
                != previous["last_stream_id"]
            ):
                raise ArchiveVerificationError(
                    "archive checkpoint predecessor range is invalid"
                )
        entries = self.read_archive_batch(batch_id, organization=organization)
        if (
            not entries
            or entries[-1].stream_id != last_stream_id
            or int(entries[-1].event["ledger_sequence"]) != last_sequence
        ):
            raise ArchiveVerificationError(
                "latest archive artifact does not reach checkpoint head"
            )
        return dict(head)

    def verify_checkpoint_chain(
        self,
        organization: str,
        *,
        max_batches: int = 10_000,
    ) -> Mapping[str, Any]:
        """Walk and verify the immutable archive chain through genesis.

        The walk is explicitly bounded so damaged or cyclic evidence cannot
        turn an operator verification request into unbounded work. Every
        checkpoint and every parent/cascade Parquet artifact is read and
        revalidated.
        """

        if not organization:
            raise ValueError("organization is required")
        _safe_archive_organization(organization)
        if max_batches <= 0:
            raise ValueError("max_batches must be positive")
        head = self.verify_checkpoint_head(organization)
        if head is None:
            return {
                "organization": organization,
                "batch_count": 0,
                "first_sequence": 0,
                "last_sequence": 0,
                "latest_batch_id": None,
            }

        current_batch_id = str(head["batch_id"])
        expected_hash = str(head["checkpoint_sha256"])
        expected_path = str(head["checkpoint_path"])
        expected_last_sequence = int(head["last_sequence"])
        expected_last_stream_id = str(head["last_stream_id"])
        seen: set[str] = set()
        count = 0
        genesis_sequence = 0
        while True:
            if current_batch_id in seen:
                raise ArchiveVerificationError(
                    "archive checkpoint chain contains a cycle"
                )
            if count >= max_batches:
                raise ArchiveVerificationError(
                    f"archive checkpoint chain exceeds {max_batches} batches"
                )
            seen.add(current_batch_id)
            manifest = self._read_checkpoint_manifest(
                current_batch_id,
                organization=organization,
                expected_manifest_hash=expected_hash,
                expected_path=expected_path,
            )
            if (
                int(manifest["last_sequence"]) != expected_last_sequence
                or manifest["last_stream_id"] != expected_last_stream_id
            ):
                raise ArchiveVerificationError(
                    "archive checkpoint chain sequence is not contiguous"
                )
            self.read_archive_batch(
                current_batch_id, organization=organization,
            )
            count += 1
            previous = manifest.get("previous")
            if previous is None:
                if int(manifest["first_sequence"]) != 1:
                    raise ArchiveVerificationError(
                        "archive checkpoint chain does not reach genesis"
                    )
                genesis_sequence = int(manifest["first_sequence"])
                break
            expected_last_sequence = int(previous["last_sequence"])
            expected_last_stream_id = str(previous["last_stream_id"])
            current_batch_id = str(previous["batch_id"])
            expected_hash = str(previous["manifest_sha256"])
            expected_path = str(previous["path"])

        return {
            "organization": organization,
            "batch_count": count,
            "first_sequence": genesis_sequence,
            "last_sequence": int(head["last_sequence"]),
            "latest_batch_id": str(head["batch_id"]),
        }

    def health(self) -> OutboxHealth:
        """Check Redis reachability and stream/group metadata without swallowing errors."""
        pong = self._redis_call("ping", "ping")
        if pong is not True and pong != b"PONG" and pong != "PONG":
            raise OutboxRecordError("Redis PING returned an unexpected response")
        raw_exists = self._redis_call(
            "check stream existence", "exists", self.stream_key
        )
        if isinstance(raw_exists, bool):
            exists = raw_exists
        elif type(raw_exists) is int and raw_exists >= 0:
            exists = raw_exists > 0
        else:
            raise OutboxRecordError("Redis EXISTS returned an invalid response")
        if not exists:
            return OutboxHealth(True, False, 0, 0)
        length = self._redis_call("read stream length", "xlen", self.stream_key)
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise OutboxRecordError("Redis XLEN returned an invalid response")
        groups = self._redis_call("list consumer groups", "xinfo_groups", self.stream_key)
        if not isinstance(groups, (list, tuple)):
            raise OutboxRecordError("XINFO GROUPS returned an invalid response")
        return OutboxHealth(True, True, length, len(groups))

    def trim_delivered(self, group: str, *, through_id: str, max_entries: int = 1000) -> int:
        """Delete entries through an ID only after archive markers and group ACKs.

        This is intentionally conservative: *any* pending entry in the group
        blocks trimming, even when that pending entry is newer than the cutoff.
        """
        if not group:
            raise ValueError("group is required")
        _stream_id_tuple(through_id)
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        if max_entries > self._MAX_ARCHIVE_BATCH_ENTRIES:
            raise ValueError(
                f"max_entries must not exceed {self._MAX_ARCHIVE_BATCH_ENTRIES}"
            )
        groups = self._redis_call("list consumer groups", "xinfo_groups", self.stream_key)
        group_info: Optional[Mapping[Any, Any]] = None
        for item in groups or []:
            decoded = {_text(k, field="group metadata key"): v for k, v in item.items()}
            if _text(decoded.get("name", ""), field="group name") == group:
                group_info = decoded
                break
        if group_info is None:
            raise DeliveryPendingError("consumer group does not exist")
        pending = int(group_info.get("pending", 0))
        last_delivered = _text(group_info.get("last-delivered-id", "0-0"), field="last-delivered-id")
        if pending or _stream_id_tuple(last_delivered) < _stream_id_tuple(through_id):
            raise DeliveryPendingError(
                "consumer group has not acknowledged delivery through the cutoff"
            )

        raw_entries = self._redis_call(
            "query trim candidates",
            "xrange",
            self.stream_key,
            min="-",
            max=through_id,
            count=max_entries + 2,
        )
        if raw_entries is None or len(raw_entries) > max_entries + 2:
            raise OutboxRecordError(
                "Redis returned an invalid bounded trim-candidate range"
            )
        entries = [self._decode_entry(raw) for raw in raw_entries]
        # Retain the newest stream entry as a durable sequence/head anchor.
        # The Lua producer cross-checks it against the meta hash before every
        # RBAC commit, so losing either side cannot silently restart at 1.
        newest = self.query(count=1, newest_first=True)
        newest_id = newest[0].stream_id if newest else None
        entries = [entry for entry in entries if entry.stream_id != newest_id]
        if len(entries) > max_entries:
            raise DeliveryPendingError(
                f"more than {max_entries} entries are eligible; trim in a smaller verified batch"
            )
        if not entries:
            return 0
        fields = [self._entry_field(entry.stream_id) for entry in entries]
        markers = self._redis_call(
            "read delivery markers", "hmget", self.delivery_ledger_key, fields
        )
        if markers is None or len(markers) != len(fields):
            raise OutboxRecordError("HMGET returned an invalid delivery-marker result")
        missing = [entry.stream_id for entry, marker in zip(entries, markers) if marker is None]
        if missing:
            raise DeliveryPendingError(
                "archive delivery markers are missing for stream IDs: " + ", ".join(missing)
            )
        cascade_claims: Dict[str, Dict[str, str]] = {}
        verified_batches: Dict[str, Tuple[OutboxEntry, ...]] = {}
        verified_cascades: Dict[str, Dict[str, CascadeManifest]] = {}
        selected_ids = {entry.stream_id for entry in entries}
        batch_records: Dict[str, Mapping[str, Any]] = {}
        batch_raws: Dict[str, str] = {}
        for entry, raw_marker in zip(entries, markers):
            batch_id = _text(raw_marker, field="archive delivery marker")
            archived_entries = verified_batches.get(batch_id)
            if archived_entries is None:
                archived_entries = self.read_archive_batch(batch_id)
                verified_batches[batch_id] = archived_entries
                raw_batch = self._ledger_get(self._batch_field(batch_id))
                if raw_batch is None:
                    raise DeliveryPendingError(
                        "archive batch metadata disappeared before source trim"
                    )
                batch_record = self._decode_ledger(raw_batch)
                if batch_record.get("status") != "delivered":
                    raise DeliveryPendingError(
                        "archive batch is not durably delivered"
                    )
                archive_metadata = batch_record.get("archive")
                if not isinstance(archive_metadata, Mapping):
                    raise OutboxRecordError(
                        "delivered batch has no archive metadata"
                    )
                batch_records[batch_id] = batch_record
                batch_raws[batch_id] = raw_batch
                verified_cascades[batch_id] = {
                    manifest.event_id: manifest
                    for manifest in self._read_verified_cascade_archive(
                        archive_metadata, archived_entries,
                    )
                }
            if not any(
                self._same_entry(archived, entry)
                for archived in archived_entries
            ):
                raise DeliveryPendingError(
                    "source entry is absent from its exact verified archive batch"
                )
            manifest_id = entry.event.get("cascade_manifest_id")
            if manifest_id:
                archived_manifest = verified_cascades[batch_id].get(
                    str(entry.event["event_id"]),
                )
                live_manifest = self.get_cascade_manifest(entry)
                if live_manifest is None or live_manifest != archived_manifest:
                    raise DeliveryPendingError(
                        "live cascade manifest differs from its archived evidence"
                    )
                cascade_key = self._cascade_key(
                    str(entry.event["organization"]), str(manifest_id),
                )
                canonical_fields = self._cascade_manifest_fields(live_manifest)
                previous_claim = cascade_claims.setdefault(
                    cascade_key, canonical_fields,
                )
                if previous_claim != canonical_fields:
                    raise OutboxRecordError(
                        "cascade manifest key has conflicting verified contents"
                    )

        gc_expected: Dict[str, str] = {
            field: _text(marker, field="archive delivery marker")
            for field, marker in zip(fields, markers)
        }
        for batch_id, batch_record in batch_records.items():
            membership = batch_record.get("stream_ids")
            if not isinstance(membership, list) or not membership:
                raise OutboxRecordError("delivered batch membership is invalid")
            batch_fully_trimmed = True
            for stream_id in membership:
                if stream_id in selected_ids:
                    continue
                remaining = self._redis_call(
                    "check archive batch source retention",
                    "xrange",
                    self.stream_key,
                    min=stream_id,
                    max=stream_id,
                    count=1,
                )
                if remaining:
                    batch_fully_trimmed = False
                    break
            if batch_fully_trimmed:
                try:
                    first_sequence = int(batch_record["first_sequence"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise OutboxRecordError(
                        "delivered batch first sequence is invalid"
                    ) from None
                gc_expected[self._sequence_claim_field(first_sequence)] = batch_id
                gc_expected[self._batch_field(batch_id)] = batch_raws[batch_id]
        unique_gc_fields = tuple(gc_expected)
        unique_cascade_keys = tuple(cascade_claims)
        trim_keys = (
            self.stream_key,
            self.delivery_ledger_key,
            *unique_cascade_keys,
        )
        result = self._redis_call(
            "atomically trim delivered stream entries and cascade manifests",
            "eval",
            self._TRIM_DELIVERED_LUA,
            len(trim_keys),
            *trim_keys,
            str(len(entries)),
            str(len(unique_gc_fields)),
            *(entry.stream_id for entry in entries),
            *(
                value
                for field in unique_gc_fields
                for value in (field, gc_expected[field])
            ),
            *(
                value
                for key in unique_cascade_keys
                for value in (
                    str(len(cascade_claims[key])),
                    *(
                        pair_value
                        for field, expected in sorted(
                            cascade_claims[key].items()
                        )
                        for pair_value in (field, expected)
                    ),
                )
            ),
        )
        if not isinstance(result, (tuple, list)) or len(result) != 3:
            raise OutboxRecordError("privileged trim Lua returned an invalid result")
        try:
            removed_entries = int(result[0])
            removed_manifests = int(result[1])
            removed_metadata = int(result[2])
        except (TypeError, ValueError) as exc:
            raise OutboxRecordError(
                "privileged trim Lua returned non-integer counts"
            ) from None
        if (
            removed_entries != len(entries)
            or removed_manifests != len(unique_cascade_keys)
            or removed_metadata != len(unique_gc_fields)
        ):
            raise OutboxRecordError(
                "privileged trim Lua did not remove every verified artifact"
            )
        return removed_entries


__all__ = [
    "ArchiveBatchResult",
    "ArchiveVerificationError",
    "AutoClaimResult",
    "CascadeEvidenceRow",
    "CascadeManifest",
    "DeliveryPendingError",
    "OutboxBackendError",
    "OutboxEntry",
    "OutboxHealth",
    "OutboxRecordError",
    "PrivilegedAuditOutbox",
    "PrivilegedOutboxError",
]
