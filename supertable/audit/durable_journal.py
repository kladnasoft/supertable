"""Durable general-audit admission, archival coordination, and day closure.

Hash-chain proofs may only cover events admitted through this journal.  Redis
TIME owns membership in an epoch day, accepted records remain recoverable after
an emitter crash, and a day cannot enter ``closing`` until every admitted event
has an exact durable archive receipt and no reservation remains in flight.

The coordination scripts intentionally target the same standalone/Sentinel
Redis deployment as the privileged audit ledger.  Dynamic per-day keys cannot
be made Redis-Cluster atomic without changing the repository-wide key layout.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from supertable import redis_keys as RK
from supertable.audit.chain import (
    GENESIS_HASH,
    InstanceChain,
    MerkleProof,
    compute_event_batch_hash,
)
from supertable.audit.diagnostics import safe_audit_error_type
from supertable.audit.events import AuditEvent


_DAY_MS = 86_400_000
_MAX_ACTIVE_DAYS = 31
_MAX_DAY_EVENTS = 250_000
_MAX_DAY_EVENT_BYTES = 512 * 1024 * 1024
_MAX_DAY_ARCHIVE_BYTES = 512 * 1024 * 1024
_MAX_DAY_RECEIPTS = 8_192
_MAX_DAY_INSTANCES = 4_096
_MAX_RESERVATION_EVENTS = 1_000
_MAX_RESERVATION_BYTES = 8 * 1024 * 1024
_MAX_EVENT_BYTES = 96 * 1024
_MAX_EVENT_FIELD_BYTES = 64 * 1024
_MAX_RECEIPT_BYTES = 32 * 1024
_MAX_PROOF_BYTES = 1024 * 1024
_MAX_CLOSE_MANIFEST_BYTES = 1024 * 1024
_DEFAULT_RESERVATION_LEASE_MS = 300_000
_DEFAULT_CLOSE_GRACE_MS = 300_000
_MAX_EVENT_ID_CLOCK_SKEW_MS = 10 * 60 * 1000
_EVENT_ID_RE = re.compile(r"[0-9a-f]{12}-[0-9a-f]{4}-[0-9a-f]{8}")
_STREAM_ID_RE = re.compile(r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})")
_HASH_RE = re.compile(r"[0-9a-f]{64}")
_AUDIT_ERROR_CODE_RE = re.compile(
    r"(?<![A-Z0-9_])(AUDIT_[A-Z0-9_]{1,63})(?![A-Z0-9_])"
)
_MAX_BACKEND_ERROR_ARG_BYTES = 4096


def _safe_error_type(exc: BaseException) -> str:
    return safe_audit_error_type(exc)


def _audit_error_codes(exc: BaseException) -> frozenset[str]:
    """Extract only bounded Lua status codes from backend exception arguments."""

    try:
        arguments = exc.args
    except Exception:
        return frozenset()
    if not isinstance(arguments, tuple):
        return frozenset()
    codes: set[str] = set()
    for value in arguments[:4]:
        if isinstance(value, bytes):
            if len(value) > _MAX_BACKEND_ERROR_ARG_BYTES:
                continue
            text = value.decode("ascii", errors="ignore")
        elif isinstance(value, str):
            if len(value.encode("utf-8", errors="ignore")) > _MAX_BACKEND_ERROR_ARG_BYTES:
                continue
            text = value
        else:
            continue
        codes.update(_AUDIT_ERROR_CODE_RE.findall(text))
    return frozenset(codes)


class AuditJournalError(RuntimeError):
    """A durable journal operation failed without a complete result."""


class AuditJournalLimitError(AuditJournalError):
    """A hard journal resource ceiling was reached."""


class AuditJournalCollisionError(AuditJournalError):
    """Immutable journal or closure evidence disagreed with an existing value."""


class AuditDayOpenError(AuditJournalError):
    """A proof was requested before durable day membership was closed."""


class AuditJournalConfigurationError(AuditJournalError):
    """The Redis/storage topology cannot provide atomic journal semantics."""


@dataclass(frozen=True)
class JournalAdmission:
    journal_id: str
    day: int
    event: AuditEvent
    duplicate: bool = False


@dataclass(frozen=True)
class JournalReservation:
    token: str
    day: int
    instance_id: str
    journal_ids: tuple[str, ...]
    list_entries: tuple[str, ...]
    events: tuple[AuditEvent, ...]
    previous_head: str
    previous_batch_count: int
    expires_ms: int

    @property
    def batch_id(self) -> str:
        payload = {
            "day": self.day,
            "instance_id": self.instance_id,
            "journal_ids": list(self.journal_ids),
            "previous_head": self.previous_head,
            "previous_batch_count": self.previous_batch_count,
            "version": 1,
        }
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class JournalArchiveReceipt:
    batch_id: str
    day: int
    instance_id: str
    journal_ids: tuple[str, ...]
    previous_head: str
    chain_head: str
    previous_batch_count: int
    batch_count: int
    event_count: int
    path: str
    file_hash: str
    bytes_written: int
    publication_id: str
    min_timestamp_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_count": self.batch_count,
            "batch_id": self.batch_id,
            "bytes_written": self.bytes_written,
            "chain_head": self.chain_head,
            "day": self.day,
            "event_count": self.event_count,
            "file_hash": self.file_hash,
            "instance_id": self.instance_id,
            "journal_ids": list(self.journal_ids),
            "min_timestamp_ms": self.min_timestamp_ms,
            "path": self.path,
            "previous_batch_count": self.previous_batch_count,
            "previous_head": self.previous_head,
            "publication_id": self.publication_id,
            "version": 1,
        }

    def to_json(self) -> str:
        raw = _canonical_json(self.to_dict())
        if len(raw.encode("utf-8")) > _MAX_RECEIPT_BYTES:
            raise AuditJournalLimitError("audit archive receipt is oversized")
        return raw

    @classmethod
    def from_json(cls, raw: Any, *, organization: str) -> "JournalArchiveReceipt":
        text = _text(raw, maximum_bytes=_MAX_RECEIPT_BYTES)
        try:
            value = json.loads(text)
        except (TypeError, ValueError) as exc:
            raise AuditJournalError(
                "audit archive receipt is invalid JSON"
            ) from None
        if not isinstance(value, dict) or set(value) != {
            "batch_count", "batch_id", "bytes_written", "chain_head", "day",
            "event_count", "file_hash", "instance_id", "journal_ids",
            "min_timestamp_ms", "path", "previous_batch_count", "previous_head",
            "publication_id", "version",
        }:
            raise AuditJournalError("audit archive receipt schema is invalid")
        if value.get("version") != 1:
            raise AuditJournalError("audit archive receipt version is invalid")
        instance_id = value.get("instance_id")
        if not isinstance(instance_id, str):
            raise AuditJournalError(
                "audit archive receipt instance identity is invalid"
            )
        RK.audit_chain_head(organization, instance_id)
        journal_ids = value.get("journal_ids")
        if (
            not isinstance(journal_ids, list)
            or not journal_ids
            or len(journal_ids) > _MAX_RESERVATION_EVENTS
            or any(not _is_stream_id(item) for item in journal_ids)
            or len(set(journal_ids)) != len(journal_ids)
        ):
            raise AuditJournalError("audit archive receipt membership is invalid")
        for name in ("batch_id", "file_hash", "publication_id", "chain_head", "previous_head"):
            if not isinstance(value.get(name), str) or not _HASH_RE.fullmatch(value[name]):
                raise AuditJournalError("audit archive receipt hash is invalid")
        for name, minimum, maximum in (
            ("day", 0, 4_000_000),
            ("event_count", 1, _MAX_RESERVATION_EVENTS),
            ("bytes_written", 1, _MAX_DAY_ARCHIVE_BYTES),
            ("previous_batch_count", 0, (1 << 63) - 1),
            ("batch_count", 1, (1 << 63) - 1),
            ("min_timestamp_ms", 0, (1 << 63) - 1),
        ):
            item = value.get(name)
            if type(item) is not int or not minimum <= item <= maximum:
                raise AuditJournalError("audit archive receipt counter is invalid")
        if value["event_count"] != len(journal_ids):
            raise AuditJournalError("audit archive receipt event count is inconsistent")
        if value["batch_count"] != value["previous_batch_count"] + 1:
            raise AuditJournalError("audit archive receipt batch count is inconsistent")
        expected_prefix = f"{organization}/__audit__/"
        path = value.get("path")
        if (
            not isinstance(path, str)
            or not path.startswith(expected_prefix)
            or len(path.encode("utf-8")) > 2_048
        ):
            raise AuditJournalError("audit archive receipt path is invalid")
        result = cls(
            batch_id=value["batch_id"],
            day=value["day"],
            instance_id=instance_id,
            journal_ids=tuple(
                item for item in journal_ids if isinstance(item, str)
            ),
            previous_head=value["previous_head"],
            chain_head=value["chain_head"],
            previous_batch_count=value["previous_batch_count"],
            batch_count=value["batch_count"],
            event_count=value["event_count"],
            path=path,
            file_hash=value["file_hash"],
            bytes_written=value["bytes_written"],
            publication_id=value["publication_id"],
            min_timestamp_ms=value["min_timestamp_ms"],
        )
        if result.to_json() != text:
            raise AuditJournalError("audit archive receipt is not canonical")
        return result


@dataclass(frozen=True)
class DayCloseLease:
    day: int
    token: str
    requested_ms: int
    admitted: int
    admitted_bytes: int
    receipts: int
    archive_bytes: int


_ADMIT_LUA = r"""
local now = redis.call('TIME')
local seconds = tonumber(now[1])
local milliseconds = seconds * 1000 + math.floor(tonumber(now[2]) / 1000)
local day = math.floor(seconds / 86400)
local base = ARGV[1]
local event_id = ARGV[2]
local instance_id = ARGV[3]
local template = ARGV[4]
local event_size = tonumber(ARGV[5])
local event_id_ms = tonumber(ARGV[6])
local existing = redis.call('HGET', base .. ':event-index', event_id)
if existing then
    local first = string.find(existing, '|', 1, true)
    local second = string.find(existing, '|', first + 1, true)
    local third = second and string.find(existing, '|', second + 1, true)
    if not first or not second or not third then
        return redis.error_reply('AUDIT_EVENT_INDEX')
    end
    local stream_id = string.sub(existing, 1, first - 1)
    local timestamp_ms = string.sub(existing, first + 1, second - 1)
    local existing_day = string.sub(existing, second + 1, third - 1)
    local existing_instance = string.sub(existing, third + 1)
    local existing_payload = redis.call('HGET', base .. ':event-data', stream_id)
    if existing_instance ~= instance_id or existing_payload ~= timestamp_ms .. '\n' .. template then
        return redis.error_reply('AUDIT_EVENT_COLLISION')
    end
    return {stream_id, timestamp_ms, existing_day, '1'}
end
if math.abs(milliseconds - event_id_ms) > tonumber(ARGV[11]) then
    return redis.error_reply('AUDIT_EVENT_ID_TIME')
end
local state_key = base .. ':day:' .. tostring(day) .. ':state'
local status = redis.call('HGET', state_key, 'status')
if status and status ~= 'open' then
    return redis.error_reply('AUDIT_DAY_NOT_OPEN')
end
local active_key = base .. ':active-days'
if not redis.call('ZSCORE', active_key, tostring(day)) then
    if redis.call('ZCARD', active_key) >= tonumber(ARGV[9]) then
        return redis.error_reply('AUDIT_ACTIVE_DAY_LIMIT')
    end
    redis.call('ZADD', active_key, day, tostring(day))
end
local admitted = tonumber(redis.call('HGET', state_key, 'admitted') or '0')
local admitted_bytes = tonumber(redis.call('HGET', state_key, 'admitted_bytes') or '0')
local archived = tonumber(redis.call('HGET', state_key, 'archived') or '0')
local receipts = tonumber(redis.call('HGET', state_key, 'receipts') or '0')
if admitted >= tonumber(ARGV[7]) then
    return redis.error_reply('AUDIT_DAY_EVENT_LIMIT')
end
if admitted_bytes + event_size > tonumber(ARGV[8]) then
    return redis.error_reply('AUDIT_DAY_BYTE_LIMIT')
end
-- Treat every accepted-but-unarchived event as a possible one-row immutable
-- object. This conservative bound ensures accepted work can never require
-- more close-time files than the sealed verifier is willing to inspect.
if receipts + admitted - archived >= tonumber(ARGV[12]) then
    return redis.error_reply('AUDIT_RECEIPT_BACKPRESSURE_LIMIT')
end
local instances_key = base .. ':day:' .. tostring(day) .. ':instances'
if redis.call('SISMEMBER', instances_key, instance_id) == 0 then
    if redis.call('SCARD', instances_key) >= tonumber(ARGV[10]) then
        return redis.error_reply('AUDIT_DAY_INSTANCE_LIMIT')
    end
    redis.call('SADD', instances_key, instance_id)
end
local stream_id = redis.call(
    'XADD', base .. ':events', '*',
    'event_id', event_id,
    'instance_id', instance_id,
    'timestamp_ms', tostring(milliseconds),
    'day', tostring(day),
    'size_bytes', tostring(event_size)
)
redis.call('HSET', base .. ':event-data', stream_id, tostring(milliseconds) .. '\n' .. template)
redis.call('RPUSH', base .. ':day:' .. tostring(day) .. ':pending:' .. instance_id, stream_id .. '|' .. tostring(event_size))
redis.call('HSET', base .. ':event-index', event_id, stream_id .. '|' .. tostring(milliseconds) .. '|' .. tostring(day) .. '|' .. instance_id)
redis.call('HSET', state_key, 'status', 'open')
redis.call('HINCRBY', state_key, 'admitted', 1)
redis.call('HINCRBY', state_key, 'admitted_bytes', event_size)
redis.call('HSETNX', state_key, 'archived', '0')
redis.call('HSETNX', state_key, 'inflight', '0')
redis.call('HSETNX', state_key, 'receipts', '0')
redis.call('HSETNX', state_key, 'archive_bytes', '0')
redis.call('HSETNX', base .. ':meta', 'start_day', tostring(day))
return {stream_id, tostring(milliseconds), tostring(day), '0'}
"""


_ACTIVATE_LUA = r"""
local base = ARGV[1]
local expected_version = ARGV[2]
local existing = redis.call('HGET', base .. ':meta', 'format_version')
if existing and existing ~= expected_version then
    return redis.error_reply('AUDIT_FORMAT_VERSION')
end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local day = math.floor(tonumber(now[1]) / 86400)
redis.call('HSETNX', base .. ':meta', 'format_version', expected_version)
redis.call('HSETNX', base .. ':meta', 'cutover_day', tostring(day))
redis.call('HSETNX', base .. ':meta', 'cutover_ms', tostring(now_ms))
return {
    redis.call('HGET', base .. ':meta', 'format_version'),
    redis.call('HGET', base .. ':meta', 'cutover_day'),
    redis.call('HGET', base .. ':meta', 'cutover_ms')
}
"""


_CLAIM_LUA = r"""
local base = ARGV[1]
local day = ARGV[2]
local instance_id = ARGV[3]
local requested_token = ARGV[4]
local max_events = tonumber(ARGV[5])
local max_bytes = tonumber(ARGV[6])
local lease_ms = tonumber(ARGV[7])
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local reservation_key = base .. ':reservation:' .. instance_id
if redis.call('EXISTS', reservation_key) == 1 then
    local reserved_day = redis.call('HGET', reservation_key, 'day')
    if reserved_day ~= day then
        return {'BLOCKED'}
    end
    local owner = redis.call('HGET', reservation_key, 'token')
    local expires = tonumber(redis.call('HGET', reservation_key, 'expires_ms') or '0')
    if owner ~= requested_token and expires > now_ms then
        return {'BLOCKED'}
    end
    if owner ~= requested_token then
        redis.call('HSET', reservation_key, 'token', requested_token)
        redis.call('HSET', reservation_key, 'expires_ms', tostring(now_ms + lease_ms))
    end
    local ids = redis.call('LRANGE', reservation_key .. ':entries', 0, -1)
    local checkpoint = redis.call('HMGET', base .. ':checkpoint:' .. instance_id, 'head', 'batch_count')
    local result = {'OK', requested_token, tostring(now_ms + lease_ms), checkpoint[1] or ARGV[8], checkpoint[2] or '0'}
    for _, value in ipairs(ids) do table.insert(result, value) end
    return result
end
local pending_key = base .. ':day:' .. day .. ':pending:' .. instance_id
local candidates = redis.call('LRANGE', pending_key, 0, max_events - 1)
if #candidates == 0 then
    return {'EMPTY'}
end
local selected = {}
local selected_bytes = 0
for _, value in ipairs(candidates) do
    local separator = string.find(value, '|', 1, true)
    if not separator then return redis.error_reply('AUDIT_PENDING_ENTRY') end
    local size = tonumber(string.sub(value, separator + 1))
    if not size or size <= 0 then return redis.error_reply('AUDIT_PENDING_SIZE') end
    if #selected > 0 and selected_bytes + size > max_bytes then break end
    if size > max_bytes then return redis.error_reply('AUDIT_PENDING_SIZE') end
    table.insert(selected, value)
    selected_bytes = selected_bytes + size
end
redis.call('HSET', reservation_key, 'token', requested_token, 'day', day, 'expires_ms', tostring(now_ms + lease_ms), 'count', tostring(#selected))
for _, value in ipairs(selected) do redis.call('RPUSH', reservation_key .. ':entries', value) end
redis.call('HINCRBY', base .. ':day:' .. day .. ':state', 'inflight', #selected)
local checkpoint = redis.call('HMGET', base .. ':checkpoint:' .. instance_id, 'head', 'batch_count')
local result = {'OK', requested_token, tostring(now_ms + lease_ms), checkpoint[1] or ARGV[8], checkpoint[2] or '0'}
for _, value in ipairs(selected) do table.insert(result, value) end
return result
"""


_COMPLETE_LUA = r"""
local base = ARGV[1]
local day = ARGV[2]
local instance_id = ARGV[3]
local token = ARGV[4]
local batch_id = ARGV[5]
local receipt = ARGV[6]
local previous_head = ARGV[7]
local previous_count = ARGV[8]
local new_head = ARGV[9]
local new_count = ARGV[10]
local event_count = tonumber(ARGV[11])
local archive_bytes = tonumber(ARGV[12])
local receipt_key = base .. ':day:' .. day .. ':receipts'
local existing = redis.call('HGET', receipt_key, batch_id)
if existing then
    if existing ~= receipt then return redis.error_reply('AUDIT_RECEIPT_COLLISION') end
    return 2
end
local reservation_key = base .. ':reservation:' .. instance_id
if redis.call('HGET', reservation_key, 'token') ~= token or redis.call('HGET', reservation_key, 'day') ~= day then
    return redis.error_reply('AUDIT_RESERVATION_LOST')
end
local reserved = redis.call('LRANGE', reservation_key .. ':entries', 0, -1)
if #reserved ~= event_count then return redis.error_reply('AUDIT_RESERVATION_COUNT') end
local pending_key = base .. ':day:' .. day .. ':pending:' .. instance_id
local pending = redis.call('LRANGE', pending_key, 0, event_count - 1)
if #pending ~= #reserved then return redis.error_reply('AUDIT_PENDING_COUNT') end
for index, value in ipairs(reserved) do
    if pending[index] ~= value then return redis.error_reply('AUDIT_PENDING_ORDER') end
end
local checkpoint_key = base .. ':checkpoint:' .. instance_id
local checkpoint = redis.call('HMGET', checkpoint_key, 'head', 'batch_count')
if (checkpoint[1] or ARGV[13]) ~= previous_head or (checkpoint[2] or '0') ~= previous_count then
    return redis.error_reply('AUDIT_CHAIN_CAS')
end
local state_key = base .. ':day:' .. day .. ':state'
if redis.call('HGET', state_key, 'status') ~= 'open' then return redis.error_reply('AUDIT_DAY_NOT_OPEN') end
local receipts = tonumber(redis.call('HGET', state_key, 'receipts') or '0')
local total_archive_bytes = tonumber(redis.call('HGET', state_key, 'archive_bytes') or '0')
if receipts >= tonumber(ARGV[14]) then return redis.error_reply('AUDIT_RECEIPT_LIMIT') end
if total_archive_bytes + archive_bytes > tonumber(ARGV[15]) then return redis.error_reply('AUDIT_ARCHIVE_BYTE_LIMIT') end
redis.call('HSET', receipt_key, batch_id, receipt)
redis.call('HSET', checkpoint_key, 'head', new_head, 'batch_count', new_count)
redis.call('LTRIM', pending_key, event_count, -1)
for _, value in ipairs(reserved) do
    local separator = string.find(value, '|', 1, true)
    local journal_id = string.sub(value, 1, separator - 1)
    redis.call('HSET', base .. ':archived-events', journal_id, batch_id)
end
redis.call('HINCRBY', state_key, 'archived', event_count)
redis.call('HINCRBY', state_key, 'inflight', -event_count)
redis.call('HINCRBY', state_key, 'receipts', 1)
redis.call('HINCRBY', state_key, 'archive_bytes', archive_bytes)
redis.call('DEL', reservation_key)
redis.call('DEL', reservation_key .. ':entries')
return 1
"""


_BEGIN_CLOSE_LUA = r"""
local base = ARGV[1]
local day = tonumber(ARGV[2])
local requested_token = ARGV[3]
local grace_ms = tonumber(ARGV[4])
local cutover_day = tonumber(redis.call('HGET', base .. ':meta', 'cutover_day') or '-1')
local last_closed = tonumber(redis.call('HGET', base .. ':meta', 'last_closed_day') or '-1')
if day < cutover_day or day <= last_closed then return {'CLOSED'} end
local expected_day = cutover_day
if last_closed >= 0 then expected_day = last_closed + 1 end
if day ~= expected_day then return {'GAP'} end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if now_ms < (day + 1) * 86400000 + grace_ms then return {'OPEN'} end
local state_key = base .. ':day:' .. tostring(day) .. ':state'
local status = redis.call('HGET', state_key, 'status')
if not status then
    redis.call('HSET', state_key, 'status', 'open', 'admitted', '0', 'admitted_bytes', '0', 'archived', '0', 'inflight', '0', 'receipts', '0', 'archive_bytes', '0')
    redis.call('ZADD', base .. ':active-days', day, tostring(day))
    status = 'open'
end
if status == 'closed' then return {'CLOSED'} end
if status == 'closing' then
    return {'OK', redis.call('HGET', state_key, 'close_token'), redis.call('HGET', state_key, 'close_requested_ms'), redis.call('HGET', state_key, 'admitted'), redis.call('HGET', state_key, 'admitted_bytes'), redis.call('HGET', state_key, 'receipts'), redis.call('HGET', state_key, 'archive_bytes')}
end
local values = redis.call('HMGET', state_key, 'admitted', 'admitted_bytes', 'archived', 'inflight', 'receipts', 'archive_bytes')
if values[1] ~= values[3] or tonumber(values[4]) ~= 0 then return {'PENDING'} end
redis.call('HSET', state_key, 'status', 'closing', 'close_token', requested_token, 'close_requested_ms', tostring(now_ms))
return {'OK', requested_token, tostring(now_ms), values[1], values[2], values[5], values[6]}
"""


_FINALIZE_CLOSE_LUA = r"""
local base = ARGV[1]
local day = ARGV[2]
local token = ARGV[3]
local proof_hash = ARGV[4]
local manifest_hash = ARGV[5]
local state_key = base .. ':day:' .. day .. ':state'
local status = redis.call('HGET', state_key, 'status')
if status == 'closed' then
    if redis.call('HGET', state_key, 'proof_hash') ~= proof_hash or redis.call('HGET', state_key, 'manifest_hash') ~= manifest_hash then
        return redis.error_reply('AUDIT_CLOSE_COLLISION')
    end
    return 2
end
if status ~= 'closing' or redis.call('HGET', state_key, 'close_token') ~= token then
    return redis.error_reply('AUDIT_CLOSE_LEASE')
end
local admitted = redis.call('HGET', state_key, 'admitted')
local archived = redis.call('HGET', state_key, 'archived')
local inflight = tonumber(redis.call('HGET', state_key, 'inflight') or '-1')
if admitted ~= archived or inflight ~= 0 then return redis.error_reply('AUDIT_CLOSE_RACE') end
redis.call('HSET', state_key, 'status', 'closed', 'proof_hash', proof_hash, 'manifest_hash', manifest_hash)
redis.call('HSET', base .. ':meta', 'last_closed_day', day, 'last_closed_proof_hash', proof_hash, 'cleanup_day', day)
redis.call('ZREM', base .. ':active-days', day)
return 1
"""


_CLEANUP_CHUNK_LUA = r"""
local base = ARGV[1]
local day = ARGV[2]
local state_key = base .. ':day:' .. day .. ':state'
if redis.call('HGET', state_key, 'status') ~= 'closed' then
    return redis.error_reply('AUDIT_CLEANUP_NOT_CLOSED')
end
local removed = 0
for index = 3, #ARGV do
    local journal_id = ARGV[index]
    local marker = redis.call('HGET', base .. ':archived-events', journal_id)
    if marker then
        local entries = redis.call('XRANGE', base .. ':events', journal_id, journal_id, 'COUNT', 1)
        if #entries ~= 1 then return redis.error_reply('AUDIT_CLEANUP_STREAM_GAP') end
        local fields = entries[1][2]
        local event_id = nil
        local event_day = nil
        for field_index = 1, #fields, 2 do
            if fields[field_index] == 'event_id' then event_id = fields[field_index + 1] end
            if fields[field_index] == 'day' then event_day = fields[field_index + 1] end
        end
        if not event_id or event_day ~= day then return redis.error_reply('AUDIT_CLEANUP_IDENTITY') end
        redis.call('HDEL', base .. ':event-index', event_id)
        redis.call('XDEL', base .. ':events', journal_id)
        redis.call('HDEL', base .. ':archived-events', journal_id)
        redis.call('HDEL', base .. ':event-data', journal_id)
        removed = removed + 1
    end
end
return removed
"""


_FINISH_CLEANUP_LUA = r"""
local base = ARGV[1]
local day = ARGV[2]
local state_key = base .. ':day:' .. day .. ':state'
if redis.call('HGET', state_key, 'status') ~= 'closed' then
    return redis.error_reply('AUDIT_CLEANUP_NOT_CLOSED')
end
local instances_key = base .. ':day:' .. day .. ':instances'
local instances = redis.call('SMEMBERS', instances_key)
if #instances > tonumber(ARGV[3]) then return redis.error_reply('AUDIT_DAY_INSTANCE_LIMIT') end
for _, instance_id in ipairs(instances) do
    redis.call('DEL', base .. ':day:' .. day .. ':pending:' .. instance_id)
end
redis.call('DEL', instances_key)
redis.call('DEL', base .. ':day:' .. day .. ':receipts')
redis.call('DEL', state_key)
redis.call('HSET', base .. ':meta', 'last_cleaned_day', day)
if redis.call('HGET', base .. ':meta', 'cleanup_day') == day then
    redis.call('HDEL', base .. ':meta', 'cleanup_day')
end
return 1
"""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _text(value: Any, *, maximum_bytes: int = 16_384) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            raise AuditJournalError("audit journal text is invalid") from None
    if not isinstance(value, str):
        raise AuditJournalError("audit journal text is invalid")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise AuditJournalError("audit journal text is invalid") from None
    if size > maximum_bytes:
        raise AuditJournalLimitError("audit journal text is oversized")
    return value


def _integer(value: Any, *, minimum: int = 0, maximum: int = (1 << 63) - 1) -> int:
    text = _text(value, maximum_bytes=32)
    if not text.isascii() or not text.isdigit():
        raise AuditJournalError("audit journal integer is invalid")
    result = int(text)
    if not minimum <= result <= maximum or str(result) != text:
        raise AuditJournalError("audit journal integer is invalid")
    return result


def _is_stream_id(value: Any) -> bool:
    if not isinstance(value, str) or not _STREAM_ID_RE.fullmatch(value):
        return False
    first, second = value.split("-", 1)
    return int(first) <= (1 << 64) - 1 and int(second) <= (1 << 64) - 1


def _event_template(event: AuditEvent) -> tuple[str, int, int]:
    if not _EVENT_ID_RE.fullmatch(event.event_id):
        raise ValueError("durable audit event ID is invalid")
    if event.chain_hash:
        raise ValueError("durable audit admission cannot accept a chain hash")
    data = event.to_dict()
    data["timestamp_ms"] = 0
    raw = _canonical_json(data)
    encoded = raw.encode("utf-8")
    if len(encoded) > _MAX_EVENT_BYTES:
        raise AuditJournalLimitError("durable audit event is oversized")
    for name, value in data.items():
        if name == "timestamp_ms":
            continue
        if not isinstance(value, str) or len(value.encode("utf-8")) > _MAX_EVENT_FIELD_BYTES:
            raise ValueError("durable audit event field is invalid")
    return raw, len(encoded), int(event.event_id.split("-", 1)[0], 16)


def _decode_event_payload(
    value: Any,
    *,
    organization: str,
    instance_id: str,
    journal_id: str,
    day: int,
) -> AuditEvent:
    text = _text(value, maximum_bytes=_MAX_EVENT_BYTES + 32)
    timestamp_text, separator, template = text.partition("\n")
    if not separator:
        raise AuditJournalError("durable audit payload envelope is invalid")
    timestamp_ms = _integer(timestamp_text)
    if timestamp_ms // _DAY_MS != day:
        raise AuditJournalError("durable audit payload day is inconsistent")
    try:
        data = json.loads(template)
    except (TypeError, ValueError) as exc:
        raise AuditJournalError("durable audit payload is invalid JSON") from None
    expected = set(AuditEvent.__dataclass_fields__)
    if not isinstance(data, dict) or set(data) != expected:
        raise AuditJournalError("durable audit payload schema is invalid")
    if data.get("timestamp_ms") != 0 or data.get("chain_hash") != "":
        raise AuditJournalError("durable audit payload integrity fields are invalid")
    data["timestamp_ms"] = timestamp_ms
    try:
        event = AuditEvent(**data)
    except (TypeError, ValueError) as exc:
        raise AuditJournalError("durable audit payload cannot be decoded") from None
    if event.organization != organization or event.instance_id != instance_id:
        raise AuditJournalError("durable audit payload identity is inconsistent")
    if not _EVENT_ID_RE.fullmatch(event.event_id) or not _is_stream_id(journal_id):
        raise AuditJournalError("durable audit payload ID is invalid")
    canonical, _, _ = _event_template(event)
    if canonical != template:
        raise AuditJournalError("durable audit payload is not canonical")
    return event


class RedisAuditJournal:
    """Redis-coordinated durable admission and exact archive reservations."""

    def __init__(self, redis_client: Any, organization: str):
        RK.audit_stream(organization)
        self._validate_topology(redis_client)
        self._redis = redis_client
        self.organization = organization
        self.base_key = f"{RK.audit_stream(organization)}:durable:v1"
        response = self._eval(
            "format activation", _ACTIVATE_LUA, self.base_key, "1",
        )
        if not isinstance(response, (list, tuple)) or len(response) != 3:
            raise AuditJournalConfigurationError(
                "durable audit format marker is invalid"
            )
        if _text(response[0], maximum_bytes=8) != "1":
            raise AuditJournalConfigurationError(
                "durable audit format version is unsupported"
            )
        self.cutover_day = _integer(response[1], maximum=4_000_000)
        self.cutover_ms = _integer(response[2])

    def _eval(self, operation: str, script: str, *args: Any) -> Any:
        try:
            return self._redis.eval(script, 0, *args)
        except Exception as exc:
            name = _safe_error_type(exc)
            codes = _audit_error_codes(exc)
            if "AUDIT_FORMAT_VERSION" in codes:
                raise AuditJournalConfigurationError(
                    "durable audit format version is unsupported"
                ) from None
            if any(code.endswith("_COLLISION") for code in codes):
                raise AuditJournalCollisionError(
                    f"durable audit {operation} found conflicting immutable data"
                ) from None
            if any(code.endswith("_LIMIT") for code in codes):
                raise AuditJournalLimitError(
                    f"durable audit {operation} reached a safety limit"
                ) from None
            raise AuditJournalError(
                f"durable audit {operation} failed ({name})"
            ) from None

    @staticmethod
    def _validate_topology(redis_client: Any) -> None:
        redis_type = type(redis_client)
        if (
            "rediscluster" in redis_type.__name__.lower()
            or redis_type.__module__.startswith("redis.cluster")
            or getattr(redis_client, "nodes_manager", None) is not None
        ):
            raise AuditJournalConfigurationError(
                "durable audit journal requires standalone Redis or Sentinel"
            )

    @classmethod
    def inspect_day_state(
        cls,
        redis_client: Any,
        organization: str,
        day: int,
    ) -> Dict[str, str]:
        """Read a day state without activating or mutating journal metadata."""
        RK.audit_stream(organization)
        cls._validate_topology(redis_client)
        if type(day) is not int or not 0 <= day <= 4_000_000:
            raise ValueError("durable audit day is invalid")
        base_key = f"{RK.audit_stream(organization)}:durable:v1"
        try:
            raw = redis_client.hgetall(f"{base_key}:day:{day}:state") or {}
        except Exception as exc:
            raise AuditJournalError(
                "durable audit day-state read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if not isinstance(raw, dict) or len(raw) > 16:
            raise AuditJournalError("durable audit day state is invalid")
        result = {
            _text(key, maximum_bytes=64): _text(value, maximum_bytes=128)
            for key, value in raw.items()
        }
        if result:
            return result
        try:
            meta = redis_client.hmget(
                base_key + ":meta",
                ["format_version", "cutover_day", "last_closed_day"],
            )
        except Exception as exc:
            raise AuditJournalError(
                "durable audit close-head read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if not isinstance(meta, (list, tuple)) or len(meta) != 3:
            raise AuditJournalError("durable audit close-head response is invalid")
        if meta[0] is None:
            return {}
        if _text(meta[0], maximum_bytes=8) != "1":
            raise AuditJournalConfigurationError(
                "durable audit format version is unsupported"
            )
        cutover = _integer(meta[1], maximum=4_000_000)
        if meta[2] is not None:
            last_closed = _integer(meta[2], maximum=4_000_000)
            if cutover <= day <= last_closed:
                return {"status": "closed"}
        return {}

    def admit(self, event: AuditEvent) -> JournalAdmission:
        """Synchronously journal one protected event using Redis-owned time."""
        if event.organization != self.organization:
            raise ValueError("durable audit event organization is inconsistent")
        RK.audit_chain_head(self.organization, event.instance_id)
        template, event_size, event_id_ms = _event_template(event)
        try:
            response = self._eval(
                "admission",
                _ADMIT_LUA,
                self.base_key,
                event.event_id,
                event.instance_id,
                template,
                event_size,
                event_id_ms,
                _MAX_DAY_EVENTS,
                _MAX_DAY_EVENT_BYTES,
                _MAX_ACTIVE_DAYS,
                _MAX_DAY_INSTANCES,
                _MAX_EVENT_ID_CLOCK_SKEW_MS,
                _MAX_DAY_RECEIPTS,
            )
        except AuditJournalError as admission_error:
            # A timeout may follow a successful Lua commit. Reconcile using the
            # same writer-owned event ID; never allocate a replacement ID here.
            try:
                indexed = self._redis.hget(
                    self.base_key + ":event-index", event.event_id,
                )
            except Exception as exc:
                raise AuditJournalError(
                    "durable audit admission could not be reconciled "
                    f"({_safe_error_type(exc)})"
                ) from None
            if indexed is None:
                raise admission_error from None
            index_text = _text(indexed, maximum_bytes=128)
            pieces = index_text.split("|")
            if len(pieces) != 4:
                raise AuditJournalError(
                    "durable audit admission reconciliation is invalid"
                ) from None
            try:
                payload = self._redis.hget(
                    self.base_key + ":event-data", pieces[0],
                )
            except Exception as exc:
                raise AuditJournalError(
                    "durable audit admission could not be reconciled "
                    f"({_safe_error_type(exc)})"
                ) from None
            if (
                pieces[3] != event.instance_id
                or _text(payload, maximum_bytes=_MAX_EVENT_BYTES + 32)
                != pieces[1] + "\n" + template
            ):
                raise AuditJournalCollisionError(
                    "durable audit event ID contains different content"
                ) from None
            response = [pieces[0], pieces[1], pieces[2], "1"]
        if not isinstance(response, (list, tuple)) or len(response) != 4:
            raise AuditJournalError("durable audit admission response is invalid")
        journal_id = _text(response[0], maximum_bytes=64)
        if not _is_stream_id(journal_id):
            raise AuditJournalError("durable audit admission ID is invalid")
        timestamp_ms = _integer(response[1])
        day = _integer(response[2], maximum=4_000_000)
        if timestamp_ms // _DAY_MS != day:
            raise AuditJournalError("durable audit admission day is inconsistent")
        duplicate = _integer(response[3], maximum=1) == 1
        admitted = replace(event, timestamp_ms=timestamp_ms)
        return JournalAdmission(journal_id, day, admitted, duplicate)

    def _active_days(self) -> List[int]:
        try:
            values = self._redis.zrange(self.base_key + ":active-days", 0, _MAX_ACTIVE_DAYS)
        except Exception as exc:
            raise AuditJournalError(
                f"durable audit active-day read failed ({_safe_error_type(exc)})"
            ) from None
        if not isinstance(values, (list, tuple)) or len(values) > _MAX_ACTIVE_DAYS:
            raise AuditJournalLimitError("durable audit active-day index is oversized")
        days = [_integer(value, maximum=4_000_000) for value in values]
        if days != sorted(set(days)):
            raise AuditJournalError("durable audit active-day index is invalid")
        return days

    def active_days(self) -> List[int]:
        """Return the bounded ordered set of journal days not yet closed."""
        return self._active_days()

    def load_checkpoint(self, instance_id: str) -> tuple[str, int]:
        RK.audit_chain_head(self.organization, instance_id)
        try:
            values = self._redis.hmget(
                f"{self.base_key}:checkpoint:{instance_id}",
                ["head", "batch_count"],
            )
        except Exception as exc:
            raise AuditJournalError(
                f"durable audit checkpoint read failed ({_safe_error_type(exc)})"
            ) from None
        if not isinstance(values, (list, tuple)) or len(values) != 2:
            raise AuditJournalError("durable audit checkpoint response is invalid")
        if values[0] is None and values[1] is None:
            return GENESIS_HASH, 0
        head = _text(values[0], maximum_bytes=64)
        if not _HASH_RE.fullmatch(head):
            raise AuditJournalError("durable audit checkpoint head is invalid")
        count = _integer(values[1])
        return head, count

    def archived_membership(
        self,
        journal_ids: Sequence[str],
        *,
        admission_days: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, str]:
        if len(journal_ids) > _MAX_RESERVATION_EVENTS * 10:
            raise AuditJournalLimitError(
                "durable audit archived-membership query is oversized"
            )
        if any(not _is_stream_id(item) for item in journal_ids):
            raise ValueError("durable audit journal ID is invalid")
        if admission_days is not None:
            if (
                not isinstance(admission_days, Mapping)
                or set(admission_days) != set(journal_ids)
                or any(
                    type(day) is not int
                    or not self.cutover_day <= day <= 4_000_000
                    for day in admission_days.values()
                )
            ):
                raise ValueError(
                    "durable audit admission-day reconciliation is invalid"
                )
        try:
            values = self._redis.hmget(
                self.base_key + ":archived-events",
                list(journal_ids),
            )
        except Exception as exc:
            raise AuditJournalError(
                "durable audit archived-membership read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if not isinstance(values, (list, tuple)) or len(values) != len(journal_ids):
            raise AuditJournalError(
                "durable audit archived-membership response is invalid"
            )
        result: Dict[str, str] = {}
        missing: List[str] = []
        for journal_id, value in zip(journal_ids, values):
            if value is None:
                missing.append(journal_id)
                continue
            batch_id = _text(value, maximum_bytes=64)
            if not _HASH_RE.fullmatch(batch_id):
                raise AuditJournalError(
                    "durable audit archived-membership value is invalid"
                )
            result[journal_id] = batch_id
        if missing and admission_days is not None:
            try:
                cleaned = self._redis.hget(
                    self.base_key + ":meta", "last_cleaned_day",
                )
            except Exception as exc:
                raise AuditJournalError(
                    "durable audit cleanup watermark read failed "
                    f"({_safe_error_type(exc)})"
                ) from None
            if cleaned is not None:
                last_cleaned_day = _integer(cleaned, maximum=4_000_000)
                for journal_id in missing:
                    if admission_days[journal_id] <= last_cleaned_day:
                        # Closed-day cleanup deletes the per-event receipt map
                        # only after admitted==archived, exact proof/manifest
                        # publication, and Redis finalization. The caller's
                        # trusted admission day is therefore a durable flush
                        # completion watermark even after receipt compaction.
                        result[journal_id] = GENESIS_HASH
        return result

    def _day_instances(self, day: int) -> List[str]:
        key = f"{self.base_key}:day:{day}:instances"
        try:
            values = self._redis.smembers(key) or set()
        except Exception as exc:
            raise AuditJournalError(
                "durable audit instance-index read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if not isinstance(values, (set, list, tuple)) or len(values) > _MAX_DAY_INSTANCES:
            raise AuditJournalLimitError("durable audit instance index is oversized")
        instances = sorted(_text(value, maximum_bytes=64) for value in values)
        for instance_id in instances:
            RK.audit_chain_head(self.organization, instance_id)
        if len(instances) != len(set(instances)):
            raise AuditJournalError("durable audit instance index is invalid")
        return instances

    def claim(
        self,
        *,
        count: int = _MAX_RESERVATION_EVENTS,
        lease_ms: int = _DEFAULT_RESERVATION_LEASE_MS,
    ) -> Optional[JournalReservation]:
        """Claim or recover the oldest exact per-instance journal batch."""
        if type(count) is not int or not 1 <= count <= _MAX_RESERVATION_EVENTS:
            raise ValueError("durable audit reservation count is invalid")
        if type(lease_ms) is not int or not 1_000 <= lease_ms <= 3_600_000:
            raise ValueError("durable audit reservation lease is invalid")
        for day in self._active_days():
            for instance_id in self._day_instances(day):
                token = secrets.token_hex(16)
                response = self._eval(
                    "reservation",
                    _CLAIM_LUA,
                    self.base_key,
                    day,
                    instance_id,
                    token,
                    count,
                    _MAX_RESERVATION_BYTES,
                    lease_ms,
                    GENESIS_HASH,
                )
                if not isinstance(response, (list, tuple)) or not response:
                    raise AuditJournalError("durable audit reservation response is invalid")
                status = _text(response[0], maximum_bytes=16)
                if status in {"EMPTY", "BLOCKED"}:
                    continue
                if status != "OK" or len(response) < 6:
                    raise AuditJournalError("durable audit reservation response is invalid")
                returned_token = _text(response[1], maximum_bytes=64)
                if returned_token != token:
                    raise AuditJournalError("durable audit reservation token is inconsistent")
                expires_ms = _integer(response[2])
                previous_head = _text(response[3], maximum_bytes=64)
                if not _HASH_RE.fullmatch(previous_head):
                    raise AuditJournalError("durable audit chain checkpoint is invalid")
                previous_count = _integer(response[4])
                list_entries = tuple(
                    _text(item, maximum_bytes=128) for item in response[5:]
                )
                if not list_entries or len(list_entries) > count:
                    raise AuditJournalError("durable audit reservation membership is invalid")
                journal_ids: List[str] = []
                total_bytes = 0
                for item in list_entries:
                    journal_id, separator, size_text = item.partition("|")
                    if not separator or not _is_stream_id(journal_id):
                        raise AuditJournalError("durable audit pending entry is invalid")
                    size = _integer(size_text, minimum=1, maximum=_MAX_EVENT_BYTES)
                    total_bytes += size
                    journal_ids.append(journal_id)
                if total_bytes > _MAX_RESERVATION_BYTES or len(set(journal_ids)) != len(journal_ids):
                    raise AuditJournalLimitError("durable audit reservation is oversized")
                try:
                    payloads = self._redis.hmget(
                        self.base_key + ":event-data", journal_ids,
                    )
                except Exception as exc:
                    raise AuditJournalError(
                        "durable audit payload read failed "
                        f"({_safe_error_type(exc)})"
                    ) from None
                if not isinstance(payloads, (list, tuple)) or len(payloads) != len(journal_ids):
                    raise AuditJournalError("durable audit payload response is invalid")
                events = tuple(
                    _decode_event_payload(
                        payload,
                        organization=self.organization,
                        instance_id=instance_id,
                        journal_id=journal_id,
                        day=day,
                    )
                    for journal_id, payload in zip(journal_ids, payloads)
                )
                return JournalReservation(
                    token=token,
                    day=day,
                    instance_id=instance_id,
                    journal_ids=tuple(journal_ids),
                    list_entries=list_entries,
                    events=events,
                    previous_head=previous_head,
                    previous_batch_count=previous_count,
                    expires_ms=expires_ms,
                )
        return None

    def complete(
        self,
        reservation: JournalReservation,
        receipt: JournalArchiveReceipt,
    ) -> bool:
        """Atomically commit an exact archive receipt and chain checkpoint."""
        if receipt.batch_id != reservation.batch_id:
            raise ValueError("durable audit receipt batch ID is inconsistent")
        if (
            receipt.day != reservation.day
            or receipt.instance_id != reservation.instance_id
            or receipt.journal_ids != reservation.journal_ids
            or receipt.previous_head != reservation.previous_head
            or receipt.previous_batch_count != reservation.previous_batch_count
            or receipt.event_count != len(reservation.events)
        ):
            raise ValueError("durable audit receipt reservation is inconsistent")
        raw = receipt.to_json()
        result = self._eval(
            "archive completion",
            _COMPLETE_LUA,
            self.base_key,
            reservation.day,
            reservation.instance_id,
            reservation.token,
            receipt.batch_id,
            raw,
            receipt.previous_head,
            receipt.previous_batch_count,
            receipt.chain_head,
            receipt.batch_count,
            receipt.event_count,
            receipt.bytes_written,
            GENESIS_HASH,
            _MAX_DAY_RECEIPTS,
            _MAX_DAY_ARCHIVE_BYTES,
        )
        if type(result) is not int or result not in {1, 2}:
            raise AuditJournalError("durable audit completion response is invalid")
        return result == 1

    def begin_close(
        self,
        day: int,
        *,
        grace_ms: int = _DEFAULT_CLOSE_GRACE_MS,
    ) -> Optional[DayCloseLease]:
        if type(day) is not int or not 0 <= day <= 4_000_000:
            raise ValueError("durable audit close day is invalid")
        if type(grace_ms) is not int or not 0 <= grace_ms <= 86_400_000:
            raise ValueError("durable audit close grace is invalid")
        response = self._eval(
            "day close",
            _BEGIN_CLOSE_LUA,
            self.base_key,
            day,
            secrets.token_hex(16),
            grace_ms,
        )
        if not isinstance(response, (list, tuple)) or not response:
            raise AuditJournalError("durable audit close response is invalid")
        status = _text(response[0], maximum_bytes=16)
        if status in {"OPEN", "PENDING", "CLOSED", "GAP"}:
            return None
        if status != "OK" or len(response) != 7:
            raise AuditJournalError("durable audit close response is invalid")
        return DayCloseLease(
            day=day,
            token=_text(response[1], maximum_bytes=64),
            requested_ms=_integer(response[2]),
            admitted=_integer(response[3], maximum=_MAX_DAY_EVENTS),
            admitted_bytes=_integer(response[4], maximum=_MAX_DAY_EVENT_BYTES),
            receipts=_integer(response[5], maximum=_MAX_DAY_RECEIPTS),
            archive_bytes=_integer(response[6], maximum=_MAX_DAY_ARCHIVE_BYTES),
        )

    def list_receipts(self, lease: DayCloseLease) -> List[JournalArchiveReceipt]:
        key = f"{self.base_key}:day:{lease.day}:receipts"
        try:
            raw = self._redis.hgetall(key) or {}
        except Exception as exc:
            raise AuditJournalError(
                f"durable audit receipt read failed ({_safe_error_type(exc)})"
            ) from None
        if not isinstance(raw, dict) or len(raw) > _MAX_DAY_RECEIPTS:
            raise AuditJournalLimitError("durable audit receipt index is oversized")
        receipts: List[JournalArchiveReceipt] = []
        retained = 0
        for raw_batch_id, value in raw.items():
            batch_id = _text(raw_batch_id, maximum_bytes=64)
            if not _HASH_RE.fullmatch(batch_id):
                raise AuditJournalError("durable audit receipt key is invalid")
            receipt = JournalArchiveReceipt.from_json(
                value, organization=self.organization,
            )
            if receipt.batch_id != batch_id or receipt.day != lease.day:
                raise AuditJournalError("durable audit receipt index is inconsistent")
            retained += len(receipt.to_json().encode("utf-8"))
            if retained > _MAX_DAY_EVENT_BYTES:
                raise AuditJournalLimitError("durable audit receipt index bytes are oversized")
            receipts.append(receipt)
        if len(receipts) != lease.receipts:
            raise AuditJournalError("durable audit receipt count changed during close")
        if sum(item.event_count for item in receipts) != lease.admitted:
            raise AuditJournalError("durable audit archived membership is incomplete")
        if sum(item.bytes_written for item in receipts) != lease.archive_bytes:
            raise AuditJournalError("durable audit archive byte count is inconsistent")
        return sorted(receipts, key=lambda item: item.batch_id)

    def finalize_close(
        self,
        lease: DayCloseLease,
        *,
        proof_hash: str,
        manifest_hash: str,
    ) -> bool:
        if not _HASH_RE.fullmatch(proof_hash) or not _HASH_RE.fullmatch(manifest_hash):
            raise ValueError("durable audit close hash is invalid")
        result = self._eval(
            "day close finalization",
            _FINALIZE_CLOSE_LUA,
            self.base_key,
            lease.day,
            lease.token,
            proof_hash,
            manifest_hash,
        )
        if type(result) is not int or result not in {1, 2}:
            raise AuditJournalError("durable audit close finalization response is invalid")
        return result == 1

    def day_state(self, day: int) -> Dict[str, Any]:
        if type(day) is not int or not 0 <= day <= 4_000_000:
            raise ValueError("durable audit day is invalid")
        try:
            raw = self._redis.hgetall(
                f"{self.base_key}:day:{day}:state"
            ) or {}
        except Exception as exc:
            raise AuditJournalError(
                f"durable audit day-state read failed ({_safe_error_type(exc)})"
            ) from None
        if not isinstance(raw, dict) or len(raw) > 16:
            raise AuditJournalError("durable audit day state is invalid")
        result = {
            _text(key, maximum_bytes=64): _text(value, maximum_bytes=128)
            for key, value in raw.items()
        }
        return result

    def cleanup_pending_day(self, *, chunk_size: int = 1_000) -> int:
        """Boundedly trim a closed journal day after its immutable manifest."""
        if type(chunk_size) is not int or not 1 <= chunk_size <= 1_000:
            raise ValueError("durable audit cleanup chunk size is invalid")
        try:
            raw_day = self._redis.hget(
                self.base_key + ":meta", "cleanup_day",
            )
        except Exception as exc:
            raise AuditJournalError(
                "durable audit cleanup-head read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if raw_day is None:
            return 0
        day = _integer(raw_day, maximum=4_000_000)
        try:
            raw_receipts = self._redis.hgetall(
                f"{self.base_key}:day:{day}:receipts"
            ) or {}
        except Exception as exc:
            raise AuditJournalError(
                "durable audit cleanup receipt read failed "
                f"({_safe_error_type(exc)})"
            ) from None
        if not isinstance(raw_receipts, dict) or len(raw_receipts) > _MAX_DAY_RECEIPTS:
            raise AuditJournalLimitError("durable audit cleanup receipt index is oversized")
        journal_ids: List[str] = []
        for value in raw_receipts.values():
            receipt = JournalArchiveReceipt.from_json(
                value, organization=self.organization,
            )
            if receipt.day != day:
                raise AuditJournalError("durable audit cleanup receipt day is invalid")
            journal_ids.extend(receipt.journal_ids)
            if len(journal_ids) > _MAX_DAY_EVENTS:
                raise AuditJournalLimitError("durable audit cleanup membership is oversized")
        if len(journal_ids) != len(set(journal_ids)):
            raise AuditJournalError("durable audit cleanup membership is duplicated")
        removed = 0
        ordered = sorted(
            journal_ids,
            key=lambda value: tuple(int(part) for part in value.split("-", 1)),
        )
        for offset in range(0, len(ordered), chunk_size):
            result = self._eval(
                "closed-day cleanup",
                _CLEANUP_CHUNK_LUA,
                self.base_key,
                day,
                *ordered[offset:offset + chunk_size],
            )
            if type(result) is not int or not 0 <= result <= chunk_size:
                raise AuditJournalError("durable audit cleanup response is invalid")
            removed += result
        result = self._eval(
            "cleanup finalization",
            _FINISH_CLEANUP_LUA,
            self.base_key,
            day,
            _MAX_DAY_INSTANCES,
        )
        if result != 1:
            raise AuditJournalError("durable audit cleanup finalization is invalid")
        return removed

    def next_close_day(self) -> int:
        """Return the only day that may close next under the cutover chain."""
        try:
            values = self._redis.hmget(
                self.base_key + ":meta", ["cutover_day", "last_closed_day"],
            )
        except Exception as exc:
            raise AuditJournalError(
                f"durable audit close-head read failed ({_safe_error_type(exc)})"
            ) from None
        if not isinstance(values, (list, tuple)) or len(values) != 2:
            raise AuditJournalError("durable audit close-head response is invalid")
        cutover = _integer(values[0], maximum=4_000_000)
        if values[1] is None:
            return cutover
        last_closed = _integer(values[1], maximum=4_000_000)
        if last_closed < cutover:
            raise AuditJournalError("durable audit close head precedes cutover")
        return last_closed + 1


class DurableAuditArchiver:
    """Archive exact durable journal reservations and close completed days."""

    def __init__(
        self,
        journal: RedisAuditJournal,
        parquet_writer: Any,
        *,
        redis_writer: Any = None,
    ) -> None:
        self.journal = journal
        self.parquet_writer = parquet_writer
        self.redis_writer = redis_writer
        self._pending: Optional[JournalReservation] = None

    def archive_once(self, *, count: int = _MAX_RESERVATION_EVENTS) -> Optional[JournalArchiveReceipt]:
        reservation = self._pending or self.journal.claim(count=count)
        if reservation is None:
            return None
        self._pending = reservation
        event_dicts = [event.to_dict() for event in reservation.events]
        chain = InstanceChain(
            instance_id=reservation.instance_id,
            head=reservation.previous_head,
            batch_count=reservation.previous_batch_count,
        )
        new_head = chain.next_for_events(event_dicts)
        for event in event_dicts:
            event["chain_hash"] = new_head
        result = self.parquet_writer.write_batch(
            self.journal.organization,
            event_dicts,
            publication_id=reservation.batch_id,
            published_at_ms=max(event.timestamp_ms for event in reservation.events),
        )
        if not isinstance(result, dict):
            raise AuditJournalError("durable audit archive receipt is missing")
        path = result.get("path")
        file_hash = result.get("file_hash")
        bytes_written = result.get("bytes_written")
        publication_id = result.get("publication_id")
        if (
            not isinstance(path, str)
            or not isinstance(file_hash, str)
            or not _HASH_RE.fullmatch(file_hash)
            or isinstance(bytes_written, bool)
            or not isinstance(bytes_written, int)
            or not 1 <= bytes_written <= _MAX_DAY_ARCHIVE_BYTES
            or not isinstance(publication_id, str)
            or publication_id != reservation.batch_id
            or result.get("event_count") != len(reservation.events)
        ):
            raise AuditJournalError(
                "durable audit archive receipt is invalid"
            )
        receipt = JournalArchiveReceipt(
            batch_id=reservation.batch_id,
            day=reservation.day,
            instance_id=reservation.instance_id,
            journal_ids=reservation.journal_ids,
            previous_head=reservation.previous_head,
            chain_head=new_head,
            previous_batch_count=reservation.previous_batch_count,
            batch_count=reservation.previous_batch_count + 1,
            event_count=len(reservation.events),
            path=path,
            file_hash=file_hash,
            bytes_written=bytes_written,
            publication_id=publication_id,
            min_timestamp_ms=min(event.timestamp_ms for event in reservation.events),
        )
        # Canonical validation is deliberately performed before the Redis CAS.
        JournalArchiveReceipt.from_json(
            receipt.to_json(), organization=self.journal.organization,
        )
        self.journal.complete(reservation, receipt)
        self._pending = None
        if self.redis_writer is not None:
            try:
                self.redis_writer.write_batch(event_dicts)
            except Exception:
                pass
        return receipt


def _day_date(day: int) -> str:
    return datetime.fromtimestamp(day * 86_400, tz=timezone.utc).strftime("%Y-%m-%d")


def _previous_day_proof(
    writer: Any,
    journal: RedisAuditJournal,
    day: int,
) -> Optional[MerkleProof]:
    """Load the exact prior closed proof, never a pre-cutover orphan."""
    if day <= journal.cutover_day:
        return None
    previous_date = datetime.fromtimestamp(
        (day - 1) * 86_400, tz=timezone.utc,
    ).strftime("%Y%m%d")
    manifest = writer.load_day_close_manifest(
        journal.organization, previous_date, strict=True,
    )
    proof = writer.load_chain_proof(
        journal.organization,
        previous_date,
        strict=True,
    )
    if manifest is None or proof is None:
        raise AuditJournalError(
            "durable audit predecessor closure evidence is unavailable"
        )
    proof_raw = _canonical_json(proof.to_dict()).encode("utf-8")
    if hashlib.sha256(proof_raw).hexdigest() != manifest.get("proof_hash"):
        raise AuditJournalCollisionError(
            "durable audit predecessor proof differs from its close manifest"
        )
    return proof


def _immutable_create_exact(
    storage: Any,
    path: str,
    payload: bytes,
    *,
    maximum_size: int,
) -> None:
    """Conditional-create exact bytes and reconcile every ambiguous result."""
    from supertable.audit.writer_parquet import _read_sealed_bytes

    if not payload or len(payload) > maximum_size:
        raise AuditJournalLimitError("durable audit close artifact is oversized")
    create = getattr(storage, "create_bytes_if_absent", None)
    if not callable(create):
        raise AuditJournalError(
            "audit storage lacks immutable conditional-create support"
        )
    try:
        created = create(path, payload)
        if type(created) is not bool:
            raise AuditJournalError(
                "audit storage conditional-create result is invalid"
            )
    except Exception:
        try:
            existing = _read_sealed_bytes(
                storage, path, maximum_size=maximum_size, minimum_size=1,
            )
        except Exception:
            raise AuditJournalError(
                "durable audit close publication could not be reconciled"
            ) from None
        if existing != payload:
            raise AuditJournalCollisionError(
                "durable audit close path contains different bytes"
            ) from None
        created = False
    else:
        if not created:
            existing = _read_sealed_bytes(
                storage, path, maximum_size=maximum_size, minimum_size=1,
            )
            if existing != payload:
                raise AuditJournalCollisionError(
                    "durable audit close path contains different bytes"
                )
    if not created:
        ensure_durable = getattr(storage, "ensure_bytes_durable", None)
        if callable(ensure_durable):
            ensure_durable(path)
    confirmed = _read_sealed_bytes(
        storage, path, maximum_size=maximum_size, minimum_size=1,
    )
    if confirmed != payload:
        raise AuditJournalCollisionError(
            "durable audit close publication is not exact"
        )


class DurableAuditDayCloser:
    """Deterministically aggregate sealed receipts into an immutable proof."""

    def __init__(self, journal: RedisAuditJournal, parquet_writer: Any) -> None:
        self.journal = journal
        self.parquet_writer = parquet_writer

    def close_day(
        self,
        day: int,
        *,
        grace_ms: int = _DEFAULT_CLOSE_GRACE_MS,
    ) -> Optional[Dict[str, Any]]:
        lease = self.journal.begin_close(day, grace_ms=grace_ms)
        if lease is None:
            return None
        receipts = self.journal.list_receipts(lease)
        date = datetime.fromtimestamp(day * 86_400, tz=timezone.utc)
        batches = self.parquet_writer.read_batch_events(
            self.journal.organization,
            date.year,
            date.month,
            date.day,
            limit=_MAX_DAY_EVENTS,
            strict=True,
            expected_files=tuple(sorted(receipt.path for receipt in receipts)),
        )
        receipt_paths = {receipt.path for receipt in receipts}
        by_path = {
            batch.get("file_path"): batch
            for batch in batches
            if batch.get("file_path") in receipt_paths
        }
        if len(by_path) != len(receipt_paths) or set(by_path) != receipt_paths:
            raise AuditJournalError(
                "durable audit closed-day archive membership is inconsistent"
            )
        current_instances: Dict[str, Dict[str, Any]] = {}
        daily_events: Dict[str, int] = {}
        remaining = list(receipts)
        previous = _previous_day_proof(
            self.parquet_writer, self.journal, day,
        )
        if previous is not None:
            for instance_id, entry in previous.instances.items():
                current_instances[instance_id] = dict(entry)
                current_instances[instance_id]["events"] = 0
        while remaining:
            candidates: Dict[str, JournalArchiveReceipt] = {}
            for receipt in remaining:
                prior = current_instances.get(receipt.instance_id)
                prior_head = prior["head"] if prior else GENESIS_HASH
                prior_count = prior["batches"] if prior else 0
                if (
                    receipt.previous_head == prior_head
                    and receipt.previous_batch_count == prior_count
                ):
                    if receipt.instance_id in candidates:
                        raise AuditJournalError(
                            "durable audit receipt chain branches"
                        )
                    candidates[receipt.instance_id] = receipt
            if not candidates:
                raise AuditJournalError("durable audit receipt chain has a gap")
            # Independent instances may advance in any order; sorting makes the
            # aggregation deterministic while requiring one next edge per ID.
            for receipt in sorted(
                candidates.values(), key=lambda item: item.batch_id,
            ):
                batch = by_path[receipt.path]
                events = batch.get("events")
                if (
                    batch.get("file_hash") != receipt.file_hash
                    or batch.get("event_count") != receipt.event_count
                    or batch.get("chain_hash") != receipt.chain_head
                    or batch.get("instance_id") != receipt.instance_id
                    or not isinstance(events, list)
                ):
                    raise AuditJournalError(
                        "durable audit archive differs from its receipt"
                    )
                if compute_event_batch_hash(events) != compute_event_batch_hash([
                    {**event, "chain_hash": ""} for event in events
                ]):
                    # compute_event_batch_hash intentionally excludes chain_hash;
                    # retain this explicit call as a schema/canonicality check.
                    raise AuditJournalError("durable audit archive content is invalid")
                expected_head = InstanceChain(
                    receipt.instance_id,
                    head=receipt.previous_head,
                    batch_count=receipt.previous_batch_count,
                ).next_for_events([{**event, "chain_hash": ""} for event in events])
                if expected_head != receipt.chain_head:
                    raise AuditJournalError("durable audit archive chain is invalid")
                current_instances[receipt.instance_id] = {
                    "head": receipt.chain_head,
                    "batches": receipt.batch_count,
                    "events": daily_events.get(receipt.instance_id, 0)
                    + receipt.event_count,
                }
                daily_events[receipt.instance_id] = current_instances[
                    receipt.instance_id
                ]["events"]
                remaining.remove(receipt)
        if len(current_instances) > _MAX_DAY_INSTANCES:
            raise AuditJournalLimitError("durable audit proof has too many instances")
        proof = MerkleProof(date=_day_date(day), created_ms=lease.requested_ms)
        proof.instances = {
            instance_id: current_instances[instance_id]
            for instance_id in sorted(current_instances)
        }
        proof.total_events = sum(daily_events.values())
        proof.compute_root()
        proof_raw = _canonical_json(proof.to_dict()).encode("utf-8")
        if len(proof_raw) > _MAX_PROOF_BYTES:
            raise AuditJournalLimitError("durable audit proof is oversized")
        receipt_root = hashlib.sha256("\n".join(
            hashlib.sha256(receipt.to_json().encode("utf-8")).hexdigest()
            for receipt in receipts
        ).encode("ascii")).hexdigest()
        proof_hash = hashlib.sha256(proof_raw).hexdigest()
        manifest = {
            "admitted": lease.admitted,
            "admitted_bytes": lease.admitted_bytes,
            "archive_bytes": lease.archive_bytes,
            "batch_ids": [receipt.batch_id for receipt in receipts],
            "close_requested_ms": lease.requested_ms,
            "cutover_ms": self.journal.cutover_ms,
            "day": day,
            "date": _day_date(day),
            "cutover_day": self.journal.cutover_day,
            "format_version": 1,
            "organization": self.journal.organization,
            "proof_hash": proof_hash,
            "receipt_count": len(receipts),
            "receipt_root": receipt_root,
            "version": 1,
        }
        manifest_raw = _canonical_json(manifest).encode("utf-8")
        if len(manifest_raw) > _MAX_CLOSE_MANIFEST_BYTES:
            raise AuditJournalLimitError("durable audit close manifest is oversized")
        manifest_hash = hashlib.sha256(manifest_raw).hexdigest()
        storage = self.parquet_writer._get_storage()
        proof_path = (
            f"{self.journal.organization}/__audit__/_chain/"
            f"chain_{date.strftime('%Y%m%d')}.json"
        )
        manifest_path = (
            f"{self.journal.organization}/__audit__/_chain/"
            f"closed_{date.strftime('%Y%m%d')}.json"
        )
        _immutable_create_exact(
            storage, proof_path, proof_raw, maximum_size=_MAX_PROOF_BYTES,
        )
        _immutable_create_exact(
            storage,
            manifest_path,
            manifest_raw,
            maximum_size=_MAX_CLOSE_MANIFEST_BYTES,
        )
        self.journal.finalize_close(
            lease, proof_hash=proof_hash, manifest_hash=manifest_hash,
        )
        return {
            "day": day,
            "date": _day_date(day),
            "manifest_hash": manifest_hash,
            "proof_hash": proof_hash,
            "receipts": len(receipts),
            "events": lease.admitted,
        }
