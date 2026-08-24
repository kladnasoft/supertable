# route: supertable.redis_catalog
from __future__ import annotations

import json
import math
import random
import re
import time
import hashlib
import secrets
import functools
import inspect
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence

import redis
from supertable.config.defaults import logger
from supertable.errors import LockLostError, SnapshotCommitConflictError

try:
    from .redis_connector import RedisConnector, RedisOptions
except ImportError:  # pragma: no cover
    from redis_connector import RedisConnector, RedisOptions

from supertable.locking.redis_lock import RedisLocking
from supertable import redis_keys as RK
from supertable.utils.spark_security import (
    spark_storage_credential_keys,
    validate_spark_storage_config,
)
from supertable.utils.snapshot import (
    complete_snapshot_payload,
    snapshot_cache_payload,
)
from supertable.tombstone_manifest_v2 import (
    MAX_JSON_EXACT_INTEGER as MAX_TOMBSTONE_JSON_EXACT_INTEGER,
    TOMBSTONE_FORMAT_V2,
    TOMBSTONE_FORMAT_V3,
    TombstoneManifestV2Error,
    validate_snapshot_tombstone_state,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


_ROOT_CLONE_TYPES = frozenset({"readonly", "replica"})
_REDIS_LUA_MAX_SAFE_INTEGER = (1 << 53) - 1
_UNPINNED_MIRROR_CONFIG = object()
_DV_FORMAT_CONFIG_KEY = "deletion_vector_format"
_DV_V2_FLEET_CONFIG_KEY = "dv_v2_reader_fleet_confirmed"
_DV_V3_FLEET_CONFIG_KEY = "dv_v3_reader_fleet_confirmed"


def _validate_table_config_document(
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    """Return a table config whose durable DV activation is unambiguous.

    Legacy documents contain neither activation field and remain valid.  Once
    any marker appears, the format and exactly its matching fleet proof must
    carry the rollout values emitted by ``DataWriter.configure_table``.
    Python/JSON booleans do not pass as integer format values and
    integer/string truthy values do not pass as fleet confirmations.
    """
    if not isinstance(config, Mapping):
        raise ValueError("table configuration must be an object")
    document = dict(config)
    format_present = _DV_FORMAT_CONFIG_KEY in document
    v2_fleet_present = _DV_V2_FLEET_CONFIG_KEY in document
    v3_fleet_present = _DV_V3_FLEET_CONFIG_KEY in document
    if not (format_present or v2_fleet_present or v3_fleet_present):
        return document

    format_value = document.get(_DV_FORMAT_CONFIG_KEY)
    valid_v2 = (
        format_present
        and type(format_value) is int
        and format_value == TOMBSTONE_FORMAT_V2
        and v2_fleet_present
        and document[_DV_V2_FLEET_CONFIG_KEY] is True
        and not v3_fleet_present
    )
    valid_v3 = (
        format_present
        and type(format_value) is int
        and format_value == TOMBSTONE_FORMAT_V3
        and v3_fleet_present
        and document[_DV_V3_FLEET_CONFIG_KEY] is True
        and not v2_fleet_present
    )
    if not (valid_v2 or valid_v3):
        raise ValueError(
            "DV-v2 activation requires deletion_vector_format=2 and "
            "dv_v2_reader_fleet_confirmed=true exclusively; DV-v3 activation "
            "requires deletion_vector_format=3 and "
            "dv_v3_reader_fleet_confirmed=true exclusively"
        )
    return document


def _strict_json_object_with_tokens(
        raw: Any, *, field: str,
) -> tuple[str, Dict[str, Any], Dict[str, tuple[str, str]]]:
    """Decode one JSON object without accepting ambiguous member syntax.

    ``json.loads`` deliberately accepts duplicate object members and non-JSON
    constants such as ``NaN``.  Neither is a safe pin for a later atomic Redis
    mutation: the Python and Lua decoders can select different values.  Keep
    the exact source text, reject semantic duplicates (including escaped key
    aliases), and expose the top-level key/value tokens needed by activation
    fields whose spelling is itself part of the durable contract.
    """
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{field} is not valid UTF-8 JSON") from exc
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"{field} must be a non-empty JSON object")

    def reject_constant(value: str) -> None:
        raise ValueError(f"{field} contains invalid JSON constant {value!r}")

    def unique_object(pairs: list[tuple[str, Any]]) -> Dict[str, Any]:
        document: Dict[str, Any] = {}
        for key, value in pairs:
            if key in document:
                raise ValueError(f"{field} contains duplicate member {key!r}")
            document[key] = value
        return document

    decoder = json.JSONDecoder(
        object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )
    try:
        document = decoder.decode(raw)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        raise ValueError(f"{field} is not valid JSON") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{field} must be a JSON object")

    def validate_interoperable_value(value: Any) -> None:
        if isinstance(value, str):
            if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
                raise ValueError(f"{field} contains an invalid Unicode surrogate")
            return
        if value is None or type(value) in (bool, int):
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{field} contains a non-finite number")
            return
        if isinstance(value, list):
            for item in value:
                validate_interoperable_value(item)
            return
        if isinstance(value, dict):
            for key, item in value.items():
                validate_interoperable_value(key)
                validate_interoperable_value(item)
            return
        raise ValueError(f"{field} contains an unsupported JSON value")

    validate_interoperable_value(document)

    # Scan the already-validated top-level object with raw_decode so exact
    # tokens are available without implementing a second JSON grammar.
    token_decoder = json.JSONDecoder(parse_constant=reject_constant)
    length = len(raw)

    def skip_space(index: int) -> int:
        while index < length and raw[index] in " \t\r\n":
            index += 1
        return index

    index = skip_space(0)
    if index >= length or raw[index] != "{":  # guarded by decode; defensive
        raise ValueError(f"{field} must be a JSON object")
    index += 1
    tokens: Dict[str, tuple[str, str]] = {}
    while True:
        index = skip_space(index)
        if index < length and raw[index] == "}":
            index += 1
            break
        key_start = index
        try:
            key, key_end = token_decoder.raw_decode(raw, index)
        except json.JSONDecodeError as exc:  # pragma: no cover - decoded above
            raise ValueError(f"{field} is not valid JSON") from exc
        if not isinstance(key, str):  # pragma: no cover - JSON object grammar
            raise ValueError(f"{field} has a non-string member name")
        index = skip_space(key_end)
        if index >= length or raw[index] != ":":  # pragma: no cover
            raise ValueError(f"{field} is not valid JSON")
        index = skip_space(index + 1)
        value_start = index
        try:
            _, value_end = token_decoder.raw_decode(raw, index)
        except json.JSONDecodeError as exc:  # pragma: no cover - decoded above
            raise ValueError(f"{field} is not valid JSON") from exc
        tokens[key] = (raw[key_start:key_end], raw[value_start:value_end])
        index = skip_space(value_end)
        if index < length and raw[index] == ",":
            index += 1
            continue
        if index < length and raw[index] == "}":
            index += 1
            break
        raise ValueError(f"{field} is not valid JSON")  # pragma: no cover
    if skip_space(index) != length:  # pragma: no cover - decoded above
        raise ValueError(f"{field} is not valid JSON")
    return raw, document, tokens


def _validate_initial_table_config_pin(raw: Any) -> tuple[str, Dict[str, Any]]:
    raw, document, tokens = _strict_json_object_with_tokens(
        raw, field="table configuration",
    )
    document = _validate_table_config_document(document)
    if _DV_FORMAT_CONFIG_KEY in document:
        format_value = document[_DV_FORMAT_CONFIG_KEY]
        format_tokens = tokens.get(_DV_FORMAT_CONFIG_KEY)
        if format_value == TOMBSTONE_FORMAT_V2:
            fleet_tokens = tokens.get(_DV_V2_FLEET_CONFIG_KEY)
            expected_format_token = "2"
            expected_fleet_token = (
                '"dv_v2_reader_fleet_confirmed"', "true"
            )
        else:  # _validate_table_config_document proved exact integer v3.
            fleet_tokens = tokens.get(_DV_V3_FLEET_CONFIG_KEY)
            expected_format_token = "3"
            expected_fleet_token = (
                '"dv_v3_reader_fleet_confirmed"', "true"
            )
        if (
            format_tokens
            != ('"deletion_vector_format"', expected_format_token)
            or fleet_tokens != expected_fleet_token
        ):
            raise ValueError(
                "DV activation fields must use exact JSON tokens"
            )
    return raw, document


def _validate_initial_mirror_pin(raw: Any) -> tuple[str, List[str]]:
    raw, document, tokens = _strict_json_object_with_tokens(
        raw, field="mirror configuration",
    )
    formats = document.get("formats")
    timestamp = document.get("ts")
    # ``list`` plus the captured leading token proves an array rather than the
    # empty-object/empty-array ambiguity of Redis Lua's cjson tables.
    format_member_token, format_token = tokens.get("formats", ("", ""))
    timestamp_member_token, timestamp_token = tokens.get("ts", ("", ""))
    if (
        not isinstance(formats, list)
        or format_member_token != '"formats"'
        or not format_token.startswith("[")
        or type(timestamp) is not int
        or timestamp < 0
        or timestamp > _REDIS_LUA_MAX_SAFE_INTEGER
        or timestamp_member_token != '"ts"'
        or not re.fullmatch(r"(?:0|[1-9][0-9]*)", timestamp_token)
    ):
        raise ValueError("mirror configuration is invalid")
    mirrors: List[str] = []
    for value in formats:
        if not isinstance(value, str):
            raise ValueError("mirror configuration is invalid")
        normalized = value.upper()
        if normalized not in ("DELTA", "ICEBERG", "PARQUET"):
            raise ValueError("mirror configuration is invalid")
        if normalized in mirrors:
            raise ValueError("mirror configuration is invalid")
        mirrors.append(normalized)
    return raw, mirrors


def _complete_table_bound_snapshot_payload(
        payload: object,
        *,
        expected_version: int,
        org: str,
        sup: str,
        simple: str,
) -> Optional[Dict[str, Any]]:
    """Return a complete Redis cache only when explicit DVs are table-bound."""
    candidate = complete_snapshot_payload(
        payload,
        expected_version=expected_version,
        require_policy_marker=True,
    )
    if candidate is None:
        return None
    tombstone_format = candidate.get("tombstone_format", 1)
    if tombstone_format not in (TOMBSTONE_FORMAT_V2, TOMBSTONE_FORMAT_V3):
        return candidate
    pointer = candidate.get("tombstone")
    if (
        candidate["snapshot_version"] > MAX_TOMBSTONE_JSON_EXACT_INTEGER
        or (
            pointer is not None
            and (
                candidate["snapshot_version"] < 1
                or candidate["tombstone_rows"]
                > MAX_TOMBSTONE_JSON_EXACT_INTEGER
                or not pointer.startswith(
                    f"{org}/{sup}/tables/{simple}/tombstone/"
                )
            )
        )
    ):
        return None
    return candidate


class _PreparedTableMutationLeaf:
    """One-shot parsed leaf tied to its exact Redis representation.

    The object is deliberately private and owner-bound.  A writer prepares it
    only after taking the table lease, then :meth:`begin_table_mutation`
    consumes it exactly once.  Lua still compares ``raw_leaf`` byte-for-byte
    with the live key before trusting the already-validated scalar row-ID
    floor.  A changed key therefore takes the ordinary full-validation path.
    """

    __slots__ = (
        "_consumed",
        "_leaf",
        "_owner",
        "_raw_leaf",
        "_rowid_floor",
        "_snapshot_payload",
    )

    def __init__(
            self,
            *,
            owner: "RedisCatalog",
            raw_leaf: str,
            leaf: Dict[str, Any],
            snapshot_payload: Optional[Dict[str, Any]],
            rowid_floor: Optional[int],
    ) -> None:
        self._owner = owner
        self._raw_leaf = raw_leaf
        self._leaf = leaf
        self._snapshot_payload = snapshot_payload
        self._rowid_floor = rowid_floor
        self._consumed = False

    def take(
            self, owner: "RedisCatalog",
    ) -> tuple[str, Dict[str, Any], Optional[Dict[str, Any]], Optional[int]]:
        if owner is not self._owner:
            raise ValueError("prepared mutation leaf belongs to another catalog")
        if self._consumed:
            raise ValueError("prepared mutation leaf has already been consumed")
        self._consumed = True
        return (
            self._raw_leaf,
            self._leaf,
            self._snapshot_payload,
            self._rowid_floor,
        )


def _lua_safe_integer(
        value: Any, *, field: str, minimum: int = 0,
) -> int:
    """Return an integer that Redis Lua can compare without precision loss."""
    if (
        type(value) is not int
        or value < minimum
        or value > _REDIS_LUA_MAX_SAFE_INTEGER
    ):
        raise ValueError(
            f"{field} must be an integer from {minimum} through "
            f"{_REDIS_LUA_MAX_SAFE_INTEGER}"
        )
    return value


def _publication_timestamp(now_ms: Optional[int]) -> int:
    return _lua_safe_integer(
        _now_ms() if now_ms is None else now_ms,
        field="publication timestamp",
    )


def persist_unresolved_quality_generation(
        redis_client: Any,
        org: str,
        sup: str,
        simple: str,
        generation: str,
) -> bool:
    """Persist one post-ingest generation under a live catalog incarnation.

    Production ``RedisCatalog.commit_snapshot`` performs this write inside its
    snapshot transaction.  This single-round-trip helper is the compatibility
    path for catalog adapters that implement the fenced snapshot contract but
    do not yet advertise atomic quality-generation support.
    """
    if not isinstance(generation, str) or not generation:
        raise ValueError("quality generation must be a non-empty string")
    root_key = RK.meta_root(org, sup)
    leaf_key = RK.meta_leaf(org, sup, simple)
    simple_intent_key = RK.meta_simple_deletion_intent(org, sup, simple)
    namespace_intent_key = RK.meta_namespace_deletion_intent(org, sup)
    unresolved_key = (
        RK.quality_prefix(org, sup) + f"pending_unresolved:{simple}"
    )
    script = """
    -- persist a compatibility ingest generation under one live incarnation
    if redis.call('exists', KEYS[3]) == 1
        or redis.call('exists', KEYS[4]) == 1 then
        return 0
    end
    local root_payload = redis.call('get', KEYS[1])
    local leaf_payload = redis.call('get', KEYS[2])
    if not root_payload or not leaf_payload then return 0 end
    local root_ok, root = pcall(cjson.decode, root_payload)
    local leaf_ok, leaf = pcall(cjson.decode, leaf_payload)
    if not root_ok or type(root) ~= 'table'
        or type(root['version']) ~= 'number' or root['version'] < 0
        or root['version'] > 9007199254740991
        or root['version'] ~= math.floor(root['version'])
        or type(root['ts']) ~= 'number' or root['ts'] < 0
        or root['ts'] > 9007199254740991
        or root['ts'] ~= math.floor(root['ts'])
        or not leaf_ok or type(leaf) ~= 'table'
        or type(leaf['version']) ~= 'number' or leaf['version'] < 0
        or leaf['version'] > 9007199254740991
        or leaf['version'] ~= math.floor(leaf['version'])
        or type(leaf['ts']) ~= 'number' or leaf['ts'] < 0
        or leaf['ts'] > 9007199254740991
        or leaf['ts'] ~= math.floor(leaf['ts'])
        or type(leaf['path']) ~= 'string' or leaf['path'] == '' then
        return 0
    end
    redis.call('set', KEYS[5], ARGV[1])
    return 1
    """
    return bool(redis_client.eval(
        script,
        5,
        root_key,
        leaf_key,
        simple_intent_key,
        namespace_intent_key,
        unresolved_key,
        generation,
    ))


# Every mutation script that consumes ``meta:root`` prepends this helper.  A
# Python authorization preflight is not a write fence: root flags can change
# before the final Redis publication.  ``root_document_state`` therefore
# validates the full persisted lifecycle contract and returns 0 for a valid
# but non-writable root at the same atomic boundary as the mutation.
_LUA_ROOT_DOCUMENT_GUARD = r"""
local ROOT_MAX_SAFE_INTEGER = 9007199254740991

local function root_safe_segment(value)
  if type(value) ~= 'string'
      or string.len(value) < 1
      or string.len(value) > 64 then return false end
  if string.match(value, '^[a-z0-9][a-z0-9_-]*$') then return true end
  if string.match(value, '^__[a-z0-9][a-z0-9_-]*__$') then return true end
  return false
end

local function root_valid_string_array(value)
  if value == cjson.null then return true end
  if type(value) ~= 'table' then return false end
  local encoded_ok, encoded = pcall(cjson.encode, value)
  if not encoded_ok or string.sub(encoded, 1, 1) ~= '[' then return false end
  local count = 0
  local seen = {}
  for key, item in pairs(value) do
    if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
        or not root_safe_segment(item) or seen[item] then return false end
    seen[item] = true
    count = count + 1
  end
  for index = 1, count do
    if value[index] == nil then return false end
  end
  return true
end

local function root_document_state(root, target_super)
  if type(root) ~= 'table'
      or type(root['version']) ~= 'number'
      or root['version'] < 0
      or root['version'] > ROOT_MAX_SAFE_INTEGER
      or root['version'] ~= math.floor(root['version'])
      or type(root['ts']) ~= 'number'
      or root['ts'] < 0
      or root['ts'] > ROOT_MAX_SAFE_INTEGER
      or root['ts'] ~= math.floor(root['ts']) then return -1 end
  if root['read_only'] ~= nil
      and type(root['read_only']) ~= 'boolean' then return -1 end
  local clone_type = root['clone_type']
  if clone_type ~= nil and clone_type ~= cjson.null
      and clone_type ~= 'readonly' and clone_type ~= 'replica' then return -1 end
  local source = root['cloned_from']
  if source ~= nil and source ~= cjson.null then
    if not root_safe_segment(source)
        or (target_super ~= nil and target_super ~= ''
            and source == target_super) then return -1 end
  end
  local clone_ts = root['clone_ts']
  if clone_ts ~= nil and clone_ts ~= cjson.null
      and (type(clone_ts) ~= 'number' or clone_ts < 0
          or clone_ts > ROOT_MAX_SAFE_INTEGER
          or clone_ts ~= math.floor(clone_ts)) then return -1 end
  if root['replica_tables'] ~= nil
      and not root_valid_string_array(root['replica_tables']) then return -1 end
  local commit_id = root['commit_id']
  if commit_id ~= nil and commit_id ~= cjson.null
      and (type(commit_id) ~= 'string' or string.len(commit_id) < 1) then
    return -1
  end
  if (clone_type ~= nil and clone_type ~= cjson.null)
      or (source ~= nil and source ~= cjson.null) then
    if root['read_only'] ~= true
        or source == nil or source == cjson.null then return -1 end
  end
  if root['read_only'] == true
      or (clone_type ~= nil and clone_type ~= cjson.null)
      or (source ~= nil and source ~= cjson.null) then return 0 end
  return 1
end

local function publication_deadline_exceeded(not_after_ms)
  if not_after_ms == nil or not_after_ms <= 0 then return false end
  local server_time = redis.call('TIME')
  local server_ms = tonumber(server_time[1]) * 1000
      + math.floor(tonumber(server_time[2]) / 1000)
  return server_ms > not_after_ms
end

local function linked_manifest_digest_ok(value)
  return type(value) == 'string'
      and string.len(value) == 64
      and string.match(value, '^[0-9a-f]+$') ~= nil
end

local function linked_instance_nonce_ok(value)
  return type(value) == 'string'
      and string.len(value) == 81
      and string.match(value, '^link%-instance%-v1:[0-9a-f]+$') ~= nil
end
"""


# Snapshot publication has two final Lua paths.  Both prepend this exact
# discriminator so monkey-patched/older Python callers still cannot publish a
# partial, hybrid, future, or foreign-table deletion-vector state.
_LUA_SNAPSHOT_TOMBSTONE_GUARD = r"""
local TOMBSTONE_JSON_MAX_EXACT_INTEGER = 99999999999999

local function snapshot_table_artifact_path_ok(
    path, expected_prefix, required_suffix
)
  if type(path) ~= 'string' or path == ''
      or type(expected_prefix) ~= 'string' or expected_prefix == ''
      or type(required_suffix) ~= 'string' or required_suffix == ''
      or string.len(path) > 4096
      or string.sub(path, 1, 1) == '/'
      or string.sub(path, -1) == '/'
      or string.sub(path, -string.len(required_suffix)) ~= required_suffix
      or string.sub(path, 1, string.len(expected_prefix)) ~= expected_prefix
      or string.find(path, string.char(92), 1, true)
      or string.find(path, '//', 1, true)
      or string.find(path, '?', 1, true)
      or string.find(path, '#', 1, true)
      or string.find(path, '://', 1, true)
      or string.match(path, '^[%a][%w+%.%-]*:')
      or string.match(path, '[%c]')
      or string.match(path, '^%s')
      or string.match(path, '%s$') then return false end
  local components = 0
  for component in string.gmatch(path, '[^/]+') do
    if component == '.' or component == '..' then return false end
    components = components + 1
  end
  return components > 0
end

local function snapshot_tombstone_state_ok(candidate, expected_prefix)
  if type(candidate) ~= 'table' then return false end
  local pointer = candidate['tombstone']
  local tombstone_rows = candidate['tombstone_rows']
  local digest = candidate['tombstone_digest']
  local format_marker = candidate['tombstone_format']
  local tombstone_format = 1
  if format_marker == nil then
    tombstone_format = 1
  elseif type(format_marker) == 'number'
      and format_marker == math.floor(format_marker)
      and (format_marker == 1 or format_marker == 2
          or format_marker == 3) then
    tombstone_format = format_marker
  else
    return false
  end

  if (tombstone_format == 2 or tombstone_format == 3)
      and (type(candidate['snapshot_version']) ~= 'number'
          or candidate['snapshot_version'] < 0
          or candidate['snapshot_version'] ~= math.floor(
            candidate['snapshot_version']
          )
          or candidate['snapshot_version']
            > TOMBSTONE_JSON_MAX_EXACT_INTEGER) then
    return false
  end

  if pointer == cjson.null then
    return type(tombstone_rows) == 'number'
        and tombstone_rows == 0
        and digest == cjson.null
  end
  if type(pointer) ~= 'string' or pointer == ''
      or type(tombstone_rows) ~= 'number'
      or tombstone_rows <= 0
      or tombstone_rows ~= math.floor(tombstone_rows)
      or type(digest) ~= 'string'
      or string.len(digest) ~= 64
      or not string.match(digest, '^[0-9a-f]+$') then
    return false
  end
  if tombstone_format == 2 then
    return type(candidate['snapshot_version']) == 'number'
        and candidate['snapshot_version'] >= 1
        and tombstone_rows <= TOMBSTONE_JSON_MAX_EXACT_INTEGER
        and snapshot_table_artifact_path_ok(
          pointer, expected_prefix, '.json'
        )
  end
  if tombstone_format == 3 then
    return type(candidate['snapshot_version']) == 'number'
        and candidate['snapshot_version'] >= 1
        and tombstone_rows <= TOMBSTONE_JSON_MAX_EXACT_INTEGER
        and snapshot_table_artifact_path_ok(
          pointer, expected_prefix, '.parquet'
        )
  end
  -- Old readers interpret a missing/1 discriminator as one Parquet vector.
  return string.sub(pointer, -5) ~= '.json'
end
"""


def _validate_root_document(
        document: Any, *, org: str, sup: str,
) -> Dict[str, Any]:
    """Validate security- and lifecycle-relevant root fields fail closed."""
    if (
        not isinstance(document, dict)
        or type(document.get("version")) is not int
        or document["version"] < 0
        or document["version"] > _REDIS_LUA_MAX_SAFE_INTEGER
        or type(document.get("ts")) is not int
        or document["ts"] < 0
        or document["ts"] > _REDIS_LUA_MAX_SAFE_INTEGER
    ):
        raise ValueError("invalid root identity fields")
    if "read_only" in document and type(document["read_only"]) is not bool:
        raise ValueError("invalid root read_only flag")

    clone_type = document.get("clone_type")
    if clone_type is not None and clone_type not in _ROOT_CLONE_TYPES:
        raise ValueError("invalid root clone_type")

    source = document.get("cloned_from")
    if source is not None:
        if not isinstance(source, str) or not source or source == sup:
            raise ValueError("invalid root clone source")
        try:
            RK.meta_root(org, source)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid root clone source") from exc

    clone_ts = document.get("clone_ts")
    if clone_ts is not None and (
        type(clone_ts) is not int
        or clone_ts < 0
        or clone_ts > _REDIS_LUA_MAX_SAFE_INTEGER
    ):
        raise ValueError("invalid root clone timestamp")

    commit_id = document.get("commit_id")
    if commit_id is not None and (
        not isinstance(commit_id, str) or not commit_id
    ):
        raise ValueError("invalid root commit identity")

    replica_tables = document.get("replica_tables")
    if replica_tables is not None:
        if not isinstance(replica_tables, list):
            raise ValueError("invalid root replica table allowlist")
        seen: set[str] = set()
        for table in replica_tables:
            if not isinstance(table, str) or not table or table in seen:
                raise ValueError("invalid root replica table allowlist")
            try:
                RK.meta_leaf(org, source or sup, table)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid root replica table allowlist") from exc
            seen.add(table)

    # ``cloned_from`` predates the explicit clone_type marker.  It has always
    # represented a clone boundary, so legacy roots carrying it must be just as
    # immutable as newer readonly/replica roots.
    if clone_type in _ROOT_CLONE_TYPES or source is not None:
        if document.get("read_only") is not True or source is None:
            raise ValueError("clone roots must be read-only and source-bound")
    return document


def _validate_leaf_document(document: Any) -> Dict[str, Any]:
    """Validate the live leaf identity contract without normalizing values."""
    if not isinstance(document, dict):
        raise ValueError("leaf is not a JSON object")
    if (
        type(document.get("version")) is not int
        or document["version"] < 0
        or document["version"] > _REDIS_LUA_MAX_SAFE_INTEGER
        or type(document.get("ts")) is not int
        or document["ts"] < 0
        or document["ts"] > _REDIS_LUA_MAX_SAFE_INTEGER
        or not isinstance(document.get("path"), str)
        or not document["path"]
    ):
        raise ValueError("leaf has invalid version/ts/path")
    return document


class RbacAuditAttemptError(RuntimeError):
    """A non-mutating RBAC attempt could not be durably appended."""


class RbacAuditConditionConflict(ValueError):
    """The state justifying a no-change attempt changed before its append."""


class RbacDecisionError(ValueError):
    """A typed, non-ambiguous RBAC decision that requires audit evidence."""

    def __init__(
        self,
        message: str,
        *,
        outcome: str,
        cause: str,
        conditions: Optional[Sequence[Mapping[str, Any]]] = None,
        before_document: Optional[Mapping[str, Any]] = None,
        before_version: int = 0,
        severity: str = "warning",
    ) -> None:
        super().__init__(message)
        if outcome not in {"failure", "denied", "no_change"}:
            raise ValueError("typed RBAC decision has an invalid outcome")
        if outcome == "no_change" and not conditions:
            raise ValueError("typed RBAC no-change decisions require conditions")
        self.outcome = outcome
        self.cause = cause
        self.conditions = tuple(conditions or ())
        self.before_document = before_document
        self.before_version = before_version
        self.severity = severity


class RbacDuplicateIdentityError(RbacDecisionError):
    """A duplicate identity claim whose durable no-change event already exists."""


class RbacIntegrityError(RuntimeError):
    """Persisted RBAC state failed a deterministic integrity check."""


class DeletionIntentConflictError(RuntimeError):
    """A durable delete intent fences ordinary creation or mutation."""


class ReadOnlyCatalogError(PermissionError):
    """The atomic catalog boundary observed a non-writable root."""


_RBAC_ATTEMPT_IDENTITIES = {
    "role_create": ("role", "role"),
    "role_update": ("role", "role"),
    "role_delete": ("role", "role"),
    "user_create": ("user", "user"),
    "user_update": ("user", "user"),
    "user_delete": ("user", "user"),
    "user_role_assign": ("user_role_assignment", "user"),
    "user_role_remove": ("user_role_assignment", "user"),
    "token_create": ("auth_token", "token"),
    "token_delete": ("auth_token", "token"),
}

# State-dependent ``no_change`` evidence has a deliberately closed grammar.
# The public append API accepts only these semantic predicates; it derives all
# Redis keys from the audited scope/resource and converts them to the small
# low-level predicate language understood by Lua.  Exact ordered shapes keep a
# caller from combining individually valid predicates into a different claim.
_RBAC_NO_CHANGE_CONDITION_SHAPES = {
    ("role_create", "resource_already_exists"): {
        ("resource_exists",),
    },
    ("role_create", "identity_claim_unchanged"): {
        ("identity_claim",),
    },
    ("role_create", "idempotent_replay"): {
        ("identity_claim", "resource_fields"),
    },
    ("role_update", "resource_missing"): {
        ("resource_absent",),
    },
    ("role_update", "resource_disappeared"): {
        ("resource_absent",),
    },
    ("role_update", "identity_claim_unchanged"): {
        ("identity_claim",),
    },
    ("role_delete", "resource_missing"): {
        ("resource_absent",),
    },
    ("role_delete", "resource_disappeared"): {
        ("resource_absent",),
    },
    ("user_create", "resource_already_exists"): {
        ("resource_exists",),
    },
    ("user_create", "identity_claim_unchanged"): {
        ("identity_claim",),
    },
    ("user_create", "idempotent_replay"): {
        ("identity_claim", "resource_fields", "user_roles_equal"),
    },
    ("user_create", "resource_missing"): {
        ("role_absent",),
    },
    ("user_update", "resource_missing"): {
        ("resource_absent",),
        ("role_absent",),
    },
    ("user_update", "resource_disappeared"): {
        ("resource_absent",),
    },
    ("user_update", "identity_claim_unchanged"): {
        ("identity_claim",),
    },
    ("user_update", "empty_update"): {
        ("resource_fields",),
    },
    ("user_delete", "resource_missing"): {
        ("resource_absent",),
    },
    ("user_delete", "resource_disappeared"): {
        ("resource_absent",),
    },
    ("user_role_assign", "user_missing"): {
        ("assignment_user_absent",),
    },
    ("user_role_assign", "resource_missing"): {
        ("assignment_role_absent",),
    },
    ("user_role_assign", "role_already_assigned"): {
        ("assignment_membership",),
    },
    ("user_role_assign", "assignment_not_changed"): {
        ("assignment_membership",),
    },
    ("user_role_remove", "user_missing"): {
        ("assignment_user_absent",),
    },
    ("user_role_remove", "role_not_assigned"): {
        ("assignment_membership",),
    },
    ("user_role_remove", "assignment_not_changed"): {
        ("assignment_membership",),
    },
    ("token_create", "token_identity_collision"): {
        ("token_present",),
    },
    ("token_delete", "resource_missing"): {
        ("token_absent",),
    },
    ("token_delete", "resource_disappeared"): {
        ("token_absent",),
    },
}

_AUTH_AUDIT_SUPER_NAME = "_organization_"
_AUTH_TOKEN_VERSION_FIELD = "version"
_AUTH_TOKEN_META_FIELDS = frozenset({
    _AUTH_TOKEN_VERSION_FIELD,
    "last_updated_ms",
    "initialized",
    "_audit_initialized",
})
_AUTH_TOKEN_ID_RE = re.compile(r"^[0-9a-f]{64}$")

_RBAC_DETERMINISTIC_INTEGRITY_MARKERS = (
    "RBAC commit key has wrong Redis type",
    "RBAC/audit revision counter is corrupt",
    "RBAC/audit revision counter cannot be incremented safely",
    "RBAC namespace revision head is missing",
    "RBAC namespace revision counter is corrupt",
    "RBAC namespace revision counter is out of range",
    "RBAC user namespace revision counter is corrupt",
    "RBAC user namespace revision counter is out of range",
    "auth token audit marker is corrupt",
    "WRONGTYPE Operation against a key",
)


def _safe_rbac_audit_resource_id(*parts: Any, fallback: str) -> str:
    """Return a bounded, log-safe resource identity without leaking input."""

    try:
        value = ":".join(str(part) for part in parts)
    except Exception:
        return fallback
    if not value:
        return fallback
    if len(value) <= 512 and not any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        return value
    digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
    return f"{fallback}:sha256:{digest}"


def _rbac_decision(error: BaseException) -> tuple[
    str,
    str,
    Optional[Sequence[Mapping[str, Any]]],
    Optional[Mapping[str, Any]],
    int,
    str,
]:
    """Return trusted audit metadata without inspecting exception messages."""

    if isinstance(error, RbacDecisionError):
        return (
            error.outcome,
            error.cause,
            error.conditions or None,
            error.before_document,
            error.before_version,
            error.severity,
        )
    if isinstance(error, RbacIntegrityError):
        return "failure", "state_integrity_error", None, None, 0, "critical"
    if isinstance(error, PermissionError):
        return "denied", "authorization_denied", None, None, 0, "warning"
    return "denied", "request_rejected", None, None, 0, "warning"


def _rbac_absent_decision(
    message: str,
    *,
    cause: str = "resource_missing",
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="no_change",
        cause=cause,
        conditions=[{"kind": "resource_absent"}],
    )


def _rbac_role_absent_decision(
    message: str,
    role_id: str,
    *,
    cause: str = "resource_missing",
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="no_change",
        cause=cause,
        conditions=[{"kind": "role_absent", "role_id": role_id}],
    )


def _rbac_assignment_role_absent_decision(
    message: str,
    user_id: str,
    role_id: str,
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="no_change",
        cause="resource_missing",
        conditions=[{
            "kind": "assignment_role_absent",
            "user_id": user_id,
            "role_id": role_id,
        }],
    )


def _rbac_identity_decision(
    message: str,
    name: str,
    identity_id: str,
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="no_change",
        cause="identity_claim_unchanged",
        conditions=[{
            "kind": "identity_claim",
            "name": name,
            "identity_id": identity_id,
        }],
    )


def _rbac_failure_decision(
    message: str,
    *,
    cause: str = "concurrent_modification",
    severity: str = "warning",
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="failure",
        cause=cause,
        severity=severity,
    )


def _rbac_denied_decision(
    message: str,
    *,
    cause: str,
    severity: str = "warning",
) -> RbacDecisionError:
    return RbacDecisionError(
        message,
        outcome="denied",
        cause=cause,
        severity=severity,
    )


def _audit_catalog_rejections(
    *,
    action: str,
    resource_type: str,
    namespace: str,
    resource_fields: tuple[str, ...],
):
    """Durably audit known catalog request failures, never backend ambiguity."""

    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def guarded(self, *args, **kwargs):
            try:
                bound = signature.bind(self, *args, **kwargs)
            except TypeError:
                # Without a reliably bound organization/super-table scope no
                # durable tenant ledger can be selected.  Preserve Python's
                # normal call-contract error instead of guessing a scope.
                return function(self, *args, **kwargs)
            try:
                return function(self, *args, **kwargs)
            except redis.exceptions.ResponseError as error:
                if not any(
                    marker in str(error)
                    for marker in _RBAC_DETERMINISTIC_INTEGRITY_MARKERS
                ):
                    raise
                integrity_error = RbacIntegrityError(
                    "Persisted RBAC state failed an atomic integrity preflight"
                )
                resource_id = _safe_rbac_audit_resource_id(
                    *(bound.arguments.get(name, "") for name in resource_fields),
                    fallback=f"pending-{resource_type}",
                )
                try:
                    self.rbac_append_attempt(
                        bound.arguments["org"],
                        bound.arguments["sup"],
                        action=action,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        namespace=namespace,
                        outcome="failure",
                        cause="state_integrity_error",
                        action_context=bound.arguments.get("action_context"),
                        severity="critical",
                    )
                except Exception as audit_error:
                    raise audit_error from error
                self._rbac_mark_attempt_recorded(integrity_error)
                raise integrity_error from error
            except (
                RbacIntegrityError,
                ValueError,
                TypeError,
                PermissionError,
            ) as error:
                if self.rbac_attempt_was_recorded(error):
                    raise
                (
                    outcome,
                    cause,
                    conditions,
                    before_document,
                    before_version,
                    severity,
                ) = _rbac_decision(error)
                resource_id = _safe_rbac_audit_resource_id(
                    *(bound.arguments.get(name, "") for name in resource_fields),
                    fallback=f"pending-{resource_type}",
                )
                try:
                    self.rbac_append_attempt(
                        bound.arguments["org"],
                        bound.arguments["sup"],
                        action=action,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        namespace=namespace,
                        outcome=outcome,
                        cause=cause,
                        action_context=bound.arguments.get("action_context"),
                        before_document=before_document,
                        before_version=before_version,
                        severity=severity,
                        conditions=conditions,
                    )
                except Exception as audit_error:
                    raise audit_error from error
                self._rbac_mark_attempt_recorded(error)
                raise

        return guarded

    return decorate


def audit_rbac_manager_rejections(
    *,
    action: str,
    resource_type: str,
    namespace: str,
    resource_fields: tuple[str, ...] = (),
):
    """Guard public manager validation without duplicating catalog evidence."""

    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def guarded(self, *args, **kwargs):
            try:
                bound = signature.bind(self, *args, **kwargs)
            except TypeError:
                return function(self, *args, **kwargs)
            try:
                return function(self, *args, **kwargs)
            except (
                RbacIntegrityError,
                ValueError,
                TypeError,
                PermissionError,
            ) as error:
                catalog = self._catalog
                if catalog.rbac_attempt_was_recorded(error):
                    raise
                (
                    outcome,
                    cause,
                    conditions,
                    before_document,
                    before_version,
                    severity,
                ) = _rbac_decision(error)
                resource_id = _safe_rbac_audit_resource_id(
                    *(bound.arguments.get(name, "") for name in resource_fields),
                    fallback=f"pending-{resource_type}",
                )
                try:
                    catalog.rbac_append_attempt(
                        self.organization,
                        self.super_name,
                        action=action,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        namespace=namespace,
                        outcome=outcome,
                        cause=cause,
                        action_context=bound.arguments.get("action_context"),
                        before_document=before_document,
                        before_version=before_version,
                        severity=severity,
                        conditions=conditions,
                    )
                except Exception as audit_error:
                    raise audit_error from error
                catalog._rbac_mark_attempt_recorded(error)
                raise

        return guarded

    return decorate


# ---------------------------------------------------------------------------
# Role-name safety check
# ---------------------------------------------------------------------------
#
# Role names get interpolated into Redis keys, hash fields, and log lines, so
# the safe set is intentionally small: ASCII letters / digits + underscore,
# hyphen, dot, space. First character must be a letter or underscore so
# names can't be confused with numeric IDs or hidden-file-style paths.
#
# Lives on the *catalog* layer (not just RoleManager) so direct callers —
# admin scripts, migrations, tests using ``cat.rbac_create_role`` — can't
# bypass it. Two write paths, one rule.
SAFE_ROLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-. ]{0,126}$")


def validate_role_name(role_name: str) -> None:
    """Raise ``ValueError`` if ``role_name`` doesn't match :data:`SAFE_ROLE_NAME_RE`.

    Empty / ``None`` is a no-op — role_name is optional in the data model
    and absent names skip the lookup index. Only non-empty names get
    validated.
    """
    if role_name is None or role_name == "":
        return
    if not isinstance(role_name, str):
        raise ValueError("role_name must be a string")
    if not SAFE_ROLE_NAME_RE.fullmatch(role_name):
        raise ValueError(
            f"Invalid role_name: {role_name!r}. Must be 1-127 characters, "
            "start with a letter or underscore, contain only ASCII letters, "
            "digits, underscores, hyphens, dots, and spaces."
        )


# ---------------------------------------------------------------------------
# Username safety check
# ---------------------------------------------------------------------------
#
# Usernames have the same Redis-key / log-line interpolation risk as role
# names, plus the practical need to accept email-style identifiers when the
# IdP emits them (``alice@acme.com``). So the safe set is the role-name set
# minus space (CLIs and logs handle spaceless usernames much better) plus
# ``@`` for email-style logins.
#
# Same first-character rule: ASCII letter or underscore. Same length cap
# (1-127). Unlike role_name, an empty username is *always* invalid here
# because usernames are required at every call site.
SAFE_USERNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-.@]{0,126}$")


def validate_username(username: str) -> None:
    """Raise ``ValueError`` if ``username`` doesn't match :data:`SAFE_USERNAME_RE`.

    Empty / ``None`` raises — unlike role_name, username is required by
    every UserManager / catalog call site. Callers that genuinely don't
    want to update the username field must skip calling this helper.
    """
    if not username:
        raise ValueError("username is required and must be non-empty")
    if not SAFE_USERNAME_RE.fullmatch(username):
        raise ValueError(
            f"Invalid username: {username!r}. Must be 1-127 characters, "
            "start with a letter or underscore, contain only ASCII letters, "
            "digits, underscores, hyphens, dots, and the '@' character."
        )


def _decode_role_json_field(value: Any, *, field: str) -> Any:
    """Decode one catalog role field or reject corrupt persisted JSON."""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"role field {field!r} is not valid UTF-8") from exc
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"role field {field!r} is not valid JSON") from exc
    return value


def _canonicalize_role_document(
    role_data: Dict[str, Any],
    *,
    default_if_empty: bool,
) -> Dict[str, Any]:
    """Validate/canonicalise policy fields while preserving catalog metadata.

    Imported lazily to keep the low-level catalog module independent during
    package bootstrap.  Invalid persisted policies raise and are omitted by
    read methods, making corruption a denial rather than an implicit grant.
    """
    from supertable.rbac.row_column_security import (
        RowColumnSecurity,
        canonicalize_role_tables,
    )

    if not isinstance(role_data, dict):
        raise ValueError("role document must be an object")
    document = dict(role_data)

    role_type = document.get("role")
    if isinstance(role_type, bytes):
        try:
            role_type = role_type.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("role type is not valid UTF-8") from exc

    raw_tables = document.get("tables")
    if isinstance(raw_tables, (str, bytes)):
        raw_tables = _decode_role_json_field(raw_tables, field="tables")
    tables = canonicalize_role_tables(
        raw_tables,
        default_if_empty=default_if_empty,
        allow_legacy_list=True,
    )

    rcs = RowColumnSecurity(role=role_type, tables=tables)
    # Do not call prepare(): its public creation compatibility default would
    # turn a read-time empty policy into wildcard access.
    rcs.tables = tables
    rcs.create_content_hash()
    document["role"] = rcs.role.value
    document["tables"] = tables
    document["content_hash"] = rcs.content_hash

    role_name = document.get("role_name", "")
    if isinstance(role_name, bytes):
        try:
            role_name = role_name.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("role_name is not valid UTF-8") from exc
        document["role_name"] = role_name
    validate_role_name(role_name)
    if role_name and role_name.casefold() == "superadmin":
        if rcs.role.value != "superadmin":
            raise ValueError("The reserved 'superadmin' name requires superadmin type")
    return document


# All Redis key strings are constructed via `supertable.redis_keys` (RK).
# This module deliberately contains no `f"supertable:..."` string literals;
# any new key must be added to redis_keys.py first.


class RedisCatalog:
    """
    Redis-backed catalog for SuperTable:
      * meta:root -> {"version": int, "ts": epoch_ms}
      * meta:leaf:{simple} -> {"version": int, "ts": epoch_ms, "path": ".../snapshot.json"}
      * meta:mirrors -> {"formats": [...], "ts": epoch_ms}
      * lock:leaf:{simple} -> token (SET NX EX)
      * lock:stat -> token (SET NX EX)  # for monitoring stats updates
    """

    # One role deletion is deliberately bounded so Redis is not blocked by an
    # unbounded evidence fan-out.  Larger revocations must be split by an
    # operator-controlled migration before the role can be deleted.
    _RBAC_CASCADE_MANIFEST_USER_LIMIT = 10_000
    _RBAC_ATTEMPT_CONDITION_LIMIT = 16
    _RBAC_ATTEMPT_CONDITION_BYTES_LIMIT = 16_384
    _QUALITY_MODES = ("quick", "deep", "custom")
    _QUALITY_DYNAMIC_SCAN_COUNT = 256
    _QUALITY_DYNAMIC_KEY_LIMIT = 100_000
    _QUALITY_DYNAMIC_SCAN_CALL_LIMIT = 100_000
    # DataWriter may pass ``quality_generation`` to ``commit_snapshot`` only
    # when a catalog advertises this capability.  Third-party/test adapters
    # without it retain the post-commit compatibility path.
    supports_atomic_quality_generation = True
    # DataWriter may pass the exact raw mirror configuration returned by
    # begin_table_mutation to select the no-mirror commit hot path.
    supports_pinned_no_mirror_commit = True
    # A writer that observed an absent leaf may acquire the namespace lock
    # before its table lock and pass that exact token to begin_table_mutation.
    # The fused boundary can then reserve the first row-id range without first
    # publishing an empty bootstrap snapshot.
    supports_one_shot_table_creation = True
    _STAGE_LOCK_SCAN_COUNT = 256
    _STAGE_LOCK_DRAIN_LIMIT = 10_000
    _STAGE_LOCK_SCAN_CALL_LIMIT = 100_000
    _LEAF_LOCK_SCAN_COUNT = 256
    _LEAF_LOCK_DRAIN_LIMIT = 10_000
    _LEAF_LOCK_SCAN_CALL_LIMIT = 100_000

    # ------------- Lua sources -------------
    _LUA_LEAF_CAS_SET = _LUA_ROOT_DOCUMENT_GUARD + """
local key = KEYS[1]
local namespace_lock = KEYS[2]
local table_names = KEYS[3]
local namespace_delete = KEYS[4]
local simple_delete = KEYS[5]
local root_key = KEYS[6]
local new_path = ARGV[1]
local now_ms = tonumber(ARGV[2])
local namespace_token = ARGV[3]
local simple_name = ARGV[4]

if not now_ms or now_ms < 0 or now_ms > ROOT_MAX_SAFE_INTEGER
    or now_ms ~= math.floor(now_ms) then return -8 end

local namespace_holder = redis.call('GET', namespace_lock)
if namespace_holder and namespace_holder ~= namespace_token then
  return -2
end
if redis.call('EXISTS', namespace_delete) == 1 then return -3 end
if redis.call('EXISTS', simple_delete) == 1 then return -4 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -5 end
if root_type ~= 'string' then return -6 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -6 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -6 end
if root_state == 0 then return -7 end
local cur = redis.call('GET', key)
if cur then
  return -1
end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then
  return redis.error_reply('table-name index has wrong Redis type')
end
local new_val = cjson.encode({version=0, ts=now_ms, path=new_path})
redis.call('SET', key, new_val)
redis.call('SADD', table_names, simple_name)
return 0
"""

    _LUA_LEAF_PAYLOAD_CAS_SET = _LUA_ROOT_DOCUMENT_GUARD + """
local key = KEYS[1]
local namespace_lock = KEYS[2]
local table_names = KEYS[3]
local namespace_delete = KEYS[4]
local simple_delete = KEYS[5]
local root_key = KEYS[6]
local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])
local namespace_token = ARGV[4]
local simple_name = ARGV[5]
local not_after_ms = tonumber(ARGV[6] or '0')

if not now_ms or now_ms < 0 or now_ms > ROOT_MAX_SAFE_INTEGER
    or now_ms ~= math.floor(now_ms) then return -8 end
if not not_after_ms or not_after_ms < 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -9 end
if publication_deadline_exceeded(not_after_ms) then return -9 end

local namespace_holder = redis.call('GET', namespace_lock)
if namespace_holder and namespace_holder ~= namespace_token then
  return -2
end
if redis.call('EXISTS', namespace_delete) == 1 then return -3 end
if redis.call('EXISTS', simple_delete) == 1 then return -4 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -5 end
if root_type ~= 'string' then return -6 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -6 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -6 end
if root_state == 0 then return -7 end
local cur = redis.call('GET', key)
if cur then
  return -1
end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then
  return redis.error_reply('table-name index has wrong Redis type')
end

local payload = {}
local okp, pobj = pcall(cjson.decode, payload_json)
if okp and pobj then
  payload = pobj
end

local new_val = cjson.encode({version=0, ts=now_ms, path=new_path, payload=payload})
-- Re-sample Redis TIME after all validation/decoding.  Entry admission alone
-- cannot authorize a mutation that finishes preparing after its fence.
if publication_deadline_exceeded(not_after_ms) then return -9 end
redis.call('SET', key, new_val)
redis.call('SADD', table_names, simple_name)
return 0
"""

    _LUA_ALLOCATE_LINKED_PUBLICATION = _LUA_ROOT_DOCUMENT_GUARD + """
local generation_key = KEYS[1]
local server_time = redis.call('TIME')
local seconds = tonumber(server_time[1])
local micros = tonumber(server_time[2])
local server_ms = seconds * 1000 + math.floor(micros / 1000)
local clock_generation = seconds * 1000000 + micros
if clock_generation > ROOT_MAX_SAFE_INTEGER then return {-1} end
local current = tonumber(redis.call('GET', generation_key) or '0')
if not current or current < 0 or current > ROOT_MAX_SAFE_INTEGER
    or current ~= math.floor(current) then return {-2} end
local generation = clock_generation
if generation <= current then generation = current + 1 end
if generation > ROOT_MAX_SAFE_INTEGER then return {-1} end
redis.call('SET', generation_key, tostring(generation))
return {generation, server_ms}
"""

    _LUA_ALLOCATE_SHARE_MANIFEST_GENERATION = """
local generation_key = KEYS[1]
local server_time = redis.call('TIME')
local server_ms = tonumber(server_time[1]) * 1000
    + math.floor(tonumber(server_time[2]) / 1000)
if not server_ms or server_ms <= 0 or server_ms > 9007199254740991 then
  return -1
end
local current = tonumber(redis.call('GET', generation_key) or '0')
if not current or current < 0 or current > 9007199254740991
    or current ~= math.floor(current) then return -2 end
local generation = server_ms
if generation <= current then generation = current + 1 end
if generation > 9007199254740991 then return -1 end
redis.call('SET', generation_key, tostring(generation))
return generation
"""

    _LUA_RESERVE_LINKED_PROVIDER_PUBLICATION = _LUA_ROOT_DOCUMENT_GUARD + """
local reservation_key = KEYS[1]
local unlink_tombstone = KEYS[2]
local namespace_delete = KEYS[3]
local root_key = KEYS[4]
local provider_generation = tonumber(ARGV[1])
local manifest_digest = ARGV[2]
local local_generation = tonumber(ARGV[3])
local not_after_ms = tonumber(ARGV[4])
local instance_nonce = ARGV[5]

if not provider_generation or provider_generation <= 0
    or provider_generation > ROOT_MAX_SAFE_INTEGER
    or provider_generation ~= math.floor(provider_generation) then return -8 end
if not linked_manifest_digest_ok(manifest_digest)
    or not linked_instance_nonce_ok(instance_nonce) then return -8 end
if not local_generation or local_generation <= 0
    or local_generation > ROOT_MAX_SAFE_INTEGER
    or local_generation ~= math.floor(local_generation) then return -8 end
if not not_after_ms or not_after_ms <= 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -9 end
if publication_deadline_exceeded(not_after_ms) then return -9 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -1 end
if redis.call('EXISTS', namespace_delete) == 1 then return -2 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -3 end
if root_type ~= 'string' then return -4 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -4 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -4 end
if root_state == 0 then return -5 end

local reservation_type = redis.call('TYPE', reservation_key)
if type(reservation_type) == 'table' then reservation_type = reservation_type['ok'] end
if reservation_type ~= 'none' and reservation_type ~= 'string' then return -6 end
if reservation_type == 'string' then
  local current_ok, current = pcall(
      cjson.decode, redis.call('GET', reservation_key)
  )
  if not current_ok or type(current) ~= 'table' then return -6 end
  local current_provider = tonumber(current['provider_generated_ms'])
  local current_local = tonumber(current['publication_generation'])
  local current_digest = current['manifest_digest']
  local current_state = current['state']
  local current_instance = current['instance_nonce']
  if not current_provider or current_provider <= 0
      or current_provider > ROOT_MAX_SAFE_INTEGER
      or current_provider ~= math.floor(current_provider)
      or not current_local or current_local <= 0
      or current_local > ROOT_MAX_SAFE_INTEGER
      or current_local ~= math.floor(current_local)
      or not linked_manifest_digest_ok(current_digest)
      or (current_state ~= 'preparing' and current_state ~= 'committed') then
    return -6
  end
  -- Legacy reservations predate instance nonces and can be upgraded by the
  -- first v2 refresh. Once present, the nonce is immutable for this link.
  if current_instance ~= nil and current_instance ~= instance_nonce then
    return -12
  end
  if current_provider > provider_generation then return -10 end
  if current_provider == provider_generation then
    if current_digest ~= manifest_digest then return -11 end
    if current_state == 'committed' then return 0 end
    if current_local > local_generation then return -10 end
    if current_local == local_generation then return 1 end
  end
end

local reservation = cjson.encode({
  provider_generated_ms=provider_generation,
  manifest_digest=manifest_digest,
  publication_generation=local_generation,
  instance_nonce=instance_nonce,
  state='preparing',
})
if publication_deadline_exceeded(not_after_ms) then return -9 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -1 end
redis.call('SET', reservation_key, reservation)
return 1
"""

    _LUA_ABORT_LINKED_PROVIDER_PUBLICATION = _LUA_ROOT_DOCUMENT_GUARD + """
local document_key = KEYS[1]
local index_key = KEYS[2]
local reservation_key = KEYS[3]
local unlink_tombstone = KEYS[4]
local namespace_delete = KEYS[5]
local root_key = KEYS[6]
local link_id = ARGV[1]
local instance_nonce = ARGV[2]

if not linked_instance_nonce_ok(instance_nonce) then return -8 end
if redis.call('EXISTS', namespace_delete) == 1 then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -2 end
if root_type ~= 'string' then return -3 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -3 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -3 end
if root_state == 0 then return -4 end

local document_type = redis.call('TYPE', document_key)
if type(document_type) == 'table' then document_type = document_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
local reservation_type = redis.call('TYPE', reservation_key)
if type(reservation_type) == 'table' then reservation_type = reservation_type['ok'] end
local tombstone_type = redis.call('TYPE', unlink_tombstone)
if type(tombstone_type) == 'table' then tombstone_type = tombstone_type['ok'] end
if (document_type ~= 'none' and document_type ~= 'string')
    or (index_type ~= 'none' and index_type ~= 'set')
    or reservation_type ~= 'string'
    or (tombstone_type ~= 'none' and tombstone_type ~= 'string') then
  return -5
end

if tombstone_type == 'string' then
  local tombstone_ok, tombstone = pcall(
      cjson.decode, redis.call('GET', unlink_tombstone)
  )
  if not tombstone_ok or type(tombstone) ~= 'table'
      or tombstone['link_id'] ~= link_id
      or (tombstone['state'] ~= 'deleting'
          and tombstone['state'] ~= 'deleted') then return -5 end
  return 2
end

local reservation_ok, reservation = pcall(
    cjson.decode, redis.call('GET', reservation_key)
)
if not reservation_ok or type(reservation) ~= 'table'
    or (reservation['state'] ~= 'preparing'
        and reservation['state'] ~= 'committed')
    or reservation['instance_nonce'] ~= instance_nonce then
  return -6
end

local indexed = redis.call('SISMEMBER', index_key, link_id)
local link_document = nil
if document_type == 'none' then
  if indexed ~= 0 then return -5 end
else
  if indexed ~= 1 then return -5 end
  local document_ok, document = pcall(
      cjson.decode, redis.call('GET', document_key)
  )
  if not document_ok or type(document) ~= 'table'
      or document['link_id'] ~= link_id
      or document['_linked_instance_nonce'] ~= instance_nonce then
    return -6
  end
  link_document = document
end

local tombstone = {
  link_id=link_id,
  state='deleting',
  aborted_publication={
    instance_nonce=instance_nonce,
  },
}
if link_document ~= nil then tombstone['link_doc'] = link_document end
redis.call('SET', unlink_tombstone, cjson.encode(tombstone))
redis.call('DEL', document_key)
redis.call('SREM', index_key, link_id)
redis.call('SET', reservation_key, cjson.encode({
  instance_nonce=instance_nonce,
  state='aborted',
}))
return 1
"""

    _LUA_UPSERT_LINKED_LEAF = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_key = KEYS[1]
local namespace_lock = KEYS[2]
local table_names = KEYS[3]
local namespace_delete = KEYS[4]
local simple_delete = KEYS[5]
local root_key = KEYS[6]
local reservation_key = KEYS[7]
local linked_leaf_names = KEYS[8]
local unlink_tombstone = KEYS[9]
local payload_json = ARGV[1]
local new_path = ARGV[2]
local simple_name = ARGV[3]
local link_id = ARGV[4]
local generation = tonumber(ARGV[5])
local not_after_ms = tonumber(ARGV[6])

if not generation or generation <= 0 or generation > ROOT_MAX_SAFE_INTEGER
    or generation ~= math.floor(generation) then return -12 end
if not not_after_ms or not_after_ms <= 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -13 end
if publication_deadline_exceeded(not_after_ms) then return -13 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -17 end
local server_time = redis.call('TIME')
local server_ms = tonumber(server_time[1]) * 1000
    + math.floor(tonumber(server_time[2]) / 1000)
if redis.call('EXISTS', namespace_lock) == 1 then return -3 end
if redis.call('EXISTS', namespace_delete) == 1 then return -4 end
if redis.call('EXISTS', simple_delete) == 1 then return -5 end

local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -6 end
if root_type ~= 'string' then return -7 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -7 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -7 end
if root_state == 0 then return -8 end

local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then return -9 end
local linked_names_type = redis.call('TYPE', linked_leaf_names)
if type(linked_names_type) == 'table' then linked_names_type = linked_names_type['ok'] end
if linked_names_type ~= 'none' and linked_names_type ~= 'set' then return -9 end
if not string.match(payload_json, '^%s*{') then return -10 end
local payload_ok, payload = pcall(cjson.decode, payload_json)
if not payload_ok or type(payload) ~= 'table'
    or payload['_linked_share'] ~= link_id
    or tonumber(payload['_linked_generation']) ~= generation then return -10 end
local provider_generation = tonumber(payload['_linked_provider_generated_ms'])
local manifest_digest = payload['_linked_provider_manifest_digest']
if not provider_generation or provider_generation <= 0
    or provider_generation > ROOT_MAX_SAFE_INTEGER
    or provider_generation ~= math.floor(provider_generation)
    or not linked_manifest_digest_ok(manifest_digest) then return -10 end

local reservation_type = redis.call('TYPE', reservation_key)
if type(reservation_type) == 'table' then reservation_type = reservation_type['ok'] end
if reservation_type ~= 'string' then return -18 end
local reservation_ok, reservation = pcall(
    cjson.decode, redis.call('GET', reservation_key)
)
if not reservation_ok or type(reservation) ~= 'table'
    or reservation['state'] ~= 'preparing'
    or tonumber(reservation['provider_generated_ms']) ~= provider_generation
    or reservation['manifest_digest'] ~= manifest_digest
    or tonumber(reservation['publication_generation']) ~= generation then
  return -18
end

local version = 0
local current_raw = redis.call('GET', leaf_key)
if current_raw then
  local current_ok, current = pcall(cjson.decode, current_raw)
  if not current_ok or type(current) ~= 'table'
      or type(current['payload']) ~= 'table' then return -11 end
  local current_link = current['payload']['_linked_share']
  if current_link == nil then return -1 end
  if current_link ~= link_id then return -2 end
  local current_generation = tonumber(
      current['payload']['_linked_generation'] or '0'
  )
  if not current_generation or current_generation < 0
      or current_generation > ROOT_MAX_SAFE_INTEGER
      or current_generation ~= math.floor(current_generation) then return -11 end
  local current_provider = tonumber(
      current['payload']['_linked_provider_generated_ms'] or '0'
  )
  if not current_provider or current_provider < 0
      or current_provider > ROOT_MAX_SAFE_INTEGER
      or current_provider ~= math.floor(current_provider) then return -11 end
  if current_provider > provider_generation then return -15 end
  if current_provider == provider_generation and current_provider > 0 then
    local current_digest = current['payload']['_linked_provider_manifest_digest']
    if current_digest ~= manifest_digest then return -16 end
  end
  if current_provider == provider_generation
      and current_generation > generation then return -14 end
  local current_version = tonumber(current['version'])
  if not current_version or current_version < 0
      or current_version >= ROOT_MAX_SAFE_INTEGER
      or current_version ~= math.floor(current_version) then return -11 end
  version = current_version + 1
end

local new_value = cjson.encode({
  version=version,
  ts=server_ms,
  path=new_path,
  payload=payload,
})
if publication_deadline_exceeded(not_after_ms) then return -13 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -17 end
redis.call('SET', leaf_key, new_value)
redis.call('SADD', table_names, simple_name)
redis.call('SADD', linked_leaf_names, simple_name)
return current_raw and 2 or 1
"""

    _LUA_DELETE_LINKED_LEAF = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_key = KEYS[1]
local table_names = KEYS[2]
local namespace_delete = KEYS[3]
local root_key = KEYS[4]
local linked_leaf_names = KEYS[5]
local simple_name = ARGV[1]
local link_id = ARGV[2]
local expected_generation = tonumber(ARGV[3])
local not_after_ms = tonumber(ARGV[4] or '0')

if not expected_generation or expected_generation < 0
    or expected_generation > ROOT_MAX_SAFE_INTEGER
    or expected_generation ~= math.floor(expected_generation) then return -8 end
if not not_after_ms or not_after_ms < 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -9 end
if publication_deadline_exceeded(not_after_ms) then return -9 end
if redis.call('EXISTS', namespace_delete) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -4 end
if root_type ~= 'string' then return -5 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -5 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -5 end
if root_state == 0 then return -6 end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then return -7 end
local linked_names_type = redis.call('TYPE', linked_leaf_names)
if type(linked_names_type) == 'table' then linked_names_type = linked_names_type['ok'] end
if linked_names_type ~= 'none' and linked_names_type ~= 'set' then return -7 end

local current_raw = redis.call('GET', leaf_key)
if not current_raw then return 0 end
local current_ok, current = pcall(cjson.decode, current_raw)
if not current_ok or type(current) ~= 'table'
    or type(current['payload']) ~= 'table' then return -5 end
if current['payload']['_linked_share'] ~= link_id then
  redis.call('SREM', linked_leaf_names, simple_name)
  return 0
end
local current_generation = tonumber(
    current['payload']['_linked_generation'] or '0'
)
if not current_generation or current_generation ~= expected_generation then
  return -2
end
if publication_deadline_exceeded(not_after_ms) then return -9 end
redis.call('DEL', leaf_key)
redis.call('SREM', table_names, simple_name)
redis.call('SREM', linked_leaf_names, simple_name)
return 1
"""

    _LUA_DELETE_STALE_LINKED_LEAF = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_key = KEYS[1]
local table_names = KEYS[2]
local linked_leaf_names = KEYS[3]
local namespace_delete = KEYS[4]
local root_key = KEYS[5]
local reservation_key = KEYS[6]
local unlink_tombstone = KEYS[7]
local simple_name = ARGV[1]
local link_id = ARGV[2]
local provider_generation = tonumber(ARGV[3])
local manifest_digest = ARGV[4]
local local_generation = tonumber(ARGV[5])
local not_after_ms = tonumber(ARGV[6])

if not provider_generation or provider_generation <= 0
    or provider_generation > ROOT_MAX_SAFE_INTEGER
    or provider_generation ~= math.floor(provider_generation)
    or not linked_manifest_digest_ok(manifest_digest)
    or not local_generation or local_generation <= 0
    or local_generation > ROOT_MAX_SAFE_INTEGER
    or local_generation ~= math.floor(local_generation) then return -8 end
if not not_after_ms or not_after_ms <= 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -9 end
if publication_deadline_exceeded(not_after_ms) then return -9 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -10 end
if redis.call('EXISTS', namespace_delete) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -4 end
if root_type ~= 'string' then return -5 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -5 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -5 end
if root_state == 0 then return -6 end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
local linked_names_type = redis.call('TYPE', linked_leaf_names)
if type(linked_names_type) == 'table' then linked_names_type = linked_names_type['ok'] end
if (names_type ~= 'none' and names_type ~= 'set')
    or (linked_names_type ~= 'none' and linked_names_type ~= 'set') then
  return -7
end
local reservation_ok, reservation = pcall(
    cjson.decode, redis.call('GET', reservation_key) or ''
)
if not reservation_ok or type(reservation) ~= 'table'
    or reservation['state'] ~= 'preparing'
    or tonumber(reservation['provider_generated_ms']) ~= provider_generation
    or reservation['manifest_digest'] ~= manifest_digest
    or tonumber(reservation['publication_generation']) ~= local_generation then
  return -11
end
local current_raw = redis.call('GET', leaf_key)
if not current_raw then
  redis.call('SREM', linked_leaf_names, simple_name)
  return 0
end
local current_ok, current = pcall(cjson.decode, current_raw)
if not current_ok or type(current) ~= 'table'
    or type(current['payload']) ~= 'table' then return -5 end
if current['payload']['_linked_share'] ~= link_id then
  redis.call('SREM', linked_leaf_names, simple_name)
  return 0
end
local current_provider = tonumber(
    current['payload']['_linked_provider_generated_ms'] or '0'
)
if not current_provider or current_provider < 0
    or current_provider > ROOT_MAX_SAFE_INTEGER
    or current_provider ~= math.floor(current_provider) then return -5 end
if current_provider >= provider_generation then return 0 end
if publication_deadline_exceeded(not_after_ms) then return -9 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -10 end
redis.call('DEL', leaf_key)
redis.call('SREM', table_names, simple_name)
redis.call('SREM', linked_leaf_names, simple_name)
return 1
"""

    _LUA_DELETE_UNLINKED_LEAF = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_key = KEYS[1]
local table_names = KEYS[2]
local linked_leaf_names = KEYS[3]
local unlink_tombstone = KEYS[4]
local root_key = KEYS[5]
local simple_name = ARGV[1]
local link_id = ARGV[2]

local tombstone_ok, tombstone = pcall(
    cjson.decode, redis.call('GET', unlink_tombstone) or ''
)
if not tombstone_ok or type(tombstone) ~= 'table'
    or tombstone['link_id'] ~= link_id
    or (tombstone['state'] ~= 'deleting'
        and tombstone['state'] ~= 'deleted') then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -2 end
if root_type ~= 'string' then return -3 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table'
    or root_document_state(root, nil) == -1 then return -3 end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
local linked_names_type = redis.call('TYPE', linked_leaf_names)
if type(linked_names_type) == 'table' then linked_names_type = linked_names_type['ok'] end
if (names_type ~= 'none' and names_type ~= 'set')
    or (linked_names_type ~= 'none' and linked_names_type ~= 'set') then
  return -4
end
local current_raw = redis.call('GET', leaf_key)
if not current_raw then
  redis.call('SREM', linked_leaf_names, simple_name)
  return 0
end
local current_ok, current = pcall(cjson.decode, current_raw)
if not current_ok or type(current) ~= 'table'
    or type(current['payload']) ~= 'table' then return -3 end
if current['payload']['_linked_share'] ~= link_id then
  -- The indexed name is stale, but the table now belongs to a local table or
  -- another link. Detach only this link's bookkeeping; never delete the leaf.
  redis.call('SREM', linked_leaf_names, simple_name)
  return 0
end
redis.call('DEL', leaf_key)
redis.call('SREM', table_names, simple_name)
redis.call('SREM', linked_leaf_names, simple_name)
return 1
"""

    _LUA_GET_REPLICA_LEAF = _LUA_ROOT_DOCUMENT_GUARD + """
local target_root_key = KEYS[1]
local target_intent = KEYS[2]
local source_root_key = KEYS[3]
local source_intent = KEYS[4]
local source_leaf_key = KEYS[5]
local expected_target_raw = ARGV[1]
local target_super = ARGV[2]
local source_super = ARGV[3]

if redis.call('GET', target_root_key) ~= expected_target_raw then return {-1} end
if redis.call('EXISTS', target_intent) == 1 then return {-2} end
if redis.call('EXISTS', source_intent) == 1 then return {-3} end
local target_ok, target = pcall(cjson.decode, expected_target_raw)
if not target_ok or root_document_state(target, target_super) == -1 then
  return {-4}
end
if target['clone_type'] ~= 'replica'
    or target['cloned_from'] ~= source_super then return {-1} end
local source_raw = redis.call('GET', source_root_key)
if not source_raw then return {-5} end
local source_ok, source = pcall(cjson.decode, source_raw)
if not source_ok or root_document_state(source, source_super) == -1 then
  return {-6}
end
if source['clone_type'] == 'replica' then return {-6} end
local leaf_raw = redis.call('GET', source_leaf_key)
if not leaf_raw then return {0} end
return {1, leaf_raw}
"""

    _LUA_DELETE_SIMPLE_TABLE = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf = KEYS[1]
local leaf_lock = KEYS[2]
local table_names = KEYS[3]
local schema = KEYS[4]
local rowid = KEYS[5]
local table_config = KEYS[6]
local mirror_publication = KEYS[7]
local root = KEYS[8]
local simple_delete = KEYS[9]
local simple_delete_index = KEYS[10]
local namespace_lock = KEYS[11]
local namespace_delete = KEYS[12]
local expected_leaf_token = ARGV[1]
local simple_name = ARGV[2]
local expected_namespace_token = ARGV[3]
local expected_intent_id = ARGV[4]
local now_ms = tonumber(ARGV[5])

if expected_leaf_token == ''
    or redis.call('GET', leaf_lock) ~= expected_leaf_token then
  return -1
end
if expected_namespace_token == ''
    or redis.call('GET', namespace_lock) ~= expected_namespace_token then
  return -4
end
if redis.call('EXISTS', namespace_delete) == 1 then return -5 end
local intent_raw = redis.call('GET', simple_delete)
if not intent_raw then return -6 end
local intent_ok, intent = pcall(cjson.decode, intent_raw)
if not intent_ok or type(intent) ~= 'table' then return -7 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['leaf_lock_token'] or '') ~= expected_leaf_token
    or tostring(intent['namespace_lock_token'] or '')
        ~= expected_namespace_token then
  return -6
end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then return -2 end
local deletion_index_type = redis.call('TYPE', simple_delete_index)
if type(deletion_index_type) == 'table' then
  deletion_index_type = deletion_index_type['ok']
end
if deletion_index_type ~= 'none' and deletion_index_type ~= 'set' then
  return -8
end

-- Recovery can finish after a parent cleanup has already removed the root.
-- When the root is still present, however, validate and recheck writability at
-- this exact finalization boundary before mutating it.
local root_doc = nil
local root_raw = redis.call('GET', root)
if root_raw then
  local root_ok
  root_ok, root_doc = pcall(cjson.decode, root_raw)
  if not root_ok then return -3 end
  local root_state = root_document_state(root_doc, nil)
  if root_state == -1 then return -3 end
  if root_state == 0 then return -9 end
end

local removed = redis.call(
  'DEL', leaf, schema, rowid, table_config, mirror_publication
)
-- Remaining keys are the finite mutable quality state owned by this table.
-- Delivered history and the shared durable history outbox are immutable audit
-- records, not recreation inputs, and intentionally survive table deletion.
for index = 13, #KEYS do
  removed = removed + redis.call('DEL', KEYS[index])
end
removed = removed + redis.call('SREM', table_names, simple_name)
if removed > 0 and root_doc ~= nil then
  root_doc['version'] = root_doc['version'] + 1
  root_doc['ts'] = now_ms
  redis.call('SET', root, cjson.encode(root_doc))
end
intent['status'] = 'deleted'
intent['deleted_at_ms'] = now_ms
redis.call('SET', simple_delete, cjson.encode(intent))
redis.call('SADD', simple_delete_index, simple_name)
return removed + 1
"""

    _LUA_DELETE_SIMPLE_QUALITY_KEYS = """
local namespace_lock = KEYS[1]
local leaf_lock = KEYS[2]
local namespace_intent = KEYS[3]
local simple_intent = KEYS[4]
local namespace_token = ARGV[1]
local leaf_token = ARGV[2]
local expected_intent_id = ARGV[3]
local expected_prefix = ARGV[4]
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then return -1 end
if leaf_token == ''
    or redis.call('GET', leaf_lock) ~= leaf_token then return -1 end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
local raw = redis.call('GET', simple_intent)
if not raw then return -2 end
local intent_ok, intent = pcall(cjson.decode, raw)
if not intent_ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['namespace_lock_token'] or '') ~= namespace_token
    or tostring(intent['leaf_lock_token'] or '') ~= leaf_token
    or (tostring(intent['status'] or '') ~= 'deleting'
        and tostring(intent['status'] or '') ~= 'deleted') then return -2 end
local removed = 0
for index = 5, #KEYS do
  if string.sub(KEYS[index], 1, string.len(expected_prefix))
      ~= expected_prefix then return -4 end
  removed = removed + redis.call('DEL', KEYS[index])
end
return removed
"""

    _LUA_ROOT_ENSURE = """
local ROOT_MAX_SAFE_INTEGER = 9007199254740991
local root_key = KEYS[1]
local namespace_lock = KEYS[2]
local namespace_delete = KEYS[3]
local initial_json = ARGV[1]
local namespace_token = ARGV[2]
local initial_ok, initial = pcall(cjson.decode, initial_json)
if not initial_ok or type(initial) ~= 'table' then return -3 end
if type(initial['version']) ~= 'number'
    or initial['version'] < 0
    or initial['version'] > ROOT_MAX_SAFE_INTEGER
    or initial['version'] ~= math.floor(initial['version'])
    or type(initial['ts']) ~= 'number'
    or initial['ts'] < 0
    or initial['ts'] > ROOT_MAX_SAFE_INTEGER
    or initial['ts'] ~= math.floor(initial['ts']) then return -3 end
local namespace_holder = redis.call('GET', namespace_lock)
if namespace_holder and namespace_holder ~= namespace_token then
  return -1
end
if redis.call('EXISTS', namespace_delete) == 1 then return -2 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type ~= 'none' and root_type ~= 'string' then return -3 end
if root_type == 'string' then
  local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
  if not root_ok or type(root) ~= 'table' then return -3 end
  if type(root['version']) ~= 'number'
      or root['version'] < 0
      or root['version'] > ROOT_MAX_SAFE_INTEGER
      or root['version'] ~= math.floor(root['version'])
      or type(root['ts']) ~= 'number'
      or root['ts'] < 0
      or root['ts'] > ROOT_MAX_SAFE_INTEGER
      or root['ts'] ~= math.floor(root['ts']) then return -3 end
  return 0
end
redis.call('SET', root_key, initial_json)
return 1
"""

    _LUA_UPDATE_ROOT_FLAGS = _LUA_ROOT_DOCUMENT_GUARD + """
local root_key = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent_index = KEYS[3]
local stage_intent_index = KEYS[4]
local flags_json = ARGV[1]
local now_ms = tonumber(ARGV[2])
local target_super = ARGV[3]
local expected_root_raw = ARGV[4]

if redis.call('EXISTS', namespace_intent) == 1 then return -1 end
local simple_index_type = redis.call('TYPE', simple_intent_index)
if type(simple_index_type) == 'table' then simple_index_type = simple_index_type['ok'] end
local stage_index_type = redis.call('TYPE', stage_intent_index)
if type(stage_index_type) == 'table' then stage_index_type = stage_index_type['ok'] end
if simple_index_type ~= 'none' and simple_index_type ~= 'set' then return -3 end
if stage_index_type ~= 'none' and stage_index_type ~= 'set' then return -3 end
if redis.call('SCARD', simple_intent_index) ~= 0
    or redis.call('SCARD', stage_intent_index) ~= 0 then return -6 end
local flags_ok, flags = pcall(cjson.decode, flags_json)
if not flags_ok or type(flags) ~= 'table' then return -2 end
for key, _ in pairs(flags) do
  if type(key) ~= 'string' then return -2 end
end
if flags['version'] ~= nil or flags['ts'] ~= nil
    or flags['commit_id'] ~= nil then return -2 end
local raw = redis.call('GET', root_key)
if not raw then return -4 end
local root_ok, root = pcall(cjson.decode, raw)
if not root_ok or root_document_state(root, target_super) == -1 then return -3 end
if raw ~= expected_root_raw then return -5 end
for key, value in pairs(flags) do root[key] = value end
if root_document_state(root, target_super) == -1 then return -2 end
redis.call('SET', root_key, cjson.encode(root))
return 1
"""

    _LUA_SET_MIRRORS = _LUA_ROOT_DOCUMENT_GUARD + """
local mirrors_key = KEYS[1]
local namespace_intent = KEYS[2]
local root_key = KEYS[3]
local document_json = ARGV[1]
if redis.call('EXISTS', namespace_intent) == 1 then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -3 end
if root_type ~= 'string' then return -4 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -4 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -4 end
if root_state == 0 then return -5 end
local ok, document = pcall(cjson.decode, document_json)
if not ok or type(document) ~= 'table'
    or type(document['formats']) ~= 'table'
    or type(document['ts']) ~= 'number'
    or document['ts'] < 0
    or document['ts'] ~= math.floor(document['ts']) then return -2 end
redis.call('SET', mirrors_key, document_json)
return 1
"""

    _LUA_MUTATE_MIRROR = _LUA_ROOT_DOCUMENT_GUARD + """
local mirrors_key = KEYS[1]
local namespace_intent = KEYS[2]
local root_key = KEYS[3]
local requested_format = ARGV[1]
local enable = ARGV[2]
local now_ms = tonumber(ARGV[3])
if redis.call('EXISTS', namespace_intent) == 1 then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -3 end
if root_type ~= 'string' then return -4 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -4 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -4 end
if root_state == 0 then return -6 end

local current = {}
local seen = {}
local mirrors_type = redis.call('TYPE', mirrors_key)
if type(mirrors_type) == 'table' then mirrors_type = mirrors_type['ok'] end
if mirrors_type ~= 'none' and mirrors_type ~= 'string' then return -5 end
if mirrors_type == 'string' then
  local config_ok, config = pcall(cjson.decode, redis.call('GET', mirrors_key))
  if not config_ok or type(config) ~= 'table'
      or type(config['formats']) ~= 'table'
      or type(config['ts']) ~= 'number'
      or config['ts'] < 0
      or config['ts'] ~= math.floor(config['ts']) then return -5 end
  local count = 0
  for key, value in pairs(config['formats']) do
    if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
        or type(value) ~= 'string' then return -5 end
    count = count + 1
    local normalized = string.upper(value)
    if normalized ~= 'DELTA' and normalized ~= 'ICEBERG'
        and normalized ~= 'PARQUET' then return -5 end
    if not seen[normalized] then
      seen[normalized] = true
      table.insert(current, normalized)
    end
  end
  if count ~= #config['formats'] then return -5 end
end

local changed = false
if enable == '1' then
  if not seen[requested_format] then
    table.insert(current, requested_format)
    changed = true
  end
else
  local retained = {}
  for _, value in ipairs(current) do
    if value == requested_format then
      changed = true
    else
      table.insert(retained, value)
    end
  end
  current = retained
end
if changed then
  redis.call('SET', mirrors_key, cjson.encode({formats=current, ts=now_ms}))
end
return current
"""

    _LUA_UPSERT_LINKED_SHARE = _LUA_ROOT_DOCUMENT_GUARD + """
local document_key = KEYS[1]
local index_key = KEYS[2]
local namespace_intent = KEYS[3]
local root_key = KEYS[4]
local reservation_key = KEYS[5]
local unlink_tombstone = KEYS[6]
local document_json = ARGV[1]
local link_id = ARGV[2]
local mode = ARGV[3]
local not_after_ms = tonumber(ARGV[4] or '0')
if not not_after_ms or not_after_ms < 0
    or not_after_ms > ROOT_MAX_SAFE_INTEGER
    or not_after_ms ~= math.floor(not_after_ms) then return -8 end
if publication_deadline_exceeded(not_after_ms) then return -8 end
if redis.call('EXISTS', unlink_tombstone) == 1 then return -12 end
if redis.call('EXISTS', namespace_intent) == 1 then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -3 end
if root_type ~= 'string' then return -4 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -4 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -4 end
if root_state == 0 then return -7 end
-- Lua represents JSON arrays and objects as tables.  Python emits canonical
-- object JSON here, so retain an explicit object-shape check at the atomic
-- boundary instead of accepting an array as a control-plane document.
if not string.match(document_json, '^%s*{') then return -2 end
local ok, document = pcall(cjson.decode, document_json)
if not ok or type(document) ~= 'table' then return -2 end
local new_provider = document['_linked_provider_generated_ms']
local new_digest = document['_linked_provider_manifest_digest']
local new_generation = document['publication_generation']
local new_instance = document['_linked_instance_nonce']
local provider_publication = (
    new_provider ~= nil or new_digest ~= nil
)
if provider_publication then
  new_provider = tonumber(new_provider)
  new_generation = tonumber(new_generation)
  if not new_provider or new_provider <= 0
      or new_provider > ROOT_MAX_SAFE_INTEGER
      or new_provider ~= math.floor(new_provider)
      or not linked_manifest_digest_ok(new_digest)
      or not linked_instance_nonce_ok(new_instance)
      or not new_generation or new_generation <= 0
      or new_generation > ROOT_MAX_SAFE_INTEGER
      or new_generation ~= math.floor(new_generation) then return -2 end
  local reservation_ok, reservation = pcall(
      cjson.decode, redis.call('GET', reservation_key) or ''
  )
  if not reservation_ok or type(reservation) ~= 'table'
      or reservation['state'] ~= 'preparing'
      or tonumber(reservation['provider_generated_ms']) ~= new_provider
      or reservation['manifest_digest'] ~= new_digest
      or tonumber(reservation['publication_generation']) ~= new_generation
      or reservation['instance_nonce'] ~= new_instance then
    return -13
  end
end
local document_type = redis.call('TYPE', document_key)
if type(document_type) == 'table' then document_type = document_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if document_type ~= 'none' and document_type ~= 'string' then return -5 end
if index_type ~= 'none' and index_type ~= 'set' then return -5 end
local indexed = redis.call('SISMEMBER', index_key, link_id)
if mode == 'create' then
  if document_type ~= 'none' then return -6 end
  if indexed == 1 then return -5 end
  if publication_deadline_exceeded(not_after_ms) then return -8 end
  if redis.call('EXISTS', unlink_tombstone) == 1 then return -12 end
  redis.call('SET', document_key, document_json)
  redis.call('SADD', index_key, link_id)
  if provider_publication then
    redis.call('SET', reservation_key, cjson.encode({
      provider_generated_ms=new_provider,
      manifest_digest=new_digest,
      publication_generation=new_generation,
      instance_nonce=new_instance,
      state='committed',
    }))
  end
elseif mode == 'update' then
  if document_type == 'none' then
    if indexed == 1 then return -5 end
    return 0
  end
  if indexed ~= 1 then return -5 end
  local current_ok, current = pcall(
      cjson.decode, redis.call('GET', document_key)
  )
  if not current_ok or type(current) ~= 'table' then return -5 end
  local current_instance = current['_linked_instance_nonce']
  if current_instance ~= nil and current_instance ~= new_instance then
    return -14
  end
  local current_provider = tonumber(
      current['_linked_provider_generated_ms'] or '0'
  )
  local current_generation = tonumber(
      current['publication_generation'] or '0'
  )
  if not current_provider or current_provider < 0
      or current_provider > ROOT_MAX_SAFE_INTEGER
      or current_provider ~= math.floor(current_provider)
      or not current_generation or current_generation < 0
      or current_generation > ROOT_MAX_SAFE_INTEGER
      or current_generation ~= math.floor(current_generation) then return -5 end
  if provider_publication then
    if current_provider > new_provider then return -10 end
    if current_provider == new_provider and current_provider > 0 then
      if current['_linked_provider_manifest_digest'] ~= new_digest then
        return -11
      end
      if current_generation > new_generation then return -9 end
    end
  elseif current_provider > 0 then
    return -10
  elseif current_generation > tonumber(new_generation or '0') then
    return -9
  end
  if publication_deadline_exceeded(not_after_ms) then return -8 end
  if redis.call('EXISTS', unlink_tombstone) == 1 then return -12 end
  redis.call('SET', document_key, document_json)
  if provider_publication then
    redis.call('SET', reservation_key, cjson.encode({
      provider_generated_ms=new_provider,
      manifest_digest=new_digest,
      publication_generation=new_generation,
      instance_nonce=new_instance,
      state='committed',
    }))
  end
else
  return -2
end
return 1
"""

    _LUA_MUTATE_SHARE = """
local document_key = KEYS[1]
local index_key = KEYS[2]
local document_json = ARGV[1]
local share_id = ARGV[2]
local mode = ARGV[3]
local document_type = redis.call('TYPE', document_key)
if type(document_type) == 'table' then document_type = document_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if document_type ~= 'none' and document_type ~= 'string' then return -2 end
if index_type ~= 'none' and index_type ~= 'set' then return -2 end
local indexed = redis.call('SISMEMBER', index_key, share_id)

if mode == 'delete' then
  if document_type == 'none' then
    if indexed == 1 then return -2 end
    return 0
  end
  if indexed ~= 1 then return -2 end
  redis.call('DEL', document_key)
  redis.call('SREM', index_key, share_id)
  return 1
end

if not string.match(document_json, '^%s*{') then return -3 end
local ok, document = pcall(cjson.decode, document_json)
if not ok or type(document) ~= 'table' then return -3 end
if mode == 'create' then
  if document_type ~= 'none' then return -1 end
  if indexed == 1 then return -2 end
  redis.call('SET', document_key, document_json)
  redis.call('SADD', index_key, share_id)
  return 1
end
if mode == 'update' then
  if document_type == 'none' then
    if indexed == 1 then return -2 end
    return 0
  end
  if indexed ~= 1 then return -2 end
  redis.call('SET', document_key, document_json)
  return 1
end
return -3
"""

    _LUA_DELETE_LINKED_SHARE = _LUA_ROOT_DOCUMENT_GUARD + """
local document_key = KEYS[1]
local index_key = KEYS[2]
local namespace_intent = KEYS[3]
local root_key = KEYS[4]
local unlink_tombstone = KEYS[5]
local link_id = ARGV[1]
if redis.call('EXISTS', namespace_intent) == 1 then return -1 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -2 end
if root_type ~= 'string' then return -3 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -3 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -3 end
if root_state == 0 then return -5 end
local document_type = redis.call('TYPE', document_key)
if type(document_type) == 'table' then document_type = document_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
local tombstone_type = redis.call('TYPE', unlink_tombstone)
if type(tombstone_type) == 'table' then tombstone_type = tombstone_type['ok'] end
if document_type ~= 'none' and document_type ~= 'string' then return -4 end
if index_type ~= 'none' and index_type ~= 'set' then return -4 end
if tombstone_type ~= 'none' and tombstone_type ~= 'string' then return -4 end
local indexed = redis.call('SISMEMBER', index_key, link_id)
if document_type == 'none' then
  if indexed == 1 then return -4 end
  if tombstone_type == 'none' then return {0} end
  local tombstone_ok, tombstone = pcall(
      cjson.decode, redis.call('GET', unlink_tombstone)
  )
  if not tombstone_ok or type(tombstone) ~= 'table'
      or tombstone['link_id'] ~= link_id
      or (tombstone['state'] ~= 'deleting'
          and tombstone['state'] ~= 'deleted') then return {-4} end
  if tombstone['state'] == 'deleted' then return {0} end
  if type(tombstone['link_doc']) ~= 'table' then return {-4} end
  return {2, cjson.encode(tombstone['link_doc'])}
end
if indexed ~= 1 then return -4 end
if tombstone_type ~= 'none' then return {-4} end
local document_raw = redis.call('GET', document_key)
local document_ok, document = pcall(cjson.decode, document_raw)
if not document_ok or type(document) ~= 'table' then return {-4} end
local tombstone = cjson.encode({
  link_id=link_id,
  state='deleting',
  link_doc=document,
})
redis.call('SET', unlink_tombstone, tombstone)
redis.call('DEL', document_key)
redis.call('SREM', index_key, link_id)
return {1, document_raw}
"""

    _LUA_FINISH_UNLINK_LINKED_SHARE = """
local unlink_tombstone = KEYS[1]
local linked_leaf_names = KEYS[2]
local link_id = ARGV[1]
local tombstone_type = redis.call('TYPE', unlink_tombstone)
if type(tombstone_type) == 'table' then tombstone_type = tombstone_type['ok'] end
local names_type = redis.call('TYPE', linked_leaf_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if tombstone_type ~= 'string'
    or (names_type ~= 'none' and names_type ~= 'set') then return -1 end
if names_type == 'set' and redis.call('SCARD', linked_leaf_names) ~= 0 then
  return -2
end
local tombstone_ok, tombstone = pcall(
    cjson.decode, redis.call('GET', unlink_tombstone)
)
if not tombstone_ok or type(tombstone) ~= 'table'
    or tombstone['link_id'] ~= link_id
    or (tombstone['state'] ~= 'deleting'
        and tombstone['state'] ~= 'deleted') then return -1 end
redis.call('SET', unlink_tombstone, cjson.encode({
  link_id=link_id,
  state='deleted',
}))
redis.call('DEL', linked_leaf_names)
return 1
"""

    _LUA_GET_AUTHORITATIVE_LINKED_SHARE = """
local document_key = KEYS[1]
local index_key = KEYS[2]
local unlink_tombstone = KEYS[3]
local link_id = ARGV[1]

local document_type = redis.call('TYPE', document_key)
if type(document_type) == 'table' then document_type = document_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
local tombstone_type = redis.call('TYPE', unlink_tombstone)
if type(tombstone_type) == 'table' then tombstone_type = tombstone_type['ok'] end
if (document_type ~= 'none' and document_type ~= 'string')
    or (index_type ~= 'none' and index_type ~= 'set')
    or (tombstone_type ~= 'none' and tombstone_type ~= 'string') then
  return {-2}
end

-- Any durable unlink marker denies authority. It is intentionally checked
-- before the document/index pair so a partially completed unlink cannot expose
-- a control document through an inconsistent intermediate state.
if tombstone_type ~= 'none' then return {-3} end

local indexed = redis.call('SISMEMBER', index_key, link_id)
if document_type == 'none' then
  if indexed ~= 0 then return {-2} end
  return {0}
end
if indexed ~= 1 then return {-2} end

local document_raw = redis.call('GET', document_key)
local document_ok, document = pcall(cjson.decode, document_raw)
if not document_ok or type(document) ~= 'table'
    or document['link_id'] ~= link_id then return {-2} end
return {1, document_raw}
"""

    _LUA_ROOT_BUMP = _LUA_ROOT_DOCUMENT_GUARD + """
local key = KEYS[1]
local namespace_intent = KEYS[2]
local now_ms = tonumber(ARGV[1])

if not now_ms or now_ms < 0 or now_ms > ROOT_MAX_SAFE_INTEGER
    or now_ms ~= math.floor(now_ms) then return -5 end

if redis.call('EXISTS', namespace_intent) == 1 then return -1 end

local cur = redis.call('GET', key)
if not cur then return -2 end
local ok, obj = pcall(cjson.decode, cur)
if not ok or type(obj) ~= 'table' then return -3 end
local root_state = root_document_state(obj, nil)
if root_state == -1 then return -3 end
if root_state == 0 then return -4 end
local old_version = obj['version']
if old_version >= ROOT_MAX_SAFE_INTEGER then return -5 end
local new_version = old_version + 1
obj['version'] = new_version
obj['ts'] = now_ms
local new_val = cjson.encode(obj)
redis.call('SET', key, new_val)
return new_version
"""

    # Durable deletion intents outlive their expiring locks.  Ordinary delete
    # calls are create-only: an intent owned by a process whose lease expired
    # is never silently taken over, because that process may still resume an
    # in-flight object-store prefix delete.  The RECOVER scripts are exposed
    # only through APIs that require an explicit operator acknowledgement.
    _LUA_BEGIN_SIMPLE_DELETION = _LUA_ROOT_DOCUMENT_GUARD + """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local namespace_intent = KEYS[3]
local namespace_lock = KEYS[4]
local leaf_lock = KEYS[5]
local leaf = KEYS[6]
local quality_running = KEYS[7]
local root_key = KEYS[8]
local record_json = ARGV[1]
local intent_id = ARGV[2]
local namespace_token = ARGV[3]
local leaf_token = ARGV[4]
local simple_name = ARGV[5]

if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then
  return -2
end
if redis.call('EXISTS', namespace_intent) == 1 then return -3 end
local root_raw = redis.call('GET', root_key)
if not root_raw then return -8 end
local root_ok, root = pcall(cjson.decode, root_raw)
if not root_ok then return -9 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -9 end
if root_state == 0 then return -10 end

local index_type = redis.call('TYPE', intent_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if index_type ~= 'none' and index_type ~= 'set' then return -7 end

local current_raw = redis.call('GET', intent_key)
if current_raw then
  local current_ok, current = pcall(cjson.decode, current_raw)
  if not current_ok or type(current) ~= 'table' then return -5 end
  if tostring(current['intent_id'] or '') == intent_id
      and tostring(current['namespace_lock_token'] or '') == namespace_token
      and tostring(current['leaf_lock_token'] or '') == leaf_token then
    redis.call('DEL', quality_running)
    redis.call('SADD', intent_index, simple_name)
    return 2
  end
  return -4
end
if redis.call('EXISTS', leaf) ~= 1 then return -6 end

local record_ok, record = pcall(cjson.decode, record_json)
if not record_ok or type(record) ~= 'table'
    or tostring(record['intent_id'] or '') ~= intent_id
    or tostring(record['namespace_lock_token'] or '') ~= namespace_token
    or tostring(record['leaf_lock_token'] or '') ~= leaf_token
    or tostring(record['table_name'] or '') ~= simple_name then
  return -5
end
redis.call('SET', intent_key, record_json)
redis.call('SADD', intent_index, simple_name)
redis.call('DEL', quality_running)
return 1
"""

    _LUA_RECOVER_SIMPLE_DELETION = """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local namespace_intent = KEYS[3]
local namespace_lock = KEYS[4]
local leaf_lock = KEYS[5]
local quality_running = KEYS[6]
local expected_intent_id = ARGV[1]
local namespace_token = ARGV[2]
local leaf_token = ARGV[3]
local simple_name = ARGV[4]
local now_ms = tonumber(ARGV[5])

if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then
  return -2
end
if redis.call('EXISTS', namespace_intent) == 1 then return -3 end
local current_raw = redis.call('GET', intent_key)
if not current_raw then return -4 end
local current_ok, current = pcall(cjson.decode, current_raw)
if not current_ok or type(current) ~= 'table' then return -5 end
if tostring(current['intent_id'] or '') ~= expected_intent_id then return -4 end
current['namespace_lock_token'] = namespace_token
current['leaf_lock_token'] = leaf_token
current['status'] = 'deleting'
current['recovered_at_ms'] = now_ms
current['recovery_count'] = tonumber(current['recovery_count'] or 0) + 1
redis.call('SET', intent_key, cjson.encode(current))
redis.call('SADD', intent_index, simple_name)
redis.call('DEL', quality_running)
return 1
"""

    _LUA_CLEAR_SIMPLE_DELETION = """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local namespace_intent = KEYS[3]
local namespace_lock = KEYS[4]
local leaf_lock = KEYS[5]
local leaf = KEYS[6]
local schema = KEYS[7]
local rowid = KEYS[8]
local table_config = KEYS[9]
local mirror_publication = KEYS[10]
local table_names = KEYS[11]
local expected_intent_id = ARGV[1]
local namespace_token = ARGV[2]
local leaf_token = ARGV[3]
local simple_name = ARGV[4]
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then return -1 end
if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then return -2 end
if redis.call('EXISTS', namespace_intent) == 1 then return -3 end
local raw = redis.call('GET', intent_key)
if not raw then return -4 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -5 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['namespace_lock_token'] or '') ~= namespace_token
    or tostring(intent['leaf_lock_token'] or '') ~= leaf_token
    or tostring(intent['status'] or '') ~= 'deleted' then return -4 end
if redis.call('EXISTS', leaf, schema, rowid, table_config, mirror_publication) ~= 0
    or redis.call('SISMEMBER', table_names, simple_name) ~= 0 then return -6 end
for index = 12, #KEYS do
  if redis.call('EXISTS', KEYS[index]) ~= 0 then return -6 end
end
redis.call('DEL', intent_key)
redis.call('SREM', intent_index, simple_name)
return 1
"""

    _LUA_BEGIN_NAMESPACE_DELETION = _LUA_ROOT_DOCUMENT_GUARD + """
local intent_key = KEYS[1]
local namespace_lock = KEYS[2]
local simple_intent_index = KEYS[3]
local stage_intent_index = KEYS[4]
local root_key = KEYS[5]
local record_json = ARGV[1]
local intent_id = ARGV[2]
local namespace_token = ARGV[3]

if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
local root_raw = redis.call('GET', root_key)
if not root_raw then return -8 end
local root_ok, root = pcall(cjson.decode, root_raw)
if not root_ok then return -9 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -9 end
local index_type = redis.call('TYPE', simple_intent_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if index_type ~= 'none' and index_type ~= 'set' then return -5 end
if redis.call('SCARD', simple_intent_index) ~= 0 then return -3 end
local stage_index_type = redis.call('TYPE', stage_intent_index)
if type(stage_index_type) == 'table' then
  stage_index_type = stage_index_type['ok']
end
if stage_index_type ~= 'none' and stage_index_type ~= 'set' then return -6 end
if redis.call('SCARD', stage_intent_index) ~= 0 then return -7 end

local current_raw = redis.call('GET', intent_key)
if current_raw then
  local current_ok, current = pcall(cjson.decode, current_raw)
  if not current_ok or type(current) ~= 'table' then return -4 end
  if tostring(current['intent_id'] or '') == intent_id
      and tostring(current['namespace_lock_token'] or '') == namespace_token then
    return 2
  end
  return -2
end
local record_ok, record = pcall(cjson.decode, record_json)
if not record_ok or type(record) ~= 'table'
    or tostring(record['intent_id'] or '') ~= intent_id
    or tostring(record['namespace_lock_token'] or '') ~= namespace_token then
  return -4
end
redis.call('SET', intent_key, record_json)
return 1
"""

    _LUA_RECOVER_NAMESPACE_DELETION = """
local intent_key = KEYS[1]
local namespace_lock = KEYS[2]
local simple_intent_index = KEYS[3]
local stage_intent_index = KEYS[4]
local expected_intent_id = ARGV[1]
local namespace_token = ARGV[2]
local now_ms = tonumber(ARGV[3])

if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
local index_type = redis.call('TYPE', simple_intent_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if index_type ~= 'none' and index_type ~= 'set' then return -5 end
if redis.call('SCARD', simple_intent_index) ~= 0 then return -3 end
local stage_index_type = redis.call('TYPE', stage_intent_index)
if type(stage_index_type) == 'table' then
  stage_index_type = stage_index_type['ok']
end
if stage_index_type ~= 'none' and stage_index_type ~= 'set' then return -6 end
if redis.call('SCARD', stage_intent_index) ~= 0 then return -7 end
local current_raw = redis.call('GET', intent_key)
if not current_raw then return -2 end
local current_ok, current = pcall(cjson.decode, current_raw)
if not current_ok or type(current) ~= 'table' then return -4 end
if tostring(current['intent_id'] or '') ~= expected_intent_id then return -2 end
current['namespace_lock_token'] = namespace_token
current['status'] = 'deleting'
current['recovered_at_ms'] = now_ms
current['recovery_count'] = tonumber(current['recovery_count'] or 0) + 1
redis.call('SET', intent_key, cjson.encode(current))
return 1
"""

    _LUA_CLEAR_NAMESPACE_DELETION = """
local intent_key = KEYS[1]
local namespace_lock = KEYS[2]
local root = KEYS[3]
local simple_intent_index = KEYS[4]
local stage_intent_index = KEYS[5]
local expected_intent_id = ARGV[1]
local namespace_token = ARGV[2]
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then return -1 end
local raw = redis.call('GET', intent_key)
if not raw then return -2 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['namespace_lock_token'] or '') ~= namespace_token
    or tostring(intent['status'] or '') ~= 'deleted' then return -2 end
if redis.call('EXISTS', root) ~= 0
    or redis.call('SCARD', simple_intent_index) ~= 0
    or redis.call('SCARD', stage_intent_index) ~= 0 then return -4 end
redis.call('DEL', intent_key)
return 1
"""

    _LUA_ASSERT_TABLE_MUTATION_ALLOWED = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_lock = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent = KEYS[3]
local root_key = KEYS[4]
local leaf_token = ARGV[1]
if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', simple_intent) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -4 end
if root_type ~= 'string' then return -5 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -5 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -5 end
if root_state == 0 then return -6 end
return 1
"""

    _LUA_BEGIN_TABLE_MUTATION = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_lock = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent = KEYS[3]
local root_key = KEYS[4]
local leaf_key = KEYS[5]
local config_key = KEYS[6]
local mirrors_key = KEYS[7]
local rowid_key = KEYS[8]
local namespace_lock = KEYS[9]
local leaf_token = ARGV[1]
local reserve_count = ARGV[2]
local expected_leaf_raw = ARGV[3]
local prepared_floor = ARGV[4]
local namespace_token = ARGV[5]
local expected_tombstone_prefix = ARGV[6]
local tombstone_json_max_exact_integer = 99999999999999

if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then
  return {-1}
end
if redis.call('EXISTS', namespace_intent) == 1 then return {-2} end
if redis.call('EXISTS', simple_intent) == 1 then return {-3} end
if namespace_token ~= ''
    and redis.call('GET', namespace_lock) ~= namespace_token then
  return {-10}
end

local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return {-4} end
if root_type ~= 'string' then return {-5} end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return {-5} end
local root_state = root_document_state(root, nil)
if root_state == -1 then return {-5} end
if root_state == 0 then return {-6} end

local config_type = redis.call('TYPE', config_key)
if type(config_type) == 'table' then config_type = config_type['ok'] end
if config_type ~= 'none' and config_type ~= 'string' then return {-8} end
local config_raw = ''
if config_type == 'string' then
  config_raw = redis.call('GET', config_key)
  if config_raw == '' then return {-8} end
end
if config_raw ~= '' then
  -- cjson represents every JSON number as a Lua number, so its decoded value
  -- cannot distinguish the required integer tokens `2`/`3` from `2.0`,
  -- `3.0`, or exponent notation.
  -- Locate top-level member value tokens without being confused by nested
  -- objects or quoted text.  This also makes duplicate activation keys and
  -- escaped aliases fail closed instead of letting Python reject them only
  -- after the row-ID allocator has already changed.
  local function json_skip_space(raw, index)
    while index <= string.len(raw) do
      local byte = string.byte(raw, index)
      if byte ~= 32 and byte ~= 9 and byte ~= 10 and byte ~= 13 then break end
      index = index + 1
    end
    return index
  end
  local function json_string_end(raw, index)
    if string.byte(raw, index) ~= 34 then return nil end
    index = index + 1
    while index <= string.len(raw) do
      local byte = string.byte(raw, index)
      if byte == 34 then return index + 1 end
      if byte == 92 then index = index + 1 end
      index = index + 1
    end
    return nil
  end
  local function top_level_json_token(raw, member_name)
    local length = string.len(raw)
    local index = json_skip_space(raw, 1)
    if string.byte(raw, index) ~= 123 then return 0, nil, nil end
    index = index + 1
    local member_literal = '"' .. member_name .. '"'
    local matches = 0
    local matched_key_token = nil
    local matched_token = nil
    while true do
      index = json_skip_space(raw, index)
      if string.byte(raw, index) == 125 then
        return matches, matched_key_token, matched_token
      end
      local key_start = index
      local key_end = json_string_end(raw, index)
      if not key_end then return -1, nil, nil end
      local key_token = string.sub(raw, key_start, key_end - 1)
      local key_value = nil
      if key_token == member_literal then
        key_value = member_name
      elseif string.find(key_token, string.char(92), 1, true) then
        local key_ok
        key_ok, key_value = pcall(cjson.decode, key_token)
        if not key_ok or type(key_value) ~= 'string' then
          return -1, nil, nil
        end
      end
      index = json_skip_space(raw, key_end)
      if string.byte(raw, index) ~= 58 then return -1, nil, nil end
      index = json_skip_space(raw, index + 1)
      local value_start = index
      local nested = 0
      local quoted = false
      while index <= length do
        local byte = string.byte(raw, index)
        if quoted then
          if byte == 92 then
            index = index + 1
          elseif byte == 34 then
            quoted = false
          end
        elseif byte == 34 then
          quoted = true
        elseif byte == 123 or byte == 91 then
          nested = nested + 1
        elseif byte == 125 then
          if nested == 0 then break end
          nested = nested - 1
        elseif byte == 93 then
          if nested == 0 then return -1, nil, nil end
          nested = nested - 1
        elseif byte == 44 and nested == 0 then
          break
        end
        index = index + 1
      end
      local value_end = index - 1
      while value_end >= value_start do
        local byte = string.byte(raw, value_end)
        if byte ~= 32 and byte ~= 9 and byte ~= 10 and byte ~= 13 then break end
        value_end = value_end - 1
      end
      if key_value == member_name then
        matches = matches + 1
        matched_key_token = key_token
        matched_token = string.sub(raw, value_start, value_end)
      end
      if string.byte(raw, index) == 44 then
        index = index + 1
      elseif string.byte(raw, index) == 125 then
        return matches, matched_key_token, matched_token
      else
        return -1, nil, nil
      end
    end
  end
  local config_ok, config = pcall(cjson.decode, config_raw)
  if not string.match(config_raw, '^%s*{')
      or not config_ok or type(config) ~= 'table' then return {-8} end
  local format_marker = config['deletion_vector_format']
  local v2_fleet_marker = config['dv_v2_reader_fleet_confirmed']
  local v3_fleet_marker = config['dv_v3_reader_fleet_confirmed']
  local format_present = format_marker ~= nil
  local v2_fleet_present = v2_fleet_marker ~= nil
  local v3_fleet_present = v3_fleet_marker ~= nil
  local format_count, format_key_token, format_token = top_level_json_token(
    config_raw, 'deletion_vector_format'
  )
  local v2_fleet_count, v2_fleet_key_token, v2_fleet_token =
    top_level_json_token(
      config_raw, 'dv_v2_reader_fleet_confirmed'
    )
  local v3_fleet_count, v3_fleet_key_token, v3_fleet_token =
    top_level_json_token(
      config_raw, 'dv_v3_reader_fleet_confirmed'
    )
  if format_count < 0 or v2_fleet_count < 0
      or v3_fleet_count < 0 then return {-8} end
  if format_present then
    if type(format_marker) ~= 'number' or format_count ~= 1 then
      return {-8}
    end
    if format_marker == 2 then
      if v2_fleet_marker ~= true or not v2_fleet_present
          or v3_fleet_present or format_token ~= '2'
          or format_key_token ~= '"deletion_vector_format"'
          or v2_fleet_count ~= 1 or v2_fleet_token ~= 'true'
          or v2_fleet_key_token ~= '"dv_v2_reader_fleet_confirmed"'
          or v3_fleet_count ~= 0 then return {-8} end
    elseif format_marker == 3 then
      if v3_fleet_marker ~= true or not v3_fleet_present
          or v2_fleet_present or format_token ~= '3'
          or format_key_token ~= '"deletion_vector_format"'
          or v3_fleet_count ~= 1 or v3_fleet_token ~= 'true'
          or v3_fleet_key_token ~= '"dv_v3_reader_fleet_confirmed"'
          or v2_fleet_count ~= 0 then return {-8} end
    else
      return {-8}
    end
  elseif v2_fleet_present or v3_fleet_present
      or format_count ~= 0 or v2_fleet_count ~= 0
      or v3_fleet_count ~= 0 then
    return {-8}
  end
end

-- Validate the mirror document at the same linearization point as the leaf.
-- The final snapshot CAS compares this exact set again, because mirror config
-- intentionally remains mutable while a table lease is held.
local mirrors_raw = redis.call('GET', mirrors_key) or ''
if mirrors_raw ~= '' then
  local mirrors_ok, mirrors = pcall(cjson.decode, mirrors_raw)
  if not string.match(mirrors_raw, '^%s*{')
      or not mirrors_ok or type(mirrors) ~= 'table'
      or type(mirrors['formats']) ~= 'table'
      or type(mirrors['ts']) ~= 'number'
      or mirrors['ts'] < 0
      or mirrors['ts'] > ROOT_MAX_SAFE_INTEGER
      or mirrors['ts'] ~= math.floor(mirrors['ts']) then return {-9} end
  local seen = {}
  local physical_count = 0
  for key, value in pairs(mirrors['formats']) do
    if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
        or type(value) ~= 'string' then return {-9} end
    physical_count = physical_count + 1
    local normalized = string.upper(value)
    if normalized ~= 'DELTA' and normalized ~= 'ICEBERG'
        and normalized ~= 'PARQUET' then return {-9} end
    if seen[normalized] then return {-9} end
    seen[normalized] = true
  end
  if physical_count ~= #mirrors['formats'] then return {-9} end
end

local leaf_type = redis.call('TYPE', leaf_key)
if type(leaf_type) == 'table' then leaf_type = leaf_type['ok'] end
if leaf_type == 'none' then
  -- Only a canonical namespace->leaf lock holder may allocate IDs for an
  -- expected-absent table.  The reservation can safely precede immutable
  -- storage I/O: a failed attempt leaves a gap, never a duplicate ID.
  if namespace_token == '' then
    return {0, '', config_raw, mirrors_raw, '0', '', '0', '', '', '0'}
  end
  local rowid_type = redis.call('TYPE', rowid_key)
  if type(rowid_type) == 'table' then rowid_type = rowid_type['ok'] end
  if rowid_type ~= 'none' and rowid_type ~= 'string' then return {-11} end
  local current = redis.call('GET', rowid_key) or '0'
  if not string.match(current, '^%d+$')
      or not string.match(reserve_count, '^%d+$') then
    return redis.error_reply('invalid non-negative rowid sequence')
  end
  local normalized = string.gsub(current, '^0+', '')
  if normalized == '' then normalized = '0' end
  local reserved = '0'
  local new_value = normalized
  if reserve_count ~= '0' then
    redis.call('INCRBY', rowid_key, reserve_count)
    new_value = redis.call('GET', rowid_key)
    reserved = '1'
  end
  return {
    0, '', config_raw, mirrors_raw,
    '1', normalized, reserved, normalized, new_value, '0'
  }
end
if leaf_type ~= 'string' then return {-7} end
local leaf_raw = redis.call('GET', leaf_key)
local prepared_match = expected_leaf_raw ~= '' and leaf_raw == expected_leaf_raw
local leaf = nil
if not prepared_match then
  local leaf_ok
  leaf_ok, leaf = pcall(cjson.decode, leaf_raw)
  if not leaf_ok or type(leaf) ~= 'table'
      or type(leaf['version']) ~= 'number'
      or leaf['version'] < 0
      or leaf['version'] > ROOT_MAX_SAFE_INTEGER
      or leaf['version'] ~= math.floor(leaf['version'])
      or type(leaf['ts']) ~= 'number'
      or leaf['ts'] < 0
      or leaf['ts'] > ROOT_MAX_SAFE_INTEGER
      or leaf['ts'] ~= math.floor(leaf['ts'])
      or type(leaf['path']) ~= 'string'
      or leaf['path'] == '' then return {-7} end
end

-- A current Redis payload is an atomic cache of the immutable snapshot.  Use
-- its floor only when it satisfies the same conservative shape needed by the
-- Python cache validator and is exactly representable by Redis Lua.  Legacy,
-- incomplete, corrupt, or >2^53 floors deliberately take the existing
-- storage-derived exact-Int64 fallback after this call.
local floor_available = false
local floor = ''
if prepared_match then
  if prepared_floor ~= '' then
    floor_available = true
    floor = prepared_floor
  end
else
  local payload = leaf['payload']
  if type(payload) == 'table' and payload['_row_filter'] ~= nil then
    -- Redis cjson collapses integral JSON floats to Lua numbers. Recover the
    -- raw v3 discriminator/count/version tokens before reserving IDs so the
    -- atomic boundary enforces the same exact-integer contract as Python.
    local function cached_top_level_json_token(raw, member_name)
      if type(raw) ~= 'string' then return 0, nil, nil end
      local length = string.len(raw)
      local function skip_space(index)
        while index <= length do
          local byte = string.byte(raw, index)
          if byte ~= 32 and byte ~= 9 and byte ~= 10 and byte ~= 13 then
            break
          end
          index = index + 1
        end
        return index
      end
      local function string_end(index)
        if string.byte(raw, index) ~= 34 then return nil end
        index = index + 1
        while index <= length do
          local byte = string.byte(raw, index)
          if byte == 34 then return index + 1 end
          if byte == 92 then index = index + 1 end
          index = index + 1
        end
        return nil
      end
      local index = skip_space(1)
      if string.byte(raw, index) ~= 123 then return 0, nil, nil end
      index = index + 1
      local member_literal = '"' .. member_name .. '"'
      local matches = 0
      local matched_key_token = nil
      local matched_value_token = nil
      while true do
        index = skip_space(index)
        if string.byte(raw, index) == 125 then
          return matches, matched_key_token, matched_value_token
        end
        local key_start = index
        local key_end = string_end(index)
        if not key_end then return -1, nil, nil end
        local key_token = string.sub(raw, key_start, key_end - 1)
        local key_value = nil
        if key_token == member_literal then
          key_value = member_name
        elseif string.find(key_token, string.char(92), 1, true) then
          local key_ok
          key_ok, key_value = pcall(cjson.decode, key_token)
          if not key_ok or type(key_value) ~= 'string' then
            return -1, nil, nil
          end
        end
        index = skip_space(key_end)
        if string.byte(raw, index) ~= 58 then return -1, nil, nil end
        index = skip_space(index + 1)
        local value_start = index
        local nested = 0
        local quoted = false
        while index <= length do
          local byte = string.byte(raw, index)
          if quoted then
            if byte == 92 then
              index = index + 1
            elseif byte == 34 then
              quoted = false
            end
          elseif byte == 34 then
            quoted = true
          elseif byte == 123 or byte == 91 then
            nested = nested + 1
          elseif byte == 125 then
            if nested == 0 then break end
            nested = nested - 1
          elseif byte == 93 then
            if nested == 0 then return -1, nil, nil end
            nested = nested - 1
          elseif byte == 44 and nested == 0 then
            break
          end
          index = index + 1
        end
        local value_end = index - 1
        while value_end >= value_start do
          local byte = string.byte(raw, value_end)
          if byte ~= 32 and byte ~= 9 and byte ~= 10 and byte ~= 13 then
            break
          end
          value_end = value_end - 1
        end
        if key_value == member_name then
          matches = matches + 1
          matched_key_token = key_token
          matched_value_token = string.sub(raw, value_start, value_end)
        end
        if string.byte(raw, index) == 44 then
          index = index + 1
        elseif string.byte(raw, index) == 125 then
          return matches, matched_key_token, matched_value_token
        else
          return -1, nil, nil
        end
      end
    end
    local payload_count, payload_key_token, payload_raw =
      cached_top_level_json_token(leaf_raw, 'payload')
    local candidate_raw = nil
    if payload_count == 1 and payload_key_token == '"payload"' then
      candidate_raw = payload_raw
    end
    local candidate = payload
    if type(candidate['resources']) ~= 'table'
        and type(payload['snapshot']) == 'table' then
      candidate = payload['snapshot']
      local snapshot_count, snapshot_key_token, snapshot_raw =
        cached_top_level_json_token(payload_raw, 'snapshot')
      if snapshot_count == 1 and snapshot_key_token == '"snapshot"' then
        candidate_raw = snapshot_raw
      else
        candidate_raw = nil
      end
    end
    local pointer = candidate['tombstone']
    local tombstone_rows = candidate['tombstone_rows']
    local digest = candidate['tombstone_digest']
    local format_marker = candidate['tombstone_format']
    local tombstone_format = 1
    local format_ok = false
    if format_marker == nil then
      format_ok = true
    elseif type(format_marker) == 'number'
        and format_marker == math.floor(format_marker)
        and (format_marker == 1 or format_marker == 2
            or format_marker == 3) then
      tombstone_format = format_marker
      format_ok = true
    end
    local v3_exact_tokens_ok = true
    if tombstone_format == 3 then
      v3_exact_tokens_ok = false
      if candidate_raw ~= nil then
        local format_count, format_key_token, format_token =
          cached_top_level_json_token(candidate_raw, 'tombstone_format')
        local rows_count, rows_key_token, rows_token =
          cached_top_level_json_token(candidate_raw, 'tombstone_rows')
        local version_count, version_key_token, version_token =
          cached_top_level_json_token(candidate_raw, 'snapshot_version')
        local rows_token_ok = rows_token == '0'
            or (type(rows_token) == 'string'
                and string.match(rows_token, '^[1-9][0-9]*$'))
        local version_token_ok = version_token == '0'
            or (type(version_token) == 'string'
                and string.match(version_token, '^[1-9][0-9]*$'))
        v3_exact_tokens_ok = format_count == 1
            and format_key_token == '"tombstone_format"'
            and format_token == '3'
            and rows_count == 1
            and rows_key_token == '"tombstone_rows"'
            and rows_token_ok
            and version_count == 1
            and version_key_token == '"snapshot_version"'
            and version_token_ok
      end
    end
    local function table_artifact_path_ok(
        path, expected_prefix, required_suffix
    )
      if type(path) ~= 'string' or path == ''
          or type(expected_prefix) ~= 'string' or expected_prefix == ''
          or type(required_suffix) ~= 'string' or required_suffix == ''
          or string.len(path) > 4096
          or string.sub(path, 1, 1) == '/'
          or string.sub(path, -1) == '/'
          or string.sub(path, -string.len(required_suffix)) ~= required_suffix
          or string.sub(path, 1, string.len(expected_prefix))
            ~= expected_prefix
          or string.find(path, string.char(92), 1, true)
          or string.find(path, '//', 1, true)
          or string.find(path, '?', 1, true)
          or string.find(path, '#', 1, true)
          or string.find(path, '://', 1, true)
          or string.match(path, '^[%a][%w+%.%-]*:')
          or string.match(path, '[%c]')
          or string.match(path, '^%s')
          or string.match(path, '%s$') then return false end
      local components = 0
      for component in string.gmatch(path, '[^/]+') do
        if component == '.' or component == '..' then return false end
        components = components + 1
      end
      return components > 0
    end
    local tombstone_ok = false
    local explicit_version_ok = tombstone_format == 1
        or (type(candidate['snapshot_version']) == 'number'
            and candidate['snapshot_version'] >= 0
            and candidate['snapshot_version']
              <= tombstone_json_max_exact_integer
            and candidate['snapshot_version'] == math.floor(
              candidate['snapshot_version']
            ))
    if format_ok and v3_exact_tokens_ok and explicit_version_ok
        and pointer == cjson.null
        and type(tombstone_rows) == 'number'
        and tombstone_rows == 0 and digest == cjson.null then
      tombstone_ok = true
    elseif format_ok and v3_exact_tokens_ok and explicit_version_ok
        and type(pointer) == 'string' and pointer ~= ''
        and type(tombstone_rows) == 'number'
        and tombstone_rows > 0
        and tombstone_rows == math.floor(tombstone_rows)
        and type(digest) == 'string' and string.len(digest) == 64
        and string.match(digest, '^[0-9a-f]+$') then
      if tombstone_format == 2 then
        tombstone_ok = candidate['snapshot_version'] >= 1
            and tombstone_rows <= tombstone_json_max_exact_integer
            and table_artifact_path_ok(
              pointer, expected_tombstone_prefix, '.json'
            )
      elseif tombstone_format == 3 then
        tombstone_ok = candidate['snapshot_version'] >= 1
            and tombstone_rows <= tombstone_json_max_exact_integer
            and table_artifact_path_ok(
              pointer, expected_tombstone_prefix, '.parquet'
            )
      else
        -- A JSON root without the explicit v2 discriminator is a malformed
        -- hybrid that an old reader would try to open as Parquet.
        tombstone_ok = string.sub(pointer, -5) ~= '.json'
      end
    end
    local raw_floor = candidate['rowid_high_watermark']
    if type(candidate['snapshot_version']) == 'number'
        and candidate['snapshot_version'] == leaf['version']
        and candidate['snapshot_version'] == math.floor(candidate['snapshot_version'])
        and type(candidate['schema']) == 'table'
        and type(candidate['resources']) == 'table'
        and tombstone_ok
        and type(raw_floor) == 'number'
        and raw_floor >= 0
        and raw_floor <= ROOT_MAX_SAFE_INTEGER
        and raw_floor == math.floor(raw_floor) then
      floor_available = true
      floor = string.format('%.0f', raw_floor)
    end
  end
end

local reserved = '0'
local previous = ''
local new_value = ''
if floor_available and reserve_count ~= '0' then
  local cur = redis.call('GET', rowid_key) or '0'
  local function normalize_decimal(value)
    local normalized = string.gsub(value, '^0+', '')
    if normalized == '' then return '0' end
    return normalized
  end
  local function decimal_lt(a, b)
    a = normalize_decimal(a)
    b = normalize_decimal(b)
    if string.len(a) ~= string.len(b) then
      return string.len(a) < string.len(b)
    end
    return a < b
  end
  if not string.match(cur, '^%d+$')
      or not string.match(floor, '^%d+$')
      or not string.match(reserve_count, '^%d+$') then
    return redis.error_reply('invalid non-negative rowid sequence')
  end
  cur = normalize_decimal(cur)
  floor = normalize_decimal(floor)
  if decimal_lt(cur, floor) then
    cur = floor
    redis.call('SET', rowid_key, cur)
  end
  redis.call('INCRBY', rowid_key, reserve_count)
  previous = cur
  new_value = redis.call('GET', rowid_key)
  reserved = '1'
end

return {
  1, prepared_match and '' or leaf_raw, config_raw, mirrors_raw,
  floor_available and '1' or '0', floor, reserved,
  previous, new_value, prepared_match and '1' or '0'
}
"""

    # Expected-absent creation has already strictly validated its small
    # configuration documents in Python. This boundary only repeats a cjson
    # grammar-compatibility decode (not the general script's recursive policy
    # checks), then proves the exact raw pins at the same instant as leaf
    # absence and the first row-ID reservation.
    #
    # Status 2 is a no-write config/mirror race (the caller may re-pin once).
    # Status 3 is a no-write creator race (the caller uses the general begin
    # with reserve_count=0 so CREATE -> WRITE reauthorization remains exact).
    _LUA_BEGIN_INITIAL_TABLE_MUTATION = _LUA_ROOT_DOCUMENT_GUARD + """
local leaf_lock = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent = KEYS[3]
local root_key = KEYS[4]
local leaf_key = KEYS[5]
local config_key = KEYS[6]
local mirrors_key = KEYS[7]
local rowid_key = KEYS[8]
local namespace_lock = KEYS[9]

local leaf_token = ARGV[1]
local reserve_count = ARGV[2]
local namespace_token = ARGV[3]
local config_present = ARGV[4]
local config_raw = ARGV[5]
local config_valid = ARGV[6]
local mirrors_present = ARGV[7]
local mirrors_raw = ARGV[8]
local mirrors_valid = ARGV[9]

if leaf_token == '' or redis.call('GET', leaf_lock) ~= leaf_token then
  return {-1}
end
if redis.call('EXISTS', namespace_intent) == 1 then return {-2} end
if redis.call('EXISTS', simple_intent) == 1 then return {-3} end
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return {-10}
end

local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return {-4} end
if root_type ~= 'string' then return {-5} end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return {-5} end
local root_state = root_document_state(root, nil)
if root_state == -1 then return {-5} end
if root_state == 0 then return {-6} end

local config_type = redis.call('TYPE', config_key)
if type(config_type) == 'table' then config_type = config_type['ok'] end
if config_type ~= 'none' and config_type ~= 'string' then return {-8} end
if config_present ~= '0' and config_present ~= '1' then return {-8} end
if (config_present == '1') ~= (config_type == 'string') then return {2} end
if config_present == '1' and redis.call('GET', config_key) ~= config_raw then
  return {2}
end
if config_valid ~= '1' then return {-8} end
if config_present == '1' then
  -- Backstop the Python decoder with Redis' own cjson grammar. This preserves
  -- the general boundary's rejection of values (for example over-wide number
  -- tokens) that Python can represent but Redis cjson cannot.
  local config_ok, config = pcall(cjson.decode, config_raw)
  if not string.match(config_raw, '^%s*{')
      or not config_ok or type(config) ~= 'table' then return {-8} end
end

local mirrors_type = redis.call('TYPE', mirrors_key)
if type(mirrors_type) == 'table' then mirrors_type = mirrors_type['ok'] end
if mirrors_type ~= 'none' and mirrors_type ~= 'string' then return {-9} end
if mirrors_present ~= '0' and mirrors_present ~= '1' then return {-9} end
if (mirrors_present == '1') ~= (mirrors_type == 'string') then return {2} end
if mirrors_present == '1' and redis.call('GET', mirrors_key) ~= mirrors_raw then
  return {2}
end
if mirrors_valid ~= '1' then return {-9} end
if mirrors_present == '1' then
  local mirrors_ok, mirrors = pcall(cjson.decode, mirrors_raw)
  if not string.match(mirrors_raw, '^%s*{')
      or not mirrors_ok or type(mirrors) ~= 'table' then return {-9} end
end

local leaf_type = redis.call('TYPE', leaf_key)
if type(leaf_type) == 'table' then leaf_type = leaf_type['ok'] end
if leaf_type == 'string' then return {3} end
if leaf_type ~= 'none' then return {-7} end

local rowid_type = redis.call('TYPE', rowid_key)
if type(rowid_type) == 'table' then rowid_type = rowid_type['ok'] end
if rowid_type ~= 'none' and rowid_type ~= 'string' then return {-11} end
local current = redis.call('GET', rowid_key) or '0'
if not string.match(current, '^%d+$')
    or not string.match(reserve_count, '^%d+$') then return {-11} end
local normalized = string.gsub(current, '^0+', '')
if normalized == '' then normalized = '0' end
local reserved = '0'
local new_value = normalized
if reserve_count ~= '0' then
  -- The only mutating command in this script. Redis rejects signed-Int64
  -- overflow without modifying the existing sequence.
  redis.call('INCRBY', rowid_key, reserve_count)
  new_value = redis.call('GET', rowid_key)
  reserved = '1'
end
return {
  0, '', config_present == '1' and config_raw or '',
  mirrors_present == '1' and mirrors_raw or '',
  '1', normalized, reserved, normalized, new_value, '0'
}
"""

    _LUA_ASSERT_INITIALIZATION_ALLOWED = """
local namespace_lock = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent = KEYS[3]
local namespace_token = ARGV[1]
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if simple_intent ~= namespace_intent
    and redis.call('EXISTS', simple_intent) == 1 then
  return -3
end
return 1
"""

    _LUA_SET_TABLE_CONFIG = _LUA_ROOT_DOCUMENT_GUARD + """
local config_key = KEYS[1]
local leaf_key = KEYS[2]
local leaf_lock = KEYS[3]
local namespace_intent = KEYS[4]
local simple_intent = KEYS[5]
local root_key = KEYS[6]
local config_json = ARGV[1]
local lock_token = ARGV[2]
if lock_token == '' or redis.call('GET', leaf_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', simple_intent) == 1 then return -3 end
if redis.call('EXISTS', leaf_key) ~= 1 then return -4 end
local root_raw = redis.call('GET', root_key)
if not root_raw then return -6 end
local root_ok, root = pcall(cjson.decode, root_raw)
if not root_ok then return -7 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -7 end
if root_state == 0 then return -8 end
local ok, config = pcall(cjson.decode, config_json)
if not ok or type(config) ~= 'table' then return -5 end
redis.call('SET', config_key, config_json)
return 1
"""

    _LUA_DELETE_NAMESPACE_BATCH = """
local namespace_lock = KEYS[1]
local namespace_intent = KEYS[2]
local namespace_token = ARGV[1]
local expected_intent_id = ARGV[2]
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
local intent_raw = redis.call('GET', namespace_intent)
if not intent_raw then return -2 end
local intent_ok, intent = pcall(cjson.decode, intent_raw)
if not intent_ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['namespace_lock_token'] or '') ~= namespace_token then
  return -2
end
local removed = 0
for index = 3, #KEYS do
  removed = removed + redis.call('DEL', KEYS[index])
end
return removed
"""

    _LUA_FINALIZE_NAMESPACE_DELETION = """
local namespace_lock = KEYS[1]
local namespace_intent = KEYS[2]
local simple_intent_index = KEYS[3]
local stage_intent_index = KEYS[4]
local namespace_token = ARGV[1]
local expected_intent_id = ARGV[2]
local now_ms = tonumber(ARGV[3])
if namespace_token == ''
    or redis.call('GET', namespace_lock) ~= namespace_token then
  return -1
end
local intent_raw = redis.call('GET', namespace_intent)
if not intent_raw then return -2 end
local intent_ok, intent = pcall(cjson.decode, intent_raw)
if not intent_ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['namespace_lock_token'] or '') ~= namespace_token then
  return -2
end
local index_type = redis.call('TYPE', simple_intent_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if index_type ~= 'none' and index_type ~= 'set' then return -3 end
if redis.call('SCARD', simple_intent_index) ~= 0 then return -4 end
local stage_index_type = redis.call('TYPE', stage_intent_index)
if type(stage_index_type) == 'table' then
  stage_index_type = stage_index_type['ok']
end
if stage_index_type ~= 'none' and stage_index_type ~= 'set' then return -3 end
if redis.call('SCARD', stage_intent_index) ~= 0 then return -4 end
redis.call('DEL', simple_intent_index)
redis.call('DEL', stage_intent_index)
intent['status'] = 'deleted'
intent['deleted_at_ms'] = now_ms
redis.call('SET', namespace_intent, cjson.encode(intent))
return 1
"""

    _LUA_BEGIN_STAGE_DELETION = _LUA_ROOT_DOCUMENT_GUARD + """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local namespace_intent = KEYS[3]
local stage_lock = KEYS[4]
local root_key = KEYS[5]
local record_json = ARGV[1]
local intent_id = ARGV[2]
local lock_token = ARGV[3]
local stage_name = ARGV[4]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
local root_raw = redis.call('GET', root_key)
if not root_raw then return -6 end
local root_ok, root = pcall(cjson.decode, root_raw)
if not root_ok then return -7 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -7 end
if root_state == 0 then return -8 end
local index_type = redis.call('TYPE', intent_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if index_type ~= 'none' and index_type ~= 'set' then return -5 end
local current_raw = redis.call('GET', intent_key)
if current_raw then
  local ok, current = pcall(cjson.decode, current_raw)
  if not ok or type(current) ~= 'table' then return -4 end
  if tostring(current['intent_id'] or '') == intent_id
      and tostring(current['lock_token'] or '') == lock_token then
    redis.call('SADD', intent_index, stage_name)
    return 2
  end
  return -3
end
local ok, record = pcall(cjson.decode, record_json)
if not ok or type(record) ~= 'table'
    or tostring(record['intent_id'] or '') ~= intent_id
    or tostring(record['lock_token'] or '') ~= lock_token
    or tostring(record['staging_name'] or '') ~= stage_name then
  return -4
end
redis.call('SET', intent_key, record_json)
redis.call('SADD', intent_index, stage_name)
return 1
"""

    _LUA_RECOVER_STAGE_DELETION = """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local namespace_intent = KEYS[3]
local stage_lock = KEYS[4]
local expected_intent_id = ARGV[1]
local lock_token = ARGV[2]
local stage_name = ARGV[3]
local now_ms = tonumber(ARGV[4])
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
local raw = redis.call('GET', intent_key)
if not raw then return -3 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -4 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id then return -3 end
intent['lock_token'] = lock_token
intent['status'] = 'deleting'
intent['recovered_at_ms'] = now_ms
intent['recovery_count'] = tonumber(intent['recovery_count'] or 0) + 1
redis.call('SET', intent_key, cjson.encode(intent))
redis.call('SADD', intent_index, stage_name)
return 1
"""

    _LUA_CLEAR_STAGE_DELETION = """
local intent_key = KEYS[1]
local intent_index = KEYS[2]
local stage_lock = KEYS[3]
local stage_meta = KEYS[4]
local stage_index = KEYS[5]
local expected_intent_id = ARGV[1]
local lock_token = ARGV[2]
local stage_name = ARGV[3]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
local raw = redis.call('GET', intent_key)
if not raw then return -2 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['lock_token'] or '') ~= lock_token
    or tostring(intent['status'] or '') ~= 'deleted' then return -2 end
if redis.call('EXISTS', stage_meta) ~= 0
    or redis.call('SISMEMBER', stage_index, stage_name) ~= 0 then return -4 end
redis.call('DEL', intent_key)
redis.call('SREM', intent_index, stage_name)
return 1
"""

    _LUA_ASSERT_STAGE_MUTATION_ALLOWED = _LUA_ROOT_DOCUMENT_GUARD + """
local stage_lock = KEYS[1]
local namespace_intent = KEYS[2]
local stage_intent = KEYS[3]
local root_key = KEYS[4]
local lock_token = ARGV[1]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', stage_intent) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -4 end
if root_type ~= 'string' then return -5 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -5 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -5 end
if root_state == 0 then return -6 end
return 1
"""

    _LUA_UPSERT_STAGING_META = _LUA_ROOT_DOCUMENT_GUARD + """
local meta_key = KEYS[1]
local index_key = KEYS[2]
local stage_lock = KEYS[3]
local namespace_intent = KEYS[4]
local stage_intent = KEYS[5]
local root_key = KEYS[6]
local payload_json = ARGV[1]
local stage_name = ARGV[2]
local lock_token = ARGV[3]
local create_only = ARGV[4]
local organization = ARGV[5]
local super_name = ARGV[6]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', stage_intent) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -7 end
if root_type ~= 'string' then return -8 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -8 end
local root_state = root_document_state(root, super_name)
if root_state == -1 then return -8 end
if root_state == 0 then return -9 end
local ok, payload = pcall(cjson.decode, payload_json)
if not ok or type(payload) ~= 'table' then return -4 end
if type(payload['organization']) ~= 'string'
    or payload['organization'] ~= organization
    or type(payload['super_name']) ~= 'string'
    or payload['super_name'] ~= super_name
    or type(payload['staging_name']) ~= 'string'
    or payload['staging_name'] ~= stage_name then return -4 end
local meta_type = redis.call('TYPE', meta_key)
if type(meta_type) == 'table' then meta_type = meta_type['ok'] end
local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if meta_type ~= 'none' and meta_type ~= 'string' then return -6 end
if index_type ~= 'none' and index_type ~= 'set' then return -6 end
if create_only == '1' and meta_type ~= 'none' then return -5 end
redis.call('SET', meta_key, payload_json)
redis.call('SADD', index_key, stage_name)
return 1
"""

    _LUA_UPSERT_PIPE_META = _LUA_ROOT_DOCUMENT_GUARD + """
local pipe_key = KEYS[1]
local pipe_index = KEYS[2]
local stage_lock = KEYS[3]
local namespace_intent = KEYS[4]
local stage_intent = KEYS[5]
local stage_meta = KEYS[6]
local root_key = KEYS[7]
local payload_json = ARGV[1]
local pipe_name = ARGV[2]
local lock_token = ARGV[3]
local create_only = ARGV[4]
local organization = ARGV[5]
local super_name = ARGV[6]
local stage_name = ARGV[7]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', stage_intent) == 1 then return -3 end
local stage_type = redis.call('TYPE', stage_meta)
if type(stage_type) == 'table' then stage_type = stage_type['ok'] end
if stage_type == 'none' then return -7 end
if stage_type ~= 'string' then return -8 end
local stage_ok, stage = pcall(cjson.decode, redis.call('GET', stage_meta))
if not stage_ok or type(stage) ~= 'table' then return -8 end
if type(stage['organization']) ~= 'string'
    or stage['organization'] ~= organization
    or type(stage['super_name']) ~= 'string'
    or stage['super_name'] ~= super_name
    or type(stage['staging_name']) ~= 'string'
    or stage['staging_name'] ~= stage_name then return -8 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -9 end
if root_type ~= 'string' then return -10 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -10 end
local root_state = root_document_state(root, super_name)
if root_state == -1 then return -10 end
if root_state == 0 then return -11 end
local ok, payload = pcall(cjson.decode, payload_json)
if not ok or type(payload) ~= 'table' then return -4 end
if type(payload['organization']) ~= 'string'
    or payload['organization'] ~= organization
    or type(payload['super_name']) ~= 'string'
    or payload['super_name'] ~= super_name
    or type(payload['staging_name']) ~= 'string'
    or payload['staging_name'] ~= stage_name
    or type(payload['pipe_name']) ~= 'string'
    or payload['pipe_name'] ~= pipe_name then return -4 end
local pipe_type = redis.call('TYPE', pipe_key)
if type(pipe_type) == 'table' then pipe_type = pipe_type['ok'] end
local index_type = redis.call('TYPE', pipe_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if pipe_type ~= 'none' and pipe_type ~= 'string' then return -6 end
if index_type ~= 'none' and index_type ~= 'set' then return -6 end
if create_only == '1' and pipe_type ~= 'none' then return -5 end
redis.call('SET', pipe_key, payload_json)
redis.call('SADD', pipe_index, pipe_name)
return 1
"""

    _LUA_UPSERT_STAGING_FILE_META = _LUA_ROOT_DOCUMENT_GUARD + """
local meta_key = KEYS[1]
local stage_lock = KEYS[2]
local namespace_intent = KEYS[3]
local stage_intent = KEYS[4]
local root_key = KEYS[5]
local payload_json = ARGV[1]
local file_name = ARGV[2]
local lock_token = ARGV[3]
local organization = ARGV[4]
local super_name = ARGV[5]
local stage_name = ARGV[6]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', stage_intent) == 1 then return -3 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -7 end
if root_type ~= 'string' then return -8 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -8 end
local root_state = root_document_state(root, super_name)
if root_state == -1 then return -8 end
if root_state == 0 then return -9 end
local meta_type = redis.call('TYPE', meta_key)
if type(meta_type) == 'table' then meta_type = meta_type['ok'] end
if meta_type == 'none' then return -6 end
if meta_type ~= 'string' then return -5 end
local payload_ok, payload = pcall(cjson.decode, payload_json)
if not payload_ok or type(payload) ~= 'table' then return -4 end
if type(payload['file']) ~= 'string'
    or payload['file'] ~= file_name then return -4 end
local raw_meta = redis.call('GET', meta_key)
local meta_ok, meta = pcall(cjson.decode, raw_meta)
if not meta_ok or type(meta) ~= 'table' then return -5 end
if type(meta['organization']) ~= 'string'
    or meta['organization'] ~= organization
    or type(meta['super_name']) ~= 'string'
    or meta['super_name'] ~= super_name
    or type(meta['staging_name']) ~= 'string'
    or meta['staging_name'] ~= stage_name then return -5 end
local files = meta['files']
if files == nil then
  files = {}
elseif type(files) ~= 'table' then
  return -5
end
files[file_name] = payload
meta['files'] = files
redis.call('SET', meta_key, cjson.encode(meta))
return 1
"""

    _LUA_DELETE_PIPE_META = _LUA_ROOT_DOCUMENT_GUARD + """
local pipe_key = KEYS[1]
local pipe_index = KEYS[2]
local stage_lock = KEYS[3]
local namespace_intent = KEYS[4]
local stage_intent = KEYS[5]
local stage_meta = KEYS[6]
local root_key = KEYS[7]
local pipe_name = ARGV[1]
local lock_token = ARGV[2]
local organization = ARGV[3]
local super_name = ARGV[4]
local stage_name = ARGV[5]
if lock_token == '' or redis.call('GET', stage_lock) ~= lock_token then
  return -1
end
if redis.call('EXISTS', namespace_intent) == 1 then return -2 end
if redis.call('EXISTS', stage_intent) == 1 then return -3 end
local stage_type = redis.call('TYPE', stage_meta)
if type(stage_type) == 'table' then stage_type = stage_type['ok'] end
if stage_type == 'none' then return -5 end
if stage_type ~= 'string' then return -6 end
local stage_ok, stage = pcall(cjson.decode, redis.call('GET', stage_meta))
if not stage_ok or type(stage) ~= 'table' then return -6 end
if type(stage['organization']) ~= 'string'
    or stage['organization'] ~= organization
    or type(stage['super_name']) ~= 'string'
    or stage['super_name'] ~= super_name
    or type(stage['staging_name']) ~= 'string'
    or stage['staging_name'] ~= stage_name then return -6 end
local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return -7 end
if root_type ~= 'string' then return -8 end
local root_ok, root = pcall(cjson.decode, redis.call('GET', root_key))
if not root_ok or type(root) ~= 'table' then return -8 end
local root_state = root_document_state(root, super_name)
if root_state == -1 then return -8 end
if root_state == 0 then return -9 end
local pipe_type = redis.call('TYPE', pipe_key)
if type(pipe_type) == 'table' then pipe_type = pipe_type['ok'] end
local index_type = redis.call('TYPE', pipe_index)
if type(index_type) == 'table' then index_type = index_type['ok'] end
if pipe_type ~= 'none' and pipe_type ~= 'string' then return -4 end
if index_type ~= 'none' and index_type ~= 'set' then return -4 end
local removed = redis.call('DEL', pipe_key)
redis.call('SREM', pipe_index, pipe_name)
return removed
"""

    # Finalize deletion of one staging namespace after its child keys have been
    # removed and verified.  The stage lock token is checked in the same Redis
    # command as the metadata/index mutation so an expired or stolen lease can
    # never make a stage undiscoverable underneath a new writer.
    #
    # Return values:
    #   >= 0  number of metadata/index entries removed (zero is idempotent)
    #     -1  caller no longer owns the staging lock
    _LUA_STAGING_DELETE_CHILDREN = """
local lock_key = KEYS[1]
local intent_key = KEYS[2]
local lock_token = ARGV[1]
local expected_intent_id = ARGV[2]

if redis.call('GET', lock_key) ~= lock_token then
  return -1
end
local raw = redis.call('GET', intent_key)
if not raw then return -2 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['lock_token'] or '') ~= lock_token then
  return -2
end

local removed = 0
for index = 3, #KEYS do
  removed = removed + redis.call('DEL', KEYS[index])
end
return removed
"""

    _LUA_STAGING_DELETE_META = """
local index_key = KEYS[1]
local meta_key = KEYS[2]
local lock_key = KEYS[3]
local intent_key = KEYS[4]
local intent_index = KEYS[5]
local staging_name = ARGV[1]
local lock_token = ARGV[2]
local expected_intent_id = ARGV[3]
local now_ms = tonumber(ARGV[4])

if redis.call('GET', lock_key) ~= lock_token then
  return -1
end
local raw = redis.call('GET', intent_key)
if not raw then return -2 end
local ok, intent = pcall(cjson.decode, raw)
if not ok or type(intent) ~= 'table' then return -3 end
if tostring(intent['intent_id'] or '') ~= expected_intent_id
    or tostring(intent['lock_token'] or '') ~= lock_token then
  return -2
end

local removed_meta = redis.call('DEL', meta_key)
local removed_index = redis.call('SREM', index_key, staging_name)
intent['status'] = 'deleted'
intent['deleted_at_ms'] = now_ms
redis.call('SET', intent_key, cjson.encode(intent))
redis.call('SADD', intent_index, staging_name)
return removed_meta + removed_index + 1
"""

    # Publish the leaf payload and invalidate the supertable root in one
    # fenced transaction.  KEYS deliberately include the table lock: checking
    # it in the same script as the writes closes the expiry/heartbeat race
    # between a Python ownership check and publication.
    #
    # Return codes:
    #   1  committed
    #  -1  stale base leaf (path/version changed)
    #  -2  fencing lock lost
    #  -3  corrupt existing catalog JSON
    #  -4  invalid new payload JSON
    #  -5  missing/wrong mirror publication intent
    #  -6  mirror publication intent is not prepared
    #  -7  SuperTable namespace lock is held by a delete
    #  -8  durable SuperTable deletion intent exists
    #  -9  durable SimpleTable deletion intent exists
    # -10  mirror publication is owned by another publisher
    # -11  SuperTable root no longer exists
    # -12  SuperTable root is read-only
    # -13  leaf/root numeric identity is invalid or exhausted
    # -14  mirror configuration changed after the writer pinned it
    # -15  mirror configuration is corrupt
    # -16  payload snapshot_version mismatches the fenced successor
    # -17  invalid one-shot initial publication flag or base identity
    _LUA_SNAPSHOT_COMMIT = (
        _LUA_ROOT_DOCUMENT_GUARD + _LUA_SNAPSHOT_TOMBSTONE_GUARD + """
local leaf_key = KEYS[1]
local root_key = KEYS[2]
local lock_key = KEYS[3]
local mirror_key = KEYS[4]
local namespace_lock = KEYS[5]
local table_names = KEYS[6]
local namespace_delete = KEYS[7]
local simple_delete = KEYS[8]
local schema_key = KEYS[9]
local quality_unresolved_key = KEYS[11]

local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])
local expected_version = tonumber(ARGV[4])
local expected_path = ARGV[5]
local lock_token = ARGV[6]
local commit_id = ARGV[7]
local mirror_required = ARGV[8]
local simple_name = ARGV[9]
local schema_json = ARGV[10]
local expected_mirrors_json = ARGV[11]
local quality_generation = ARGV[12]
local expected_tombstone_prefix = ARGV[13]
local payload_digest = ARGV[14]
local one_shot_initial = ARGV[15]

if not now_ms or now_ms < 0 or now_ms > ROOT_MAX_SAFE_INTEGER
    or now_ms ~= math.floor(now_ms)
    or not expected_version or expected_version < -1
    or expected_version > ROOT_MAX_SAFE_INTEGER
    or expected_version ~= math.floor(expected_version) then
  return {-13, 0, 0}
end
if one_shot_initial ~= '0' and one_shot_initial ~= '1' then
  return {-17, 0, 0}
end
if one_shot_initial == '1'
    and (expected_version ~= -1 or expected_path ~= '') then
  return {-17, 0, 0}
end
if one_shot_initial == '1' then
  if string.len(payload_digest) ~= 64
      or not string.match(payload_digest, '^[0-9a-f]+$') then
    return {-4, 0, 0}
  end
elseif payload_digest ~= '' then
  return {-4, 0, 0}
end

local early_held_token = redis.call('GET', lock_key)
if not early_held_token or early_held_token ~= lock_token then
  return {-2, 0, 0}
end

-- Mirror configuration changes are part of snapshot publication semantics.
-- Compare the set observed by the writer at this same atomic boundary so an
-- acknowledged enable cannot be omitted by a later snapshot commit.
if not string.match(expected_mirrors_json, '^%s*%[') then
  return {-15, 0, 0}
end
local oke, expected_mirrors = pcall(cjson.decode, expected_mirrors_json)
if not oke or type(expected_mirrors) ~= 'table' then
  return {-15, 0, 0}
end
local expected_seen = {}
local expected_count = 0
for key, value in pairs(expected_mirrors) do
  if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
      or type(value) ~= 'string' then return {-15, 0, 0} end
  local normalized = string.upper(value)
  if normalized ~= 'DELTA' and normalized ~= 'ICEBERG'
      and normalized ~= 'PARQUET' then return {-15, 0, 0} end
  if expected_seen[normalized] then return {-15, 0, 0} end
  expected_seen[normalized] = true
  expected_count = expected_count + 1
end
if expected_count ~= #expected_mirrors then return {-15, 0, 0} end

local configured_seen = {}
local configured_count = 0
local mirrors_config_key = KEYS[10]
local mirrors_config_type = redis.call('TYPE', mirrors_config_key)
if type(mirrors_config_type) == 'table' then
  mirrors_config_type = mirrors_config_type['ok']
end
if mirrors_config_type ~= 'none' and mirrors_config_type ~= 'string' then
  return {-15, 0, 0}
end
if mirrors_config_type == 'string' then
  local okc, configured = pcall(
    cjson.decode, redis.call('GET', mirrors_config_key)
  )
  if not okc or type(configured) ~= 'table'
      or type(configured['formats']) ~= 'table'
      or type(configured['ts']) ~= 'number'
      or configured['ts'] < 0
      or configured['ts'] > ROOT_MAX_SAFE_INTEGER
      or configured['ts'] ~= math.floor(configured['ts']) then
    return {-15, 0, 0}
  end
  local physical_count = 0
  for key, value in pairs(configured['formats']) do
    if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
        or type(value) ~= 'string' then return {-15, 0, 0} end
    physical_count = physical_count + 1
    local normalized = string.upper(value)
    if normalized ~= 'DELTA' and normalized ~= 'ICEBERG'
        and normalized ~= 'PARQUET' then return {-15, 0, 0} end
    if configured_seen[normalized] then return {-15, 0, 0} end
    configured_seen[normalized] = true
    configured_count = configured_count + 1
  end
  if physical_count ~= #configured['formats'] then return {-15, 0, 0} end
end
if configured_count ~= expected_count then return {-14, 0, 0} end
for format, _ in pairs(expected_seen) do
  if not configured_seen[format] then return {-14, 0, 0} end
end
if expected_count > 0 and mirror_required ~= '1' then
  return {-15, 0, 0}
end

local held_token = redis.call('GET', lock_key)
if not held_token or held_token ~= lock_token then
  return {-2, 0, 0}
end
-- A waiting first-time creator may hold the namespace lock while blocked on
-- this writer's leaf lease.  It must not wound the current flagged one-shot
-- publisher.  A real delete linearizes by persisting namespace_delete before
-- draining this lease; that durable intent remains an unconditional fence.
if one_shot_initial ~= '1'
    and redis.call('EXISTS', namespace_lock) == 1 then
  return {-7, 0, 0}
end
if redis.call('EXISTS', namespace_delete) == 1 then
  return {-8, 0, 0}
end
if redis.call('EXISTS', simple_delete) == 1 then
  return {-9, 0, 0}
end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then
  return {-3, 0, 0}
end

local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return {-11, 0, 0} end
if root_type ~= 'string' then return {-3, 0, 0} end
local raw_root = redis.call('GET', root_key)
local okr, root = pcall(cjson.decode, raw_root)
if not okr or type(root) ~= 'table' then return {-3, 0, 0} end
local root_state = root_document_state(root, nil)
if root_state == -1 then return {-3, 0, 0} end
if root_state == 0 then return {-12, 0, 0} end
local root_version = root['version']

local old_version = -1
local old_path = ''
local cur = redis.call('GET', leaf_key)
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if not ok or type(obj) ~= 'table'
      or type(obj['version']) ~= 'number'
      or obj['version'] < 0
      or obj['version'] > ROOT_MAX_SAFE_INTEGER
      or obj['version'] ~= math.floor(obj['version'])
      or type(obj['ts']) ~= 'number'
      or obj['ts'] < 0
      or obj['ts'] > ROOT_MAX_SAFE_INTEGER
      or obj['ts'] ~= math.floor(obj['ts'])
      or type(obj['path']) ~= 'string'
      or obj['path'] == '' then
    return {-3, 0, 0}
  end
  old_version = obj['version']
  old_path = obj['path']
end

if old_version ~= expected_version or old_path ~= expected_path then
  return {-1, old_version, 0}
end

local okp, payload = pcall(cjson.decode, payload_json)
if not okp or type(payload) ~= 'table'
    or not snapshot_tombstone_state_ok(
      payload, expected_tombstone_prefix
    ) then
  return {-4, 0, 0}
end
local new_leaf_version = old_version + 1
if one_shot_initial == '1' then new_leaf_version = 1 end
if type(payload['snapshot_version']) ~= 'number'
    or payload['snapshot_version'] ~= math.floor(payload['snapshot_version'])
    or payload['snapshot_version'] ~= new_leaf_version then
  return {-16, 0, 0}
end
local oks, schema = pcall(cjson.decode, schema_json)
if not oks or type(schema) ~= 'table' then
  return {-4, 0, 0}
end

local mirror = nil
if mirror_required == '1' then
  local raw_mirror = redis.call('GET', mirror_key)
  if not raw_mirror then
    return {-5, 0, 0}
  end
  local okm, parsed_mirror = pcall(cjson.decode, raw_mirror)
  if not okm or type(parsed_mirror) ~= 'table'
      or tostring(parsed_mirror['commit_id'] or '') ~= commit_id then
    return {-5, 0, 0}
  end
  if parsed_mirror['status'] ~= 'prepared' then
    return {-6, 0, 0}
  end
  if tostring(parsed_mirror['publication_owner'] or '') ~= lock_token then
    return {-10, 0, 0}
  end
  if expected_count > 0 then
    local recorded_mirrors = parsed_mirror['mirrors']
    if type(recorded_mirrors) ~= 'table' then return {-5, 0, 0} end
    local recorded_seen = {}
    local recorded_count = 0
    for key, value in pairs(recorded_mirrors) do
      if type(key) ~= 'number' or key < 1 or key ~= math.floor(key)
          or type(value) ~= 'string' then return {-5, 0, 0} end
      local normalized = string.upper(value)
      if not expected_seen[normalized] or recorded_seen[normalized] then
        return {-5, 0, 0}
      end
      recorded_seen[normalized] = true
      recorded_count = recorded_count + 1
    end
    if recorded_count ~= expected_count
        or recorded_count ~= #recorded_mirrors then return {-5, 0, 0} end
  end
  mirror = parsed_mirror
end

if old_version >= ROOT_MAX_SAFE_INTEGER
    or root_version >= ROOT_MAX_SAFE_INTEGER then
  return {-13, 0, 0}
end
local new_root_version = root_version + 1
local leaf = {
  version = new_leaf_version,
  ts = now_ms,
  path = new_path,
  payload = payload,
  commit_id = commit_id
}
if one_shot_initial == '1' then leaf['payload_digest'] = payload_digest end
root['version'] = new_root_version
root['ts'] = now_ms
root['commit_id'] = commit_id

if mirror ~= nil then
  mirror['status'] = 'core_committed'
  mirror['core_committed'] = true
  mirror['publisher_quiesced'] = false
  mirror['core_committed_at_ms'] = now_ms
  mirror['updated_at_ms'] = now_ms
  mirror['leaf_version'] = new_leaf_version
  mirror['root_version'] = new_root_version
end

redis.call('SET', leaf_key, cjson.encode(leaf))
redis.call('SET', root_key, cjson.encode(root))
redis.call('SET', schema_key, schema_json)
redis.call('SADD', table_names, simple_name)
if mirror ~= nil then
  redis.call('SET', mirror_key, cjson.encode(mirror))
end
if quality_generation ~= '' then
  -- The scheduler resolves this persistent generation into configured modes.
  -- Publication here is atomic with root/leaf commit and all lifecycle fences.
  redis.call('SET', quality_unresolved_key, quality_generation)
end
return {1, new_leaf_version, new_root_version}
""")

    # No-mirror snapshot publication hot path.  The begin-mutation boundary
    # already validated the raw mirror document and proved that its format set
    # was empty.  Comparing that exact raw value (including absent-vs-present)
    # here preserves the acknowledged-enable race fence without decoding or
    # normalizing mirror configuration, and no publication intent can exist
    # for a commit with an empty mirror set.  Every core publication invariant
    # remains identical to _LUA_SNAPSHOT_COMMIT.
    _LUA_SNAPSHOT_COMMIT_NO_MIRRORS = (
        _LUA_ROOT_DOCUMENT_GUARD + _LUA_SNAPSHOT_TOMBSTONE_GUARD + """
local leaf_key = KEYS[1]
local root_key = KEYS[2]
local lock_key = KEYS[3]
local namespace_lock = KEYS[4]
local table_names = KEYS[5]
local namespace_delete = KEYS[6]
local simple_delete = KEYS[7]
local schema_key = KEYS[8]
local quality_unresolved_key = KEYS[9]
local mirrors_config_key = KEYS[10]

local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])
local expected_version = tonumber(ARGV[4])
local expected_path = ARGV[5]
local lock_token = ARGV[6]
local commit_id = ARGV[7]
local simple_name = ARGV[8]
local schema_json = ARGV[9]
local quality_generation = ARGV[10]
local mirror_pin_present = ARGV[11]
local expected_mirror_raw = ARGV[12]
local expected_tombstone_prefix = ARGV[13]
local payload_digest = ARGV[14]
local one_shot_initial = ARGV[15]

if not now_ms or now_ms < 0 or now_ms > ROOT_MAX_SAFE_INTEGER
    or now_ms ~= math.floor(now_ms)
    or not expected_version or expected_version < -1
    or expected_version > ROOT_MAX_SAFE_INTEGER
    or expected_version ~= math.floor(expected_version) then
  return {-13, 0, 0}
end
if one_shot_initial ~= '0' and one_shot_initial ~= '1' then
  return {-17, 0, 0}
end
if one_shot_initial == '1'
    and (expected_version ~= -1 or expected_path ~= '') then
  return {-17, 0, 0}
end
if one_shot_initial == '1' then
  if string.len(payload_digest) ~= 64
      or not string.match(payload_digest, '^[0-9a-f]+$') then
    return {-4, 0, 0}
  end
elseif payload_digest ~= '' then
  return {-4, 0, 0}
end

local early_held_token = redis.call('GET', lock_key)
if not early_held_token or early_held_token ~= lock_token then
  return {-2, 0, 0}
end

-- The raw pin was validated by begin_table_mutation.  Exact comparison is
-- stronger than comparing only normalized formats: any distinct generation
-- conflicts rather than letting this writer omit a newly enabled mirror.
local mirrors_config_type = redis.call('TYPE', mirrors_config_key)
if type(mirrors_config_type) == 'table' then
  mirrors_config_type = mirrors_config_type['ok']
end
if mirrors_config_type ~= 'none' and mirrors_config_type ~= 'string' then
  return {-15, 0, 0}
end
if mirror_pin_present == '0' then
  if mirrors_config_type ~= 'none' then return {-14, 0, 0} end
elseif mirror_pin_present == '1' then
  if mirrors_config_type ~= 'string'
      or redis.call('GET', mirrors_config_key) ~= expected_mirror_raw then
    return {-14, 0, 0}
  end
else
  return {-15, 0, 0}
end

local held_token = redis.call('GET', lock_key)
if not held_token or held_token ~= lock_token then
  return {-2, 0, 0}
end
-- Do not let a waiting creator's namespace lock wound the current first
-- publisher.  Namespace deletion is still fenced by its durable intent below.
if one_shot_initial ~= '1'
    and redis.call('EXISTS', namespace_lock) == 1 then
  return {-7, 0, 0}
end
if redis.call('EXISTS', namespace_delete) == 1 then
  return {-8, 0, 0}
end
if redis.call('EXISTS', simple_delete) == 1 then
  return {-9, 0, 0}
end
local names_type = redis.call('TYPE', table_names)
if type(names_type) == 'table' then names_type = names_type['ok'] end
if names_type ~= 'none' and names_type ~= 'set' then
  return {-3, 0, 0}
end

local root_type = redis.call('TYPE', root_key)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return {-11, 0, 0} end
if root_type ~= 'string' then return {-3, 0, 0} end
local raw_root = redis.call('GET', root_key)
local okr, root = pcall(cjson.decode, raw_root)
if not okr or type(root) ~= 'table' then return {-3, 0, 0} end
local root_state = root_document_state(root, nil)
if root_state == -1 then return {-3, 0, 0} end
if root_state == 0 then return {-12, 0, 0} end
local root_version = root['version']

local old_version = -1
local old_path = ''
local cur = redis.call('GET', leaf_key)
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if not ok or type(obj) ~= 'table'
      or type(obj['version']) ~= 'number'
      or obj['version'] < 0
      or obj['version'] > ROOT_MAX_SAFE_INTEGER
      or obj['version'] ~= math.floor(obj['version'])
      or type(obj['ts']) ~= 'number'
      or obj['ts'] < 0
      or obj['ts'] > ROOT_MAX_SAFE_INTEGER
      or obj['ts'] ~= math.floor(obj['ts'])
      or type(obj['path']) ~= 'string'
      or obj['path'] == '' then
    return {-3, 0, 0}
  end
  old_version = obj['version']
  old_path = obj['path']
end

if old_version ~= expected_version or old_path ~= expected_path then
  return {-1, old_version, 0}
end

local okp, payload = pcall(cjson.decode, payload_json)
if not okp or type(payload) ~= 'table'
    or not snapshot_tombstone_state_ok(
      payload, expected_tombstone_prefix
    ) then
  return {-4, 0, 0}
end
local new_leaf_version = old_version + 1
if one_shot_initial == '1' then new_leaf_version = 1 end
if type(payload['snapshot_version']) ~= 'number'
    or payload['snapshot_version'] ~= math.floor(payload['snapshot_version'])
    or payload['snapshot_version'] ~= new_leaf_version then
  return {-16, 0, 0}
end
local oks, schema = pcall(cjson.decode, schema_json)
if not oks or type(schema) ~= 'table' then
  return {-4, 0, 0}
end
if old_version >= ROOT_MAX_SAFE_INTEGER
    or root_version >= ROOT_MAX_SAFE_INTEGER then
  return {-13, 0, 0}
end

local new_root_version = root_version + 1
local leaf = {
  version = new_leaf_version,
  ts = now_ms,
  path = new_path,
  payload = payload,
  commit_id = commit_id
}
if one_shot_initial == '1' then leaf['payload_digest'] = payload_digest end
root['version'] = new_root_version
root['ts'] = now_ms
root['commit_id'] = commit_id

redis.call('SET', leaf_key, cjson.encode(leaf))
redis.call('SET', root_key, cjson.encode(root))
redis.call('SET', schema_key, schema_json)
redis.call('SADD', table_names, simple_name)
if quality_generation ~= '' then
  redis.call('SET', quality_unresolved_key, quality_generation)
end
return {1, new_leaf_version, new_root_version}
""")

    _LUA_MIRROR_PUBLICATION_PREPARE = _LUA_ROOT_DOCUMENT_GUARD + """
local state_key = KEYS[1]
local lock_key = KEYS[2]
local namespace_delete = KEYS[3]
local simple_delete = KEYS[4]
local root_key = KEYS[5]
local record_json = ARGV[1]
local lock_token = ARGV[2]
local commit_id = ARGV[3]

local held_token = redis.call('GET', lock_key)
if not held_token or held_token ~= lock_token then
  return -2
end
if redis.call('EXISTS', namespace_delete) == 1 then return -5 end
if redis.call('EXISTS', simple_delete) == 1 then return -6 end
local root_raw = redis.call('GET', root_key)
if not root_raw then return -8 end
local root_ok, root = pcall(cjson.decode, root_raw)
if not root_ok then return -9 end
local root_state = root_document_state(root, nil)
if root_state == -1 then return -9 end
if root_state == 0 then return -10 end

local okr, record = pcall(cjson.decode, record_json)
if not okr or type(record) ~= 'table'
    or tostring(record['commit_id'] or '') ~= commit_id
    or tostring(record['publication_owner'] or '') ~= lock_token
    or record['status'] ~= 'prepared' then
  return -4
end

local current_raw = redis.call('GET', state_key)
if current_raw then
  local okc, current = pcall(cjson.decode, current_raw)
  if not okc or type(current) ~= 'table' then
    return -3
  end
  if tostring(current['commit_id'] or '') == commit_id then
    if tostring(current['publication_owner'] or '') ~= lock_token then
      return -7
    end
    if tostring(current['snapshot_path'] or '')
          ~= tostring(record['snapshot_path'] or '') then
      return -4
    end
    local current_mirrors = current['mirrors'] or {}
    local new_mirrors = record['mirrors'] or {}
    if #current_mirrors ~= #new_mirrors then
      return -4
    end
    for i = 1, #current_mirrors do
      if tostring(current_mirrors[i]) ~= tostring(new_mirrors[i]) then
        return -4
      end
    end
    return 2
  end
  if current['status'] ~= 'complete'
      and not (current['status'] == 'failed'
          and current['core_committed'] ~= true) then
    return -1
  end
end

redis.call('SET', state_key, record_json)
return 1
"""

    # Rebind an unresolved mirror intent only after an operator has established
    # that the exact previous publisher cannot resume object-store I/O.  A
    # Redis lease expiring is intentionally insufficient evidence.
    _LUA_MIRROR_PUBLICATION_CLAIM = """
local state_key = KEYS[1]
local lock_key = KEYS[2]
local namespace_delete = KEYS[3]
local simple_delete = KEYS[4]
local commit_id = ARGV[1]
local expected_previous_owner = ARGV[2]
local lock_token = ARGV[3]
local now_ms = tonumber(ARGV[4])
local previous_owner_stopped = ARGV[5]

if lock_token == '' or redis.call('GET', lock_key) ~= lock_token then
  return -2
end
if redis.call('EXISTS', namespace_delete) == 1 then return -6 end
if redis.call('EXISTS', simple_delete) == 1 then return -7 end
local raw = redis.call('GET', state_key)
if not raw then return -1 end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then return -3 end
if tostring(record['commit_id'] or '') ~= commit_id then return -1 end
if tostring(record['status'] or '') == 'complete' then return -4 end

-- A cooperative, exact-owner failure transition runs only after its mirror
-- call has returned, and durably marks that publisher quiescent.  Any
-- abandoned prepared/core-committed owner still needs operator confirmation.
if record['publisher_quiesced'] ~= true
    and previous_owner_stopped ~= '1' then
  return -5
end

local current_owner = tostring(record['publication_owner'] or '')
if current_owner == lock_token then return 2 end
if current_owner ~= expected_previous_owner then return -4 end

record['previous_publication_owner'] = current_owner
record['publication_owner'] = lock_token
record['owner_claimed_at_ms'] = now_ms
record['updated_at_ms'] = now_ms
record['owner_generation'] = tonumber(record['owner_generation'] or 0) + 1
record['publisher_quiesced'] = false
redis.call('SET', state_key, cjson.encode(record))
return 1
"""

    _LUA_MIRROR_PUBLICATION_TRANSITION = """
local state_key = KEYS[1]
local lock_key = KEYS[2]
local namespace_delete = KEYS[3]
local simple_delete = KEYS[4]
local commit_id = ARGV[1]
local target_status = ARGV[2]
local now_ms = tonumber(ARGV[3])
local lock_token = ARGV[4]
local failure_stage = ARGV[5]
local error_type = ARGV[6]
local error_message = ARGV[7]

local held_token = redis.call('GET', lock_key)
if not held_token or held_token ~= lock_token then
  return -2
end
if redis.call('EXISTS', namespace_delete) == 1 then return -5 end
if redis.call('EXISTS', simple_delete) == 1 then return -6 end

local raw = redis.call('GET', state_key)
if not raw then
  return -1
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
  return -3
end
if tostring(record['commit_id'] or '') ~= commit_id then
  return -1
end
if tostring(record['publication_owner'] or '') ~= lock_token then
  return -7
end

local current_status = tostring(record['status'] or '')
if target_status == 'complete' then
  if current_status == 'complete' then
    return 2
  end
  if current_status ~= 'core_committed'
      and not (current_status == 'failed' and record['core_committed'] == true) then
    return -4
  end
  record['status'] = 'complete'
  record['completed_at_ms'] = now_ms
  record['updated_at_ms'] = now_ms
  record['publisher_quiesced'] = true
  record['error'] = cjson.null
elseif target_status == 'failed' then
  if current_status == 'complete' then
    return -4
  end
  record['status'] = 'failed'
  record['updated_at_ms'] = now_ms
  record['failed_at_ms'] = now_ms
  -- A generic object-store error is ambiguous: a timed-out remote request may
  -- become visible after the Python call returns.  Only failures known to be
  -- outside mirror I/O can establish quiescence without operator evidence.
  record['publisher_quiesced'] = (
      failure_stage == 'core_commit'
      or failure_stage == 'recovery:core_not_committed'
      or failure_stage == 'outbox_complete'
  )
  record['failure_stage'] = failure_stage
  record['error'] = {type=error_type, message=error_message}
else
  return -4
end

redis.call('SET', state_key, cjson.encode(record))
return 1
"""

    _LUA_RESERVE_ROWIDS_AT_LEAST = _LUA_ROOT_DOCUMENT_GUARD + """
local key = KEYS[1]
local leaf_lock = KEYS[2]
local leaf = KEYS[3]
local root = KEYS[4]
local namespace_intent = KEYS[5]
local simple_intent = KEYS[6]
local floor = ARGV[1]
local count = ARGV[2]
local lock_token = ARGV[3]

if lock_token == '' or redis.call('GET', leaf_lock) ~= lock_token then
  return {-1}
end
if redis.call('EXISTS', namespace_intent) == 1 then return {-2} end
if redis.call('EXISTS', simple_intent) == 1 then return {-3} end

local root_type = redis.call('TYPE', root)
if type(root_type) == 'table' then root_type = root_type['ok'] end
if root_type == 'none' then return {-4} end
if root_type ~= 'string' then return {-5} end
local root_ok, root_doc = pcall(cjson.decode, redis.call('GET', root))
if not root_ok or type(root_doc) ~= 'table' then return {-5} end
local root_state = root_document_state(root_doc, nil)
if root_state == -1 then return {-5} end
if root_state == 0 then return {-8} end

local leaf_type = redis.call('TYPE', leaf)
if type(leaf_type) == 'table' then leaf_type = leaf_type['ok'] end
if leaf_type == 'none' then return {-6} end
if leaf_type ~= 'string' then return {-7} end
local leaf_ok, leaf_doc = pcall(cjson.decode, redis.call('GET', leaf))
if not leaf_ok or type(leaf_doc) ~= 'table'
    or type(leaf_doc['version']) ~= 'number'
    or leaf_doc['version'] < 0
    or leaf_doc['version'] > ROOT_MAX_SAFE_INTEGER
    or leaf_doc['version'] ~= math.floor(leaf_doc['version'])
    or type(leaf_doc['ts']) ~= 'number'
    or leaf_doc['ts'] < 0
    or leaf_doc['ts'] > ROOT_MAX_SAFE_INTEGER
    or leaf_doc['ts'] ~= math.floor(leaf_doc['ts'])
    or type(leaf_doc['path']) ~= 'string'
    or leaf_doc['path'] == '' then return {-7} end

local cur = redis.call('GET', key) or '0'

-- Lua numbers lose integer precision above 2^53.  Row ids are signed Int64,
-- so compare their non-negative decimal strings by length/lexicographic order
-- and let Redis INCRBY perform the exact 64-bit addition/overflow check.
local function normalize_decimal(value)
  local normalized = string.gsub(value, '^0+', '')
  if normalized == '' then
    return '0'
  end
  return normalized
end
local function decimal_lt(a, b)
  a = normalize_decimal(a)
  b = normalize_decimal(b)
  if string.len(a) ~= string.len(b) then
    return string.len(a) < string.len(b)
  end
  return a < b
end

-- Never let a corrupt/negative Redis counter generate zero, negative, or
-- reused row ids.  Redis INCRBY is exact for signed Int64 values, but it will
-- happily increment a negative integer unless we reject it first.
if not string.match(cur, '^%d+$')
    or not string.match(floor, '^%d+$')
    or not string.match(count, '^%d+$') then
  return redis.error_reply('invalid non-negative rowid sequence')
end

cur = normalize_decimal(cur)
floor = normalize_decimal(floor)
if decimal_lt(cur, floor) then
  cur = floor
  redis.call('SET', key, cur)
end
redis.call('INCRBY', key, count)
local new_value = redis.call('GET', key)
-- Return strings so the Redis/Lua boundary cannot round either value.
return {cur, new_value}
"""

    # ------------- RBAC Lua scripts ------------- #

    # Mandatory privileged-audit WAL support shared by every RBAC mutation.
    #
    # The final three KEYS are always the immutable activation anchor, the
    # organization-level privileged outbox STREAM, and its sequence/meta HASH;
    # the final ARGV is a validated event
    # template JSON produced by ``audit.privileged``.  The append happens only
    # after all data-dependent validation and immediately before deterministic
    # commit commands.  Audit configuration is deliberately irrelevant here:
    # a privileged state transition cannot succeed without its durable record.
    #
    # Redis Lua does not roll back commands preceding a runtime error, so this
    # preamble validates the two audit key types, sequence value, event shape,
    # and size before any mutation script performs a write.
    _LUA_RBAC_AUDIT_PREAMBLE = """
local privileged_audit_activation = KEYS[#KEYS - 2]
local privileged_audit_outbox = KEYS[#KEYS - 1]
local privileged_audit_meta = KEYS[#KEYS]
local privileged_audit_json = ARGV[#ARGV]
local privileged_audit_org = ARGV[#ARGV - 2]
local privileged_audit_super = ARGV[#ARGV - 1]

local function normalized_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then value = value['ok'] end
    return tostring(value)
end

local function require_key_type(key, expected, allow_none)
    local actual = normalized_type(key)
    if actual ~= expected and not (allow_none and actual == 'none') then
        error(
            'RBAC commit key has wrong Redis type: expected '
            .. expected .. ', got ' .. actual
        )
    end
end

local function normalized_decimal(value)
    local normalized = string.gsub(value, '^0+', '')
    return (normalized == '') and '0' or normalized
end

local function increment_decimal(value)
    value = normalized_decimal(value)
    if string.len(value) > 19
        or (string.len(value) == 19 and value > '9223372036854775806') then
        error('RBAC/audit revision counter cannot be incremented safely')
    end
    local carry = 1
    local output = ''
    for index = string.len(value), 1, -1 do
        local digit = tonumber(string.sub(value, index, index))
        local next_digit = digit + carry
        if next_digit >= 10 then
            next_digit = next_digit - 10
            carry = 1
        else
            carry = 0
        end
        output = tostring(next_digit) .. output
    end
    if carry == 1 then output = '1' .. output end
    return output
end

local function incrementable_hash_counter(key, field)
    local value = redis.call('HGET', key, field) or '0'
    if value ~= '0' and not string.match(value, '^[1-9]%d*$') then
        error('RBAC/audit revision counter is corrupt')
    end
    return increment_decimal(value)
end

local function incrementable_namespace_counter(key, field)
    local value = redis.call('HGET', key, field)
    if value == false then
        local key_type = normalized_type(key)
        if key_type == 'hash' and redis.call('HLEN', key) > 0 then
            error('RBAC namespace revision head is missing')
        end
        value = '0'
    end
    if value ~= '0' and not string.match(value, '^[1-9]%d*$') then
        error('RBAC/audit revision counter is corrupt')
    end
    return increment_decimal(value)
end

local function require_namespace_head_for_state(key, has_state)
    if has_state and redis.call('HGET', key, 'version') == false then
        error('RBAC namespace revision head is missing')
    end
end

local activation_type = normalized_type(privileged_audit_activation)
if activation_type ~= 'string' then
    return redis.error_reply(
        'privileged audit activation baseline is not anchored'
    )
end
local activation_json = redis.call('GET', privileged_audit_activation)
local activation_ok, activation = pcall(cjson.decode, activation_json or '')
if not activation_ok
    or type(activation) ~= 'table'
    or activation['version'] ~= 1
    or activation['kind'] ~= 'supertable_privileged_activation_anchor'
    or activation['organization'] ~= privileged_audit_org
    or type(activation['activation_id']) ~= 'string'
    or activation['activation_id'] == ''
    or type(activation['state_sha256']) ~= 'string'
    or string.len(activation['state_sha256']) ~= 64
    or not string.match(activation['state_sha256'], '^[0-9a-f]+$')
    or type(activation['artifact_sha256']) ~= 'string'
    or string.len(activation['artifact_sha256']) ~= 64
    or not string.match(activation['artifact_sha256'], '^[0-9a-f]+$') then
    return redis.error_reply(
        'privileged audit activation baseline anchor is invalid'
    )
end

local outbox_type = normalized_type(privileged_audit_outbox)
if outbox_type ~= 'none' and outbox_type ~= 'stream' then
    return redis.error_reply('privileged audit outbox has wrong Redis type')
end
local audit_meta_type = normalized_type(privileged_audit_meta)
if audit_meta_type ~= 'none' and audit_meta_type ~= 'hash' then
    return redis.error_reply('privileged audit meta has wrong Redis type')
end
local outbox_length = 0
if outbox_type == 'stream' then
    outbox_length = redis.call('XLEN', privileged_audit_outbox)
end
local stored_audit_sequence = redis.call(
    'HGET', privileged_audit_meta, 'sequence'
)
if stored_audit_sequence == false then
    -- Once either side of the ledger exists, losing its monotonic head is an
    -- integrity failure, never permission to silently start again at 1.
    local meta_fields = 0
    if audit_meta_type == 'hash' then
        meta_fields = redis.call('HLEN', privileged_audit_meta)
    end
    if outbox_length > 0 or meta_fields > 0 then
        return redis.error_reply('privileged audit sequence head is missing')
    end
end
local current_audit_sequence = stored_audit_sequence or '0'
if current_audit_sequence ~= '0'
    and not string.match(current_audit_sequence, '^[1-9]%d*$') then
    return redis.error_reply('privileged audit sequence is corrupt')
end
if outbox_type == 'none' and current_audit_sequence ~= '0' then
    return redis.error_reply('privileged audit stream is missing')
end
if outbox_type == 'stream'
    and outbox_length == 0
    and current_audit_sequence ~= '0' then
    return redis.error_reply('privileged audit stream head is missing')
end
if outbox_length > 0 then
    if current_audit_sequence == '0' then
        return redis.error_reply('privileged audit stream has a zero sequence head')
    end
    local latest = redis.call(
        'XREVRANGE', privileged_audit_outbox, '+', '-', 'COUNT', 1
    )
    if #latest ~= 1 then
        return redis.error_reply('privileged audit stream head is unreadable')
    end
    local latest_id = latest[1][1]
    local latest_sequence = nil
    local latest_event_id = nil
    local latest_payload_hash = nil
    local latest_fields = latest[1][2]
    for index = 1, #latest_fields, 2 do
        if latest_fields[index] == 'ledger_sequence' then
            latest_sequence = latest_fields[index + 1]
        elseif latest_fields[index] == 'event_id' then
            latest_event_id = latest_fields[index + 1]
        elseif latest_fields[index] == 'payload_hash' then
            latest_payload_hash = latest_fields[index + 1]
        end
    end
    local stored_stream_id = redis.call(
        'HGET', privileged_audit_meta, 'last_stream_id'
    )
    local stored_event_id = redis.call(
        'HGET', privileged_audit_meta, 'last_event_id'
    )
    local stored_payload_hash = redis.call(
        'HGET', privileged_audit_meta, 'last_payload_hash'
    )
    if stored_stream_id == false
        or stored_stream_id ~= latest_id
        or latest_sequence == nil
        or latest_sequence ~= current_audit_sequence
        or stored_event_id == false
        or stored_event_id ~= latest_event_id
        or stored_payload_hash == false
        or stored_payload_hash ~= latest_payload_hash then
        return redis.error_reply('privileged audit stream/meta heads disagree')
    end
end
local next_audit_sequence = increment_decimal(current_audit_sequence)
if string.len(privileged_audit_json) > 65536 then
    return redis.error_reply('privileged audit record exceeds 65536 bytes')
end
local audit_ok, privileged_audit_event = pcall(
    cjson.decode, privileged_audit_json
)
if not audit_ok or type(privileged_audit_event) ~= 'table' then
    return redis.error_reply('invalid privileged audit record')
end
if privileged_audit_event['organization'] ~= privileged_audit_org
    or privileged_audit_event['super_name'] ~= privileged_audit_super then
    return redis.error_reply('privileged audit scope does not match RBAC commit')
end
for _, required_field in ipairs({
    'event_id', 'mutation_id', 'organization', 'super_name', 'action',
    'actor_type', 'actor_id', 'resource_type', 'resource_id', 'payload_hash'
}) do
    local value = privileged_audit_event[required_field]
    if value == nil or type(value) ~= 'string' or value == '' then
        return redis.error_reply(
            'privileged audit record missing ' .. required_field
        )
    end
end
local privileged_audit_outcome = privileged_audit_event['outcome']
if privileged_audit_event['schema_version'] ~= 1
    or (
        privileged_audit_outcome ~= 'success'
        and privileged_audit_outcome ~= 'failure'
        and privileged_audit_outcome ~= 'denied'
        and privileged_audit_outcome ~= 'no_change'
    )
    or privileged_audit_event['ledger_sequence'] ~= 0
    or privileged_audit_event['namespace_version'] ~= 0
    or privileged_audit_event['affected_count'] ~= 0
    or privileged_audit_event['cascade_assignment_count'] ~= 0
    or privileged_audit_event['user_namespace_version_before'] ~= 0
    or privileged_audit_event['user_namespace_version_after'] ~= 0 then
    return redis.error_reply('privileged audit template has invalid commit fields')
end

local function require_audit_identity(action, resource_type, resource_id)
    if privileged_audit_outcome ~= 'success'
        or privileged_audit_event['action'] ~= action
        or privileged_audit_event['resource_type'] ~= resource_type
        or privileged_audit_event['resource_id'] ~= resource_id then
        error('privileged audit identity does not match RBAC commit')
    end
end
local privileged_cascade_manifest_id = privileged_audit_event[
    'cascade_manifest_id'
]
if type(privileged_cascade_manifest_id) ~= 'string' then
    return redis.error_reply('privileged audit cascade manifest ID is invalid')
end
if privileged_audit_outcome == 'success'
    and privileged_audit_event['action'] == 'role_delete' then
    if privileged_cascade_manifest_id ~= privileged_audit_event['event_id'] then
        return redis.error_reply(
            'role deletion cascade manifest must equal its event ID'
        )
    end
elseif privileged_cascade_manifest_id ~= '' then
    return redis.error_reply(
        'only successful role deletion may reference a cascade manifest'
    )
end
if string.len(privileged_audit_event['payload_hash']) ~= 64
    or not string.match(privileged_audit_event['payload_hash'], '^[0-9a-f]+$') then
    return redis.error_reply('privileged audit payload hash is invalid')
end

local function append_privileged_audit(extra_fields)
    local sequence = next_audit_sequence
    local namespace_version = '0'
    local affected_count = '0'
    local cascade_assignment_count = '0'
    local user_namespace_version_before = '0'
    local user_namespace_version_after = '0'
    if extra_fields and extra_fields['namespace_version'] ~= nil then
        namespace_version = tostring(extra_fields['namespace_version'])
    end
    if extra_fields and extra_fields['affected_count'] ~= nil then
        affected_count = tostring(extra_fields['affected_count'])
    end
    if extra_fields and extra_fields['cascade_assignment_count'] ~= nil then
        cascade_assignment_count = tostring(
            extra_fields['cascade_assignment_count']
        )
    end
    if extra_fields and extra_fields['user_namespace_version_before'] ~= nil then
        user_namespace_version_before = tostring(
            extra_fields['user_namespace_version_before']
        )
    end
    if extra_fields and extra_fields['user_namespace_version_after'] ~= nil then
        user_namespace_version_after = tostring(
            extra_fields['user_namespace_version_after']
        )
    end
    local stream_id = redis.call(
        'XADD', privileged_audit_outbox, '*',
        -- Preserve the Python-validated template byte-for-byte.  Redis cjson
        -- cannot round-trip empty arrays or integers above 2^53 reliably.
        -- Exact commit-assigned decimals therefore live in the immutable
        -- stream envelope and are merged by PrivilegedAuditOutbox.
        'event_json', privileged_audit_json,
        'event_id', privileged_audit_event['event_id'],
        'mutation_id', privileged_audit_event['mutation_id'],
        'action', privileged_audit_event['action'],
        'resource_type', privileged_audit_event['resource_type'],
        'resource_id', privileged_audit_event['resource_id'],
        'organization', privileged_audit_event['organization'],
        'super_name', privileged_audit_event['super_name'],
        'ledger_sequence', tostring(sequence),
        'namespace_version', namespace_version,
        'affected_count', affected_count,
        'cascade_manifest_id', privileged_audit_event['cascade_manifest_id'],
        'cascade_assignment_count', cascade_assignment_count,
        'user_namespace_version_before', user_namespace_version_before,
        'user_namespace_version_after', user_namespace_version_after,
        'payload_hash', privileged_audit_event['payload_hash']
    )
    redis.call(
        'HSET', privileged_audit_meta,
        'sequence', sequence,
        'last_stream_id', stream_id,
        'last_event_id', privileged_audit_event['event_id'],
        'last_payload_hash', privileged_audit_event['payload_hash'],
        'updated_ms', tostring(privileged_audit_event['timestamp_ms'] or '')
    )
    return stream_id
end
"""

    # Append a rejected/failed/no-change privileged attempt without touching
    # RBAC state.  The relevant namespace revision is sampled inside the same
    # Lua invocation as XADD so the evidence cannot claim a revision observed
    # before or after some unrelated race.  Successful mutations are rejected
    # here; they must use one of the transactional mutation scripts above.
    # KEYS: namespace meta, optional condition keys, activation anchor,
    #       privileged outbox, privileged audit meta
    # ARGV: optional condition JSON, organization, super table, validated
    #       event template JSON
    _LUA_RBAC_APPEND_ATTEMPT = _LUA_RBAC_AUDIT_PREAMBLE + """
if privileged_audit_outcome == 'success' then
    return redis.error_reply(
        'successful privileged events require an RBAC mutation script'
    )
end
local attempt_action = privileged_audit_event['action']
local attempt_resource_type = privileged_audit_event['resource_type']
local valid_attempt_identity = (
    (
        attempt_action == 'role_create'
        or attempt_action == 'role_update'
        or attempt_action == 'role_delete'
    )
    and attempt_resource_type == 'role'
) or (
    (
        attempt_action == 'user_create'
        or attempt_action == 'user_update'
        or attempt_action == 'user_delete'
    )
    and attempt_resource_type == 'user'
) or (
    (
        attempt_action == 'user_role_assign'
        or attempt_action == 'user_role_remove'
    )
    and attempt_resource_type == 'user_role_assignment'
) or (
    (
        attempt_action == 'token_create'
        or attempt_action == 'token_delete'
    )
    and attempt_resource_type == 'auth_token'
)
if not valid_attempt_identity then
    return redis.error_reply('invalid privileged RBAC attempt identity')
end
local namespace_type = normalized_type(KEYS[1])
local namespace_version = '0'
if privileged_audit_outcome == 'failure'
    and namespace_type ~= 'none'
    and namespace_type ~= 'hash' then
    -- A deterministic state-key integrity failure must still be recordable
    -- when the organization audit stream/meta boundary itself is healthy.
    namespace_version = '0'
else
    require_key_type(KEYS[1], 'hash', true)
    local stored_namespace_version = redis.call('HGET', KEYS[1], 'version')
    if stored_namespace_version == false then
        if namespace_type == 'hash' and redis.call('HLEN', KEYS[1]) > 0 then
            if privileged_audit_outcome ~= 'failure' then
                return redis.error_reply('RBAC namespace revision head is missing')
            end
        else
            namespace_version = '0'
        end
    else
        namespace_version = stored_namespace_version
    end
end
if namespace_version ~= '0'
    and not string.match(namespace_version, '^[1-9]%d*$') then
    if privileged_audit_outcome == 'failure' then
        namespace_version = '0'
    else
        return redis.error_reply('RBAC namespace revision counter is corrupt')
    end
end
if string.len(namespace_version) > 19
    or (
        string.len(namespace_version) == 19
        and namespace_version > '9223372036854775807'
    ) then
    if privileged_audit_outcome == 'failure' then
        namespace_version = '0'
    else
        return redis.error_reply('RBAC namespace revision counter is out of range')
    end
end

-- A state-dependent no-op is evidence only while the state that justified it
-- is still true.  Conditions are generated internally, strictly bounded, and
-- evaluated in this same script as XADD.  A miss returns 0 without advancing
-- the ledger; the caller must surface a concurrent-state retry.
if #ARGV ~= 3 and #ARGV ~= 4 then
    return redis.error_reply('invalid privileged RBAC attempt arguments')
end
if privileged_audit_outcome == 'no_change' and #ARGV ~= 4 then
    return redis.error_reply('no-change RBAC attempts require conditions')
end
if #ARGV == 4 then
    if privileged_audit_outcome ~= 'no_change' then
        return redis.error_reply('only no-change attempts may be conditional')
    end
    local condition_json = ARGV[1]
    if string.len(condition_json) == 0
        or string.len(condition_json) > 16384
        or string.sub(condition_json, 1, 1) ~= '['
        or string.sub(condition_json, -1) ~= ']' then
        return redis.error_reply('invalid RBAC attempt condition envelope')
    end
    local decoded_ok, conditions = pcall(cjson.decode, condition_json)
    if not decoded_ok or type(conditions) ~= 'table'
        or #conditions == 0 or #conditions > 16
        or #conditions ~= (#KEYS - 4) then
        return redis.error_reply('invalid RBAC attempt conditions')
    end
    for condition_index, condition in ipairs(conditions) do
        if type(condition) ~= 'table' or type(condition['kind']) ~= 'string' then
            return redis.error_reply('invalid RBAC attempt condition')
        end
        local condition_key = KEYS[condition_index + 1]
        local condition_kind = condition['kind']
        local actual_type = normalized_type(condition_key)
        if condition_kind == 'absent' then
            if actual_type ~= 'none' then return 0 end
        elseif condition_kind == 'exists' then
            if actual_type == 'none' then return 0 end
        elseif condition_kind == 'hash_fields' then
            local fields = condition['fields']
            if actual_type ~= 'hash' or type(fields) ~= 'table'
                or #fields == 0 or #fields > 16 then
                return 0
            end
            for _, field in ipairs(fields) do
                if type(field) ~= 'table'
                    or type(field['name']) ~= 'string'
                    or type(field['value']) ~= 'string' then
                    return redis.error_reply('invalid RBAC hash condition')
                end
                if redis.call('HSTRLEN', condition_key, field['name'])
                    ~= string.len(field['value'])
                    or redis.call('HGET', condition_key, field['name'])
                        ~= field['value'] then
                    return 0
                end
            end
        elseif condition_kind == 'hash_field_absent' then
            if type(condition['field']) ~= 'string'
                or condition['field'] == '' then
                return redis.error_reply('invalid RBAC absent-hash-field condition')
            end
            if actual_type == 'hash'
                and redis.call('HEXISTS', condition_key, condition['field']) == 1 then
                return 0
            elseif actual_type ~= 'hash' and actual_type ~= 'none' then
                return 0
            end
        elseif condition_kind == 'json_array_membership' then
            if actual_type ~= 'hash'
                or type(condition['field']) ~= 'string'
                or type(condition['item']) ~= 'string'
                or type(condition['present']) ~= 'boolean'
                or type(condition['version']) ~= 'string' then
                return 0
            end
            local roles_json = redis.call(
                'HGET', condition_key, condition['field']
            )
            local actual_version = redis.call(
                'HGET', condition_key, 'doc_version'
            ) or '0'
            if not roles_json or actual_version ~= condition['version'] then
                return 0
            end
            if string.len(roles_json) > 65536 then
                return redis.error_reply('RBAC assignment list exceeds audit limit')
            end
            if string.sub(roles_json, 1, 1) ~= '['
                or string.sub(roles_json, -1) ~= ']' then
                return redis.error_reply('RBAC assignment list is corrupt')
            end
            local roles_ok, roles = pcall(cjson.decode, roles_json)
            if not roles_ok or type(roles) ~= 'table' then
                return redis.error_reply('RBAC assignment list is corrupt')
            end
            local found = false
            for _, assigned_id in ipairs(roles) do
                if type(assigned_id) ~= 'string' then
                    return redis.error_reply('RBAC assignment list is corrupt')
                end
                if assigned_id == condition['item'] then found = true end
            end
            if found ~= condition['present'] then return 0 end
        elseif condition_kind == 'json_array_equals' then
            if actual_type ~= 'hash'
                or type(condition['field']) ~= 'string'
                or type(condition['items']) ~= 'table'
                or type(condition['version']) ~= 'string' then
                return 0
            end
            local actual_json = redis.call(
                'HGET', condition_key, condition['field']
            )
            local actual_version = redis.call(
                'HGET', condition_key, 'doc_version'
            ) or '0'
            if not actual_json or actual_version ~= condition['version'] then
                return 0
            end
            if string.len(actual_json) > 65536
                or string.sub(actual_json, 1, 1) ~= '['
                or string.sub(actual_json, -1) ~= ']' then
                return redis.error_reply('RBAC assignment list is corrupt')
            end
            local actual_ok, actual_items = pcall(cjson.decode, actual_json)
            if not actual_ok or type(actual_items) ~= 'table'
                or #actual_items ~= #condition['items'] then
                return 0
            end
            for _, item in ipairs(actual_items) do
                if type(item) ~= 'string' then
                    return redis.error_reply('RBAC assignment list is corrupt')
                end
            end
            for _, item in ipairs(condition['items']) do
                if type(item) ~= 'string' then
                    return redis.error_reply('invalid RBAC array condition')
                end
            end
            table.sort(actual_items)
            table.sort(condition['items'])
            for index, item in ipairs(actual_items) do
                if item ~= condition['items'][index] then return 0 end
            end
        elseif condition_kind == 'set_cardinality' then
            if type(condition['count']) ~= 'string'
                or (actual_type ~= 'set' and actual_type ~= 'none') then
                return 0
            end
            local actual_count = '0'
            if actual_type == 'set' then
                actual_count = tostring(redis.call('SCARD', condition_key))
            end
            if actual_count ~= condition['count'] then return 0 end
        else
            return redis.error_reply('unknown RBAC attempt condition')
        end
    end
end
append_privileged_audit({namespace_version=namespace_version})
return 1
"""

    # Validate one RBAC namespace without changing it.  The first audited
    # mutation creates its revision metadata in the same Lua transaction as
    # the security document and success event; bootstrap validation must not
    # manufacture an unaudited namespace revision.
    # KEYS: namespace meta HASH, authoritative ID SET, identity-name HASH
    _LUA_RBAC_VALIDATE_META = """
local function normalized_type(key)
    local reply = redis.call('TYPE', key)
    if type(reply) == 'table' then return reply['ok'] end
    return reply
end
local meta_type = normalized_type(KEYS[1])
local index_type = normalized_type(KEYS[2])
local name_type = normalized_type(KEYS[3])
if (meta_type ~= 'none' and meta_type ~= 'hash')
    or (index_type ~= 'none' and index_type ~= 'set')
    or (name_type ~= 'none' and name_type ~= 'hash') then
    return -2
end
if meta_type == 'none' then
    local indexed = index_type == 'set' and redis.call('SCARD', KEYS[2]) or 0
    local named = name_type == 'hash' and redis.call('HLEN', KEYS[3]) or 0
    if indexed ~= 0 or named ~= 0 then return -1 end
    return 0
end
local version = redis.call('HGET', KEYS[1], 'version')
if version == false then return -1 end
if version ~= '0' and not string.match(version, '^[1-9]%d*$') then return -1 end
if string.len(version) > 19
    or (string.len(version) == 19 and version > '9223372036854775807') then
    return -1
end
return 0
"""

    # Atomically claim the case-insensitive role name and publish every role
    # index together with the document and revision.  Client-side uniqueness
    # checks remain useful for friendly errors, but cannot close a concurrent
    # check-then-create race on their own.
    # KEYS: document, role index, type index, name map, role meta
    # ARGV: role_id, role_type, lower-name, serialized-field-map JSON, now_ms
    _LUA_RBAC_CREATE_ROLE = _LUA_RBAC_AUDIT_PREAMBLE + """
local role_id = ARGV[1]
local role_type = ARGV[2]
local role_name_lower = ARGV[3]
local role_document_json = ARGV[4]
local now_ms = ARGV[5]
require_audit_identity('role_create', 'role', role_id)

require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'set', true)
require_key_type(KEYS[4], 'hash', true)
require_key_type(KEYS[5], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 1 then
    return -1
end
if role_name_lower ~= '' and redis.call('HEXISTS', KEYS[4], role_name_lower) == 1 then
    return -2
end

local ok, document = pcall(cjson.decode, role_document_json)
if not ok or type(document) ~= 'table' then
    return redis.error_reply('invalid canonical role document')
end
require_namespace_head_for_state(
    KEYS[5], redis.call('SCARD', KEYS[2]) > 0
        or redis.call('HLEN', KEYS[4]) > 0
)
local next_role_namespace_version = incrementable_namespace_counter(
    KEYS[5], 'version'
)
append_privileged_audit({namespace_version=next_role_namespace_version})
for field, value in pairs(document) do
    redis.call('HSET', KEYS[1], field, value)
end
redis.call('SADD', KEYS[2], role_id)
redis.call('SADD', KEYS[3], role_id)
if role_name_lower ~= '' then
    redis.call('HSET', KEYS[4], role_name_lower, role_id)
end
redis.call('HINCRBY', KEYS[5], 'version', 1)
redis.call('HSET', KEYS[5], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[5], 'initialized', 'true')
return 1
"""

    # Same atomic uniqueness/publication boundary for users.  In particular,
    # concurrent process bootstrap must not mint two default superuser docs
    # and leave only one reachable through the name map.
    # KEYS: document, user index, name map, user meta
    # ARGV: user_id, lower-name, serialized-field-map JSON, now_ms
    _LUA_RBAC_CREATE_USER = _LUA_RBAC_AUDIT_PREAMBLE + """
local user_id = ARGV[1]
local username_lower = ARGV[2]
local user_document_json = ARGV[3]
local now_ms = ARGV[4]
local role_doc_key_prefix = ARGV[5]
require_audit_identity('user_create', 'user', user_id)

require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'hash', true)
require_key_type(KEYS[4], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 1 then
    return -1
end
if redis.call('HEXISTS', KEYS[3], username_lower) == 1 then
    return -2
end
local ok, document = pcall(cjson.decode, user_document_json)
if not ok or type(document) ~= 'table' then
    return redis.error_reply('invalid canonical user document')
end
local roles_ok, roles = pcall(cjson.decode, document['roles'] or '[]')
if not roles_ok or type(roles) ~= 'table' then return -3 end
for _, assigned_role_id in ipairs(roles) do
    local assigned_type = redis.call(
        'HGET', role_doc_key_prefix .. assigned_role_id, 'role'
    )
    if assigned_type ~= 'superadmin'
        and assigned_type ~= 'admin'
        and assigned_type ~= 'writer'
        and assigned_type ~= 'reader'
        and assigned_type ~= 'meta' then
        return -3
    end
end
require_namespace_head_for_state(
    KEYS[4], redis.call('SCARD', KEYS[2]) > 0
        or redis.call('HLEN', KEYS[3]) > 0
)
local next_user_namespace_version = incrementable_namespace_counter(
    KEYS[4], 'version'
)
append_privileged_audit({namespace_version=next_user_namespace_version})
for field, value in pairs(document) do
    redis.call('HSET', KEYS[1], field, value)
end
redis.call('SADD', KEYS[2], user_id)
redis.call('HSET', KEYS[3], username_lower, user_id)
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[4], 'initialized', 'true')
return 1
"""

    # Compare-and-set role update.  Validation happens in Python, while this
    # script closes the interval between that read and the commit: a delete or
    # another update cannot be silently overwritten, and a renamed role claims
    # its case-insensitive name in the same transaction as the document.
    # KEYS: document, role index, old type index, new type index, name map, meta
    # ARGV: id, expected role/name/tables, new role/name, field-map JSON, now
    _LUA_RBAC_UPDATE_ROLE = _LUA_RBAC_AUDIT_PREAMBLE + """
local role_id = ARGV[1]
local expected_role = ARGV[2]
local expected_name = ARGV[3]
local expected_tables = ARGV[4]
local expected_modified = ARGV[5]
local expected_doc_version = ARGV[6]
local new_role = ARGV[7]
local new_name = ARGV[8]
local role_update_document_json = ARGV[9]
local now_ms = ARGV[10]
require_audit_identity('role_update', 'role', role_id)

require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'set', true)
require_key_type(KEYS[4], 'set', true)
require_key_type(KEYS[5], 'hash', true)
require_key_type(KEYS[6], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local current_role = redis.call('HGET', KEYS[1], 'role') or ''
local current_name = redis.call('HGET', KEYS[1], 'role_name') or ''
local current_tables = redis.call('HGET', KEYS[1], 'tables') or ''
local current_modified = redis.call('HGET', KEYS[1], 'modified_ms') or ''
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if current_role ~= expected_role
    or current_name ~= expected_name
    or current_tables ~= expected_tables
    or current_modified ~= expected_modified
    or current_doc_version ~= expected_doc_version then
    return -3
end
local stored_id = redis.call('HGET', KEYS[1], 'role_id')
if stored_id and stored_id ~= role_id then return -6 end

local bootstrap_id = redis.call('HGET', KEYS[5], 'superadmin')
local is_bootstrap = string.lower(current_name) == 'superadmin'
    or bootstrap_id == role_id
if (current_role == 'superadmin' or is_bootstrap)
    and new_role ~= 'superadmin' then
    return -4
end
if is_bootstrap and string.lower(new_name) ~= 'superadmin' then return -5 end

local new_name_lower = string.lower(new_name)
if new_name_lower ~= '' then
    local mapped = redis.call('HGET', KEYS[5], new_name_lower)
    if mapped and mapped ~= role_id then return -2 end
end

local ok, document = pcall(cjson.decode, role_update_document_json)
if not ok or type(document) ~= 'table' then
    return redis.error_reply('invalid canonical role update document')
end
require_namespace_head_for_state(KEYS[6], true)
local next_role_namespace_version = incrementable_namespace_counter(
    KEYS[6], 'version'
)
append_privileged_audit({namespace_version=next_role_namespace_version})
for field, value in pairs(document) do
    redis.call('HSET', KEYS[1], field, value)
end
redis.call('HINCRBY', KEYS[1], 'doc_version', 1)
redis.call('SADD', KEYS[2], role_id)
if current_role ~= new_role then redis.call('SREM', KEYS[3], role_id) end
redis.call('SADD', KEYS[4], role_id)

local current_name_lower = string.lower(current_name)
if current_name_lower ~= new_name_lower and current_name_lower ~= '' then
    local mapped_old = redis.call('HGET', KEYS[5], current_name_lower)
    if mapped_old == role_id then redis.call('HDEL', KEYS[5], current_name_lower) end
end
if new_name_lower ~= '' then
    redis.call('HSET', KEYS[5], new_name_lower, role_id)
end
redis.call('HINCRBY', KEYS[6], 'version', 1)
redis.call('HSET', KEYS[6], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[6], 'initialized', 'true')
return 1
"""

    # CAS user update used by both partial updates and rename.  The final
    # superuser checks intentionally live here, beside the mutation.
    # KEYS: document, user index, name map, user meta
    # ARGV: id, expected username/roles, new username, resulting roles JSON,
    #       field-map JSON, now, role-document prefix
    _LUA_RBAC_UPDATE_USER = _LUA_RBAC_AUDIT_PREAMBLE + """
local user_id = ARGV[1]
local expected_username = ARGV[2]
local expected_roles = ARGV[3]
local expected_modified = ARGV[4]
local expected_doc_version = ARGV[5]
local new_username = ARGV[6]
local resulting_roles_json = ARGV[7]
local user_update_document_json = ARGV[8]
local now_ms = ARGV[9]
local role_doc_key_prefix = ARGV[10]
require_audit_identity('user_update', 'user', user_id)

require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'hash', true)
require_key_type(KEYS[4], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local current_username = redis.call('HGET', KEYS[1], 'username') or ''
local current_roles = redis.call('HGET', KEYS[1], 'roles') or ''
local current_modified = redis.call('HGET', KEYS[1], 'modified_ms') or ''
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if current_username ~= expected_username
    or current_roles ~= expected_roles
    or current_modified ~= expected_modified
    or current_doc_version ~= expected_doc_version then
    return -3
end
local stored_id = redis.call('HGET', KEYS[1], 'user_id')
if stored_id and stored_id ~= user_id then return -7 end

local new_username_lower = string.lower(new_username)
local mapped_new = redis.call('HGET', KEYS[3], new_username_lower)
if mapped_new and mapped_new ~= user_id then return -2 end

local protected_id = redis.call('HGET', KEYS[3], 'superuser')
local is_protected = string.lower(current_username) == 'superuser'
    or protected_id == user_id
if is_protected and new_username_lower ~= 'superuser' then return -4 end

local roles_ok, resulting_roles = pcall(cjson.decode, resulting_roles_json)
if not roles_ok or type(resulting_roles) ~= 'table' then return -6 end
local has_superadmin = false
for _, assigned_role_id in ipairs(resulting_roles) do
    local assigned_type = redis.call(
        'HGET', role_doc_key_prefix .. assigned_role_id, 'role'
    )
    if assigned_type ~= 'superadmin'
        and assigned_type ~= 'admin'
        and assigned_type ~= 'writer'
        and assigned_type ~= 'reader'
        and assigned_type ~= 'meta' then
        return -8
    end
    if assigned_type == 'superadmin' then has_superadmin = true end
end
if (is_protected or new_username_lower == 'superuser')
    and not has_superadmin then
    return -5
end

local ok, document = pcall(cjson.decode, user_update_document_json)
if not ok or type(document) ~= 'table' then
    return redis.error_reply('invalid canonical user update document')
end
require_namespace_head_for_state(KEYS[4], true)
local next_user_namespace_version = incrementable_namespace_counter(
    KEYS[4], 'version'
)
append_privileged_audit({namespace_version=next_user_namespace_version})
for field, value in pairs(document) do
    redis.call('HSET', KEYS[1], field, value)
end
redis.call('HINCRBY', KEYS[1], 'doc_version', 1)
redis.call('SADD', KEYS[2], user_id)
local current_username_lower = string.lower(current_username)
if current_username_lower ~= new_username_lower and current_username_lower ~= '' then
    local mapped_old = redis.call('HGET', KEYS[3], current_username_lower)
    if mapped_old == user_id then
        redis.call('HDEL', KEYS[3], current_username_lower)
    end
end
redis.call('HSET', KEYS[3], new_username_lower, user_id)
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[4], 'initialized', 'true')
return 1
"""

    # CAS user deletion keeps document, index, name map, and revision in one
    # commit and repeats the reserved-superuser decision inside that commit.
    # KEYS: document, user index, name map, user meta
    # ARGV: id, expected username, expected roles, now
    _LUA_RBAC_DELETE_USER = _LUA_RBAC_AUDIT_PREAMBLE + """
local user_id = ARGV[1]
local expected_username = ARGV[2]
local expected_roles = ARGV[3]
local expected_modified = ARGV[4]
local expected_doc_version = ARGV[5]
local now_ms = ARGV[6]
require_audit_identity('user_delete', 'user', user_id)

require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'hash', true)
require_key_type(KEYS[4], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local current_username = redis.call('HGET', KEYS[1], 'username') or ''
local current_roles = redis.call('HGET', KEYS[1], 'roles') or ''
local current_modified = redis.call('HGET', KEYS[1], 'modified_ms') or ''
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if current_username ~= expected_username
    or current_roles ~= expected_roles
    or current_modified ~= expected_modified
    or current_doc_version ~= expected_doc_version then
    return -2
end
local stored_id = redis.call('HGET', KEYS[1], 'user_id')
if stored_id and stored_id ~= user_id then return -3 end
local protected_id = redis.call('HGET', KEYS[3], 'superuser')
if string.lower(current_username) == 'superuser' or protected_id == user_id then
    return -1
end

require_namespace_head_for_state(KEYS[4], true)
local next_user_namespace_version = incrementable_namespace_counter(
    KEYS[4], 'version'
)
append_privileged_audit({namespace_version=next_user_namespace_version})

local username_lower = string.lower(current_username)
local mapped = redis.call('HGET', KEYS[3], username_lower)
if mapped == user_id then redis.call('HDEL', KEYS[3], username_lower) end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], user_id)
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[4], 'initialized', 'true')
return 1
"""

    # ARGV layout:
    #   ARGV[1] role_id
    #   ARGV[2] now_ms (string)
    #   ARGV[3:5] expected role, role_name, and raw tables JSON
    #   ARGV[6] user_doc_key_prefix
    # KEYS layout:
    #   KEYS[1] role_doc_key
    #   KEYS[2] role_index_key
    #   KEYS[3] role_type_index_key
    #   KEYS[4] role_meta_key
    #   KEYS[5] user_index_key
    #   KEYS[6] rolename_to_id_key
    #   KEYS[7] user_meta_key
    #   KEYS[8] immutable cascade evidence HASH (unique event ID)
    #   KEYS[9] username_to_id_key
    _LUA_RBAC_DELETE_ROLE = _LUA_RBAC_AUDIT_PREAMBLE + """
local role_id              = ARGV[1]
local now_ms               = ARGV[2]
local expected_role        = ARGV[3]
local expected_name        = ARGV[4]
local expected_tables      = ARGV[5]
local expected_modified    = ARGV[6]
local expected_doc_version = ARGV[7]
local user_doc_key_prefix  = ARGV[8]
local cascade_manifest_key = ARGV[9]
local requested_cascade_limit = ARGV[10]
if not string.match(requested_cascade_limit, '^[1-9]%d*$')
    or string.len(requested_cascade_limit) > 5
    or tonumber(requested_cascade_limit) > 10000 then
    return redis.error_reply('invalid role deletion cascade manifest limit')
end
local max_cascade_manifest_users = tonumber(requested_cascade_limit)
require_audit_identity('role_delete', 'role', role_id)

require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'set', true)
require_key_type(KEYS[3], 'set', true)
require_key_type(KEYS[4], 'hash', true)
require_key_type(KEYS[5], 'set', true)
require_key_type(KEYS[6], 'hash', true)
require_key_type(KEYS[7], 'hash', true)
require_key_type(KEYS[9], 'hash', true)

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local current_role = redis.call('HGET', KEYS[1], 'role') or ''
local current_name = redis.call('HGET', KEYS[1], 'role_name') or ''
local current_tables = redis.call('HGET', KEYS[1], 'tables') or ''
local current_modified = redis.call('HGET', KEYS[1], 'modified_ms') or ''
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if current_role ~= expected_role
    or current_name ~= expected_name
    or current_tables ~= expected_tables
    or current_modified ~= expected_modified
    or current_doc_version ~= expected_doc_version then
    return -2
end
local stored_id = redis.call('HGET', KEYS[1], 'role_id')
if stored_id and stored_id ~= role_id then return -3 end
local bootstrap_id = redis.call('HGET', KEYS[6], 'superadmin')
if current_role == 'superadmin'
    or string.lower(current_name) == 'superadmin'
    or bootstrap_id == role_id then
    return -1
end
if cascade_manifest_key ~= KEYS[8]
    or privileged_audit_event['cascade_manifest_id']
        ~= privileged_audit_event['event_id'] then
    return redis.error_reply('role deletion cascade manifest identity mismatch')
end
require_key_type(KEYS[8], 'hash', true)
if redis.call('EXISTS', KEYS[8]) ~= 0 then
    return redis.error_reply('role deletion cascade manifest already exists')
end

-- Bound the authoritative index before materialising or scanning it.  A cap
-- on affected users alone would still let an unrelated million-user tenant
-- monopolise Redis while proving that a role has no assignments.
local indexed_user_count = redis.call('SCARD', KEYS[5])
local named_user_count = redis.call('HLEN', KEYS[9])
if indexed_user_count > max_cascade_manifest_users
    or named_user_count > max_cascade_manifest_users then
    return -5
end
if indexed_user_count ~= named_user_count then return -6 end
local user_ids = redis.call('SMEMBERS', KEYS[5])
local encoded_roles_by_uid = {}
local cascade_rows = {}
local affected_user_count = 0
local removed_assignment_count = 0
for _, uid in ipairs(user_ids) do
    -- User IDs are Redis-key segments at every supported write boundary.
    -- Re-check the invariant before copying an identity to durable evidence,
    -- so a corrupt index cannot produce an unarchivable manifest.
    local plain_uid = string.match(uid, '^[a-z0-9][a-z0-9_-]*$')
    local internal_uid = string.match(uid, '^__[a-z0-9][a-z0-9_-]*__$')
    if string.len(uid) > 64 or (not plain_uid and not internal_uid) then
        return -4
    end
    local ukey = user_doc_key_prefix .. uid
    if redis.call('EXISTS', ukey) ~= 1 then return -6 end
    require_key_type(ukey, 'hash', false)
    local stored_uid = redis.call('HGET', ukey, 'user_id')
    local stored_username = redis.call('HGET', ukey, 'username')
    if stored_uid ~= uid
        or not stored_username or stored_username == ''
        or redis.call('HGET', KEYS[9], string.lower(stored_username)) ~= uid then
        return -6
    end
    local roles_json = redis.call('HGET', ukey, 'roles')
    if not roles_json then return -4 end
    if not string.match(roles_json, '^%s*%[')
        or not string.match(roles_json, '%]%s*$') then
        return -4
    end
    local ok, roles = pcall(cjson.decode, roles_json)
    if not ok or type(roles) ~= 'table' then return -4 end
    local role_count = 0
    for index, assigned_role_id in pairs(roles) do
        if type(index) ~= 'number'
            or index < 1
            or index ~= math.floor(index)
            or type(assigned_role_id) ~= 'string' then
            return -4
        end
        role_count = role_count + 1
    end
    if role_count ~= #roles then return -4 end
    local new_roles = {}
    local removed_occurrences = 0
    for _, assigned_role_id in ipairs(roles) do
        if assigned_role_id == role_id then
            removed_occurrences = removed_occurrences + 1
        else
            new_roles[#new_roles + 1] = assigned_role_id
        end
    end
    if removed_occurrences > 0 then
        local before_doc_version = redis.call(
            'HGET', ukey, 'doc_version'
        ) or '0'
        local after_doc_version = incrementable_hash_counter(
            ukey, 'doc_version'
        )
        affected_user_count = affected_user_count + 1
        if affected_user_count > max_cascade_manifest_users then
            return -5
        end
        removed_assignment_count = removed_assignment_count
            + removed_occurrences
        encoded_roles_by_uid[uid] = (#new_roles == 0) and '[]'
            or cjson.encode(new_roles)
        cascade_rows[#cascade_rows + 1] = {
            field='user:' .. uid,
            value=before_doc_version .. '|' .. after_doc_version .. '|'
                .. tostring(removed_occurrences) .. '|'
                .. tostring(role_count) .. '|' .. tostring(#new_roles)
        }
    end
end
for _, mapped_uid in ipairs(redis.call('HVALS', KEYS[9])) do
    if redis.call('SISMEMBER', KEYS[5], mapped_uid) ~= 1 then return -6 end
end
require_namespace_head_for_state(KEYS[4], true)
require_namespace_head_for_state(KEYS[7], indexed_user_count > 0)
local next_role_namespace_version = incrementable_namespace_counter(
    KEYS[4], 'version'
)
local user_namespace_version_before = redis.call(
    'HGET', KEYS[7], 'version'
) or '0'
if user_namespace_version_before ~= '0'
    and not string.match(user_namespace_version_before, '^[1-9]%d*$') then
    return redis.error_reply('RBAC user namespace revision counter is corrupt')
end
if string.len(user_namespace_version_before) > 19
    or (
        string.len(user_namespace_version_before) == 19
        and user_namespace_version_before > '9223372036854775807'
    ) then
    return redis.error_reply('RBAC user namespace revision counter is out of range')
end
local user_namespace_version_after = user_namespace_version_before
if affected_user_count > 0 then
    user_namespace_version_after = incrementable_namespace_counter(
        KEYS[7], 'version'
    )
end

-- Create exact, bounded-per-row evidence before the success event.  A Redis
-- command error can leave only an unreferenced manifest, never a committed
-- role deletion whose parent event lacks its affected identities.
local manifest_header = {
    'schema_version', '1',
    'event_id', privileged_audit_event['event_id'],
    'mutation_id', privileged_audit_event['mutation_id'],
    'organization', privileged_audit_org,
    'super_name', privileged_audit_super,
    'role_id', role_id,
    'user_count', tostring(affected_user_count),
    'removed_assignment_count', tostring(removed_assignment_count),
    'user_namespace_version_before', user_namespace_version_before,
    'user_namespace_version_after', user_namespace_version_after,
    'created_ms', now_ms
}
local header_result = redis.pcall('HSET', KEYS[8], unpack(manifest_header))
if type(header_result) == 'table' and header_result['err'] then
    redis.call('DEL', KEYS[8])
    return redis.error_reply('could not create role deletion cascade manifest')
end
for _, row in ipairs(cascade_rows) do
    local row_result = redis.pcall(
        'HSET', KEYS[8], row['field'], row['value']
    )
    if type(row_result) == 'table' and row_result['err'] then
        redis.call('DEL', KEYS[8])
        return redis.error_reply('could not populate role deletion cascade manifest')
    end
end
if redis.call('HLEN', KEYS[8]) ~= 11 + affected_user_count then
    redis.call('DEL', KEYS[8])
    return redis.error_reply('role deletion cascade manifest is incomplete')
end
local append_ok, append_error = pcall(append_privileged_audit, {
    namespace_version=next_role_namespace_version,
    affected_count=affected_user_count,
    cascade_assignment_count=removed_assignment_count,
    user_namespace_version_before=user_namespace_version_before,
    user_namespace_version_after=user_namespace_version_after
})
if not append_ok then
    -- If XADD itself failed, remove the now-unreferenced sidecar.  If XADD
    -- succeeded but the subsequent meta update failed, retain its manifest:
    -- the stream is intentionally left fail-closed for operator recovery.
    local referenced = false
    local latest = redis.call(
        'XREVRANGE', privileged_audit_outbox, '+', '-', 'COUNT', 1
    )
    if #latest == 1 then
        local latest_fields = latest[1][2]
        for index = 1, #latest_fields, 2 do
            if latest_fields[index] == 'event_id'
                and latest_fields[index + 1]
                    == privileged_audit_event['event_id'] then
                referenced = true
            end
        end
    end
    if not referenced then redis.call('DEL', KEYS[8]) end
    return redis.error_reply(
        'could not append role deletion privileged audit record: '
        .. tostring(append_error)
    )
end
for _, uid in ipairs(user_ids) do
    local ukey = user_doc_key_prefix .. uid
    local encoded_roles = encoded_roles_by_uid[uid]
    if encoded_roles then
        redis.call('HSET', ukey, 'roles', encoded_roles)
        redis.call('HSET', ukey, 'modified_ms', now_ms)
        redis.call('HINCRBY', ukey, 'doc_version', 1)
    end
end

-- Clean up role_name → role_id mapping atomically
local role_name_lower = string.lower(current_name)
if role_name_lower ~= '' then
    local mapped_role_id = redis.call('HGET', KEYS[6], role_name_lower)
    if mapped_role_id == role_id then
        redis.call('HDEL', KEYS[6], role_name_lower)
    end
end

redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], role_id)
redis.call('SREM', KEYS[3], role_id)
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[4], 'initialized', 'true')
if affected_user_count > 0 then
    redis.call('HINCRBY', KEYS[7], 'version', 1)
    redis.call('HSET', KEYS[7], 'last_updated_ms', now_ms)
    redis.call('HSET', KEYS[7], 'initialized', 'true')
end
return 1
"""

    _LUA_RBAC_REMOVE_ROLE_FROM_USER = _LUA_RBAC_AUDIT_PREAMBLE + """
local role_id = ARGV[1]
local now_ms  = ARGV[2]
local protected_username = ARGV[3]
local role_doc_key_prefix = ARGV[4]
local user_id = ARGV[5]
local expected_roles = ARGV[6]
local expected_doc_version = ARGV[7]
require_audit_identity(
    'user_role_remove', 'user_role_assignment', user_id .. ':' .. role_id
)
require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'hash', true)
require_key_type(KEYS[3], 'hash', true)
local roles_json = redis.call('HGET', KEYS[1], 'roles')
if not roles_json then return 0 end
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if roles_json ~= expected_roles
    or current_doc_version ~= expected_doc_version then
    return -2
end

local ok, roles = pcall(cjson.decode, roles_json)
if not ok or type(roles) ~= 'table' then return 0 end

local username = redis.call('HGET', KEYS[1], 'username') or ''
local protected_user_id = redis.call('HGET', KEYS[3], protected_username)
if string.lower(username) == protected_username or protected_user_id == user_id then
    local target_type = redis.call('HGET', role_doc_key_prefix .. role_id, 'role')
    if target_type == 'superadmin' then
        local other_superadmins = 0
        for _, assigned_role_id in ipairs(roles) do
            if assigned_role_id ~= role_id then
                local assigned_type = redis.call(
                    'HGET', role_doc_key_prefix .. assigned_role_id, 'role'
                )
                if assigned_type == 'superadmin' then
                    other_superadmins = other_superadmins + 1
                end
            end
        end
        if other_superadmins == 0 then return -1 end
    end
end

local new_roles = {}
local changed = false
for _, r in ipairs(roles) do
    if r == role_id then
        changed = true
    else
        new_roles[#new_roles + 1] = r
    end
end
if not changed then return 0 end

incrementable_hash_counter(KEYS[1], 'doc_version')
require_namespace_head_for_state(KEYS[2], true)
local next_user_namespace_version = incrementable_namespace_counter(
    KEYS[2], 'version'
)
append_privileged_audit({namespace_version=next_user_namespace_version})
local encoded_roles = (#new_roles == 0) and '[]' or cjson.encode(new_roles)
redis.call('HSET', KEYS[1], 'roles', encoded_roles)
redis.call('HSET', KEYS[1], 'modified_ms', now_ms)
redis.call('HINCRBY', KEYS[1], 'doc_version', 1)
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('HSET', KEYS[2], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[2], 'initialized', 'true')
return 1
"""

    _LUA_RBAC_ADD_ROLE_TO_USER = _LUA_RBAC_AUDIT_PREAMBLE + """
local role_id = ARGV[1]
local now_ms  = ARGV[2]
local user_id = ARGV[3]
local expected_roles = ARGV[4]
local expected_doc_version = ARGV[5]
require_audit_identity(
    'user_role_assign', 'user_role_assignment', user_id .. ':' .. role_id
)
require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'hash', true)
require_key_type(KEYS[3], 'hash', true)
local roles_json = redis.call('HGET', KEYS[1], 'roles')
if not roles_json then return 0 end
local current_doc_version = redis.call('HGET', KEYS[1], 'doc_version') or '0'
if roles_json ~= expected_roles
    or current_doc_version ~= expected_doc_version then
    return -2
end
local assigned_type = redis.call('HGET', KEYS[3], 'role')
if assigned_type ~= 'superadmin'
    and assigned_type ~= 'admin'
    and assigned_type ~= 'writer'
    and assigned_type ~= 'reader'
    and assigned_type ~= 'meta' then
    return -1
end

local ok, roles = pcall(cjson.decode, roles_json)
if not ok or type(roles) ~= 'table' then return 0 end

for _, r in ipairs(roles) do
    if r == role_id then return 0 end
end

roles[#roles + 1] = role_id
incrementable_hash_counter(KEYS[1], 'doc_version')
require_namespace_head_for_state(KEYS[2], true)
local next_user_namespace_version = incrementable_namespace_counter(
    KEYS[2], 'version'
)
append_privileged_audit({namespace_version=next_user_namespace_version})
redis.call('HSET', KEYS[1], 'roles', cjson.encode(roles))
redis.call('HSET', KEYS[1], 'modified_ms', now_ms)
redis.call('HINCRBY', KEYS[1], 'doc_version', 1)
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('HSET', KEYS[2], 'last_updated_ms', now_ms)
redis.call('HSET', KEYS[2], 'initialized', 'true')
return 1
"""

    # Create/delete organization login-token records at the same atomic
    # boundary as their privileged audit evidence.  Token plaintext never
    # enters Redis or the ledger; resource identity is the SHA-256 token ID.
    # KEYS: token HASH, token namespace meta HASH, audit outbox, audit meta
    # ARGV: token_id, metadata JSON, now_ms, organization, scope, audit JSON
    _LUA_AUTH_CREATE_TOKEN = _LUA_RBAC_AUDIT_PREAMBLE + """
local token_id = ARGV[1]
local metadata_json = ARGV[2]
local now_ms = ARGV[3]
require_audit_identity('token_create', 'auth_token', token_id)
require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'hash', true)
if not string.match(token_id, '^[0-9a-f]+$') or string.len(token_id) ~= 64 then
    return redis.error_reply('invalid auth token identity')
end
if string.len(metadata_json) == 0 or string.len(metadata_json) > 16384 then
    return redis.error_reply('invalid auth token metadata')
end
local metadata_ok, metadata = pcall(cjson.decode, metadata_json)
if not metadata_ok or type(metadata) ~= 'table'
    or metadata['token_id'] ~= token_id then
    return redis.error_reply('invalid auth token metadata')
end
if redis.call('HEXISTS', KEYS[1], token_id) == 1 then return -1 end
local token_audit_initialized = redis.call(
    'HGET', KEYS[1], '_audit_initialized'
)
if token_audit_initialized and token_audit_initialized ~= 'true' then
    return redis.error_reply('auth token audit marker is corrupt')
end
if redis.call('HGET', KEYS[2], 'version') == false
    and token_audit_initialized == 'true' then
    return redis.error_reply('RBAC namespace revision head is missing')
end
local next_token_namespace_version = incrementable_namespace_counter(
    KEYS[2], 'version'
)
append_privileged_audit({namespace_version=next_token_namespace_version})
redis.call('HSET', KEYS[1], token_id, metadata_json)
redis.call('HSET', KEYS[1], '_audit_initialized', 'true')
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call(
    'HSET', KEYS[2],
    'last_updated_ms', now_ms,
    'initialized', 'true'
)
return 1
"""

    # ARGV: token_id, expected metadata JSON, now_ms, organization, scope,
    #       audit JSON
    _LUA_AUTH_DELETE_TOKEN = _LUA_RBAC_AUDIT_PREAMBLE + """
local token_id = ARGV[1]
local expected_metadata = ARGV[2]
local now_ms = ARGV[3]
require_audit_identity('token_delete', 'auth_token', token_id)
require_key_type(KEYS[1], 'hash', true)
require_key_type(KEYS[2], 'hash', true)
if not string.match(token_id, '^[0-9a-f]+$') or string.len(token_id) ~= 64 then
    return redis.error_reply('invalid auth token identity')
end
local current_metadata = redis.call('HGET', KEYS[1], token_id)
if current_metadata == false then return 0 end
if current_metadata ~= expected_metadata then return -1 end
local token_audit_initialized = redis.call(
    'HGET', KEYS[1], '_audit_initialized'
)
if token_audit_initialized and token_audit_initialized ~= 'true' then
    return redis.error_reply('auth token audit marker is corrupt')
end
if redis.call('HGET', KEYS[2], 'version') == false
    and token_audit_initialized == 'true' then
    return redis.error_reply('RBAC namespace revision head is missing')
end
local next_token_namespace_version = incrementable_namespace_counter(
    KEYS[2], 'version'
)
append_privileged_audit({namespace_version=next_token_namespace_version})
redis.call('HDEL', KEYS[1], token_id)
redis.call('HSET', KEYS[1], '_audit_initialized', 'true')
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call(
    'HSET', KEYS[2],
    'last_updated_ms', now_ms,
    'initialized', 'true'
)
return 1
"""

    def __init__(
        self,
        options: Optional[RedisOptions] = None,
        *,
        redis_client: Optional[redis.Redis] = None,
    ):
        if options is not None and redis_client is not None:
            raise ValueError("options and redis_client are mutually exclusive")
        self.r = redis_client if redis_client is not None else RedisConnector(options).r

        # Register scripts
        self._leaf_cas_set = self.r.register_script(self._LUA_LEAF_CAS_SET)
        self._leaf_payload_cas_set = self.r.register_script(self._LUA_LEAF_PAYLOAD_CAS_SET)
        self._allocate_linked_publication = self.r.register_script(
            self._LUA_ALLOCATE_LINKED_PUBLICATION
        )
        self._allocate_share_manifest_generation = self.r.register_script(
            self._LUA_ALLOCATE_SHARE_MANIFEST_GENERATION
        )
        self._reserve_linked_provider_publication = self.r.register_script(
            self._LUA_RESERVE_LINKED_PROVIDER_PUBLICATION
        )
        self._abort_linked_provider_publication = self.r.register_script(
            self._LUA_ABORT_LINKED_PROVIDER_PUBLICATION
        )
        self._upsert_linked_leaf = self.r.register_script(
            self._LUA_UPSERT_LINKED_LEAF
        )
        self._delete_linked_leaf = self.r.register_script(
            self._LUA_DELETE_LINKED_LEAF
        )
        self._delete_stale_linked_leaf = self.r.register_script(
            self._LUA_DELETE_STALE_LINKED_LEAF
        )
        self._delete_unlinked_leaf = self.r.register_script(
            self._LUA_DELETE_UNLINKED_LEAF
        )
        self._get_replica_leaf = self.r.register_script(
            self._LUA_GET_REPLICA_LEAF
        )
        self._delete_simple_table = self.r.register_script(
            self._LUA_DELETE_SIMPLE_TABLE
        )
        self._delete_simple_quality_keys = self.r.register_script(
            self._LUA_DELETE_SIMPLE_QUALITY_KEYS
        )
        self._root_ensure = self.r.register_script(self._LUA_ROOT_ENSURE)
        self._root_bump = self.r.register_script(self._LUA_ROOT_BUMP)
        self._update_root_flags = self.r.register_script(
            self._LUA_UPDATE_ROOT_FLAGS
        )
        self._set_mirrors_fenced = self.r.register_script(
            self._LUA_SET_MIRRORS
        )
        self._mutate_mirror_fenced = self.r.register_script(
            self._LUA_MUTATE_MIRROR
        )
        self._upsert_linked_share_fenced = self.r.register_script(
            self._LUA_UPSERT_LINKED_SHARE
        )
        self._mutate_share = self.r.register_script(self._LUA_MUTATE_SHARE)
        self._delete_linked_share_fenced = self.r.register_script(
            self._LUA_DELETE_LINKED_SHARE
        )
        self._finish_unlink_linked_share = self.r.register_script(
            self._LUA_FINISH_UNLINK_LINKED_SHARE
        )
        self._get_authoritative_linked_share = self.r.register_script(
            self._LUA_GET_AUTHORITATIVE_LINKED_SHARE
        )
        self._begin_simple_deletion = self.r.register_script(
            self._LUA_BEGIN_SIMPLE_DELETION
        )
        self._recover_simple_deletion = self.r.register_script(
            self._LUA_RECOVER_SIMPLE_DELETION
        )
        self._clear_simple_deletion = self.r.register_script(
            self._LUA_CLEAR_SIMPLE_DELETION
        )
        self._begin_namespace_deletion = self.r.register_script(
            self._LUA_BEGIN_NAMESPACE_DELETION
        )
        self._recover_namespace_deletion = self.r.register_script(
            self._LUA_RECOVER_NAMESPACE_DELETION
        )
        self._clear_namespace_deletion = self.r.register_script(
            self._LUA_CLEAR_NAMESPACE_DELETION
        )
        self._assert_table_mutation_allowed = self.r.register_script(
            self._LUA_ASSERT_TABLE_MUTATION_ALLOWED
        )
        self._begin_table_mutation = self.r.register_script(
            self._LUA_BEGIN_TABLE_MUTATION
        )
        self._begin_initial_table_mutation = self.r.register_script(
            self._LUA_BEGIN_INITIAL_TABLE_MUTATION
        )
        self._assert_initialization_allowed = self.r.register_script(
            self._LUA_ASSERT_INITIALIZATION_ALLOWED
        )
        self._set_table_config_fenced = self.r.register_script(
            self._LUA_SET_TABLE_CONFIG
        )
        self._delete_namespace_batch = self.r.register_script(
            self._LUA_DELETE_NAMESPACE_BATCH
        )
        self._finalize_namespace_deletion = self.r.register_script(
            self._LUA_FINALIZE_NAMESPACE_DELETION
        )
        self._snapshot_commit = self.r.register_script(self._LUA_SNAPSHOT_COMMIT)
        self._snapshot_commit_no_mirrors = self.r.register_script(
            self._LUA_SNAPSHOT_COMMIT_NO_MIRRORS
        )
        self._mirror_publication_prepare = self.r.register_script(
            self._LUA_MIRROR_PUBLICATION_PREPARE
        )
        self._mirror_publication_claim = self.r.register_script(
            self._LUA_MIRROR_PUBLICATION_CLAIM
        )
        self._mirror_publication_transition = self.r.register_script(
            self._LUA_MIRROR_PUBLICATION_TRANSITION
        )
        self._reserve_rowids_at_least = self.r.register_script(
            self._LUA_RESERVE_ROWIDS_AT_LEAST
        )
        self._staging_delete_meta = self.r.register_script(
            self._LUA_STAGING_DELETE_META
        )
        self._staging_delete_children = self.r.register_script(
            self._LUA_STAGING_DELETE_CHILDREN
        )
        self._begin_stage_deletion = self.r.register_script(
            self._LUA_BEGIN_STAGE_DELETION
        )
        self._recover_stage_deletion = self.r.register_script(
            self._LUA_RECOVER_STAGE_DELETION
        )
        self._clear_stage_deletion = self.r.register_script(
            self._LUA_CLEAR_STAGE_DELETION
        )
        self._assert_stage_mutation_allowed = self.r.register_script(
            self._LUA_ASSERT_STAGE_MUTATION_ALLOWED
        )
        self._upsert_staging_meta = self.r.register_script(
            self._LUA_UPSERT_STAGING_META
        )
        self._upsert_pipe_meta = self.r.register_script(
            self._LUA_UPSERT_PIPE_META
        )
        self._upsert_staging_file_meta = self.r.register_script(
            self._LUA_UPSERT_STAGING_FILE_META
        )
        self._delete_pipe_meta = self.r.register_script(
            self._LUA_DELETE_PIPE_META
        )

        # Distributed locking (delegates to supertable.locking.redis_lock)
        self._locker = RedisLocking(self.r)

        # RBAC Lua scripts
        self._rbac_validate_meta = self.r.register_script(
            self._LUA_RBAC_VALIDATE_META
        )
        self._rbac_append_attempt = self.r.register_script(
            self._LUA_RBAC_APPEND_ATTEMPT
        )
        self._rbac_create_role = self.r.register_script(self._LUA_RBAC_CREATE_ROLE)
        self._rbac_create_user = self.r.register_script(self._LUA_RBAC_CREATE_USER)
        self._rbac_update_role = self.r.register_script(self._LUA_RBAC_UPDATE_ROLE)
        self._rbac_update_user = self.r.register_script(self._LUA_RBAC_UPDATE_USER)
        self._rbac_delete_user = self.r.register_script(self._LUA_RBAC_DELETE_USER)
        self._rbac_delete_role = self.r.register_script(self._LUA_RBAC_DELETE_ROLE)
        self._rbac_remove_role_from_user = self.r.register_script(self._LUA_RBAC_REMOVE_ROLE_FROM_USER)
        self._rbac_add_role_to_user = self.r.register_script(self._LUA_RBAC_ADD_ROLE_TO_USER)
        self._auth_create_token = self.r.register_script(
            self._LUA_AUTH_CREATE_TOKEN
        )
        self._auth_delete_token = self.r.register_script(
            self._LUA_AUTH_DELETE_TOKEN
        )

    # ------------- Health check -------------

    def ping(self) -> bool:
        """Test Redis connectivity. Returns True if the server responds to PING."""
        try:
            return bool(self.r.ping())
        except redis.RedisError as e:
            logger.debug(f"[redis-catalog] ping failed: {e}")
            return False

    # ------------- Locking -------------

    def acquire_simple_lock(self, org: str, sup: str, simple: str, ttl_s: int = 30, timeout_s: int = 30) -> Optional[
        str]:
        """SET lock key NX EX with retry/backoff <= timeout. Returns token if acquired else None."""
        return self._locker.acquire(RK.lock_leaf(org, sup, simple), ttl_s=ttl_s, timeout_s=timeout_s)

    def release_simple_lock(self, org: str, sup: str, simple: str, token: str) -> bool:
        """Compare-and-delete via Lua."""
        return self._locker.release(RK.lock_leaf(org, sup, simple), token)

    def acquire_namespace_lock(
            self, org: str, sup: str, ttl_s: int = 30,
            timeout_s: int = 30,
    ) -> Optional[str]:
        """Fence creation/publication while deleting a SuperTable namespace."""
        return self._locker.acquire(
            RK.lock_namespace(org, sup), ttl_s=ttl_s, timeout_s=timeout_s,
        )

    def release_namespace_lock(
            self, org: str, sup: str, token: str,
    ) -> bool:
        return self._locker.release(RK.lock_namespace(org, sup), token)

    @staticmethod
    def _decode_deletion_intent(
            raw: Any, *, scope: str,
    ) -> Optional[Dict[str, Any]]:
        if not raw:
            return None
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"Corrupt deletion intent for {scope}") from exc
        if not isinstance(value, dict) or not value.get("intent_id"):
            raise RuntimeError(f"Corrupt deletion intent for {scope}")
        return value

    def get_simple_deletion_intent(
            self, org: str, sup: str, simple: str,
    ) -> Optional[Dict[str, Any]]:
        raw = self.r.get(RK.meta_simple_deletion_intent(org, sup, simple))
        return self._decode_deletion_intent(
            raw, scope=f"{org}/{sup}/{simple}",
        )

    def get_namespace_deletion_intent(
            self, org: str, sup: str,
    ) -> Optional[Dict[str, Any]]:
        raw = self.r.get(RK.meta_namespace_deletion_intent(org, sup))
        return self._decode_deletion_intent(raw, scope=f"{org}/{sup}")

    def check_deletion_intent_absent(
            self,
            org: str,
            sup: str,
            *,
            simple: Optional[str] = None,
            stage: Optional[str] = None,
    ) -> None:
        """Reject an already-opened object while a durable tombstone exists.

        This is an early, read-only guard for constructor fast paths. Mutation
        publication still repeats the check atomically with its lock token;
        this guard prevents stale metadata recreated behind a terminal
        tombstone from making a deleted table appear openable.
        """
        if simple is not None and stage is not None:
            raise ValueError("Only one child deletion scope may be checked")
        keys = [RK.meta_namespace_deletion_intent(org, sup)]
        if simple is not None:
            keys.append(RK.meta_simple_deletion_intent(org, sup, simple))
        if stage is not None:
            keys.append(RK.meta_stage_deletion_intent(org, sup, stage))
        values = self.r.mget(keys)
        if any(value is not None for value in values):
            child = simple if simple is not None else stage
            scope = f"{org}/{sup}" + (f"/{child}" if child else "")
            raise DeletionIntentConflictError(
                f"Durable deletion intent blocks opening {scope}"
            )

    def check_table_mutation_allowed(
            self, org: str, sup: str, simple: str, *, lock_token: str,
    ) -> None:
        """Fail before storage I/O when a durable deletion intent exists.

        The check shares one Lua boundary with ownership validation.  Since a
        SimpleTable deleter needs this same leaf lock, absence of its intent is
        stable while the caller retains the lease.  The commit and mirror Lua
        scripts repeat the durable checks as the authoritative final fence.
        """
        result = int(self._assert_table_mutation_allowed(
            keys=[
                RK.lock_leaf(org, sup, simple),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent(org, sup, simple),
                RK.meta_root(org, sup),
            ],
            args=[lock_token or ""],
        ) or 0)
        if result == -1:
            raise LockLostError(
                f"Lost fencing lock before mutating {org}/{sup}/{simple}"
            )
        if result in (-2, -3):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        if result == -4:
            raise FileNotFoundError(
                f"SuperTable does not exist: {org}/{sup}"
            )
        if result == -5:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if result == -6:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        if result != 1:
            raise RuntimeError(f"Invalid mutation fence result: {result}")

    def _read_initial_table_mutation_pins(
            self, org: str, sup: str, simple: str,
    ) -> tuple[
        Any, Dict[str, Any], bool,
        Any, List[str], bool,
    ]:
        """Read and strictly parse the two expected-absent context pins.

        This helper conveys no authority and its result is never accepted from
        a caller. ``begin_table_mutation`` invokes the class implementation
        directly, then proves both exact raw values again inside Lua. Invalid
        documents are represented by validity flags so lock/root/deletion
        failure precedence remains atomic and no row IDs are allocated.
        """
        try:
            values = self.r.mget([
                RK.meta_table_config(org, sup, simple),
                RK.meta_mirrors(org, sup),
            ])
        except redis.RedisError as exc:
            logger.error("[redis-catalog] initial mutation pin read error: %s", exc)
            raise
        if not isinstance(values, (list, tuple)) or len(values) != 2:
            raise RuntimeError(f"Invalid initial mutation pin result: {values!r}")
        config_raw, mirrors_raw = values

        config: Dict[str, Any] = {}
        config_valid = True
        if config_raw is not None:
            try:
                config_raw, config = _validate_initial_table_config_pin(
                    config_raw,
                )
            except (ValueError, RecursionError):
                config_valid = False

        mirrors: List[str] = []
        mirrors_valid = True
        if mirrors_raw is not None:
            try:
                mirrors_raw, mirrors = _validate_initial_mirror_pin(mirrors_raw)
            except (ValueError, RecursionError):
                mirrors_valid = False
        return (
            config_raw,
            config,
            config_valid,
            mirrors_raw,
            mirrors,
            mirrors_valid,
        )

    def begin_table_mutation(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            lock_token: str,
            reserve_count: int = 0,
            prepared_leaf: Optional[_PreparedTableMutationLeaf] = None,
            namespace_token: str = "",
    ) -> Dict[str, Any]:
        """Pin one write context and reserve ordinary row IDs atomically.

        The returned leaf is the exact document observed together with the
        authoritative table and mirror configurations.  For current cached
        snapshots whose row-ID floor is exactly representable by Redis Lua,
        ``reserve_count`` is allocated in this same command.  Legacy,
        incomplete, or large-Int64 floors return no reservation so the writer
        can use :meth:`reserve_rowids_at_least` after deriving the immutable
        storage floor.  For an absent leaf, passing the namespace lock acquired
        before ``lock_token`` proves a canonical creation boundary and reserves
        above any exact orphaned sequence left by an earlier failed attempt.

        This is an early optimization boundary, not the publication boundary:
        :meth:`commit_snapshot` still repeats the live lock, root/deletion,
        mirror-generation, and exact leaf version/path CAS invariants.
        """
        if type(reserve_count) is not int:
            raise TypeError("rowid count must be an integer")
        if reserve_count < 0:
            raise ValueError("rowid count must be non-negative")
        if reserve_count > (1 << 63) - 1:
            raise OverflowError("rowid reservation exceeds signed Int64")
        prepared_raw = ""
        prepared_document: Optional[Dict[str, Any]] = None
        prepared_snapshot: Optional[Dict[str, Any]] = None
        prepared_floor: Optional[int] = None
        if prepared_leaf is not None:
            if type(prepared_leaf) is not _PreparedTableMutationLeaf:
                raise TypeError("prepared_leaf is not a catalog mutation pin")
            (
                prepared_raw,
                prepared_document,
                prepared_snapshot,
                prepared_floor,
            ) = prepared_leaf.take(self)

        mutation_keys = [
            RK.lock_leaf(org, sup, simple),
            RK.meta_namespace_deletion_intent(org, sup),
            RK.meta_simple_deletion_intent(org, sup, simple),
            RK.meta_root(org, sup),
            RK.meta_leaf(org, sup, simple),
            RK.meta_table_config(org, sup, simple),
            RK.meta_mirrors(org, sup),
            RK.meta_rowid_seq(org, sup, simple),
            RK.lock_namespace(org, sup),
        ]

        def general_begin(count: int):
            return self._begin_table_mutation(
                keys=mutation_keys,
                args=[
                    lock_token or "",
                    str(count),
                    prepared_raw,
                    "" if prepared_floor is None else str(prepared_floor),
                    namespace_token or "",
                    f"{org}/{sup}/tables/{simple}/tombstone/",
                ],
            )

        compact_pins: Optional[tuple[
            Any, Dict[str, Any], bool,
            Any, List[str], bool,
        ]] = None
        compact_calls = 0
        compact_pin_retries = 0
        compact_general_fallbacks = 0
        use_compact_initial = bool(
            type(self) is RedisCatalog
            and namespace_token
            and prepared_leaf is None
        )
        if use_compact_initial:
            # One bounded re-pin absorbs an ordinary config generation race.
            # A second mismatch is churn, not a state from which this mutation
            # can safely derive one authoritative context.
            for pin_attempt in range(2):
                try:
                    pins = RedisCatalog._read_initial_table_mutation_pins(
                        self, org, sup, simple,
                    )
                except UnicodeDecodeError:
                    # A decode_responses client can fail before exposing the
                    # invalid bytes needed for an exact pin. The existing Lua
                    # boundary still establishes fence/root failure precedence;
                    # zero reservation keeps its later Python decode fail-closed.
                    compact_general_fallbacks += 1
                    raw = general_begin(0)
                    break
                (
                    config_pin_raw,
                    config_pin,
                    config_pin_valid,
                    mirrors_pin_raw,
                    mirrors_pin,
                    mirrors_pin_valid,
                ) = pins
                compact_calls += 1
                raw = self._begin_initial_table_mutation(
                    keys=mutation_keys,
                    args=[
                        lock_token or "",
                        str(reserve_count),
                        namespace_token or "",
                        "1" if config_pin_raw is not None else "0",
                        config_pin_raw or "",
                        "1" if config_pin_valid else "0",
                        "1" if mirrors_pin_raw is not None else "0",
                        mirrors_pin_raw or "",
                        "1" if mirrors_pin_valid else "0",
                    ],
                )
                if isinstance(raw, (list, tuple)) and len(raw) == 1:
                    try:
                        compact_status = int(raw[0])
                    except (TypeError, ValueError):
                        compact_status = None
                    if compact_status == 2:
                        if pin_attempt == 0:
                            compact_pin_retries += 1
                            continue
                        raise SnapshotCommitConflictError(
                            "Initial table configuration changed repeatedly "
                            f"while beginning {org}/{sup}/{simple}"
                        )
                    if compact_status == 3:
                        # A creator appeared after CREATE authorization. The
                        # general boundary must pin that exact live leaf, but a
                        # zero reservation ensures Python validates every raw
                        # config before any IDs can be consumed. DataWriter then
                        # performs WRITE reauthorization and its ordinary exact
                        # floor-fenced reservation if permitted.
                        compact_general_fallbacks += 1
                        raw = general_begin(0)
                        break
                compact_pins = pins
                break
        else:
            raw = general_begin(reserve_count)
        if not isinstance(raw, (list, tuple)) or not raw:
            raise RuntimeError(f"Invalid table mutation context: {raw!r}")
        try:
            status = int(raw[0])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid table mutation context: {raw!r}") from exc
        if status == -1:
            raise LockLostError(
                f"Lost fencing lock before mutating {org}/{sup}/{simple}"
            )
        if status in (-2, -3):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        if status == -4:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if status == -5:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if status == -6:
            raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
        if status == -7:
            raise RuntimeError(
                f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
            )
        if status == -8:
            raise RuntimeError(
                f"Corrupt table configuration for {org}/{sup}/{simple}"
            )
        if status == -9:
            raise ValueError(
                f"Mirror configuration is invalid for {org}/{sup}"
            )
        if status == -10:
            raise LockLostError(
                f"Lost namespace creation lock before mutating "
                f"{org}/{sup}/{simple}"
            )
        if status == -11:
            raise RuntimeError(
                f"Corrupt Redis rowid sequence for {org}/{sup}/{simple}"
            )
        if status not in (0, 1) or len(raw) != 10:
            raise RuntimeError(f"Invalid table mutation context: {raw!r}")

        if compact_pins is not None:
            (
                config_pin_raw,
                config_pin,
                config_pin_valid,
                mirrors_pin_raw,
                mirrors_pin,
                mirrors_pin_valid,
            ) = compact_pins

            def compact_text(value: Any) -> str:
                if isinstance(value, bytes):
                    try:
                        return value.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise RuntimeError(
                            f"Invalid table mutation context: {raw!r}"
                        ) from exc
                if isinstance(value, str):
                    return value
                raise RuntimeError(f"Invalid table mutation context: {raw!r}")

            text_fields = [compact_text(value) for value in raw[1:]]
            expected_config_raw = config_pin_raw or ""
            expected_mirrors_raw = mirrors_pin_raw or ""
            if (
                type(raw[0]) is not int
                or status != 0
                or not config_pin_valid
                or not mirrors_pin_valid
                or text_fields[0] != ""
                or text_fields[1] != expected_config_raw
                or text_fields[2] != expected_mirrors_raw
                or text_fields[3] != "1"
                or text_fields[5] not in ("0", "1")
                or text_fields[8] != "0"
                or not re.fullmatch(r"(?:0|[1-9][0-9]*)", text_fields[4])
                or not re.fullmatch(r"(?:0|[1-9][0-9]*)", text_fields[6])
                or not re.fullmatch(r"(?:0|[1-9][0-9]*)", text_fields[7])
            ):
                raise RuntimeError(f"Invalid table mutation context: {raw!r}")
            try:
                floor = int(text_fields[4])
                previous = int(text_fields[6])
                new_high = int(text_fields[7])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Invalid initial rowid reservation: {raw!r}"
                ) from exc
            reserved = text_fields[5] == "1"
            if (
                floor > (1 << 63) - 1
                or previous != floor
                or (reserve_count == 0 and (
                    reserved or new_high != floor
                ))
                or (reserve_count > 0 and (
                    not reserved
                    or new_high != floor + reserve_count
                    or new_high > (1 << 63) - 1
                ))
            ):
                raise RuntimeError(f"Unsafe initial rowid reservation: {raw!r}")
            return {
                "leaf": None,
                "table_config": dict(config_pin),
                "mirrors": list(mirrors_pin),
                "mirror_pin": mirrors_pin_raw,
                "rowid_floor": floor,
                "rowid_reservation": (
                    (floor + 1, new_high) if reserved else None
                ),
                "_initial_compact_begin_calls": compact_calls,
                "_initial_compact_begin_pin_retries": compact_pin_retries,
                "_initial_compact_begin_general_fallbacks": (
                    compact_general_fallbacks
                ),
            }

        def decode_json(value: Any, *, field: str) -> Any:
            if value in (None, "", b""):
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
                raise RuntimeError(
                    f"Corrupt {field} for {org}/{sup}/{simple}"
                ) from exc

        config = decode_json(raw[2], field="table configuration")
        if config is not None and not isinstance(config, dict):
            raise RuntimeError(
                f"Corrupt table configuration for {org}/{sup}/{simple}"
            )
        if config is not None:
            try:
                config = _validate_table_config_document(config)
            except ValueError as exc:
                raise RuntimeError(
                    f"Corrupt table configuration for {org}/{sup}/{simple}: "
                    f"{exc}"
                ) from exc

        mirror_pin_raw = raw[3]
        if isinstance(mirror_pin_raw, bytes):
            try:
                mirror_pin_raw = mirror_pin_raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError(
                    f"Corrupt mirror configuration for {org}/{sup}/{simple}"
                ) from exc
        mirror_pin = None if mirror_pin_raw in (None, "") else mirror_pin_raw
        if mirror_pin is not None and not isinstance(mirror_pin, str):
            raise RuntimeError(
                f"Corrupt mirror configuration for {org}/{sup}/{simple}"
            )
        mirrors_document = decode_json(
            mirror_pin_raw, field="mirror configuration",
        )
        mirrors: List[str] = []
        if mirrors_document is not None:
            if not isinstance(mirrors_document, dict):
                raise ValueError(
                    f"Mirror configuration is invalid for {org}/{sup}"
                )
            formats = mirrors_document.get("formats")
            timestamp = mirrors_document.get("ts")
            if (
                not isinstance(formats, list)
                or type(timestamp) is not int
                or timestamp < 0
                or timestamp > _REDIS_LUA_MAX_SAFE_INTEGER
            ):
                raise ValueError(
                    f"Mirror configuration is invalid for {org}/{sup}"
                )
            for value in formats:
                if not isinstance(value, str):
                    raise ValueError(
                        f"Mirror configuration is invalid for {org}/{sup}"
                    )
                normalized = value.upper()
                if normalized not in ("DELTA", "ICEBERG", "PARQUET"):
                    raise ValueError(
                        f"Mirror configuration is invalid for {org}/{sup}"
                    )
                if normalized in mirrors:
                    raise ValueError(
                        f"Mirror configuration is invalid for {org}/{sup}"
                    )
                mirrors.append(normalized)

        leaf: Optional[Dict[str, Any]] = None
        validated_snapshot: Optional[Dict[str, Any]] = None
        rowid_floor: Optional[int] = None
        rowid_reservation: Optional[tuple[int, int]] = None
        if status == 1:
            prepared_match = str(raw[9]) == "1"
            if prepared_match:
                if prepared_document is None:
                    raise RuntimeError(
                        "Redis accepted a missing prepared mutation leaf"
                    )
                leaf = prepared_document
                payload = prepared_snapshot
            else:
                leaf_document = decode_json(raw[1], field="Redis leaf JSON")
                try:
                    leaf = _validate_leaf_document(leaf_document)
                except ValueError as exc:
                    raise RuntimeError(
                        f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
                    ) from exc

                payload = _complete_table_bound_snapshot_payload(
                    leaf.get("payload"),
                    expected_version=leaf["version"],
                    org=org,
                    sup=sup,
                    simple=simple,
                )
            validated_snapshot = payload
            floor_available = str(raw[4]) == "1"
            if floor_available and payload is not None:
                candidate_floor = payload.get("rowid_high_watermark")
                if (
                    type(candidate_floor) is int
                    and 0 <= candidate_floor <= _REDIS_LUA_MAX_SAFE_INTEGER
                    and str(candidate_floor) == str(raw[5])
                ):
                    rowid_floor = candidate_floor
                    if str(raw[6]) == "1":
                        try:
                            previous = int(raw[7])
                            new_high = int(raw[8])
                        except (TypeError, ValueError) as exc:
                            raise RuntimeError(
                                f"Invalid rowid reservation result: {raw!r}"
                            ) from exc
                        start = previous + 1
                        if (
                            reserve_count <= 0
                            or start <= candidate_floor
                            or new_high != previous + reserve_count
                            or new_high > (1 << 63) - 1
                        ):
                            raise RuntimeError(
                                f"Unsafe rowid reservation result: {raw!r}"
                            )
                        rowid_reservation = (start, new_high)
        elif str(raw[4]) == "1":
            # An absent leaf has no snapshot payload from which to derive a
            # floor.  The namespace-fenced Lua branch instead returns the exact
            # Redis integer strings it observed/reserved before any storage I/O.
            try:
                candidate_floor = int(raw[5])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Invalid initial rowid floor: {raw!r}"
                ) from exc
            if not 0 <= candidate_floor <= (1 << 63) - 1:
                raise RuntimeError(f"Unsafe initial rowid floor: {raw!r}")
            rowid_floor = candidate_floor
            if str(raw[6]) == "1":
                try:
                    previous = int(raw[7])
                    new_high = int(raw[8])
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        f"Invalid initial rowid reservation: {raw!r}"
                    ) from exc
                start = previous + 1
                if (
                    reserve_count <= 0
                    or previous != candidate_floor
                    or start <= candidate_floor
                    or new_high != previous + reserve_count
                    or new_high > (1 << 63) - 1
                ):
                    raise RuntimeError(
                        f"Unsafe initial rowid reservation: {raw!r}"
                    )
                rowid_reservation = (start, new_high)

        context = {
            "leaf": leaf,
            "table_config": dict(config or {}),
            "mirrors": mirrors,
            "mirror_pin": mirror_pin,
            "rowid_floor": rowid_floor,
            "rowid_reservation": rowid_reservation,
        }
        if use_compact_initial:
            context.update({
                "_initial_compact_begin_calls": compact_calls,
                "_initial_compact_begin_pin_retries": compact_pin_retries,
                "_initial_compact_begin_general_fallbacks": (
                    compact_general_fallbacks
                ),
            })
        if validated_snapshot is not None:
            context["validated_snapshot"] = validated_snapshot
        return context

    def prepare_table_mutation_leaf(
            self, org: str, sup: str, simple: str,
    ) -> Optional[_PreparedTableMutationLeaf]:
        """Parse one leaf once for an immediately-following fenced begin.

        Callers must already hold the table lease.  The returned object is not
        itself authority: ``begin_table_mutation`` consumes it once and Lua
        compares its exact raw JSON with the live Redis value before skipping
        the expensive recursive cjson materialization.
        """
        try:
            raw = self.r.get(RK.meta_leaf(org, sup, simple))
        except redis.RedisError as exc:
            logger.error("[redis-catalog] prepare mutation leaf error: %s", exc)
            raise
        if not raw:
            return None
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError(
                    f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
                ) from exc
        if not isinstance(raw, str):
            raise RuntimeError(
                f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
            )
        try:
            leaf = _validate_leaf_document(json.loads(raw))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
            ) from exc
        snapshot = _complete_table_bound_snapshot_payload(
            leaf.get("payload"),
            expected_version=leaf["version"],
            org=org,
            sup=sup,
            simple=simple,
        )
        floor: Optional[int] = None
        if snapshot is not None:
            candidate = snapshot.get("rowid_high_watermark")
            if type(candidate) is int and 0 <= candidate <= _REDIS_LUA_MAX_SAFE_INTEGER:
                floor = candidate
        return _PreparedTableMutationLeaf(
            owner=self,
            raw_leaf=raw,
            leaf=leaf,
            snapshot_payload=snapshot,
            rowid_floor=floor,
        )

    def check_initialization_allowed(
            self,
            org: str,
            sup: str,
            *,
            namespace_token: str,
            simple: Optional[str] = None,
    ) -> None:
        """Check durable deletion state before the first storage-side init."""
        namespace_intent = RK.meta_namespace_deletion_intent(org, sup)
        simple_intent = (
            RK.meta_simple_deletion_intent(org, sup, simple)
            if simple is not None else namespace_intent
        )
        result = int(self._assert_initialization_allowed(
            keys=[
                RK.lock_namespace(org, sup),
                namespace_intent,
                simple_intent,
            ],
            args=[namespace_token or ""],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost namespace initialization lock")
        if result in (-2, -3):
            scope = f"{org}/{sup}" + (f"/{simple}" if simple else "")
            raise DeletionIntentConflictError(
                f"Durable deletion intent blocks initialization of {scope}"
            )
        if result != 1:
            raise RuntimeError(f"Invalid initialization fence result: {result}")

    def begin_simple_deletion(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            namespace_token: str,
            lock_token: str,
            intent_id: Optional[str] = None,
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Create a no-TTL SimpleTable delete intent under both live locks."""
        iid = intent_id or secrets.token_hex(16)
        timestamp = _publication_timestamp(now_ms)
        record = {
            "schema_version": 1,
            "kind": "simple_table",
            "organization": org,
            "super_name": sup,
            "table_name": simple,
            "intent_id": iid,
            "status": "deleting",
            "namespace_lock_token": namespace_token,
            "leaf_lock_token": lock_token,
            "created_at_ms": timestamp,
            "recovery_count": 0,
        }
        result = int(self._begin_simple_deletion(
            keys=[
                RK.meta_simple_deletion_intent(org, sup, simple),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.lock_leaf(org, sup, simple),
                RK.meta_leaf(org, sup, simple),
                RK.quality_prefix(org, sup) + f"running:{simple}",
                RK.meta_root(org, sup),
            ],
            args=[
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                iid,
                namespace_token or "",
                lock_token or "",
                simple,
            ],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost namespace lock before deletion intent")
        if result == -2:
            raise LockLostError("Lost table lock before deletion intent")
        if result == -3:
            raise DeletionIntentConflictError(
                f"SuperTable deletion is already pending: {org}/{sup}"
            )
        if result == -4:
            raise DeletionIntentConflictError(
                f"A prior deletion intent still fences {org}/{sup}/{simple}; "
                "ordinary retry is unsafe"
            )
        if result == -5:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}/{simple}")
        if result == -6:
            raise RuntimeError(f"Cannot delete missing table {org}/{sup}/{simple}")
        if result == -7:
            raise RuntimeError("simple deletion-intent index has wrong Redis type")
        if result == -8:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result == -9:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if result == -10:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        if result not in (1, 2):
            raise RuntimeError(f"Invalid begin_simple_deletion result: {result}")
        return self.get_simple_deletion_intent(org, sup, simple) or record

    def recover_simple_deletion(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            expected_intent_id: str,
            namespace_token: str,
            lock_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> Dict[str, Any]:
        """Explicitly rebind an abandoned intent after operator liveness proof.

        A boolean cannot itself prove process death.  Its deliberately verbose
        name makes the required operational precondition part of the API: the
        caller must first ensure the former worker cannot resume its old
        fixed-prefix object-store delete.
        """
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Deletion recovery requires confirmation that the previous "
                "owner has stopped"
            )
        result = int(self._recover_simple_deletion(
            keys=[
                RK.meta_simple_deletion_intent(org, sup, simple),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.lock_leaf(org, sup, simple),
                RK.quality_prefix(org, sup) + f"running:{simple}",
            ],
            args=[
                expected_intent_id or "",
                namespace_token or "",
                lock_token or "",
                simple,
                _now_ms(),
            ],
        ) or 0)
        if result in (-1, -2):
            raise LockLostError("Lost deletion-recovery lock")
        if result == -3:
            raise DeletionIntentConflictError(
                f"SuperTable deletion is already pending: {org}/{sup}"
            )
        if result == -4:
            raise DeletionIntentConflictError(
                "Deletion intent changed or no longer exists"
            )
        if result == -5:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}/{simple}")
        if result != 1:
            raise RuntimeError(f"Invalid recover_simple_deletion result: {result}")
        record = self.get_simple_deletion_intent(org, sup, simple)
        if record is None:
            raise RuntimeError("Recovered deletion intent disappeared")
        return record

    def clear_simple_deletion_tombstone(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            expected_intent_id: str,
            namespace_token: str,
            lock_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> None:
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Tombstone clearing requires confirmation that every previous "
                "mutation owner has stopped"
            )
        self._assert_simple_quality_column_keys_absent(org, sup, simple)
        result = int(self._clear_simple_deletion(
            keys=[
                RK.meta_simple_deletion_intent(org, sup, simple),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.lock_leaf(org, sup, simple),
                RK.meta_leaf(org, sup, simple),
                RK.schema(org, sup, simple),
                RK.meta_rowid_seq(org, sup, simple),
                RK.meta_table_config(org, sup, simple),
                RK.meta_mirror_publication(org, sup, simple),
                RK.meta_table_names(org, sup),
                *self._quality_table_mutable_keys(org, sup, simple),
            ],
            args=[
                expected_intent_id or "",
                namespace_token or "",
                lock_token or "",
                simple,
            ],
        ) or 0)
        if result in (-1, -2):
            raise LockLostError("Lost lock before clearing table tombstone")
        if result == -3:
            raise DeletionIntentConflictError("Parent deletion is pending")
        if result == -4:
            raise DeletionIntentConflictError(
                "Table deletion tombstone changed or is not terminal"
            )
        if result == -5:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}/{simple}")
        if result == -6:
            raise RuntimeError("Table catalog state remains after deletion")
        if result != 1:
            raise RuntimeError(f"Invalid table tombstone clear result: {result}")

    def begin_namespace_deletion(
            self,
            org: str,
            sup: str,
            *,
            namespace_token: str,
            intent_id: Optional[str] = None,
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Create a no-TTL whole-SuperTable deletion intent."""
        iid = intent_id or secrets.token_hex(16)
        timestamp = _publication_timestamp(now_ms)
        record = {
            "schema_version": 1,
            "kind": "super_table",
            "organization": org,
            "super_name": sup,
            "intent_id": iid,
            "status": "deleting",
            "namespace_lock_token": namespace_token,
            "created_at_ms": timestamp,
            "recovery_count": 0,
        }
        result = int(self._begin_namespace_deletion(
            keys=[
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_stage_deletion_intent_index(org, sup),
                RK.meta_root(org, sup),
            ],
            args=[
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                iid,
                namespace_token or "",
            ],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost namespace lock before deletion intent")
        if result == -2:
            raise DeletionIntentConflictError(
                f"A prior deletion intent still fences {org}/{sup}; "
                "ordinary retry is unsafe"
            )
        if result == -3:
            raise DeletionIntentConflictError(
                "A SimpleTable deletion must be recovered before deleting its parent"
            )
        if result == -4:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}")
        if result == -5:
            raise RuntimeError("simple deletion-intent index has wrong Redis type")
        if result == -6:
            raise RuntimeError("stage deletion-intent index has wrong Redis type")
        if result == -7:
            raise DeletionIntentConflictError(
                "A staging deletion must be recovered before deleting its parent"
            )
        if result == -8:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result == -9:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if result not in (1, 2):
            raise RuntimeError(f"Invalid begin_namespace_deletion result: {result}")
        return self.get_namespace_deletion_intent(org, sup) or record

    def recover_namespace_deletion(
            self,
            org: str,
            sup: str,
            *,
            expected_intent_id: str,
            namespace_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> Dict[str, Any]:
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Deletion recovery requires confirmation that the previous "
                "owner has stopped"
            )
        result = int(self._recover_namespace_deletion(
            keys=[
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_stage_deletion_intent_index(org, sup),
            ],
            args=[expected_intent_id or "", namespace_token or "", _now_ms()],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost namespace deletion-recovery lock")
        if result == -2:
            raise DeletionIntentConflictError(
                "Deletion intent changed or no longer exists"
            )
        if result == -3:
            raise DeletionIntentConflictError(
                "A SimpleTable deletion must be recovered before its parent"
            )
        if result == -4:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}")
        if result == -5:
            raise RuntimeError("simple deletion-intent index has wrong Redis type")
        if result == -6:
            raise RuntimeError("stage deletion-intent index has wrong Redis type")
        if result == -7:
            raise DeletionIntentConflictError(
                "A staging deletion must be recovered before its parent"
            )
        if result != 1:
            raise RuntimeError(
                f"Invalid recover_namespace_deletion result: {result}"
            )
        record = self.get_namespace_deletion_intent(org, sup)
        if record is None:
            raise RuntimeError("Recovered namespace deletion intent disappeared")
        return record

    def clear_namespace_deletion_tombstone(
            self,
            org: str,
            sup: str,
            *,
            expected_intent_id: str,
            namespace_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> None:
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Tombstone clearing requires confirmation that every previous "
                "mutation owner has stopped"
            )
        result = int(self._clear_namespace_deletion(
            keys=[
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_namespace(org, sup),
                RK.meta_root(org, sup),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_stage_deletion_intent_index(org, sup),
            ],
            args=[expected_intent_id or "", namespace_token or ""],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost lock before clearing namespace tombstone")
        if result == -2:
            raise DeletionIntentConflictError(
                "Namespace deletion tombstone changed or is not terminal"
            )
        if result == -3:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}")
        if result == -4:
            raise RuntimeError("Namespace catalog state remains after deletion")
        if result != 1:
            raise RuntimeError(
                f"Invalid namespace tombstone clear result: {result}"
            )


    def scan_stage_lock_names(self, org: str, sup: str) -> List[str]:
        """Return a bounded, validated snapshot of live stage-lock names.

        Namespace deletion calls this only after its durable parent intent is
        visible. A full Redis SCAN therefore includes every pre-intent stage
        writer whose renewable lock remains live, including creators that have
        not published a staging document/index entry yet.
        """
        prefix = RK.lock_stage_prefix(org, sup)
        pattern = RK.lock_stage_pattern(org, sup)
        names: set[str] = set()
        cursor = 0
        calls = 0
        try:
            while True:
                calls += 1
                if calls > self._STAGE_LOCK_SCAN_CALL_LIMIT:
                    raise RuntimeError(
                        "Stage-lock discovery exceeded its scan-call bound"
                    )
                cursor, raw_keys = self.r.scan(
                    cursor=cursor,
                    match=pattern,
                    count=self._STAGE_LOCK_SCAN_COUNT,
                )
                for raw_key in raw_keys:
                    key = self._redis_key_text(raw_key)
                    if not key.startswith(prefix):
                        raise RuntimeError(
                            "Redis stage-lock scan returned an out-of-scope key"
                        )
                    stage_name = key[len(prefix):]
                    try:
                        canonical = RK.lock_stage(org, sup, stage_name)
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Redis stage-lock scan returned an invalid stage name"
                        ) from exc
                    if canonical != key:
                        raise RuntimeError(
                            "Redis stage-lock scan returned a non-canonical key"
                        )
                    names.add(stage_name)
                    if len(names) > self._STAGE_LOCK_DRAIN_LIMIT:
                        raise RuntimeError(
                            "Stage-lock discovery exceeded its key bound"
                        )
                cursor = int(cursor)
                if cursor == 0:
                    return sorted(names)
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] stage-lock SCAN error: {exc}")
            raise

    def scan_leaf_lock_names(self, org: str, sup: str) -> List[str]:
        """Return every live table-lock name, including pre-leaf creators."""
        prefix = RK.lock_leaf_prefix(org, sup)
        pattern = RK.lock_leaf_pattern(org, sup)
        names: set[str] = set()
        cursor = 0
        calls = 0
        try:
            while True:
                calls += 1
                if calls > self._LEAF_LOCK_SCAN_CALL_LIMIT:
                    raise RuntimeError(
                        "Leaf-lock discovery exceeded its scan-call bound"
                    )
                cursor, raw_keys = self.r.scan(
                    cursor=cursor,
                    match=pattern,
                    count=self._LEAF_LOCK_SCAN_COUNT,
                )
                for raw_key in raw_keys:
                    key = self._redis_key_text(raw_key)
                    if not key.startswith(prefix):
                        raise RuntimeError(
                            "Redis leaf-lock scan returned an out-of-scope key"
                        )
                    simple_name = key[len(prefix):]
                    try:
                        canonical = RK.lock_leaf(org, sup, simple_name)
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Redis leaf-lock scan returned an invalid table name"
                        ) from exc
                    if canonical != key:
                        raise RuntimeError(
                            "Redis leaf-lock scan returned a non-canonical key"
                        )
                    names.add(simple_name)
                    if len(names) > self._LEAF_LOCK_DRAIN_LIMIT:
                        raise RuntimeError(
                            "Leaf-lock discovery exceeded its key bound"
                        )
                cursor = int(cursor)
                if cursor == 0:
                    return sorted(names)
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] leaf-lock SCAN error: {exc}")
            raise


    def acquire_stage_lock(
            self,
            org: str,
            sup: str,
            stage_name: str,
            ttl_s: int = 30,
            timeout_s: int = 30,
    ) -> Optional[str]:
        """Acquire lock for staging/pipe operations:
            supertable:{org}:lakes:{sup}:lock:stage:doc:{stage_name}
        """
        return self._locker.acquire(RK.lock_stage(org, sup, stage_name), ttl_s=ttl_s, timeout_s=timeout_s)

    def release_stage_lock(
            self, org: str, sup: str, stage_name: str, token: str,
    ) -> bool:
        """Release a stage lock through the same auto-renewing lock owner."""
        return self._locker.release(RK.lock_stage(org, sup, stage_name), token)

    def get_stage_deletion_intent(
            self, org: str, sup: str, stage_name: str,
    ) -> Optional[Dict[str, Any]]:
        raw = self.r.get(RK.meta_stage_deletion_intent(org, sup, stage_name))
        return self._decode_deletion_intent(
            raw, scope=f"{org}/{sup}/staging/{stage_name}",
        )

    def check_stage_mutation_allowed(
            self, org: str, sup: str, stage_name: str, *, lock_token: str,
    ) -> None:
        result = int(self._assert_stage_mutation_allowed(
            keys=[
                RK.lock_stage(org, sup, stage_name),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_stage_deletion_intent(org, sup, stage_name),
                RK.meta_root(org, sup),
            ],
            args=[lock_token or ""],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost staging mutation lock")
        if result in (-2, -3):
            raise DeletionIntentConflictError(
                f"Durable deletion intent fences staging {org}/{sup}/{stage_name}"
            )
        if result == -4:
            raise FileNotFoundError(
                f"SuperTable does not exist: {org}/{sup}"
            )
        if result == -5:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if result == -6:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        if result != 1:
            raise RuntimeError(f"Invalid staging mutation fence result: {result}")

    def begin_stage_deletion(
            self,
            org: str,
            sup: str,
            stage_name: str,
            *,
            lock_token: str,
            intent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        iid = intent_id or secrets.token_hex(16)
        record = {
            "schema_version": 1,
            "kind": "staging",
            "organization": org,
            "super_name": sup,
            "staging_name": stage_name,
            "intent_id": iid,
            "status": "deleting",
            "lock_token": lock_token,
            "created_at_ms": _now_ms(),
            "recovery_count": 0,
        }
        result = int(self._begin_stage_deletion(
            keys=[
                RK.meta_stage_deletion_intent(org, sup, stage_name),
                RK.meta_stage_deletion_intent_index(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_stage(org, sup, stage_name),
                RK.meta_root(org, sup),
            ],
            args=[
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                iid,
                lock_token or "",
                stage_name,
            ],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost staging lock before deletion intent")
        if result == -2:
            raise DeletionIntentConflictError(
                f"SuperTable deletion is already pending: {org}/{sup}"
            )
        if result == -3:
            raise DeletionIntentConflictError(
                f"A prior deletion intent still fences staging "
                f"{org}/{sup}/{stage_name}; ordinary retry is unsafe"
            )
        if result == -4:
            raise RuntimeError(
                f"Corrupt deletion intent for staging {org}/{sup}/{stage_name}"
            )
        if result == -5:
            raise RuntimeError("stage deletion-intent index has wrong Redis type")
        if result == -6:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result == -7:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if result == -8:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        if result not in (1, 2):
            raise RuntimeError(f"Invalid begin_stage_deletion result: {result}")
        return self.get_stage_deletion_intent(org, sup, stage_name) or record

    def recover_stage_deletion(
            self,
            org: str,
            sup: str,
            stage_name: str,
            *,
            expected_intent_id: str,
            lock_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> Dict[str, Any]:
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Deletion recovery requires confirmation that the previous "
                "owner has stopped"
            )
        result = int(self._recover_stage_deletion(
            keys=[
                RK.meta_stage_deletion_intent(org, sup, stage_name),
                RK.meta_stage_deletion_intent_index(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.lock_stage(org, sup, stage_name),
            ],
            args=[
                expected_intent_id or "",
                lock_token or "",
                stage_name,
                _now_ms(),
            ],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost staging deletion-recovery lock")
        if result == -2:
            raise DeletionIntentConflictError(
                f"SuperTable deletion is already pending: {org}/{sup}"
            )
        if result == -3:
            raise DeletionIntentConflictError(
                "Staging deletion intent changed or no longer exists"
            )
        if result == -4:
            raise RuntimeError(
                f"Corrupt deletion intent for staging {org}/{sup}/{stage_name}"
            )
        if result != 1:
            raise RuntimeError(f"Invalid recover_stage_deletion result: {result}")
        record = self.get_stage_deletion_intent(org, sup, stage_name)
        if record is None:
            raise RuntimeError("Recovered staging deletion intent disappeared")
        return record

    def clear_stage_deletion_tombstone(
            self,
            org: str,
            sup: str,
            stage_name: str,
            *,
            expected_intent_id: str,
            lock_token: str,
            confirm_previous_owner_stopped: bool,
    ) -> None:
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Tombstone clearing requires confirmation that every previous "
                "mutation owner has stopped"
            )
        result = int(self._clear_stage_deletion(
            keys=[
                RK.meta_stage_deletion_intent(org, sup, stage_name),
                RK.meta_stage_deletion_intent_index(org, sup),
                RK.lock_stage(org, sup, stage_name),
                RK.staging_doc(org, sup, stage_name),
                RK.staging_index(org, sup),
            ],
            args=[expected_intent_id or "", lock_token or "", stage_name],
        ) or 0)
        if result == -1:
            raise LockLostError("Lost lock before clearing staging tombstone")
        if result == -2:
            raise DeletionIntentConflictError(
                "Staging deletion tombstone changed or is not terminal"
            )
        if result == -3:
            raise RuntimeError(
                f"Corrupt deletion intent for staging {org}/{sup}/{stage_name}"
            )
        if result == -4:
            raise RuntimeError("Staging catalog state remains after deletion")
        if result != 1:
            raise RuntimeError(f"Invalid staging tombstone clear result: {result}")





    def ensure_root(
            self, org: str, sup: str, *, namespace_token: str = "",
    ) -> None:
        """Initialize meta:root if missing with version=0."""
        key = RK.meta_root(org, sup)
        try:
            init = {"version": 0, "ts": _publication_timestamp(None)}
            # The namespace fence and initialize-only write share one Redis
            # transaction, so recreation cannot slip into a verified delete.
            result = int(self._root_ensure(
                keys=[
                    key,
                    RK.lock_namespace(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                ],
                args=[json.dumps(init), namespace_token or ""],
            ) or 0)
            if result == -1:
                raise RuntimeError(
                    f"SuperTable namespace is fenced for deletion: {org}/{sup}"
                )
            if result == -2:
                raise DeletionIntentConflictError(
                    f"SuperTable has a durable deletion intent: {org}/{sup}"
                )
            if result == -3:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result not in (0, 1):
                raise RuntimeError(f"Invalid root initialization result: {result}")
            # The Lua boundary validates mandatory identity atomically. This
            # strict read also rejects malformed optional lifecycle/security
            # fields on a pre-existing root instead of acknowledging it.
            self.get_root(org, sup)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] ensure_root failed: {e}")
            raise

    def root_exists(self, org: str, sup: str) -> bool:
        """Check for a structurally valid meta:root document."""
        return self.get_root(org, sup) is not None

    def leaf_exists(self, org: str, sup: str, simple: str) -> bool:
        """Check existence of meta:leaf key for a simple table (replica-aware)."""
        info = self._resolve_replica_info(org, sup)
        if info:
            source, allowed = info
            if allowed is not None and simple not in allowed:
                return False
            return self._get_replica_leaf_atomic(
                org, sup, simple, info=info,
            ) is not None
        return self._leaf_exists_raw(org, sup, simple)

    def _leaf_exists_raw(self, org: str, sup: str, simple: str) -> bool:
        try:
            return bool(self.r.exists(RK.meta_leaf(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_exists error: {e}")
            raise

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        try:
            raw = self.r.get(RK.meta_root(org, sup))
            if not raw:
                return None
            root = json.loads(raw)
            return _validate_root_document(root, org=org, sup=sup)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_root error: {e}")
            raise
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}") from exc

    def update_root_flags(self, org: str, sup: str, flags: Dict[str, Any]) -> bool:
        """Merge *flags* into the existing meta:root JSON document.

        Used to set read_only, cloned_from, clone_type, clone_ts without
        disturbing version/ts. The root must already exist.
        """
        if not isinstance(flags, dict):
            raise TypeError("Root flags must be a JSON object")
        if {"version", "ts", "commit_id"}.intersection(flags):
            raise ValueError("Root publication identity fields are immutable")
        self.check_deletion_intent_absent(org, sup)
        key = RK.meta_root(org, sup)
        try:
            expected_raw = self.r.get(key)
            if not expected_raw:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            try:
                current = _validate_root_document(
                    json.loads(expected_raw), org=org, sup=sup,
                )
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Corrupt Redis root JSON for {org}/{sup}"
                ) from exc
            candidate = dict(current)
            candidate.update(flags)
            try:
                _validate_root_document(candidate, org=org, sup=sup)
            except ValueError as exc:
                raise ValueError("Invalid root lifecycle flags") from exc

            result = int(self._update_root_flags(
                keys=[
                    key,
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_simple_deletion_intent_index(org, sup),
                    RK.meta_stage_deletion_intent_index(org, sup),
                ],
                args=[
                    json.dumps(flags),
                    _now_ms(),
                    sup,
                    expected_raw,
                ],
            ) or 0)
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise ValueError("Root flags are not valid JSON")
            if result == -3:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -4:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -5:
                raise SnapshotCommitConflictError(
                    f"SuperTable root changed before flags were updated: {org}/{sup}"
                )
            if result == -6:
                raise DeletionIntentConflictError(
                    f"A child deletion intent fences root flags for {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(f"Invalid root flag update result: {result}")
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] update_root_flags error: {e}")
            raise

    def find_readonly_clones(self, org: str, source_sup: str) -> List[str]:
        """Return names of supertables that are read-only clones of *source_sup*."""
        clones: List[str] = []
        pattern = RK.meta_root_pattern_for_org(org)
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=500)
                for k in keys:
                    ks = k if isinstance(k, str) else k.decode("utf-8")
                    parsed = RK.parse_lake_key(ks)
                    if parsed is None:
                        continue
                    _, sup_name = parsed
                    if sup_name == source_sup:
                        continue
                    try:
                        raw = self.r.get(ks)
                        if not raw:
                            continue
                        doc = json.loads(raw)
                        if doc.get("read_only") and doc.get("cloned_from") == source_sup:
                            clones.append(sup_name)
                    except Exception:
                        continue
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] find_readonly_clones error: {e}")
        return clones

    def bump_root(self, org: str, sup: str, now_ms: Optional[int] = None) -> int:
        self.check_deletion_intent_absent(org, sup)
        if self.get_root(org, sup) is None:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        timestamp = _publication_timestamp(now_ms)
        try:
            result = int(self._root_bump(
                keys=[
                    RK.meta_root(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                ],
                args=[timestamp],
            ) or 0)
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -3:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -4:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result == -5:
                raise RuntimeError(
                    f"Redis root numeric identity is exhausted or invalid: "
                    f"{org}/{sup}"
                )
            self.get_root(org, sup)
            return result
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_bump error: {e}")
            raise

    @staticmethod
    def _snapshot_schema_document(payload: Mapping[str, Any]) -> Dict[str, Any]:
        """Return the legacy schema HASH-shaped JSON stored beside the leaf."""
        schema_raw = (payload or {}).get("schema", {})
        if isinstance(schema_raw, dict):
            return dict(schema_raw)
        if isinstance(schema_raw, list):
            merged: Dict[str, Any] = {}
            for item in schema_raw:
                if isinstance(item, dict):
                    merged.update(item)
            return merged
        return {}

    def commit_snapshot(
            self,
            org: str,
            sup: str,
            simple: str,
            payload: Dict[str, Any],
            path: str,
            *,
            expected_version: int,
            expected_path: str,
            lock_token: str,
            commit_id: Optional[str] = None,
            mirror_publication: bool = False,
            expected_mirrors: Optional[Sequence[str]] = None,
            expected_mirror_pin: Any = _UNPINNED_MIRROR_CONFIG,
            quality_generation: Optional[str] = None,
            now_ms: Optional[int] = None,
            one_shot_initial: bool = False,
    ) -> tuple[int, int]:
        """Atomically publish one fenced table snapshot and bump its root.

        ``expected_version`` and ``expected_path`` identify the exact leaf
        snapshot from which the writer derived its immutable successor.
        When ``one_shot_initial`` is explicitly true, an expected-absent base
        (``-1``/empty path) publishes the first visible snapshot as version
        one, preserving the legacy generation after its removed empty
        version-zero bootstrap. Unflagged expected-absent calls retain the
        ordinary version-zero contract.
        ``lock_token`` must still own the per-table Redis lock when the Lua
        script executes.  The comparison, fencing check, leaf update, and root
        invalidation happen in one Redis transaction, so readers can never
        combine a new leaf with an old root generation (or vice versa).

        An ambiguous flagged one-shot creation is reconciled once against its
        exact immutable leaf ``commit_id``, version, path, and payload digest;
        blindly retrying it would conflict with its own created leaf. Normal
        mutation ambiguity retains the established propagation behavior.

        ``quality_generation``, when present, must be this commit's opaque ID.
        The same transaction persists it as unresolved post-ingest work.  The
        scheduler later expands it into configured modes under exact root/leaf
        lifecycle pins, so schedule reads never delay the writer.

        ``expected_mirror_pin`` is the exact raw empty-mirror configuration
        observed by :meth:`begin_table_mutation`; explicit ``None`` pins key
        absence.  It selects a smaller commit script that compares this raw
        generation without decoding mirror state.  Omission retains the
        general mirror-capable path for adapters and direct callers.
        """
        if not lock_token:
            raise LockLostError("snapshot publication requires a fencing lock token")
        if type(one_shot_initial) is not bool:
            raise TypeError("one_shot_initial must be a boolean")
        timestamp = _publication_timestamp(now_ms)
        base_version = _lua_safe_integer(
            expected_version,
            field="expected snapshot version",
            minimum=-1,
        )
        if one_shot_initial and (
            base_version != -1 or expected_path != ""
        ):
            raise ValueError(
                "one_shot_initial requires expected_version=-1 and an empty "
                "expected_path"
            )
        if not isinstance(payload, Mapping):
            raise ValueError("snapshot payload must be a JSON object")
        successor_version = 1 if one_shot_initial else base_version + 1
        if (
            type(payload.get("snapshot_version")) is not int
            or payload["snapshot_version"] != successor_version
        ):
            raise ValueError(
                "snapshot payload has an invalid version: payload generation "
                "does not match the exact successor fenced by expected_version"
            )
        if not {
            "tombstone", "tombstone_rows", "tombstone_digest",
        }.issubset(payload):
            raise ValueError(
                "snapshot payload must carry an explicit deletion-vector state"
            )
        try:
            tombstone_format = validate_snapshot_tombstone_state(
                payload.get("tombstone"),
                payload.get("tombstone_rows"),
                payload.get("tombstone_digest"),
                format_present="tombstone_format" in payload,
                tombstone_format=payload.get("tombstone_format"),
            )
        except (TypeError, TombstoneManifestV2Error) as exc:
            raise ValueError(
                "snapshot payload has an invalid deletion-vector state"
            ) from exc
        explicit_immutable_format = tombstone_format in (
            TOMBSTONE_FORMAT_V2,
            TOMBSTONE_FORMAT_V3,
        )
        if (
            explicit_immutable_format
            and successor_version > MAX_TOMBSTONE_JSON_EXACT_INTEGER
        ):
            raise ValueError(
                "explicit snapshot version exceeds Redis JSON's exact "
                "integer range"
            )
        if (
            explicit_immutable_format
            and payload.get("tombstone") is not None
            and (
                successor_version < 1
                or payload["tombstone_rows"] > MAX_TOMBSTONE_JSON_EXACT_INTEGER
            )
        ):
            raise ValueError(
                "active explicit snapshot deletion-vector state has an "
                "invalid version or row-count bound"
            )
        tombstone_prefix = f"{org}/{sup}/tables/{simple}/tombstone/"
        if (
            explicit_immutable_format
            and payload.get("tombstone") is not None
            and not payload["tombstone"].startswith(tombstone_prefix)
        ):
            raise ValueError(
                "snapshot tombstone pointer escapes the table tombstone namespace"
            )
        try:
            payload = snapshot_cache_payload(payload)
            payload_json = json.dumps(payload)
        except Exception as exc:
            raise ValueError(
                "snapshot payload is not JSON serializable"
            ) from exc
        if one_shot_initial:
            payload_version = payload.get("snapshot_version")
            if payload_version is not None and (
                type(payload_version) is not int
                or payload_version != successor_version
            ):
                raise ValueError(
                    "invalid snapshot payload: payload generation does not "
                    "match its fenced successor: expected "
                    f"{successor_version}, got {payload_version!r}"
                )
        payload_digest = (
            hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            if one_shot_initial
            else ""
        )

        if mirror_publication and not commit_id:
            raise ValueError(
                "mirror-tracked snapshot publication requires an explicit commit_id"
            )
        cid = commit_id or secrets.token_hex(16)
        if quality_generation is None:
            quality_generation = ""
        elif quality_generation != cid:
            raise ValueError(
                "quality_generation must equal the snapshot commit_id"
            )
        if expected_mirrors is None:
            expected_mirrors = self.get_mirrors(org, sup)
        if isinstance(expected_mirrors, (str, bytes)):
            raise TypeError("expected_mirrors must be a sequence of formats")
        normalized_mirrors: List[str] = []
        for value in expected_mirrors:
            if not isinstance(value, str):
                raise ValueError("expected_mirrors contains an invalid format")
            normalized = value.upper()
            if normalized not in ("DELTA", "ICEBERG", "PARQUET"):
                raise ValueError("expected_mirrors contains an invalid format")
            if normalized not in normalized_mirrors:
                normalized_mirrors.append(normalized)

        use_no_mirror_path = (
            expected_mirror_pin is not _UNPINNED_MIRROR_CONFIG
        )
        mirror_pin_present = False
        mirror_pin_raw = ""
        if use_no_mirror_path:
            if mirror_publication or normalized_mirrors:
                raise ValueError(
                    "no-mirror snapshot commit received configured mirrors"
                )
            if expected_mirror_pin is not None:
                if not isinstance(expected_mirror_pin, str) or not expected_mirror_pin:
                    raise TypeError(
                        "expected_mirror_pin must be raw JSON or explicit None"
                    )
                try:
                    pinned_document = json.loads(expected_mirror_pin)
                except (json.JSONDecodeError, TypeError) as exc:
                    raise ValueError(
                        "expected_mirror_pin is not valid JSON"
                    ) from exc
                if (
                    not isinstance(pinned_document, dict)
                    or pinned_document.get("formats") != []
                    or type(pinned_document.get("ts")) is not int
                    or pinned_document["ts"] < 0
                    or pinned_document["ts"] > _REDIS_LUA_MAX_SAFE_INTEGER
                ):
                    raise ValueError(
                        "expected_mirror_pin is not an empty mirror configuration"
                    )
                mirror_pin_present = True
                mirror_pin_raw = expected_mirror_pin
        try:
            schema_json = json.dumps(self._snapshot_schema_document(payload))
            if use_no_mirror_path:
                raw = self._snapshot_commit_no_mirrors(
                    keys=[
                        RK.meta_leaf(org, sup, simple),
                        RK.meta_root(org, sup),
                        RK.lock_leaf(org, sup, simple),
                        RK.lock_namespace(org, sup),
                        RK.meta_table_names(org, sup),
                        RK.meta_namespace_deletion_intent(org, sup),
                        RK.meta_simple_deletion_intent(org, sup, simple),
                        RK.schema(org, sup, simple),
                        self._quality_key(
                            org, sup, "pending_unresolved", simple,
                        ),
                        RK.meta_mirrors(org, sup),
                    ],
                    args=[
                        payload_json,
                        path,
                        timestamp,
                        base_version,
                        expected_path or "",
                        lock_token,
                        cid,
                        simple,
                        schema_json,
                        quality_generation,
                        "1" if mirror_pin_present else "0",
                        mirror_pin_raw,
                        tombstone_prefix,
                        payload_digest,
                        "1" if one_shot_initial else "0",
                    ],
                )
            else:
                raw = self._snapshot_commit(
                    keys=[
                        RK.meta_leaf(org, sup, simple),
                        RK.meta_root(org, sup),
                        RK.lock_leaf(org, sup, simple),
                        RK.meta_mirror_publication(org, sup, simple),
                        RK.lock_namespace(org, sup),
                        RK.meta_table_names(org, sup),
                        RK.meta_namespace_deletion_intent(org, sup),
                        RK.meta_simple_deletion_intent(org, sup, simple),
                        RK.schema(org, sup, simple),
                        RK.meta_mirrors(org, sup),
                        self._quality_key(
                            org, sup, "pending_unresolved", simple,
                        ),
                    ],
                    args=[
                        payload_json,
                        path,
                        timestamp,
                        base_version,
                        expected_path or "",
                        lock_token,
                        cid,
                        "1" if mirror_publication else "0",
                        simple,
                        schema_json,
                        json.dumps(normalized_mirrors),
                        quality_generation,
                        tombstone_prefix,
                        payload_digest,
                        "1" if one_shot_initial else "0",
                    ],
                )
        except redis.RedisError as exc:
            # A timeout/disconnect can arrive after Redis executed the atomic
            # script. A flagged one-shot creation cannot be blindly retried:
            # it would conflict with its own successfully-created leaf, so
            # prove that exact first commit from its persisted payload digest.
            # Normal mutations retain the established ambiguous-error behavior
            # and leaf shape; they do not pay a hash/reconciliation cost.
            if one_shot_initial:
                reconciled = self._reconcile_snapshot_commit(
                    org,
                    sup,
                    simple,
                    path=path,
                    expected_version=base_version,
                    commit_id=cid,
                    payload_digest=payload_digest,
                )
                if reconciled is not None:
                    logger.warning(
                        "[redis-catalog] reconciled ambiguous initial snapshot "
                        "commit for %s/%s/%s commit %s",
                        org,
                        sup,
                        simple,
                        cid,
                    )
                    return reconciled
            logger.error(f"[redis-catalog] snapshot commit error: {exc}")
            raise

        try:
            code, leaf_version, root_version = [int(v) for v in raw]
        except Exception as exc:
            raise RuntimeError(f"Invalid snapshot commit result: {raw!r}") from exc
        if code == 1:
            return leaf_version, root_version
        if code == -1:
            raise SnapshotCommitConflictError(
                f"Snapshot base changed for {org}/{sup}/{simple}: "
                f"expected version={expected_version}, path={expected_path!r}; "
                f"current version={leaf_version}"
            )
        if code == -2:
            raise LockLostError(
                f"Lost fencing lock before publishing {org}/{sup}/{simple}"
            )
        if code == -3:
            raise RuntimeError(
                f"Corrupt Redis catalog JSON for {org}/{sup}/{simple}"
            )
        if code == -4:
            raise ValueError(
                "Redis rejected invalid snapshot payload JSON"
            )
        if code == -5:
            raise RuntimeError(
                f"Missing or mismatched mirror publication intent for "
                f"{org}/{sup}/{simple} commit {cid}"
            )
        if code == -6:
            raise RuntimeError(
                f"Mirror publication intent is not prepared for "
                f"{org}/{sup}/{simple} commit {cid}"
            )
        if code == -7:
            raise RuntimeError(
                f"SuperTable namespace is fenced for deletion: {org}/{sup}"
            )
        if code in (-8, -9):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        if code == -10:
            raise SnapshotCommitConflictError(
                f"Mirror publication is owned by another publisher for "
                f"{org}/{sup}/{simple} commit {cid}"
            )
        if code == -11:
            raise FileNotFoundError(
                f"SuperTable does not exist: {org}/{sup}"
            )
        if code == -12:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        if code == -13:
            raise RuntimeError(
                f"Redis snapshot numeric identity is exhausted or invalid: "
                f"{org}/{sup}/{simple}"
            )
        if code == -14:
            raise SnapshotCommitConflictError(
                f"Mirror configuration changed before snapshot publication: "
                f"{org}/{sup}/{simple}"
            )
        if code == -15:
            raise RuntimeError(
                f"Corrupt mirror configuration during snapshot publication: "
                f"{org}/{sup}/{simple}"
            )
        if code == -16:
            raise ValueError(
                "Redis rejected invalid snapshot payload: generation does not "
                f"match its fenced successor for {org}/{sup}/{simple}"
            )
        if code == -17:
            raise ValueError(
                "Redis rejected an invalid one-shot initial publication flag "
                f"or base identity for {org}/{sup}/{simple}"
            )
        raise RuntimeError(f"Unknown snapshot commit status {code}")

    def _reconcile_snapshot_commit(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            path: str,
            expected_version: int,
            commit_id: str,
            payload_digest: str,
    ) -> Optional[tuple[int, int]]:
        """Return committed versions when an ambiguous reply actually landed.

        This proof is intentionally restricted by the caller to a flagged
        one-shot expected-absent creation.
        The exact leaf commit id, path, version-one successor, and digest of
        the original payload JSON are written atomically with root/schema/index
        state. Comparing the digest rather than Lua's decoded/re-encoded cache
        stays exact for Int64 values and empty JSON object/array normalization.
        The root may already have advanced because another table has its own
        lock, so only require a structurally valid current root.
        Any read ambiguity or mismatch returns ``None`` and preserves the
        original transport exception.
        """
        if expected_version != -1:
            return None
        try:
            raw_leaf, raw_root = self.r.mget([
                RK.meta_leaf(org, sup, simple),
                RK.meta_root(org, sup),
            ])
            if raw_leaf is None or raw_root is None:
                return None
            leaf = _validate_leaf_document(json.loads(raw_leaf))
            root = _validate_root_document(
                json.loads(raw_root), org=org, sup=sup,
            )
            successor = 1 if expected_version == -1 else expected_version + 1
            if (
                leaf.get("commit_id") != commit_id
                or leaf.get("path") != path
                or leaf.get("version") != successor
                or leaf.get("payload_digest") != payload_digest
                or root.get("version", -1) < 0
            ):
                return None
            return successor, int(root["version"])
        except Exception:
            return None

    def prepare_mirror_publication(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            commit_id: str,
            snapshot_path: str,
            mirrors: List[str],
            lock_token: str,
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Durably prepare one mirror outbox record under the table lock.

        A previous non-complete record blocks replacement. This deliberately
        forces explicit reconciliation instead of losing evidence that a
        latest-only mirror may expose an older snapshot.
        """
        if not commit_id or not snapshot_path or not lock_token:
            raise ValueError("mirror publication requires commit, snapshot, and lock")
        normalized: List[str] = []
        for value in mirrors or []:
            fmt = str(value).upper()
            if fmt not in ("DELTA", "ICEBERG", "PARQUET"):
                raise ValueError(f"Unsupported mirror format: {value!r}")
            if fmt not in normalized:
                normalized.append(fmt)
        if not normalized:
            raise ValueError("mirror publication requires at least one format")
        timestamp = _publication_timestamp(now_ms)
        record: Dict[str, Any] = {
            "schema_version": 2,
            "status": "prepared",
            "organization": org,
            "super_name": sup,
            "table_name": simple,
            "commit_id": commit_id,
            "snapshot_path": snapshot_path,
            "mirrors": normalized,
            "core_committed": False,
            "publication_owner": lock_token,
            "owner_generation": 0,
            "publisher_quiesced": False,
            "created_at_ms": timestamp,
            "updated_at_ms": timestamp,
            "error": None,
        }
        raw = self._mirror_publication_prepare(
            keys=[
                RK.meta_mirror_publication(org, sup, simple),
                RK.lock_leaf(org, sup, simple),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent(org, sup, simple),
                RK.meta_root(org, sup),
            ],
            args=[json.dumps(record), lock_token, commit_id],
        )
        code = int(raw or 0)
        if code in (1, 2):
            return self.get_mirror_publication(org, sup, simple) or record
        if code == -1:
            raise SnapshotCommitConflictError(
                f"Unresolved mirror publication blocks {org}/{sup}/{simple}"
            )
        if code == -2:
            raise LockLostError(
                f"Lost fencing lock before preparing mirror publication for "
                f"{org}/{sup}/{simple}"
            )
        if code == -3:
            raise RuntimeError(
                f"Corrupt mirror publication state for {org}/{sup}/{simple}"
            )
        if code in (-5, -6):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        if code == -7:
            raise SnapshotCommitConflictError(
                f"Mirror publication is owned by another publisher for "
                f"{org}/{sup}/{simple} commit {commit_id}"
            )
        if code == -8:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if code == -9:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if code == -10:
            raise ReadOnlyCatalogError(
                f"SuperTable is read-only: {org}/{sup}"
            )
        raise RuntimeError(f"Invalid mirror publication prepare status {code}")

    def get_mirror_publication(
            self, org: str, sup: str, simple: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest durable mirror outbox record, if any."""
        raw = self.r.get(RK.meta_mirror_publication(org, sup, simple))
        if not raw:
            return None
        try:
            record = json.loads(raw)
        except Exception as exc:
            raise RuntimeError(
                f"Corrupt mirror publication state for {org}/{sup}/{simple}"
            ) from exc
        if not isinstance(record, dict):
            raise RuntimeError(
                f"Corrupt mirror publication state for {org}/{sup}/{simple}"
            )
        return record

    def claim_mirror_publication(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            commit_id: str,
            expected_previous_owner: str,
            lock_token: str,
            confirm_previous_owner_stopped: bool,
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Explicitly rebind one unresolved mirror publisher.

        The durable owner is deliberately independent of the expiring Redis
        lease.  Only an allowlisted pre-I/O or post-success transition can mark
        itself quiescent and be claimed automatically; generic storage errors
        remain ambiguous.  Otherwise the caller must identify the exact intent
        and previous owner and attest that the old process cannot resume.  The
        Lua claim compares that identity, the new live lock, and deletion
        fences in one atomic operation.
        """
        if not commit_id or expected_previous_owner is None or not lock_token:
            raise ValueError(
                "Mirror recovery requires an exact commit, previous owner, "
                "and live lock token"
            )
        raw = self._mirror_publication_claim(
            keys=[
                RK.meta_mirror_publication(org, sup, simple),
                RK.lock_leaf(org, sup, simple),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent(org, sup, simple),
            ],
            args=[
                commit_id,
                expected_previous_owner,
                lock_token,
                _publication_timestamp(now_ms),
                "1" if confirm_previous_owner_stopped is True else "0",
            ],
        )
        code = int(raw or 0)
        if code in (1, 2):
            record = self.get_mirror_publication(org, sup, simple)
            if record is None:
                raise RuntimeError("Claimed mirror publication disappeared")
            if (
                str(record.get("commit_id") or "") != commit_id
                or str(record.get("publication_owner") or "") != lock_token
            ):
                raise RuntimeError("Mirror owner claim returned invalid state")
            return record
        if code == -1:
            raise SnapshotCommitConflictError(
                f"Mirror publication intent changed for {org}/{sup}/{simple}"
            )
        if code == -2:
            raise LockLostError(
                f"Lost fencing lock while claiming mirror publication for "
                f"{org}/{sup}/{simple}"
            )
        if code == -3:
            raise RuntimeError(
                f"Corrupt mirror publication state for {org}/{sup}/{simple}"
            )
        if code == -4:
            raise SnapshotCommitConflictError(
                f"Mirror publication owner or status changed for "
                f"{org}/{sup}/{simple}"
            )
        if code == -5:
            raise PermissionError(
                "Mirror recovery requires confirmation that the previous "
                "publisher has stopped"
            )
        if code in (-6, -7):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        raise RuntimeError(f"Invalid mirror owner claim result: {code}")

    def _transition_mirror_publication(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            commit_id: str,
            status: str,
            lock_token: str,
            failure_stage: str = "",
            error_type: str = "",
            error_message: str = "",
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        raw = self._mirror_publication_transition(
            keys=[
                RK.meta_mirror_publication(org, sup, simple),
                RK.lock_leaf(org, sup, simple),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent(org, sup, simple),
            ],
            args=[
                commit_id,
                status,
                _publication_timestamp(now_ms),
                lock_token,
                failure_stage,
                error_type,
                error_message[:4096],
            ],
        )
        code = int(raw or 0)
        if code in (1, 2):
            record = self.get_mirror_publication(org, sup, simple)
            if record is None:
                raise RuntimeError("Mirror publication state disappeared")
            return record
        if code == -1:
            raise SnapshotCommitConflictError(
                f"Mirror publication commit changed for {org}/{sup}/{simple}"
            )
        if code == -2:
            raise LockLostError(
                f"Lost fencing lock while updating mirror publication for "
                f"{org}/{sup}/{simple}"
            )
        if code == -3:
            raise RuntimeError(
                f"Corrupt mirror publication state for {org}/{sup}/{simple}"
            )
        if code in (-5, -6):
            raise DeletionIntentConflictError(
                f"Table has a durable deletion intent: {org}/{sup}/{simple}"
            )
        if code == -7:
            raise SnapshotCommitConflictError(
                f"Mirror publication is owned by another publisher for "
                f"{org}/{sup}/{simple}"
            )
        raise RuntimeError(
            f"Invalid mirror publication transition {status!r} from current state"
        )

    def complete_mirror_publication(
            self, org: str, sup: str, simple: str, *, commit_id: str,
            lock_token: str, now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self._transition_mirror_publication(
            org, sup, simple, commit_id=commit_id, status="complete",
            lock_token=lock_token, now_ms=now_ms,
        )

    def fail_mirror_publication(
            self, org: str, sup: str, simple: str, *, commit_id: str,
            lock_token: str, failure_stage: str, error: Exception,
            now_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self._transition_mirror_publication(
            org, sup, simple, commit_id=commit_id, status="failed",
            lock_token=lock_token, failure_stage=failure_stage,
            error_type=type(error).__name__, error_message=str(error),
            now_ms=now_ms,
        )

    # ------------- Replica resolution ------------------------------------

    @staticmethod
    def _replica_binding_from_root(
            root: Optional[Mapping[str, Any]], *, org: str, sup: str,
    ) -> Optional[tuple[str, Optional[frozenset[str]]]]:
        """Return the canonical one-hop replica binding in a valid root."""
        if not root or root.get("clone_type") != "replica":
            return None
        source = root.get("cloned_from")
        if not isinstance(source, str) or not source or source == sup:
            raise RuntimeError(
                f"Replica {org}/{sup} has an invalid source binding"
            )
        try:
            RK.meta_root(org, source)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Replica {org}/{sup} has an invalid source binding"
            ) from exc

        tables = root.get("replica_tables")
        if tables is None:
            # Historical full replicas use JSON null (or omit the field).
            allowed: Optional[frozenset[str]] = None
        elif isinstance(tables, list):
            normalized: set[str] = set()
            for table in tables:
                if not isinstance(table, str) or not table:
                    raise RuntimeError(
                        f"Replica {org}/{sup} has an invalid table allowlist"
                    )
                try:
                    RK.meta_leaf(org, source, table)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        f"Replica {org}/{sup} has an invalid table allowlist"
                    ) from exc
                normalized.add(table)
            # An explicit empty list is an empty subset, never an implicit
            # authorization widening to every table in the source.
            allowed = frozenset(normalized)
        else:
            raise RuntimeError(
                f"Replica {org}/{sup} has an invalid table allowlist"
            )
        return source, allowed

    @staticmethod
    def _decode_root_snapshot(
            raw: Any, *, org: str, sup: str,
    ) -> Dict[str, Any]:
        try:
            root = json.loads(raw)
            return _validate_root_document(root, org=org, sup=sup)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}") from exc

    def _resolve_replica_info(self, org: str, sup: str) -> Optional[tuple]:
        """If *sup* is a replica clone, return (source_name, allowed_tables).

        Returns None for non-replicas.  Never follows chains (if source
        is itself a replica, we stop — one level only).
        Single get_root() call — avoids redundant Redis reads.
        """
        root = self.get_root(org, sup)
        try:
            target_intent = self.r.get(
                RK.meta_namespace_deletion_intent(org, sup)
            )
        except redis.RedisError as exc:
            logger.error("[redis-catalog] replica target lifecycle read error: %s", exc)
            raise
        if target_intent:
            raise DeletionIntentConflictError(
                f"Durable deletion intent fences {org}/{sup}"
            )

        binding = self._replica_binding_from_root(root, org=org, sup=sup)
        if binding is None:
            return None
        source, allowed = binding

        # A replica is a live one-hop reference, not permission to consume
        # orphaned source leaves.  Read the source deletion fence before the
        # root so this operation linearizes before or after deletion begins.
        try:
            source_intent = self.r.get(
                RK.meta_namespace_deletion_intent(org, source)
            )
            source_raw = self.r.get(RK.meta_root(org, source))
        except redis.RedisError as exc:
            logger.error("[redis-catalog] replica source lifecycle read error: %s", exc)
            raise
        if source_intent:
            raise DeletionIntentConflictError(
                f"Replica source is fenced for deletion: {org}/{source}"
            )
        if not source_raw:
            raise RuntimeError(
                f"Replica {org}/{sup} refers to a missing source namespace"
            )
        source_root = self._decode_root_snapshot(
            source_raw, org=org, sup=source,
        )
        if source_root.get("clone_type") == "replica":
            raise RuntimeError(
                f"Replica {org}/{sup} cannot reference another replica"
            )
        return source, allowed

    # Keep backward-compatible wrappers for gc.py and other callers
    def _resolve_replica_source(self, org: str, sup: str) -> Optional[str]:
        info = self._resolve_replica_info(org, sup)
        return info[0] if info else None

    # ------------- Leaf access (with replica resolution) ------------------

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        info = self._resolve_replica_info(org, sup)
        if info:
            source, allowed = info
            if allowed is not None and simple not in allowed:
                return None
            return self._get_replica_leaf_atomic(
                org, sup, simple, info=info,
            )
        return self._get_leaf_raw(org, sup, simple)

    def _get_replica_leaf_atomic(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            info: tuple[str, Optional[frozenset[str]]],
    ) -> Optional[Dict[str, Any]]:
        """Read one replica leaf at a single target/source lifecycle point."""
        source, _allowed = info
        try:
            target_raw = self.r.get(RK.meta_root(org, sup))
            if not target_raw:
                raise RuntimeError(
                    f"Replica target disappeared while reading {org}/{sup}"
                )
            target_root = self._decode_root_snapshot(
                target_raw, org=org, sup=sup,
            )
            if self._replica_binding_from_root(
                    target_root, org=org, sup=sup,
            ) != info:
                raise SnapshotCommitConflictError(
                    f"Replica binding changed while reading {org}/{sup}"
                )
            result = self._get_replica_leaf(
                keys=[
                    RK.meta_root(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, source),
                    RK.meta_namespace_deletion_intent(org, source),
                    RK.meta_leaf(org, source, simple),
                ],
                args=[target_raw, sup, source],
            )
        except redis.RedisError as exc:
            logger.error("[redis-catalog] atomic replica leaf read error: %s", exc)
            raise
        if not isinstance(result, (list, tuple)) or not result:
            raise RuntimeError(f"Invalid replica leaf read result: {result!r}")
        code = int(result[0])
        if code == 0:
            return None
        if code == -1:
            raise SnapshotCommitConflictError(
                f"Replica binding changed while reading {org}/{sup}"
            )
        if code == -2:
            raise DeletionIntentConflictError(
                f"Durable deletion intent fences {org}/{sup}"
            )
        if code == -3:
            raise DeletionIntentConflictError(
                f"Replica source is fenced for deletion: {org}/{source}"
            )
        if code == -4:
            raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
        if code == -5:
            raise RuntimeError(
                f"Replica {org}/{sup} refers to a missing source namespace"
            )
        if code == -6:
            raise RuntimeError(
                f"Replica {org}/{sup} refers to an invalid source namespace"
            )
        if code != 1 or len(result) != 2:
            raise RuntimeError(f"Invalid replica leaf read result: {result!r}")
        try:
            return _validate_leaf_document(json.loads(result[1]))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Corrupt Redis leaf JSON for {org}/{source}/{simple}"
            ) from exc

    def _get_leaf_raw(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        try:
            raw = self.r.get(RK.meta_leaf(org, sup, simple))
            if not raw:
                return None
            return _validate_leaf_document(json.loads(raw))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_leaf error: {e}")
            raise
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
            ) from exc

    def reserve_rowids(self, org: str, sup: str, simple: str, count: int) -> int:
        """Reject the retired, lifecycle-unfenced row-id allocator.

        A caller must pin the immutable snapshot floor and use
        :meth:`reserve_rowids_at_least`, which also requires the exact live
        table lease and validates the parent/leaf lifecycle in one Lua command.
        Retaining the method as an explicit failure gives older integrations a
        deterministic migration error instead of silently recreating state.
        """
        if count <= 0:
            return 0
        raise RuntimeError(
            "reserve_rowids is retired; use the lock- and floor-fenced "
            "reserve_rowids_at_least API"
        )

    def reserve_rowids_at_least(
            self,
            org: str,
            sup: str,
            simple: str,
            count: int,
            floor: int,
            *,
            lock_token: str,
    ) -> tuple[int, int]:
        """Reserve IDs strictly above a snapshot-persisted high-water mark.

        Redis is the fast allocator, while ``floor`` is the durable recovery
        boundary stored in the immutable table snapshot. If Redis was flushed
        or restored from an older backup, this atomic max-and-increment prevents
        reusing identifiers that existing Parquet rows may still carry. The
        reservation is atomically fenced by the exact live leaf lease, both
        deletion intents, and valid parent/leaf catalog documents.
        Returns ``(first_reserved, new_high_watermark)``.
        """
        if type(count) is not int or type(floor) is not int:
            raise TypeError("rowid count and floor must be integers")
        if count <= 0:
            safe_floor = max(0, floor)
            return 0, safe_floor
        safe_floor = max(0, floor)
        int64_max = (1 << 63) - 1
        if safe_floor > int64_max:
            raise OverflowError("rowid high-watermark exceeds signed Int64")
        if count > int64_max - safe_floor:
            raise OverflowError("rowid reservation exceeds signed Int64")
        raw = self._reserve_rowids_at_least(
            keys=[
                RK.meta_rowid_seq(org, sup, simple),
                RK.lock_leaf(org, sup, simple),
                RK.meta_leaf(org, sup, simple),
                RK.meta_root(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent(org, sup, simple),
            ],
            args=[str(safe_floor), str(count), lock_token or ""],
        )
        if isinstance(raw, (list, tuple)) and len(raw) == 1:
            status = int(raw[0])
            if status == -1:
                raise LockLostError("Lost table lock before rowid reservation")
            if status in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Deletion intent blocks rowid reservation for "
                    f"{org}/{sup}/{simple}"
                )
            if status == -4:
                raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
            if status == -5:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if status == -6:
                raise FileNotFoundError(
                    f"SimpleTable does not exist: {org}/{sup}/{simple}"
                )
            if status == -7:
                raise RuntimeError(
                    f"Corrupt Redis leaf JSON for {org}/{sup}/{simple}"
                )
            if status == -8:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            raise RuntimeError(f"Invalid rowid reservation status: {status}")
        try:
            previous, new_high = [int(v) for v in raw]
            start = previous + 1
        except Exception as exc:
            raise RuntimeError(f"Invalid rowid reservation result: {raw!r}") from exc
        if new_high > int64_max or new_high != previous + count:
            raise RuntimeError(f"Unsafe rowid reservation result: {raw!r}")
        return start, new_high

    def delete_leaf(self, org: str, sup: str, simple: str) -> bool:
        """Delete a leaf pointer (used when unlinking shared tables)."""
        try:
            return bool(self.r.delete(RK.meta_leaf(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_leaf error: {e}")
            return False

    def set_leaf_path_cas(
            self, org: str, sup: str, simple: str, path: str,
            now_ms: Optional[int] = None, *, namespace_token: str = "",
    ) -> int:
        timestamp = _publication_timestamp(now_ms)
        try:
            result = int(
                self._leaf_cas_set(
                    keys=[
                        RK.meta_leaf(org, sup, simple),
                        RK.lock_namespace(org, sup),
                        RK.meta_table_names(org, sup),
                        RK.meta_namespace_deletion_intent(org, sup),
                        RK.meta_simple_deletion_intent(org, sup, simple),
                        RK.meta_root(org, sup),
                    ],
                    args=[
                        path,
                        timestamp,
                        namespace_token or "",
                        simple,
                    ],
                ) or 0
            )
            if result == -2:
                raise RuntimeError(
                    f"SuperTable namespace is fenced for deletion: {org}/{sup}"
                )
            if result in (-3, -4):
                raise DeletionIntentConflictError(
                    f"Table has a durable deletion intent: {org}/{sup}/{simple}"
                )
            if result == -5:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -6:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -7:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result == -8:
                raise ValueError("Leaf timestamp is outside Redis Lua's exact range")
            if result < 0:
                raise SnapshotCommitConflictError(
                    f"Cannot initialize existing table {org}/{sup}/{simple}"
                )
            return result
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_cas_set error: {e}")
            raise

    def set_leaf_payload_cas(
            self,
            org: str,
            sup: str,
            simple: str,
            payload: Dict[str, Any],
            path: str,
            now_ms: Optional[int] = None,
            *,
            namespace_token: str = "",
            not_after_ms: Optional[int] = None,
    ) -> int:
        """Atomically write a leaf pointer *and* snapshot payload (so readers avoid storage reads)."""
        timestamp = _publication_timestamp(now_ms)
        publication_deadline = (
            0 if not_after_ms is None else _publication_timestamp(not_after_ms)
        )
        try:
            payload_json = json.dumps(snapshot_cache_payload(payload))
        except Exception as exc:
            raise ValueError("snapshot payload is not JSON serializable") from exc

        try:
            result = int(
                self._leaf_payload_cas_set(
                    keys=[
                        RK.meta_leaf(org, sup, simple),
                        RK.lock_namespace(org, sup),
                        RK.meta_table_names(org, sup),
                        RK.meta_namespace_deletion_intent(org, sup),
                        RK.meta_simple_deletion_intent(org, sup, simple),
                        RK.meta_root(org, sup),
                    ],
                    args=[
                        payload_json,
                        path,
                        timestamp,
                        namespace_token or "",
                        simple,
                        publication_deadline,
                    ],
                )
                or 0
            )
            if result == -2:
                raise RuntimeError(
                    f"SuperTable namespace is fenced for deletion: {org}/{sup}"
                )
            if result in (-3, -4):
                raise DeletionIntentConflictError(
                    f"Table has a durable deletion intent: {org}/{sup}/{simple}"
                )
            if result == -5:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -6:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -7:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result == -8:
                raise ValueError("Leaf timestamp is outside Redis Lua's exact range")
            if result == -9:
                raise TimeoutError("Leaf publication deadline was exceeded")
            if result < 0:
                raise SnapshotCommitConflictError(
                    f"Cannot initialize existing table {org}/{sup}/{simple}"
                )
            return result
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_payload_cas_set error: {e}")
            raise

    @staticmethod
    def _linked_publication_generation_key(
        org: str, sup: str, link_id: str,
    ) -> str:
        return RK.linked_share_doc(org, sup, link_id) + ":publication_generation"

    @staticmethod
    def _linked_provider_reservation_key(
        org: str, sup: str, link_id: str,
    ) -> str:
        return RK.linked_share_doc(org, sup, link_id) + ":provider_publication"

    @staticmethod
    def _linked_leaf_names_key(org: str, sup: str, link_id: str) -> str:
        return RK.linked_share_doc(org, sup, link_id) + ":leaf_names"

    @staticmethod
    def _linked_unlink_tombstone_key(
        org: str, sup: str, link_id: str,
    ) -> str:
        return RK.linked_share_doc(org, sup, link_id) + ":unlinked"

    def allocate_linked_share_publication_generation(
        self, org: str, sup: str, link_id: str,
    ) -> tuple[int, int]:
        """Allocate a Redis-clock-ordered generation and return server ms."""
        try:
            raw = self._allocate_linked_publication(
                keys=[self._linked_publication_generation_key(org, sup, link_id)],
                args=[],
            )
            if not isinstance(raw, (list, tuple)) or len(raw) != 2:
                raise RuntimeError("Invalid linked publication generation result")
            generation, server_ms = int(raw[0]), int(raw[1])
            if generation <= 0 or server_ms < 0:
                raise RuntimeError("Invalid linked publication generation state")
            return generation, server_ms
        except redis.RedisError as exc:
            logger.error(
                "[redis-catalog] linked publication generation error: %s",
                exc,
            )
            raise

    def reserve_linked_provider_publication(
        self,
        org: str,
        sup: str,
        link_id: str,
        *,
        provider_generated_ms: int,
        manifest_digest: str,
        publication_generation: int,
        not_after_ms: int,
        instance_nonce: str,
    ) -> bool:
        """Reserve one provider-ordered publication before any leaf mutation.

        ``False`` means the exact provider manifest is already fully committed.
        A lower provider timestamp or equal timestamp with different bytes is a
        conflict, independent of local response completion order.
        """
        provider_generation = _lua_safe_integer(
            provider_generated_ms,
            field="provider manifest generation",
            minimum=1,
        )
        local_generation = _lua_safe_integer(
            publication_generation,
            field="linked publication generation",
            minimum=1,
        )
        deadline = _lua_safe_integer(
            not_after_ms,
            field="linked publication deadline",
            minimum=1,
        )
        if not isinstance(manifest_digest, str) or re.fullmatch(
            r"[0-9a-f]{64}", manifest_digest,
        ) is None:
            raise ValueError("provider manifest digest is invalid")
        if not isinstance(instance_nonce, str) or re.fullmatch(
            r"link-instance-v1:[0-9a-f]{64}", instance_nonce,
        ) is None:
            raise ValueError("linked-share instance nonce is invalid")
        result = int(self._reserve_linked_provider_publication(
            keys=[
                self._linked_provider_reservation_key(org, sup, link_id),
                self._linked_unlink_tombstone_key(org, sup, link_id),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_root(org, sup),
            ],
            args=[
                provider_generation,
                manifest_digest,
                local_generation,
                deadline,
                instance_nonce,
            ],
        ) or 0)
        if result == 0:
            return False
        if result == -1:
            raise FileNotFoundError("Linked share is unlinked")
        if result == -2:
            raise DeletionIntentConflictError(
                f"Catalog mutation is fenced: {org}/{sup}"
            )
        if result == -3:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result in (-4, -6):
            raise RuntimeError(f"Corrupt linked publication state: {org}/{sup}")
        if result == -5:
            raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
        if result == -8:
            raise ValueError("Linked provider publication metadata is invalid")
        if result == -9:
            raise TimeoutError("Linked publication deadline was exceeded")
        if result == -10:
            raise SnapshotCommitConflictError(
                "A newer provider manifest publication already started"
            )
        if result == -11:
            raise SnapshotCommitConflictError(
                "Provider manifest generation is ambiguous"
            )
        if result == -12:
            raise SnapshotCommitConflictError(
                "Linked-share instance identity changed"
            )
        if result != 1:
            raise RuntimeError(f"Invalid linked publication reservation: {result}")
        return True

    def abort_linked_provider_publication(
        self,
        org: str,
        sup: str,
        link_id: str,
        *,
        instance_nonce: str,
    ) -> bool:
        """Durably fence and detach one failed publication owned by the caller.

        The immutable link-instance nonce lets an ambiguous initial-create
        caller abort its own link even if a newer refresh advanced provider or
        local generations before the error response arrived. It cannot detach
        a different incarnation that happens to reuse the same link id.
        """
        if not isinstance(instance_nonce, str) or re.fullmatch(
            r"link-instance-v1:[0-9a-f]{64}", instance_nonce,
        ) is None:
            raise ValueError("linked-share instance nonce is invalid")
        result = int(self._abort_linked_provider_publication(
            keys=[
                RK.linked_share_doc(org, sup, link_id),
                RK.linked_share_index(org, sup),
                self._linked_provider_reservation_key(org, sup, link_id),
                self._linked_unlink_tombstone_key(org, sup, link_id),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_root(org, sup),
            ],
            args=[
                link_id,
                instance_nonce,
            ],
        ) or 0)
        if result in (1, 2):
            return True
        if result == -1:
            raise DeletionIntentConflictError(
                f"Catalog mutation is fenced: {org}/{sup}"
            )
        if result == -2:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result in (-3, -5):
            raise RuntimeError(f"Corrupt linked publication state: {org}/{sup}")
        if result == -4:
            raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
        if result == -6:
            raise SnapshotCommitConflictError(
                "Linked publication ownership changed before abort"
            )
        if result == -8:
            raise ValueError("Linked provider publication metadata is invalid")
        raise RuntimeError(f"Invalid linked publication abort result: {result}")

    def list_linked_share_leaf_names(
        self, org: str, sup: str, link_id: str, *, limit: int = 10_000,
    ) -> List[str]:
        """Return the bounded per-link leaf index used for batch repair."""
        return self._bounded_set_members(
            self._linked_leaf_names_key(org, sup, link_id),
            limit=limit,
            description=f"linked leaf index for {org}/{sup}/{link_id}",
        )

    def scan_linked_share_leaf_names(
        self,
        org: str,
        sup: str,
        link_id: str,
        *,
        cursor: int = 0,
        count: int = 256,
    ) -> tuple[int, List[str]]:
        """Read one bounded cleanup page without materializing the full set.

        Cleanup callers hold a durable unlink tombstone or provider
        reservation and may safely restart at cursor zero after deletions.
        This API is deliberately not an authoritative snapshot API.
        """
        safe_cursor = _lua_safe_integer(
            cursor, field="linked leaf scan cursor", minimum=0,
        )
        safe_count = _lua_safe_integer(
            count, field="linked leaf scan count", minimum=1,
        )
        if safe_count > 1024:
            raise ValueError("linked leaf scan count exceeds its safety limit")
        try:
            raw_cursor, raw_members = self.r.sscan(
                self._linked_leaf_names_key(org, sup, link_id),
                cursor=safe_cursor,
                count=safe_count,
            )
            next_cursor = int(raw_cursor)
            if next_cursor < 0 or next_cursor > (1 << 64) - 1:
                raise RuntimeError("Corrupt linked leaf scan cursor")
            if not isinstance(raw_members, (list, tuple, set)):
                raise RuntimeError("Corrupt linked leaf scan page")
            if len(raw_members) > safe_count * 8 + 64:
                raise RuntimeError("Linked leaf scan page exceeds its safety limit")
            decoded = {
                self._decode_index_member(
                    value,
                    description=f"linked leaf index for {org}/{sup}/{link_id}",
                )
                for value in raw_members
            }
            return next_cursor, sorted(decoded)
        except redis.RedisError as exc:
            logger.error("[redis-catalog] linked leaf scan failed: %s", exc)
            raise

    def upsert_linked_leaf(
        self,
        org: str,
        sup: str,
        simple: str,
        payload: Dict[str, Any],
        path: str,
        *,
        link_id: str,
        generation: int,
        not_after_ms: int,
    ) -> bool:
        """Create/update only this link's leaf, ordered and deadline-fenced."""
        safe_generation = _publication_timestamp(generation)
        publication_deadline = _publication_timestamp(not_after_ms)
        if safe_generation <= 0 or publication_deadline <= 0:
            raise ValueError("Linked publication generation/deadline is invalid")
        try:
            payload_json = json.dumps(snapshot_cache_payload(payload))
        except Exception as exc:
            raise ValueError("snapshot payload is not JSON serializable") from exc
        try:
            result = int(self._upsert_linked_leaf(
                keys=[
                    RK.meta_leaf(org, sup, simple),
                    RK.lock_namespace(org, sup),
                    RK.meta_table_names(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_simple_deletion_intent(org, sup, simple),
                    RK.meta_root(org, sup),
                    self._linked_provider_reservation_key(org, sup, link_id),
                    self._linked_leaf_names_key(org, sup, link_id),
                    self._linked_unlink_tombstone_key(org, sup, link_id),
                ],
                args=[
                    payload_json,
                    path,
                    simple,
                    link_id,
                    safe_generation,
                    publication_deadline,
                ],
            ) or 0)
            if result == -1:
                raise FileExistsError(
                    f"Local table blocks linked leaf: {org}/{sup}/{simple}"
                )
            if result == -2:
                raise FileExistsError(
                    f"Another linked share owns table: {org}/{sup}/{simple}"
                )
            if result in (-3, -4, -5):
                raise DeletionIntentConflictError(
                    f"Catalog mutation is fenced: {org}/{sup}/{simple}"
                )
            if result == -6:
                raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
            if result in (-7, -9, -11):
                raise RuntimeError(f"Corrupt linked leaf catalog state: {org}/{sup}")
            if result == -8:
                raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
            if result in (-10, -12):
                raise ValueError("Linked leaf publication metadata is invalid")
            if result == -13:
                raise TimeoutError("Linked leaf publication deadline was exceeded")
            if result == -14:
                raise SnapshotCommitConflictError(
                    "A newer linked leaf publication already committed"
                )
            if result == -15:
                raise SnapshotCommitConflictError(
                    "A newer provider manifest leaf already committed"
                )
            if result == -16:
                raise SnapshotCommitConflictError(
                    "Provider manifest generation is ambiguous"
                )
            if result == -17:
                raise FileNotFoundError("Linked share is unlinked")
            if result == -18:
                raise SnapshotCommitConflictError(
                    "Linked provider publication reservation changed"
                )
            if result not in (1, 2):
                raise RuntimeError(f"Invalid linked leaf publication result: {result}")
            return result == 1
        except redis.RedisError as exc:
            logger.error("[redis-catalog] linked leaf upsert error: %s", exc)
            raise

    def delete_linked_leaf_if_generation(
        self,
        org: str,
        sup: str,
        simple: str,
        *,
        link_id: str,
        expected_generation: int,
        not_after_ms: Optional[int] = None,
    ) -> bool:
        """Delete exactly one observed link generation; never a replacement."""
        generation = _publication_timestamp(expected_generation)
        publication_deadline = (
            0 if not_after_ms is None else _publication_timestamp(not_after_ms)
        )
        try:
            result = int(self._delete_linked_leaf(
                keys=[
                    RK.meta_leaf(org, sup, simple),
                    RK.meta_table_names(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                    self._linked_leaf_names_key(org, sup, link_id),
                ],
                args=[
                    simple,
                    link_id,
                    generation,
                    publication_deadline,
                ],
            ) or 0)
            if result in (-1, -2):
                return False
            if result == -3:
                raise DeletionIntentConflictError(
                    f"Catalog mutation is fenced: {org}/{sup}/{simple}"
                )
            if result == -4:
                raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
            if result in (-5, -7):
                raise RuntimeError(f"Corrupt linked leaf catalog state: {org}/{sup}")
            if result == -6:
                raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
            if result == -8:
                raise ValueError("Linked leaf generation is invalid")
            if result == -9:
                raise TimeoutError("Linked leaf deletion deadline was exceeded")
            if result not in (0, 1):
                raise RuntimeError(f"Invalid linked leaf deletion result: {result}")
            return result == 1
        except redis.RedisError as exc:
            logger.error("[redis-catalog] linked leaf deletion error: %s", exc)
            raise

    def delete_stale_linked_leaf(
        self,
        org: str,
        sup: str,
        simple: str,
        *,
        link_id: str,
        provider_generated_ms: int,
        manifest_digest: str,
        publication_generation: int,
        not_after_ms: int,
    ) -> bool:
        """Remove an indexed leaf older than the live publication reservation."""
        provider_generation = _lua_safe_integer(
            provider_generated_ms,
            field="provider manifest generation",
            minimum=1,
        )
        local_generation = _lua_safe_integer(
            publication_generation,
            field="linked publication generation",
            minimum=1,
        )
        deadline = _lua_safe_integer(
            not_after_ms,
            field="linked publication deadline",
            minimum=1,
        )
        if not isinstance(manifest_digest, str) or re.fullmatch(
            r"[0-9a-f]{64}", manifest_digest,
        ) is None:
            raise ValueError("provider manifest digest is invalid")
        result = int(self._delete_stale_linked_leaf(
            keys=[
                RK.meta_leaf(org, sup, simple),
                RK.meta_table_names(org, sup),
                self._linked_leaf_names_key(org, sup, link_id),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_root(org, sup),
                self._linked_provider_reservation_key(org, sup, link_id),
                self._linked_unlink_tombstone_key(org, sup, link_id),
            ],
            args=[
                simple,
                link_id,
                provider_generation,
                manifest_digest,
                local_generation,
                deadline,
            ],
        ) or 0)
        if result in (-1, 0):
            return False
        if result == -3:
            raise DeletionIntentConflictError(
                f"Catalog mutation is fenced: {org}/{sup}/{simple}"
            )
        if result == -4:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result in (-5, -7):
            raise RuntimeError(f"Corrupt linked leaf catalog state: {org}/{sup}")
        if result == -6:
            raise ReadOnlyCatalogError(f"SuperTable is read-only: {org}/{sup}")
        if result == -8:
            raise ValueError("Linked provider publication metadata is invalid")
        if result == -9:
            raise TimeoutError("Linked leaf deletion deadline was exceeded")
        if result == -10:
            raise FileNotFoundError("Linked share is unlinked")
        if result == -11:
            raise SnapshotCommitConflictError(
                "Linked provider publication reservation changed"
            )
        if result != 1:
            raise RuntimeError(f"Invalid stale linked leaf deletion: {result}")
        return True

    def delete_unlinked_leaf(
        self,
        org: str,
        sup: str,
        simple: str,
        *,
        link_id: str,
    ) -> bool:
        """Delete one same-link leaf only after a durable unlink tombstone."""
        result = int(self._delete_unlinked_leaf(
            keys=[
                RK.meta_leaf(org, sup, simple),
                RK.meta_table_names(org, sup),
                self._linked_leaf_names_key(org, sup, link_id),
                self._linked_unlink_tombstone_key(org, sup, link_id),
                RK.meta_root(org, sup),
            ],
            args=[simple, link_id],
        ) or 0)
        if result == 0:
            return False
        if result == -1:
            raise RuntimeError("Linked unlink tombstone is invalid")
        if result == -2:
            raise FileNotFoundError(f"SuperTable does not exist: {org}/{sup}")
        if result in (-3, -4):
            raise RuntimeError(f"Corrupt linked leaf catalog state: {org}/{sup}")
        if result == -5:
            return False
        if result != 1:
            raise RuntimeError(f"Invalid unlinked leaf deletion: {result}")
        return True

    # ------------- Mirror formats (Redis-backed) -------------

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        """Read enabled mirror formats from Redis key."""
        try:
            raw = self.r.get(RK.meta_mirrors(org, sup))
            if not raw:
                return []
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise ValueError("Mirror configuration must be a JSON object")
            if (
                type(obj.get("ts")) is not int
                or obj["ts"] < 0
                or obj["ts"] > _REDIS_LUA_MAX_SAFE_INTEGER
            ):
                raise ValueError("Mirror configuration timestamp is invalid")
            formats = obj.get("formats")
            if not isinstance(formats, list):
                raise ValueError("Mirror configuration formats must be a list")
            seen = set()
            out: List[str] = []
            for f in (formats or []):
                fu = str(f).upper()
                if fu not in ("DELTA", "ICEBERG", "PARQUET"):
                    raise ValueError(f"Unsupported configured mirror format: {f!r}")
                if fu in seen:
                    raise ValueError("Mirror configuration contains duplicates")
                seen.add(fu)
                out.append(fu)
            return out
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_mirrors error: {e}")
            # Mirror state is part of the write's correctness decision: an
            # enabled latest-only mirror must force the deletion vector to be
            # physically drained before resources are copied.  Treating an
            # unavailable/corrupt setting as "disabled" could publish a stale
            # mirror that resurrects deleted rows.
            raise
        except Exception as e:
            logger.error(f"[redis-catalog] invalid mirror configuration: {e}")
            raise

    def set_mirrors(self, org: str, sup: str, formats: List[str], now_ms: Optional[int] = None) -> List[str]:
        """Atomically set enabled mirror formats."""
        seen = set()
        ordered: List[str] = []
        for f in formats or []:
            fu = str(f).upper()
            if fu not in ("DELTA", "ICEBERG", "PARQUET"):
                raise ValueError(f"Unsupported mirror format: {f!r}")
            if fu not in seen:
                seen.add(fu)
                ordered.append(fu)
        try:
            payload = {
                "formats": ordered,
                "ts": _publication_timestamp(now_ms),
            }
            result = int(self._set_mirrors_fenced(
                keys=[
                    RK.meta_mirrors(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                ],
                args=[json.dumps(payload)],
            ) or 0)
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise ValueError("Mirror configuration is not valid JSON")
            if result == -3:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -4:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -5:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(f"Invalid mirror configuration result: {result}")
            return ordered
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_mirrors error: {e}")
            raise

    def enable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        return self._mutate_mirror(org, sup, fmt, enable=True)

    def disable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        return self._mutate_mirror(org, sup, fmt, enable=False)

    def _mutate_mirror(
            self,
            org: str,
            sup: str,
            fmt: str,
            *,
            enable: bool,
    ) -> List[str]:
        """Atomically add or remove one mirror format.

        The Lua boundary validates the namespace root and durable deletion
        fence before reading or replacing the configuration. Concurrent
        acknowledged mutations therefore compose without a client-side
        read/replace race, while stale handles cannot recreate mirror state
        after namespace deletion.
        """
        fu = str(fmt).upper()
        if fu not in ("DELTA", "ICEBERG", "PARQUET"):
            raise ValueError(f"Unsupported mirror format: {fmt!r}")
        try:
            result = self._mutate_mirror_fenced(
                keys=[
                    RK.meta_mirrors(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                ],
                args=[fu, "1" if enable else "0", _now_ms()],
            )
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -3:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -4:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -5:
                raise ValueError("Persisted mirror configuration is invalid")
            if result == -6:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if not isinstance(result, (list, tuple)):
                raise RuntimeError(
                    f"Invalid mirror configuration result: {result!r}"
                )
            return [self._decode_member(value) for value in result]
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] mutate_mirror error: {exc}")
            raise

    # ------------- User and Role Management (RBAC, UUID-based) ------------- #

    @staticmethod
    def _decode_member(m) -> str:
        return m if isinstance(m, str) else m.decode("utf-8")

    @classmethod
    def _decode_user_roles(cls, value: Any) -> List[str]:
        """Decode persisted user roles without treating corruption as no roles."""
        value = cls._rbac_text(value)
        try:
            roles = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise RbacIntegrityError(
                "Persisted user roles are not valid JSON"
            ) from exc
        if not isinstance(roles, list) or not all(
            isinstance(role_id, str) and role_id
            for role_id in roles
        ):
            raise RbacIntegrityError(
                "Persisted user roles must be a list of non-empty role IDs"
            )
        return roles

    @classmethod
    def _canonical_persisted_role(
        cls, raw: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Canonicalize one stored role and verify its persisted digest."""
        try:
            canonical = _canonicalize_role_document(
                dict(raw), default_if_empty=False,
            )
        except (TypeError, ValueError) as exc:
            raise RbacIntegrityError(
                "Persisted role document is invalid"
            ) from exc
        stored_hash = cls._rbac_text(raw.get("content_hash"))
        if stored_hash and stored_hash != canonical["content_hash"]:
            raise RbacIntegrityError(
                "Persisted role content hash does not match its policy"
            )
        return canonical

    def get_users(self, org: str, sup: str) -> List[Dict[str, Any]]:
        """Get all users for organization (pipeline batch, not N+1 reads)."""
        users: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(RK.rbac_user_index(org, sup))
            if not members:
                return users
            uids = [self._decode_member(m) for m in members]
            # Pipeline: batch HGETALL for all user docs
            with self.r.pipeline() as pipe:
                for uid in uids:
                    pipe.hgetall(RK.rbac_user_doc(org, sup, uid))
                results = pipe.execute()
            if len(results) != len(uids):
                raise RbacIntegrityError(
                    "RBAC user listing returned an incomplete document set"
                )
            for uid, raw in zip(uids, results):
                if not raw:
                    raise RbacIntegrityError(
                        "RBAC user index references a missing document"
                    )
                data: Dict[str, Any] = dict(raw)
                stored_id = self._rbac_text(data.get("user_id"))
                if stored_id and stored_id != uid:
                    raise RbacIntegrityError(
                        "Persisted user identity does not match its index"
                    )
                data.setdefault("user_id", uid)
                try:
                    validate_username(self._rbac_text(data.get("username")))
                except (TypeError, ValueError) as exc:
                    raise RbacIntegrityError(
                        "Persisted username is invalid"
                    ) from exc
                if "roles" not in data:
                    raise RbacIntegrityError(
                        "Persisted user document is missing roles"
                    )
                data["roles"] = self._decode_user_roles(data["roles"])
                users.append(data)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_users error: {e}")
            raise
        return users

    def get_roles(self, org: str, sup: str) -> List[Dict[str, Any]]:
        """Get all roles for organization (pipeline batch, not N+1 reads)."""
        roles: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(RK.rbac_role_index(org, sup))
            if not members:
                return roles
            rids = [self._decode_member(m) for m in members]
            # Pipeline: batch HGETALL for all role docs
            with self.r.pipeline() as pipe:
                for rid in rids:
                    pipe.hgetall(RK.rbac_role_doc(org, sup, rid))
                results = pipe.execute()
            if len(results) != len(rids):
                raise RbacIntegrityError(
                    "RBAC role listing returned an incomplete document set"
                )
            for rid, raw in zip(rids, results):
                if not raw:
                    raise RbacIntegrityError(
                        "RBAC role index references a missing document"
                    )
                data = self._canonical_persisted_role(raw)
                stored_id = self._rbac_text(data.get("role_id"))
                if stored_id and stored_id != rid:
                    raise RbacIntegrityError(
                        "Persisted role identity does not match its index"
                    )
                data.setdefault("role_id", rid)
                roles.append(data)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_roles error: {e}")
            raise
        return roles

    def get_role_details(self, org: str, sup: str, role_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed role information by role_id."""
        try:
            raw = self.r.hgetall(RK.rbac_role_doc(org, sup, role_id))
            if not raw:
                return None
            data = self._canonical_persisted_role(raw)
            data.setdefault("role_id", role_id)
            return data
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_role_details error: {e}")
            raise

    def get_user_details(self, org: str, sup: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed user information by user_id."""
        try:
            raw = self.r.hgetall(RK.rbac_user_doc(org, sup, user_id))
            if not raw:
                return None
            data: Dict[str, Any] = dict(raw)
            stored_id = self._rbac_text(data.get("user_id"))
            if stored_id and stored_id != user_id:
                raise RbacIntegrityError(
                    "Persisted user identity does not match its storage key"
                )
            data.setdefault("user_id", user_id)
            try:
                validate_username(self._rbac_text(data.get("username")))
            except (TypeError, ValueError) as exc:
                raise RbacIntegrityError(
                    "Persisted username is invalid"
                ) from exc
            if "roles" not in data:
                raise RbacIntegrityError(
                    "Persisted user document is missing roles"
                )
            data["roles"] = self._decode_user_roles(data["roles"])
            return data
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_user_details error: {e}")
            raise

    # ------------- RBAC write operations ------------- #

    @staticmethod
    def _rbac_serialize(value: Any) -> str:
        """Convert a Python value to a Redis-safe string for RBAC storage."""
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    @staticmethod
    def _rbac_text(value: Any) -> str:
        """Return the exact text Redis uses for a CAS comparison."""
        if value is None:
            return ""
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("RBAC document contains invalid UTF-8") from exc
        return str(value)

    @classmethod
    def _rbac_doc_version(cls, raw: Dict[str, Any]) -> str:
        """Return a validated Redis integer version; legacy documents are 0."""
        value = cls._rbac_text(raw.get("doc_version"))
        if value == "":
            return "0"
        if (
            not value.isdigit()
            or (value != "0" and value.startswith("0"))
            or int(value) > 9_223_372_036_854_775_806
        ):
            raise RbacIntegrityError(
                "Persisted RBAC document has an invalid doc_version"
            )
        return value

    @classmethod
    def _rbac_string_document(cls, raw: Mapping[str, Any]) -> Dict[str, str]:
        """Return the exact text representation of one Redis RBAC HASH.

        Privileged audit records retain only the canonical SHA-256 of this
        representation, never policy/filter contents.  Using Redis text here
        makes the before/after digests describe the exact document committed
        by the Lua script instead of a lossy business-object projection.
        """
        if not isinstance(raw, Mapping):
            raise RbacIntegrityError("Persisted RBAC document must be a mapping")
        document: Dict[str, str] = {}
        for key, value in raw.items():
            text_key = cls._rbac_text(key)
            if not text_key:
                raise RbacIntegrityError(
                    "Persisted RBAC document contains an empty field name"
                )
            document[text_key] = cls._rbac_text(value)
        return document

    @staticmethod
    def _rbac_audit_keys(org: str) -> List[str]:
        return [
            RK.audit_privileged_activation(org),
            RK.audit_privileged_outbox(org),
            RK.audit_privileged_meta(org),
        ]

    @staticmethod
    def _auth_token_meta_key(org: str) -> str:
        """Return the private revision key for the organization token HASH."""
        return f"{RK.auth_tokens(org)}:audit_meta"

    @staticmethod
    def _rbac_audit_json(
        *,
        action_context: Any,
        org: str,
        sup: str,
        action: str,
        resource_type: str,
        resource_id: str,
        before_document: Optional[Mapping[str, Any]],
        after_document: Optional[Mapping[str, Any]],
        before_version: int,
        after_version: int,
        changed_fields: Any = (),
        role_ids_added: Any = (),
        role_ids_removed: Any = (),
        timestamp_ms: Optional[int] = None,
        severity: str = "warning",
        outcome: str = "success",
        cause: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> str:
        """Build a bounded, digest-only record for the transactional WAL."""
        from supertable.audit.privileged import build_record

        record = build_record(
            context=action_context,
            organization=org,
            super_name=sup,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            before_document=before_document,
            after_document=after_document,
            before_version=before_version,
            after_version=after_version,
            changed_fields=changed_fields,
            role_ids_added=role_ids_added,
            role_ids_removed=role_ids_removed,
            timestamp_ms=timestamp_ms,
            severity=severity,
            outcome=outcome,
            cause=cause,
            reason=reason,
        )
        return record.to_json()

    @staticmethod
    def rbac_attempt_was_recorded(error: BaseException) -> bool:
        """Return whether ``error`` already has durable attempt evidence."""

        return bool(getattr(error, "_supertable_rbac_attempt_recorded", False))

    @staticmethod
    def _rbac_mark_attempt_recorded(error: BaseException) -> BaseException:
        try:
            setattr(error, "_supertable_rbac_attempt_recorded", True)
        except Exception:
            # Built-in RBAC exceptions currently allow attributes.  Keep the
            # marker defensive for custom exception implementations.
            pass
        return error

    @staticmethod
    def _rbac_condition_identifier(
        value: Any,
        label: str,
        *,
        allow_colon: bool = True,
    ) -> str:
        """Return one bounded identifier safe for a derived condition key."""

        if not isinstance(value, str) or not value:
            raise ValueError(f"RBAC attempt {label} is invalid")
        if len(value.encode("utf-8")) > 512 or any(
            ord(character) < 32 or ord(character) == 127
            for character in value
        ):
            raise ValueError(f"RBAC attempt {label} is invalid")
        if not allow_colon and ":" in value:
            raise ValueError(f"RBAC attempt {label} is invalid")
        return value

    @classmethod
    def _rbac_condition_version(cls, value: Any) -> str:
        version = cls._rbac_text(value)
        if (
            not version.isdigit()
            or (version != "0" and version.startswith("0"))
            or len(version) > 19
            or (
                len(version) == 19
                and version > "9223372036854775807"
            )
        ):
            raise ValueError("RBAC attempt condition version is invalid")
        return version

    @staticmethod
    def _rbac_require_condition_fields(
        condition: Mapping[str, Any],
        expected: set[str],
    ) -> None:
        if set(condition) != expected:
            raise ValueError("RBAC attempt condition fields are invalid")

    def _rbac_normalize_attempt_conditions(
        self,
        org: str,
        sup: str,
        *,
        action: str,
        resource_type: str,
        resource_id: str,
        cause: str,
        conditions: Sequence[Mapping[str, Any]],
    ) -> tuple[List[str], str]:
        """Compile keyless, action-bound predicates for the Lua append.

        The descriptors are intentionally semantic.  No caller-controlled
        Redis key, hash field name, or unrelated resource selector crosses
        this boundary.
        """

        if isinstance(conditions, (str, bytes)) or not isinstance(
            conditions, Sequence
        ):
            raise ValueError("RBAC attempt conditions must be a sequence")
        if not 1 <= len(conditions) <= self._RBAC_ATTEMPT_CONDITION_LIMIT:
            raise ValueError("RBAC attempt condition count is out of range")
        if any(not isinstance(condition, Mapping) for condition in conditions):
            raise ValueError("RBAC attempt condition must be an object")

        kinds = tuple(condition.get("kind") for condition in conditions)
        if not all(isinstance(kind, str) for kind in kinds):
            raise ValueError("RBAC attempt condition kind is invalid")
        allowed_shapes = _RBAC_NO_CHANGE_CONDITION_SHAPES.get((action, cause))
        if not allowed_shapes or kinds not in allowed_shapes:
            raise ValueError(
                "RBAC attempt conditions do not match the action and cause"
            )

        resource_id = self._rbac_condition_identifier(
            resource_id, "resource_id"
        )
        if resource_type == "role":
            resource_key = RK.rbac_role_doc(org, sup, resource_id)
        elif resource_type == "user":
            resource_key = RK.rbac_user_doc(org, sup, resource_id)
        else:
            resource_key = ""

        condition_keys: List[str] = []
        normalized_conditions: List[Dict[str, Any]] = []
        for condition, kind in zip(conditions, kinds):
            if kind in {"resource_absent", "resource_exists"}:
                self._rbac_require_condition_fields(condition, {"kind"})
                if not resource_key:
                    raise ValueError(
                        "RBAC resource condition has no derived document key"
                    )
                condition_keys.append(resource_key)
                normalized_conditions.append({
                    "kind": (
                        "absent" if kind == "resource_absent" else "exists"
                    ),
                })
                continue

            if kind == "identity_claim":
                self._rbac_require_condition_fields(
                    condition, {"kind", "name", "identity_id"}
                )
                name = condition["name"]
                identity_id = self._rbac_condition_identifier(
                    condition["identity_id"], "identity_id"
                )
                if resource_type == "role":
                    validate_role_name(name)
                    if not name:
                        raise ValueError("RBAC role identity name is invalid")
                    condition_key = RK.rbac_rolename_to_id(org, sup)
                elif resource_type == "user":
                    validate_username(name)
                    condition_key = RK.rbac_username_to_id(org, sup)
                else:
                    raise ValueError("RBAC identity condition is invalid")
                if cause == "idempotent_replay" and identity_id != resource_id:
                    raise ValueError(
                        "RBAC replay identity does not match the audited resource"
                    )
                condition_keys.append(condition_key)
                normalized_conditions.append({
                    "kind": "hash_fields",
                    "fields": [{
                        "name": name.casefold(),
                        "value": identity_id,
                    }],
                })
                continue

            if kind == "resource_fields":
                self._rbac_require_condition_fields(
                    condition, {"kind", "fields"}
                )
                fields = condition["fields"]
                if not isinstance(fields, Mapping):
                    raise ValueError("RBAC resource condition fields are invalid")
                if action == "role_create":
                    expected_fields = {"role", "content_hash", "doc_version"}
                elif action in {"user_create", "user_update"}:
                    expected_fields = {"username", "doc_version"}
                else:
                    raise ValueError("RBAC resource field condition is invalid")
                if set(fields) != expected_fields:
                    raise ValueError("RBAC resource condition fields are invalid")
                normalized_fields = []
                for name in sorted(expected_fields):
                    value = self._rbac_text(fields[name])
                    if len(value.encode("utf-8")) > 4096:
                        raise ValueError(
                            "RBAC resource condition value is too large"
                        )
                    if name == "doc_version":
                        value = self._rbac_condition_version(value)
                    normalized_fields.append({"name": name, "value": value})
                condition_keys.append(resource_key)
                normalized_conditions.append({
                    "kind": "hash_fields",
                    "fields": normalized_fields,
                })
                continue

            if kind == "role_absent":
                self._rbac_require_condition_fields(
                    condition, {"kind", "role_id"}
                )
                role_id = self._rbac_condition_identifier(
                    condition["role_id"], "role_id"
                )
                condition_keys.append(RK.rbac_role_doc(org, sup, role_id))
                normalized_conditions.append({"kind": "absent"})
                continue

            if kind == "user_roles_equal":
                self._rbac_require_condition_fields(
                    condition, {"kind", "role_ids", "version"}
                )
                role_ids = condition["role_ids"]
                if (
                    not isinstance(role_ids, Sequence)
                    or isinstance(role_ids, (str, bytes))
                    or len(role_ids) > 1024
                ):
                    raise ValueError("RBAC role array condition is invalid")
                normalized_role_ids = [
                    self._rbac_condition_identifier(role_id, "role_id")
                    for role_id in role_ids
                ]
                condition_keys.append(resource_key)
                normalized_conditions.append({
                    "kind": "json_array_equals",
                    "field": "roles",
                    "items": normalized_role_ids,
                    "version": self._rbac_condition_version(
                        condition["version"]
                    ),
                })
                continue

            if kind in {
                "assignment_user_absent",
                "assignment_role_absent",
                "assignment_membership",
            }:
                expected_fields = {"kind", "user_id", "role_id"}
                if kind == "assignment_membership":
                    expected_fields |= {"present", "version"}
                self._rbac_require_condition_fields(condition, expected_fields)
                user_id = self._rbac_condition_identifier(
                    condition["user_id"], "user_id", allow_colon=False
                )
                role_id = self._rbac_condition_identifier(
                    condition["role_id"], "role_id", allow_colon=False
                )
                if resource_id != f"{user_id}:{role_id}":
                    raise ValueError(
                        "RBAC assignment condition does not match the audited resource"
                    )
                if kind == "assignment_user_absent":
                    condition_keys.append(
                        RK.rbac_user_doc(org, sup, user_id)
                    )
                    normalized_conditions.append({"kind": "absent"})
                elif kind == "assignment_role_absent":
                    condition_keys.append(
                        RK.rbac_role_doc(org, sup, role_id)
                    )
                    normalized_conditions.append({"kind": "absent"})
                else:
                    present = condition["present"]
                    if not isinstance(present, bool):
                        raise ValueError(
                            "RBAC assignment membership condition is invalid"
                        )
                    condition_keys.append(
                        RK.rbac_user_doc(org, sup, user_id)
                    )
                    normalized_conditions.append({
                        "kind": "json_array_membership",
                        "field": "roles",
                        "item": role_id,
                        "present": present,
                        "version": self._rbac_condition_version(
                            condition["version"]
                        ),
                    })
                continue

            if kind == "token_present":
                self._rbac_require_condition_fields(
                    condition, {"kind", "metadata_json"}
                )
                if not _AUTH_TOKEN_ID_RE.fullmatch(resource_id):
                    raise ValueError("RBAC token condition resource is invalid")
                metadata_json = self._rbac_text(condition["metadata_json"])
                if len(metadata_json.encode("utf-8")) > 4096:
                    raise ValueError("RBAC token condition is too large")
                condition_keys.append(RK.auth_tokens(org))
                normalized_conditions.append({
                    "kind": "hash_fields",
                    "fields": [{
                        "name": resource_id,
                        "value": metadata_json,
                    }],
                })
                continue

            if kind == "token_absent":
                self._rbac_require_condition_fields(condition, {"kind"})
                if not _AUTH_TOKEN_ID_RE.fullmatch(resource_id):
                    raise ValueError("RBAC token condition resource is invalid")
                condition_keys.append(RK.auth_tokens(org))
                normalized_conditions.append({
                    "kind": "hash_field_absent",
                    "field": resource_id,
                })
                continue

            raise ValueError("Unknown RBAC attempt condition")

        condition_json = json.dumps(
            normalized_conditions, sort_keys=True, separators=(",", ":")
        )
        if (
            len(condition_json.encode("utf-8"))
            > self._RBAC_ATTEMPT_CONDITION_BYTES_LIMIT
        ):
            raise ValueError("RBAC attempt conditions are too large")
        return condition_keys, condition_json

    def rbac_append_attempt(
        self,
        org: str,
        sup: str,
        *,
        action: str,
        resource_type: str,
        resource_id: str,
        namespace: str,
        outcome: str,
        cause: str,
        action_context: Any = None,
        before_document: Optional[Mapping[str, Any]] = None,
        before_version: int = 0,
        severity: str = "warning",
        conditions: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> None:
        """Durably append a state-neutral privileged RBAC attempt.

        This path is deliberately incapable of recording ``success``.  It
        samples the role/user namespace counter and advances the shared audit
        ledger atomically, while making no write to any RBAC key.  A
        state-dependent ``no_change`` supplies bounded conditions; Redis
        verifies them in the same Lua invocation as the stream append.
        """

        if not isinstance(outcome, str) or outcome not in {
            "failure", "denied", "no_change",
        }:
            raise ValueError(
                "RBAC attempt outcome must be failure, denied, or no_change"
            )
        if not all(
            isinstance(value, str) and bool(value)
            for value in (action, resource_type, resource_id, namespace, cause)
        ):
            raise ValueError("RBAC attempt identity and cause must be strings")
        expected_identity = _RBAC_ATTEMPT_IDENTITIES.get(action)
        if expected_identity != (resource_type, namespace):
            raise ValueError("Invalid RBAC attempt action/resource/namespace")
        if namespace == "role":
            namespace_key = RK.rbac_role_meta(org, sup)
        elif namespace == "user":
            namespace_key = RK.rbac_user_meta(org, sup)
        elif namespace == "token":
            if sup != _AUTH_AUDIT_SUPER_NAME:
                raise ValueError("Token audit attempts require organization scope")
            namespace_key = self._auth_token_meta_key(org)
        else:
            raise ValueError("RBAC attempt namespace must be role, user, or token")
        condition_keys: List[str] = []
        condition_json: Optional[str] = None
        if outcome == "no_change" and not conditions:
            raise ValueError("No-change RBAC attempts require state conditions")
        if conditions is not None:
            if outcome != "no_change":
                raise ValueError("Only no-change RBAC attempts may be conditional")
            condition_keys, condition_json = (
                self._rbac_normalize_attempt_conditions(
                    org,
                    sup,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    cause=cause,
                    conditions=conditions,
                )
            )
        try:
            now_ms = _now_ms()
            audit_json = self._rbac_audit_json(
                action_context=action_context,
                org=org,
                sup=sup,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                before_document=before_document,
                after_document=before_document,
                before_version=before_version,
                after_version=before_version,
                timestamp_ms=now_ms,
                severity=severity,
                outcome=outcome,
                cause=cause,
            )
            keys = [namespace_key] + condition_keys + self._rbac_audit_keys(org)
            args = [org, sup, audit_json]
            if condition_json is not None:
                args.insert(0, condition_json)
            result = int(self._rbac_append_attempt(keys=keys, args=args) or 0)
            if result == 0 and condition_json is not None:
                # The no-change claim was disproved inside Redis.  Preserve a
                # complete attempt trail with a state-neutral failure record;
                # it needs no optimistic document claim and is safe to append
                # against the namespace revision sampled by its own script.
                self.rbac_append_attempt(
                    org,
                    sup,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    namespace=namespace,
                    outcome="failure",
                    cause="concurrent_modification",
                    action_context=action_context,
                    severity=severity,
                )
                conflict = RbacAuditConditionConflict(
                    "RBAC state changed concurrently before the no-change "
                    "audit append; retry"
                )
                self._rbac_mark_attempt_recorded(conflict)
                raise conflict
            if result != 1:
                raise RuntimeError(
                    "Privileged RBAC attempt was not durably recorded"
                )
        except (RbacAuditAttemptError, RbacAuditConditionConflict):
            raise
        except Exception as error:
            raise RbacAuditAttemptError(
                "Privileged RBAC attempt could not be durably recorded"
            ) from error

    # -- Role meta init --

    def rbac_init_role_meta(self, org: str, sup: str) -> None:
        """Validate role namespace metadata without mutating it."""
        result = int(self._rbac_validate_meta(
            keys=[
                RK.rbac_role_meta(org, sup),
                RK.rbac_role_index(org, sup),
                RK.rbac_rolename_to_id(org, sup),
            ],
            args=[],
        ) or 0)
        if result in {-1, -2}:
            raise RbacIntegrityError(
                "RBAC role namespace metadata cannot be safely initialized"
            )
        if result != 0:
            raise RuntimeError("Atomic RBAC role metadata validation failed")

    # -- Role CRUD --

    def _rbac_is_bootstrap_superadmin(
        self,
        org: str,
        sup: str,
        role_id: str,
        *,
        role_name: str = "",
    ) -> bool:
        """Identify the bootstrap role despite one corrupt identity source."""
        if str(role_name).casefold() == "superadmin":
            return True
        mapped_id = self.r.hget(
            RK.rbac_rolename_to_id(org, sup), "superadmin",
        )
        return (
            mapped_id is not None
            and self._decode_member(mapped_id) == str(role_id)
        )

    @_audit_catalog_rejections(
        action="role_create",
        resource_type="role",
        namespace="role",
        resource_fields=("role_id",),
    )
    def rbac_create_role(
        self,
        org: str,
        sup: str,
        role_id: str,
        role_data: Dict[str, Any],
        *,
        action_context: Any = None,
    ) -> None:
        """Persist a new role document and update indexes.

        Validates ``role_name`` against :data:`SAFE_ROLE_NAME_RE` before
        writing — direct callers (tests, admin scripts, migrations) can't
        bypass the rule by skipping ``RoleManager.create_role``.
        """
        if not isinstance(role_data, dict):
            raise ValueError("role document must be an object")
        supplied_role_id = role_data.get("role_id")
        if supplied_role_id is not None and str(supplied_role_id) != str(role_id):
            raise ValueError("role document role_id does not match the storage key")

        canonical = _canonicalize_role_document(
            role_data, default_if_empty=True,
        )
        canonical["role_id"] = role_id
        canonical["doc_version"] = "1"
        key = RK.rbac_role_doc(org, sup, role_id)
        existing_raw = self.r.hgetall(key)
        if existing_raw:
            existing_document = self._canonical_persisted_role(existing_raw)
            stored_id = self._rbac_text(existing_document.get("role_id"))
            if stored_id and stored_id != role_id:
                raise RbacIntegrityError(
                    "Persisted role identity does not match its storage key"
                )
            self.rbac_append_attempt(
                org,
                sup,
                action="role_create",
                resource_type="role",
                resource_id=role_id,
                namespace="role",
                outcome="no_change",
                cause="resource_already_exists",
                action_context=action_context,
                conditions=[{"kind": "resource_exists"}],
            )
            error = RbacDuplicateIdentityError(
                f"Role {role_id} already exists",
                outcome="no_change",
                cause="resource_already_exists",
                conditions=[{"kind": "resource_exists"}],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        role_name = canonical.get("role_name", "")
        if role_name:
            existing_id = self.r.hget(
                RK.rbac_rolename_to_id(org, sup), role_name.lower(),
            )
            if existing_id is not None:
                existing_id = self._decode_member(existing_id)
                if existing_id != role_id:
                    existing_document = self.get_role_details(
                        org, sup, existing_id,
                    )
                    if (
                        not existing_document
                        or str(existing_document.get("role_name", "")).casefold()
                        != role_name.casefold()
                    ):
                        raise RbacIntegrityError(
                            "RBAC role name map and document are inconsistent"
                        )
                    self.rbac_append_attempt(
                        org,
                        sup,
                        action="role_create",
                        resource_type="role",
                        resource_id=role_id,
                        namespace="role",
                        outcome="no_change",
                        cause="identity_claim_unchanged",
                        action_context=action_context,
                        conditions=[{
                            "kind": "identity_claim",
                            "name": role_name,
                            "identity_id": existing_id,
                        }],
                    )
                    error = RbacDuplicateIdentityError(
                        f"Role name {role_name!r} is already assigned to "
                        f"role {existing_id}",
                        outcome="no_change",
                        cause="identity_claim_unchanged",
                        conditions=[{
                            "kind": "identity_claim",
                            "name": role_name,
                            "identity_id": existing_id,
                        }],
                    )
                    self._rbac_mark_attempt_recorded(error)
                    raise error
        role_type = canonical.get("role", "")
        redis_data = {k: self._rbac_serialize(v) for k, v in canonical.items()}
        now_ms = _now_ms()
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="role_create",
            resource_type="role",
            resource_id=role_id,
            before_document=None,
            after_document=redis_data,
            before_version=0,
            after_version=1,
            changed_fields=redis_data.keys(),
            timestamp_ms=now_ms,
        )
        result = int(self._rbac_create_role(
            keys=[
                key,
                RK.rbac_role_index(org, sup),
                RK.rbac_role_type_index(org, sup, role_type),
                RK.rbac_rolename_to_id(org, sup),
                RK.rbac_role_meta(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                role_id,
                role_type,
                role_name.lower() if role_name else "",
                json.dumps(redis_data, sort_keys=True, separators=(",", ":")),
                str(now_ms),
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            self.rbac_append_attempt(
                org,
                sup,
                action="role_create",
                resource_type="role",
                resource_id=role_id,
                namespace="role",
                outcome="no_change",
                cause="resource_already_exists",
                action_context=action_context,
                conditions=[{"kind": "resource_exists"}],
            )
            error = RbacDuplicateIdentityError(
                f"Role {role_id} already exists",
                outcome="no_change",
                cause="resource_already_exists",
                conditions=[{"kind": "resource_exists"}],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result == -2:
            winner = self.r.hget(
                RK.rbac_rolename_to_id(org, sup), role_name.lower(),
            )
            if winner is None:
                raise _rbac_failure_decision(
                    "Role name claim changed concurrently; retry creation"
                )
            winner = self._decode_member(winner)
            winner_document = self.get_role_details(org, sup, winner)
            if (
                not winner_document
                or str(winner_document.get("role_name", "")).casefold()
                != role_name.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC role name map and document are inconsistent"
                )
            self.rbac_append_attempt(
                org,
                sup,
                action="role_create",
                resource_type="role",
                resource_id=role_id,
                namespace="role",
                outcome="no_change",
                cause="identity_claim_unchanged",
                action_context=action_context,
                conditions=[{
                    "kind": "identity_claim",
                    "name": role_name,
                    "identity_id": winner,
                }],
            )
            error = RbacDuplicateIdentityError(
                f"Role name {role_name!r} is already assigned to role {winner}",
                outcome="no_change",
                cause="identity_claim_unchanged",
                conditions=[{
                    "kind": "identity_claim",
                    "name": role_name,
                    "identity_id": winner,
                }],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result != 1:
            raise RuntimeError("Atomic role creation did not commit")

    @_audit_catalog_rejections(
        action="role_update",
        resource_type="role",
        namespace="role",
        resource_fields=("role_id",),
    )
    def rbac_update_role(
        self,
        org: str,
        sup: str,
        role_id: str,
        fields: Dict[str, Any],
        *,
        action_context: Any = None,
    ) -> None:
        """Update specific fields of an existing role in-place.

        Validates ``role_name`` (when the update touches it) so direct
        callers can't introduce unsafe names via a partial update either.
        """
        if not isinstance(fields, dict):
            raise ValueError("role update fields must be an object")
        if "role_id" in fields and str(fields["role_id"]) != str(role_id):
            raise ValueError("role_id cannot be changed")
        if "doc_version" in fields:
            raise ValueError("doc_version cannot be changed")
        key = RK.rbac_role_doc(org, sup, role_id)
        if not self.r.exists(key):
            raise _rbac_absent_decision(
                f"Role {role_id} does not exist",
            )

        # Validate the complete effective document, not merely the partial
        # patch.  This prevents a direct caller from preserving or combining
        # malformed stored policy with a superficially valid update.
        current_raw = self.r.hgetall(key)
        if not current_raw:
            raise _rbac_absent_decision(
                f"Role {role_id} does not exist",
            )
        self._canonical_persisted_role(current_raw)
        expected_tables = self._rbac_text(current_raw.get("tables"))
        expected_modified = self._rbac_text(current_raw.get("modified_ms"))
        expected_doc_version = self._rbac_doc_version(current_raw)
        current: Dict[str, Any] = dict(current_raw or {})
        if "tables" in current:
            try:
                current["tables"] = _decode_role_json_field(
                    current["tables"], field="tables",
                )
            except (TypeError, ValueError) as exc:
                raise RbacIntegrityError(
                    "Persisted role policy is invalid"
                ) from exc

        current_role = current.get("role", "")
        if isinstance(current_role, bytes):
            current_role = current_role.decode("utf-8")
        current_name = current.get("role_name", "")
        if isinstance(current_name, bytes):
            current_name = current_name.decode("utf-8")
        current_name = current_name or ""
        is_bootstrap = self._rbac_is_bootstrap_superadmin(
            org, sup, role_id, role_name=current_name,
        )
        requested_role = fields.get("role", current_role)
        requested_name = fields.get("role_name", current_name) or ""
        if (
            current_role == "superadmin"
            or is_bootstrap
        ) and requested_role != "superadmin":
            raise ValueError("A superadmin role cannot be demoted.")
        if (
            is_bootstrap
            and str(requested_name).casefold() != "superadmin"
        ):
            raise ValueError("The bootstrap superadmin role cannot be renamed.")

        merged = dict(current)
        merged.update(fields)
        canonical = _canonicalize_role_document(
            merged, default_if_empty=False,
        )

        old_role = current.get("role", "")
        if isinstance(old_role, bytes):
            old_role = old_role.decode("utf-8")
        new_role = canonical["role"]
        old_name = current.get("role_name", "")
        if isinstance(old_name, bytes):
            old_name = old_name.decode("utf-8")
        old_name = old_name or ""
        if (old_role == "superadmin" or is_bootstrap) and new_role != "superadmin":
            raise ValueError("A superadmin role cannot be demoted.")

        new_name = canonical.get("role_name", old_name) or ""
        if is_bootstrap:
            if new_name.casefold() != "superadmin":
                raise ValueError("The bootstrap superadmin role cannot be renamed.")
        if new_name and new_name.casefold() != old_name.casefold():
            conflicting_id = self.r.hget(
                RK.rbac_rolename_to_id(org, sup), new_name.lower(),
            )
            if conflicting_id is not None:
                conflicting_id = self._decode_member(conflicting_id)
                if conflicting_id != role_id:
                    conflicting_document = self.get_role_details(
                        org, sup, conflicting_id,
                    )
                    if (
                        not conflicting_document
                        or str(conflicting_document.get("role_name", "")).casefold()
                        != new_name.casefold()
                    ):
                        raise RbacIntegrityError(
                            "RBAC role name map and document are inconsistent"
                        )
                    raise _rbac_identity_decision(
                        f"Role name {new_name!r} is already assigned to "
                        f"role {conflicting_id}",
                        new_name,
                        conflicting_id,
                    )

        # Persist requested metadata fields plus the complete canonical policy
        # and its matching hash.  Normalising all policy fields closes the
        # direct-catalog bypass without overwriting unrelated document data.
        write_fields = dict(fields)
        write_fields.update({
            "role": new_role,
            "tables": canonical["tables"],
            "content_hash": canonical["content_hash"],
        })
        redis_data = {k: self._rbac_serialize(v) for k, v in write_fields.items()}
        now_ms = _now_ms()
        redis_data["modified_ms"] = str(now_ms)
        before_document = self._rbac_string_document(current_raw)
        after_document = dict(before_document)
        after_document.update(redis_data)
        after_document["doc_version"] = str(int(expected_doc_version) + 1)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="role_update",
            resource_type="role",
            resource_id=role_id,
            before_document=before_document,
            after_document=after_document,
            before_version=int(expected_doc_version),
            after_version=int(expected_doc_version) + 1,
            changed_fields=set(redis_data) | {"doc_version"},
            timestamp_ms=now_ms,
        )

        result = int(self._rbac_update_role(
            keys=[
                key,
                RK.rbac_role_index(org, sup),
                RK.rbac_role_type_index(org, sup, old_role),
                RK.rbac_role_type_index(org, sup, new_role),
                RK.rbac_rolename_to_id(org, sup),
                RK.rbac_role_meta(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                role_id,
                old_role,
                old_name,
                expected_tables,
                expected_modified,
                expected_doc_version,
                new_role,
                new_name,
                json.dumps(redis_data, sort_keys=True, separators=(",", ":")),
                str(now_ms),
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            raise _rbac_absent_decision(
                f"Role {role_id} does not exist",
            )
        if result == -2:
            winner = self.r.hget(
                RK.rbac_rolename_to_id(org, sup), new_name.lower(),
            )
            if winner is None:
                raise _rbac_failure_decision(
                    "Role name claim changed concurrently; retry the update"
                )
            winner_id = self._decode_member(winner)
            winner_document = self.get_role_details(org, sup, winner_id)
            if (
                not winner_document
                or str(winner_document.get("role_name", "")).casefold()
                != new_name.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC role name map and document are inconsistent"
                )
            raise _rbac_identity_decision(
                f"Role name {new_name!r} is already assigned",
                new_name,
                winner_id,
            )
        if result == -3:
            raise _rbac_failure_decision(
                f"Role {role_id} changed concurrently; retry the update"
            )
        if result == -4:
            raise _rbac_denied_decision(
                "A superadmin role cannot be demoted.",
                cause="protected_identity",
            )
        if result == -5:
            raise _rbac_denied_decision(
                "The bootstrap superadmin role cannot be renamed.",
                cause="protected_identity",
            )
        if result == -6:
            raise RbacIntegrityError(
                "Persisted role identity does not match its storage key"
            )
        if result != 1:
            raise RuntimeError("Atomic role update did not commit")

    @_audit_catalog_rejections(
        action="role_delete",
        resource_type="role",
        namespace="role",
        resource_fields=("role_id",),
    )
    def rbac_delete_role(
        self,
        org: str,
        sup: str,
        role_id: str,
        *,
        action_context: Any = None,
    ) -> bool:
        """Atomically delete a role, strip from users, and clean name→id mapping."""
        key = RK.rbac_role_doc(org, sup, role_id)
        raw = self.r.hgetall(key)
        if not raw:
            self.rbac_append_attempt(
                org,
                sup,
                action="role_delete",
                resource_type="role",
                resource_id=_safe_rbac_audit_resource_id(
                    role_id, fallback="pending-role"
                ),
                namespace="role",
                outcome="no_change",
                cause="resource_missing",
                action_context=action_context,
                conditions=[{"kind": "resource_absent"}],
            )
            return False
        role_type = self._rbac_text(raw.get("role"))
        if role_type == "superadmin":
            raise _rbac_denied_decision(
                "A superadmin role cannot be deleted.",
                cause="protected_identity",
                severity="critical",
            )
        role_name = self._rbac_text(raw.get("role_name"))
        expected_tables = self._rbac_text(raw.get("tables"))
        expected_modified = self._rbac_text(raw.get("modified_ms"))
        expected_doc_version = self._rbac_doc_version(raw)
        if self._rbac_is_bootstrap_superadmin(
            org, sup, role_id, role_name=role_name,
        ):
            raise _rbac_denied_decision(
                "The bootstrap superadmin role cannot be deleted.",
                cause="protected_identity",
                severity="critical",
            )
        # The Lua script appends each user_id to this prefix.
        user_doc_key_prefix = RK.rbac_user_doc_prefix(org, sup)
        now_ms = _now_ms()
        before_document = self._rbac_string_document(raw)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="role_delete",
            resource_type="role",
            resource_id=role_id,
            before_document=before_document,
            after_document=None,
            before_version=int(expected_doc_version),
            after_version=0,
            changed_fields=set(before_document) | {"user.roles"},
            timestamp_ms=now_ms,
            severity="critical",
        )
        cascade_manifest_id = json.loads(audit_json)["cascade_manifest_id"]
        cascade_manifest_key = RK.audit_privileged_cascade(
            org, cascade_manifest_id,
        )
        result = self._rbac_delete_role(
            keys=[
                key,
                RK.rbac_role_index(org, sup),
                RK.rbac_role_type_index(org, sup, role_type),
                RK.rbac_role_meta(org, sup),
                RK.rbac_user_index(org, sup),
                RK.rbac_rolename_to_id(org, sup),
                RK.rbac_user_meta(org, sup),
                cascade_manifest_key,
                RK.rbac_username_to_id(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                role_id,
                str(now_ms),
                role_type,
                role_name,
                expected_tables,
                expected_modified,
                expected_doc_version,
                user_doc_key_prefix,
                cascade_manifest_key,
                str(self._RBAC_CASCADE_MANIFEST_USER_LIMIT),
                org,
                sup,
                audit_json,
            ],
        )
        result = int(result or 0)
        if result == -1:
            raise _rbac_denied_decision(
                "A superadmin role cannot be deleted.",
                cause="protected_identity",
                severity="critical",
            )
        if result == -2:
            raise _rbac_failure_decision(
                f"Role {role_id} changed concurrently; retry the delete",
                severity="critical",
            )
        if result == -3:
            raise RbacIntegrityError(
                "Persisted role identity does not match its storage key"
            )
        if result == -4:
            raise RbacIntegrityError(
                "Persisted user role assignments are corrupt"
            )
        if result == -5:
            raise _rbac_denied_decision(
                "role deletion must scan more than the "
                f"{self._RBAC_CASCADE_MANIFEST_USER_LIMIT}-user atomic audit limit",
                cause="operation_limit_exceeded",
                severity="critical",
            )
        if result == -6:
            raise RbacIntegrityError(
                "RBAC user index, identity map, and documents are inconsistent"
            )
        if result not in (0, 1):
            raise RuntimeError("Atomic role deletion did not commit")
        if result == 0:
            self.rbac_append_attempt(
                org,
                sup,
                action="role_delete",
                resource_type="role",
                resource_id=_safe_rbac_audit_resource_id(
                    role_id, fallback="pending-role"
                ),
                namespace="role",
                outcome="no_change",
                cause="resource_disappeared",
                action_context=action_context,
                before_document=before_document,
                before_version=int(expected_doc_version),
                conditions=[{"kind": "resource_absent"}],
            )
        return result == 1

    def rbac_role_exists(self, org: str, sup: str, role_id: str) -> bool:
        return bool(self.r.exists(RK.rbac_role_doc(org, sup, role_id)))


    def rbac_get_role_ids_by_type(self, org: str, sup: str, role_type: str) -> List[str]:
        """Return role_ids belonging to a specific role type."""
        members = self.r.smembers(RK.rbac_role_type_index(org, sup, role_type))
        return [self._decode_member(m) for m in (members or [])]

    def rbac_get_superadmin_role_id(self, org: str, sup: str) -> Optional[str]:
        """Return the first superadmin role_id, or None."""
        ids = self.rbac_get_role_ids_by_type(org, sup, "superadmin")
        return ids[0] if ids else None

    def rbac_get_role_id_by_name(self, org: str, sup: str, role_name: str) -> Optional[str]:
        """Look up a role_id from a role_name (case-insensitive)."""
        val = self.r.hget(RK.rbac_rolename_to_id(org, sup), role_name.lower())
        if val is None:
            return None
        return self._decode_member(val)

    # -- User meta init --

    def rbac_init_user_meta(self, org: str, sup: str) -> None:
        """Validate user namespace metadata without mutating it."""
        result = int(self._rbac_validate_meta(
            keys=[
                RK.rbac_user_meta(org, sup),
                RK.rbac_user_index(org, sup),
                RK.rbac_username_to_id(org, sup),
            ],
            args=[],
        ) or 0)
        if result in {-1, -2}:
            raise RbacIntegrityError(
                "RBAC user namespace metadata cannot be safely initialized"
            )
        if result != 0:
            raise RuntimeError("Atomic RBAC user metadata validation failed")

    # -- User CRUD --

    def _rbac_is_default_superuser(
        self,
        org: str,
        sup: str,
        user_id: str,
        *,
        username: str = "",
    ) -> bool:
        """Identify the recovery user despite one corrupt identity source."""
        if str(username).casefold() == "superuser":
            return True
        mapped_id = self.r.hget(
            RK.rbac_username_to_id(org, sup), "superuser",
        )
        return (
            mapped_id is not None
            and self._decode_member(mapped_id) == str(user_id)
        )

    def _rbac_roles_include_superadmin(
        self, org: str, sup: str, role_ids: Any,
    ) -> bool:
        if not isinstance(role_ids, list):
            return False
        for role_id in role_ids:
            if not isinstance(role_id, str):
                continue
            role_type = self.r.hget(
                RK.rbac_role_doc(org, sup, role_id), "role",
            )
            if isinstance(role_type, bytes):
                role_type = role_type.decode("utf-8")
            if role_type == "superadmin":
                return True
        return False

    @_audit_catalog_rejections(
        action="user_create",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def rbac_create_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        user_data: Dict[str, Any],
        *,
        action_context: Any = None,
    ) -> None:
        """Persist a new user document and update indexes.

        Validates ``username`` against :data:`SAFE_USERNAME_RE` before
        writing — direct callers (tests, admin scripts, migrations) can't
        bypass the rule by skipping ``UserManager.create_user``.
        """
        if not isinstance(user_data, dict):
            raise ValueError("user document must be an object")
        supplied_user_id = user_data.get("user_id")
        if supplied_user_id is not None and str(supplied_user_id) != str(user_id):
            raise ValueError("user document user_id does not match the storage key")
        if "username" not in user_data:
            raise ValueError("username is required")
        username = user_data["username"]
        validate_username(username)
        roles = user_data.get("roles", [])
        if not isinstance(roles, list) or not all(
            isinstance(role_id, str) and role_id for role_id in roles
        ):
            raise ValueError("roles must be a list of role IDs")
        for role_id in roles:
            role_key = RK.rbac_role_doc(org, sup, role_id)
            assigned_type = self.r.hget(role_key, "role")
            if assigned_type is None:
                raise _rbac_role_absent_decision(
                    f"Role {role_id} does not exist", role_id,
                )
            assigned_type = self._rbac_text(assigned_type)
            if assigned_type not in {
                "superadmin", "admin", "writer", "reader", "meta",
            }:
                raise RbacIntegrityError(
                    "Persisted role document has an invalid role type"
                )
        if (
            username.casefold() == "superuser"
            and not self._rbac_roles_include_superadmin(org, sup, roles)
        ):
            raise ValueError("The default superuser must retain a superadmin role")
        key = RK.rbac_user_doc(org, sup, user_id)
        existing_raw = self.r.hgetall(key)
        if existing_raw:
            stored_id = self._rbac_text(existing_raw.get("user_id"))
            if stored_id and stored_id != user_id:
                raise RbacIntegrityError(
                    "Persisted user identity does not match its storage key"
                )
            persisted_username = self._rbac_text(existing_raw.get("username"))
            try:
                validate_username(persisted_username)
            except (TypeError, ValueError) as exc:
                raise RbacIntegrityError(
                    "Persisted username is invalid"
                ) from exc
            self._decode_user_roles(existing_raw.get("roles"))
            self.rbac_append_attempt(
                org,
                sup,
                action="user_create",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="resource_already_exists",
                action_context=action_context,
                conditions=[{"kind": "resource_exists"}],
            )
            error = RbacDuplicateIdentityError(
                f"User {user_id} already exists",
                outcome="no_change",
                cause="resource_already_exists",
                conditions=[{"kind": "resource_exists"}],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        existing_id = self.r.hget(
            RK.rbac_username_to_id(org, sup), username.lower(),
        )
        if existing_id is not None and self._decode_member(existing_id) != user_id:
            winner = self._decode_member(existing_id)
            winner_document = self.get_user_details(org, sup, winner)
            if (
                not winner_document
                or str(winner_document.get("username", "")).casefold()
                != username.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC username map and document are inconsistent"
                )
            self.rbac_append_attempt(
                org,
                sup,
                action="user_create",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="identity_claim_unchanged",
                action_context=action_context,
                conditions=[{
                    "kind": "identity_claim",
                    "name": username,
                    "identity_id": winner,
                }],
            )
            error = RbacDuplicateIdentityError(
                f"Username {username!r} is already assigned",
                outcome="no_change",
                cause="identity_claim_unchanged",
                conditions=[{
                    "kind": "identity_claim",
                    "name": username,
                    "identity_id": winner,
                }],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        user_data = dict(user_data)
        user_data["user_id"] = user_id
        user_data["roles"] = roles
        user_data["doc_version"] = "1"
        redis_data = {k: self._rbac_serialize(v) for k, v in user_data.items()}
        now_ms = _now_ms()
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="user_create",
            resource_type="user",
            resource_id=user_id,
            before_document=None,
            after_document=redis_data,
            before_version=0,
            after_version=1,
            changed_fields=redis_data.keys(),
            role_ids_added=roles,
            timestamp_ms=now_ms,
        )
        result = int(self._rbac_create_user(
            keys=[
                key,
                RK.rbac_user_index(org, sup),
                RK.rbac_username_to_id(org, sup),
                RK.rbac_user_meta(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                user_id,
                username.lower(),
                json.dumps(redis_data, sort_keys=True, separators=(",", ":")),
                str(now_ms),
                RK.rbac_role_doc_prefix(org, sup),
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_create",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="resource_already_exists",
                action_context=action_context,
                conditions=[{"kind": "resource_exists"}],
            )
            error = RbacDuplicateIdentityError(
                f"User {user_id} already exists",
                outcome="no_change",
                cause="resource_already_exists",
                conditions=[{"kind": "resource_exists"}],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result == -2:
            winner = self.r.hget(
                RK.rbac_username_to_id(org, sup), username.lower(),
            )
            if winner is None:
                raise _rbac_failure_decision(
                    "Username claim changed concurrently; retry creation"
                )
            winner = self._decode_member(winner)
            winner_document = self.get_user_details(org, sup, winner)
            if (
                not winner_document
                or str(winner_document.get("username", "")).casefold()
                != username.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC username map and document are inconsistent"
                )
            self.rbac_append_attempt(
                org,
                sup,
                action="user_create",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="identity_claim_unchanged",
                action_context=action_context,
                conditions=[{
                    "kind": "identity_claim",
                    "name": username,
                    "identity_id": winner,
                }],
            )
            error = RbacDuplicateIdentityError(
                f"Username {username!r} is already assigned to user {winner}",
                outcome="no_change",
                cause="identity_claim_unchanged",
                conditions=[{
                    "kind": "identity_claim",
                    "name": username,
                    "identity_id": winner,
                }],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result == -3:
            for role_id in roles:
                role_key = RK.rbac_role_doc(org, sup, role_id)
                assigned_type = self.r.hget(role_key, "role")
                if assigned_type is None:
                    raise _rbac_role_absent_decision(
                        f"Role {role_id} does not exist", role_id,
                    )
                if self._rbac_text(assigned_type) not in {
                    "superadmin", "admin", "writer", "reader", "meta",
                }:
                    raise RbacIntegrityError(
                        "Persisted role document has an invalid role type"
                    )
            raise _rbac_failure_decision(
                "User role assignment validation changed concurrently"
            )
        if result != 1:
            raise RuntimeError("Atomic user creation did not commit")

    @_audit_catalog_rejections(
        action="user_update",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def rbac_update_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        fields: Dict[str, Any],
        *,
        action_context: Any = None,
    ) -> None:
        """Update specific fields of an existing user in-place.

        Values are serialized the same way as ``rbac_create_user`` — callers
        should pass raw Python objects, not pre-encoded JSON. Validates
        ``username`` (when present in the update) so partial updates can't
        slip an unsafe name past the rule.
        """
        if not isinstance(fields, dict):
            raise ValueError("user update fields must be an object")
        fields = dict(fields)
        if "user_id" in fields and str(fields["user_id"]) != str(user_id):
            raise ValueError("user_id cannot be changed")
        if "doc_version" in fields:
            raise ValueError("doc_version cannot be changed")
        if "username" in fields:
            validate_username(fields.get("username", ""))
        key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(key) or {}
        if not raw:
            raise _rbac_absent_decision(
                f"User {user_id} does not exist",
            )
        self._rbac_commit_user_update(
            org,
            sup,
            user_id,
            raw,
            fields,
            action_context=action_context,
        )

    def _rbac_commit_user_update(
        self,
        org: str,
        sup: str,
        user_id: str,
        raw: Dict[str, Any],
        fields: Dict[str, Any],
        *,
        action_context: Any = None,
    ) -> None:
        """CAS one validated user snapshot into its complete index boundary."""
        old_username = self._rbac_text(raw.get("username"))
        expected_roles = self._rbac_text(raw.get("roles"))
        expected_modified = self._rbac_text(raw.get("modified_ms"))
        expected_doc_version = self._rbac_doc_version(raw)
        try:
            validate_username(old_username)
        except (TypeError, ValueError) as exc:
            raise RbacIntegrityError(
                "Persisted username is invalid"
            ) from exc
        current_roles = self._decode_user_roles(expected_roles)

        new_username = fields.get("username", old_username)
        validate_username(new_username)
        resulting_roles = fields.get("roles", current_roles)
        if not isinstance(resulting_roles, list) or not all(
            isinstance(role_id, str) and role_id for role_id in resulting_roles
        ):
            raise ValueError("roles must be a list of role IDs")
        for role_id in resulting_roles:
            role_key = RK.rbac_role_doc(org, sup, role_id)
            assigned_type = self.r.hget(role_key, "role")
            if assigned_type is None:
                raise _rbac_role_absent_decision(
                    f"Role {role_id} does not exist", role_id,
                )
            if self._rbac_text(assigned_type) not in {
                "superadmin", "admin", "writer", "reader", "meta",
            }:
                raise RbacIntegrityError(
                    "Persisted role document has an invalid role type"
                )
        is_default_superuser = self._rbac_is_default_superuser(
            org, sup, user_id, username=old_username,
        )
        if (
            is_default_superuser
            and new_username.casefold() != "superuser"
        ):
            raise _rbac_denied_decision(
                "The default superuser cannot be renamed",
                cause="protected_identity",
            )
        if (
            is_default_superuser
            and not self._rbac_roles_include_superadmin(
                org, sup, resulting_roles,
            )
        ):
            raise _rbac_denied_decision(
                "The default superuser must retain a superadmin role",
                cause="protected_identity",
            )
        if new_username.casefold() != old_username.casefold():
            existing_id = self.r.hget(
                RK.rbac_username_to_id(org, sup), new_username.lower(),
            )
            if (
                existing_id is not None
                and self._decode_member(existing_id) != user_id
            ):
                winner = self._decode_member(existing_id)
                winner_document = self.get_user_details(org, sup, winner)
                if (
                    not winner_document
                    or str(winner_document.get("username", "")).casefold()
                    != new_username.casefold()
                ):
                    raise RbacIntegrityError(
                        "RBAC username map and document are inconsistent"
                    )
                raise _rbac_identity_decision(
                    f"Username {new_username!r} is already assigned",
                    new_username,
                    winner,
                )

        now_ms = _now_ms()
        fields = dict(fields)
        fields["modified_ms"] = str(now_ms)
        redis_data = {k: self._rbac_serialize(v) for k, v in fields.items()}
        before_document = self._rbac_string_document(raw)
        after_document = dict(before_document)
        after_document.update(redis_data)
        after_document["doc_version"] = str(int(expected_doc_version) + 1)
        previous_roles = set(current_roles)
        next_roles = set(resulting_roles)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="user_update",
            resource_type="user",
            resource_id=user_id,
            before_document=before_document,
            after_document=after_document,
            before_version=int(expected_doc_version),
            after_version=int(expected_doc_version) + 1,
            changed_fields=set(redis_data) | {"doc_version"},
            role_ids_added=next_roles - previous_roles,
            role_ids_removed=previous_roles - next_roles,
            timestamp_ms=now_ms,
        )
        result = int(self._rbac_update_user(
            keys=[
                RK.rbac_user_doc(org, sup, user_id),
                RK.rbac_user_index(org, sup),
                RK.rbac_username_to_id(org, sup),
                RK.rbac_user_meta(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                user_id,
                old_username,
                expected_roles,
                expected_modified,
                expected_doc_version,
                new_username,
                json.dumps(resulting_roles, separators=(",", ":")),
                json.dumps(redis_data, sort_keys=True, separators=(",", ":")),
                str(now_ms),
                RK.rbac_role_doc_prefix(org, sup),
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            raise _rbac_absent_decision(
                f"User {user_id} does not exist",
            )
        if result == -2:
            winner = self.r.hget(
                RK.rbac_username_to_id(org, sup), new_username.lower(),
            )
            if winner is None:
                raise _rbac_failure_decision(
                    "Username claim changed concurrently; retry the update"
                )
            winner_id = self._decode_member(winner)
            winner_document = self.get_user_details(org, sup, winner_id)
            if (
                not winner_document
                or str(winner_document.get("username", "")).casefold()
                != new_username.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC username map and document are inconsistent"
                )
            raise _rbac_identity_decision(
                f"Username {new_username!r} is already assigned",
                new_username,
                winner_id,
            )
        if result == -3:
            raise _rbac_failure_decision(
                f"User {user_id} changed concurrently; retry the update"
            )
        if result == -4:
            raise _rbac_denied_decision(
                "The default superuser cannot be renamed",
                cause="protected_identity",
            )
        if result == -5:
            raise _rbac_denied_decision(
                "The default superuser must retain a superadmin role",
                cause="protected_identity",
            )
        if result == -6:
            raise RbacIntegrityError("Persisted user roles are invalid")
        if result == -7:
            raise RbacIntegrityError(
                "Persisted user identity does not match its storage key"
            )
        if result == -8:
            for role_id in resulting_roles:
                role_key = RK.rbac_role_doc(org, sup, role_id)
                assigned_type = self.r.hget(role_key, "role")
                if assigned_type is None:
                    raise _rbac_role_absent_decision(
                        f"Role {role_id} does not exist", role_id,
                    )
                if self._rbac_text(assigned_type) not in {
                    "superadmin", "admin", "writer", "reader", "meta",
                }:
                    raise RbacIntegrityError(
                        "Persisted role document has an invalid role type"
                    )
            raise _rbac_failure_decision(
                "User role assignment validation changed concurrently"
            )
        if result != 1:
            raise RuntimeError("Atomic user update did not commit")

    @_audit_catalog_rejections(
        action="user_update",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def rbac_rename_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        old_username: str,
        new_username: str,
        *,
        action_context: Any = None,
    ) -> None:
        """Atomically update the username → user_id mapping.

        Validates ``new_username`` — this is the third write path that can
        touch the username index, so it must enforce the same rule as
        ``rbac_create_user`` and ``rbac_update_user``.
        """
        validate_username(old_username)
        validate_username(new_username)
        key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(key)
        if not raw:
            raise _rbac_absent_decision(
                f"User {user_id} does not exist",
            )
        actual_username = self._rbac_text(raw.get("username"))
        if actual_username.casefold() != old_username.casefold():
            raise _rbac_failure_decision(
                "old_username does not match the user document",
                cause="stale_identity",
            )
        if self._rbac_is_default_superuser(
            org, sup, user_id, username=actual_username,
        ):
            raise _rbac_denied_decision(
                "The default superuser cannot be renamed",
                cause="protected_identity",
            )
        conflict = self.r.hget(
            RK.rbac_username_to_id(org, sup), new_username.lower(),
        )
        if conflict is not None and self._decode_member(conflict) != user_id:
            conflict_id = self._decode_member(conflict)
            conflict_document = self.get_user_details(org, sup, conflict_id)
            if (
                not conflict_document
                or str(conflict_document.get("username", "")).casefold()
                != new_username.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC username map and document are inconsistent"
                )
            raise _rbac_identity_decision(
                f"Username {new_username!r} is already assigned",
                new_username,
                conflict_id,
            )
        self._rbac_commit_user_update(
            org,
            sup,
            user_id,
            raw,
            {"username": new_username},
            action_context=action_context,
        )

    @_audit_catalog_rejections(
        action="user_delete",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def rbac_delete_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        *,
        action_context: Any = None,
    ) -> None:
        """Delete a user document and remove from all indexes."""
        key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(key)
        if not raw:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_delete",
                resource_type="user",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, fallback="pending-user"
                ),
                namespace="user",
                outcome="no_change",
                cause="resource_missing",
                action_context=action_context,
                conditions=[{"kind": "resource_absent"}],
            )
            error = ValueError(f"User {user_id} does not exist")
            self._rbac_mark_attempt_recorded(error)
            raise error
        username = self._rbac_text(raw.get("username"))
        expected_roles = self._rbac_text(raw.get("roles"))
        expected_modified = self._rbac_text(raw.get("modified_ms"))
        expected_doc_version = self._rbac_doc_version(raw)
        if self._rbac_is_default_superuser(
            org, sup, user_id, username=username,
        ):
            raise _rbac_denied_decision(
                "The default superuser cannot be deleted",
                cause="protected_identity",
                severity="critical",
            )
        now_ms = _now_ms()
        before_document = self._rbac_string_document(raw)
        current_roles = self._decode_user_roles(expected_roles)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="user_delete",
            resource_type="user",
            resource_id=user_id,
            before_document=before_document,
            after_document=None,
            before_version=int(expected_doc_version),
            after_version=0,
            changed_fields=before_document.keys(),
            role_ids_removed=current_roles,
            timestamp_ms=now_ms,
            severity="critical",
        )
        result = int(self._rbac_delete_user(
            keys=[
                key,
                RK.rbac_user_index(org, sup),
                RK.rbac_username_to_id(org, sup),
                RK.rbac_user_meta(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                user_id, username, expected_roles, expected_modified,
                expected_doc_version,
                str(now_ms),
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == 0:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_delete",
                resource_type="user",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, fallback="pending-user"
                ),
                namespace="user",
                outcome="no_change",
                cause="resource_disappeared",
                action_context=action_context,
                before_document=before_document,
                before_version=int(expected_doc_version),
                conditions=[{"kind": "resource_absent"}],
            )
            error = ValueError(f"User {user_id} does not exist")
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result == -1:
            raise _rbac_denied_decision(
                "The default superuser cannot be deleted",
                cause="protected_identity",
                severity="critical",
            )
        if result == -2:
            raise _rbac_failure_decision(
                f"User {user_id} changed concurrently; retry the delete",
                severity="critical",
            )
        if result == -3:
            raise RbacIntegrityError(
                "Persisted user identity does not match its storage key"
            )
        if result != 1:
            raise RuntimeError("Atomic user deletion did not commit")


    def rbac_get_user_id_by_username(self, org: str, sup: str, username: str) -> Optional[str]:
        """Look up a user_id from a username (case-insensitive)."""
        val = self.r.hget(RK.rbac_username_to_id(org, sup), username.lower())
        if val is None:
            return None
        return self._decode_member(val)

    def rbac_list_user_ids(self, org: str, sup: str) -> List[str]:
        """Return all user_ids from the index SET."""
        members = self.r.smembers(RK.rbac_user_index(org, sup))
        return [self._decode_member(m) for m in (members or [])]

    # -- Atomic role ↔ user mutations --

    @_audit_catalog_rejections(
        action="user_role_assign",
        resource_type="user_role_assignment",
        namespace="user",
        resource_fields=("user_id", "role_id"),
    )
    def rbac_add_role_to_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        role_id: str,
        *,
        action_context: Any = None,
    ) -> bool:
        """Atomically add a role to a user's role list (no-op if already present)."""
        user_key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(user_key) or {}
        if not raw:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_assign",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="user_missing",
                action_context=action_context,
                conditions=[{
                    "kind": "assignment_user_absent",
                    "user_id": user_id,
                    "role_id": role_id,
                }],
            )
            return False
        role_key = RK.rbac_role_doc(org, sup, role_id)
        assigned_type = self.r.hget(role_key, "role")
        if assigned_type is None:
            raise _rbac_assignment_role_absent_decision(
                f"Role {role_id} does not exist", user_id, role_id,
            )
        if self._rbac_text(assigned_type) not in {
            "superadmin", "admin", "writer", "reader", "meta",
        }:
            raise RbacIntegrityError(
                "Persisted role document has an invalid role type"
            )
        expected_roles = self._rbac_text(raw.get("roles"))
        expected_doc_version = self._rbac_doc_version(raw)
        roles = self._decode_user_roles(expected_roles)
        if role_id in roles:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_assign",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="role_already_assigned",
                action_context=action_context,
                before_document=self._rbac_string_document(raw),
                before_version=int(expected_doc_version),
                conditions=[{
                    "kind": "assignment_membership",
                    "user_id": user_id,
                    "role_id": role_id,
                    "present": True,
                    "version": expected_doc_version,
                }],
            )
            return False
        now_ms = _now_ms()
        before_document = self._rbac_string_document(raw)
        after_document = dict(before_document)
        after_document["roles"] = json.dumps(
            roles + [role_id], separators=(",", ":"),
        )
        after_document["modified_ms"] = str(now_ms)
        after_document["doc_version"] = str(int(expected_doc_version) + 1)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="user_role_assign",
            resource_type="user_role_assignment",
            resource_id=f"{user_id}:{role_id}",
            before_document=before_document,
            after_document=after_document,
            before_version=int(expected_doc_version),
            after_version=int(expected_doc_version) + 1,
            changed_fields=("roles", "modified_ms", "doc_version"),
            role_ids_added=(role_id,),
            timestamp_ms=now_ms,
        )
        result = int(self._rbac_add_role_to_user(
            keys=[
                user_key,
                RK.rbac_user_meta(org, sup),
                RK.rbac_role_doc(org, sup, role_id),
            ] + self._rbac_audit_keys(org),
            args=[
                role_id,
                str(now_ms),
                user_id,
                expected_roles,
                expected_doc_version,
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            assigned_type = self.r.hget(role_key, "role")
            if assigned_type is None:
                raise _rbac_assignment_role_absent_decision(
                    f"Role {role_id} does not exist", user_id, role_id,
                )
            if self._rbac_text(assigned_type) not in {
                "superadmin", "admin", "writer", "reader", "meta",
            }:
                raise RbacIntegrityError(
                    "Persisted role document has an invalid role type"
                )
            raise _rbac_failure_decision(
                "Role validation changed concurrently; retry the assignment"
            )
        if result == -2:
            raise _rbac_failure_decision(
                f"User {user_id} changed concurrently; retry the assignment"
            )
        if result == 0:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_assign",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="assignment_not_changed",
                action_context=action_context,
                before_document=before_document,
                before_version=int(expected_doc_version),
                conditions=[{
                    "kind": "assignment_membership",
                    "user_id": user_id,
                    "role_id": role_id,
                    "present": True,
                    "version": expected_doc_version,
                }],
            )
        return result == 1

    @_audit_catalog_rejections(
        action="user_role_remove",
        resource_type="user_role_assignment",
        namespace="user",
        resource_fields=("user_id", "role_id"),
    )
    def rbac_remove_role_from_user(
        self,
        org: str,
        sup: str,
        user_id: str,
        role_id: str,
        *,
        action_context: Any = None,
    ) -> bool:
        """Atomically remove a role from a user's role list."""
        user_key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(user_key) or {}
        if not raw:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_remove",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="user_missing",
                action_context=action_context,
                conditions=[{
                    "kind": "assignment_user_absent",
                    "user_id": user_id,
                    "role_id": role_id,
                }],
            )
            return False
        expected_roles = self._rbac_text(raw.get("roles"))
        expected_doc_version = self._rbac_doc_version(raw)
        roles = self._decode_user_roles(expected_roles)
        if role_id not in roles:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_remove",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="role_not_assigned",
                action_context=action_context,
                before_document=self._rbac_string_document(raw),
                before_version=int(expected_doc_version),
                conditions=[{
                    "kind": "assignment_membership",
                    "user_id": user_id,
                    "role_id": role_id,
                    "present": False,
                    "version": expected_doc_version,
                }],
            )
            return False
        now_ms = _now_ms()
        resulting_roles = [assigned_id for assigned_id in roles if assigned_id != role_id]
        before_document = self._rbac_string_document(raw)
        after_document = dict(before_document)
        after_document["roles"] = json.dumps(
            resulting_roles, separators=(",", ":"),
        )
        after_document["modified_ms"] = str(now_ms)
        after_document["doc_version"] = str(int(expected_doc_version) + 1)
        audit_json = self._rbac_audit_json(
            action_context=action_context,
            org=org,
            sup=sup,
            action="user_role_remove",
            resource_type="user_role_assignment",
            resource_id=f"{user_id}:{role_id}",
            before_document=before_document,
            after_document=after_document,
            before_version=int(expected_doc_version),
            after_version=int(expected_doc_version) + 1,
            changed_fields=("roles", "modified_ms", "doc_version"),
            role_ids_removed=(role_id,),
            timestamp_ms=now_ms,
        )
        result = int(self._rbac_remove_role_from_user(
            keys=[
                user_key,
                RK.rbac_user_meta(org, sup),
                RK.rbac_username_to_id(org, sup),
            ] + self._rbac_audit_keys(org),
            args=[
                role_id,
                str(now_ms),
                "superuser",
                RK.rbac_role_doc_prefix(org, sup),
                user_id,
                expected_roles,
                expected_doc_version,
                org,
                sup,
                audit_json,
            ],
        ) or 0)
        if result == -1:
            raise _rbac_denied_decision(
                "The default superuser must retain a superadmin role",
                cause="protected_identity",
            )
        if result == -2:
            raise _rbac_failure_decision(
                f"User {user_id} changed concurrently; retry the assignment removal"
            )
        if result == 0:
            self.rbac_append_attempt(
                org,
                sup,
                action="user_role_remove",
                resource_type="user_role_assignment",
                resource_id=_safe_rbac_audit_resource_id(
                    user_id, role_id, fallback="pending-user-role-assignment"
                ),
                namespace="user",
                outcome="no_change",
                cause="assignment_not_changed",
                action_context=action_context,
                before_document=before_document,
                before_version=int(expected_doc_version),
                conditions=[{
                    "kind": "assignment_membership",
                    "user_id": user_id,
                    "role_id": role_id,
                    "present": False,
                    "version": expected_doc_version,
                }],
            )
        return result == 1

    # ------------- Organization auth tokens (login tokens) -------------

    def _auth_token_commit(
        self,
        script: Any,
        *,
        keys: Sequence[str],
        args: Sequence[Any],
        org: str,
        action: str,
        token_id: str,
        action_context: Any,
    ) -> int:
        """Run one token mutation and evidence deterministic preflight faults."""
        try:
            return int(script(keys=list(keys), args=list(args)) or 0)
        except redis.exceptions.ResponseError as error:
            if not any(
                marker in str(error)
                for marker in _RBAC_DETERMINISTIC_INTEGRITY_MARKERS
            ):
                raise
            integrity_error = RbacIntegrityError(
                "Persisted auth-token state failed an atomic integrity preflight"
            )
            try:
                self.rbac_append_attempt(
                    org,
                    _AUTH_AUDIT_SUPER_NAME,
                    action=action,
                    resource_type="auth_token",
                    resource_id=token_id,
                    namespace="token",
                    outcome="failure",
                    cause="state_integrity_error",
                    action_context=action_context,
                    severity="critical",
                )
            except Exception as audit_error:
                raise audit_error from error
            self._rbac_mark_attempt_recorded(integrity_error)
            raise integrity_error from error

    def list_auth_tokens(self, org: str) -> List[Dict[str, Any]]:
        """List auth tokens for an organization (tokens are stored hashed; only token_id is returned)."""
        key = RK.auth_tokens(org)
        out: List[Dict[str, Any]] = []
        try:
            raw_map = self.r.hgetall(key) or {}
            for token_id, raw in raw_map.items():
                token_id_str = token_id if isinstance(token_id, str) else token_id.decode('utf-8')
                if token_id_str in _AUTH_TOKEN_META_FIELDS:
                    continue
                if not _AUTH_TOKEN_ID_RE.fullmatch(token_id_str):
                    raise RbacIntegrityError(
                        "Persisted auth-token identity is invalid"
                    )
                try:
                    meta = json.loads(raw) if raw else {}
                except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
                    raise RbacIntegrityError(
                        "Persisted auth-token metadata is invalid"
                    ) from exc
                if not isinstance(meta, dict) or meta.get("token_id") != token_id_str:
                    raise RbacIntegrityError(
                        "Persisted auth-token metadata does not match its identity"
                    )
                meta = dict(meta)
                meta.setdefault("token_id", token_id_str)
                out.append(meta)
            try:
                out.sort(
                    key=lambda x: int(x.get("created_ms") or 0),
                    reverse=True,
                )
            except (TypeError, ValueError, OverflowError) as exc:
                raise RbacIntegrityError(
                    "Persisted auth-token creation timestamp is invalid"
                ) from exc
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_auth_tokens error: {e}")
        return out

    def create_auth_token(
            self,
            org: str,
            created_by: str,
            label: Optional[str] = None,
            enabled: bool = True,
            username: str = "",
            user_id: str = "",
            expires_ms: Optional[int] = None,
            *,
            action_context: Any,
    ) -> Dict[str, Any]:
        """Create a new auth token.

        The plaintext token is returned ONLY once. Redis stores only token_id (sha256(token)).
        When ``username`` is provided, the token is linked to that user and
        login validation can enforce the username-token binding.

        ``expires_ms`` is an optional absolute epoch-ms expiry.  After that
        time, ``validate_auth_token_full`` returns None.  ``None`` (default)
        means the token never expires by time alone — it can still be
        disabled via ``enabled=False``.
        """
        normalized_context: Any = None
        context_error = False
        try:
            token = secrets.token_urlsafe(24)
            token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
            from supertable.audit.privileged import PrivilegedActionContext

            try:
                normalized_context = PrivilegedActionContext.coerce(
                    action_context
                )
                if normalized_context.context_missing:
                    raise ValueError(
                        "auth-token mutations require explicit actor context"
                    )
            except (TypeError, ValueError):
                context_error = True
                raise
            now_ms = _now_ms()
            meta = {
                "token_id": token_id,
                "created_ms": now_ms,
                "created_by": str(created_by or ""),
                "label": (str(label).strip() if label is not None else ""),
                "enabled": bool(enabled),
                "username": str(username or ""),
                "user_id": str(user_id or ""),
                "expires_ms": int(expires_ms) if expires_ms else 0,
            }
            metadata_json = json.dumps(
                meta, sort_keys=True, separators=(",", ":"),
            )
            audit_json = self._rbac_audit_json(
                action_context=normalized_context,
                org=org,
                sup=_AUTH_AUDIT_SUPER_NAME,
                action="token_create",
                resource_type="auth_token",
                resource_id=token_id,
                before_document=None,
                after_document={
                    "token_id": token_id,
                    "metadata_json": metadata_json,
                },
                before_version=0,
                after_version=1,
                changed_fields=("token",),
                timestamp_ms=now_ms,
                severity="critical",
            )
        except (TypeError, ValueError) as error:
            resource_id = locals().get("token_id", "pending-auth-token")
            try:
                self.rbac_append_attempt(
                    org,
                    _AUTH_AUDIT_SUPER_NAME,
                    action="token_create",
                    resource_type="auth_token",
                    resource_id=resource_id,
                    namespace="token",
                    outcome="denied",
                    cause=(
                        "missing_actor_context"
                        if context_error else "request_rejected"
                    ),
                    action_context=(
                        None if context_error else normalized_context
                    ),
                    severity="critical",
                )
            except Exception as audit_error:
                raise RbacAuditAttemptError(
                    "Auth-token creation rejection could not be durably recorded"
                ) from audit_error
            raise
        result = self._auth_token_commit(
            self._auth_create_token,
            keys=[
                RK.auth_tokens(org),
                self._auth_token_meta_key(org),
            ] + self._rbac_audit_keys(org),
            args=[
                token_id,
                metadata_json,
                str(now_ms),
                org,
                _AUTH_AUDIT_SUPER_NAME,
                audit_json,
            ],
            org=org,
            action="token_create",
            token_id=token_id,
            action_context=normalized_context,
        )
        if result == -1:
            existing = self.r.hget(RK.auth_tokens(org), token_id)
            if existing is None:
                raise RbacAuditConditionConflict(
                    "Auth-token identity collision changed concurrently; retry"
                )
            existing_text = self._rbac_text(existing)
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_create",
                resource_type="auth_token",
                resource_id=token_id,
                namespace="token",
                outcome="no_change",
                cause="token_identity_collision",
                action_context=normalized_context,
                severity="critical",
                conditions=[{
                    "kind": "token_present",
                    "metadata_json": existing_text,
                }],
            )
            error = RbacDuplicateIdentityError(
                "Generated auth-token identity already exists",
                outcome="no_change",
                cause="token_identity_collision",
                conditions=[{
                    "kind": "token_present",
                    "metadata_json": existing_text,
                }],
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result != 1:
            raise RuntimeError("Atomic auth-token creation did not commit")
        return {"token": token, **meta}

    def delete_auth_token(
        self,
        org: str,
        token_id: str,
        *,
        action_context: Any,
    ) -> bool:
        """Delete an auth token by token_id (sha256)."""
        from supertable.audit.privileged import PrivilegedActionContext

        try:
            normalized_context = PrivilegedActionContext.coerce(action_context)
            if normalized_context.context_missing:
                raise ValueError(
                    "auth-token mutations require explicit actor context"
                )
        except (TypeError, ValueError) as error:
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_delete",
                resource_type="auth_token",
                resource_id=_safe_rbac_audit_resource_id(
                    token_id, fallback="pending-auth-token"
                ),
                namespace="token",
                outcome="denied",
                cause="missing_actor_context",
                action_context=None,
                severity="critical",
            )
            self._rbac_mark_attempt_recorded(error)
            raise
        if not isinstance(token_id, str) or not _AUTH_TOKEN_ID_RE.fullmatch(token_id):
            error = ValueError("token_id must be a lowercase SHA-256 digest")
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_delete",
                resource_type="auth_token",
                resource_id=_safe_rbac_audit_resource_id(
                    token_id, fallback="pending-auth-token"
                ),
                namespace="token",
                outcome="denied",
                cause="request_rejected",
                action_context=normalized_context,
                severity="critical",
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        key = RK.auth_tokens(org)
        raw = self.r.hget(key, token_id)
        if raw is None:
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_delete",
                resource_type="auth_token",
                resource_id=token_id,
                namespace="token",
                outcome="no_change",
                cause="resource_missing",
                action_context=normalized_context,
                severity="critical",
                conditions=[{"kind": "token_absent"}],
            )
            return False
        expected_metadata = self._rbac_text(raw)
        now_ms = _now_ms()
        audit_json = self._rbac_audit_json(
            action_context=normalized_context,
            org=org,
            sup=_AUTH_AUDIT_SUPER_NAME,
            action="token_delete",
            resource_type="auth_token",
            resource_id=token_id,
            before_document={
                "token_id": token_id,
                "metadata_json": expected_metadata,
            },
            after_document=None,
            before_version=1,
            after_version=0,
            changed_fields=("token",),
            timestamp_ms=now_ms,
            severity="critical",
        )
        result = self._auth_token_commit(
            self._auth_delete_token,
            keys=[
                key,
                self._auth_token_meta_key(org),
            ] + self._rbac_audit_keys(org),
            args=[
                token_id,
                expected_metadata,
                str(now_ms),
                org,
                _AUTH_AUDIT_SUPER_NAME,
                audit_json,
            ],
            org=org,
            action="token_delete",
            token_id=token_id,
            action_context=normalized_context,
        )
        if result == 0:
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_delete",
                resource_type="auth_token",
                resource_id=token_id,
                namespace="token",
                outcome="no_change",
                cause="resource_disappeared",
                action_context=normalized_context,
                severity="critical",
                conditions=[{"kind": "token_absent"}],
            )
            return False
        if result == -1:
            error = RbacDecisionError(
                "Auth-token metadata changed concurrently; retry deletion",
                outcome="failure",
                cause="concurrent_modification",
                severity="critical",
            )
            self.rbac_append_attempt(
                org,
                _AUTH_AUDIT_SUPER_NAME,
                action="token_delete",
                resource_type="auth_token",
                resource_id=token_id,
                namespace="token",
                outcome="failure",
                cause="concurrent_modification",
                action_context=normalized_context,
                severity="critical",
            )
            self._rbac_mark_attempt_recorded(error)
            raise error
        if result != 1:
            raise RuntimeError("Atomic auth-token deletion did not commit")
        return True

    def validate_auth_token(self, org: str, token: str) -> bool:
        """Validate through the same state/expiry path as metadata callers."""
        return self.validate_auth_token_full(org, token) is not None

    def validate_auth_token_full(self, org: str, token: str) -> Optional[Dict[str, Any]]:
        """Validate a plaintext auth token and return its metadata.

        Returns the token metadata dict (including ``username``, ``user_id``)
        if the token exists, is ``enabled=True``, and has not expired.
        Returns ``None`` for any failure: missing, disabled, expired, or
        malformed metadata.
        """
        if not token:
            return None
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        try:
            raw = self.r.hget(RK.auth_tokens(org), token_id)
            if not raw:
                return None
            raw_str = raw if isinstance(raw, str) else raw.decode("utf-8")
            meta = json.loads(raw_str)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] validate_auth_token_full error: {e}")
            return None
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(meta, dict):
            return None
        # Missing means enabled for pre-field documents, but malformed values
        # must not gain truthiness (for example, the string ``"false"``).
        if "enabled" in meta and meta.get("enabled") is not True:
            return None
        # expiry — 0 / missing means "never expires"
        try:
            raw_expiry = meta.get("expires_ms", 0)
            if isinstance(raw_expiry, bool):
                return None
            exp = int(raw_expiry or 0)
        except (TypeError, ValueError, OverflowError):
            return None
        if exp < 0:
            return None
        if exp and exp <= _now_ms():
            return None
        return meta

    # ------------- Listings via SCAN -------------

    def _scan_leaf_keys_raw(
            self,
            org: str,
            sup: str,
            *,
            allowed: Optional[frozenset[str]],
            count: int,
    ) -> Iterator[str]:
        """Enumerate one physical namespace without resolving it again."""
        pattern = RK.meta_leaf_pattern(org, sup)
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(
                    cursor=cursor, match=pattern, count=count,
                )
                for key in keys:
                    key_text = (
                        key if isinstance(key, str) else key.decode("utf-8")
                    )
                    if allowed is not None:
                        simple = key_text.rsplit("meta:leaf:doc:", 1)[-1]
                        if simple not in allowed:
                            continue
                    yield key_text
                if cursor == 0:
                    break
        except redis.RedisError as exc:
            logger.error("[redis-catalog] SCAN error: %s", exc)
            raise

    def _pin_leaf_scan(
            self,
            org: str,
            sup: str,
            info: Optional[tuple],
    ) -> Dict[str, Any]:
        """Atomically pin target and physical-source lifecycle documents."""
        effective_sup = info[0] if info else sup
        keys = [
            RK.meta_root(org, sup),
            RK.meta_namespace_deletion_intent(org, sup),
        ]
        if effective_sup != sup:
            keys.extend([
                RK.meta_root(org, effective_sup),
                RK.meta_namespace_deletion_intent(org, effective_sup),
            ])
        try:
            with self.r.pipeline(transaction=True) as pipe:
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()
        except redis.RedisError as exc:
            logger.error("[redis-catalog] leaf-scan lifecycle pin error: %s", exc)
            raise
        if len(values) != len(keys):
            raise RuntimeError("Redis returned an incomplete lifecycle pin")

        target_raw, target_intent = values[:2]
        if target_intent:
            raise DeletionIntentConflictError(
                f"Durable deletion intent fences {org}/{sup}"
            )
        if not target_raw:
            raise RuntimeError(
                f"Missing catalog root while enumerating {org}/{sup}"
            )
        target_root = self._decode_root_snapshot(
            target_raw, org=org, sup=sup,
        )
        pinned_binding = self._replica_binding_from_root(
            target_root, org=org, sup=sup,
        )
        if pinned_binding != info:
            raise SnapshotCommitConflictError(
                f"Catalog changed while pinning {org}/{sup}"
            )

        if effective_sup == sup:
            source_raw = target_raw
            source_intent = target_intent
        else:
            source_raw, source_intent = values[2:]
            if source_intent:
                raise DeletionIntentConflictError(
                    f"Replica source is fenced for deletion: "
                    f"{org}/{effective_sup}"
                )
            if not source_raw:
                raise RuntimeError(
                    f"Replica {org}/{sup} refers to a missing source namespace"
                )
            source_root = self._decode_root_snapshot(
                source_raw, org=org, sup=effective_sup,
            )
            if source_root.get("clone_type") == "replica":
                raise RuntimeError(
                    f"Replica {org}/{sup} cannot reference another replica"
                )

        return {
            "keys": tuple(keys),
            "values": tuple(values),
            "effective_sup": effective_sup,
            "allowed": info[1] if info else None,
            "source_root": source_raw,
        }

    def _verify_leaf_scan_pin(
            self, *, org: str, effective_sup: str, pin: Mapping[str, Any],
    ) -> None:
        keys = list(pin["keys"])
        try:
            with self.r.pipeline(transaction=True) as pipe:
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()
        except redis.RedisError as exc:
            logger.error("[redis-catalog] leaf-scan lifecycle recheck error: %s", exc)
            raise
        if tuple(values) != tuple(pin["values"]):
            raise SnapshotCommitConflictError(
                f"Catalog changed while enumerating {org}/{effective_sup}; "
                "refusing a partial snapshot"
            )

    def scan_leaf_keys(
            self,
            org: str,
            sup: str,
            count: int = 1000,
            *,
            resolve_replica: bool = True,
    ) -> Iterator[str]:
        """Yield full leaf keys, optionally resolving a replica source.

        Namespace deletion passes ``resolve_replica=False``: it must enumerate
        only children physically owned by the namespace being removed, and
        recovery must remain able to sweep a stale/corrupt root document.
        """
        batch_size = max(1, int(count))
        info = self._resolve_replica_info(org, sup) if resolve_replica else None
        effective_sup = info[0] if info else sup
        allowed = info[1] if info else None
        yield from self._scan_leaf_keys_raw(
            org, effective_sup, allowed=allowed, count=batch_size,
        )

    def scan_leaf_items(self, org: str, sup: str, count: int = 1000) -> Iterator[Dict]:
        """Iterate one root-generation-consistent set of leaf documents.

        Redis SCAN is incremental and may otherwise return a successful partial
        table set when a page/pipeline fails or a writer changes the catalog
        mid-enumeration.  Reads must fail/retry instead of silently omitting
        physical tables (and their deletion vectors).
        """
        batch_size = max(1, int(count))
        info = self._resolve_replica_info(org, sup)
        pin = self._pin_leaf_scan(org, sup, info)
        effective_sup = str(pin["effective_sup"])
        batch: List[str] = []
        for key in self._scan_leaf_keys_raw(
                org,
                effective_sup,
                allowed=pin["allowed"],
                count=batch_size,
        ):
            batch.append(key)
            if len(batch) >= batch_size:
                yield from self._fetch_batch(batch)
                batch = []
        if batch:
            yield from self._fetch_batch(batch)
        self._verify_leaf_scan_pin(
            org=org, effective_sup=effective_sup, pin=pin,
        )

    def _fetch_batch(self, keys: List[str]) -> Iterator[Dict]:
        try:
            with self.r.pipeline() as p:
                for k in keys:
                    p.get(k)
                vals = p.execute()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] pipeline GET error: {e}")
            raise

        if len(vals) != len(keys):
            raise RuntimeError("Redis leaf batch returned an incomplete result")
        for k, raw in zip(keys, vals):
            if not raw:
                raise RuntimeError(
                    f"Catalog leaf disappeared during snapshot enumeration: {k}"
                )
            try:
                obj = _validate_leaf_document(json.loads(raw))
                simple = k.rsplit("meta:leaf:doc:", 1)[-1]
                item = {
                    "simple": simple,
                    "version": obj["version"],
                    "ts": obj["ts"],
                    "path": obj["path"],
                    "payload": obj.get("payload"),
                    # Linked-share policy overlays have existed beside the
                    # cached payload as well as inside it. Preserve the raw
                    # marker (including malformed non-null values) so the read
                    # boundary can fail closed instead of silently dropping it.
                    "_row_filter": obj.get("_row_filter"),
                }
                yield item
            except Exception as exc:
                raise RuntimeError(f"Malformed catalog leaf {k}") from exc

    @staticmethod
    def _quality_key(org: str, sup: str, *parts: str) -> str:
        return RK.quality_prefix(org, sup) + ":".join(parts)

    @classmethod
    def _quality_table_mutable_keys(
            cls, org: str, sup: str, simple: str,
    ) -> List[str]:
        """Return the finite mutable DQ state owned by one table.

        Column-level ``latest`` documents are dynamic and are removed through
        a separately bounded, deletion-intent-fenced scan.  Immutable delivered
        history, the shared durable history outbox, and name-scoped custom-rule
        policy deliberately survive.
        """
        keys = [
            cls._quality_key(org, sup, "config", simple),
            cls._quality_key(org, sup, "schedule", simple),
            cls._quality_key(org, sup, "latest", simple),
            cls._quality_key(org, sup, "anomalies", simple),
            cls._quality_key(org, sup, "pending", simple),
            cls._quality_key(org, sup, "pending_unresolved", simple),
            cls._quality_key(org, sup, "running", simple),
            cls._quality_key(org, sup, "cooldown", simple),
        ]
        for mode in cls._QUALITY_MODES:
            keys.extend([
                cls._quality_key(org, sup, "pending_mode", simple, mode),
                cls._quality_key(org, sup, "cooldown", simple, mode),
                cls._quality_key(org, sup, "retry", simple, mode),
                cls._quality_key(org, sup, "cron_state", simple, mode),
            ])
        return keys

    @staticmethod
    def _redis_key_text(raw: Any) -> str:
        if isinstance(raw, str):
            return raw
        if isinstance(raw, bytes):
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError("Redis returned a non-UTF-8 quality key") from exc
        raise RuntimeError(
            f"Redis returned an invalid quality key type: {type(raw).__name__}"
        )

    def _delete_simple_quality_column_keys(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            lock_token: str,
            namespace_token: str,
            intent_id: str,
    ) -> None:
        """Restart-safely remove bounded ``latest:{table}:*`` documents."""
        prefix = self._quality_key(org, sup, "latest", simple) + ":"
        pattern = prefix + "*"
        seen: set[str] = set()
        scan_calls = 0

        # Deleting while SCAN walks Redis can move buckets.  Complete passes
        # are repeated until one observes no matching keys.  Both discovery
        # and server round trips have explicit bounds; a failure leaves the
        # durable tombstone in place so recovery can safely resume.
        while True:
            cursor = 0
            found_this_pass = False
            while True:
                scan_calls += 1
                if scan_calls > self._QUALITY_DYNAMIC_SCAN_CALL_LIMIT:
                    raise RuntimeError(
                        "Quality column-key cleanup exceeded its scan-call bound"
                    )
                cursor, raw_keys = self.r.scan(
                    cursor=cursor,
                    match=pattern,
                    count=self._QUALITY_DYNAMIC_SCAN_COUNT,
                )
                candidates: List[str] = []
                for raw_key in raw_keys:
                    key = self._redis_key_text(raw_key)
                    if not key.startswith(prefix):
                        raise RuntimeError(
                            "Redis quality scan returned a key outside its table prefix"
                        )
                    seen.add(key)
                    if len(seen) > self._QUALITY_DYNAMIC_KEY_LIMIT:
                        raise RuntimeError(
                            "Quality column-key cleanup exceeded its key bound"
                        )
                    candidates.append(key)

                for offset in range(0, len(candidates), self._QUALITY_DYNAMIC_SCAN_COUNT):
                    chunk = candidates[
                        offset:offset + self._QUALITY_DYNAMIC_SCAN_COUNT
                    ]
                    result = int(self._delete_simple_quality_keys(
                        keys=[
                            RK.lock_namespace(org, sup),
                            RK.lock_leaf(org, sup, simple),
                            RK.meta_namespace_deletion_intent(org, sup),
                            RK.meta_simple_deletion_intent(org, sup, simple),
                            *chunk,
                        ],
                        args=[
                            namespace_token or "",
                            lock_token or "",
                            intent_id or "",
                            prefix,
                        ],
                    ) or 0)
                    if result == -1:
                        raise LockLostError(
                            "Lost lock while cleaning table quality state"
                        )
                    if result == -2:
                        raise DeletionIntentConflictError(
                            "Deletion intent changed while cleaning table quality state"
                        )
                    if result == -3:
                        raise RuntimeError(
                            f"Corrupt deletion intent for {org}/{sup}/{simple}"
                        )
                    if result == -4:
                        raise RuntimeError(
                            "Unsafe quality column key returned by Redis scan"
                        )
                    if result < 0:
                        raise RuntimeError(
                            f"Invalid quality cleanup result: {result}"
                        )
                if candidates:
                    found_this_pass = True
                cursor = int(cursor)
                if cursor == 0:
                    break
            if not found_this_pass:
                return

    def _assert_simple_quality_column_keys_absent(
            self, org: str, sup: str, simple: str,
    ) -> None:
        """Fail closed if dynamic quality state remains before tombstone clear."""
        prefix = self._quality_key(org, sup, "latest", simple) + ":"
        pattern = prefix + "*"
        cursor = 0
        calls = 0
        while True:
            calls += 1
            if calls > self._QUALITY_DYNAMIC_SCAN_CALL_LIMIT:
                raise RuntimeError(
                    "Quality column-key verification exceeded its scan-call bound"
                )
            cursor, raw_keys = self.r.scan(
                cursor=cursor,
                match=pattern,
                count=self._QUALITY_DYNAMIC_SCAN_COUNT,
            )
            for raw_key in raw_keys:
                key = self._redis_key_text(raw_key)
                if not key.startswith(prefix):
                    raise RuntimeError(
                        "Redis quality scan returned a key outside its table prefix"
                    )
                raise RuntimeError(
                    "Dynamic table quality state remains after deletion"
                )
            cursor = int(cursor)
            if cursor == 0:
                return

    # ------------- Deletions (dangerous) -------------

    def delete_simple_table(
            self,
            org: str,
            sup: str,
            simple: str,
            *,
            lock_token: str,
            namespace_token: str,
            intent_id: str,
    ) -> bool:
        """Atomically remove every table-scoped catalog/control record.

        Storage must already be empty and the caller must still own the same
        auto-renewed leaf lock used by writers. The table-name index, schema,
        row-id allocator, runtime config, mirror-recovery intent, and mutable
        quality state are removed with the leaf so delete/recreate cannot
        inherit stale operational state. Immutable delivered quality history,
        its durable outbox, and user-authored name-scoped rules survive.
        """
        if not (
            org and sup and simple and lock_token and namespace_token
            and intent_id
        ):
            return False
        try:
            self._delete_simple_quality_column_keys(
                org,
                sup,
                simple,
                lock_token=lock_token,
                namespace_token=namespace_token,
                intent_id=intent_id,
            )
            result = int(self._delete_simple_table(
                keys=[
                    RK.meta_leaf(org, sup, simple),
                    RK.lock_leaf(org, sup, simple),
                    RK.meta_table_names(org, sup),
                    RK.schema(org, sup, simple),
                    RK.meta_rowid_seq(org, sup, simple),
                    RK.meta_table_config(org, sup, simple),
                    RK.meta_mirror_publication(org, sup, simple),
                    RK.meta_root(org, sup),
                    RK.meta_simple_deletion_intent(org, sup, simple),
                    RK.meta_simple_deletion_intent_index(org, sup),
                    RK.lock_namespace(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    *self._quality_table_mutable_keys(org, sup, simple),
                ],
                args=[
                    lock_token,
                    simple,
                    namespace_token,
                    intent_id,
                    _now_ms(),
                ],
            ))
            if result == -1:
                logger.error(
                    "[redis-catalog] delete_simple_table lost its leaf lock"
                )
                return False
            if result == -2:
                raise RuntimeError("table-name index has wrong Redis type")
            if result == -3:
                raise RuntimeError("catalog root is corrupt")
            if result == -4:
                logger.error(
                    "[redis-catalog] delete_simple_table lost its namespace lock"
                )
                return False
            if result == -5:
                raise DeletionIntentConflictError(
                    f"SuperTable deletion supersedes {org}/{sup}/{simple}"
                )
            if result == -6:
                logger.error(
                    "[redis-catalog] delete_simple_table intent ownership changed"
                )
                return False
            if result == -7:
                raise RuntimeError(
                    f"Corrupt deletion intent for {org}/{sup}/{simple}"
                )
            if result == -8:
                raise RuntimeError(
                    "simple deletion-intent index has wrong Redis type"
                )
            if result == -9:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result < 0:
                raise RuntimeError(
                    f"invalid delete_simple_table result: {result}"
                )
            return True
        except LockLostError as e:
            logger.error(f"[redis-catalog] delete_simple_table error: {e}")
            return False
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_simple_table error: {e}")
            return False

    def delete_super_table(
            self, org: str, sup: str, count: int = 1000,
            *, namespace_token: str, intent_id: str,
    ) -> int:
        """Delete data/metadata keys for a SuperTable, preserving RBAC state.

        Role, user, and assignment state is security-control data.  Removing it
        through this generic SCAN path would bypass the mandatory privileged
        audit ledger.  It therefore survives data-namespace deletion and can
        only be changed through the audited RBAC APIs.

        This is implemented via SCAN to avoid blocking Redis.
        Returns the number of keys deleted (best-effort).
        """
        if not (org and sup and namespace_token and intent_id):
            return 0
        pattern = RK.super_table_pattern(org, sup)
        # RBAC is security-control state. Locks and the no-TTL intent must stay
        # visible throughout every SCAN batch; removing any of them early
        # would let a stale caller finish after lease takeover.
        preserved = (
            RK.rbac_scope(org, sup) + ":",
            RK.lock_scope_prefix(org, sup),
            RK.meta_deletion_scope_prefix(org, sup),
        )
        deleted = self._delete_by_scan(
            pattern=pattern,
            count=count,
            preserve_prefixes=preserved,
            strict=True,
            namespace_lock=RK.lock_namespace(org, sup),
            namespace_intent=RK.meta_namespace_deletion_intent(org, sup),
            namespace_token=namespace_token,
            intent_id=intent_id,
        )
        result = int(self._finalize_namespace_deletion(
            keys=[
                RK.lock_namespace(org, sup),
                RK.meta_namespace_deletion_intent(org, sup),
                RK.meta_simple_deletion_intent_index(org, sup),
                RK.meta_stage_deletion_intent_index(org, sup),
            ],
            args=[namespace_token, intent_id, _now_ms()],
        ) or 0)
        if result == -1:
            raise LockLostError(
                f"Lost namespace lock before finalizing deletion of {org}/{sup}"
            )
        if result == -2:
            raise DeletionIntentConflictError(
                f"Namespace deletion intent changed for {org}/{sup}"
            )
        if result == -3:
            raise RuntimeError(f"Corrupt deletion intent for {org}/{sup}")
        if result == -4:
            raise DeletionIntentConflictError(
                "SimpleTable deletion intents remain under this namespace"
            )
        if result != 1:
            raise RuntimeError(
                f"Invalid namespace deletion finalizer result: {result}"
            )
        return deleted

    def _delete_by_scan(
        self,
        pattern: str,
        count: int = 1000,
        *,
        preserve_prefixes: tuple[str, ...] = (),
        strict: bool = False,
        namespace_lock: str = "",
        namespace_intent: str = "",
        namespace_token: str = "",
        intent_id: str = "",
    ) -> int:
        deleted = 0
        try:
            # Deleting while iterating SCAN can move buckets and omit keys.
            # Repeat complete passes until one observes no deletable key. The
            # durable intent prevents supported writers from replenishing the
            # namespace between passes.
            while True:
                cursor = 0
                candidates = 0
                while True:
                    cursor, keys = self.r.scan(
                        cursor=cursor,
                        match=pattern,
                        count=max(1, int(count)),
                    )
                    str_keys = [
                        k if isinstance(k, str) else k.decode("utf-8")
                        for k in (keys or [])
                    ]
                    if preserve_prefixes:
                        str_keys = [
                            key
                            for key in str_keys
                            if not any(
                                key.startswith(prefix)
                                for prefix in preserve_prefixes
                            )
                        ]
                    if str_keys:
                        candidates += len(str_keys)
                        if not (
                            namespace_lock and namespace_intent
                            and namespace_token and intent_id
                        ):
                            raise RuntimeError(
                                "Strict namespace cleanup requires a deletion fence"
                            )
                        raw = int(self._delete_namespace_batch(
                            keys=[
                                namespace_lock,
                                namespace_intent,
                                *str_keys,
                            ],
                            args=[namespace_token, intent_id],
                        ) or 0)
                        if raw == -1:
                            raise LockLostError(
                                "Lost namespace lock during catalog cleanup"
                            )
                        if raw == -2:
                            raise DeletionIntentConflictError(
                                "Namespace deletion intent ownership changed"
                            )
                        if raw == -3:
                            raise RuntimeError("Corrupt namespace deletion intent")
                        if raw < 0:
                            raise RuntimeError(
                                f"Invalid namespace batch deletion result: {raw}"
                            )
                        deleted += raw
                    if cursor == 0:
                        break
                if candidates == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] SCAN delete error: {e}")
            if strict:
                raise
        return deleted

    # --------------------------------------------------------------------------- #
    # Data Sharing — provider-side share definitions
    # --------------------------------------------------------------------------- #

    @staticmethod
    def _decode_control_object(raw: Any, *, description: str) -> Dict[str, Any]:
        try:
            document = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Corrupt {description}") from exc
        if not isinstance(document, dict):
            raise RuntimeError(f"Corrupt {description}")
        return document

    @staticmethod
    def _decode_index_member(raw: Any, *, description: str) -> str:
        if isinstance(raw, str):
            value = raw
        elif isinstance(raw, bytes):
            try:
                value = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError(f"Corrupt {description}") from exc
        else:
            raise RuntimeError(f"Corrupt {description}")
        if not value:
            raise RuntimeError(f"Corrupt {description}")
        return value

    def _bounded_set_members(
        self,
        key: str,
        *,
        limit: int,
        description: str,
    ) -> List[str]:
        """Read a Redis set without materializing an attacker-sized index."""
        safe_limit = _lua_safe_integer(limit, field=description, minimum=1)
        try:
            cardinality = int(self.r.scard(key) or 0)
            if cardinality < 0 or cardinality > safe_limit:
                raise RuntimeError(f"{description} exceeds its safety limit")
            cursor = 0
            visited_cursors: set[int] = set()
            decoded: set[str] = set()
            observations = 0
            call_budget = max(
                64,
                min(4096, ((safe_limit + 63) // 64) * 8 + 64),
            )
            for _call in range(call_budget):
                raw_cursor, raw_members = self.r.sscan(
                    key,
                    cursor=cursor,
                    count=min(128, safe_limit + 1),
                )
                try:
                    next_cursor = int(raw_cursor)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise RuntimeError(f"Corrupt {description}") from exc
                if next_cursor < 0 or next_cursor > (1 << 64) - 1:
                    raise RuntimeError(f"Corrupt {description}")
                if not isinstance(raw_members, (list, tuple, set)):
                    raise RuntimeError(f"Corrupt {description}")
                observations += len(raw_members)
                if observations > safe_limit * 8 + 1024:
                    raise RuntimeError(f"Corrupt or unstable {description}")
                for raw_member in raw_members:
                    decoded.add(self._decode_index_member(
                        raw_member, description=description,
                    ))
                    if len(decoded) > safe_limit:
                        raise RuntimeError(
                            f"{description} exceeds its safety limit"
                        )
                if next_cursor == 0:
                    # Require one stable authoritative view. A concurrent
                    # add/remove (including a same-cardinality replacement
                    # that exposes both generations during SSCAN) must not
                    # silently produce a partial control-plane index.
                    if len(decoded) != cardinality:
                        raise RuntimeError(f"Corrupt or unstable {description}")
                    return sorted(decoded)
                if next_cursor == cursor or next_cursor in visited_cursors:
                    raise RuntimeError(f"Corrupt or unstable {description}")
                visited_cursors.add(next_cursor)
                cursor = next_cursor
            raise RuntimeError(f"Corrupt or unstable {description}")
        except redis.RedisError as exc:
            logger.error("[redis-catalog] bounded set scan failed: %s", exc)
            raise

    def create_share(self, org: str, share_id: str, share_doc: Dict[str, Any]) -> None:
        """Create a share definition and its index entry atomically."""
        if not isinstance(share_doc, dict):
            raise TypeError("Share document must be a JSON object")
        try:
            result = int(self._mutate_share(
                keys=[RK.share_doc(org, share_id), RK.share_index(org)],
                args=[json.dumps(share_doc), share_id, "create"],
            ) or 0)
            if result == -1:
                raise FileExistsError(f"Share already exists: {org}/{share_id}")
            if result == -2:
                raise RuntimeError(f"Corrupt share metadata/index for {org}")
            if result == -3:
                raise ValueError("Share document is not valid JSON")
            if result != 1:
                raise RuntimeError(f"Invalid share creation result: {result}")
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_share error: {e}")
            raise

    def allocate_share_manifest_generation(
        self, org: str, share_id: str, incarnation: str,
    ) -> int:
        """Allocate a Redis-clock monotonic issuance value for one share token.

        Token-hash scoping prevents a revoked/recreated share id from
        inheriting the prior incarnation's ordering state.
        """
        if not isinstance(incarnation, str) or re.fullmatch(
            r"[0-9a-f]{64}", incarnation,
        ) is None:
            raise ValueError("share incarnation is invalid")
        key = (
            RK.share_doc(org, share_id)
            + ":manifest_generation:"
            + incarnation
        )
        try:
            result = int(self._allocate_share_manifest_generation(
                keys=[key], args=[],
            ) or 0)
            if result == -1:
                raise OverflowError("share manifest generation overflow")
            if result == -2:
                raise RuntimeError("Corrupt share manifest generation state")
            if result <= 0 or result > _REDIS_LUA_MAX_SAFE_INTEGER:
                raise RuntimeError("Invalid share manifest generation")
            return result
        except redis.RedisError as exc:
            logger.error("[redis-catalog] share manifest generation error: %s", exc)
            raise

    def update_share(
            self, org: str, share_id: str, share_doc: Dict[str, Any],
    ) -> bool:
        """Replace an existing, indexed share without creating an orphan."""
        if not isinstance(share_doc, dict):
            raise TypeError("Share document must be a JSON object")
        try:
            result = int(self._mutate_share(
                keys=[RK.share_doc(org, share_id), RK.share_index(org)],
                args=[json.dumps(share_doc), share_id, "update"],
            ) or 0)
            if result == 0:
                return False
            if result == -2:
                raise RuntimeError(f"Corrupt share metadata/index for {org}")
            if result == -3:
                raise ValueError("Share document is not valid JSON")
            if result != 1:
                raise RuntimeError(f"Invalid share update result: {result}")
            return True
        except redis.RedisError as exc:
            logger.error("[redis-catalog] update_share error: %s", exc)
            raise

    def get_share(self, org: str, share_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(RK.share_doc(org, share_id))
            if raw is None:
                return None
            return self._decode_control_object(
                raw, description=f"share metadata for {org}/{share_id}",
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_share error: {e}")
            raise

    def delete_share(self, org: str, share_id: str) -> bool:
        try:
            result = int(self._mutate_share(
                keys=[RK.share_doc(org, share_id), RK.share_index(org)],
                args=["", share_id, "delete"],
            ) or 0)
            if result == -2:
                raise RuntimeError(f"Corrupt share metadata/index for {org}")
            if result < 0:
                raise RuntimeError(f"Invalid share deletion result: {result}")
            return result == 1
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_share error: {e}")
            raise

    def list_shares(
        self, org: str, *, limit: int = 10_000,
    ) -> List[Dict[str, Any]]:
        shares: List[Dict[str, Any]] = []
        try:
            members = self._bounded_set_members(
                RK.share_index(org),
                limit=limit,
                description=f"share index for {org}",
            )
            for sid in members:
                raw = self.r.get(RK.share_doc(org, sid))
                if raw is None:
                    raise RuntimeError(
                        f"Share index references missing metadata for {org}/{sid}"
                    )
                shares.append(self._decode_control_object(
                    raw, description=f"share metadata for {org}/{sid}",
                ))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_shares error: {e}")
            raise
        return shares

    # --------------------------------------------------------------------------- #
    # Data Sharing — consumer-side linked shares
    # --------------------------------------------------------------------------- #

    def create_linked_share(
        self,
        org: str,
        sup: str,
        link_id: str,
        link_doc: Dict[str, Any],
        *,
        not_after_ms: Optional[int] = None,
    ) -> None:
        if not isinstance(link_doc, dict):
            raise TypeError("Linked-share document must be a JSON object")
        publication_deadline = (
            0 if not_after_ms is None else _publication_timestamp(not_after_ms)
        )
        try:
            result = int(self._upsert_linked_share_fenced(
                keys=[
                    RK.linked_share_doc(org, sup, link_id),
                    RK.linked_share_index(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                    self._linked_provider_reservation_key(org, sup, link_id),
                    self._linked_unlink_tombstone_key(org, sup, link_id),
                ],
                args=[
                    json.dumps(link_doc), link_id, "create", publication_deadline,
                ],
            ) or 0)
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise ValueError("Linked-share document is not valid JSON")
            if result == -3:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -4:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -5:
                raise RuntimeError(
                    f"Corrupt linked-share metadata/index for {org}/{sup}"
                )
            if result == -6:
                raise FileExistsError(
                    f"Linked share already exists: {org}/{sup}/{link_id}"
                )
            if result == -7:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result == -8:
                raise TimeoutError(
                    "Linked-share publication deadline was exceeded"
                )
            if result == -12:
                raise FileNotFoundError("Linked share is unlinked")
            if result == -13:
                raise SnapshotCommitConflictError(
                    "Linked provider publication reservation changed"
                )
            if result == -14:
                raise SnapshotCommitConflictError(
                    "Linked-share instance identity changed"
                )
            if result != 1:
                raise RuntimeError(
                    f"Invalid linked-share publication result: {result}"
                )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_linked_share error: {e}")
            raise

    def get_linked_share(self, org: str, sup: str, link_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(RK.linked_share_doc(org, sup, link_id))
            if raw is None:
                return None
            return self._decode_control_object(
                raw,
                description=(
                    f"linked-share metadata for {org}/{sup}/{link_id}"
                ),
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_linked_share error: {e}")
            raise

    def get_authoritative_linked_share(
        self, org: str, sup: str, link_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Atomically read an active, indexed linked-share control document.

        A bare document is not authority: the matching index membership must
        exist in the same Redis execution, and any durable unlink tombstone
        denies the read. Consistent absence returns ``None``; partial/corrupt
        state raises instead of being confused with a never-created link.
        """
        if (
            not isinstance(link_id, str)
            or not link_id
            or len(link_id.encode("utf-8")) > 1024
            or "\x00" in link_id
        ):
            raise ValueError("Linked-share identity is invalid")
        try:
            raw = self._get_authoritative_linked_share(
                keys=[
                    RK.linked_share_doc(org, sup, link_id),
                    RK.linked_share_index(org, sup),
                    self._linked_unlink_tombstone_key(org, sup, link_id),
                ],
                args=[link_id],
            )
            if not isinstance(raw, (list, tuple)) or not raw:
                raise RuntimeError("Invalid authoritative linked-share result")
            result = int(raw[0])
            if result == 0:
                return None
            if result == -2:
                raise RuntimeError(
                    f"Corrupt linked-share authority for {org}/{sup}/{link_id}"
                )
            if result == -3:
                raise FileNotFoundError(
                    f"Linked share is unlinked: {org}/{sup}/{link_id}"
                )
            if result != 1 or len(raw) != 2:
                raise RuntimeError("Invalid authoritative linked-share result")
            return self._decode_control_object(
                raw[1],
                description=(
                    f"authoritative linked-share metadata for "
                    f"{org}/{sup}/{link_id}"
                ),
            )
        except redis.RedisError as exc:
            logger.error(
                "[redis-catalog] authoritative linked-share read error: %s",
                exc,
            )
            raise

    def update_linked_share(
        self,
        org: str,
        sup: str,
        link_id: str,
        doc: Dict[str, Any],
        *,
        not_after_ms: Optional[int] = None,
    ) -> bool:
        if not isinstance(doc, dict):
            raise TypeError("Linked-share document must be a JSON object")
        publication_deadline = (
            0 if not_after_ms is None else _publication_timestamp(not_after_ms)
        )
        try:
            result = int(self._upsert_linked_share_fenced(
                keys=[
                    RK.linked_share_doc(org, sup, link_id),
                    RK.linked_share_index(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                    self._linked_provider_reservation_key(org, sup, link_id),
                    self._linked_unlink_tombstone_key(org, sup, link_id),
                ],
                args=[
                    json.dumps(doc), link_id, "update", publication_deadline,
                ],
            ) or 0)
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise ValueError("Linked-share document is not valid JSON")
            if result == -3:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -4:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -5:
                raise RuntimeError(
                    f"Corrupt linked-share metadata/index for {org}/{sup}"
                )
            if result == -7:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result == -8:
                raise TimeoutError(
                    "Linked-share publication deadline was exceeded"
                )
            if result == -9:
                raise SnapshotCommitConflictError(
                    "A newer linked-share publication already committed"
                )
            if result == -10:
                raise SnapshotCommitConflictError(
                    "A newer provider manifest already committed"
                )
            if result == -11:
                raise SnapshotCommitConflictError(
                    "Provider manifest generation is ambiguous"
                )
            if result == -12:
                raise FileNotFoundError("Linked share is unlinked")
            if result == -13:
                raise SnapshotCommitConflictError(
                    "Linked provider publication reservation changed"
                )
            if result == -14:
                raise SnapshotCommitConflictError(
                    "Linked-share instance identity changed"
                )
            if result == 0:
                return False
            if result != 1:
                raise RuntimeError(
                    f"Invalid linked-share publication result: {result}"
                )
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] update_linked_share error: {e}")
            raise

    def begin_unlink_linked_share(
        self, org: str, sup: str, link_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Atomically tombstone a link and return cleanup-authoritative state."""
        try:
            raw = self._delete_linked_share_fenced(
                keys=[
                    RK.linked_share_doc(org, sup, link_id),
                    RK.linked_share_index(org, sup),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_root(org, sup),
                    self._linked_unlink_tombstone_key(org, sup, link_id),
                ],
                args=[link_id],
            )
            if isinstance(raw, (list, tuple)):
                if not raw:
                    raise RuntimeError("Invalid linked-share unlink result")
                result = int(raw[0])
                document_raw = raw[1] if len(raw) > 1 else None
            else:
                result = int(raw or 0)
                document_raw = None
            if result == -1:
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}"
                )
            if result == -2:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -3:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -4:
                raise RuntimeError(
                    f"Corrupt linked-share metadata/index for {org}/{sup}"
                )
            if result == -5:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result < 0:
                raise RuntimeError(
                    f"Invalid linked-share deletion result: {result}"
                )
            if result == 0:
                return None
            if result not in (1, 2) or document_raw is None:
                raise RuntimeError("Invalid linked-share unlink result")
            return self._decode_control_object(
                document_raw,
                description=f"linked-share unlink state for {org}/{sup}/{link_id}",
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_linked_share error: {e}")
            raise

    def finish_unlink_linked_share(
        self, org: str, sup: str, link_id: str,
    ) -> None:
        """Compact an unlink tombstone only after its per-link leaf index drains."""
        result = int(self._finish_unlink_linked_share(
            keys=[
                self._linked_unlink_tombstone_key(org, sup, link_id),
                self._linked_leaf_names_key(org, sup, link_id),
            ],
            args=[link_id],
        ) or 0)
        if result == -1:
            raise RuntimeError("Linked unlink tombstone is invalid")
        if result == -2:
            raise RuntimeError("Linked-share leaves remain during unlink")
        if result != 1:
            raise RuntimeError(f"Invalid linked-share unlink finish: {result}")

    def delete_linked_share(self, org: str, sup: str, link_id: str) -> bool:
        """Compatibility wrapper that begins durable unlink fencing."""
        document = self.begin_unlink_linked_share(org, sup, link_id)
        if document is None:
            return False
        if not self.list_linked_share_leaf_names(org, sup, link_id):
            self.finish_unlink_linked_share(org, sup, link_id)
        return True

    def list_linked_shares(
        self,
        org: str,
        sup: str,
        *,
        limit: int = 10_000,
    ) -> List[Dict[str, Any]]:
        links: List[Dict[str, Any]] = []
        try:
            members = self._bounded_set_members(
                RK.linked_share_index(org, sup),
                limit=limit,
                description=f"linked-share index for {org}/{sup}",
            )
            for lid in members:
                raw = self.r.get(RK.linked_share_doc(org, sup, lid))
                if raw is None:
                    raise RuntimeError(
                        "Linked-share index references missing metadata for "
                        f"{org}/{sup}/{lid}"
                    )
                links.append(self._decode_control_object(
                    raw,
                    description=(
                        f"linked-share metadata for {org}/{sup}/{lid}"
                    ),
                ))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_linked_shares error: {e}")
            raise
        return links

    # --------------------------------------------------------------------------- #
    # Staging / Pipe meta (for website UI)
    # --------------------------------------------------------------------------- #

    def upsert_staging_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            meta: Dict[str, Any],
            *,
            lock_token: str,
            create_only: bool = False,
    ) -> bool:
        """Publish staging metadata and ensure it is indexed for listing.

        ``create_only`` makes initial stage registration atomic with the lock
        and deletion-intent checks.  It prevents a caller that observed an
        absent document from replacing metadata created before publication.
        """
        if not (org and sup and staging_name):
            return False
        payload = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload["updated_at_ms"] = _now_ms()

        try:
            result = int(self._upsert_staging_meta(
                keys=[
                    RK.staging_doc(org, sup, staging_name),
                    RK.staging_index(org, sup),
                    RK.lock_stage(org, sup, staging_name),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_stage_deletion_intent(org, sup, staging_name),
                    RK.meta_root(org, sup),
                ],
                args=[
                    json.dumps(payload), staging_name, lock_token or "",
                    "1" if create_only else "0", org, sup,
                ],
            ) or 0)
            if result == -1:
                raise LockLostError("Lost staging lock before metadata publication")
            if result in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences staging "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -4:
                raise ValueError("Staging metadata is not valid JSON")
            if result == -5:
                raise FileExistsError(
                    f"Staging metadata already exists for "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -6:
                raise RuntimeError(
                    f"Corrupt staging metadata/index for "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -7:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -8:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -9:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(f"Invalid staging upsert result: {result}")
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_staging_meta error: {e}")
            raise


    def get_staging_meta(self, org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name):
            return None
        try:
            raw = self.r.get(RK.staging_doc(org, sup, staging_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_staging_meta error: {e}")
            raise
        if not raw:
            return None
        try:
            obj = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            raise RuntimeError(
                f"Corrupt staging metadata for {org}/{sup}/{staging_name}"
            ) from e
        if not isinstance(obj, dict):
            raise RuntimeError(
                f"Corrupt staging metadata for {org}/{sup}/{staging_name}"
            )
        return obj

    def upsert_staging_file_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            file_name: str,
            meta: Dict[str, Any],
            *,
            lock_token: str,
    ) -> bool:
        """Publish one staged file into the lock-fenced stage document.

        The physical parquet object has a unique generated name, so a rejected
        stale writer leaves only an unreferenced object. The live file index is
        updated in Redis in the same Lua boundary that verifies lock ownership
        and both durable deletion intents.
        """
        if not (org and sup and staging_name and file_name):
            raise ValueError(
                "organization, supertable, staging, and file name are required"
            )
        payload = dict(meta or {})
        payload["file"] = file_name
        try:
            result = int(self._upsert_staging_file_meta(
                keys=[
                    RK.staging_doc(org, sup, staging_name),
                    RK.lock_stage(org, sup, staging_name),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_stage_deletion_intent(org, sup, staging_name),
                    RK.meta_root(org, sup),
                ],
                args=[
                    json.dumps(payload), file_name, lock_token or "",
                    org, sup, staging_name,
                ],
            ) or 0)
            if result == -1:
                raise LockLostError(
                    "Lost staging lock before file-index publication"
                )
            if result in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences staging "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -4:
                raise ValueError("Staging file metadata is not valid JSON")
            if result == -5:
                raise RuntimeError(
                    f"Corrupt staging metadata for {org}/{sup}/{staging_name}"
                )
            if result == -6:
                raise RuntimeError(
                    f"Cannot index a file for missing staging "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -7:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -8:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -9:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(
                    f"Invalid staging file-index result: {result}"
                )
            return True
        except redis.RedisError as exc:
            logger.error(
                "[redis-catalog] upsert_staging_file_meta failed for "
                "%s/%s/%s/%s: %s",
                org, sup, staging_name, file_name, exc,
            )
            raise

    def list_stagings(self, org: str, sup: str, *, count: int = 1000) -> List[str]:
        """List staging names from the staging index set."""
        if not (org and sup):
            return []
        try:
            names = self.r.smembers(RK.staging_index(org, sup)) or set()
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                f"Corrupt staging index for {org}/{sup}"
            ) from exc
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings smembers error: {e}")
            raise
        decoded: set[str] = set()
        for raw_name in names:
            name = self._decode_index_member(
                raw_name, description=f"staging index for {org}/{sup}",
            )
            try:
                RK.staging_doc(org, sup, name)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Corrupt staging index for {org}/{sup}"
                ) from exc
            decoded.add(name)
        return sorted(decoded)

    def delete_staging_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            *,
            lock_token: str,
            intent_id: str,
            count: int = 1000,
    ) -> bool:
        """Delete one staging's Redis state without losing discoverability.

        Child pipe keys are removed first with strict Redis error propagation and
        an explicit empty-prefix check.  Only then does one bounded Lua command
        atomically remove the base metadata and its listing-index membership,
        conditional on continued ownership of the staging lock.  Zero removals
        are a successful idempotent retry; any uncertain or partial result raises.
        """
        if not (org and sup and staging_name):
            raise ValueError("organization, supertable, and staging name are required")
        if not lock_token or not intent_id:
            raise ValueError(
                "staging metadata deletion requires lock and intent tokens"
            )

        meta_key = RK.staging_doc(org, sup, staging_name)
        pattern = RK.staging_subkey_pattern(org, sup, staging_name)
        lock_key = RK.lock_stage(org, sup, staging_name)
        intent_key = RK.meta_stage_deletion_intent(org, sup, staging_name)
        try:
            batch_size = max(1, min(int(count), 1000))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("staging deletion count must be an integer") from exc

        # ``staging_subkey_pattern`` also matches the base ``:meta`` key. Keep
        # that discoverable until every actual child is gone; if SCAN skips a
        # key while the keyspace is changing, the verification below fails and
        # a retry resumes from the same visible staging record.
        cursor: Any = 0
        try:
            while True:
                cursor, keys = self.r.scan(
                    cursor=cursor,
                    match=pattern,
                    count=batch_size,
                )
                children = [
                    key if isinstance(key, str) else key.decode("utf-8")
                    for key in (keys or [])
                    if (key if isinstance(key, str) else key.decode("utf-8"))
                    != meta_key
                ]
                for offset in range(0, len(children), batch_size):
                    result = int(self._staging_delete_children(
                        keys=[
                            lock_key,
                            intent_key,
                            *children[offset:offset + batch_size],
                        ],
                        args=[lock_token, intent_id],
                    ) or 0)
                    if result == -1:
                        raise LockLostError(
                            f"Lost staging lock while deleting metadata for "
                            f"{org}/{sup}/{staging_name}"
                        )
                    if result < 0:
                        if result == -2:
                            raise DeletionIntentConflictError(
                                "Staging deletion intent ownership changed"
                            )
                        if result == -3:
                            raise RuntimeError("Corrupt staging deletion intent")
                        raise RuntimeError(
                            f"Invalid staging child deletion result: {result}"
                        )
                if int(cursor) == 0:
                    break

            # Deleting keys during SCAN can make a cursor skip an entry.  A
            # separate complete verification pass detects that ambiguity and
            # leaves the base metadata/index intact for a safe retry.
            cursor = 0
            while True:
                cursor, keys = self.r.scan(
                    cursor=cursor,
                    match=pattern,
                    count=batch_size,
                )
                remaining = [
                    key if isinstance(key, str) else key.decode("utf-8")
                    for key in (keys or [])
                    if (key if isinstance(key, str) else key.decode("utf-8"))
                    != meta_key
                ]
                if remaining:
                    raise RuntimeError(
                        f"Staging metadata children remain after deletion: "
                        f"{remaining[0]!r}"
                    )
                if int(cursor) == 0:
                    break

            result = int(self._staging_delete_meta(
                keys=[
                    RK.staging_index(org, sup),
                    meta_key,
                    lock_key,
                    intent_key,
                    RK.meta_stage_deletion_intent_index(org, sup),
                ],
                args=[staging_name, lock_token, intent_id, _now_ms()],
            ) or 0)
            if result == -1:
                raise LockLostError(
                    f"Lost staging lock before deleting metadata for "
                    f"{org}/{sup}/{staging_name}"
                )
            if result < 0:
                if result == -2:
                    raise DeletionIntentConflictError(
                        "Staging deletion intent ownership changed"
                    )
                if result == -3:
                    raise RuntimeError("Corrupt staging deletion intent")
                raise RuntimeError(
                    f"Invalid staging metadata deletion result: {result}"
                )
            if self.r.exists(meta_key) or self.r.sismember(
                RK.staging_index(org, sup), staging_name,
            ):
                raise RuntimeError(
                    f"Staging metadata deletion failed verification for "
                    f"{org}/{sup}/{staging_name}"
                )
        except (LockLostError, RuntimeError, ValueError):
            raise
        except redis.RedisError as exc:
            logger.error(
                "[redis-catalog] delete_staging_meta failed for %s/%s/%s: %s",
                org, sup, staging_name, exc,
            )
            raise
        return True

    def upsert_pipe_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            pipe_name: str,
            meta: Dict[str, Any],
            *,
            lock_token: str,
            create_only: bool = False,
    ) -> bool:
        """Upsert pipe metadata and ensure it is indexed for listing under a staging."""
        if not (org and sup and staging_name and pipe_name):
            return False
        payload = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload.setdefault("pipe_name", pipe_name)
        payload["updated_at_ms"] = _now_ms()

        try:
            result = int(self._upsert_pipe_meta(
                keys=[
                    RK.pipe_doc(org, sup, staging_name, pipe_name),
                    RK.pipe_index(org, sup, staging_name),
                    RK.lock_stage(org, sup, staging_name),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_stage_deletion_intent(org, sup, staging_name),
                    RK.staging_doc(org, sup, staging_name),
                    RK.meta_root(org, sup),
                ],
                args=[
                    json.dumps(payload), pipe_name, lock_token or "",
                    "1" if create_only else "0", org, sup, staging_name,
                ],
            ) or 0)
            if result == -1:
                raise LockLostError("Lost staging lock before pipe publication")
            if result in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences staging "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -4:
                raise ValueError("Pipe metadata is not valid JSON")
            if result == -5:
                raise FileExistsError(
                    f"Pipe metadata already exists for "
                    f"{org}/{sup}/{staging_name}/{pipe_name}"
                )
            if result == -6:
                raise RuntimeError(
                    f"Corrupt pipe metadata/index for "
                    f"{org}/{sup}/{staging_name}/{pipe_name}"
                )
            if result == -7:
                raise FileNotFoundError(
                    f"Staging does not exist: {org}/{sup}/{staging_name}"
                )
            if result == -8:
                raise RuntimeError(
                    f"Corrupt staging metadata for "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -9:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -10:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -11:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(f"Invalid pipe upsert result: {result}")
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_meta error: {e}")
            raise

    def get_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name and pipe_name):
            return None
        try:
            raw = self.r.get(
                RK.pipe_doc(org, sup, staging_name, pipe_name)
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_pipe_meta error: {e}")
            raise
        if not raw:
            return None
        try:
            obj = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            raise RuntimeError(
                f"Corrupt pipe metadata for "
                f"{org}/{sup}/{staging_name}/{pipe_name}"
            ) from e
        if not isinstance(obj, dict):
            raise RuntimeError(
                f"Corrupt pipe metadata for "
                f"{org}/{sup}/{staging_name}/{pipe_name}"
            )
        return obj


    def list_pipe_metas(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> List[Dict[str, Any]]:
        """List pipe metadata objects for a staging (back-compat).

        This is primarily used by SuperPipe to check for existing pipes.
        If a pipe has no stored dict meta (e.g. only defs list exists), a minimal
        payload with at least "pipe_name" is returned.
        """
        if not (org and sup and staging_name):
            return []
        out_metas: List[Dict[str, Any]] = []
        for name in self.list_pipes(org, sup, staging_name, count=count):
            meta = self.get_pipe_meta(org, sup, staging_name, name)
            if isinstance(meta, dict) and meta:
                out_metas.append(meta)
            else:
                out_metas.append(
                    {
                        "organization": org,
                        "super_name": sup,
                        "staging_name": staging_name,
                        "pipe_name": name,
                    }
                )
        return out_metas

    def list_pipes(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> List[str]:
        """List pipe names for a staging. Prefers the pipe index set; falls back to SCAN."""
        if not (org and sup and staging_name):
            return []
        try:
            names = list(self.r.smembers(RK.pipe_index(org, sup, staging_name)) or [])
            if names:
                return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_pipes smembers error: {e}")
            raise

        # Fallback: scan pipe definition keys.
        pattern = RK.pipe_pattern(org, sup, staging_name)
        seen = set()
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys or []:
                    kk = k if isinstance(k, str) else k.decode("utf-8")
                    if kk.endswith(":meta"):
                        continue
                    name = kk.rsplit(":pipe:", 1)[-1]
                    if name:
                        seen.add(name)
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_pipes scan error: {e}")
            raise
        return sorted(seen)

    def delete_pipe_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            pipe_name: str,
            *,
            lock_token: str,
    ) -> int:
        """Atomically delete a pipe while its staging lease still belongs to us."""
        if not (org and sup and staging_name and pipe_name):
            return 0
        try:
            result = int(self._delete_pipe_meta(
                keys=[
                    RK.pipe_doc(org, sup, staging_name, pipe_name),
                    RK.pipe_index(org, sup, staging_name),
                    RK.lock_stage(org, sup, staging_name),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_stage_deletion_intent(org, sup, staging_name),
                    RK.staging_doc(org, sup, staging_name),
                    RK.meta_root(org, sup),
                ],
                args=[pipe_name, lock_token or "", org, sup, staging_name],
            ) or 0)
            if result == -1:
                raise LockLostError("Lost staging lock before pipe deletion")
            if result in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences staging "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -4:
                raise RuntimeError(
                    f"Corrupt pipe metadata/index for "
                    f"{org}/{sup}/{staging_name}/{pipe_name}"
                )
            if result == -5:
                raise FileNotFoundError(
                    f"Staging does not exist: {org}/{sup}/{staging_name}"
                )
            if result == -6:
                raise RuntimeError(
                    f"Corrupt staging metadata for "
                    f"{org}/{sup}/{staging_name}"
                )
            if result == -7:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -8:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -9:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result < 0:
                raise RuntimeError(f"Invalid pipe deletion result: {result}")
            return result
        except redis.RedisError as exc:
            logger.error(
                "[redis-catalog] delete_pipe_meta failed for %s/%s/%s/%s: %s",
                org, sup, staging_name, pipe_name, exc,
            )
            raise

    # ========================================================================= #
    # Spark Thrift cluster management (org-scoped: supertable:{org}:system:engine:thrifts)
    # ========================================================================= #

    def register_spark_cluster(self, org: str, cluster_id: str, config: Dict[str, Any]) -> None:
        """
        Register or update a Spark Thrift cluster for an organization.

        config should include:
            thrift_host: str       — hostname of the Thrift Server
            thrift_port: int       — port (default 10000)
            name: str              — human-readable name
            min_bytes: int         — minimum job size for this cluster (default 0)
            max_bytes: int         — maximum job size (default unlimited)
            status: str            — "active" | "draining" | "offline"
            s3_enabled: bool       — can read from S3/MinIO directly
        """
        if not org:
            raise ValueError("organization is required")
        # Reject reusable object-store credentials and malformed session SET
        # values before the document reaches Redis.  The separate ``password``
        # field is the Thrift transport credential and remains supported.
        validate_spark_storage_config(config)
        try:
            doc = dict(config)
            doc["cluster_id"] = cluster_id
            doc["organization"] = org
            doc.setdefault("status", "active")
            doc.setdefault("thrift_port", 10000)
            doc.setdefault("min_bytes", 0)
            doc.setdefault("max_bytes", 0)
            doc.setdefault("s3_enabled", True)
            doc["modified_ms"] = _now_ms()
            self.r.hset(
                RK.engine_thrifts(org),
                cluster_id,
                json.dumps(doc, default=str),
            )
            logger.info(f"[redis-catalog] spark thrift registered: {org}/{cluster_id}")
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] register_spark_cluster error: {e}")



    def list_spark_clusters(self, org: str) -> List[Dict[str, Any]]:
        """Return all registered Spark Thrift clusters for an organization."""
        if not org:
            return []
        try:
            raw = self.r.hgetall(RK.engine_thrifts(org))
            clusters = []
            for _cid, data in raw.items():
                try:
                    cluster = json.loads(data)
                    if not isinstance(cluster, dict):
                        continue
                    unsafe_keys = spark_storage_credential_keys(cluster)
                    if unsafe_keys:
                        # Legacy documents may predate registration validation.
                        # Never return their credential values to API callers or
                        # select them for execution. Redis maintenance can then
                        # replace/delete the quarantined document deliberately.
                        for key in unsafe_keys:
                            cluster.pop(key, None)
                        cluster["status"] = "offline"
                        cluster["security_error"] = (
                            "inline_object_store_credentials"
                        )
                    clusters.append(cluster)
                except json.JSONDecodeError:
                    pass
            return clusters
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_spark_clusters error: {e}")
            return []


    def delete_spark_cluster(self, org: str, cluster_id: str) -> bool:
        """Remove a registered Spark Thrift cluster. Returns True if it existed."""
        if not (org and cluster_id):
            return False
        try:
            removed = self.r.hdel(RK.engine_thrifts(org), cluster_id)
            if removed:
                logger.info(f"[redis-catalog] spark thrift deleted: {org}/{cluster_id}")
            return bool(removed)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_spark_cluster error: {e}")
            return False


    def select_spark_cluster(self, org: str, job_bytes: int, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Select an active Spark Thrift cluster for a job of the given size.

        Selection logic:
        1. Filter clusters where status == "active".
        2. If force=False: keep clusters whose size window contains the job,
           i.e. ``min_bytes <= job_bytes`` AND (``max_bytes == 0`` (unbounded)
           OR ``job_bytes <= max_bytes``).
        3. If force=True: skip size filtering (user explicitly requested Spark).
        4. Among the clusters that can take the job, pick one at **random** so
           load spreads evenly across the eligible fleet.
        """
        clusters = self.list_spark_clusters(org)
        candidates = []

        for c in clusters:
            if c.get("status") != "active":
                continue
            if not force:
                try:
                    min_b = int(c.get("min_bytes", 0))
                    max_b = int(c.get("max_bytes", 0))
                except (TypeError, ValueError):
                    continue
                if job_bytes < min_b:
                    continue
                if max_b > 0 and job_bytes > max_b:
                    continue
            candidates.append(c)

        if not candidates:
            return None

        # Any candidate fits the job window; spread load by picking at random.
        return random.choice(candidates)

    # ========================================================================= #
    # Spark Plug management (org-scoped: supertable:{org}:system:engine:plugs)
    # ========================================================================= #

    def register_spark_plug(self, org: str, plug_id: str, config: Dict[str, Any]) -> None:
        """
        Register or update a Spark Plug (PySpark notebook runtime).

        config should include:
            name: str              — human-readable name
            spark_master: str      — Spark master URL (e.g. spark://spark-master:7077)
            ws_url: str            — WebSocket URL (e.g. ws://host:8010/ws/spark)
            webui_url: str         — Spark Master Web UI URL (optional)
            status: str            — "active" | "draining" | "offline"
        """
        if not org:
            raise ValueError("organization is required")
        try:
            doc = dict(config)
            doc["plug_id"] = plug_id
            doc["organization"] = org
            doc.setdefault("status", "active")
            doc.setdefault("spark_master", "spark://localhost:7077")
            doc.setdefault("ws_url", "ws://localhost:8010/ws/spark")
            doc.setdefault("webui_url", "")
            doc["modified_ms"] = _now_ms()
            self.r.hset(
                RK.engine_plugs(org),
                plug_id,
                json.dumps(doc, default=str),
            )
            logger.info(f"[redis-catalog] spark plug registered: {org}/{plug_id}")
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] register_spark_plug error: {e}")





    def set_table_config(
            self,
            org: str,
            sup: str,
            simple: str,
            config: Dict[str, Any],
            *,
            lock_token: str,
    ) -> bool:
        """Store per-table configuration (primary keys, dedup mode, etc.).

        The config dict is stored as a JSON string under a dedicated key.
        Existing config is fully replaced (last-write-wins).
        """
        if not (org and sup and simple):
            return False
        try:
            doc = _validate_table_config_document(config)
            doc["modified_ms"] = _now_ms()
            result = int(self._set_table_config_fenced(
                keys=[
                    RK.meta_table_config(org, sup, simple),
                    RK.meta_leaf(org, sup, simple),
                    RK.lock_leaf(org, sup, simple),
                    RK.meta_namespace_deletion_intent(org, sup),
                    RK.meta_simple_deletion_intent(org, sup, simple),
                    RK.meta_root(org, sup),
                ],
                args=[json.dumps(doc, default=str), lock_token or ""],
            ) or 0)
            if result == -1:
                raise LockLostError("Lost table lock before configuration commit")
            if result in (-2, -3):
                raise DeletionIntentConflictError(
                    f"Durable deletion intent fences {org}/{sup}/{simple}"
                )
            if result == -4:
                raise RuntimeError(f"Cannot configure missing table {org}/{sup}/{simple}")
            if result == -5:
                raise ValueError("Table configuration is not valid JSON")
            if result == -6:
                raise FileNotFoundError(
                    f"SuperTable does not exist: {org}/{sup}"
                )
            if result == -7:
                raise RuntimeError(f"Corrupt Redis root JSON for {org}/{sup}")
            if result == -8:
                raise ReadOnlyCatalogError(
                    f"SuperTable is read-only: {org}/{sup}"
                )
            if result != 1:
                raise RuntimeError(f"Invalid table configuration result: {result}")
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_table_config error: {e}")
            raise

    def get_table_config(
            self,
            org: str,
            sup: str,
            simple: str,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve per-table configuration.

        Returns None if no config has been set for this table.
        """
        if not (org and sup and simple):
            return None
        try:
            raw = self.r.get(RK.meta_table_config(org, sup, simple))
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] get_table_config error: {exc}")
            raise
        if raw is None:
            return None
        try:
            document = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                f"Corrupt table configuration for {org}/{sup}/{simple}"
            ) from exc
        if not isinstance(document, dict):
            raise RuntimeError(
                f"Corrupt table configuration for {org}/{sup}/{simple}"
            )
        try:
            return _validate_table_config_document(document)
        except ValueError as exc:
            raise RuntimeError(
                f"Corrupt table configuration for {org}/{sup}/{simple}: {exc}"
            ) from exc

    # ========================================================================= #
    # Engine runtime configuration (DuckDB memory, threads, caches, thresholds)
    # ========================================================================= #

    # Canonical field names and their env-var counterparts.  Used by the
    # resolver in engine_common to fall back to os.getenv when a field
    # is absent from Redis.
    # Runtime pragmas for the sole DuckDB engine.
    DUCKDB_CONFIG_FIELDS = (
        "duckdb_memory_limit",
        "duckdb_io_multiplier",
        "duckdb_threads",
        "duckdb_http_timeout",
        "duckdb_external_cache_size",
    )

    def _set_engine_document_section(
            self,
            org: str,
            section_name: str,
            section_value: Any,
    ) -> bool:
        """CAS one section of the shared engine document.

        DuckDB pragmas and AUTO routing policy share one Redis document.  A
        normal GET followed by SET loses acknowledged concurrent changes and,
        worse, can replace a real document after an ambiguous/failed read.
        Watching the exact key makes the read and replacement one optimistic
        transaction. Malformed or unavailable state fails closed and is never
        interpreted as an empty document.
        """
        if section_name not in {"duckdb", "auto_policy"}:
            raise ValueError("Unsupported engine configuration section")
        key = RK.engine_duckdb(org)
        pipe = None
        try:
            pipe = self.r.pipeline()
            for _attempt in range(64):
                try:
                    pipe.watch(key)
                    raw = pipe.get(key)
                    if raw is None:
                        document: Dict[str, Any] = {}
                    else:
                        try:
                            decoded = json.loads(raw)
                        except (TypeError, ValueError, UnicodeError) as exc:
                            logger.error(
                                "[redis-catalog] persisted engine configuration "
                                "is malformed: %s",
                                exc,
                            )
                            pipe.unwatch()
                            return False
                        if not isinstance(decoded, dict):
                            logger.error(
                                "[redis-catalog] persisted engine configuration "
                                "has the wrong shape"
                            )
                            pipe.unwatch()
                            return False
                        document = dict(decoded)

                    document[section_name] = section_value
                    document["modified_ms"] = _now_ms()
                    payload = json.dumps(document, default=str)
                    pipe.multi()
                    pipe.set(key, payload)
                    result = pipe.execute()
                    if (
                        not isinstance(result, (list, tuple))
                        or len(result) != 1
                        or not result[0]
                    ):
                        logger.error(
                            "[redis-catalog] engine configuration transaction "
                            "was not acknowledged: %r",
                            result,
                        )
                        return False
                    return True
                except redis.WatchError:
                    continue
            logger.error(
                "[redis-catalog] engine configuration changed too frequently "
                "to update safely"
            )
            return False
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] engine configuration update error: {exc}")
            return False
        finally:
            if pipe is not None:
                try:
                    pipe.reset()
                except Exception:
                    pass

    def set_engine_config(
            self,
            org: str,
            config: Dict[str, Any],
    ) -> bool:
        """Store the organization-wide DuckDB runtime configuration.

        Org-level system scope: one engine document per organization, applied
        globally across all supertables (not per-supertable).

        The DuckDB section is fully replaced (last-write-wins); shared AUTO
        thresholds and policy are preserved.

        Only recognised DuckDB fields (see DUCKDB_CONFIG_FIELDS) are persisted —
        unknown keys are silently dropped to prevent injection of arbitrary
        settings.
        """
        if not org:
            return False
        try:
            from supertable.engine.engine_config import normalize_memory_size

            # Whitelist recognised DuckDB fields only for this engine section.
            section = {
                k: config[k]
                for k in self.DUCKDB_CONFIG_FIELDS
                if k in config and config[k] is not None and str(config[k]).strip() != ""
            }
            # Memory-size fields arrive from the UI as bare numbers (GB).  Persist
            # them already unit-suffixed so the stored document is DuckDB-valid and
            # never yields a `PRAGMA memory_limit='2'` ParserException downstream.
            if "duckdb_memory_limit" in section:
                section["duckdb_memory_limit"] = normalize_memory_size(
                    section["duckdb_memory_limit"], default="1GB"
                )
            if "duckdb_external_cache_size" in section:
                normalized_cache = normalize_memory_size(
                    section["duckdb_external_cache_size"], default=""
                )
                if normalized_cache:
                    section["duckdb_external_cache_size"] = normalized_cache
                else:
                    section.pop("duckdb_external_cache_size")
            return self._set_engine_document_section(org, "duckdb", section)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_engine_config error: {e}")
            return False

    def get_engine_config(
            self,
            org: str,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve engine runtime configuration (org-level system scope).

        Returns None if no config has been stored for this organization.
        """
        if not org:
            return None
        try:
            raw = self.r.get(RK.engine_duckdb(org))
        except redis.RedisError as exc:
            logger.error(f"[redis-catalog] get_engine_config error: {exc}")
            raise
        if raw is None:
            return None
        try:
            document = json.loads(raw)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                f"Corrupt engine configuration for organization {org}"
            ) from exc
        if not isinstance(document, dict):
            raise RuntimeError(
                f"Corrupt engine configuration for organization {org}"
            )
        return document

    def set_auto_routing_policy(
            self, org: str, rules: list[Dict[str, Any]],
    ) -> bool:
        """Atomically replace the org-wide estimated-scan AUTO policy.

        Validation is strict and occurs before Redis is mutated. An empty list
        removes manual range selection and restores the adaptive cost model.
        """
        if not org:
            return False
        from supertable.engine.engine_config import normalize_auto_routing_policy

        normalized = normalize_auto_routing_policy(rules)
        try:
            return self._set_engine_document_section(
                org,
                "auto_policy",
                [rule.as_dict() for rule in normalized],
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_auto_routing_policy error: {e}")
            return False
