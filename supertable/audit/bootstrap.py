"""Create the privileged audit genesis for a genuinely empty organization.

Existing estates still require explicit, pinned activation.  A new organization
has no prior privileged state to attest, so its empty-state baseline can be
installed atomically before the ordinary audited bootstrap mutations run.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any

from supertable import redis_keys as RK
from supertable.audit.privileged_worker import (
    ActivationBaselineError,
    ActivationBaselineReport,
    _activation_anchor_payload,
    _activation_digest_frame,
)
from supertable.audit.diagnostics import safe_audit_error_type


# SCAN is bounded even when an otherwise empty organization shares a Redis
# database with a large estate.  Exceeding the bound requires explicit activation.
MAX_GREENFIELD_SCAN_CALLS = 1_000
MAX_GREENFIELD_EMPTY_META_KEYS = 1_000
_SCAN_COUNT = 1_000

_LUA_ENSURE_GREENFIELD_ACTIVATION = """
local activation_key = KEYS[1]
local payload = ARGV[1]
local max_scan_calls = tonumber(ARGV[5])
local max_empty_keys = tonumber(ARGV[6])
local scan_count = tonumber(ARGV[7])

local function normalized_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then value = value['ok'] end
    return tostring(value)
end

local activation_type = normalized_type(activation_key)
if activation_type == 'string' then
    return {0, redis.call('GET', activation_key)}
elseif activation_type ~= 'none' then
    return {-1}
end

local empty_meta = {}
local empty_meta_count = 0
local scan_calls = 0
for namespace = 2, 4 do
    local cursor = '0'
    repeat
        scan_calls = scan_calls + 1
        if scan_calls > max_scan_calls then return {-3} end
        local result = redis.call(
            'SCAN', cursor, 'MATCH', ARGV[namespace], 'COUNT', scan_count
        )
        cursor = tostring(result[1])
        for _, key in ipairs(result[2]) do
            local key_type = normalized_type(key)
            if namespace == 2 then
                -- Old interrupted bootstraps may leave only version-zero
                -- namespace metadata.  No documents, indexes, or revisions
                -- may be discarded when establishing empty-state genesis.
                local is_meta = string.match(key, ':rbac:roles:meta$')
                    or string.match(key, ':rbac:users:meta$')
                if not is_meta or key_type ~= 'hash'
                    or redis.call('HGET', key, 'version') ~= '0' then
                    return {-2}
                end
                local updated = redis.call('HGET', key, 'last_updated_ms')
                local expected_fields = 1
                if updated ~= false then
                    expected_fields = 2
                    if string.len(updated) > 19
                        or not string.match(updated, '^%d+$') then
                        return {-2}
                    end
                end
                if redis.call('HLEN', key) ~= expected_fields then return {-2} end
                if not empty_meta[key] then
                    empty_meta[key] = true
                    empty_meta_count = empty_meta_count + 1
                    if empty_meta_count > max_empty_keys then return {-3} end
                end
            else
                -- Tokens, revision heads, and every privileged ledger key are
                -- evidence of prior state.  Even an empty stream may retain
                -- deleted history or pending deliveries, so do not restart it.
                return {-2}
            end
        end
    until cursor == '0'
end

-- All checks finish before any writes.  SCAN and installation share this Lua
-- boundary, so another first creator cannot add privileged state in between.
-- Normalizing inert metadata makes the baseline exactly the empty-state digest
-- used by the existing worker's verifier.
for key, _ in pairs(empty_meta) do redis.call('DEL', key) end
redis.call('SET', activation_key, payload)
return {1, payload}
"""


def _canonical(document: dict[str, Any]) -> bytes:
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _baseline_from_anchor(payload: Any, organization: str) -> bytes:
    """Validate an anchor and reconstruct its exact canonical artifact."""
    try:
        if not isinstance(payload, (str, bytes)) or len(payload) > 16_384:
            raise ValueError
        document = json.loads(payload)
        if not isinstance(document, dict) or set(document) != {
            "version", "kind", "organization", "activation_id", "created_ms",
            "state_sha256", "artifact_sha256",
        }:
            raise ValueError
        if (
            type(document["version"]) is not int
            or document["version"] != 1
            or document["kind"] != "supertable_privileged_activation_anchor"
            or document["organization"] != organization
            or not isinstance(document["activation_id"], str)
            or not 1 <= len(document["activation_id"]) <= 256
            or type(document["created_ms"]) is not int
            or document["created_ms"] < 1
        ):
            raise ValueError
        for field in ("state_sha256", "artifact_sha256"):
            value = document[field]
            if (
                not isinstance(value, str) or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError
        encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
        if encoded != _canonical(document):
            raise ValueError
        expected_sha256 = document.pop("artifact_sha256")
        document["kind"] = "supertable_privileged_activation_baseline"
        baseline = _canonical(document)
        if not hmac.compare_digest(hashlib.sha256(baseline).hexdigest(), expected_sha256):
            raise ValueError
        return baseline
    except (TypeError, ValueError, UnicodeError):
        raise ActivationBaselineError(
            "privileged audit activation baseline anchor is invalid"
        ) from None


def read_activation_baseline(redis_client: Any, organization: str) -> bytes:
    """Export canonical baseline bytes from the immutable activation anchor.

    The anchor losslessly contains all baseline fields and its SHA-256.  Write
    these bytes to a file and independently pin their SHA-256 when configuring
    the supervised archive worker; its existing verifier accepts this artifact.
    """
    key = RK.audit_privileged_activation(organization)
    try:
        payload = redis_client.get(key)
    except Exception as exc:
        raise ActivationBaselineError(
            "cannot read privileged activation anchor; "
            f"error_type={safe_audit_error_type(exc)}"
        ) from None
    if payload is None:
        raise ActivationBaselineError("privileged audit activation baseline is not anchored")
    return _baseline_from_anchor(payload, organization)


def ensure_greenfield_activation(redis_client: Any, organization: str) -> bool:
    """Install empty-estate genesis once, or validate an existing anchor.

    Returns ``True`` only for the call that installs the anchor.  Any existing
    privileged state without an anchor requires the explicit existing-estate
    activation workflow.  Redis failures propagate as activation errors and
    never cause an unaudited fallback.
    """
    activation_key = RK.audit_privileged_activation(organization)
    digest = hashlib.sha256(b"supertable-privileged-state-v1\x00")
    _activation_digest_frame(digest, organization.encode("utf-8"))
    activation_id = "greenfield-" + uuid.uuid4().hex
    created_ms = time.time_ns() // 1_000_000
    state_sha256 = digest.hexdigest()
    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": organization,
        "activation_id": activation_id,
        "created_ms": created_ms,
        "state_sha256": state_sha256,
    }
    baseline = _canonical(document)
    report = ActivationBaselineReport(
        organization=organization,
        activation_id=activation_id,
        created_ms=created_ms,
        state_sha256=state_sha256,
        artifact_sha256=hashlib.sha256(baseline).hexdigest(),
    )
    payload = _activation_anchor_payload(report)
    try:
        result = redis_client.eval(
            _LUA_ENSURE_GREENFIELD_ACTIVATION,
            1,
            activation_key,
            payload,
            RK.rbac_pattern_for_org(organization),
            RK.auth_tokens(organization) + "*",
            activation_key.rsplit(":", 1)[0] + ":*",
            MAX_GREENFIELD_SCAN_CALLS,
            MAX_GREENFIELD_EMPTY_META_KEYS,
            _SCAN_COUNT,
        )
    except Exception as exc:
        raise ActivationBaselineError(
            "cannot initialize privileged audit activation; "
            f"error_type={safe_audit_error_type(exc)}"
        ) from None
    if not isinstance(result, (list, tuple)) or not result:
        raise ActivationBaselineError("invalid privileged activation response")
    status = result[0]
    if status == -1:
        raise ActivationBaselineError("privileged activation anchor has an invalid Redis type")
    if status == -2:
        raise ActivationBaselineError(
            "organization already contains privileged state without an activation anchor; "
            "an explicit existing-estate activation baseline is required"
        )
    if status == -3:
        raise ActivationBaselineError(
            "empty-organization activation scan exceeded its bound; "
            "an explicit activation baseline is required"
        )
    if status not in (0, 1) or len(result) != 2:
        raise ActivationBaselineError("invalid privileged activation response")
    _baseline_from_anchor(result[1], organization)
    return status == 1
