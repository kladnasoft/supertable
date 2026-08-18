"""Fail-closed Redis disaster recovery from sealed storage artifacts.

A loose snapshot with the highest version is not necessarily committed: data
writes upload immutable JSON before publishing the Redis leaf.  This module
therefore creates a content-sealed checkpoint while Redis is healthy and only
restores that checkpoint.  Restore verifies every referenced snapshot and
data artifact plus the complete privileged-audit archive chain before making
any Redis write.

Both checkpointing and restore require an application maintenance window.
The destination must be empty or already match the exact plan; partial or
conflicting state is never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from supertable import redis_keys as RK
from supertable.utils.snapshot import complete_snapshot_payload


_CHECKPOINT_VERSION = 2
_CHECKPOINT_KIND = "supertable_redis_catalog_checkpoint"
_CHECKPOINT_RE = re.compile(
    r"^checkpoint_([0-9]{1,19})_([0-9a-f]{64})\.json$"
)
_AUDIT_MANIFEST_RE = re.compile(r"^batch_([0-9a-f]{64})\.json$")
_MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
_MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
_MAX_CHECKPOINTS = 10_000
_MAX_AUDIT_BATCHES = 10_000
_ARCHIVE_GROUP = "__privileged_archival__"
_REDIS_TYPES = frozenset({"string", "hash", "set", "list", "zset"})


class RecoveryError(RuntimeError):
    """Recovery evidence or destination state is unsafe or ambiguous."""


@dataclass(frozen=True)
class CatalogCheckpoint:
    organization: str
    created_ms: int
    path: str
    manifest_sha256: str
    key_count: int
    snapshot_count: int


@dataclass(frozen=True)
class RecoveryPlan:
    organization: str
    checkpoint_path: str
    checkpoint_sha256: str
    redis_state: tuple[Mapping[str, Any], ...]
    snapshot_count: int
    audit_batch_count: int
    audit_last_sequence: int
    audit_last_stream_id: Optional[str]
    audit_tip_batch_id: Optional[str]
    audit_checkpoint_path: Optional[str]
    audit_checkpoint_sha256: Optional[str]
    audit_stream_fields: Optional[Mapping[str, str]]
    audit_meta: Optional[Mapping[str, str]]
    audit_delivery: Optional[Mapping[str, str]]

    @property
    def key_count(self) -> int:
        return len(self.redis_state) + (3 if self.audit_stream_fields else 0)


@dataclass(frozen=True)
class RecoveryResult:
    organization: str
    dry_run: bool
    applied: bool
    already_current: bool
    key_count: int
    snapshot_count: int
    audit_batch_count: int
    audit_last_sequence: int
    checkpoint_sha256: str


class _ArchiveOnlyRedis:
    """Prove archive verification is independent of surviving Redis state."""

    @staticmethod
    def hget(_key: str, _field: str) -> None:
        return None


@dataclass(frozen=True)
class _AuditRecoveryState:
    """One verified archive position that can be bound into a catalog seal."""

    batch_count: int = 0
    last_sequence: int = 0
    last_stream_id: Optional[str] = None
    tip_batch_id: Optional[str] = None
    checkpoint_path: Optional[str] = None
    checkpoint_sha256: Optional[str] = None
    stream_fields: Optional[Mapping[str, str]] = None
    meta: Optional[Mapping[str, str]] = None
    delivery: Optional[Mapping[str, str]] = None

    def binding(self) -> Optional[dict[str, Any]]:
        if self.tip_batch_id is None:
            return None
        return {
            "version": 1,
            "batch_count": self.batch_count,
            "last_sequence": self.last_sequence,
            "last_stream_id": self.last_stream_id,
            "tip_batch_id": self.tip_batch_id,
            "checkpoint_path": self.checkpoint_path,
            "checkpoint_sha256": self.checkpoint_sha256,
        }


_UNBOUND_AUDIT_POSITION = object()


def _text(value: Any, *, field: str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RecoveryError(f"{field} is not UTF-8") from exc
    if isinstance(value, str):
        return value
    raise RecoveryError(f"{field} must be Redis text")


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _validate_identity(organization: str) -> None:
    try:
        RK.org_prefix(organization)
    except (TypeError, ValueError) as exc:
        raise RecoveryError("organization is not a canonical Redis identifier") from exc


def _logical_path(path: Any, *, field: str) -> str:
    if not isinstance(path, str) or not path or "\\" in path:
        raise RecoveryError(f"{field} is not a logical storage path")
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or any(part in {"", ".", ".."} for part in candidate.parts):
        raise RecoveryError(f"{field} escapes the logical storage namespace")
    normalized = candidate.as_posix()
    if normalized != path or path != path.strip("/"):
        raise RecoveryError(f"{field} is not canonical")
    return normalized


def _read_bytes(storage: Any, path: str, *, maximum: int, label: str) -> bytes:
    try:
        size = getattr(storage, "size", None)
        if callable(size):
            observed = int(size(path))
            if observed < 1 or observed > maximum:
                raise RecoveryError(f"{label} size is outside the supported bound")
        payload = storage.read_bytes(path)
    except RecoveryError:
        raise
    except Exception as exc:
        raise RecoveryError(f"cannot read {label} {path!r}: {exc}") from exc
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= maximum:
        raise RecoveryError(f"{label} bytes are outside the supported bound")
    return payload


def _scan(redis_client: Any, pattern: str) -> tuple[str, ...]:
    cursor: Any = 0
    found: set[str] = set()
    while True:
        try:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
        except Exception as exc:
            raise RecoveryError(f"Redis SCAN failed: {exc}") from exc
        for raw in keys or ():
            found.add(_text(raw, field="Redis key"))
        if int(cursor) == 0:
            break
    return tuple(sorted(found))


def _runtime_prefixes(organization: str) -> tuple[str, str]:
    audit_prefix = RK.audit_stream(organization).rsplit(":stream", 1)[0] + ":"
    monitor_probe = RK.monitor_partition(organization, "writes", "2000-01-01")
    monitor_prefix = monitor_probe.split(":writes:doc:", 1)[0] + ":"
    return audit_prefix, monitor_prefix


def _is_excluded_runtime_key(organization: str, key: str, pttl: int) -> bool:
    """Return True only for state that must not be resurrected after DR."""
    if pttl >= 0 or ":lock:" in key:
        return True
    audit_prefix, monitor_prefix = _runtime_prefixes(organization)
    # The outbox/meta/delivery keys are rebuilt from a verified archive
    # position, but the activation anchor is immutable privileged-state
    # genesis.  Dropping it would correctly fence every later RBAC/token
    # mutation and the original baseline generally cannot be re-anchored after
    # state has evolved.  Preserve it inside the content-sealed catalog state.
    if key == RK.audit_privileged_activation(organization):
        return False
    return key.startswith(audit_prefix) or key.startswith(monitor_prefix)


def _validate_activation_anchor(organization: str, item: Mapping[str, Any]) -> str:
    """Return the exact canonical activation JSON or reject the checkpoint."""

    if item.get("type") != "string" or not isinstance(item.get("value"), str):
        raise RecoveryError("privileged activation anchor is not a Redis string")
    value = str(item["value"])
    try:
        document = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise RecoveryError("privileged activation anchor is not valid JSON") from exc
    required = {
        "version",
        "kind",
        "organization",
        "activation_id",
        "created_ms",
        "state_sha256",
        "artifact_sha256",
    }
    if not isinstance(document, dict) or set(document) != required:
        raise RecoveryError("privileged activation anchor has an invalid schema")
    activation_id = document.get("activation_id")
    created_ms = document.get("created_ms")
    digests = (document.get("state_sha256"), document.get("artifact_sha256"))
    if (
        document.get("version") != 1
        or document.get("kind") != "supertable_privileged_activation_anchor"
        or document.get("organization") != organization
        or not isinstance(activation_id, str)
        or not activation_id
        or len(activation_id) > 256
        or isinstance(created_ms, bool)
        or not isinstance(created_ms, int)
        or created_ms < 1
        or any(
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            for digest in digests
        )
        or value != _canonical(document).decode("utf-8")
    ):
        raise RecoveryError("privileged activation anchor is invalid or non-canonical")
    return value


def _redis_value(redis_client: Any, key: str) -> dict[str, Any]:
    try:
        raw_type = _text(redis_client.type(key), field=f"TYPE {key}").lower()
    except Exception as exc:
        raise RecoveryError(f"cannot inspect Redis key {key!r}: {exc}") from exc
    if raw_type == "none":
        raise RecoveryError(f"Redis key disappeared while checkpointing: {key}")
    if raw_type not in _REDIS_TYPES:
        raise RecoveryError(
            f"persistent Redis key {key!r} has unsupported type {raw_type!r}"
        )
    try:
        if raw_type == "string":
            value: Any = _text(redis_client.get(key), field=f"value {key}")
        elif raw_type == "hash":
            raw = redis_client.hgetall(key)
            value = dict(sorted({
                _text(k, field=f"hash field {key}"): _text(v, field=f"hash value {key}")
                for k, v in (raw or {}).items()
            }.items()))
        elif raw_type == "set":
            value = sorted(
                _text(v, field=f"set member {key}")
                for v in redis_client.smembers(key)
            )
        elif raw_type == "list":
            value = [
                _text(v, field=f"list item {key}")
                for v in redis_client.lrange(key, 0, -1)
            ]
        else:
            value = [
                [_text(member, field=f"zset member {key}"), format(float(score), ".17g")]
                for member, score in redis_client.zrange(key, 0, -1, withscores=True)
            ]
    except RecoveryError:
        raise
    except Exception as exc:
        raise RecoveryError(f"cannot read Redis key {key!r}: {exc}") from exc
    return {"key": key, "type": raw_type, "value": value}


def _capture_state_once(redis_client: Any, organization: str) -> tuple[dict[str, Any], ...]:
    state: list[dict[str, Any]] = []
    for key in _scan(redis_client, RK.org_pattern(organization)):
        try:
            pttl = int(redis_client.pttl(key))
        except Exception as exc:
            raise RecoveryError(f"cannot inspect TTL for {key!r}: {exc}") from exc
        if pttl == -2:
            raise RecoveryError(f"Redis key disappeared while checkpointing: {key}")
        if pttl < -2:
            raise RecoveryError(f"Redis returned an invalid TTL for {key}")
        if _is_excluded_runtime_key(organization, key, pttl):
            continue
        state.append(_redis_value(redis_client, key))
    return tuple(sorted(state, key=lambda item: item["key"]))


def _decode_json(payload: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise RecoveryError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise RecoveryError(f"{label} must be a JSON object")
    return value


def _validate_state_items(
    organization: str,
    state: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    prefix = RK.org_prefix(organization)
    audit_prefix, monitor_prefix = _runtime_prefixes(organization)
    activation_key = RK.audit_privileged_activation(organization)
    for item in state:
        if not isinstance(item, Mapping):
            raise RecoveryError("checkpoint Redis state contains a non-object entry")
        key = item.get("key")
        key_type = item.get("type")
        value = item.get("value")
        if (
            not isinstance(key, str)
            or not key.startswith(prefix)
            or key in result
            or (key.startswith(audit_prefix) and key != activation_key)
            or key.startswith(monitor_prefix)
            or ":lock:" in key
        ):
            raise RecoveryError("checkpoint Redis state contains a duplicate/unsafe key")
        if key_type not in _REDIS_TYPES:
            raise RecoveryError(f"checkpoint has unsupported Redis type for {key!r}")
        if key_type == "string" and not isinstance(value, str):
            raise RecoveryError(f"checkpoint string {key!r} has invalid data")
        if key_type == "hash" and (
            not isinstance(value, dict)
            or any(not isinstance(k, str) or not isinstance(v, str) for k, v in value.items())
        ):
            raise RecoveryError(f"checkpoint hash {key!r} has invalid data")
        if key_type in {"set", "list"} and (
            not isinstance(value, list) or any(not isinstance(v, str) for v in value)
        ):
            raise RecoveryError(f"checkpoint collection {key!r} has invalid data")
        if key_type == "set" and value != sorted(set(value)):
            raise RecoveryError(f"checkpoint set {key!r} is not canonical")
        if key_type == "zset":
            if not isinstance(value, list):
                raise RecoveryError(f"checkpoint zset {key!r} has invalid data")
            seen: set[str] = set()
            for pair in value:
                if (
                    not isinstance(pair, list)
                    or len(pair) != 2
                    or not isinstance(pair[0], str)
                    or not isinstance(pair[1], str)
                    or pair[0] in seen
                ):
                    raise RecoveryError(f"checkpoint zset {key!r} has invalid data")
                try:
                    float(pair[1])
                except ValueError as exc:
                    raise RecoveryError(f"checkpoint zset {key!r} has invalid score") from exc
                seen.add(pair[0])
        if key == activation_key:
            _validate_activation_anchor(organization, item)
        result[key] = item
    return result


def _snapshot_artifacts(
    snapshot: Mapping[str, Any],
) -> Iterable[tuple[Any, Optional[int], str]]:
    resources = snapshot.get("resources")
    if not isinstance(resources, list):
        raise RecoveryError("snapshot resources must be a list")
    for resource in resources:
        if not isinstance(resource, Mapping):
            raise RecoveryError("snapshot contains an invalid resource")
        declared_size = resource.get("file_size")
        if (
            isinstance(declared_size, bool)
            or not isinstance(declared_size, int)
            or declared_size < 1
        ):
            raise RecoveryError(
                "snapshot resource is missing a positive declared file_size"
            )
        yield resource.get("file"), declared_size, "data"
    for field in ("tombstone", "stats_file"):
        value = snapshot.get(field)
        if value is not None:
            yield value, None, field


def _content_seal(
    storage: Any,
    path: str,
    *,
    declared_size: Optional[int],
) -> tuple[int, str]:
    """Read one immutable artifact exactly and return its size/content hash."""

    try:
        content_sha256 = getattr(storage, "content_sha256", None)
        if callable(content_sha256):
            size, digest = content_sha256(path)
        else:
            # Compatibility for older third-party adapters used during an
            # offline maintenance operation. Built-in adapters use bounded
            # download/range I/O inside StorageInterface.content_sha256().
            payload = storage.read_bytes(path)
            if not isinstance(payload, bytes):
                raise TypeError("read_bytes returned a non-bytes value")
            size, digest = len(payload), _sha256(payload)
    except Exception as exc:
        raise RecoveryError(
            f"cannot content-seal snapshot artifact {path!r}: {exc}"
        ) from exc
    try:
        size = int(size)
    except (TypeError, ValueError) as exc:
        raise RecoveryError(
            f"snapshot artifact {path!r} returned an invalid size"
        ) from exc
    if (
        size < 1
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise RecoveryError(
            f"snapshot artifact {path!r} returned an invalid content seal"
        )
    if declared_size is not None and size != declared_size:
        raise RecoveryError(
            f"snapshot artifact {path!r} size differs from file_size "
            f"({size} != {declared_size})"
        )
    return size, digest


def _validate_snapshot(
    storage: Any,
    *,
    organization: str,
    super_name: str,
    simple_name: str,
    leaf_raw: str,
) -> dict[str, Any]:
    try:
        leaf = json.loads(leaf_raw)
    except (TypeError, ValueError) as exc:
        raise RecoveryError(f"leaf {super_name}/{simple_name} is invalid JSON") from exc
    if not isinstance(leaf, dict):
        raise RecoveryError(f"leaf {super_name}/{simple_name} is not an object")
    path = _logical_path(leaf.get("path"), field="snapshot path")
    snapshot_prefix = f"{organization}/{super_name}/tables/{simple_name}/snapshots/"
    if not path.startswith(snapshot_prefix) or not path.endswith(".json"):
        raise RecoveryError(
            f"leaf {super_name}/{simple_name} points outside its snapshot namespace"
        )
    payload = _read_bytes(storage, path, maximum=_MAX_SNAPSHOT_BYTES, label="snapshot")
    snapshot = _decode_json(payload, label=f"snapshot {path}")
    complete = complete_snapshot_payload(snapshot)
    if complete is None:
        raise RecoveryError(
            f"snapshot {path!r} is incomplete or has an invalid deletion vector"
        )
    if complete.get("simple_name") != simple_name:
        raise RecoveryError(f"snapshot {path!r} has the wrong table identity")
    try:
        leaf_version = int(leaf["version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RecoveryError(f"leaf {super_name}/{simple_name} has no valid version") from exc
    if leaf_version != int(complete["snapshot_version"]):
        raise RecoveryError(f"leaf {super_name}/{simple_name} version differs from snapshot")
    table_prefix = f"{organization}/{super_name}/tables/{simple_name}/"
    artifact_seals: dict[str, dict[str, Any]] = {}
    for raw_artifact, declared_size, artifact_kind in _snapshot_artifacts(complete):
        artifact = _logical_path(raw_artifact, field="snapshot artifact path")
        if not artifact.startswith(table_prefix):
            raise RecoveryError(f"snapshot artifact {artifact!r} escapes table namespace")
        size, content_sha256 = _content_seal(
            storage, artifact, declared_size=declared_size,
        )
        seal = {
            "path": artifact,
            "kind": artifact_kind,
            "bytes": size,
            "sha256": content_sha256,
        }
        previous = artifact_seals.setdefault(artifact, seal)
        if previous != seal:
            raise RecoveryError(
                f"snapshot artifact {artifact!r} has conflicting declarations"
            )
    return {
        "organization": organization,
        "super_name": super_name,
        "simple_name": simple_name,
        "path": path,
        "sha256": _sha256(payload),
        "snapshot_version": int(complete["snapshot_version"]),
        "artifacts": [artifact_seals[path] for path in sorted(artifact_seals)],
    }


def _validate_catalog_state(
    storage: Any,
    organization: str,
    state: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    by_key = _validate_state_items(organization, state)
    roots: set[str] = set()
    for key, item in by_key.items():
        parsed = RK.parse_lake_key(key)
        if parsed is None or parsed[0] != organization:
            continue
        if key == RK.meta_root(*parsed):
            roots.add(parsed[1])
            if item.get("type") != "string":
                raise RecoveryError(f"catalog root {parsed[1]!r} is not a string")
    snapshots: list[dict[str, Any]] = []
    names_by_super: dict[str, set[str]] = {sup: set() for sup in roots}
    for key, item in by_key.items():
        parsed = RK.parse_lake_key(key)
        if parsed is None or parsed[0] != organization:
            continue
        org, sup = parsed
        leaf_prefix = RK.meta_leaf_pattern(org, sup)[:-1]
        if not key.startswith(leaf_prefix):
            continue
        simple = key[len(leaf_prefix):]
        try:
            if key != RK.meta_leaf(org, sup, simple):
                raise ValueError("non-canonical leaf key")
        except (TypeError, ValueError) as exc:
            raise RecoveryError(f"catalog contains an unsafe leaf key {key!r}") from exc
        if sup not in roots:
            raise RecoveryError(f"leaf {sup}/{simple} has no catalog root")
        if item.get("type") != "string" or not isinstance(item.get("value"), str):
            raise RecoveryError(f"leaf {sup}/{simple} is not a Redis string")
        snapshots.append(_validate_snapshot(
            storage,
            organization=org,
            super_name=sup,
            simple_name=simple,
            leaf_raw=str(item["value"]),
        ))
        names_by_super[sup].add(simple)
    for sup, names in names_by_super.items():
        names_item = by_key.get(RK.meta_table_names(organization, sup))
        if names_item is None:
            if names:
                raise RecoveryError(f"catalog table-name index is missing for {sup!r}")
            continue
        if names_item.get("type") != "set" or set(names_item.get("value", ())) != names:
            raise RecoveryError(
                f"catalog table-name index disagrees with leaves for {sup!r}"
            )
    snapshots.sort(key=lambda item: (item["super_name"], item["simple_name"]))
    return tuple(snapshots)


def _checkpoint_dir(organization: str) -> str:
    return f"{organization}/__recovery__/redis_catalog"


def _activation_sha256_from_state(
    organization: str,
    state: Sequence[Mapping[str, Any]],
) -> Optional[str]:
    activation_key = RK.audit_privileged_activation(organization)
    matches = [item for item in state if item.get("key") == activation_key]
    if not matches:
        return None
    if len(matches) != 1:
        raise RecoveryError("catalog contains duplicate privileged activation anchors")
    value = _validate_activation_anchor(organization, matches[0])
    return _sha256(value.encode("utf-8"))


def _redis_mapping(value: Mapping[Any, Any], *, label: str) -> dict[str, str]:
    try:
        return {
            _text(key, field=f"{label} field"):
            _text(item, field=f"{label} value")
            for key, item in value.items()
        }
    except AttributeError as exc:
        raise RecoveryError(f"{label} is not a Redis mapping") from exc


def _stream_id(value: Any, *, field: str) -> tuple[int, int]:
    text = _text(value, field=field)
    match = re.fullmatch(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)", text)
    if match is None:
        raise RecoveryError(f"{field} is not a canonical Redis stream ID")
    return int(match.group(1)), int(match.group(2))


def _verify_live_audit_position(
    redis_client: Any,
    organization: str,
    audit: _AuditRecoveryState,
    *,
    activation_sha256: Optional[str],
) -> None:
    """Require the Redis WAL to be fully delivered at the sealed archive tip."""

    stream_key = RK.audit_privileged_outbox(organization)
    meta_key = RK.audit_privileged_meta(organization)
    delivery_key = RK.audit_privileged_delivery(organization)
    try:
        types = {
            key: _text(redis_client.type(key), field=f"TYPE {key}").lower()
            for key in (stream_key, meta_key, delivery_key)
        }
        stream_length = (
            int(redis_client.xlen(stream_key))
            if types[stream_key] == "stream" else 0
        )
        meta = (
            _redis_mapping(redis_client.hgetall(meta_key), label="audit meta")
            if types[meta_key] == "hash" else {}
        )
        delivery = (
            _redis_mapping(redis_client.hgetall(delivery_key), label="audit delivery")
            if types[delivery_key] == "hash" else {}
        )
    except RecoveryError:
        raise
    except Exception as exc:
        raise RecoveryError(f"cannot inspect live privileged audit state: {exc}") from exc

    allowed_types = {
        stream_key: {"none", "stream"},
        meta_key: {"none", "hash"},
        delivery_key: {"none", "hash"},
    }
    if any(types[key] not in allowed_types[key] for key in allowed_types):
        raise RecoveryError("live privileged audit state has an invalid Redis type")

    if audit.tip_batch_id is None:
        if stream_length or meta or delivery:
            raise RecoveryError(
                "privileged audit WAL is not empty but no durable archive tip exists"
            )
        return
    if activation_sha256 is None:
        raise RecoveryError(
            "privileged audit history exists without an activation anchor"
        )
    if types[stream_key] != "stream" or types[meta_key] != "hash":
        raise RecoveryError("privileged audit WAL/meta is missing at archive checkpoint")
    if meta != dict(audit.meta or {}):
        raise RecoveryError(
            "live privileged audit sequence is ahead of or behind its archive tip"
        )
    expected_delivery = dict(audit.delivery or {})
    for field, expected_value in expected_delivery.items():
        actual_value = delivery.get(field)
        if field == "archive:head":
            try:
                actual_head = json.loads(actual_value or "")
                expected_head = json.loads(expected_value)
            except (TypeError, ValueError) as exc:
                raise RecoveryError(
                    "privileged audit delivery head is invalid JSON"
                ) from exc
            if not isinstance(actual_head, dict) or not isinstance(expected_head, dict):
                raise RecoveryError("privileged audit delivery head is invalid")
            # Delivery wall-clock time is operational metadata, not ledger
            # identity. All chain/path/hash/sequence fields must match exactly.
            actual_head.pop("delivered_ms", None)
            expected_head.pop("delivered_ms", None)
            matches = actual_head == expected_head
        else:
            matches = actual_value == expected_value
        if not matches:
            raise RecoveryError(
                "privileged audit delivery ledger does not match its archive tip"
            )
    try:
        latest_rows = redis_client.xrevrange(stream_key, max="+", min="-", count=1)
        groups = redis_client.xinfo_groups(stream_key)
    except Exception as exc:
        raise RecoveryError(f"cannot inspect privileged audit delivery state: {exc}") from exc
    if latest_rows:
        latest_id = _text(latest_rows[0][0], field="latest audit stream ID")
        latest_fields = _redis_mapping(latest_rows[0][1], label="latest audit stream")
        if (
            latest_id != audit.last_stream_id
            or latest_fields != dict(audit.stream_fields or {})
        ):
            raise RecoveryError(
                "live privileged audit stream is ahead of or differs from its archive tip"
            )
    archival_groups = []
    for raw_group in groups or ():
        decoded = {
            _text(key, field="audit group field"):
            (_text(value, field="audit group value") if isinstance(value, (str, bytes)) else value)
            for key, value in raw_group.items()
        }
        if decoded.get("name") == _ARCHIVE_GROUP:
            archival_groups.append(decoded)
    if len(archival_groups) != 1:
        raise RecoveryError("privileged archive consumer group is missing or ambiguous")
    group = archival_groups[0]
    try:
        pending = int(group.get("pending", -1))
        delivered = _stream_id(
            group.get("last-delivered-id"), field="archive group last-delivered-id",
        )
        expected_delivered = _stream_id(
            audit.last_stream_id, field="archive tip stream ID",
        )
    except (TypeError, ValueError) as exc:
        raise RecoveryError("privileged archive consumer group is invalid") from exc
    if pending != 0 or delivered < expected_delivered:
        raise RecoveryError(
            "privileged audit WAL is not fully acknowledged through its archive tip"
        )


def create_catalog_checkpoint(
    redis_client: Any,
    storage: Any,
    organization: str,
    *,
    clock_ms: Optional[Callable[[], int]] = None,
) -> CatalogCheckpoint:
    """Seal a stable, persistent organization Redis snapshot in storage."""
    _validate_identity(organization)
    previous: Optional[tuple[dict[str, Any], ...]] = None
    state: Optional[tuple[dict[str, Any], ...]] = None
    for _ in range(4):
        candidate = _capture_state_once(redis_client, organization)
        if candidate == previous:
            state = candidate
            break
        previous = candidate
    if state is None:
        raise RecoveryError(
            "Redis catalog changed while checkpointing; retry while writers are stopped"
        )
    snapshots = _validate_catalog_state(storage, organization, state)
    activation_sha256 = _activation_sha256_from_state(organization, state)
    # The storage archive and Redis WAL must describe the same exact committed
    # privileged position while the maintenance window is held. Re-read both
    # once to catch a writer/archiver that was not actually stopped.
    audit = _audit_plan(storage, organization)
    _verify_live_audit_position(
        redis_client,
        organization,
        audit,
        activation_sha256=activation_sha256,
    )
    audit_again = _audit_plan(storage, organization)
    if audit_again != audit:
        raise RecoveryError(
            "privileged audit archive changed while checkpointing; stop workers and retry"
        )
    _verify_live_audit_position(
        redis_client,
        organization,
        audit_again,
        activation_sha256=activation_sha256,
    )
    created_ms = int((clock_ms or (lambda: int(time.time() * 1000)))())
    if created_ms < 1 or created_ms > 9_223_372_036_854_775_807:
        raise RecoveryError("checkpoint clock returned an invalid timestamp")
    unsigned: dict[str, Any] = {
        "version": _CHECKPOINT_VERSION,
        "kind": _CHECKPOINT_KIND,
        "organization": organization,
        "created_ms": created_ms,
        "redis_state": list(state),
        "snapshots": list(snapshots),
        "privileged_activation_sha256": activation_sha256,
        "privileged_audit": audit.binding(),
    }
    manifest_hash = _sha256(_canonical(unsigned))
    manifest = dict(unsigned)
    manifest["manifest_sha256"] = manifest_hash
    payload = _canonical(manifest)
    if len(payload) > _MAX_CHECKPOINT_BYTES:
        raise RecoveryError("catalog checkpoint exceeds the 64-MiB bound")
    path = (
        f"{_checkpoint_dir(organization)}/"
        f"checkpoint_{created_ms}_{manifest_hash}.json"
    )
    try:
        if storage.exists(path):
            if storage.read_bytes(path) != payload:
                raise RecoveryError("content-addressed catalog checkpoint already differs")
        else:
            writer = getattr(storage, "write_bytes_atomic", None)
            if callable(writer):
                writer(path, payload)
            else:
                storage.write_bytes(path, payload)
            ensure = getattr(storage, "ensure_bytes_durable", None)
            if callable(ensure):
                ensure(path)
        if storage.read_bytes(path) != payload:
            raise RecoveryError("catalog checkpoint failed read-after-write verification")
    except RecoveryError:
        raise
    except Exception as exc:
        raise RecoveryError(f"cannot durably publish catalog checkpoint: {exc}") from exc
    return CatalogCheckpoint(
        organization, created_ms, path, manifest_hash, len(state), len(snapshots),
    )


def _validate_checkpoint_manifest(
    storage: Any,
    organization: str,
    path: str,
) -> dict[str, Any]:
    path = _logical_path(path, field="checkpoint path")
    match = _CHECKPOINT_RE.fullmatch(PurePosixPath(path).name)
    if match is None:
        raise RecoveryError(f"unexpected file in catalog checkpoint set: {path}")
    payload = _read_bytes(
        storage, path, maximum=_MAX_CHECKPOINT_BYTES, label="catalog checkpoint",
    )
    manifest = _decode_json(payload, label=f"catalog checkpoint {path}")
    supplied = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    computed = _sha256(_canonical(unsigned))
    try:
        created_ms = int(manifest["created_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RecoveryError("catalog checkpoint timestamp is invalid") from exc
    if (
        manifest.get("version") != _CHECKPOINT_VERSION
        or manifest.get("kind") != _CHECKPOINT_KIND
        or manifest.get("organization") != organization
        or supplied != computed
        or match.group(1) != str(created_ms)
        or match.group(2) != computed
        or payload != _canonical(manifest)
    ):
        raise RecoveryError(f"catalog checkpoint identity/hash is invalid: {path}")
    state = manifest.get("redis_state")
    snapshots = manifest.get("snapshots")
    if not isinstance(state, list) or not isinstance(snapshots, list):
        raise RecoveryError("catalog checkpoint has invalid state/snapshot lists")
    actual_snapshots = _validate_catalog_state(storage, organization, state)
    if list(actual_snapshots) != snapshots:
        raise RecoveryError("catalog checkpoint snapshot seals differ from storage")
    activation_sha256 = _activation_sha256_from_state(organization, state)
    if manifest.get("privileged_activation_sha256") != activation_sha256:
        raise RecoveryError(
            "catalog checkpoint privileged activation seal differs from Redis state"
        )
    audit_binding = manifest.get("privileged_audit")
    if audit_binding is not None and not isinstance(audit_binding, Mapping):
        raise RecoveryError("catalog checkpoint has an invalid privileged audit binding")
    audit = _audit_plan(storage, organization, expected=audit_binding)
    if audit.tip_batch_id is not None and activation_sha256 is None:
        raise RecoveryError(
            "catalog checkpoint binds privileged audit history without activation"
        )
    return manifest


def _latest_checkpoint(storage: Any, organization: str) -> tuple[str, dict[str, Any]]:
    try:
        paths = storage.list_files(_checkpoint_dir(organization), "checkpoint_*.json")
    except Exception as exc:
        raise RecoveryError(f"cannot list catalog checkpoints: {exc}") from exc
    if not paths:
        raise RecoveryError(
            "no sealed Redis catalog checkpoint exists; refusing to guess from loose snapshots"
        )
    if len(paths) > _MAX_CHECKPOINTS:
        raise RecoveryError("catalog checkpoint set exceeds the verification bound")
    validated = [
        (path, _validate_checkpoint_manifest(storage, organization, path))
        for path in sorted(set(paths))
    ]
    newest_ms = max(int(manifest["created_ms"]) for _, manifest in validated)
    newest = [
        (path, manifest)
        for path, manifest in validated
        if int(manifest["created_ms"]) == newest_ms
    ]
    if len(newest) != 1:
        raise RecoveryError("latest catalog checkpoint is ambiguous")
    return newest[0]


def _audit_plan(
    storage: Any,
    organization: str,
    *,
    expected: object | Optional[Mapping[str, Any]] = _UNBOUND_AUDIT_POSITION,
    require_bound_tip_is_latest: bool = False,
) -> _AuditRecoveryState:
    """Verify either the current archive tip or one checkpoint-pinned tip."""

    from supertable.audit import get_privileged_audit_outbox
    from supertable.audit.privileged_outbox import PrivilegedAuditOutbox

    manifest_dir = f"{organization}/__audit__/privileged/manifests"
    try:
        paths = storage.list_files(manifest_dir, "batch_*.json")
    except Exception as exc:
        raise RecoveryError(f"cannot list privileged audit checkpoints: {exc}") from exc
    if not paths:
        if expected is not _UNBOUND_AUDIT_POSITION and expected is not None:
            raise RecoveryError("checkpoint-pinned privileged audit tip is missing")
        return _AuditRecoveryState()
    # A sealed catalog checkpoint may deliberately precede later archive
    # generations. Manifest validation can still prove that historical prefix,
    # but an actual rebuild may not roll the producer sequence behind it.
    if expected is None:
        if require_bound_tip_is_latest:
            raise RecoveryError(
                "privileged audit archive advanced beyond the catalog checkpoint"
            )
        return _AuditRecoveryState()
    if len(paths) > _MAX_AUDIT_BATCHES:
        raise RecoveryError("privileged audit chain exceeds the verification bound")
    outbox = get_privileged_audit_outbox(
        organization, redis_client=_ArchiveOnlyRedis(), storage=storage,
    )
    manifests: dict[str, Mapping[str, Any]] = {}
    for path in sorted(set(paths)):
        logical = _logical_path(path, field="audit checkpoint path")
        match = _AUDIT_MANIFEST_RE.fullmatch(PurePosixPath(logical).name)
        if match is None or logical != f"{manifest_dir}/{PurePosixPath(logical).name}":
            raise RecoveryError(f"unexpected privileged audit checkpoint path: {logical}")
        batch_id = match.group(1)
        try:
            manifest = outbox._read_checkpoint_manifest(
                batch_id, organization=organization, expected_path=logical,
            )
        except Exception as exc:
            raise RecoveryError(
                f"privileged audit checkpoint verification failed: {exc}"
            ) from exc
        manifests[batch_id] = manifest
    referenced = {
        str(previous["batch_id"])
        for manifest in manifests.values()
        for previous in (manifest.get("previous"),)
        if isinstance(previous, Mapping)
    }
    if not referenced.issubset(manifests):
        raise RecoveryError("privileged audit checkpoint predecessor is missing")
    if expected is _UNBOUND_AUDIT_POSITION:
        tips = set(manifests) - referenced
        if len(tips) != 1:
            raise RecoveryError("privileged audit checkpoint chain has an ambiguous tip")
        tip = tips.pop()
    else:
        if not isinstance(expected, Mapping):
            raise RecoveryError("catalog checkpoint has an invalid privileged audit binding")
        required_binding = {
            "version",
            "batch_count",
            "last_sequence",
            "last_stream_id",
            "tip_batch_id",
            "checkpoint_path",
            "checkpoint_sha256",
        }
        if set(expected) != required_binding or expected.get("version") != 1:
            raise RecoveryError("catalog checkpoint has an invalid privileged audit binding")
        tip_value = expected.get("tip_batch_id")
        if (
            not isinstance(tip_value, str)
            or not _AUDIT_MANIFEST_RE.fullmatch(f"batch_{tip_value}.json")
            or tip_value not in manifests
        ):
            raise RecoveryError("checkpoint-pinned privileged audit tip is missing")
        tip = tip_value
    order: list[str] = []
    current: Optional[str] = tip
    while current is not None:
        if current in order:
            raise RecoveryError("privileged audit checkpoint chain contains a cycle")
        order.append(current)
        previous = manifests[current].get("previous")
        current = str(previous["batch_id"]) if isinstance(previous, Mapping) else None
    if expected is _UNBOUND_AUDIT_POSITION and len(order) != len(manifests):
        raise RecoveryError("privileged audit checkpoint set is disconnected")
    if (
        expected is not _UNBOUND_AUDIT_POSITION
        and require_bound_tip_is_latest
        and len(order) != len(manifests)
    ):
        # Rolling the Redis producer head back while retaining a later durable
        # archive would reuse a ledger sequence and create two children of one
        # manifest. Refuse the recovery plan until catalog and audit are
        # checkpointed at one common tip (or storage is restored to that same
        # point-in-time generation).
        raise RecoveryError(
            "privileged audit archive advanced beyond the catalog checkpoint"
        )
    for batch_id in reversed(order):
        try:
            outbox.read_archive_batch(batch_id, organization=organization)
        except Exception as exc:
            raise RecoveryError(
                f"privileged audit archive verification failed: {exc}"
            ) from exc
    latest_manifest = manifests[tip]
    latest_entries = outbox.read_archive_batch(tip, organization=organization)
    if not latest_entries:
        raise RecoveryError("latest privileged audit archive is empty")
    latest = latest_entries[-1]
    sequence = int(latest.event["ledger_sequence"])
    if sequence != int(latest_manifest["last_sequence"]):
        raise RecoveryError("privileged audit tip sequence differs from archive")
    stream_fields = {"event_json": latest.event_json}
    stream_fields.update({
        field: str(latest.event.get(field, ""))
        for field in PrivilegedAuditOutbox._INDEX_FIELDS
    })
    meta = {
        "sequence": str(sequence),
        "last_stream_id": latest.stream_id,
        "last_event_id": str(latest.event["event_id"]),
        "last_payload_hash": str(latest.event["payload_hash"]),
        "updated_ms": str(latest.event.get("timestamp_ms", "")),
    }
    head = {
        "version": PrivilegedAuditOutbox._LEDGER_VERSION,
        "kind": "archive_head",
        "organization": organization,
        "batch_id": tip,
        "first_sequence": int(latest_manifest["first_sequence"]),
        "last_sequence": int(latest_manifest["last_sequence"]),
        "first_stream_id": str(latest_manifest["first_stream_id"]),
        "last_stream_id": str(latest_manifest["last_stream_id"]),
        "checkpoint_path": f"{manifest_dir}/batch_{tip}.json",
        "checkpoint_sha256": str(latest_manifest["manifest_sha256"]),
        "delivered_ms": int(latest.event.get("timestamp_ms", 0)),
    }
    delivery = {
        PrivilegedAuditOutbox._ARCHIVE_HEAD_FIELD: _canonical(head).decode("utf-8"),
        PrivilegedAuditOutbox._entry_field(latest.stream_id): tip,
    }
    checkpoint_path = f"{manifest_dir}/batch_{tip}.json"
    checkpoint_sha256 = str(latest_manifest["manifest_sha256"])
    state = _AuditRecoveryState(
        batch_count=len(order),
        last_sequence=sequence,
        last_stream_id=latest.stream_id,
        tip_batch_id=tip,
        checkpoint_path=checkpoint_path,
        checkpoint_sha256=checkpoint_sha256,
        stream_fields=stream_fields,
        meta=meta,
        delivery=delivery,
    )
    if expected is not _UNBOUND_AUDIT_POSITION:
        assert isinstance(expected, Mapping)
        if dict(expected) != state.binding():
            raise RecoveryError(
                "checkpoint-pinned privileged audit position differs from its archive"
            )
    return state


def plan_redis_rebuild(storage: Any, organization: str) -> RecoveryPlan:
    """Verify every source artifact and return an immutable restore plan."""
    _validate_identity(organization)
    checkpoint_path, manifest = _latest_checkpoint(storage, organization)
    audit = _audit_plan(
        storage,
        organization,
        expected=manifest.get("privileged_audit"),
        require_bound_tip_is_latest=True,
    )
    return RecoveryPlan(
        organization=organization,
        checkpoint_path=checkpoint_path,
        checkpoint_sha256=str(manifest["manifest_sha256"]),
        redis_state=tuple(manifest["redis_state"]),
        snapshot_count=len(manifest["snapshots"]),
        audit_batch_count=audit.batch_count,
        audit_last_sequence=audit.last_sequence,
        audit_last_stream_id=audit.last_stream_id,
        audit_tip_batch_id=audit.tip_batch_id,
        audit_checkpoint_path=audit.checkpoint_path,
        audit_checkpoint_sha256=audit.checkpoint_sha256,
        audit_stream_fields=audit.stream_fields,
        audit_meta=audit.meta,
        audit_delivery=audit.delivery,
    )


def _normalize_mapping(value: Mapping[Any, Any]) -> dict[str, str]:
    return {
        _text(key, field="Redis mapping key"):
        _text(item, field="Redis mapping value")
        for key, item in value.items()
    }


def _current_item(redis_client: Any, item: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    key = str(item["key"])
    try:
        key_type = _text(redis_client.type(key), field=f"TYPE {key}").lower()
        if key_type == "none":
            return None
        if key_type == "string":
            value: Any = _text(redis_client.get(key), field=f"value {key}")
        elif key_type == "hash":
            value = dict(sorted(_normalize_mapping(redis_client.hgetall(key)).items()))
        elif key_type == "set":
            value = sorted(
                _text(v, field=f"set member {key}")
                for v in redis_client.smembers(key)
            )
        elif key_type == "list":
            value = [
                _text(v, field=f"list item {key}")
                for v in redis_client.lrange(key, 0, -1)
            ]
        elif key_type == "zset":
            value = [
                [_text(member, field=f"zset member {key}"), format(float(score), ".17g")]
                for member, score in redis_client.zrange(key, 0, -1, withscores=True)
            ]
        else:
            return {"key": key, "type": key_type, "value": None}
    except Exception as exc:
        raise RecoveryError(f"cannot inspect recovery destination: {exc}") from exc
    return {"key": key, "type": key_type, "value": value}


def _audit_destination_state(redis_client: Any, plan: RecoveryPlan) -> str:
    stream_key = RK.audit_privileged_outbox(plan.organization)
    meta_key = RK.audit_privileged_meta(plan.organization)
    delivery_key = RK.audit_privileged_delivery(plan.organization)
    try:
        types = [
            _text(redis_client.type(key), field=f"TYPE {key}").lower()
            for key in (stream_key, meta_key, delivery_key)
        ]
    except Exception as exc:
        raise RecoveryError(f"cannot inspect privileged audit destination: {exc}") from exc
    if plan.audit_stream_fields is None:
        return "empty" if types == ["none", "none", "none"] else "conflict"
    if types == ["none", "none", "none"]:
        return "empty"
    if types != ["stream", "hash", "hash"]:
        return "conflict"
    try:
        entries = redis_client.xrange(stream_key, min="-", max="+")
        if len(entries) != 1:
            return "conflict"
        stream_id = _text(entries[0][0], field="audit stream ID")
        fields = _normalize_mapping(entries[0][1])
        if stream_id != plan.audit_last_stream_id or fields != dict(plan.audit_stream_fields):
            return "conflict"
        if _normalize_mapping(redis_client.hgetall(meta_key)) != dict(plan.audit_meta or {}):
            return "conflict"
        if _normalize_mapping(redis_client.hgetall(delivery_key)) != dict(plan.audit_delivery or {}):
            return "conflict"
        groups = redis_client.xinfo_groups(stream_key)
        if len(groups) != 1:
            return "conflict"
        group = {
            _text(key, field="consumer group field"):
            (_text(value, field="consumer group value") if isinstance(value, (str, bytes)) else str(value))
            for key, value in groups[0].items()
        }
        if (
            group.get("name") != _ARCHIVE_GROUP
            or group.get("last-delivered-id") != stream_id
        ):
            return "conflict"
    except Exception:
        return "conflict"
    return "exact"


def _destination_state(redis_client: Any, plan: RecoveryPlan) -> str:
    expected = _validate_state_items(plan.organization, plan.redis_state)
    present = set(_scan(redis_client, RK.org_pattern(plan.organization)))
    allowed = set(expected) | {
        RK.audit_privileged_outbox(plan.organization),
        RK.audit_privileged_meta(plan.organization),
        RK.audit_privileged_delivery(plan.organization),
    }
    if present - allowed:
        return "conflict"
    matches = 0
    missing = 0
    for item in expected.values():
        current = _current_item(redis_client, item)
        if current is None:
            missing += 1
        elif current == dict(item):
            matches += 1
        else:
            return "conflict"
    audit_state = _audit_destination_state(redis_client, plan)
    if matches == 0 and audit_state == "empty":
        return "empty"
    if missing == 0 and matches == len(expected):
        if plan.audit_stream_fields is None and audit_state == "empty":
            return "exact"
        if plan.audit_stream_fields is not None and audit_state == "exact":
            return "exact"
    return "conflict"


def _queue_state(pipe: Any, state: Sequence[Mapping[str, Any]]) -> None:
    for item in state:
        key = str(item["key"])
        key_type = item["type"]
        value = item["value"]
        if key_type == "string":
            pipe.set(key, value)
        elif key_type == "hash" and value:
            pipe.hset(key, mapping=value)
        elif key_type == "set" and value:
            pipe.sadd(key, *value)
        elif key_type == "list" and value:
            pipe.rpush(key, *value)
        elif key_type == "zset" and value:
            pipe.zadd(key, {member: float(score) for member, score in value})
        elif key_type not in _REDIS_TYPES:
            raise RecoveryError(f"unsupported recovery Redis type {key_type!r}")


def rebuild_redis(
    redis_client: Any,
    storage: Any,
    organization: str,
    *,
    dry_run: bool = True,
) -> RecoveryResult:
    """Verify and, only when explicitly requested, apply one exact DR plan."""
    if not isinstance(dry_run, bool):
        raise ValueError("dry_run must be a boolean")
    plan = plan_redis_rebuild(storage, organization)
    state = _destination_state(redis_client, plan)
    if state == "conflict":
        raise RecoveryError(
            "destination contains partial, stale, or unexpected organization state; "
            "recovery never overwrites it"
        )
    if dry_run or state == "exact":
        return RecoveryResult(
            organization, dry_run, False, state == "exact", plan.key_count,
            plan.snapshot_count, plan.audit_batch_count, plan.audit_last_sequence,
            plan.checkpoint_sha256,
        )
    try:
        pipe = redis_client.pipeline(transaction=True)
        _queue_state(pipe, plan.redis_state)
        if plan.audit_stream_fields is not None:
            stream_key = RK.audit_privileged_outbox(organization)
            pipe.xadd(
                stream_key,
                fields=dict(plan.audit_stream_fields),
                id=str(plan.audit_last_stream_id),
            )
            pipe.hset(
                RK.audit_privileged_meta(organization),
                mapping=dict(plan.audit_meta or {}),
            )
            pipe.hset(
                RK.audit_privileged_delivery(organization),
                mapping=dict(plan.audit_delivery or {}),
            )
            pipe.xgroup_create(
                stream_key, _ARCHIVE_GROUP, id=str(plan.audit_last_stream_id),
            )
        pipe.execute()
    except Exception as exc:
        raise RecoveryError(
            "Redis recovery transaction failed; keep the deployment stopped "
            f"and inspect destination: {exc}"
        ) from exc
    if _destination_state(redis_client, plan) != "exact":
        raise RecoveryError("Redis recovery failed exact read-back verification")
    return RecoveryResult(
        organization, False, True, False, plan.key_count, plan.snapshot_count,
        plan.audit_batch_count, plan.audit_last_sequence, plan.checkpoint_sha256,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="supertable-redis-recovery",
        description=(
            "Seal or rebuild organization Redis state using verified storage artifacts."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--organization", required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--checkpoint", action="store_true",
        help="seal current persistent Redis catalog state",
    )
    action.add_argument(
        "--dry-run", action="store_true",
        help="verify and print a rebuild plan without writes",
    )
    action.add_argument(
        "--apply", action="store_true",
        help="apply the verified plan to an empty destination",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    from supertable.redis_connector import RedisConnector
    from supertable.storage.storage_factory import get_storage

    redis_client = RedisConnector().r
    storage = get_storage()
    try:
        if args.checkpoint:
            result: Any = create_catalog_checkpoint(
                redis_client, storage, args.organization,
            )
        else:
            result = rebuild_redis(
                redis_client, storage, args.organization, dry_run=not args.apply,
            )
    except RecoveryError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, **result.__dict__}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
