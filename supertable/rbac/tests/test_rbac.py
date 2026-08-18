"""
Comprehensive RBAC test suite.

Tests every layer of the RBAC system:
  1. Permissions & RoleType logic
  2. RowColumnSecurity value object
  3. RedisCatalog RBAC operations (via in-memory FakeRedis)
  4. RoleManager business logic
  5. UserManager business logic
  6. access_control enforcement (check_write_access, check_meta_access)
  7. Edge cases, concurrency-like scenarios, and error paths

Run from project root:
  python -m pytest supertable/rbac/tests/test_rbac.py -v
"""

import json
import threading
import unittest
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List, Optional, Set


def _rbac_list_role_ids(cat, org: str, sup: str) -> List[str]:
    """RedisCatalog has ``rbac_list_user_ids`` but no symmetric
    ``rbac_list_role_ids``. The role index set key is owned by
    ``redis_keys.rbac_role_index`` though, so we mirror the user-side helper
    here rather than ask source code to grow a new method.
    """
    from supertable import redis_keys as RK
    members = cat.r.smembers(RK.rbac_role_index(org, sup))
    return [cat._decode_member(m) for m in (members or [])]


# ---------------------------------------------------------------------------
# Minimal in-memory Redis fake
# ---------------------------------------------------------------------------


class _FakeStream(list):
    """Type marker used to keep HASH/SET/STRING/STREAM failures realistic."""


class FakePipeline:
    """Buffers commands and executes them sequentially."""

    def __init__(self, store: "FakeRedis"):
        self._store = store
        self._cmds: list = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def hset(self, key, field=None, value=None, mapping=None):
        self._cmds.append(("hset", key, field, value, mapping))

    def sadd(self, key, *values):
        self._cmds.append(("sadd", key, values))

    def srem(self, key, *values):
        self._cmds.append(("srem", key, values))

    def hdel(self, key, *fields):
        self._cmds.append(("hdel", key, fields))

    def hincrby(self, key, field, amount=1):
        self._cmds.append(("hincrby", key, field, amount))

    def delete(self, *keys):
        for k in keys:
            self._cmds.append(("delete", k))

    def set(self, key, value, **kwargs):
        self._cmds.append(("set", key, value, kwargs))

    def get(self, key):
        self._cmds.append(("get", key))

    def hgetall(self, key):
        # The pipelined HGETALL used by RedisCatalog.get_users / get_roles.
        self._cmds.append(("hgetall", key))

    def execute(self):
        results = []
        for cmd in self._cmds:
            op = cmd[0]
            if op == "hset":
                results.append(self._store.hset(cmd[1], field=cmd[2], value=cmd[3], mapping=cmd[4]))
            elif op == "sadd":
                results.append(self._store.sadd(cmd[1], *cmd[2]))
            elif op == "srem":
                results.append(self._store.srem(cmd[1], *cmd[2]))
            elif op == "hdel":
                results.append(self._store.hdel(cmd[1], *cmd[2]))
            elif op == "hincrby":
                results.append(self._store.hincrby(cmd[1], cmd[2], cmd[3]))
            elif op == "delete":
                results.append(self._store.delete(cmd[1]))
            elif op == "set":
                results.append(self._store.set(cmd[1], cmd[2], **cmd[3]))
            elif op == "get":
                results.append(self._store.get(cmd[1]))
            elif op == "hgetall":
                results.append(self._store.hgetall(cmd[1]))
        self._cmds.clear()
        return results


class FakeScript:
    """Wraps a Lua source and executes a Python approximation."""

    def __init__(self, store: "FakeRedis", lua_src: str):
        self._store = store
        self._src = lua_src

    def __call__(self, keys=None, args=None):
        keys = list(keys or [])
        args = list(args or [])

        append_audit = None
        require_audit_identity = None
        if "privileged_audit_outbox" in self._src:
            if len(keys) < 2 or len(args) < 3:
                raise RuntimeError("missing privileged audit script arguments")

            outbox_key, audit_meta_key = keys[-2:]
            audit_org, audit_super, event_json = args[-3:]
            keys = keys[:-2]
            args = args[:-3]

            outbox = self._store._data.get(outbox_key)
            if outbox is not None and not isinstance(outbox, _FakeStream):
                raise RuntimeError("privileged audit outbox has wrong Redis type")
            audit_meta = self._store._data.get(audit_meta_key)
            if audit_meta is not None and not isinstance(audit_meta, dict):
                raise RuntimeError("privileged audit meta has wrong Redis type")
            if not isinstance(event_json, str):
                raise RuntimeError("invalid privileged audit record")
            if len(event_json.encode("utf-8")) > 65536:
                raise RuntimeError("privileged audit record exceeds 65536 bytes")
            try:
                audit_event = json.loads(event_json)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("invalid privileged audit record") from exc
            if not isinstance(audit_event, dict):
                raise RuntimeError("invalid privileged audit record")
            if audit_event.get("outcome") not in {
                "success", "failure", "denied", "no_change",
            }:
                raise RuntimeError("invalid privileged audit record")
            if (
                audit_event.get("organization") != audit_org
                or audit_event.get("super_name") != audit_super
            ):
                raise RuntimeError(
                    "privileged audit scope does not match RBAC commit"
                )
            required = (
                "event_id", "mutation_id", "organization", "super_name",
                "action", "resource_type", "resource_id", "payload_hash",
            )
            if any(
                not isinstance(audit_event.get(field), str)
                or not audit_event[field]
                for field in required
            ):
                raise RuntimeError("invalid privileged audit record")
            payload_hash = audit_event["payload_hash"]
            if len(payload_hash) != 64 or any(
                character not in "0123456789abcdef"
                for character in payload_hash
            ):
                raise RuntimeError("privileged audit payload hash is invalid")
            current_sequence = (
                self._store.hget(audit_meta_key, "sequence") or "0"
            )
            if (
                not isinstance(current_sequence, str)
                or not current_sequence.isdigit()
                or int(current_sequence) >= 9_223_372_036_854_775_807
            ):
                raise RuntimeError("privileged audit sequence is corrupt")
            next_sequence = str(int(current_sequence) + 1)

            def _require_audit_identity(
                action: str,
                resource_type: str,
                resource_id: str,
            ) -> None:
                if (
                    audit_event["outcome"] != "success"
                    or audit_event["action"] != action
                    or audit_event["resource_type"] != resource_type
                    or audit_event["resource_id"] != resource_id
                ):
                    raise RuntimeError(
                        "privileged audit identity does not match RBAC commit"
                    )

            def _append_audit(
                *,
                namespace_version: Any,
                affected_count: Any = 0,
                cascade_assignment_count: Any = 0,
                user_namespace_version_before: Any = 0,
                user_namespace_version_after: Any = 0,
            ) -> str:
                fields = {
                    "event_json": event_json,
                    "event_id": audit_event["event_id"],
                    "mutation_id": audit_event["mutation_id"],
                    "action": audit_event["action"],
                    "resource_type": audit_event["resource_type"],
                    "resource_id": audit_event["resource_id"],
                    "organization": audit_event["organization"],
                    "super_name": audit_event["super_name"],
                    "ledger_sequence": next_sequence,
                    "namespace_version": str(namespace_version),
                    "affected_count": str(affected_count),
                    "cascade_manifest_id": audit_event.get(
                        "cascade_manifest_id", ""
                    ),
                    "cascade_assignment_count": str(
                        cascade_assignment_count
                    ),
                    "user_namespace_version_before": str(
                        user_namespace_version_before
                    ),
                    "user_namespace_version_after": str(
                        user_namespace_version_after
                    ),
                    "payload_hash": payload_hash,
                }
                stream_id = self._store.xadd(outbox_key, fields)
                self._store.hset(
                    audit_meta_key,
                    mapping={
                        "sequence": next_sequence,
                        "last_stream_id": stream_id,
                        "last_event_id": audit_event["event_id"],
                        "last_payload_hash": payload_hash,
                        "updated_ms": str(audit_event.get("timestamp_ms", "")),
                    },
                )
                return stream_id

            append_audit = _append_audit
            require_audit_identity = _require_audit_identity

        def next_hash_counter(key: str, field: str = "version") -> str:
            value = self._store._data.get(key)
            if value is not None and not isinstance(value, dict):
                raise RuntimeError("RBAC commit key has wrong Redis type")
            current = self._store.hget(key, field) or "0"
            if (
                not isinstance(current, str)
                or not current.isdigit()
                or int(current) >= 9_223_372_036_854_775_807
            ):
                raise RuntimeError("RBAC/audit revision counter is corrupt")
            return str(int(current) + 1)

        if "successful privileged events require an RBAC mutation script" in self._src:
            with self._store._script_lock:
                namespace_key, *condition_keys = keys
                if audit_event["outcome"] == "success":
                    raise RuntimeError(
                        "successful privileged events require an RBAC mutation script"
                    )
                namespace = self._store._data.get(namespace_key)
                if namespace is not None and not isinstance(namespace, dict):
                    raise RuntimeError("RBAC commit key has wrong Redis type")
                namespace_version = self._store.hget(
                    namespace_key, "version",
                ) or "0"
                if (
                    not isinstance(namespace_version, str)
                    or not namespace_version.isdigit()
                    or (
                        len(namespace_version) > 1
                        and namespace_version.startswith("0")
                    )
                    or int(namespace_version) > 9_223_372_036_854_775_807
                ):
                    raise RuntimeError(
                        "RBAC namespace revision counter is corrupt"
                    )
                if args:
                    if len(args) != 1 or audit_event["outcome"] != "no_change":
                        raise RuntimeError("invalid RBAC attempt conditions")
                    conditions = json.loads(args[0])
                    if len(conditions) != len(condition_keys):
                        raise RuntimeError("invalid RBAC attempt conditions")
                    for condition, condition_key in zip(
                        conditions, condition_keys,
                    ):
                        value = self._store._data.get(condition_key)
                        kind = condition["kind"]
                        if kind == "absent":
                            matches = value is None
                        elif kind == "exists":
                            matches = value is not None
                        elif kind == "hash_fields":
                            matches = isinstance(value, dict) and all(
                                self._store.hget(condition_key, field["name"])
                                == field["value"]
                                for field in condition["fields"]
                            )
                        elif kind == "hash_field_absent":
                            matches = value is None or (
                                isinstance(value, dict)
                                and condition["field"] not in value
                            )
                        elif kind == "json_array_membership":
                            raw_roles = self._store.hget(
                                condition_key, condition["field"],
                            )
                            try:
                                roles = json.loads(raw_roles)
                            except (TypeError, ValueError):
                                roles = None
                            matches = (
                                isinstance(value, dict)
                                and isinstance(roles, list)
                                and self._store.hget(
                                    condition_key, "doc_version",
                                ) == condition["version"]
                                and (
                                    condition["item"] in roles
                                ) is condition["present"]
                            )
                        elif kind == "json_array_equals":
                            raw_roles = self._store.hget(
                                condition_key, condition["field"],
                            )
                            try:
                                roles = json.loads(raw_roles)
                            except (TypeError, ValueError):
                                roles = None
                            matches = (
                                isinstance(value, dict)
                                and isinstance(roles, list)
                                and all(isinstance(role, str) for role in roles)
                                and self._store.hget(
                                    condition_key, "doc_version",
                                ) == condition["version"]
                                and sorted(roles) == sorted(condition["items"])
                            )
                        elif kind == "set_cardinality":
                            matches = (
                                value is None and condition["count"] == "0"
                            ) or (
                                isinstance(value, set)
                                and str(len(value)) == condition["count"]
                            )
                        else:
                            raise RuntimeError("unknown RBAC attempt condition")
                        if not matches:
                            return 0
                append_audit(namespace_version=namespace_version)
                return 1

        if "local meta_type = normalized_type(KEYS[1])" in self._src:
            with self._store._script_lock:
                meta_key, index_key, name_key = keys
                meta = self._store._data.get(meta_key)
                index = self._store._data.get(index_key)
                names = self._store._data.get(name_key)
                if (
                    (meta is not None and not isinstance(meta, dict))
                    or (index is not None and not isinstance(index, set))
                    or (names is not None and not isinstance(names, dict))
                ):
                    return -2
                if meta is None:
                    if index or names:
                        return -1
                    return 0
                version = meta.get("version")
                if (
                    not isinstance(version, str)
                    or not version.isdigit()
                    or (len(version) > 1 and version.startswith("0"))
                    or int(version) > 9_223_372_036_854_775_807
                ):
                    return -1
                return 0

        if "role_document_json" in self._src and "HEXISTS" in self._src:
            with self._store._script_lock:
                doc_key, index_key, type_key, name_key, meta_key = keys
                role_id, _role_type, lower_name, document_json, now_ms = args
                require_audit_identity("role_create", "role", role_id)
                if self._store.exists(doc_key):
                    return -1
                if lower_name and self._store.hget(name_key, lower_name) is not None:
                    return -2
                document = json.loads(document_json)
                append_audit(namespace_version=next_hash_counter(meta_key))
                self._store.hset(doc_key, mapping=document)
                self._store.sadd(index_key, role_id)
                self._store.sadd(type_key, role_id)
                if lower_name:
                    self._store.hset(name_key, lower_name, role_id)
                self._store.hincrby(meta_key, "version", 1)
                self._store.hset(meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "user_document_json" in self._src and "HEXISTS" in self._src:
            with self._store._script_lock:
                doc_key, index_key, name_key, meta_key = keys
                user_id, lower_name, document_json, now_ms, role_doc_prefix = args
                require_audit_identity("user_create", "user", user_id)
                if self._store.exists(doc_key):
                    return -1
                if self._store.hget(name_key, lower_name) is not None:
                    return -2
                document = json.loads(document_json)
                try:
                    roles = json.loads(document.get("roles", "[]"))
                except (TypeError, ValueError):
                    return -3
                if not isinstance(roles, list) or any(
                    self._store.hget(
                        f"{role_doc_prefix}{assigned}", "role",
                    ) not in {"superadmin", "admin", "writer", "reader", "meta"}
                    for assigned in roles
                ):
                    return -3
                append_audit(namespace_version=next_hash_counter(meta_key))
                self._store.hset(doc_key, mapping=document)
                self._store.sadd(index_key, user_id)
                self._store.hset(name_key, lower_name, user_id)
                self._store.hincrby(meta_key, "version", 1)
                self._store.hset(meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "token_create" in self._src and "metadata_json = ARGV[2]" in self._src:
            with self._store._script_lock:
                token_key, token_meta_key = keys
                token_id, metadata_json, now_ms = args
                require_audit_identity("token_create", "auth_token", token_id)
                if self._store.hexists(token_key, token_id):
                    return -1
                metadata = json.loads(metadata_json)
                if metadata.get("token_id") != token_id:
                    raise RuntimeError("invalid auth token metadata")
                append_audit(
                    namespace_version=next_hash_counter(token_meta_key)
                )
                self._store.hset(token_key, token_id, metadata_json)
                self._store.hset(token_key, "_audit_initialized", "true")
                self._store.hincrby(token_meta_key, "version", 1)
                self._store.hset(token_meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "token_delete" in self._src and "expected_metadata = ARGV[2]" in self._src:
            with self._store._script_lock:
                token_key, token_meta_key = keys
                token_id, expected_metadata, now_ms = args
                require_audit_identity("token_delete", "auth_token", token_id)
                current = self._store.hget(token_key, token_id)
                if current is None:
                    return 0
                if current != expected_metadata:
                    return -1
                append_audit(
                    namespace_version=next_hash_counter(token_meta_key)
                )
                self._store.hdel(token_key, token_id)
                self._store.hset(token_key, "_audit_initialized", "true")
                self._store.hincrby(token_meta_key, "version", 1)
                self._store.hset(token_meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "role_update_document_json" in self._src:
            with self._store._script_lock:
                (
                    doc_key, index_key, old_type_key, new_type_key,
                    name_key, meta_key,
                ) = keys
                (
                    role_id, expected_role, expected_name, expected_tables,
                    expected_modified, expected_doc_version, new_role,
                    new_name, document_json, now_ms,
                ) = args
                require_audit_identity("role_update", "role", role_id)
                if not self._store.exists(doc_key):
                    return -1
                current_role = self._store.hget(doc_key, "role") or ""
                current_name = self._store.hget(doc_key, "role_name") or ""
                current_tables = self._store.hget(doc_key, "tables") or ""
                current_modified = self._store.hget(doc_key, "modified_ms") or ""
                current_doc_version = self._store.hget(
                    doc_key, "doc_version",
                ) or "0"
                if (
                    current_role != expected_role
                    or current_name != expected_name
                    or current_tables != expected_tables
                    or current_modified != expected_modified
                    or current_doc_version != expected_doc_version
                ):
                    return -3
                stored_id = self._store.hget(doc_key, "role_id")
                if stored_id is not None and stored_id != role_id:
                    return -6
                bootstrap_id = self._store.hget(name_key, "superadmin")
                is_bootstrap = (
                    current_name.lower() == "superadmin"
                    or bootstrap_id == role_id
                )
                if (
                    current_role == "superadmin" or is_bootstrap
                ) and new_role != "superadmin":
                    return -4
                if is_bootstrap and new_name.lower() != "superadmin":
                    return -5
                new_lower = new_name.lower()
                mapped_new = self._store.hget(name_key, new_lower)
                if new_lower and mapped_new is not None and mapped_new != role_id:
                    return -2

                document = json.loads(document_json)
                append_audit(namespace_version=next_hash_counter(meta_key))
                self._store.hset(doc_key, mapping=document)
                self._store.hincrby(doc_key, "doc_version", 1)
                self._store.sadd(index_key, role_id)
                if current_role != new_role:
                    self._store.srem(old_type_key, role_id)
                self._store.sadd(new_type_key, role_id)
                current_lower = current_name.lower()
                if current_lower != new_lower and current_lower:
                    if self._store.hget(name_key, current_lower) == role_id:
                        self._store.hdel(name_key, current_lower)
                if new_lower:
                    self._store.hset(name_key, new_lower, role_id)
                self._store.hincrby(meta_key, "version", 1)
                self._store.hset(meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "user_update_document_json" in self._src:
            with self._store._script_lock:
                doc_key, index_key, name_key, meta_key = keys
                (
                    user_id, expected_username, expected_roles,
                    expected_modified, expected_doc_version, new_username,
                    resulting_roles_json, document_json, now_ms, role_doc_prefix,
                ) = args
                require_audit_identity("user_update", "user", user_id)
                if not self._store.exists(doc_key):
                    return -1
                current_username = self._store.hget(doc_key, "username") or ""
                current_roles = self._store.hget(doc_key, "roles") or ""
                current_modified = self._store.hget(doc_key, "modified_ms") or ""
                current_doc_version = self._store.hget(
                    doc_key, "doc_version",
                ) or "0"
                if (
                    current_username != expected_username
                    or current_roles != expected_roles
                    or current_modified != expected_modified
                    or current_doc_version != expected_doc_version
                ):
                    return -3
                stored_id = self._store.hget(doc_key, "user_id")
                if stored_id is not None and stored_id != user_id:
                    return -7
                new_lower = new_username.lower()
                mapped_new = self._store.hget(name_key, new_lower)
                if mapped_new is not None and mapped_new != user_id:
                    return -2
                protected_id = self._store.hget(name_key, "superuser")
                is_protected = (
                    current_username.lower() == "superuser"
                    or protected_id == user_id
                )
                if is_protected and new_lower != "superuser":
                    return -4
                try:
                    resulting_roles = json.loads(resulting_roles_json)
                except (TypeError, ValueError):
                    return -6
                if not isinstance(resulting_roles, list):
                    return -6
                assigned_types = [
                    self._store.hget(
                        f"{role_doc_prefix}{assigned}", "role",
                    )
                    for assigned in resulting_roles
                ]
                if any(
                    role_type not in {
                        "superadmin", "admin", "writer", "reader", "meta",
                    }
                    for role_type in assigned_types
                ):
                    return -8
                if (
                    is_protected or new_lower == "superuser"
                ) and "superadmin" not in assigned_types:
                    return -5

                document = json.loads(document_json)
                append_audit(namespace_version=next_hash_counter(meta_key))
                self._store.hset(doc_key, mapping=document)
                self._store.hincrby(doc_key, "doc_version", 1)
                self._store.sadd(index_key, user_id)
                current_lower = current_username.lower()
                if current_lower != new_lower and current_lower:
                    if self._store.hget(name_key, current_lower) == user_id:
                        self._store.hdel(name_key, current_lower)
                self._store.hset(name_key, new_lower, user_id)
                self._store.hincrby(meta_key, "version", 1)
                self._store.hset(meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if (
            "expected_username" in self._src
            and "expected_roles" in self._src
            and "redis.call('DEL', KEYS[1])" in self._src
        ):
            with self._store._script_lock:
                doc_key, index_key, name_key, meta_key = keys
                (
                    user_id, expected_username, expected_roles,
                    expected_modified, expected_doc_version, now_ms,
                ) = args
                require_audit_identity("user_delete", "user", user_id)
                if not self._store.exists(doc_key):
                    return 0
                current_username = self._store.hget(doc_key, "username") or ""
                current_roles = self._store.hget(doc_key, "roles") or ""
                current_modified = self._store.hget(doc_key, "modified_ms") or ""
                current_doc_version = self._store.hget(
                    doc_key, "doc_version",
                ) or "0"
                if (
                    current_username != expected_username
                    or current_roles != expected_roles
                    or current_modified != expected_modified
                    or current_doc_version != expected_doc_version
                ):
                    return -2
                stored_id = self._store.hget(doc_key, "user_id")
                if stored_id is not None and stored_id != user_id:
                    return -3
                protected_id = self._store.hget(name_key, "superuser")
                if (
                    current_username.lower() == "superuser"
                    or protected_id == user_id
                ):
                    return -1
                append_audit(namespace_version=next_hash_counter(meta_key))
                current_lower = current_username.lower()
                if self._store.hget(name_key, current_lower) == user_id:
                    self._store.hdel(name_key, current_lower)
                self._store.delete(doc_key)
                self._store.srem(index_key, user_id)
                self._store.hincrby(meta_key, "version", 1)
                self._store.hset(meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                return 1

        if "HINCRBY" in self._src and "last_updated_ms" in self._src and "cjson" not in self._src:
            key = keys[0]
            now = args[0]
            v = self._store.hincrby(key, "version", 1)
            self._store.hset(key, mapping={"last_updated_ms": now})
            return v

        elif "SMEMBERS" in self._src and "DEL" in self._src and "SREM" in self._src:
            with self._store._script_lock:
                role_doc_key, role_index_key, role_type_index_key = keys[0:3]
                role_meta_key, user_index_key, name_key, user_meta_key = keys[3:7]
                cascade_manifest_key = keys[7]
                username_map_key = keys[8]
                (
                    role_id, now_ms, expected_role, expected_name,
                    expected_tables, expected_modified, expected_doc_version,
                    user_doc_key_prefix, expected_manifest_key,
                    cascade_user_limit,
                ) = args
                require_audit_identity("role_delete", "role", role_id)
                if not self._store.exists(role_doc_key):
                    return 0
                current_role = self._store.hget(role_doc_key, "role") or ""
                current_name = self._store.hget(role_doc_key, "role_name") or ""
                current_tables = self._store.hget(role_doc_key, "tables") or ""
                current_modified = self._store.hget(
                    role_doc_key, "modified_ms",
                ) or ""
                current_doc_version = self._store.hget(
                    role_doc_key, "doc_version",
                ) or "0"
                if (
                    current_role != expected_role
                    or current_name != expected_name
                    or current_tables != expected_tables
                    or current_modified != expected_modified
                    or current_doc_version != expected_doc_version
                ):
                    return -2
                stored_id = self._store.hget(role_doc_key, "role_id")
                if stored_id is not None and stored_id != role_id:
                    return -3
                if (
                    current_role == "superadmin"
                    or current_name.lower() == "superadmin"
                    or self._store.hget(name_key, "superadmin") == role_id
                ):
                    return -1
                if expected_manifest_key != cascade_manifest_key:
                    raise RuntimeError("role deletion cascade manifest identity mismatch")
                if self._store.exists(cascade_manifest_key):
                    raise RuntimeError("role deletion cascade manifest already exists")
                decoded_roles = {}
                user_ids = list(self._store.smembers(user_index_key) or set())
                username_map = self._store._data.get(username_map_key) or {}
                if (
                    len(user_ids) > int(cascade_user_limit)
                    or len(username_map) > int(cascade_user_limit)
                ):
                    return -5
                if len(user_ids) != len(username_map):
                    return -6
                for uid in user_ids:
                    ukey = f"{user_doc_key_prefix}{uid}"
                    if not self._store.exists(ukey):
                        return -6
                    stored_uid = self._store.hget(ukey, "user_id")
                    stored_username = self._store.hget(ukey, "username")
                    if (
                        stored_uid != uid
                        or not stored_username
                        or self._store.hget(
                            username_map_key, stored_username.lower(),
                        ) != uid
                    ):
                        return -6
                    roles_json = self._store.hget(ukey, "roles")
                    if roles_json is None:
                        return -4
                    try:
                        roles = json.loads(roles_json)
                    except (TypeError, ValueError):
                        return -4
                    if not isinstance(roles, list):
                        return -4
                    decoded_roles[uid] = roles
                if any(mapped not in user_ids for mapped in username_map.values()):
                    return -6
                affected_count = sum(
                    1 for roles in decoded_roles.values() if role_id in roles
                )
                assignment_count = sum(
                    roles.count(role_id) for roles in decoded_roles.values()
                )
                user_namespace_before = self._store.hget(
                    user_meta_key, "version",
                ) or "0"
                user_namespace_after = (
                    next_hash_counter(user_meta_key)
                    if affected_count else user_namespace_before
                )
                manifest = {
                    "schema_version": "1",
                    "event_id": audit_event["event_id"],
                    "mutation_id": audit_event["mutation_id"],
                    "organization": audit_org,
                    "super_name": audit_super,
                    "role_id": role_id,
                    "user_count": str(affected_count),
                    "removed_assignment_count": str(assignment_count),
                    "user_namespace_version_before": user_namespace_before,
                    "user_namespace_version_after": user_namespace_after,
                    "created_ms": now_ms,
                }
                for uid, roles in decoded_roles.items():
                    occurrences = roles.count(role_id)
                    if not occurrences:
                        continue
                    ukey = f"{user_doc_key_prefix}{uid}"
                    before_version = self._store.hget(
                        ukey, "doc_version",
                    ) or "0"
                    manifest[f"user:{uid}"] = "|".join((
                        before_version,
                        str(int(before_version) + 1),
                        str(occurrences),
                        str(len(roles)),
                        str(len(roles) - occurrences),
                    ))
                self._store.hset(cascade_manifest_key, mapping=manifest)
                append_audit(
                    namespace_version=next_hash_counter(role_meta_key),
                    affected_count=affected_count,
                    cascade_assignment_count=assignment_count,
                    user_namespace_version_before=user_namespace_before,
                    user_namespace_version_after=user_namespace_after,
                )
                users_changed = False
                for uid in user_ids:
                    ukey = f"{user_doc_key_prefix}{uid}"
                    roles = decoded_roles.get(uid)
                    if roles is not None:
                        new_roles = [assigned for assigned in roles if assigned != role_id]
                        if new_roles != roles:
                            self._store.hset(
                                ukey,
                                mapping={
                                    "roles": json.dumps(new_roles),
                                    "modified_ms": now_ms,
                                },
                            )
                            self._store.hincrby(ukey, "doc_version", 1)
                            users_changed = True
                role_name_lower = current_name.lower()
                if (
                    role_name_lower
                    and self._store.hget(name_key, role_name_lower) == role_id
                ):
                    self._store.hdel(name_key, role_name_lower)
                self._store.delete(role_doc_key)
                self._store.srem(role_index_key, role_id)
                self._store.srem(role_type_index_key, role_id)
                self._store.hincrby(role_meta_key, "version", 1)
                self._store.hset(role_meta_key, mapping={
                    "last_updated_ms": now_ms,
                    "initialized": "true",
                })
                if users_changed:
                    self._store.hincrby(user_meta_key, "version", 1)
                    self._store.hset(user_meta_key, mapping={
                        "last_updated_ms": now_ms,
                        "initialized": "true",
                    })
                return 1

        elif "roles[#roles + 1] = role_id" in self._src:
            user_doc_key, user_meta_key, role_doc_key = keys[0:3]
            role_id, now_ms, user_id = args[0:3]
            expected_roles = args[3]
            expected_doc_version = args[4]
            require_audit_identity(
                "user_role_assign",
                "user_role_assignment",
                f"{user_id}:{role_id}",
            )
            roles_json = self._store.hget(user_doc_key, "roles")
            if not roles_json:
                return 0
            current_doc_version = (
                self._store.hget(user_doc_key, "doc_version") or "0"
            )
            if (
                roles_json != expected_roles
                or current_doc_version != expected_doc_version
            ):
                return -2
            if self._store.hget(role_doc_key, "role") not in {
                "superadmin", "admin", "writer", "reader", "meta",
            }:
                return -1
            roles = json.loads(roles_json)
            if role_id in roles:
                return 0
            roles.append(role_id)
            next_hash_counter(user_doc_key, "doc_version")
            append_audit(namespace_version=next_hash_counter(user_meta_key))
            self._store.hset(user_doc_key, mapping={"roles": json.dumps(roles), "modified_ms": now_ms})
            self._store.hincrby(user_doc_key, "doc_version", 1)
            self._store.hincrby(user_meta_key, "version", 1)
            self._store.hset(user_meta_key, mapping={
                "last_updated_ms": now_ms,
                "initialized": "true",
            })
            return 1

        elif "new_roles[#new_roles + 1] = r" in self._src:
            user_doc_key, user_meta_key = keys[0], keys[1]
            role_id, now_ms = args[0], args[1]
            user_id = args[4]
            expected_roles = args[5]
            expected_doc_version = args[6]
            require_audit_identity(
                "user_role_remove",
                "user_role_assignment",
                f"{user_id}:{role_id}",
            )
            roles_json = self._store.hget(user_doc_key, "roles")
            if not roles_json:
                return 0
            current_doc_version = (
                self._store.hget(user_doc_key, "doc_version") or "0"
            )
            if (
                roles_json != expected_roles
                or current_doc_version != expected_doc_version
            ):
                return -2
            roles = json.loads(roles_json)
            if role_id not in roles:
                return 0
            protected_username = args[2] if len(args) > 2 else ""
            role_doc_prefix = args[3] if len(args) > 3 else ""
            username = self._store.hget(user_doc_key, "username") or ""
            protected_user_id = (
                self._store.hget(keys[2], protected_username)
                if len(keys) > 2 else None
            )
            target_type = self._store.hget(
                f"{role_doc_prefix}{role_id}", "role",
            )
            if (
                username.lower() == protected_username
                or protected_user_id == user_id
            ) and target_type == "superadmin":
                other_superadmins = [
                    assigned
                    for assigned in roles
                    if assigned != role_id
                    and self._store.hget(
                        f"{role_doc_prefix}{assigned}", "role",
                    ) == "superadmin"
                ]
                if not other_superadmins:
                    return -1
            roles.remove(role_id)
            next_hash_counter(user_doc_key, "doc_version")
            append_audit(namespace_version=next_hash_counter(user_meta_key))
            self._store.hset(user_doc_key, mapping={"roles": json.dumps(roles), "modified_ms": now_ms})
            self._store.hincrby(user_doc_key, "doc_version", 1)
            self._store.hincrby(user_meta_key, "version", 1)
            self._store.hset(user_meta_key, mapping={
                "last_updated_ms": now_ms,
                "initialized": "true",
            })
            return 1

        else:
            return 0


class FakeRedis:
    """Minimal in-memory Redis mock."""

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._script_lock = threading.Lock()
        self._stream_clock = 0

    def exists(self, key):
        return 1 if key in self._data else 0

    def delete(self, *keys):
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
        return count

    def get(self, key):
        v = self._data.get(key)
        return v if isinstance(v, str) else None

    def set(self, key, value, nx=False, ex=None, **kwargs):
        if nx and key in self._data:
            return None
        self._data[key] = str(value)
        return True

    def hset(self, key, field=None, value=None, mapping=None):
        if key not in self._data or not isinstance(self._data[key], dict):
            self._data[key] = {}
        if mapping:
            for k, v in mapping.items():
                self._data[key][k] = str(v) if not isinstance(v, str) else v
        if field is not None:
            self._data[key][field] = str(value) if not isinstance(value, str) else value
        return 1

    def hsetnx(self, key, field, value):
        if key not in self._data:
            self._data[key] = {}
        if not isinstance(self._data[key], dict):
            raise RuntimeError("WRONGTYPE Operation against a key")
        if field in self._data[key]:
            return 0
        self._data[key][field] = (
            str(value) if not isinstance(value, str) else value
        )
        return 1

    def hget(self, key, field):
        d = self._data.get(key)
        return d.get(field) if isinstance(d, dict) else None

    def hgetall(self, key):
        d = self._data.get(key)
        return dict(d) if isinstance(d, dict) else {}

    def hdel(self, key, *fields):
        d = self._data.get(key)
        count = 0
        if isinstance(d, dict):
            for f in fields:
                if f in d:
                    del d[f]
                    count += 1
        return count

    def hexists(self, key, field):
        d = self._data.get(key)
        return field in d if isinstance(d, dict) else False

    def hincrby(self, key, field, amount=1):
        if key not in self._data or not isinstance(self._data[key], dict):
            self._data[key] = {}
        cur = int(self._data[key].get(field, 0)) + amount
        self._data[key][field] = str(cur)
        return cur

    def sadd(self, key, *values):
        if key not in self._data or not isinstance(self._data[key], set):
            self._data[key] = set()
        added = 0
        for v in values:
            if v not in self._data[key]:
                self._data[key].add(v)
                added += 1
        return added

    def srem(self, key, *values):
        s = self._data.get(key)
        removed = 0
        if isinstance(s, set):
            for v in values:
                if v in s:
                    s.discard(v)
                    removed += 1
        return removed

    def smembers(self, key):
        s = self._data.get(key)
        return set(s) if isinstance(s, set) else set()

    def xadd(self, key, fields, id="*", **kwargs):
        existing = self._data.get(key)
        if existing is None:
            existing = _FakeStream()
            self._data[key] = existing
        if not isinstance(existing, _FakeStream):
            raise RuntimeError("WRONGTYPE Operation against a key")
        self._stream_clock += 1
        stream_id = (
            f"{self._stream_clock}-0" if id == "*" else str(id)
        )
        existing.append((stream_id, dict(fields)))
        return stream_id

    def xrange(self, key, min="-", max="+", count=None):
        stream = self._data.get(key)
        if stream is None:
            return []
        if not isinstance(stream, _FakeStream):
            raise RuntimeError("WRONGTYPE Operation against a key")
        entries = list(stream)
        if min != "-":
            entries = [entry for entry in entries if entry[0] >= min]
        if max != "+":
            entries = [entry for entry in entries if entry[0] <= max]
        return entries if count is None else entries[:count]

    def xrevrange(self, key, max="+", min="-", count=None):
        entries = list(reversed(self.xrange(key, min=min, max=max)))
        return entries if count is None else entries[:count]

    def xlen(self, key):
        return len(self.xrange(key))

    def pipeline(self):
        return FakePipeline(self)

    def register_script(self, lua_src):
        return FakeScript(self, lua_src)

    def scan(self, cursor=0, match=None, count=100):
        return (0, [])


# ---------------------------------------------------------------------------
# Import the real project modules — no sys.modules hacking
# ---------------------------------------------------------------------------

import supertable.redis_catalog as rc_module
from supertable import redis_keys as RK
from supertable.redis_catalog import (
    RbacDecisionError,
    RbacIntegrityError,
    RedisCatalog,
)
from supertable.rbac.permissions import Permission, RoleType, has_permission, ROLE_PERMISSIONS
from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager


# ---------------------------------------------------------------------------
# Helper to get a fresh RedisCatalog backed by FakeRedis
# ---------------------------------------------------------------------------

_shared_fake_redis = FakeRedis()

# ---------------------------------------------------------------------------
# Helper to get a fresh RedisCatalog + wiped FakeRedis for each test
# ---------------------------------------------------------------------------

def fresh_catalog() -> RedisCatalog:
    """Return a RedisCatalog backed by a freshly-wiped FakeRedis."""
    global _shared_fake_redis
    _shared_fake_redis = FakeRedis()
    # Patch the module-level reference so new RedisCatalog instances pick it up
    rc_module.RedisConnector = type("RC", (), {"__init__": lambda self, o=None: setattr(self, "r", _shared_fake_redis)})
    return RedisCatalog()


ORG = "test_org"
SUP = "test_super"


# ═══════════════════════════════════════════════════════════════════════════ #
#  1. Permissions tests                                                      #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestPermissions(unittest.TestCase):

    def test_superadmin_has_all_permissions(self):
        for perm in Permission:
            self.assertTrue(has_permission(RoleType.SUPERADMIN, perm))

    def test_admin_has_all_permissions(self):
        for perm in Permission:
            self.assertTrue(has_permission(RoleType.ADMIN, perm))

    def test_writer_permissions(self):
        self.assertTrue(has_permission(RoleType.WRITER, Permission.READ))
        self.assertTrue(has_permission(RoleType.WRITER, Permission.WRITE))
        self.assertTrue(has_permission(RoleType.WRITER, Permission.META))
        self.assertFalse(has_permission(RoleType.WRITER, Permission.CONTROL))
        self.assertFalse(has_permission(RoleType.WRITER, Permission.CREATE))

    def test_reader_permissions(self):
        self.assertTrue(has_permission(RoleType.READER, Permission.READ))
        self.assertTrue(has_permission(RoleType.READER, Permission.META))
        self.assertFalse(has_permission(RoleType.READER, Permission.WRITE))
        self.assertFalse(has_permission(RoleType.READER, Permission.CONTROL))

    def test_meta_permissions(self):
        self.assertTrue(has_permission(RoleType.META, Permission.META))
        self.assertFalse(has_permission(RoleType.META, Permission.READ))
        self.assertFalse(has_permission(RoleType.META, Permission.WRITE))

    def test_role_type_enum_values(self):
        self.assertEqual(RoleType.SUPERADMIN.value, "superadmin")
        self.assertEqual(RoleType.ADMIN.value, "admin")
        self.assertEqual(RoleType.WRITER.value, "writer")
        self.assertEqual(RoleType.READER.value, "reader")
        self.assertEqual(RoleType.META.value, "meta")

    def test_invalid_role_type_returns_no_permissions(self):
        # Passing something not in the map
        self.assertFalse(has_permission(MagicMock(), Permission.READ))


# ═══════════════════════════════════════════════════════════════════════════ #
#  2. RowColumnSecurity tests                                                #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRowColumnSecurity(unittest.TestCase):

    def test_basic_prepare(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["a", "b"]}, "t2": {"columns": ["a", "b"]}})
        rcs.prepare()
        self.assertIn("t1", rcs.tables)
        self.assertIn("t2", rcs.tables)
        self.assertEqual(rcs.tables["t1"]["columns"], ["a", "b"])
        self.assertIsNotNone(rcs.content_hash)

    def test_empty_tables_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables={})
        rcs.prepare()
        self.assertIn("*", rcs.tables)
        self.assertEqual(rcs.tables["*"]["columns"], ["*"])
        self.assertEqual(rcs.tables["*"]["filters"], ["*"])

    def test_empty_columns_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables={"t1": {}})
        rcs.prepare()
        self.assertEqual(rcs.tables["t1"]["columns"], ["*"])

    def test_empty_filters_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables={"t1": {}})
        rcs.prepare()
        self.assertEqual(rcs.tables["t1"]["filters"], ["*"])

    def test_tables_sorted_and_deduped(self):
        rcs = RowColumnSecurity(role="reader", tables={"z": {}, "a": {}, "m": {}})
        rcs.prepare()
        self.assertEqual(set(rcs.tables.keys()), {"a", "m", "z"})

    def test_columns_sorted_and_deduped(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["z", "a", "a"]}})
        rcs.prepare()
        self.assertEqual(rcs.tables["t1"]["columns"], ["a", "z"])

    def test_wildcard_columns_not_sorted(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["*"]}})
        rcs.prepare()
        self.assertEqual(rcs.tables["t1"]["columns"], ["*"])

    def test_content_hash_deterministic(self):
        rcs1 = RowColumnSecurity(role="reader", tables={"b": {"columns": ["x", "y"]}, "a": {"columns": ["x", "y"]}})
        rcs1.prepare()
        rcs2 = RowColumnSecurity(role="reader", tables={"a": {"columns": ["y", "x"]}, "b": {"columns": ["y", "x"]}})
        rcs2.prepare()
        self.assertEqual(rcs1.content_hash, rcs2.content_hash)

    def test_different_content_different_hash(self):
        rcs1 = RowColumnSecurity(role="reader", tables={"t1": {}})
        rcs1.prepare()
        rcs2 = RowColumnSecurity(role="writer", tables={"t1": {}})
        rcs2.prepare()
        self.assertNotEqual(rcs1.content_hash, rcs2.content_hash)

    def test_hash_property_alias(self):
        rcs = RowColumnSecurity(role="admin", tables={"t1": {}})
        rcs.prepare()
        self.assertEqual(rcs.hash, rcs.content_hash)

    def test_to_json_rejects_malformed_filter_shorthand(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"columns": ["a"], "filters": {"x": 1}}})
        with self.assertRaisesRegex(ValueError, "invalid filters"):
            rcs.prepare()

    def test_invalid_role_raises(self):
        with self.assertRaises(ValueError):
            RowColumnSecurity(role="nonexistent", tables={"t1": {}})


# ═══════════════════════════════════════════════════════════════════════════ #
#  3. RedisCatalog RBAC operations (low-level)                               #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRedisCatalogRbac(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()

    def test_init_role_meta_idempotent(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_role_meta(ORG, SUP)  # no error

    def test_init_user_meta_idempotent(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)

    def test_create_and_get_role(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        role_data = {
            "role_id": "r1",
            "role": "reader",
            "tables": {"t1": {"columns": ["a"], "filters": ["*"]}},
            "content_hash": "abc123",
        }
        self.cat.rbac_create_role(ORG, SUP, "r1", role_data)

        fetched = self.cat.get_role_details(ORG, SUP, "r1")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["role"], "reader")
        self.assertIn("t1", fetched["tables"])
        self.assertEqual(fetched["tables"]["t1"]["columns"], ["a"])

    def test_role_exists(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, "nope"))
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "admin", "role_id": "r1"})
        self.assertTrue(self.cat.rbac_role_exists(ORG, SUP, "r1"))

    def test_list_role_ids(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "admin", "role_id": "r1"})
        self.cat.rbac_create_role(ORG, SUP, "r2", {"role": "reader", "role_id": "r2"})
        ids = _rbac_list_role_ids(self.cat, ORG, SUP)
        self.assertEqual(sorted(ids), ["r1", "r2"])

    def test_get_role_ids_by_type(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "admin", "role_id": "r1"})
        self.cat.rbac_create_role(ORG, SUP, "r2", {"role": "reader", "role_id": "r2"})
        self.cat.rbac_create_role(ORG, SUP, "r3", {"role": "reader", "role_id": "r3"})
        admin_ids = self.cat.rbac_get_role_ids_by_type(ORG, SUP, "admin")
        reader_ids = self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader")
        self.assertEqual(admin_ids, ["r1"])
        self.assertEqual(sorted(reader_ids), ["r2", "r3"])

    def test_update_role(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "r1", {
            "role": "reader", "role_id": "r1",
            "tables": {"t1": {"columns": ["a"]}},
        })
        self.cat.rbac_update_role(ORG, SUP, "r1", {
            "tables": {"t1": {"columns": ["a", "b", "c"]}},
        })
        fetched = self.cat.get_role_details(ORG, SUP, "r1")
        self.assertEqual(fetched["tables"]["t1"]["columns"], ["a", "b", "c"])

    def test_catalog_create_canonicalizes_exclusion_policy(self):
        self.cat.rbac_create_role(ORG, SUP, "r-exclude", {
            "role": "reader",
            "role_id": "r-exclude",
            "tables": {
                "*": {"columns": ["*"], "filters": ["*"]},
                "account": {"access": "deny"},
                "card": {
                    "columns": ["*"],
                    "exclude_columns": ["pan", "cvv", "pan"],
                },
            },
            # A direct caller cannot persist a hash that describes a different
            # policy: the catalog recomputes it from canonical content.
            "content_hash": "forged",
        })

        fetched = self.cat.get_role_details(ORG, SUP, "r-exclude")
        self.assertEqual(fetched["tables"]["account"], {"access": "deny"})
        self.assertEqual(
            fetched["tables"]["card"]["exclude_columns"], ["cvv", "pan"],
        )
        self.assertNotEqual(fetched["content_hash"], "forged")

    def test_catalog_direct_create_rejects_malformed_policy_without_writing(self):
        with self.assertRaises(ValueError):
            self.cat.rbac_create_role(ORG, SUP, "bad", {
                "role": "reader",
                "tables": {"card": {"columns": ["*", "cvv"]}},
            })
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, "bad"))

    def test_catalog_partial_update_validates_merged_role(self):
        self.cat.rbac_create_role(ORG, SUP, "r-merge", {
            "role": "reader",
            "role_id": "r-merge",
            "tables": {"card": {"columns": ["*"]}},
        })
        # Model persisted corruption that is not part of the update patch.
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, "r-merge"),
            "tables",
            json.dumps({"card": {"access": "deny", "columns": ["id"]}}),
        )

        with self.assertRaises(RbacIntegrityError):
            self.cat.rbac_update_role(ORG, SUP, "r-merge", {"role_name": "renamed"})
        raw = self.cat.r.hgetall(RK.rbac_role_doc(ORG, SUP, "r-merge"))
        self.assertNotEqual(raw.get("role_name"), "renamed")

    def test_catalog_partial_update_rejects_tampered_empty_policy(self):
        self.cat.rbac_create_role(ORG, SUP, "r-empty", {
            "role": "reader", "role_id": "r-empty", "tables": {"t": {}},
        })
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, "r-empty"), "tables", "{}",
        )

        with self.assertRaises(RbacIntegrityError):
            self.cat.rbac_update_role(
                ORG, SUP, "r-empty", {"role_name": "empty"},
            )
        self.assertIsNone(
            self.cat.r.hget(
                RK.rbac_role_doc(ORG, SUP, "r-empty"), "role_name",
            )
        )

    def test_catalog_invalid_persisted_role_raises_integrity_error(self):
        self.cat.rbac_create_role(ORG, SUP, "r-corrupt", {
            "role": "reader", "role_id": "r-corrupt", "tables": {"card": {}},
        })
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, "r-corrupt"), "tables", "not-json",
        )
        with self.assertRaises(RbacIntegrityError):
            self.cat.get_role_details(ORG, SUP, "r-corrupt")
        with self.assertRaises(RbacIntegrityError):
            self.cat.get_roles(ORG, SUP)

    def test_catalog_direct_update_keeps_type_indexes_consistent(self):
        self.cat.rbac_create_role(ORG, SUP, "r-type", {
            "role": "reader", "role_id": "r-type", "tables": {"t": {}},
        })
        self.cat.rbac_update_role(ORG, SUP, "r-type", {"role": "writer"})

        self.assertNotIn(
            "r-type", self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"),
        )
        self.assertIn(
            "r-type", self.cat.rbac_get_role_ids_by_type(ORG, SUP, "writer"),
        )

    def test_catalog_direct_paths_protect_superadmin(self):
        self.cat.rbac_create_role(ORG, SUP, "sa-protected", {
            "role": "superadmin",
            "role_id": "sa-protected",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })

        with self.assertRaisesRegex(ValueError, "demoted"):
            self.cat.rbac_update_role(
                ORG, SUP, "sa-protected", {"role": "reader"},
            )
        with self.assertRaisesRegex(ValueError, "renamed"):
            self.cat.rbac_update_role(
                ORG, SUP, "sa-protected", {"role_name": "former_admin"},
            )
        with self.assertRaisesRegex(ValueError, "deleted"):
            self.cat.rbac_delete_role(ORG, SUP, "sa-protected")

        role = self.cat.get_role_details(ORG, SUP, "sa-protected")
        self.assertEqual(role["role"], "superadmin")
        self.assertEqual(role["role_name"], "superadmin")

    def test_catalog_reserves_bootstrap_name_and_protects_tampered_type(self):
        with self.assertRaisesRegex(ValueError, "reserved"):
            self.cat.rbac_create_role(ORG, SUP, "fake-root", {
                "role": "reader",
                "role_id": "fake-root",
                "role_name": "superadmin",
                "tables": {"*": {}},
            })

        self.cat.rbac_create_role(ORG, SUP, "sa-tampered", {
            "role": "superadmin",
            "role_id": "sa-tampered",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })
        # Model corruption produced by an older unprotected writer.
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, "sa-tampered"), "role", "reader",
        )
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            self.cat.rbac_delete_role(ORG, SUP, "sa-tampered")
        # Standard mutation APIs fail closed on corrupt persisted documents;
        # repair requires a dedicated operator recovery workflow.
        with self.assertRaises(RbacIntegrityError):
            self.cat.rbac_update_role(
                ORG, SUP, "sa-tampered", {"role": "superadmin"},
            )

    def test_bootstrap_mapping_protects_a_fully_tampered_role_document(self):
        self.cat.rbac_create_role(ORG, SUP, "sa-mapped", {
            "role": "superadmin",
            "role_id": "sa-mapped",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, "sa-mapped"),
            mapping={"role": "reader", "role_name": "former_admin"},
        )

        with self.assertRaises(RbacIntegrityError):
            self.cat.rbac_update_role(
                ORG, SUP, "sa-mapped", {"tables": {"public": {}}},
            )
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            self.cat.rbac_delete_role(ORG, SUP, "sa-mapped")

        with self.assertRaises(RbacIntegrityError):
            self.cat.rbac_update_role(ORG, SUP, "sa-mapped", {
                "role": "superadmin", "role_name": "superadmin",
            })

    def test_catalog_update_renames_mapping_and_document_together(self):
        self.cat.rbac_create_role(ORG, SUP, "r-rename", {
            "role": "reader",
            "role_id": "r-rename",
            "role_name": "old_name",
            "tables": {"t": {}},
        })
        version_before = int(
            self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version") or 0
        )

        self.cat.rbac_update_role(
            ORG, SUP, "r-rename", {"role_name": "new_name"},
        )

        self.assertIsNone(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "old_name"),
        )
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "new_name"), "r-rename",
        )
        self.assertEqual(
            self.cat.get_role_details(ORG, SUP, "r-rename")["role_name"],
            "new_name",
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version")),
            version_before + 1,
        )

    def test_atomic_role_create_closes_case_insensitive_name_race(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        original_script = self.cat._rbac_create_role
        barrier = threading.Barrier(2)

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_create_role = gated_script
        outcomes = []

        def create(role_id, role_name):
            try:
                self.cat.rbac_create_role(ORG, SUP, role_id, {
                    "role": "reader",
                    "role_id": role_id,
                    "role_name": role_name,
                    "tables": {"t": {}},
                })
                outcomes.append(("ok", role_id))
            except ValueError:
                outcomes.append(("conflict", role_id))

        threads = [
            threading.Thread(target=create, args=("race-a", "RaceRole")),
            threading.Thread(target=create, args=("race-b", "racerole")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
        winner = next(role_id for kind, role_id in outcomes if kind == "ok")
        loser = next(role_id for kind, role_id in outcomes if kind == "conflict")
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "RACEROLE"), winner,
        )
        self.assertTrue(self.cat.rbac_role_exists(ORG, SUP, winner))
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, loser))
        self.assertEqual(
            self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"), [winner],
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version")), 1,
        )

    def test_atomic_role_create_closes_bootstrap_superadmin_race(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        original_script = self.cat._rbac_create_role
        barrier = threading.Barrier(2)

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_create_role = gated_script
        outcomes = []

        def create(role_id, name):
            try:
                self.cat.rbac_create_role(ORG, SUP, role_id, {
                    "role": "superadmin",
                    "role_id": role_id,
                    "role_name": name,
                    "tables": {"*": {}},
                })
                outcomes.append(("ok", role_id))
            except ValueError:
                outcomes.append(("conflict", role_id))

        threads = [
            threading.Thread(target=create, args=("root-a", "superadmin")),
            threading.Thread(target=create, args=("root-b", "SUPERADMIN")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
        winner = next(role_id for kind, role_id in outcomes if kind == "ok")
        self.assertEqual(self.cat.rbac_get_superadmin_role_id(ORG, SUP), winner)
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "superadmin"), winner,
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version")), 1,
        )

    def test_atomic_role_rename_has_one_case_insensitive_winner(self):
        for role_id, name in (("rename-a", "old_a"), ("rename-b", "old_b")):
            self.cat.rbac_create_role(ORG, SUP, role_id, {
                "role": "reader", "role_id": role_id,
                "role_name": name, "tables": {"t": {}},
            })
        version_before = int(
            self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version") or 0
        )
        original_script = self.cat._rbac_update_role
        barrier = threading.Barrier(2)
        outcomes = []

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_update_role = gated_script

        def rename(role_id, name):
            try:
                self.cat.rbac_update_role(
                    ORG, SUP, role_id, {"role_name": name},
                )
                outcomes.append(("ok", role_id))
            except ValueError:
                outcomes.append(("conflict", role_id))

        threads = [
            threading.Thread(target=rename, args=("rename-a", "SharedRole")),
            threading.Thread(target=rename, args=("rename-b", "sharedrole")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
        winner = next(role_id for kind, role_id in outcomes if kind == "ok")
        loser = next(role_id for kind, role_id in outcomes if kind == "conflict")
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "SHAREDROLE"), winner,
        )
        self.assertEqual(
            self.cat.get_role_details(ORG, SUP, loser)["role_name"],
            "old_a" if loser == "rename-a" else "old_b",
        )
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(
                ORG, SUP, "old_a" if loser == "rename-a" else "old_b",
            ),
            loser,
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version")),
            version_before + 1,
        )

    def test_role_update_cannot_resurrect_concurrently_deleted_document(self):
        self.cat.rbac_create_role(ORG, SUP, "update-delete", {
            "role": "reader", "role_id": "update-delete",
            "role_name": "before_delete", "tables": {"t": {}},
        })
        original_script = self.cat._rbac_update_role
        invoked = False

        def delete_before_commit(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.assertTrue(
                    self.cat.rbac_delete_role(ORG, SUP, "update-delete")
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_update_role = delete_before_commit
        with self.assertRaisesRegex(ValueError, "does not exist"):
            self.cat.rbac_update_role(
                ORG, SUP, "update-delete", {"role_name": "resurrected"},
            )
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, "update-delete"))
        self.assertIsNone(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "before_delete"),
        )
        self.assertIsNone(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "resurrected"),
        )

    def test_role_delete_rejects_stale_snapshot_after_rename(self):
        self.cat.rbac_create_role(ORG, SUP, "delete-rename", {
            "role": "reader", "role_id": "delete-rename",
            "role_name": "delete_old", "tables": {"t": {}},
        })
        original_script = self.cat._rbac_delete_role
        invoked = False

        def rename_before_delete(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.cat.rbac_update_role(
                    ORG, SUP, "delete-rename", {"role_name": "delete_new"},
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_delete_role = rename_before_delete
        with self.assertRaisesRegex(ValueError, "changed concurrently"):
            self.cat.rbac_delete_role(ORG, SUP, "delete-rename")
        self.assertTrue(self.cat.rbac_role_exists(ORG, SUP, "delete-rename"))
        self.assertIsNone(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "delete_old"),
        )
        self.assertEqual(
            self.cat.rbac_get_role_id_by_name(ORG, SUP, "delete_new"),
            "delete-rename",
        )

    def test_role_delete_cannot_race_a_superadmin_promotion(self):
        self.cat.rbac_create_role(ORG, SUP, "promoted", {
            "role": "reader", "role_id": "promoted",
            "role_name": "promoted_role", "tables": {"t": {}},
        })
        original_script = self.cat._rbac_delete_role
        invoked = False

        def promote_before_delete(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.cat.rbac_update_role(
                    ORG, SUP, "promoted", {"role": "superadmin"},
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_delete_role = promote_before_delete
        with self.assertRaisesRegex(ValueError, "changed concurrently"):
            self.cat.rbac_delete_role(ORG, SUP, "promoted")
        self.assertEqual(
            self.cat.get_role_details(ORG, SUP, "promoted")["role"],
            "superadmin",
        )

    def test_update_nonexistent_role_raises(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        with self.assertRaises(ValueError):
            self.cat.rbac_update_role(ORG, SUP, "nope", {"tables": {"t1": {}}})

    def test_delete_role_strips_from_users(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)

        # Create a role and two users with that role
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "reader", "role_id": "r1"})
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": ["r1"],
            "created_ms": "0", "modified_ms": "0",
        })
        self.cat.rbac_create_user(ORG, SUP, "u2", {
            "user_id": "u2", "username": "bob", "roles": ["r1"],
            "created_ms": "0", "modified_ms": "0",
        })

        # Delete the role
        result = self.cat.rbac_delete_role(ORG, SUP, "r1")
        self.assertTrue(result)

        # Role should be gone
        self.assertIsNone(self.cat.get_role_details(ORG, SUP, "r1"))
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, "r1"))

        # Users should no longer have the role
        u1 = self.cat.get_user_details(ORG, SUP, "u1")
        u2 = self.cat.get_user_details(ORG, SUP, "u2")
        self.assertNotIn("r1", u1["roles"])
        self.assertNotIn("r1", u2["roles"])

    def test_delete_nonexistent_role_returns_false(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertFalse(self.cat.rbac_delete_role(ORG, SUP, "nope"))

    def test_create_and_get_user(self):
        for role_id in ("r1", "r2"):
            self.cat.rbac_create_role(ORG, SUP, role_id, {
                "role": "reader", "role_id": role_id, "tables": {"t": {}},
            })
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": ["r1", "r2"],
            "created_ms": "1000", "modified_ms": "1000",
        })
        fetched = self.cat.get_user_details(ORG, SUP, "u1")
        self.assertEqual(fetched["username"], "alice")
        self.assertEqual(fetched["roles"], ["r1", "r2"])

    def test_user_id_by_username(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "Alice",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        # Case-insensitive
        self.assertEqual(self.cat.rbac_get_user_id_by_username(ORG, SUP, "alice"), "u1")
        self.assertEqual(self.cat.rbac_get_user_id_by_username(ORG, SUP, "ALICE"), "u1")
        self.assertIsNone(self.cat.rbac_get_user_id_by_username(ORG, SUP, "bob"))

    def test_rename_user(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        self.cat.rbac_rename_user(ORG, SUP, "u1", "alice", "alice_new")
        self.assertIsNone(self.cat.rbac_get_user_id_by_username(ORG, SUP, "alice"))
        self.assertEqual(self.cat.rbac_get_user_id_by_username(ORG, SUP, "alice_new"), "u1")

    def test_delete_user(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        self.cat.rbac_delete_user(ORG, SUP, "u1")
        self.assertIsNone(self.cat.get_user_details(ORG, SUP, "u1"))
        self.assertIsNone(self.cat.rbac_get_user_id_by_username(ORG, SUP, "alice"))

    def test_delete_nonexistent_user_raises(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        with self.assertRaises(ValueError):
            self.cat.rbac_delete_user(ORG, SUP, "nope")

    def test_low_level_default_superuser_invariants(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "root-role", {
            "role": "superadmin",
            "role_id": "root-role",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })

        with self.assertRaisesRegex(ValueError, "retain"):
            self.cat.rbac_create_user(ORG, SUP, "bad-root", {
                "user_id": "bad-root", "username": "superuser", "roles": [],
            })

        self.cat.rbac_create_user(ORG, SUP, "root-user", {
            "user_id": "root-user",
            "username": "superuser",
            "roles": ["root-role"],
        })
        with self.assertRaisesRegex(ValueError, "renamed"):
            self.cat.rbac_update_user(
                ORG, SUP, "root-user", {"username": "former_root"},
            )
        with self.assertRaisesRegex(ValueError, "renamed"):
            self.cat.rbac_rename_user(
                ORG, SUP, "root-user", "superuser", "former_root",
            )
        with self.assertRaisesRegex(ValueError, "retain"):
            self.cat.rbac_update_user(ORG, SUP, "root-user", {"roles": []})
        with self.assertRaisesRegex(ValueError, "retain"):
            self.cat.rbac_remove_role_from_user(
                ORG, SUP, "root-user", "root-role",
            )
        with self.assertRaisesRegex(ValueError, "deleted"):
            self.cat.rbac_delete_user(ORG, SUP, "root-user")

        user = self.cat.get_user_details(ORG, SUP, "root-user")
        self.assertEqual(user["username"], "superuser")
        self.assertEqual(user["roles"], ["root-role"])

    def test_reserved_mapping_protects_a_tampered_superuser_document(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "root-role", {
            "role": "superadmin",
            "role_id": "root-role",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })
        self.cat.rbac_create_user(ORG, SUP, "root-user", {
            "user_id": "root-user",
            "username": "superuser",
            "roles": ["root-role"],
        })
        self.cat.r.hset(
            RK.rbac_user_doc(ORG, SUP, "root-user"),
            "username", "former_root",
        )

        with self.assertRaises(ValueError):
            self.cat.rbac_update_user(ORG, SUP, "root-user", {"roles": []})
        with self.assertRaisesRegex(ValueError, "renamed"):
            self.cat.rbac_rename_user(
                ORG, SUP, "root-user", "former_root", "another_user",
            )
        with self.assertRaisesRegex(ValueError, "retain"):
            self.cat.rbac_remove_role_from_user(
                ORG, SUP, "root-user", "root-role",
            )
        with self.assertRaisesRegex(ValueError, "deleted"):
            self.cat.rbac_delete_user(ORG, SUP, "root-user")

        self.cat.rbac_update_user(
            ORG, SUP, "root-user", {"username": "superuser"},
        )
        self.assertEqual(
            self.cat.get_user_details(ORG, SUP, "root-user")["username"],
            "superuser",
        )

    def test_low_level_superuser_can_drop_one_of_two_superadmin_roles(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)
        for role_id, name in (("root-a", "superadmin"), ("root-b", "backup_root")):
            self.cat.rbac_create_role(ORG, SUP, role_id, {
                "role": "superadmin",
                "role_id": role_id,
                "role_name": name,
                "tables": {"*": {}},
            })
        self.cat.rbac_create_user(ORG, SUP, "root-user", {
            "user_id": "root-user",
            "username": "superuser",
            "roles": ["root-a", "root-b"],
        })

        self.assertTrue(self.cat.rbac_remove_role_from_user(
            ORG, SUP, "root-user", "root-b",
        ))
        with self.assertRaisesRegex(ValueError, "retain"):
            self.cat.rbac_remove_role_from_user(
                ORG, SUP, "root-user", "root-a",
            )

    def test_atomic_user_create_closes_default_superuser_race(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "root-role", {
            "role": "superadmin",
            "role_id": "root-role",
            "role_name": "superadmin",
            "tables": {"*": {}},
        })
        original_script = self.cat._rbac_create_user
        barrier = threading.Barrier(2)

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_create_user = gated_script
        outcomes = []

        def create(user_id, username):
            try:
                self.cat.rbac_create_user(ORG, SUP, user_id, {
                    "user_id": user_id,
                    "username": username,
                    "roles": ["root-role"],
                })
                outcomes.append(("ok", user_id))
            except ValueError:
                outcomes.append(("conflict", user_id))

        threads = [
            threading.Thread(target=create, args=("user-a", "superuser")),
            threading.Thread(target=create, args=("user-b", "SUPERUSER")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
        winner = next(user_id for kind, user_id in outcomes if kind == "ok")
        loser = next(user_id for kind, user_id in outcomes if kind == "conflict")
        self.assertEqual(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, "SUPERUSER"), winner,
        )
        self.assertIsNotNone(self.cat.get_user_details(ORG, SUP, winner))
        self.assertIsNone(self.cat.get_user_details(ORG, SUP, loser))
        self.assertEqual(self.cat.rbac_list_user_ids(ORG, SUP), [winner])
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_user_meta(ORG, SUP), "version")), 1,
        )

    def test_atomic_user_rename_has_one_case_insensitive_winner(self):
        for user_id, username in (("rename-u-a", "user_old_a"), ("rename-u-b", "user_old_b")):
            self.cat.rbac_create_user(ORG, SUP, user_id, {
                "user_id": user_id, "username": username, "roles": [],
                "created_ms": "0", "modified_ms": "0",
            })
        version_before = int(
            self.cat.r.hget(RK.rbac_user_meta(ORG, SUP), "version") or 0
        )
        original_script = self.cat._rbac_update_user
        barrier = threading.Barrier(2)
        outcomes = []

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_update_user = gated_script

        def rename(user_id, old_name, new_name):
            try:
                self.cat.rbac_rename_user(
                    ORG, SUP, user_id, old_name, new_name,
                )
                outcomes.append(("ok", user_id))
            except ValueError:
                outcomes.append(("conflict", user_id))

        threads = [
            threading.Thread(
                target=rename,
                args=("rename-u-a", "user_old_a", "SharedUser"),
            ),
            threading.Thread(
                target=rename,
                args=("rename-u-b", "user_old_b", "shareduser"),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
        winner = next(user_id for kind, user_id in outcomes if kind == "ok")
        loser = next(user_id for kind, user_id in outcomes if kind == "conflict")
        self.assertEqual(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, "SHAREDUSER"),
            winner,
        )
        loser_old = "user_old_a" if loser == "rename-u-a" else "user_old_b"
        self.assertEqual(
            self.cat.get_user_details(ORG, SUP, loser)["username"], loser_old,
        )
        self.assertEqual(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, loser_old), loser,
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_user_meta(ORG, SUP), "version")),
            version_before + 1,
        )

    def test_user_update_cannot_resurrect_concurrently_deleted_document(self):
        self.cat.rbac_create_user(ORG, SUP, "update-delete-user", {
            "user_id": "update-delete-user", "username": "doomed_user",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        original_script = self.cat._rbac_update_user
        invoked = False

        def delete_before_commit(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.cat.rbac_delete_user(ORG, SUP, "update-delete-user")
            return original_script(*args, **kwargs)

        self.cat._rbac_update_user = delete_before_commit
        with self.assertRaisesRegex(ValueError, "does not exist"):
            self.cat.rbac_update_user(
                ORG, SUP, "update-delete-user", {"display_name": "ghost"},
            )
        self.assertIsNone(
            self.cat.get_user_details(ORG, SUP, "update-delete-user"),
        )
        self.assertIsNone(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, "doomed_user"),
        )

    def test_monotonic_doc_version_closes_same_millisecond_role_race(self):
        self.cat.rbac_create_role(ORG, SUP, "same-ms-role", {
            "role": "reader", "role_id": "same-ms-role",
            "role_name": "same_ms_role", "tables": {"t": {}},
        })
        original_script = self.cat._rbac_update_role
        barrier = threading.Barrier(2)
        outcomes = []

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_update_role = gated_script

        def update(value):
            try:
                self.cat.rbac_update_role(
                    ORG, SUP, "same-ms-role", {"enabled": value},
                )
                outcomes.append("ok")
            except ValueError:
                outcomes.append("stale")

        with patch("supertable.redis_catalog._now_ms", return_value=123456):
            threads = [
                threading.Thread(target=update, args=("first",)),
                threading.Thread(target=update, args=("second",)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sorted(outcomes), ["ok", "stale"])
        raw = self.cat.r.hgetall(RK.rbac_role_doc(ORG, SUP, "same-ms-role"))
        self.assertEqual(raw["doc_version"], "2")
        self.assertEqual(raw["modified_ms"], "123456")
        self.assertIn(raw["enabled"], ("first", "second"))

    def test_monotonic_doc_version_closes_same_millisecond_user_race(self):
        self.cat.rbac_create_user(ORG, SUP, "same-ms-user", {
            "user_id": "same-ms-user", "username": "same_ms_user",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        original_script = self.cat._rbac_update_user
        barrier = threading.Barrier(2)
        outcomes = []

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_update_user = gated_script

        def update(value):
            try:
                self.cat.rbac_update_user(
                    ORG, SUP, "same-ms-user", {"display_name": value},
                )
                outcomes.append("ok")
            except ValueError:
                outcomes.append("stale")

        with patch("supertable.redis_catalog._now_ms", return_value=123456):
            threads = [
                threading.Thread(target=update, args=("first",)),
                threading.Thread(target=update, args=("second",)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(sorted(outcomes), ["ok", "stale"])
        raw = self.cat.r.hgetall(RK.rbac_user_doc(ORG, SUP, "same-ms-user"))
        self.assertEqual(raw["doc_version"], "2")
        self.assertEqual(raw["modified_ms"], "123456")
        self.assertIn(raw["display_name"], ("first", "second"))

    def test_role_assignment_rechecks_existence_inside_atomic_commit(self):
        self.cat.rbac_create_role(ORG, SUP, "assignment-race", {
            "role": "reader", "role_id": "assignment-race",
            "role_name": "assignment_race", "tables": {"t": {}},
        })
        self.cat.rbac_create_user(ORG, SUP, "assignment-user", {
            "user_id": "assignment-user", "username": "assignment_user",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        self.assertTrue(
            self.cat.rbac_role_exists(ORG, SUP, "assignment-race")
        )
        original_script = self.cat._rbac_add_role_to_user
        invoked = False

        def delete_before_assignment(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.assertTrue(
                    self.cat.rbac_delete_role(ORG, SUP, "assignment-race")
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_add_role_to_user = delete_before_assignment
        with self.assertRaisesRegex(ValueError, "does not exist"):
            self.cat.rbac_add_role_to_user(
                ORG, SUP, "assignment-user", "assignment-race",
            )
        self.assertEqual(
            self.cat.get_user_details(ORG, SUP, "assignment-user")["roles"],
            [],
        )

    def test_user_role_update_rechecks_roles_inside_atomic_commit(self):
        self.cat.rbac_create_role(ORG, SUP, "role-update-race", {
            "role": "reader", "role_id": "role-update-race",
            "role_name": "role_update_race", "tables": {"t": {}},
        })
        self.cat.rbac_create_user(ORG, SUP, "role-update-user", {
            "user_id": "role-update-user", "username": "role_update_user",
            "roles": [], "created_ms": "0", "modified_ms": "0",
        })
        self.assertTrue(
            self.cat.rbac_role_exists(ORG, SUP, "role-update-race")
        )
        original_script = self.cat._rbac_update_user
        invoked = False

        def delete_before_update(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.assertTrue(
                    self.cat.rbac_delete_role(ORG, SUP, "role-update-race")
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_update_user = delete_before_update
        with self.assertRaisesRegex(RbacDecisionError, "does not exist"):
            self.cat.rbac_update_user(
                ORG, SUP, "role-update-user", {"roles": ["role-update-race"]},
            )
        self.assertEqual(
            self.cat.get_user_details(ORG, SUP, "role-update-user")["roles"],
            [],
        )

    def test_user_create_rechecks_roles_inside_atomic_commit(self):
        self.cat.rbac_create_role(ORG, SUP, "role-create-race", {
            "role": "reader", "role_id": "role-create-race",
            "role_name": "role_create_race", "tables": {"t": {}},
        })
        original_script = self.cat._rbac_create_user
        invoked = False

        def delete_before_create(*args, **kwargs):
            nonlocal invoked
            if not invoked:
                invoked = True
                self.assertTrue(
                    self.cat.rbac_delete_role(ORG, SUP, "role-create-race")
                )
            return original_script(*args, **kwargs)

        self.cat._rbac_create_user = delete_before_create
        with self.assertRaisesRegex(RbacDecisionError, "does not exist"):
            self.cat.rbac_create_user(ORG, SUP, "role-create-user", {
                "user_id": "role-create-user", "username": "role_create_user",
                "roles": ["role-create-race"],
                "created_ms": "0", "modified_ms": "0",
            })
        self.assertIsNone(
            self.cat.get_user_details(ORG, SUP, "role-create-user"),
        )
        self.assertIsNone(
            self.cat.rbac_get_user_id_by_username(
                ORG, SUP, "role_create_user",
            ),
        )

    def test_add_role_to_user(self):
        self.cat.rbac_create_role(ORG, SUP, "r1", {
            "role": "reader", "role_id": "r1", "tables": {"t": {}},
        })
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": [],
            "created_ms": "0", "modified_ms": "0",
        })
        result = self.cat.rbac_add_role_to_user(ORG, SUP, "u1", "r1")
        self.assertTrue(result)
        user = self.cat.get_user_details(ORG, SUP, "u1")
        self.assertIn("r1", user["roles"])

    def test_add_role_idempotent(self):
        self.cat.rbac_create_role(ORG, SUP, "r1", {
            "role": "reader", "role_id": "r1", "tables": {"t": {}},
        })
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": ["r1"],
            "created_ms": "0", "modified_ms": "0",
        })
        result = self.cat.rbac_add_role_to_user(ORG, SUP, "u1", "r1")
        self.assertFalse(result)  # Already present
        user = self.cat.get_user_details(ORG, SUP, "u1")
        self.assertEqual(user["roles"].count("r1"), 1)

    def test_remove_role_from_user(self):
        for role_id in ("r1", "r2"):
            self.cat.rbac_create_role(ORG, SUP, role_id, {
                "role": "reader", "role_id": role_id, "tables": {"t": {}},
            })
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": ["r1", "r2"],
            "created_ms": "0", "modified_ms": "0",
        })
        result = self.cat.rbac_remove_role_from_user(ORG, SUP, "u1", "r1")
        self.assertTrue(result)
        user = self.cat.get_user_details(ORG, SUP, "u1")
        self.assertEqual(user["roles"], ["r2"])

    def test_remove_nonexistent_role_noop(self):
        self.cat.rbac_create_role(ORG, SUP, "r1", {
            "role": "reader", "role_id": "r1", "tables": {"t": {}},
        })
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.cat.rbac_create_user(ORG, SUP, "u1", {
            "user_id": "u1", "username": "alice", "roles": ["r1"],
            "created_ms": "0", "modified_ms": "0",
        })
        result = self.cat.rbac_remove_role_from_user(ORG, SUP, "u1", "r999")
        self.assertFalse(result)

    def test_get_superadmin_role_id(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertIsNone(self.cat.rbac_get_superadmin_role_id(ORG, SUP))
        self.cat.rbac_create_role(ORG, SUP, "sa1", {"role": "superadmin", "role_id": "sa1"})
        self.assertEqual(self.cat.rbac_get_superadmin_role_id(ORG, SUP), "sa1")


class TestDurableRbacAuditBoundary(unittest.TestCase):
    """Adversarial coverage for the mandatory mutation/outbox transaction."""

    def setUp(self):
        self.cat = fresh_catalog()
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_init_user_meta(ORG, SUP)

    @property
    def outbox_key(self):
        return RK.audit_privileged_outbox(ORG)

    @property
    def audit_meta_key(self):
        return RK.audit_privileged_meta(ORG)

    def _reset_ledger(self):
        self.cat.r.delete(self.outbox_key, self.audit_meta_key)

    def _entries(self):
        return self.cat.r.xrange(self.outbox_key)

    def test_all_eight_mutations_append_one_ordered_commit_record(self):
        from supertable.audit.privileged import PrivilegedAuditRecord

        self.cat.rbac_create_role(ORG, SUP, "base-role", {
            "role": "reader",
            "role_id": "base-role",
            "role_name": "base_role",
            "tables": {"base": {}},
        })
        self._reset_ledger()

        self.cat.rbac_create_role(ORG, SUP, "target-role", {
            "role": "reader",
            "role_id": "target-role",
            "role_name": "target_role",
            "tables": {"target": {}},
        })
        self.cat.rbac_update_role(
            ORG, SUP, "target-role", {"role_name": "renamed_target"},
        )
        self.cat.rbac_create_user(ORG, SUP, "target-user", {
            "user_id": "target-user",
            "username": "target_user",
            "display_name": "Before",
            "roles": ["target-role"],
            "created_ms": "0",
            "modified_ms": "0",
        })
        self.cat.rbac_update_user(
            ORG, SUP, "target-user", {"display_name": "After"},
        )
        self.assertTrue(self.cat.rbac_add_role_to_user(
            ORG, SUP, "target-user", "base-role",
        ))
        self.assertTrue(self.cat.rbac_remove_role_from_user(
            ORG, SUP, "target-user", "base-role",
        ))
        self.assertTrue(self.cat.rbac_delete_role(
            ORG, SUP, "target-role",
        ))
        self.cat.rbac_delete_user(ORG, SUP, "target-user")

        entries = self._entries()
        expected_actions = [
            "role_create",
            "role_update",
            "user_create",
            "user_update",
            "user_role_assign",
            "user_role_remove",
            "role_delete",
            "user_delete",
        ]
        self.assertEqual(
            [fields["action"] for _, fields in entries],
            expected_actions,
        )
        self.assertEqual(
            [fields["ledger_sequence"] for _, fields in entries],
            [str(sequence) for sequence in range(1, 9)],
        )
        self.assertEqual(len({stream_id for stream_id, _ in entries}), 8)

        for sequence, (_, fields) in enumerate(entries, start=1):
            record = PrivilegedAuditRecord.from_json(fields["event_json"])
            self.assertEqual(record.action, fields["action"])
            self.assertEqual(record.event_id, fields["event_id"])
            self.assertEqual(record.mutation_id, fields["mutation_id"])
            self.assertEqual(record.resource_type, fields["resource_type"])
            self.assertEqual(record.resource_id, fields["resource_id"])
            self.assertEqual(record.organization, ORG)
            self.assertEqual(record.super_name, SUP)
            self.assertEqual(record.payload_hash, fields["payload_hash"])
            self.assertEqual(fields["ledger_sequence"], str(sequence))
            self.assertGreater(int(fields["namespace_version"]), 0)

        role_delete = entries[6][1]
        self.assertEqual(role_delete["affected_count"], "1")
        self.assertTrue(all(
            fields["affected_count"] == "0"
            for _, fields in entries[:6] + entries[7:]
        ))
        self.assertEqual(
            self.cat.r.hget(self.audit_meta_key, "sequence"), "8",
        )
        self.assertEqual(
            self.cat.r.hget(self.audit_meta_key, "last_event_id"),
            entries[-1][1]["event_id"],
        )

    def test_assignment_noops_and_cas_conflict_are_durably_recorded(self):
        from supertable.audit.privileged import PrivilegedAuditRecord

        self.cat.rbac_create_role(ORG, SUP, "stable-role", {
            "role": "reader",
            "role_id": "stable-role",
            "role_name": "stable_role",
            "tables": {"stable": {}},
        })
        self.cat.rbac_create_user(ORG, SUP, "stable-user", {
            "user_id": "stable-user",
            "username": "stable_user",
            "roles": ["stable-role"],
            "created_ms": "0",
            "modified_ms": "0",
        })
        self._reset_ledger()

        self.assertFalse(self.cat.rbac_add_role_to_user(
            ORG, SUP, "stable-user", "stable-role",
        ))
        self.assertFalse(self.cat.rbac_remove_role_from_user(
            ORG, SUP, "stable-user", "absent-role",
        ))

        original_script = self.cat._rbac_update_role

        def force_conflict(*args, **kwargs):
            script_keys = kwargs.get("keys", args[0] if args else [])
            self.cat.r.hset(script_keys[0], "doc_version", "99")
            return original_script(*args, **kwargs)

        self.cat._rbac_update_role = force_conflict
        with self.assertRaisesRegex(ValueError, "changed concurrently"):
            self.cat.rbac_update_role(
                ORG, SUP, "stable-role", {"role_name": "not_committed"},
            )

        entries = self._entries()
        self.assertEqual(
            [fields["action"] for _, fields in entries],
            ["user_role_assign", "user_role_remove", "role_update"],
        )
        records = [
            PrivilegedAuditRecord.from_json(fields["event_json"])
            for _, fields in entries
        ]
        self.assertEqual(
            [record.outcome for record in records],
            ["no_change", "no_change", "failure"],
        )
        self.assertEqual(
            [record.cause for record in records],
            [
                "role_already_assigned",
                "role_not_assigned",
                "concurrent_modification",
            ],
        )
        self.assertEqual(
            [fields["ledger_sequence"] for _, fields in entries],
            ["1", "2", "3"],
        )
        self.assertEqual(
            self.cat.r.hget(
                RK.rbac_role_doc(ORG, SUP, "stable-role"), "role_name",
            ),
            "stable_role",
        )

    def test_wrong_type_outbox_fails_closed_before_rbac_state_changes(self):
        self._reset_ledger()
        role_meta_before = self.cat.r.hgetall(RK.rbac_role_meta(ORG, SUP))
        role_index_before = self.cat.r.smembers(RK.rbac_role_index(ORG, SUP))
        self.cat.r.set(self.outbox_key, "not-a-stream")

        with self.assertRaisesRegex(RuntimeError, "outbox.*wrong Redis type"):
            self.cat.rbac_create_role(ORG, SUP, "must-not-exist", {
                "role": "reader",
                "role_id": "must-not-exist",
                "role_name": "must_not_exist",
                "tables": {"private": {}},
            })

        self.assertFalse(self.cat.rbac_role_exists(
            ORG, SUP, "must-not-exist",
        ))
        self.assertEqual(
            self.cat.r.smembers(RK.rbac_role_index(ORG, SUP)),
            role_index_before,
        )
        self.assertEqual(
            self.cat.r.hgetall(RK.rbac_role_meta(ORG, SUP)),
            role_meta_before,
        )
        self.assertEqual(self.cat.r.get(self.outbox_key), "not-a-stream")
        self.assertFalse(self.cat.r.exists(self.audit_meta_key))


# ═══════════════════════════════════════════════════════════════════════════ #
#  4. RoleManager tests                                                      #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRoleManager(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_superadmin_created_on_init(self):
        sa_id = self.rm.get_superadmin_role_id()
        self.assertIsNotNone(sa_id)
        role = self.rm.get_role(sa_id)
        self.assertEqual(role["role"], "superadmin")
        self.assertIn("*", role["tables"])

    def test_create_role_returns_uuid(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.assertEqual(len(role_id), 32)  # UUID hex

    def test_create_role_stored_correctly(self):
        role_id = self.rm.create_role({
            "role": "reader", "tables": {"t1": {"columns": ["a", "b"]}},
        })
        role = self.rm.get_role(role_id)
        self.assertEqual(role["role"], "reader")
        self.assertIn("t1", role["tables"])
        self.assertEqual(role["tables"]["t1"]["columns"], ["a", "b"])
        self.assertIn("content_hash", role)
        self.assertEqual(role["role_id"], role_id)

    def test_duplicate_content_creates_separate_roles(self):
        """No dedup — two roles with same content get different IDs."""
        id1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        id2 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.assertNotEqual(id1, id2)

    def test_named_create_is_idempotent_only_for_identical_content(self):
        role_data = {
            "role": "reader",
            "role_name": "named_reader",
            "tables": {"card": {"columns": ["id"]}},
        }
        role_id = self.rm.create_role(role_data)
        self.assertEqual(self.rm.create_role(role_data), role_id)

        with self.assertRaisesRegex(ValueError, "different content"):
            self.rm.create_role({
                **role_data,
                "tables": {
                    "card": {
                        "columns": ["*"],
                        "exclude_columns": ["cvv"],
                    }
                },
            })

        self.assertEqual(
            self.rm.get_role(role_id)["tables"],
            {"card": {"columns": ["id"], "filters": ["*"]}},
        )

    def test_concurrent_identical_named_creates_return_the_single_winner(self):
        original_script = self.cat._rbac_create_role
        barrier = threading.Barrier(2)
        version_before = int(
            self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version") or 0
        )

        def gated_script(*args, **kwargs):
            barrier.wait(timeout=2)
            return original_script(*args, **kwargs)

        self.cat._rbac_create_role = gated_script
        results = []
        errors = []
        role_data = {
            "role": "reader",
            "role_name": "concurrent_reader",
            "tables": {"card": {"columns": ["id"]}},
        }

        def create():
            try:
                results.append(self.rm.create_role(role_data))
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=create) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(set(results)), 1)
        self.assertEqual(
            self.rm.get_role_by_name("CONCURRENT_READER")["role_id"],
            results[0],
        )
        self.assertEqual(
            int(self.cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version")),
            version_before + 1,
        )

    def test_update_role_in_place(self):
        role_id = self.rm.create_role({
            "role": "reader", "tables": {"t1": {"columns": ["a"]}},
        })
        old_hash = self.rm.get_role(role_id)["content_hash"]
        new_hash = self.rm.update_role(role_id, {"tables": {"t1": {"columns": ["a", "b", "c"]}}})
        self.assertNotEqual(old_hash, new_hash)

        role = self.rm.get_role(role_id)
        self.assertEqual(role["tables"]["t1"]["columns"], ["a", "b", "c"])
        self.assertEqual(role["role_id"], role_id)  # ID unchanged

    def test_update_role_partial(self):
        """Only supplied fields change."""
        role_id = self.rm.create_role({
            "role": "reader", "tables": {"t1": {"columns": ["a"]}, "t2": {"columns": ["a"]}},
        })
        self.rm.update_role(role_id, {"tables": {"t1": {"columns": ["x", "y"]}, "t2": {"columns": ["x", "y"]}}})
        role = self.rm.get_role(role_id)
        self.assertIn("t1", role["tables"])
        self.assertEqual(role["tables"]["t1"]["columns"], ["x", "y"])

    def test_update_nonexistent_raises(self):
        with self.assertRaises(ValueError):
            self.rm.update_role("bogus", {"tables": {"t1": {"columns": ["x"]}}})

    def test_delete_role(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.assertTrue(self.rm.delete_role(role_id))
        self.assertEqual(self.rm.get_role(role_id), {})

    def test_update_cannot_demote_or_rename_bootstrap_superadmin(self):
        role_id = self.rm.get_superadmin_role_id()
        with self.assertRaisesRegex(ValueError, "demoted"):
            self.rm.update_role(role_id, {"role": "reader"})
        with self.assertRaisesRegex(ValueError, "renamed"):
            self.rm.update_role(role_id, {"role_name": "retired_root"})
        role = self.rm.get_role(role_id)
        self.assertEqual(role["role"], "superadmin")
        self.assertEqual(role["role_name"], "superadmin")

    def test_partial_manager_update_rejects_tampered_empty_policy(self):
        role_id = self.rm.create_role({
            "role": "reader", "role_name": "empty_reader", "tables": {"t": {}},
        })
        self.cat.r.hset(
            RK.rbac_role_doc(ORG, SUP, role_id), "tables", "{}",
        )
        with self.assertRaises(RbacIntegrityError):
            self.rm.update_role(
                role_id, {"role_name": "empty_reader_renamed"},
            )

    def test_delete_nonexistent_returns_false(self):
        self.assertFalse(self.rm.delete_role("bogus"))

    def test_list_roles(self):
        self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        roles = self.rm.list_roles()
        # +1 for the auto-created superadmin
        self.assertEqual(len(roles), 3)

    def test_get_roles_by_type(self):
        self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.create_role({"role": "reader", "tables": {"t2": {}}})
        self.rm.create_role({"role": "writer", "tables": {"t3": {}}})
        readers = self.rm.get_roles_by_type("reader")
        writers = self.rm.get_roles_by_type("writer")
        self.assertEqual(len(readers), 2)
        self.assertEqual(len(writers), 1)

    def test_invalid_role_type_raises(self):
        with self.assertRaises(ValueError):
            self.rm.create_role({"role": "invalid_type", "tables": {"t1": {}}})


# ═══════════════════════════════════════════════════════════════════════════ #
#  5. UserManager tests                                                      #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestUserManager(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_superuser_created_on_init(self):
        uid = self.um.get_or_create_default_user()
        self.assertIsNotNone(uid)
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "superuser")
        sa_id = self.rm.get_superadmin_role_id()
        self.assertIn(sa_id, user["roles"])

    def test_create_user_returns_uuid(self):
        uid = self.um.create_user({"username": "alice", "roles": []})
        self.assertEqual(len(uid), 32)

    def test_create_user_idempotent(self):
        uid1 = self.um.create_user({"username": "alice", "roles": []})
        uid2 = self.um.create_user({"username": "alice", "roles": []})
        self.assertEqual(uid1, uid2)

    def test_create_user_same_name_rejects_different_role_content(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t": {}}})
        uid = self.um.create_user({"username": "content_owner", "roles": []})
        with self.assertRaisesRegex(ValueError, "different content"):
            self.um.create_user({
                "username": "CONTENT_OWNER", "roles": [role_id],
            })
        self.assertEqual(
            self.um.get_user(uid)["roles"], [],
        )

    def test_create_user_case_insensitive_username(self):
        uid1 = self.um.create_user({"username": "Alice", "roles": []})
        uid2 = self.um.create_user({"username": "alice", "roles": []})
        self.assertEqual(uid1, uid2)

    def test_create_user_requires_username(self):
        with self.assertRaises(ValueError):
            self.um.create_user({"roles": []})

    def test_create_user_validates_roles(self):
        with self.assertRaises(ValueError):
            self.um.create_user({"username": "bad", "roles": ["nonexistent_role"]})

    def test_create_user_accepts_safe_usernames(self):
        """Plain ASCII, hyphenated, dotted, and email-style usernames
        must all round-trip — these are the shapes real IdPs emit."""
        for name in ("alice", "bob_smith", "ops-lead", "team.alpha",
                     "alice@acme.com", "_internal"):
            uid = self.um.create_user({"username": name, "roles": []})
            self.assertEqual(len(uid), 32, f"create_user should succeed for {name!r}")

    def test_create_user_rejects_special_and_non_ascii_chars(self):
        """``%``, ``$``, ``/``, ``|``, spaces, and accented Latin letters
        stay banned — usernames get interpolated into Redis keys and log
        lines so the safe set is intentionally small."""
        for name in ("bad%name", "É_admin", "ops/team", "ad$min",
                     "x|y", "alice bob", " leading", "trailing ", ""):
            with self.assertRaises(ValueError, msg=f"should reject {name!r}"):
                self.um.create_user({"username": name, "roles": []})

    def test_create_user_rejects_leading_digit_and_dot(self):
        """First character must be a letter or underscore — same rule
        as role_name so the two safe sets are easy to reason about."""
        for name in ("1alice", ".hidden", "-leading-dash", "@alice"):
            with self.assertRaises(ValueError, msg=f"should reject {name!r}"):
                self.um.create_user({"username": name, "roles": []})

    def test_modify_user_validates_new_username(self):
        """Renames are gated by the same rule — uniqueness check would
        otherwise be the only barrier, leaving unsafe names reachable
        via a rename of an existing safe user."""
        uid = self.um.create_user({"username": "renamable", "roles": []})
        with self.assertRaises(ValueError):
            self.um.modify_user(uid, {"username": "bad%name"})
        with self.assertRaises(ValueError):
            self.um.modify_user(uid, {"username": ""})

    def test_direct_catalog_write_cannot_bypass_username_validation(self):
        """The catalog-layer ``rbac_create_user`` / ``rbac_update_user``
        / ``rbac_rename_user`` re-check the rule so admin scripts and
        tests that call the lower layer directly can't slip an unsafe
        name past the validator."""
        # Dotted / email-style names are now legal at the catalog layer.
        self.cat.rbac_create_user(
            ORG, SUP, "u-ok",
            {"user_id": "u-ok", "username": "alice@acme.com", "roles": []},
        )
        # Special-char name is rejected at the catalog layer.
        with self.assertRaises(ValueError):
            self.cat.rbac_create_user(
                ORG, SUP, "u-bad",
                {"user_id": "u-bad", "username": "bad%name", "roles": []},
            )
        # Update path also gates on a renamed username.
        with self.assertRaises(ValueError):
            self.cat.rbac_update_user(
                ORG, SUP, "u-ok", {"username": "É_admin"},
            )
        # Rename path is the third write door — gate it too.
        with self.assertRaises(ValueError):
            self.cat.rbac_rename_user(
                ORG, SUP, "u-ok", "alice@acme.com", "bad name",
            )

    def test_get_user_not_found_raises(self):
        with self.assertRaises(ValueError):
            self.um.get_user("bogus")

    def test_get_user_by_name(self):
        uid = self.um.create_user({"username": "bob", "roles": []})
        user = self.um.get_user_by_name("bob")
        self.assertEqual(user["user_id"], uid)

    def test_get_user_by_name_not_found_raises(self):
        with self.assertRaises(ValueError):
            self.um.get_user_by_name("nobody")

    def test_modify_user_roles(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "carol", "roles": []})
        self.um.modify_user(uid, {"roles": [role_id]})
        user = self.um.get_user(uid)
        self.assertIn(role_id, user["roles"])

    def test_modify_user_invalid_role_raises(self):
        uid = self.um.create_user({"username": "dave", "roles": []})
        with self.assertRaises(ValueError):
            self.um.modify_user(uid, {"roles": ["fake_role"]})

    def test_modify_user_rename(self):
        uid = self.um.create_user({"username": "eve", "roles": []})
        self.um.modify_user(uid, {"username": "eve_updated"})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "eve_updated")
        # Old name should no longer resolve
        with self.assertRaises(ValueError):
            self.um.get_user_by_name("eve")
        # New name should resolve
        self.assertEqual(self.um.get_user_by_name("eve_updated")["user_id"], uid)

    def test_modify_user_rename_collision(self):
        self.um.create_user({"username": "frank", "roles": []})
        uid2 = self.um.create_user({"username": "grace", "roles": []})
        with self.assertRaises(ValueError):
            self.um.modify_user(uid2, {"username": "frank"})

    def test_modify_nonexistent_user_raises(self):
        with self.assertRaises(ValueError):
            self.um.modify_user("bogus", {"username": "x"})

    def test_delete_user(self):
        uid = self.um.create_user({"username": "heidi", "roles": []})
        self.um.delete_user(uid)
        with self.assertRaises(ValueError):
            self.um.get_user(uid)

    def test_delete_superuser_raises(self):
        uid = self.um.get_or_create_default_user()
        with self.assertRaises(ValueError):
            self.um.delete_user(uid)

    def test_delete_nonexistent_raises(self):
        with self.assertRaises(ValueError):
            self.um.delete_user("bogus")

    def test_list_users(self):
        self.um.create_user({"username": "user1", "roles": []})
        self.um.create_user({"username": "user2", "roles": []})
        users = self.um.list_users()
        usernames = {u["username"] for u in users}
        self.assertIn("user1", usernames)
        self.assertIn("user2", usernames)
        self.assertIn("superuser", usernames)

    def test_add_role_atomic(self):
        role_id = self.rm.create_role({"role": "writer", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "ivan", "roles": []})
        self.assertTrue(self.um.add_role(uid, role_id))
        user = self.um.get_user(uid)
        self.assertIn(role_id, user["roles"])

    def test_add_role_idempotent(self):
        role_id = self.rm.create_role({"role": "writer", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "judy", "roles": [role_id]})
        self.assertFalse(self.um.add_role(uid, role_id))  # Already has it

    def test_add_nonexistent_role_raises(self):
        uid = self.um.create_user({"username": "kevin", "roles": []})
        with self.assertRaises(ValueError):
            self.um.add_role(uid, "fake_role")

    def test_remove_role_atomic(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "lisa", "roles": [role_id]})
        self.assertTrue(self.um.remove_role(uid, role_id))
        user = self.um.get_user(uid)
        self.assertNotIn(role_id, user["roles"])

    def test_remove_role_not_present_noop(self):
        uid = self.um.create_user({"username": "mike", "roles": []})
        self.assertFalse(self.um.remove_role(uid, "r999"))

    def test_user_with_multiple_roles(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        r3 = self.rm.create_role({"role": "admin", "tables": {"*": {}}})
        uid = self.um.create_user({"username": "multi", "roles": [r1, r2, r3]})
        user = self.um.get_user(uid)
        self.assertEqual(len(user["roles"]), 3)
        self.assertIn(r1, user["roles"])
        self.assertIn(r2, user["roles"])
        self.assertIn(r3, user["roles"])


# ═══════════════════════════════════════════════════════════════════════════ #
#  6. Access control tests                                                   #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestAccessControl(unittest.TestCase):
    """Test check_write_access and check_meta_access (role-name based)."""

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def _patch_manager(self):
        """Patch RoleManager constructor to return our instance."""
        return patch("supertable.rbac.access_control.RoleManager", return_value=self.rm)

    def test_superadmin_can_write_anything(self):
        from supertable.rbac.access_control import check_write_access
        with self._patch_manager():
            check_write_access(SUP, ORG, "superadmin", "any_table")

    def test_superadmin_can_meta_anything(self):
        from supertable.rbac.access_control import check_meta_access
        with self._patch_manager():
            check_meta_access(SUP, ORG, "superadmin", "any_table")

    def test_writer_can_write_allowed_table(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "sales_writer", "tables": {"sales": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "sales_writer", "sales")

    def test_writer_cannot_write_other_table(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "sales_writer2", "tables": {"sales": {}}})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "sales_writer2", "secrets")

    def test_reader_cannot_write(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "reader", "role_name": "all_reader", "tables": {"*": {}}})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "all_reader", "any_table")

    def test_meta_role_can_meta(self):
        from supertable.rbac.access_control import check_meta_access
        self.rm.create_role({"role": "meta", "role_name": "stats_meta", "tables": {"stats": {}}})
        with self._patch_manager():
            check_meta_access(SUP, ORG, "stats_meta", "stats")

    def test_meta_role_cannot_meta_other_table(self):
        from supertable.rbac.access_control import check_meta_access
        self.rm.create_role({"role": "meta", "role_name": "stats_meta2", "tables": {"stats": {}}})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_meta_access(SUP, ORG, "stats_meta2", "other")

    def test_nonexistent_role_denied(self):
        from supertable.rbac.access_control import check_write_access
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "bogus_role_name", "t1")

    def test_wildcard_table_grants_all(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "admin", "role_name": "full_admin", "tables": {"*": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "full_admin", "anything")
            check_write_access(SUP, ORG, "full_admin", "some_other_table")

    def test_writer_role_covers_only_its_tables(self):
        """Must USE the correct role — writer for t2 cannot write t1."""
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "reader", "role_name": "t1_reader", "tables": {"t1": {}}})
        self.rm.create_role({"role": "writer", "role_name": "t2_writer", "tables": {"t2": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "t2_writer", "t2")  # OK
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "t1_reader", "t1")  # Reader can't write

    def test_role_name_case_insensitive(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "CamelWriter", "tables": {"t1": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "camelwriter", "t1")
            check_write_access(SUP, ORG, "CAMELWRITER", "t1")

    def test_role_name_accepts_dot_hyphen_underscore_space(self):
        """Dotted role names like ``Write.All`` (and other real-world
        conventions) must round-trip through create + lookup. The
        validator was tightened to reject ``.`` in a previous refactor,
        which broke login-time provisioning for installs whose IdP emits
        OAuth-scope-style role names."""
        for name in ("Write.All", "Read.All", "ops-team_lead", "team a.b"):
            rid = self.rm.create_role(
                {"role": "writer", "role_name": name, "tables": {"t1": {}}},
            )
            self.assertTrue(rid, f"create_role should succeed for {name!r}")

    def test_role_name_rejects_special_and_non_ascii_chars(self):
        """``%``, ``$``, ``/`` and accented Latin letters stay banned —
        role names get interpolated into Redis keys and log lines, so
        the safe set is intentionally small."""
        for name in ("Write%All", "É_admin", "ops/team", "ad$min", "x|y"):
            with self.assertRaises(ValueError, msg=f"should reject {name!r}"):
                self.rm.create_role(
                    {"role": "writer", "role_name": name, "tables": {"t1": {}}},
                )

    def test_role_name_rejects_leading_digit_and_dot(self):
        """First character must still be a letter or underscore — leading
        digits / dots are too easy to confuse with IDs or hidden files."""
        for name in ("1Writer", ".hidden", "-leading-dash"):
            with self.assertRaises(ValueError, msg=f"should reject {name!r}"):
                self.rm.create_role(
                    {"role": "writer", "role_name": name, "tables": {"t1": {}}},
                )

    def test_direct_catalog_write_cannot_bypass_role_name_validation(self):
        """The catalog-layer ``rbac_create_role`` / ``rbac_update_role``
        re-check the name rule, so admin scripts, migration jobs, and
        tests that call the lower layer directly can't slip an unsafe
        name past the validator. This is how ``Write.All`` originally
        landed in production — defense in depth closes that gap."""
        self.cat.rbac_init_role_meta(ORG, SUP)
        # Dotted name is now legal (allowed by the relaxed rule).
        self.cat.rbac_create_role(
            ORG, SUP, "r-dot",
            {"role": "writer", "role_id": "r-dot", "role_name": "Write.All"},
        )
        # Special-char name is rejected at the catalog layer.
        with self.assertRaises(ValueError):
            self.cat.rbac_create_role(
                ORG, SUP, "r-bad",
                {"role": "writer", "role_id": "r-bad", "role_name": "bad%name"},
            )
        # Update path also gates on the name.
        with self.assertRaises(ValueError):
            self.cat.rbac_update_role(
                ORG, SUP, "r-dot", {"role_name": "É_admin"},
            )


# ═══════════════════════════════════════════════════════════════════════════ #
#  7. Integration / edge case tests                                          #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestIntegrationEdgeCases(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_role_update_visible_to_existing_users(self):
        """Update a role's columns — user's resolved permissions change instantly."""
        role_id = self.rm.create_role({
            "role": "reader", "tables": {"t1": {"columns": ["a"]}},
        })
        uid = self.um.create_user({"username": "alice", "roles": [role_id]})

        # Before update
        user = self.um.get_user(uid)
        role = self.rm.get_role(user["roles"][0])
        self.assertEqual(role["tables"]["t1"]["columns"], ["a"])

        # Update the role
        self.rm.update_role(role_id, {"tables": {"t1": {"columns": ["a", "b", "c"]}}})

        # Same user, same role_id — new content
        role_after = self.rm.get_role(user["roles"][0])
        self.assertEqual(role_after["tables"]["t1"]["columns"], ["a", "b", "c"])

    def test_delete_role_with_many_users(self):
        """Role deleted — stripped from all 10 users atomically."""
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        user_ids = []
        for i in range(10):
            uid = self.um.create_user({"username": f"user_{i}", "roles": [role_id]})
            user_ids.append(uid)

        self.rm.delete_role(role_id)

        for uid in user_ids:
            user = self.um.get_user(uid)
            self.assertNotIn(role_id, user["roles"])

    def test_user_retains_other_roles_after_one_deleted(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        uid = self.um.create_user({"username": "multi", "roles": [r1, r2]})

        self.rm.delete_role(r1)

        user = self.um.get_user(uid)
        self.assertNotIn(r1, user["roles"])
        self.assertIn(r2, user["roles"])

    def test_create_many_roles_different_types(self):
        ids = {}
        for rtype in ("admin", "writer", "reader", "meta"):
            ids[rtype] = self.rm.create_role({"role": rtype, "tables": {"*": {}}})

        for rtype, rid in ids.items():
            role = self.rm.get_role(rid)
            self.assertEqual(role["role"], rtype)

    def test_shared_catalog_between_managers(self):
        """RoleManager and UserManager sharing same catalog see each other's data."""
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        # UserManager should be able to validate this role
        uid = self.um.create_user({"username": "shared_test", "roles": [role_id]})
        user = self.um.get_user(uid)
        self.assertIn(role_id, user["roles"])

    def test_role_id_stable_after_multiple_updates(self):
        role_id = self.rm.create_role({"role": "reader", "tables": {"t1": {"columns": ["a"]}}})
        for i in range(5):
            self.rm.update_role(role_id, {"tables": {"t1": {"columns": [f"col_{i}"]}}})
        role = self.rm.get_role(role_id)
        self.assertEqual(role["role_id"], role_id)
        self.assertEqual(role["tables"]["t1"]["columns"], ["col_4"])

    def test_user_id_stable_after_modifications(self):
        uid = self.um.create_user({"username": "stable", "roles": []})
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.um.add_role(uid, r1)
        self.um.modify_user(uid, {"username": "stable_renamed"})
        user = self.um.get_user(uid)
        self.assertEqual(user["user_id"], uid)

    def test_restrict_read_access_disabled(self):
        """restrict_read_access returns empty dict for superadmin with no tables."""
        from supertable.rbac.access_control import restrict_read_access
        with patch("supertable.rbac.access_control.RoleManager", return_value=self.rm):
            result = restrict_read_access(SUP, ORG, "superadmin", [], [])
        self.assertEqual(result, {})


# ═══════════════════════════════════════════════════════════════════════════ #
#  8. Bulk / stress tests                                                    #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestBulkOperations(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_create_50_roles(self):
        ids = []
        for i in range(50):
            rid = self.rm.create_role({"role": "reader", "tables": {f"t{i}": {}}})
            ids.append(rid)
        self.assertEqual(len(set(ids)), 50)
        # +1 for superadmin
        self.assertEqual(len(self.rm.list_roles()), 51)

    def test_create_50_users(self):
        ids = []
        for i in range(50):
            uid = self.um.create_user({"username": f"user_{i}", "roles": []})
            ids.append(uid)
        self.assertEqual(len(set(ids)), 50)
        # +1 for superuser
        users = self.um.list_users()
        self.assertEqual(len(users), 51)

    def test_assign_many_roles_to_one_user(self):
        role_ids = [self.rm.create_role({"role": "reader", "tables": {f"t{i}": {}}}) for i in range(20)]
        uid = self.um.create_user({"username": "multi_role", "roles": role_ids})
        user = self.um.get_user(uid)
        self.assertEqual(len(user["roles"]), 20)

    def test_one_role_assigned_to_many_users(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"shared": {}}})
        uids = []
        for i in range(30):
            uid = self.um.create_user({"username": f"user_{i}", "roles": [rid]})
            uids.append(uid)
        # Delete role — all 30 users should be stripped
        self.rm.delete_role(rid)
        for uid in uids:
            user = self.um.get_user(uid)
            self.assertNotIn(rid, user["roles"])

    def test_sequential_add_remove_roles(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        r3 = self.rm.create_role({"role": "admin", "tables": {"*": {}}})
        uid = self.um.create_user({"username": "toggle", "roles": []})

        self.um.add_role(uid, r1)
        self.um.add_role(uid, r2)
        self.um.add_role(uid, r3)
        self.assertEqual(len(self.um.get_user(uid)["roles"]), 3)

        self.um.remove_role(uid, r2)
        roles = self.um.get_user(uid)["roles"]
        self.assertEqual(len(roles), 2)
        self.assertNotIn(r2, roles)

        self.um.remove_role(uid, r1)
        self.um.remove_role(uid, r3)
        self.assertEqual(self.um.get_user(uid)["roles"], [])

    def test_delete_all_roles_except_superadmin(self):
        ids = [self.rm.create_role({"role": "reader", "tables": {f"t{i}": {}}}) for i in range(10)]
        for rid in ids:
            self.rm.delete_role(rid)
        remaining = self.rm.list_roles()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["role"], "superadmin")


# ═══════════════════════════════════════════════════════════════════════════ #
#  9. Cascade / dependency tests                                             #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestCascadeAndDependency(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_delete_role_removes_from_type_index(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.assertIn(rid, self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"))
        self.rm.delete_role(rid)
        self.assertNotIn(rid, self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"))

    def test_delete_role_removes_from_global_index(self):
        rid = self.rm.create_role({"role": "writer", "tables": {"t1": {}}})
        self.assertIn(rid, _rbac_list_role_ids(self.cat, ORG, SUP))
        self.rm.delete_role(rid)
        self.assertNotIn(rid, _rbac_list_role_ids(self.cat, ORG, SUP))

    def test_delete_user_removes_from_index(self):
        uid = self.um.create_user({"username": "temp", "roles": []})
        self.assertIn(uid, self.cat.rbac_list_user_ids(ORG, SUP))
        self.um.delete_user(uid)
        self.assertNotIn(uid, self.cat.rbac_list_user_ids(ORG, SUP))

    def test_delete_user_removes_username_mapping(self):
        uid = self.um.create_user({"username": "removeme", "roles": []})
        self.assertEqual(self.cat.rbac_get_user_id_by_username(ORG, SUP, "removeme"), uid)
        self.um.delete_user(uid)
        self.assertIsNone(self.cat.rbac_get_user_id_by_username(ORG, SUP, "removeme"))

    def test_user_with_all_roles_deleted(self):
        """User had 3 roles, all 3 get deleted — user ends up with empty roles."""
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        r3 = self.rm.create_role({"role": "admin", "tables": {"t3": {}}})
        uid = self.um.create_user({"username": "doomed", "roles": [r1, r2, r3]})

        self.rm.delete_role(r1)
        self.rm.delete_role(r2)
        self.rm.delete_role(r3)

        user = self.um.get_user(uid)
        self.assertEqual(user["roles"], [])

    def test_role_update_does_not_affect_other_roles(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {"columns": ["a"]}}})
        r2 = self.rm.create_role({"role": "reader", "tables": {"t2": {"columns": ["b"]}}})
        self.rm.update_role(r1, {"tables": {"t1": {"columns": ["x", "y", "z"]}}})
        role2 = self.rm.get_role(r2)
        self.assertEqual(role2["tables"]["t2"]["columns"], ["b"])  # Untouched

    def test_role_update_changes_content_hash(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {"columns": ["a"]}}})
        hash_before = self.rm.get_role(rid)["content_hash"]
        self.rm.update_role(rid, {"tables": {"t1": {"columns": ["a", "b"]}}})
        hash_after = self.rm.get_role(rid)["content_hash"]
        self.assertNotEqual(hash_before, hash_after)

    def test_delete_role_then_recreate_same_content(self):
        """After deleting a role, creating one with same content gets a new ID."""
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.delete_role(r1)
        r2 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.assertNotEqual(r1, r2)
        self.assertIsNotNone(self.rm.get_role(r2))

    def test_user_references_to_deleted_role_are_cleaned(self):
        """After role deletion, user's role list no longer contains it, even via list_users."""
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "checker", "roles": [rid]})
        self.rm.delete_role(rid)

        # Via direct get
        self.assertNotIn(rid, self.um.get_user(uid)["roles"])
        # Via list
        for u in self.um.list_users():
            if u.get("user_id") == uid:
                self.assertNotIn(rid, u["roles"])


# ═══════════════════════════════════════════════════════════════════════════ #
#  10. Username edge cases                                                   #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestUsernameEdgeCases(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_username_with_spaces_rejected(self):
        """Spaces in usernames are now rejected.

        Spaceless usernames are safer to interpolate into Redis keys,
        log lines, and CLI arguments. The previous permissive behavior
        worked only because no validator existed; with one in place we
        intentionally bar this shape. See ``SAFE_USERNAME_RE`` in
        ``redis_catalog`` for the contract."""
        with self.assertRaises(ValueError):
            self.um.create_user({"username": "john doe", "roles": []})

    def test_username_with_special_chars(self):
        uid = self.um.create_user({"username": "user@domain.com", "roles": []})
        user = self.um.get_user_by_name("user@domain.com")
        self.assertEqual(user["user_id"], uid)

    def test_username_unicode_rejected(self):
        """Non-ASCII usernames are now rejected.

        Accented Latin and CJK glyphs make audit trails ambiguous (the
        same logical name can be encoded multiple ways) and complicate
        safe Redis-key interpolation. The previous permissive behavior
        worked only because no validator existed; with one in place we
        intentionally restrict to the ASCII safe set."""
        with self.assertRaises(ValueError):
            self.um.create_user({"username": "用户名", "roles": []})

    def test_username_mixed_case_lookup(self):
        uid = self.um.create_user({"username": "MiXeD_CaSe", "roles": []})
        self.assertEqual(self.um.get_user_by_name("mixed_case")["user_id"], uid)
        self.assertEqual(self.um.get_user_by_name("MIXED_CASE")["user_id"], uid)

    def test_rename_to_same_case_variation(self):
        """Rename alice -> Alice (same username, different case) should work."""
        uid = self.um.create_user({"username": "alice", "roles": []})
        self.um.modify_user(uid, {"username": "Alice"})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "Alice")

    def test_rename_preserves_roles(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "before", "roles": [rid]})
        self.um.modify_user(uid, {"username": "after"})
        user = self.um.get_user(uid)
        self.assertIn(rid, user["roles"])


# ═══════════════════════════════════════════════════════════════════════════ #
#  11. Backward-compatibility alias tests                                    #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestBackwardCompatAliases(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_get_superadmin_role_hash_alias(self):
        sa_id = self.rm.get_superadmin_role_hash()
        self.assertIsNotNone(sa_id)
        self.assertEqual(sa_id, self.rm.get_superadmin_role_id())

    def test_get_user_hash_by_name_alias(self):
        uid = self.um.create_user({"username": "bob", "roles": []})
        user = self.um.get_user_hash_by_name("bob")
        self.assertEqual(user["user_id"], uid)

    def test_remove_role_from_users_deprecated(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        u1 = self.um.create_user({"username": "u1", "roles": [rid]})
        u2 = self.um.create_user({"username": "u2", "roles": [rid]})
        with self.assertRaisesRegex(RbacDecisionError, "unsupported"):
            self.um.remove_role_from_users(rid)
        self.assertIn(rid, self.um.get_user(u1)["roles"])
        self.assertIn(rid, self.um.get_user(u2)["roles"])


# ═══════════════════════════════════════════════════════════════════════════ #
#  12. Empty state / edge cases                                              #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestEmptyState(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()

    def test_list_roles_on_empty_state(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertEqual(self.cat.get_roles(ORG, SUP), [])

    def test_list_users_on_empty_state(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.assertEqual(self.cat.get_users(ORG, SUP), [])

    def test_get_nonexistent_role(self):
        self.assertIsNone(self.cat.get_role_details(ORG, SUP, "fake"))

    def test_get_nonexistent_user(self):
        self.assertIsNone(self.cat.get_user_details(ORG, SUP, "fake"))

    def test_superadmin_role_id_none_when_empty(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertIsNone(self.cat.rbac_get_superadmin_role_id(ORG, SUP))

    def test_list_role_ids_empty(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertEqual(_rbac_list_role_ids(self.cat, ORG, SUP), [])

    def test_list_user_ids_empty(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.assertEqual(self.cat.rbac_list_user_ids(ORG, SUP), [])

    def test_user_id_by_username_none_when_empty(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        self.assertIsNone(self.cat.rbac_get_user_id_by_username(ORG, SUP, "nobody"))

    def test_role_ids_by_type_empty(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertEqual(self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"), [])

    def test_add_role_to_nonexistent_user(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        result = self.cat.rbac_add_role_to_user(ORG, SUP, "fake_user", "fake_role")
        self.assertFalse(result)

    def test_remove_role_from_nonexistent_user(self):
        self.cat.rbac_init_user_meta(ORG, SUP)
        result = self.cat.rbac_remove_role_from_user(ORG, SUP, "fake_user", "fake_role")
        self.assertFalse(result)


# ═══════════════════════════════════════════════════════════════════════════ #
#  13. Version tracking tests                                                #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestVersionTracking(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def _get_role_meta_version(self):
        from supertable import redis_keys as RK
        raw = self.cat.r.hgetall(RK.rbac_role_meta(ORG, SUP))
        return int(raw.get("version", 0))

    def _get_user_meta_version(self):
        from supertable import redis_keys as RK
        raw = self.cat.r.hgetall(RK.rbac_user_meta(ORG, SUP))
        return int(raw.get("version", 0))

    def test_role_create_bumps_version(self):
        v0 = self._get_role_meta_version()
        self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        v1 = self._get_role_meta_version()
        self.assertGreater(v1, v0)

    def test_role_update_bumps_version(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        v0 = self._get_role_meta_version()
        self.rm.update_role(rid, {"tables": {"t1": {"columns": ["a"]}}})
        v1 = self._get_role_meta_version()
        self.assertGreater(v1, v0)

    def test_role_delete_bumps_version(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        v0 = self._get_role_meta_version()
        self.rm.delete_role(rid)
        v1 = self._get_role_meta_version()
        self.assertGreater(v1, v0)

    def test_user_create_bumps_version(self):
        v0 = self._get_user_meta_version()
        self.um.create_user({"username": "bump_test", "roles": []})
        v1 = self._get_user_meta_version()
        self.assertGreater(v1, v0)

    def test_user_delete_bumps_version(self):
        uid = self.um.create_user({"username": "del_bump", "roles": []})
        v0 = self._get_user_meta_version()
        self.um.delete_user(uid)
        v1 = self._get_user_meta_version()
        self.assertGreater(v1, v0)

    def test_add_role_to_user_bumps_user_version(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "v_test", "roles": []})
        v0 = self._get_user_meta_version()
        self.um.add_role(uid, rid)
        v1 = self._get_user_meta_version()
        self.assertGreater(v1, v0)

    def test_multiple_operations_monotonic_version(self):
        versions = [self._get_role_meta_version()]
        for i in range(5):
            self.rm.create_role({"role": "reader", "tables": {f"t{i}": {}}})
            versions.append(self._get_role_meta_version())
        for i in range(1, len(versions)):
            self.assertGreater(versions[i], versions[i - 1])


# ═══════════════════════════════════════════════════════════════════════════ #
#  14. Cross-organization isolation                                          #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestCrossOrgIsolation(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm_a = RoleManager(super_name="sup_a", organization="org_a", redis_catalog=self.cat)
        self.rm_b = RoleManager(super_name="sup_b", organization="org_b", redis_catalog=self.cat)
        self.um_a = UserManager(super_name="sup_a", organization="org_a", redis_catalog=self.cat)
        self.um_b = UserManager(super_name="sup_b", organization="org_b", redis_catalog=self.cat)

    def test_roles_isolated_between_orgs(self):
        r_a = self.rm_a.create_role({"role": "reader", "tables": {"org_a_table": {}}})
        r_b = self.rm_b.create_role({"role": "writer", "tables": {"org_b_table": {}}})

        roles_a = self.rm_a.list_roles()
        roles_b = self.rm_b.list_roles()
        role_ids_a = {r.get("role_id") for r in roles_a}
        role_ids_b = {r.get("role_id") for r in roles_b}

        self.assertIn(r_a, role_ids_a)
        self.assertNotIn(r_b, role_ids_a)
        self.assertIn(r_b, role_ids_b)
        self.assertNotIn(r_a, role_ids_b)

    def test_users_isolated_between_orgs(self):
        u_a = self.um_a.create_user({"username": "alice", "roles": []})
        u_b = self.um_b.create_user({"username": "alice", "roles": []})
        # Same username but different user_ids in different orgs
        self.assertNotEqual(u_a, u_b)

    def test_delete_role_in_org_a_does_not_affect_org_b(self):
        r_a = self.rm_a.create_role({"role": "reader", "tables": {"t1": {}}})
        r_b = self.rm_b.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm_a.delete_role(r_a)
        # org_b role unaffected
        self.assertIsNotNone(self.rm_b.get_role(r_b))


# ═══════════════════════════════════════════════════════════════════════════ #
#  15. RedisCatalog read methods (get_users, get_roles) direct               #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRedisCatalogReadMethods(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_get_roles_returns_list_with_deserialized_fields(self):
        self.rm.create_role({"role": "reader", "tables": {"t1": {"columns": ["a"]}, "t2": {"columns": ["a"]}}})
        roles = self.cat.get_roles(ORG, SUP)
        reader_roles = [r for r in roles if r.get("role") == "reader"]
        self.assertEqual(len(reader_roles), 1)
        self.assertIsInstance(reader_roles[0]["tables"], dict)
        self.assertIn("t1", reader_roles[0]["tables"])

    def test_get_users_returns_list_with_deserialized_roles(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.um.create_user({"username": "test", "roles": [rid]})
        users = self.cat.get_users(ORG, SUP)
        test_users = [u for u in users if u.get("username") == "test"]
        self.assertEqual(len(test_users), 1)
        self.assertIsInstance(test_users[0]["roles"], list)
        self.assertIn(rid, test_users[0]["roles"])

    def test_get_role_details_returns_none_for_missing(self):
        self.assertIsNone(self.cat.get_role_details(ORG, SUP, "missing"))

    def test_get_user_details_returns_none_for_missing(self):
        self.assertIsNone(self.cat.get_user_details(ORG, SUP, "missing"))


# ═══════════════════════════════════════════════════════════════════════════ #
#  16. Access control advanced scenarios                                     #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestAccessControlAdvanced(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def _patch_manager(self):
        return patch("supertable.rbac.access_control.RoleManager", return_value=self.rm)

    def test_admin_can_write_any_table(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "admin", "role_name": "adv_admin", "tables": {"*": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "adv_admin", "any_table")
            check_write_access(SUP, ORG, "adv_admin", "another_table")

    def test_writer_specific_tables_only(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "order_writer", "tables": {"sales": {}, "orders": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "order_writer", "sales")
            check_write_access(SUP, ORG, "order_writer", "orders")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "order_writer", "secrets")

    def test_access_after_role_update(self):
        """Update a role's tables — access should reflect new tables."""
        from supertable.rbac.access_control import check_write_access
        rid = self.rm.create_role({"role": "writer", "role_name": "evolving_role", "tables": {"old_table": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "evolving_role", "old_table")

        self.rm.update_role(rid, {"tables": {"new_table": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "evolving_role", "new_table")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "evolving_role", "old_table")

    def test_access_after_role_deleted(self):
        """Role deleted entirely — using its name gets denied."""
        from supertable.rbac.access_control import check_write_access
        rid = self.rm.create_role({"role": "writer", "role_name": "doomed_role", "tables": {"t1": {}}})
        self.rm.delete_role(rid)
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "doomed_role", "t1")

    def test_two_writer_roles_different_tables(self):
        """Two separate named writer roles — each covers only its own tables."""
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "w_t1", "tables": {"t1": {}}})
        self.rm.create_role({"role": "writer", "role_name": "w_t2", "tables": {"t2": {}}})
        with self._patch_manager():
            check_write_access(SUP, ORG, "w_t1", "t1")
            check_write_access(SUP, ORG, "w_t2", "t2")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "w_t1", "t2")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "w_t2", "t1")

    def test_reader_role_can_meta(self):
        """Reader role has META permission."""
        from supertable.rbac.access_control import check_meta_access
        self.rm.create_role({"role": "reader", "role_name": "adv_reader", "tables": {"t1": {}}})
        with self._patch_manager():
            check_meta_access(SUP, ORG, "adv_reader", "t1")

    def test_meta_role_cannot_write(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "meta", "role_name": "meta_only", "tables": {"*": {}}})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "meta_only", "any_table")


# ═══════════════════════════════════════════════════════════════════════════ #
#  17. get_or_create_default_user                                            #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestGetOrCreateDefaultUser(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_returns_existing_superuser(self):
        uid = self.um.get_or_create_default_user()
        uid2 = self.um.get_or_create_default_user()
        self.assertEqual(uid, uid2)

    def test_superuser_has_superadmin_role(self):
        uid = self.um.get_or_create_default_user()
        user = self.um.get_user(uid)
        sa_id = self.rm.get_superadmin_role_id()
        self.assertIn(sa_id, user["roles"])


# ═══════════════════════════════════════════════════════════════════════════ #
#  18. Role type update and data integrity                                   #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRoleTypeUpdate(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_update_role_tables(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.update_role(rid, {"tables": {"t1": {}, "t2": {}, "t3": {}}})
        role = self.rm.get_role(rid)
        self.assertEqual(set(role["tables"].keys()), {"t1", "t2", "t3"})

    def test_update_role_filters(self):
        rid = self.rm.create_role({
            "role": "reader", "tables": {"t1": {"filters": ["*"]}},
        })
        with self.assertRaisesRegex(ValueError, "invalid filters"):
            self.rm.update_role(
                rid,
                {"tables": {"t1": {"filters": {"col": "new_val"}}}},
            )
        role = self.rm.get_role(rid)
        self.assertEqual(role["tables"]["t1"]["filters"], ["*"])

    def test_update_preserves_role_id(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.update_role(rid, {"tables": {"t2": {}}})
        role = self.rm.get_role(rid)
        self.assertEqual(role["role_id"], rid)

    def test_update_preserves_role_type(self):
        rid = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        self.rm.update_role(rid, {"tables": {"t1": {"columns": ["x"]}}})
        role = self.rm.get_role(rid)
        self.assertEqual(role["role"], "reader")


# ═══════════════════════════════════════════════════════════════════════════ #
#  19. Key namespace correctness                                             #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestKeyNamespace(unittest.TestCase):

    def test_rbac_keys_use_rbac_prefix(self):
        from supertable import redis_keys as RK
        keys = [
            RK.rbac_user_meta("o", "s"),
            RK.rbac_user_index("o", "s"),
            RK.rbac_user_doc("o", "s", "uid"),
            RK.rbac_username_to_id("o", "s"),
            RK.rbac_role_meta("o", "s"),
            RK.rbac_role_index("o", "s"),
            RK.rbac_role_doc("o", "s", "rid"),
            RK.rbac_role_type_index("o", "s", "admin"),
        ]
        for k in keys:
            self.assertTrue(k.startswith("supertable:o:lakes:s:rbac:"), f"Bad key: {k}")

    def test_rbac_keys_do_not_collide(self):
        """All 8 key patterns for same org/sup produce distinct keys."""
        from supertable import redis_keys as RK
        keys = set()
        keys.add(RK.rbac_user_meta("o", "s"))
        keys.add(RK.rbac_user_index("o", "s"))
        keys.add(RK.rbac_user_doc("o", "s", "id1"))
        keys.add(RK.rbac_username_to_id("o", "s"))
        keys.add(RK.rbac_role_meta("o", "s"))
        keys.add(RK.rbac_role_index("o", "s"))
        keys.add(RK.rbac_role_doc("o", "s", "id1"))
        keys.add(RK.rbac_role_type_index("o", "s", "admin"))
        self.assertEqual(len(keys), 8)

    def test_user_doc_and_role_doc_different_namespace(self):
        """Same ID used as user_id and role_id should produce different keys."""
        from supertable import redis_keys as RK
        self.assertNotEqual(
            RK.rbac_user_doc("o", "s", "same_id"),
            RK.rbac_role_doc("o", "s", "same_id"),
        )


# ═══════════════════════════════════════════════════════════════════════════ #
#  20. Modify user — combined fields                                         #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestModifyUserCombined(unittest.TestCase):

    def setUp(self):
        self.cat = fresh_catalog()
        self.rm = RoleManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)
        self.um = UserManager(super_name=SUP, organization=ORG, redis_catalog=self.cat)

    def test_modify_username_and_roles_together(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        uid = self.um.create_user({"username": "before", "roles": [r1]})
        self.um.modify_user(uid, {"username": "after", "roles": [r2]})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "after")
        self.assertEqual(user["roles"], [r2])

    def test_combined_rename_failure_does_not_partially_commit(self):
        uid = self.um.create_user({"username": "atomic_before", "roles": []})
        self.cat._rbac_update_user = MagicMock(return_value=-3)

        with self.assertRaisesRegex(ValueError, "changed concurrently"):
            self.um.modify_user(uid, {
                "username": "atomic_after", "display_name": "Not committed",
            })

        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "atomic_before")
        self.assertNotIn("display_name", user)
        self.assertEqual(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, "atomic_before"), uid,
        )
        self.assertIsNone(
            self.cat.rbac_get_user_id_by_username(ORG, SUP, "atomic_after"),
        )

    def test_modify_empty_data_noop(self):
        uid = self.um.create_user({"username": "static", "roles": []})
        self.um.modify_user(uid, {})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "static")

    def test_modify_roles_to_empty(self):
        r = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        uid = self.um.create_user({"username": "clearing", "roles": [r]})
        self.um.modify_user(uid, {"roles": []})
        user = self.um.get_user(uid)
        self.assertEqual(user["roles"], [])

    def test_modify_roles_replace_all(self):
        r1 = self.rm.create_role({"role": "reader", "tables": {"t1": {}}})
        r2 = self.rm.create_role({"role": "writer", "tables": {"t2": {}}})
        r3 = self.rm.create_role({"role": "admin", "tables": {"*": {}}})
        uid = self.um.create_user({"username": "replacer", "roles": [r1, r2]})
        self.um.modify_user(uid, {"roles": [r3]})
        user = self.um.get_user(uid)
        self.assertEqual(user["roles"], [r3])


# ═══════════════════════════════════════════════════════════════════════════ #
#  21. RowColumnSecurity advanced                                            #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestRowColumnSecurityAdvanced(unittest.TestCase):

    def test_all_role_types_accepted(self):
        for rtype in ("superadmin", "admin", "writer", "reader", "meta"):
            rcs = RowColumnSecurity(role=rtype, tables={"t1": {}})
            rcs.prepare()
            self.assertEqual(rcs.role.value, rtype)

    def test_malformed_filter_shorthand_rejected(self):
        f = {"region": "US", "status": "active"}
        rcs = RowColumnSecurity(role="reader", tables={"t1": {"filters": f}})
        with self.assertRaisesRegex(ValueError, "invalid filters"):
            rcs.prepare()

    def test_create_content_hash_alias(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {}})
        rcs.prepare()
        old_hash = rcs.content_hash
        rcs.create_content_hash()  # re-compute
        self.assertEqual(rcs.content_hash, old_hash)

    def test_content_hash_is_32_hex(self):
        rcs = RowColumnSecurity(role="reader", tables={"t1": {}})
        rcs.prepare()
        self.assertEqual(len(rcs.content_hash), 32)
        int(rcs.content_hash, 16)  # should not raise

    def test_single_table_no_sort_needed(self):
        rcs = RowColumnSecurity(role="reader", tables={"only_one": {}})
        rcs.prepare()
        self.assertIn("only_one", rcs.tables)

    def test_wildcard_table(self):
        rcs = RowColumnSecurity(role="admin", tables={"*": {}})
        rcs.prepare()
        self.assertIn("*", rcs.tables)


# ═══════════════════════════════════════════════════════════════════════════ #

if __name__ == "__main__":
    unittest.main(verbosity=2)
