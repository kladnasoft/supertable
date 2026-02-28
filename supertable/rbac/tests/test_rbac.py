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
import unittest
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Minimal in-memory Redis fake
# ---------------------------------------------------------------------------


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

    def delete(self, *keys):
        for k in keys:
            self._cmds.append(("delete", k))

    def set(self, key, value, **kwargs):
        self._cmds.append(("set", key, value, kwargs))

    def get(self, key):
        self._cmds.append(("get", key))

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
            elif op == "delete":
                results.append(self._store.delete(cmd[1]))
            elif op == "set":
                results.append(self._store.set(cmd[1], cmd[2], **cmd[3]))
            elif op == "get":
                results.append(self._store.get(cmd[1]))
        self._cmds.clear()
        return results


class FakeScript:
    """Wraps a Lua source and executes a Python approximation."""

    def __init__(self, store: "FakeRedis", lua_src: str):
        self._store = store
        self._src = lua_src

    def __call__(self, keys=None, args=None):
        keys = keys or []
        args = args or []

        if "HINCRBY" in self._src and "last_updated_ms" in self._src and "cjson" not in self._src:
            key = keys[0]
            now = args[0]
            v = self._store.hincrby(key, "version", 1)
            self._store.hset(key, mapping={"last_updated_ms": now})
            return v

        elif "SMEMBERS" in self._src and "DEL" in self._src and "SREM" in self._src:
            role_doc_key, role_index_key, role_type_index_key = keys[0], keys[1], keys[2]
            role_meta_key, user_index_key = keys[3], keys[4]
            role_id, now_ms, org, sup = args[0], args[1], args[2], args[3]
            user_ids = self._store.smembers(user_index_key) or set()
            for uid in list(user_ids):
                ukey = f"supertable:{org}:{sup}:rbac:users:doc:{uid}"
                roles_json = self._store.hget(ukey, "roles")
                if roles_json:
                    roles = json.loads(roles_json)
                    if role_id in roles:
                        roles.remove(role_id)
                        self._store.hset(ukey, mapping={"roles": json.dumps(roles), "modified_ms": now_ms})
            self._store.delete(role_doc_key)
            self._store.srem(role_index_key, role_id)
            self._store.srem(role_type_index_key, role_id)
            self._store.hincrby(role_meta_key, "version", 1)
            self._store.hset(role_meta_key, mapping={"last_updated_ms": now_ms})
            return 1

        elif "roles[#roles + 1] = role_id" in self._src:
            user_doc_key, user_meta_key = keys[0], keys[1]
            role_id, now_ms = args[0], args[1]
            roles_json = self._store.hget(user_doc_key, "roles")
            if not roles_json:
                return 0
            roles = json.loads(roles_json)
            if role_id in roles:
                return 0
            roles.append(role_id)
            self._store.hset(user_doc_key, mapping={"roles": json.dumps(roles), "modified_ms": now_ms})
            self._store.hincrby(user_meta_key, "version", 1)
            self._store.hset(user_meta_key, mapping={"last_updated_ms": now_ms})
            return 1

        elif "new_roles[#new_roles + 1] = r" in self._src:
            user_doc_key, user_meta_key = keys[0], keys[1]
            role_id, now_ms = args[0], args[1]
            roles_json = self._store.hget(user_doc_key, "roles")
            if not roles_json:
                return 0
            roles = json.loads(roles_json)
            if role_id not in roles:
                return 0
            roles.remove(role_id)
            self._store.hset(user_doc_key, mapping={"roles": json.dumps(roles), "modified_ms": now_ms})
            self._store.hincrby(user_meta_key, "version", 1)
            self._store.hset(user_meta_key, mapping={"last_updated_ms": now_ms})
            return 1

        else:
            return 0


class FakeRedis:
    """Minimal in-memory Redis mock."""

    def __init__(self):
        self._data: Dict[str, Any] = {}

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
from supertable.redis_catalog import RedisCatalog
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
        rcs = RowColumnSecurity(role="reader", tables=["t1", "t2"], columns=["a", "b"])
        rcs.prepare()
        self.assertEqual(rcs.tables, ["t1", "t2"])
        self.assertEqual(rcs.columns, ["a", "b"])
        self.assertIsNotNone(rcs.content_hash)

    def test_empty_tables_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables=[])
        rcs.prepare()
        self.assertEqual(rcs.tables, ["*"])

    def test_empty_columns_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables=["t1"])
        rcs.prepare()
        self.assertEqual(rcs.columns, ["*"])

    def test_empty_filters_defaults_to_wildcard(self):
        rcs = RowColumnSecurity(role="admin", tables=["t1"])
        rcs.prepare()
        self.assertEqual(rcs.filters, ["*"])

    def test_tables_sorted_and_deduped(self):
        rcs = RowColumnSecurity(role="reader", tables=["z", "a", "a", "m"])
        rcs.prepare()
        self.assertEqual(rcs.tables, ["a", "m", "z"])

    def test_columns_sorted_and_deduped(self):
        rcs = RowColumnSecurity(role="reader", tables=["t1"], columns=["z", "a", "a"])
        rcs.prepare()
        self.assertEqual(rcs.columns, ["a", "z"])

    def test_wildcard_columns_not_sorted(self):
        rcs = RowColumnSecurity(role="reader", tables=["t1"], columns=["*"])
        rcs.prepare()
        self.assertEqual(rcs.columns, ["*"])

    def test_content_hash_deterministic(self):
        rcs1 = RowColumnSecurity(role="reader", tables=["b", "a"], columns=["x", "y"])
        rcs1.prepare()
        rcs2 = RowColumnSecurity(role="reader", tables=["a", "b"], columns=["y", "x"])
        rcs2.prepare()
        self.assertEqual(rcs1.content_hash, rcs2.content_hash)

    def test_different_content_different_hash(self):
        rcs1 = RowColumnSecurity(role="reader", tables=["t1"])
        rcs1.prepare()
        rcs2 = RowColumnSecurity(role="writer", tables=["t1"])
        rcs2.prepare()
        self.assertNotEqual(rcs1.content_hash, rcs2.content_hash)

    def test_hash_property_alias(self):
        rcs = RowColumnSecurity(role="admin", tables=["t1"])
        rcs.prepare()
        self.assertEqual(rcs.hash, rcs.content_hash)

    def test_to_json(self):
        rcs = RowColumnSecurity(role="reader", tables=["t1"], columns=["a"], filters={"x": 1})
        rcs.prepare()
        j = rcs.to_json()
        self.assertEqual(j["role"], "reader")
        self.assertEqual(j["tables"], ["t1"])
        self.assertEqual(j["columns"], ["a"])
        self.assertEqual(j["filters"], {"x": 1})

    def test_invalid_role_raises(self):
        with self.assertRaises(ValueError):
            RowColumnSecurity(role="nonexistent", tables=["t1"])


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
            "tables": ["t1"],
            "columns": ["a"],
            "filters": ["*"],
            "content_hash": "abc123",
        }
        self.cat.rbac_create_role(ORG, SUP, "r1", role_data)

        fetched = self.cat.get_role_details(ORG, SUP, "r1")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["role"], "reader")
        self.assertEqual(fetched["tables"], ["t1"])
        self.assertEqual(fetched["columns"], ["a"])

    def test_role_exists(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.assertFalse(self.cat.rbac_role_exists(ORG, SUP, "nope"))
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "admin", "role_id": "r1"})
        self.assertTrue(self.cat.rbac_role_exists(ORG, SUP, "r1"))

    def test_list_role_ids(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        self.cat.rbac_create_role(ORG, SUP, "r1", {"role": "admin", "role_id": "r1"})
        self.cat.rbac_create_role(ORG, SUP, "r2", {"role": "reader", "role_id": "r2"})
        ids = self.cat.rbac_list_role_ids(ORG, SUP)
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
            "role": "reader", "role_id": "r1", "tables": '["t1"]', "columns": '["a"]',
        })
        self.cat.rbac_update_role(ORG, SUP, "r1", {"columns": ["a", "b", "c"]})
        fetched = self.cat.get_role_details(ORG, SUP, "r1")
        self.assertEqual(fetched["columns"], ["a", "b", "c"])

    def test_update_nonexistent_role_raises(self):
        self.cat.rbac_init_role_meta(ORG, SUP)
        with self.assertRaises(ValueError):
            self.cat.rbac_update_role(ORG, SUP, "nope", {"columns": ["x"]})

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

    def test_add_role_to_user(self):
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
        self.assertEqual(role["tables"], ["*"])

    def test_create_role_returns_uuid(self):
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.assertEqual(len(role_id), 32)  # UUID hex

    def test_create_role_stored_correctly(self):
        role_id = self.rm.create_role({
            "role": "reader", "tables": ["t1"], "columns": ["a", "b"],
        })
        role = self.rm.get_role(role_id)
        self.assertEqual(role["role"], "reader")
        self.assertEqual(role["tables"], ["t1"])
        self.assertEqual(role["columns"], ["a", "b"])
        self.assertIn("content_hash", role)
        self.assertEqual(role["role_id"], role_id)

    def test_duplicate_content_creates_separate_roles(self):
        """No dedup — two roles with same content get different IDs."""
        id1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        id2 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.assertNotEqual(id1, id2)

    def test_update_role_in_place(self):
        role_id = self.rm.create_role({
            "role": "reader", "tables": ["t1"], "columns": ["a"],
        })
        old_hash = self.rm.get_role(role_id)["content_hash"]
        new_hash = self.rm.update_role(role_id, {"columns": ["a", "b", "c"]})
        self.assertNotEqual(old_hash, new_hash)

        role = self.rm.get_role(role_id)
        self.assertEqual(role["columns"], ["a", "b", "c"])
        self.assertEqual(role["role_id"], role_id)  # ID unchanged

    def test_update_role_partial(self):
        """Only supplied fields change."""
        role_id = self.rm.create_role({
            "role": "reader", "tables": ["t1", "t2"], "columns": ["a"],
        })
        self.rm.update_role(role_id, {"columns": ["x", "y"]})
        role = self.rm.get_role(role_id)
        self.assertEqual(role["tables"], ["t1", "t2"])  # Unchanged
        self.assertEqual(role["columns"], ["x", "y"])    # Changed

    def test_update_nonexistent_raises(self):
        with self.assertRaises(ValueError):
            self.rm.update_role("bogus", {"columns": ["x"]})

    def test_delete_role(self):
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.assertTrue(self.rm.delete_role(role_id))
        self.assertEqual(self.rm.get_role(role_id), {})

    def test_delete_nonexistent_returns_false(self):
        self.assertFalse(self.rm.delete_role("bogus"))

    def test_list_roles(self):
        self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.create_role({"role": "writer", "tables": ["t2"]})
        roles = self.rm.list_roles()
        # +1 for the auto-created superadmin
        self.assertEqual(len(roles), 3)

    def test_get_roles_by_type(self):
        self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.create_role({"role": "reader", "tables": ["t2"]})
        self.rm.create_role({"role": "writer", "tables": ["t3"]})
        readers = self.rm.get_roles_by_type("reader")
        writers = self.rm.get_roles_by_type("writer")
        self.assertEqual(len(readers), 2)
        self.assertEqual(len(writers), 1)

    def test_invalid_role_type_raises(self):
        with self.assertRaises(ValueError):
            self.rm.create_role({"role": "invalid_type", "tables": ["t1"]})


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
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
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
        role_id = self.rm.create_role({"role": "writer", "tables": ["t1"]})
        uid = self.um.create_user({"username": "ivan", "roles": []})
        self.assertTrue(self.um.add_role(uid, role_id))
        user = self.um.get_user(uid)
        self.assertIn(role_id, user["roles"])

    def test_add_role_idempotent(self):
        role_id = self.rm.create_role({"role": "writer", "tables": ["t1"]})
        uid = self.um.create_user({"username": "judy", "roles": [role_id]})
        self.assertFalse(self.um.add_role(uid, role_id))  # Already has it

    def test_add_nonexistent_role_raises(self):
        uid = self.um.create_user({"username": "kevin", "roles": []})
        with self.assertRaises(ValueError):
            self.um.add_role(uid, "fake_role")

    def test_remove_role_atomic(self):
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        uid = self.um.create_user({"username": "lisa", "roles": [role_id]})
        self.assertTrue(self.um.remove_role(uid, role_id))
        user = self.um.get_user(uid)
        self.assertNotIn(role_id, user["roles"])

    def test_remove_role_not_present_noop(self):
        uid = self.um.create_user({"username": "mike", "roles": []})
        self.assertFalse(self.um.remove_role(uid, "r999"))

    def test_user_with_multiple_roles(self):
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        r3 = self.rm.create_role({"role": "admin", "tables": ["*"]})
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
        self.rm.create_role({"role": "writer", "role_name": "sales_writer", "tables": ["sales"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "sales_writer", "sales")

    def test_writer_cannot_write_other_table(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "sales_writer2", "tables": ["sales"]})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "sales_writer2", "secrets")

    def test_reader_cannot_write(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "reader", "role_name": "all_reader", "tables": ["*"]})
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "all_reader", "any_table")

    def test_meta_role_can_meta(self):
        from supertable.rbac.access_control import check_meta_access
        self.rm.create_role({"role": "meta", "role_name": "stats_meta", "tables": ["stats"]})
        with self._patch_manager():
            check_meta_access(SUP, ORG, "stats_meta", "stats")

    def test_meta_role_cannot_meta_other_table(self):
        from supertable.rbac.access_control import check_meta_access
        self.rm.create_role({"role": "meta", "role_name": "stats_meta2", "tables": ["stats"]})
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
        self.rm.create_role({"role": "admin", "role_name": "full_admin", "tables": ["*"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "full_admin", "anything")
            check_write_access(SUP, ORG, "full_admin", "some_other_table")

    def test_writer_role_covers_only_its_tables(self):
        """Must USE the correct role — writer for t2 cannot write t1."""
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "reader", "role_name": "t1_reader", "tables": ["t1"]})
        self.rm.create_role({"role": "writer", "role_name": "t2_writer", "tables": ["t2"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "t2_writer", "t2")  # OK
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "t1_reader", "t1")  # Reader can't write

    def test_role_name_case_insensitive(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "CamelWriter", "tables": ["t1"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "camelwriter", "t1")
            check_write_access(SUP, ORG, "CAMELWRITER", "t1")


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
            "role": "reader", "tables": ["t1"], "columns": ["a"],
        })
        uid = self.um.create_user({"username": "alice", "roles": [role_id]})

        # Before update
        user = self.um.get_user(uid)
        role = self.rm.get_role(user["roles"][0])
        self.assertEqual(role["columns"], ["a"])

        # Update the role
        self.rm.update_role(role_id, {"columns": ["a", "b", "c"]})

        # Same user, same role_id — new content
        role_after = self.rm.get_role(user["roles"][0])
        self.assertEqual(role_after["columns"], ["a", "b", "c"])

    def test_delete_role_with_many_users(self):
        """Role deleted — stripped from all 10 users atomically."""
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        user_ids = []
        for i in range(10):
            uid = self.um.create_user({"username": f"user_{i}", "roles": [role_id]})
            user_ids.append(uid)

        self.rm.delete_role(role_id)

        for uid in user_ids:
            user = self.um.get_user(uid)
            self.assertNotIn(role_id, user["roles"])

    def test_user_retains_other_roles_after_one_deleted(self):
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        uid = self.um.create_user({"username": "multi", "roles": [r1, r2]})

        self.rm.delete_role(r1)

        user = self.um.get_user(uid)
        self.assertNotIn(r1, user["roles"])
        self.assertIn(r2, user["roles"])

    def test_create_many_roles_different_types(self):
        ids = {}
        for rtype in ("admin", "writer", "reader", "meta"):
            ids[rtype] = self.rm.create_role({"role": rtype, "tables": ["*"]})

        for rtype, rid in ids.items():
            role = self.rm.get_role(rid)
            self.assertEqual(role["role"], rtype)

    def test_shared_catalog_between_managers(self):
        """RoleManager and UserManager sharing same catalog see each other's data."""
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        # UserManager should be able to validate this role
        uid = self.um.create_user({"username": "shared_test", "roles": [role_id]})
        user = self.um.get_user(uid)
        self.assertIn(role_id, user["roles"])

    def test_role_id_stable_after_multiple_updates(self):
        role_id = self.rm.create_role({"role": "reader", "tables": ["t1"], "columns": ["a"]})
        for i in range(5):
            self.rm.update_role(role_id, {"columns": [f"col_{i}"]})
        role = self.rm.get_role(role_id)
        self.assertEqual(role["role_id"], role_id)
        self.assertEqual(role["columns"], ["col_4"])

    def test_user_id_stable_after_modifications(self):
        uid = self.um.create_user({"username": "stable", "roles": []})
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.um.add_role(uid, r1)
        self.um.modify_user(uid, {"username": "stable_renamed"})
        user = self.um.get_user(uid)
        self.assertEqual(user["user_id"], uid)

    def test_restrict_read_access_disabled(self):
        """restrict_read_access returns immediately (disabled)."""
        from supertable.rbac.access_control import restrict_read_access
        # Should not raise
        result = restrict_read_access(SUP, ORG, "any_role_name", [])
        self.assertIsNone(result)


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
            rid = self.rm.create_role({"role": "reader", "tables": [f"t{i}"]})
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
        role_ids = [self.rm.create_role({"role": "reader", "tables": [f"t{i}"]}) for i in range(20)]
        uid = self.um.create_user({"username": "multi_role", "roles": role_ids})
        user = self.um.get_user(uid)
        self.assertEqual(len(user["roles"]), 20)

    def test_one_role_assigned_to_many_users(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["shared"]})
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
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        r3 = self.rm.create_role({"role": "admin", "tables": ["*"]})
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
        ids = [self.rm.create_role({"role": "reader", "tables": [f"t{i}"]}) for i in range(10)]
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
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.assertIn(rid, self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"))
        self.rm.delete_role(rid)
        self.assertNotIn(rid, self.cat.rbac_get_role_ids_by_type(ORG, SUP, "reader"))

    def test_delete_role_removes_from_global_index(self):
        rid = self.rm.create_role({"role": "writer", "tables": ["t1"]})
        self.assertIn(rid, self.cat.rbac_list_role_ids(ORG, SUP))
        self.rm.delete_role(rid)
        self.assertNotIn(rid, self.cat.rbac_list_role_ids(ORG, SUP))

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
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        r3 = self.rm.create_role({"role": "admin", "tables": ["t3"]})
        uid = self.um.create_user({"username": "doomed", "roles": [r1, r2, r3]})

        self.rm.delete_role(r1)
        self.rm.delete_role(r2)
        self.rm.delete_role(r3)

        user = self.um.get_user(uid)
        self.assertEqual(user["roles"], [])

    def test_role_update_does_not_affect_other_roles(self):
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"], "columns": ["a"]})
        r2 = self.rm.create_role({"role": "reader", "tables": ["t2"], "columns": ["b"]})
        self.rm.update_role(r1, {"columns": ["x", "y", "z"]})
        role2 = self.rm.get_role(r2)
        self.assertEqual(role2["columns"], ["b"])

    def test_role_update_changes_content_hash(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"], "columns": ["a"]})
        hash_before = self.rm.get_role(rid)["content_hash"]
        self.rm.update_role(rid, {"columns": ["a", "b"]})
        hash_after = self.rm.get_role(rid)["content_hash"]
        self.assertNotEqual(hash_before, hash_after)

    def test_delete_role_then_recreate_same_content(self):
        """After deleting a role, creating one with same content gets a new ID."""
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.delete_role(r1)
        r2 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.assertNotEqual(r1, r2)
        self.assertIsNotNone(self.rm.get_role(r2))

    def test_user_references_to_deleted_role_are_cleaned(self):
        """After role deletion, user's role list no longer contains it, even via list_users."""
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
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

    def test_username_with_spaces(self):
        uid = self.um.create_user({"username": "john doe", "roles": []})
        user = self.um.get_user_by_name("john doe")
        self.assertEqual(user["user_id"], uid)

    def test_username_with_special_chars(self):
        uid = self.um.create_user({"username": "user@domain.com", "roles": []})
        user = self.um.get_user_by_name("user@domain.com")
        self.assertEqual(user["user_id"], uid)

    def test_username_unicode(self):
        uid = self.um.create_user({"username": "用户名", "roles": []})
        user = self.um.get_user_by_name("用户名")
        self.assertEqual(user["user_id"], uid)

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
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
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
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        u1 = self.um.create_user({"username": "u1", "roles": [rid]})
        u2 = self.um.create_user({"username": "u2", "roles": [rid]})
        self.um.remove_role_from_users(rid)
        self.assertNotIn(rid, self.um.get_user(u1)["roles"])
        self.assertNotIn(rid, self.um.get_user(u2)["roles"])

    def test_deprecated_key_helpers_delegate(self):
        """Old key helpers should return the same keys as new _rbac_ helpers."""
        from supertable.redis_catalog import (
            _users_key, _roles_key, _user_hash_key, _role_hash_key,
            _user_name_to_hash_key, _role_type_to_hash_key,
            _rbac_user_meta_key, _rbac_role_meta_key, _rbac_user_doc_key,
            _rbac_role_doc_key, _rbac_username_to_id_key, _rbac_role_type_index_key,
        )
        self.assertEqual(_users_key("o", "s"), _rbac_user_meta_key("o", "s"))
        self.assertEqual(_roles_key("o", "s"), _rbac_role_meta_key("o", "s"))
        self.assertEqual(_user_hash_key("o", "s", "h"), _rbac_user_doc_key("o", "s", "h"))
        self.assertEqual(_role_hash_key("o", "s", "h"), _rbac_role_doc_key("o", "s", "h"))
        self.assertEqual(_user_name_to_hash_key("o", "s"), _rbac_username_to_id_key("o", "s"))
        self.assertEqual(_role_type_to_hash_key("o", "s", "admin"), _rbac_role_type_index_key("o", "s", "admin"))


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
        self.assertEqual(self.cat.rbac_list_role_ids(ORG, SUP), [])

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
        raw = self.cat.r.hgetall(f"supertable:{ORG}:{SUP}:rbac:roles:meta")
        return int(raw.get("version", 0))

    def _get_user_meta_version(self):
        raw = self.cat.r.hgetall(f"supertable:{ORG}:{SUP}:rbac:users:meta")
        return int(raw.get("version", 0))

    def test_role_create_bumps_version(self):
        v0 = self._get_role_meta_version()
        self.rm.create_role({"role": "reader", "tables": ["t1"]})
        v1 = self._get_role_meta_version()
        self.assertGreater(v1, v0)

    def test_role_update_bumps_version(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        v0 = self._get_role_meta_version()
        self.rm.update_role(rid, {"columns": ["a"]})
        v1 = self._get_role_meta_version()
        self.assertGreater(v1, v0)

    def test_role_delete_bumps_version(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
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
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        uid = self.um.create_user({"username": "v_test", "roles": []})
        v0 = self._get_user_meta_version()
        self.um.add_role(uid, rid)
        v1 = self._get_user_meta_version()
        self.assertGreater(v1, v0)

    def test_multiple_operations_monotonic_version(self):
        versions = [self._get_role_meta_version()]
        for i in range(5):
            self.rm.create_role({"role": "reader", "tables": [f"t{i}"]})
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
        r_a = self.rm_a.create_role({"role": "reader", "tables": ["org_a_table"]})
        r_b = self.rm_b.create_role({"role": "writer", "tables": ["org_b_table"]})

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
        r_a = self.rm_a.create_role({"role": "reader", "tables": ["t1"]})
        r_b = self.rm_b.create_role({"role": "reader", "tables": ["t1"]})
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
        self.rm.create_role({"role": "reader", "tables": ["t1", "t2"], "columns": ["a"]})
        roles = self.cat.get_roles(ORG, SUP)
        reader_roles = [r for r in roles if r.get("role") == "reader"]
        self.assertEqual(len(reader_roles), 1)
        self.assertIsInstance(reader_roles[0]["tables"], list)
        self.assertIsInstance(reader_roles[0]["columns"], list)

    def test_get_users_returns_list_with_deserialized_roles(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
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
        self.rm.create_role({"role": "admin", "role_name": "adv_admin", "tables": ["*"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "adv_admin", "any_table")
            check_write_access(SUP, ORG, "adv_admin", "another_table")

    def test_writer_specific_tables_only(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "order_writer", "tables": ["sales", "orders"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "order_writer", "sales")
            check_write_access(SUP, ORG, "order_writer", "orders")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "order_writer", "secrets")

    def test_access_after_role_update(self):
        """Update a role's tables — access should reflect new tables."""
        from supertable.rbac.access_control import check_write_access
        rid = self.rm.create_role({"role": "writer", "role_name": "evolving_role", "tables": ["old_table"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "evolving_role", "old_table")

        self.rm.update_role(rid, {"tables": ["new_table"]})
        with self._patch_manager():
            check_write_access(SUP, ORG, "evolving_role", "new_table")
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "evolving_role", "old_table")

    def test_access_after_role_deleted(self):
        """Role deleted entirely — using its name gets denied."""
        from supertable.rbac.access_control import check_write_access
        rid = self.rm.create_role({"role": "writer", "role_name": "doomed_role", "tables": ["t1"]})
        self.rm.delete_role(rid)
        with self._patch_manager():
            with self.assertRaises(PermissionError):
                check_write_access(SUP, ORG, "doomed_role", "t1")

    def test_two_writer_roles_different_tables(self):
        """Two separate named writer roles — each covers only its own tables."""
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "writer", "role_name": "w_t1", "tables": ["t1"]})
        self.rm.create_role({"role": "writer", "role_name": "w_t2", "tables": ["t2"]})
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
        self.rm.create_role({"role": "reader", "role_name": "adv_reader", "tables": ["t1"]})
        with self._patch_manager():
            check_meta_access(SUP, ORG, "adv_reader", "t1")

    def test_meta_role_cannot_write(self):
        from supertable.rbac.access_control import check_write_access
        self.rm.create_role({"role": "meta", "role_name": "meta_only", "tables": ["*"]})
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
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.update_role(rid, {"tables": ["t1", "t2", "t3"]})
        role = self.rm.get_role(rid)
        self.assertEqual(role["tables"], ["t1", "t2", "t3"])

    def test_update_role_filters(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"], "filters": {"col": "val"}})
        self.rm.update_role(rid, {"filters": {"col": "new_val"}})
        role = self.rm.get_role(rid)
        self.assertEqual(role["filters"], {"col": "new_val"})

    def test_update_preserves_role_id(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.update_role(rid, {"tables": ["t2"]})
        role = self.rm.get_role(rid)
        self.assertEqual(role["role_id"], rid)

    def test_update_preserves_role_type(self):
        rid = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        self.rm.update_role(rid, {"columns": ["x"]})
        role = self.rm.get_role(rid)
        self.assertEqual(role["role"], "reader")


# ═══════════════════════════════════════════════════════════════════════════ #
#  19. Key namespace correctness                                             #
# ═══════════════════════════════════════════════════════════════════════════ #

class TestKeyNamespace(unittest.TestCase):

    def test_rbac_keys_use_rbac_prefix(self):
        from supertable.redis_catalog import (
            _rbac_user_meta_key, _rbac_user_index_key, _rbac_user_doc_key,
            _rbac_username_to_id_key, _rbac_role_meta_key, _rbac_role_index_key,
            _rbac_role_doc_key, _rbac_role_type_index_key,
        )
        keys = [
            _rbac_user_meta_key("o", "s"),
            _rbac_user_index_key("o", "s"),
            _rbac_user_doc_key("o", "s", "uid"),
            _rbac_username_to_id_key("o", "s"),
            _rbac_role_meta_key("o", "s"),
            _rbac_role_index_key("o", "s"),
            _rbac_role_doc_key("o", "s", "rid"),
            _rbac_role_type_index_key("o", "s", "admin"),
        ]
        for k in keys:
            self.assertTrue(k.startswith("supertable:o:s:rbac:"), f"Bad key: {k}")

    def test_rbac_keys_do_not_collide(self):
        """All 8 key patterns for same org/sup produce distinct keys."""
        from supertable.redis_catalog import (
            _rbac_user_meta_key, _rbac_user_index_key, _rbac_user_doc_key,
            _rbac_username_to_id_key, _rbac_role_meta_key, _rbac_role_index_key,
            _rbac_role_doc_key, _rbac_role_type_index_key,
        )
        keys = set()
        keys.add(_rbac_user_meta_key("o", "s"))
        keys.add(_rbac_user_index_key("o", "s"))
        keys.add(_rbac_user_doc_key("o", "s", "id1"))
        keys.add(_rbac_username_to_id_key("o", "s"))
        keys.add(_rbac_role_meta_key("o", "s"))
        keys.add(_rbac_role_index_key("o", "s"))
        keys.add(_rbac_role_doc_key("o", "s", "id1"))
        keys.add(_rbac_role_type_index_key("o", "s", "admin"))
        self.assertEqual(len(keys), 8)

    def test_user_doc_and_role_doc_different_namespace(self):
        """Same ID used as user_id and role_id should produce different keys."""
        from supertable.redis_catalog import _rbac_user_doc_key, _rbac_role_doc_key
        self.assertNotEqual(
            _rbac_user_doc_key("o", "s", "same_id"),
            _rbac_role_doc_key("o", "s", "same_id"),
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
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        uid = self.um.create_user({"username": "before", "roles": [r1]})
        self.um.modify_user(uid, {"username": "after", "roles": [r2]})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "after")
        self.assertEqual(user["roles"], [r2])

    def test_modify_empty_data_noop(self):
        uid = self.um.create_user({"username": "static", "roles": []})
        self.um.modify_user(uid, {})
        user = self.um.get_user(uid)
        self.assertEqual(user["username"], "static")

    def test_modify_roles_to_empty(self):
        r = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        uid = self.um.create_user({"username": "clearing", "roles": [r]})
        self.um.modify_user(uid, {"roles": []})
        user = self.um.get_user(uid)
        self.assertEqual(user["roles"], [])

    def test_modify_roles_replace_all(self):
        r1 = self.rm.create_role({"role": "reader", "tables": ["t1"]})
        r2 = self.rm.create_role({"role": "writer", "tables": ["t2"]})
        r3 = self.rm.create_role({"role": "admin", "tables": ["*"]})
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
            rcs = RowColumnSecurity(role=rtype, tables=["t1"])
            rcs.prepare()
            self.assertEqual(rcs.role.value, rtype)

    def test_filters_preserved(self):
        f = {"region": "US", "status": "active"}
        rcs = RowColumnSecurity(role="reader", tables=["t1"], filters=f)
        rcs.prepare()
        self.assertEqual(rcs.filters, f)

    def test_create_content_hash_alias(self):
        rcs = RowColumnSecurity(role="reader", tables=["t1"])
        rcs.prepare()
        old_hash = rcs.content_hash
        rcs.create_content_hash()  # re-compute
        self.assertEqual(rcs.content_hash, old_hash)

    def test_content_hash_is_32_hex(self):
        rcs = RowColumnSecurity(role="reader", tables=["t1"])
        rcs.prepare()
        self.assertEqual(len(rcs.content_hash), 32)
        int(rcs.content_hash, 16)  # should not raise

    def test_single_table_no_sort_needed(self):
        rcs = RowColumnSecurity(role="reader", tables=["only_one"])
        rcs.prepare()
        self.assertEqual(rcs.tables, ["only_one"])

    def test_wildcard_table(self):
        rcs = RowColumnSecurity(role="admin", tables=["*"])
        rcs.prepare()
        self.assertEqual(rcs.tables, ["*"])


# ═══════════════════════════════════════════════════════════════════════════ #

if __name__ == "__main__":
    unittest.main(verbosity=2)