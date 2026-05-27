# route: supertable.redis_catalog
from __future__ import annotations

import json
import time
import hashlib
import secrets
from typing import Any, Dict, Iterator, List, Optional

import redis
from supertable.config.defaults import logger

try:
    from .redis_connector import RedisConnector, RedisOptions
except ImportError:  # pragma: no cover
    from redis_connector import RedisConnector, RedisOptions

from supertable.locking.redis_lock import RedisLocking
from supertable import redis_keys as RK


def _now_ms() -> int:
    return int(time.time() * 1000)


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

    # ------------- Lua sources -------------
    _LUA_LEAF_CAS_SET = """
local key = KEYS[1]
local new_path = ARGV[1]
local now_ms = tonumber(ARGV[2])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1
local new_val = cjson.encode({version=new_version, ts=now_ms, path=new_path})
redis.call('SET', key, new_val)
return new_version
"""

    _LUA_LEAF_PAYLOAD_CAS_SET = """
local key = KEYS[1]
local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1

local payload = {}
local okp, pobj = pcall(cjson.decode, payload_json)
if okp and pobj then
  payload = pobj
end

local new_val = cjson.encode({version=new_version, ts=now_ms, path=new_path, payload=payload})
redis.call('SET', key, new_val)
return new_version
"""

    _LUA_ROOT_BUMP = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1
local new_val = cjson.encode({version=new_version, ts=now_ms})
redis.call('SET', key, new_val)
return new_version
"""

    # ------------- RBAC Lua scripts ------------- #

    _LUA_RBAC_BUMP_META = """
local key = KEYS[1]
local now = ARGV[1]
local v = redis.call('HINCRBY', key, 'version', 1)
redis.call('HSET', key, 'last_updated_ms', now)
return v
"""

    # ARGV layout:
    #   ARGV[1] role_id
    #   ARGV[2] now_ms (string)
    #   ARGV[3] role_name_lower (or "")
    #   ARGV[4] user_doc_key_prefix  (e.g. "supertable:{org}:lakes:{sup}:rbac:users:doc:")
    # KEYS layout:
    #   KEYS[1] role_doc_key
    #   KEYS[2] role_index_key
    #   KEYS[3] role_type_index_key
    #   KEYS[4] role_meta_key
    #   KEYS[5] user_index_key
    #   KEYS[6] rolename_to_id_key
    _LUA_RBAC_DELETE_ROLE = """
local role_id              = ARGV[1]
local now_ms               = ARGV[2]
local role_name_lower      = ARGV[3]
local user_doc_key_prefix  = ARGV[4]

local user_ids = redis.call('SMEMBERS', KEYS[5])
for _, uid in ipairs(user_ids) do
    local ukey = user_doc_key_prefix .. uid
    local roles_json = redis.call('HGET', ukey, 'roles')
    if roles_json then
        local ok, roles = pcall(cjson.decode, roles_json)
        if ok and type(roles) == 'table' then
            local new_roles = {}
            local changed = false
            for _, r in ipairs(roles) do
                if r == role_id then
                    changed = true
                else
                    new_roles[#new_roles + 1] = r
                end
            end
            if changed then
                redis.call('HSET', ukey, 'roles', cjson.encode(new_roles))
                redis.call('HSET', ukey, 'modified_ms', now_ms)
            end
        end
    end
end

-- Clean up role_name → role_id mapping atomically
if role_name_lower and role_name_lower ~= '' then
    redis.call('HDEL', KEYS[6], role_name_lower)
end

redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], role_id)
redis.call('SREM', KEYS[3], role_id)
redis.call('HINCRBY', KEYS[4], 'version', 1)
redis.call('HSET', KEYS[4], 'last_updated_ms', now_ms)
return 1
"""

    _LUA_RBAC_REMOVE_ROLE_FROM_USER = """
local role_id = ARGV[1]
local now_ms  = ARGV[2]
local roles_json = redis.call('HGET', KEYS[1], 'roles')
if not roles_json then return 0 end

local ok, roles = pcall(cjson.decode, roles_json)
if not ok or type(roles) ~= 'table' then return 0 end

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

redis.call('HSET', KEYS[1], 'roles', cjson.encode(new_roles))
redis.call('HSET', KEYS[1], 'modified_ms', now_ms)
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('HSET', KEYS[2], 'last_updated_ms', now_ms)
return 1
"""

    _LUA_RBAC_ADD_ROLE_TO_USER = """
local role_id = ARGV[1]
local now_ms  = ARGV[2]
local roles_json = redis.call('HGET', KEYS[1], 'roles')
if not roles_json then return 0 end

local ok, roles = pcall(cjson.decode, roles_json)
if not ok or type(roles) ~= 'table' then return 0 end

for _, r in ipairs(roles) do
    if r == role_id then return 0 end
end

roles[#roles + 1] = role_id
redis.call('HSET', KEYS[1], 'roles', cjson.encode(roles))
redis.call('HSET', KEYS[1], 'modified_ms', now_ms)
redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('HSET', KEYS[2], 'last_updated_ms', now_ms)
return 1
"""

    def __init__(self, options: Optional[RedisOptions] = None):
        self.r = RedisConnector(options).r

        # Register scripts
        self._leaf_cas_set = self.r.register_script(self._LUA_LEAF_CAS_SET)
        self._leaf_payload_cas_set = self.r.register_script(self._LUA_LEAF_PAYLOAD_CAS_SET)
        self._root_bump = self.r.register_script(self._LUA_ROOT_BUMP)

        # Distributed locking (delegates to supertable.locking.redis_lock)
        self._locker = RedisLocking(self.r)

        # RBAC Lua scripts
        self._rbac_bump_meta = self.r.register_script(self._LUA_RBAC_BUMP_META)
        self._rbac_delete_role = self.r.register_script(self._LUA_RBAC_DELETE_ROLE)
        self._rbac_remove_role_from_user = self.r.register_script(self._LUA_RBAC_REMOVE_ROLE_FROM_USER)
        self._rbac_add_role_to_user = self.r.register_script(self._LUA_RBAC_ADD_ROLE_TO_USER)

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





    def ensure_root(self, org: str, sup: str) -> None:
        """Initialize meta:root if missing with version=0."""
        key = RK.meta_root(org, sup)
        try:
            if not self.r.exists(key):
                init = {"version": 0, "ts": _now_ms()}
                self.r.set(key, json.dumps(init))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] ensure_root failed: {e}")
            raise

    def root_exists(self, org: str, sup: str) -> bool:
        """Check existence of meta:root key."""
        try:
            return bool(self.r.exists(RK.meta_root(org, sup)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_exists error: {e}")
            return False

    def leaf_exists(self, org: str, sup: str, simple: str) -> bool:
        """Check existence of meta:leaf key for a simple table (replica-aware)."""
        info = self._resolve_replica_info(org, sup)
        if info:
            source, allowed = info
            if allowed and simple not in allowed:
                return False
            return self._leaf_exists_raw(org, source, simple)
        return self._leaf_exists_raw(org, sup, simple)

    def _leaf_exists_raw(self, org: str, sup: str, simple: str) -> bool:
        try:
            return bool(self.r.exists(RK.meta_leaf(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_exists error: {e}")
            return False

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        try:
            raw = self.r.get(RK.meta_root(org, sup))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_root error: {e}")
            return None

    def update_root_flags(self, org: str, sup: str, flags: Dict[str, Any]) -> bool:
        """Merge *flags* into the existing meta:root JSON document.

        Used to set read_only, cloned_from, clone_type, clone_ts without
        disturbing version/ts.  Creates the root if it does not exist.
        """
        key = RK.meta_root(org, sup)
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {"version": 0, "ts": _now_ms()}
            doc.update(flags)
            self.r.set(key, json.dumps(doc))
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] update_root_flags error: {e}")
            return False

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
        try:
            return int(self._root_bump(keys=[RK.meta_root(org, sup)], args=[int(now_ms or _now_ms())]) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_bump error: {e}")
            raise

    # ------------- Replica resolution ------------------------------------

    def _resolve_replica_info(self, org: str, sup: str) -> Optional[tuple]:
        """If *sup* is a replica clone, return (source_name, allowed_tables).

        Returns None for non-replicas.  Never follows chains (if source
        is itself a replica, we stop — one level only).
        Single get_root() call — avoids redundant Redis reads.
        """
        try:
            root = self.get_root(org, sup)
            if root and root.get("clone_type") == "replica":
                source = root.get("cloned_from", "")
                if source and source != sup:
                    tables = root.get("replica_tables")
                    allowed = tables if isinstance(tables, list) and tables else None
                    return (source, allowed)
        except Exception:
            pass
        return None

    # Keep backward-compatible wrappers for gc.py and other callers
    def _resolve_replica_source(self, org: str, sup: str) -> Optional[str]:
        info = self._resolve_replica_info(org, sup)
        return info[0] if info else None

    # ------------- Leaf access (with replica resolution) ------------------

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        info = self._resolve_replica_info(org, sup)
        if info:
            source, allowed = info
            if allowed and simple not in allowed:
                return None
            return self._get_leaf_raw(org, source, simple)
        return self._get_leaf_raw(org, sup, simple)

    def _get_leaf_raw(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        try:
            raw = self.r.get(RK.meta_leaf(org, sup, simple))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_leaf error: {e}")
            return None

    def delete_leaf(self, org: str, sup: str, simple: str) -> bool:
        """Delete a leaf pointer (used when unlinking shared tables)."""
        try:
            return bool(self.r.delete(RK.meta_leaf(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_leaf error: {e}")
            return False

    def set_leaf_path_cas(self, org: str, sup: str, simple: str, path: str, now_ms: Optional[int] = None) -> int:
        try:
            return int(
                self._leaf_cas_set(keys=[RK.meta_leaf(org, sup, simple)], args=[path, int(now_ms or _now_ms())]) or 0
            )
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
    ) -> int:
        """Atomically write a leaf pointer *and* snapshot payload (so readers avoid storage reads)."""
        try:
            payload_json = json.dumps(payload or {})
        except Exception:
            payload_json = "{}"

        try:
            return int(
                self._leaf_payload_cas_set(
                    keys=[RK.meta_leaf(org, sup, simple)],
                    args=[payload_json, path, int(now_ms or _now_ms())],
                )
                or 0
            )
        except AttributeError:
            # Backward compatible fallback if script isn't registered for some reason.
            return self.set_leaf_path_cas(org, sup, simple, path, now_ms=now_ms)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_payload_cas_set error: {e}")
            raise

    # ------------- Mirror formats (Redis-backed) -------------

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        """Read enabled mirror formats from Redis key."""
        try:
            raw = self.r.get(RK.meta_mirrors(org, sup))
            if not raw:
                return []
            obj = json.loads(raw)
            formats = obj.get("formats", [])
            seen = set()
            out: List[str] = []
            for f in (formats or []):
                fu = str(f).upper()
                if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in seen:
                    seen.add(fu)
                    out.append(fu)
            return out
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_mirrors error: {e}")
            return []
        except Exception:
            return []

    def set_mirrors(self, org: str, sup: str, formats: List[str], now_ms: Optional[int] = None) -> List[str]:
        """Atomically set enabled mirror formats."""
        seen = set()
        ordered: List[str] = []
        for f in formats or []:
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in seen:
                seen.add(fu)
                ordered.append(fu)
        try:
            payload = {"formats": ordered, "ts": int(now_ms or _now_ms())}
            self.r.set(RK.meta_mirrors(org, sup), json.dumps(payload))
            return ordered
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_mirrors error: {e}")
            raise

    def enable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        if fu not in ("DELTA", "ICEBERG", "PARQUET"):
            return cur
        if fu in cur:
            return cur
        return self.set_mirrors(org, sup, cur + [fu])

    def disable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        nxt = [x for x in cur if x != fu]
        if nxt == cur:
            return cur
        return self.set_mirrors(org, sup, nxt)

    # ------------- User and Role Management (RBAC, UUID-based) ------------- #

    @staticmethod
    def _decode_member(m) -> str:
        return m if isinstance(m, str) else m.decode("utf-8")

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
            for uid, raw in zip(uids, results):
                if raw:
                    data: Dict[str, Any] = dict(raw)
                    data.setdefault("user_id", uid)
                    if "roles" in data:
                        try:
                            data["roles"] = json.loads(data["roles"])
                        except (json.JSONDecodeError, TypeError):
                            data["roles"] = []
                    users.append(data)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_users error: {e}")
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
            for rid, raw in zip(rids, results):
                if raw:
                    data: Dict[str, Any] = dict(raw)
                    data.setdefault("role_id", rid)
                    for field in ("tables", "columns", "filters"):
                        if field in data:
                            try:
                                data[field] = json.loads(data[field])
                            except (json.JSONDecodeError, TypeError):
                                pass
                    roles.append(data)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_roles error: {e}")
        return roles

    def get_role_details(self, org: str, sup: str, role_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed role information by role_id."""
        try:
            raw = self.r.hgetall(RK.rbac_role_doc(org, sup, role_id))
            if not raw:
                return None
            data: Dict[str, Any] = dict(raw)
            data.setdefault("role_id", role_id)
            for field in ("tables", "columns", "filters"):
                if field in data:
                    try:
                        data[field] = json.loads(data[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            return data
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_role_details error: {e}")
        return None

    def get_user_details(self, org: str, sup: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed user information by user_id."""
        try:
            raw = self.r.hgetall(RK.rbac_user_doc(org, sup, user_id))
            if not raw:
                return None
            data: Dict[str, Any] = dict(raw)
            data.setdefault("user_id", user_id)
            if "roles" in data:
                try:
                    data["roles"] = json.loads(data["roles"])
                except (json.JSONDecodeError, TypeError):
                    data["roles"] = []
            return data
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_user_details error: {e}")
        return None

    # ------------- RBAC write operations ------------- #

    @staticmethod
    def _rbac_serialize(value: Any) -> str:
        """Convert a Python value to a Redis-safe string for RBAC storage."""
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    def _rbac_bump(self, meta_key: str) -> int:
        """Atomically increment version and update timestamp on an RBAC meta key."""
        return int(self._rbac_bump_meta(keys=[meta_key], args=[str(_now_ms())]) or 0)

    # -- Role meta init --

    def rbac_init_role_meta(self, org: str, sup: str) -> None:
        """Ensure the RBAC role meta HASH exists. Idempotent."""
        key = RK.rbac_role_meta(org, sup)
        if not self.r.exists(key):
            self.r.hset(key, mapping={
                "version": "0",
                "last_updated_ms": str(_now_ms()),
                "initialized": "true",
            })

    # -- Role CRUD --

    def rbac_create_role(self, org: str, sup: str, role_id: str, role_data: Dict[str, Any]) -> None:
        """Persist a new role document and update indexes."""
        key = RK.rbac_role_doc(org, sup, role_id)
        redis_data = {k: self._rbac_serialize(v) for k, v in role_data.items()}
        pipe = self.r.pipeline()
        pipe.hset(key, mapping=redis_data)
        pipe.sadd(RK.rbac_role_index(org, sup), role_id)
        role_type = role_data.get("role", "")
        if role_type:
            pipe.sadd(RK.rbac_role_type_index(org, sup, role_type), role_id)
        role_name = role_data.get("role_name", "")
        if role_name:
            pipe.hset(RK.rbac_rolename_to_id(org, sup), role_name.lower(), role_id)
        pipe.execute()
        self._rbac_bump(RK.rbac_role_meta(org, sup))

    def rbac_update_role(self, org: str, sup: str, role_id: str, fields: Dict[str, Any]) -> None:
        """Update specific fields of an existing role in-place."""
        key = RK.rbac_role_doc(org, sup, role_id)
        if not self.r.exists(key):
            raise ValueError(f"Role {role_id} does not exist")
        redis_data = {k: self._rbac_serialize(v) for k, v in fields.items()}
        redis_data["modified_ms"] = str(_now_ms())
        self.r.hset(key, mapping=redis_data)
        self._rbac_bump(RK.rbac_role_meta(org, sup))

    def rbac_delete_role(self, org: str, sup: str, role_id: str) -> bool:
        """Atomically delete a role, strip from users, and clean name→id mapping."""
        key = RK.rbac_role_doc(org, sup, role_id)
        if not self.r.exists(key):
            return False
        role_type = self.r.hget(key, "role") or ""
        if isinstance(role_type, bytes):
            role_type = role_type.decode("utf-8")
        role_name = self.r.hget(key, "role_name") or ""
        if isinstance(role_name, bytes):
            role_name = role_name.decode("utf-8")
        # The Lua script appends each user_id to this prefix.
        user_doc_key_prefix = RK.rbac_user_doc_prefix(org, sup)
        result = self._rbac_delete_role(
            keys=[
                key,
                RK.rbac_role_index(org, sup),
                RK.rbac_role_type_index(org, sup, role_type),
                RK.rbac_role_meta(org, sup),
                RK.rbac_user_index(org, sup),
                RK.rbac_rolename_to_id(org, sup),
            ],
            args=[
                role_id,
                str(_now_ms()),
                role_name.lower() if role_name else "",
                user_doc_key_prefix,
            ],
        )
        return int(result or 0) == 1

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
        """Ensure the RBAC user meta HASH exists. Idempotent."""
        key = RK.rbac_user_meta(org, sup)
        if not self.r.exists(key):
            self.r.hset(key, mapping={
                "version": "0",
                "last_updated_ms": str(_now_ms()),
                "initialized": "true",
            })

    # -- User CRUD --

    def rbac_create_user(self, org: str, sup: str, user_id: str, user_data: Dict[str, Any]) -> None:
        """Persist a new user document and update indexes."""
        key = RK.rbac_user_doc(org, sup, user_id)
        username = user_data["username"]
        redis_data = {k: self._rbac_serialize(v) for k, v in user_data.items()}
        pipe = self.r.pipeline()
        pipe.hset(key, mapping=redis_data)
        pipe.sadd(RK.rbac_user_index(org, sup), user_id)
        pipe.hset(RK.rbac_username_to_id(org, sup), username.lower(), user_id)
        pipe.execute()
        self._rbac_bump(RK.rbac_user_meta(org, sup))

    def rbac_update_user(self, org: str, sup: str, user_id: str, fields: Dict[str, Any]) -> None:
        """Update specific fields of an existing user in-place.

        Values are serialized the same way as ``rbac_create_user`` — callers
        should pass raw Python objects, not pre-encoded JSON.
        """
        key = RK.rbac_user_doc(org, sup, user_id)
        if not self.r.exists(key):
            raise ValueError(f"User {user_id} does not exist")
        fields["modified_ms"] = str(_now_ms())
        redis_data = {k: self._rbac_serialize(v) for k, v in fields.items()}
        self.r.hset(key, mapping=redis_data)
        self._rbac_bump(RK.rbac_user_meta(org, sup))

    def rbac_rename_user(self, org: str, sup: str, user_id: str, old_username: str, new_username: str) -> None:
        """Atomically update the username → user_id mapping."""
        mapping_key = RK.rbac_username_to_id(org, sup)
        pipe = self.r.pipeline()
        pipe.hdel(mapping_key, old_username.lower())
        pipe.hset(mapping_key, new_username.lower(), user_id)
        pipe.execute()

    def rbac_delete_user(self, org: str, sup: str, user_id: str) -> None:
        """Delete a user document and remove from all indexes."""
        key = RK.rbac_user_doc(org, sup, user_id)
        raw = self.r.hgetall(key)
        if not raw:
            raise ValueError(f"User {user_id} does not exist")
        username = raw.get("username", "")
        if isinstance(username, bytes):
            username = username.decode("utf-8")
        pipe = self.r.pipeline()
        pipe.delete(key)
        pipe.srem(RK.rbac_user_index(org, sup), user_id)
        if username:
            pipe.hdel(RK.rbac_username_to_id(org, sup), username.lower())
        pipe.execute()
        self._rbac_bump(RK.rbac_user_meta(org, sup))


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

    def rbac_add_role_to_user(self, org: str, sup: str, user_id: str, role_id: str) -> bool:
        """Atomically add a role to a user's role list (no-op if already present)."""
        return int(self._rbac_add_role_to_user(
            keys=[
                RK.rbac_user_doc(org, sup, user_id),
                RK.rbac_user_meta(org, sup),
            ],
            args=[role_id, str(_now_ms())],
        ) or 0) == 1

    def rbac_remove_role_from_user(self, org: str, sup: str, user_id: str, role_id: str) -> bool:
        """Atomically remove a role from a user's role list."""
        return int(self._rbac_remove_role_from_user(
            keys=[
                RK.rbac_user_doc(org, sup, user_id),
                RK.rbac_user_meta(org, sup),
            ],
            args=[role_id, str(_now_ms())],
        ) or 0) == 1

    # ------------- Organization auth tokens (login tokens) -------------

    def list_auth_tokens(self, org: str) -> List[Dict[str, Any]]:
        """List auth tokens for an organization (tokens are stored hashed; only token_id is returned)."""
        key = RK.auth_tokens(org)
        out: List[Dict[str, Any]] = []
        try:
            raw_map = self.r.hgetall(key) or {}
            for token_id, raw in raw_map.items():
                token_id_str = token_id if isinstance(token_id, str) else token_id.decode('utf-8')
                try:
                    meta = json.loads(raw) if raw else {}
                except Exception:
                    if isinstance(raw, bytes):
                        meta = {"value": raw.decode('utf-8', errors='replace')}
                    else:
                        meta = {"value": raw}
                if isinstance(meta, dict):
                    meta = dict(meta)
                else:
                    meta = {"value": meta}
                meta.setdefault("token_id", token_id_str)
                out.append(meta)
            out.sort(key=lambda x: int(x.get("created_ms") or 0), reverse=True)
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
        token = secrets.token_urlsafe(24)
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        meta = {
            "token_id": token_id,
            "created_ms": _now_ms(),
            "created_by": str(created_by or ""),
            "label": (str(label).strip() if label is not None else ""),
            "enabled": bool(enabled),
            "username": str(username or ""),
            "user_id": str(user_id or ""),
            "expires_ms": int(expires_ms) if expires_ms else 0,
        }
        try:
            self.r.hset(RK.auth_tokens(org), token_id, json.dumps(meta))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_auth_token error: {e}")
            raise
        return {"token": token, **meta}

    def delete_auth_token(self, org: str, token_id: str) -> bool:
        """Delete an auth token by token_id (sha256)."""
        if not token_id:
            return False
        try:
            return bool(self.r.hdel(RK.auth_tokens(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_auth_token error: {e}")
            return False

    def validate_auth_token(self, org: str, token: str) -> bool:
        """Validate a plaintext auth token."""
        if not token:
            return False
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        try:
            return bool(self.r.hexists(RK.auth_tokens(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] validate_auth_token error: {e}")
            return False

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
        # enabled flag — if missing, treat as enabled (back-compat)
        if "enabled" in meta and not bool(meta.get("enabled")):
            return None
        # expiry — 0 / missing means "never expires"
        try:
            exp = int(meta.get("expires_ms") or 0)
        except (TypeError, ValueError):
            exp = 0
        if exp and exp < _now_ms():
            return None
        return meta

    # ------------- Listings via SCAN -------------

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        """Yields full Redis keys: supertable:{org}:lakes:{sup}:meta:leaf:doc:* (replica-aware)."""
        info = self._resolve_replica_info(org, sup)
        effective_sup = info[0] if info else sup
        allowed = info[1] if info else None

        pattern = RK.meta_leaf_pattern(org, effective_sup)
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys:
                    ks = k if isinstance(k, str) else k.decode('utf-8')
                    if allowed:
                        simple = ks.rsplit("meta:leaf:doc:", 1)[-1]
                        if simple not in allowed:
                            continue
                    yield ks
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] SCAN error: {e}")
            return

    def scan_leaf_items(self, org: str, sup: str, count: int = 1000) -> Iterator[Dict]:
        """Iterates SCAN pages and fetches values in batches (pipeline)."""
        batch: List[str] = []
        for key in self.scan_leaf_keys(org, sup, count=count):
            batch.append(key)
            if len(batch) >= count:
                yield from self._fetch_batch(batch)
                batch = []
        if batch:
            yield from self._fetch_batch(batch)

    def _fetch_batch(self, keys: List[str]) -> Iterator[Dict]:
        try:
            with self.r.pipeline() as p:
                for k in keys:
                    p.get(k)
                vals = p.execute()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] pipeline GET error: {e}")
            return

        for k, raw in zip(keys, vals):
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                simple = k.rsplit("meta:leaf:doc:", 1)[-1]
                yield {
                    "simple": simple,
                    "version": int(obj.get("version", -1)),
                    "ts": int(obj.get("ts", 0)),
                    "path": obj.get("path", ""),
                    "payload": obj.get("payload"),
                }
            except Exception:
                continue

    # ------------- Deletions (dangerous) -------------

    def delete_simple_table(self, org: str, sup: str, simple: str) -> bool:
        """Delete a simple table's Redis meta (leaf pointer + lock).

        This does **not** delete storage. Callers should delete storage first, then call this.
        """
        if not (org and sup and simple):
            return False
        keys = [RK.meta_leaf(org, sup, simple), RK.lock_leaf(org, sup, simple)]
        try:
            # DEL returns number of keys removed
            self.r.delete(*keys)
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_simple_table error: {e}")
            return False

    def delete_super_table(self, org: str, sup: str, count: int = 1000) -> int:
        """Delete **all** Redis keys for a given supertable (meta + locks + RBAC, etc).

        This is implemented via SCAN to avoid blocking Redis.
        Returns the number of keys deleted (best-effort).
        """
        if not (org and sup):
            return 0
        pattern = RK.super_table_pattern(org, sup)
        return self._delete_by_scan(pattern=pattern, count=count)

    def _delete_by_scan(self, pattern: str, count: int = 1000) -> int:
        deleted = 0
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                # redis-py may return list[str] or list[bytes]
                str_keys = [k if isinstance(k, str) else k.decode("utf-8") for k in (keys or [])]
                if str_keys:
                    try:
                        with self.r.pipeline() as p:
                            for k in str_keys:
                                p.delete(k)
                            res = p.execute()
                        # Each delete returns 0/1, sum them
                        deleted += sum(int(x or 0) for x in res)
                    except redis.RedisError as e:
                        logger.error(f"[redis-catalog] pipeline DEL error: {e}")
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] SCAN delete error: {e}")
        return deleted

    # --------------------------------------------------------------------------- #
    # Data Sharing — provider-side share definitions
    # --------------------------------------------------------------------------- #

    def create_share(self, org: str, share_id: str, share_doc: Dict[str, Any]) -> None:
        """Store a share definition and add to the org-level index."""
        try:
            with self.r.pipeline() as p:
                p.set(RK.share_doc(org, share_id), json.dumps(share_doc))
                p.sadd(RK.share_index(org), share_id)
                p.execute()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_share error: {e}")
            raise

    def get_share(self, org: str, share_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(RK.share_doc(org, share_id))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_share error: {e}")
            return None

    def delete_share(self, org: str, share_id: str) -> bool:
        try:
            with self.r.pipeline() as p:
                p.delete(RK.share_doc(org, share_id))
                p.srem(RK.share_index(org), share_id)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_share error: {e}")
            return False

    def list_shares(self, org: str) -> List[Dict[str, Any]]:
        shares: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(RK.share_index(org))
            for sid_raw in (members or []):
                sid = sid_raw if isinstance(sid_raw, str) else sid_raw.decode("utf-8")
                raw = self.r.get(RK.share_doc(org, sid))
                if raw:
                    try:
                        shares.append(json.loads(raw))
                    except Exception:
                        continue
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_shares error: {e}")
        return shares

    # --------------------------------------------------------------------------- #
    # Data Sharing — consumer-side linked shares
    # --------------------------------------------------------------------------- #

    def create_linked_share(self, org: str, sup: str, link_id: str, link_doc: Dict[str, Any]) -> None:
        try:
            with self.r.pipeline() as p:
                p.set(RK.linked_share_doc(org, sup, link_id), json.dumps(link_doc))
                p.sadd(RK.linked_share_index(org, sup), link_id)
                p.execute()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_linked_share error: {e}")
            raise

    def get_linked_share(self, org: str, sup: str, link_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = self.r.get(RK.linked_share_doc(org, sup, link_id))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_linked_share error: {e}")
            return None

    def update_linked_share(self, org: str, sup: str, link_id: str, doc: Dict[str, Any]) -> bool:
        try:
            self.r.set(RK.linked_share_doc(org, sup, link_id), json.dumps(doc))
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] update_linked_share error: {e}")
            return False

    def delete_linked_share(self, org: str, sup: str, link_id: str) -> bool:
        try:
            with self.r.pipeline() as p:
                p.delete(RK.linked_share_doc(org, sup, link_id))
                p.srem(RK.linked_share_index(org, sup), link_id)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_linked_share error: {e}")
            return False

    def list_linked_shares(self, org: str, sup: str) -> List[Dict[str, Any]]:
        links: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(RK.linked_share_index(org, sup))
            for lid_raw in (members or []):
                lid = lid_raw if isinstance(lid_raw, str) else lid_raw.decode("utf-8")
                raw = self.r.get(RK.linked_share_doc(org, sup, lid))
                if raw:
                    try:
                        links.append(json.loads(raw))
                    except Exception:
                        continue
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_linked_shares error: {e}")
        return links

    # --------------------------------------------------------------------------- #
    # Staging / Pipe meta (for website UI)
    # --------------------------------------------------------------------------- #

    def upsert_staging_meta(self, org: str, sup: str, staging_name: str, meta: Dict[str, Any]) -> bool:
        """Upsert staging metadata and ensure it is indexed for listing."""
        if not (org and sup and staging_name):
            return False
        payload = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload["updated_at_ms"] = _now_ms()

        try:
            with self.r.pipeline() as p:
                p.set(RK.staging(org, sup, staging_name), json.dumps(payload))
                p.sadd(RK.staging_index(org, sup), staging_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_staging_meta error: {e}")
            raise


    def get_staging_meta(self, org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name):
            return None
        try:
            raw = self.r.get(RK.staging(org, sup, staging_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_staging_meta error: {e}")
            return None
        if not raw:
            return None
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def list_stagings(self, org: str, sup: str, *, count: int = 1000) -> List[str]:
        """List staging names from the staging index set."""
        if not (org and sup):
            return []
        try:
            names = self.r.smembers(RK.staging_index(org, sup)) or set()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings smembers error: {e}")
            return []
        return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})

    def delete_staging_meta(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> int:
        """Delete staging meta and *all* related keys under the staging prefix.

        This removes the staging from the staging index set, deletes the staging meta key,
        and deletes any keys matching:
            supertable:{org}:lakes:{sup}:meta:staging:doc:{staging_name}:*
        Returns number of keys deleted (best-effort; does not include SREM).
        """
        if not (org and sup and staging_name):
            return 0

        deleted = 0
        try:
            # Remove from list index
            self.r.srem(RK.staging_index(org, sup), staging_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta srem error: {e}")

        try:
            # Delete the base meta key
            deleted += int(self.r.delete(RK.staging(org, sup, staging_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta del error: {e}")

        # Delete everything under the staging namespace (pipes, pipe meta set, etc.)
        pattern = RK.staging_subkey_pattern(org, sup, staging_name)
        deleted += self._delete_by_scan(pattern=pattern, count=count)
        return deleted

    def upsert_pipe_meta(
            self,
            org: str,
            sup: str,
            staging_name: str,
            pipe_name: str,
            meta: Dict[str, Any],
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
            with self.r.pipeline() as p:
                p.set(RK.pipe(org, sup, staging_name, pipe_name), json.dumps(payload))
                p.sadd(RK.pipe_index(org, sup, staging_name), pipe_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_meta error: {e}")
            raise

    def get_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name and pipe_name):
            return None
        try:
            raw = self.r.get(RK.pipe(org, sup, staging_name, pipe_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_pipe_meta error: {e}")
            return None
        if not raw:
            return None
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None


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
        return sorted(seen)

    def delete_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> int:
        """Delete a pipe meta key and remove it from the pipe index set."""
        if not (org and sup and staging_name and pipe_name):
            return 0
        try:
            self.r.srem(RK.pipe_index(org, sup, staging_name), pipe_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta srem error: {e}")
        try:
            return int(self.r.delete(RK.pipe(org, sup, staging_name, pipe_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta del error: {e}")
            return 0

    # ========================================================================= #
    # Spark Thrift cluster management (org-scoped: supertable:{org}:_system_:spark:thrifts)
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
                RK.spark_thrifts(org),
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
            raw = self.r.hgetall(RK.spark_thrifts(org))
            clusters = []
            for _cid, data in raw.items():
                try:
                    clusters.append(json.loads(data))
                except json.JSONDecodeError:
                    pass
            return clusters
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_spark_clusters error: {e}")
            return []


    def select_spark_cluster(self, org: str, job_bytes: int, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Select the best active Spark Thrift cluster for a job of the given size.

        Selection logic:
        1. Filter clusters where status == "active"
        2. If force=False: filter where min_bytes <= job_bytes AND (max_bytes >= job_bytes OR max_bytes == 0)
        3. If force=True: skip size filtering (user explicitly requested Spark)
        4. Among matches, prefer the cluster with the tightest max_bytes
           (most specialized for this job size), breaking ties by name.
        """
        clusters = self.list_spark_clusters(org)
        candidates = []

        for c in clusters:
            if c.get("status") != "active":
                continue
            if not force:
                min_b = int(c.get("min_bytes", 0))
                max_b = int(c.get("max_bytes", 0))
                if job_bytes < min_b:
                    continue
                if max_b > 0 and job_bytes > max_b:
                    continue
            candidates.append(c)

        if not candidates:
            return None

        # Prefer tightest fit: smallest max_bytes > 0, then by name
        def sort_key(c):
            max_b = int(c.get("max_bytes", 0))
            # 0 means unlimited — sort last
            return (0 if max_b > 0 else 1, max_b, c.get("name", ""))

        candidates.sort(key=sort_key)
        return candidates[0]

    # ========================================================================= #
    # Spark Plug management (org-scoped: supertable:{org}:_system_:spark:plugs)
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
                RK.spark_plugs(org),
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
    ) -> bool:
        """Store per-table configuration (primary keys, dedup mode, etc.).

        The config dict is stored as a JSON string under a dedicated key.
        Existing config is fully replaced (last-write-wins).
        """
        if not (org and sup and simple):
            return False
        try:
            doc = dict(config)
            doc["modified_ms"] = _now_ms()
            self.r.set(
                RK.meta_table_config(org, sup, simple),
                json.dumps(doc, default=str),
            )
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_table_config error: {e}")
            return False

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
            if raw:
                return json.loads(raw)
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"[redis-catalog] get_table_config error: {e}")
        return None

    # ========================================================================= #
    # Engine runtime configuration (DuckDB memory, threads, caches, thresholds)
    # ========================================================================= #

    # Canonical field names and their env-var counterparts.  Used by the
    # resolver in engine_common to fall back to os.getenv when a field
    # is absent from Redis.
    ENGINE_CONFIG_FIELDS = {
        "engine_lite_max_bytes":       "SUPERTABLE_ENGINE_LITE_MAX_BYTES",
        "engine_spark_min_bytes":      "SUPERTABLE_ENGINE_SPARK_MIN_BYTES",
        "engine_freshness_sec":        "SUPERTABLE_ENGINE_FRESHNESS_SEC",
        "duckdb_memory_limit":         "SUPERTABLE_DUCKDB_MEMORY_LIMIT",
        "duckdb_io_multiplier":        "SUPERTABLE_DUCKDB_IO_MULTIPLIER",
        "duckdb_threads":              "SUPERTABLE_DUCKDB_THREADS",
        "duckdb_http_timeout":         "SUPERTABLE_DUCKDB_HTTP_TIMEOUT",
        "duckdb_external_cache_size":  "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE",
    }

    def set_engine_config(
            self,
            org: str,
            sup: str,
            config: Dict[str, Any],
    ) -> bool:
        """Store engine runtime configuration (DuckDB + auto-pick thresholds).

        The config dict is stored as a JSON string.  Only recognised fields
        (see ENGINE_CONFIG_FIELDS) are persisted — unknown keys are silently
        dropped to prevent injection of arbitrary settings.

        Existing config is fully replaced (last-write-wins).
        """
        if not (org and sup):
            return False
        try:
            # Whitelist recognised fields only.
            doc = {
                k: config[k]
                for k in self.ENGINE_CONFIG_FIELDS
                if k in config and config[k] is not None and str(config[k]).strip() != ""
            }
            doc["modified_ms"] = _now_ms()
            self.r.set(
                RK.config_engine(org, sup),
                json.dumps(doc, default=str),
            )
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_engine_config error: {e}")
            return False

    def get_engine_config(
            self,
            org: str,
            sup: str,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve engine runtime configuration.

        Returns None if no config has been stored for this tenant.
        """
        if not (org and sup):
            return None
        try:
            raw = self.r.get(RK.config_engine(org, sup))
            if raw:
                return json.loads(raw)
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"[redis-catalog] get_engine_config error: {e}")
        return None