from __future__ import annotations

import json
import time
import uuid
import hashlib
import secrets
from typing import Any, Dict, Iterator, List, Optional

import redis
from supertable.config.defaults import logger

try:
    from .redis_connector import RedisConnector, RedisOptions
except ImportError:  # pragma: no cover
    from redis_connector import RedisConnector, RedisOptions


def _now_ms() -> int:
    return int(time.time() * 1000)


def _root_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:root"


def _leaf_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:meta:leaf:{simple}"


def _lock_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:lock:leaf:{simple}"


def _stat_lock_key(org: str, sup: str) -> str:
    # Dedicated lock for stats updates
    return f"supertable:{org}:{sup}:lock:stat"


def _mirrors_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:mirrors"


# -- RBAC key helpers (UUID-based identity) -------------------------------- #

def _rbac_user_meta_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:meta"


def _rbac_user_index_key(org: str, sup: str) -> str:
    """SET of all user_ids."""
    return f"supertable:{org}:{sup}:rbac:users:index"


def _rbac_user_doc_key(org: str, sup: str, user_id: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:doc:{user_id}"


def _rbac_username_to_id_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:name_to_id"


def _rbac_role_meta_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:roles:meta"


def _rbac_role_index_key(org: str, sup: str) -> str:
    """SET of all role_ids."""
    return f"supertable:{org}:{sup}:rbac:roles:index"


def _rbac_role_doc_key(org: str, sup: str, role_id: str) -> str:
    return f"supertable:{org}:{sup}:rbac:roles:doc:{role_id}"


def _rbac_role_type_index_key(org: str, sup: str, role_type: str) -> str:
    return f"supertable:{org}:{sup}:rbac:roles:type:{role_type}"


def _rbac_rolename_to_id_key(org: str, sup: str) -> str:
    """HASH mapping lowercase role_name → role_id."""
    return f"supertable:{org}:{sup}:rbac:roles:name_to_id"


def _auth_tokens_key(org: str) -> str:
    # Store hashed tokens (sha256) -> JSON metadata in a single Redis hash.
    # Value example: {"created_ms": 123, "created_by": "superuser", "label": "dev", "enabled": true}
    return f"supertable:{org}:auth:tokens"


# Deprecated key helpers — kept for reference, no longer used by RBAC code.
def _users_key(org: str, sup: str) -> str:
    return _rbac_user_meta_key(org, sup)


def _roles_key(org: str, sup: str) -> str:
    return _rbac_role_meta_key(org, sup)


def _user_hash_key(org: str, sup: str, user_hash: str) -> str:
    return _rbac_user_doc_key(org, sup, user_hash)


def _role_hash_key(org: str, sup: str, role_hash: str) -> str:
    return _rbac_role_doc_key(org, sup, role_hash)


def _user_name_to_hash_key(org: str, sup: str) -> str:
    return _rbac_username_to_id_key(org, sup)


def _role_type_to_hash_key(org: str, sup: str, role_type: str) -> str:
    return _rbac_role_type_index_key(org, sup, role_type)


# --------------------------------------------------------------------------- #
# Spark cluster key helpers
# --------------------------------------------------------------------------- #

def _spark_clusters_key() -> str:
    """HASH: cluster_id → JSON cluster config (global, not org-scoped)."""
    return "supertable:spark:clusters"


def _spark_cluster_doc_key(cluster_id: str) -> str:
    """HASH fields for a single cluster document."""
    return f"supertable:spark:cluster:{cluster_id}"


# --------------------------------------------------------------------------- #
# Staging / Pipe meta keys (Redis-backed UI listing)
# --------------------------------------------------------------------------- #

def _staging_key(org: str, sup: str, staging_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}"


def _stagings_meta_key(org: str, sup: str) -> str:
    # Set of staging names for fast listing
    return f"supertable:{org}:{sup}:meta:staging:meta"


def _pipe_key(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def _pipes_meta_key(org: str, sup: str, staging_name: str) -> str:
    # Set of pipe names for a staging for fast listing
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"


def _stage_lock_key(org: str, sup: str, stage_name: str) -> str:
    return f"supertable:{org}:{sup}:lock:stage:{stage_name}"


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

    _LUA_LOCK_RELEASE_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('DEL', key)
  return 1
end
return 0
"""

    _LUA_LOCK_EXTEND_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
return 0
"""

    # ------------- RBAC Lua scripts ------------- #

    _LUA_RBAC_BUMP_META = """
local key = KEYS[1]
local now = ARGV[1]
local v = redis.call('HINCRBY', key, 'version', 1)
redis.call('HSET', key, 'last_updated_ms', now)
return v
"""

    _LUA_RBAC_DELETE_ROLE = """
local role_id = ARGV[1]
local now_ms  = ARGV[2]
local org     = ARGV[3]
local sup     = ARGV[4]

local user_ids = redis.call('SMEMBERS', KEYS[5])
for _, uid in ipairs(user_ids) do
    local ukey = 'supertable:' .. org .. ':' .. sup .. ':rbac:users:doc:' .. uid
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
        self._lock_release_if_token = self.r.register_script(self._LUA_LOCK_RELEASE_IF_TOKEN)
        self._lock_extend_if_token = self.r.register_script(self._LUA_LOCK_EXTEND_IF_TOKEN)

        # RBAC Lua scripts
        self._rbac_bump_meta = self.r.register_script(self._LUA_RBAC_BUMP_META)
        self._rbac_delete_role = self.r.register_script(self._LUA_RBAC_DELETE_ROLE)
        self._rbac_remove_role_from_user = self.r.register_script(self._LUA_RBAC_REMOVE_ROLE_FROM_USER)
        self._rbac_add_role_to_user = self.r.register_script(self._LUA_RBAC_ADD_ROLE_TO_USER)

    # ------------- Locking -------------

    def acquire_simple_lock(self, org: str, sup: str, simple: str, ttl_s: int = 30, timeout_s: int = 30) -> Optional[
        str]:
        """SET lock key NX EX with retry/backoff <= timeout. Returns token if acquired else None."""
        key = _lock_key(org, sup, simple)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_simple_lock(self, org: str, sup: str, simple: str, token: str) -> bool:
        """Compare-and-delete via Lua."""
        try:
            res = self._lock_release_if_token(keys=[_lock_key(org, sup, simple)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] release error: {e}")
            return False

    def extend_simple_lock(self, org: str, sup: str, simple: str, token: str, ttl_ms: int) -> bool:
        """Optionally extend TTL if token matches."""
        try:
            res = self._lock_extend_if_token(keys=[_lock_key(org, sup, simple)], args=[token, int(ttl_ms)])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] extend error: {e}")
            return False

    # ---- Stage lock (staging + pipe mutations) ----

    def acquire_stage_lock(
            self,
            org: str,
            sup: str,
            stage_name: str,
            ttl_s: int = 30,
            timeout_s: int = 30,
    ) -> Optional[str]:
        """Acquire lock for staging/pipe operations:
            supertable:{org}:{sup}:lock:stage:{stage_name}
        """
        key = _stage_lock_key(org, sup, stage_name)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-stage-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_stage_lock(self, org: str, sup: str, stage_name: str, token: str) -> bool:
        """Release stage lock if token matches."""
        try:
            res = self._lock_release_if_token(keys=[_stage_lock_key(org, sup, stage_name)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stage-lock] release error: {e}")
            return False

    def extend_stage_lock(self, org: str, sup: str, stage_name: str, token: str, ttl_ms: int) -> bool:
        """Optionally extend stage lock TTL if token matches."""
        try:
            res = self._lock_extend_if_token(keys=[_stage_lock_key(org, sup, stage_name)], args=[token, int(ttl_ms)])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stage-lock] extend error: {e}")
            return False

    # ---- Stats lock (for monitoring _stats.json updates) ----

    def acquire_stat_lock(self, org: str, sup: str, ttl_s: int = 10, timeout_s: int = 10) -> Optional[str]:
        """Acquire stat lock: supertable:{org}:{sup}:lock:stat"""
        key = _stat_lock_key(org, sup)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-stat-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_stat_lock(self, org: str, sup: str, token: str) -> bool:
        """Release stat lock if token matches."""
        try:
            res = self._lock_release_if_token(keys=[_stat_lock_key(org, sup)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stat-lock] release error: {e}")
            return False

    # ------------- Pointers (root/leaf) -------------

    def ensure_root(self, org: str, sup: str) -> None:
        """Initialize meta:root if missing with version=0."""
        key = _root_key(org, sup)
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
            return bool(self.r.exists(_root_key(org, sup)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_exists error: {e}")
            return False

    def leaf_exists(self, org: str, sup: str, simple: str) -> bool:
        """Check existence of meta:leaf key for a simple table."""
        try:
            return bool(self.r.exists(_leaf_key(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_exists error: {e}")
            return False

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        try:
            raw = self.r.get(_root_key(org, sup))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_root error: {e}")
            return None

    def bump_root(self, org: str, sup: str, now_ms: Optional[int] = None) -> int:
        try:
            return int(self._root_bump(keys=[_root_key(org, sup)], args=[int(now_ms or _now_ms())]) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_bump error: {e}")
            raise

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        try:
            raw = self.r.get(_leaf_key(org, sup, simple))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_leaf error: {e}")
            return None

    def set_leaf_path_cas(self, org: str, sup: str, simple: str, path: str, now_ms: Optional[int] = None) -> int:
        try:
            return int(
                self._leaf_cas_set(keys=[_leaf_key(org, sup, simple)], args=[path, int(now_ms or _now_ms())]) or 0
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
                    keys=[_leaf_key(org, sup, simple)],
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
            raw = self.r.get(_mirrors_key(org, sup))
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
            self.r.set(_mirrors_key(org, sup), json.dumps(payload))
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
        """Get all users for organization (reads from SET index, not SCAN)."""
        users: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(_rbac_user_index_key(org, sup))
            for uid_raw in (members or []):
                uid = self._decode_member(uid_raw)
                raw = self.r.hgetall(_rbac_user_doc_key(org, sup, uid))
                if raw:
                    data: Dict[str, Any] = dict(raw)
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
        """Get all roles for organization (reads from SET index, not SCAN)."""
        roles: List[Dict[str, Any]] = []
        try:
            members = self.r.smembers(_rbac_role_index_key(org, sup))
            for rid_raw in (members or []):
                rid = self._decode_member(rid_raw)
                raw = self.r.hgetall(_rbac_role_doc_key(org, sup, rid))
                if raw:
                    data: Dict[str, Any] = dict(raw)
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
            raw = self.r.hgetall(_rbac_role_doc_key(org, sup, role_id))
            if not raw:
                return None
            data: Dict[str, Any] = dict(raw)
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
            raw = self.r.hgetall(_rbac_user_doc_key(org, sup, user_id))
            if not raw:
                return None
            data: Dict[str, Any] = dict(raw)
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
        key = _rbac_role_meta_key(org, sup)
        if not self.r.exists(key):
            self.r.hset(key, mapping={
                "version": "0",
                "last_updated_ms": str(_now_ms()),
                "initialized": "true",
            })

    # -- Role CRUD --

    def rbac_create_role(self, org: str, sup: str, role_id: str, role_data: Dict[str, Any]) -> None:
        """Persist a new role document and update indexes."""
        key = _rbac_role_doc_key(org, sup, role_id)
        redis_data = {k: self._rbac_serialize(v) for k, v in role_data.items()}
        pipe = self.r.pipeline()
        pipe.hset(key, mapping=redis_data)
        pipe.sadd(_rbac_role_index_key(org, sup), role_id)
        role_type = role_data.get("role", "")
        if role_type:
            pipe.sadd(_rbac_role_type_index_key(org, sup, role_type), role_id)
        role_name = role_data.get("role_name", "")
        if role_name:
            pipe.hset(_rbac_rolename_to_id_key(org, sup), role_name.lower(), role_id)
        pipe.execute()
        self._rbac_bump(_rbac_role_meta_key(org, sup))

    def rbac_update_role(self, org: str, sup: str, role_id: str, fields: Dict[str, Any]) -> None:
        """Update specific fields of an existing role in-place."""
        key = _rbac_role_doc_key(org, sup, role_id)
        if not self.r.exists(key):
            raise ValueError(f"Role {role_id} does not exist")
        redis_data = {k: self._rbac_serialize(v) for k, v in fields.items()}
        redis_data["modified_ms"] = str(_now_ms())
        self.r.hset(key, mapping=redis_data)
        self._rbac_bump(_rbac_role_meta_key(org, sup))

    def rbac_delete_role(self, org: str, sup: str, role_id: str) -> bool:
        """Atomically delete a role and strip it from every user's role list."""
        key = _rbac_role_doc_key(org, sup, role_id)
        if not self.r.exists(key):
            return False
        role_type = self.r.hget(key, "role") or ""
        if isinstance(role_type, bytes):
            role_type = role_type.decode("utf-8")
        role_name = self.r.hget(key, "role_name") or ""
        if isinstance(role_name, bytes):
            role_name = role_name.decode("utf-8")
        result = self._rbac_delete_role(
            keys=[
                key,
                _rbac_role_index_key(org, sup),
                _rbac_role_type_index_key(org, sup, role_type),
                _rbac_role_meta_key(org, sup),
                _rbac_user_index_key(org, sup),
            ],
            args=[role_id, str(_now_ms()), org, sup],
        )
        if int(result or 0) == 1:
            if role_name:
                self.r.hdel(_rbac_rolename_to_id_key(org, sup), role_name.lower())
            return True
        return False

    def rbac_role_exists(self, org: str, sup: str, role_id: str) -> bool:
        return bool(self.r.exists(_rbac_role_doc_key(org, sup, role_id)))

    def rbac_list_role_ids(self, org: str, sup: str) -> List[str]:
        """Return all role_ids from the index SET."""
        members = self.r.smembers(_rbac_role_index_key(org, sup))
        return [self._decode_member(m) for m in (members or [])]

    def rbac_get_role_ids_by_type(self, org: str, sup: str, role_type: str) -> List[str]:
        """Return role_ids belonging to a specific role type."""
        members = self.r.smembers(_rbac_role_type_index_key(org, sup, role_type))
        return [self._decode_member(m) for m in (members or [])]

    def rbac_get_superadmin_role_id(self, org: str, sup: str) -> Optional[str]:
        """Return the first superadmin role_id, or None."""
        ids = self.rbac_get_role_ids_by_type(org, sup, "superadmin")
        return ids[0] if ids else None

    def rbac_get_role_id_by_name(self, org: str, sup: str, role_name: str) -> Optional[str]:
        """Look up a role_id from a role_name (case-insensitive)."""
        val = self.r.hget(_rbac_rolename_to_id_key(org, sup), role_name.lower())
        if val is None:
            return None
        return self._decode_member(val)

    # -- User meta init --

    def rbac_init_user_meta(self, org: str, sup: str) -> None:
        """Ensure the RBAC user meta HASH exists. Idempotent."""
        key = _rbac_user_meta_key(org, sup)
        if not self.r.exists(key):
            self.r.hset(key, mapping={
                "version": "0",
                "last_updated_ms": str(_now_ms()),
                "initialized": "true",
            })

    # -- User CRUD --

    def rbac_create_user(self, org: str, sup: str, user_id: str, user_data: Dict[str, Any]) -> None:
        """Persist a new user document and update indexes."""
        key = _rbac_user_doc_key(org, sup, user_id)
        username = user_data["username"]
        redis_data = {k: self._rbac_serialize(v) for k, v in user_data.items()}
        pipe = self.r.pipeline()
        pipe.hset(key, mapping=redis_data)
        pipe.sadd(_rbac_user_index_key(org, sup), user_id)
        pipe.hset(_rbac_username_to_id_key(org, sup), username.lower(), user_id)
        pipe.execute()
        self._rbac_bump(_rbac_user_meta_key(org, sup))

    def rbac_update_user(self, org: str, sup: str, user_id: str, fields: Dict[str, str]) -> None:
        """Update specific fields of an existing user in-place."""
        key = _rbac_user_doc_key(org, sup, user_id)
        if not self.r.exists(key):
            raise ValueError(f"User {user_id} does not exist")
        fields["modified_ms"] = str(_now_ms())
        self.r.hset(key, mapping=fields)
        self._rbac_bump(_rbac_user_meta_key(org, sup))

    def rbac_rename_user(self, org: str, sup: str, user_id: str, old_username: str, new_username: str) -> None:
        """Atomically update the username → user_id mapping."""
        mapping_key = _rbac_username_to_id_key(org, sup)
        pipe = self.r.pipeline()
        pipe.hdel(mapping_key, old_username.lower())
        pipe.hset(mapping_key, new_username.lower(), user_id)
        pipe.execute()

    def rbac_delete_user(self, org: str, sup: str, user_id: str) -> None:
        """Delete a user document and remove from all indexes."""
        key = _rbac_user_doc_key(org, sup, user_id)
        raw = self.r.hgetall(key)
        if not raw:
            raise ValueError(f"User {user_id} does not exist")
        username = raw.get("username", "")
        if isinstance(username, bytes):
            username = username.decode("utf-8")
        pipe = self.r.pipeline()
        pipe.delete(key)
        pipe.srem(_rbac_user_index_key(org, sup), user_id)
        if username:
            pipe.hdel(_rbac_username_to_id_key(org, sup), username.lower())
        pipe.execute()
        self._rbac_bump(_rbac_user_meta_key(org, sup))

    def rbac_user_exists(self, org: str, sup: str, user_id: str) -> bool:
        return bool(self.r.exists(_rbac_user_doc_key(org, sup, user_id)))

    def rbac_get_user_id_by_username(self, org: str, sup: str, username: str) -> Optional[str]:
        """Look up a user_id from a username (case-insensitive)."""
        val = self.r.hget(_rbac_username_to_id_key(org, sup), username.lower())
        if val is None:
            return None
        return self._decode_member(val)

    def rbac_list_user_ids(self, org: str, sup: str) -> List[str]:
        """Return all user_ids from the index SET."""
        members = self.r.smembers(_rbac_user_index_key(org, sup))
        return [self._decode_member(m) for m in (members or [])]

    # -- Atomic role ↔ user mutations --

    def rbac_add_role_to_user(self, org: str, sup: str, user_id: str, role_id: str) -> bool:
        """Atomically add a role to a user's role list (no-op if already present)."""
        return int(self._rbac_add_role_to_user(
            keys=[
                _rbac_user_doc_key(org, sup, user_id),
                _rbac_user_meta_key(org, sup),
            ],
            args=[role_id, str(_now_ms())],
        ) or 0) == 1

    def rbac_remove_role_from_user(self, org: str, sup: str, user_id: str, role_id: str) -> bool:
        """Atomically remove a role from a user's role list."""
        return int(self._rbac_remove_role_from_user(
            keys=[
                _rbac_user_doc_key(org, sup, user_id),
                _rbac_user_meta_key(org, sup),
            ],
            args=[role_id, str(_now_ms())],
        ) or 0) == 1

    # ------------- Organization auth tokens (login tokens) -------------

    def list_auth_tokens(self, org: str) -> List[Dict[str, Any]]:
        """List auth tokens for an organization (tokens are stored hashed; only token_id is returned)."""
        key = _auth_tokens_key(org)
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
    ) -> Dict[str, Any]:
        """Create a new auth token.

        The plaintext token is returned ONLY once. Redis stores only token_id (sha256(token)).
        """
        token = secrets.token_urlsafe(24)
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        meta = {
            "token_id": token_id,
            "created_ms": _now_ms(),
            "created_by": str(created_by or ""),
            "label": (str(label).strip() if label is not None else ""),
            "enabled": bool(enabled),
        }
        try:
            self.r.hset(_auth_tokens_key(org), token_id, json.dumps(meta))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_auth_token error: {e}")
            raise
        return {"token": token, **meta}

    def delete_auth_token(self, org: str, token_id: str) -> bool:
        """Delete an auth token by token_id (sha256)."""
        if not token_id:
            return False
        try:
            return bool(self.r.hdel(_auth_tokens_key(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_auth_token error: {e}")
            return False

    def validate_auth_token(self, org: str, token: str) -> bool:
        """Validate a plaintext auth token."""
        if not token:
            return False
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        try:
            return bool(self.r.hexists(_auth_tokens_key(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] validate_auth_token error: {e}")
            return False

    # ------------- Listings via SCAN -------------

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        """Yields full Redis keys: supertable:{org}:{sup}:meta:leaf:*"""
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys:
                    yield k if isinstance(k, str) else k.decode('utf-8')
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
                simple = k.rsplit("meta:leaf:", 1)[-1]
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
        keys = [_leaf_key(org, sup, simple), _lock_key(org, sup, simple)]
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
        pattern = f"supertable:{org}:{sup}:*"
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
                p.set(_staging_key(org, sup, staging_name), json.dumps(payload))
                p.sadd(_stagings_meta_key(org, sup), staging_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_staging_meta error: {e}")
            raise

    def staging_exists(self, org: str, sup: str, staging_name: str) -> bool:
        """Fast existence check for a staging (Redis-backed)."""
        if not (org and sup and staging_name):
            return False
        try:
            return bool(self.r.exists(_staging_key(org, sup, staging_name)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] staging_exists error: {e}")
            return False

    def get_staging_meta(self, org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name):
            return None
        try:
            raw = self.r.get(_staging_key(org, sup, staging_name))
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
        """List staging names. Prefers the staging index set; falls back to SCAN."""
        if not (org and sup):
            return []
        try:
            names = list(self.r.smembers(_stagings_meta_key(org, sup)) or [])
            if names:
                return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings smembers error: {e}")

        # Fallback (migration/back-compat): scan exact staging meta keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:*"
        seen = set()
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys or []:
                    kk = k if isinstance(k, str) else k.decode("utf-8")
                    if ":pipe:" in kk:
                        continue
                    if kk.endswith(":meta"):
                        continue
                    name = kk.rsplit("meta:staging:", 1)[-1]
                    if name:
                        seen.add(name)
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings scan error: {e}")
        return sorted(seen)

    def delete_staging_meta(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> int:
        """Delete staging meta and *all* related keys under the staging prefix.

        This removes the staging from the staging index set, deletes the staging meta key,
        and deletes any keys matching:
            supertable:{org}:{sup}:meta:staging:{staging_name}:*
        Returns number of keys deleted (best-effort; does not include SREM).
        """
        if not (org and sup and staging_name):
            return 0

        deleted = 0
        try:
            # Remove from list index
            self.r.srem(_stagings_meta_key(org, sup), staging_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta srem error: {e}")

        try:
            # Delete the base meta key
            deleted += int(self.r.delete(_staging_key(org, sup, staging_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta del error: {e}")

        # Delete everything under the staging namespace (pipes, pipe meta set, etc.)
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:*"
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
                p.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(payload))
                p.sadd(_pipes_meta_key(org, sup, staging_name), pipe_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_meta error: {e}")
            raise

    def get_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name and pipe_name):
            return None
        try:
            raw = self.r.get(_pipe_key(org, sup, staging_name, pipe_name))
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

    def get_pipe_defs(self, org: str, sup: str, staging_name: str, pipe_name: str) -> List[Dict[str, Any]]:
        """Read a pipe definition payload stored as a JSON **list** (spec).

        Back-compat: if a dict is stored, it is returned as a single-item list.
        """
        if not (org and sup and staging_name and pipe_name):
            return []
        try:
            raw = self.r.get(_pipe_key(org, sup, staging_name, pipe_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_pipe_defs error: {e}")
            return []
        if not raw:
            return []
        try:
            obj = json.loads(raw)
        except Exception:
            return []
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
        return []

    def upsert_pipe_defs(
            self,
            org: str,
            sup: str,
            staging_name: str,
            pipe_name: str,
            defs: List[Dict[str, Any]],
    ) -> bool:
        """Store a pipe definition payload as a JSON **list** and index it for listing."""
        if not (org and sup and staging_name and pipe_name):
            return False
        safe_defs = [d for d in (defs or []) if isinstance(d, dict)]
        try:
            with self.r.pipeline() as p:
                p.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(safe_defs))
                p.sadd(_pipes_meta_key(org, sup, staging_name), pipe_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_defs error: {e}")
            raise

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
            names = list(self.r.smembers(_pipes_meta_key(org, sup, staging_name)) or [])
            if names:
                return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_pipes smembers error: {e}")

        # Fallback: scan pipe definition keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:*"
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
            self.r.srem(_pipes_meta_key(org, sup, staging_name), pipe_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta srem error: {e}")
        try:
            return int(self.r.delete(_pipe_key(org, sup, staging_name, pipe_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta del error: {e}")
            return 0

    # ========================================================================= #
    # Spark cluster management
    # ========================================================================= #

    def register_spark_cluster(self, cluster_id: str, config: Dict[str, Any]) -> None:
        """
        Register or update a Spark Thrift cluster.

        config should include:
            thrift_host: str       — hostname of the Thrift Server
            thrift_port: int       — port (default 10000)
            name: str              — human-readable name
            min_bytes: int         — minimum job size for this cluster (default 0)
            max_bytes: int         — maximum job size (default unlimited)
            status: str            — "active" | "draining" | "offline"
            s3_enabled: bool       — can read from S3/MinIO directly
        """
        try:
            doc = dict(config)
            doc["cluster_id"] = cluster_id
            doc.setdefault("status", "active")
            doc.setdefault("thrift_port", 10000)
            doc.setdefault("min_bytes", 0)
            doc.setdefault("max_bytes", 0)
            doc.setdefault("s3_enabled", True)
            doc["modified_ms"] = _now_ms()
            self.r.hset(
                _spark_clusters_key(),
                cluster_id,
                json.dumps(doc, default=str),
            )
            logger.info(f"[redis-catalog] spark cluster registered: {cluster_id}")
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] register_spark_cluster error: {e}")

    def deregister_spark_cluster(self, cluster_id: str) -> bool:
        """Remove a Spark cluster from the registry."""
        try:
            return bool(self.r.hdel(_spark_clusters_key(), cluster_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] deregister_spark_cluster error: {e}")
            return False

    def get_spark_cluster(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Return config for a specific cluster."""
        try:
            raw = self.r.hget(_spark_clusters_key(), cluster_id)
            if raw:
                return json.loads(raw)
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"[redis-catalog] get_spark_cluster error: {e}")
        return None

    def list_spark_clusters(self) -> List[Dict[str, Any]]:
        """Return all registered Spark clusters."""
        try:
            raw = self.r.hgetall(_spark_clusters_key())
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

    def update_spark_cluster_status(self, cluster_id: str, status: str) -> bool:
        """Update only the status of a cluster (active/draining/offline)."""
        try:
            raw = self.r.hget(_spark_clusters_key(), cluster_id)
            if not raw:
                return False
            doc = json.loads(raw)
            doc["status"] = status
            doc["modified_ms"] = _now_ms()
            self.r.hset(_spark_clusters_key(), cluster_id, json.dumps(doc, default=str))
            return True
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"[redis-catalog] update_spark_cluster_status error: {e}")
            return False

    def select_spark_cluster(self, job_bytes: int, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Select the best active Spark cluster for a job of the given size.

        Selection logic:
        1. Filter clusters where status == "active"
        2. If force=False: filter where min_bytes <= job_bytes AND (max_bytes >= job_bytes OR max_bytes == 0)
        3. If force=True: skip size filtering (user explicitly requested Spark)
        4. Among matches, prefer the cluster with the tightest max_bytes
           (most specialized for this job size), breaking ties by name.
        """
        clusters = self.list_spark_clusters()
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