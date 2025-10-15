# supertable/rbac/redis_repo.py
from __future__ import annotations

import os
from typing import Iterable

import redis


def get_redis_client() -> "redis.Redis":
    """
    Create a Redis client from environment variables (or defaults).
    This keeps the rbac code decoupled from the rest of the application wiring.
    """
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD")
    return redis.Redis(host=host, port=port, db=db, password=password, decode_responses=False)


def users_index_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:users"  # HSET user_hash -> username


def user_doc_key(org: str, sup: str, user_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:users:{user_hash}"  # JSON payload (bytes)


def user_roles_key(org: str, sup: str, user_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:user_roles:{user_hash}"  # SET of role_hash


def roles_index_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles"  # HSET role_hash -> role_type


def role_doc_key(org: str, sup: str, role_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles:{role_hash}"  # JSON payload (bytes)


def role_users_key(org: str, sup: str, role_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:role_users:{role_hash}"  # SET of user_hash


def role_tables_key(org: str, sup: str, role_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:role_tables:{role_hash}"  # SET of tables (or "*")


def access_cache_key(org: str, sup: str, user_hash: str, permission: str, table: str) -> str:
    return f"supertable:{org}:{sup}:meta:access:{user_hash}:{permission}:{table}"


# ------------------------ Lua integrity guards ---------------------------- #

# Assign role to user (bidirectional set update)
# ARGV: [user_hash, role_hash]
# KEYS: [user_roles_key, role_users_key]
LUA_ASSIGN_ROLE = """
redis.call('SADD', KEYS[1], ARGV[2])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
"""

# Remove role from user (bidirectional)
# ARGV: [user_hash, role_hash]
# KEYS: [user_roles_key, role_users_key]
LUA_REMOVE_ROLE = """
redis.call('SREM', KEYS[1], ARGV[2])
redis.call('SREM', KEYS[2], ARGV[1])
return 1
"""

# Delete role: remove role from all users, then delete role keys
# ARGV: []
# KEYS: [role_users_key, role_tables_key, role_doc_key, roles_index_key, role_hash]
LUA_DELETE_ROLE = """
local users = redis.call('SMEMBERS', KEYS[1])
for _, u in ipairs(users) do
  local ukey = string.gsub(KEYS[1], 'meta:role_users:' .. KEYS[5], 'meta:user_roles:' .. u)
  redis.call('SREM', ukey, KEYS[5])
end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])
redis.call('HDEL', KEYS[4], KEYS[5])
return 1
"""
