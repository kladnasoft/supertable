# route: supertable.redis_keys
"""
Single source of truth for every Redis key used by SuperTable.

Rule (enforced by tests/test_redis_key_prefix.py):
  Every key MUST start with the global PREFIX ("supertable:").  The only
  module allowed to construct Redis key strings is this one.  All other
  modules must import the helper functions below.

Hierarchy
---------

    supertable:
      registry:{service_type}:{host}:{pid}     ← global service registry
      {org}:                                   ← organization / company scope
        auth:tokens
        shares:doc:{share_id}
        shares:index
        audit:stream
        audit:chain_head:{instance_id}
        audit:config                           ← runtime toggle (per org)
        spark:thrifts
        spark:plugs
        {sup}:                                 ← supertable scope
          meta:root
          meta:leaf:{simple}
          meta:mirrors
          meta:table_config:{simple}
          meta:staging:{staging_name}
          meta:staging:meta
          meta:staging:{staging_name}:pipe:{pipe_name}
          meta:staging:{staging_name}:pipe:meta
          config:engine
          lock:leaf:{simple}
          lock:stage:{stage_name}
          rbac:users:meta
          rbac:users:index
          rbac:users:doc:{user_id}
          rbac:users:name_to_id
          rbac:roles:meta
          rbac:roles:index
          rbac:roles:doc:{role_id}
          rbac:roles:type:{role_type}
          rbac:roles:name_to_id
          schema:{simple}
          table_names
          linked_shares:doc:{link_id}
          linked_shares:index
          monitor:{monitor_type}
"""
from __future__ import annotations

PREFIX: str = "supertable"


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

def assert_prefixed(key: str) -> str:
    """Raise ValueError if *key* does not start with the canonical prefix.

    Returns the key unchanged so callers can write::

        self.r.set(assert_prefixed(some_key), value)
    """
    if not isinstance(key, str) or not key.startswith(PREFIX + ":"):
        raise ValueError(
            f"Redis key violates namespace policy (must start with "
            f"'{PREFIX}:'): {key!r}"
        )
    return key


# --------------------------------------------------------------------------- #
# Global service registry
# --------------------------------------------------------------------------- #

REGISTRY_PREFIX: str = f"{PREFIX}:registry"


def registry(service_type: str, host: str, pid: int) -> str:
    return f"{REGISTRY_PREFIX}:{service_type}:{host}:{pid}"


def registry_pattern() -> str:
    """SCAN pattern for all registered service instances."""
    return f"{REGISTRY_PREFIX}:*"


# --------------------------------------------------------------------------- #
# Organization scope
# --------------------------------------------------------------------------- #

def auth_tokens(org: str) -> str:
    return f"{PREFIX}:{org}:auth:tokens"


def share_doc(org: str, share_id: str) -> str:
    return f"{PREFIX}:{org}:shares:doc:{share_id}"


def share_index(org: str) -> str:
    return f"{PREFIX}:{org}:shares:index"


def audit_stream(org: str) -> str:
    return f"{PREFIX}:{org}:audit:stream"


def audit_chain_head(org: str, instance_id: str) -> str:
    return f"{PREFIX}:{org}:audit:chain_head:{instance_id}"


def audit_config(org: str) -> str:
    """Per-organization runtime audit configuration (enable toggle, sub-flags)."""
    return f"{PREFIX}:{org}:audit:config"


def spark_thrifts(org: str) -> str:
    return f"{PREFIX}:{org}:spark:thrifts"


def spark_plugs(org: str) -> str:
    return f"{PREFIX}:{org}:spark:plugs"


# --------------------------------------------------------------------------- #
# SuperTable scope — meta
# --------------------------------------------------------------------------- #

def meta_root(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:root"


def meta_leaf(org: str, sup: str, simple: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:leaf:{simple}"


def meta_leaf_pattern(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:leaf:*"


def meta_root_pattern_for_org(org: str) -> str:
    return f"{PREFIX}:{org}:*:meta:root"


def meta_mirrors(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:mirrors"


def meta_table_config(org: str, sup: str, simple: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:table_config:{simple}"


# --------------------------------------------------------------------------- #
# SuperTable scope — engine config
# --------------------------------------------------------------------------- #

def config_engine(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:config:engine"


# --------------------------------------------------------------------------- #
# SuperTable scope — locks
# --------------------------------------------------------------------------- #

def lock_leaf(org: str, sup: str, simple: str) -> str:
    return f"{PREFIX}:{org}:{sup}:lock:leaf:{simple}"


def lock_stage(org: str, sup: str, stage_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:lock:stage:{stage_name}"


# --------------------------------------------------------------------------- #
# SuperTable scope — RBAC users
# --------------------------------------------------------------------------- #

def rbac_user_meta(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:users:meta"


def rbac_user_index(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:users:index"


def rbac_user_doc(org: str, sup: str, user_id: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:users:doc:{user_id}"


def rbac_username_to_id(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:users:name_to_id"


# --------------------------------------------------------------------------- #
# SuperTable scope — RBAC roles
# --------------------------------------------------------------------------- #

def rbac_role_meta(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:roles:meta"


def rbac_role_index(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:roles:index"


def rbac_role_doc(org: str, sup: str, role_id: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:roles:doc:{role_id}"


def rbac_role_type_index(org: str, sup: str, role_type: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:roles:type:{role_type}"


def rbac_rolename_to_id(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:rbac:roles:name_to_id"


# --------------------------------------------------------------------------- #
# SuperTable scope — staging / pipes
# --------------------------------------------------------------------------- #

def staging(org: str, sup: str, staging_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:{staging_name}"


def staging_index(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:meta"


def staging_pattern(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:*"


def staging_subkey_pattern(org: str, sup: str, staging_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:{staging_name}:*"


def pipe(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def pipe_index(org: str, sup: str, staging_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"


def pipe_pattern(org: str, sup: str, staging_name: str) -> str:
    return f"{PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:*"


# --------------------------------------------------------------------------- #
# SuperTable scope — schema / table_names / linked shares
# --------------------------------------------------------------------------- #

def schema(org: str, sup: str, simple: str) -> str:
    return f"{PREFIX}:{org}:{sup}:schema:{simple}"


def table_names(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:table_names"


def linked_share_doc(org: str, sup: str, link_id: str) -> str:
    return f"{PREFIX}:{org}:{sup}:linked_shares:doc:{link_id}"


def linked_share_index(org: str, sup: str) -> str:
    return f"{PREFIX}:{org}:{sup}:linked_shares:index"


# --------------------------------------------------------------------------- #
# SuperTable scope — monitoring
# --------------------------------------------------------------------------- #

def monitor(org: str, sup: str, monitor_type: str) -> str:
    return f"{PREFIX}:{org}:{sup}:monitor:{monitor_type}"


# --------------------------------------------------------------------------- #
# Wildcard / SCAN helpers used by deletes
# --------------------------------------------------------------------------- #

def super_table_pattern(org: str, sup: str) -> str:
    """Matches every key for one supertable (used by delete_super_table)."""
    return f"{PREFIX}:{org}:{sup}:*"
