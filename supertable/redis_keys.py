# route: supertable.redis_keys
"""
Single source of truth for every Redis key used by SuperTable + the
platform layer (dataisland-core).

Rule (enforced by tests/test_redis_key_prefix.py):
  Every key constructed here MUST start with one of the two recognised
  top-level prefixes:
    * ``supertable:`` — SuperTable SDK state (catalog, RBAC, locks,
                       audit, share federation, table meta, …).
    * ``dataisland:`` — platform / dataisland-core state that is not
                       SuperTable's concern (service registry, app
                       bootstrap config, etc.).
  Per-app config keys (lighthouse, gatekeeper, studio, …) live under
  their own app-name prefix and are written via the MCP server's
  ``store_app_config`` tool — they do **not** go through this module.

Hierarchy
---------

    dataisland:
      apps:{app_name}:master_mcp                ← per-app bootstrap (one global key)
      {org}:
        registry:{service_type}:{host}:{pid}    ← service-instance heartbeats (TTL)

    supertable:
      {org}:                                   ← organization / company scope
        _system_:                              ← reserved system scope (supertable-side)
          auth:tokens                          ← organization login tokens
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

Reserved supertable names
-------------------------

``_system_`` is reserved.  It MUST NOT be used as a supertable name and
``SuperTable(..., super_name="_system_")`` raises ``ValueError``.  The
name is reserved because we keep everything system-related — service
registry heartbeats, organization-level auth tokens, future
system-only scopes — under that prefix to avoid collisions with
user-created supertables.
"""
from __future__ import annotations

from typing import FrozenSet

# The canonical SuperTable SDK prefix. Everything the SDK itself writes
# lives under this.
SUPERTABLE_PREFIX: str = "supertable"

# The platform-layer prefix. dataisland-core writes its own
# infrastructure (service-registry heartbeats, app-bootstrap configs)
# here. Per-app config keys (lighthouse:*, gatekeeper:*, …) live in
# yet another set of namespaces and are not built through this module.
DATAISLAND_PREFIX: str = "dataisland"

# Recognised top-level prefixes for assert_prefixed(). Adding a new
# layer to the platform → extend this set.
_RECOGNISED_PREFIXES: FrozenSet[str] = frozenset({SUPERTABLE_PREFIX, DATAISLAND_PREFIX})

# The org-level system scope.  Reserved as a supertable name (see
# ``is_reserved_super_name`` below).  Everything system-related under an
# organization lives here so user-supplied supertable names can never
# collide with infrastructure keys.
SYSTEM_SCOPE: str = "_system_"

# All names that may NOT be used for user-created supertables.
RESERVED_SUPER_NAMES: FrozenSet[str] = frozenset({SYSTEM_SCOPE})


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

def assert_prefixed(key: str) -> str:
    """Raise ValueError if *key* does not start with a recognised prefix.

    Recognised prefixes: ``supertable:`` (SDK state) and
    ``dataisland:`` (platform state). Returns the key unchanged so
    callers can write::

        self.r.set(assert_prefixed(some_key), value)
    """
    if not isinstance(key, str):
        raise ValueError(
            f"Redis key violates namespace policy (must be a str): {key!r}"
        )
    if not any(key.startswith(p + ":") for p in _RECOGNISED_PREFIXES):
        raise ValueError(
            f"Redis key violates namespace policy (must start with one "
            f"of {sorted(_RECOGNISED_PREFIXES)}): {key!r}"
        )
    return key


def is_reserved_super_name(name: str) -> bool:
    """Return True when *name* is reserved (cannot be a supertable name)."""
    if not isinstance(name, str):
        return False
    return name.strip() in RESERVED_SUPER_NAMES


# --------------------------------------------------------------------------- #
# Per-organization system scope (registry, auth tokens, future system keys)
# --------------------------------------------------------------------------- #

def system_scope(org: str) -> str:
    """Return the system-scope prefix for one organization."""
    return f"{SUPERTABLE_PREFIX}:{org}:{SYSTEM_SCOPE}"


def system_scope_pattern(org: str) -> str:
    """SCAN pattern for everything under one org's system scope."""
    return f"{SUPERTABLE_PREFIX}:{org}:{SYSTEM_SCOPE}:*"


# --------------------------------------------------------------------------- #
# Service registry (per organization) — under the dataisland: prefix
# --------------------------------------------------------------------------- #
#
# Service-instance heartbeats are a *platform* concern, not a SuperTable
# concern. They live at:
#
#     dataisland:{org}:registry:{service_type}:{host}:{pid}
#
# (No ``_system_`` segment — that segment exists only inside
# ``supertable:{org}:`` to avoid colliding with user-created supertable
# names. Under ``dataisland:`` there are no user supertables, so the
# extra segment is dropped.)

def registry(org: str, service_type: str, host: str, pid: int) -> str:
    """Per-organization service registry entry.

    Example::

        dataisland:kladna-soft:registry:api:host1:1234
    """
    return f"{DATAISLAND_PREFIX}:{org}:registry:{service_type}:{host}:{pid}"


def registry_pattern_for_org(org: str) -> str:
    """SCAN pattern for all service-registry entries in one organization."""
    return f"{DATAISLAND_PREFIX}:{org}:registry:*"


def registry_pattern() -> str:
    """SCAN pattern for service-registry entries across every organization.

    Matches ``dataisland:*:registry:*`` — the cross-org pattern that
    scanners use when no org is supplied (e.g. fleet-wide monitoring).
    """
    return f"{DATAISLAND_PREFIX}:*:registry:*"


# --------------------------------------------------------------------------- #
# App bootstrap (per application — Lighthouse, Gatekeeper, Studio, …)
# --------------------------------------------------------------------------- #
#
# The very first thing an app reads on boot is its master-MCP config.
# That happens before any org context exists, so the key sits at a
# single global location keyed only by app_name:
#
#     dataisland:apps:{app_name}:master_mcp
#
# Written via the platform REST API (``POST /api/v1/apps/{app}/master-mcp``,
# admin-only) and read on every app boot.

def app_master_mcp(app_name: str) -> str:
    """The platform-side key holding an app's master-MCP coordinates."""
    return f"{DATAISLAND_PREFIX}:apps:{app_name}:master_mcp"


# --------------------------------------------------------------------------- #
# Organization auth (login tokens) — also under _system_ for the same reason:
# "auth" must not collide with a possible user-created supertable called "auth".
# --------------------------------------------------------------------------- #

def auth_tokens(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{SYSTEM_SCOPE}:auth:tokens"


# --------------------------------------------------------------------------- #
# Organization scope (non-system)
# --------------------------------------------------------------------------- #

def share_doc(org: str, share_id: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:shares:doc:{share_id}"


def share_index(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:shares:index"


def audit_stream(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:audit:stream"


def audit_chain_head(org: str, instance_id: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:audit:chain_head:{instance_id}"


def audit_config(org: str) -> str:
    """Per-organization runtime audit configuration (enable toggle, sub-flags)."""
    return f"{SUPERTABLE_PREFIX}:{org}:audit:config"


def spark_thrifts(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:spark:thrifts"


def spark_plugs(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:spark:plugs"


# --------------------------------------------------------------------------- #
# SuperTable scope — meta
# --------------------------------------------------------------------------- #

def meta_root(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:root"


def meta_leaf(org: str, sup: str, simple: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:leaf:{simple}"


def meta_leaf_pattern(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:leaf:*"


def meta_root_pattern_for_org(org: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:*:meta:root"


def meta_mirrors(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:mirrors"


def meta_table_config(org: str, sup: str, simple: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:table_config:{simple}"


# --------------------------------------------------------------------------- #
# SuperTable scope — engine config
# --------------------------------------------------------------------------- #

def config_engine(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:config:engine"


# --------------------------------------------------------------------------- #
# SuperTable scope — locks
# --------------------------------------------------------------------------- #

def lock_leaf(org: str, sup: str, simple: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:lock:leaf:{simple}"


def lock_stage(org: str, sup: str, stage_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:lock:stage:{stage_name}"


# --------------------------------------------------------------------------- #
# SuperTable scope — RBAC users
# --------------------------------------------------------------------------- #

def rbac_user_meta(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:users:meta"


def rbac_user_index(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:users:index"


def rbac_user_doc(org: str, sup: str, user_id: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:users:doc:{user_id}"


def rbac_username_to_id(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:users:name_to_id"


# --------------------------------------------------------------------------- #
# SuperTable scope — RBAC roles
# --------------------------------------------------------------------------- #

def rbac_role_meta(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:roles:meta"


def rbac_role_index(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:roles:index"


def rbac_role_doc(org: str, sup: str, role_id: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:roles:doc:{role_id}"


def rbac_role_type_index(org: str, sup: str, role_type: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:roles:type:{role_type}"


def rbac_rolename_to_id(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:rbac:roles:name_to_id"


# --------------------------------------------------------------------------- #
# SuperTable scope — staging / pipes
# --------------------------------------------------------------------------- #

def staging(org: str, sup: str, staging_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:{staging_name}"


def staging_index(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:meta"


def staging_pattern(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:*"


def staging_subkey_pattern(org: str, sup: str, staging_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:{staging_name}:*"


def pipe(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def pipe_index(org: str, sup: str, staging_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"


def pipe_pattern(org: str, sup: str, staging_name: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:meta:staging:{staging_name}:pipe:*"


# --------------------------------------------------------------------------- #
# SuperTable scope — schema / table_names / linked shares
# --------------------------------------------------------------------------- #

def schema(org: str, sup: str, simple: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:schema:{simple}"


def table_names(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:table_names"


def linked_share_doc(org: str, sup: str, link_id: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:linked_shares:doc:{link_id}"


def linked_share_index(org: str, sup: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:linked_shares:index"


# --------------------------------------------------------------------------- #
# SuperTable scope — monitoring
# --------------------------------------------------------------------------- #

def monitor(org: str, sup: str, monitor_type: str) -> str:
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:monitor:{monitor_type}"


# --------------------------------------------------------------------------- #
# Wildcard / SCAN helpers used by deletes
# --------------------------------------------------------------------------- #

def super_table_pattern(org: str, sup: str) -> str:
    """Matches every key for one supertable (used by delete_super_table)."""
    return f"{SUPERTABLE_PREFIX}:{org}:{sup}:*"
