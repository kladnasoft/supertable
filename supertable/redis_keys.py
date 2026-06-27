# route: supertable.redis_keys
"""
Single source of truth for every Redis key used by SuperTable + the
platform layer (dataisland-core).

This is the **v2 layout** (see ``docs/16_redis_layout.md``). v1 keys
are not readable by this module and there is no migration shim.

Two root prefixes — and only two:

    supertable:     SuperTable SDK state (catalog, RBAC, locks, audit,
                    share federation, table meta, lakes / supertables).
    dataisland:     Platform / dataisland-core state that is not the
                    SDK's concern (service registry, app bootstrap).

Hierarchy (v2)
--------------

    dataisland:
      _apps_:                                       ← app-bootstrap sentinel (no org)
        doc:{app_name}:
          master_mcp                                 STRING  per-app master MCP
      {org}:                                        ← organization scope
        registry:
          {service_type}:{host}:{pid}                STRING  heartbeat (TTL 30s)

    supertable:
      {org}:                                        ← organization scope
        system:                                   ← system sentinel
          auth:tokens                                HASH    org login tokens
          audit:
            stream                                   STREAM  audit events
            chain_head:doc:{instance_id}             HASH    per-instance chain state
            config                                   HASH    runtime audit toggle
          shares:
            doc:{share_id}                           STRING  share definition
            index                                    SET     share IDs
          engine:
            thrifts                                  HASH    Spark Thrift clusters
            plugs                                    HASH    Spark Plug runtimes
            duckdb                                   STRING  DuckDB runtime config
        lakes:                                      ← user-data sentinel
          {sup}:                                    ← supertable scope
            meta:
              root                                   STRING  root pointer
              mirrors                                STRING  enabled mirror formats
              table_names                            SET     all simple table names
              leaf:
                doc:{simple}                         STRING  leaf snapshot pointer
              rowid_seq:
                doc:{simple}                         STRING  __rowid__ counter (INCRBY)
              table_config:
                doc:{simple}                         STRING  per-table config
              staging:
                index                                SET     staging names
                doc:{staging_name}:                  ← per-staging scope
                  meta                               STRING  staging metadata blob
                  pipes:
                    index                            SET     pipe names
                    doc:{pipe_name}                  STRING  pipe definition
            lock:
              leaf:
                doc:{simple}                         STRING  per-table lock token
              stage:
                doc:{stage_name}                     STRING  per-staging lock token
            rbac:
              users:
                meta                                 HASH    version, last_updated_ms
                index                                SET     all user_ids
                name_to_id                           HASH    username → user_id
                doc:{user_id}                        HASH    user document
              roles:
                meta                                 HASH    version, last_updated_ms
                index                                SET     all role_ids
                name_to_id                           HASH    role_name → role_id
                doc:{role_id}                        HASH    role document
                type:
                  doc:{role_type}                    SET     role IDs by type
            schema:
              doc:{simple}                           STRING  table schema JSON
            linked_shares:
              index                                  SET     linked share IDs
              doc:{link_id}                          STRING  consumer-side linked share
        monitor:                                    ← position-2 (org-wide, NOT per-sup)
          {monitor_type}                             LIST    monitoring metrics
                                                            (monitor_type ∈ closed set)
                                                            Each entry payload carries a
                                                            ``supertables: [str]`` field for
                                                            attribution under cross-supertable
                                                            queries.

Design invariants (enforced by ``tests/test_redis_key_prefix.py``)
------------------------------------------------------------------

1. Every key returned by this module starts with one of
   ``_RECOGNISED_PREFIXES``.
2. Position 2 under ``supertable:{org}:`` is *always* a literal
   sentinel (``system`` or ``lakes``). User input never lives at
   position 2 — it lives at position 3, behind a sentinel that
   classifies it (``lakes:{sup}`` for supertables).
3. Position 1 under ``dataisland:`` is *always* an org name or the
   ``_apps_`` sentinel. The ``apps`` org name is reserved
   (``RESERVED_ORG_NAMES``) as defence in depth.
4. Where user input lives at the same level as a literal sibling, the
   literal is wrapped in ``:doc:`` / ``:index`` to disambiguate.
5. Underscore-wrapped names (``^_..._$``) are sentinels and are never
   accepted as user-supplied identifiers.
6. Every constructor validates its segments with ``_safe(label, value)``.
7. The only file in the codebase that may construct keys under
   ``supertable:`` or ``dataisland:`` is this one. The regression test
   ``test_no_raw_fstring_keys_outside_redis_keys`` enforces it.
"""
from __future__ import annotations

import re
from typing import FrozenSet, Optional, Tuple

# --------------------------------------------------------------------------- #
# Prefixes & sentinels (constants)
# --------------------------------------------------------------------------- #

SUPERTABLE_PREFIX: str = "supertable"
DATAISLAND_PREFIX: str = "dataisland"

# Position-2 sentinel under ``supertable:{org}:`` for org-level platform
# state (auth tokens, audit, shares, engine).
SYSTEM_SCOPE: str = "system"

# Position-2 sentinel under ``supertable:{org}:`` for user-supplied
# supertables. Everything user-created lives at
# ``supertable:{org}:lakes:{sup}:*``.
LAKES_SCOPE: str = "lakes"

# Position-2 literal under ``supertable:{org}:`` for org-wide runtime
# telemetry. Lives at this level (not under ``system``) because
# monitoring is high-volume runtime data, conceptually distinct from
# the low-volume identity / federation / compliance state under
# ``system``. Cross-supertable queries record exactly one entry per
# query at the org level, carrying a ``supertables: [...]`` field for
# attribution.
MONITOR_SCOPE: str = "monitor"

# Position-1 sentinel under ``dataisland:`` for per-app bootstrap
# entries (one key per app, no org scope).
APPS_SCOPE: str = "_apps_"

# Recognised root prefixes. Adding a new layer to the platform → extend
# this set + update the regression test.
_RECOGNISED_PREFIXES: FrozenSet[str] = frozenset(
    {SUPERTABLE_PREFIX, DATAISLAND_PREFIX}
)

# --------------------------------------------------------------------------- #
# Naming rules (regexes)
# --------------------------------------------------------------------------- #

# A sentinel is a *single*-underscore-wrapped name (e.g. ``_apps_``,
# ``_system_``). Sentinels are reserved across every user-supplied
# identifier (org, sup, simple, staging_name, pipe_name, app_name,
# share_id, link_id, user_id, role_id, role_type, instance_id).
#
# Note: this regex deliberately requires ``[a-z0-9]`` immediately
# after the leading underscore, so *double*-underscore-wrapped names
# (e.g. ``__data_quality__``) do NOT match. Double-underscore wrap is
# the SDK-internal-table convention — those names are allowed by
# ``_SAFE_SEGMENT`` below and hidden from user-facing feeds by the
# OData handler's shape detection.
SENTINEL_RE: re.Pattern[str] = re.compile(r"^_[a-z0-9][a-z0-9_-]*_$")

# Universal safe-segment regex used by every key constructor.
#
# Accepts either:
#   * a plain identifier ``[a-z0-9][a-z0-9_-]{0,63}`` — the common case
#     for user-supplied names (org, sup, simple table, staging, pipe,
#     user_id, role_id, etc.), and
#   * a *double*-underscore-wrapped name ``__[a-z0-9][a-z0-9_-]{0,59}__``
#     — the convention for SDK-internal tables (e.g. ``__data_quality__``)
#     that users normally would not create but the SDK needs to write to.
#
# Single-underscore-wrapped names (``_foo_``) match SENTINEL_RE and are
# rejected explicitly by ``_safe()``; they are reserved for SDK
# sentinels at fixed positions (``_apps_`` under ``dataisland:``).
_SAFE_SEGMENT: re.Pattern[str] = re.compile(
    r"^(__[a-z0-9][a-z0-9_-]{0,59}__|[a-z0-9][a-z0-9_-]{0,63})$"
)

# --------------------------------------------------------------------------- #
# Explicit reservations
# --------------------------------------------------------------------------- #
#
# Names that fail the user-input check on top of the sentinel pattern.
# Keep these tight — the structural rules (sentinel pattern + position-2
# sentinels) already prevent every documented collision class. These
# explicit lists exist as belt-and-braces for high-risk literals.

RESERVED_ORG_NAMES: FrozenSet[str] = frozenset({"apps"})
RESERVED_SUPER_NAMES: FrozenSet[str] = frozenset()  # sentinel regex is sufficient


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

def _safe(label: str, value: str) -> str:
    """Validate a user-supplied key segment.

    Raises ValueError if *value* is empty, longer than 64 chars,
    contains a forbidden character, or matches the sentinel pattern.
    Returns *value* unchanged on success so callers can use it inline::

        f"…:{_safe('sup', sup)}:meta:root"
    """
    if value is None:
        raise ValueError(
            f"Invalid Redis key segment for {label!r}: None "
            f"(must match {_SAFE_SEGMENT.pattern!r})"
        )
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid Redis key segment for {label!r}: {value!r} "
            f"(must be a str, got {type(value).__name__})"
        )
    if not _SAFE_SEGMENT.match(value):
        raise ValueError(
            f"Invalid Redis key segment for {label!r}: {value!r} "
            f"(must match {_SAFE_SEGMENT.pattern!r})"
        )
    if SENTINEL_RE.match(value):
        raise ValueError(
            f"Invalid Redis key segment for {label!r}: {value!r} "
            f"(matches sentinel pattern {SENTINEL_RE.pattern!r} — reserved)"
        )
    return value


def assert_prefixed(key: str) -> str:
    """Raise ValueError if *key* does not start with a recognised prefix.

    Recognised prefixes: ``supertable:`` and ``dataisland:``. Returns
    the key unchanged on success so callers can write::

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


def is_sentinel(name: str) -> bool:
    """Return True when *name* matches the underscore-wrapped sentinel
    pattern (``_foo_``). Used by callers that want to refuse sentinel
    names before passing them to ``_safe()``."""
    if not isinstance(name, str):
        return False
    return bool(SENTINEL_RE.match(name.strip()))


def is_reserved_org_name(name: str) -> bool:
    """Return True when *name* is reserved as an organization name.

    Reserved if either:
      * it matches the sentinel pattern (``^_..._$``), or
      * it appears in ``RESERVED_ORG_NAMES`` (currently ``{"apps"}``).
    """
    if not isinstance(name, str):
        return False
    stripped = name.strip()
    return is_sentinel(stripped) or stripped in RESERVED_ORG_NAMES


def is_reserved_super_name(name: str) -> bool:
    """Return True when *name* may not be used as a supertable name.

    Reserved iff it matches the sentinel pattern (``^_..._$``).
    ``RESERVED_SUPER_NAMES`` is currently empty — the structural
    sentinel check covers every collision case.
    """
    if not isinstance(name, str):
        return False
    return is_sentinel(name.strip())


# --------------------------------------------------------------------------- #
# Key parsers (for scanners that walk Redis SCAN output)
# --------------------------------------------------------------------------- #

def parse_lake_key(key: str) -> Optional[Tuple[str, str]]:
    """Extract ``(org, sup)`` from a ``supertable:{org}:lakes:{sup}:*`` key.

    Returns ``None`` when the key does not match the lakes-scoped shape.
    This is the canonical way for scanners (gc, quality, summary,
    monitoring) to walk SCAN output and recover (org, supertable_name)
    tuples without re-implementing the parse rules.
    """
    if not isinstance(key, str):
        return None
    parts = key.split(":")
    if (
        len(parts) >= 4
        and parts[0] == SUPERTABLE_PREFIX
        and parts[2] == LAKES_SCOPE
    ):
        return parts[1], parts[3]
    return None


def parse_registry_key(key: str) -> Optional[Tuple[str, str, str, str]]:
    """Extract ``(org, service_type, host, pid)`` from a registry key.

    Returns ``None`` when the key does not match
    ``dataisland:{org}:registry:{service_type}:{host}:{pid}``.
    """
    if not isinstance(key, str):
        return None
    parts = key.split(":")
    if (
        len(parts) >= 6
        and parts[0] == DATAISLAND_PREFIX
        and parts[2] == "registry"
    ):
        return parts[1], parts[3], parts[4], parts[5]
    return None


# =========================================================================== #
# Per-org system scope (supertable:{org}:system:*)
# =========================================================================== #

def system_scope(org: str) -> str:
    """The system-scope prefix for one organization (no trailing colon)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}"


def system_scope_pattern(org: str) -> str:
    """SCAN pattern for everything under one org's system scope."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:*"


# --- Org-level auth -------------------------------------------------------- #

def auth_tokens(org: str) -> str:
    """Org-level login tokens (HASH)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:auth:tokens"


# --- Org-level audit ------------------------------------------------------- #

def audit_stream(org: str) -> str:
    """Audit event stream (STREAM)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:audit:stream"


def audit_chain_head(org: str, instance_id: str) -> str:
    """Per-instance audit hash chain head (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}"
        f":audit:chain_head:doc:{_safe('instance_id', instance_id)}"
    )


def audit_config(org: str) -> str:
    """Per-org runtime audit configuration (HASH)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:audit:config"


def audit_legal_hold(org: str) -> str:
    """Per-org audit legal-hold scopes (HASH).

    Lives under the ``system:audit:`` namespace alongside the audit
    stream, chain head, and runtime config. The legal-hold record
    pins one or more (org, table, date-range) scopes whose audit
    partitions must not be deleted by the retention sweeper.
    """
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:audit:legal_hold"


# --- Org-level shares ------------------------------------------------------ #

def share_doc(org: str, share_id: str) -> str:
    """Provider-side share definition (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}"
        f":shares:doc:{_safe('share_id', share_id)}"
    )


def share_index(org: str) -> str:
    """Index of all share IDs in this org (SET)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:shares:index"


# --- Org-level Engine ------------------------------------------------------ #
#
# All query-engine state lives under one ``system:engine:`` namespace: the
# Spark Thrift cluster registry, the Spark Plug runtime registry, and the
# DuckDB (Lite/Pro) runtime config document. Grouping them keeps every engine
# knob the UI exposes under a single, discoverable path.

def engine_thrifts(org: str) -> str:
    """Spark Thrift cluster registry (HASH)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:engine:thrifts"


def engine_plugs(org: str) -> str:
    """Spark Plug runtime registry (HASH)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:engine:plugs"


# =========================================================================== #
# Lakes scope (supertable:{org}:lakes:{sup}:*)
# =========================================================================== #

def lakes_scope(org: str) -> str:
    """The lakes-scope prefix for one organization (no trailing colon)."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"


def lakes_pattern(org: str) -> str:
    """SCAN pattern for every lake (supertable) in one org."""
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}:*"


def super_table_pattern(org: str, sup: str) -> str:
    """Matches every key for one supertable (used by ``delete_super_table``)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:*"
    )


# --- Meta ------------------------------------------------------------------ #

def meta_root(org: str, sup: str) -> str:
    """Root pointer + version (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:root"
    )


def meta_root_pattern_for_org(org: str) -> str:
    """SCAN pattern matching every supertable's root pointer in this org."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}:*:meta:root"
    )


def meta_root_pattern_all_orgs() -> str:
    """SCAN pattern matching every supertable's root pointer across every org.

    Used by GC, data-quality, and summary schedulers that walk the whole
    deployment to discover ``(org, sup)`` pairs.
    """
    return f"{SUPERTABLE_PREFIX}:*:{LAKES_SCOPE}:*:meta:root"


def meta_mirrors(org: str, sup: str) -> str:
    """Enabled mirror formats (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:mirrors"
    )


def meta_table_names(org: str, sup: str) -> str:
    """Set of all simple table names in this supertable (SET).

    v1 used the flat key ``{sup}:table_names``; v2 folds it into the
    ``meta:`` family for consistency with every other supertable-level
    sub-namespace.
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:table_names"
    )


def meta_leaf(org: str, sup: str, simple: str) -> str:
    """Leaf snapshot pointer for one simple table (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:leaf:doc:{_safe('simple', simple)}"
    )


def meta_leaf_pattern(org: str, sup: str) -> str:
    """SCAN pattern matching every leaf in this supertable."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:leaf:doc:*"
    )


def meta_rowid_seq(org: str, sup: str, simple: str) -> str:
    """Monotonic ``__rowid__`` counter for one simple table (STRING, INCRBY).

    Lives in its own ``meta:rowid_seq:doc:`` namespace so it never collides
    with the ``meta:leaf:doc:*`` scan used to enumerate tables. INCRBY makes
    id reservation atomic across concurrent writers, guaranteeing
    ``__rowid__`` uniqueness within the table.
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:rowid_seq:doc:{_safe('simple', simple)}"
    )


def meta_table_config(org: str, sup: str, simple: str) -> str:
    """Per-table runtime config (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:table_config:doc:{_safe('simple', simple)}"
    )


# --- Staging + Pipes ------------------------------------------------------- #

def staging_index(org: str, sup: str) -> str:
    """Set of all staging names in this supertable (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:index"
    )


def staging_doc(org: str, sup: str, staging_name: str) -> str:
    """Staging metadata blob (STRING).

    Lives at ``…:meta:staging:doc:{name}:meta`` so the ``doc:{name}:``
    prefix can simultaneously host the staging's children
    (``doc:{name}:pipes:*``).
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc"
        f":{_safe('staging_name', staging_name)}:meta"
    )


def staging_pattern(org: str, sup: str) -> str:
    """SCAN pattern matching every staging blob in this supertable."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc:*:meta"
    )


def staging_subkey_pattern(org: str, sup: str, staging_name: str) -> str:
    """SCAN pattern matching every child key under one staging."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc"
        f":{_safe('staging_name', staging_name)}:*"
    )


def pipe_index(org: str, sup: str, staging_name: str) -> str:
    """Set of pipe names belonging to one staging (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc"
        f":{_safe('staging_name', staging_name)}:pipes:index"
    )


def pipe_doc(
    org: str, sup: str, staging_name: str, pipe_name: str
) -> str:
    """One pipe definition (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc"
        f":{_safe('staging_name', staging_name)}:pipes:doc"
        f":{_safe('pipe_name', pipe_name)}"
    )


def pipe_pattern(org: str, sup: str, staging_name: str) -> str:
    """SCAN pattern matching every pipe in one staging."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:meta:staging:doc"
        f":{_safe('staging_name', staging_name)}:pipes:doc:*"
    )


# --- DuckDB engine config -------------------------------------------------- #

def engine_duckdb(org: str) -> str:
    """DuckDB (Lite/Pro) runtime config (STRING). Org-level system scope (global, not per-supertable).

    One document holds both engines' DuckDB pragmas plus the shared auto-pick
    thresholds; see :mod:`supertable.engine.engine_config`.
    """
    return f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{SYSTEM_SCOPE}:engine:duckdb"


# --- Locks ----------------------------------------------------------------- #

def lock_leaf(org: str, sup: str, simple: str) -> str:
    """Per-table lock token (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:lock:leaf:doc:{_safe('simple', simple)}"
    )


def lock_leaf_pattern(org: str, sup: str) -> str:
    """SCAN pattern matching every per-table lock in this supertable."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:lock:leaf:doc:*"
    )


def lock_leaf_prefix(org: str, sup: str) -> str:
    """Trimmable prefix (no trailing ``*``) used by callers that strip the
    leading bytes to recover the table name from each lock key."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:lock:leaf:doc:"
    )


def lock_stage(org: str, sup: str, stage_name: str) -> str:
    """Per-staging lock token (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:lock:stage:doc:{_safe('stage_name', stage_name)}"
    )


# --- RBAC — users ---------------------------------------------------------- #

def rbac_user_meta(org: str, sup: str) -> str:
    """Users index version + last_updated_ms (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:users:meta"
    )


def rbac_user_index(org: str, sup: str) -> str:
    """Set of all user_ids (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:users:index"
    )


def rbac_username_to_id(org: str, sup: str) -> str:
    """username → user_id reverse index (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:users:name_to_id"
    )


def rbac_user_doc(org: str, sup: str, user_id: str) -> str:
    """One user document (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:users:doc:{_safe('user_id', user_id)}"
    )


def rbac_user_doc_prefix(org: str, sup: str) -> str:
    """Trimmable prefix (ending with ``doc:``) used by Lua scripts that
    concatenate a user_id at the tail — avoids the
    ``rbac_user_doc(..., "")`` idiom which is rejected by ``_safe()``."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:users:doc:"
    )


# --- RBAC — roles ---------------------------------------------------------- #

def rbac_role_meta(org: str, sup: str) -> str:
    """Roles index version + last_updated_ms (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:roles:meta"
    )


def rbac_role_index(org: str, sup: str) -> str:
    """Set of all role_ids (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:roles:index"
    )


def rbac_rolename_to_id(org: str, sup: str) -> str:
    """role_name → role_id reverse index (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:roles:name_to_id"
    )


def rbac_role_doc(org: str, sup: str, role_id: str) -> str:
    """One role document (HASH)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:roles:doc:{_safe('role_id', role_id)}"
    )


def rbac_role_type_index(org: str, sup: str, role_type: str) -> str:
    """Role IDs grouped by type (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:rbac:roles:type:doc:{_safe('role_type', role_type)}"
    )


# --- Schema ---------------------------------------------------------------- #

def schema(org: str, sup: str, simple: str) -> str:
    """Schema JSON for one simple table (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:schema:doc:{_safe('simple', simple)}"
    )


# --- Linked shares --------------------------------------------------------- #

def linked_share_index(org: str, sup: str) -> str:
    """Set of linked-share IDs (SET)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:linked_shares:index"
    )


def linked_share_doc(org: str, sup: str, link_id: str) -> str:
    """One consumer-side linked share (STRING)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:linked_shares:doc:{_safe('link_id', link_id)}"
    )


# --- Data Quality (dataisland-core quality module owns its sub-structure) #

def quality_prefix(org: str, sup: str) -> str:
    """Per-supertable data-quality namespace prefix (with trailing colon).

    The dataisland-core quality module appends its own sub-keys to
    this prefix (e.g. ``config:__global__``, ``rules:index``,
    ``rules:doc:{rule_id}``, ``schedule:{table}``, ``latest:{table}``,
    ``history``, ``pending:{table}``, ``running:{table}``,
    ``cooldown:{table}``, ``run-all-progress``).

    Centralised here so the lakes-scope prefix is enforced from a
    single source.
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}:{LAKES_SCOPE}"
        f":{_safe('sup', sup)}:quality:"
    )


# --- Monitoring ------------------------------------------------------------ #
#
# Monitoring data is stored in **daily-partitioned** Redis LISTs. The
# writer pushes to a key suffixed with the current UTC date; an external
# orchestrator drains older partitions to internal sink tables
# (``__writes__``, ``__reads__``, ``__mcp__``, ``__plans__``) and deletes
# the source keys. This bounds Redis growth to roughly one day per
# (org, monitor_type) and gives the orchestrator clean handles to grab.
#
# Key layout::
#
#     supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}    LIST  partition
#     supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}:_drain
#                                                                LIST  in-progress drain handle
#
# The ``:_drain`` suffix is the rename target used by chunked iteration
# to take an atomic snapshot of a partition before reading it in pieces.

_VALID_MONITOR_TYPES: FrozenSet[str] = frozenset(
    {"plans", "writes", "mcp", "odata", "errors", "locks", "compact"}
)

# ISO 8601 calendar date (``YYYY-MM-DD``). Anything else is rejected — the
# date is part of the key, so a malformed value would land in storage as
# a key we can't parse back.
_DATE_RE: re.Pattern[str] = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _safe_monitor_type(monitor_type: str) -> str:
    if monitor_type not in _VALID_MONITOR_TYPES:
        raise ValueError(
            f"Invalid monitor_type {monitor_type!r}; "
            f"must be one of {sorted(_VALID_MONITOR_TYPES)}"
        )
    return monitor_type


def _safe_date(date: str) -> str:
    if not isinstance(date, str) or not _DATE_RE.match(date):
        raise ValueError(
            f"Invalid date {date!r}; must be ISO 8601 YYYY-MM-DD"
        )
    return date


def monitor_partition(org: str, monitor_type: str, date: str) -> str:
    """Per-day monitoring LIST (STRING key, LIST value).

    Lives at ``supertable:{org}:monitor:{type}:doc:{date}``. The writer
    ``RPUSH``-es JSON payloads to ``date = today (UTC)``. An external
    orchestrator drains older partitions via the helpers in
    ``supertable.monitoring.partitions``.
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}"
        f":{MONITOR_SCOPE}:{_safe_monitor_type(monitor_type)}"
        f":doc:{_safe_date(date)}"
    )


def monitor_partition_drain(org: str, monitor_type: str, date: str) -> str:
    """Drain handle for chunked iteration over one partition.

    During ``iter_partition_chunks`` the source key is ``RENAME``-d to
    this handle. That gives the chunked iterator an atomic snapshot
    while leaving the source key free for new writes (which would
    otherwise be lost mid-iteration). The handle is deleted when the
    iterator exhausts.
    """
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}"
        f":{MONITOR_SCOPE}:{_safe_monitor_type(monitor_type)}"
        f":doc:{_safe_date(date)}:_drain"
    )


def monitor_partition_pattern(org: str, monitor_type: str) -> str:
    """SCAN pattern matching every partition for one (org, monitor_type)."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}"
        f":{MONITOR_SCOPE}:{_safe_monitor_type(monitor_type)}:doc:*"
    )


def monitor_partition_pattern_for_org(org: str) -> str:
    """SCAN pattern matching every monitoring partition in one org."""
    return (
        f"{SUPERTABLE_PREFIX}:{_safe('org', org)}"
        f":{MONITOR_SCOPE}:*:doc:*"
    )


def parse_monitor_partition_key(key: str) -> Optional[Tuple[str, str, str]]:
    """Extract ``(org, monitor_type, date)`` from a monitor partition key.

    Returns ``None`` when the key does not match the
    ``supertable:{org}:monitor:{type}:doc:{date}`` shape. The ``:_drain``
    handle variant is also rejected (it would round-trip back as a
    six-segment key with ``_drain`` at the tail).

    This is the canonical way for an orchestrator to walk SCAN output
    and recover the per-day coordinates without re-implementing the
    parse rules.
    """
    if not isinstance(key, str):
        return None
    parts = key.split(":")
    # supertable : {org} : monitor : {type} : doc : {date}
    if (
        len(parts) == 6
        and parts[0] == SUPERTABLE_PREFIX
        and parts[2] == MONITOR_SCOPE
        and parts[4] == "doc"
        and parts[1]
        and parts[3] in _VALID_MONITOR_TYPES
        and _DATE_RE.match(parts[5] or "")
    ):
        return parts[1], parts[3], parts[5]
    return None


# =========================================================================== #
# Platform (dataisland:) — service registry + app bootstrap
# =========================================================================== #

_VALID_SERVICE_TYPES: FrozenSet[str] = frozenset(
    {"api", "webui", "odata", "mcp", "sdk", "lighthouse"}
)


def registry(
    org: str, service_type: str, host: str, pid: int
) -> str:
    """One service-instance heartbeat key (STRING, TTL 30s).

    ``service_type`` comes from a closed set
    (``api | webui | odata | mcp | sdk | lighthouse``).
    ``host`` is the OS hostname; ``pid`` is the process id.
    """
    if service_type not in _VALID_SERVICE_TYPES:
        raise ValueError(
            f"Invalid service_type {service_type!r}; "
            f"must be one of {sorted(_VALID_SERVICE_TYPES)}"
        )
    if not isinstance(host, str) or not host or ":" in host:
        raise ValueError(f"Invalid host: {host!r}")
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid pid: {pid!r}")
    if pid_int <= 0:
        raise ValueError(f"pid must be positive: {pid_int}")
    return (
        f"{DATAISLAND_PREFIX}:{_safe('org', org)}:registry"
        f":{service_type}:{host}:{pid_int}"
    )


def registry_pattern_for_org(org: str) -> str:
    """SCAN pattern for all service-registry entries in one org."""
    return f"{DATAISLAND_PREFIX}:{_safe('org', org)}:registry:*"


def registry_pattern() -> str:
    """SCAN pattern for service-registry entries across every org."""
    return f"{DATAISLAND_PREFIX}:*:registry:*"


def app_master_mcp(app_name: str) -> str:
    """Per-app master-MCP coordinates (STRING, global — no org scope).

    Lives at ``dataisland:_apps_:doc:{app_name}:master_mcp``. The
    ``_apps_:doc:`` discipline lets us add sibling per-app keys in
    the future without restructuring.
    """
    return (
        f"{DATAISLAND_PREFIX}:{APPS_SCOPE}:doc"
        f":{_safe('app_name', app_name)}:master_mcp"
    )


def app_scope_pattern() -> str:
    """SCAN pattern for every per-app bootstrap document."""
    return f"{DATAISLAND_PREFIX}:{APPS_SCOPE}:doc:*"
