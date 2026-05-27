"""
Regression guard for the Redis namespace policy.

Rule: every Redis key constructed in this codebase MUST start with
``supertable:``.  The only module allowed to construct key strings is
``supertable/redis_keys.py``.  Any direct ``f"supertable:..."`` /
``f"monitor:..."`` / ``f"spark:..."`` / ``f"audit:..."`` / ``f"registry:..."``
literal in another module is a regression and fails this test.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

# Ensure mandatory env is present so that imports don't blow up.
os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_keys as RK  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Every helper produces a properly-prefixed key
# ---------------------------------------------------------------------------

ORG = "acme"
SUP = "customer_db"
SIMPLE = "orders"
USER_ID = "user_abc"
ROLE_ID = "role_xyz"
STAGING = "stg1"
PIPE = "pipe1"


def _all_helpers() -> list[tuple[str, str]]:
    """Return (helper_name, sample_key) pairs for every key formatter."""
    return [
        ("system_scope",                RK.system_scope(ORG)),
        ("system_scope_pattern",        RK.system_scope_pattern(ORG)),
        ("registry",                    RK.registry(ORG, "api", "host", 1234)),
        ("registry_pattern_for_org",    RK.registry_pattern_for_org(ORG)),
        ("registry_pattern",            RK.registry_pattern()),
        ("app_master_mcp",              RK.app_master_mcp("lighthouse")),
        ("auth_tokens",                 RK.auth_tokens(ORG)),
        ("share_doc",                   RK.share_doc(ORG, "share_1")),
        ("share_index",                 RK.share_index(ORG)),
        ("audit_stream",                RK.audit_stream(ORG)),
        ("audit_chain_head",            RK.audit_chain_head(ORG, "instance_1")),
        ("audit_config",                RK.audit_config(ORG)),
        ("spark_thrifts",               RK.spark_thrifts(ORG)),
        ("spark_plugs",                 RK.spark_plugs(ORG)),
        ("meta_root",                   RK.meta_root(ORG, SUP)),
        ("meta_leaf",                   RK.meta_leaf(ORG, SUP, SIMPLE)),
        ("meta_leaf_pattern",           RK.meta_leaf_pattern(ORG, SUP)),
        ("meta_root_pattern_for_org",   RK.meta_root_pattern_for_org(ORG)),
        ("meta_mirrors",                RK.meta_mirrors(ORG, SUP)),
        ("meta_table_config",           RK.meta_table_config(ORG, SUP, SIMPLE)),
        ("config_engine",               RK.config_engine(ORG, SUP)),
        ("lock_leaf",                   RK.lock_leaf(ORG, SUP, SIMPLE)),
        ("lock_stage",                  RK.lock_stage(ORG, SUP, STAGING)),
        ("rbac_user_meta",              RK.rbac_user_meta(ORG, SUP)),
        ("rbac_user_index",             RK.rbac_user_index(ORG, SUP)),
        ("rbac_user_doc",               RK.rbac_user_doc(ORG, SUP, USER_ID)),
        ("rbac_username_to_id",         RK.rbac_username_to_id(ORG, SUP)),
        ("rbac_role_meta",              RK.rbac_role_meta(ORG, SUP)),
        ("rbac_role_index",             RK.rbac_role_index(ORG, SUP)),
        ("rbac_role_doc",               RK.rbac_role_doc(ORG, SUP, ROLE_ID)),
        ("rbac_role_type_index",        RK.rbac_role_type_index(ORG, SUP, "admin")),
        ("rbac_rolename_to_id",         RK.rbac_rolename_to_id(ORG, SUP)),
        ("staging",                     RK.staging(ORG, SUP, STAGING)),
        ("staging_index",               RK.staging_index(ORG, SUP)),
        ("staging_pattern",             RK.staging_pattern(ORG, SUP)),
        ("staging_subkey_pattern",      RK.staging_subkey_pattern(ORG, SUP, STAGING)),
        ("pipe",                        RK.pipe(ORG, SUP, STAGING, PIPE)),
        ("pipe_index",                  RK.pipe_index(ORG, SUP, STAGING)),
        ("pipe_pattern",                RK.pipe_pattern(ORG, SUP, STAGING)),
        ("schema",                      RK.schema(ORG, SUP, SIMPLE)),
        ("table_names",                 RK.table_names(ORG, SUP)),
        ("linked_share_doc",            RK.linked_share_doc(ORG, SUP, "link_1")),
        ("linked_share_index",          RK.linked_share_index(ORG, SUP)),
        ("monitor",                     RK.monitor(ORG, SUP, "plans")),
        ("super_table_pattern",         RK.super_table_pattern(ORG, SUP)),
    ]


@pytest.mark.parametrize("name,key", _all_helpers())
def test_helpers_emit_recognised_prefix(name, key):
    """Every constructor must emit a key under a recognised top-level prefix.

    Recognised: ``supertable:`` (SDK state) or ``dataisland:`` (platform
    state — service registry + app bootstrap). Per-app prefixes
    (``lighthouse:``, ``gatekeeper:``, …) are not built through this
    module — they go via the MCP ``store_app_config`` tool.
    """
    assert isinstance(key, str), f"{name} returned non-str: {key!r}"
    assert (
        key.startswith("supertable:") or key.startswith("dataisland:")
    ), (
        f"{name} returned {key!r} which violates the namespace policy "
        f"(must start with 'supertable:' or 'dataisland:')"
    )


def test_assert_prefixed_accepts_supertable_key():
    assert RK.assert_prefixed("supertable:org:foo") == "supertable:org:foo"


def test_assert_prefixed_accepts_dataisland_key():
    assert RK.assert_prefixed("dataisland:org:foo") == "dataisland:org:foo"


def test_assert_prefixed_rejects_bare_root_keys():
    for bad in ("monitor:org:sup:plans", "spark:org:thrifts", "registry:api",
                "lighthouse:acme:config"):  # per-app prefixes not built here
        with pytest.raises(ValueError):
            RK.assert_prefixed(bad)


# ---------------------------------------------------------------------------
# 1b. Layout invariants
# ---------------------------------------------------------------------------

def test_registry_key_lives_under_dataisland_prefix():
    """Service-registry keys belong to the platform layer, not SuperTable."""
    key = RK.registry(ORG, "api", "host1", 1234)
    assert key == f"dataisland:{ORG}:registry:api:host1:1234"


def test_app_master_mcp_lives_under_dataisland_prefix():
    """Bootstrap key for an app's master MCP — global, not org-scoped."""
    key = RK.app_master_mcp("lighthouse")
    assert key == "dataisland:apps:lighthouse:master_mcp"


def test_auth_tokens_still_lives_under_system_scope():
    """Org auth tokens stay under supertable:{org}:_system_:auth:tokens
    (managed by the SuperTable SDK's RBAC / auth subsystem)."""
    key = RK.auth_tokens(ORG)
    assert key == f"supertable:{ORG}:_system_:auth:tokens"


def test_registry_pattern_targets_all_orgs():
    """The cross-org SCAN pattern reaches every org's registry."""
    assert RK.registry_pattern() == "dataisland:*:registry:*"


def test_registry_pattern_for_org_is_scoped():
    assert RK.registry_pattern_for_org(ORG) == f"dataisland:{ORG}:registry:*"


# ---------------------------------------------------------------------------
# 1c. Reserved supertable names
# ---------------------------------------------------------------------------

def test_system_scope_name_is_reserved():
    assert RK.is_reserved_super_name("_system_") is True
    assert "_system_" in RK.RESERVED_SUPER_NAMES


def test_arbitrary_supertable_name_is_not_reserved():
    for name in ("orders", "customers", "_system", "system_", "auth"):
        assert RK.is_reserved_super_name(name) is False


def test_super_table_refuses_reserved_name(monkeypatch):
    """``SuperTable(super_name='_system_')`` must raise before touching Redis."""
    # Import lazily so the regression also covers fresh imports.
    from supertable.super_table import SuperTable
    with pytest.raises(ValueError, match="reserved"):
        SuperTable(super_name="_system_", organization=ORG)


# ---------------------------------------------------------------------------
# 2. No raw f-string keys outside redis_keys.py / Lua / docs
# ---------------------------------------------------------------------------

# Patterns that would constitute a violation if found anywhere outside
# redis_keys.py.  Note the search is restricted to source files in the
# library itself.
_FORBIDDEN_PATTERNS = re.compile(
    r"""f["'](?:supertable|dataisland|monitor|spark|registry|audit):"""
)

# Files / paths that are intentionally exempt from the scan:
#   - redis_keys.py is the single source of truth
#   - this very test file lists the patterns as strings
#   - the regression test is allowed to refer to legacy patterns in comments
#   - egg-info / .venv / build artifacts
_EXEMPT_NAMES = {
    "redis_keys.py",
    "test_redis_key_prefix.py",
}
_EXEMPT_PATH_PARTS = {".venv", "site-packages", "supertable.egg-info", "__pycache__", "build", "dist"}


def _iter_source_files() -> list[Path]:
    root = Path(__file__).resolve().parents[2]  # repo root
    out: list[Path] = []
    for p in root.rglob("*.py"):
        if any(part in _EXEMPT_PATH_PARTS for part in p.parts):
            continue
        if p.name in _EXEMPT_NAMES:
            continue
        out.append(p)
    return out


def test_no_raw_fstring_keys_outside_redis_keys():
    offenders: list[tuple[str, int, str]] = []
    for path in _iter_source_files():
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip comments and the literal monitoring-thread name patterns
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if "monitor:{self._key.path_key}" in line:
                # thread name, not a Redis key
                continue
            if 'name=f"audit:' in line or 'name=f"monitor:' in line:
                # thread/worker name, not a Redis key
                continue
            if _FORBIDDEN_PATTERNS.search(line):
                offenders.append((str(path), lineno, line.rstrip()))

    assert not offenders, (
        "Raw Redis key f-strings detected outside redis_keys.py. "
        "Move every key constructor to supertable/redis_keys.py.\n"
        + "\n".join(f"  {p}:{ln}: {ln_text}" for p, ln, ln_text in offenders)
    )
