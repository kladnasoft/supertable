"""
Regression guard for the v2 Redis namespace policy.

Rules (see ``docs/16_redis_layout.md``):

  1. Every Redis key in the codebase starts with one of two recognised
     root prefixes: ``supertable:`` or ``dataisland:``.
  2. Every key is constructed inside ``supertable/redis_keys.py``.
     No other source file may contain ``f"supertable:..."`` /
     ``f"dataisland:..."`` / ``f"monitor:..."`` / ``f"spark:..."`` /
     ``f"audit:..."`` / ``f"registry:..."`` / ``f"shares:..."`` /
     ``f"lakes:..."`` / ``f"_apps_:..."`` literals.
  3. Position 2 under ``supertable:{org}:`` is always a literal
     sentinel (``_system_`` or ``lakes``). User input lives at
     position 3 behind one of those sentinels.
  4. User-supplied segments are validated via ``_safe(...)``. Sentinel
     pattern (``^_..._$``) and explicit reserved sets reject reserved
     names.
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
# Sample arguments used across the parametric tests
# ---------------------------------------------------------------------------

ORG = "acme"
SUP = "customer_db"
SIMPLE = "orders"
USER_ID = "user_abc"
ROLE_ID = "role_xyz"
STAGING = "stg1"
PIPE = "pipe1"
LINK = "link_1"
SHARE = "share_1"
INSTANCE = "inst_001"
APP = "lighthouse"


# ---------------------------------------------------------------------------
# 1. Every helper produces a key with the exact expected v2 shape
# ---------------------------------------------------------------------------

def _all_helpers() -> list[tuple[str, str, str]]:
    """Return ``(name, actual, expected)`` triples for every key formatter."""
    sys_pre = f"supertable:{ORG}:_system_"
    lake_pre = f"supertable:{ORG}:lakes:{SUP}"
    return [
        # ---- System scope (org-level platform state) --------------------
        ("system_scope",                 RK.system_scope(ORG),                 f"supertable:{ORG}:_system_"),
        ("system_scope_pattern",         RK.system_scope_pattern(ORG),         f"supertable:{ORG}:_system_:*"),
        ("auth_tokens",                  RK.auth_tokens(ORG),                  f"{sys_pre}:auth:tokens"),
        ("audit_stream",                 RK.audit_stream(ORG),                 f"{sys_pre}:audit:stream"),
        ("audit_chain_head",             RK.audit_chain_head(ORG, INSTANCE),   f"{sys_pre}:audit:chain_head:doc:{INSTANCE}"),
        ("audit_config",                 RK.audit_config(ORG),                 f"{sys_pre}:audit:config"),
        ("audit_legal_hold",             RK.audit_legal_hold(ORG),             f"{sys_pre}:audit:legal_hold"),
        ("share_doc",                    RK.share_doc(ORG, SHARE),             f"{sys_pre}:shares:doc:{SHARE}"),
        ("share_index",                  RK.share_index(ORG),                  f"{sys_pre}:shares:index"),
        ("spark_thrifts",                RK.spark_thrifts(ORG),                f"{sys_pre}:spark:thrifts"),
        ("spark_plugs",                  RK.spark_plugs(ORG),                  f"{sys_pre}:spark:plugs"),

        # ---- Lakes scope (per-org supertable enumeration) ---------------
        ("lakes_scope",                  RK.lakes_scope(ORG),                  f"supertable:{ORG}:lakes"),
        ("lakes_pattern",                RK.lakes_pattern(ORG),                f"supertable:{ORG}:lakes:*"),
        ("super_table_pattern",          RK.super_table_pattern(ORG, SUP),     f"{lake_pre}:*"),

        # ---- Meta -------------------------------------------------------
        ("meta_root",                    RK.meta_root(ORG, SUP),               f"{lake_pre}:meta:root"),
        ("meta_root_pattern_for_org",    RK.meta_root_pattern_for_org(ORG),    f"supertable:{ORG}:lakes:*:meta:root"),
        ("meta_root_pattern_all_orgs",   RK.meta_root_pattern_all_orgs(),      f"supertable:*:lakes:*:meta:root"),
        ("meta_mirrors",                 RK.meta_mirrors(ORG, SUP),            f"{lake_pre}:meta:mirrors"),
        ("meta_table_names",             RK.meta_table_names(ORG, SUP),        f"{lake_pre}:meta:table_names"),
        ("meta_leaf",                    RK.meta_leaf(ORG, SUP, SIMPLE),       f"{lake_pre}:meta:leaf:doc:{SIMPLE}"),
        ("meta_leaf_pattern",            RK.meta_leaf_pattern(ORG, SUP),       f"{lake_pre}:meta:leaf:doc:*"),
        ("meta_table_config",            RK.meta_table_config(ORG, SUP, SIMPLE), f"{lake_pre}:meta:table_config:doc:{SIMPLE}"),

        # ---- Staging + Pipes -------------------------------------------
        ("staging_index",                RK.staging_index(ORG, SUP),           f"{lake_pre}:meta:staging:index"),
        ("staging_doc",                  RK.staging_doc(ORG, SUP, STAGING),    f"{lake_pre}:meta:staging:doc:{STAGING}:meta"),
        ("staging_pattern",              RK.staging_pattern(ORG, SUP),         f"{lake_pre}:meta:staging:doc:*:meta"),
        ("staging_subkey_pattern",       RK.staging_subkey_pattern(ORG, SUP, STAGING), f"{lake_pre}:meta:staging:doc:{STAGING}:*"),
        ("pipe_index",                   RK.pipe_index(ORG, SUP, STAGING),     f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:index"),
        ("pipe_doc",                     RK.pipe_doc(ORG, SUP, STAGING, PIPE), f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:doc:{PIPE}"),
        ("pipe_pattern",                 RK.pipe_pattern(ORG, SUP, STAGING),   f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:doc:*"),

        # ---- Config -----------------------------------------------------
        ("config_engine",                RK.config_engine(ORG, SUP),           f"{lake_pre}:config:engine"),

        # ---- Locks ------------------------------------------------------
        ("lock_leaf",                    RK.lock_leaf(ORG, SUP, SIMPLE),       f"{lake_pre}:lock:leaf:doc:{SIMPLE}"),
        ("lock_leaf_pattern",            RK.lock_leaf_pattern(ORG, SUP),       f"{lake_pre}:lock:leaf:doc:*"),
        ("lock_leaf_prefix",             RK.lock_leaf_prefix(ORG, SUP),        f"{lake_pre}:lock:leaf:doc:"),
        ("lock_stage",                   RK.lock_stage(ORG, SUP, STAGING),     f"{lake_pre}:lock:stage:doc:{STAGING}"),

        # ---- RBAC — users ----------------------------------------------
        ("rbac_user_meta",               RK.rbac_user_meta(ORG, SUP),          f"{lake_pre}:rbac:users:meta"),
        ("rbac_user_index",              RK.rbac_user_index(ORG, SUP),         f"{lake_pre}:rbac:users:index"),
        ("rbac_username_to_id",          RK.rbac_username_to_id(ORG, SUP),     f"{lake_pre}:rbac:users:name_to_id"),
        ("rbac_user_doc",                RK.rbac_user_doc(ORG, SUP, USER_ID),  f"{lake_pre}:rbac:users:doc:{USER_ID}"),
        ("rbac_user_doc_prefix",         RK.rbac_user_doc_prefix(ORG, SUP),    f"{lake_pre}:rbac:users:doc:"),

        # ---- RBAC — roles ----------------------------------------------
        ("rbac_role_meta",               RK.rbac_role_meta(ORG, SUP),          f"{lake_pre}:rbac:roles:meta"),
        ("rbac_role_index",              RK.rbac_role_index(ORG, SUP),         f"{lake_pre}:rbac:roles:index"),
        ("rbac_rolename_to_id",          RK.rbac_rolename_to_id(ORG, SUP),     f"{lake_pre}:rbac:roles:name_to_id"),
        ("rbac_role_doc",                RK.rbac_role_doc(ORG, SUP, ROLE_ID),  f"{lake_pre}:rbac:roles:doc:{ROLE_ID}"),
        ("rbac_role_type_index",         RK.rbac_role_type_index(ORG, SUP, "admin"), f"{lake_pre}:rbac:roles:type:doc:admin"),

        # ---- Schema -----------------------------------------------------
        ("schema",                       RK.schema(ORG, SUP, SIMPLE),          f"{lake_pre}:schema:doc:{SIMPLE}"),

        # ---- Linked shares ---------------------------------------------
        ("linked_share_index",           RK.linked_share_index(ORG, SUP),      f"{lake_pre}:linked_shares:index"),
        ("linked_share_doc",             RK.linked_share_doc(ORG, SUP, LINK),  f"{lake_pre}:linked_shares:doc:{LINK}"),

        # ---- Monitoring (org-level, closed set) ------------------------
        ("monitor",                      RK.monitor(ORG, "plans"),             f"supertable:{ORG}:monitor:plans"),
        ("monitor_writes",               RK.monitor(ORG, "writes"),            f"supertable:{ORG}:monitor:writes"),
        ("monitor_mcp",                  RK.monitor(ORG, "mcp"),               f"supertable:{ORG}:monitor:mcp"),
        ("monitor_odata",                RK.monitor(ORG, "odata"),             f"supertable:{ORG}:monitor:odata"),
        ("monitor_errors",               RK.monitor(ORG, "errors"),            f"supertable:{ORG}:monitor:errors"),
        ("monitor_locks",                RK.monitor(ORG, "locks"),             f"supertable:{ORG}:monitor:locks"),
        ("monitor_pattern_for_org",      RK.monitor_pattern_for_org(ORG),      f"supertable:{ORG}:monitor:*"),

        # ---- Platform: dataisland: --------------------------------------
        ("registry",                     RK.registry(ORG, "api", "host1", 1234), f"dataisland:{ORG}:registry:api:host1:1234"),
        ("registry_pattern_for_org",     RK.registry_pattern_for_org(ORG),     f"dataisland:{ORG}:registry:*"),
        ("registry_pattern",             RK.registry_pattern(),                "dataisland:*:registry:*"),
        ("app_master_mcp",               RK.app_master_mcp(APP),               f"dataisland:_apps_:doc:{APP}:master_mcp"),
        ("app_scope_pattern",            RK.app_scope_pattern(),               "dataisland:_apps_:doc:*"),
    ]


@pytest.mark.parametrize("name,actual,expected", _all_helpers())
def test_helper_returns_exact_v2_shape(name, actual, expected):
    """Every constructor must emit the documented v2 shape exactly."""
    assert isinstance(actual, str), f"{name} returned non-str: {actual!r}"
    assert actual == expected, (
        f"{name} returned {actual!r}, expected {expected!r}"
    )


@pytest.mark.parametrize("name,actual,expected", _all_helpers())
def test_helper_emits_recognised_prefix(name, actual, expected):
    """Every constructor must emit a key under a recognised root prefix."""
    assert (
        actual.startswith("supertable:") or actual.startswith("dataisland:")
    ), (
        f"{name} returned {actual!r} which violates the namespace policy"
    )


# ---------------------------------------------------------------------------
# 2. ``assert_prefixed`` guard
# ---------------------------------------------------------------------------

def test_assert_prefixed_accepts_supertable_key():
    assert RK.assert_prefixed("supertable:org:foo") == "supertable:org:foo"


def test_assert_prefixed_accepts_dataisland_key():
    assert RK.assert_prefixed("dataisland:org:foo") == "dataisland:org:foo"


def test_assert_prefixed_rejects_bare_root_keys():
    bad_keys = (
        "monitor:org:sup:plans",
        "spark:org:thrifts",
        "registry:api",
        "lighthouse:acme:config",
        "lakes:acme:demo:meta:root",
        "_apps_:lighthouse:master_mcp",
        "",
    )
    for bad in bad_keys:
        with pytest.raises(ValueError):
            RK.assert_prefixed(bad)


def test_assert_prefixed_rejects_non_strings():
    for bad in (None, 42, b"supertable:bytes", ["supertable:list"]):
        with pytest.raises(ValueError):
            RK.assert_prefixed(bad)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 3. Sentinel & reservations
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["_system_", "_apps_", "_internal_", "_x_"])
def test_sentinel_pattern_matches(name):
    assert RK.is_sentinel(name) is True


@pytest.mark.parametrize(
    "name",
    ["system_", "_system", "system", "apps", "auth", "audit",
     "", "abc", "__", "_"]
)
def test_sentinel_pattern_rejects_non_sentinel(name):
    assert RK.is_sentinel(name) is False


def test_reserved_org_names_includes_apps():
    assert "apps" in RK.RESERVED_ORG_NAMES
    assert RK.is_reserved_org_name("apps") is True


def test_reserved_org_names_includes_every_sentinel():
    assert RK.is_reserved_org_name("_system_") is True
    assert RK.is_reserved_org_name("_apps_") is True
    assert RK.is_reserved_org_name("_anything_") is True


def test_reserved_org_names_rejects_normal_orgs():
    for name in ("acme", "kladna-soft", "my_org", "tenant1"):
        assert RK.is_reserved_org_name(name) is False


def test_reserved_super_names_blocks_system_via_sentinel():
    """``_system_`` is rejected via the sentinel pattern check."""
    assert RK.is_reserved_super_name("_system_") is True
    # The explicit set is empty — sentinel regex is sufficient.
    assert RK.RESERVED_SUPER_NAMES == frozenset()


def test_reserved_super_names_blocks_sentinel_pattern():
    """v2: any sentinel-pattern name is rejected as a super_name."""
    assert RK.is_reserved_super_name("_apps_") is True
    assert RK.is_reserved_super_name("_anything_") is True


def test_reserved_super_names_accepts_normal_names():
    for name in ("customers", "orders", "audit", "shares", "spark", "system_"):
        # NB: "audit", "shares", "spark" are no longer collision risks
        # (they would land at supertable:{org}:lakes:audit:* etc.),
        # so they're legal super_names in v2.
        assert RK.is_reserved_super_name(name) is False


# ---------------------------------------------------------------------------
# 4. ``_safe`` segment validator
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "good", ["acme", "my-org", "tenant_1", "a", "abc123", "x" * 64]
)
def test_safe_accepts_good_segments(good):
    assert RK._safe("test", good) == good


@pytest.mark.parametrize(
    "bad",
    [
        "",                # empty
        " ",               # whitespace
        "ACME",            # uppercase
        "ac:me",           # colon
        "ac/me",           # slash
        "ac.me",           # dot
        "_system_",        # sentinel
        "_foo_",           # sentinel
        "-leading-hyphen", # starts with hyphen
        "_leading",        # starts with underscore (sentinel territory)
        "x" * 65,          # too long
    ]
)
def test_safe_rejects_bad_segments(bad):
    with pytest.raises(ValueError):
        RK._safe("test", bad)


def test_safe_rejects_none_and_non_strings():
    for bad in (None, 42, b"bytes", ["list"]):
        with pytest.raises(ValueError):
            RK._safe("test", bad)  # type: ignore[arg-type]


def test_constructors_reject_unsafe_org():
    with pytest.raises(ValueError):
        RK.meta_root("a:b", "demo")
    with pytest.raises(ValueError):
        RK.auth_tokens("ACME")


def test_constructors_reject_unsafe_sup():
    with pytest.raises(ValueError):
        RK.meta_root("acme", "_system_")
    with pytest.raises(ValueError):
        RK.meta_root("acme", "with:colon")


def test_monitor_rejects_unknown_type():
    with pytest.raises(ValueError):
        RK.monitor("acme", "bogus")


def test_monitor_lives_at_org_level():
    """Monitoring is org-wide; the supertable scope does not appear in the key.

    Cross-supertable queries record one canonical entry per query;
    attribution is preserved in the entry's ``supertables: [...]``
    field, not in the Redis key.
    """
    key = RK.monitor("acme", "writes")
    assert key == "supertable:acme:monitor:writes"
    # No "lakes" or "_system_" segment, no super_name segment.
    assert ":lakes:" not in key
    assert ":_system_:" not in key


def test_registry_rejects_unknown_service_type():
    with pytest.raises(ValueError):
        RK.registry("acme", "bogus", "host1", 1)


def test_registry_rejects_bad_pid():
    with pytest.raises(ValueError):
        RK.registry("acme", "api", "host1", 0)
    with pytest.raises(ValueError):
        RK.registry("acme", "api", "host1", -1)
    with pytest.raises(ValueError):
        RK.registry("acme", "api", "host1", "not_a_pid")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 5. SuperTable refuses reserved super_name
# ---------------------------------------------------------------------------

def test_super_table_refuses_reserved_name():
    """``SuperTable(super_name='_system_')`` must raise before touching Redis."""
    from supertable.super_table import SuperTable
    with pytest.raises(ValueError, match="reserved"):
        SuperTable(super_name="_system_", organization=ORG)


def test_super_table_refuses_any_sentinel():
    """In v2 the sentinel pattern is also rejected."""
    from supertable.super_table import SuperTable
    with pytest.raises(ValueError, match="reserved"):
        SuperTable(super_name="_apps_", organization=ORG)


# ---------------------------------------------------------------------------
# 6. Parsers
# ---------------------------------------------------------------------------

def test_parse_lake_key_extracts_org_and_sup():
    key = RK.meta_root("acme", "demo")
    assert RK.parse_lake_key(key) == ("acme", "demo")
    key2 = RK.meta_leaf("acme", "demo", "orders")
    assert RK.parse_lake_key(key2) == ("acme", "demo")


def test_parse_lake_key_returns_none_for_non_lake_keys():
    assert RK.parse_lake_key(RK.auth_tokens("acme")) is None
    assert RK.parse_lake_key(RK.audit_stream("acme")) is None
    assert RK.parse_lake_key(RK.registry("acme", "api", "h", 1)) is None
    assert RK.parse_lake_key("not:a:valid:key") is None
    assert RK.parse_lake_key("") is None
    assert RK.parse_lake_key(None) is None  # type: ignore[arg-type]


def test_parse_registry_key_extracts_fields():
    key = RK.registry("acme", "api", "host1", 1234)
    assert RK.parse_registry_key(key) == ("acme", "api", "host1", "1234")


def test_parse_registry_key_returns_none_for_other_keys():
    assert RK.parse_registry_key(RK.meta_root("acme", "demo")) is None
    assert RK.parse_registry_key(RK.app_master_mcp("lighthouse")) is None


# ---------------------------------------------------------------------------
# 7. No raw f-string keys outside redis_keys.py
# ---------------------------------------------------------------------------

_FORBIDDEN_PATTERNS = re.compile(
    r"""f["'](?:supertable|dataisland|monitor|spark|registry|audit|shares|lakes|_apps_):"""
)

_EXEMPT_NAMES = {
    "redis_keys.py",
    "test_redis_key_prefix.py",
}
_EXEMPT_PATH_PARTS = {
    ".venv", "site-packages", "supertable.egg-info",
    "__pycache__", "build", "dist", "docs",
}


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
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            # Allow thread / worker name strings that look like keys
            if "monitor:{self._key.path_key}" in line:
                continue
            if 'name=f"audit:' in line or 'name=f"monitor:' in line:
                continue
            if _FORBIDDEN_PATTERNS.search(line):
                offenders.append((str(path), lineno, line.rstrip()))

    assert not offenders, (
        "Raw Redis key f-strings detected outside redis_keys.py. "
        "Move every key constructor to supertable/redis_keys.py.\n"
        + "\n".join(f"  {p}:{ln}: {ln_text}" for p, ln, ln_text in offenders)
    )
