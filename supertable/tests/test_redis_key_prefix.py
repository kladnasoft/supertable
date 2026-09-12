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
  3. Position 2 under ``supertable:{org}:`` is always a closed-set
     SDK literal (``system``, ``lakes``, ``monitor`` or ``query``).
     User input lives at position 3 under ``lakes:``.

     ``system``, ``monitor`` and ``query`` are company-level: a query may
     span supertables and monitoring aggregates across them, so neither
     can be filed under one. ``lakes`` is the level below, and everything
     under it names its supertable at position 3.
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
    sys_pre = f"supertable:{ORG}:system"
    lake_pre = f"supertable:{ORG}:lakes:{SUP}"
    return [
        # ---- System scope (org-level platform state) --------------------
        ("system_scope",                 RK.system_scope(ORG),                 f"supertable:{ORG}:system"),
        ("system_scope_pattern",         RK.system_scope_pattern(ORG),         f"supertable:{ORG}:system:*"),
        ("auth_tokens",                  RK.auth_tokens(ORG),                  f"{sys_pre}:auth:tokens"),
        ("audit_stream",                 RK.audit_stream(ORG),                 f"{sys_pre}:audit:stream"),
        ("audit_chain_head",             RK.audit_chain_head(ORG, INSTANCE),   f"{sys_pre}:audit:chain_head:doc:{INSTANCE}"),
        ("audit_config",                 RK.audit_config(ORG),                 f"{sys_pre}:audit:config"),
        ("audit_legal_hold",             RK.audit_legal_hold(ORG),             f"{sys_pre}:audit:legal_hold"),
        ("share_doc",                    RK.share_doc(ORG, SHARE),             f"{sys_pre}:shares:doc:{SHARE}"),
        ("share_index",                  RK.share_index(ORG),                  f"{sys_pre}:shares:index"),
        ("engine_thrifts",               RK.engine_thrifts(ORG),               f"{sys_pre}:engine:thrifts"),
        ("engine_plugs",                 RK.engine_plugs(ORG),                 f"{sys_pre}:engine:plugs"),
        ("engine_duckdb",                RK.engine_duckdb(ORG),                f"{sys_pre}:engine:duckdb"),

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
        ("meta_rowid_seq",               RK.meta_rowid_seq(ORG, SUP, SIMPLE),  f"{lake_pre}:meta:rowid_seq:doc:{SIMPLE}"),
        ("meta_table_config",            RK.meta_table_config(ORG, SUP, SIMPLE), f"{lake_pre}:meta:table_config:doc:{SIMPLE}"),

        # ---- Staging + Pipes -------------------------------------------
        ("staging_index",                RK.staging_index(ORG, SUP),           f"{lake_pre}:meta:staging:index"),
        ("staging_doc",                  RK.staging_doc(ORG, SUP, STAGING),    f"{lake_pre}:meta:staging:doc:{STAGING}:meta"),
        ("staging_pattern",              RK.staging_pattern(ORG, SUP),         f"{lake_pre}:meta:staging:doc:*:meta"),
        ("staging_subkey_pattern",       RK.staging_subkey_pattern(ORG, SUP, STAGING), f"{lake_pre}:meta:staging:doc:{STAGING}:*"),
        ("pipe_index",                   RK.pipe_index(ORG, SUP, STAGING),     f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:index"),
        ("pipe_doc",                     RK.pipe_doc(ORG, SUP, STAGING, PIPE), f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:doc:{PIPE}"),
        ("pipe_pattern",                 RK.pipe_pattern(ORG, SUP, STAGING),   f"{lake_pre}:meta:staging:doc:{STAGING}:pipes:doc:*"),

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
        ("quality_prefix",               RK.quality_prefix(ORG, SUP),          f"{lake_pre}:quality:"),

        # ---- Monitoring (org-level, closed set, daily-partitioned) ----
        ("monitor_partition_plans",      RK.monitor_partition(ORG, "plans", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:plans:doc:2026-06-09"),
        ("monitor_partition_writes",     RK.monitor_partition(ORG, "writes", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:writes:doc:2026-06-09"),
        ("monitor_partition_mcp",        RK.monitor_partition(ORG, "mcp", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:mcp:doc:2026-06-09"),
        ("monitor_partition_odata",      RK.monitor_partition(ORG, "odata", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:odata:doc:2026-06-09"),
        ("monitor_partition_errors",     RK.monitor_partition(ORG, "errors", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:errors:doc:2026-06-09"),
        ("monitor_partition_locks",      RK.monitor_partition(ORG, "locks", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:locks:doc:2026-06-09"),
        ("monitor_partition_compact",    RK.monitor_partition(ORG, "compact", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:compact:doc:2026-06-09"),
        ("monitor_partition_drain",      RK.monitor_partition_drain(ORG, "writes", "2026-06-09"),
                                                                               f"supertable:{ORG}:monitor:writes:doc:2026-06-09:_drain"),
        ("monitor_partition_pattern",    RK.monitor_partition_pattern(ORG, "writes"),
                                                                               f"supertable:{ORG}:monitor:writes:doc:*"),
        ("monitor_partition_pattern_for_org", RK.monitor_partition_pattern_for_org(ORG),
                                                                               f"supertable:{ORG}:monitor:*:doc:*"),

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
    "good",
    [
        "acme",
        "my-org",
        "tenant_1",
        "a",
        "abc123",
        "x" * 64,
        # Double-underscore-wrapped names are the SDK-internal-table
        # convention (e.g. __data_quality__). _safe() accepts them so
        # the SDK can write to internal tables; single-underscore
        # sentinels (_apps_) are still rejected below.
        "__data_quality__",
        "__audit__",
        "__a__",
    ],
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
        "_system_",        # sentinel (single-underscore-wrap)
        "_foo_",           # sentinel
        "-leading-hyphen", # starts with hyphen
        "_leading",        # starts with single underscore (not wrapped)
        "__leading",       # starts with double underscore but no closing wrap
        "____",            # only underscores, no name body
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


def test_monitor_partition_rejects_unknown_type():
    with pytest.raises(ValueError):
        RK.monitor_partition("acme", "bogus", "2026-06-09")


def test_monitor_partition_lives_at_org_level():
    """Monitoring is org-wide; the supertable scope does not appear in the key.

    Cross-supertable queries record one canonical entry per query;
    attribution is preserved in the entry's ``supertables: [...]``
    field, not in the Redis key. The partition (date) suffix bounds
    Redis growth to one day per (org, monitor_type).
    """
    key = RK.monitor_partition("acme", "writes", "2026-06-09")
    assert key == "supertable:acme:monitor:writes:doc:2026-06-09"
    # No "lakes" or "system" segment, no super_name segment.
    assert ":lakes:" not in key
    assert ":system:" not in key


def test_rowid_seq_not_matched_by_leaf_scan():
    """The __rowid__ counter must not be enumerated as a table.

    Regression: when the counter lived at ``meta:leaf:doc:{simple}:rowid_seq``
    the ``meta:leaf:doc:*`` SCAN matched it, so table listings tried to parse
    ``{simple}:rowid_seq`` as a table name and blew up in ``_safe``. Its own
    ``meta:rowid_seq:doc:`` namespace keeps it out of that scan.
    """
    from fnmatch import fnmatchcase

    seq_key = RK.meta_rowid_seq(ORG, SUP, SIMPLE)
    leaf_glob = RK.meta_leaf_pattern(ORG, SUP)
    assert not fnmatchcase(seq_key, leaf_glob)
    assert ":meta:leaf:doc:" not in seq_key
    # The genuine leaf key, by contrast, *is* matched by the same glob.
    assert fnmatchcase(RK.meta_leaf(ORG, SUP, SIMPLE), leaf_glob)


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


def test_parse_monitor_partition_key_extracts_fields():
    key = RK.monitor_partition("acme", "writes", "2026-06-09")
    assert RK.parse_monitor_partition_key(key) == ("acme", "writes", "2026-06-09")


def test_parse_monitor_partition_key_returns_none_for_other_keys():
    assert RK.parse_monitor_partition_key(RK.meta_root("acme", "demo")) is None
    # Drain handle is rejected (7 segments, not 6)
    assert RK.parse_monitor_partition_key(
        RK.monitor_partition_drain("acme", "writes", "2026-06-09")
    ) is None
    # Invalid monitor_type
    assert RK.parse_monitor_partition_key(
        "supertable:acme:monitor:bogus:doc:2026-06-09"
    ) is None
    # Invalid date format
    assert RK.parse_monitor_partition_key(
        "supertable:acme:monitor:writes:doc:not-a-date"
    ) is None
    assert RK.parse_monitor_partition_key("") is None
    assert RK.parse_monitor_partition_key(None) is None  # type: ignore[arg-type]


def test_monitor_partition_rejects_invalid_monitor_type():
    with pytest.raises(ValueError):
        RK.monitor_partition("acme", "garbage", "2026-06-09")


def test_monitor_partition_rejects_invalid_date():
    with pytest.raises(ValueError):
        RK.monitor_partition("acme", "writes", "not-a-date")
    with pytest.raises(ValueError):
        RK.monitor_partition("acme", "writes", "2026/06/09")
    with pytest.raises(ValueError):
        RK.monitor_partition("acme", "writes", "")


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


# ---------------------------------------------------------------------------
# The other way to build a key: append to a validated prefix
# ---------------------------------------------------------------------------

_PREFIX_APPEND = re.compile(
    r"RK\.\w*prefix\w*\s*\([^)]*\)\s*(?:\+|%)"       # RK.x_prefix(...) + ...
    r"|RK\.\w*prefix\w*\s*\([^)]*\)\s*\.\s*(?:join|format)\b"
)


def test_no_key_built_by_appending_to_a_prefix():
    """A validated prefix plus unvalidated text is still an unvalidated key.

    The f-string guard above scans for the literal ``supertable:``, so it only
    sees keys written out in full. It missed three real offenders that took a
    correctly-built prefix and appended to it:

        RK.quality_prefix(org, sup) + f"pending:{table}"
        RK.quality_prefix(org, sup) + ":".join(parts)

    The prefix is safe; the appended segment never passed ``_safe``. A table
    name containing a colon would write outside its own namespace, and one
    containing a glob character would make a later SCAN match keys it does not
    own — which is how a cleanup deletes someone else's data.

    Key construction belongs in redis_keys.py in full, not in halves.
    """
    offenders: list[tuple[str, int, str]] = []
    for path in _iter_source_files():
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if line.lstrip().startswith("#"):
                continue
            if _PREFIX_APPEND.search(line):
                offenders.append((str(path), lineno, line.strip()))

    assert not offenders, (
        "Redis keys built by appending to a prefix — the appended segment is "
        "never validated. Add a constructor in supertable/redis_keys.py that "
        "builds the whole key through _safe().\n"
        + "\n".join(f"  {p}:{ln}: {t}" for p, ln, t in offenders)
    )


def test_the_appending_guard_actually_catches_the_shape():
    """Guard the guard: a rule nobody has seen fire is not a rule."""
    for bad in (
        'return RK.quality_prefix(org, sup) + f"pending:{table}"',
        'return RK.quality_prefix(org, sup) + ":".join(parts)',
        'key = RK.audit_prefix(org) % table',
    ):
        assert _PREFIX_APPEND.search(bad), bad
    for good in (
        "return RK.quality_table_key(org, sup, 'pending', table)",
        "prefix = RK.quality_prefix(org, sup)",
        "keys = [RK.meta_leaf(org, sup, t) for t in tables]",
    ):
        assert not _PREFIX_APPEND.search(good), good


# ---------------------------------------------------------------------------
# 9. Structural properties of the namespace as a whole
#
# The tests above check one constructor at a time against a hand-written
# expected string. That catches a typo in a key someone remembered to add to
# the table, and nothing else. These check properties of the whole set, by
# reflection, so a constructor added next year is covered the day it lands.
# ---------------------------------------------------------------------------

#: Argument values by parameter NAME. Reflection can't guess what a parameter
#: means, but the module is consistent about naming, so one entry per distinct
#: parameter name covers every constructor.
_ARG_BY_NAME = {
    "org": ORG, "sup": SUP, "simple": SIMPLE, "table": SIMPLE,
    "user_id": USER_ID, "role_id": ROLE_ID, "share_id": SHARE,
    "staging_name": STAGING, "stage_name": STAGING, "pipe_name": PIPE,
    "instance_id": INSTANCE, "app_name": APP, "job_id": "job_1",
    "rule_id": "rule_1", "kind": "pending", "part": "p1",
    "monitor_type": "plans", "date": "2026-06-09",
    "link_id": LINK, "role_type": "admin",
    "service_type": "api", "host": "host1", "pid": 1234,
}


def _is_key_constructor(fn) -> bool:
    """Key constructors return a key. Predicates like ``is_sentinel`` don't.

    Keyed off the return annotation rather than a name list, so a new
    predicate doesn't have to be remembered here to avoid tripping the
    reachability guard.
    """
    import inspect

    return inspect.signature(fn).return_annotation not in ("bool", bool)


def _reflect_keys() -> dict[str, str]:
    """Call every constructor with sample args; return ``{name: key}``.

    Constructors whose parameters aren't all in ``_ARG_BY_NAME`` are skipped
    rather than guessed at — and ``test_every_constructor_is_reachable``
    below fails if that skip list ever grows, so a new parameter name can't
    quietly drop a key out of these checks.
    """
    import inspect

    out: dict[str, str] = {}
    for name, fn in sorted(vars(RK).items()):
        if not inspect.isfunction(fn) or name.startswith("_"):
            continue
        if name.startswith(("parse_", "assert_")) or not _is_key_constructor(fn):
            continue
        args, resolvable = [], True
        for p in inspect.signature(fn).parameters.values():
            if p.kind is p.VAR_KEYWORD:
                continue
            if p.kind is p.VAR_POSITIONAL:
                # *parts: pass one, since the zero-part form is an error for
                # exactly the reason test_quality_doc_refuses_to_degenerate
                # covers — it would return the namespace prefix.
                args.append("part1")
                continue
            if p.name in _ARG_BY_NAME:
                args.append(_ARG_BY_NAME[p.name])
            elif p.default is not p.empty:
                continue
            else:
                resolvable = False
                break
        if not resolvable:
            continue
        try:
            key = fn(*args)
        except Exception:
            continue
        if isinstance(key, str) and key.startswith(("supertable:", "dataisland:")):
            out[name] = key
    return out


def test_every_constructor_is_reachable_by_reflection():
    """No constructor may be invisible to the structural checks below.

    If this fails, a parameter name was introduced that ``_ARG_BY_NAME``
    doesn't know, and that key silently stopped being checked for clashes
    and correct scope placement.
    """
    import inspect

    expected = {
        n for n, f in vars(RK).items()
        if inspect.isfunction(f) and not n.startswith("_")
        and not n.startswith(("parse_", "assert_"))
        and _is_key_constructor(f)
    }
    missing = expected - set(_reflect_keys())
    assert not missing, (
        "constructors not reachable by reflection — add their parameter "
        f"names to _ARG_BY_NAME: {sorted(missing)}"
    )


def test_no_two_constructors_produce_the_same_key():
    """Distinct constructors must name distinct keys.

    Two names for one key is not a style problem: one caller's write is the
    other caller's read, and a delete through either name destroys both.
    """
    from collections import defaultdict

    by_key = defaultdict(list)
    for name, key in _reflect_keys().items():
        if "*" in key:
            continue  # patterns are meant to overlap keys; checked separately
        by_key[key].append(name)

    clashes = {k: v for k, v in by_key.items() if len(v) > 1}
    assert not clashes, "constructors producing identical keys:\n" + "\n".join(
        f"  {k}  <-  {sorted(v)}" for k, v in sorted(clashes.items())
    )


def test_quality_doc_refuses_to_degenerate_into_its_own_prefix():
    """``quality_doc(org, sup)`` with no parts once returned the bare prefix.

    Which is exactly ``quality_prefix(org, sup)`` — so an empty parts list
    addressed the namespace root instead of a document inside it. Guard the
    specific shape, since the clash test above can't call a variadic with
    zero args and still know what it should have produced.
    """
    with pytest.raises(ValueError, match="at least one path segment"):
        RK.quality_doc(ORG, SUP)
    assert RK.quality_doc(ORG, SUP, "pending", SIMPLE) != RK.quality_prefix(ORG, SUP)


def test_scope_segment_is_a_closed_set():
    """Position 2 under ``supertable:{org}:`` is always an SDK literal.

    Documented as rule 3 since v2 and never enforced. User input must not
    reach this position — a supertable named ``system`` that landed here
    would address platform state.
    """
    allowed = {"system", "lakes", "monitor", "query"}
    offenders = {
        name: key for name, key in _reflect_keys().items()
        if key.startswith("supertable:")
        and len(key.split(":")) > 2
        and key.split(":")[2] not in allowed
    }
    assert not offenders, (
        f"scope segment outside {sorted(allowed)}:\n"
        + "\n".join(f"  {n} = {k}" for n, k in sorted(offenders.items()))
    )


def test_org_level_scopes_carry_no_supertable_segment():
    """``monitor``, ``query`` and ``system`` are company-level.

    A query may span supertables and monitoring aggregates across them, so
    neither can be filed under one. If a supertable name appeared in these
    keys the data would shard per-table and every cross-table read would
    silently see a fraction of it.
    """
    offenders = []
    for name, key in _reflect_keys().items():
        parts = key.split(":")
        if len(parts) > 2 and parts[2] in ("monitor", "query", "system"):
            if SUP in parts:
                offenders.append(f"  {name} = {key}")
    assert not offenders, (
        "org-level key filed under a supertable:\n" + "\n".join(offenders)
    )


def test_lake_scoped_keys_name_their_supertable():
    """Everything under ``lakes:`` is per-supertable, one level below org.

    The exceptions are the scope root and the patterns that deliberately
    wildcard the supertable to enumerate across them; they are listed
    explicitly so a new key cannot join them by accident.
    """
    cross_lake = {
        "lakes_scope", "lakes_pattern",
        "meta_root_pattern_for_org", "meta_root_pattern_all_orgs",
    }
    offenders = []
    for name, key in _reflect_keys().items():
        parts = key.split(":")
        if len(parts) > 2 and parts[2] == "lakes" and name not in cross_lake:
            if len(parts) < 4 or parts[3] != SUP:
                offenders.append(f"  {name} = {key}")
    assert not offenders, (
        "lake-scoped key that does not name its supertable at position 3:\n"
        + "\n".join(offenders)
    )
