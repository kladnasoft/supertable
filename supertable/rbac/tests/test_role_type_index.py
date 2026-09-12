"""The per-type role index must agree with the role documents.

``rbac_create_role`` adds a role to ``rbac:roles:type:doc:{type}`` and
``rbac_delete_role`` removes it, but ``rbac_update_role`` used to only rewrite
the document. A role whose type changed therefore stayed listed under its old
type and never appeared under its new one, so
``rbac_get_role_ids_by_type`` answered from an index that no longer described
the documents — wrong in both directions at once.

The invariant asserted throughout: for every role, the type index containing
it is the one named by its own ``role`` field, and no other.
"""
from __future__ import annotations

import os

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

import supertable.redis_catalog as rc_module  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.rbac.permissions import RoleType  # noqa: E402
from supertable.rbac.role_manager import RoleManager  # noqa: E402

ORG = "test_org"
SUP = "test_super"
ALL_TYPES = [rt.value for rt in RoleType]


@pytest.fixture()
def cat():
    fake = FakeRedis(decode_responses=True)
    rc_module.RedisConnector = type(
        "RC", (), {"__init__": lambda self, o=None: setattr(self, "r", fake)},
    )
    return RedisCatalog()


@pytest.fixture()
def rm(cat):
    return RoleManager(super_name=SUP, organization=ORG, redis_catalog=cat)


def _index_membership(cat, role_id):
    """Every type whose index set currently contains *role_id*."""
    return sorted(
        t for t in ALL_TYPES
        if role_id in cat.rbac_get_role_ids_by_type(ORG, SUP, t)
    )


def assert_index_agrees(cat, role_id):
    """The one invariant: indexed under exactly the type it claims."""
    doc = cat.get_role_details(ORG, SUP, role_id) or {}
    actual = _index_membership(cat, role_id)
    assert actual == [doc.get("role")], (
        f"role {role_id} has role={doc.get('role')!r} but is indexed "
        f"under {actual}"
    )


# --------------------------------------------------------------------------
# The bug
# --------------------------------------------------------------------------

def test_a_type_change_moves_the_role_between_index_sets(rm, cat):
    role_id = rm.create_role({"role": "reader", "role_name": "analyst",
                              "tables": {"orders": {"columns": ["id"]}}})
    assert _index_membership(cat, role_id) == ["reader"]

    rm.update_role(role_id, {"role": "writer"})

    assert _index_membership(cat, role_id) == ["writer"], (
        "the role must leave 'reader' and join 'writer'"
    )
    assert_index_agrees(cat, role_id)


def test_the_old_type_no_longer_lists_the_role(rm, cat):
    """The half that made the index actively misleading, not just incomplete."""
    role_id = rm.create_role({"role": "reader", "role_name": "analyst",
                              "tables": {}})
    rm.update_role(role_id, {"role": "meta"})

    assert role_id not in cat.rbac_get_role_ids_by_type(ORG, SUP, "reader")


@pytest.mark.parametrize("start", ALL_TYPES)
@pytest.mark.parametrize("end", ALL_TYPES)
def test_every_type_transition_keeps_the_index_honest(rm, cat, start, end):
    """All 25 ordered pairs, including the no-op diagonal.

    ``superadmin`` is immutable in both directions, so transitions touching
    it are expected to be refused — and the invariant must survive a refusal
    just as it survives a success.
    """
    allow = start == "superadmin"          # bootstrap door
    try:
        role_id = rm.create_role(
            {"role": start, "role_name": f"r_{start}_{end}", "tables": {}},
            allow_reserved=allow,
        )
    except ValueError:
        pytest.skip(f"{start} cannot be created by a tenant")

    assert_index_agrees(cat, role_id)

    try:
        rm.update_role(role_id, {"role": end})
    except ValueError:
        # Refused (superadmin in either direction) — nothing may have moved.
        assert_index_agrees(cat, role_id)
        return

    assert _index_membership(cat, role_id) == [end]
    assert_index_agrees(cat, role_id)


def test_repeated_transitions_leave_no_residue(rm, cat):
    """Walk a role through several types; it must never be in two sets."""
    role_id = rm.create_role({"role": "meta", "role_name": "walker",
                              "tables": {}})
    for role_type in ("reader", "writer", "admin", "writer", "meta", "meta"):
        rm.update_role(role_id, {"role": role_type})
        assert _index_membership(cat, role_id) == [role_type]


# --------------------------------------------------------------------------
# Updates that do not touch the type
# --------------------------------------------------------------------------

def test_a_grants_only_update_leaves_the_index_alone(rm, cat):
    role_id = rm.create_role({"role": "reader", "role_name": "analyst",
                              "tables": {"orders": {"columns": ["id"]}}})
    rm.update_role(role_id, {"tables": {"orders": {"columns": ["id", "amt"]}}})

    assert _index_membership(cat, role_id) == ["reader"]
    assert cat.get_role_details(ORG, SUP, role_id)["tables"]


def test_a_rename_leaves_the_index_alone(rm, cat):
    role_id = rm.create_role({"role": "writer", "role_name": "old_name",
                              "tables": {}})
    rm.update_role(role_id, {"role_name": "new_name"})

    assert _index_membership(cat, role_id) == ["writer"]
    assert rm.get_role_by_name("new_name")["role_id"] == role_id


def test_setting_the_type_to_its_current_value_is_a_no_op(rm, cat):
    role_id = rm.create_role({"role": "writer", "role_name": "same",
                              "tables": {}})
    rm.update_role(role_id, {"role": "writer"})

    assert _index_membership(cat, role_id) == ["writer"]


# --------------------------------------------------------------------------
# The catalog layer directly
# --------------------------------------------------------------------------

def test_unknown_role_type_is_refused_before_it_reaches_a_key(cat, rm):
    """The type becomes part of a key name assembled inside Lua.

    ``_safe()`` cannot run there, so the catalog restricts the value to known
    RoleType members first. A colon would otherwise add a key segment.
    """
    role_id = rm.create_role({"role": "reader", "role_name": "x",
                              "tables": {}})
    for bad in ("root", "superadmin:extra", "", " ", "READER "):
        if bad.strip() in {rt.value for rt in RoleType}:
            continue
        with pytest.raises(ValueError, match="Unknown role type"):
            cat.rbac_update_role(ORG, SUP, role_id, {"role": bad})

    assert _index_membership(cat, role_id) == ["reader"], "nothing moved"


def test_a_refused_type_leaves_the_document_unchanged(cat, rm):
    """The validation happens before the script runs, so nothing is written."""
    role_id = rm.create_role({"role": "reader", "role_name": "x",
                              "tables": {}})
    before = dict(cat.get_role_details(ORG, SUP, role_id))

    with pytest.raises(ValueError, match="Unknown role type"):
        cat.rbac_update_role(ORG, SUP, role_id,
                             {"role": "root", "role_name": "hijacked"})

    after = cat.get_role_details(ORG, SUP, role_id)
    assert after["role"] == before["role"]
    assert after.get("role_name") == before.get("role_name")


def test_updating_a_missing_role_raises(cat):
    with pytest.raises(ValueError, match="does not exist"):
        cat.rbac_update_role(ORG, SUP, "no_such_role", {"role": "reader"})


def test_the_index_prefix_matches_the_full_key_constructor():
    """The Lua script builds a key the constructor must also produce."""
    prefix = RK.rbac_role_type_index_prefix(ORG, SUP)
    for role_type in ALL_TYPES:
        assert prefix + role_type == RK.rbac_role_type_index(
            ORG, SUP, role_type,
        )


def test_delete_removes_the_role_from_its_current_type_index(rm, cat):
    """Delete reads the live type, so it must still be right after a move."""
    role_id = rm.create_role({"role": "reader", "role_name": "doomed",
                              "tables": {}})
    rm.update_role(role_id, {"role": "writer"})
    rm.delete_role(role_id)

    assert _index_membership(cat, role_id) == [], (
        "a moved-then-deleted role must not linger in any type index"
    )


def test_the_document_and_index_both_advance_the_meta_version(rm, cat):
    """The script bumps the meta version itself; it must not regress."""
    role_id = rm.create_role({"role": "reader", "role_name": "v",
                              "tables": {}})
    before = int(cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version") or 0)
    rm.update_role(role_id, {"role": "writer"})
    after = int(cat.r.hget(RK.rbac_role_meta(ORG, SUP), "version") or 0)
    assert after > before
