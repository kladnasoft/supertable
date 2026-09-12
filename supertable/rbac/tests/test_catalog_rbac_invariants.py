"""Superadmin invariants hold on the catalog write path, not just the manager.

``RedisCatalog`` is a documented public class (docs/15_python_sdk.md lists it
under "all public classes can be imported directly from the top-level
package"), and every RBAC write on it is reachable without going through
``RoleManager``. So a rule enforced only in the manager was one import wide:

    RedisCatalog().rbac_update_role(org, sup, my_reader_id,
                                    {"role": "superadmin"})

promoted a narrowly-granted reader to unrestricted reads of every table *and*
the ability to administer roles and users — the exact transition the manager
exists to forbid.

Authorization cannot live at this layer: the catalog has no actor and no way
to obtain one. What lives here are the invariants that hold regardless of who
is asking — there is exactly one superadmin role per SuperTable, and it cannot
be created twice, promoted to, demoted, renamed, disabled, or deleted.

Each test below corresponds to a bypass that was verified exploitable before
these guards existed.
"""
from __future__ import annotations

import os

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

import supertable.redis_catalog as rc_module  # noqa: E402
from supertable.redis_catalog import (  # noqa: E402
    RESERVED_ROLE_TYPE,
    RedisCatalog,
    _as_bool,
)
from supertable.rbac.access_control import (  # noqa: E402
    check_rbac_access,
    check_write_access,
)
from supertable.rbac.role_manager import RoleManager  # noqa: E402

ORG = "test_org"
SUP = "test_super"


@pytest.fixture()
def cat():
    fake = FakeRedis(decode_responses=True)
    rc_module.RedisConnector = type(
        "RC", (), {"__init__": lambda self, o=None: setattr(self, "r", fake)},
    )
    return RedisCatalog()


@pytest.fixture()
def admin(cat):
    return RoleManager(super_name=SUP, organization=ORG, redis_catalog=cat,
                       actor_role_name="superadmin")


@pytest.fixture()
def reader_id(admin):
    return admin.create_role({"role": "reader", "role_name": "analyst",
                              "tables": {"orders": {"columns": ["id"]}}})


# --------------------------------------------------------------------------
# Promotion — the headline bypass
# --------------------------------------------------------------------------

def test_the_catalog_refuses_to_promote_a_role_to_superadmin(cat, reader_id):
    with pytest.raises(ValueError, match="reserved"):
        cat.rbac_update_role(ORG, SUP, reader_id, {"role": "superadmin"})

    assert cat.get_role_details(ORG, SUP, reader_id)["role"] == "reader"


def test_a_refused_promotion_grants_nothing(cat, admin, reader_id):
    """The consequence, asserted at the enforcement layer.

    Before the guard this exact sequence gave the reader unrestricted reads
    and the RBAC permission. Checking the gates directly proves the refusal
    is what matters, not merely that a ValueError was raised.
    """
    with pytest.raises(ValueError):
        cat.rbac_update_role(ORG, SUP, reader_id, {"role": "superadmin"})

    with pytest.raises(PermissionError):
        check_rbac_access(super_name=SUP, organization=ORG,
                          role_name="analyst")
    with pytest.raises(PermissionError):
        check_write_access(super_name=SUP, organization=ORG,
                           role_name="analyst", table_name="orders")


def test_promotion_is_refused_however_it_is_spelled(cat, reader_id):
    for spelling in ("superadmin", "SuperAdmin", "SUPERADMIN", " superadmin "):
        with pytest.raises(ValueError):
            cat.rbac_update_role(ORG, SUP, reader_id, {"role": spelling})


# --------------------------------------------------------------------------
# Demotion, rename, disable, delete
# --------------------------------------------------------------------------

def test_the_catalog_refuses_to_demote_the_superadmin(cat, admin):
    sa_id = admin.get_superadmin_role_id()
    with pytest.raises(ValueError, match="cannot be changed"):
        cat.rbac_update_role(ORG, SUP, sa_id, {"role": "reader"})

    assert cat.get_role_details(ORG, SUP, sa_id)["role"] == RESERVED_ROLE_TYPE


def test_the_catalog_refuses_to_delete_the_superadmin(cat, admin):
    sa_id = admin.get_superadmin_role_id()
    with pytest.raises(ValueError, match="cannot be deleted"):
        cat.rbac_delete_role(ORG, SUP, sa_id)

    assert admin.get_superadmin_role_id() == sa_id


def test_the_superadmin_cannot_be_renamed(cat, admin):
    """Renaming it orphans every caller that addresses it by name.

    The docs, the demo scripts and the package docstring all pass
    ``role_name="superadmin"``. After a rename, ``get_role_by_name`` returns
    nothing, bootstrap will not re-mint (the renamed role still holds the
    type), and no replacement can be created because the name is reserved.
    """
    sa_id = admin.get_superadmin_role_id()

    with pytest.raises(ValueError, match="cannot be renamed"):
        cat.rbac_update_role(ORG, SUP, sa_id, {"role_name": "renamed_sa"})

    assert admin.get_role_by_name("superadmin").get("role_id") == sa_id


def test_renaming_it_to_its_own_name_is_not_a_rename(cat, admin):
    """A whole-document PUT resends the unchanged name; that must not trip."""
    sa_id = admin.get_superadmin_role_id()
    cat.rbac_update_role(ORG, SUP, sa_id, {"role_name": "superadmin"})
    assert admin.get_role_by_name("superadmin").get("role_id") == sa_id


@pytest.mark.parametrize("falsey", ["false", "False", "0", False, ""])
def test_the_superadmin_cannot_be_disabled(cat, admin, falsey):
    """Disabling it was a strictly better delete, and unrecoverable.

    ``_resolve_role`` denies a disabled role *everywhere* — including inside
    the RBAC check that would be needed to re-enable it. So one ungated
    ``{"enabled": "false"}`` bricked the lake with no path back through any
    gated API, while the class already refused the equivalent delete.
    """
    sa_id = admin.get_superadmin_role_id()

    with pytest.raises(ValueError, match="cannot be disabled"):
        cat.rbac_update_role(ORG, SUP, sa_id, {"enabled": falsey})

    # Still usable: the gate it protects must still pass.
    check_rbac_access(super_name=SUP, organization=ORG,
                      role_name="superadmin")


def test_enabling_it_is_still_allowed(cat, admin):
    sa_id = admin.get_superadmin_role_id()
    cat.rbac_update_role(ORG, SUP, sa_id, {"enabled": True})
    check_rbac_access(super_name=SUP, organization=ORG,
                      role_name="superadmin")


def test_an_ordinary_role_can_still_be_disabled(cat, reader_id):
    """The guard is specific to superadmin, not a blanket freeze."""
    cat.rbac_update_role(ORG, SUP, reader_id, {"enabled": "false"})
    with pytest.raises(PermissionError, match="disabled"):
        check_write_access(super_name=SUP, organization=ORG,
                           role_name="analyst", table_name="orders")


# --------------------------------------------------------------------------
# The bool spelling must agree with enforcement
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("false", False), ("FALSE", False), ("0", False), ("", False),
    (False, False), (None, True), ("true", True), ("1", True), (True, True),
    ("anything", True),
])
def test_as_bool_agrees_with_resolve_role(value, expected):
    """A value this reads True but enforcement reads False is a hole.

    ``_resolve_role`` treats ``"false"``, ``"0"`` and bool ``False`` as
    disabled and a missing flag as enabled. If ``_as_bool`` disagreed on any
    spelling, the superadmin role could be disabled through the gap.
    """
    assert _as_bool(value) is expected


# --------------------------------------------------------------------------
# Exactly one
# --------------------------------------------------------------------------

def test_the_catalog_refuses_a_second_superadmin(cat, admin):
    with pytest.raises(ValueError, match="already exists"):
        cat.rbac_create_role(ORG, SUP, "deadbeef" * 4, {
            "role": "superadmin", "role_name": "shadow",
            "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        })

    assert len(admin.get_roles_by_type(RESERVED_ROLE_TYPE)) == 1


def test_bootstrap_still_works_on_a_fresh_lake(cat):
    """The invariant must not close the door it exists to leave open."""
    fresh = RoleManager(super_name="brand_new_lake", organization=ORG,
                        redis_catalog=cat)
    assert fresh.get_superadmin_role_id()
    assert len(fresh.get_roles_by_type(RESERVED_ROLE_TYPE)) == 1


def test_rewriting_the_existing_superadmin_in_place_is_allowed(cat, admin):
    """Same role_id, still superadmin — a repair, not a second role."""
    sa_id = admin.get_superadmin_role_id()
    cat.rbac_create_role(ORG, SUP, sa_id, {
        "role": "superadmin", "role_name": "superadmin",
        "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
    })
    assert admin.get_superadmin_role_id() == sa_id


# --------------------------------------------------------------------------
# Ordinary operations stay unaffected
# --------------------------------------------------------------------------

def test_ordinary_type_changes_still_work(cat, reader_id):
    cat.rbac_update_role(ORG, SUP, reader_id, {"role": "writer"})
    assert cat.get_role_details(ORG, SUP, reader_id)["role"] == "writer"


def test_ordinary_renames_still_work(cat, reader_id):
    cat.rbac_update_role(ORG, SUP, reader_id, {"role_name": "analyst_v2"})
    assert cat.get_role_details(ORG, SUP, reader_id)["role_name"] == "analyst_v2"


def test_ordinary_deletes_still_work(cat, reader_id):
    assert cat.rbac_delete_role(ORG, SUP, reader_id) is True


def test_creating_ordinary_roles_still_works(cat, admin):
    for role_type in ("admin", "writer", "reader", "meta"):
        assert admin.create_role({"role": role_type,
                                  "role_name": f"r_{role_type}",
                                  "tables": {}})
