"""The superadmin role is created by init, and cannot be created or dropped.

There is exactly one superadmin role per SuperTable, minted by
``RoleManager._init_role_storage`` at bootstrap. A tenant may not create
another, may not promote an existing role into one, may not demote the real
one, and may not delete it.

The name was already reserved. The *type* was not — and the type is the half
that matters, because ``access_control`` reads ``role_info["role"]`` and, for
``superadmin``, returns an empty view set: no row filters, no column masks.
So a role stored as ``{"role": "superadmin", "role_name": "anything"}`` had
its own ``tables`` restriction discarded at read time (S11).

The demote-then-delete chain is the subtle one and has its own test: guarding
``delete_role`` alone was bypassable by first changing the type.
"""
from __future__ import annotations

import os

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

import supertable.redis_catalog as rc_module  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.rbac.role_manager import (  # noqa: E402
    RESERVED_ROLE_TYPE,
    RoleManager,
)

ORG = "test_org"
SUP = "test_super"


@pytest.fixture()
def rm():
    """A RoleManager on a fresh FakeRedis, bootstrapped as in production."""
    fake = FakeRedis(decode_responses=True)
    rc_module.RedisConnector = type(
        "RC", (), {"__init__": lambda self, o=None: setattr(self, "r", fake)},
    )
    catalog = RedisCatalog()
    manager = RoleManager(super_name=SUP, organization=ORG,
                          redis_catalog=catalog, actor_role_name="superadmin")
    return manager


def _reader(**over):
    data = {"role": "reader", "tables": {"orders": {"columns": ["id"]}}}
    data.update(over)
    return data


# --------------------------------------------------------------------------
# It exists after init, exactly once
# --------------------------------------------------------------------------

def test_init_creates_exactly_one_superadmin(rm):
    role_id = rm.get_superadmin_role_id()
    assert role_id, "bootstrap must create the superadmin role"
    assert len(rm.get_roles_by_type(RESERVED_ROLE_TYPE)) == 1


def test_the_bootstrap_superadmin_has_wildcard_grants(rm):
    doc = rm.get_role(rm.get_superadmin_role_id())
    assert doc["role"] == RESERVED_ROLE_TYPE
    assert "*" in doc["tables"]


# --------------------------------------------------------------------------
# It cannot be created
# --------------------------------------------------------------------------

def test_cannot_create_a_role_of_superadmin_type(rm):
    """The S11 repro: an innocuous NAME with the reserved TYPE."""
    with pytest.raises(ValueError, match="reserved by SuperTable"):
        rm.create_role({
            "role": "superadmin",
            "role_name": "quarterly_report_viewer",
            "tables": {"orders": {"columns": ["order_id"]}},
        })


@pytest.mark.parametrize("spelling", [
    "superadmin", "SuperAdmin", "SUPERADMIN", "  superadmin  ",
])
def test_reserved_type_check_is_case_and_space_insensitive(rm, spelling):
    with pytest.raises(ValueError, match="reserved by SuperTable"):
        rm.create_role({"role": spelling, "role_name": "x", "tables": {}})


def test_cannot_create_a_role_named_superadmin(rm):
    """The pre-existing half of the reservation, still enforced."""
    with pytest.raises(ValueError, match="reserved by SuperTable"):
        rm.create_role(_reader(role_name="superadmin"))


def test_recreating_the_identical_bootstrap_role_is_idempotent(rm):
    """``allow_reserved`` with the same name and content returns the same id.

    This is the retry path, not a second role: ``create_role`` sees the name
    collision, finds an identical ``content_hash``, and hands back the
    existing id without reaching the catalog.

    An earlier version of this test asserted only ``assert role_id`` and
    claimed to be testing "the bootstrap door". It passed because of the
    idempotent-return above, so it never exercised creating a *second*
    superadmin at all — which is the case that mattered. See the next test.
    """
    existing = rm.get_superadmin_role_id()
    role_id = rm.create_role(
        {"role": "superadmin", "role_name": "superadmin",
         "tables": {"*": {"columns": ["*"], "filters": ["*"]}}},
        allow_reserved=True,
    )
    assert role_id == existing, "must return the bootstrap role, not a new one"
    assert len(rm.get_roles_by_type(RESERVED_ROLE_TYPE)) == 1


def test_allow_reserved_cannot_plant_a_second_superadmin(rm):
    """``allow_reserved`` is a *public* parameter, so it is not a door.

    Its docstring asked tenant callers not to set it, which is a comment
    rather than a check. Passing it with a fresh name minted a second
    superadmin role — and the immutability rules then worked in the
    attacker's favour: that role could never be deleted or demoted by
    anyone, including the real superadmin.

    "Exactly one per SuperTable" is now an invariant of the write path, so
    bootstrap still works (none exists yet) and everything after is refused.
    """
    with pytest.raises(ValueError, match="already exists"):
        rm.create_role(
            {"role": "superadmin", "role_name": "shadow_admin",
             "tables": {"*": {"columns": ["*"], "filters": ["*"]}}},
            allow_reserved=True,
        )

    assert len(rm.get_roles_by_type(RESERVED_ROLE_TYPE)) == 1


def test_ordinary_roles_are_unaffected(rm):
    """The guard must not make the normal path harder."""
    for role_type in ("admin", "writer", "reader", "meta"):
        assert rm.create_role(_reader(role=role_type,
                                      role_name=f"r_{role_type}"))


# --------------------------------------------------------------------------
# It cannot be promoted to
# --------------------------------------------------------------------------

def test_cannot_promote_an_existing_role_to_superadmin(rm):
    role_id = rm.create_role(_reader(role_name="analyst"))
    with pytest.raises(ValueError, match="reserved by SuperTable"):
        rm.update_role(role_id, {"role": "superadmin"})


def test_a_refused_promotion_leaves_the_role_untouched(rm):
    role_id = rm.create_role(_reader(role_name="analyst"))
    before = rm.get_role(role_id)

    with pytest.raises(ValueError):
        rm.update_role(role_id, {"role": "superadmin"})

    assert rm.get_role(role_id)["role"] == before["role"] == "reader"


def test_a_type_change_is_reflected_in_the_type_listing(rm):
    """Promotion used to be invisible as well as effective.

    ``rbac_update_role`` rewrote the document without moving the role between
    the per-type index sets, so a promoted role stayed filed under its old
    type: fully effective at enforcement, absent from the only listing that
    answers "who holds this type". Both halves are now consistent, shown here
    through the legitimate path — a change between two ordinary types.

    Note ``get_roles_by_type`` returns role *documents*, not ids. An earlier
    version of this test asserted ``role_id not in get_roles_by_type(...)``,
    comparing a string against a list of dicts — vacuously true, and so a
    test that could not fail either way.
    """
    role_id = rm.create_role(_reader(role_name="analyst"))
    assert [r["role_id"] for r in rm.get_roles_by_type("reader")] == [role_id]

    rm.update_role(role_id, {"role": "writer"})

    assert rm.get_role(role_id)["role"] == "writer"
    assert [r["role_id"] for r in rm.get_roles_by_type("writer")] == [role_id]
    assert [r["role_id"] for r in rm.get_roles_by_type("reader")] == []


# --------------------------------------------------------------------------
# It cannot be dropped — directly or by demote-then-delete
# --------------------------------------------------------------------------

def test_cannot_delete_the_superadmin_role(rm):
    with pytest.raises(ValueError, match="cannot be deleted"):
        rm.delete_role(rm.get_superadmin_role_id())


def test_cannot_demote_the_superadmin_role(rm):
    with pytest.raises(ValueError, match="type cannot be changed"):
        rm.update_role(rm.get_superadmin_role_id(), {"role": "reader"})


def test_the_demote_then_delete_chain_is_closed(rm):
    """The two-step bypass of "superadmin cannot be deleted".

    ``delete_role`` refuses by reading the document's type. Change the type
    first and the check stops matching, so guarding the delete alone left the
    role removable in two calls — and with it, the only role able to restore
    access if every other role were misconfigured.
    """
    role_id = rm.get_superadmin_role_id()

    with pytest.raises(ValueError, match="type cannot be changed"):
        rm.update_role(role_id, {"role": "writer"})

    # Step two must be unreachable, and is — the type never changed.
    assert rm.get_role(role_id)["role"] == RESERVED_ROLE_TYPE
    with pytest.raises(ValueError, match="cannot be deleted"):
        rm.delete_role(role_id)

    assert rm.get_superadmin_role_id() == role_id, "it must still be there"


def test_the_superadmin_role_survives_a_grant_update(rm):
    """Immutable type, not an immutable document.

    Its ``tables`` can still be rewritten — the guard is on the type alone,
    so legitimate administration of the role is unaffected.
    """
    role_id = rm.get_superadmin_role_id()
    rm.update_role(role_id, {"tables": {"*": {"columns": ["*"],
                                              "filters": ["*"]}}})
    assert rm.get_role(role_id)["role"] == RESERVED_ROLE_TYPE


def test_setting_the_type_to_its_current_value_is_not_a_change(rm):
    """A no-op rewrite must not trip the immutability guard.

    A caller that PUTs the whole document back — a common REST pattern —
    sends ``role: "superadmin"`` unchanged, and rejecting that would make the
    role unadministrable rather than merely immutable.
    """
    role_id = rm.get_superadmin_role_id()
    rm.update_role(role_id, {"role": RESERVED_ROLE_TYPE,
                             "tables": {"*": {"columns": ["*"]}}})
    assert rm.get_role(role_id)["role"] == RESERVED_ROLE_TYPE
