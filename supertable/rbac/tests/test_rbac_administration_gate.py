"""Administering roles and users requires Permission.RBAC.

Before this, ``RoleManager`` and ``UserManager`` took no actor at all, so
every mutation was unauthenticated: any code that could construct a manager
could mint a superadmin role and bind it to itself. ``Permission.RBAC``
existed in the enum and was checked nowhere.

The check lives in the mutating methods, not the constructor, for two
structural reasons — both tested below:

  * ``__init__`` bootstraps. It mints the SuperTable's ``superadmin`` role,
    so a constructor demanding RBAC could never run the first time.
  * ``check_rbac_access`` resolves the actor by *building a RoleManager*. A
    validating constructor would validate its own helper, unboundedly.

ON THE TRUST BOUNDARY

The actor is a role-name string supplied by the caller, so this gate is
exactly as strong as the host's binding of authenticated principal to role
name — the same trust model as every other gate in the library. It is not a
substitute for the host authenticating; it is what makes the host's
authentication mean something here.
"""
from __future__ import annotations

import os

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

import supertable.redis_catalog as rc_module  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.rbac.role_manager import RoleManager  # noqa: E402
from supertable.rbac.user_manager import UserManager  # noqa: E402

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
    """A manager acting as the bootstrap superadmin — the only RBAC holder."""
    return RoleManager(super_name=SUP, organization=ORG, redis_catalog=cat,
                       actor_role_name="superadmin")


@pytest.fixture()
def writer_role(admin):
    """A writer role, created legitimately, for use as a non-RBAC actor."""
    admin.create_role({"role": "writer", "role_name": "etl_bot",
                       "tables": {"*": {"columns": ["*"], "filters": ["*"]}}})
    return "etl_bot"


def _rm(cat, actor=None):
    return RoleManager(super_name=SUP, organization=ORG, redis_catalog=cat,
                       actor_role_name=actor)


def _um(cat, actor=None):
    return UserManager(super_name=SUP, organization=ORG, redis_catalog=cat,
                       actor_role_name=actor)


# --------------------------------------------------------------------------
# Bootstrap must survive the gate
# --------------------------------------------------------------------------

def test_bootstrap_works_with_no_actor(cat):
    """The chicken-and-egg: this call creates the role that authorises it."""
    manager = _rm(cat)                      # no actor at all
    assert manager.get_superadmin_role_id(), (
        "bootstrap must mint the superadmin role without an actor, or a "
        "brand-new SuperTable could never get one"
    )


def test_bootstrap_also_creates_the_superuser_with_no_actor(cat):
    _rm(cat)                                 # roles first
    users = _um(cat)                         # no actor
    assert users.get_or_create_default_user()


def test_the_gate_does_not_recurse(cat, writer_role):
    """``check_rbac_access`` builds a RoleManager; that must not re-check.

    If the constructor validated, resolving the actor would construct a
    manager that resolves its actor, without bound. A refusal arriving as a
    PermissionError rather than a RecursionError is the assertion.
    """
    with pytest.raises(PermissionError):
        _rm(cat, writer_role).create_role(
            {"role": "reader", "role_name": "x", "tables": {}},
        )


# --------------------------------------------------------------------------
# No actor: fail closed
# --------------------------------------------------------------------------

@pytest.mark.parametrize("call", [
    lambda m: m.create_role({"role": "reader", "role_name": "x", "tables": {}}),
    lambda m: m.update_role("some_id", {"role": "reader"}),
    lambda m: m.delete_role("some_id"),
])
def test_role_mutations_refuse_without_an_actor(cat, call):
    """Missing actor denies rather than waves through.

    Treating "no actor" as unrestricted would mean the gate only protected
    callers who had already opted into being checked.
    """
    with pytest.raises(PermissionError, match="no actor role was supplied"):
        call(_rm(cat))


@pytest.mark.parametrize("call", [
    lambda m: m.create_user({"username": "bob"}),
    lambda m: m.modify_user("uid", {"username": "bob2"}),
    lambda m: m.delete_user("uid"),
    lambda m: m.add_role("uid", "rid"),
    lambda m: m.remove_role("uid", "rid"),
    lambda m: m.remove_role_from_users("rid"),
])
def test_user_mutations_refuse_without_an_actor(cat, call):
    with pytest.raises(PermissionError, match="no actor role was supplied"):
        call(_um(cat))


# --------------------------------------------------------------------------
# A non-RBAC actor is denied
# --------------------------------------------------------------------------

def test_a_writer_cannot_create_a_role(cat, writer_role):
    with pytest.raises(PermissionError, match="administer roles and users"):
        _rm(cat, writer_role).create_role(
            {"role": "reader", "role_name": "sneaky", "tables": {}},
        )


def test_a_writer_cannot_update_or_delete_a_role(cat, admin, writer_role):
    victim = admin.create_role({"role": "reader", "role_name": "victim",
                                "tables": {}})
    actor = _rm(cat, writer_role)

    with pytest.raises(PermissionError):
        actor.update_role(victim, {"role": "admin"})
    with pytest.raises(PermissionError):
        actor.delete_role(victim)

    assert admin.get_role(victim)["role"] == "reader", "nothing changed"


def test_a_writer_cannot_grant_itself_the_superadmin_role(cat, admin,
                                                          writer_role):
    """The two-line takeover this gate exists to stop.

    ``get_superadmin_role_id()`` is public, so the target id was never the
    secret. Binding it was the whole attack.
    """
    users = _um(cat, "superadmin")
    alice = users.create_user({"username": "alice"})
    superadmin_id = admin.get_superadmin_role_id()

    with pytest.raises(PermissionError, match="administer roles and users"):
        _um(cat, writer_role).add_role(alice, superadmin_id)

    assert superadmin_id not in users.get_user(alice).get("roles", [])


def test_a_writer_cannot_grant_roles_via_modify_user(cat, admin, writer_role):
    """``modify_user`` accepts ``roles``, so it is a granting path too.

    Gating ``add_role`` alone would have left this one open.
    """
    users = _um(cat, "superadmin")
    alice = users.create_user({"username": "alice2"})
    superadmin_id = admin.get_superadmin_role_id()

    with pytest.raises(PermissionError):
        _um(cat, writer_role).modify_user(alice, {"roles": [superadmin_id]})

    assert users.get_user(alice).get("roles", []) == []


def test_a_writer_cannot_revoke_an_admins_role(cat, admin, writer_role):
    """Revocation is gated too — stripping admins locks out the lake."""
    users = _um(cat, "superadmin")
    bob = users.create_user({"username": "bob"})
    superadmin_id = admin.get_superadmin_role_id()
    users.add_role(bob, superadmin_id)

    with pytest.raises(PermissionError):
        _um(cat, writer_role).remove_role(bob, superadmin_id)

    assert superadmin_id in users.get_user(bob)["roles"]


@pytest.mark.parametrize("role_type", ["writer", "reader", "meta"])
def test_no_tier_below_admin_holds_rbac(cat, admin, role_type):
    admin.create_role({"role": role_type, "role_name": f"a_{role_type}",
                       "tables": {"*": {"columns": ["*"], "filters": ["*"]}}})
    with pytest.raises(PermissionError, match="administer roles and users"):
        _rm(cat, f"a_{role_type}").create_role(
            {"role": "reader", "role_name": f"z_{role_type}", "tables": {}},
        )


# --------------------------------------------------------------------------
# Both admin tiers are allowed
# --------------------------------------------------------------------------

def test_superadmin_may_administer(cat, admin):
    assert admin.create_role({"role": "reader", "role_name": "ok",
                              "tables": {}})


def test_admin_may_administer_too(cat, admin):
    """ADMIN and SUPERADMIN hold identical permissions by decision."""
    admin.create_role({"role": "admin", "role_name": "ops",
                       "tables": {"*": {"columns": ["*"], "filters": ["*"]}}})

    as_admin = _rm(cat, "ops")
    role_id = as_admin.create_role({"role": "reader", "role_name": "made_by_admin",
                                    "tables": {}})
    as_admin.update_role(role_id, {"role": "writer"})
    assert as_admin.delete_role(role_id)


def test_an_unknown_actor_is_refused(cat):
    """A nonexistent role name must not pass for a privileged one."""
    with pytest.raises(PermissionError):
        _rm(cat, "no_such_role").create_role(
            {"role": "reader", "role_name": "x", "tables": {}},
        )


# --------------------------------------------------------------------------
# Reads stay open
# --------------------------------------------------------------------------

def test_reads_need_no_actor(cat, admin):
    """access_control builds throwaway managers to resolve roles.

    If reading required an actor, every permission check in the library
    would need one to perform a permission check.
    """
    admin.create_role({"role": "reader", "role_name": "readable",
                       "tables": {}})
    reader = _rm(cat)                        # no actor

    assert reader.get_superadmin_role_id()
    assert reader.get_role_by_name("readable")["role"] == "reader"
    assert reader.list_roles()
    assert reader.get_roles_by_type("reader")


def test_user_reads_need_no_actor(cat, admin):
    users = _um(cat, "superadmin")
    users.create_user({"username": "carol"})

    plain = _um(cat)                         # no actor
    assert plain.get_user_by_name("carol")["username"] == "carol"
    assert plain.list_users()
