"""
An access check is made on one read of Redis, or on the five it always made.

``check_write_access`` asks ``RoleManager.read_for_access_check`` for the
lake's root and the role in one round trip, and goes the old way, the guard's
own read of the root and then a constructed ``RoleManager``, whenever that is
not to be had: a ``RoleManager`` or a catalog that does not know the read
(every test double), an answer that is not the root's text and a role
document, or a failure of the call.

The collaborators here are doubles that record what they are asked, so that
which way was taken, in which order, and what each answer leads to can be read
off exactly.  The real ones are on the other side of this:
``test_an_access_check_reads_redis_once.py``.
"""
from __future__ import annotations

import logging
import os
from unittest.mock import MagicMock, patch

import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.rbac import access_control  # noqa: E402
from supertable.rbac.access_control import check_write_access  # noqa: E402

ORG, LAKE = "acme", "sales"
LOGGER = access_control.logger.name
EVERYTHING = {"*": {"columns": ["*"], "filters": ["*"]}}
WRITER = {"role": "writer", "role_name": "loader", "role_id": "a" * 32, "tables": EVERYTHING}
A_ROOT = '{"version": 3, "ts": 1794000000000}'

ONE_READ = ["RedisCatalog()", "read_for_access_check"]
FIVE_READS = ["RedisCatalog()", "get_root", "RoleManager()", "get_role_by_name"]


class _Doubles:
    """A catalog class and a manager class that record, and answer as told.

    ``read`` is what ``RoleManager.read_for_access_check`` answers ("absent":
    the class has no such method); ``root`` what ``get_root`` returns and
    ``role`` what ``get_role_by_name`` returns, the old way.  An exception is
    raised instead of returned.  ``catalogs`` are what building a catalog does,
    one after another: an exception to raise, anything else to build one.
    """

    def __init__(self, read="absent", root=None, role=None, catalogs=()):
        calls = self.calls = []
        asked = self.asked = []
        to_build = list(catalogs)

        def answer(value):
            if isinstance(value, BaseException):
                raise value
            return value

        class Catalog:
            def __init__(self):
                calls.append("RedisCatalog()")
                if to_build:
                    answer(to_build.pop(0))

            def get_root(self, organization, super_name):
                calls.append("get_root")
                asked.append(("get_root", organization, super_name))
                return answer(root)

        class Manager:
            def __init__(self, super_name, organization):
                calls.append("RoleManager()")
                asked.append(("RoleManager()", super_name, organization))

            def get_role_by_name(self, role_name):
                calls.append("get_role_by_name")
                asked.append(("get_role_by_name", role_name))
                return answer(role)

        if not (isinstance(read, str) and read == "absent"):
            def read_for_access_check(super_name, organization, role_name, redis_catalog=None):
                calls.append("read_for_access_check")
                asked.append(("read_for_access_check", super_name, organization, role_name,
                              type(redis_catalog)))
                return answer(read)

            Manager.read_for_access_check = staticmethod(read_for_access_check)

        self.Catalog, self.Manager = Catalog, Manager

    def check(self, role_name="loader", table="orders"):
        with patch.object(access_control, "RedisCatalog", self.Catalog), \
                patch.object(access_control, "RoleManager", self.Manager):
            try:
                return ("returned", check_write_access(LAKE, ORG, role_name, table))
            except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
                return ("raised", type(error).__name__, str(error))


def _refused(message):
    return ("raised", "PermissionError", message)


NO_PERMISSION = _refused("You don't have permission to write to this table.")


# ---------------------------------------------------------------------------
# One read
# ---------------------------------------------------------------------------

def test_the_one_read_is_all_that_is_asked_when_it_answers():
    doubles = _Doubles(read=(A_ROOT, dict(WRITER)))

    assert doubles.check() == ("returned", None)

    assert doubles.calls == ONE_READ
    assert doubles.asked == [("read_for_access_check", LAKE, ORG, "loader", doubles.Catalog)]


def test_the_catalog_handed_to_the_read_is_one_the_check_builds_itself():
    """The guard's collaborator: whoever puts a catalog in its place in the
    access check decides what the one read is asked of."""
    doubles = _Doubles(read=(None, dict(WRITER)))

    doubles.check()

    assert doubles.calls[0] == "RedisCatalog()"
    assert doubles.asked[0][4] is doubles.Catalog


@pytest.mark.parametrize("root", [None, "", A_ROOT, "null", "false", "0", "[]", "{}",
                                  '{"read_only": false}', '{"read_only": 0}', '{"read_only": null}',
                                  '{"read_only": "", "clone_type": "replica"}'])
def test_a_root_that_is_not_read_only_lets_the_role_decide(root):
    doubles = _Doubles(read=(root, dict(WRITER)))

    assert doubles.check() == ("returned", None)
    assert doubles.calls == ONE_READ


@pytest.mark.parametrize("root,reason", [
    ('{"read_only": true}', "locked"),
    ('{"read_only": true, "clone_type": "replica"}', "a live replica"),
    ('{"read_only": true, "clone_type": "replica", "cloned_from": "source"}', "a live replica"),
    ('{"read_only": true, "clone_type": "readonly"}', "a read-only snapshot clone"),
    ('{"read_only": true, "clone_type": "readonly", "cloned_from": "source"}', "a read-only snapshot clone"),
    ('{"read_only": true, "cloned_from": "source"}', "a read-only clone"),
    ('{"read_only": true, "clone_type": "something else", "cloned_from": "source"}', "a read-only clone"),
    ('{"read_only": true, "clone_type": "something else"}', "locked"),
    ('{"read_only": true, "cloned_from": ""}', "locked"),
    ('{"read_only": 1}', "locked"),
    ('{"read_only": "no"}', "locked"),
])
def test_a_read_only_root_refuses_before_the_role_is_looked_at(root, reason):
    """Whatever the role: a role that may not write is not what is reported."""
    for role in (dict(WRITER), {}, {"role": "reader", "tables": EVERYTHING}):
        doubles = _Doubles(read=(root, role))

        assert doubles.check() == _refused(f"This SuperTable is {reason}. Cannot write to this table.")
        assert doubles.calls == ONE_READ, "and the five reads do not follow a refusal"


@pytest.mark.parametrize("root", ["{not json", "   ", '{"read_only": tr', "[" * 100_000, "\x00"])
def test_a_root_that_cannot_be_read_as_a_document_refuses(caplog, root):
    doubles = _Doubles(read=(root, dict(WRITER)))

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        outcome = doubles.check()

    assert outcome == _refused(
        "Cannot verify whether this SuperTable is read-only. Cannot write to this table.")
    assert doubles.calls == ONE_READ
    logged = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]
    assert len(logged) == 1 and logged[0][0] == "ERROR"
    assert logged[0][1].startswith("[readonly-guard] cannot determine read-only state of acme/sales: ")
    assert logged[0][1].endswith("refusing rather than assuming writable")


@pytest.mark.parametrize("root", ["[1]", '"text"', "7", "true", "1.5"])
def test_a_root_that_is_no_object_fails_as_the_guard_fails_on_it(root):
    doubles = _Doubles(read=(root, dict(WRITER)))

    assert doubles.check()[:2] == ("raised", "AttributeError")
    assert doubles.calls == ONE_READ


# ---------------------------------------------------------------------------
# The role read with the root is judged as a role read by a manager
# ---------------------------------------------------------------------------

ROLES = {
    "a writer of everything": (dict(WRITER), ("returned", None)),
    "a writer of this table": ({"role": "writer", "tables": {"orders": {"columns": ["id"]}}},
                               ("returned", None)),
    "a writer of another table": (
        {"role": "writer", "tables": {"items": {}, "other": {"columns": ["*"]}}},
        _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer whose entry for the table is empty": (
        {"role": "writer", "tables": {"orders": {}}},
        _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer whose entry is empty, with a default": (
        {"role": "writer", "tables": {"orders": {}, "*": {"columns": ["*"]}}}, ("returned", None)),
    "a writer of an old list of tables": ({"role": "writer", "tables": ["orders", "items"]},
                                          ("returned", None)),
    "a writer of an old list without the table": (
        {"role": "writer", "tables": ["items"]},
        _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer of an old list of everything": ({"role": "writer", "tables": ["*"]}, ("returned", None)),
    "a writer of nothing": ({"role": "writer", "tables": {}},
                            _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer without tables": ({"role": "writer"},
                                _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer whose tables are text": (
        {"role": "writer", "tables": "orders"},
        _refused("You don't have permission to write to this table table 'orders'.")),
    "a writer whose tables are nothing": (
        {"role": "writer", "tables": None},
        _refused("You don't have permission to write to this table table 'orders'.")),
    "a superadmin": ({"role": "superadmin", "tables": EVERYTHING}, ("returned", None)),
    "an admin": ({"role": "admin", "tables": EVERYTHING}, ("returned", None)),
    "an admin of nothing": ({"role": "admin", "tables": {}},
                            _refused("You don't have permission to write to this table table 'orders'.")),
    "a reader": ({"role": "reader", "tables": EVERYTHING}, NO_PERMISSION),
    "a meta role": ({"role": "meta", "tables": EVERYTHING}, NO_PERMISSION),
    "no role type": ({"tables": EVERYTHING}, NO_PERMISSION),
    "an empty role type": ({"role": "", "tables": EVERYTHING}, NO_PERMISSION),
    "a role type nobody knows": ({"role": "owner", "tables": EVERYTHING}, NO_PERMISSION),
    "a role type in capitals": ({"role": "WRITER", "tables": EVERYTHING}, NO_PERMISSION),
    "no role": ({}, _refused("Invalid or nonexistent role: loader")),
    "disabled as text": (dict(WRITER, enabled="false"), _refused("Role 'loader' is disabled.")),
    "disabled as text in capitals": (dict(WRITER, enabled="FALSE"), _refused("Role 'loader' is disabled.")),
    "disabled as 0": (dict(WRITER, enabled="0"), _refused("Role 'loader' is disabled.")),
    "disabled as bytes": (dict(WRITER, enabled=b"false"), _refused("Role 'loader' is disabled.")),
    "disabled as a boolean": (dict(WRITER, enabled=False), _refused("Role 'loader' is disabled.")),
    "enabled as text": (dict(WRITER, enabled="true"), ("returned", None)),
    "enabled as a boolean": (dict(WRITER, enabled=True), ("returned", None)),
    "enabled as something else": (dict(WRITER, enabled="no"), ("returned", None)),
    "enabled as empty text": (dict(WRITER, enabled=""), ("returned", None)),
    "enabled as nothing": (dict(WRITER, enabled=None), ("returned", None)),
    "enabled as a number": (dict(WRITER, enabled=0), ("returned", None)),
    "a disabled reader": (dict(WRITER, role="reader", enabled="false"),
                          _refused("Role 'loader' is disabled.")),
}


@pytest.mark.parametrize("role", list(ROLES))
def test_a_role_read_with_the_root_is_judged_as_one_a_manager_read(caplog, role):
    document, expected = ROLES[role]
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        one = _Doubles(read=(A_ROOT, dict(document)))
        on_one_read = one.check()
        logged_on_one_read = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]
        caplog.clear()
        five = _Doubles(root=None, role=dict(document))
        on_five_reads = five.check()
        logged_on_five_reads = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]

    assert on_one_read == on_five_reads == expected
    assert logged_on_one_read == logged_on_five_reads
    assert one.calls == ONE_READ and five.calls == FIVE_READS


def test_what_is_logged_names_the_role_that_was_asked_for(caplog):
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        for document in ({}, dict(WRITER, enabled="false"), {"tables": EVERYTHING},
                         {"role": "owner"}, {"role": "reader"}):
            _Doubles(read=(A_ROOT, document)).check(role_name="Loader")

    assert [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER] == [
        ("ERROR", "Role not found: Loader"),
        ("WARNING", "Role 'Loader' is disabled"),
        ("ERROR", "Role 'Loader' has no role type"),
        ("ERROR", "Role 'Loader' has invalid role type: owner"),
        ("ERROR", "Role 'Loader' does not have WRITE permission."),
    ]


@pytest.mark.parametrize("table,expected", [
    ("orders", ("returned", None)), ("items", ("returned", None)),
    ("secrets", _refused("You don't have permission to write to this table table 'secrets'.")),
    ("ORDERS", _refused("You don't have permission to write to this table table 'ORDERS'.")),
    ("", _refused("You don't have permission to write to this table table ''.")),
])
def test_the_table_asked_for_is_looked_up_in_the_role_that_was_read(table, expected):
    role = {"role": "writer", "tables": {"orders": {"columns": ["*"]}, "items": {"columns": ["id"]}}}

    assert _Doubles(read=(A_ROOT, role)).check(table=table) == expected


def test_no_role_is_an_answer_and_not_a_reason_to_read_again():
    doubles = _Doubles(read=(A_ROOT, {}), role=dict(WRITER))

    assert doubles.check() == _refused("Invalid or nonexistent role: loader")
    assert doubles.calls == ONE_READ


# ---------------------------------------------------------------------------
# Five, when the one is not to be had
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("answer", [
    None, MagicMock(), [A_ROOT, dict(WRITER)], (A_ROOT,), (A_ROOT, dict(WRITER), 1),
    (7, dict(WRITER)), (b"{}", dict(WRITER)), ({"read_only": True}, dict(WRITER)),
    (A_ROOT, None), (A_ROOT, [("role", "writer")]), (A_ROOT, MagicMock()), (A_ROOT, "writer"),
    "OK", 0, {"root": A_ROOT, "role": dict(WRITER)},
], ids=["nothing", "a mock", "a list", "one value", "three values", "a root that is a number",
        "a root in bytes", "a root already parsed", "no role document", "a role in pairs",
        "a role that is a mock", "a role that is text", "text", "zero", "a dict"])
def test_an_answer_that_is_not_the_root_text_and_a_role_is_followed_by_the_five_reads(answer):
    """What a test double answers.  The tests that give the access check a
    manager of their own rest on this."""
    doubles = _Doubles(read=answer, root=None, role=dict(WRITER))

    assert doubles.check() == ("returned", None)

    assert doubles.calls == ONE_READ + FIVE_READS
    assert doubles.asked[1:] == [("get_root", ORG, LAKE), ("RoleManager()", LAKE, ORG),
                                 ("get_role_by_name", "loader")]


def test_a_manager_without_the_one_read_is_asked_the_five():
    doubles = _Doubles(root=None, role=dict(WRITER))

    assert doubles.check() == ("returned", None)

    assert doubles.calls == FIVE_READS
    assert doubles.asked == [("get_root", ORG, LAKE), ("RoleManager()", LAKE, ORG),
                             ("get_role_by_name", "loader")]


def test_a_read_that_is_no_function_is_not_called():
    doubles = _Doubles(root=None, role=dict(WRITER))
    doubles.Manager.read_for_access_check = "not to be called"

    assert doubles.check() == ("returned", None)
    assert doubles.calls == FIVE_READS


@pytest.mark.parametrize("failure", [
    redis.exceptions.TimeoutError("Timeout reading from socket"),
    redis.ResponseError("WRONGTYPE"), ValueError("Invalid Redis key segment"),
    AttributeError("rbac_access_read"), TypeError("unexpected keyword"), RuntimeError("anything"),
], ids=["a timeout", "a refusal", "an id that is no segment", "a manager half built",
        "another signature", "anything else"])
def test_a_failure_of_the_one_read_is_followed_by_the_five(failure):
    doubles = _Doubles(read=failure, root=None, role=dict(WRITER))

    assert doubles.check() == ("returned", None)
    assert doubles.calls == ONE_READ + FIVE_READS


def test_a_failed_read_says_nothing_of_the_lake_the_guard_reads_the_root_itself():
    doubles = _Doubles(read=redis.exceptions.TimeoutError("Timeout"),
                       root={"read_only": True, "clone_type": "replica"}, role=dict(WRITER))

    assert doubles.check() == _refused("This SuperTable is a live replica. Cannot write to this table.")
    assert doubles.calls == ONE_READ + ["RedisCatalog()", "get_root"]


@pytest.mark.parametrize("unbuildable,expected,calls", [
    (redis.exceptions.ConnectionError("no sentinel answers"), ("returned", None),
     ["RedisCatalog()", "RedisCatalog()", "RoleManager()", "get_role_by_name"]),
    (ValueError("bad settings"),
     _refused("Cannot verify whether this SuperTable is read-only. Cannot write to this table."),
     ["RedisCatalog()", "RedisCatalog()"]),
], ids=["Redis cannot be reached", "anything else"])
def test_a_catalog_that_cannot_be_built_is_met_by_the_guard_as_it_was(unbuildable, expected, calls):
    """The one read is not asked, and the guard, building its own catalog,
    meets the same failure the way it always did."""
    doubles = _Doubles(read=(A_ROOT, dict(WRITER)), role=dict(WRITER),
                       catalogs=[unbuildable, unbuildable])

    assert doubles.check() == expected
    assert doubles.calls == calls


# ---------------------------------------------------------------------------
# The five reads are what they were
# ---------------------------------------------------------------------------

def test_the_guard_passes_a_root_that_cannot_be_fetched_as_it_always_did():
    doubles = _Doubles(root=redis.exceptions.TimeoutError("Timeout"), role=dict(WRITER))

    assert doubles.check() == ("returned", None)
    assert doubles.calls == FIVE_READS


def test_the_guard_refuses_a_root_that_cannot_be_parsed_as_it_always_did(caplog):
    doubles = _Doubles(root=ValueError("Expecting value"), role=dict(WRITER))

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        outcome = doubles.check()

    assert outcome == _refused(
        "Cannot verify whether this SuperTable is read-only. Cannot write to this table.")
    assert doubles.calls == ["RedisCatalog()", "get_root"]
    assert [r.getMessage() for r in caplog.records if r.name == LOGGER] == [
        "[readonly-guard] cannot determine read-only state of acme/sales: Expecting value "
        "— refusing rather than assuming writable"]


def test_a_refusal_raised_by_the_catalog_itself_is_passed_on():
    doubles = _Doubles(root=PermissionError("not for you"), role=dict(WRITER))

    assert doubles.check() == _refused("not for you")
    assert doubles.calls == ["RedisCatalog()", "get_root"]


def test_a_read_only_root_stops_the_five_before_the_role():
    doubles = _Doubles(root={"read_only": True}, role=dict(WRITER))

    assert doubles.check() == _refused("This SuperTable is locked. Cannot write to this table.")
    assert doubles.calls == ["RedisCatalog()", "get_root"]


def test_a_mocked_manager_is_asked_the_five_reads():
    """The shape the access-control tests have: ``patch(...RoleManager,
    return_value=manager)``."""
    manager = MagicMock()
    manager.get_role_by_name.return_value = dict(WRITER)
    catalog = MagicMock()
    catalog.get_root.return_value = None

    with patch.object(access_control, "RoleManager", return_value=manager) as replaced, \
            patch.object(access_control, "RedisCatalog", return_value=catalog):
        check_write_access(LAKE, ORG, "loader", "orders")

    replaced.read_for_access_check.assert_called_once()
    replaced.assert_called_once_with(super_name=LAKE, organization=ORG)
    manager.get_role_by_name.assert_called_once_with("loader")
    catalog.get_root.assert_called_once_with(ORG, LAKE)
