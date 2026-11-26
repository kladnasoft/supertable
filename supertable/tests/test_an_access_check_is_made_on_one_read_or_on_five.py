"""
An access check is made on one read of Redis, or on the five it always made.

``check_write_access`` asks ``RoleManager.read_for_access_check`` for the
lake's root and the role in one round trip, and goes the old way, the guard's
own read of the root and then a constructed ``RoleManager``, whenever that is
not to be had: a ``RoleManager`` or a catalog that does not know the read
(every test double), a catalog put in the place of the guard's and not of the
manager's or the other way round, an answer that is not the root's text and a
role document, or a failure of the call.

A query's check, ``restrict_read_access``, has no read-only guard: it asks the
same read for the role alone, without the root, and goes the old way, a
constructed ``RoleManager`` and its lookup, under the same conditions.

The collaborators here are doubles that record what they are asked, so that
which way was taken, in which order, and what each answer leads to can be read
off exactly.  The real ones are on the other side of this:
``test_an_access_check_reads_redis_once.py``.
"""
from __future__ import annotations

import logging
import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.data_classes import TableDefinition  # noqa: E402
from supertable.rbac import access_control, role_manager  # noqa: E402
from supertable.rbac.access_control import check_write_access, restrict_read_access  # noqa: E402

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
            def read_for_access_check(super_name, organization, role_name, redis_catalog=None,
                                      with_root=True):
                calls.append("read_for_access_check")
                asked.append(("read_for_access_check", super_name, organization, role_name,
                              type(redis_catalog)) + (() if with_root is True else (("with_root", with_root),)))
                return answer(read)

            Manager.read_for_access_check = staticmethod(read_for_access_check)

        self.Catalog, self.Manager = Catalog, Manager

    def check(self, role_name="loader", table="orders", everywhere=True):
        """``check_write_access`` with the doubles in the place of the catalog and the manager.

        The catalog double stands where the access check builds a catalog for
        its guard and, unless *everywhere* is false, where a manager builds
        one for itself: the one read asks one catalog for the root and the
        role, and is asked for only where the two are one class.
        """
        with ExitStack() as places:
            places.enter_context(patch.object(access_control, "RedisCatalog", self.Catalog))
            places.enter_context(patch.object(access_control, "RoleManager", self.Manager))
            if everywhere:
                places.enter_context(patch.object(role_manager, "RedisCatalog", self.Catalog))
            try:
                return ("returned", check_write_access(LAKE, ORG, role_name, table))
            except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
                return ("raised", type(error).__name__, str(error))

    def read(self, role_name="analyst", table="orders", columns=("id",)):
        """``restrict_read_access`` for one table of a query; the views as plain values.

        The catalog double stands in the access check's place alone here: a
        query's check builds no catalog, and which one the access check names
        is nothing to it.
        """
        asked_for = [TableDefinition(super_name=LAKE, simple_name=table, alias="t", columns=list(columns))]
        with patch.object(access_control, "RedisCatalog", self.Catalog), \
                patch.object(access_control, "RoleManager", self.Manager):
            try:
                views = restrict_read_access(LAKE, ORG, role_name, asked_for, asked_for)
                return ("returned", {alias: (list(view.allowed_columns), view.where_clause)
                                     for alias, view in views.items()})
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


def test_a_catalog_in_the_guard_s_place_alone_is_asked_for_the_root_as_before():
    """The guard read the root through the access check's catalog, and the
    role came from the catalog a manager builds for itself.  The one read asks
    one catalog for both: where the two are not one class, each is asked what
    it was asked before, and the one read is not asked for."""
    doubles = _Doubles(read=(A_ROOT, dict(WRITER)), root=None, role=dict(WRITER))

    assert doubles.check(everywhere=False) == ("returned", None)

    assert doubles.calls == FIVE_READS
    assert doubles.asked == [("get_root", ORG, LAKE), ("RoleManager()", LAKE, ORG),
                             ("get_role_by_name", "loader")]


def test_the_root_the_guard_s_own_catalog_reads_decides_and_not_the_one_the_read_would_hand_over():
    doubles = _Doubles(read=(A_ROOT, dict(WRITER)), root={"read_only": True}, role=dict(WRITER))

    assert doubles.check(everywhere=False) == _refused("This SuperTable is locked. Cannot write to this table.")
    assert doubles.calls == ["RedisCatalog()", "get_root"]


def test_a_manager_s_module_that_names_no_catalog_is_asked_the_five(monkeypatch):
    doubles = _Doubles(read=(A_ROOT, dict(WRITER)), root=None, role=dict(WRITER))
    monkeypatch.delattr(role_manager, "RedisCatalog")

    assert doubles.check(everywhere=False) == ("returned", None)
    assert doubles.calls == FIVE_READS


def test_the_two_catalogs_are_one_class_as_the_library_stands():
    from supertable.redis_catalog import RedisCatalog

    assert access_control.RedisCatalog is role_manager.RedisCatalog is RedisCatalog
    assert access_control._role_manager is role_manager


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


@pytest.mark.parametrize("everywhere", [False, True],
                         ids=["a catalog mocked for the guard", "the same mock where a manager builds its own"])
def test_a_mocked_manager_is_asked_the_five_reads(everywhere):
    """The shape the access-control tests have: ``patch(...RoleManager,
    return_value=manager)`` and ``patch(...RedisCatalog, return_value=catalog)``.
    A mocked catalog for the guard alone is not asked for the one read; one
    that stands in both places is, and what a mock answers is no answer."""
    manager = MagicMock()
    manager.get_role_by_name.return_value = dict(WRITER)
    catalog = MagicMock()
    catalog.get_root.return_value = None
    catalog_class = MagicMock(return_value=catalog)

    with ExitStack() as places:
        replaced = places.enter_context(patch.object(access_control, "RoleManager", return_value=manager))
        places.enter_context(patch.object(access_control, "RedisCatalog", catalog_class))
        if everywhere:
            places.enter_context(patch.object(role_manager, "RedisCatalog", catalog_class))
        check_write_access(LAKE, ORG, "loader", "orders")

    assert replaced.read_for_access_check.call_count == (1 if everywhere else 0)
    replaced.assert_called_once_with(super_name=LAKE, organization=ORG)
    manager.get_role_by_name.assert_called_once_with("loader")
    catalog.get_root.assert_called_once_with(ORG, LAKE)


# ---------------------------------------------------------------------------
# A query: the role alone, on one read or on four
# ---------------------------------------------------------------------------

READER = {"role": "reader", "role_name": "analyst", "role_id": "b" * 32,
          "tables": {"orders": {"columns": ["id"], "filters": ["*"]}}}
ONE_VIEW = ("returned", {"t": (["id"], "")})
ROLE_READ = ["read_for_access_check"]
FOUR_READS = ["RoleManager()", "get_role_by_name"]
NOT_TO_READ = _refused("You don't have permission to read the table.")


def test_a_query_asks_the_one_read_for_the_role_and_not_for_the_root():
    doubles = _Doubles(read=(None, dict(READER)))

    assert doubles.read() == ONE_VIEW

    assert doubles.calls == ROLE_READ, "no catalog is built by the check, and no manager"
    assert doubles.asked == [("read_for_access_check", LAKE, ORG, "analyst", type(None), ("with_root", False))]


@pytest.mark.parametrize("root", [None, "", A_ROOT, '{"read_only": true, "clone_type": "replica"}',
                                  "{not json", "[1]", 7, b"{}", {"read_only": True}, MagicMock()],
                         ids=["none", "empty", "a root", "a replica", "not JSON", "a list", "a number", "bytes",
                              "a document", "a mock"])
def test_whatever_comes_back_in_the_place_of_the_root_is_not_looked_at(caplog, root):
    """A query has no read-only guard.  A lake that is locked or a replica is
    read, and what stands where the root would be is never opened."""
    doubles = _Doubles(read=(root, dict(READER)))

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        assert doubles.read() == ONE_VIEW

    assert doubles.calls == ROLE_READ
    assert not [r for r in caplog.records if r.name == LOGGER]


READERS = {
    "one column of the table": (dict(READER), ONE_VIEW),
    "every column of every table": ({"role": "reader", "tables": EVERYTHING}, ("returned", {})),
    "the table, rows of one region": (
        {"role": "reader", "tables": {"orders": {"columns": ["*"], "filters": [
            {"region": {"operation": "=", "type": "value", "value": "eu"}}]}}},
        ("returned", {"t": (["*"], "\"region\" = 'eu'")})),
    "another column of the table": (
        {"role": "reader", "tables": {"orders": {"columns": ["amount"]}}},
        _refused("You don't have permission to columns: {'id'} in table 'orders'.")),
    "another table": ({"role": "reader", "tables": {"items": {"columns": ["*"]}}},
                      _refused("You don't have permission to read table 'orders'.")),
    "no column of the table": ({"role": "reader", "tables": {"orders": {"columns": []}}},
                               _refused("You don't have permission to read any columns in 'orders'.")),
    "a filter that cannot be rendered": (
        {"role": "reader", "tables": {"orders": {"columns": ["*"], "filters": [
            {"region": {"operation": "DROP", "type": "value", "value": "x"}}]}}},
        _refused("You don't have permission to read table 'orders'.")),
    "a writer of the table": ({"role": "writer", "tables": {"orders": {"columns": ["id"]}}}, ONE_VIEW),
    "an admin of nothing": ({"role": "admin", "tables": {}}, ("returned", {})),
    "a superadmin": ({"role": "superadmin", "tables": EVERYTHING}, ("returned", {})),
    "a meta role": ({"role": "meta", "tables": EVERYTHING}, NOT_TO_READ),
    "no role type": ({"tables": EVERYTHING}, NOT_TO_READ),
    "a role type nobody knows": ({"role": "owner", "tables": EVERYTHING}, NOT_TO_READ),
    "no role": ({}, _refused("Invalid or nonexistent role: analyst")),
    "disabled": (dict(READER, enabled="false"), _refused("Role 'analyst' is disabled.")),
    "disabled as a boolean": (dict(READER, enabled=False), _refused("Role 'analyst' is disabled.")),
    "enabled": (dict(READER, enabled="true"), ONE_VIEW),
}


@pytest.mark.parametrize("reader", list(READERS))
def test_a_role_read_at_once_is_judged_as_one_a_manager_read(caplog, reader):
    document, expected = READERS[reader]
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        one = _Doubles(read=(None, json_copy(document)))
        on_one_read = one.read()
        logged_on_one_read = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]
        caplog.clear()
        four = _Doubles(role=json_copy(document))
        on_four_reads = four.read()
        logged_on_four_reads = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]

    assert on_one_read == on_four_reads == expected
    assert logged_on_one_read == logged_on_four_reads
    assert one.calls == ROLE_READ and four.calls == FOUR_READS


def json_copy(document):
    """A copy the check may change without changing the table above."""
    import copy
    return copy.deepcopy(document)


def test_no_role_for_a_query_is_an_answer_and_not_a_reason_to_read_again():
    doubles = _Doubles(read=(None, {}), role=dict(READER))

    assert doubles.read() == _refused("Invalid or nonexistent role: analyst")
    assert doubles.calls == ROLE_READ


@pytest.mark.parametrize("answer", [
    None, MagicMock(), [None, dict(READER)], (None,), (None, dict(READER), 1), (None, None),
    (None, [("role", "reader")]), (None, MagicMock()), (None, "reader"), "OK", 0, dict(READER),
], ids=["nothing", "a mock", "a list", "one value", "three values", "no role document", "a role in pairs",
        "a role that is a mock", "a role that is text", "text", "zero", "the role by itself"])
def test_an_answer_that_is_not_a_root_and_a_role_is_followed_by_the_four_reads(answer):
    doubles = _Doubles(read=answer, role=dict(READER))

    assert doubles.read() == ONE_VIEW

    assert doubles.calls == ROLE_READ + FOUR_READS
    assert doubles.asked[1:] == [("RoleManager()", LAKE, ORG), ("get_role_by_name", "analyst")]


def test_a_manager_without_the_one_read_is_asked_the_four():
    doubles = _Doubles(role=dict(READER))

    assert doubles.read() == ONE_VIEW
    assert doubles.calls == FOUR_READS


def test_a_read_that_is_no_function_is_not_called_by_a_query():
    doubles = _Doubles(role=dict(READER))
    doubles.Manager.read_for_access_check = "not to be called"

    assert doubles.read() == ONE_VIEW
    assert doubles.calls == FOUR_READS


@pytest.mark.parametrize("failure", [
    redis.exceptions.TimeoutError("Timeout reading from socket"), redis.ResponseError("WRONGTYPE"),
    NotImplementedError("this Redis does not run it"), TypeError("unexpected keyword 'with_root'"),
    RuntimeError("anything"),
], ids=["a timeout", "a refusal", "a Redis that does not run the script", "another signature", "anything else"])
def test_a_failure_of_the_one_read_of_a_query_is_followed_by_the_four(failure):
    doubles = _Doubles(read=failure, role=dict(READER))

    assert doubles.read() == ONE_VIEW
    assert doubles.calls == ROLE_READ + FOUR_READS


def test_the_four_reads_of_a_query_are_what_they_were():
    """A manager is constructed, asked for the role, and what it answers is
    put to the same tests; the catalog of the read-only guard is not built."""
    assert _Doubles(role={}).read() == _refused("Invalid or nonexistent role: analyst")
    assert _Doubles(role=dict(READER, enabled="0")).read() == _refused("Role 'analyst' is disabled.")
    refusing = _Doubles(role=redis.exceptions.TimeoutError("Timeout"))
    assert refusing.read() == ("raised", "TimeoutError", "Timeout")
    assert refusing.calls == FOUR_READS


def test_a_mocked_manager_is_asked_the_four_reads_of_a_query():
    """The shape the tests of the read check have: ``patch(...RoleManager,
    return_value=manager)``."""
    manager = MagicMock()
    manager.get_role_by_name.return_value = dict(READER)
    table = [TableDefinition(super_name=LAKE, simple_name="orders", alias="t", columns=["id"])]

    with patch.object(access_control, "RoleManager", return_value=manager) as replaced:
        views = restrict_read_access(LAKE, ORG, "analyst", table, table)

    assert {alias: view.allowed_columns for alias, view in views.items()} == {"t": ["id"]}
    replaced.read_for_access_check.assert_called_once_with(
        super_name=LAKE, organization=ORG, role_name="analyst", with_root=False)
    replaced.assert_called_once_with(super_name=LAKE, organization=ORG)
    manager.get_role_by_name.assert_called_once_with("analyst")

