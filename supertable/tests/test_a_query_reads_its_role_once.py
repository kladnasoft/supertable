"""
A query reads its role from Redis once.

``restrict_read_access``, the check every query passes, constructed a
``RoleManager`` and asked it for the role: four reads of Redis, one round trip
each (two checks that the role store is initialised, the id the role's name
stands for, the role's document).  ``RedisCatalog.rbac_access_read`` reads the
four in one script, the one the write check uses, which is then not given the
lake's root to read; a query has no read-only guard to apply, and its read
neither touches the root nor names it.

The four reads are still there, as the way to find the role when the one is
not to be had: a role store that is not initialised, a ``RoleManager`` that is
not the class that knows the read, a Redis that does not run the read's
script, a read that failed.

The comparison is with ``restrict_read_access`` as it was, kept in this file
word for word.  For every state the role store, the name index and the role's
document can be in, and every table and column a query can ask for, the check
must return the views it returned or raise what it raised, log what it logged,
and leave in Redis what it left.  Two fake Redis servers are given the same
content, the old check runs on one and the new on the other; the servers, the
lake and the states are those of ``test_an_access_check_reads_redis_once.py``.

``test_an_access_check_is_made_on_one_read_or_on_five.py`` has the decision
between the two ways, ``test_suite/test_access_check_read.py`` repeats the
comparison on a real Redis, and
``tests/characterization/test_the_round_trips_of_a_query_to_redis.py`` runs
the real reader.
"""
from __future__ import annotations

import logging
import os
import random
from typing import List
from unittest.mock import MagicMock, patch

import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.data_classes import TableDefinition  # noqa: E402
from supertable.rbac import access_control  # noqa: E402
from supertable.rbac.access_control import (  # noqa: E402
    _normalize_tables,
    _resolve_table_entry,
    restrict_read_access,
)
from supertable.rbac.filter_builder import FilterBuilder  # noqa: E402
from supertable.rbac.permissions import Permission, RoleType, has_permission  # noqa: E402
from supertable.rbac.role_manager import RoleManager  # noqa: E402
from supertable.rbac.row_column_security import normalize_allowed_columns  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.tests.test_an_access_check_reads_redis_once import (  # noqa: E402
    DOC_PREFIX,
    DOCUMENTS,
    LAKE,
    LOGGER,
    LOOKUPS,
    NAMES,
    NAMES_ASKED,
    ORG,
    READ_REFUSED,
    ROLE_META,
    ROOT,
    ROOTS,
    STORES,
    SUPERADMINS,
    THE_QUESTION,
    _Side,
    _build,
    _catalog_on,
    _doc_key,
    _field,
    _get_role_details_as_it_was,
    _known_ids,
    _left,
    _logged,
    _named,
    _refusing,
    _resolve_role_as_it_was,
    _role_id,
)

logger = access_control.logger

#: The round trip a query's role is read in: the script of the write check,
#: over the three keys of the role.  The fourth, the root, is not named.
THE_ROLE_READ = ("EVAL_RO", (ROLE_META, SUPERADMINS, NAMES))
FOUR = ["EXISTS", "SMEMBERS", "HGET", "HGETALL"]


# ---------------------------------------------------------------------------
# The check as it was (rbac/access_control.py at 117bcf0)
# ---------------------------------------------------------------------------

def _restrict_read_access_as_it_was(
        super_name: str,
        organization: str,
        role_name: str,
        tables: List[TableDefinition],
        physical_tables: List[TableDefinition],
) -> dict:
    from supertable.data_classes import RbacViewDef

    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role_as_it_was(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        raise PermissionError("You don't have permission to read the table.")

    try:
        role_type = RoleType(role_type_str)
    except ValueError:
        raise PermissionError("You don't have permission to read the table.")

    if not has_permission(role_type, Permission.READ):
        raise PermissionError("You don't have permission to read the table.")

    # Superadmin/admin: no filtering needed
    if role_type in (RoleType.SUPERADMIN, RoleType.ADMIN):
        return {}

    role_tables = _normalize_tables(role_info.get("tables", {}))
    default_entry = role_tables.get("*")

    # Fast path: if the only entry is "*" with all-wildcards, no views needed
    if (
        default_entry
        and len(role_tables) == 1
        and default_entry.get("columns", ["*"]) == ["*"]
        and default_entry.get("filters", ["*"]) == ["*"]
    ):
        return {}

    # ── Phase 1: Validate physical tables (merged, CTE-free) ──────── #
    #
    # This validates the *complete* set of physical columns the query
    # touches per table — columns from SELECT, WHERE, JOIN ON, GROUP BY,
    # HAVING, window functions, ORDER BY — all merged and deduplicated
    # by get_physical_tables().

    for pt in physical_tables:
        table_entry = _resolve_table_entry(role_tables, pt.simple_name)
        if table_entry is None:
            raise PermissionError(
                f"You don't have permission to read table '{pt.simple_name}'."
            )

        allowed_columns = normalize_allowed_columns(table_entry.get("columns"))
        if allowed_columns == ["*"]:
            # Unrestricted columns for this table — skip column validation.
            continue

        if not allowed_columns:
            raise PermissionError(
                f"You don't have permission to read any columns in '{pt.simple_name}'."
            )

        # pt.columns is [] when SELECT * or t.* was used.
        # In that case we cannot enumerate the requested columns at parse
        # time — the RBAC view will enforce column projection at execution.
        if pt.columns:
            requested_lower = {c.lower() for c in pt.columns}
            allowed_lower = {c.lower() for c in allowed_columns}
            denied = requested_lower - allowed_lower
            if denied:
                raise PermissionError(
                    f"You don't have permission to columns: {denied} "
                    f"in table '{pt.simple_name}'."
                )

    # ── Phase 2: Build per-alias RbacViewDef ──────────────────────── #
    #
    # Each alias gets its own view definition derived from its physical
    # table's entry in the role.  CTE aliases (which have no physical
    # table entry and no "*" default) are silently skipped — they have
    # already been validated transitively via physical_tables.

    rbac_views = {}

    for td in tables:
        table_entry = _resolve_table_entry(role_tables, td.simple_name)
        if table_entry is None:
            # Not a physical table in the role (CTE alias) — the engine
            # will also skip this alias since it has no snapshot.
            continue

        allowed_columns = normalize_allowed_columns(table_entry.get("columns"))
        filters = table_entry.get("filters", ["*"])

        # Row-level filtering from per-table filters
        where_clause = ""
        if filters != ["*"]:
            try:
                fb = FilterBuilder(
                    table_name="__PLACEHOLDER__",
                    columns=["*"],
                    role_info={"filters": filters},
                )
            except Exception as exc:
                # A filter that cannot be rendered is a broken policy.  Serving
                # the table unfiltered would turn a rendering bug straight into
                # a missing control, so deny instead (S9/S6).  The catch is
                # deliberately broad: a malformed filter document raises
                # TypeError/KeyError rather than ValueError, and *every* way of
                # failing to render a configured restriction has to land on
                # "deny", never on "no filter".
                logger.error(
                    f"Role '{role_name}' has an unusable row filter on "
                    f"'{td.simple_name}': {exc}"
                )
                raise PermissionError(
                    f"You don't have permission to read table "
                    f"'{td.simple_name}'."
                ) from exc

            generated = fb.filter_query
            where_idx = generated.upper().find("WHERE ")
            if where_idx >= 0:
                where_clause = generated[where_idx + 6:]

            if not where_clause:
                # Belt and braces: the decision to skip the view must be driven
                # by the *policy* (is it restrictive?), never by whether we
                # managed to render it.
                logger.error(
                    f"Role '{role_name}' has a non-wildcard row filter on "
                    f"'{td.simple_name}' that produced no WHERE clause"
                )
                raise PermissionError(
                    f"You don't have permission to read table "
                    f"'{td.simple_name}'."
                )

        # Only add an entry if there's actual filtering to apply
        if allowed_columns != ["*"] or where_clause:
            rbac_views[td.alias] = RbacViewDef(
                allowed_columns=list(allowed_columns),
                where_clause=where_clause,
            )

    return rbac_views


# ---------------------------------------------------------------------------
# What a query asks for, and what a reader's role can say
# ---------------------------------------------------------------------------

def _table(name, alias=None, columns=()):
    return TableDefinition(super_name=LAKE, simple_name=name, alias=alias or name, columns=list(columns))


#: (the tables of the query with their aliases, the physical tables behind them)
REQUESTS = {
    "one column of the table": ([_table("orders", "o", ["id"])], [_table("orders", "orders", ["id"])]),
    "two columns of the table": ([_table("orders", "o", ["id", "amount"])],
                                 [_table("orders", "orders", ["id", "amount"])]),
    "the column in capitals": ([_table("orders", "o", ["ID"])], [_table("orders", "orders", ["ID"])]),
    "every column of the table": ([_table("orders", "o")], [_table("orders")]),
    "a table the reader was not given": ([_table("items", "i", ["sku"])], [_table("items", "items", ["sku"])]),
    "two tables joined": ([_table("orders", "o", ["id"]), _table("items", "i", ["sku"])],
                          [_table("orders", "orders", ["id"]), _table("items", "items", ["sku"])]),
    "one table under two aliases": ([_table("orders", "a", ["id"]), _table("orders", "b", ["id"])],
                                    [_table("orders", "orders", ["id"])]),
    "a table and a name that is no table": ([_table("orders", "o", ["id"]), _table("recent", "r")],
                                            [_table("orders", "orders", ["id"])]),
    "no table at all": ([], []),
}

EVERYTHING = '{"*": {"columns": ["*"], "filters": ["*"]}}'
IN_EUROPE = '[{"region": {"operation": "=", "type": "value", "value": "eu"}}]'


def _tables(text):
    return _field("analyst", "tables", text)


#: Changes to the document of ``analyst``, a reader of one column of ``orders``.
READERS = {
    "as written": lambda content: None,
    "every table, every column": _tables(EVERYTHING),
    "every table, two columns": _tables('{"*": {"columns": ["id", "amount"], "filters": ["*"]}}'),
    "the table, every column": _tables('{"orders": {"columns": ["*"], "filters": ["*"]}}'),
    "the table, rows of one region": _tables('{"orders": {"columns": ["*"], "filters": %s}}' % IN_EUROPE),
    "one column, rows of one region": _tables('{"orders": {"columns": ["id"], "filters": %s}}' % IN_EUROPE),
    "a filter that cannot be rendered": _tables(
        '{"orders": {"columns": ["*"], "filters": [{"region": {"operation": "DROP", "type": "value", "value": "x"}}]}}'),
    "a filter that is text": _tables('{"orders": {"columns": ["*"], "filters": "region = \'eu\'"}}'),
    "a filter that is an empty list": _tables('{"orders": {"columns": ["id"], "filters": []}}'),
    "a star among the columns": _tables('{"orders": {"columns": ["id", "*"], "filters": ["*"]}}'),
    "no column at all": _tables('{"orders": {"columns": [], "filters": ["*"]}}'),
    "the columns as text": _tables('{"orders": {"columns": "id", "filters": ["*"]}}'),
    "an entry without columns": _tables('{"orders": {"filters": ["*"]}}'),
    "columns that are a number": _tables('{"orders": {"columns": 7}}'),
    "an entry that is empty": _tables('{"orders": {}}'),
    "an entry that is nothing, and a default": _tables('{"orders": null, "*": {"columns": ["id"]}}'),
    "two tables": _tables('{"orders": {"columns": ["id"]}, "items": {"columns": ["sku"], "filters": ["*"]}}'),
    "an old list of tables": _tables('["orders"]'),
    "an old list of everything": _tables('["*"]'),
    "tables that are not JSON": _tables("orders"),
    "no tables": _field("analyst", "tables", None),
    "tables an empty object": _tables("{}"),
    "tables null": _tables("null"),
    "become a writer": _field("analyst", "role", "writer"),
    "become a meta role": _field("analyst", "role", "meta"),
    "become an admin": _field("analyst", "role", "admin"),
    "become a superadmin": _field("analyst", "role", "superadmin"),
    "a role type nobody knows": _field("analyst", "role", "owner"),
    "no role type": _field("analyst", "role", None),
    "an empty role type": _field("analyst", "role", ""),
    "disabled": _field("analyst", "enabled", "false"),
    "disabled as 0": _field("analyst", "enabled", "0"),
    "enabled": _field("analyst", "enabled", "true"),
    "a document with one field": lambda content: content.__setitem__(
        _doc_key("analyst"), ("hash", {b"note": b"left behind"})),
}


# ---------------------------------------------------------------------------
# Running one way and the other
# ---------------------------------------------------------------------------

def _views(found) -> dict:
    return {alias: (list(view.allowed_columns), view.where_clause) for alias, view in found.items()}


def _outcome(function, role_name, request):
    tables, physical = REQUESTS[request] if isinstance(request, str) else request
    try:
        return ("returned", _views(function(LAKE, ORG, role_name, list(tables), list(physical))))
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


def _as_it_was(side, caplog, role_name, request):
    caplog.clear()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(RedisCatalog, "get_role_details", _get_role_details_as_it_was)
        outcome = _outcome(_restrict_read_access_as_it_was, role_name, request)
    return outcome, _logged(caplog, _known_ids())


def _now(side, caplog, role_name, request):
    caplog.clear()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(restrict_read_access, role_name, request)
    return outcome, _logged(caplog, _known_ids())


class _Ran:
    pass


def _same(caplog, content, role_name, request, label=""):
    """Both ways end the same: the views returned or what is raised, what is
    logged, what is left in Redis.  ``read_refused`` says whether Redis
    refused the one read, which is logged, once, before what the old check
    logs."""
    ran = _Ran()
    ran.old, ran.new = _Side().load(content), _Side().load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        ran.was = _as_it_was(ran.old, caplog, role_name, request)
        ran.now = _now(ran.new, caplog, role_name, request)
    assert ran.now[0] == ran.was[0], label
    logged = ran.now[1]
    ran.read_refused = bool(logged) and logged[0][0] == "ERROR" and logged[0][1].startswith(READ_REFUSED)
    assert (logged[1:] if ran.read_refused else logged) == ran.was[1], label
    assert _left(ran.new) == _left(ran.old), label
    return ran


# ---------------------------------------------------------------------------
# The check decides what it decided
# ---------------------------------------------------------------------------

def test_the_lake_gives_a_reader_one_column_and_refuses_the_rest(caplog):
    """The comparison compares something: a view, a role without limits, and
    each kind of refusal."""
    ran = _same(caplog, _build(), "analyst", "one column of the table")
    assert ran.now[0] == ("returned", {"o": (["id"], "")})
    assert ran.new.trips == [THE_ROLE_READ]
    assert [trip[0] for trip in ran.old.trips] == FOUR

    assert _same(caplog, _build(), "superadmin", "two tables joined").now[0] == ("returned", {})
    assert _same(caplog, _build(READERS["one column, rows of one region"]), "analyst",
                 "one column of the table").now[0] == ("returned", {"o": (["id"], "\"region\" = 'eu'")})
    refused = {
        ("analyst", "two columns of the table"): "You don't have permission to columns: {'amount'} in table 'orders'.",
        ("analyst", "a table the reader was not given"): "You don't have permission to read table 'items'.",
        ("catalog_viewer", "one column of the table"): "You don't have permission to read the table.",
        ("nobody", "one column of the table"): "Invalid or nonexistent role: nobody",
    }
    for (name, request), message in refused.items():
        assert _same(caplog, _build(), name, request).now[0] == ("raised", "PermissionError", message)


@pytest.mark.parametrize("reader", list(READERS))
@pytest.mark.parametrize("request_", list(REQUESTS))
def test_every_reader_document_is_judged_as_it_was(caplog, reader, request_):
    ran = _same(caplog, _build(READERS[reader]), "analyst", request_)

    assert ran.new.trips == [THE_ROLE_READ]
    assert [trip[0] for trip in ran.old.trips] == FOUR


@pytest.mark.parametrize("asked", list(NAMES_ASKED))
@pytest.mark.parametrize("request_", ["one column of the table", "two tables joined", "every column of the table"])
def test_every_name_asked_for_is_looked_up_as_it_was(caplog, asked, request_):
    ran = _same(caplog, _build(), NAMES_ASKED[asked], request_)

    assert ran.new.trips == [THE_ROLE_READ]


@pytest.mark.parametrize("store", list(STORES))
@pytest.mark.parametrize("name", ["analyst", "superadmin", "nobody", None])
def test_every_role_store_is_left_as_the_constructor_left_it(caplog, store, name):
    """A store the constructor has work to do on is still given to the
    constructor, by a query as by a write: both ways end with the same roles
    in Redis."""
    _same(caplog, _build(STORES[store]), name, "one column of the table")


@pytest.mark.parametrize("store", ["no role store at all", "no meta key", "no superadmin listed",
                                   "a superadmin index holding empty text",
                                   "a superadmin index holding empty text beside the id"])
def test_a_store_that_is_not_initialised_is_left_to_the_four_reads(caplog, store):
    ran = _same(caplog, _build(STORES[store]), "superadmin", "one column of the table")

    assert ran.new.trips[0] == THE_ROLE_READ
    assert _named(ran.new.trips[1:]) == _named(ran.old.trips), "then everything the check did before"
    assert len(ran.old.trips) >= 4


@pytest.mark.parametrize("lookup", list(LOOKUPS))
@pytest.mark.parametrize("request_", ["one column of the table", "a table the reader was not given"])
def test_every_answer_of_the_name_index_is_taken_as_it_was(caplog, lookup, request_):
    _same(caplog, _build(LOOKUPS[lookup]), "loader", request_)


@pytest.mark.parametrize("document", list(DOCUMENTS))
def test_every_document_of_a_writer_reads_as_it_did(caplog, document):
    """The states the write check is compared on, read: a writer may read."""
    ran = _same(caplog, _build(DOCUMENTS[document]), "loader", "one column of the table")

    assert ran.new.trips == [THE_ROLE_READ]


@pytest.mark.parametrize("root", list(ROOTS))
def test_the_root_is_nothing_to_a_query(caplog, root):
    """A lake that is locked, a replica, or whose root cannot be read at all
    is read as before: the read-only guard is the mutations', and this read
    does not touch the root, whatever kind of value it holds."""
    ran = _same(caplog, _build(ROOTS[root]), "analyst", "one column of the table")

    assert ran.now[0] == ("returned", {"o": (["id"], "")})
    assert ran.new.trips == [THE_ROLE_READ] and not ran.read_refused
    assert [trip[0] for trip in ran.old.trips] == FOUR


def test_any_mix_of_states_is_decided_the_same(caplog):
    """Not only one thing wrong at a time."""
    pick = random.Random(20261126)
    roots, stores, lookups = list(ROOTS), list(STORES), list(LOOKUPS)
    readers, documents, names, requests = list(READERS), list(DOCUMENTS), list(NAMES_ASKED), list(REQUESTS)
    views = refusals = 0
    for _ in range(400):
        chosen = (pick.choice(roots), pick.choice(stores), pick.choice(lookups), pick.choice(readers),
                  pick.choice(documents), pick.choice(names), pick.choice(requests))
        content = _build()
        for change in (READERS[chosen[3]], DOCUMENTS[chosen[4]], LOOKUPS[chosen[2]], STORES[chosen[1]],
                       ROOTS[chosen[0]]):
            try:
                change(content)
            except KeyError:
                pass    # an earlier change took away what this one would change
        ran = _same(caplog, content, NAMES_ASKED[chosen[5]], chosen[6], label=repr(chosen))
        views += ran.now[0][0] == "returned"
        refusals += ran.now[0][0] == "raised"
    assert views > 20 and refusals > 100, (views, refusals)


# ---------------------------------------------------------------------------
# One round trip, the root left alone, nothing written
# ---------------------------------------------------------------------------

def test_the_read_of_a_query_neither_reads_the_root_nor_names_it():
    """The keys a script is given are keys its user must be allowed to touch:
    a query's read is given the three of the role and no other."""
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    sent = []
    real = side.client.eval_ro

    def recording(script, numkeys, *rest):
        sent.append((list(rest[:numkeys]), list(rest[numkeys:])))
        return real(script, numkeys, *rest)

    side.client.eval_ro = recording
    with_root = catalog.rbac_access_read(ORG, LAKE, "Analyst")
    without = catalog.rbac_access_read(ORG, LAKE, "Analyst", with_root=False)

    prefix = RK.rbac_role_doc_prefix(ORG, LAKE)
    assert [args for _keys, args in sent] == [["analyst", prefix]] * 2
    assert [keys for keys, _args in sent] == [[ROLE_META, SUPERADMINS, NAMES, ROOT], [ROLE_META, SUPERADMINS, NAMES]]
    assert with_root[0] is not None and without[0] is None
    assert without[1:] == with_root[1:], "everything but the root is the same"


@pytest.mark.parametrize("root", ["a root that is a hash", "a root that is no text", "no root", "locked"])
def test_the_read_without_the_root_is_the_same_whatever_the_root_is(root):
    side = _Side().load(_build(ROOTS[root]))
    catalog = _catalog_on(side)
    before = side.dump()

    root_read, meta, superadmins, role = catalog.rbac_access_read(ORG, LAKE, "analyst", with_root=False)

    assert (root_read, meta, superadmins) == (None, True, [_role_id("superadmin")])
    assert role["role"] == "reader" and role["tables"] == {"orders": {"columns": ["id"], "filters": ["*"]}}
    assert side.dump() == before


def test_the_manager_hands_over_no_root_when_none_is_asked_for():
    side = _Side().load(_build(ROOTS["a root that is a hash"]))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        answer = RoleManager.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="analyst", with_root=False)
        manager = RoleManager(super_name=LAKE, organization=ORG)
        assert answer == (None, manager.get_role_by_name("analyst"))
        assert RoleManager.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="analyst") is None, \
            "with the root asked for, a root of another kind fails the read"


def test_a_check_writes_nothing():
    for change in list(READERS.values()) + list(LOOKUPS.values()):
        side = _Side().load(_build(change))
        before = side.dump()
        with pytest.MonkeyPatch.context() as patched:
            side.use(patched)
            _outcome(restrict_read_access, "analyst", "two tables joined")
        assert side.dump() == before


# ---------------------------------------------------------------------------
# When the one read is not to be had
# ---------------------------------------------------------------------------

def _failing(*names):
    return lambda name, key: name in names


@pytest.mark.parametrize("name,request_", [("analyst", "one column of the table"),
                                           ("analyst", "two columns of the table"),
                                           ("nobody", "one column of the table"),
                                           ("superadmin", "two tables joined")])
def test_a_read_that_fails_is_followed_by_the_four_as_they_were(caplog, name, request_):
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    new.fail = _failing("EVAL_RO")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, name, request_)
        now = _now(new, caplog, name, request_)

    assert now[0] == was[0]
    assert now[1] == [("ERROR", "[redis-catalog] rbac_access_read error: Timeout reading from socket")] + was[1]
    assert new.trips == [THE_ROLE_READ] + old.trips
    assert new.dump() == old.dump()


@pytest.mark.parametrize("also", FOUR)
def test_one_of_the_four_failing_after_it_ends_as_it_did(caplog, also):
    """With the one read gone, each of the four is still the read it was: a
    failure of the first three is raised, and a document that cannot be
    fetched is a role that does not exist."""
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    old.fail = _failing(also)
    new.fail = _failing("EVAL_RO", also)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "analyst", "one column of the table")
        now = _now(new, caplog, "analyst", "one column of the table")

    assert now[0] == was[0]
    assert now[0][1] == ("PermissionError" if also == "HGETALL" else "TimeoutError")
    assert now[1][1:] == was[1]
    assert new.trips == [THE_ROLE_READ] + old.trips


def test_a_redis_before_7_is_asked_the_four_reads_and_nothing_is_logged(caplog):
    content = _build()
    old, new = _Side().load(content), _Side(redis_7=False, asked=False).load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "analyst", "one column of the table")
        first = _now(new, caplog, "analyst", "one column of the table")
        first_trips = list(new.trips)
        new.trips.clear()
        second = _now(new, caplog, "analyst", "a table the reader was not given")

    assert first == was == (("returned", {"o": (["id"], "")}), [])
    assert first_trips == [THE_QUESTION] + old.trips, "asked once, refused, and the four reads followed"
    assert second == (("raised", "PermissionError", "You don't have permission to read table 'items'."), [])
    assert [trip[0] for trip in new.trips] == FOUR


def test_a_manager_put_in_its_place_is_asked_as_before():
    """How the tests of the read check give it a role store of their own."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        manager = RoleManager(super_name=LAKE, organization=ORG)
        side.trips.clear()
        with patch("supertable.rbac.access_control.RoleManager", return_value=manager) as replaced:
            outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("returned", {"o": (["id"], "")})
    replaced.assert_called_once_with(super_name=LAKE, organization=ORG)
    replaced.read_for_access_check.assert_called_once_with(
        super_name=LAKE, organization=ORG, role_name="analyst", with_root=False)
    assert side.trips == [("HGET", NAMES), ("HGETALL", _doc_key("analyst"))]


class _NobodyIsARole(RoleManager):
    def get_role_by_name(self, role_name):
        return {}


def test_a_subclass_of_the_manager_is_asked_the_lookup_it_may_have_changed():
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.access_control.RoleManager", _NobodyIsARole):
            outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("raised", "PermissionError", "Invalid or nonexistent role: analyst")
    assert [trip[0] for trip in side.trips] == ["EXISTS", "SMEMBERS"], "the constructor's two, and its own lookup"


@pytest.mark.parametrize("missing", ["the catalog's read", "the manager's read"])
def test_a_library_without_the_read_checks_as_before(caplog, missing):
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "analyst", "one column of the table")
        with pytest.MonkeyPatch.context() as patched:
            if missing == "the catalog's read":
                patched.delattr(RedisCatalog, "rbac_access_read")
            else:
                patched.delattr(RoleManager, "read_for_access_check")
            now = _now(new, caplog, "analyst", "one column of the table")

    assert now == was
    assert new.trips == old.trips and len(old.trips) == 4


def test_a_role_changed_between_two_queries_decides_the_second():
    """Nothing is remembered from one query to the next."""
    side = _Side().load(_build())
    seen = []
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        seen.append(_outcome(restrict_read_access, "analyst", "one column of the table"))
        side.bytes.hset(_doc_key("analyst"), "tables", '{"orders": {"columns": ["amount"], "filters": ["*"]}}')
        seen.append(_outcome(restrict_read_access, "analyst", "one column of the table"))
        side.bytes.hset(_doc_key("analyst"), "enabled", "false")
        seen.append(_outcome(restrict_read_access, "analyst", "one column of the table"))
        side.bytes.hdel(NAMES, "analyst")
        seen.append(_outcome(restrict_read_access, "analyst", "one column of the table"))

    assert seen == [
        ("returned", {"o": (["id"], "")}),
        ("raised", "PermissionError", "You don't have permission to columns: {'id'} in table 'orders'."),
        ("raised", "PermissionError", "Role 'analyst' is disabled."),
        ("raised", "PermissionError", "Invalid or nonexistent role: analyst"),
    ]
    assert side.trips == [THE_ROLE_READ] * 4


# ---------------------------------------------------------------------------
# A lookup someone has replaced is the lookup that is asked
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,replacement,expected,trips", [
    ("get_role_by_name", lambda self, role_name: {},
     ("raised", "PermissionError", "Invalid or nonexistent role: analyst"), ["EXISTS", "SMEMBERS"]),
    ("get_role", lambda self, role_id: {"role": "meta", "tables": {"*": {}}},
     ("raised", "PermissionError", "You don't have permission to read the table."), ["EXISTS", "SMEMBERS", "HGET"]),
    ("_init_role_storage", lambda self: None, ("returned", {"o": (["id"], "")}), ["HGET", "HGETALL"]),
], ids=["the lookup by name", "the lookup by id", "the check of the role store"])
def test_a_manager_with_one_of_its_reads_replaced_is_asked_that_read(monkeypatch, name, replacement,
                                                                     expected, trips):
    side = _Side().load(_build())
    monkeypatch.setattr(RoleManager, name, replacement)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == expected
    assert [trip[0] for trip in side.trips] == trips


@pytest.mark.parametrize("name", ["get_role_details", "rbac_get_role_id_by_name", "rbac_get_superadmin_role_id"])
def test_a_catalog_with_one_of_its_reads_replaced_is_asked_that_read(monkeypatch, name):
    side = _Side().load(_build())
    called = []
    original = getattr(RedisCatalog, name)

    def watching(self, *args, **options):
        called.append(name)
        return original(self, *args, **options)

    monkeypatch.setattr(RedisCatalog, name, watching)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("returned", {"o": (["id"], "")})
    assert called == [name]
    assert [trip[0] for trip in side.trips] == FOUR


def test_a_resolution_put_in_the_place_of_the_module_s_is_the_one_that_runs(monkeypatch):
    side = _Side().load(_build())
    monkeypatch.setattr(access_control, "_resolve_role",
                        lambda role_manager, role_name: {"role": "reader", "tables": {"orders": {"columns": ["amount"]}}})
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(restrict_read_access, "analyst", "every column of the table")

    assert outcome == ("returned", {"o": (["amount"], "")})
    assert [trip[0] for trip in side.trips] == ["EXISTS", "SMEMBERS"], "a manager was built for it, as before"


# ---------------------------------------------------------------------------
# The catalog a manager builds, the client's own commands, a document's key
# ---------------------------------------------------------------------------

class _NoNameIsARole(RedisCatalog):
    """A catalog a test might put where a manager builds its own."""

    def rbac_get_role_id_by_name(self, org, sup, role_name):
        return None


def test_a_catalog_put_where_a_manager_builds_its_own_is_the_one_asked():
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.role_manager.RedisCatalog", _NoNameIsARole):
            outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("raised", "PermissionError", "Invalid or nonexistent role: analyst")
    assert [trip[0] for trip in side.trips] == ["EXISTS", "SMEMBERS"], "the constructor's two, and its own lookup"


def test_a_catalog_put_in_the_access_check_s_place_is_nothing_to_a_query():
    """A query's check builds no catalog: the role is read by the one a
    manager builds for itself, as it always was."""
    side = _Side().load(_build())
    in_its_place = MagicMock()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.access_control.RedisCatalog", in_its_place):
            outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("returned", {"o": (["id"], "")})
    assert side.trips == [THE_ROLE_READ]
    in_its_place.assert_not_called()


@pytest.mark.parametrize("name", ["exists", "smembers", "hget", "hgetall"])
def test_a_client_with_one_of_its_reads_replaced_is_asked_that_read(name):
    """What a test does to have Redis fail or answer as the test likes.  The
    script would go round the command that was put there."""
    side = _Side().load(_build())
    asked = []
    original = getattr(side.client, name)

    def watching(*args, **options):
        asked.append(name)
        return original(*args, **options)

    setattr(side.client, name, watching)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(restrict_read_access, "analyst", "one column of the table")

    assert outcome == ("returned", {"o": (["id"], "")})
    assert [trip[0] for trip in side.trips] == FOUR
    assert asked == [name]


def test_a_redis_that_said_yes_and_refuses_the_read_is_not_sent_it_again_for_a_minute(caplog, monkeypatch):
    """A user that may run a script and may not read a role's document.  One
    refusal, logged once; the four reads decide; a minute on it is asked."""
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    clock = {"now": 9.0}
    monkeypatch.setattr(redis_catalog, "_monotonic", lambda: clock["now"])
    refusal = redis.ResponseError("ACL failure in script: No permissions to access a key script: "
                                  "83cd8e8f, on @user_script:9.")
    sent, state = [], {"with": refusal}
    _refusing(new, state, sent)
    request = "one column of the table"

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "analyst", request)
        first = _now(new, caplog, "analyst", request)
        second = _now(new, caplog, "analyst", request)
        clock["now"] += redis_catalog._ASK_AGAIN_AFTER_S
        state["with"] = None
        third = _now(new, caplog, "analyst", request)
        fourth = _now(new, caplog, "analyst", request)

    assert was == (("returned", {"o": (["id"], "")}), [])
    assert first == (was[0], [("ERROR", READ_REFUSED + str(refusal))])
    assert second == third == fourth == was
    assert sent == ["the read", "the question", "the read", "the read"]
    assert [trip[0] for trip in new.trips] == FOUR * 2 + ["EVAL_RO"] * 3
    assert new.trips[-1] == THE_ROLE_READ and new.trips[-3] == THE_QUESTION


def test_a_document_key_built_another_way_is_the_key_the_check_reads(caplog, monkeypatch):
    """The script appends the id to a prefix; the check has always read the
    key the library's own constructor makes."""
    content = _build()
    moved = DOC_PREFIX.replace(":roles:doc:", ":roles:moved:")
    monkeypatch.setattr(RK, "rbac_role_doc", lambda org, sup, role_id: f"{moved}{role_id}")

    ran = _same(caplog, content, "analyst", "one column of the table")

    assert ran.now[0] == ("raised", "PermissionError", "Invalid or nonexistent role: analyst")
    assert ran.new.trips == [THE_ROLE_READ] + ran.old.trips and [trip[0] for trip in ran.old.trips] == FOUR
