"""
An access check reads Redis once.

``check_write_access`` read Redis five times, one round trip each: the lake's
root (the read-only guard), two checks that the role store is initialised
(made by ``RoleManager`` when it is constructed), the id the role's name
stands for, and the role's document.  ``RedisCatalog.rbac_access_read`` reads
the five in one script, ``RoleManager.read_for_access_check`` says whether the
role store is one the constructor would have left alone, and the checks made
on what was read are the ones that were made before.

The five reads are still there, as the way to check when the one is not to be
had: a role store that is not initialised, a collaborator that does not know
the read, a Redis that does not run the read's script, a read that failed.

The script is sent as a read (``EVAL_RO``), a command Redis 7 and later know
and fakeredis, like a Redis before 7, does not.  The fake Redis of these tests
is therefore one of two: by default it runs a script sent that way, as Redis 7
does, and where a test says so it is left as it is, a Redis that answers
"unknown command" and is asked the five reads.

The comparison here is with the check as it was, kept in this file word for
word: the functions of ``rbac/access_control.py`` and ``get_role_details`` as
they stood before the change.  For every state the root, the role store, the
name index and the role's document can be in, and every name and table asked
for, the check must return or raise what it did, log what it did, and leave in
Redis what it did.  Two fake Redis servers are given the same content, the old
check runs on one and the new on the other.

``test_an_access_check_is_made_on_one_read_or_on_five.py`` has the decision
between the two ways, ``test_suite/test_access_check_read.py`` repeats the
comparison on a real Redis, and
``tests/characterization/test_the_round_trips_of_a_write_to_redis.py`` runs
the real writer.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.rbac import access_control  # noqa: E402
from supertable.rbac.access_control import (  # noqa: E402
    _check_table_access,
    _normalize_tables,
    check_control_access,
    check_create_access,
    check_meta_access,
    check_rbac_access,
    check_write_access,
)
from supertable.rbac.permissions import Permission, RoleType, has_permission  # noqa: E402
from supertable.rbac.role_manager import RoleManager  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402

ORG, LAKE = "acme", "sales"
ROOT = RK.meta_root(ORG, LAKE)
ROLE_META = RK.rbac_role_meta(ORG, LAKE)
SUPERADMINS = RK.rbac_role_type_index(ORG, LAKE, "superadmin")
NAMES = RK.rbac_rolename_to_id(ORG, LAKE)
DOC_PREFIX = RK.rbac_role_doc_prefix(ORG, LAKE)
LOGGER = redis_catalog.logger.name
logger = access_control.logger

THE_ONE_READ = ("EVAL_RO", (ROLE_META, SUPERADMINS, NAMES, ROOT))
A_ROOT = '{"version": 3, "ts": 1794000000000}'


# ---------------------------------------------------------------------------
# The check as it was (rbac/access_control.py and get_role_details at 117bcf0)
# ---------------------------------------------------------------------------

def _get_role_details_as_it_was(self, org, sup, role_id):
    try:
        raw = self.r.hgetall(RK.rbac_role_doc(org, sup, role_id))
        if not raw:
            return None
        data = dict(raw)
        data.setdefault("role_id", role_id)
        for field in ("tables", "columns", "filters"):
            if field in data:
                try:
                    data[field] = json.loads(data[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return data
    except redis.RedisError as e:
        logger.error(f"[redis-catalog] get_role_details error: {e}")
    return None


def _resolve_role_as_it_was(role_manager, role_name):
    role_info = role_manager.get_role_by_name(role_name)
    if not role_info:
        logger.error(f"Role not found: {role_name}")
        raise PermissionError(f"Invalid or nonexistent role: {role_name}")
    enabled_val = role_info.get("enabled")
    if isinstance(enabled_val, bytes):
        enabled_val = enabled_val.decode("utf-8")
    if isinstance(enabled_val, str) and enabled_val.lower() in ("false", "0"):
        logger.warning(f"Role '{role_name}' is disabled")
        raise PermissionError(f"Role '{role_name}' is disabled.")
    if isinstance(enabled_val, bool) and not enabled_val:
        logger.warning(f"Role '{role_name}' is disabled")
        raise PermissionError(f"Role '{role_name}' is disabled.")
    return role_info


def _check_scope_access_as_it_was(super_name, organization, role_name, permission, label):
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role_as_it_was(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        logger.error(f"Role '{role_name}' has no role type")
        raise PermissionError(f"You don't have permission to {label}.")

    try:
        role_type = RoleType(role_type_str)
    except ValueError:
        logger.error(f"Role '{role_name}' has invalid role type: {role_type_str}")
        raise PermissionError(f"You don't have permission to {label}.")

    if not has_permission(role_type, permission):
        logger.error(f"Role '{role_name}' does not have {permission.name} permission.")
        raise PermissionError(f"You don't have permission to {label}.")

    return role_info


def _check_operation_access_as_it_was(super_name, organization, role_name, table_name,
                                      permission, label):
    role_info = _check_scope_access_as_it_was(
        super_name, organization, role_name, permission, label,
    )
    role_tables = _normalize_tables(role_info.get("tables", {}))
    _check_table_access(role_tables, table_name, label)


def _check_readonly_guard_as_it_was(super_name, organization, label):
    try:
        root = RedisCatalog().get_root(organization, super_name)
    except redis.RedisError:
        return  # Redis is down; the role lookup below will fail too.
    except PermissionError:
        raise
    except Exception as e:
        logger.error(
            "[readonly-guard] cannot determine read-only state of %s/%s: %s "
            "— refusing rather than assuming writable",
            organization, super_name, e,
        )
        raise PermissionError(
            f"Cannot verify whether this SuperTable is read-only. Cannot {label}."
        )

    if root and root.get("read_only"):
        clone_type = root.get("clone_type", "")
        if clone_type == "replica":
            reason = "a live replica"
        elif clone_type == "readonly":
            reason = "a read-only snapshot clone"
        elif root.get("cloned_from"):
            reason = "a read-only clone"
        else:
            reason = "locked"
        raise PermissionError(
            f"This SuperTable is {reason}. Cannot {label}."
        )


def _check_write_access_as_it_was(super_name, organization, role_name, table_name):
    _check_readonly_guard_as_it_was(super_name, organization, "write to this table")
    _check_operation_access_as_it_was(
        super_name, organization, role_name, table_name,
        Permission.WRITE, "write to this table",
    )


# ---------------------------------------------------------------------------
# Two fake Redis servers with the same content
# ---------------------------------------------------------------------------

#: The round trip that asks a Redis whether it runs a script sent as a read.
THE_QUESTION = ("EVAL_RO", ())


class _Side:
    """A fake Redis of its own, and the round trips made to it.

    ``redis_7``: the fake runs a script sent as a read (``EVAL_RO``), as
    Redis 7 does; without it the fake is what fakeredis is, a Redis that does
    not know the command.
    ``asked``: the catalog has already asked this Redis which of the two it is
    (once for each client, see ``RedisCatalog._runs_read_only_scripts``), so
    the round trips a test sees are those of the check alone.
    """

    def __init__(self, redis_7: bool = True, asked: bool = True):
        server = fakeredis.FakeServer()
        self.client = fakeredis.FakeRedis(server=server, decode_responses=True)
        self.bytes = fakeredis.FakeRedis(server=server)     # this test's own look at it
        self.trips = []
        self.fail = None        # a round trip that times out instead: called with its name and key
        self.asked = asked
        real = self.client.execute_command

        def command(*args, **options):
            name = str(args[0]).upper()
            if name in ("EVALSHA", "EVAL", "EVAL_RO"):
                trip = (name, tuple(args[3:3 + int(args[2])]))
                if name == "EVAL_RO" and redis_7:
                    # what Redis 7 does with it: the script is run, as a read
                    args = ("EVAL",) + tuple(args[1:])
            elif name.startswith("SCRIPT"):
                # a script this Redis had not seen: the EVALSHA before it was
                # refused, and is sent again once the script is loaded
                if self.trips and self.trips[-1][0] == "EVALSHA":
                    self.trips.pop()
                return real(*args, **options)
            else:
                trip = (name, args[1] if len(args) > 1 else None)
            self.trips.append(trip)
            if self.fail is not None and self.fail(*trip):
                raise redis.exceptions.TimeoutError("Timeout reading from socket")
            return real(*args, **options)

        self.client.execute_command = command

    def use(self, patched) -> None:
        """Every catalog built from here on talks to this Redis."""
        patched.setattr(redis_catalog, "RedisConnector",
                        lambda options=None: SimpleNamespace(r=self.client))

    def dump(self) -> dict:
        out = {}
        for key in sorted(self.bytes.keys("*")):
            kind = self.bytes.type(key)
            if kind == b"string":
                value = self.bytes.get(key)
            elif kind == b"hash":
                value = dict(sorted(self.bytes.hgetall(key).items()))
            elif kind == b"set":
                value = sorted(self.bytes.smembers(key))
            else:
                raise AssertionError(f"a {kind!r} at {key!r}")
            out[key.decode("utf-8")] = (kind.decode("ascii"), value)
        return out

    def load(self, content: dict) -> _Side:
        self.bytes.flushall()
        for key, (kind, value) in content.items():
            if kind == "string":
                self.bytes.set(key, value)
            elif kind == "hash":
                self.bytes.hset(key, mapping=value)
            else:
                self.bytes.sadd(key, *value)
        if self.asked:
            _catalog_on(self)._runs_read_only_scripts()
        self.trips.clear()
        return self


#: Roles as the library writes them, beside the superadmin it makes itself.
ROLES = [
    {"role": "admin", "role_name": "ops_admin",
     "tables": {"*": {"columns": ["*"], "filters": ["*"]}}},
    {"role": "writer", "role_name": "loader",
     "tables": {"*": {"columns": ["*"], "filters": ["*"]}}},
    {"role": "writer", "role_name": "Orders_Writer",
     "tables": {"orders": {"columns": ["*"], "filters": ["*"]},
                "items": {"columns": ["id", "sku"], "filters": ["*"]}}},
    {"role": "writer", "role_name": "writer_of_nothing", "tables": {}},
    {"role": "reader", "role_name": "analyst",
     "tables": {"orders": {"columns": ["id"], "filters": ["*"]}}},
    {"role": "meta", "role_name": "catalog_viewer",
     "tables": {"*": {"columns": ["*"], "filters": ["*"]}}},
]

_BUILT: dict = {}


def _lake() -> None:
    """Build, once, the content of a lake with a root and seven roles: the
    library writes it, so it is what a lake holds."""
    if not _BUILT:
        side = _Side()
        with pytest.MonkeyPatch.context() as patched:
            side.use(patched)
            manager = RoleManager(super_name=LAKE, organization=ORG)   # makes the superadmin
            for role in ROLES:
                manager._create_role(json.loads(json.dumps(role)))
        side.client.set(ROOT, A_ROOT)
        _BUILT["content"] = side.dump()
        _BUILT["ids"] = dict(side.client.hgetall(NAMES))


def _content() -> dict:
    """A fresh copy of the lake's content, as ``{key: (kind, value)}`` in bytes."""
    _lake()
    out = {}
    for key, (kind, value) in _BUILT["content"].items():
        if kind == "hash":
            value = dict(value)
        elif kind == "set":
            value = list(value)
        out[key] = (kind, value)
    return out


def _role_id(name: str) -> str:
    _lake()
    return _BUILT["ids"][name.lower()]


def _doc_key(name: str) -> str:
    return RK.rbac_role_doc(ORG, LAKE, _role_id(name))


# ---------------------------------------------------------------------------
# States: each takes the content of the lake and changes it
# ---------------------------------------------------------------------------

def _set(key, text):
    def change(content):
        content[key] = ("string", text if isinstance(text, bytes) else text.encode("utf-8"))
    return change


def _drop(*keys):
    def change(content):
        for key in keys:
            content.pop(key, None)
    return change


def _field(name, field, value):
    """Set, or with ``None`` remove, one field of a role's document."""
    def change(content):
        kind, fields = content[_doc_key(name)]
        if value is None:
            fields.pop(field.encode(), None)
        else:
            fields[field.encode()] = value.encode("utf-8")
    return change


def _nothing(content):
    return None


def _all(*changes):
    def change(content):
        for one in changes:
            one(content)
    return change


ROOTS = {
    "no root": _drop(ROOT),
    "a root": _nothing,
    "a root with clone flags, not read-only": _set(ROOT, json.dumps({
        "version": 7, "ts": 1, "read_only": False, "cloned_from": "source", "clone_type": "readonly"})),
    "locked": _set(ROOT, '{"version": 7, "read_only": true}'),
    "a live replica": _set(ROOT, json.dumps({
        "version": 7, "read_only": True, "clone_type": "replica", "cloned_from": "source",
        "replica_tables": ["orders"]})),
    "a read-only snapshot clone": _set(ROOT, json.dumps({
        "version": 7, "read_only": True, "clone_type": "readonly", "cloned_from": "source"})),
    "a read-only clone of no stated kind": _set(ROOT, json.dumps({
        "version": 7, "read_only": True, "cloned_from": "source"})),
    "read_only is 1": _set(ROOT, '{"read_only": 1}'),
    "read_only is the text false": _set(ROOT, '{"read_only": "false"}'),
    "read_only is 0": _set(ROOT, '{"read_only": 0}'),
    "read_only is empty text": _set(ROOT, '{"read_only": ""}'),
    "read_only is null": _set(ROOT, '{"read_only": null, "clone_type": "replica"}'),
    "read_only is a list": _set(ROOT, '{"read_only": [0]}'),
    "a root that is not JSON": _set(ROOT, "{not json"),
    "a root cut short": _set(ROOT, '{"version": 7, "read_only": tr'),
    "an empty root": _set(ROOT, ""),
    "a root of blanks": _set(ROOT, "   "),
    "a root that is null": _set(ROOT, "null"),
    "a root that is false": _set(ROOT, "false"),
    "a root that is 0": _set(ROOT, "0"),
    "a root that is an empty list": _set(ROOT, "[]"),
    "a root that is an empty object": _set(ROOT, "{}"),
    "a root that is a list": _set(ROOT, '[{"read_only": true}]'),
    "a root that is text": _set(ROOT, '"read_only"'),
    "a root that is a number": _set(ROOT, "7"),
    "a root that is true": _set(ROOT, "true"),
    "a root nested too deep to parse": _set(ROOT, "[" * 100_000),
    "a root that is no text": _set(ROOT, b"\xff\xfe{}"),
    "a root that is a hash": lambda content: content.__setitem__(ROOT, ("hash", {b"read_only": b"true"})),
}

STORES = {
    "a role store": _nothing,
    "no role store at all": lambda content: [content.pop(k) for k in list(content) if ":rbac:" in k],
    "no meta key": _drop(ROLE_META),
    "no superadmin listed": _drop(SUPERADMINS),
    "a superadmin index holding empty text": lambda content: content.__setitem__(
        SUPERADMINS, ("set", [b""])),
    "a superadmin index holding empty text beside the id": lambda content: content[SUPERADMINS][1].append(b""),
    "a meta key that is text": _set(ROLE_META, "initialized"),
    "a superadmin index that is text": _set(SUPERADMINS, "an id"),
}

#: What the name index says of the name ``loader``, and what is at its document.
LOOKUPS = {
    "the name and its document": _nothing,
    "no name index": _drop(NAMES),
    "the name is not in the index": lambda content: content[NAMES][1].pop(b"loader"),
    "the name stands for empty text": lambda content: content[NAMES][1].__setitem__(b"loader", b""),
    "the name stands for an id with no document": lambda content: content[NAMES][1].__setitem__(
        b"loader", b"0" * 32),
    "the name stands for an id that is no key segment": lambda content: content[NAMES][1].__setitem__(
        b"loader", b"an id: with a colon"),
    "the name stands for an id too long for a key": lambda content: content[NAMES][1].__setitem__(
        b"loader", b"a" * 65),
    "a name index that is text": _set(NAMES, "loader"),
    "a document that is text": lambda content: content.__setitem__(_doc_key("loader"), ("string", b"{}")),
    "a document that is a set": lambda content: content.__setitem__(_doc_key("loader"), ("set", [b"role"])),
}

#: Changes to the document of ``loader``, a writer of every table.
DOCUMENTS = {
    "as written": _nothing,
    "enabled false": _field("loader", "enabled", "false"),
    "enabled False": _field("loader", "enabled", "False"),
    "enabled 0": _field("loader", "enabled", "0"),
    "enabled true": _field("loader", "enabled", "true"),
    "enabled 1": _field("loader", "enabled", "1"),
    "enabled no": _field("loader", "enabled", "no"),
    "enabled empty": _field("loader", "enabled", ""),
    "no role type": _field("loader", "role", None),
    "an empty role type": _field("loader", "role", ""),
    "a role type nobody knows": _field("loader", "role", "owner"),
    "a role type in capitals": _field("loader", "role", "WRITER"),
    "become a reader": _field("loader", "role", "reader"),
    "become a meta role": _field("loader", "role", "meta"),
    "become an admin": _field("loader", "role", "admin"),
    "become a superadmin": _field("loader", "role", "superadmin"),
    "no tables": _field("loader", "tables", None),
    "tables an empty object": _field("loader", "tables", "{}"),
    "tables an old list of names": _field("loader", "tables", '["orders", "items"]'),
    "tables an old list of everything": _field("loader", "tables", '["*"]'),
    "tables an empty list": _field("loader", "tables", "[]"),
    "tables that are not JSON": _field("loader", "tables", "orders, items"),
    "tables null": _field("loader", "tables", "null"),
    "tables a number": _field("loader", "tables", "7"),
    "tables text": _field("loader", "tables", '"orders"'),
    "tables naming one table": _field("loader", "tables", '{"orders": {"columns": ["*"]}}'),
    "tables with an empty entry for the table": _field("loader", "tables", '{"orders": {}}'),
    "tables with a null entry and a default": _field(
        "loader", "tables", '{"orders": null, "*": {"columns": ["id"]}}'),
    "no role id in the document": _field("loader", "role_id", None),
    "columns and filters beside the tables": _all(
        _field("loader", "columns", '["id"]'), _field("loader", "filters", "not json")),
    "a document with one field": lambda content: content.__setitem__(
        _doc_key("loader"), ("hash", {b"note": b"left behind"})),
}

NAMES_ASKED = {
    "the name": "loader", "the name in capitals": "LOADER", "the name in mixed case": "LoAdEr",
    "a name written with capitals": "orders_writer", "that name as written": "Orders_Writer",
    "the superadmin": "superadmin", "an admin": "ops_admin", "a reader": "analyst",
    "a meta role": "catalog_viewer", "a writer of nothing": "writer_of_nothing",
    "a name nobody has": "nobody", "the name with blanks": " loader ", "no name": None,
    "an empty name": "", "a number": 7, "bytes": b"loader", "a list": ["loader"],
    "a name whose lower case is longer": "LOADERİ", "a name with a line break": "loader\n",
}

TABLES = ["orders", "items", "secrets", "*", ""]


def _build(*changes) -> dict:
    content = _content()
    for change in changes:
        change(content)
    return content


# ---------------------------------------------------------------------------
# Running one way and the other
# ---------------------------------------------------------------------------

_HEX = re.compile(r"[0-9a-f]{32}")


def _outcome(function, *args):
    try:
        return ("returned", function(*args))
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


def _logged(caplog, known_ids=()) -> list:
    """What was logged, with ids this run made up written as ``<new>``."""
    def text(message):
        return _HEX.sub(lambda m: m.group(0) if m.group(0) in known_ids else "<new>", message)
    return [(record.levelname, text(record.getMessage())) for record in caplog.records
            if record.name == LOGGER]


def _as_it_was(side, caplog, role_name, table):
    caplog.clear()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(RedisCatalog, "get_role_details", _get_role_details_as_it_was)
        outcome = _outcome(_check_write_access_as_it_was, LAKE, ORG, role_name, table)
    return outcome, _logged(caplog, _known_ids())


def _now(side, caplog, role_name, table):
    caplog.clear()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(check_write_access, LAKE, ORG, role_name, table)
    return outcome, _logged(caplog, _known_ids())


def _known_ids() -> set:
    _lake()
    return set(_BUILT["ids"].values()) | {"0" * 32}


def _named(trips) -> list:
    """Round trips, with the id of a role a run made up written as ``<new>``."""
    known = _known_ids()

    def text(key):
        if not isinstance(key, str):
            return key
        return _HEX.sub(lambda m: m.group(0) if m.group(0) in known else "<new>", key)

    return [(name, tuple(text(k) for k in key) if isinstance(key, tuple) else text(key))
            for name, key in trips]


def _left(side) -> dict:
    """What is in Redis, with what a run made up (ids, times) made comparable."""
    known = _known_ids()

    def text(value: bytes) -> str:
        return _HEX.sub(lambda m: m.group(0) if m.group(0) in known else "<new>",
                        value.decode("latin-1"))

    out = {}
    for key, (kind, value) in side.dump().items():
        if kind == "string":
            value = text(value)
        elif kind == "hash":
            value = {text(f): ("<a time>" if f.endswith(b"_ms") else text(v)) for f, v in value.items()}
        else:
            value = sorted(text(v) for v in value)
        out[text(key.encode("latin-1"))] = (kind, value)
    return out


def _both_ways(caplog, content, role_name, table):
    """The old check on one Redis and the new on another holding the same."""
    old, new = _Side().load(content), _Side().load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, role_name, table)
        now = _now(new, caplog, role_name, table)
    return SimpleNamespace(was=was, now=now, old=old, new=new)


#: The one line the new check may log that the old did not: Redis refused the
#: one read (a key of another kind), and the five reads followed.
READ_REFUSED = "[redis-catalog] rbac_access_read error: "


def _same(caplog, content, role_name, table, label=""):
    """Both ways end the same: what is returned or raised, what is logged,
    what is left in Redis.  ``read_refused`` says whether Redis refused the
    one read, which is logged, once, before everything the old check logs."""
    ran = _both_ways(caplog, content, role_name, table)
    assert ran.now[0] == ran.was[0], label
    logged = ran.now[1]
    ran.read_refused = bool(logged) and logged[0] == ("ERROR", logged[0][1]) \
        and logged[0][1].startswith(READ_REFUSED)
    assert (logged[1:] if ran.read_refused else logged) == ran.was[1], label
    assert _left(ran.new) == _left(ran.old), label
    return ran


# ---------------------------------------------------------------------------
# The check decides what it decided
# ---------------------------------------------------------------------------

def test_the_lake_these_tests_use_is_one_a_writer_may_write_to(caplog):
    """The comparison compares something: a grant, and each kind of refusal."""
    ran = _same(caplog, _build(), "loader", "orders")
    assert ran.now[0] == ("returned", None)
    assert ran.new.trips == [THE_ONE_READ]
    assert [trip[0] for trip in ran.old.trips] == ["GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"]

    refused = {
        ("analyst", "orders"): "You don't have permission to write to this table.",
        ("Orders_Writer", "secrets"): "You don't have permission to write to this table table 'secrets'.",
        ("nobody", "orders"): "Invalid or nonexistent role: nobody",
    }
    for (name, table), message in refused.items():
        ran = _same(caplog, _build(), name, table)
        assert ran.now[0] == ("raised", "PermissionError", message)


@pytest.mark.parametrize("root", list(ROOTS))
@pytest.mark.parametrize("name", ["loader", "analyst", "nobody"])
def test_every_root_is_read_as_the_guard_read_it(caplog, root, name):
    _same(caplog, _build(ROOTS[root]), name, "orders")


@pytest.mark.parametrize("root,reason", [
    ("locked", "locked"), ("a live replica", "a live replica"),
    ("a read-only snapshot clone", "a read-only snapshot clone"),
    ("a read-only clone of no stated kind", "a read-only clone"),
    ("read_only is 1", "locked"), ("read_only is the text false", "locked"),
    ("read_only is a list", "locked"),
])
def test_a_read_only_lake_refuses_the_write_with_its_reason(caplog, root, reason):
    ran = _same(caplog, _build(ROOTS[root]), "superadmin", "orders")

    assert ran.now[0] == ("raised", "PermissionError",
                          f"This SuperTable is {reason}. Cannot write to this table.")
    assert ran.new.trips == [THE_ONE_READ]
    assert ran.old.trips == [("GET", ROOT)], "and before, the role was never looked at"


@pytest.mark.parametrize("root", ["a root that is not JSON", "a root cut short", "a root of blanks",
                                  "a root nested too deep to parse"])
def test_a_root_that_cannot_be_read_as_a_document_refuses_the_write(caplog, root):
    """Fail closed, as the guard does: an unverifiable flag is not an absent one."""
    ran = _same(caplog, _build(ROOTS[root]), "superadmin", "orders")

    assert ran.now[0] == ("raised", "PermissionError",
                          "Cannot verify whether this SuperTable is read-only. Cannot write to this table.")
    assert ran.new.trips == [THE_ONE_READ]
    assert [level for level, _message in ran.now[1]] == ["ERROR"]
    assert ran.now[1][0][1].startswith("[readonly-guard] cannot determine read-only state of acme/sales: ")


@pytest.mark.parametrize("root", ["a root that is a list", "a root that is text",
                                  "a root that is a number", "a root that is true"])
def test_a_root_that_is_no_object_fails_as_it_did(caplog, root):
    """Not a refusal: the guard asks the document for a key, and these have none."""
    ran = _same(caplog, _build(ROOTS[root]), "superadmin", "orders")

    assert ran.now[0][:2] == ("raised", "AttributeError")
    assert ran.new.trips == [THE_ONE_READ]


@pytest.mark.parametrize("store", list(STORES))
@pytest.mark.parametrize("name", ["loader", "superadmin", "nobody", None])
def test_every_role_store_is_left_as_the_constructor_left_it(caplog, store, name):
    """A store the constructor has work to do on is still given to the
    constructor: both ways end with the same roles in Redis."""
    _same(caplog, _build(STORES[store]), name, "orders")


@pytest.mark.parametrize("store", ["no role store at all", "no meta key", "no superadmin listed",
                                   "a superadmin index holding empty text",
                                   "a superadmin index holding empty text beside the id"])
def test_a_store_that_is_not_initialised_is_left_to_the_five_reads(caplog, store):
    ran = _same(caplog, _build(STORES[store]), "superadmin", "orders")

    assert ran.new.trips[0] == THE_ONE_READ
    assert _named(ran.new.trips[1:]) == _named(ran.old.trips), \
        "then everything the check did before, from its start"
    assert len(ran.old.trips) >= 5


@pytest.mark.parametrize("lookup", list(LOOKUPS))
@pytest.mark.parametrize("table", ["orders", "secrets"])
def test_every_answer_of_the_name_index_is_taken_as_it_was(caplog, lookup, table):
    _same(caplog, _build(LOOKUPS[lookup]), "loader", table)


@pytest.mark.parametrize("lookup", ["the name stands for an id that is no key segment",
                                    "the name stands for an id too long for a key"])
def test_an_id_that_is_no_key_segment_fails_as_it_did(caplog, lookup):
    ran = _same(caplog, _build(LOOKUPS[lookup]), "loader", "orders")

    assert ran.now[0][:2] == ("raised", "ValueError")
    assert "Invalid Redis key segment for 'role_id'" in ran.now[0][2]


@pytest.mark.parametrize("document", list(DOCUMENTS))
@pytest.mark.parametrize("table", TABLES)
def test_every_role_document_is_judged_as_it_was(caplog, document, table):
    ran = _same(caplog, _build(DOCUMENTS[document]), "loader", table)

    assert ran.new.trips == [THE_ONE_READ]
    assert len(ran.old.trips) == 5


@pytest.mark.parametrize("asked", list(NAMES_ASKED))
@pytest.mark.parametrize("table", ["orders", "secrets"])
def test_every_name_asked_for_is_looked_up_as_it_was(caplog, asked, table):
    ran = _same(caplog, _build(), NAMES_ASKED[asked], table)

    assert ran.new.trips == [THE_ONE_READ]


def test_any_mix_of_states_is_decided_the_same(caplog):
    """Not only one thing wrong at a time."""
    pick = random.Random(20261125)
    roots, stores, lookups = list(ROOTS), list(STORES), list(LOOKUPS)
    documents, names = list(DOCUMENTS), list(NAMES_ASKED)
    grants = refusals = 0
    for _ in range(400):
        chosen = (pick.choice(roots), pick.choice(stores), pick.choice(lookups),
                  pick.choice(documents), pick.choice(names), pick.choice(TABLES))
        content = _content()
        for change in (DOCUMENTS[chosen[3]], LOOKUPS[chosen[2]], STORES[chosen[1]], ROOTS[chosen[0]]):
            try:
                change(content)
            except KeyError:
                pass    # an earlier change took away what this one would change
        ran = _same(caplog, content, NAMES_ASKED[chosen[4]], chosen[5], label=repr(chosen))
        grants += ran.now[0] == ("returned", None)
        refusals += ran.now[0][0] == "raised"
    assert grants > 5 and refusals > 100, (grants, refusals)


# ---------------------------------------------------------------------------
# One round trip, and nothing written
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,table", [("loader", "orders"), ("analyst", "orders"),
                                        ("nobody", "orders"), (None, "orders"), ("", "t")])
def test_the_check_is_one_script_over_four_keys(caplog, name, table):
    """Five before, three for a name that cannot be one: the name index and
    the document were never asked."""
    ran = _same(caplog, _build(), name, table)

    assert ran.new.trips == [THE_ONE_READ]
    assert len(ran.old.trips) == (5 if name == "loader" or name == "analyst" else
                                  4 if name == "nobody" else 3)


def test_the_check_writes_nothing():
    for change in list(ROOTS.values()) + list(LOOKUPS.values()) + list(DOCUMENTS.values()):
        content = _build(change)
        side = _Side().load(content)
        before = side.dump()
        with pytest.MonkeyPatch.context() as patched:
            side.use(patched)
            _outcome(check_write_access, LAKE, ORG, "loader", "orders")
        assert side.dump() == before


def test_the_script_names_no_command_that_changes_a_key():
    """``rbac/tests/test_rbac.py`` has a Redis of its own making that picks
    the script to stand in for by what the script's text mentions, and it
    replaces the connector for the rest of the process: a text naming those
    commands would be taken for a role's deletion, and run as one."""
    for text in (RedisCatalog._LUA_RBAC_ACCESS_READ, RedisCatalog._LUA_READ_ONLY_PROBE):
        for marker in ("DEL", "SREM", "HINCRBY", "HSET", "SADD", "SET'", "last_updated_ms",
                       "type_prefix", "cjson", "roles[#roles + 1]", "new_roles[#new_roles + 1]"):
            assert marker not in text, marker
        assert "pcall" not in text and "#!" not in text
    commands = re.findall(r"redis\.call\('([A-Z]+)'", RedisCatalog._LUA_RBAC_ACCESS_READ)
    assert commands == ["EXISTS", "SMEMBERS", "HGET", "HGETALL", "GET"]


def test_the_script_is_sent_as_a_read_and_in_full():
    """``EVAL_RO``, with the text of the script and not its hash.  Redis then
    takes it for the reads it is made of: it is answered while writes are
    paused whether or not Redis has seen the script, and for a user whose keys
    are read-only, as the five reads were; ``test_suite`` shows both on a real
    Redis.  The question that goes before it is sent the same way, so that a
    Redis which answers it runs the read."""
    side = _Side(asked=False).load(_build())
    sent = []
    real = side.client.execute_command

    def command(*args, **options):
        sent.append(args[:3])
        return real(*args, **options)

    side.client.execute_command = command
    _catalog_on(side).rbac_access_read(ORG, LAKE, "loader")

    assert sent == [("EVAL_RO", RedisCatalog._LUA_READ_ONLY_PROBE, 0),
                    ("EVAL_RO", RedisCatalog._LUA_RBAC_ACCESS_READ, 4)]
    assert RedisCatalog._LUA_READ_ONLY_PROBE == "return 1"


def test_the_document_key_the_script_builds_is_the_one_the_library_builds():
    for name in ("loader", "superadmin", "Orders_Writer"):
        role_id = _role_id(name)
        assert f"{DOC_PREFIX}{role_id}" == RK.rbac_role_doc(ORG, LAKE, role_id)
    assert DOC_PREFIX.endswith(":rbac:roles:doc:")


# ---------------------------------------------------------------------------
# The catalog's read, beside the five calls it stands for
# ---------------------------------------------------------------------------

def _five_calls(catalog: RedisCatalog, role_name):
    """What the access check asked the catalog, one call after another.  The
    superadmins are every id listed: the check took the first, and the order
    they are listed in is the client's and not Redis's to promise."""
    root = catalog.r.get(ROOT)
    meta = bool(catalog.r.exists(ROLE_META))
    superadmins = sorted(catalog.rbac_get_role_ids_by_type(ORG, LAKE, "superadmin"))
    role_id = catalog.rbac_get_role_id_by_name(ORG, LAKE, role_name)
    role = (_get_role_details_as_it_was(catalog, ORG, LAKE, role_id) or {}) if role_id else {}
    assert catalog.rbac_get_superadmin_role_id(ORG, LAKE) in (superadmins or [None])
    return root, meta, superadmins, role


def _one_read(catalog: RedisCatalog, role_name):
    root, meta, superadmins, role = catalog.rbac_access_read(ORG, LAKE, role_name)
    assert type(superadmins) is list
    return root, meta, sorted(superadmins), role


def _catalog_on(side) -> RedisCatalog:
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        return RedisCatalog()


_PLAIN = [state for state in ROOTS if state not in ("a root that is no text", "a root that is a hash")]


@pytest.mark.parametrize("root", _PLAIN)
def test_the_root_comes_back_as_the_text_that_is_stored(root):
    side = _Side().load(_build(ROOTS[root]))
    catalog = _catalog_on(side)

    assert _one_read(catalog, "loader") == _five_calls(catalog, "loader")


@pytest.mark.parametrize("asked", list(NAMES_ASKED))
@pytest.mark.parametrize("document", ["as written", "tables that are not JSON", "no role id in the document",
                                      "columns and filters beside the tables", "a document with one field"])
def test_the_role_comes_back_as_get_role_by_name_returns_it(asked, document):
    side = _Side().load(_build(DOCUMENTS[document]))
    catalog = _catalog_on(side)
    name = NAMES_ASKED[asked]

    read = catalog.rbac_access_read(ORG, LAKE, name)

    assert _one_read(catalog, name) == _five_calls(catalog, name)
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(RedisCatalog, "get_role_details", _get_role_details_as_it_was)
        manager = RoleManager(super_name=LAKE, organization=ORG, redis_catalog=catalog)
        assert read[3] == manager.get_role_by_name(name)
    assert type(read) is tuple and type(read[1]) is bool and type(read[3]) is dict
    assert read[2] == [_role_id("superadmin")]


@pytest.mark.parametrize("store,meta,listed", [
    ("a role store", True, ["the superadmin"]), ("no role store at all", False, []),
    ("no meta key", False, ["the superadmin"]), ("no superadmin listed", True, []),
    ("a superadmin index holding empty text", True, [""]),
    ("a superadmin index holding empty text beside the id", True, ["", "the superadmin"]),
    ("a meta key that is text", True, ["the superadmin"]),
])
def test_the_role_store_is_reported_as_it_is(store, meta, listed):
    side = _Side().load(_build(STORES[store]))
    catalog = _catalog_on(side)

    read = _one_read(catalog, "loader")

    assert read == _five_calls(catalog, "loader")
    assert read[1:3] == (meta, sorted(_role_id("superadmin") if i else i for i in listed))


@pytest.mark.parametrize("lookup", ["no name index", "the name is not in the index",
                                    "the name stands for empty text",
                                    "the name stands for an id with no document"])
def test_a_name_that_leads_to_no_document_is_no_role(lookup):
    side = _Side().load(_build(LOOKUPS[lookup]))
    catalog = _catalog_on(side)

    read = _one_read(catalog, "loader")

    assert read == _five_calls(catalog, "loader")
    assert read[3] == {}


def test_an_id_that_is_no_key_segment_raises_what_the_key_constructor_raises():
    side = _Side().load(_build(LOOKUPS["the name stands for an id that is no key segment"]))
    catalog = _catalog_on(side)

    with pytest.raises(ValueError, match="Invalid Redis key segment for 'role_id'"):
        catalog.rbac_access_read(ORG, LAKE, "loader")
    with pytest.raises(ValueError, match="Invalid Redis key segment for 'role_id'"):
        _five_calls(catalog, "loader")


@pytest.mark.parametrize("change", [
    ROOTS["a root that is a hash"], STORES["a superadmin index that is text"],
    LOOKUPS["a name index that is text"], LOOKUPS["a document that is text"],
    LOOKUPS["a document that is a set"],
], ids=["the root", "the superadmin index", "the name index", "the document as text", "the document as a set"])
def test_a_key_of_another_kind_fails_the_read_whole(caplog, change):
    """One script, one answer: a read Redis refuses is the read refused.  The
    five calls took each refusal their own way, which is why the check goes
    back to them when this read fails."""
    side = _Side().load(_build(change))
    catalog = _catalog_on(side)
    before = side.dump()

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.ResponseError):
            catalog.rbac_access_read(ORG, LAKE, "loader")

    assert side.dump() == before
    assert [r.getMessage()[:40] for r in caplog.records if r.name == LOGGER] == [
        "[redis-catalog] rbac_access_read error: "]


def test_a_redis_that_does_not_answer_is_logged_and_raised(caplog):
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    side.fail = lambda name, key: name == "EVAL_RO"

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.exceptions.TimeoutError):
            catalog.rbac_access_read(ORG, LAKE, "loader")

    assert [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER] == [
        ("ERROR", "[redis-catalog] rbac_access_read error: Timeout reading from socket")]


def _answering(side, reply, sent=None) -> None:
    """Have this Redis answer the access read with *reply*, whatever it holds;
    what the read was sent with goes into *sent*."""
    real = side.client.eval_ro

    def eval_ro(script, numkeys, *rest):
        if script != RedisCatalog._LUA_RBAC_ACCESS_READ:
            return real(script, numkeys, *rest)
        if sent is not None:
            sent.append((list(rest[:numkeys]), list(rest[numkeys:])))
        return reply

    side.client.eval_ro = eval_ro


@pytest.mark.parametrize("reply", [
    0, 1, None, "OK", [], [None, 1, [], None], [None, 1, [], None, [], None],
    [7, 1, [], None, []], [None, "1", [], None, []], [None, 1, "ids", None, []],
    [None, 1, [b"id"], None, []], [None, 1, [], 7, []], [None, 1, [], None, "role"],
    [None, 1, [], "id", ["role"]], [None, 1, [], "id", ["role", b"writer"]],
    [None, 1, [], "id", ["role", None]], {"root": None}, MagicMock(),
], ids=["zero", "one", "nothing", "text", "an empty list", "four values", "six values",
        "a root that is a number", "a count that is text", "ids that are text", "ids in bytes",
        "an id that is a number", "fields that are text", "a field without a value",
        "a value in bytes", "a value that is nothing", "a dict", "a mock"])
def test_an_answer_that_is_not_the_five_values_is_refused(caplog, reply):
    """What a stand-in for Redis answers that does not run the script."""
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    _answering(side, reply)

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        with pytest.raises(redis.ResponseError, match="unexpected reply from the access-read script"):
            catalog.rbac_access_read(ORG, LAKE, "loader")

    assert not [r for r in caplog.records if r.name == LOGGER], "no failure of Redis: nothing to log"


def test_the_refusal_of_an_answer_does_not_repeat_what_was_answered():
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    _answering(side, ["a root", 1, ["an id"], "id", ["secret"]])

    with pytest.raises(redis.ResponseError) as raised:
        catalog.rbac_access_read(ORG, LAKE, "loader")

    assert "secret" not in str(raised.value) and "a root" not in str(raised.value)


def test_the_read_sends_the_name_in_lower_case_and_the_four_keys():
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    sent = []
    _answering(side, [None, 0, [], None, []], sent)
    for name in ("LoAdEr", None, "", 7, b"loader"):
        catalog.rbac_access_read(ORG, LAKE, name)

    assert [keys for keys, _args in sent] == [[ROLE_META, SUPERADMINS, NAMES, ROOT]] * 5
    assert [args for _keys, args in sent] == [["loader", DOC_PREFIX]] + [["", DOC_PREFIX]] * 4, \
        "the name and the prefix of a role's document"


@pytest.mark.parametrize("reply,expected", [
    ([None, 1, ["id"], None, []], (None, True, ["id"], {})),
    ([False, 1, ["id"], False, []], (None, True, ["id"], {})),
    (["{}", 0, [], False, []], ("{}", False, [], {})),
    ((None, 1, ("id",), None, ()), (None, True, ["id"], {})),
], ids=["nothing where nothing is", "false where nothing is", "false for no id", "tuples for lists"])
def test_what_is_not_there_may_come_back_as_nothing_or_as_false(reply, expected):
    """A client that speaks the newer protocol is handed false for what a
    script found missing, where the older one is handed nothing."""
    side = _Side().load(_build())
    _answering(side, reply)

    assert _catalog_on(side).rbac_access_read(ORG, LAKE, "nobody") == expected


@pytest.mark.parametrize("reply", [[None, True, ["id"], None, []], [True, 1, ["id"], None, []],
                                   [None, 1, ["id"], True, []]],
                         ids=["a count that is true", "a root that is true", "an id that is true"])
def test_true_is_no_value_of_the_five(reply):
    side = _Side().load(_build())
    _answering(side, reply)

    with pytest.raises(redis.ResponseError, match="unexpected reply"):
        _catalog_on(side).rbac_access_read(ORG, LAKE, "nobody")


# ---------------------------------------------------------------------------
# The hash of a role, read by either reader
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fields", [
    {}, {"role": "writer"}, {"role": "writer", "role_id": "stored"},
    {"tables": '{"orders": {"columns": ["*"]}}', "columns": '["id"]', "filters": '[{"a": 1}]'},
    {"tables": "not json", "columns": "", "filters": "null"},
    {"tables": '["orders"]', "role_name": "Loader", "enabled": "false", "content_hash": "abc"},
    {"tables": "7", "note": '{"kept": "as text"}'},
], ids=["no hash", "one field", "its own id", "three JSON fields", "three that are not",
        "an old list", "a number and a note"])
def test_get_role_details_reads_a_hash_as_it_did(fields):
    side = _Side()
    role_id = "c" * 32
    if fields:
        side.client.hset(RK.rbac_role_doc(ORG, LAKE, role_id), mapping=fields)
    catalog = _catalog_on(side)

    now = catalog.get_role_details(ORG, LAKE, role_id)

    assert now == _get_role_details_as_it_was(catalog, ORG, LAKE, role_id)
    assert now is None or now["role_id"] == fields.get("role_id", role_id)
    assert RedisCatalog._role_from_hash(dict(fields), role_id) == now


def test_get_role_details_still_answers_nothing_when_redis_fails(caplog):
    side = _Side()
    side.client.hset(RK.rbac_role_doc(ORG, LAKE, "c" * 32), mapping={"role": "writer"})
    catalog = _catalog_on(side)
    side.fail = lambda name, key: name == "HGETALL"

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert catalog.get_role_details(ORG, LAKE, "c" * 32) is None

    assert [r.getMessage() for r in caplog.records if r.name == LOGGER] == [
        "[redis-catalog] get_role_details error: Timeout reading from socket"]


# ---------------------------------------------------------------------------
# RoleManager's part: is the store one the constructor would leave alone
# ---------------------------------------------------------------------------

def _read_with(side, name="loader", **options):
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        return RoleManager.read_for_access_check(super_name=LAKE, organization=ORG,
                                                 role_name=name, **options)


@pytest.mark.parametrize("asked", ["the name", "the name in capitals", "a name nobody has", "no name"])
def test_the_manager_hands_over_the_root_and_the_role(asked):
    side = _Side().load(_build())
    name = NAMES_ASKED[asked]

    answer = _read_with(side, name)

    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        manager = RoleManager(super_name=LAKE, organization=ORG)
        assert answer == (A_ROOT, manager.get_role_by_name(name))
    assert type(answer) is tuple and side.trips[0] == THE_ONE_READ


@pytest.mark.parametrize("store", ["no role store at all", "no meta key", "no superadmin listed",
                                   "a superadmin index holding empty text",
                                   "a superadmin index holding empty text beside the id"])
def test_a_store_the_constructor_would_work_on_is_not_answered_for(store):
    """And it is not worked on here: no instance is made, nothing is written."""
    side = _Side().load(_build(STORES[store]))
    before = side.dump()

    assert _read_with(side) is None

    assert side.dump() == before
    assert side.trips == [THE_ONE_READ]


@pytest.mark.parametrize("change", [
    ROOTS["a root that is a hash"], STORES["a superadmin index that is text"],
    LOOKUPS["a name index that is text"], LOOKUPS["a document that is text"],
    LOOKUPS["the name stands for an id that is no key segment"],
], ids=["the root", "the superadmin index", "the name index", "the document", "an id that is no segment"])
def test_a_read_that_fails_is_no_answer(change):
    side = _Side().load(_build(change))
    before = side.dump()

    assert _read_with(side) is None

    assert side.dump() == before


def test_a_redis_that_does_not_answer_is_no_answer():
    side = _Side().load(_build())
    side.fail = lambda name, key: True

    assert _read_with(side) is None

    assert side.trips == [THE_ONE_READ]


@pytest.mark.parametrize("answer", [
    None, MagicMock(), [A_ROOT, True, ["id"], {}], (A_ROOT, True, ["id"]), (A_ROOT, True, ["id"], {}, 1),
    (7, True, ["id"], {}), (b"{}", True, ["id"], {}), (A_ROOT, True, ["id"], None),
    (A_ROOT, True, ["id"], [("role", "writer")]), (A_ROOT, True, ["id"], MagicMock()),
    (A_ROOT, False, ["id"], {}), (A_ROOT, 0, ["id"], {}), (A_ROOT, None, ["id"], {}),
    (A_ROOT, True, [], {}), (A_ROOT, True, None, {}), (A_ROOT, True, [""], {}),
    (A_ROOT, True, ["id", ""], {}), (A_ROOT, True, ["", "id"], {}), (A_ROOT, True, "id", {}),
    (A_ROOT, True, ("id",), {}), (A_ROOT, True, {"id"}, {}), (A_ROOT, True, [7], {}),
    (A_ROOT, True, [b"id"], {}), (A_ROOT, True, [None], {}), (A_ROOT, True, MagicMock(), {}),
], ids=["nothing", "a mock", "a list", "three values", "five values", "a root that is a number",
        "a root in bytes", "no role", "a role in pairs", "a role that is a mock", "no meta key",
        "a meta count of zero", "a meta key that is nothing", "no superadmin", "superadmins that are nothing",
        "a superadmin that is empty text", "an empty id after the superadmin",
        "an empty id before the superadmin", "superadmins as text", "superadmins as a tuple",
        "superadmins as a set", "a superadmin that is a number", "a superadmin in bytes",
        "a superadmin that is nothing", "superadmins that are a mock"])
def test_a_catalog_whose_answer_is_not_the_four_values_is_no_answer(answer):
    catalog = MagicMock()
    catalog.rbac_access_read.return_value = answer

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None

    catalog.rbac_access_read.assert_called_once_with(ORG, LAKE, "loader", with_root=True)
    assert [call[0] for call in catalog.method_calls] == ["rbac_access_read"], "and nothing else was asked"


@pytest.mark.parametrize("asked,sent", [(True, True), (False, False), (1, True), (0, False), (None, False)])
def test_the_manager_passes_on_whether_the_root_is_wanted(asked, sent):
    catalog = MagicMock()
    catalog.rbac_access_read.return_value = (None, True, ["id"], {"role": "reader"})

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="analyst", redis_catalog=catalog, with_root=asked,
    ) == (None, {"role": "reader"})

    catalog.rbac_access_read.assert_called_once_with(ORG, LAKE, "analyst", with_root=sent)


def test_two_superadmins_listed_are_as_good_as_one():
    """One is the rule, and the constructor takes the first of what it finds:
    with nothing empty among them, whichever comes first is one."""
    catalog = MagicMock()
    catalog.rbac_access_read.return_value = (A_ROOT, 1, ["one", "another"], {"role": "writer"})

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog,
    ) == (A_ROOT, {"role": "writer"})


@pytest.mark.parametrize("root", [None, "", A_ROOT, "{not json"])
def test_a_root_of_any_text_or_none_is_handed_over_untouched(root):
    catalog = MagicMock()
    catalog.rbac_access_read.return_value = (root, True, ["id"], {"role": "writer"})

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog,
    ) == (root, {"role": "writer"})


def test_a_catalog_without_the_read_is_no_answer():
    catalog = SimpleNamespace(r=MagicMock())

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None

    assert catalog.r.method_calls == []


@pytest.mark.parametrize("failure", [
    redis.exceptions.ConnectionError("gone"), redis.ResponseError("WRONGTYPE"),
    ValueError("Invalid Redis key segment"), AttributeError("eval_ro"),
    RuntimeError("anything"),
], ids=["no connection", "a refusal", "an id that is no segment", "a catalog half built", "anything"])
def test_a_catalog_whose_read_raises_is_no_answer(failure):
    catalog = MagicMock()
    catalog.rbac_access_read.side_effect = failure

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None


def test_a_catalog_that_cannot_be_built_is_no_answer(monkeypatch):
    def unreachable(options=None):
        raise redis.exceptions.ConnectionError("no sentinel answers")

    monkeypatch.setattr(redis_catalog, "RedisConnector", unreachable)

    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader") is None


def test_the_manager_builds_its_own_catalog_when_given_none():
    side = _Side().load(_build())

    assert _read_with(side)[0] == A_ROOT
    assert _read_with(side, redis_catalog=None)[1]["role"] == "writer"


# ---------------------------------------------------------------------------
# When the one read is not to be had
# ---------------------------------------------------------------------------

def _failing(*names):
    return lambda name, key: name in names


@pytest.mark.parametrize("state", ["a root", "locked", "a root that is not JSON"])
@pytest.mark.parametrize("name", ["loader", "analyst", "nobody"])
def test_a_read_that_fails_is_followed_by_the_five_as_they_were(caplog, state, name):
    content = _build(ROOTS[state])
    old, new = _Side().load(content), _Side().load(content)
    new.fail = _failing("EVAL_RO")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, name, "orders")
        now = _now(new, caplog, name, "orders")

    assert now[0] == was[0]
    assert now[1] == [("ERROR", "[redis-catalog] rbac_access_read error: Timeout reading from socket")] + was[1]
    assert new.trips == [THE_ONE_READ] + old.trips
    assert new.dump() == old.dump()


def test_a_read_that_fails_does_not_let_a_write_into_a_locked_lake(caplog):
    """The guard reads the root itself then, and the lake is as locked as it was."""
    side = _Side().load(_build(ROOTS["locked"]))
    side.fail = _failing("EVAL_RO")
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        outcome, _logs = _now(side, caplog, "superadmin", "orders")

    assert outcome == ("raised", "PermissionError", "This SuperTable is locked. Cannot write to this table.")
    assert side.trips == [THE_ONE_READ, ("GET", ROOT)]


@pytest.mark.parametrize("also", ["GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"])
@pytest.mark.parametrize("state", ["a root", "locked"])
def test_one_of_the_five_failing_after_it_ends_as_it_did(caplog, also, state):
    """With the one read gone, each of the five is still the read it was, and
    a failure of it still ends the check the way that read's failure did."""
    content = _build(ROOTS[state])
    old, new = _Side().load(content), _Side().load(content)
    old.fail = _failing(also)
    new.fail = _failing("EVAL_RO", also)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        now = _now(new, caplog, "loader", "orders")

    assert now[0] == was[0]
    assert now[1][1:] == was[1]
    assert new.trips == [THE_ONE_READ] + old.trips


def test_a_redis_that_answers_nothing_fails_the_check_as_it_did(caplog):
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    old.fail = new.fail = lambda name, key: True
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        now = _now(new, caplog, "loader", "orders")

    assert now[0] == was[0] == ("raised", "TimeoutError", "Timeout reading from socket")
    assert new.trips == [THE_ONE_READ, ("GET", ROOT), ("EXISTS", ROLE_META)]
    assert old.trips == [("GET", ROOT), ("EXISTS", ROLE_META)]


@pytest.mark.parametrize("table,expected", [
    ("orders", ("returned", None)),
    ("secrets", ("raised", "PermissionError",
                 "You don't have permission to write to this table table 'secrets'.")),
])
def test_a_manager_put_in_its_place_is_asked_as_before(table, expected):
    """How the tests of the access check give it a role store of their own:
    the one read is not asked for, and the manager they made is."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        manager = RoleManager(super_name=LAKE, organization=ORG)
        side.trips.clear()
        with patch("supertable.rbac.access_control.RoleManager", return_value=manager) as replaced:
            outcome = _outcome(check_write_access, LAKE, ORG, "Orders_Writer", table)

    assert outcome == expected
    replaced.assert_called_once_with(super_name=LAKE, organization=ORG)
    assert side.trips == [("GET", ROOT), ("HGET", NAMES), ("HGETALL", _doc_key("Orders_Writer"))]


def test_a_catalog_put_in_its_place_is_asked_for_the_root_as_before():
    """The guard's own collaborator: a catalog replaced there still decides."""
    side = _Side().load(_build())
    stand_in = MagicMock()
    stand_in.get_root.return_value = {"read_only": True, "clone_type": "replica"}
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.access_control.RedisCatalog", return_value=stand_in):
            outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError",
                       "This SuperTable is a live replica. Cannot write to this table.")
    stand_in.get_root.assert_called_once_with(ORG, LAKE)
    assert side.trips == [], "the role was not looked at, and no script was sent"


@pytest.mark.parametrize("missing", ["the catalog's read", "the manager's read"])
def test_a_library_without_the_read_checks_as_before(caplog, missing):
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        with pytest.MonkeyPatch.context() as patched:
            if missing == "the catalog's read":
                patched.delattr(RedisCatalog, "rbac_access_read")
            else:
                patched.delattr(RoleManager, "read_for_access_check")
            now = _now(new, caplog, "loader", "orders")

    assert now == was == (("returned", None), [])
    assert new.trips == old.trips and len(old.trips) == 5


def test_a_redis_stand_in_that_answers_a_script_with_zero_checks_as_before(caplog):
    """A stand-in for Redis that answers 0 to a script it does not know, as
    the hand-written one of ``rbac/tests/test_rbac.py`` answers its scripts.
    It is asked whether it runs a script sent as a read, answers 0, and is
    never sent the read."""
    content = _build()
    old, new = _Side().load(content), _Side(redis_7=False, asked=False).load(content)
    sent = []

    def eval_ro(script, numkeys, *rest):
        sent.append(script)
        return 0

    new.client.eval_ro = eval_ro
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "analyst", "orders")
        now = _now(new, caplog, "analyst", "orders")
        again = _now(new, caplog, "analyst", "orders")

    assert now == again == was
    assert now[0] == ("raised", "PermissionError", "You don't have permission to write to this table.")
    assert new.trips == old.trips * 2
    assert sent == [RedisCatalog._LUA_READ_ONLY_PROBE], "asked once, and the read was never sent"


def test_a_redis_of_someone_s_own_making_without_the_command_checks_as_before(caplog):
    """The hand-written Redis of ``rbac/tests/test_rbac.py`` has no such
    method at all.  Nothing is sent, nothing is logged, and nothing is kept:
    there is no answer to keep."""
    content = _build()
    old, new = _Side().load(content), _Side(redis_7=False, asked=False).load(content)
    new.client.eval_ro = None
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        now = _now(new, caplog, "loader", "orders")

    assert now == was == (("returned", None), [])
    assert new.trips == old.trips
    assert new.client not in redis_catalog._RUNS_READ_ONLY_SCRIPTS


# ---------------------------------------------------------------------------
# A Redis that does not run a script sent as a read
# ---------------------------------------------------------------------------

FIVE = ["GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"]


def test_a_redis_before_7_is_asked_once_and_then_the_five_reads_as_before(caplog):
    """Nothing is logged: a Redis that does not run the script is no failure."""
    content = _build()
    old, new = _Side().load(content), _Side(redis_7=False, asked=False).load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        first = _now(new, caplog, "loader", "orders")
        first_trips = list(new.trips)
        new.trips.clear()
        second = _now(new, caplog, "analyst", "orders")

    assert first == was == (("returned", None), [])
    assert first_trips == [THE_QUESTION] + old.trips, "asked, refused, and the five reads followed"
    assert second == (("raised", "PermissionError", "You don't have permission to write to this table."),
                      [("ERROR", "Role 'analyst' does not have WRITE permission.")])
    assert [trip[0] for trip in new.trips] == FIVE, "and it is not asked again"
    assert new.dump() == old.dump()


@pytest.mark.parametrize("root", ["locked", "a root that is not JSON", "a root"])
@pytest.mark.parametrize("name", ["loader", "analyst", "nobody", None])
def test_a_redis_before_7_decides_what_it_decided(caplog, root, name):
    content = _build(ROOTS[root])
    old, new = _Side().load(content), _Side(redis_7=False).load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, name, "orders")
        now = _now(new, caplog, name, "orders")

    assert now == was
    assert new.trips == old.trips and THE_ONE_READ not in new.trips
    assert new.dump() == old.dump()


def test_a_redis_7_is_asked_once_for_each_client():
    side, other = _Side(asked=False).load(_build()), _Side(asked=False).load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        for _ in range(3):
            assert _outcome(check_write_access, LAKE, ORG, "loader", "orders") == ("returned", None)
        other.use(patched)
        assert _outcome(check_write_access, LAKE, ORG, "loader", "orders") == ("returned", None)

    assert side.trips == [THE_QUESTION, THE_ONE_READ, THE_ONE_READ, THE_ONE_READ]
    assert other.trips == [THE_QUESTION, THE_ONE_READ], "another client, another Redis: its own answer"


def test_the_read_itself_says_so_and_sends_nothing(caplog):
    side = _Side(redis_7=False).load(_build())
    catalog = _catalog_on(side)

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        with pytest.raises(NotImplementedError, match="sent as a read \\(EVAL_RO\\), which this Redis does not run"):
            catalog.rbac_access_read(ORG, LAKE, "loader")

    assert side.trips == [] and not [r for r in caplog.records if r.name == LOGGER]
    assert _read_with(side) is None


def _asking(answer):
    """A catalog whose Redis answers the question with *answer* (raised, if an exception)."""
    side = _Side(redis_7=False, asked=False).load(_build())
    catalog = _catalog_on(side)
    asked = []

    def eval_ro(script, numkeys, *rest):
        asked.append((script, numkeys, rest))
        if isinstance(answer, BaseException):
            raise answer
        return answer

    side.client.eval_ro = eval_ro
    return side, catalog, asked


ASKED_ONCE = [("return 1", 0, ())]


NOT_YES = [0, 2, None, "1", True, 1.0, [1], b"1", MagicMock(),
           redis.ResponseError("unknown command 'EVAL_RO', with args beginning with: 'return 1' '0'"),
           redis.ResponseError("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."),
           redis.ResponseError("MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."),
           redis.exceptions.NoPermissionError("this user has no permissions to run the 'eval_ro' command")]
NOT_YES_IDS = ["zero", "two", "nothing", "text", "true", "a float", "a list", "bytes", "a mock",
               "a Redis that does not know the command", "a Redis busy with another script",
               "a replica that is still syncing", "a user that may not run scripts"]


@pytest.mark.parametrize("answer", NOT_YES, ids=NOT_YES_IDS)
def test_an_answer_that_is_not_1_is_no_and_is_kept_for_a_minute(caplog, monkeypatch, answer):
    """Kept, so that a Redis that will never say yes is not asked at every
    check; for a minute, because a refusal can be of the moment, and a process
    is not to go without the one read for the rest of its life for one."""
    side, catalog, asked = _asking(answer)
    clock = {"now": 1_000.0}
    monkeypatch.setattr(redis_catalog, "_monotonic", lambda: clock["now"])

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        assert catalog._runs_read_only_scripts() is False
        assert catalog._runs_read_only_scripts() is False
        assert _catalog_on(side)._runs_read_only_scripts() is False, "another catalog on the same client"
        clock["now"] += redis_catalog._ASK_AGAIN_AFTER_S - 0.001
        assert catalog._runs_read_only_scripts() is False
        assert asked == ASKED_ONCE, "asked once, with no key and no argument"

        clock["now"] += 0.001
        assert catalog._runs_read_only_scripts() is False
        assert asked == ASKED_ONCE * 2, "a minute on, it is asked again"
        assert catalog._runs_read_only_scripts() is False
        assert asked == ASKED_ONCE * 2

    assert not [r for r in caplog.records if r.name == LOGGER]
    assert redis_catalog._ASK_AGAIN_AFTER_S == 60.0


def test_a_redis_that_says_no_and_then_yes_is_taken_at_its_new_word(monkeypatch):
    """The refusal of a moment: the one read is back with the next question."""
    side = _Side(asked=False).load(_build())
    catalog = _catalog_on(side)
    clock = {"now": 5.0}
    monkeypatch.setattr(redis_catalog, "_monotonic", lambda: clock["now"])
    real, busy = side.client.eval_ro, {"busy": True}

    def eval_ro(script, numkeys, *rest):
        if busy["busy"]:
            raise redis.ResponseError("BUSY Redis is busy running a script.")
        return real(script, numkeys, *rest)

    side.client.eval_ro = eval_ro
    assert catalog._runs_read_only_scripts() is False
    busy["busy"] = False
    assert catalog._runs_read_only_scripts() is False, "still within the minute"
    with pytest.raises(NotImplementedError):
        catalog.rbac_access_read(ORG, LAKE, "loader")

    clock["now"] += redis_catalog._ASK_AGAIN_AFTER_S
    assert catalog._runs_read_only_scripts() is True
    assert catalog.rbac_access_read(ORG, LAKE, "loader")[3]["role"] == "writer"


def test_the_answer_1_is_yes_and_is_kept(monkeypatch):
    side, catalog, asked = _asking(1)
    clock = {"now": 0.0}
    monkeypatch.setattr(redis_catalog, "_monotonic", lambda: clock["now"])

    assert catalog._runs_read_only_scripts() is True
    clock["now"] += 10 * redis_catalog._ASK_AGAIN_AFTER_S
    assert catalog._runs_read_only_scripts() is True
    assert asked == ASKED_ONCE, "a yes is not asked for again"


@pytest.mark.parametrize("failure", [redis.exceptions.TimeoutError("Timeout reading from socket"),
                                     redis.exceptions.ConnectionError("Connection refused")],
                         ids=["a timeout", "no connection"])
def test_a_question_redis_did_not_hear_is_no_answer_and_is_asked_again(caplog, failure):
    side, catalog, asked = _asking(failure)

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(type(failure)):
            catalog.rbac_access_read(ORG, LAKE, "loader")
        with pytest.raises(type(failure)):
            catalog.rbac_access_read(ORG, LAKE, "loader")

    assert len(asked) == 2, "nothing was remembered"
    assert [r.getMessage() for r in caplog.records if r.name == LOGGER] == [
        f"[redis-catalog] rbac_access_read error: {failure}"] * 2
    assert side.trips == [], "and the read was not sent"


def test_a_redis_that_could_not_be_asked_is_followed_by_the_five_reads_and_asked_next_time(caplog):
    content = _build()
    old, new = _Side().load(content), _Side(asked=False).load(content)
    failed = []
    new.fail = lambda name, key: name == "EVAL_RO" and not failed and not failed.append(1)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        now = _now(new, caplog, "loader", "orders")
        first_trips = list(new.trips)
        new.trips.clear()
        again = _now(new, caplog, "loader", "orders")

    assert now[0] == again[0] == was[0] == ("returned", None)
    assert first_trips == [THE_QUESTION] + old.trips
    assert new.trips == [THE_QUESTION, THE_ONE_READ]


def test_a_client_that_cannot_be_remembered_is_not_asked():
    """Its answer could not be kept, and it would be asked at every check."""
    sent = []

    class Unhashable:
        __hash__ = None

        def register_script(self, text):
            return lambda keys=None, args=None: 1

        def eval_ro(self, script, numkeys, *rest):
            sent.append(script)
            return 1

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=Unhashable()))
        catalog = RedisCatalog()

    assert catalog._runs_read_only_scripts() is False
    assert catalog._does_not_run_read_only_scripts_for_now() is None, "nor can a no be kept: no failure"
    with pytest.raises(NotImplementedError):
        catalog.rbac_access_read(ORG, LAKE, "loader")
    assert sent == []


def test_what_is_remembered_goes_with_the_client():
    """Kept for as long as the client lives, and no longer: nothing piles up
    in a process that makes clients and lets them go."""
    import gc

    gc.collect()
    before = len(redis_catalog._RUNS_READ_ONLY_SCRIPTS)
    sides = [_Side().load(_build()) for _ in range(5)]
    assert len(redis_catalog._RUNS_READ_ONLY_SCRIPTS) == before + 5
    assert all(redis_catalog._RUNS_READ_ONLY_SCRIPTS[side.client][0] is True for side in sides)

    del sides
    gc.collect()

    assert len(redis_catalog._RUNS_READ_ONLY_SCRIPTS) == before


# ---------------------------------------------------------------------------
# Subclasses keep their own reads
# ---------------------------------------------------------------------------

class _ReadsTheRootItsOwnWay(RedisCatalog):
    """A catalog a host might make: the root comes from somewhere else."""

    def get_root(self, org, sup):
        return {"read_only": True, "clone_type": "readonly"}


class _DefinesTheRead(RedisCatalog):
    """A subclass that says what the one read answers."""

    def rbac_access_read(self, org, sup, role_name, with_root=True):
        return ('{"read_only": true}', True, ["an id"], {"role": "writer", "tables": {"*": {}}})


def test_a_subclass_of_the_catalog_is_asked_the_five_reads_it_may_have_changed():
    """The one read stands for five reads of ``RedisCatalog`` itself.  A
    subclass may make any of them differently, so it is not asked for the one:
    a root it reads its own way still decides."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        catalog = _ReadsTheRootItsOwnWay()
        with pytest.raises(NotImplementedError, match="five reads of RedisCatalog as it defines them"):
            catalog.rbac_access_read(ORG, LAKE, "loader")
        assert RoleManager.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None
        assert side.trips == [], "nothing was sent"
        with patch("supertable.rbac.access_control.RedisCatalog", _ReadsTheRootItsOwnWay):
            outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError",
                       "This SuperTable is a read-only snapshot clone. Cannot write to this table.")
    assert side.trips == [], "its own root refused the write before the role was looked at"


def test_a_subclass_that_defines_the_read_is_asked_for_it():
    """Put where the access check builds its catalog and where a manager
    builds its own: one class for the root and the role, as the read needs."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.access_control.RedisCatalog", _DefinesTheRead), \
                patch("supertable.rbac.role_manager.RedisCatalog", _DefinesTheRead):
            outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError", "This SuperTable is locked. Cannot write to this table.")
    assert side.trips == []


# ---------------------------------------------------------------------------
# The guard's catalog and a manager's: one class, or each is asked its own
# ---------------------------------------------------------------------------

class _NoNameIsARole(RedisCatalog):
    """A catalog a test might put where a manager builds its own: no name
    stands for a role there."""

    def rbac_get_role_id_by_name(self, org, sup, role_name):
        return None


def test_a_catalog_in_the_manager_s_place_alone_is_asked_for_the_role():
    """The guard read the root through the access check's catalog, and the
    role came from the one a manager builds for itself.  The one read would
    have asked the access check's for both, and found the role there."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.role_manager.RedisCatalog", _NoNameIsARole):
            outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError", "Invalid or nonexistent role: loader")
    assert [trip[0] for trip in side.trips] == ["GET", "EXISTS", "SMEMBERS"], \
        "the root by the guard, the store by the constructor, and a lookup that asks Redis nothing"


def test_a_catalog_in_the_guard_s_place_alone_is_asked_for_the_root():
    """The other way round: a catalog that would answer the one read for both
    is asked for the root, and the role comes from a manager's own catalog."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with patch("supertable.rbac.access_control.RedisCatalog", _DefinesTheRead):
            outcome = _outcome(check_write_access, LAKE, ORG, "analyst", "orders")

    assert outcome == ("raised", "PermissionError", "You don't have permission to write to this table.")
    assert [trip[0] for trip in side.trips] == FIVE, "its read of both was not asked for"


class _LooksRolesUpItsOwnWay(RoleManager):
    """A manager a host might make: nobody is a role here."""

    def get_role_by_name(self, role_name):
        return {}


def test_a_subclass_of_the_manager_is_asked_the_lookup_it_may_have_changed():
    """As with the catalog: the one read stands for what ``RoleManager``
    itself does when it is constructed and asked for a role."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert _LooksRolesUpItsOwnWay.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="loader") is None
        assert side.trips == [], "nothing was sent"
        assert RoleManager.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="loader")[1]["role"] == "writer"
        side.trips.clear()
        with patch("supertable.rbac.access_control.RoleManager", _LooksRolesUpItsOwnWay):
            outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError", "Invalid or nonexistent role: loader")
    assert [trip[0] for trip in side.trips] == ["GET", "EXISTS", "SMEMBERS"], \
        "the guard, the constructor, and its own lookup, which asks Redis nothing"


# ---------------------------------------------------------------------------
# A read someone has replaced is the read that is asked
# ---------------------------------------------------------------------------

STANDS_FOR = ["get_root", "rbac_get_role_ids_by_type", "rbac_get_superadmin_role_id",
              "rbac_get_role_id_by_name", "get_role_details", "_role_from_hash", "_decode_member"]
#: Those of them that take no catalog: replaced, they are put there as they are.
NO_SELF = ("_role_from_hash", "_decode_member")


@pytest.mark.parametrize("name", STANDS_FOR)
@pytest.mark.parametrize("where", ["on the class", "on the object"])
def test_a_catalog_with_one_of_its_reads_replaced_is_not_asked_for_the_one(monkeypatch, name, where):
    """The one read stands for these reads as the class defines them.  A
    replacement of one of them, however it reads, would not be asked."""
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    original = getattr(RedisCatalog, name)
    if where == "on the class":
        another = lambda *a, **k: original(*a, **k)     # noqa: E731 - the same read, by another function
        monkeypatch.setattr(RedisCatalog, name, staticmethod(another) if name in NO_SELF else another)
    else:
        bound = original if name in NO_SELF else (lambda *a, **k: original(catalog, *a, **k))
        setattr(catalog, name, bound)

    assert catalog._reads_are_its_own() is False
    with pytest.raises(NotImplementedError, match="five reads of RedisCatalog as it defines them"):
        catalog.rbac_access_read(ORG, LAKE, "loader")
    assert side.trips == [], "nothing was sent"
    assert RoleManager.read_for_access_check(
        super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None


def test_a_catalog_as_the_library_defines_it_is_asked_for_the_one():
    side = _Side().load(_build())
    catalog = _catalog_on(side)

    assert catalog._reads_are_its_own() is True
    assert sorted(redis_catalog._ACCESS_READ_STANDS_FOR) == sorted(STANDS_FOR)


def test_ids_decoded_another_way_are_the_ids_the_check_looks_up(monkeypatch):
    """The lookups hand every id through the catalog's decoding.  With another
    put in its place, the role a name stands for is the one it says."""
    side = _Side().load(_build())
    loader, analyst = _role_id("loader"), _role_id("analyst")
    monkeypatch.setattr(RedisCatalog, "_decode_member",
                        staticmethod(lambda member: analyst if member == loader else member))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError", "You don't have permission to write to this table.")
    assert [trip[0] for trip in side.trips] == FIVE, "the reader's document, by the five reads"


def test_a_root_read_put_in_the_place_of_the_catalog_s_decides_the_write(monkeypatch):
    """What a test does to make a lake read-only without touching Redis."""
    side = _Side().load(_build())
    monkeypatch.setattr(RedisCatalog, "get_root", lambda self, org, sup: {"read_only": True})
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("raised", "PermissionError", "This SuperTable is locked. Cannot write to this table.")
    assert side.trips == [], "neither the one read nor the root was asked of Redis"


@pytest.mark.parametrize("name,replacement,expected,trips", [
    ("get_role_by_name", lambda self, role_name: {},
     ("raised", "PermissionError", "Invalid or nonexistent role: loader"), ["GET", "EXISTS", "SMEMBERS"]),
    ("get_role", lambda self, role_id: {"role": "reader", "tables": {"*": {}}},
     ("raised", "PermissionError", "You don't have permission to write to this table."),
     ["GET", "EXISTS", "SMEMBERS", "HGET"]),
    ("_init_role_storage", lambda self: None, ("returned", None), ["GET", "HGET", "HGETALL"]),
], ids=["the lookup by name", "the lookup by id", "the check of the role store"])
def test_a_manager_with_one_of_its_reads_replaced_is_asked_that_read(monkeypatch, name, replacement,
                                                                     expected, trips):
    side = _Side().load(_build())
    monkeypatch.setattr(RoleManager, name, replacement)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert RoleManager.read_for_access_check(super_name=LAKE, organization=ORG, role_name="loader") is None
        assert side.trips == [], "nothing was sent"
        outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == expected
    assert [trip[0] for trip in side.trips] == trips


def test_a_manager_whose_constructor_was_replaced_is_constructed(monkeypatch):
    side = _Side().load(_build())
    built = []
    original = RoleManager.__init__

    def watching(self, *args, **options):
        built.append(options)
        original(self, *args, **options)

    monkeypatch.setattr(RoleManager, "__init__", watching)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert _outcome(check_write_access, LAKE, ORG, "loader", "orders") == ("returned", None)

    assert built == [{"super_name": LAKE, "organization": ORG}]
    assert [trip[0] for trip in side.trips] == FIVE


def test_the_manager_as_the_library_defines_it_is_asked_for_the_one():
    from supertable.rbac import role_manager

    assert sorted(role_manager._ACCESS_READ_STANDS_FOR) == [
        "__init__", "_init_role_storage", "get_role", "get_role_by_name"]
    assert all(RoleManager.__dict__[name] is original
               for name, original in role_manager._ACCESS_READ_STANDS_FOR.items())


def test_a_guard_put_in_the_place_of_the_module_s_is_the_guard_that_runs(monkeypatch):
    """A host's test switches the read-only guard off this way.  The one read
    would have applied the module's own guard to a root the test never meant
    to be looked at."""
    side = _Side().load(_build(ROOTS["locked"]))
    seen = []
    monkeypatch.setattr(access_control, "_check_readonly_guard",
                        lambda super_name, organization, label: seen.append((super_name, organization, label)))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(check_write_access, LAKE, ORG, "loader", "orders")

    assert outcome == ("returned", None)
    assert seen == [(LAKE, ORG, "write to this table")]
    assert [trip[0] for trip in side.trips] == FIVE[1:], "the role, by the four reads; the root by nobody"


@pytest.mark.parametrize("name", ["_check_operation_access", "_check_scope_access", "_resolve_role"])
def test_a_check_put_in_the_place_of_the_module_s_is_the_check_that_runs(monkeypatch, name):
    side = _Side().load(_build())
    called = []
    original = getattr(access_control, name)

    def watching(*args, **options):
        called.append(name)
        return original(*args, **options)

    monkeypatch.setattr(access_control, name, watching)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        outcome = _outcome(check_write_access, LAKE, ORG, "analyst", "orders")

    assert outcome == ("raised", "PermissionError", "You don't have permission to write to this table.")
    assert called == [name]
    assert [trip[0] for trip in side.trips] == FIVE


def test_the_module_s_own_checks_are_what_the_one_read_stands_for():
    assert access_control._checks_are_this_modules() is True
    assert access_control._ONE_READ_STANDS_FOR == (
        access_control._check_readonly_guard, access_control._check_operation_access,
        access_control._check_scope_access, access_control._resolve_role)


# ---------------------------------------------------------------------------
# A command of the client's someone has replaced is the command that is sent
# ---------------------------------------------------------------------------

CLIENT_READS = ["get", "exists", "smembers", "hget", "hgetall"]


@pytest.mark.parametrize("name", CLIENT_READS)
@pytest.mark.parametrize("where", ["on the class", "on the object"])
def test_a_client_with_one_of_its_reads_replaced_is_asked_that_read(monkeypatch, name, where):
    """What a test does to have Redis fail, or answer as the test likes: it
    replaces a command on the client.  The script would go round it."""
    side = _Side().load(_build())
    catalog = _catalog_on(side)
    original = getattr(redis.Redis, name)
    asked = []

    if where == "on the class":
        def watching(client, *args, **options):
            asked.append(name)
            return original(client, *args, **options)

        monkeypatch.setattr(type(side.client), name, watching)
    else:
        def watching(*args, **options):
            asked.append(name)
            return original(side.client, *args, **options)

        setattr(side.client, name, watching)

    assert catalog._reads_are_its_own() is False
    with pytest.raises(NotImplementedError, match="five reads of RedisCatalog as it defines them"):
        catalog.rbac_access_read(ORG, LAKE, "loader")
    assert side.trips == [] and asked == [], "nothing was sent"
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert _outcome(check_write_access, LAKE, ORG, "loader", "orders") == ("returned", None)

    assert [trip[0] for trip in side.trips] == FIVE
    assert asked == [name], "the command that was put there is the one that was sent"


def test_the_clients_the_library_builds_are_sent_the_one_read():
    """``redis.Redis``, also from a sentinel, and the fake of these tests:
    their five reads are the client library's own."""
    import inspect

    from redis.sentinel import Sentinel

    assert sorted(redis_catalog._CLIENT_READS) == sorted(CLIENT_READS)
    for kind in (redis.Redis, fakeredis.FakeRedis):
        assert all(getattr(kind, name) is original for name, original in redis_catalog._CLIENT_READS.items())
    assert inspect.signature(Sentinel.master_for).parameters["redis_class"].default is redis.Redis

    client = redis.Redis(host="127.0.0.1", port=1, decode_responses=True)       # never connected to
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
        assert RedisCatalog()._reads_are_its_own() is True


@pytest.mark.parametrize("name", CLIENT_READS)
def test_a_command_replaced_before_the_library_was_imported_is_still_told_apart(monkeypatch, name):
    """The library's own reads are taken from the classes ``redis.Redis`` is
    made of, not from ``redis.Redis`` as it happens to stand at the import."""
    original = getattr(redis.Redis, name)
    assert redis_catalog._client_reads()[name] is original
    monkeypatch.setattr(redis.Redis, name, lambda self, *args: None)

    assert redis_catalog._client_reads()[name] is original, "what was put on redis.Redis is not taken for it"
    side = _Side().load(_build())
    assert _catalog_on(side)._reads_are_its_own() is False


class _ItsOwnReads:
    """What a stand-in for the client has, written by hand: its own reads."""
    __slots__ = ()

    def register_script(self, text):
        return lambda keys=None, args=None: 0

    def get(self, key):
        return None

    def exists(self, key):
        return 0

    def smembers(self, key):
        return set()

    def hget(self, key, field):
        return None

    def hgetall(self, key):
        return {}

    def eval_ro(self, script, numkeys, *rest):
        raise AssertionError("a stand-in is not sent the read, nor asked whether it runs it")


class _HandWritten(_ItsOwnReads):
    pass


class _WithNoAttributesOfItsOwn(_ItsOwnReads):
    __slots__ = ()


@pytest.mark.parametrize("stand_in", [MagicMock, _HandWritten, _WithNoAttributesOfItsOwn],
                         ids=["a mock", "a class written by hand", "one that keeps no attributes"])
def test_a_stand_in_for_the_client_is_asked_its_own_reads(stand_in):
    client = stand_in()
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
        catalog = RedisCatalog()

        assert catalog._reads_are_its_own() is False
        with pytest.raises(NotImplementedError, match="five reads of RedisCatalog as it defines them"):
            catalog.rbac_access_read(ORG, LAKE, "loader")
        assert RoleManager.read_for_access_check(
            super_name=LAKE, organization=ORG, role_name="loader", redis_catalog=catalog) is None
    if stand_in is MagicMock:
        client.eval_ro.assert_not_called()


# ---------------------------------------------------------------------------
# A Redis that says yes, and then refuses the read
# ---------------------------------------------------------------------------

def _refusing(side, refusal: dict, sent: list) -> None:
    """Have this Redis refuse the access read with ``refusal["with"]`` for as
    long as there is one; which of the two scripts was sent goes into *sent*."""
    real = side.client.eval_ro

    def eval_ro(script, numkeys, *rest):
        sent.append("the question" if script == RedisCatalog._LUA_READ_ONLY_PROBE else "the read")
        if script == RedisCatalog._LUA_RBAC_ACCESS_READ and refusal.get("with") is not None:
            raise refusal["with"]
        return real(script, numkeys, *rest)

    side.client.eval_ro = eval_ro


#: Refusals that are this Redis's, and will be the same for every lake on it.
OF_THIS_REDIS = [
    redis.ResponseError("ACL failure in script: No permissions to access a key script: "
                        "83cd8e8f, on @user_script:9."),
    redis.exceptions.NoPermissionError("No permissions to access a key"),
    redis.ResponseError("Script attempted to access a non local key in a cluster node script: "
                        "83cd8e8f, on @user_script:9."),
    redis.ResponseError("unknown command 'EVAL_RO', with args beginning with: 'local meta' '4'"),
]
OF_THIS_REDIS_IDS = ["a user that may not read a role's document", "a user that may not read a key it names",
                     "a cluster that does not hold the document's key", "a server that knows the command no more"]


@pytest.mark.parametrize("refusal", OF_THIS_REDIS, ids=OF_THIS_REDIS_IDS)
def test_a_redis_that_said_yes_and_refuses_the_read_is_not_sent_it_again_for_a_minute(caplog, monkeypatch,
                                                                                      refusal):
    """A user that may run a script and may not read a role's document, a
    server that runs "return 1" and not this read: asked, it said yes.  It is
    not sent the read at every check, to refuse it and be logged each time.  A
    minute on it is asked again, and sent the read again if it says yes."""
    content = _build()
    old, new = _Side().load(content), _Side().load(content)
    clock = {"now": 500.0}
    monkeypatch.setattr(redis_catalog, "_monotonic", lambda: clock["now"])
    sent, state = [], {"with": refusal}
    _refusing(new, state, sent)
    line = ("ERROR", READ_REFUSED + str(refusal))

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _as_it_was(old, caplog, "loader", "orders")
        first = _now(new, caplog, "loader", "orders")
        second = _now(new, caplog, "loader", "orders")
        clock["now"] += redis_catalog._ASK_AGAIN_AFTER_S - 0.001
        third = _now(new, caplog, "loader", "orders")
        within_the_minute = list(sent)
        clock["now"] += 0.001
        fourth = _now(new, caplog, "loader", "orders")
        a_minute_on = list(sent)
        state["with"] = None
        fifth = _now(new, caplog, "loader", "orders")
        clock["now"] += redis_catalog._ASK_AGAIN_AFTER_S
        sixth = _now(new, caplog, "loader", "orders")
        seventh = _now(new, caplog, "loader", "orders")

    assert was == (("returned", None), [])
    assert first == fourth == (was[0], [line]), "refused, logged once, and the five reads decided"
    assert second == third == fifth == sixth == seventh == was, "decided the same, and nothing logged"
    assert within_the_minute == ["the read"]
    assert a_minute_on == ["the read", "the question", "the read"], "asked again, said yes, refused again"
    assert sent == a_minute_on + ["the question", "the read", "the read"], "and a yes it keeps is kept again"
    assert [trip[0] for trip in new.trips] == FIVE * 3 + ["EVAL_RO"] + FIVE * 2 + ["EVAL_RO"] * 3
    assert new.dump() == old.dump()


#: Refusals that are one lake's: a key of it holds another kind of value.
OF_THE_LAKE = [
    redis.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value script: "
                        "83cd8e8f, on @user_script:14."),
    redis.ResponseError("Error running script (call to f_83cd8e8f): "
                        "@user_script:?: WRONGTYPE Operation against a key holding the wrong kind of value"),
]


@pytest.mark.parametrize("refusal", OF_THE_LAKE, ids=["as Redis 7 words it", "as a stand-in for Redis wraps it"])
def test_a_key_of_another_kind_is_the_lake_s_matter_and_the_read_is_sent_again(caplog, refusal):
    """One lake with a key of the wrong kind is not to cost every other lake
    on the same Redis its one read."""
    side = _Side().load(_build())
    sent, state = [], {"with": refusal}
    _refusing(side, state, sent)
    line = ("ERROR", READ_REFUSED + str(refusal))

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        first = _now(side, caplog, "loader", "orders")
        second = _now(side, caplog, "loader", "orders")
        state["with"] = None
        third = _now(side, caplog, "loader", "orders")

    assert first == second == (("returned", None), [line])
    assert third == (("returned", None), [])
    assert sent == ["the read"] * 3, "sent each time, and never asked again whether it runs"
    assert redis_catalog._RUNS_READ_ONLY_SCRIPTS[side.client][0] is True


def test_a_lake_with_a_key_of_another_kind_is_sent_the_read_at_every_check(caplog):
    """The same with a key that is of another kind, and no refusal made up."""
    ran = _same(caplog, _build(ROOTS["a root that is a hash"]), "loader", "orders")
    assert ran.read_refused
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        again = _now(ran.new, caplog, "loader", "orders")

    assert again == ran.now
    assert [trip[0] for trip in ran.new.trips] == (["EVAL_RO"] + FIVE) * 2
    assert redis_catalog._RUNS_READ_ONLY_SCRIPTS[ran.new.client][0] is True


def test_a_yes_heard_meanwhile_is_not_taken_back_by_a_refusal_of_the_moment():
    """Two threads ask a new client's Redis at the same moment: one is told
    yes, the other is refused.  Whichever answer is written last, the yes
    stands, and the one read is not lost for a minute."""
    side = _Side(asked=False).load(_build())
    catalog = _catalog_on(side)
    real = side.client.eval_ro

    def eval_ro(script, numkeys, *rest):
        if script == RedisCatalog._LUA_READ_ONLY_PROBE:
            redis_catalog._RUNS_READ_ONLY_SCRIPTS[side.client] = (True, 0.0)      # what the other thread heard
            raise redis.ResponseError("BUSY Redis is busy running a script.")
        return real(script, numkeys, *rest)

    side.client.eval_ro = eval_ro

    assert catalog._runs_read_only_scripts() is True
    assert redis_catalog._RUNS_READ_ONLY_SCRIPTS[side.client] == (True, 0.0)
    assert catalog.rbac_access_read(ORG, LAKE, "loader")[3]["role"] == "writer"


# ---------------------------------------------------------------------------
# The key of a role's document
# ---------------------------------------------------------------------------

def test_a_document_key_built_another_way_is_the_key_the_check_reads(caplog, monkeypatch):
    """The script appends the id to a prefix.  Should the library's own
    constructor ever make another key of it, it is the constructor's key the
    check has always read, and reads."""
    content = _build()
    moved = DOC_PREFIX.replace(":roles:doc:", ":roles:moved:")
    assert moved != DOC_PREFIX
    monkeypatch.setattr(RK, "rbac_role_doc", lambda org, sup, role_id: f"{moved}{role_id}")

    ran = _same(caplog, content, "loader", "orders")

    assert ran.now[0] == ("raised", "PermissionError", "Invalid or nonexistent role: loader")
    assert ran.new.trips == [THE_ONE_READ] + ran.old.trips and [trip[0] for trip in ran.old.trips] == FIVE
    with pytest.raises(NotImplementedError, match="another key than rbac_role_doc names"):
        _catalog_on(ran.new).rbac_access_read(ORG, LAKE, "loader")
    assert _catalog_on(ran.new).rbac_access_read(ORG, LAKE, "nobody")[3] == {}, "no id, and no key to build"


# ---------------------------------------------------------------------------
# Only the write check
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("check,arguments", [
    (check_control_access, (LAKE, ORG, "ops_admin", "orders")),
    (check_rbac_access, (LAKE, ORG, "ops_admin")),
    (check_meta_access, (LAKE, ORG, "analyst", "orders")),
    (check_create_access, (LAKE, ORG, "loader", "orders")),
], ids=["control", "rbac", "meta", "create"])
def test_the_other_checks_read_as_they_did(check, arguments):
    """The one read is the write check's.  Widening it is a decision of its own."""
    side = _Side().load(_build())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert _outcome(check, *arguments) == ("returned", None)

    assert THE_ONE_READ not in side.trips
    expected = ["EXISTS", "SMEMBERS", "HGET", "HGETALL"]
    assert [trip[0] for trip in side.trips] == (expected if check is check_meta_access else ["GET"] + expected)
