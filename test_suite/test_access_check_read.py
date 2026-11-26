# route: test_suite.test_access_check_read
"""The reads of a write's access check, on a real Redis.

WHAT THIS PROVES

Before a write the library checks that the lake is not read-only and that the
role may write to the table.  That took five reads of Redis, one round trip
each: the lake's root, two checks that the role store is initialised, the id
the role's name stands for, and the role's document.  One script reads the
five now (``RedisCatalog.rbac_access_read``), and the five reads are kept as
the way to check when the script is not to be had.

What a script hands back is made by the real Redis and its Lua: a key that is
not there, a hash as a flat list, a set.  So it is here that the one read is
compared with the five calls where they must agree:

  * the script answers, value for value, what the five calls answer, for the
    states the root, the role store, the name index and the document can be
    in;
  * it runs the five reads and nothing else, and writes nothing;
  * it is answered while Redis holds writes back, whether or not Redis has
    seen the script, and for a user whose keys are read-only, as the five
    reads are: the script is sent as a read (``EVAL_RO``), and the same script
    sent the usual way is held with the writes and refused to that user;
  * the check decides on the one read what it decides on the five, and a read
    that Redis refuses sends it back to the five;
  * a role store that is gone is made again by the check, as before;
  * a write of the real writer asks for access once;
  * a query asks the same script for its role alone: the four reads it made,
    the lake's root left untouched, the same views and the same refusals.
"""

from __future__ import annotations

import json
import secrets
import time
import uuid
from types import SimpleNamespace

import pyarrow as pa
import pytest
import redis
from redis.client import Pipeline

ORG = "correctness"
EVERYTHING = {"*": {"columns": ["*"], "filters": ["*"]}}
A_ROOT = '{"version": 3, "ts": 1794000000000}'

ROLES = [
    {"role": "admin", "role_name": "ops_admin", "tables": EVERYTHING},
    {"role": "writer", "role_name": "loader", "tables": EVERYTHING},
    {"role": "writer", "role_name": "Orders_Writer",
     "tables": {"orders": {"columns": ["*"], "filters": ["*"]},
                "tételek": {"columns": ["id"], "filters": ["*"]}}},
    {"role": "reader", "role_name": "analyst", "tables": EVERYTHING},
    {"role": "meta", "role_name": "catalog_viewer", "tables": EVERYTHING},
]


@pytest.fixture(scope="module")
def catalog():
    from supertable.redis_catalog import RedisCatalog
    return RedisCatalog()


def _keys(lake: str) -> SimpleNamespace:
    from supertable import redis_keys as RK
    root = RK.meta_root(ORG, lake)
    return SimpleNamespace(
        root=root, prefix=root[:-len("meta:root")], meta=RK.rbac_role_meta(ORG, lake),
        superadmins=RK.rbac_role_type_index(ORG, lake, "superadmin"),
        names=RK.rbac_rolename_to_id(ORG, lake),
        document=lambda role_id: RK.rbac_role_doc(ORG, lake, role_id))


def _make(catalog, lake: str) -> SimpleNamespace:
    """A lake with a root and six roles, written by the library."""
    from supertable.rbac.role_manager import RoleManager

    manager = RoleManager(super_name=lake, organization=ORG, redis_catalog=catalog)
    for role in ROLES:
        manager._create_role(json.loads(json.dumps(role)))
    keys = _keys(lake)
    catalog.r.set(keys.root, A_ROOT)
    ids = catalog.r.hgetall(keys.names)
    return SimpleNamespace(name=lake, keys=keys, ids=ids, document=lambda name: keys.document(ids[name]))


def _remove(catalog, lake: str) -> None:
    found = list(catalog.r.scan_iter(match=_keys(lake).prefix + "*", count=1000))
    if found:
        catalog.r.delete(*found)


@pytest.fixture
def lake(catalog):
    name = "access_" + uuid.uuid4().hex[:10]
    yield _make(catalog, name)
    _remove(catalog, name)


@pytest.fixture
def two_lakes(catalog):
    names = ("access_five_" + uuid.uuid4().hex[:8], "access_one_" + uuid.uuid4().hex[:8])
    yield tuple(_make(catalog, name) for name in names)
    for name in names:
        _remove(catalog, name)


def _held(catalog, lake) -> dict:
    """Everything the lake has in Redis."""
    client, out = catalog.r, {}
    for key in sorted(client.scan_iter(match=lake.keys.prefix + "*", count=1000)):
        kind = client.type(key)
        if kind == "string":
            out[key] = client.get(key)
        elif kind == "hash":
            out[key] = dict(sorted(client.hgetall(key).items()))
        elif kind == "set":
            out[key] = sorted(client.smembers(key))
        else:
            raise AssertionError(f"a {kind} at {key}")
    return out


def _outcome(do):
    try:
        return ("returned", do())
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


def _five_calls(catalog, lake, role_name):
    """What the access check asked the catalog, one call after another.  The
    superadmins are every id listed: the check took the first of them."""
    root = catalog.r.get(lake.keys.root)
    meta = bool(catalog.r.exists(lake.keys.meta))
    superadmins = sorted(catalog.rbac_get_role_ids_by_type(ORG, lake.name, "superadmin"))
    role_id = catalog.rbac_get_role_id_by_name(ORG, lake.name, role_name)
    role = (catalog.get_role_details(ORG, lake.name, role_id) or {}) if role_id else {}
    assert catalog.rbac_get_superadmin_role_id(ORG, lake.name) in (superadmins or [None])
    return root, meta, superadmins, role


def _one_read(catalog, lake, role_name):
    root, meta, superadmins, role = catalog.rbac_access_read(ORG, lake.name, role_name)
    assert type(superadmins) is list
    return root, meta, sorted(superadmins), role


# ---------------------------------------------------------------------------
# States: each changes what the lake holds
# ---------------------------------------------------------------------------

def _root(text):
    return lambda catalog, lake: catalog.r.set(lake.keys.root, text)


def _field(field, value):
    def change(catalog, lake):
        if value is None:
            catalog.r.hdel(lake.document("loader"), field)
        else:
            catalog.r.hset(lake.document("loader"), field, value)
    return change


STATES = {
    "as the library wrote it": lambda catalog, lake: None,
    "no root": lambda catalog, lake: catalog.r.delete(lake.keys.root),
    "a locked root": _root('{"version": 7, "read_only": true}'),
    "a replica's root": _root(json.dumps({"read_only": True, "clone_type": "replica", "cloned_from": "forrás"})),
    "a root that is not JSON": _root("{not json"),
    "an empty root": _root(""),
    "a root that is null": _root("null"),
    "a root that is a list": _root('[{"read_only": true}]'),
    "a root with text outside ASCII": _root('{"version": 1, "megjegyzés": "árvíztűrő tükörfúrógép"}'),
    "no meta key": lambda catalog, lake: catalog.r.delete(lake.keys.meta),
    "no superadmin listed": lambda catalog, lake: catalog.r.delete(lake.keys.superadmins),
    "no role store at all": lambda catalog, lake: catalog.r.delete(
        *[k for k in catalog.r.scan_iter(match=lake.keys.prefix + "rbac:*", count=1000)]),
    "the name is not in the index": lambda catalog, lake: catalog.r.hdel(lake.keys.names, "loader"),
    "the name stands for empty text": lambda catalog, lake: catalog.r.hset(lake.keys.names, "loader", ""),
    "the name stands for an id with no document": lambda catalog, lake: catalog.r.hset(
        lake.keys.names, "loader", "0" * 32),
    "a disabled role": _field("enabled", "false"),
    "a role without a type": _field("role", None),
    "a role of a type nobody knows": _field("role", "owner"),
    "a role turned reader": _field("role", "reader"),
    "tables as an old list": _field("tables", '["orders"]'),
    "tables that are not JSON": _field("tables", "orders, items"),
    "tables outside ASCII": _field("tables", json.dumps({"tételek": {"columns": ["ár"]}}, ensure_ascii=False)),
    "no tables": _field("tables", None),
    "no role id in the document": _field("role_id", None),
}

#: States the role store is not initialised in: the check then makes it, so
#: they are compared on two lakes and not twice on one.
NOT_INITIALISED = {"no meta key", "no superadmin listed", "no role store at all"}

NAMES = ["loader", "LOADER", "Orders_Writer", "orders_writer", "superadmin", "ops_admin", "analyst",
         "catalog_viewer", "nobody", " loader ", "", None, 7, "LOADERİ"]


# ---------------------------------------------------------------------------
# The one read, beside the five calls
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", sorted(STATES))
def test_the_one_read_answers_what_the_five_calls_answer(catalog, lake, state):
    STATES[state](catalog, lake)
    before = _held(catalog, lake)

    for name in NAMES:
        assert _one_read(catalog, lake, name) == _five_calls(catalog, lake, name), name

    assert _held(catalog, lake) == before, "and neither wrote anything"


def test_what_comes_back_is_what_redis_holds(catalog, lake):
    """Not only equal to the five calls: the values themselves."""
    root, meta, superadmins, role = catalog.rbac_access_read(ORG, lake.name, "Orders_Writer")

    assert (root, meta, superadmins) == (A_ROOT, True, [lake.ids["superadmin"]])
    assert role["role_id"] == lake.ids["orders_writer"] and role["role"] == "writer"
    assert role["tables"] == {"orders": {"columns": ["*"], "filters": ["*"]},
                              "tételek": {"columns": ["id"], "filters": ["*"]}}
    assert role["role_name"] == "Orders_Writer"

    catalog.r.delete(lake.keys.root)
    assert catalog.rbac_access_read(ORG, lake.name, "nobody") == (None, True, [lake.ids["superadmin"]], {})

    catalog.r.sadd(lake.keys.superadmins, "", "another")
    assert _one_read(catalog, lake, "nobody")[2] == sorted(["", "another", lake.ids["superadmin"]])


def _script_commands(client, keys, do) -> dict:
    """The commands a script ran on any of *keys* while ``do()`` ran, counted
    by name; and, under ``"others"``, the names of what it ran on other keys.

    Read from ``MONITOR`` and kept only when they came from a script.  The
    server's own counters count every client of the server.
    """
    token = "access-test-" + uuid.uuid4().hex
    watcher = redis.Redis(**{**client.connection_pool.connection_kwargs, "socket_timeout": 30})
    made: dict = {"others": []}
    try:
        with watcher.monitor() as monitor:
            do()
            client.echo(token)
            for seen in monitor.listen():
                words = seen["command"].split(" ")
                if words[-1] == token:
                    break
                if seen["client_type"] != "lua":
                    continue
                if set(words[1:2]) & set(keys):
                    made[words[0].lower()] = made.get(words[0].lower(), 0) + 1
                else:
                    made["others"].append(words[0].lower())
    finally:
        watcher.close()
    return made


@pytest.mark.parametrize("name,expected", [
    ("loader", {"get": 1, "exists": 1, "smembers": 1, "hget": 1, "hgetall": 1}),
    ("LOADER", {"get": 1, "exists": 1, "smembers": 1, "hget": 1, "hgetall": 1}),
    ("nobody", {"get": 1, "exists": 1, "smembers": 1, "hget": 1}),
    ("", {"get": 1, "exists": 1, "smembers": 1}),
    (None, {"get": 1, "exists": 1, "smembers": 1}),
])
def test_the_script_runs_the_five_reads_and_nothing_else(catalog, lake, name, expected):
    """Five for a role that is there, four for a name the index does not
    have, three for no name: the reads the five calls made, no more."""
    catalog.rbac_access_read(ORG, lake.name, "loader")        # the script is known to Redis
    keys = [lake.keys.root, lake.keys.meta, lake.keys.superadmins, lake.keys.names, lake.document("loader")]

    made = _script_commands(catalog.r, keys, lambda: catalog.rbac_access_read(ORG, lake.name, name))

    assert made.pop("others") == []
    assert made == expected


def test_a_name_that_stands_for_no_id_reads_no_document(catalog, lake):
    """Empty text in the name index is no id: no key is built from it."""
    catalog.r.hset(lake.keys.names, "loader", "")
    catalog.rbac_access_read(ORG, lake.name, "loader")
    keys = [lake.keys.root, lake.keys.meta, lake.keys.superadmins, lake.keys.names]

    made = _script_commands(catalog.r, keys, lambda: catalog.rbac_access_read(ORG, lake.name, "loader"))

    assert made.pop("others") == []
    assert made == {"get": 1, "exists": 1, "smembers": 1, "hget": 1}


# ---------------------------------------------------------------------------
# A read, wherever Redis answers reads
# ---------------------------------------------------------------------------

def test_this_redis_runs_a_script_sent_as_a_read(catalog):
    """Redis 7 and later.  The catalog asks once and is told so; a Redis that
    is not is asked the five reads (``supertable/tests`` has that side)."""
    from supertable.redis_catalog import RedisCatalog

    assert catalog._runs_read_only_scripts() is True
    assert RedisCatalog()._runs_read_only_scripts() is True
    assert catalog.r.eval_ro(RedisCatalog._LUA_READ_ONLY_PROBE, 0) == 1


def test_redis_itself_refuses_a_write_from_a_script_sent_as_a_read(catalog, lake):
    """What sending it that way is worth: not this suite's reading of the
    script, Redis's own refusal."""
    with pytest.raises(redis.ResponseError, match="Write commands are not allowed from read-only scripts"):
        catalog.r.eval_ro("redis.call('SET', KEYS[1], 'x') return 1", 1, lake.keys.root)

    assert catalog.r.get(lake.keys.root) == A_ROOT


def _reads_sent_the_usual_way(client, lake, name="loader"):
    """The same script through EVALSHA, as every other script of the catalog
    is sent; and what it was given."""
    from supertable.redis_catalog import RedisCatalog

    keys = [lake.keys.meta, lake.keys.superadmins, lake.keys.names, lake.keys.root]
    arguments = [name, lake.keys.prefix + "rbac:roles:doc:"]
    script = client.register_script(RedisCatalog._LUA_RBAC_ACCESS_READ)
    return lambda: script(keys=keys, args=arguments)


def test_the_check_is_answered_while_writes_are_paused_as_its_five_reads_were(catalog, lake):
    """Redis holds writes back for a failover or a shutdown and goes on
    answering reads.  A script sent the usual way counts as a write whenever
    Redis has not got it yet, and waits for the pause to end; a refusal would
    have waited with it.  Sent as a read it is answered, script known to
    Redis or not: Redis is made to forget every script before the pause."""
    options = catalog.r.connection_pool.connection_kwargs
    operator = redis.Redis(**options)                                   # pauses, and ends the pause
    impatient = redis.Redis(**{**options, "socket_timeout": 1.0, "decode_responses": True})
    the_usual_way = _reads_sent_the_usual_way(impatient, lake)
    assert the_usual_way()[3] == lake.ids["loader"], "it is the same read"
    took = {}

    def timed(label, do):
        start = time.perf_counter()
        try:
            return do()
        finally:
            took[label] = time.perf_counter() - start

    operator.script_flush()
    operator.execute_command("CLIENT", "PAUSE", "8000", "WRITE")
    try:
        assert timed("a read", lambda: catalog.r.get(lake.keys.root)) == A_ROOT
        read = timed("the one read", lambda: catalog.rbac_access_read(ORG, lake.name, "loader"))
        allowed = timed("a check that allows", lambda: _check(lake, "loader", "orders"))
        refused = timed("a check that refuses", lambda: _check(lake, "analyst", "orders"))
        with pytest.raises(redis.exceptions.TimeoutError):
            timed("the same reads, sent the usual way", the_usual_way)
        impatient.close()
        with pytest.raises(redis.exceptions.TimeoutError):
            timed("a write", lambda: impatient.set(lake.keys.prefix + "paused", "1"))
    finally:
        operator.execute_command("CLIENT", "UNPAUSE")
        impatient.close()
        operator.delete(lake.keys.prefix + "paused")
        operator.close()

    assert read[3]["role"] == "writer"
    assert allowed == ("returned", None)
    assert refused == ("raised", "PermissionError", "You don't have permission to write to this table.")
    for answered in ("a read", "the one read", "a check that allows", "a check that refuses"):
        assert took[answered] < 0.5, (answered, took)
    for held in ("the same reads, sent the usual way", "a write"):
        assert took[held] >= 0.9, (held, took)


def test_a_user_whose_keys_are_read_only_is_answered_as_its_reads_were(catalog, lake):
    """A reader hardened this way could always make the five reads.  Redis
    refuses it EVAL and EVALSHA over the same keys, whatever the script does,
    and answers EVAL_RO."""
    import supertable.redis_catalog as rc_module
    from supertable.redis_catalog import RedisCatalog

    options = dict(catalog.r.connection_pool.connection_kwargs)
    user, password = "reader_" + uuid.uuid4().hex[:8], secrets.token_hex(16)
    catalog.r.execute_command("ACL", "SETUSER", user, "on", ">" + password,
                              "%R~" + lake.keys.prefix + "*", "+@all")
    reader = redis.Redis(**{**options, "username": user, "password": password, "decode_responses": True})
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            reader.set(lake.keys.prefix + "written", "1")
        with pytest.raises(redis.exceptions.NoPermissionError):
            _reads_sent_the_usual_way(reader, lake)()
        with pytest.MonkeyPatch.context() as patched:
            patched.setattr(rc_module, "RedisConnector", lambda options=None: SimpleNamespace(r=reader))
            assert RedisCatalog()._runs_read_only_scripts() is True
            read = RedisCatalog().rbac_access_read(ORG, lake.name, "loader")
            trips, seen = [], {}
            trips += _round_trips(lambda: seen.setdefault("write", _check(lake, "analyst", "orders")))
            trips += _round_trips(lambda: seen.setdefault("read", _read_check(lake, "analyst", "one column")))
    finally:
        reader.close()
        catalog.r.execute_command("ACL", "DELUSER", user)

    assert read == _five_calls(catalog, lake, "loader")
    assert seen["write"] == ("raised", "PermissionError", "You don't have permission to write to this table.")
    assert seen["read"] == ("returned", {})
    assert trips == ["EVAL_RO over 4", "EVAL_RO over 3"], "one round trip each, and none refused"


def test_a_user_that_may_not_read_a_role_s_document_is_refused_once_and_asked_the_five(catalog, lake):
    """Redis lets such a user run a script, and refuses it the document from
    inside the script.  The one read is refused once; the reads that follow
    end as they always did for this user, and the read is not sent again, to
    be refused and logged at every check."""
    import supertable.redis_catalog as rc_module
    from supertable.redis_catalog import RedisCatalog

    options = dict(catalog.r.connection_pool.connection_kwargs)
    user, password = "limited_" + uuid.uuid4().hex[:8], secrets.token_hex(16)
    keys = lake.keys
    catalog.r.execute_command("ACL", "SETUSER", user, "on", ">" + password, "+@all",
                              *("%R~" + key for key in (keys.root, keys.meta, keys.superadmins, keys.names)))
    limited = redis.Redis(**{**options, "username": user, "password": password, "decode_responses": True})
    seen = {}
    try:
        with pytest.MonkeyPatch.context() as patched:
            patched.setattr(rc_module, "RedisConnector", lambda options=None: SimpleNamespace(r=limited))
            assert RedisCatalog()._runs_read_only_scripts() is True
            with pytest.raises(redis.ResponseError, match="No permissions to access a key"):
                RedisCatalog().rbac_access_read(ORG, lake.name, "loader")
            assert RedisCatalog()._runs_read_only_scripts() is False, "taken at its word for now"
            rc_module._RUNS_READ_ONLY_SCRIPTS.pop(limited)      # as in a process that has not met the refusal
            first = _round_trips(lambda: seen.setdefault("first", _check(lake, "loader", "orders")))
            again = _round_trips(lambda: seen.setdefault("again", _check(lake, "loader", "orders")))
            query = _round_trips(lambda: seen.setdefault("query", _read_check(lake, "analyst", "one column")))
            seen["five"] = _check(lake, "loader", "orders", five=True)
            seen["four"] = _read_check(lake, "analyst", "one column", four=True)
    finally:
        limited.close()
        catalog.r.execute_command("ACL", "DELUSER", user)

    assert seen["first"] == seen["again"] == seen["five"]
    assert seen["five"][:2] == ("raised", "PermissionError"), "a document it may not read is no role"
    assert seen["query"] == seen["four"]
    assert first == ["EVAL_RO over 0", "EVAL_RO over 4", "GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"]
    assert again == ["GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"]
    assert query == ["EXISTS", "SMEMBERS", "HGET", "HGETALL"]


# ---------------------------------------------------------------------------
# The check, on the one read and on the five
# ---------------------------------------------------------------------------

def _check(lake, name, table, five: bool = False):
    """``check_write_access``; with *five*, as a library without the one read."""
    from supertable.rbac.access_control import check_write_access
    from supertable.rbac.role_manager import RoleManager

    with pytest.MonkeyPatch.context() as patched:
        if five:
            patched.delattr(RoleManager, "read_for_access_check")
        return _outcome(lambda: check_write_access(lake.name, ORG, name, table))


@pytest.mark.parametrize("state", sorted(set(STATES) - NOT_INITIALISED))
def test_the_check_decides_on_one_read_what_it_decides_on_five(catalog, lake, state):
    STATES[state](catalog, lake)
    before = _held(catalog, lake)

    for name in NAMES:
        for table in ("orders", "tételek", "secrets"):
            assert _check(lake, name, table) == _check(lake, name, table, five=True), (name, table)

    assert _held(catalog, lake) == before, "a check of an initialised role store writes nothing"


def test_the_decisions_compared_are_grants_and_refusals_of_every_kind(catalog, lake):
    assert _check(lake, "loader", "orders") == ("returned", None)
    assert _check(lake, "Orders_Writer", "tételek") == ("returned", None)
    refusals = {
        ("analyst", "orders"): "You don't have permission to write to this table.",
        ("orders_writer", "secrets"): "You don't have permission to write to this table table 'secrets'.",
        ("nobody", "orders"): "Invalid or nonexistent role: nobody",
    }
    for (name, table), message in refusals.items():
        assert _check(lake, name, table) == ("raised", "PermissionError", message)

    catalog.r.set(lake.keys.root, '{"read_only": true, "clone_type": "replica"}')
    assert _check(lake, "superadmin", "orders") == (
        "raised", "PermissionError", "This SuperTable is a live replica. Cannot write to this table.")
    catalog.r.set(lake.keys.root, "{not json")
    assert _check(lake, "superadmin", "orders") == (
        "raised", "PermissionError",
        "Cannot verify whether this SuperTable is read-only. Cannot write to this table.")


@pytest.mark.parametrize("state", sorted(NOT_INITIALISED))
def test_a_role_store_that_is_gone_is_made_again_as_before(catalog, two_lakes, state):
    five, one = two_lakes
    for lake in two_lakes:
        STATES[state](catalog, lake)

    was = _check(five, "superadmin", "orders", five=True)
    now = _check(one, "superadmin", "orders")

    assert now == was == ("returned", None)

    def shape(lake):
        """What kinds of keys the role store has, how many superadmins are
        listed, and whether the store says it is initialised."""
        held = _held(catalog, lake)
        kinds = sorted({key[len(lake.keys.prefix):].split(":doc:")[0] for key in held if ":rbac:roles:" in key})
        return kinds, len(held.get(lake.keys.superadmins, [])), held[lake.keys.meta].get("initialized")

    assert shape(one) == shape(five)
    # A superadmin whose document is still there is found by its name and is
    # not listed again: the constructor left that as it was, and so does this.
    assert shape(one)[1:] == ((0 if state == "no superadmin listed" else 1), "true")


def _of_another_kind(what):
    def change(catalog, lake):
        client, keys = catalog.r, lake.keys
        key = {"the root": keys.root, "the superadmin index": keys.superadmins,
               "the name index": keys.names, "the document": lake.document("loader")}[what]
        client.delete(key)
        if what == "the root":
            client.hset(key, "read_only", "true")
        else:
            client.set(key, "text where another kind of value belongs")
    return change


@pytest.mark.parametrize("what", ["the root", "the superadmin index", "the name index", "the document"])
def test_a_read_redis_refuses_sends_the_check_back_to_the_five(catalog, lake, what):
    """A key that holds another kind of value: Redis refuses the script whole.
    Each of the five reads met that refusal its own way, and still does."""
    _of_another_kind(what)(catalog, lake)
    before = _held(catalog, lake)

    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        catalog.rbac_access_read(ORG, lake.name, "loader")
    now, was = _check(lake, "loader", "orders"), _check(lake, "loader", "orders", five=True)

    assert now == was
    assert now[:2] == {
        "the root": ("returned", None),                         # the guard lets a root it cannot fetch pass
        "the superadmin index": ("raised", "ResponseError"),
        "the name index": ("raised", "ResponseError"),
        "the document": ("raised", "PermissionError"),          # no role by that name
    }[what]
    assert _held(catalog, lake) == before


def test_a_key_of_another_kind_in_one_lake_leaves_every_lake_its_one_read(catalog, two_lakes):
    """Redis's refusal names the kind of value, and is taken for that lake's
    matter: the read is sent again, to this lake and to every other on the
    same Redis."""
    spoiled, sound = two_lakes
    _of_another_kind("the name index")(catalog, spoiled)
    _check(sound, "loader", "orders")       # whichever client the check uses has been asked by now

    trips = [_round_trips(lambda: _check(spoiled, "loader", "orders")),
             _round_trips(lambda: _check(sound, "loader", "orders")),
             _round_trips(lambda: _check(spoiled, "loader", "orders"))]

    assert trips[0] == trips[2] == ["EVAL_RO over 4", "GET", "EXISTS", "SMEMBERS", "HGET"]
    assert trips[1] == ["EVAL_RO over 4"]


# ---------------------------------------------------------------------------
# The real writer
# ---------------------------------------------------------------------------

def _round_trips(do) -> list:
    """The round trips made while ``do()`` ran: each command with the number
    of keys a script was given, each pipeline with the number of its commands."""
    trips = []
    real_command, real_execute = redis.Redis.execute_command, Pipeline.execute

    def command(self, *args, **options):
        name = str(args[0]).upper()
        if name in ("EVALSHA", "EVAL", "EVAL_RO"):
            trips.append(f"{name} over {int(args[2])}")
        elif name.startswith("SCRIPT"):
            if trips and trips[-1].startswith("EVALSHA"):
                trips.pop()         # refused, and sent again once the script is loaded
        else:
            trips.append(name)
        return real_command(self, *args, **options)

    def execute(self, *args, **options):
        trips.append(f"PIPELINE of {len(self.command_stack)}")
        return real_execute(self, *args, **options)

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(redis.Redis, "execute_command", command)
        patched.setattr(Pipeline, "execute", execute)
        do()
    return trips


def test_a_write_of_the_real_writer_asks_for_access_once(catalog):
    from supertable.data_reader import DataReader, Status, engine
    from supertable.data_writer import DataWriter
    from supertable.rbac.role_manager import RoleManager
    from supertable.super_table import SuperTable

    lake = "access_writes_" + uuid.uuid4().hex[:8]
    try:
        SuperTable(lake, ORG)
        writer = DataWriter(super_name=lake, organization=ORG)
        rows = lambda first: pa.table({"id": list(range(first, first + 3)),         # noqa: E731
                                       "v": [f"r{i}" for i in range(first, first + 3)]})
        writer.write("superadmin", "t", data=rows(0), overwrite_columns=[])
        writer.write("superadmin", "t", data=rows(3), overwrite_columns=[])

        now = _round_trips(lambda: writer.write("superadmin", "t", data=rows(6), overwrite_columns=[]))
        with pytest.MonkeyPatch.context() as patched:
            patched.delattr(RoleManager, "read_for_access_check")
            was = _round_trips(lambda: writer.write("superadmin", "t", data=rows(9), overwrite_columns=[]))

        rest = ["INCRBY", "SET", "GET", "GET", "EVALSHA over 3", "PIPELINE of 3", "EVALSHA over 1"]
        assert now == ["EVAL_RO over 4"] + rest
        assert was == ["GET", "EXISTS", "SMEMBERS", "HGET", "HGETALL"] + rest

        frame, status, message = DataReader(
            super_name=lake, organization=ORG, query="SELECT id, v FROM t ORDER BY id",
        ).execute(role_name="superadmin", engine=engine.DUCKDB)
        assert status is Status.OK, message
        assert frame.to_dicts() == [{"id": i, "v": f"r{i}"} for i in range(12)]

        catalog.update_root_flags(ORG, lake, {"read_only": True})
        refused = []
        trips = _round_trips(lambda: refused.append(_outcome(
            lambda: writer.write("superadmin", "t", data=rows(12), overwrite_columns=[]))))
        assert refused == [("raised", "PermissionError",
                            "This SuperTable is locked. Cannot write to this table.")]
        assert trips == ["EVAL_RO over 4"]
    finally:
        _remove(catalog, lake)


# ---------------------------------------------------------------------------
# A query: the role alone
# ---------------------------------------------------------------------------

def _four_calls(catalog, lake, role_name):
    """What a query's check asked the catalog: no root."""
    return (None,) + _five_calls(catalog, lake, role_name)[1:]


@pytest.mark.parametrize("state", sorted(STATES))
def test_the_read_of_a_query_answers_what_its_four_calls_answer(catalog, lake, state):
    STATES[state](catalog, lake)
    before = _held(catalog, lake)

    for name in NAMES:
        root, meta, superadmins, role = catalog.rbac_access_read(ORG, lake.name, name, with_root=False)
        assert (root, meta, sorted(superadmins), role) == _four_calls(catalog, lake, name), name

    assert _held(catalog, lake) == before


@pytest.mark.parametrize("name,expected", [
    ("loader", {"exists": 1, "smembers": 1, "hget": 1, "hgetall": 1}),
    ("nobody", {"exists": 1, "smembers": 1, "hget": 1}),
    (None, {"exists": 1, "smembers": 1}),
])
def test_the_read_of_a_query_does_not_touch_the_root(catalog, lake, name, expected):
    """The four reads a manager made, and no fifth: the root is neither read
    nor named, and one that holds another kind of value does not fail it."""
    catalog.rbac_access_read(ORG, lake.name, "loader", with_root=False)
    keys = [lake.keys.root, lake.keys.meta, lake.keys.superadmins, lake.keys.names, lake.document("loader")]

    made = _script_commands(catalog.r, keys,
                            lambda: catalog.rbac_access_read(ORG, lake.name, name, with_root=False))

    assert made.pop("others") == []
    assert made == expected

    catalog.r.delete(lake.keys.root)
    catalog.r.hset(lake.keys.root, "read_only", "true")
    assert catalog.rbac_access_read(ORG, lake.name, "loader", with_root=False)[3]["role"] == "writer"


def _asked_for(*tables):
    from supertable.data_classes import TableDefinition
    return [TableDefinition(super_name="unused", simple_name=name, alias=alias, columns=list(columns))
            for name, alias, columns in tables]


REQUESTS = {
    "one column": (("orders", "o", ["id"]),),
    "two columns": (("orders", "o", ["id", "amount"]),),
    "every column": (("orders", "o", []),),
    "a table outside ASCII": (("tételek", "t", ["id"]),),
    "two tables": (("orders", "o", ["id"]), ("tételek", "t", ["id"])),
    "a table nobody was given": (("secrets", "s", ["id"]),),
}


def _read_check(lake, name, request, four: bool = False):
    """``restrict_read_access``; with *four*, as a library without the one read."""
    from supertable.rbac.access_control import restrict_read_access
    from supertable.rbac.role_manager import RoleManager

    tables = _asked_for(*REQUESTS[request])
    with pytest.MonkeyPatch.context() as patched:
        if four:
            patched.delattr(RoleManager, "read_for_access_check")
        return _outcome(lambda: {
            alias: (list(view.allowed_columns), view.where_clause)
            for alias, view in restrict_read_access(lake.name, ORG, name, tables, tables).items()})


@pytest.mark.parametrize("state", sorted(set(STATES) - NOT_INITIALISED))
def test_a_query_is_let_through_on_one_read_as_on_four(catalog, lake, state):
    STATES[state](catalog, lake)
    before = _held(catalog, lake)

    for name in NAMES:
        for request in REQUESTS:
            assert _read_check(lake, name, request) == _read_check(lake, name, request, four=True), (name, request)

    assert _held(catalog, lake) == before


def test_the_views_and_refusals_compared_are_of_every_kind(catalog, lake):
    assert _read_check(lake, "superadmin", "two tables") == ("returned", {})
    assert _read_check(lake, "Orders_Writer", "two tables") == ("returned", {"t": (["id"], "")})
    assert _read_check(lake, "orders_writer", "every column") == ("returned", {})
    refusals = {
        ("catalog_viewer", "one column"): "You don't have permission to read the table.",
        ("Orders_Writer", "a table nobody was given"): "You don't have permission to read table 'secrets'.",
        ("nobody", "one column"): "Invalid or nonexistent role: nobody",
    }
    for (name, request), message in refusals.items():
        assert _read_check(lake, name, request) == ("raised", "PermissionError", message)

    catalog.r.hset(lake.document("orders_writer"), "tables", json.dumps({"orders": {"columns": ["id"], "filters": [
        {"megye": {"operation": "=", "type": "value", "value": "Győr"}}]}}, ensure_ascii=False))
    assert _read_check(lake, "Orders_Writer", "one column") == (
        "returned", {"o": (["id"], "\"megye\" = 'Győr'")}) == _read_check(lake, "Orders_Writer", "one column", four=True)

    catalog.r.set(lake.keys.root, '{"read_only": true, "clone_type": "replica"}')
    assert _read_check(lake, "superadmin", "one column") == ("returned", {}), "a replica is read"


def test_a_query_is_answered_while_writes_are_paused(catalog, lake):
    operator = redis.Redis(**catalog.r.connection_pool.connection_kwargs)
    took = {}
    operator.script_flush()                                     # Redis has seen no script
    operator.execute_command("CLIENT", "PAUSE", "6000", "WRITE")
    try:
        start = time.perf_counter()
        allowed = _read_check(lake, "analyst", "one column")
        took["allowed"] = time.perf_counter() - start
        start = time.perf_counter()
        refused = _read_check(lake, "catalog_viewer", "one column")
        took["refused"] = time.perf_counter() - start
    finally:
        operator.execute_command("CLIENT", "UNPAUSE")
        operator.close()

    assert allowed == ("returned", {})
    assert refused == ("raised", "PermissionError", "You don't have permission to read the table.")
    assert max(took.values()) < 0.5, took


def test_a_query_of_the_real_reader_asks_for_its_role_once(catalog):
    from supertable.data_reader import DataReader, Status, engine
    from supertable.data_writer import DataWriter
    from supertable.rbac.role_manager import RoleManager
    from supertable.super_table import SuperTable

    lake = "access_reads_" + uuid.uuid4().hex[:8]
    try:
        SuperTable(lake, ORG)
        writer = DataWriter(super_name=lake, organization=ORG)
        writer.write("superadmin", "t", overwrite_columns=[], data=pa.table({
            "id": list(range(6)), "v": [f"r{i}" for i in range(6)], "megye": ["Győr", "Pest"] * 3}))
        roles = RoleManager(super_name=lake, organization=ORG, actor_role_name="superadmin")
        roles.create_role({"role": "reader", "role_name": "gyori", "tables": {"t": {
            "columns": ["id", "megye"],
            "filters": [{"megye": {"operation": "=", "type": "value", "value": "Győr"}}]}}})

        def query(sql, role):
            frame, status, message = DataReader(super_name=lake, organization=ORG, query=sql).execute(
                role_name=role, engine=engine.DUCKDB)
            assert status is Status.OK, message
            return frame.to_dicts()

        seen = {}
        query("SELECT id FROM t ORDER BY id", "superadmin")
        now = _round_trips(lambda: seen.setdefault("now", query("SELECT * FROM t ORDER BY id", "gyori")))
        with pytest.MonkeyPatch.context() as patched:
            patched.delattr(RoleManager, "read_for_access_check")
            was = _round_trips(lambda: seen.setdefault("was", query("SELECT * FROM t ORDER BY id", "gyori")))

        assert seen["now"] == seen["was"] == [{"id": i, "megye": "Győr"} for i in (0, 2, 4)]
        # the tables are asked about in a transaction before the role and one
        # after it (test_read_tables_once.py); then the engine's settings
        around = (["PIPELINE of 3"], ["PIPELINE of 3", "GET"])
        assert now == around[0] + ["EVAL_RO over 3"] + around[1]
        assert was == around[0] + ["EXISTS", "SMEMBERS", "HGET", "HGETALL"] + around[1]
    finally:
        _remove(catalog, lake)

