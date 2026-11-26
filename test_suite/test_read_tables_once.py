"""
What a query asks the catalog about its tables, on a real Redis.

A query asked about its tables one command at a time, a round trip each.  The
same commands are now sent in two transactions: one before the role is
checked (``RedisCatalog.root_and_leaves_exist``: is the lake there, is it a
replica, are the tables there) and one after it
(``RedisCatalog.get_root_and_leaf_items``: the lake again, and what the
tables' entries hold).

``supertable/tests/test_a_query_asks_the_catalog_about_its_tables_once.py``
compares both with the separate calls on a fake Redis, state by state.  What
only a real Redis shows is here: what a transaction of reads hands back (a
count, text, nothing for a key that is not there), that it runs those commands
and no other, that it is answered wherever the separate reads were (while
writes are paused, for a user whose keys are read-only), and the round trips
of the real reader, on a lake of its own and on a replica of another.
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

from test_suite.test_access_check_read import _outcome, _round_trips

ORG = "correctness"
A_ROOT = '{"version": 3, "ts": 1794000000000}'


def _snapshot(version: int, files) -> dict:
    return {"snapshot_version": version,
            "schema": [{"name": "id", "type": "BIGINT"}],
            "resources": [{"file": f, "file_size": 100 + i} for i, f in enumerate(files)]}


def _leaf(payload=None, *, path="snap.json", version=1) -> str:
    doc = {"version": version, "ts": 1_700_000_000_000, "path": path}
    if payload is not None:
        doc["payload"] = payload
    return json.dumps(doc)


LEAVES = {
    "orders": _leaf(_snapshot(3, ["o1.parquet", "o2.parquet"]), version=3),
    "items": _leaf(_snapshot(2, ["i1.parquet"]), version=2),
    "elsewhere": _leaf(None, path="somewhere/snap-5.json", version=5),
    "blank": "",
    "broken": "{not json",
}
SOURCE_LEAVES = {
    "orders": _leaf(_snapshot(9, ["s-o1.parquet"]), version=9),
    "extras": _leaf(_snapshot(1, ["s-x1.parquet"]), version=1),
}
ASKED = {
    "one table": ["orders"],
    "two tables": ["orders", "items"],
    "one there, one not": ["orders", "nothere"],
    "every kind of leaf": list(LEAVES),
    "a table only the source has": ["extras"],
    "no table": [],
    "a name in capitals": ["Orders"],
}


@pytest.fixture(scope="module")
def catalog():
    from supertable.redis_catalog import RedisCatalog
    return RedisCatalog()


def _prefix(lake: str) -> str:
    from supertable import redis_keys as RK
    return RK.meta_root(ORG, lake)[:-len("meta:root")]


def _remove(catalog, *lakes) -> None:
    for lake in lakes:
        found = list(catalog.r.scan_iter(match=_prefix(lake) + "*", count=1000))
        if found:
            catalog.r.delete(*found)


@pytest.fixture
def lakes(catalog):
    """Lakes of this test's own, written by hand: one with leaves of every
    kind, the lake two of the others are replicas of, and five more whose
    roots say something else."""
    from supertable import redis_keys as RK

    tag = uuid.uuid4().hex[:8]
    names = SimpleNamespace(**{kind: f"tables_{kind}_{tag}" for kind in (
        "plain", "source", "replica", "limited", "rootless", "unreadable", "emptyroot")})
    client = catalog.r

    def put(lake, root, leaves):
        if root is not None:
            client.set(RK.meta_root(ORG, lake), root)
        for name, text in leaves.items():
            client.set(RK.meta_leaf(ORG, lake, name), text)

    def replica(**more):
        return json.dumps({"version": 1, "ts": 1, "clone_type": "replica", "cloned_from": names.source, **more})

    put(names.plain, A_ROOT, LEAVES)
    put(names.source, A_ROOT, SOURCE_LEAVES)
    put(names.replica, replica(), LEAVES)
    put(names.limited, replica(replica_tables=["orders", "items"]), LEAVES)
    put(names.rootless, None, LEAVES)
    put(names.unreadable, "{not json", LEAVES)
    put(names.emptyroot, "", LEAVES)
    yield names
    _remove(catalog, *vars(names).values())


KINDS = ["plain", "source", "replica", "limited", "rootless", "unreadable", "emptyroot"]


# ---------------------------------------------------------------------------
# The two reads, beside the calls they stand for
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("asked", list(ASKED))
@pytest.mark.parametrize("kind", KINDS)
def test_the_check_s_read_answers_what_the_separate_calls_answer(catalog, lakes, kind, asked):
    lake, names = getattr(lakes, kind), ASKED[asked]

    at_once = _outcome(lambda: catalog.root_and_leaves_exist(ORG, lake, names))
    one_by_one = _outcome(lambda: (
        catalog.root_exists(ORG, lake),
        {name: catalog.leaf_exists(ORG, lake, name) for name in dict.fromkeys(names)}))

    assert at_once == one_by_one
    if at_once[0] == "returned":
        assert type(at_once[1][0]) is bool and all(type(v) is bool for v in at_once[1][1].values())


@pytest.mark.parametrize("asked", list(ASKED))
@pytest.mark.parametrize("kind", KINDS)
def test_the_estimate_s_read_answers_what_the_separate_calls_answer(catalog, lakes, kind, asked):
    lake, names = getattr(lakes, kind), ASKED[asked]

    at_once = _outcome(lambda: catalog.get_root_and_leaf_items(ORG, lake, names))
    one_by_one = _outcome(lambda: (
        catalog.root_exists(ORG, lake), catalog.get_leaf_items(ORG, lake, names)))

    assert at_once == one_by_one and at_once[0] == "returned"


def test_what_is_compared_is_of_every_kind(catalog, lakes):
    exist, leaves = catalog.root_and_leaves_exist, catalog.get_root_and_leaf_items

    assert exist(ORG, lakes.plain, ["orders", "nothere", "blank"]) == (
        True, {"orders": True, "nothere": False, "blank": True})
    assert [i["version"] for i in leaves(ORG, lakes.plain, ["orders", "items", "blank", "broken"])[1]] == [3, 2]
    assert exist(ORG, lakes.rootless, ["orders"]) == (False, {"orders": True})
    assert exist(ORG, lakes.emptyroot, ["orders"])[0] is True, "a key that is there, whatever it holds"
    assert leaves(ORG, lakes.unreadable, ["orders"])[1][0]["version"] == 3, "no replica: its own leaves"
    assert exist(ORG, lakes.replica, ["orders", "items", "extras"]) == (
        True, {"orders": True, "items": False, "extras": True}), "the source's tables"
    assert leaves(ORG, lakes.replica, ["orders"])[1][0]["version"] == 9
    assert exist(ORG, lakes.limited, ["orders", "extras", "Orders"]) == (
        True, {"orders": True, "extras": False, "Orders": False}), "what it was not given is not there"
    with pytest.raises(ValueError):
        exist(ORG, lakes.plain, ["Orders"])
    assert leaves(ORG, lakes.plain, ["Orders"]) == (True, [])


def _commands(client, do) -> list:
    """Every command Redis received while ``do()`` ran, as ``"NAME key"``,
    read from ``MONITOR``."""
    token = "tables-test-" + uuid.uuid4().hex
    watcher = redis.Redis(**{**client.connection_pool.connection_kwargs, "socket_timeout": 30})
    received = []
    try:
        with watcher.monitor() as monitor:
            do()
            client.echo(token)
            for seen in monitor.listen():
                words = seen["command"].split(" ")
                if words[-1] == token:
                    break
                received.append(" ".join(words[:2]))
    finally:
        watcher.close()
    return received


def test_each_read_is_one_transaction_of_reads_and_nothing_else(catalog, lakes):
    from supertable import redis_keys as RK

    root = RK.meta_root(ORG, lakes.plain)
    orders, items = (RK.meta_leaf(ORG, lakes.plain, name) for name in ("orders", "items"))

    check = _commands(catalog.r, lambda: catalog.root_and_leaves_exist(ORG, lakes.plain, ["orders", "items"]))
    entries = _commands(catalog.r, lambda: catalog.get_root_and_leaf_items(ORG, lakes.plain, ["orders", "items"]))

    assert check == ["MULTI", f"EXISTS {root}", f"GET {root}", f"EXISTS {orders}", f"EXISTS {items}", "EXEC"]
    assert entries == ["MULTI", f"EXISTS {root}", f"GET {root}", f"GET {orders}", f"GET {items}", "EXEC"]


def test_a_replica_is_read_in_two_transactions_the_second_of_its_source(catalog, lakes):
    from supertable import redis_keys as RK

    of_the_source = RK.meta_leaf(ORG, lakes.source, "orders")

    check = _commands(catalog.r, lambda: catalog.root_and_leaves_exist(ORG, lakes.replica, ["orders"]))
    entries = _commands(catalog.r, lambda: catalog.get_root_and_leaf_items(ORG, lakes.replica, ["orders"]))

    assert check[-3:] == ["MULTI", f"EXISTS {of_the_source}", "EXEC"] and check.count("MULTI") == 2
    assert entries[-3:] == ["MULTI", f"GET {of_the_source}", "EXEC"] and entries.count("MULTI") == 2


# ---------------------------------------------------------------------------
# Answered wherever the separate reads were
# ---------------------------------------------------------------------------

def test_both_reads_are_answered_while_writes_are_paused_as_the_separate_reads_were(catalog, lakes):
    """Redis holds writes back for a failover or a shutdown and goes on
    answering reads.  A transaction of reads is one of them."""
    from supertable import redis_keys as RK

    options = catalog.r.connection_pool.connection_kwargs
    operator = redis.Redis(**options)                                   # pauses, and ends the pause
    impatient = redis.Redis(**{**options, "socket_timeout": 1.0, "decode_responses": True})
    took = {}

    def timed(label, do):
        start = time.perf_counter()
        try:
            return do()
        finally:
            took[label] = time.perf_counter() - start

    operator.execute_command("CLIENT", "PAUSE", "8000", "WRITE")
    try:
        assert timed("a read", lambda: catalog.r.get(RK.meta_root(ORG, lakes.plain))) == A_ROOT
        check = timed("the check's read", lambda: catalog.root_and_leaves_exist(ORG, lakes.plain, ["orders"]))
        entries = timed("the estimate's read",
                        lambda: catalog.get_root_and_leaf_items(ORG, lakes.plain, ["orders"]))
        of_a_replica = timed("the reads of a replica",
                             lambda: catalog.get_root_and_leaf_items(ORG, lakes.replica, ["orders"]))
        with pytest.raises(redis.exceptions.TimeoutError):
            timed("a write", lambda: impatient.set(_prefix(lakes.plain) + "paused", "1"))
    finally:
        operator.execute_command("CLIENT", "UNPAUSE")
        impatient.close()
        operator.delete(_prefix(lakes.plain) + "paused")
        operator.close()

    assert check == (True, {"orders": True})
    assert entries[1][0]["version"] == 3 and of_a_replica[1][0]["version"] == 9
    for answered in ("a read", "the check's read", "the estimate's read", "the reads of a replica"):
        assert took[answered] < 0.5, (answered, took)
    assert took["a write"] >= 0.9, took


def test_a_user_whose_keys_are_read_only_is_answered_as_its_reads_were(catalog, lakes):
    import supertable.redis_catalog as rc_module
    from supertable.redis_catalog import RedisCatalog

    options = dict(catalog.r.connection_pool.connection_kwargs)
    user, password = "reader_" + uuid.uuid4().hex[:8], secrets.token_hex(16)
    catalog.r.execute_command("ACL", "SETUSER", user, "on", ">" + password, "+@all",
                              "%R~" + _prefix(lakes.plain) + "*")
    reader = redis.Redis(**{**options, "username": user, "password": password, "decode_responses": True})
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            reader.set(_prefix(lakes.plain) + "written", "1")
        with pytest.MonkeyPatch.context() as patched:
            patched.setattr(rc_module, "RedisConnector", lambda options=None: SimpleNamespace(r=reader))
            limited = RedisCatalog()
            check = limited.root_and_leaves_exist(ORG, lakes.plain, ["orders", "nothere"])
            entries = limited.get_root_and_leaf_items(ORG, lakes.plain, ["orders", "items"])
    finally:
        reader.close()
        catalog.r.execute_command("ACL", "DELUSER", user)

    assert check == catalog.root_and_leaves_exist(ORG, lakes.plain, ["orders", "nothere"])
    assert entries == catalog.get_root_and_leaf_items(ORG, lakes.plain, ["orders", "items"])


def test_a_root_of_another_kind_refuses_the_read_and_the_check_ends_as_it_did(catalog, lakes):
    """Redis refuses the transaction whole.  The separate calls met the same
    key their own way, and still do."""
    from supertable import redis_keys as RK
    from supertable.data_classes import TableDefinition
    from supertable.data_reader import DataReader
    from supertable.redis_catalog import RedisCatalog

    root = RK.meta_root(ORG, lakes.plain)
    catalog.r.delete(root)
    catalog.r.hset(root, "read_only", "true")
    tables = [TableDefinition(super_name=lakes.plain, simple_name="orders", alias="o", columns=[])]

    def check():
        reader = DataReader.__new__(DataReader)
        reader.organization, reader.super_name, reader._log_ctx = ORG, lakes.plain, ""
        return _outcome(lambda: reader._assert_targets_exist(tables))

    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        catalog.root_and_leaves_exist(ORG, lakes.plain, ["orders"])
    now = _round_trips(lambda: check())
    seen = {"now": check()}
    with pytest.MonkeyPatch.context() as patched:
        patched.delattr(RedisCatalog, "root_and_leaves_exist")
        seen["was"] = check()
        was = _round_trips(lambda: check())

    assert seen["now"] == seen["was"] and seen["now"][:2] == ("raised", "ResponseError")
    assert was == ["EXISTS", "GET"] and now == ["PIPELINE of 3"] + was


# ---------------------------------------------------------------------------
# The real reader
# ---------------------------------------------------------------------------

AS_IT_WAS = (["EXISTS", "GET", "EXISTS"], ["EXISTS", "SMEMBERS", "HGET", "HGETALL"],
             ["GET", "PIPELINE of 1", "EXISTS"])


@pytest.fixture
def written(catalog):
    """A lake with two tables written by the real writer, and a second lake
    that is a replica of it."""
    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    tag = uuid.uuid4().hex[:8]
    lake, mirror = "tables_real_" + tag, "tables_mirror_" + tag
    try:
        SuperTable(lake, ORG)
        writer = DataWriter(super_name=lake, organization=ORG)
        writer.write("superadmin", "t", overwrite_columns=[], data=pa.table({
            "id": list(range(6)), "v": [f"r{i}" for i in range(6)]}))
        writer.write("superadmin", "u", overwrite_columns=[], data=pa.table({
            "id": [0, 2, 4], "w": ["a", "b", "c"]}))
        SuperTable(mirror, ORG)
        catalog.update_root_flags(ORG, mirror, {"clone_type": "replica", "cloned_from": lake})
        yield SimpleNamespace(lake=lake, mirror=mirror)
    finally:
        _remove(catalog, lake, mirror)


def _query(lake, sql, role="superadmin"):
    from supertable.data_reader import DataReader, engine

    try:
        frame, status, message = DataReader(super_name=lake, organization=ORG, query=sql).execute(
            role_name=role, engine=engine.DUCKDB)
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))
    return (status.value, message, frame.to_dicts())


def _both_ways(lake, sql, role="superadmin"):
    """One query as the library runs it, and as a library without the two
    reads of the tables and the one read of the role runs it."""
    from supertable.rbac.role_manager import RoleManager
    from supertable.redis_catalog import RedisCatalog

    seen = {}
    _query(lake, sql, role)                                         # connections, the question of the role's read
    now = _round_trips(lambda: seen.setdefault("now", _query(lake, sql, role)))
    with pytest.MonkeyPatch.context() as patched:
        patched.delattr(RedisCatalog, "root_and_leaves_exist")
        patched.delattr(RedisCatalog, "get_root_and_leaf_items")
        patched.delattr(RoleManager, "read_for_access_check")
        was = _round_trips(lambda: seen.setdefault("was", _query(lake, sql, role)))
    assert seen["now"] == seen["was"], sql
    return SimpleNamespace(answer=seen["now"], now=now, was=was)


def test_a_query_of_the_real_reader_is_four_round_trips_where_it_was_eleven(written):
    ran = _both_ways(written.lake, "SELECT id, v FROM t WHERE id < 3 ORDER BY id")

    assert ran.answer == ("ok", None, [{"id": i, "v": f"r{i}"} for i in range(3)])
    assert ran.now == ["PIPELINE of 3", "EVAL_RO over 3", "PIPELINE of 3", "GET"]
    assert ran.was == AS_IT_WAS[0] + AS_IT_WAS[1] + AS_IT_WAS[2] + ["GET"]


def test_a_join_of_the_real_reader_is_four_round_trips_where_it_was_fifteen(written):
    ran = _both_ways(written.lake, "SELECT t.id, u.w FROM t JOIN u ON t.id = u.id ORDER BY t.id")

    assert ran.answer == ("ok", None, [{"id": 0, "w": "a"}, {"id": 2, "w": "b"}, {"id": 4, "w": "c"}])
    assert ran.now == ["PIPELINE of 4", "EVAL_RO over 3", "PIPELINE of 4", "GET"]
    assert ran.was == AS_IT_WAS[0] * 2 + AS_IT_WAS[1] + ["GET", "PIPELINE of 2", "EXISTS", "EXISTS", "GET"]


def test_a_query_of_a_replica_reads_its_source_s_tables_in_two_round_trips_more(written):
    ran = _both_ways(written.mirror, "SELECT id FROM t WHERE id > 3 ORDER BY id")

    assert ran.answer == ("ok", None, [{"id": 4}, {"id": 5}])
    assert ran.now == ["PIPELINE of 3", "PIPELINE of 1", "EVAL_RO over 3", "PIPELINE of 3", "PIPELINE of 1", "GET"]
    assert len(ran.was) == 11, "the same commands as for any lake: the leaf asked for was the source's"


@pytest.mark.parametrize("sql,expected,now,was", [
    ("SELECT id FROM nothere", "Table not found", 1, 3),
    ("SELECT t.id FROM t JOIN nothere n ON t.id = n.id", "Table not found", 1, 6),
    ("SELECT id FROM nolake.t", "SuperTable not found", 1, 1),
    ("SHOW STATS nothere", "Table not found", 1, 3),
], ids=["a table that is not there", "one beside a table that is", "a lake that is not there",
        "the statistics of a table that is not there"])
def test_what_is_not_there_is_reported_as_it_was_before_any_role_is_read(written, sql, expected, now, was):
    ran = _both_ways(written.lake, sql)

    assert ran.answer[0] == "error" and ran.answer[1].startswith(expected) and ran.answer[2] == []
    assert (len(ran.now), len(ran.was)) == (now, was)


def test_a_query_by_a_role_that_is_refused_is_refused_as_it_was(written):
    ran = _both_ways(written.lake, "SELECT id FROM t", role="nobody")

    assert ran.answer == ("raised", "PermissionError", "Invalid or nonexistent role: nobody")
    assert ran.now == ["PIPELINE of 3", "EVAL_RO over 3"]
    assert ran.was == AS_IT_WAS[0] + AS_IT_WAS[1][:3]


def test_the_statistics_of_a_table_are_four_round_trips_where_they_were_nine(written):
    ran = _both_ways(written.lake, "SHOW STATS t")

    assert ran.answer[0] == "ok" and {row["column_name"] for row in ran.answer[2]} >= {"id", "v"}
    assert ran.now == ["PIPELINE of 3", "EVAL_RO over 3", "GET", "GET"]
    assert ran.was == AS_IT_WAS[0] + AS_IT_WAS[1] + ["GET", "GET"]
