"""
The round trips a query makes to Redis, and that fewer of them change nothing.

A query made eleven round trips to Redis here, ten of them about its tables
and its role, each asked for by itself.  Before the role: is the lake there,
is it a replica of another, is the table there.  The role: two checks that the
role store is initialised, made by ``RoleManager`` when it is constructed, then
the id the role's name stands for and the role's document.  After the role:
the root once more, the table's entry, and whether the lake is there, asked
again by the table's lake object.

They are three round trips now, of the same commands.  The check that the
tables exist is one transaction (``RedisCatalog.root_and_leaves_exist``), the
role is one script (``RedisCatalog.rbac_access_read``, the script of the
write's access check, not given the lake's root to read), and the entries are
one transaction with the root (``RedisCatalog.get_root_and_leaf_items``),
after which the lake object does not ask.  The fourth is the engine's
settings, which is as it was.

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every round trip of a query is recorded.  The sequence is pinned,
so that a round trip added later shows up as a failure here.  The way it was
is still reachable (a role manager without its one read, a catalog without its
two), and the same queries are run both ways, by a role that may read
everything, by one that may read one column, by one that may read some rows,
by roles that are refused, and for a table and a lake that are not there:
they must come back with the same rows, the same refusal, or the same error.

The fake Redis is made to run a script sent as a read (``EVAL_RO``), as Redis
7 does; one test leaves it as it is, a Redis that does not know the command,
which is asked the four reads of the role as before.  The harness is that of
``test_the_round_trips_of_a_write_to_redis.py``; the checks themselves are
compared with the checks as they were, state by state, in
``supertable/tests/test_a_query_reads_its_role_once.py`` and
``supertable/tests/test_a_query_asks_the_catalog_about_its_tables_once.py``.
"""

from __future__ import annotations

import pytest

from supertable import redis_keys as RK
from supertable.engine import duckdb as engine_duckdb
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from tests.characterization.test_the_round_trips_of_a_write_to_redis import (
    ADMIN,
    ORG,
    TABLE,
    _lake,
    _rows,
    _text,
)

# The role of a query, in one script sent as a read: is the role store
# initialised, the role's id and its document.  The lake's root is not among
# the keys the script is given.
ROLE = "EVAL_RO rbac:roles:meta rbac:roles:type:doc:superadmin rbac:roles:name_to_id"
# The same four reads one by one, as they were.
FOUR_READS = [
    "EXISTS rbac:roles:meta",
    "SMEMBERS rbac:roles:type:doc:superadmin",
    "HGET rbac:roles:name_to_id",
    "HGETALL rbac:roles:doc:<id>",
]
# What a query of one table asks before its role is checked: is the lake
# there, is it a replica of another, is the table there.  One transaction.
THE_CHECK = [
    "EXISTS meta:root",
    "GET meta:root",
    "EXISTS meta:leaf:doc:orders",
]
# And once the role is known: the lake again, and what the table's entry
# holds.  One transaction; the table's lake object then does not ask.
THE_ENTRIES = [
    "EXISTS meta:root",
    "GET meta:root",
    "GET meta:leaf:doc:orders",
]
# The same commands one by one, as they were: three round trips before the
# role and three after it (the entries were a transaction of their own).
BEFORE_THE_ROLE = list(THE_CHECK)
AFTER_THE_ROLE = ["GET meta:root", ["GET meta:leaf:doc:orders"], "EXISTS meta:root"]


def _settings(trip) -> bool:
    """The engine's settings: the one round trip of a query that is not about
    its tables or its role."""
    return isinstance(trip, str) and trip.startswith("GET ") and trip.endswith(":system:engine:duckdb")


EVERYTHING = {"*": {"columns": ["*"], "filters": ["*"]}}
SELECT = f"SELECT id, amount FROM {TABLE} WHERE id < 4 ORDER BY id"


def _with_roles(lake):
    roles = RoleManager(super_name=lake.sup, organization=ORG, actor_role_name=ADMIN)
    ids = {
        "analyst": roles.create_role({"role": "reader", "role_name": "analyst",
                                      "tables": {TABLE: {"columns": ["id"], "filters": ["*"]}}}),
        "europe": roles.create_role({"role": "reader", "role_name": "europe", "tables": {TABLE: {
            "columns": ["*"],
            "filters": [{"region": {"operation": "=", "type": "value", "value": "eu"}}]}}}),
        "loader": roles.create_role({"role": "writer", "role_name": "loader", "tables": EVERYTHING}),
        "viewer": roles.create_role({"role": "meta", "role_name": "viewer", "tables": EVERYTHING}),
        "elsewhere": roles.create_role({"role": "reader", "role_name": "elsewhere",
                                        "tables": {"items": {"columns": ["*"], "filters": ["*"]}}}),
    }
    return roles, ids


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    lake = _lake(monkeypatch)
    for first in (0, 3, 6):
        lake.write(_rows(first))
    yield lake
    engine_duckdb.reset_shared_duckdb_state()


def _both(monkeypatch, scenario):
    """Run *scenario* on a lake the way it was and on one the way it is."""
    def run(as_it_was):
        with monkeypatch.context() as patched:
            lake = _lake(patched, as_it_was=as_it_was)
            for first in (0, 3, 6):
                lake.write(_rows(first))
            return scenario(lake)
    was, now = run(True), run(False)
    engine_duckdb.reset_shared_duckdb_state()
    return was, now


# ---------------------------------------------------------------------------
# The sequence, pinned
# ---------------------------------------------------------------------------

def test_a_query_is_three_round_trips_about_its_tables_and_its_role_and_one_for_the_settings(lake):
    lake.query(SELECT)

    answer, trips = lake.query(SELECT)

    assert answer == ("rows", [(0, 0), (1, 10), (2, 20), (3, 30)], ["id", "amount"])
    assert trips[:3] == [THE_CHECK, ROLE, THE_ENTRIES]
    assert len(trips) == 4 and _settings(trips[3]), trips
    assert not any(read in trips for read in FOUR_READS + BEFORE_THE_ROLE)


def test_the_way_it_was_is_seven_round_trips_more_and_the_same_otherwise(hermetic_fakeredis, monkeypatch):
    def scenario(lake):
        lake.query(SELECT)
        return lake.query(SELECT)

    (was_answer, was), (now_answer, now) = _both(monkeypatch, scenario)

    assert now_answer == was_answer
    assert was[:10] == BEFORE_THE_ROLE + FOUR_READS + AFTER_THE_ROLE
    assert now[:3] == [THE_CHECK, ROLE, THE_ENTRIES]
    assert now[3:] == was[10:] and _settings(now[3]), "nothing else of the query changed"
    assert (len(was), len(now)) == (11, 4)


def test_the_tables_asked_about_call_by_call_and_the_role_at_once_is_how_it_stood_before(
        hermetic_fakeredis, monkeypatch):
    """A catalog without its two reads: the six round trips about the tables
    are made as they were, around the one of the role."""
    monkeypatch.delattr(RedisCatalog, "root_and_leaves_exist")
    monkeypatch.delattr(RedisCatalog, "get_root_and_leaf_items")
    lake = _lake(monkeypatch)
    for first in (0, 3, 6):
        lake.write(_rows(first))
    lake.query(SELECT)

    answer, trips = lake.query(SELECT)
    engine_duckdb.reset_shared_duckdb_state()

    assert answer == ("rows", [(0, 0), (1, 10), (2, 20), (3, 30)], ["id", "amount"])
    assert trips[:7] == BEFORE_THE_ROLE + [ROLE] + AFTER_THE_ROLE
    assert len(trips) == 8 and _settings(trips[7])


def test_a_query_of_two_tables_is_no_more_round_trips_than_a_query_of_one(hermetic_fakeredis, monkeypatch):
    join = f"SELECT o.id, i.amount FROM {TABLE} o JOIN items i ON o.id = i.id ORDER BY o.id"

    def scenario(lake):
        lake.writer.write(role_name=ADMIN, simple_name="items", overwrite_columns=[], data=_rows(0))
        lake.query(join)
        return lake.query(join)

    (was_answer, was), (now_answer, now) = _both(monkeypatch, scenario)

    assert now_answer == was_answer
    assert now_answer[0] == "rows" and [row[0] for row in now_answer[1]] == [0, 1, 2]
    assert now[:3] == [
        ["EXISTS meta:root", "GET meta:root", "EXISTS meta:leaf:doc:orders", "EXISTS meta:leaf:doc:items"],
        ROLE,
        ["EXISTS meta:root", "GET meta:root", "GET meta:leaf:doc:items", "GET meta:leaf:doc:orders"],
    ]
    assert len(now) == 4 and _settings(now[3])
    assert was[:6] == BEFORE_THE_ROLE + ["EXISTS meta:root", "GET meta:root", "EXISTS meta:leaf:doc:items"]
    assert was[10:14] == ["GET meta:root", ["GET meta:leaf:doc:items", "GET meta:leaf:doc:orders"],
                          "EXISTS meta:root", "EXISTS meta:root"]
    assert len(was) == 15, "three for each table, four for the role, four after it, and the settings"


# ---------------------------------------------------------------------------
# The same answers, both ways
# ---------------------------------------------------------------------------

QUERIES = {
    "everything, as the superadmin": (ADMIN, f"SELECT id, region, amount FROM {TABLE} ORDER BY id"),
    "one column, as a reader of one column": ("analyst", f"SELECT id FROM {TABLE} ORDER BY id"),
    "every column, as a reader of one column": ("analyst", f"SELECT * FROM {TABLE} ORDER BY id"),
    "a column the reader may not see": ("analyst", f"SELECT id, amount FROM {TABLE} ORDER BY id"),
    "a count, as a reader of one region": ("europe", f"SELECT COUNT(*) AS n FROM {TABLE}"),
    "rows, as a reader of one region": ("europe", f"SELECT id, region FROM {TABLE} ORDER BY id"),
    "as a writer": ("loader", f"SELECT id FROM {TABLE} WHERE id > 5 ORDER BY id"),
    "as a writer, by any spelling": ("LOADER", f"SELECT id FROM {TABLE} WHERE id > 5 ORDER BY id"),
    "as a role that may only look at metadata": ("viewer", f"SELECT id FROM {TABLE}"),
    "as a reader of another table": ("elsewhere", f"SELECT id FROM {TABLE}"),
    "as a role nobody has": ("nobody", f"SELECT id FROM {TABLE}"),
    "the statistics, as a reader of one column": ("analyst", f"SHOW STATS {TABLE}"),
    "a table that is not there": (ADMIN, "SELECT id FROM nothere"),
    "a table that is not there, as a role nobody has": ("nobody", "SELECT id FROM nothere"),
    "a table that is not there, beside one that is": (
        ADMIN, f"SELECT o.id FROM {TABLE} o JOIN nothere n ON o.id = n.id"),
    "a lake that is not there": (ADMIN, "SELECT id FROM nolake.orders"),
    "the statistics of a table that is not there": (ADMIN, "SHOW STATS nothere"),
}
#: The round trips each of them makes, as it was and as it is.
ROUND_TRIPS = {
    "everything, as the superadmin": (11, 4),
    "one column, as a reader of one column": (11, 4),
    "every column, as a reader of one column": (11, 4),
    "a column the reader may not see": (7, 2),              # refused once the role is read
    "a count, as a reader of one region": (11, 4),
    "rows, as a reader of one region": (11, 4),
    "as a writer": (11, 4),
    "as a writer, by any spelling": (11, 4),
    "as a role that may only look at metadata": (7, 2),
    "as a reader of another table": (7, 2),
    "as a role nobody has": (6, 2),                         # no document is fetched for no id
    "the statistics, as a reader of one column": (9, 4),    # no estimate: the entry is fetched by itself
    "a table that is not there": (3, 1),                    # before any role is read
    "a table that is not there, as a role nobody has": (3, 1),
    "a table that is not there, beside one that is": (6, 1),
    "a lake that is not there": (1, 1),
    "the statistics of a table that is not there": (3, 1),
}


def test_the_same_queries_both_ways_come_back_the_same(hermetic_fakeredis, monkeypatch):
    def scenario(lake):
        _with_roles(lake)
        asked = {label: lake.query(sql, role=role) for label, (role, sql) in QUERIES.items()}
        # an error names the lake, and each way has a lake of its own
        asked = {label: ((answer[0], answer[1].replace(lake.sup, "<lake>")) if answer[0] == "error" else answer, trips)
                 for label, (answer, trips) in asked.items()}
        # the statistics name the files of this lake: kept are the columns they are of, and the bounds
        label = "the statistics, as a reader of one column"
        (kind, rows, columns), trips = asked[label]
        named = [dict(zip(columns, row)) for row in rows]
        asked[label] = ((kind, sorted((row["column_name"], row["min_bigint"], row["max_bigint"]) for row in named),
                         columns), trips)
        return asked

    was, now = _both(monkeypatch, scenario)

    for label in QUERIES:
        assert now[label][0] == was[label][0], label
        assert (len(was[label][1]), len(now[label][1])) == ROUND_TRIPS[label], label
    answers = {label: answer for label, (answer, _trips) in now.items()}
    assert answers["everything, as the superadmin"][1][:2] == [(0, "us", 0), (1, "eu", 10)]
    assert answers["one column, as a reader of one column"] == ("rows", [(i,) for i in range(9)], ["id"])
    assert answers["every column, as a reader of one column"] == ("rows", [(i,) for i in range(9)], ["id"])
    assert answers["a column the reader may not see"] == (
        "refused", "You don't have permission to columns: {'amount'} in table 'orders'.")
    assert answers["a count, as a reader of one region"] == ("rows", [(4,)], ["n"])
    assert answers["rows, as a reader of one region"][1] == [(1, "eu"), (3, "eu"), (5, "eu"), (7, "eu")]
    assert answers["as a writer"] == answers["as a writer, by any spelling"] == (
        "rows", [(6,), (7,), (8,)], ["id"])
    assert answers["as a role that may only look at metadata"] == (
        "refused", "You don't have permission to read the table.")
    assert answers["as a reader of another table"] == (
        "refused", "You don't have permission to read table 'orders'.")
    assert answers["as a role nobody has"] == ("refused", "Invalid or nonexistent role: nobody")
    assert answers["the statistics, as a reader of one column"][1] == [
        ("id", 0, 2), ("id", 3, 5), ("id", 6, 8)], "the one column the role may see, file by file"
    for label in ("a table that is not there", "a table that is not there, as a role nobody has",
                  "a table that is not there, beside one that is", "the statistics of a table that is not there"):
        assert answers[label][0] == "error" and "nothere" in answers[label][1], label
    assert answers["a lake that is not there"][0] == "error" and "nolake" in answers["a lake that is not there"][1]


def test_a_role_changed_between_two_queries_decides_the_second(hermetic_fakeredis, monkeypatch):
    """Nothing of the check is remembered from one query to the next."""
    def scenario(lake):
        roles, ids = _with_roles(lake)
        sql = f"SELECT id FROM {TABLE} WHERE id < 2 ORDER BY id"
        seen = [lake.query(sql, role="analyst")[0]]
        roles.update_role(ids["analyst"], {"tables": {TABLE: {"columns": ["amount"], "filters": ["*"]}}})
        seen.append(lake.query(sql, role="analyst")[0])
        roles.update_role(ids["analyst"], {"role": "meta", "tables": EVERYTHING})
        seen.append(lake.query(sql, role="analyst")[0])
        roles.update_role(ids["analyst"], {"role": "reader"})
        seen.append(lake.query(sql, role="analyst")[0])
        RedisCatalog().r.hset(RK.rbac_role_doc(ORG, lake.sup, ids["analyst"]), "enabled", "false")
        seen.append(lake.query(sql, role="analyst")[0])
        roles.delete_role(ids["analyst"])
        seen.append(lake.query(sql, role="analyst")[0])
        return seen

    was, now = _both(monkeypatch, scenario)

    assert now == was == [
        ("rows", [(0,), (1,)], ["id"]),
        ("refused", "You don't have permission to columns: {'id'} in table 'orders'."),
        ("refused", "You don't have permission to read the table."),
        ("rows", [(0,), (1,)], ["id"]),
        ("refused", "Role 'analyst' is disabled."),
        ("refused", "Invalid or nonexistent role: analyst"),
    ]


def test_a_lake_that_is_locked_is_read_as_before(hermetic_fakeredis, monkeypatch):
    """The read-only guard is the mutations': a query reads the root to see
    whether the lake is a replica of another, and a locked lake answers as any
    other."""
    def scenario(lake):
        RedisCatalog().update_root_flags(ORG, lake.sup, {"read_only": True})
        answer, trips = lake.query(SELECT)
        by_itself = trips.count("GET meta:root")
        in_a_transaction = sum(1 for trip in trips if isinstance(trip, list) and "GET meta:root" in trip)
        return answer, (by_itself, in_a_transaction)

    was, now = _both(monkeypatch, scenario)

    assert now[0] == was[0] and now[0][0] == "rows"
    assert was[1] == (2, 0) and now[1] == (0, 2), "twice, as it was: with the check, and with the entries"


# ---------------------------------------------------------------------------
# When the one read is not to be had
# ---------------------------------------------------------------------------

def _failing_once(lake, the_trip) -> None:
    failed = {"times": 0}

    def fail(trip):
        if trip == the_trip and not failed["times"]:
            failed["times"] += 1
            return True
        return False

    lake.state["fail"] = fail


def test_a_role_read_that_fails_is_followed_by_the_four_reads(lake):
    lake.query(SELECT)
    _failing_once(lake, ROLE)
    answer, trips = lake.query(SELECT)
    lake.state["fail"] = None

    assert answer[0] == "rows" and len(answer[1]) == 4
    assert trips[:6] == [THE_CHECK, ROLE] + FOUR_READS and trips[6] == THE_ENTRIES
    again, trips = lake.query(SELECT)
    assert again == answer and trips.count(ROLE) == 1 and FOUR_READS[0] not in trips


def test_a_check_of_the_tables_that_fails_is_followed_by_the_three_calls(lake):
    answer, _trips = lake.query(SELECT)
    _failing_once(lake, THE_CHECK)
    again, trips = lake.query(SELECT)
    lake.state["fail"] = None

    assert again == answer and answer[0] == "rows"
    assert trips[:6] == [THE_CHECK] + BEFORE_THE_ROLE + [ROLE, THE_ENTRIES]
    assert len(trips) == 7 and _settings(trips[6])
    assert lake.query(SELECT)[1][:3] == [THE_CHECK, ROLE, THE_ENTRIES], "and the next query asks at once again"


def test_a_read_of_the_entries_that_fails_is_followed_by_the_three_calls(lake):
    answer, _trips = lake.query(SELECT)
    _failing_once(lake, THE_ENTRIES)
    again, trips = lake.query(SELECT)
    lake.state["fail"] = None

    assert again == answer and answer[0] == "rows"
    assert trips[:6] == [THE_CHECK, ROLE, THE_ENTRIES] + AFTER_THE_ROLE
    assert len(trips) == 7 and _settings(trips[6])


def test_a_table_dropped_between_the_check_and_the_entries_ends_the_query_as_it_did(
        hermetic_fakeredis, monkeypatch):
    """The check and the entries are asked at two moments, as they were, with
    the role between them.  What is gone by the second is met there."""
    def scenario(lake):
        lake.query(SELECT)
        client = RedisCatalog().r
        leaf = RK.meta_leaf(ORG, lake.sup, TABLE)

        def before(trip):
            if trip == ROLE or trip == FOUR_READS[0]:
                client.delete(leaf)

        lake.state["before"] = before
        answer, _trips = lake.query(SELECT)
        lake.state["before"] = None
        return answer

    was, now = _both(monkeypatch, scenario)

    assert now == was and now[0] == "error"


def test_a_role_store_taken_away_under_a_reader_is_made_again_as_before(hermetic_fakeredis, monkeypatch):
    """A query of a lake without a role store has always had the constructor
    make the default role.  It still does: a store that is gone is found by
    the one read, and given to the constructor."""
    def scenario(lake):
        client = RedisCatalog().r
        gone = [key for key in client.keys("*") if ":rbac:roles:" in _text(key)]
        client.delete(*gone)
        answer, trips = lake.query(SELECT)
        again = sorted(_text(key).split(":rbac:roles:")[1].split(":")[0]
                       for key in client.keys("*") if ":rbac:roles:" in _text(key))
        return answer, again, trips

    was, now = _both(monkeypatch, scenario)

    assert now[0] == was[0] and now[0][0] == "rows"
    assert now[1] == was[1] == ["doc", "index", "meta", "name_to_id", "type"]
    assert now[2][:2] == [THE_CHECK, ROLE] and was[2][:3] == BEFORE_THE_ROLE
    made_again = was[2][3:-4]
    assert len(made_again) > 4, "the constructor's two checks, and the default role it then makes"
    assert now[2][2:-2] == made_again, "the one read of the role, then all the constructor did before"
    assert was[2][-4:-1] == AFTER_THE_ROLE and now[2][-2] == THE_ENTRIES


def test_a_redis_that_does_not_run_a_script_sent_as_a_read_is_queried_as_before(hermetic_fakeredis, monkeypatch):
    """A Redis before 7: the four reads, and the same rows."""
    old = _lake(monkeypatch, redis_7=False)
    for first in (0, 3, 6):
        old.write(_rows(first))
    old.query(SELECT)

    answer, trips = old.query(SELECT)
    engine_duckdb.reset_shared_duckdb_state()

    assert answer == ("rows", [(0, 0), (1, 10), (2, 20), (3, 30)], ["id", "amount"])
    assert trips[:6] == [THE_CHECK] + FOUR_READS + [THE_ENTRIES], \
        "the tables are asked about in two transactions, which any Redis runs; the role in four reads"
    assert len(trips) == 7 and _settings(trips[6]) and ROLE not in trips
