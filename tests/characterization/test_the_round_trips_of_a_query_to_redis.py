"""
The round trips a query makes to Redis, and that fewer of them change nothing.

A query read Redis four times for its role, a round trip each: two checks
that the role store is initialised, made by ``RoleManager`` when it is
constructed, then the id the role's name stands for and the role's document.
One script reads the four now (``RedisCatalog.rbac_access_read``, by way of
``RoleManager.read_for_access_check`` and ``restrict_read_access``); it is the
script of the write's access check, not given the lake's root to read.

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every round trip of a query is recorded.  The sequence is pinned,
so that a round trip added later shows up as a failure here.  The way it was
is still reachable (a role manager without its one read), and the same
queries are run both ways, by a role that may read everything, by one that may
read one column, by one that may read some rows, and by roles that are
refused: they must come back with the same rows, or the same refusal.

The fake Redis is made to run a script sent as a read (``EVAL_RO``), as Redis
7 does; one test leaves it as it is, a Redis that does not know the command,
which is asked the four reads as before.  The harness is that of
``test_the_round_trips_of_a_write_to_redis.py``; the check itself is compared
with the check as it was, state by state, in
``supertable/tests/test_a_query_reads_its_role_once.py``.
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
# A query of one table, in order.
BEFORE_THE_ROLE = [
    "EXISTS meta:root",                 # the lake is there
    "GET meta:root",                    # is it a replica of another
    "EXISTS meta:leaf:doc:orders",      # the table is there
]


def _after_the_role(trips) -> list:
    return trips[trips.index(ROLE) + 1:] if ROLE in trips else trips[trips.index(FOUR_READS[-1]) + 1:]


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

def test_a_query_asks_for_its_role_in_one_round_trip(lake):
    lake.query(SELECT)

    answer, trips = lake.query(SELECT)

    assert answer == ("rows", [(0, 0), (1, 10), (2, 20), (3, 30)], ["id", "amount"])
    assert trips[:4] == BEFORE_THE_ROLE + [ROLE]
    assert trips.count(ROLE) == 1 and not any(read in trips for read in FOUR_READS)
    assert len(trips) == 8, trips


def test_the_way_it_was_is_three_round_trips_more_and_the_same_otherwise(hermetic_fakeredis, monkeypatch):
    def scenario(lake):
        lake.query(SELECT)
        return lake.query(SELECT)

    (was_answer, was), (now_answer, now) = _both(monkeypatch, scenario)

    assert now_answer == was_answer
    assert was[:7] == BEFORE_THE_ROLE + FOUR_READS
    assert now[:4] == BEFORE_THE_ROLE + [ROLE]
    assert _after_the_role(now) == _after_the_role(was), "nothing else of the query changed"
    assert (len(was), len(now)) == (11, 8)


def test_a_query_of_two_tables_asks_for_its_role_once(lake):
    lake.writer.write(role_name=ADMIN, simple_name="items", overwrite_columns=[], data=_rows(0))

    answer, trips = lake.query(
        f"SELECT o.id, i.amount FROM {TABLE} o JOIN items i ON o.id = i.id ORDER BY o.id")

    assert answer[0] == "rows" and [row[0] for row in answer[1]] == [0, 1, 2]
    assert trips.count(ROLE) == 1 and not any(read in trips for read in FOUR_READS)


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
}


def test_the_same_queries_both_ways_come_back_the_same(hermetic_fakeredis, monkeypatch):
    def scenario(lake):
        _with_roles(lake)
        asked = {label: lake.query(sql, role=role) for label, (role, sql) in QUERIES.items()}
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
        fewer = len(was[label][1]) - len(now[label][1])
        assert fewer == (2 if label == "as a role nobody has" else 3), (label, fewer)
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
    """The read-only guard is the mutations': a query does not read the root
    for it, and a locked lake or a replica answers as any other."""
    def scenario(lake):
        RedisCatalog().update_root_flags(ORG, lake.sup, {"read_only": True})
        answer, trips = lake.query(SELECT)
        return answer, trips.count("GET meta:root")

    was, now = _both(monkeypatch, scenario)

    assert now == was
    assert now[0][0] == "rows" and now[1] == 2, "the root is read to see whether the lake is a replica, twice, as it was"


# ---------------------------------------------------------------------------
# When the one read is not to be had
# ---------------------------------------------------------------------------

def test_a_role_read_that_fails_is_followed_by_the_four_reads(lake):
    lake.query(SELECT)
    failed = {"times": 0}

    def fail(trip):
        if trip == ROLE and not failed["times"]:
            failed["times"] += 1
            return True
        return False

    lake.state["fail"] = fail
    answer, trips = lake.query(SELECT)
    lake.state["fail"] = None

    assert answer[0] == "rows" and len(answer[1]) == 4
    assert trips[:8] == BEFORE_THE_ROLE + [ROLE] + FOUR_READS
    again, trips = lake.query(SELECT)
    assert again == answer and trips.count(ROLE) == 1 and FOUR_READS[0] not in trips


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
    assert now[2][3] == ROLE and now[2][4:] == was[2][3:], "the one read, then all the query did before"


def test_a_redis_that_does_not_run_a_script_sent_as_a_read_is_queried_as_before(hermetic_fakeredis, monkeypatch):
    """A Redis before 7: the four reads, and the same rows."""
    old = _lake(monkeypatch, redis_7=False)
    for first in (0, 3, 6):
        old.write(_rows(first))
    old.query(SELECT)

    answer, trips = old.query(SELECT)
    engine_duckdb.reset_shared_duckdb_state()

    assert answer == ("rows", [(0, 0), (1, 10), (2, 20), (3, 30)], ["id", "amount"])
    assert trips[:7] == BEFORE_THE_ROLE + FOUR_READS
    assert len(trips) == 11 and ROLE not in trips
