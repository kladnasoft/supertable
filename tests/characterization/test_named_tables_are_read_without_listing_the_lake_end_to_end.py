"""A read fetches the tables it names; behind a real reader it returns what it did.

Every read used to list its whole lake to find its tables: a SCAN of the Redis
database, then a fetch and decode of every leaf in the lake. ``estimate`` now
asks the catalog for the named tables' leaves and keeps the listing for what a
name cannot answer (``supertable/tests/test_estimator_reads_named_tables_by_name.py``
compares the two estimates field by field).

Here the same comparison is made on what a caller actually gets. Tables are
written with the real writer on LOCAL storage and read with the real reader,
RBAC and DuckDB, twice: as the code now runs, and with the named fetch switched
off, which is the path every read took before. The rows must be the same rows,
through a deletion vector, a restricted role, a join, another lake and a
replica lake; and the named reads must not have scanned for leaves at all.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``region`` of the ``emp`` rows in ``eu``.
READER = "eu_two_columns"

LEAF_PATTERN_END = ":meta:leaf:doc:*"


@pytest.fixture
def lakes(hermetic_fakeredis):
    """Two lakes and a replica of the first.

    ``emp`` holds a deleted row that is still physically in its first file
    (id 2 was written in ``eu`` and then overwritten), so a read that lost the
    deletion vector would show it.
    """
    engine_duckdb.reset_shared_duckdb_state()
    tag = uuid.uuid4().hex[:10]
    main, other, mirror = f"named_{tag}", f"other_{tag}", f"mirror_{tag}"

    SuperTable(main, ORG)
    writer = DataWriter(super_name=main, organization=ORG)
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3, 4], "region": ["eu", "eu", "us", "eu"],
                           "dept_id": [10, 10, 20, 30],
                           "salary": [40_000, 900_000, 55_000, 61_000]}))
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
        data=pl.DataFrame({"id": [2], "region": ["apac"], "dept_id": [20],
                           "salary": [910_000]}))
    writer.write(
        role_name=ADMIN, simple_name="dept", overwrite_columns=[],
        data=pl.DataFrame({"dept_id": [10, 20, 30], "name": ["ops", "sales", "lab"]}))
    for i in range(12):                       # tables no query here names
        writer.write(
            role_name=ADMIN, simple_name=f"filler_{i:02d}", overwrite_columns=[],
            data=pl.DataFrame({"id": [i], "note": [f"n{i}"]}))
    RoleManager(super_name=main, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": READER, "role": "reader", "tables": {
            "emp": {"columns": ["id", "region"],
                    "filters": {"region": {"operation": "=", "type": "value",
                                           "value": "eu"}}}}})

    SuperTable(other, ORG)
    DataWriter(super_name=other, organization=ORG).write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [3, 4, 5], "region": ["jp", "jp", "jp"],
                           "dept_id": [1, 1, 1], "salary": [1, 2, 3]}))

    catalog = RedisCatalog()
    catalog.ensure_root(ORG, mirror)
    catalog.update_root_flags(ORG, mirror, {
        "clone_type": "replica", "cloned_from": main,
        "replica_tables": ["emp", "dept"], "read_only": True})

    yield SimpleNamespace(main=main, other=other, mirror=mirror)
    engine_duckdb.reset_shared_duckdb_state()


def _read(lake: str, sql: str, role: str = ADMIN):
    """``(status, message, rows)``; an exception is returned as its outcome."""
    try:
        frame, status, message = DataReader(
            super_name=lake, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
    except Exception as error:  # noqa: BLE001 - the error IS the outcome
        return ("raised", type(error).__name__, str(error))
    return (status, message, frame.to_dicts())


@pytest.fixture
def list_the_lake(monkeypatch):
    """Switch the named fetch off: every read lists its lake, as it used to."""
    def arm():
        monkeypatch.setattr(DataEstimator, "_named_snapshots",
                            lambda self, *args, **kwargs: None)

    def disarm():
        monkeypatch.undo()

    return SimpleNamespace(arm=arm, disarm=disarm)


@pytest.fixture
def redis_commands(hermetic_fakeredis, monkeypatch):
    """Every command sent to Redis: ``("SCAN", pattern)`` for a scan, the
    command's name otherwise, ``"PIPELINE"`` for each pipeline opened."""
    seen = []
    real_execute = hermetic_fakeredis.execute_command
    real_pipeline = hermetic_fakeredis.pipeline
    real_scan = hermetic_fakeredis.scan

    def execute_command(*args, **kwargs):
        name = str(args[0]).upper()
        if name != "SCAN":
            seen.append(name)
        return real_execute(*args, **kwargs)

    def pipeline(*args, **kwargs):
        seen.append("PIPELINE")
        return real_pipeline(*args, **kwargs)

    def scan(*args, **kwargs):
        seen.append(("SCAN", str(kwargs.get("match"))))
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(hermetic_fakeredis, "execute_command", execute_command)
    monkeypatch.setattr(hermetic_fakeredis, "pipeline", pipeline)
    monkeypatch.setattr(hermetic_fakeredis, "scan", scan)
    return seen


def _leaf_scans(commands) -> list:
    return [c for c in commands
            if isinstance(c, tuple) and c[0] == "SCAN" and c[1].endswith(LEAF_PATTERN_END)]


#: ``(case, lake attribute, role, SQL)``. ``{other}`` is the second lake's name.
READS = [
    ("all rows", "main", ADMIN, "SELECT id, region FROM emp ORDER BY id"),
    ("star", "main", ADMIN, "SELECT * FROM emp ORDER BY id"),
    ("a filter", "main", ADMIN, "SELECT id FROM emp WHERE region = 'eu' ORDER BY id"),
    ("a key", "main", ADMIN, "SELECT id, salary FROM emp WHERE id = 2"),
    ("an aggregate", "main", ADMIN,
     "SELECT region, COUNT(*) AS n, SUM(salary) AS s FROM emp GROUP BY region ORDER BY region"),
    ("a count", "main", ADMIN, "SELECT COUNT(*) AS n FROM emp"),
    ("a join", "main", ADMIN,
     "SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id ORDER BY e.id"),
    ("a self join", "main", ADMIN,
     "SELECT a.id, b.id AS other FROM emp a JOIN emp b ON a.dept_id = b.dept_id "
     "AND a.id < b.id ORDER BY 1, 2"),
    ("a subquery", "main", ADMIN,
     "SELECT id FROM emp WHERE dept_id IN (SELECT dept_id FROM dept WHERE name <> 'lab') "
     "ORDER BY id"),
    ("a cte", "main", ADMIN,
     "WITH eu AS (SELECT id, salary FROM emp WHERE region = 'eu') "
     "SELECT COUNT(*) AS n, MAX(salary) AS top FROM eu"),
    ("a qualified name", "main", ADMIN, "SELECT id FROM {main}.emp ORDER BY id"),
    ("another lake", "main", ADMIN, "SELECT id, region FROM {other}.emp ORDER BY id"),
    ("across two lakes", "main", ADMIN,
     "SELECT a.id, a.region, b.region AS elsewhere FROM emp a "
     "JOIN {other}.emp b ON a.id = b.id ORDER BY a.id"),
    ("a table nothing else names", "main", ADMIN, "SELECT id, note FROM filler_07"),
    ("a restricted role", "main", READER, "SELECT id, region FROM emp ORDER BY id"),
    ("a restricted role, star", "main", READER, "SELECT * FROM emp ORDER BY id"),
    ("a replica", "mirror", ADMIN, "SELECT id, region FROM emp ORDER BY id"),
    ("a replica, joined", "mirror", ADMIN,
     "SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id ORDER BY e.id"),
    ("explain", "main", ADMIN, "EXPLAIN SELECT id FROM emp WHERE id = 2"),
]

#: Reads that are refused, and must be refused the same way.
REFUSED = [
    ("a table that is not there", "main", ADMIN, "SELECT * FROM nothere"),
    ("one of two is not there", "main", ADMIN,
     "SELECT * FROM emp e JOIN nothere n ON e.id = n.id"),
    ("a column that is not there", "main", ADMIN, "SELECT nope FROM emp"),
    ("the lake's own name", "main", ADMIN, "SELECT * FROM {main}"),
    ("a name in another case", "main", ADMIN, "SELECT * FROM Emp"),
    ("a quoted name in another case", "main", ADMIN, 'SELECT * FROM "EMP"'),
    ("a name no table can have", "main", ADMIN, 'SELECT * FROM "my table"'),
    ("a replica not given the table", "mirror", ADMIN, "SELECT * FROM filler_07"),
    ("a role without the table", "main", READER, "SELECT * FROM dept"),
    ("a role without the column", "main", READER, "SELECT salary FROM emp"),
]


def _sql(lakes, sql: str) -> str:
    return sql.format(main=lakes.main, other=lakes.other)


@pytest.mark.parametrize("case,lake,role,sql", READS, ids=[r[0] for r in READS])
def test_a_read_returns_what_it_returned_when_the_lake_was_listed(
        lakes, list_the_lake, case, lake, role, sql):
    query = _sql(lakes, sql)
    by_name = _read(getattr(lakes, lake), query, role)
    list_the_lake.arm()
    by_listing = _read(getattr(lakes, lake), query, role)

    assert by_name[0] is Status.OK, by_name
    assert by_name == by_listing
    assert by_name[2], "the read returned no rows; the comparison proves nothing"


@pytest.mark.parametrize("case,lake,role,sql", REFUSED, ids=[r[0] for r in REFUSED])
def test_a_refused_read_is_refused_the_way_it_was(
        lakes, list_the_lake, case, lake, role, sql):
    query = _sql(lakes, sql)
    by_name = _read(getattr(lakes, lake), query, role)
    list_the_lake.arm()
    by_listing = _read(getattr(lakes, lake), query, role)

    assert by_name[0] in (Status.ERROR, "raised"), by_name
    assert by_name == by_listing


def test_the_deleted_row_stays_deleted(lakes):
    """The deletion vector comes from the snapshot fetched by name."""
    status, message, rows = _read(lakes.main, "SELECT id, region FROM emp ORDER BY id")
    assert status is Status.OK, message
    assert rows == [{"id": 1, "region": "eu"}, {"id": 2, "region": "apac"},
                    {"id": 3, "region": "us"}, {"id": 4, "region": "eu"}]
    snapshot = RedisCatalog().get_leaf(ORG, lakes.main, "emp")["payload"]
    assert snapshot["tombstone"], "the fixture must have left a deletion vector"


def test_the_restricted_role_still_sees_only_its_rows_and_columns(lakes):
    status, message, rows = _read(lakes.main, "SELECT * FROM emp ORDER BY id", READER)
    assert status is Status.OK, message
    assert rows == [{"id": 1, "region": "eu"}, {"id": 4, "region": "eu"}]


@pytest.mark.parametrize("case,lake,role,sql", READS, ids=[r[0] for r in READS])
def test_a_named_read_does_not_scan_for_leaves(
        lakes, redis_commands, case, lake, role, sql):
    del redis_commands[:]
    outcome = _read(getattr(lakes, lake), _sql(lakes, sql), role)
    assert outcome[0] is Status.OK, outcome
    assert _leaf_scans(redis_commands) == []


def test_listing_the_lake_did_scan(lakes, list_the_lake, redis_commands):
    """The contrast, and the check that the recorder above can see a scan."""
    list_the_lake.arm()
    del redis_commands[:]
    outcome = _read(lakes.main, "SELECT id FROM emp ORDER BY id")
    assert outcome[0] is Status.OK, outcome
    assert len(_leaf_scans(redis_commands)) >= 1


@pytest.mark.parametrize("sql", [
    "SELECT id FROM emp WHERE id = 2",
    "SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id ORDER BY e.id",
    "SELECT a.id FROM emp a JOIN {other}.emp b ON a.id = b.id ORDER BY a.id",
], ids=["one table", "a join", "two lakes"])
def test_a_read_sends_redis_fewer_commands_than_it_did(
        lakes, list_the_lake, redis_commands, sql):
    """Never more: the named fetch is the listing's own commands less its SCAN."""
    query = _sql(lakes, sql)
    _read(lakes.main, query)                  # warm: connections, RBAC scaffolding
    del redis_commands[:]
    assert _read(lakes.main, query)[0] is Status.OK
    by_name = list(redis_commands)

    list_the_lake.arm()
    del redis_commands[:]
    assert _read(lakes.main, query)[0] is Status.OK
    by_listing = list(redis_commands)

    assert len(by_name) < len(by_listing)
    without_scans = [c for c in by_listing if not (isinstance(c, tuple) and c[0] == "SCAN")]
    assert by_name == without_scans


def test_a_listing_that_repeats_a_table_does_not_double_a_named_read(
        lakes, list_the_lake, monkeypatch):
    """SCAN may hand an element back more than once while the database is
    being resized, and the listing passes that on. A read that took every
    listed leaf for a snapshot then read the table's files twice, and said OK.
    A table fetched by name is fetched once, whatever a listing would return.
    """
    real = RedisCatalog.scan_leaf_keys

    def repeating(self, org, sup, count=1000):
        for key in real(self, org, sup, count=count):
            yield key
            yield key

    monkeypatch.setattr(RedisCatalog, "scan_leaf_keys", repeating)
    sql = "SELECT COUNT(*) AS n, SUM(salary) AS total FROM emp"
    total = 40_000 + 910_000 + 55_000 + 61_000

    assert _read(lakes.main, sql) == (Status.OK, None, [{"n": 4, "total": total}])

    # What the same listing does to a read that still lists: the hazard the
    # named fetch takes every ordinary read out of.
    list_the_lake.arm()
    assert _read(lakes.main, sql) == (Status.OK, None, [{"n": 8, "total": 2 * total}])


def test_a_table_written_after_a_read_is_found_by_name(lakes):
    """Nothing about the lake is remembered between reads."""
    assert _read(lakes.main, "SELECT * FROM late")[0] is Status.ERROR
    DataWriter(super_name=lakes.main, organization=ORG).write(
        role_name=ADMIN, simple_name="late", overwrite_columns=[],
        data=pl.DataFrame({"id": [7], "v": ["x"]}))
    status, message, rows = _read(lakes.main, "SELECT id, v FROM late")
    assert status is Status.OK, message
    assert rows == [{"id": 7, "v": "x"}]


def test_a_read_sees_the_write_before_it(lakes):
    """The leaf fetched by name is the catalog's current one."""
    writer = DataWriter(super_name=lakes.main, organization=ORG)
    for value in (1, 2, 3):
        writer.write(
            role_name=ADMIN, simple_name="counter", overwrite_columns=["k"],
            data=pl.DataFrame({"k": ["only"], "value": [value]}))
        status, message, rows = _read(lakes.main, "SELECT value FROM counter")
        assert status is Status.OK, message
        assert rows == [{"value": value}]
