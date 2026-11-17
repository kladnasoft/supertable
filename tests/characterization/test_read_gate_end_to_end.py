"""The read gate against a real table, reader, role and engine.

``supertable/tests/test_read_gate_lateral_functions_comments.py`` checks the
gate form by form as a pure function. This file checks what those forms were
for: a role that may read two columns of one table asks for a file on the
engine's machine, and what comes back is looked at.

Nothing is stubbed: the real writer, reader and RBAC, the catalog's Lua on
fakeredis, LOCAL storage and a real DuckDB. The file asked for exists and is
readable, so a refusal here is the gate's and not the file system's — every
one of these returned ``SENTINEL`` on 3.5.3.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.data_reader import DataReader, Status, query_sql
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor
from supertable.quality import run_check
from supertable.quality.config import DQConfig
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``region`` of ``emp``. Not ``salary``.
READER = "two_columns"
#: May write ``emp`` — so may register a quality rule on it — and read no
#: more of it than ``READER``.
RULE_AUTHOR = "rule_author"

#: What the file holds. It must never be in anything a query returns.
SENTINEL = "TOP-SECRET-FILE-CONTENT-7f3a"


@pytest.fixture
def secret(tmp_path):
    path = tmp_path / "secret.txt"
    path.write_text(SENTINEL + "\n")
    return str(path)


@pytest.fixture
def lake(hermetic_fakeredis):
    """One table, two rows, two roles — in a SuperTable of this test's own.

    The name is new each time because LOCAL storage outlives a test: a second
    write to a lake of the same name would add its rows to the first's.
    """
    sup = f"gate_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    DataWriter(super_name=sup, organization=ORG).write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({
            "id": [1, 2], "region": ["eu", "us"], "salary": [40_000, 900_000],
            "tags": [["a", "b"], ["c"]],
        }))
    roles = RoleManager(super_name=sup, organization=ORG, actor_role_name=ADMIN)
    roles.create_role({"role_name": READER, "role": "reader", "tables": {
        "emp": {"columns": ["id", "region", "tags"], "filters": ["*"]}}})
    roles.create_role({"role_name": RULE_AUTHOR, "role": "writer", "tables": {
        "emp": {"columns": ["id", "region", "tags"], "filters": ["*"]}}})

    def read(sql: str, engine: Engine = Engine.DUCKDB):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=READER, engine=engine)
        return frame, status, message or ""

    def refused(sql: str, match: str) -> None:
        frame, status, message = read(sql)
        assert status is Status.ERROR, f"ran: {frame.to_dicts()}"
        assert match in message, message
        assert frame.is_empty()
        assert SENTINEL not in message

    return SimpleNamespace(sup=sup, redis=hermetic_fakeredis, read=read, refused=refused)


# --------------------------------------------------------------------------
# The forms confirmed on the Island
# --------------------------------------------------------------------------

def test_the_role_reads_its_table(lake):
    frame, status, _ = lake.read("SELECT id, region FROM emp ORDER BY id")
    assert status is Status.OK
    assert frame.to_dicts() == [{"id": 1, "region": "eu"}, {"id": 2, "region": "us"}]


@pytest.mark.parametrize("source", [
    "read_text('{secret}')",
    "read_blob('{secret}')",
    "read_csv('{secret}', header = false)",
    "glob('{secret}')",
    "sniff_csv('{secret}')",
    "main.read_text('{secret}')",
    '"read_text"(\'{secret}\')',
])
def test_a_lateral_function_does_not_read_a_file(lake, secret, source):
    lake.refused(
        "SELECT h.* FROM (SELECT 1 FROM emp LIMIT 1) x, LATERAL "
        + source.format(secret=secret) + " h",
        "not permitted on the read path")


def test_the_file_is_there_to_be_read(lake, secret):
    """Why the refusals above mean something: the engine reads the file when
    asked directly, so it was the gate that refused."""
    import duckdb

    assert duckdb.connect().execute(
        "SELECT content FROM read_text(?)", [secret]).fetchone()[0].strip() == SENTINEL


@pytest.mark.parametrize("sql", [
    "SELECT s.* FROM emp, LATERAL duckdb_secrets() s",
    "SELECT current_setting('threads') AS t FROM emp",
    "SELECT current_setting('temp_directory') AS t FROM emp",
    "SELECT id FROM emp WHERE region = current_setting('threads')",
])
def test_engine_state_is_not_read(lake, sql):
    lake.refused(sql, "not permitted on the read path")


def test_explain_and_the_mcp_entry_point_refuse_the_same(lake, secret):
    sql = f"SELECT h.content FROM emp, LATERAL read_text('{secret}') h"
    lake.refused(f"EXPLAIN {sql}", "not permitted on the read path")
    with pytest.raises(RuntimeError) as refusal:
        query_sql(ORG, lake.sup, sql, 10, Engine.DUCKDB, READER)
    assert "not permitted on the read path" in str(refusal.value)
    assert SENTINEL not in str(refusal.value)


# --------------------------------------------------------------------------
# What a LATERAL may read still runs
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql, expected", [
    ("SELECT e.id, s.twice FROM emp e, LATERAL (SELECT e.id * 2 AS twice) s ORDER BY e.id",
     [{"id": 1, "twice": 2}, {"id": 2, "twice": 4}]),
    ("SELECT e.id, r.range AS n FROM emp e, LATERAL range(0, e.id) r ORDER BY e.id, n",
     [{"id": 1, "n": 0}, {"id": 2, "n": 0}, {"id": 2, "n": 1}]),
    ("SELECT e.id, g.generate_series AS n FROM emp e, LATERAL generate_series(1, e.id) g "
     "ORDER BY e.id, n",
     [{"id": 1, "n": 1}, {"id": 2, "n": 1}, {"id": 2, "n": 2}]),
    ("SELECT e.id, u.tag FROM emp e, LATERAL UNNEST(e.tags) AS u(tag) ORDER BY e.id, u.tag",
     [{"id": 1, "tag": "a"}, {"id": 1, "tag": "b"}, {"id": 2, "tag": "c"}]),
    ("SELECT e.id, v.x FROM emp e, LATERAL (VALUES (10), (20)) v(x) ORDER BY e.id, v.x",
     [{"id": 1, "x": 10}, {"id": 1, "x": 20}, {"id": 2, "x": 10}, {"id": 2, "x": 20}]),
    ("SELECT e.id, m.top FROM emp e LEFT JOIN LATERAL "
     "(SELECT max(o.id) AS top FROM emp o WHERE o.id <= e.id) m ON TRUE ORDER BY e.id",
     [{"id": 1, "top": 1}, {"id": 2, "top": 2}]),
])
def test_an_admitted_lateral_returns_its_rows(lake, sql, expected):
    frame, status, message = lake.read(sql)
    assert status is Status.OK, message
    assert frame.to_dicts() == expected


def test_a_lateral_subquery_is_held_to_the_roles_columns(lake):
    """Its table is authorized like any other the query names."""
    with pytest.raises(PermissionError, match="salary"):
        lake.read("SELECT e.id, s.salary FROM emp e, LATERAL "
              "(SELECT o.salary FROM emp o WHERE o.id = e.id) s")


# --------------------------------------------------------------------------
# A comment's text never reaches the engine
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT id -- */ , (SELECT content FROM read_text('{secret}')) AS leak /*\nFROM emp ORDER BY id",
    "SELECT id FROM emp -- */ , LATERAL read_text('{secret}') h /*\nORDER BY id",
    "SELECT id -- */ , salary /*\nFROM emp ORDER BY id",
    "SELECT id -- */ , current_setting('threads') AS t /*\nFROM emp ORDER BY id",
])
def test_a_comment_that_closes_itself_is_only_a_comment(lake, secret, sql):
    """On 3.5.3 the first of these returned the file. The gate saw one column
    and a comment; the engine was sent the comment's text as SQL."""
    frame, status, message = lake.read(sql.format(secret=secret))
    assert status is Status.OK, message
    assert frame.to_dicts() == [{"id": 1}, {"id": 2}]


def test_the_engine_is_sent_no_comment(lake, secret, monkeypatch):
    """Looked at where it is handed to DuckDB, not inferred from the rows."""
    sent = []
    rewrite = engine_duckdb.rewrite_query_with_hashed_tables

    def recording(*args, **kwargs):
        sent.append(rewrite(*args, **kwargs))
        return sent[-1]

    monkeypatch.setattr(engine_duckdb, "rewrite_query_with_hashed_tables", recording)
    _, status, message = lake.read(
        f"SELECT id /* note */ -- */ , (SELECT content FROM read_text('{secret}')) /*\n"
        "FROM emp")
    assert status is Status.OK, message
    assert len(sent) == 1
    assert "read_text" not in sent[0] and "/*" not in sent[0] and "note" not in sent[0]


# --------------------------------------------------------------------------
# A read all the way down
# --------------------------------------------------------------------------

def test_select_into_creates_nothing(lake):
    """SQLGlot writes SELECT ... INTO out as CREATE TABLE ... AS SELECT. Until
    3.5.4 the gate admitted it, and what stopped it was a later step — its
    target is not a table of the catalog — rather than its not being a read."""
    lake.refused("SELECT id INTO stolen FROM emp", "INTO is not permitted")
    lake.refused("WITH w AS (DELETE FROM emp RETURNING *) SELECT * FROM w",
             "DELETE is not permitted")
    frame, status, _ = lake.read("SELECT count(*) AS n FROM emp")
    assert status is Status.OK and frame.to_dicts() == [{"n": 2}]


# --------------------------------------------------------------------------
# One reading, whatever the engine
# --------------------------------------------------------------------------

def test_a_query_pinned_to_spark_is_authorized_by_the_reading_spark_is_sent(
        lake, monkeypatch):
    """Spark's grammar reads one table here, ``emp``, which the role may read.
    DuckDB's reads Spark's file datasource, and DuckDB's reading is the one
    written out for Spark. On 3.5.3 the tables came from the first and the
    query from the second; now the query is refused before any engine is
    chosen, because ``text`` is no SuperTable."""
    def no_engine(self, *args, **kwargs):
        raise AssertionError("the query reached an engine")

    monkeypatch.setattr(Executor, "execute", no_engine)
    monkeypatch.setattr(Executor, "stream", no_engine)

    sql = "SELECT 'a\\' AS x FROM text.\"/etc/passwd\" WHERE value <> ' FROM emp --'"
    for engine in (Engine.SPARK_SQL, Engine.AUTO, Engine.DUCKDB):
        frame, status, message = lake.read(sql, engine=engine)
        assert status is Status.ERROR
        assert "text" in message and frame.is_empty()


def test_java_behind_a_backslash_quote_is_refused_for_spark(lake):
    sql = ("SELECT 'a\\' AS x, reflect('java.lang.System', 'getenv', 'HOME') "
           "FROM emp -- '")
    frame, status, message = lake.read(sql, engine=Engine.SPARK_SQL)
    assert status is Status.ERROR and "calls Java" in message


# --------------------------------------------------------------------------
# A custom SQL quality rule runs through the same reader
# --------------------------------------------------------------------------

def _rule(lake, sql: str, **rule):
    """*sql* names its table ``{emp}``, written in as the rule's own SQL names
    one: both parts quoted."""
    return DQConfig(lake.redis, ORG, lake.sup, actor_role_name=RULE_AUTHOR).create_rule(
        {"table_name": "emp", "rule_type": "custom_sql",
         "sql": sql.replace("{emp}", f'"{lake.sup}"."emp"'), **rule})


def _run_rules(lake):
    dqc = DQConfig(lake.redis, ORG, lake.sup)
    return run_check(lake.redis, ORG, lake.sup, "emp", "custom", dqc, 0), dqc


def test_a_custom_sql_rule_runs(lake):
    """``scheduler._run_custom_check`` hands a rule's SQL to ``DataReader`` as
    the role that registered it."""
    rule = _rule(lake, "SELECT count(*) AS violations FROM {emp} WHERE id < 0",
                 threshold=0)
    outcome, dqc = _run_rules(lake)
    assert outcome.state == "success"
    assert [(r["rule_id"], r["status"], r["value"])
            for r in dqc.get_latest("emp")["rule_results"]] == [(rule["rule_id"], "ok", 0)]


@pytest.mark.parametrize("sql", [
    # A rule's result is published to the table's latest quality record,
    # where its author reads it back.
    "SELECT h.content FROM {emp}, LATERAL read_text('{secret}') h",
    "SELECT count(*) -- */ + (SELECT length(content) FROM read_text('{secret}')) /*\n"
    "FROM {emp}",
    "SELECT current_setting('temp_directory') FROM {emp}",
    "SELECT count(*) INTO stolen FROM {emp}",
])
def test_a_custom_sql_rule_reads_no_file_and_no_engine_state(lake, secret, sql):
    import json

    _rule(lake, sql.replace("{secret}", secret))
    outcome, dqc = _run_rules(lake)

    published = json.dumps(dqc.get_latest("emp") or {}, default=str)
    assert SENTINEL not in published and str(len(SENTINEL) + 1) not in published
    if "-- */" in sql:
        # The comment is only a comment: the rule is the count it started as.
        assert outcome.state == "success"
        assert [r["value"] for r in dqc.get_latest("emp")["rule_results"]] == [2]
    else:
        assert outcome.state == "failed"
        assert dqc.get_latest("emp") is None


# --------------------------------------------------------------------------
# A CTE is a CTE only where the engine reads it as one
# --------------------------------------------------------------------------

def _reflection_view(lake, monkeypatch) -> str:
    """The name of the view the reader builds over ``emp``'s files — every
    column and every row of them, under the views that apply the deletion
    vector and the role's policy. Read off what the engine is sent."""
    import re

    sent = []
    rewrite = engine_duckdb.rewrite_query_with_hashed_tables
    monkeypatch.setattr(engine_duckdb, "rewrite_query_with_hashed_tables",
                        lambda *a, **k: sent.append(rewrite(*a, **k)) or sent[-1])
    _, status, message = lake.read("SELECT id FROM emp")
    monkeypatch.undo()
    assert status is Status.OK, message
    return re.search(r"st_[0-9a-f]{16}", sent[0]).group(0)


@pytest.mark.parametrize("sql", [
    # The engine's own catalog, behind a CTE of the same name.
    "WITH tables AS (SELECT 1 AS x FROM emp) "
    "SELECT t.table_name FROM information_schema.tables t",
    "WITH duckdb_views AS (SELECT 1 AS x FROM emp) "
    "SELECT v.view_name, v.sql FROM main.duckdb_views v",
    "SELECT v.sql FROM (WITH duckdb_views AS (SELECT 1 AS x FROM emp) "
    "SELECT x FROM duckdb_views) a, duckdb_views v",
    "WITH a AS (SELECT view_name, sql FROM duckdb_views), "
    "duckdb_views AS (SELECT 1 AS x FROM emp) SELECT * FROM a",
])
def test_a_cte_name_does_not_hide_the_engines_catalog(lake, sql):
    """On 3.5.3 each of these listed the views on the engine's connection —
    with ``sql``, the files under them — because a CTE of that name anywhere
    in the query made the reference a CTE to the parser, and the executor left
    what it had no view for as it was written."""
    lake.refused(sql, "not found")


@pytest.mark.parametrize("shape", [
    "WITH {view} AS (SELECT 1 AS x FROM emp) SELECT s.* FROM main.{view} s",
    "SELECT s.* FROM (WITH {view} AS (SELECT 1 AS x FROM emp) SELECT x FROM {view}) a, {view} s",
    "WITH a AS (SELECT * FROM {view}), {view} AS (SELECT 1 AS x FROM emp) SELECT * FROM a",
])
def test_a_cte_name_does_not_reach_a_tables_unfiltered_view(lake, monkeypatch, shape):
    """The same, aimed at the reader's own view of ``emp``. On 3.5.3 the first
    shape returned ``salary``, ``__rowid__`` and ``__timestamp__`` to a role
    that may read ``id`` and ``region``: the rows as they are in the files,
    under every view that would have masked, filtered or removed them."""
    view = _reflection_view(lake, monkeypatch)
    frame, status, message = lake.read(shape.format(view=view))
    assert status is Status.ERROR, frame.to_dicts()
    assert "not found" in message and frame.is_empty()


@pytest.mark.parametrize("sql, expected", [
    ("WITH w AS (SELECT id, region FROM emp) SELECT * FROM w ORDER BY id",
     [{"id": 1, "region": "eu"}, {"id": 2, "region": "us"}]),
    ("WITH a AS (SELECT id FROM emp), b AS (SELECT id * 10 AS ten FROM a) "
     "SELECT a.id, b.ten FROM a JOIN b ON b.ten = a.id * 10 ORDER BY a.id",
     [{"id": 1, "ten": 10}, {"id": 2, "ten": 20}]),
    ("SELECT x.id FROM (WITH w AS (SELECT id FROM emp WHERE region = 'us') SELECT * FROM w) x",
     [{"id": 2}]),
    ("WITH w AS (SELECT id FROM emp WHERE region = 'eu') "
     "SELECT e.id FROM emp e WHERE e.id IN (SELECT id FROM w)",
     [{"id": 1}]),
    ("WITH RECURSIVE r AS (SELECT id FROM emp WHERE id = 1 UNION ALL "
     "SELECT id + 1 FROM r WHERE id < 3) SELECT id FROM r ORDER BY id",
     [{"id": 1}, {"id": 2}, {"id": 3}]),
])
def test_a_cte_where_the_engine_reads_one_still_runs(lake, sql, expected):
    frame, status, message = lake.read(sql)
    assert status is Status.OK, message
    assert frame.to_dicts() == expected
