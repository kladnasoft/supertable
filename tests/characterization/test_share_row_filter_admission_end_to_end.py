"""A linked share's row filter, behind a real reader.

A linked share carries its row filter on the snapshot as ``_row_filter``, and
the reader writes it into the ``WHERE`` of the view the share is read through.
It was written in as it came:

  * text that held a second statement ran it on the reader's connection;
  * text that closed a parenthesis undid the consumer role's own row filter,
    which it is merged with;
  * a value that was not text at all (a dict, a list) was skipped, and the
    whole table was served.

A table is written here with the real writer on LOCAL storage, given a
``_row_filter`` the way a share's snapshot carries one, and read with the real
reader, RBAC and DuckDB.

``supertable/tests/test_row_predicate_admission.py`` is the unit side: what is
admitted and refused, and why the admitted text is never rewritten.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import system_query
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine import engine_common
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads the rows in ``eu``.
EU_ONLY = "eu_rows"

ROWS = [
    {"id": 1, "region": "eu", "salary": 40_000},
    {"id": 2, "region": "us", "salary": 900_000},
    {"id": 3, "region": "eu", "salary": 55_000},
    {"id": 4, "region": "apac", "salary": 70_000},
]
ALL_IDS = [1, 2, 3, 4]
SELECT_IDS = "SELECT id FROM emp ORDER BY id"


@pytest.fixture
def share(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    system_query._admit_row_predicate.cache_clear()
    sup = f"share_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    DataWriter(super_name=sup, organization=ORG).write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame(ROWS))
    RoleManager(super_name=sup, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": EU_ONLY, "role": "reader", "tables": {
            "emp": {"columns": ["*"],
                    "filters": {"region": {"operation": "=", "type": "value",
                                           "value": "eu"}}}}})

    def filtered_by(row_filter) -> None:
        """Publish the snapshot again carrying *row_filter*, as a share's does."""
        leaf = RedisCatalog().get_leaf(ORG, sup, "emp")
        payload = dict(leaf["payload"])
        payload["_row_filter"] = row_filter
        RedisCatalog().set_leaf_payload_cas(ORG, sup, "emp", payload, leaf["path"])

    def attempt(sql: str = SELECT_IDS, role: str = ADMIN):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
        return status, message, frame

    def ids(sql: str = SELECT_IDS, role: str = ADMIN):
        status, message, frame = attempt(sql, role)
        assert status is Status.OK, message
        return frame["id"].to_list()

    yield SimpleNamespace(sup=sup, filtered_by=filtered_by, attempt=attempt, ids=ids)
    engine_duckdb.reset_shared_duckdb_state()
    system_query._admit_row_predicate.cache_clear()


def _refused(share, role: str = ADMIN) -> str:
    """The read is refused and nothing is served; returns the message."""
    status, message, frame = share.attempt(role=role)
    assert status is Status.ERROR
    assert frame.height == 0
    assert "cannot establish the share row filter" in message
    return message


# ---------------------------------------------------------------------------
# A filter that is one works as it did
# ---------------------------------------------------------------------------

def test_a_table_without_a_share_filter_is_read_whole(share):
    assert share.ids() == ALL_IDS


@pytest.mark.parametrize("no_filter", [None, ""])
def test_a_share_that_says_it_has_no_filter_is_read_whole(share, no_filter):
    share.filtered_by(no_filter)
    assert share.ids() == ALL_IDS


@pytest.mark.parametrize("row_filter, expected", [
    ("region = 'eu'", [1, 3]),
    ("salary >= 55000", [2, 3, 4]),
    ("region IN ('eu', 'apac') AND salary < 60000", [1, 3]),
    ("region <> 'eu' OR id = 1", [1, 2, 4]),
    ("lower(region) LIKE 'a%'", [4]),
    ("id = ANY([2, 4])", [2, 4]),
])
def test_a_share_filter_narrows_the_table(share, row_filter, expected):
    share.filtered_by(row_filter)
    assert share.ids() == expected
    assert share.ids("SELECT count(*) AS id FROM emp") == [len(expected)]


def test_a_share_filter_and_a_role_filter_are_both_applied(share):
    share.filtered_by("salary < 50000 OR region = 'us'")
    # the role alone: 1 and 3; the share alone: 1 and 2; together: 1
    assert share.ids(role=EU_ONLY) == [1]


def test_the_text_reaches_the_view_as_the_provider_wrote_it(share, monkeypatch):
    """Admitted is not rewritten: the view's WHERE is the text itself."""
    seen = []
    real = engine_common.create_rbac_view

    def create_rbac_view(con, base, view, view_def, **kwargs):
        seen.append(view_def.where_clause)
        return real(con, base, view, view_def, **kwargs)

    monkeypatch.setattr(engine_duckdb, "create_rbac_view", create_rbac_view)
    share.filtered_by("region  IN ('eu','apac')")
    assert share.ids() == [1, 3, 4]
    assert share.ids(role=EU_ONLY) == [1, 3]
    assert seen[0] == "region  IN ('eu','apac')"
    assert seen[1].endswith("AND (region  IN ('eu','apac'))")


def test_duckdb_s_whole_string_match_is_still_a_whole_string_match(share):
    """``~`` matches the whole string in DuckDB. Had the admitted text been
    written back out by SQLGlot, this would match any part of it and let
    ``eu`` through."""
    share.filtered_by("region ~ 'u'")
    assert share.ids() == []
    share.filtered_by("region ~ '.u'")
    assert share.ids() == [1, 3]


def test_a_streamed_read_is_filtered_the_same(share):
    import pyarrow as pa

    share.filtered_by("region = 'eu'")
    handle = DataReader(
        super_name=share.sup, organization=ORG, query="SELECT id FROM emp",
    ).stream(role_name=ADMIN, engine=Engine.DUCKDB)
    with handle:
        table = pa.Table.from_batches(list(handle.batches()), schema=handle.schema)
    assert sorted(table.column("id").to_pylist()) == [1, 3]


# ---------------------------------------------------------------------------
# A filter that is not one is not run and not skipped: the read is refused
# ---------------------------------------------------------------------------

def _tables_on_the_connection() -> set:
    con = engine_duckdb._shared_state()["con"]
    return {row[0] for row in con.execute(
        "SELECT table_name FROM duckdb_tables()").fetchall()}


def test_a_second_statement_in_the_filter_is_not_run(share):
    assert share.ids() == ALL_IDS            # the connection every read shares exists
    before = _tables_on_the_connection()

    share.filtered_by("1=1; CREATE TABLE planted AS SELECT 42 AS x")
    _refused(share)

    assert _tables_on_the_connection() == before
    assert "planted" not in _tables_on_the_connection()


def test_a_filter_cannot_close_the_parenthesis_it_is_merged_in(share):
    """Merged as ``(<role>) AND (<share>)``, this text used to read
    ``(<role>) AND (1=1) OR (1=1)`` and serve every row to a role that may
    read ``eu`` alone."""
    assert share.ids(role=EU_ONLY) == [1, 3]
    share.filtered_by("1=1) OR (1=1")
    _refused(share, role=EU_ONLY)
    _refused(share)


@pytest.mark.parametrize("row_filter", [
    "id IN (SELECT id FROM emp)",
    "EXISTS (SELECT 1 FROM duckdb_tables())",
    "id = (SELECT max(id) FROM emp)",
    "current_setting('memory_limit') IS NOT NULL",
    "length(read_text('/etc/hostname')) > 0",
    "1=1 UNION SELECT 1",
    "1=1 ORDER BY id",
    "region = 'eu' -- and nothing after this",
    "WHERE region = 'eu'",
    "region = 'eu",
    "   ",
])
def test_text_that_is_not_one_expression_over_the_row_refuses_the_read(share, row_filter):
    share.filtered_by(row_filter)
    _refused(share)


@pytest.mark.parametrize("row_filter", [
    {"region": "eu"}, ["region = 'eu'"], 7, True, {}, [], 0, False,
])
def test_a_filter_that_is_not_text_refuses_the_read(share, row_filter):
    """It used to be skipped, which served the table the share was meant to
    narrow."""
    share.filtered_by(row_filter)
    _refused(share)


def test_a_refused_streamed_read_opens_no_stream(share):
    share.filtered_by("1=1; CREATE TABLE planted AS SELECT 1")
    with pytest.raises(RuntimeError, match="cannot establish the share row filter"):
        DataReader(
            super_name=share.sup, organization=ORG, query="SELECT id FROM emp",
        ).stream(role_name=ADMIN, engine=Engine.DUCKDB)


def test_the_refusal_does_not_show_the_consumer_the_provider_s_filter(share):
    share.filtered_by("customer_tier = 'PLATINUM-XJ9' GROUP BY customer_tier")
    message = _refused(share)
    assert "customer_tier" not in message
    assert "PLATINUM" not in message


def test_a_filter_that_is_repaired_is_read_again(share):
    share.filtered_by("1=1) OR (1=1")
    _refused(share)
    share.filtered_by("region = 'apac'")
    assert share.ids() == [4]


def test_a_refused_filter_on_one_table_leaves_another_table_readable(share):
    DataWriter(super_name=share.sup, organization=ORG).write(
        role_name=ADMIN, simple_name="other", overwrite_columns=[],
        data=pl.DataFrame({"id": [10, 11]}))
    share.filtered_by("1=1; CREATE TABLE planted AS SELECT 1")
    _refused(share)
    assert share.ids("SELECT id FROM other ORDER BY id") == [10, 11]
