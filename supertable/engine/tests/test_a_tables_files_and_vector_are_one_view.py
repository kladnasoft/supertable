"""
A table's files and its deletion vector are one view, not a view on a view.

A read is made of layers: the reflection (the kept files, read with
``parquet_scan``), the tombstone layer (the system columns stripped, the
deletion vector anti-joined), and for a role with a column list or a row
filter the RBAC view.  Each was a view of its own over the one below.  DuckDB
resolves a view's files when the view is created and again whenever something
is bound over it, and opens every file each time: the reflection view, the
tombstone view over it and the query made three passes over every file of a
read, 0.19 ms a file each.

The reflection is now a subquery of the tombstone layer's view, so a read
makes two passes.  The layers are the same SELECTs as before; the RBAC view is
made as before.  A view per layer is kept where the order of the statements
matters or the reflection is not a view:

* a deletion vector whose parts this query has to read from an object store
  (the data files are bound first, because whether they had to be signed
  decides how the vector is read);
* ``SUPERTABLE_DUCKDB_MATERIALIZE=table``.

Three groups: the statements (on a connection that only records), the two
shapes against each other on real DuckDB, and the engine's real stream over
real files.
"""

from __future__ import annotations

import dataclasses
import re
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

import supertable.engine.duckdb as engine_duckdb
import supertable.engine.engine_common as ec
from supertable.config.settings import settings as real_settings
from supertable.data_classes import RbacViewDef, Reflection, SuperSnapshot, TombstoneDef
from supertable.engine.engine_config import EngineRuntimeConfig
from supertable.utils.sql_parser import SQLParser

RUNTIME = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=None, duckdb_external_cache_size="",
)

STRIP = "COLUMNS(c -> c NOT IN ('__rowid__', '__timestamp__'))"
SCAN = ("parquet_scan(['a.parquet', 'b.parquet'], "
        "union_by_name=TRUE, HIVE_PARTITIONING=FALSE)")


class _Recorder:
    """A connection that records what it is sent."""

    def __init__(self, refuse=None, error="HTTP Error: HTTP GET error (HTTP 403)"):
        self.refuse, self.error, self.sql = refuse, error, []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        if self.refuse and self.refuse in str(sql):
            raise RuntimeError(self.error)
        return self


# --------------------------------------------------------------------------
# The statements
# --------------------------------------------------------------------------

def test_a_reflection_view_by_itself_is_the_statement_it_was():
    con = _Recorder()
    ec.create_reflection_view(con, "st_x", ["a.parquet", "b.parquet"], ["id", "__rowid__"])
    assert con.sql == [
        "CREATE OR REPLACE VIEW st_x AS "
        "SELECT id, COLUMNS(c -> c IN ('__rowid__', '__timestamp__')) "
        f"FROM {SCAN};"]

    ec.create_reflection_view(con, "st_y", ["it's.parquet"])
    assert con.sql[1] == ("CREATE OR REPLACE VIEW st_y AS SELECT * FROM "
                          "parquet_scan(['it''s.parquet'], union_by_name=TRUE, "
                          "HIVE_PARTITIONING=FALSE);")


@pytest.mark.parametrize("kwargs, select", [
    ({}, f"SELECT {STRIP} FROM st_x"),
    ({"expose_rowid": True}, "SELECT COLUMNS(c -> c NOT IN ('__timestamp__')) FROM st_x"),
    ({"dv_table": "dv_abc"},
     f"SELECT {STRIP} FROM st_x ANTI JOIN dv_abc AS __dv__ "
     "ON st_x.__rowid__ = __dv__.__rowid__"),
], ids=["no vector", "row id kept", "cached vector"])
def test_a_tombstone_view_by_itself_is_the_statement_it_was(kwargs, select):
    con = _Recorder()
    definition = TombstoneDef(tombstone_path=["p1.parquet"], cache_key="k") \
        if "dv_table" in kwargs else None
    assert ec.create_tombstone_view(con, "st_x", "tomb_x", definition, **kwargs) is False
    assert con.sql == [f"CREATE OR REPLACE VIEW tomb_x AS {select};"]
    # ... and the layer as a SELECT is that statement's body
    assert ec.tombstone_select("st_x", "st_x", **kwargs) == select


def test_a_tombstone_view_over_parts_is_the_statement_it_was():
    con = _Recorder()
    definition = TombstoneDef(tombstone_path=["p1.parquet", "p'2.parquet"], cache_key=None)
    assert ec.create_tombstone_view(con, "st_x", "tomb_x", definition) is False
    select = (f"SELECT {STRIP} FROM st_x ANTI JOIN (SELECT DISTINCT __rowid__ FROM "
              "read_parquet(['p1.parquet', 'p''2.parquet'])) AS __dv__ "
              "ON st_x.__rowid__ = __dv__.__rowid__")
    assert con.sql == [f"CREATE OR REPLACE VIEW tomb_x AS {select};"]
    assert ec.tombstone_select(
        "st_x", "st_x", parts=["p1.parquet", "p'2.parquet"]) == select
    # a single path, as snapshots older than the list of parts hold it
    assert ec.vector_parts(TombstoneDef(tombstone_path="p1.parquet")) == ["p1.parquet"]
    assert ec.vector_parts(TombstoneDef(tombstone_path=["p1.parquet", None, ""])) == ["p1.parquet"]
    assert ec.vector_parts(None) == [] and ec.vector_parts(TombstoneDef()) == []


def test_the_reflection_is_a_subquery_of_the_layer_folded_over_it():
    con = _Recorder()

    def over(select):
        return ec.tombstone_select(f"({select}) AS st_x", "st_x", dv_table="dv_abc")

    ec.create_reflection_view(con, "st_x_q1", ["a.parquet", "b.parquet"], over=over)
    assert con.sql == [
        f"CREATE OR REPLACE VIEW st_x_q1 AS SELECT {STRIP} "
        f"FROM (SELECT * FROM {SCAN}) AS st_x "
        "ANTI JOIN dv_abc AS __dv__ ON st_x.__rowid__ = __dv__.__rowid__;"]


def test_an_empty_table_folds_as_well():
    con = _Recorder()
    ec.create_reflection_view(
        con, "st_x_q1", [], ["id"], {"id": "Int64", "region": "String"},
        over=lambda select: ec.tombstone_select(f"({select}) AS st_x", "st_x"))
    assert con.sql == [
        f"CREATE OR REPLACE VIEW st_x_q1 AS SELECT {STRIP} "
        "FROM (SELECT CAST(NULL AS BIGINT) AS id WHERE 1=0) AS st_x;"]
    with pytest.raises(ValueError, match="No files provided for reflection view 'st_x_q1'"):
        ec.create_reflection_view(con, "st_x_q1", [], over=lambda select: select)


class _Signer:
    def __init__(self):
        self.asked = []

    def presign(self, key):
        self.asked.append(key)
        return f"https://signed.example/{key}?sig=ok"


@pytest.fixture
def no_store(monkeypatch):
    monkeypatch.setattr(ec, "configure_httpfs_and_s3", lambda con, paths: None)
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        real_settings, SUPERTABLE_DUCKDB_MATERIALIZE="view", STORAGE_BUCKET=""))


def test_refused_files_are_signed_and_the_same_view_is_made_over_them(no_store):
    """The fallback of the reflection's own files, with a layer folded over."""
    con, storage = _Recorder(refuse="s3://"), _Signer()

    def over(select):
        return ec.tombstone_select(f"({select}) AS st_x", "st_x", dv_table="dv_abc")

    used = ec.create_reflection_view_with_presign_retry(
        con, storage, "st_x_q1", ["s3://bucket/acme/a.parquet"], over=over)
    assert used is True
    assert storage.asked == ["acme/a.parquet"]
    first, second = con.sql
    assert "'s3://bucket/acme/a.parquet'" in first
    assert second == first.replace(
        "s3://bucket/acme/a.parquet", "https://signed.example/acme/a.parquet?sig=ok")
    assert "ANTI JOIN dv_abc" in second and second.startswith("CREATE OR REPLACE VIEW st_x_q1 AS")


def test_files_that_are_read_directly_are_not_signed(no_store):
    con, storage = _Recorder(), _Signer()
    assert ec.create_reflection_view_with_presign_retry(
        con, storage, "st_x_q1", ["s3://bucket/acme/a.parquet"],
        over=lambda select: ec.tombstone_select(f"({select}) AS st_x", "st_x")) is False
    assert len(con.sql) == 1 and storage.asked == []


def test_an_error_that_is_no_refusal_is_the_querys_error(no_store):
    con = _Recorder(refuse="parquet_scan", error='Binder Error: column "nope" not found')
    with pytest.raises(RuntimeError, match="Binder Error"):
        ec.create_reflection_view_with_presign_retry(
            con, _Signer(), "st_x_q1", ["s3://bucket/acme/a.parquet"],
            over=lambda select: ec.tombstone_select(f"({select}) AS st_x", "st_x"))
    assert len(con.sql) == 1


def test_nothing_is_folded_into_a_materialised_reflection(monkeypatch):
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        real_settings, SUPERTABLE_DUCKDB_MATERIALIZE="table"))
    con = _Recorder()
    with pytest.raises(ValueError, match="materialised reflection is a table"):
        ec.create_reflection_view_with_presign_retry(
            con, None, "st_x_q1", ["a.parquet"], over=lambda select: select)
    assert con.sql == []


def test_the_cache_says_which_versions_it_holds():
    con, cache = _Recorder(), ec.TombstoneCache(8, 300)
    assert cache.holds("k1") is False and cache.holds(None) is False
    cache.acquire(con, "k1", ["p1.parquet"])
    assert cache.holds("k1") is True and cache.holds("k2") is False
    cache.release(con, "k1")
    assert cache.holds("k1") is True                          # kept for the next read
    off = ec.TombstoneCache(0)
    assert off.acquire(con, "k1", ["p1.parquet"]) is None and off.holds("k1") is False


# --------------------------------------------------------------------------
# The two shapes against each other, on real DuckDB
# --------------------------------------------------------------------------

@pytest.fixture
def table(tmp_path, duckdb_con):
    """Three files of one table: the second has a column the first lacks, the
    third was written before the system columns existed.  A deletion vector in
    two parts takes three rows away."""
    def write(name, sql):
        path = str(tmp_path / name)
        duckdb_con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
        return path

    files = [
        write("a.parquet", "SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'eu' ELSE 'us' END AS region, "
                           "i * 10 AS amount, i AS __rowid__, 1000 + i AS __timestamp__ "
                           "FROM range(1, 5) t(i)"),
        write("b.parquet", "SELECT i AS id, 'eu' AS region, i * 10 AS amount, 'n' || i AS note, "
                           "i AS __rowid__, 2000 + i AS __timestamp__ FROM range(5, 9) t(i)"),
        write("c.parquet", "SELECT i AS id, 'apac' AS region, i * 10 AS amount "
                           "FROM range(9, 11) t(i)"),
    ]
    parts = [
        write("dv-1.parquet", "SELECT 'a.parquet' AS __file__, 2 AS __rowid__"),
        write("dv-2.parquet", "SELECT 'b.parquet' AS __file__, r AS __rowid__ FROM (VALUES (7), (8)) v(r)"),
    ]
    return SimpleNamespace(files=files, parts=parts)


QUERIES = [
    "SELECT * FROM {v} ORDER BY id",
    "SELECT count(*) AS n, sum(amount) AS s FROM {v}",
    "SELECT region, count(*) AS n FROM {v} GROUP BY region ORDER BY region",
    "SELECT id FROM {v} WHERE region = 'eu' AND amount > 30 ORDER BY id",
    "SELECT a.id, b.amount FROM {v} a JOIN {v} b ON a.id = b.id WHERE a.id < 4 ORDER BY a.id",
]


EVERY = None
WITH_SYSTEM = ["id", "region", "amount", "__rowid__", "__timestamp__"]
#: Without ``__rowid__``: a projection the reader never makes of a table with
#: a vector, which is applied by that column.
NAMED = ["amount", "id"]


@pytest.mark.parametrize("columns, vector", [
    (EVERY, "none"), (EVERY, "table"), (EVERY, "parts"),
    (WITH_SYSTEM, "none"), (WITH_SYSTEM, "table"), (WITH_SYSTEM, "parts"),
    (NAMED, "none"),
], ids=["every column", "every column, cached vector", "every column, vector parts",
        "named columns", "named columns, cached vector", "named columns, vector parts",
        "named columns without the row id"])
@pytest.mark.parametrize("expose_rowid", [False, True], ids=["row id hidden", "row id kept"])
def test_one_view_reads_what_the_view_on_a_view_read(duckdb_con, table, columns, vector, expose_rowid):
    con = duckdb_con
    definition, dv_table, parts = None, None, None
    if vector == "table":
        files_sql = ", ".join(f"'{part}'" for part in table.parts)
        con.execute(f"CREATE TABLE dv_t AS SELECT DISTINCT __rowid__ FROM read_parquet([{files_sql}])")
        definition, dv_table = TombstoneDef(tombstone_path=table.parts, cache_key="k"), "dv_t"
    elif vector == "parts":
        definition, parts = TombstoneDef(tombstone_path=table.parts, cache_key=None), table.parts

    # a view per layer, as every read was made
    ec.create_reflection_view(con, "st_x", table.files, columns)
    ec.create_tombstone_view(con, "st_x", "layered", definition, dv_table=dv_table,
                             expose_rowid=expose_rowid)
    # one view
    ec.create_reflection_view(
        con, "folded", table.files, columns,
        over=lambda select: ec.tombstone_select(
            f"({select}) AS st_x", "st_x", dv_table=dv_table, parts=parts,
            expose_rowid=expose_rowid))

    assert con.execute("DESCRIBE folded").fetchall() == con.execute("DESCRIBE layered").fetchall()
    for query in QUERIES:
        if "region" in query and columns == ["amount", "id"]:
            continue
        one = con.execute(query.format(v="folded"))
        names = [column[0] for column in one.description]
        rows = one.fetchall()
        other = con.execute(query.format(v="layered"))
        assert names == [column[0] for column in other.description]
        assert rows == other.fetchall()

    # and what it reads is right, not only the same
    ids = [row[0] for row in con.execute("SELECT id FROM folded ORDER BY id").fetchall()]
    assert ids == ([1, 3, 4, 5, 6, 9, 10] if vector != "none" else list(range(1, 11)))
    described = [row[0] for row in con.execute("DESCRIBE folded").fetchall()]
    assert "__timestamp__" not in described
    wants_rowid = expose_rowid and columns != ["amount", "id"]
    assert ("__rowid__" in described) is wants_rowid


def test_a_role_view_over_the_one_view_reads_what_it_read_over_the_chain(duckdb_con, table):
    con = duckdb_con
    role = RbacViewDef(allowed_columns=["id", "amount"], where_clause="region = 'eu'")
    columns = ["id", "amount", "region", "__rowid__", "__timestamp__"]
    ec.create_reflection_view(con, "st_x", table.files, columns)
    ec.create_tombstone_view(con, "st_x", "layered", None)
    ec.create_rbac_view(con, "layered", "role_layered", role,
                        projected_columns=columns, filter_only_columns=["region"])
    ec.create_reflection_view(
        con, "folded", table.files, columns,
        over=lambda select: ec.tombstone_select(f"({select}) AS st_x", "st_x"))
    ec.create_rbac_view(con, "folded", "role_folded", role,
                        projected_columns=columns, filter_only_columns=["region"])

    assert (con.execute("SELECT * FROM role_folded ORDER BY id").fetchall()
            == con.execute("SELECT * FROM role_layered ORDER BY id").fetchall()
            == [(2, 20), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80)])


# --------------------------------------------------------------------------
# The engine's real stream
# --------------------------------------------------------------------------

@pytest.fixture
def lake(tmp_path, monkeypatch, table):
    """``table``'s files as a table of a lake, read by the engine."""
    monkeypatch.setattr(ec, "get_app_home", lambda: str(tmp_path))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        real_settings,
        STORAGE_TYPE="LOCAL", STORAGE_BUCKET="", STORAGE_ENDPOINT_URL="",
        STORAGE_ACCESS_KEY="", STORAGE_SECRET_KEY="", STORAGE_SESSION_TOKEN="",
        SUPERTABLE_DUCKDB_FILE_ACCESS_LOCK=True,
        SUPERTABLE_DUCKDB_FILE_ACCESS_DIRECTORIES="",
        SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=False,
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE="",
        SUPERTABLE_DUCKDB_MATERIALIZE="view",
    ))
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()
    yield SimpleNamespace(tmp=tmp_path, files=table.files, parts=table.parts)
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()


class _Recording:
    """The engine's connection, recording what it is sent."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._con.execute(sql, *args, **kwargs)

    def sent(self, starting_with):
        return [sql for sql in self.sql if sql.startswith(starting_with)]

    def __getattr__(self, name):
        return getattr(self._con, name)


def _engine(lake, cache=None):
    engine = engine_duckdb.DuckDBEngine()
    if cache is not None:
        engine._tombstone_cache = cache
    state = engine_duckdb._shared_state()
    state["con"] = _Recording(engine._get_connection(temp_dir=str(lake.tmp / "spill")))
    return engine, state["con"]


def _stream(engine, lake, query, vector=None, cache_key=None, role=None, files=None,
            expose_rowid=False, tables=("emp",)):
    supers = [SuperSnapshot(
        super_name="lake", simple_name=name, simple_version=1,
        files=list(lake.files if files is None else files),
        columns={"id", "region", "amount", "note"},
        column_types={"id": "Int64", "region": "String", "amount": "Int64", "note": "String"},
    ) for name in tables]
    reflection = Reflection(storage_type="LOCAL", reflection_bytes=0,
                            total_reflections=len(supers), supers=supers)
    parser = SQLParser("lake", query, "duckdb")
    for definition in parser.get_table_tuples():
        if vector:
            reflection.tombstone_views[definition.alias] = TombstoneDef(
                tombstone_path=list(vector), cache_key=cache_key)
        if role:
            reflection.rbac_views[definition.alias] = role
    return engine.stream(
        reflection, parser, SimpleNamespace(temp_dir=str(lake.tmp / "spill")),
        lambda *_: None, engine_config=RUNTIME, batch_rows=4, expose_rowid=expose_rowid,
    )


def _rows(handle):
    return pa.Table.from_batches(list(handle.batches()), schema=handle.schema).to_pylist()


def _views(con):
    return sorted(name for (name,) in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall())


ONE_VIEW = re.compile(r"st_[0-9a-f]{16}_[0-9a-f]{8}")


def test_a_read_makes_one_view_and_drops_it(lake):
    engine, con = _engine(lake)
    handle = _stream(engine, lake, "SELECT id, region FROM emp ORDER BY id")
    views = _views(con)
    assert len(views) == 1 and ONE_VIEW.fullmatch(views[0])
    # the files as they are, system columns and deleted rows included, have no name
    assert not any(re.fullmatch(r"st_[0-9a-f]{16}", name) for name in views)

    assert [row["id"] for row in _rows(handle)] == list(range(1, 11))
    assert _views(con) == []
    created, dropped = con.sent("CREATE OR REPLACE VIEW"), con.sent("DROP VIEW")
    assert len(created) == 1 and len(dropped) == 1
    assert "parquet_scan(" in created[0] and STRIP in created[0]
    assert views[0] in created[0] and views[0] in dropped[0]


def test_every_read_makes_its_own_view(lake):
    engine, con = _engine(lake)
    first = _stream(engine, lake, "SELECT id FROM emp ORDER BY id")
    second = _stream(engine, lake, "SELECT id FROM emp ORDER BY id")
    assert len(_views(con)) == 2                              # two reads open at once
    assert [row["id"] for row in _rows(first)] == list(range(1, 11))
    assert len(_views(con)) == 1                              # closing one takes nothing from the other
    assert [row["id"] for row in _rows(second)] == list(range(1, 11))
    assert _views(con) == []


def test_a_cached_vector_is_anti_joined_in_the_one_view(lake):
    engine, con = _engine(lake)
    for _ in range(2):
        handle = _stream(engine, lake, "SELECT id FROM emp ORDER BY id",
                         vector=lake.parts, cache_key="acme/emp/tombstone/v1")
        assert len(_views(con)) == 1
        assert [row["id"] for row in _rows(handle)] == [1, 3, 4, 5, 6, 9, 10]
    table = ec.dv_table_name("acme/emp/tombstone/v1")
    created = con.sent("CREATE OR REPLACE VIEW")
    assert len(created) == 2 and all(f"ANTI JOIN {table} AS __dv__" in sql for sql in created)
    # the vector's table was made once, by the first read, and is kept
    assert len(con.sent("CREATE TABLE IF NOT EXISTS")) == 1
    assert [entry["ref_count"] for entry in engine._tombstone_cache.snapshot()] == [0]


def test_with_the_cache_off_the_parts_are_read_in_the_one_view(lake):
    engine, con = _engine(lake, cache=ec.TombstoneCache(0))
    handle = _stream(engine, lake, "SELECT count(*) AS n FROM emp",
                     vector=lake.parts, cache_key="acme/emp/tombstone/v1")
    assert len(_views(con)) == 1
    assert _rows(handle) == [{"n": 7}]
    created = con.sent("CREATE OR REPLACE VIEW")
    assert len(created) == 1 and "read_parquet(" in created[0] and "ANTI JOIN" in created[0]
    assert con.sent("CREATE TABLE") == []


def test_a_missing_part_of_the_vector_stops_the_read(lake):
    """Nothing is passed over: a table read without a part of its vector
    brings back the rows that part deleted."""
    engine, con = _engine(lake, cache=ec.TombstoneCache(0))
    with pytest.raises(Exception, match="dv-gone"):
        _rows(_stream(engine, lake, "SELECT id FROM emp",
                      vector=[lake.parts[0], str(lake.tmp / "dv-gone.parquet")]))
    assert _views(con) == []


def test_the_row_id_is_kept_where_it_is_asked_for(lake):
    engine, con = _engine(lake)
    rows = _rows(_stream(engine, lake, "SELECT * FROM emp ORDER BY id", files=lake.files[:2],
                         vector=lake.parts, cache_key="k", expose_rowid=True))
    assert [row["__rowid__"] for row in rows] == [1, 3, 4, 5, 6]
    assert all("__timestamp__" not in row for row in rows)
    rows = _rows(_stream(engine, lake, "SELECT * FROM emp ORDER BY id", files=lake.files[:2]))
    assert sorted(rows[0]) == ["amount", "id", "note", "region"]


def test_a_table_named_twice_has_one_view(lake):
    engine, con = _engine(lake)
    handle = _stream(engine, lake,
                     "SELECT a.id, b.id AS other FROM emp a JOIN emp b ON a.id = b.id "
                     "WHERE a.id < 3 ORDER BY a.id",
                     vector=lake.parts, cache_key="k")
    assert len(_views(con)) == 1
    assert _rows(handle) == [{"id": 1, "other": 1}]           # row 2 is deleted
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 1
    assert engine._tombstone_cache.snapshot()[0]["ref_count"] == 0


def test_a_table_read_twice_with_other_columns_has_a_view_for_each(lake):
    """The parser gives two aliases of a join the same columns, and a subquery
    its own: there the two reads of the table are two relations."""
    engine, con = _engine(lake)
    handle = _stream(engine, lake,
                     "SELECT a.id FROM emp a WHERE a.id IN (SELECT amount FROM emp) ORDER BY a.id")
    assert len(_views(con)) == 2
    assert _rows(handle) == [{"id": 10}]                      # the amount of row 1
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 2 and _views(con) == []


def test_two_tables_have_a_view_each(lake):
    engine, con = _engine(lake)
    handle = _stream(engine, lake,
                     "SELECT e.id, d.amount FROM emp e JOIN dept d ON e.id = d.id "
                     "WHERE e.id > 8 ORDER BY e.id", tables=("emp", "dept"))
    assert len(_views(con)) == 2
    assert _rows(handle) == [{"id": 9, "amount": 90}, {"id": 10, "amount": 100}]
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 2 and _views(con) == []


def test_a_role_adds_its_view_and_nothing_else(lake):
    """The RBAC view is made as it was, over the one view."""
    engine, con = _engine(lake)
    role = RbacViewDef(allowed_columns=["id", "amount"], where_clause="region = 'eu'")
    handle = _stream(engine, lake, "SELECT id, amount FROM emp ORDER BY id",
                     vector=lake.parts, cache_key="k", role=role)
    views = _views(con)
    assert len(views) == 2
    below = next(name for name in views if ONE_VIEW.fullmatch(name))
    assert sorted(views) == sorted([below, f"rbac_{below}_{below[-8:]}"])
    assert _rows(handle) == [{"id": 4, "amount": 40}, {"id": 5, "amount": 50},
                             {"id": 6, "amount": 60}]
    assert _views(con) == []
    role_view = con.sent(f"CREATE OR REPLACE VIEW rbac_{below}")
    assert len(role_view) == 1 and role_view[0].endswith(f"FROM {below} WHERE region = 'eu';")

    # a column the role may not read is still refused by the role's view
    with pytest.raises(Exception, match="region"):
        _rows(_stream(engine, lake, "SELECT region FROM emp", role=role))
    assert _views(con) == []


def test_an_empty_table_is_read_through_one_view(lake):
    engine, con = _engine(lake)
    handle = _stream(engine, lake, "SELECT count(*) AS n FROM emp", files=[])
    assert len(_views(con)) == 1
    assert _rows(handle) == [{"n": 0}]
    assert _views(con) == []


def test_a_query_that_fails_leaves_no_view(lake):
    engine, con = _engine(lake)
    with pytest.raises(Exception):
        _rows(_stream(engine, lake, "SELECT id / 'x' AS broken FROM emp"))
    assert _views(con) == []
    assert [row["id"] for row in _rows(_stream(engine, lake, "SELECT id FROM emp ORDER BY id"))] \
        == list(range(1, 11))


# --------------------------------------------------------------------------
# Where a view per layer is kept
# --------------------------------------------------------------------------

def test_which_vectors_this_query_reads_from_a_store(lake):
    engine = engine_duckdb.DuckDBEngine()
    url = ["https://store.example/bucket/acme/emp/tombstone/dv-1.parquet"]
    reads = engine._reads_vector_from_a_store

    assert reads(None) is False                                             # no vector
    assert reads(TombstoneDef(tombstone_path=lake.parts, cache_key="k")) is False     # local
    assert reads(TombstoneDef(tombstone_path=url, cache_key="k")) is True   # not cached yet
    assert reads(TombstoneDef(tombstone_path=url[0], cache_key="k")) is True
    assert reads(TombstoneDef(tombstone_path=url, cache_key=None)) is True  # never cached

    engine._tombstone_cache.acquire(_Recorder(), "k", url)                  # a read has cached it
    assert reads(TombstoneDef(tombstone_path=url, cache_key="k")) is False
    assert reads(TombstoneDef(tombstone_path=url, cache_key="k2")) is True  # another version

    engine._tombstone_cache = ec.TombstoneCache(0)                          # the cache is off
    assert reads(TombstoneDef(tombstone_path=url, cache_key="k")) is True
    assert reads(TombstoneDef(tombstone_path=lake.parts, cache_key="k")) is False


def test_a_vector_to_be_read_from_a_store_keeps_the_reflection_first(lake, monkeypatch):
    """The order the presign fallback rests on: the reflection by itself, then
    the vector, read knowing whether the data files had to be signed."""
    engine, con = _engine(lake)
    monkeypatch.setattr(engine, "_reads_vector_from_a_store", lambda definition: True)
    order = []
    real_reflection = engine_duckdb.create_reflection_view_with_presign_retry
    real_tombstone = engine_duckdb.create_tombstone_view

    def reflection(con_, storage, name, files, cols, log_prefix="", **kwargs):
        order.append(("reflection", name, kwargs.get("over")))
        return real_reflection(con_, storage, name, files, cols, log_prefix, **kwargs)

    def tombstone(con_, source, view, definition, **kwargs):
        order.append(("tombstone", source, kwargs.get("dv_table")))
        return real_tombstone(con_, source, view, definition, **kwargs)

    monkeypatch.setattr(engine_duckdb, "create_reflection_view_with_presign_retry", reflection)
    monkeypatch.setattr(engine_duckdb, "create_tombstone_view", tombstone)
    handle = _stream(engine, lake, "SELECT id FROM emp ORDER BY id",
                     vector=lake.parts, cache_key="k")
    views = _views(con)
    assert len(views) == 2
    reflection_view = next(name for name in views if re.fullmatch(r"st_[0-9a-f]{16}", name))
    assert order == [("reflection", reflection_view, None),
                     ("tombstone", reflection_view, ec.dv_table_name("k"))]
    assert [row["id"] for row in _rows(handle)] == [1, 3, 4, 5, 6, 9, 10]
    assert _views(con) == []


def test_a_materialised_reflection_keeps_its_view_per_layer(lake, monkeypatch):
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        ec.settings, SUPERTABLE_DUCKDB_MATERIALIZE="table"))
    engine, con = _engine(lake)
    handle = _stream(engine, lake, "SELECT id FROM emp ORDER BY id",
                     vector=lake.parts, cache_key="k")
    tables = [name for (name,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'st\\_%' ESCAPE '\\'"
    ).fetchall()]
    assert len(tables) == 1 and _views(con) == [f"tomb_{tables[0]}_{tables[0][-8:]}"]
    assert [row["id"] for row in _rows(handle)] == [1, 3, 4, 5, 6, 9, 10]
