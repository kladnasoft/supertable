# route: supertable.tests.test_read_gate_lateral_functions_comments
"""What the read gate missed until 3.5.4, form by form.

``test_read_path_admission`` covers the gate's first rule: a statement must be
a read and every FROM/JOIN source a named table. That rule looked only at
``exp.Table``. On 2026-11-17 a role that could read one table of a live Island
ran each of these through the query route:

    SELECT h.content FROM (SELECT 1 FROM t LIMIT 1) x,
           LATERAL read_text('/etc/hostname') h
    SELECT * FROM t, LATERAL duckdb_secrets() s
    SELECT current_setting('threads') FROM t

A function after LATERAL parses as ``Lateral(this=<function>)``, not as a
``Table``, and no scalar function was judged at all. Fixing those turned up
three more, each a way for what runs to differ from what was judged:

  * a comment's text, written back out as SQL;
  * ``SELECT ... INTO``, a read root that is written back out as CREATE TABLE;
  * a second parse of the same text with Spark's grammar, which ends a string
    in a different place than DuckDB's.

These are pure-function tests of the gate and of the writers behind it. The
same forms run against a real table, reader and role in
``tests/characterization/test_read_gate_end_to_end.py``.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from supertable import system_query
from supertable.data_reader import _ensure_sql_limit
from supertable.engine import spark_thrift
from supertable.engine.engine_common import rewrite_query_with_hashed_tables
from supertable.engine.engine_enum import Engine
from supertable.engine.spark_thrift import _spark_rewrite_query, _to_spark_predicate
from supertable.system_query import CommandKind, assert_read_only, classify_query
from supertable.utils.sql_parser import SQLParser

SUPER = "ds"


def _refused(sql: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        classify_query(sql, SUPER)
    # EXPLAIN reaches the same engine with the same text.
    with pytest.raises(ValueError, match=match):
        classify_query(f"EXPLAIN {sql}", SUPER)


# --------------------------------------------------------------------------
# LATERAL: what it may not read
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    # The three confirmed on the Island, and their kind.
    "SELECT h.size FROM (SELECT 1 FROM orders LIMIT 1) x, "
    "LATERAL read_text('/etc/hostname') h",
    "SELECT h.* FROM orders, LATERAL read_blob('/etc/hostname') h",
    "SELECT g.* FROM orders, LATERAL glob('/proc/self/*') g",
    "SELECT s.* FROM orders, LATERAL sniff_csv('/etc/passwd') s",
    "SELECT p.* FROM orders, "
    "LATERAL read_parquet('s3://lake/other-tenant/t/data/x.parquet') p",
    "SELECT s.* FROM orders, LATERAL duckdb_secrets() s",
    "SELECT s.* FROM orders, LATERAL duckdb_settings() s",
    # Every way a LATERAL is joined.
    "SELECT * FROM orders CROSS JOIN LATERAL read_text('/etc/hostname') h",
    "SELECT * FROM orders LEFT JOIN LATERAL read_csv('/etc/passwd') h ON TRUE",
    "SELECT * FROM orders JOIN LATERAL read_json('/x.json') h ON TRUE",
    # A function that runs SQL of its own, or names a table by a string.
    "SELECT * FROM orders, LATERAL query('SELECT 1') q",
    "SELECT * FROM orders, LATERAL query_table('salaries') q",
    # Wrapped, and nested where it might be assumed hidden.
    "SELECT * FROM orders, LATERAL TABLE(read_text('/etc/hostname')) h",
    "SELECT * FROM (SELECT * FROM orders, LATERAL read_text('/x') h) t",
    "WITH w AS (SELECT * FROM orders, LATERAL read_text('/x') h) SELECT * FROM w",
    "SELECT * FROM orders WHERE id IN "
    "(SELECT o.id FROM orders o, LATERAL read_text('/x') h)",
    "SELECT * FROM orders UNION ALL SELECT h.* FROM orders, LATERAL glob('/*') h",
    # Spark's LATERAL VIEW takes any generator function, Java's included.
    "SELECT * FROM orders LATERAL VIEW reflect('java.lang.System', 'getenv') x AS y",
])
def test_a_lateral_function_that_reads_is_refused(sql):
    _refused(sql, "not permitted on the read path")


@pytest.mark.parametrize("sql", [
    # Behind a schema qualifier: Lateral(this=Dot(...)). DuckDB resolves it to
    # the same function.
    "SELECT * FROM orders, LATERAL main.read_text('/etc/hostname') h",
    "SELECT * FROM orders, LATERAL system.main.read_text('/etc/hostname') h",
    # ...even when the function behind it would be allowed without one.
    "SELECT * FROM orders, LATERAL main.range(3) r",
    "SELECT * FROM orders LATERAL VIEW my.udtf(a) x AS y",
    # A quoted name.
    'SELECT * FROM orders, LATERAL "read_text"(\'/etc/hostname\') h',
    'SELECT * FROM orders, LATERAL "json_each"(orders.j) j',
    # Neither a subquery nor a function: a bare name the gate cannot resolve.
    "SELECT * FROM orders, LATERAL salaries",
    'SELECT * FROM orders, LATERAL "/etc/hostname"',
])
def test_a_lateral_source_that_is_not_plainly_a_row_generator_is_refused(sql):
    _refused(sql, "LATERAL may read")


@pytest.mark.parametrize("name", [
    # An allow-list: a function nobody has heard of is refused with the rest.
    "some_future_reader", "repeat_row", "generate_subscripts", "flatten",
    "regexp_split_to_table", "getvariable", "struct_pack", "pragma_table_info",
])
def test_lateral_is_an_allow_list_so_an_unknown_function_is_refused(name):
    _refused(f"SELECT * FROM orders, LATERAL {name}(orders.a) x",
             "LATERAL may read")


# --------------------------------------------------------------------------
# LATERAL: what it may read
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    # A subquery, correlated or not, with whatever a subquery may hold.
    "SELECT * FROM orders o, LATERAL (SELECT o.a + 1 AS b) s",
    "SELECT * FROM orders o, LATERAL "
    "(SELECT * FROM customers c WHERE c.id = o.customer_id LIMIT 3) c",
    "SELECT * FROM orders o LEFT JOIN LATERAL (SELECT max(d) AS d FROM items i "
    "WHERE i.order_id = o.id) m ON TRUE",
    "SELECT * FROM orders o, LATERAL (SELECT 1 UNION ALL SELECT 2) u",
    "SELECT * FROM orders o, LATERAL (WITH c AS (SELECT o.a AS a) SELECT * FROM c) y",
    # VALUES, both ways of writing it.
    "SELECT * FROM orders, LATERAL (VALUES (1), (2)) v(x)",
    "SELECT * FROM orders, LATERAL VALUES (1), (2)",
    # UNNEST and DuckDB's row generators.
    "SELECT * FROM orders o, LATERAL UNNEST(o.tags) AS u(tag)",
    "SELECT * FROM orders o, LATERAL unnest([1, 2, 3]) AS u(x)",
    "SELECT * FROM orders o CROSS JOIN LATERAL range(3) r",
    "SELECT * FROM orders o, LATERAL RANGE(0, o.n) r",
    "SELECT * FROM orders o, LATERAL generate_series(1, o.n) g",
    "SELECT * FROM orders o, LATERAL json_each(o.doc) j",
    "SELECT * FROM orders o, LATERAL json_tree(o.doc) j",
    # Spark's LATERAL VIEW generators and their _outer forms.
    "SELECT * FROM orders LATERAL VIEW explode(tags) x AS tag",
    "SELECT * FROM orders LATERAL VIEW OUTER explode(tags) x AS tag",
    "SELECT * FROM orders LATERAL VIEW explode_outer(tags) x AS tag",
    "SELECT * FROM orders LATERAL VIEW posexplode(tags) x AS pos, tag",
    "SELECT * FROM orders LATERAL VIEW posexplode_outer(tags) x AS pos, tag",
    "SELECT * FROM orders LATERAL VIEW inline(pairs) x AS a, b",
    "SELECT * FROM orders LATERAL VIEW inline_outer(pairs) x AS a, b",
    "SELECT * FROM orders LATERAL VIEW stack(2, a, b) x AS v",
    "SELECT * FROM orders LATERAL VIEW json_tuple(doc, 'a', 'b') x AS a, b",
])
def test_a_lateral_subquery_values_unnest_or_row_generator_is_admitted(sql):
    assert classify_query(sql, SUPER).kind is CommandKind.SELECT
    assert classify_query(f"EXPLAIN {sql}", SUPER).kind is CommandKind.EXPLAIN


def test_what_a_lateral_subquery_holds_is_judged_like_any_other():
    """Admitting the subquery admits nothing inside it."""
    _refused("SELECT * FROM orders, LATERAL (SELECT * FROM read_text('/x')) h",
             "named tables")
    _refused("SELECT * FROM orders o, LATERAL "
             "(SELECT current_setting('threads') AS t) s", "engine's own state")
    _refused("SELECT * FROM orders o, LATERAL range(0, "
             "CAST(current_setting('threads') AS INT)) r", "engine's own state")


def test_the_tables_of_a_lateral_subquery_reach_the_parser():
    """They are authorized like any other: nothing is admitted unseen."""
    sql = ("SELECT * FROM orders o, LATERAL "
           "(SELECT * FROM salaries s WHERE s.id = o.id) x")
    command = classify_query(sql, SUPER)
    parser = SQLParser(SUPER, command.sql, "duckdb", parsed=command.parsed)
    assert sorted(t.simple_name for t in parser.get_physical_tables()) == [
        "orders", "salaries"]


# --------------------------------------------------------------------------
# Scalar functions that read something other than what they are handed
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT current_setting('threads') FROM orders",
    "SELECT current_setting('s3_secret_access_key') AS s FROM orders",
    # However it is spelt: quoted, qualified, upper case.
    'SELECT "current_setting"(\'threads\') FROM orders',
    "SELECT main.current_setting('threads') FROM orders",
    "SELECT system.main.current_setting('threads') FROM orders",
    "SELECT CURRENT_SETTING('threads') FROM orders",
    # Wherever it stands.
    "SELECT a FROM orders WHERE b = current_setting('threads')",
    "SELECT a FROM orders ORDER BY current_setting('threads')",
    "SELECT a FROM orders GROUP BY a HAVING max(b) > current_setting('threads')",
    "SELECT lower(concat('x', current_setting('threads'))) FROM orders",
    "SELECT list_transform([1, 2], x -> current_setting('threads')) FROM orders",
    "SELECT (SELECT current_setting('threads')) FROM orders",
    "SELECT o.a FROM orders o JOIN customers c "
    "ON c.id = CAST(current_setting('threads') AS INT)",
    "SELECT sum(a) OVER (ORDER BY current_setting('threads')) FROM orders",
    "SELECT CASE WHEN a > 1 THEN getvariable('x') END FROM orders",
    "SELECT getenv('HOME') FROM orders",
    "SELECT which_secret('s3://lake/x', 's3') FROM orders",
    # The catalog of views the reader built: a view's SQL names its files.
    "SELECT pg_get_viewdef(1) FROM orders",
    "SELECT write_log('x') FROM orders",
    # Spark: the file a row came from.
    "SELECT input_file_name() FROM orders",
])
def test_a_function_that_reads_engine_state_is_refused(sql):
    _refused(sql, "engine's own state")


@pytest.mark.parametrize("sql", [
    "SELECT reflect('java.lang.System', 'getenv', 'HOME') FROM orders",
    "SELECT java_method('java.lang.System', 'getProperty', 'user.home') FROM orders",
    "SELECT try_reflect('java.lang.Runtime', 'getRuntime') FROM orders",
    "SELECT a FROM orders WHERE reflect('java.lang.System', 'getenv', 'K') = 'v'",
])
def test_a_java_call_is_refused(sql):
    _refused(sql, "calls Java")


@pytest.mark.parametrize("sql", [
    # Table functions written where a scalar goes. DuckDB refuses most there;
    # the gate does not wait to find out which.
    "SELECT read_text('/etc/hostname') FROM orders",
    "SELECT read_csv('/etc/passwd') FROM orders",
    "SELECT a FROM orders WHERE b IN (read_blob('/etc/hostname'))",
    "SELECT glob('/proc/self/*') FROM orders",
    "SELECT duckdb_secrets() FROM orders",
    "SELECT parquet_metadata('s3://lake/x.parquet') FROM orders",
    "SELECT pragma_database_size() FROM orders",
    # Extensions: HTTP clients and other stores' scanners.
    "SELECT http_get('http://169.254.169.254/latest/meta-data/') FROM orders",
    "SELECT sqlite_scan('/data/app.db', 'users') FROM orders",
    "SELECT postgres_scan('host=db', 'public', 'users') FROM orders",
    "SELECT ST_Read('/data/shapes.shp') FROM orders",
    # Hive: true when a line of the file equals the string.
    "SELECT in_file('root:x:0:0:root:/root:/bin/bash', '/etc/passwd') FROM orders",
])
def test_a_function_that_reads_files_or_urls_is_refused(sql):
    _refused(sql, "not permitted on the read path")


@pytest.mark.parametrize("sql", [
    "SELECT lower(a), upper(b), coalesce(c, 0), round(d, 2) FROM orders",
    "SELECT date_trunc('month', ts), strftime(ts, '%Y'), current_date FROM orders",
    "SELECT regexp_replace(a, '[0-9]+', '#'), string_split(a, ',') FROM orders",
    "SELECT json_extract(doc, '$.a'), list_transform(tags, x -> x + 1) FROM orders",
    "SELECT count(*), sum(a), avg(b), row_number() OVER (ORDER BY a) FROM orders GROUP BY a, b",
    "SELECT current_user, current_schema(), version() FROM orders",
    "SELECT unnest(tags) FROM orders",
    "SELECT a FROM orders WHERE name GLOB 'a*'",
    # A column, an alias or a table that only shares a name with a refused
    # function is not a call of it.
    "SELECT read_text, read_count, parquet_rows, current_setting FROM orders",
    "SELECT a AS glob, b AS query, c AS reflect FROM orders",
    "SELECT * FROM duckdb_stats",
    "SELECT * FROM orders AS read_text",
    "SELECT o.http_status FROM orders o WHERE o.query = 'x'",
    "SELECT 'current_setting(''threads'')' AS text FROM orders",
])
def test_ordinary_functions_and_lookalike_names_are_admitted(sql):
    assert classify_query(sql, SUPER).kind is CommandKind.SELECT


# --------------------------------------------------------------------------
# TRANSFORM ... USING runs a program
# --------------------------------------------------------------------------

def test_transform_using_is_refused_in_the_reading_the_gate_takes():
    """DuckDB's grammar has no TRANSFORM ... USING, so the one reading there
    is does not parse it."""
    _refused("SELECT TRANSFORM(a) USING 'cat /etc/passwd' AS (x) FROM orders",
             "could not parse")


def test_transform_using_is_refused_as_a_node_too():
    """The rule itself, on the tree a grammar that knows the clause builds —
    so it holds whatever grammar the gate comes to read with."""
    tree = sqlglot.parse_one(
        "SELECT TRANSFORM(a) USING 'cat /etc/passwd' AS (x) FROM orders",
        read="spark")
    assert tree.find(exp.QueryTransform) is not None
    with pytest.raises(ValueError, match="runs a program"):
        system_query._judge(tree)


# --------------------------------------------------------------------------
# A read all the way down
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql, word", [
    # A read root that is written back out as CREATE TABLE x AS SELECT ...
    ("SELECT * INTO stolen FROM orders", "INTO"),
    ("SELECT a, b INTO TEMP stolen FROM orders WHERE a > 1", "INTO"),
    # A CTE body parses as any statement.
    ("WITH w AS (DELETE FROM orders RETURNING *) SELECT * FROM w", "DELETE"),
    ("WITH w AS (INSERT INTO orders VALUES (1) RETURNING *) SELECT * FROM w", "INSERT"),
    ("WITH w AS (UPDATE orders SET a = 1 RETURNING *) SELECT * FROM w", "UPDATE"),
    ("SELECT * FROM orders FOR UPDATE", "LOCK"),
])
def test_a_read_root_that_carries_something_else_is_refused(sql, word):
    _refused(sql, f"{word} is not permitted on the read path")


def test_sqlglot_writes_select_into_out_as_create_table():
    """Why ``Into`` is refused at the gate and not left to a later step."""
    tree = sqlglot.parse_one("SELECT * INTO stolen FROM orders", read="duckdb")
    assert tree.sql(dialect="duckdb") == "CREATE TABLE stolen AS SELECT * FROM orders"


# --------------------------------------------------------------------------
# A comment is not judged, so it is never written back out
# --------------------------------------------------------------------------

#: One column and a comment to the gate. Written out with its comment, the
#: comment ends at its own ``*/`` and the rest of it is SQL.
SMUGGLED = ("SELECT id -- */ , (SELECT content FROM read_text('/etc/hostname')) AS leak /*\n"
            "FROM orders")


def test_sqlglot_writes_a_comments_text_back_out_as_sql():
    """The premise, on the pinned SQLGlot. If this starts failing SQLGlot has
    begun escaping ``*/``; the writers below stay as they are regardless."""
    tree = sqlglot.parse_one(SMUGGLED, read="duckdb")
    assert not [f for f in tree.find_all(exp.Anonymous)], "nothing here to judge"
    written = tree.sql(dialect="duckdb")
    reread = sqlglot.parse_one(written, read="duckdb")
    assert [str(f.this) for f in reread.find_all(exp.Anonymous)] == ["read_text"]


def test_admission_hands_on_a_statement_without_comments():
    command = classify_query(SMUGGLED, SUPER)
    assert not any(node.comments for node in command.parsed.walk())
    assert "read_text" not in command.parsed.sql(dialect="duckdb")
    assert "read_text" not in command.parsed.sql(dialect="spark")


@pytest.mark.parametrize("sql", [
    SMUGGLED,
    "SELECT id FROM orders -- */ , LATERAL read_text('/etc/hostname') h /*",
    "SELECT id FROM orders -- */ UNION ALL SELECT content FROM read_text('/x') /*",
    "SELECT id FROM orders o -- */ WHERE current_setting('threads') IS NOT NULL /*",
    "SELECT /* a */ id -- */ , salary /*\nFROM orders /* b */",
])
def test_no_writer_writes_a_comment_back_out(sql):
    """Each place the read path turns a parse back into text."""
    for written in (
        rewrite_query_with_hashed_tables(sql, {"orders": "st_abc", "o": "st_abc"}),
        _spark_rewrite_query(sql, {"orders": "st_abc", "o": "st_abc"}),
        _ensure_sql_limit(sql, default_limit=5),
    ):
        assert "/*" not in written and "--" not in written
        for word in ("read_text", "current_setting", "salary", "UNION"):
            assert word.lower() not in written.lower()
        # ...and what is left is the query that was judged.
        assert_read_only(written)


def test_a_row_filter_is_written_for_spark_without_its_comments():
    written = _to_spark_predicate("\"region\" = 'EU' -- */ OR TRUE /*")
    assert written == "`region` = 'EU'"


# --------------------------------------------------------------------------
# One reading: what is authorized is what runs
# --------------------------------------------------------------------------

#: DuckDB ends the first string at ``\'``; Spark reads that as a quote inside
#: it and ends the string at the next one. One table to Spark's grammar, a
#: file datasource to DuckDB's.
TWO_READINGS = ("SELECT 'a\\' AS x FROM text.\"/etc/passwd\" "
                "WHERE value <> ' FROM orders --'")


def test_the_two_grammars_read_different_tables_in_the_same_text():
    """The premise."""
    as_duckdb = sqlglot.parse_one(TWO_READINGS, read="duckdb")
    as_spark = sqlglot.parse_one(TWO_READINGS, read="spark")
    assert [(t.db, t.name) for t in as_duckdb.find_all(exp.Table)] == [
        ("text", "/etc/passwd")]
    assert [(t.db, t.name) for t in as_spark.find_all(exp.Table)] == [
        ("", "orders")]
    # And DuckDB's reading is the one written out for Spark.
    assert "text.`/etc/passwd`" in sqlglot.transpile(
        TWO_READINGS, read="duckdb", write="spark")[0]


def test_the_spark_writer_sends_on_no_table_it_did_not_map():
    """The last step holds on its own: with the tables of Spark's reading
    mapped, as 3.5.3's reader mapped them, the file datasource of DuckDB's
    reading is not one of them, and nothing is written for Spark."""
    with pytest.raises(ValueError, match="not one this query was authorized to read"):
        _spark_rewrite_query(TWO_READINGS, {"orders": "st_abc"})


def test_the_engine_enum_names_no_parser_dialect():
    """A dialect per engine is how the two readings came apart."""
    assert not hasattr(Engine.SPARK_SQL, "dialect")


@pytest.mark.parametrize("engine", [Engine.SPARK_SQL, Engine.AUTO, Engine.DUCKDB])
def test_the_reader_authorizes_the_reading_that_runs_on_every_engine(engine, monkeypatch):
    """``DataReader.execute`` builds its parser from the statement admission
    judged, whatever the engine: the tables it authorizes are DuckDB's
    reading, the one the Spark executor writes out."""
    import supertable.data_reader as data_reader

    seen = {}

    class _Stop(Exception):
        pass

    class _Parser:
        def __init__(self, super_name, query, dialect, parsed=None):
            seen.update(dialect=dialect, parsed=parsed, tables=[
                (t.db, t.name) for t in parsed.find_all(exp.Table)])
            raise _Stop

    monkeypatch.setattr(data_reader, "SQLParser", _Parser)
    reader = data_reader.DataReader.__new__(data_reader.DataReader)
    reader.query, reader.super_name, reader.organization = TWO_READINGS, SUPER, "acme"
    with pytest.raises(_Stop):
        reader.execute(role_name="reader", engine=engine)

    assert seen["dialect"] == "duckdb"
    assert seen["tables"] == [("text", "/etc/passwd")]


@pytest.mark.parametrize("sql", [
    # A string to Spark from the first quote to the last; code to DuckDB.
    "SELECT 'a\\' AS x, reflect('java.lang.System', 'getenv', 'HOME') FROM orders -- '",
    "SELECT 'a\\' AS x, java_method('java.lang.Runtime', 'getRuntime') FROM orders -- '",
    "SELECT 'a\\' AS x FROM orders, LATERAL read_text('/etc/hostname') h -- '",
])
def test_code_behind_a_backslash_quote_is_judged_as_code(sql):
    _refused(sql, "not permitted on the read path")


def test_a_backslash_in_a_string_is_written_for_spark_as_spark_reads_it():
    """So the string Spark reads is the string that was judged, and what
    follows it stays outside it."""
    written = _spark_rewrite_query(
        "SELECT a FROM orders WHERE b = 'x\\' AND c = 'it''s'", {"orders": "st_abc"})
    assert written == "SELECT a FROM st_abc AS orders WHERE b = 'x\\\\' AND c = 'it\\'s'"
    as_spark = sqlglot.parse_one(written, read="spark")
    assert sorted(lit.this for lit in as_spark.find_all(exp.Literal)) == ["it's", "x\\"]


def test_spark_is_never_sent_text_sqlglot_did_not_write(monkeypatch):
    """The quote fallback is gone: it sent DuckDB-dialect text for Spark to
    read with its own grammar when the write-out failed."""
    assert not hasattr(spark_thrift, "_double_quotes_to_backticks")

    def fails(*args, **kwargs):
        raise RuntimeError("cannot generate")

    monkeypatch.setattr(spark_thrift.sqlglot, "transpile", fails)
    with pytest.raises(ValueError, match="could not write the query for Spark"):
        _spark_rewrite_query('SELECT "a" FROM orders', {"orders": "st_abc"})


def test_spark_is_sent_one_statement(monkeypatch):
    monkeypatch.setattr(spark_thrift.sqlglot, "transpile",
                        lambda *a, **k: ["SELECT 1", "DROP TABLE t"])
    with pytest.raises(ValueError, match="expected one statement"):
        _spark_rewrite_query("SELECT a FROM orders", {"orders": "st_abc"})


@pytest.mark.parametrize("sql", ["SELECT FROM WHERE ((((", "SELECT 'unterminated FROM t"])
def test_a_query_the_rewrite_cannot_read_is_not_sent_as_written(sql):
    """It used to be: "Failed to parse SQL for rewrite; using original"."""
    with pytest.raises(ValueError, match="could not parse the query"):
        rewrite_query_with_hashed_tables(sql, {"t": "st_abc"})
    with pytest.raises(ValueError, match="could not parse the query"):
        _spark_rewrite_query(sql, {"t": "st_abc"})


# --------------------------------------------------------------------------
# A CTE is a CTE only where the engine reads it as one
# --------------------------------------------------------------------------

def _tables(sql: str):
    command = classify_query(sql, SUPER)
    parser = SQLParser(SUPER, command.sql, "duckdb", parsed=command.parsed)
    return sorted((t.super_name, t.simple_name) for t in parser.get_physical_tables())


@pytest.mark.parametrize("sql, read", [
    # A qualified name is never a CTE. The engine reads its own catalog here.
    ("WITH tables AS (SELECT 1 AS x FROM orders) "
     "SELECT t.table_name FROM information_schema.tables t",
     ("information_schema", "tables")),
    ("WITH duckdb_views AS (SELECT 1 AS x FROM orders) "
     "SELECT v.sql FROM main.duckdb_views v", ("main", "duckdb_views")),
    # ...and with the name of a reflection view, a table's unfiltered rows.
    ("WITH st_05b8117a705e9b13 AS (SELECT 1 AS x FROM orders) "
     "SELECT s.* FROM main.st_05b8117a705e9b13 s", ("main", "st_05b8117a705e9b13")),
    # Outside the query its WITH belongs to.
    ("SELECT v.sql FROM (WITH duckdb_views AS (SELECT 1 AS x FROM orders) "
     "SELECT x FROM duckdb_views) a, duckdb_views v", (SUPER, "duckdb_views")),
    ("SELECT * FROM orders WHERE id IN (WITH st_abc AS (SELECT 1 AS id) SELECT id FROM st_abc) "
     "UNION ALL SELECT s.* FROM st_abc s", (SUPER, "st_abc")),
    # Before its own definition, in an earlier one of the same WITH.
    ("WITH a AS (SELECT sql FROM duckdb_views), "
     "duckdb_views AS (SELECT 1 AS x FROM orders) SELECT * FROM a",
     (SUPER, "duckdb_views")),
])
def test_a_name_the_engine_reads_from_its_catalog_is_a_table_to_the_parser(sql, read):
    """So it is looked up, is not there, and the query is refused. Until 3.5.4
    a CTE of that name anywhere in the query made every such reference a CTE:
    not looked up, not authorized, not replaced — the engine's to resolve."""
    assert read in _tables(sql)
    with pytest.raises(ValueError, match="not one this query was authorized to read"):
        rewrite_query_with_hashed_tables(sql, {"orders": "st_orders"})


def test_a_cte_inside_its_own_definition_is_the_table_unless_recursive():
    """``WITH w AS (SELECT ... FROM w)``: the inner ``w`` is the catalog's."""
    from supertable.utils.sql_compat import is_cte_reference

    def references(sql):
        tree = sqlglot.parse_one(sql, read="duckdb")
        return [(t.name, is_cte_reference(t)) for t in tree.find_all(exp.Table)]

    assert sorted(references(
        "WITH w AS (SELECT id FROM w) SELECT * FROM w x JOIN orders USING (id)")) == [
        ("orders", False), ("w", False), ("w", True)]
    assert sorted(references(
        "WITH RECURSIVE r AS (SELECT id FROM orders UNION ALL SELECT id + 1 FROM r) "
        "SELECT * FROM r")) == [("orders", False), ("r", True), ("r", True)]


@pytest.mark.parametrize("sql", [
    "WITH w AS (SELECT * FROM orders) SELECT * FROM w",
    "WITH a AS (SELECT * FROM orders), b AS (SELECT * FROM a) SELECT * FROM b JOIN a USING (id)",
    "WITH w AS (SELECT id FROM orders) SELECT * FROM orders o WHERE o.id IN (SELECT id FROM w)",
    "SELECT * FROM (WITH w AS (SELECT * FROM orders) SELECT * FROM w) x",
    "WITH w AS (SELECT * FROM orders) SELECT * FROM w UNION ALL SELECT * FROM w",
    "WITH w AS (SELECT * FROM orders) SELECT * FROM orders o, LATERAL (SELECT * FROM w WHERE w.id = o.id) l",
    "WITH w AS (WITH v AS (SELECT * FROM orders) SELECT * FROM v) SELECT * FROM w",
    "WITH RECURSIVE r AS (SELECT id FROM orders UNION ALL SELECT id + 1 FROM r WHERE id < 3) "
    "SELECT * FROM r",
    'WITH "Weird Name" AS (SELECT * FROM orders) SELECT * FROM "Weird Name"',
])
def test_a_cte_where_the_engine_reads_one_is_still_a_cte(sql):
    assert _tables(sql) == [(SUPER, "orders")]
    written = rewrite_query_with_hashed_tables(sql, {"orders": "st_orders", "o": "st_orders"})
    assert "st_orders" in written and " orders" not in written.replace("AS orders", "")


def test_one_alias_cannot_be_a_cte_here_and_a_table_there():
    """Everything downstream is keyed by alias. The inner ``orders`` is the
    table (a CTE is not visible in its own definition) and the outer one the
    CTE, under one name: the table's view would be written over both."""
    with pytest.raises(ValueError, match="names both a CTE and a table"):
        _tables("WITH orders AS (SELECT * FROM orders WHERE a > 1) SELECT * FROM orders")
    # The same when the name outside its WITH is the engine's to resolve.
    with pytest.raises(ValueError, match="names both a CTE and a table"):
        _tables("SELECT * FROM orders WHERE id IN "
                "(WITH st_abc AS (SELECT 1 AS id) SELECT id FROM st_abc) "
                "UNION ALL SELECT * FROM st_abc")


@pytest.mark.parametrize("sql", [
    "SELECT * FROM keep_me",
    "SELECT * FROM orders JOIN other_table USING (id)",
    "SELECT * FROM orders WHERE id IN (SELECT id FROM information_schema.tables)",
])
def test_the_rewrite_sends_on_no_table_it_did_not_map(sql):
    """It used to leave such a name as written, for the engine to resolve."""
    with pytest.raises(ValueError, match="not one this query was authorized to read"):
        rewrite_query_with_hashed_tables(sql, {"orders": "st_orders"})
    with pytest.raises(ValueError, match="not one this query was authorized to read"):
        rewrite_query_with_hashed_tables(sql, {})
