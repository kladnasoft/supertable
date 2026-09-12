# route: supertable.tests.test_read_path_admission
"""The read path only runs reads, and only over named tables.

Everything that protects a row or a column lives in a VIEW the reader builds:
the deletion vector is an anti-join, RBAC is a filtered view over the
reflection, and a share filter is merged into that view. A query that never
references those views is not restricted by them — it is simply outside the
system.

DuckDB can read files directly, so before this guard existed a query could step
around the whole chain by naming one real table to get admitted and then
projecting from raw parquet. Audit finding C1: that returned a restricted role
a masked column, ``__rowid__``, and a row the deletion vector had removed.

The rule is an allow-list, because naming the dangerous functions is a losing
game — DuckDB adds more and extensions add their own. A statement must be a
read, and every FROM/JOIN source must be a named table. The AST makes that
clean: a real table is ``Table(this=Identifier)`` while every table function is
``Table`` wrapping a function node.
"""

from __future__ import annotations

import pytest

from supertable.system_query import CommandKind, classify_query

SUPER = "ds"


# --------------------------------------------------------------------------
# Reads still work
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT a FROM orders",
    "SELECT * FROM orders WHERE a > 1",
    "WITH w AS (SELECT * FROM orders) SELECT * FROM w",
    "SELECT * FROM a UNION ALL SELECT * FROM b",
    "SELECT * FROM a INTERSECT SELECT * FROM b",
    "SELECT count(*) FROM orders GROUP BY x HAVING count(*) > 2",
    "SELECT o.a, c.b FROM orders o JOIN customers c ON o.id = c.id",
    "SELECT * FROM (SELECT a FROM orders) t",
    "SELECT * FROM orders WHERE id IN (SELECT id FROM customers)",
    "SELECT row_number() OVER (ORDER BY a) FROM orders",
    'SELECT * FROM "Weird Name"',
    "SELECT * FROM sup_a.t1 JOIN sup_b.t2 ON t1.id = t2.id",
])
def test_ordinary_reads_are_admitted(sql):
    assert classify_query(sql, SUPER).kind is CommandKind.SELECT


# --------------------------------------------------------------------------
# The bypass itself
# --------------------------------------------------------------------------

def test_the_exact_audit_bypass_is_refused():
    """C1's payload: one real table to get admitted, raw parquet to project."""
    with pytest.raises(ValueError, match="named tables"):
        classify_query(
            "SELECT p.* FROM read_parquet(['/lake/org/t/data/x.parquet']) p "
            "WHERE EXISTS (SELECT 1 FROM orders)", SUPER)


@pytest.mark.parametrize("sql", [
    "SELECT * FROM read_parquet(['/x.parquet'])",
    "SELECT * FROM read_csv('/etc/hostname')",
    "SELECT * FROM read_csv_auto('/etc/passwd')",
    "SELECT * FROM read_json('/x.json')",
    "SELECT * FROM glob('/**')",
    "SELECT * FROM parquet_scan('/x.parquet')",
    # Nested inside shapes that might be assumed to hide it.
    "SELECT * FROM (SELECT * FROM read_parquet(['/x'])) t",
    "WITH w AS (SELECT * FROM read_csv('/etc/hostname')) SELECT * FROM w",
    "SELECT * FROM orders UNION ALL SELECT * FROM read_parquet(['/x'])",
    "SELECT * FROM orders o JOIN read_parquet(['/x']) p ON o.id = p.id",
])
def test_table_functions_are_refused_wherever_they_appear(sql):
    with pytest.raises(ValueError, match="named tables"):
        classify_query(sql, SUPER)


@pytest.mark.parametrize("sql", [
    "COPY (SELECT 1) TO '/tmp/exfil.csv'",
    "ATTACH '/tmp/other.db'",
    "INSTALL httpfs",
    "LOAD httpfs",
    "DELETE FROM orders WHERE id = 1",
    "INSERT INTO orders VALUES (1)",
    "UPDATE orders SET a = 1",
    "DROP TABLE orders",
    "CREATE TABLE x AS SELECT 1",
    "PRAGMA database_list",
    "SET memory_limit='1GB'",
])
def test_non_reads_are_refused(sql):
    with pytest.raises(ValueError, match="not permitted on the read path"):
        classify_query(sql, SUPER)


def test_statement_chaining_is_refused():
    """How an injected payload arrives: one request, two statements."""
    with pytest.raises(ValueError, match="single statement"):
        classify_query("SELECT * FROM orders; DROP TABLE orders", SUPER)


def test_unparseable_sql_is_refused_not_forwarded():
    """'The parser could not read it' must not mean 'let the engine try'."""
    with pytest.raises(ValueError, match="could not parse"):
        classify_query("SELECT FROM WHERE ((((", SUPER)


# --------------------------------------------------------------------------
# EXPLAIN reaches the same engine and is admitted on the same terms
# --------------------------------------------------------------------------

def test_explain_of_a_read_is_admitted():
    cmd = classify_query("EXPLAIN SELECT a FROM orders", SUPER)
    assert cmd.kind is CommandKind.EXPLAIN and cmd.explain is True


def test_explain_cannot_smuggle_a_table_function():
    """Otherwise EXPLAIN is a hole the shape of the guard.

    DuckDB's EXPLAIN still binds the query, so a file function inside one is
    still a read of that file.
    """
    with pytest.raises(ValueError):
        classify_query("EXPLAIN SELECT * FROM read_parquet(['/x'])", SUPER)


def test_show_stats_still_classifies():
    cmd = classify_query("SHOW STATS orders", SUPER)
    assert cmd.kind is CommandKind.SHOW_STATS and cmd.simple_name == "orders"


def test_empty_query_defers_to_the_parser():
    """Unchanged: the canonical 'non-empty SQL string' error is the parser's."""
    assert classify_query("", SUPER).kind is CommandKind.SELECT


# --------------------------------------------------------------------------
# The guard must not make the reader parse twice
# --------------------------------------------------------------------------

def test_admission_hands_its_ast_to_the_caller():
    """Admission parses every query; nobody should parse it again.

    The guard used to discard its AST, so SQLParser re-parsed the same text.
    On a query with a 1,000-value IN list that second parse cost 103ms —
    sqlglot rebuilds every literal node — which showed up as a 16.6% read
    regression on the benchmark's random_1000_by_key scenario.
    """
    cmd = classify_query("SELECT a FROM orders WHERE id IN (1, 2, 3)", SUPER)
    assert cmd.parsed is not None
    # It is the statement, not a fragment.
    assert cmd.parsed.sql(dialect="duckdb").lower().startswith("select")


def test_explain_also_hands_over_its_inner_ast():
    cmd = classify_query("EXPLAIN SELECT a FROM orders", SUPER)
    assert cmd.parsed is not None


def test_show_stats_has_no_ast_to_hand_over():
    """It is matched by regex and never reaches the parser."""
    assert classify_query("SHOW STATS orders", SUPER).parsed is None


def test_sqlparser_accepts_a_handed_over_ast_without_reparsing():
    import sqlglot
    from supertable.utils import sql_parser as sp

    sql = "SELECT a FROM orders WHERE id IN (1, 2, 3)"
    ast = classify_query(sql, SUPER).parsed

    calls = []
    original = sp.sqlglot.parse_one

    def counted(*a, **k):
        calls.append(a[0] if a else None)
        return original(*a, **k)

    sp.sqlglot.parse_one = counted
    try:
        handed = sp.SQLParser(super_name=SUPER, query=sql, dialect="duckdb",
                              parsed=ast)
        assert not calls, "a handed-over AST must not be re-parsed"
        fresh = sp.SQLParser(super_name=SUPER, query=sql, dialect="duckdb")
        assert calls, "without one, it must parse"
    finally:
        sp.sqlglot.parse_one = original

    # Same answers either way — the reuse must be invisible.
    assert ([(t.super_name, t.simple_name) for t in handed.get_physical_tables()]
            == [(t.super_name, t.simple_name) for t in fresh.get_physical_tables()])
