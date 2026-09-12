# route: supertable.engine.tests.test_spark_row_filter_dialect
"""Row filters must mean the same thing on Spark as on DuckDB.

Audit H6. RBAC and share filters are built once and DuckDB-quoted:
FilterBuilder emits ``"region" = 'EU'``. DuckDB reads the double quotes as an
IDENTIFIER; Spark reads them as a STRING LITERAL. So on Spark that predicate is
the comparison ``'region' = 'EU'`` — constant false — and its negation is
constant true.

The consequence is not a broken filter but an INVERTED one: ``=`` matched
nothing and ``!=`` matched everything, so a filter meant to restrict a role to
one region instead handed it the whole table.

There is no Spark Thrift server in this environment, so the end-to-end
behaviour is not exercised here. What IS proven is the mechanism — sqlglot
parses the same text as a Column under duckdb and a Literal under spark — and
that the renderer now emits Spark identifiers.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from supertable.engine.spark_thrift import _to_spark_predicate


def test_the_dialects_really_do_disagree():
    """The premise, proven rather than asserted."""
    duck = sqlglot.parse_one('"region" = \'EU\'', read="duckdb")
    spark = sqlglot.parse_one('"region" = \'EU\'', read="spark")
    assert isinstance(duck.this, exp.Column), "duckdb: an identifier"
    assert isinstance(spark.this, exp.Literal), "spark: a string literal"


def test_identifiers_become_backticks():
    assert _to_spark_predicate('"region" = \'EU\'') == "`region` = 'EU'"


def test_the_filter_is_no_longer_constant_false_on_spark():
    """The bug: the rendered predicate compared two string literals."""
    rendered = _to_spark_predicate('"region" = \'EU\'')
    parsed = sqlglot.parse_one(rendered, read="spark")
    assert isinstance(parsed.this, exp.Column), (
        "the left side must be a column on Spark, not a literal"
    )


def test_negation_is_no_longer_constant_true():
    """The dangerous half: != matched every row, removing the restriction."""
    rendered = _to_spark_predicate('"region" <> \'EU\'')
    parsed = sqlglot.parse_one(rendered, read="spark")
    assert isinstance(parsed.this, exp.Column)


@pytest.mark.parametrize("clause,expected", [
    ('("a" > 1) AND ("b" <> \'x\')', "(`a` > 1) AND (`b` <> 'x')"),
    ('"amount" >= "min_price"', "`amount` >= `min_price`"),
    ('"a" IS NULL', "`a` IS NULL"),
    ('"a" IN (1, 2, 3)', "`a` IN (1, 2, 3)"),
])
def test_compound_predicates_transpile(clause, expected):
    assert _to_spark_predicate(clause) == expected


def test_string_literals_are_left_alone():
    """Only identifiers change; a value must not become a column."""
    rendered = _to_spark_predicate('"region" = \'EU\'')
    assert "'EU'" in rendered and "`EU`" not in rendered


def test_no_filter_renders_to_nothing():
    assert _to_spark_predicate("") == ""
    assert _to_spark_predicate(None) == ""


def test_an_unparseable_filter_refuses_rather_than_dropping():
    """Dropping it would serve the full table — the failure this prevents."""
    with pytest.raises(ValueError, match="refusing to serve unfiltered"):
        _to_spark_predicate("not ) valid (")
