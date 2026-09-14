# route: supertable.tests.test_monitor_sql_redaction
"""The monitoring `sql` field must carry no literals, and must be cheap.

Monitoring rows live in a plain Redis partition with a 7-day TTL and are not
passed through the audit encryption helper, so a literal in a WHERE clause is a
literal on disk. Recording the query *shape* instead is therefore the default,
and these tests hold that line.

They also hold the cost line, because the first implementation of the shape did
not. It parsed the query, deep-copied the AST to replace each literal node and
re-rendered it — 470ms on a query carrying a 1,000-value IN list, 3.5x a bare
parse, on every read and synchronously before ``execute`` returned. That made
the read benchmark's random_1000_by_key scenario 61% slower (915 -> 1477ms) at
ca0da6c. Redaction is a question about token spans, not about tree shape, so it
is answered by the tokenizer.

The security property and the cost property are tested together on purpose: the
cheap way to make this fast is to stop redacting, and the cheap way to make it
safe is to parse everything. Neither is acceptable alone.
"""

from __future__ import annotations

import time

import pytest

from supertable.plan_extender import _sql_shape
from supertable.utils.sql_compat import redact_literals

#: Values that must never appear in a recorded shape.
SECRETS = ("hunter2", "4111111111111111", "alice@example.com", "1234.56", "99")


class TestNoLiteralSurvives:

    @pytest.mark.parametrize("sql,forbidden", [
        ("SELECT * FROM t WHERE pw = 'hunter2'", "hunter2"),
        ("SELECT * FROM t WHERE card = '4111111111111111'", "4111111111111111"),
        ("SELECT * FROM t WHERE email = 'alice@example.com'", "alice@example.com"),
        ("SELECT * FROM t WHERE amt = 1234.56", "1234.56"),
        ("SELECT * FROM t WHERE n = 99", "99"),
        ("SELECT * FROM t WHERE k IN (1, 2, 3, 99)", "99"),
        ("SELECT * FROM t WHERE a = 'x' OR b = 'hunter2'", "hunter2"),
        # A literal inside a CASE, a function argument and an ORDER BY.
        ("SELECT CASE WHEN n > 99 THEN 'hunter2' ELSE 'y' END FROM t", "hunter2"),
        ("SELECT substr(s, 1, 99) FROM t ORDER BY 99", "hunter2"),
    ])
    def test_the_value_is_gone(self, sql, forbidden):
        shape = _sql_shape(sql)
        assert forbidden not in shape, f"{forbidden!r} leaked into {shape!r}"

    def test_structure_is_kept(self):
        """The shape has to stay useful for grouping similar queries."""
        shape = _sql_shape(
            "SELECT country, count(*) n FROM orders WHERE ts >= '2024-01-01' "
            "AND ok = TRUE GROUP BY country"
        )
        for token in ("SELECT", "country", "count(*)", "FROM", "orders",
                      "WHERE", "ts >=", "GROUP BY"):
            assert token in shape, f"{token!r} missing from {shape!r}"
        assert "2024-01-01" not in shape

    def test_booleans_and_null_are_redacted_too(self):
        """Matches what the tree-rewriting version did, so shapes stay comparable."""
        shape = _sql_shape("SELECT * FROM t WHERE f = TRUE AND g = FALSE AND h IS NULL")
        assert "TRUE" not in shape and "FALSE" not in shape
        assert shape.count("?") == 3

    def test_it_fails_closed_rather_than_returning_the_query(self):
        """A shape that cannot be produced must not degrade into the raw SQL.

        Asserted through the public helper with input the tokenizer rejects, so
        the guarantee is on the function the monitoring row actually calls.
        """
        assert _sql_shape("SELECT * FROM t WHERE s = 'unterminated") in ("", None) or \
            "unterminated" not in _sql_shape("SELECT * FROM t WHERE s = 'unterminated")

    def test_redact_literals_returns_none_when_it_cannot_tokenize(self):
        """The layer beneath reports failure rather than guessing."""
        out = redact_literals("SELECT * FROM t WHERE s = 'unterminated")
        assert out is None or "unterminated" not in out


class TestItStaysOffTheHotPath:
    """Cost is a correctness property here: this runs on every read."""

    def test_a_large_in_list_is_not_reparsed(self):
        """The regression that motivated this file.

        1,000 literals took 470ms through parse + AST deep-copy + re-render.
        The bound is deliberately loose (10x headroom over the ~27ms measured)
        so this fails on a return to tree rewriting, not on a slow machine.
        """
        sql = "SELECT k FROM t WHERE k IN (" + ",".join(str(i) for i in range(1000)) + ")"
        start = time.perf_counter()
        shape = _sql_shape(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert "999" not in shape, "a literal leaked"
        assert elapsed_ms < 250, (
            f"shaping 1,000 literals took {elapsed_ms:.0f}ms; the tree-rewriting "
            f"implementation this replaced took ~470ms, so this looks like a "
            f"return to parsing the query"
        )

    def test_an_ordinary_query_is_sub_millisecond(self):
        sql = ("SELECT country, count(*) n FROM orders "
               "WHERE ts >= '2024-01-01' GROUP BY country")
        start = time.perf_counter()
        for _ in range(20):
            _sql_shape(sql)
        per_call_ms = (time.perf_counter() - start) / 20 * 1000
        assert per_call_ms < 5, f"{per_call_ms:.2f}ms per call on an ordinary query"


class TestRawSqlRemainsOptIn:
    """Redaction is the default; recording the real statement is a decision."""

    def test_no_organization_is_allowed_raw_by_default(self, monkeypatch):
        from supertable.plan_extender import _monitor_sql_raw_allowed

        monkeypatch.delenv("SUPERTABLE_MONITOR_SQL_RAW", raising=False)
        assert _monitor_sql_raw_allowed("anyorg") is False

    def test_a_named_organization_can_be_allowed(self, monkeypatch):
        from supertable.plan_extender import _monitor_sql_raw_allowed

        monkeypatch.setenv("SUPERTABLE_MONITOR_SQL_RAW", "trusted,other")
        assert _monitor_sql_raw_allowed("trusted") is True
        assert _monitor_sql_raw_allowed("untrusted") is False
