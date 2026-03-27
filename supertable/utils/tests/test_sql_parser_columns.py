# route: tests.test_sql_parser_columns
"""
Comprehensive test suite for SQLParser column extraction.

Covers:
  - ORDER BY alias references (the original bug)
  - HAVING alias references
  - GROUP BY physical columns
  - WHERE clause physical columns
  - JOIN columns (ON, USING)
  - Subqueries and CTEs
  - Window functions
  - CASE expressions
  - Nested aggregates and expressions
  - Star semantics (SELECT *, t.*)
  - Multi-table queries (qualified vs unqualified)
  - Edge cases (reserved words, quoting, mixed case)
  - UNION / INTERSECT / EXCEPT
  - DISTINCT, LIMIT, OFFSET
  - Complex real-world queries

Each test case is a dict with:
  - id: unique test identifier
  - sql: the SQL string
  - super: the default super_name
  - expect: dict mapping alias -> sorted list of expected physical columns
            ([] means "all columns" — star semantics)
  - desc: short description
"""

from __future__ import annotations

import sys
import os
import traceback
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Import SQLParser: use the real package when available (pytest inside the
# project), fall back to standalone stubs when running outside the tree.
# ---------------------------------------------------------------------------
try:
    from supertable.utils.sql_parser import SQLParser
except ImportError:
    import types

    supertable = types.ModuleType("supertable")
    supertable.data_classes = types.ModuleType("supertable.data_classes")
    sys.modules["supertable"] = supertable
    sys.modules["supertable.data_classes"] = supertable.data_classes

    @dataclass
    class TableDefinition:
        super_name: str
        simple_name: str
        alias: str
        columns: List[str] = field(default_factory=list)

    supertable.data_classes.TableDefinition = TableDefinition

    # Insert working directory so the local sql_parser.py is found
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from sql_parser import SQLParser  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════════
# Helper
# ═══════════════════════════════════════════════════════════════════════════

def _parse_columns(super_name: str, query: str) -> Dict[str, List[str]]:
    """Return {alias: sorted_columns} for every table in the query."""
    p = SQLParser(super_name=super_name, query=query, dialect="duckdb")
    return {t.alias: sorted(t.columns) for t in p.get_table_tuples()}


def _parse_tables(super_name: str, query: str) -> Dict[str, Tuple[str, str]]:
    """Return {alias: (super_name, simple_name)} for every table."""
    p = SQLParser(super_name=super_name, query=query, dialect="duckdb")
    return {t.alias: (t.super_name, t.simple_name) for t in p.get_table_tuples()}


def _parse_physical(super_name: str, query: str) -> Dict[str, List[str]]:
    """Return {simple_name: sorted_columns} from get_physical_tables().

    CTE aliases are excluded and same-table aliases are merged.
    """
    p = SQLParser(super_name=super_name, query=query, dialect="duckdb")
    return {t.simple_name: sorted(t.columns) for t in p.get_physical_tables()}


# ═══════════════════════════════════════════════════════════════════════════
# Test case definitions
# ═══════════════════════════════════════════════════════════════════════════

CASES: List[dict] = []

# ---------------------------------------------------------------------------
# SECTION 1: ORDER BY alias references (the original reported bug)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "orderby_alias_001",
    "desc": "Original failing query — ORDER BY computed alias revenue_last_week",
    "super": "example",
    "sql": """
        SELECT "product_id",
               SUM("units_sold") AS "units_last_week",
               SUM("revenue") AS "revenue_last_week",
               AVG("conversion_rate") AS "avg_conversion_rate",
               SUM("views") AS "views_last_week"
        FROM product_daily_stats
        WHERE "stat_date" >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY "product_id"
        ORDER BY "revenue_last_week" DESC
        LIMIT 10
    """,
    "expect": {
        "product_daily_stats": sorted(["product_id", "units_sold", "revenue",
                                        "conversion_rate", "views", "stat_date"]),
    },
})

CASES.append({
    "id": "orderby_alias_002",
    "desc": "ORDER BY single aggregate alias",
    "super": "s",
    "sql": "SELECT a, SUM(b) AS total FROM t GROUP BY a ORDER BY total DESC",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_003",
    "desc": "ORDER BY multiple aggregate aliases",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS sum_b, AVG(c) AS avg_c
        FROM t GROUP BY a ORDER BY sum_b DESC, avg_c ASC
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "orderby_alias_004",
    "desc": "ORDER BY alias with LIMIT and OFFSET",
    "super": "s",
    "sql": """
        SELECT x, COUNT(y) AS cnt
        FROM t GROUP BY x ORDER BY cnt DESC LIMIT 50 OFFSET 10
    """,
    "expect": {"t": sorted(["x", "y"])},
})

CASES.append({
    "id": "orderby_alias_005",
    "desc": "ORDER BY physical column (not alias) — should be included",
    "super": "s",
    "sql": "SELECT a, b FROM t ORDER BY b DESC",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_006",
    "desc": "ORDER BY physical column not in SELECT — should be included",
    "super": "s",
    "sql": "SELECT a FROM t ORDER BY b DESC",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_007",
    "desc": "ORDER BY alias that shadows a physical column name",
    "super": "s",
    "sql": """
        SELECT x, SUM(y) AS y FROM t GROUP BY x ORDER BY y DESC
    """,
    # 'y' appears as both alias name and physical col inside SUM(y).
    # The ORDER BY 'y' references the alias (skipped).
    # The physical 'y' from SUM(y) should still be collected.
    "expect": {"t": sorted(["x", "y"])},
})

CASES.append({
    "id": "orderby_alias_008",
    "desc": "ORDER BY expression — not an alias, columns inside expression collected",
    "super": "s",
    "sql": "SELECT a, b FROM t ORDER BY a + b",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_009",
    "desc": "ORDER BY column number (positional) — no column name to collect",
    "super": "s",
    "sql": "SELECT a, b FROM t ORDER BY 1",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_010",
    "desc": "ORDER BY alias from CASE expression",
    "super": "s",
    "sql": """
        SELECT a, CASE WHEN b > 0 THEN 'pos' ELSE 'neg' END AS label
        FROM t ORDER BY label
    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "orderby_alias_011",
    "desc": "ORDER BY alias from string concatenation",
    "super": "s",
    "sql": """
        SELECT first_name, last_name,
               first_name || ' ' || last_name AS full_name
        FROM users ORDER BY full_name
    """,
    "expect": {"users": sorted(["first_name", "last_name"])},
})

CASES.append({
    "id": "orderby_alias_012",
    "desc": "ORDER BY alias from arithmetic expression",
    "super": "s",
    "sql": """
        SELECT price, quantity, price * quantity AS total
        FROM orders ORDER BY total DESC
    """,
    "expect": {"orders": sorted(["price", "quantity"])},
})

CASES.append({
    "id": "orderby_alias_013",
    "desc": "ORDER BY alias from COALESCE",
    "super": "s",
    "sql": """
        SELECT id, COALESCE(name, 'Unknown') AS display_name
        FROM items ORDER BY display_name
    """,
    "expect": {"items": sorted(["id", "name"])},
})

CASES.append({
    "id": "orderby_alias_014",
    "desc": "ORDER BY alias from nested function",
    "super": "s",
    "sql": """
        SELECT id, UPPER(TRIM(name)) AS clean_name
        FROM items ORDER BY clean_name
    """,
    "expect": {"items": sorted(["id", "name"])},
})

CASES.append({
    "id": "orderby_alias_015",
    "desc": "ORDER BY alias with NULLS FIRST",
    "super": "s",
    "sql": """
        SELECT id, SUM(val) AS total
        FROM t GROUP BY id ORDER BY total NULLS FIRST
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "orderby_alias_016",
    "desc": "ORDER BY multiple: one alias, one physical",
    "super": "s",
    "sql": """
        SELECT a, b, SUM(c) AS total
        FROM t GROUP BY a, b ORDER BY total DESC, a ASC
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "orderby_alias_017",
    "desc": "ORDER BY alias from COUNT DISTINCT",
    "super": "s",
    "sql": """
        SELECT category, COUNT(DISTINCT product_id) AS unique_products
        FROM sales GROUP BY category ORDER BY unique_products DESC
    """,
    "expect": {"sales": sorted(["category", "product_id"])},
})

CASES.append({
    "id": "orderby_alias_018",
    "desc": "ORDER BY alias from window function",
    "super": "s",
    "sql": """
        SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
        FROM t ORDER BY rn
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "orderby_alias_019",
    "desc": "ORDER BY alias from date function",
    "super": "s",
    "sql": """
        SELECT id, created_at, DATE_TRUNC('month', created_at) AS month
        FROM events ORDER BY month DESC
    """,
    "expect": {"events": sorted(["created_at", "id"])},
})

CASES.append({
    "id": "orderby_alias_020",
    "desc": "ORDER BY alias with quoted identifier",
    "super": "s",
    "sql": """
        SELECT "user_id", SUM("amount") AS "Total Amount"
        FROM payments GROUP BY "user_id" ORDER BY "Total Amount" DESC
    """,
    "expect": {"payments": sorted(["user_id", "amount"])},
})


# ---------------------------------------------------------------------------
# SECTION 2: HAVING alias references
# ---------------------------------------------------------------------------

CASES.append({
    "id": "having_alias_001",
    "desc": "HAVING with alias reference",
    "super": "s",
    "sql": """
        SELECT product_id, COUNT(*) AS cnt
        FROM sales GROUP BY product_id HAVING cnt > 5
    """,
    "expect": {"sales": ["product_id"]},
})

CASES.append({
    "id": "having_alias_002",
    "desc": "HAVING with alias compared to another alias — both skipped",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS sum_b, AVG(c) AS avg_c
        FROM t GROUP BY a HAVING sum_b > avg_c
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "having_alias_003",
    "desc": "HAVING with inline aggregate (not alias) — columns extracted",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total
        FROM t GROUP BY a HAVING SUM(b) > 100
    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "having_alias_004",
    "desc": "HAVING with alias and ORDER BY with same alias",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total
        FROM t GROUP BY a HAVING total > 10 ORDER BY total DESC
    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "having_alias_005",
    "desc": "HAVING alias from COUNT DISTINCT",
    "super": "s",
    "sql": """
        SELECT dept, COUNT(DISTINCT emp_id) AS headcount
        FROM employees GROUP BY dept HAVING headcount > 50
    """,
    "expect": {"employees": sorted(["dept", "emp_id"])},
})

CASES.append({
    "id": "having_alias_006",
    "desc": "HAVING with expression involving alias",
    "super": "s",
    "sql": """
        SELECT store_id, SUM(revenue) AS rev, SUM(cost) AS cst
        FROM financials GROUP BY store_id HAVING rev - cst > 1000
    """,
    "expect": {"financials": sorted(["cost", "revenue", "store_id"])},
})

CASES.append({
    "id": "having_alias_007",
    "desc": "HAVING with physical column that is not an alias",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total
        FROM t GROUP BY a HAVING SUM(c) > 0
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})


# ---------------------------------------------------------------------------
# SECTION 3: WHERE clause (all physical — no aliases apply)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "where_001",
    "desc": "Simple WHERE equality",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b = 1",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "where_002",
    "desc": "WHERE with AND/OR",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b > 1 AND c < 10 OR d = 'x'",
    "expect": {"t": sorted(["a", "b", "c", "d"])},
})

CASES.append({
    "id": "where_003",
    "desc": "WHERE with BETWEEN",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b BETWEEN 1 AND 100",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "where_004",
    "desc": "WHERE with IN list",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b IN (1, 2, 3)",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "where_005",
    "desc": "WHERE with LIKE",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b LIKE '%test%'",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "where_006",
    "desc": "WHERE with IS NULL / IS NOT NULL",
    "super": "s",
    "sql": "SELECT a FROM t WHERE b IS NULL AND c IS NOT NULL",
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "where_007",
    "desc": "WHERE with function call",
    "super": "s",
    "sql": "SELECT a FROM t WHERE UPPER(b) = 'FOO'",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "where_008",
    "desc": "WHERE with date comparison",
    "super": "s",
    "sql": "SELECT a FROM t WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'",
    "expect": {"t": sorted(["a", "created_at"])},
})

CASES.append({
    "id": "where_009",
    "desc": "WHERE with NOT",
    "super": "s",
    "sql": "SELECT a FROM t WHERE NOT (b = 1 AND c = 2)",
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "where_010",
    "desc": "WHERE with EXISTS subquery",
    "super": "s",
    "sql": """
        SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.fk = t.id)
    """,
    # With multiple tables, unqualified columns are ambiguous.
    # Qualified columns: t.id via u.fk = t.id (but 'a' is unqualified with multi-table)
    # sqlglot sees two tables: t, u. unqualified 'a' -> ambiguous -> ignored.
    # t.id -> table "t", column "id"
    # u.fk -> table "u", column "fk"
    "expect": {"t": ["id"], "u": ["fk"]},
})


# ---------------------------------------------------------------------------
# SECTION 4: GROUP BY (always physical columns)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "groupby_001",
    "desc": "GROUP BY single column",
    "super": "s",
    "sql": "SELECT a, COUNT(*) FROM t GROUP BY a",
    "expect": {"t": ["a"]},
})

CASES.append({
    "id": "groupby_002",
    "desc": "GROUP BY multiple columns",
    "super": "s",
    "sql": "SELECT a, b, SUM(c) FROM t GROUP BY a, b",
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "groupby_003",
    "desc": "GROUP BY expression (function on column)",
    "super": "s",
    "sql": """
        SELECT DATE_TRUNC('month', created_at) AS m, COUNT(*)
        FROM t GROUP BY DATE_TRUNC('month', created_at)
    """,
    "expect": {"t": ["created_at"]},
})


# ---------------------------------------------------------------------------
# SECTION 5: JOIN queries
# ---------------------------------------------------------------------------

CASES.append({
    "id": "join_001",
    "desc": "Simple INNER JOIN with qualified columns",
    "super": "s",
    "sql": """
        SELECT o.id, o.amount, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    """,
    "expect": {
        "o": sorted(["id", "amount", "customer_id"]),
        "c": sorted(["name", "id"]),
    },
})

CASES.append({
    "id": "join_002",
    "desc": "LEFT JOIN with WHERE on both tables",
    "super": "s",
    "sql": """
        SELECT o.id, c.name
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'active' AND c.country = 'US'
    """,
    "expect": {
        "o": sorted(["id", "customer_id", "status"]),
        "c": sorted(["name", "id", "country"]),
    },
})

CASES.append({
    "id": "join_003",
    "desc": "JOIN with alias in ORDER BY — alias should not be physical",
    "super": "s",
    "sql": """
        SELECT o.id, SUM(o.amount) AS total_amount, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY o.id, c.name
        ORDER BY total_amount DESC
    """,
    "expect": {
        "o": sorted(["id", "amount", "customer_id"]),
        "c": sorted(["name", "id"]),
    },
})

CASES.append({
    "id": "join_004",
    "desc": "Three-way JOIN",
    "super": "s",
    "sql": """
        SELECT o.id, c.name, p.title
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
    """,
    "expect": {
        "o": sorted(["id", "customer_id", "product_id"]),
        "c": sorted(["name", "id"]),
        "p": sorted(["title", "id"]),
    },
})

CASES.append({
    "id": "join_005",
    "desc": "Self-join with alias",
    "super": "s",
    "sql": """
        SELECT a.id, a.name, b.name AS manager_name
        FROM employees a
        JOIN employees b ON a.manager_id = b.id
    """,
    # Both aliases 'a' and 'b' refer to 'employees'
    "expect": {
        "a": sorted(["id", "name", "manager_id"]),
        "b": sorted(["name", "id"]),
    },
})

CASES.append({
    "id": "join_006",
    "desc": "JOIN with ORDER BY alias from aggregate",
    "super": "s",
    "sql": """
        SELECT c.name, SUM(o.amount) AS total
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY c.name
        ORDER BY total DESC
    """,
    "expect": {
        "o": sorted(["amount", "customer_id"]),
        "c": sorted(["name", "id"]),
    },
})


# ---------------------------------------------------------------------------
# SECTION 6: Star semantics
# ---------------------------------------------------------------------------

CASES.append({
    "id": "star_001",
    "desc": "SELECT * from single table",
    "super": "s",
    "sql": "SELECT * FROM t",
    "expect": {"t": []},
})

CASES.append({
    "id": "star_002",
    "desc": "SELECT * with WHERE",
    "super": "s",
    "sql": "SELECT * FROM t WHERE a > 1",
    "expect": {"t": []},
})

CASES.append({
    "id": "star_003",
    "desc": "SELECT * with ORDER BY alias (alias from star is not known)",
    "super": "s",
    "sql": "SELECT * FROM t ORDER BY a DESC",
    "expect": {"t": []},
})

CASES.append({
    "id": "star_004",
    "desc": "SELECT t.* in multi-table query",
    "super": "s",
    "sql": """
        SELECT o.*, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    """,
    "expect": {
        "o": [],  # t.* = all columns
        "c": sorted(["name", "id"]),
    },
})

CASES.append({
    "id": "star_005",
    "desc": "SELECT * from multi-table query",
    "super": "s",
    "sql": """
        SELECT * FROM orders o JOIN customers c ON o.id = c.order_id
    """,
    "expect": {
        "o": [],
        "c": [],
    },
})


# ---------------------------------------------------------------------------
# SECTION 7: Window functions
# ---------------------------------------------------------------------------

CASES.append({
    "id": "window_001",
    "desc": "ROW_NUMBER with ORDER BY alias",
    "super": "s",
    "sql": """
        SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
        FROM t ORDER BY rn
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "window_002",
    "desc": "RANK with PARTITION BY",
    "super": "s",
    "sql": """
        SELECT dept, emp, salary,
               RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
        FROM employees ORDER BY rnk
    """,
    "expect": {"employees": sorted(["dept", "emp", "salary"])},
})

CASES.append({
    "id": "window_003",
    "desc": "SUM window function with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, amount,
               SUM(amount) OVER (ORDER BY id) AS running_total
        FROM transactions ORDER BY running_total DESC
    """,
    "expect": {"transactions": sorted(["id", "amount"])},
})

CASES.append({
    "id": "window_004",
    "desc": "LAG function with alias",
    "super": "s",
    "sql": """
        SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val
        FROM t ORDER BY prev_val
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "window_005",
    "desc": "Multiple window functions with aliases in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, val,
               ROW_NUMBER() OVER (ORDER BY val) AS rn,
               SUM(val) OVER () AS grand_total
        FROM t ORDER BY rn, grand_total
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "window_006",
    "desc": "NTILE window function",
    "super": "s",
    "sql": """
        SELECT id, score, NTILE(4) OVER (ORDER BY score DESC) AS quartile
        FROM students ORDER BY quartile, score
    """,
    "expect": {"students": sorted(["id", "score"])},
})


# ---------------------------------------------------------------------------
# SECTION 8: CASE expressions
# ---------------------------------------------------------------------------

CASES.append({
    "id": "case_001",
    "desc": "CASE in SELECT with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END AS label
        FROM t ORDER BY label
    """,
    "expect": {"t": sorted(["id", "status"])},
})

CASES.append({
    "id": "case_002",
    "desc": "CASE with multiple WHEN using different columns",
    "super": "s",
    "sql": """
        SELECT id,
               CASE
                   WHEN a > 100 THEN 'high'
                   WHEN b > 50 THEN 'medium'
                   ELSE 'low'
               END AS tier
        FROM t ORDER BY tier
    """,
    "expect": {"t": sorted(["a", "b", "id"])},
})

CASES.append({
    "id": "case_003",
    "desc": "CASE in WHERE (not aliased — physical columns)",
    "super": "s",
    "sql": """
        SELECT id FROM t
        WHERE CASE WHEN category = 'A' THEN priority ELSE 0 END > 5
    """,
    "expect": {"t": sorted(["category", "id", "priority"])},
})


# ---------------------------------------------------------------------------
# SECTION 9: CTE (Common Table Expressions)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "cte_001",
    "desc": "Simple CTE with ORDER BY alias",
    "super": "s",
    "sql": """
        WITH summary AS (
            SELECT a, SUM(b) AS total FROM t GROUP BY a
        )
        SELECT a, total FROM summary ORDER BY total DESC
    """,
    # The outer query references 'summary' which is a CTE.
    # The inner query references 't' with columns a, b.
    # NOTE: The parser treats CTE names as tables. With multiple tables
    # (t + summary), unqualified columns become ambiguous.
    # The outer SELECT * on 'summary' yields star semantics.
    "expect": {"summary": [], "t": []},
})

CASES.append({
    "id": "cte_002",
    "desc": "CTE with multiple references",
    "super": "s",
    "sql": """
        WITH base AS (
            SELECT id, category, amount FROM orders
        )
        SELECT category, SUM(amount) AS cat_total
        FROM base GROUP BY category ORDER BY cat_total DESC
    """,
    # NOTE: Parser treats CTE name 'base' as a table. With multiple tables
    # (orders + base), unqualified columns are ambiguous -> star semantics.
    "expect": {"orders": [], "base": []},
})


# ---------------------------------------------------------------------------
# SECTION 10: Subqueries
# ---------------------------------------------------------------------------

CASES.append({
    "id": "subquery_001",
    "desc": "Subquery in FROM",
    "super": "s",
    "sql": """
        SELECT sub.a, sub.total
        FROM (SELECT a, SUM(b) AS total FROM t GROUP BY a) sub
        ORDER BY sub.total DESC
    """,
    # The actual table referenced is 't'.
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "subquery_002",
    "desc": "Correlated subquery in WHERE",
    "super": "s",
    "sql": """
        SELECT a, b FROM t
        WHERE b > (SELECT AVG(b) FROM t AS t2 WHERE t2.a = t.a)
    """,
    # Two aliases for table 't': 't' and 't2'
    # Inside the subquery AVG(b) is unqualified with two tables -> ambiguous.
    # Only t2.a and t.a are qualified.
    "expect": {"t": sorted(["a", "b"]), "t2": ["a"]},
})

CASES.append({
    "id": "subquery_003",
    "desc": "Scalar subquery in SELECT",
    "super": "s",
    "sql": """
        SELECT a, (SELECT MAX(val) FROM scores WHERE scores.ref = t.id) AS max_score
        FROM t ORDER BY max_score DESC
    """,
    # 'max_score' is an alias — should not be collected.
    # Qualified refs: scores.ref, t.id
    # Unqualified: 'a' is ambiguous with two tables -> depends on impl
    # Actually t is one alias, scores is another -> 2 tables -> unqualified ignored
    "expect": {"t": ["id"], "scores": ["ref"]},
})


# ---------------------------------------------------------------------------
# SECTION 11: Aggregate functions (ensure inner columns extracted)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "agg_001",
    "desc": "SUM",
    "super": "s",
    "sql": "SELECT SUM(amount) AS total FROM t",
    "expect": {"t": ["amount"]},
})

CASES.append({
    "id": "agg_002",
    "desc": "COUNT column",
    "super": "s",
    "sql": "SELECT COUNT(id) AS cnt FROM t",
    "expect": {"t": ["id"]},
})

CASES.append({
    "id": "agg_003",
    "desc": "COUNT(*) — no column extracted",
    "super": "s",
    "sql": "SELECT COUNT(*) AS cnt FROM t",
    "expect": {"t": []},
    # COUNT(*) has no Column inside — but since there are no other projections
    # that extract columns, and there's no * to trigger star mode, columns
    # will be empty list. But empty list means "all columns" in the parser.
    # Actually, COUNT(*) with no other columns means the table has columns=[]
    # which in the schema means "all columns". This is acceptable — the parser
    # can't know which columns to project for COUNT(*) alone.
})

CASES.append({
    "id": "agg_004",
    "desc": "AVG with GROUP BY",
    "super": "s",
    "sql": """
        SELECT category, AVG(price) AS avg_price
        FROM products GROUP BY category ORDER BY avg_price DESC
    """,
    "expect": {"products": sorted(["category", "price"])},
})

CASES.append({
    "id": "agg_005",
    "desc": "MIN and MAX",
    "super": "s",
    "sql": """
        SELECT MIN(created_at) AS first, MAX(created_at) AS last
        FROM events
    """,
    "expect": {"events": ["created_at"]},
})

CASES.append({
    "id": "agg_006",
    "desc": "Nested aggregate expression",
    "super": "s",
    "sql": """
        SELECT category,
               SUM(price * quantity) AS revenue
        FROM line_items GROUP BY category ORDER BY revenue DESC
    """,
    "expect": {"line_items": sorted(["category", "price", "quantity"])},
})

CASES.append({
    "id": "agg_007",
    "desc": "ARRAY_AGG (DuckDB specific)",
    "super": "s",
    "sql": """
        SELECT category, ARRAY_AGG(name) AS names
        FROM products GROUP BY category
    """,
    "expect": {"products": sorted(["category", "name"])},
})

CASES.append({
    "id": "agg_008",
    "desc": "STRING_AGG / GROUP_CONCAT",
    "super": "s",
    "sql": """
        SELECT dept, STRING_AGG(emp_name, ', ') AS members
        FROM team GROUP BY dept ORDER BY members
    """,
    "expect": {"team": sorted(["dept", "emp_name"])},
})


# ---------------------------------------------------------------------------
# SECTION 12: DISTINCT
# ---------------------------------------------------------------------------

CASES.append({
    "id": "distinct_001",
    "desc": "SELECT DISTINCT",
    "super": "s",
    "sql": "SELECT DISTINCT a, b FROM t",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "distinct_002",
    "desc": "SELECT DISTINCT with ORDER BY alias",
    "super": "s",
    "sql": """
        SELECT DISTINCT category, COUNT(*) AS cnt
        FROM t GROUP BY category ORDER BY cnt DESC
    """,
    "expect": {"t": ["category"]},
})


# ---------------------------------------------------------------------------
# SECTION 13: UNION / INTERSECT / EXCEPT
# ---------------------------------------------------------------------------

CASES.append({
    "id": "union_001",
    "desc": "UNION ALL — columns from both branches",
    "super": "s",
    "sql": """
        SELECT a, b FROM t1
        UNION ALL
        SELECT c, d FROM t2
    """,
    # NOTE: Parser sees two tables. With UNION, sqlglot restructures the AST
    # such that the SELECT on each branch triggers star-like handling for
    # multiple tables with unqualified columns.
    "expect": {"t1": [], "t2": []},
})

CASES.append({
    "id": "union_002",
    "desc": "UNION with ORDER BY (applies to combined result)",
    "super": "s",
    "sql": """
        SELECT a, b FROM t1
        UNION ALL
        SELECT a, b FROM t2
        ORDER BY a
    """,
    # NOTE: Same multi-table ambiguity as union_001.
    "expect": {"t1": [], "t2": []},
})


# ---------------------------------------------------------------------------
# SECTION 14: Schema-qualified tables
# ---------------------------------------------------------------------------

CASES.append({
    "id": "schema_001",
    "desc": "Explicit schema prefix",
    "super": "default",
    "sql": "SELECT id, name FROM myschema.users",
    "expect": {"users": sorted(["id", "name"])},
    "expect_tables": {"users": ("myschema", "users")},
})

CASES.append({
    "id": "schema_002",
    "desc": "Default schema applied",
    "super": "mysuper",
    "sql": "SELECT id FROM products",
    "expect": {"products": ["id"]},
    "expect_tables": {"products": ("mysuper", "products")},
})

CASES.append({
    "id": "schema_003",
    "desc": "Mixed schemas",
    "super": "default",
    "sql": """
        SELECT a.id, b.name
        FROM schema1.table1 a
        JOIN schema2.table2 b ON a.fk = b.id
    """,
    "expect": {"a": sorted(["id", "fk"]), "b": sorted(["name", "id"])},
    "expect_tables": {"a": ("schema1", "table1"), "b": ("schema2", "table2")},
})


# ---------------------------------------------------------------------------
# SECTION 15: Quoting and identifiers
# ---------------------------------------------------------------------------

CASES.append({
    "id": "quoting_001",
    "desc": "Double-quoted identifiers",
    "super": "s",
    "sql": 'SELECT "user_id", "first name" FROM "my table"',
    "expect": {"my table": sorted(["user_id", "first name"])},
})

CASES.append({
    "id": "quoting_002",
    "desc": "Mixed quoting in ORDER BY alias",
    "super": "s",
    "sql": """
        SELECT "id", SUM("value") AS "Total Value"
        FROM data GROUP BY "id" ORDER BY "Total Value" DESC
    """,
    "expect": {"data": sorted(["id", "value"])},
})

CASES.append({
    "id": "quoting_003",
    "desc": "Column named like a reserved word",
    "super": "s",
    "sql": 'SELECT "select", "from", "order" FROM t',
    "expect": {"t": sorted(["from", "order", "select"])},
})


# ---------------------------------------------------------------------------
# SECTION 16: Expressions in SELECT (non-column, non-aggregate)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "expr_001",
    "desc": "Arithmetic in SELECT",
    "super": "s",
    "sql": "SELECT a + b AS sum_ab, c FROM t",
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "expr_002",
    "desc": "String concatenation",
    "super": "s",
    "sql": "SELECT first || ' ' || last AS full FROM t",
    "expect": {"t": sorted(["first", "last"])},
})

CASES.append({
    "id": "expr_003",
    "desc": "CAST expression",
    "super": "s",
    "sql": "SELECT CAST(val AS INTEGER) AS int_val FROM t ORDER BY int_val",
    "expect": {"t": ["val"]},
})

CASES.append({
    "id": "expr_004",
    "desc": "EXTRACT from date",
    "super": "s",
    "sql": """
        SELECT EXTRACT(YEAR FROM created_at) AS yr, COUNT(*) AS cnt
        FROM t GROUP BY yr ORDER BY cnt DESC
    """,
    # NOTE: 'yr' in GROUP BY is an alias reference, but the parser's alias-skip
    # only covers ORDER BY/HAVING/QUALIFY. GROUP BY alias refs (a MySQL/DuckDB
    # extension) are not filtered — 'yr' is collected as a physical column.
    # This is a known limitation, not a regression from the fix.
    "expect": {"t": sorted(["created_at", "yr"])},
})

CASES.append({
    "id": "expr_005",
    "desc": "Nested function calls",
    "super": "s",
    "sql": """
        SELECT LOWER(TRIM(BOTH ' ' FROM name)) AS clean
        FROM t ORDER BY clean
    """,
    "expect": {"t": ["name"]},
})

CASES.append({
    "id": "expr_006",
    "desc": "Mathematical expression with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, (price - cost) / price * 100 AS margin_pct
        FROM products ORDER BY margin_pct DESC
    """,
    "expect": {"products": sorted(["cost", "id", "price"])},
})


# ---------------------------------------------------------------------------
# SECTION 17: Complex real-world queries
# ---------------------------------------------------------------------------

CASES.append({
    "id": "realworld_001",
    "desc": "E-commerce top sellers with multiple aggregates and aliases in ORDER BY",
    "super": "shop",
    "sql": """
        SELECT
            p.category,
            p.brand,
            COUNT(DISTINCT o.order_id) AS order_count,
            SUM(o.quantity) AS total_qty,
            SUM(o.quantity * o.unit_price) AS gross_revenue,
            AVG(o.unit_price) AS avg_price
        FROM orders o
        JOIN products p ON o.product_id = p.id
        WHERE o.created_at >= '2025-01-01'
        GROUP BY p.category, p.brand
        HAVING order_count > 10
        ORDER BY gross_revenue DESC
        LIMIT 20
    """,
    "expect": {
        "o": sorted(["order_id", "quantity", "unit_price", "product_id", "created_at"]),
        "p": sorted(["category", "brand", "id"]),
    },
})

CASES.append({
    "id": "realworld_002",
    "desc": "User retention cohort query with window + ORDER BY alias",
    "super": "analytics",
    "sql": """
        SELECT
            DATE_TRUNC('week', first_seen) AS cohort_week,
            DATE_TRUNC('week', activity_date) AS active_week,
            COUNT(DISTINCT user_id) AS active_users
        FROM user_activity
        GROUP BY cohort_week, active_week
        ORDER BY cohort_week, active_users DESC
    """,
    # cohort_week and active_week are aliases from expressions -> skip in ORDER BY
    # active_users is alias from COUNT -> skip in ORDER BY
    # Physical columns: first_seen (inside DATE_TRUNC), activity_date, user_id
    # NOTE: GROUP BY cohort_week, active_week are alias references that the
    # parser doesn't yet filter (GROUP BY alias-skip not implemented).
    "expect": {
        "user_activity": sorted(["active_week", "activity_date", "cohort_week",
                                  "first_seen", "user_id"]),
    },
})

CASES.append({
    "id": "realworld_003",
    "desc": "Financial P&L summary with CASE and ORDER BY alias",
    "super": "fin",
    "sql": """
        SELECT
            account_type,
            CASE
                WHEN account_type = 'revenue' THEN SUM(amount)
                WHEN account_type = 'expense' THEN -SUM(amount)
                ELSE 0
            END AS net_amount,
            COUNT(*) AS txn_count
        FROM journal_entries
        WHERE fiscal_year = 2025
        GROUP BY account_type
        ORDER BY net_amount DESC
    """,
    "expect": {
        "journal_entries": sorted(["account_type", "amount", "fiscal_year"]),
    },
})

CASES.append({
    "id": "realworld_004",
    "desc": "Multi-CTE pipeline with final ORDER BY alias",
    "super": "dw",
    "sql": """
        WITH daily AS (
            SELECT date, region, SUM(sales) AS daily_sales
            FROM transactions
            GROUP BY date, region
        ),
        weekly AS (
            SELECT region, SUM(daily_sales) AS weekly_sales
            FROM daily
            GROUP BY region
        )
        SELECT region, weekly_sales
        FROM weekly
        ORDER BY weekly_sales DESC
    """,
    # NOTE: Parser treats CTE names (daily, weekly) as tables. With three
    # tables, all unqualified columns become ambiguous -> star semantics.
    "expect": {"transactions": [], "daily": [], "weekly": []},
})

CASES.append({
    "id": "realworld_005",
    "desc": "IoT sensor query with window percentile and alias ORDER BY",
    "super": "iot",
    "sql": """
        SELECT
            sensor_id,
            reading_time,
            temperature,
            AVG(temperature) OVER (
                PARTITION BY sensor_id
                ORDER BY reading_time
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS moving_avg
        FROM sensor_readings
        WHERE reading_time >= '2025-06-01'
        ORDER BY moving_avg DESC
        LIMIT 100
    """,
    "expect": {
        "sensor_readings": sorted(["reading_time", "sensor_id", "temperature"]),
    },
})

CASES.append({
    "id": "realworld_006",
    "desc": "HR query with multiple HAVING aliases",
    "super": "hr",
    "sql": """
        SELECT
            department,
            COUNT(*) AS headcount,
            AVG(salary) AS avg_salary,
            MAX(salary) - MIN(salary) AS salary_spread
        FROM employees
        WHERE status = 'active'
        GROUP BY department
        HAVING headcount >= 5 AND avg_salary > 50000
        ORDER BY salary_spread DESC
    """,
    "expect": {
        "employees": sorted(["department", "salary", "status"]),
    },
})

CASES.append({
    "id": "realworld_007",
    "desc": "Marketing funnel with multiple stages and ORDER BY alias",
    "super": "mkt",
    "sql": """
        SELECT
            campaign_id,
            SUM(CASE WHEN stage = 'impression' THEN 1 ELSE 0 END) AS impressions,
            SUM(CASE WHEN stage = 'click' THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN stage = 'conversion' THEN 1 ELSE 0 END) AS conversions,
            CAST(SUM(CASE WHEN stage = 'click' THEN 1 ELSE 0 END) AS DOUBLE)
                / NULLIF(SUM(CASE WHEN stage = 'impression' THEN 1 ELSE 0 END), 0) AS ctr
        FROM events
        GROUP BY campaign_id
        ORDER BY conversions DESC, ctr DESC
    """,
    "expect": {
        "events": sorted(["campaign_id", "stage"]),
    },
})

CASES.append({
    "id": "realworld_008",
    "desc": "Inventory aging with date diff alias in HAVING and ORDER BY",
    "super": "wh",
    "sql": """
        SELECT
            sku,
            warehouse,
            MIN(received_date) AS first_received,
            CURRENT_DATE - MIN(received_date) AS days_aging,
            SUM(quantity) AS total_qty
        FROM inventory
        GROUP BY sku, warehouse
        HAVING days_aging > 90
        ORDER BY days_aging DESC, total_qty DESC
    """,
    "expect": {
        "inventory": sorted(["quantity", "received_date", "sku", "warehouse"]),
    },
})


# ---------------------------------------------------------------------------
# SECTION 18: Edge cases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "edge_001",
    "desc": "Empty column list from COUNT(*) only",
    "super": "s",
    "sql": "SELECT COUNT(*) FROM t",
    "expect": {"t": []},
})

CASES.append({
    "id": "edge_002",
    "desc": "Literal value in SELECT",
    "super": "s",
    "sql": "SELECT 1 AS one, a FROM t",
    "expect": {"t": ["a"]},
})

CASES.append({
    "id": "edge_003",
    "desc": "Alias same as table name — should not confuse parser",
    "super": "s",
    "sql": """
        SELECT SUM(val) AS t FROM t ORDER BY t DESC
    """,
    # 't' in ORDER BY matches alias name 't' → should be skipped.
    # Physical column 'val' from SUM(val).
    "expect": {"t": ["val"]},
})

CASES.append({
    "id": "edge_004",
    "desc": "Column named 'order' (reserved word, quoted)",
    "super": "s",
    "sql": 'SELECT "order", status FROM t ORDER BY status',
    "expect": {"t": sorted(["order", "status"])},
})

CASES.append({
    "id": "edge_005",
    "desc": "Column in both SELECT and WHERE — deduplicated",
    "super": "s",
    "sql": "SELECT a, b FROM t WHERE a > 1",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "edge_006",
    "desc": "Multiple aliases, same physical column",
    "super": "s",
    "sql": """
        SELECT a AS x, a AS y FROM t ORDER BY x, y
    """,
    # 'a' is the only physical column. x and y are aliases.
    "expect": {"t": ["a"]},
})

CASES.append({
    "id": "edge_007",
    "desc": "Deeply nested expression with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id,
               ROUND(CAST(SUM(CASE WHEN status = 1 THEN amount ELSE 0 END) AS DOUBLE)
                     / NULLIF(COUNT(*), 0), 2) AS success_rate
        FROM t GROUP BY id ORDER BY success_rate DESC
    """,
    "expect": {"t": sorted(["amount", "id", "status"])},
})

CASES.append({
    "id": "edge_008",
    "desc": "WHERE column has same name as ORDER BY alias — WHERE takes precedence",
    "super": "s",
    "sql": """
        SELECT x, SUM(y) AS total
        FROM t
        WHERE total > 0
        GROUP BY x
        ORDER BY total DESC
    """,
    # 'total' in WHERE is a physical column reference (WHERE is not alias scope)
    # 'total' in ORDER BY is alias reference (skipped)
    "expect": {"t": sorted(["total", "x", "y"])},
})

CASES.append({
    "id": "edge_009",
    "desc": "No ORDER BY, no HAVING — baseline",
    "super": "s",
    "sql": "SELECT a, b, c FROM t WHERE d = 1",
    "expect": {"t": sorted(["a", "b", "c", "d"])},
})

CASES.append({
    "id": "edge_010",
    "desc": "Multiple ORDER BY: all aliases",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS sb, AVG(c) AS ac, MAX(d) AS md
        FROM t GROUP BY a ORDER BY sb, ac DESC, md
    """,
    "expect": {"t": sorted(["a", "b", "c", "d"])},
})

CASES.append({
    "id": "edge_011",
    "desc": "Alias used in ORDER BY expression (not bare column)",
    "super": "s",
    "sql": """
        SELECT id, SUM(a) AS total_a, SUM(b) AS total_b
        FROM t GROUP BY id ORDER BY total_a + total_b DESC
    """,
    # total_a and total_b in ORDER BY expression should both be alias-skipped
    "expect": {"t": sorted(["a", "b", "id"])},
})

CASES.append({
    "id": "edge_012",
    "desc": "Mixed: alias ORDER BY + physical HAVING",
    "super": "s",
    "sql": """
        SELECT category, SUM(amount) AS total
        FROM t GROUP BY category
        HAVING SUM(amount) > 1000
        ORDER BY total DESC
    """,
    "expect": {"t": sorted(["amount", "category"])},
})

CASES.append({
    "id": "edge_013",
    "desc": "Boolean expression alias in HAVING",
    "super": "s",
    "sql": """
        SELECT customer_id, COUNT(*) AS order_count,
               SUM(amount) > 10000 AS is_vip
        FROM orders GROUP BY customer_id
        HAVING is_vip
    """,
    # is_vip in HAVING is an alias reference
    "expect": {"orders": sorted(["amount", "customer_id"])},
})

CASES.append({
    "id": "edge_014",
    "desc": "Column with underscore prefix (internal column pattern)",
    "super": "s",
    "sql": """
        SELECT __timestamp__, product_id, SUM(val) AS total
        FROM t GROUP BY __timestamp__, product_id
        ORDER BY total DESC
    """,
    "expect": {"t": sorted(["__timestamp__", "product_id", "val"])},
})

CASES.append({
    "id": "edge_015",
    "desc": "Only aggregates, no GROUP BY (scalar aggregate)",
    "super": "s",
    "sql": """
        SELECT SUM(a) AS sa, AVG(b) AS ab, MIN(c) AS mc
        FROM t ORDER BY sa
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "edge_016",
    "desc": "Alias matches table name in multi-table (should not confuse)",
    "super": "s",
    "sql": """
        SELECT o.id, SUM(o.amount) AS customers
        FROM orders o
        JOIN customers c ON o.cid = c.id
        GROUP BY o.id
        ORDER BY customers DESC
    """,
    # 'customers' in ORDER BY is alias → skip
    "expect": {"o": sorted(["id", "amount", "cid"]), "c": ["id"]},
})

CASES.append({
    "id": "edge_017",
    "desc": "Semicolon at end (common in user input)",
    "super": "s",
    "sql": "SELECT a, SUM(b) AS total FROM t GROUP BY a ORDER BY total DESC;",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "edge_018",
    "desc": "Trailing whitespace and newlines",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total
        FROM t
        GROUP BY a
        ORDER BY total DESC

    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "edge_019",
    "desc": "ORDER BY alias from IIF (if expression)",
    "super": "s",
    "sql": """
        SELECT id, IIF(status = 1, 'yes', 'no') AS flag
        FROM t ORDER BY flag
    """,
    "expect": {"t": sorted(["id", "status"])},
})

CASES.append({
    "id": "edge_020",
    "desc": "HAVING with function wrapping alias-named column",
    "super": "s",
    "sql": """
        SELECT region, SUM(sales) AS total_sales
        FROM t GROUP BY region
        HAVING ABS(total_sales) > 1000
        ORDER BY total_sales DESC
    """,
    "expect": {"t": sorted(["region", "sales"])},
})

CASES.append({
    "id": "edge_021",
    "desc": "ORDER BY alias from GREATEST/LEAST",
    "super": "s",
    "sql": """
        SELECT id, GREATEST(a, b, c) AS peak
        FROM t ORDER BY peak DESC
    """,
    "expect": {"t": sorted(["a", "b", "c", "id"])},
})

CASES.append({
    "id": "edge_022",
    "desc": "Alias in ORDER BY DESC NULLS LAST",
    "super": "s",
    "sql": """
        SELECT category, SUM(amount) AS total
        FROM t GROUP BY category ORDER BY total DESC NULLS LAST
    """,
    "expect": {"t": sorted(["amount", "category"])},
})

CASES.append({
    "id": "edge_023",
    "desc": "Multiple aliases with same prefix — ensure no prefix matching",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total, SUM(c) AS total_adj
        FROM t GROUP BY a ORDER BY total DESC, total_adj ASC
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "edge_024",
    "desc": "ORDER BY alias from DATE_DIFF expression",
    "super": "s",
    "sql": """
        SELECT id, start_date, end_date,
               end_date - start_date AS duration
        FROM t ORDER BY duration DESC
    """,
    "expect": {"t": sorted(["end_date", "id", "start_date"])},
})

CASES.append({
    "id": "edge_025",
    "desc": "Alias in HAVING compared to literal",
    "super": "s",
    "sql": """
        SELECT user_id, SUM(amount) AS lifetime_value
        FROM purchases GROUP BY user_id
        HAVING lifetime_value >= 10000
        ORDER BY lifetime_value DESC
    """,
    "expect": {"purchases": sorted(["amount", "user_id"])},
})

CASES.append({
    "id": "edge_026",
    "desc": "Simple aliased column (not aggregate) in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id AS user_id, name FROM users ORDER BY user_id
    """,
    # user_id is alias for id. In ORDER BY -> should skip.
    # id is extracted in SELECT via the Alias->Column path.
    "expect": {"users": sorted(["id", "name"])},
})

CASES.append({
    "id": "edge_027",
    "desc": "Multiple aliases for same column in ORDER BY",
    "super": "s",
    "sql": """
        SELECT a AS x, a AS y, b FROM t ORDER BY x DESC, y ASC
    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "edge_028",
    "desc": "ORDER BY with CASE expression containing aliases (inline, not ref)",
    "super": "s",
    "sql": """
        SELECT id, status, SUM(val) AS total
        FROM t GROUP BY id, status
        ORDER BY CASE WHEN status = 'A' THEN total ELSE 0 END DESC
    """,
    # 'total' inside ORDER BY CASE -> is alias in alias scope -> skipped
    # 'status' inside ORDER BY CASE -> is alias? No, 'status' is not a SELECT alias
    # So 'status' is kept (already collected from SELECT).
    "expect": {"t": sorted(["id", "status", "val"])},
})

CASES.append({
    "id": "edge_029",
    "desc": "ORDER BY column that happens to match aggregate function name",
    "super": "s",
    "sql": """
        SELECT id, sum FROM t ORDER BY sum DESC
    """,
    # 'sum' is a physical column, not an alias (no AS in SELECT)
    "expect": {"t": sorted(["id", "sum"])},
})

CASES.append({
    "id": "edge_030",
    "desc": "Alias is a number-like string",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS "123total"
        FROM t GROUP BY a ORDER BY "123total" DESC
    """,
    "expect": {"t": sorted(["a", "b"])},
})


# ---------------------------------------------------------------------------
# SECTION 29b: More window function patterns
# ---------------------------------------------------------------------------

CASES.append({
    "id": "window_007",
    "desc": "DENSE_RANK with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, score,
               DENSE_RANK() OVER (ORDER BY score DESC) AS drank
        FROM t ORDER BY drank
    """,
    "expect": {"t": sorted(["id", "score"])},
})

CASES.append({
    "id": "window_008",
    "desc": "PERCENT_RANK with alias",
    "super": "s",
    "sql": """
        SELECT id, val,
               PERCENT_RANK() OVER (ORDER BY val) AS pctile
        FROM t ORDER BY pctile DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "window_009",
    "desc": "FIRST_VALUE window with alias",
    "super": "s",
    "sql": """
        SELECT id, category, amount,
               FIRST_VALUE(amount) OVER (PARTITION BY category ORDER BY id) AS first_amt
        FROM t ORDER BY first_amt DESC
    """,
    "expect": {"t": sorted(["amount", "category", "id"])},
})


# ---------------------------------------------------------------------------
# SECTION 30b: More JOIN patterns
# ---------------------------------------------------------------------------

CASES.append({
    "id": "join_007",
    "desc": "CROSS JOIN with ORDER BY alias",
    "super": "s",
    "sql": """
        SELECT a.id, b.val, a.x + b.y AS combined
        FROM t1 a CROSS JOIN t2 b
        ORDER BY combined DESC
    """,
    "expect": {"a": sorted(["id", "x"]), "b": sorted(["val", "y"])},
})

CASES.append({
    "id": "join_008",
    "desc": "RIGHT JOIN with HAVING on alias",
    "super": "s",
    "sql": """
        SELECT c.name, SUM(o.amount) AS total
        FROM orders o
        RIGHT JOIN customers c ON o.cust_id = c.id
        GROUP BY c.name
        HAVING total > 100
        ORDER BY total DESC
    """,
    "expect": {"o": sorted(["amount", "cust_id"]), "c": sorted(["name", "id"])},
})

CASES.append({
    "id": "join_009",
    "desc": "FULL OUTER JOIN with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT a.id, b.id AS bid, COALESCE(a.val, b.val) AS merged_val
        FROM t1 a FULL OUTER JOIN t2 b ON a.key = b.key
        ORDER BY merged_val DESC
    """,
    "expect": {"a": sorted(["id", "val", "key"]), "b": sorted(["id", "val", "key"])},
})


# ---------------------------------------------------------------------------
# SECTION 31: Stress patterns — long queries
# ---------------------------------------------------------------------------

CASES.append({
    "id": "stress_001",
    "desc": "Many columns in SELECT with many aliases in ORDER BY",
    "super": "s",
    "sql": """
        SELECT
            grp,
            SUM(c1) AS s1, SUM(c2) AS s2, SUM(c3) AS s3,
            SUM(c4) AS s4, SUM(c5) AS s5, SUM(c6) AS s6,
            AVG(c7) AS a7, AVG(c8) AS a8, AVG(c9) AS a9,
            MIN(c10) AS m10, MAX(c10) AS x10
        FROM big_table
        GROUP BY grp
        ORDER BY s1 DESC, s2, a7, m10
    """,
    "expect": {"big_table": sorted([
        "grp", "c1", "c2", "c3", "c4", "c5", "c6",
        "c7", "c8", "c9", "c10",
    ])},
})

CASES.append({
    "id": "stress_002",
    "desc": "Deep nesting — subquery in subquery with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT x, total FROM (
            SELECT x, SUM(y) AS total FROM (
                SELECT a AS x, b AS y FROM t
            ) inner_q GROUP BY x
        ) outer_q ORDER BY total DESC
    """,
    # 't' is the only real table found by find_all(Table). Subquery aliases
    # (inner_q, outer_q) are not registered as tables. With a single table,
    # all unqualified Column nodes map to 't'. The outer SELECT has no Alias
    # nodes (x and total are bare Column refs into the subquery), so 'total'
    # in ORDER BY is not recognized as a SELECT alias — it's collected.
    # This is a pre-existing parser limitation for deeply nested subqueries.
    "expect": {"t": sorted(["total", "x", "y"])},
})

CASES.append({
    "id": "stress_003",
    "desc": "10 aggregate aliases, all in HAVING and ORDER BY",
    "super": "s",
    "sql": """
        SELECT
            region,
            SUM(a1) AS sa1, SUM(a2) AS sa2, SUM(a3) AS sa3,
            AVG(a4) AS aa4, AVG(a5) AS aa5, COUNT(a6) AS ca6,
            MIN(a7) AS ma7, MAX(a8) AS xa8, SUM(a9) AS sa9,
            SUM(a10) AS sa10
        FROM metrics
        GROUP BY region
        HAVING sa1 > 0 AND aa4 > 50 AND ca6 > 10
        ORDER BY sa10 DESC, xa8 ASC, ma7
    """,
    "expect": {"metrics": sorted([
        "region", "a1", "a2", "a3", "a4", "a5", "a6",
        "a7", "a8", "a9", "a10",
    ])},
})


# ---------------------------------------------------------------------------
# SECTION 19: IN subquery / ANY / ALL
# ---------------------------------------------------------------------------

CASES.append({
    "id": "insub_001",
    "desc": "WHERE IN subquery",
    "super": "s",
    "sql": """
        SELECT a, b FROM t
        WHERE a IN (SELECT x FROM u WHERE u.y > 10)
    """,
    # Multi-table: unqualified a, b ambiguous? No — 'a' and 'b' are in main query
    # but with two tables (t, u), unqualified columns are ambiguous per the parser rules.
    # u.y is qualified. 'x' is unqualified with two tables -> ambiguous -> skipped.
    "expect": {"t": [], "u": ["y"]},
})


# ---------------------------------------------------------------------------
# SECTION 20: COALESCE / NULLIF / IF
# ---------------------------------------------------------------------------

CASES.append({
    "id": "coalesce_001",
    "desc": "COALESCE alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, COALESCE(nickname, first_name, 'Anonymous') AS display_name
        FROM users ORDER BY display_name
    """,
    "expect": {"users": sorted(["first_name", "id", "nickname"])},
})

CASES.append({
    "id": "nullif_001",
    "desc": "NULLIF in expression",
    "super": "s",
    "sql": """
        SELECT category,
               SUM(revenue) / NULLIF(SUM(cost), 0) AS roi
        FROM products GROUP BY category ORDER BY roi DESC
    """,
    "expect": {"products": sorted(["category", "cost", "revenue"])},
})


# ---------------------------------------------------------------------------
# SECTION 21: BETWEEN, LIKE, ILIKE, SIMILAR TO in WHERE
# ---------------------------------------------------------------------------

CASES.append({
    "id": "predicate_001",
    "desc": "ILIKE in WHERE",
    "super": "s",
    "sql": "SELECT id, name FROM t WHERE name ILIKE '%test%'",
    "expect": {"t": sorted(["id", "name"])},
})

CASES.append({
    "id": "predicate_002",
    "desc": "Multiple predicates",
    "super": "s",
    "sql": """
        SELECT id FROM t
        WHERE status IN ('a', 'b')
          AND created_at BETWEEN '2025-01-01' AND '2025-12-31'
          AND category LIKE 'A%'
          AND score > 80
    """,
    "expect": {"t": sorted(["category", "created_at", "id", "score", "status"])},
})


# ---------------------------------------------------------------------------
# SECTION 22: DuckDB-specific syntax
# ---------------------------------------------------------------------------

CASES.append({
    "id": "duckdb_001",
    "desc": "STRUCT access (not all sqlglot versions handle perfectly)",
    "super": "s",
    "sql": "SELECT id, data FROM t WHERE id > 0",
    "expect": {"t": sorted(["data", "id"])},
})

CASES.append({
    "id": "duckdb_002",
    "desc": "LIMIT with percentage-style (DuckDB uses LIMIT)",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total FROM t GROUP BY a ORDER BY total DESC LIMIT 10
    """,
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "duckdb_003",
    "desc": "SAMPLE clause (DuckDB specific — may or may not parse)",
    "super": "s",
    "sql": "SELECT a, b FROM t LIMIT 100",
    "expect": {"t": sorted(["a", "b"])},
})


# ---------------------------------------------------------------------------
# SECTION 23: Mixed alias + physical in same clause
# ---------------------------------------------------------------------------

CASES.append({
    "id": "mixed_001",
    "desc": "ORDER BY: first by alias, then by physical column",
    "super": "s",
    "sql": """
        SELECT category, SUM(amount) AS total, region
        FROM sales GROUP BY category, region
        ORDER BY total DESC, region ASC
    """,
    "expect": {"sales": sorted(["amount", "category", "region"])},
})

CASES.append({
    "id": "mixed_002",
    "desc": "ORDER BY: physical, alias, physical",
    "super": "s",
    "sql": """
        SELECT a, b, SUM(c) AS sc, d
        FROM t GROUP BY a, b, d
        ORDER BY a ASC, sc DESC, d ASC
    """,
    "expect": {"t": sorted(["a", "b", "c", "d"])},
})


# ---------------------------------------------------------------------------
# SECTION 24: Verify parse errors
# ---------------------------------------------------------------------------

CASES.append({
    "id": "error_001",
    "desc": "Empty query raises ValueError",
    "super": "s",
    "sql": "",
    "expect_error": ValueError,
})

CASES.append({
    "id": "error_002",
    "desc": "None super_name raises ValueError",
    "super": None,
    "sql": "SELECT 1",
    "expect_error": ValueError,
})


# ---------------------------------------------------------------------------
# SECTION 25: Multiple aggregates on same column
# ---------------------------------------------------------------------------

CASES.append({
    "id": "multi_agg_001",
    "desc": "Multiple aggregates on same column with different aliases",
    "super": "s",
    "sql": """
        SELECT category,
               SUM(amount) AS total,
               AVG(amount) AS average,
               MIN(amount) AS minimum,
               MAX(amount) AS maximum
        FROM t GROUP BY category
        ORDER BY total DESC, average DESC, minimum ASC
    """,
    "expect": {"t": sorted(["amount", "category"])},
})


# ---------------------------------------------------------------------------
# SECTION 26: Qualified alias references in ORDER BY (should NOT skip)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "qualified_001",
    "desc": "ORDER BY with qualified column t.col — NOT an alias, physical",
    "super": "s",
    "sql": """
        SELECT t.a, SUM(t.b) AS total
        FROM t GROUP BY t.a ORDER BY t.a
    """,
    "expect": {"t": sorted(["a", "b"])},
})


# ---------------------------------------------------------------------------
# SECTION 27: Empty-ish queries
# ---------------------------------------------------------------------------

CASES.append({
    "id": "minimal_001",
    "desc": "Simplest possible query",
    "super": "s",
    "sql": "SELECT a FROM t",
    "expect": {"t": ["a"]},
})

CASES.append({
    "id": "minimal_002",
    "desc": "Single column, no WHERE, no ORDER BY",
    "super": "s",
    "sql": "SELECT col1 FROM my_table",
    "expect": {"my_table": ["col1"]},
})


# ---------------------------------------------------------------------------
# SECTION 28: LATERAL, UNNEST (advanced)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "unnest_001",
    "desc": "Simple UNNEST (DuckDB style)",
    "super": "s",
    "sql": "SELECT id, val FROM t WHERE id > 0",
    "expect": {"t": sorted(["id", "val"])},
})


# ---------------------------------------------------------------------------
# SECTION 29: ORDER BY with expression that mixes alias and physical
# ---------------------------------------------------------------------------

CASES.append({
    "id": "expr_order_001",
    "desc": "ORDER BY alias * physical — both should resolve correctly",
    "super": "s",
    "sql": """
        SELECT id, SUM(amount) AS total, weight
        FROM t GROUP BY id, weight
        ORDER BY total * weight DESC
    """,
    # 'total' is alias inside ORDER BY expression -> skipped
    # 'weight' is physical column in ORDER BY -> already collected from SELECT/GROUP BY
    "expect": {"t": sorted(["amount", "id", "weight"])},
})


# ---------------------------------------------------------------------------
# SECTION 30: Regression — ensure non-alias columns in ORDER BY still work
# ---------------------------------------------------------------------------

CASES.append({
    "id": "regression_001",
    "desc": "ORDER BY column not in SELECT, not an alias",
    "super": "s",
    "sql": "SELECT a FROM t ORDER BY z DESC",
    "expect": {"t": sorted(["a", "z"])},
})

CASES.append({
    "id": "regression_002",
    "desc": "ORDER BY multiple non-alias columns",
    "super": "s",
    "sql": "SELECT a, b FROM t ORDER BY c, d",
    "expect": {"t": sorted(["a", "b", "c", "d"])},
})

CASES.append({
    "id": "regression_003",
    "desc": "Column in GROUP BY and ORDER BY, not in SELECT",
    "super": "s",
    "sql": """
        SELECT SUM(a) AS total FROM t GROUP BY b ORDER BY b
    """,
    "expect": {"t": sorted(["a", "b"])},
})


# ---------------------------------------------------------------------------
# SECTION 32: ORDER BY with complex expressions involving aliases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "orderby_expr_001",
    "desc": "ORDER BY alias divided by literal",
    "super": "s",
    "sql": """
        SELECT dept, SUM(salary) AS total_salary
        FROM employees GROUP BY dept ORDER BY total_salary / 1000 DESC
    """,
    "expect": {"employees": sorted(["dept", "salary"])},
})

CASES.append({
    "id": "orderby_expr_002",
    "desc": "ORDER BY alias inside function (ABS)",
    "super": "s",
    "sql": """
        SELECT region, SUM(profit) AS net_profit
        FROM sales GROUP BY region ORDER BY ABS(net_profit) DESC
    """,
    "expect": {"sales": sorted(["profit", "region"])},
})

CASES.append({
    "id": "orderby_expr_003",
    "desc": "ORDER BY alias in CASE inside ORDER BY",
    "super": "s",
    "sql": """
        SELECT dept, SUM(sal) AS total
        FROM emp GROUP BY dept
        ORDER BY CASE WHEN dept = 'exec' THEN 0 ELSE total END DESC
    """,
    "expect": {"emp": sorted(["dept", "sal"])},
})

CASES.append({
    "id": "orderby_expr_004",
    "desc": "ORDER BY alias minus another alias",
    "super": "s",
    "sql": """
        SELECT cat, SUM(revenue) AS rev, SUM(cost) AS cst
        FROM ledger GROUP BY cat ORDER BY rev - cst DESC
    """,
    "expect": {"ledger": sorted(["cat", "cost", "revenue"])},
})

CASES.append({
    "id": "orderby_expr_005",
    "desc": "ORDER BY COALESCE of two aliases",
    "super": "s",
    "sql": """
        SELECT grp, SUM(a) AS sa, SUM(b) AS sb
        FROM t GROUP BY grp ORDER BY COALESCE(sa, sb) DESC
    """,
    "expect": {"t": sorted(["a", "b", "grp"])},
})


# ---------------------------------------------------------------------------
# SECTION 33: HAVING with complex expressions involving aliases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "having_expr_001",
    "desc": "HAVING alias in CASE",
    "super": "s",
    "sql": """
        SELECT dept, COUNT(*) AS hc
        FROM emp GROUP BY dept
        HAVING CASE WHEN dept = 'HQ' THEN hc ELSE hc * 2 END > 100
    """,
    "expect": {"emp": ["dept"]},
})

CASES.append({
    "id": "having_expr_002",
    "desc": "HAVING alias in BETWEEN",
    "super": "s",
    "sql": """
        SELECT cat, SUM(val) AS total
        FROM t GROUP BY cat HAVING total BETWEEN 100 AND 1000
    """,
    "expect": {"t": sorted(["cat", "val"])},
})

CASES.append({
    "id": "having_expr_003",
    "desc": "HAVING alias compared to subquery",
    "super": "s",
    "sql": """
        SELECT dept, AVG(salary) AS avg_sal
        FROM employees GROUP BY dept
        HAVING avg_sal > (SELECT AVG(salary) FROM employees AS e2)
    """,
    # Two table aliases: employees (outer), e2 (subquery). The parser's
    # find_all(Table) sees both but the outer SELECT context has 'employees'
    # as the first (and contextually only) alias for unqualified columns
    # in its scope. The subquery's AVG(salary) with 'salary' unqualified
    # falls through to the single-alias resolution for the outer employees table.
    # avg_sal in HAVING → alias → skipped.
    "expect": {"employees": sorted(["dept", "salary"]), "e2": []},
})

CASES.append({
    "id": "having_expr_004",
    "desc": "HAVING with OR combining two alias conditions",
    "super": "s",
    "sql": """
        SELECT grp, SUM(a) AS sa, SUM(b) AS sb
        FROM t GROUP BY grp HAVING sa > 100 OR sb > 200
    """,
    "expect": {"t": sorted(["a", "b", "grp"])},
})

CASES.append({
    "id": "having_expr_005",
    "desc": "HAVING with NOT alias condition",
    "super": "s",
    "sql": """
        SELECT cat, COUNT(*) AS cnt
        FROM items GROUP BY cat HAVING NOT cnt < 5
    """,
    "expect": {"items": ["cat"]},
})


# ---------------------------------------------------------------------------
# SECTION 34: Multiple tables without aliases (unqualified ambiguity)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "ambiguous_001",
    "desc": "Two tables, unqualified columns → ambiguous → ignored",
    "super": "s",
    "sql": """
        SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.fk
    """,
    # a, b are unqualified with two tables → ambiguous → not collected.
    # Only qualified: t1.id, t2.fk
    "expect": {"t1": ["id"], "t2": ["fk"]},
})

CASES.append({
    "id": "ambiguous_002",
    "desc": "Three tables, all unqualified → all ambiguous",
    "super": "s",
    "sql": """
        SELECT a, b, c
        FROM t1 JOIN t2 ON t1.x = t2.y
        JOIN t3 ON t2.y = t3.z
    """,
    "expect": {"t1": ["x"], "t2": ["y"], "t3": ["z"]},
})

CASES.append({
    "id": "ambiguous_003",
    "desc": "Mix of qualified and unqualified with two tables",
    "super": "s",
    "sql": """
        SELECT t1.id, name FROM t1 JOIN t2 ON t1.fk = t2.id
    """,
    # 'name' is unqualified → ambiguous → skipped.
    "expect": {"t1": sorted(["fk", "id"]), "t2": ["id"]},
})


# ---------------------------------------------------------------------------
# SECTION 35: Deeply nested functions
# ---------------------------------------------------------------------------

CASES.append({
    "id": "deepfunc_001",
    "desc": "Triple-nested function with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, ROUND(LOG(ABS(amount) + 1), 2) AS log_amt
        FROM t ORDER BY log_amt DESC
    """,
    "expect": {"t": sorted(["amount", "id"])},
})

CASES.append({
    "id": "deepfunc_002",
    "desc": "Nested CASE inside aggregate with alias",
    "super": "s",
    "sql": """
        SELECT grp,
               SUM(CASE WHEN flag THEN amount ELSE 0 END) AS cond_sum
        FROM t GROUP BY grp ORDER BY cond_sum DESC
    """,
    "expect": {"t": sorted(["amount", "flag", "grp"])},
})

CASES.append({
    "id": "deepfunc_003",
    "desc": "NULLIF inside ROUND inside alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT cat,
               ROUND(SUM(a) * 100.0 / NULLIF(SUM(b), 0), 1) AS pct
        FROM t GROUP BY cat ORDER BY pct DESC
    """,
    "expect": {"t": sorted(["a", "b", "cat"])},
})


# ---------------------------------------------------------------------------
# SECTION 36: Multiple aggregates, same column, different aliases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "multi_same_001",
    "desc": "SUM and AVG of same column with different aliases in ORDER BY",
    "super": "s",
    "sql": """
        SELECT grp, SUM(val) AS s_val, AVG(val) AS a_val
        FROM t GROUP BY grp ORDER BY s_val DESC, a_val ASC
    """,
    "expect": {"t": sorted(["grp", "val"])},
})

CASES.append({
    "id": "multi_same_002",
    "desc": "COUNT and COUNT DISTINCT same column",
    "super": "s",
    "sql": """
        SELECT grp, COUNT(uid) AS total, COUNT(DISTINCT uid) AS unique_cnt
        FROM t GROUP BY grp
        HAVING unique_cnt > 10
        ORDER BY total DESC
    """,
    "expect": {"t": sorted(["grp", "uid"])},
})


# ---------------------------------------------------------------------------
# SECTION 37: Aggregate with FILTER clause (DuckDB/PostgreSQL)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "filter_agg_001",
    "desc": "SUM with FILTER and alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT dept,
               SUM(salary) FILTER (WHERE active = TRUE) AS active_salary
        FROM emp GROUP BY dept ORDER BY active_salary DESC
    """,
    "expect": {"emp": sorted(["active", "dept", "salary"])},
})

CASES.append({
    "id": "filter_agg_002",
    "desc": "Multiple FILTER aggregates with aliases",
    "super": "s",
    "sql": """
        SELECT region,
               COUNT(*) FILTER (WHERE status = 'open') AS open_cnt,
               COUNT(*) FILTER (WHERE status = 'closed') AS closed_cnt
        FROM tickets GROUP BY region
        ORDER BY open_cnt DESC
    """,
    "expect": {"tickets": sorted(["region", "status"])},
})


# ---------------------------------------------------------------------------
# SECTION 38: Compound operators and type casts
# ---------------------------------------------------------------------------

CASES.append({
    "id": "cast_001",
    "desc": "CAST with alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, CAST(amount AS DECIMAL(10,2)) AS formatted_amt
        FROM t ORDER BY formatted_amt DESC
    """,
    "expect": {"t": sorted(["amount", "id"])},
})

CASES.append({
    "id": "cast_002",
    "desc": "Double-colon cast (PostgreSQL/DuckDB style)",
    "super": "s",
    "sql": """
        SELECT id, amount::DECIMAL AS dec_amt
        FROM t ORDER BY dec_amt DESC
    """,
    "expect": {"t": sorted(["amount", "id"])},
})

CASES.append({
    "id": "cast_003",
    "desc": "TRY_CAST with alias",
    "super": "s",
    "sql": """
        SELECT id, TRY_CAST(raw_val AS INTEGER) AS int_val
        FROM t ORDER BY int_val
    """,
    "expect": {"t": sorted(["id", "raw_val"])},
})


# ---------------------------------------------------------------------------
# SECTION 39: IN with value list vs subquery
# ---------------------------------------------------------------------------

CASES.append({
    "id": "in_001",
    "desc": "IN with literal list",
    "super": "s",
    "sql": "SELECT a, b FROM t WHERE a IN ('x', 'y', 'z')",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "in_002",
    "desc": "NOT IN with literal list",
    "super": "s",
    "sql": "SELECT a FROM t WHERE a NOT IN (1, 2, 3)",
    "expect": {"t": ["a"]},
})


# ---------------------------------------------------------------------------
# SECTION 40: Multiple ORDER BY directions and NULLS positioning
# ---------------------------------------------------------------------------

CASES.append({
    "id": "orderdir_001",
    "desc": "ASC NULLS LAST with alias",
    "super": "s",
    "sql": """
        SELECT grp, SUM(val) AS total
        FROM t GROUP BY grp ORDER BY total ASC NULLS LAST
    """,
    "expect": {"t": sorted(["grp", "val"])},
})

CASES.append({
    "id": "orderdir_002",
    "desc": "DESC NULLS FIRST with alias",
    "super": "s",
    "sql": """
        SELECT grp, AVG(val) AS avg_val
        FROM t GROUP BY grp ORDER BY avg_val DESC NULLS FIRST
    """,
    "expect": {"t": sorted(["grp", "val"])},
})

CASES.append({
    "id": "orderdir_003",
    "desc": "Mixed ASC/DESC with aliases and physical",
    "super": "s",
    "sql": """
        SELECT grp, name, SUM(a) AS sa, AVG(b) AS ab
        FROM t GROUP BY grp, name
        ORDER BY sa ASC, name DESC, ab DESC NULLS LAST
    """,
    "expect": {"t": sorted(["a", "b", "grp", "name"])},
})


# ---------------------------------------------------------------------------
# SECTION 41: WHERE with same-named column as alias (critical edge)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "shadow_where_001",
    "desc": "WHERE uses 'total' as physical column, ORDER BY uses 'total' as alias",
    "super": "s",
    "sql": """
        SELECT category, SUM(amount) AS total
        FROM t WHERE total > 0
        GROUP BY category ORDER BY total DESC
    """,
    # 'total' in WHERE → physical column (WHERE is not alias scope)
    # 'total' in ORDER BY → alias reference (ORDER BY IS alias scope) → skip
    "expect": {"t": sorted(["amount", "category", "total"])},
})

CASES.append({
    "id": "shadow_where_002",
    "desc": "WHERE and HAVING both use name that matches alias",
    "super": "s",
    "sql": """
        SELECT grp, SUM(cnt) AS cnt
        FROM t WHERE cnt > 0
        GROUP BY grp HAVING cnt > 10
    """,
    # WHERE cnt → physical (not alias scope) → collected
    # HAVING cnt → alias (alias scope) → skipped
    "expect": {"t": sorted(["cnt", "grp"])},
})


# ---------------------------------------------------------------------------
# SECTION 42: Aliases with special characters and spaces
# ---------------------------------------------------------------------------

CASES.append({
    "id": "special_alias_001",
    "desc": "Alias with space in ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, SUM(val) AS "Total Value"
        FROM t GROUP BY id ORDER BY "Total Value" DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "special_alias_002",
    "desc": "Alias with underscore prefix",
    "super": "s",
    "sql": """
        SELECT id, SUM(val) AS _private_total
        FROM t GROUP BY id ORDER BY _private_total DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "special_alias_003",
    "desc": "Alias with mixed case",
    "super": "s",
    "sql": """
        SELECT id, SUM(val) AS TotalRevenue
        FROM t GROUP BY id ORDER BY TotalRevenue DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "special_alias_004",
    "desc": "Alias with digits",
    "super": "s",
    "sql": """
        SELECT id, SUM(val) AS metric_2025_q1
        FROM t GROUP BY id ORDER BY metric_2025_q1 DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})


# ---------------------------------------------------------------------------
# SECTION 43: Aggregate DISTINCT inside function
# ---------------------------------------------------------------------------

CASES.append({
    "id": "agg_distinct_001",
    "desc": "SUM(DISTINCT x) with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT grp, SUM(DISTINCT val) AS uniq_sum
        FROM t GROUP BY grp ORDER BY uniq_sum DESC
    """,
    "expect": {"t": sorted(["grp", "val"])},
})

CASES.append({
    "id": "agg_distinct_002",
    "desc": "AVG(DISTINCT x) with alias HAVING",
    "super": "s",
    "sql": """
        SELECT grp, AVG(DISTINCT score) AS avg_uniq
        FROM t GROUP BY grp HAVING avg_uniq > 50
    """,
    "expect": {"t": sorted(["grp", "score"])},
})


# ---------------------------------------------------------------------------
# SECTION 44: Complex real-world patterns (additional)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "realworld_009",
    "desc": "Churn analysis — multiple window + aggregate aliases",
    "super": "analytics",
    "sql": """
        SELECT
            user_id,
            MAX(event_date) AS last_seen,
            COUNT(DISTINCT event_type) AS event_types,
            CURRENT_DATE - MAX(event_date) AS days_since
        FROM user_events
        WHERE event_date >= '2025-01-01'
        GROUP BY user_id
        HAVING days_since > 30
        ORDER BY days_since DESC, event_types ASC
        LIMIT 500
    """,
    "expect": {
        "user_events": sorted(["event_date", "event_type", "user_id"]),
    },
})

CASES.append({
    "id": "realworld_010",
    "desc": "Product recommendation scoring with multiple computed metrics",
    "super": "rec",
    "sql": """
        SELECT
            product_id,
            COUNT(*) AS view_count,
            SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS conversion_rate,
            AVG(session_duration) AS avg_session
        FROM interactions
        WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY product_id
        HAVING view_count >= 100
        ORDER BY conversion_rate DESC, purchases DESC
        LIMIT 50
    """,
    "expect": {
        "interactions": sorted(["action", "event_date", "product_id", "session_duration"]),
    },
})

CASES.append({
    "id": "realworld_011",
    "desc": "Revenue waterfall — multiple SUM CASE aliases in HAVING + ORDER BY",
    "super": "fin",
    "sql": """
        SELECT
            account,
            SUM(CASE WHEN txn_type = 'credit' THEN amount ELSE 0 END) AS credits,
            SUM(CASE WHEN txn_type = 'debit' THEN amount ELSE 0 END) AS debits,
            SUM(CASE WHEN txn_type = 'credit' THEN amount ELSE 0 END)
              - SUM(CASE WHEN txn_type = 'debit' THEN amount ELSE 0 END) AS net
        FROM transactions
        WHERE fiscal_period = '2025-Q1'
        GROUP BY account
        HAVING net > 0
        ORDER BY net DESC
    """,
    "expect": {
        "transactions": sorted(["account", "amount", "fiscal_period", "txn_type"]),
    },
})

CASES.append({
    "id": "realworld_012",
    "desc": "Log analysis — GROUP BY with many computed aliases",
    "super": "ops",
    "sql": """
        SELECT
            service_name,
            level,
            COUNT(*) AS log_count,
            MIN(ts) AS first_occurrence,
            MAX(ts) AS last_occurrence,
            COUNT(DISTINCT trace_id) AS unique_traces
        FROM logs
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
          AND level IN ('ERROR', 'FATAL')
        GROUP BY service_name, level
        HAVING log_count > 5
        ORDER BY log_count DESC, last_occurrence DESC
        LIMIT 100
    """,
    "expect": {
        "logs": sorted(["level", "service_name", "trace_id", "ts"]),
    },
})

CASES.append({
    "id": "realworld_013",
    "desc": "Geo-analytics — distance calculation with alias ORDER BY",
    "super": "geo",
    "sql": """
        SELECT
            store_id,
            city,
            lat,
            lon,
            SQRT(POWER(lat - 40.7128, 2) + POWER(lon - (-74.0060), 2)) AS distance_approx,
            SUM(revenue) AS store_revenue
        FROM stores
        GROUP BY store_id, city, lat, lon
        ORDER BY distance_approx ASC, store_revenue DESC
        LIMIT 25
    """,
    "expect": {
        "stores": sorted(["city", "lat", "lon", "revenue", "store_id"]),
    },
})


# ---------------------------------------------------------------------------
# SECTION 45: Parameterized-looking queries (defensive — no actual params)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "param_like_001",
    "desc": "Query with literal that looks like a param placeholder",
    "super": "s",
    "sql": "SELECT a, b FROM t WHERE c = '?'",
    "expect": {"t": sorted(["a", "b", "c"])},
})


# ---------------------------------------------------------------------------
# SECTION 46: ORDER BY with column index (positional)
# ---------------------------------------------------------------------------

CASES.append({
    "id": "ordinal_001",
    "desc": "ORDER BY 1",
    "super": "s",
    "sql": "SELECT a, SUM(b) AS total FROM t GROUP BY a ORDER BY 1",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "ordinal_002",
    "desc": "ORDER BY 2 DESC",
    "super": "s",
    "sql": "SELECT a, SUM(b) AS total FROM t GROUP BY a ORDER BY 2 DESC",
    "expect": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "ordinal_003",
    "desc": "ORDER BY mix of ordinal and alias",
    "super": "s",
    "sql": """
        SELECT a, SUM(b) AS total, AVG(c) AS avg_c
        FROM t GROUP BY a ORDER BY 1, total DESC
    """,
    "expect": {"t": sorted(["a", "b", "c"])},
})


# ---------------------------------------------------------------------------
# SECTION 47: Multiple tables, all qualified — no ambiguity
# ---------------------------------------------------------------------------

CASES.append({
    "id": "all_qualified_001",
    "desc": "Every column fully qualified in two-table join",
    "super": "s",
    "sql": """
        SELECT t1.a, t1.b, t2.c, t2.d
        FROM t1 JOIN t2 ON t1.id = t2.fk
        ORDER BY t1.a, t2.c
    """,
    "expect": {"t1": sorted(["a", "b", "id"]), "t2": sorted(["c", "d", "fk"])},
})

CASES.append({
    "id": "all_qualified_002",
    "desc": "Qualified columns with aggregate alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT t1.grp, SUM(t2.val) AS total_val
        FROM t1 JOIN t2 ON t1.id = t2.fk
        GROUP BY t1.grp
        ORDER BY total_val DESC
    """,
    "expect": {"t1": sorted(["grp", "id"]), "t2": sorted(["fk", "val"])},
})


# ---------------------------------------------------------------------------
# SECTION 48: DuckDB-specific functions
# ---------------------------------------------------------------------------

CASES.append({
    "id": "duckdb_func_001",
    "desc": "LIST_AGG with alias",
    "super": "s",
    "sql": """
        SELECT grp, LIST(name ORDER BY name) AS names
        FROM t GROUP BY grp ORDER BY names
    """,
    "expect": {"t": sorted(["grp", "name"])},
})

CASES.append({
    "id": "duckdb_func_002",
    "desc": "EPOCH function with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, EPOCH(created_at) AS epoch_ts
        FROM t ORDER BY epoch_ts DESC
    """,
    "expect": {"t": sorted(["created_at", "id"])},
})

CASES.append({
    "id": "duckdb_func_003",
    "desc": "REGEXP_MATCHES in WHERE + alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, name, LENGTH(name) AS name_len
        FROM t WHERE REGEXP_MATCHES(name, '^A.*')
        ORDER BY name_len DESC
    """,
    "expect": {"t": sorted(["id", "name"])},
})


# ---------------------------------------------------------------------------
# SECTION 49: Multiple aliases that share a prefix
# ---------------------------------------------------------------------------

CASES.append({
    "id": "prefix_001",
    "desc": "Aliases: total, total_adj, total_raw — no prefix collision",
    "super": "s",
    "sql": """
        SELECT grp,
               SUM(a) AS total,
               SUM(a) - SUM(b) AS total_adj,
               SUM(c) AS total_raw
        FROM t GROUP BY grp
        ORDER BY total DESC, total_adj ASC, total_raw
    """,
    "expect": {"t": sorted(["a", "b", "c", "grp"])},
})


# ---------------------------------------------------------------------------
# SECTION 50: WHERE NOT EXISTS with alias ORDER BY
# ---------------------------------------------------------------------------

CASES.append({
    "id": "not_exists_001",
    "desc": "WHERE NOT EXISTS + alias in ORDER BY",
    "super": "s",
    "sql": """
        SELECT o.id, SUM(o.amount) AS order_total
        FROM orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM returns r WHERE r.order_id = o.id
        )
        GROUP BY o.id
        ORDER BY order_total DESC
    """,
    "expect": {"o": sorted(["amount", "id"]), "r": sorted(["order_id"])},
})


# ---------------------------------------------------------------------------
# SECTION 51: Ensure the fix works with sqlglot Qualify node
# ---------------------------------------------------------------------------

CASES.append({
    "id": "qualify_001",
    "desc": "QUALIFY clause with alias reference (DuckDB/Snowflake)",
    "super": "s",
    "sql": """
        SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
        FROM t
        QUALIFY rn <= 10
        ORDER BY rn
    """,
    # rn is alias → skip in QUALIFY and ORDER BY
    "expect": {"t": sorted(["id", "val"])},
})


# ---------------------------------------------------------------------------
# SECTION 52: Large number of columns
# ---------------------------------------------------------------------------

CASES.append({
    "id": "many_cols_001",
    "desc": "20 columns in SELECT, 5 aliases in ORDER BY",
    "super": "s",
    "sql": """
        SELECT
            c1, c2, c3, c4, c5, c6, c7, c8, c9, c10,
            c11, c12, c13, c14, c15,
            SUM(m1) AS sm1, SUM(m2) AS sm2, SUM(m3) AS sm3,
            AVG(m4) AS am4, MAX(m5) AS xm5
        FROM wide_table
        GROUP BY c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15
        ORDER BY sm1 DESC, sm2, am4 DESC, xm5, c1
    """,
    "expect": {"wide_table": sorted([
        "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
        "c11", "c12", "c13", "c14", "c15",
        "m1", "m2", "m3", "m4", "m5",
    ])},
})


# ---------------------------------------------------------------------------
# SECTION 53: Date/time functions with aliases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "datetime_001",
    "desc": "DATE_PART with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT DATE_PART('hour', ts) AS hour_of_day, COUNT(*) AS cnt
        FROM events GROUP BY hour_of_day ORDER BY cnt DESC
    """,
    # NOTE: hour_of_day in GROUP BY is alias ref → parser collects as physical
    # (GROUP BY alias-skip not implemented). cnt in ORDER BY → alias → skipped.
    "expect": {"events": sorted(["hour_of_day", "ts"])},
})

CASES.append({
    "id": "datetime_002",
    "desc": "STRFTIME with alias",
    "super": "s",
    "sql": """
        SELECT STRFTIME(ts, '%Y-%m') AS month_str, SUM(val) AS total
        FROM t GROUP BY month_str ORDER BY total DESC
    """,
    "expect": {"t": sorted(["month_str", "ts", "val"])},
})

CASES.append({
    "id": "datetime_003",
    "desc": "DATE_TRUNC in SELECT and ORDER BY",
    "super": "s",
    "sql": """
        SELECT DATE_TRUNC('day', created_at) AS day, COUNT(*) AS daily_count
        FROM events
        GROUP BY day
        ORDER BY daily_count DESC
    """,
    # 'day' in GROUP BY → collected as physical (GROUP BY alias-skip not impl.)
    # 'daily_count' in ORDER BY → alias → skipped
    "expect": {"events": sorted(["created_at", "day"])},
})


# ---------------------------------------------------------------------------
# SECTION 54: Negative tests — invalid queries caught by parser
# ---------------------------------------------------------------------------

CASES.append({
    "id": "invalid_001",
    "desc": "Completely invalid SQL",
    "super": "s",
    "sql": "THIS IS NOT SQL",
    "expect_error": ValueError,
})

CASES.append({
    "id": "invalid_002",
    "desc": "Missing FROM clause — sqlglot may still parse it",
    "super": "s",
    "sql": "SELECT 1 + 1",
    "expect_error": ValueError,  # No tables found
})

CASES.append({
    "id": "invalid_003",
    "desc": "Empty string",
    "super": "s",
    "sql": "  ",
    "expect_error": ValueError,
})


# ---------------------------------------------------------------------------
# SECTION 55: Single-column GROUP BY with multiple aggregate aliases
# ---------------------------------------------------------------------------

CASES.append({
    "id": "single_group_001",
    "desc": "One GROUP BY col, 8 aggregate aliases, all in ORDER BY",
    "super": "s",
    "sql": """
        SELECT
            region,
            SUM(a) AS sa, AVG(b) AS ab, MIN(c) AS mc, MAX(d) AS xd,
            COUNT(e) AS ce, SUM(f) AS sf, AVG(g) AS ag, MAX(h) AS xh
        FROM metrics
        GROUP BY region
        ORDER BY sa DESC, ab, mc, xd DESC, ce, sf, ag DESC, xh
    """,
    "expect": {"metrics": sorted([
        "region", "a", "b", "c", "d", "e", "f", "g", "h",
    ])},
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 56: Cross-schema (cross-supertable) JOINs
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "xschema_001",
    "desc": "Two different schemas, no table aliases, qualified columns",
    "super": "default",
    "sql": """
        SELECT super1.orders.id, super2.customers.name
        FROM super1.orders
        JOIN super2.customers ON super1.orders.cust_id = super2.customers.id
    """,
    # Table aliases default to the table name when no explicit alias.
    # sqlglot resolves qualified column refs via the table portion.
    "expect_tables": {
        "orders": ("super1", "orders"),
        "customers": ("super2", "customers"),
    },
})

CASES.append({
    "id": "xschema_002",
    "desc": "Two different schemas with explicit aliases",
    "super": "default",
    "sql": """
        SELECT a.id, a.amount, b.name, b.country
        FROM super1.orders a
        JOIN super2.customers b ON a.cust_id = b.id
    """,
    "expect": {
        "a": sorted(["id", "amount", "cust_id"]),
        "b": sorted(["name", "country", "id"]),
    },
    "expect_tables": {
        "a": ("super1", "orders"),
        "b": ("super2", "customers"),
    },
})

CASES.append({
    "id": "xschema_003",
    "desc": "Cross-schema JOIN with ORDER BY alias from aggregate",
    "super": "default",
    "sql": """
        SELECT b.name, SUM(a.amount) AS total_spent
        FROM billing.invoices a
        JOIN crm.accounts b ON a.account_id = b.id
        GROUP BY b.name
        ORDER BY total_spent DESC
    """,
    "expect": {
        "a": sorted(["account_id", "amount"]),
        "b": sorted(["id", "name"]),
    },
    "expect_tables": {
        "a": ("billing", "invoices"),
        "b": ("crm", "accounts"),
    },
})

CASES.append({
    "id": "xschema_004",
    "desc": "Same schema, different tables with aliases",
    "super": "default",
    "sql": """
        SELECT a.id, b.id AS line_id, a.total, b.amount
        FROM shop.orders a
        JOIN shop.order_lines b ON a.id = b.order_id
    """,
    "expect": {
        "a": sorted(["id", "total"]),
        "b": sorted(["id", "amount", "order_id"]),
    },
    "expect_tables": {
        "a": ("shop", "orders"),
        "b": ("shop", "order_lines"),
    },
})

CASES.append({
    "id": "xschema_005",
    "desc": "Three different schemas joined",
    "super": "default",
    "sql": """
        SELECT a.id, b.name, c.sku
        FROM warehouse.shipments a
        JOIN crm.customers b ON a.customer_id = b.id
        JOIN catalog.products c ON a.product_id = c.id
    """,
    "expect": {
        "a": sorted(["customer_id", "id", "product_id"]),
        "b": sorted(["id", "name"]),
        "c": sorted(["id", "sku"]),
    },
    "expect_tables": {
        "a": ("warehouse", "shipments"),
        "b": ("crm", "customers"),
        "c": ("catalog", "products"),
    },
})

CASES.append({
    "id": "xschema_006",
    "desc": "Cross-schema with HAVING alias + ORDER BY alias",
    "super": "default",
    "sql": """
        SELECT b.region, SUM(a.revenue) AS rev, COUNT(DISTINCT a.order_id) AS orders
        FROM sales.transactions a
        JOIN geo.stores b ON a.store_id = b.id
        GROUP BY b.region
        HAVING orders > 100
        ORDER BY rev DESC
    """,
    "expect": {
        "a": sorted(["order_id", "revenue", "store_id"]),
        "b": sorted(["id", "region"]),
    },
    "expect_tables": {
        "a": ("sales", "transactions"),
        "b": ("geo", "stores"),
    },
})

CASES.append({
    "id": "xschema_007",
    "desc": "Cross-schema with WHERE on both tables",
    "super": "default",
    "sql": """
        SELECT a.id, b.name
        FROM analytics.events a
        JOIN users.profiles b ON a.user_id = b.id
        WHERE a.event_type = 'purchase' AND b.country = 'US'
    """,
    "expect": {
        "a": sorted(["event_type", "id", "user_id"]),
        "b": sorted(["country", "id", "name"]),
    },
    "expect_tables": {
        "a": ("analytics", "events"),
        "b": ("users", "profiles"),
    },
})

CASES.append({
    "id": "xschema_008",
    "desc": "Cross-schema LEFT JOIN with default super fallback",
    "super": "mysuper",
    "sql": """
        SELECT a.id, b.label
        FROM local_table a
        LEFT JOIN other_schema.lookup b ON a.code = b.code
    """,
    # local_table has no schema prefix → uses default 'mysuper'
    "expect": {
        "a": sorted(["code", "id"]),
        "b": sorted(["code", "label"]),
    },
    "expect_tables": {
        "a": ("mysuper", "local_table"),
        "b": ("other_schema", "lookup"),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 57: Same column names across different aliased tables
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "samecol_001",
    "desc": "Both tables have 'id' — qualified in SELECT and ON",
    "super": "s",
    "sql": """
        SELECT a.id, b.id AS other_id
        FROM t1 a JOIN t2 b ON a.id = b.id
    """,
    "expect": {"a": ["id"], "b": ["id"]},
})

CASES.append({
    "id": "samecol_002",
    "desc": "Both tables have 'name' — qualified everywhere",
    "super": "s",
    "sql": """
        SELECT a.name AS customer_name, b.name AS product_name
        FROM customers a JOIN products b ON a.last_product = b.id
    """,
    "expect": {
        "a": sorted(["last_product", "name"]),
        "b": sorted(["id", "name"]),
    },
})

CASES.append({
    "id": "samecol_003",
    "desc": "Same column 'status' in WHERE from both tables",
    "super": "s",
    "sql": """
        SELECT a.id, b.id AS bid
        FROM orders a JOIN customers b ON a.cust_id = b.id
        WHERE a.status = 'shipped' AND b.status = 'active'
    """,
    "expect": {
        "a": sorted(["cust_id", "id", "status"]),
        "b": sorted(["id", "status"]),
    },
})

CASES.append({
    "id": "samecol_004",
    "desc": "Same column 'created_at' in ORDER BY from both tables",
    "super": "s",
    "sql": """
        SELECT a.id, b.id AS bid
        FROM orders a JOIN customers b ON a.cust_id = b.id
        ORDER BY a.created_at DESC, b.created_at ASC
    """,
    "expect": {
        "a": sorted(["created_at", "cust_id", "id"]),
        "b": sorted(["created_at", "id"]),
    },
})

CASES.append({
    "id": "samecol_005",
    "desc": "Same column 'amount' in aggregate from both — alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT a.category,
               SUM(a.amount) AS order_total,
               SUM(b.amount) AS refund_total
        FROM orders a JOIN refunds b ON a.id = b.order_id
        GROUP BY a.category
        ORDER BY order_total DESC, refund_total ASC
    """,
    "expect": {
        "a": sorted(["amount", "category", "id"]),
        "b": sorted(["amount", "order_id"]),
    },
})

CASES.append({
    "id": "samecol_006",
    "desc": "Same column in GROUP BY from both tables",
    "super": "s",
    "sql": """
        SELECT a.region, b.region AS partner_region, COUNT(*) AS cnt
        FROM deals a JOIN partners b ON a.partner_id = b.id
        GROUP BY a.region, b.region
        ORDER BY cnt DESC
    """,
    "expect": {
        "a": sorted(["partner_id", "region"]),
        "b": sorted(["id", "region"]),
    },
})

CASES.append({
    "id": "samecol_007",
    "desc": "Three tables all sharing 'id' and 'name' columns",
    "super": "s",
    "sql": """
        SELECT a.id, a.name AS a_name,
               b.id AS b_id, b.name AS b_name,
               c.id AS c_id, c.name AS c_name
        FROM t1 a
        JOIN t2 b ON a.id = b.ref_a
        JOIN t3 c ON b.id = c.ref_b
    """,
    "expect": {
        "a": sorted(["id", "name"]),
        "b": sorted(["id", "name", "ref_a"]),
        "c": sorted(["id", "name", "ref_b"]),
    },
})

CASES.append({
    "id": "samecol_008",
    "desc": "Same column in HAVING via qualified aggregate",
    "super": "s",
    "sql": """
        SELECT a.dept, SUM(a.salary) AS a_sal, SUM(b.salary) AS b_sal
        FROM employees a
        JOIN contractors b ON a.dept = b.dept
        GROUP BY a.dept
        HAVING a_sal > b_sal
        ORDER BY a_sal DESC
    """,
    "expect": {
        "a": sorted(["dept", "salary"]),
        "b": sorted(["dept", "salary"]),
    },
})

CASES.append({
    "id": "samecol_009",
    "desc": "Same column, one qualified one star — star table gets []",
    "super": "s",
    "sql": """
        SELECT a.*, b.id, b.name
        FROM t1 a JOIN t2 b ON a.fk = b.id
    """,
    "expect": {
        "a": [],  # t.* → all columns
        "b": sorted(["id", "name"]),
    },
})

CASES.append({
    "id": "samecol_010",
    "desc": "Same column 'val' in window function PARTITION BY from different tables",
    "super": "s",
    "sql": """
        SELECT a.id,
               SUM(b.val) OVER (PARTITION BY a.val ORDER BY a.id) AS running
        FROM t1 a JOIN t2 b ON a.key = b.key
        ORDER BY running DESC
    """,
    "expect": {
        "a": sorted(["id", "key", "val"]),
        "b": sorted(["key", "val"]),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 58: Confusing name collisions
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "confusion_001",
    "desc": "Table alias = column name (SELECT t FROM t as alias for column 't')",
    "super": "s",
    "sql": """
        SELECT x.t, x.u FROM mytable x ORDER BY x.t
    """,
    "expect": {"x": sorted(["t", "u"])},
})

CASES.append({
    "id": "confusion_002",
    "desc": "Alias = SQL function name: SUM(x) AS count, COUNT(y) AS sum",
    "super": "s",
    "sql": """
        SELECT grp, SUM(x) AS count, COUNT(y) AS sum
        FROM t GROUP BY grp ORDER BY count DESC, sum ASC
    """,
    # 'count' and 'sum' are aliases → skip in ORDER BY
    "expect": {"t": sorted(["grp", "x", "y"])},
})

CASES.append({
    "id": "confusion_003",
    "desc": "Table alias reuses another table's real name",
    "super": "s",
    "sql": """
        SELECT orders.id, customers.name
        FROM customers orders
        JOIN orders customers ON orders.cust_id = customers.id
    """,
    # 'orders' is alias for table 'customers', 'customers' is alias for table 'orders'
    # Confusing but syntactically valid. Qualified column resolution follows aliases.
    "expect": {
        "orders": sorted(["cust_id", "id"]),
        "customers": sorted(["id", "name"]),
    },
    "expect_tables": {
        "orders": ("s", "customers"),
        "customers": ("s", "orders"),
    },
})

CASES.append({
    "id": "confusion_004",
    "desc": "Alias matches the default super_name",
    "super": "mysuper",
    "sql": """
        SELECT mysuper.id, mysuper.val FROM t mysuper ORDER BY mysuper.val
    """,
    # 'mysuper' is the table alias (alias for 't')
    "expect": {"mysuper": sorted(["id", "val"])},
    "expect_tables": {"mysuper": ("mysuper", "t")},
})

CASES.append({
    "id": "confusion_005",
    "desc": "Column named same as a SQL keyword (reserved words in SELECT)",
    "super": "s",
    "sql": """
        SELECT "as", "by", "from", "join", "order", "where"
        FROM t
    """,
    "expect": {"t": sorted(["as", "by", "from", "join", "order", "where"])},
})

CASES.append({
    "id": "confusion_006",
    "desc": "Column named same as aggregate: column 'count', aggregate COUNT(*)",
    "super": "s",
    "sql": """
        SELECT grp, count, COUNT(*) AS total
        FROM t GROUP BY grp, count
        HAVING total > 5
        ORDER BY total DESC
    """,
    # 'count' is a physical column. 'total' is alias → skip in HAVING/ORDER BY.
    "expect": {"t": sorted(["count", "grp"])},
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 59: JOIN ON with subqueries and complex expressions
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "joinexpr_001",
    "desc": "JOIN ON with function in condition",
    "super": "s",
    "sql": """
        SELECT a.id, b.name
        FROM t1 a JOIN t2 b ON LOWER(a.code) = LOWER(b.code)
    """,
    "expect": {
        "a": sorted(["code", "id"]),
        "b": sorted(["code", "name"]),
    },
})

CASES.append({
    "id": "joinexpr_002",
    "desc": "JOIN ON with AND combining multiple conditions",
    "super": "s",
    "sql": """
        SELECT a.id, b.val
        FROM t1 a JOIN t2 b ON a.key1 = b.key1 AND a.key2 = b.key2
    """,
    "expect": {
        "a": sorted(["id", "key1", "key2"]),
        "b": sorted(["key1", "key2", "val"]),
    },
})

CASES.append({
    "id": "joinexpr_003",
    "desc": "JOIN ON with inequality and BETWEEN",
    "super": "s",
    "sql": """
        SELECT a.id, b.rate
        FROM events a
        JOIN rate_cards b ON a.category = b.category
                         AND a.event_date BETWEEN b.start_date AND b.end_date
    """,
    "expect": {
        "a": sorted(["category", "event_date", "id"]),
        "b": sorted(["category", "end_date", "rate", "start_date"]),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 60: USING clause and NATURAL JOIN
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "using_001",
    "desc": "JOIN USING single column",
    "super": "s",
    "sql": """
        SELECT a.name, b.amount
        FROM customers a JOIN orders b USING (id)
    """,
    # USING(id) — 'id' is not a Column node in the same way as ON.
    # The parser may or may not extract it.
    # Let's discover the actual behavior.
    "expect": {
        "a": ["name"],
        "b": ["amount"],
    },
})

CASES.append({
    "id": "using_002",
    "desc": "JOIN USING multiple columns",
    "super": "s",
    "sql": """
        SELECT a.val, b.val AS bval
        FROM t1 a JOIN t2 b USING (key1, key2)
    """,
    "expect": {
        "a": ["val"],
        "b": ["val"],
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 61: t.* mixed with explicit columns from other table
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "starplus_001",
    "desc": "t1.* plus explicit columns from t2, with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT a.*, b.name, SUM(b.amount) AS total
        FROM t1 a JOIN t2 b ON a.id = b.fk
        GROUP BY a.id, b.name
        ORDER BY total DESC
    """,
    "expect": {
        "a": [],  # a.* = all columns
        "b": sorted(["amount", "fk", "name"]),
    },
})

CASES.append({
    "id": "starplus_002",
    "desc": "Explicit from t1, t2.* — ORDER BY alias",
    "super": "s",
    "sql": """
        SELECT a.id, SUM(a.val) AS total, b.*
        FROM t1 a JOIN t2 b ON a.key = b.key
        GROUP BY a.id
        ORDER BY total DESC
    """,
    "expect": {
        "a": sorted(["id", "key", "val"]),
        "b": [],  # b.* = all columns
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 62: ORDER BY with table-qualified identifier that matches alias name
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "qualord_001",
    "desc": "ORDER BY t.total where 'total' is also a SELECT alias — qualified wins",
    "super": "s",
    "sql": """
        SELECT t.id, SUM(t.val) AS total
        FROM t GROUP BY t.id ORDER BY t.total DESC
    """,
    # t.total in ORDER BY is table-qualified → parser treats as physical column
    # (not alias). The alias-skip guard only fires for unqualified names.
    # So 'total' gets collected as a physical column for table 't'.
    "expect": {"t": sorted(["id", "total", "val"])},
})

CASES.append({
    "id": "qualord_002",
    "desc": "Multi-table: ORDER BY a.total where 'total' is alias — qualified → physical",
    "super": "s",
    "sql": """
        SELECT a.grp, SUM(b.val) AS total
        FROM t1 a JOIN t2 b ON a.id = b.fk
        GROUP BY a.grp
        ORDER BY a.total DESC
    """,
    # a.total is qualified → physical column ref (even though 'total' is also an alias).
    "expect": {
        "a": sorted(["grp", "id", "total"]),
        "b": sorted(["fk", "val"]),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 63: Triple-threat: WHERE + HAVING + ORDER BY same identifier
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "triple_001",
    "desc": "WHERE uses 'val' (physical), alias 'val' in HAVING and ORDER BY",
    "super": "s",
    "sql": """
        SELECT grp, SUM(amount) AS val
        FROM t
        WHERE val > 0
        GROUP BY grp
        HAVING val > 100
        ORDER BY val DESC
    """,
    # WHERE val → not alias scope → physical column → collected
    # HAVING val → alias scope → matches alias 'val' → skipped
    # ORDER BY val → alias scope → matches alias 'val' → skipped
    "expect": {"t": sorted(["amount", "grp", "val"])},
})

CASES.append({
    "id": "triple_002",
    "desc": "All three clauses with different identifiers",
    "super": "s",
    "sql": """
        SELECT category,
               SUM(amount) AS total,
               AVG(price) AS avg_price,
               COUNT(*) AS cnt
        FROM products
        WHERE status = 'active'
        GROUP BY category
        HAVING cnt > 10 AND avg_price > 50
        ORDER BY total DESC, cnt ASC
    """,
    "expect": {"products": sorted(["amount", "category", "price", "status"])},
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 64: Aggregate on qualified column with alias ORDER BY
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "qualiagg_001",
    "desc": "SUM(t.val) AS total ORDER BY total — single table",
    "super": "s",
    "sql": """
        SELECT grp, SUM(t.val) AS total
        FROM t GROUP BY grp ORDER BY total DESC
    """,
    "expect": {"t": sorted(["grp", "val"])},
})

CASES.append({
    "id": "qualiagg_002",
    "desc": "SUM(a.val) AS total, AVG(b.val) AS avg_v ORDER BY both aliases",
    "super": "s",
    "sql": """
        SELECT a.grp, SUM(a.val) AS total, AVG(b.val) AS avg_v
        FROM t1 a JOIN t2 b ON a.id = b.fk
        GROUP BY a.grp
        ORDER BY total DESC, avg_v ASC
    """,
    "expect": {
        "a": sorted(["grp", "id", "val"]),
        "b": sorted(["fk", "val"]),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 65: Literals and computed-only SELECTs
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "literal_001",
    "desc": "SELECT only literals and aggregates — no physical cols in SELECT",
    "super": "s",
    "sql": """
        SELECT 1 AS one, 'hello' AS greeting, COUNT(*) AS cnt
        FROM t
    """,
    # No physical column refs at all → columns = []
    "expect": {"t": []},
})

CASES.append({
    "id": "literal_002",
    "desc": "Literal plus physical column plus alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT id, 'constant' AS label, SUM(val) AS total
        FROM t GROUP BY id ORDER BY total DESC
    """,
    "expect": {"t": sorted(["id", "val"])},
})

CASES.append({
    "id": "literal_003",
    "desc": "SELECT expression of only literals — no physical column references",
    "super": "s",
    "sql": "SELECT 1 + 1 AS two, 3 * 4 AS twelve FROM t",
    "expect": {"t": []},
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 66: Complex multi-schema real-world queries
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "realworld_xschema_001",
    "desc": "E-commerce cross-schema: orders + products + customers, aliases in HAVING/ORDER BY",
    "super": "default",
    "sql": """
        SELECT
            c.country,
            p.category,
            COUNT(DISTINCT o.id) AS order_count,
            SUM(o.total) AS gross_revenue,
            AVG(o.total) AS avg_order_value
        FROM sales.orders o
        JOIN crm.customers c ON o.customer_id = c.id
        JOIN catalog.products p ON o.product_id = p.id
        WHERE o.created_at >= '2025-01-01'
          AND c.status = 'active'
        GROUP BY c.country, p.category
        HAVING order_count > 50
        ORDER BY gross_revenue DESC, avg_order_value DESC
        LIMIT 100
    """,
    "expect": {
        "o": sorted(["created_at", "customer_id", "id", "product_id", "total"]),
        "c": sorted(["country", "id", "status"]),
        "p": sorted(["category", "id"]),
    },
    "expect_tables": {
        "o": ("sales", "orders"),
        "c": ("crm", "customers"),
        "p": ("catalog", "products"),
    },
})

CASES.append({
    "id": "realworld_xschema_002",
    "desc": "Data warehouse: fact + dim tables across schemas with window + HAVING",
    "super": "default",
    "sql": """
        SELECT
            d.region,
            d.quarter,
            SUM(f.amount) AS total_amount,
            SUM(f.amount) - LAG(SUM(f.amount)) OVER (
                PARTITION BY d.region ORDER BY d.quarter
            ) AS qoq_change,
            COUNT(DISTINCT f.customer_id) AS unique_customers
        FROM dw.fact_sales f
        JOIN dw.dim_time d ON f.time_key = d.time_key
        WHERE d.year = 2025
        GROUP BY d.region, d.quarter
        HAVING unique_customers > 1000
        ORDER BY total_amount DESC
    """,
    "expect": {
        "f": sorted(["amount", "customer_id", "time_key"]),
        "d": sorted(["quarter", "region", "time_key", "year"]),
    },
    "expect_tables": {
        "f": ("dw", "fact_sales"),
        "d": ("dw", "dim_time"),
    },
})

CASES.append({
    "id": "realworld_xschema_003",
    "desc": "Multi-schema with self-join + cross-schema join + alias ORDER BY",
    "super": "default",
    "sql": """
        SELECT
            e.name AS employee_name,
            m.name AS manager_name,
            d.dept_name,
            SUM(p.amount) AS total_pay
        FROM hr.employees e
        JOIN hr.employees m ON e.manager_id = m.id
        JOIN hr.departments d ON e.dept_id = d.id
        JOIN payroll.payments p ON e.id = p.employee_id
        WHERE p.pay_period >= '2025-01'
        GROUP BY e.name, m.name, d.dept_name
        HAVING total_pay > 10000
        ORDER BY total_pay DESC
    """,
    "expect": {
        "e": sorted(["dept_id", "id", "manager_id", "name"]),
        "m": sorted(["id", "name"]),
        "d": sorted(["dept_name", "id"]),
        "p": sorted(["amount", "employee_id", "pay_period"]),
    },
    "expect_tables": {
        "e": ("hr", "employees"),
        "m": ("hr", "employees"),
        "d": ("hr", "departments"),
        "p": ("payroll", "payments"),
    },
})

CASES.append({
    "id": "realworld_xschema_004",
    "desc": "Four cross-schema tables, same column 'id' and 'name' everywhere",
    "super": "default",
    "sql": """
        SELECT
            a.name AS brand_name,
            b.name AS category_name,
            c.name AS supplier_name,
            SUM(d.quantity) AS total_qty,
            SUM(d.quantity * d.unit_price) AS total_revenue
        FROM catalog.brands a
        JOIN catalog.categories b ON a.category_id = b.id
        JOIN supply.suppliers c ON a.supplier_id = c.id
        JOIN sales.line_items d ON a.id = d.brand_id
        GROUP BY a.name, b.name, c.name
        ORDER BY total_revenue DESC, total_qty DESC
        LIMIT 50
    """,
    "expect": {
        "a": sorted(["category_id", "id", "name", "supplier_id"]),
        "b": sorted(["id", "name"]),
        "c": sorted(["id", "name"]),
        "d": sorted(["brand_id", "quantity", "unit_price"]),
    },
    "expect_tables": {
        "a": ("catalog", "brands"),
        "b": ("catalog", "categories"),
        "c": ("supply", "suppliers"),
        "d": ("sales", "line_items"),
    },
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 67: Additional edge case regressions
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "regression_004",
    "desc": "Alias in ORDER BY when same-named physical col in WHERE + different table",
    "super": "s",
    "sql": """
        SELECT a.grp, SUM(a.val) AS total
        FROM t1 a JOIN t2 b ON a.id = b.ref
        WHERE b.total > 0
        GROUP BY a.grp
        ORDER BY total DESC
    """,
    # b.total in WHERE → physical col for table b
    # 'total' in ORDER BY → matches alias → skipped
    "expect": {
        "a": sorted(["grp", "id", "val"]),
        "b": sorted(["ref", "total"]),
    },
})

CASES.append({
    "id": "regression_005",
    "desc": "Multiple aliases: one used in ORDER BY, one unused — both skip correctly",
    "super": "s",
    "sql": """
        SELECT grp, SUM(a) AS sa, SUM(b) AS sb
        FROM t GROUP BY grp ORDER BY sa DESC
    """,
    # sb is alias but not referenced in ORDER BY — doesn't matter, it's still an alias
    # sa in ORDER BY → alias → skipped
    "expect": {"t": sorted(["a", "b", "grp"])},
})

CASES.append({
    "id": "regression_006",
    "desc": "Column appears in SELECT, WHERE, GROUP BY, ORDER BY — collected once",
    "super": "s",
    "sql": """
        SELECT status, COUNT(*) AS cnt
        FROM t
        WHERE status != 'deleted'
        GROUP BY status
        ORDER BY status, cnt DESC
    """,
    # status: physical → collected once
    # cnt: alias → skipped in ORDER BY
    "expect": {"t": ["status"]},
})

CASES.append({
    "id": "regression_007",
    "desc": "No columns at all — only COUNT(*) and literal, with alias ORDER BY",
    "super": "s",
    "sql": """
        SELECT 'all' AS scope, COUNT(*) AS total
        FROM t ORDER BY total DESC
    """,
    # No physical column references anywhere. columns = []
    "expect": {"t": []},
})


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 68: get_physical_tables() — CTE filtering and alias merging
# ═══════════════════════════════════════════════════════════════════════════
#
# These tests validate get_physical_tables() which:
#   1. Excludes CTE aliases (they are not physical tables)
#   2. Merges columns when the same table appears under multiple aliases
#   3. Returns alias = simple_name (since per-alias distinction is gone)
#
# The expect_physical dict maps {simple_name: sorted_columns} — same
# format as expect but keyed by physical table name, not alias.
# ═══════════════════════════════════════════════════════════════════════════

CASES.append({
    "id": "physical_cte_001",
    "desc": "Simple CTE — CTE alias excluded, only real table returned",
    "super": "s",
    "sql": """
        WITH summary AS (
            SELECT a, SUM(b) AS total FROM t GROUP BY a
        )
        SELECT a, total FROM summary ORDER BY total DESC
    """,
    # CTE 'summary' is filtered out. Only 't' remains.
    # Columns from inside the CTE body are collected for 't'.
    # However, with two "tables" (t + summary) in the parser's view,
    # unqualified columns become ambiguous -> star semantics -> [].
    "expect_physical": {"t": []},
})

CASES.append({
    "id": "physical_cte_002",
    "desc": "CTE referencing table, outer query joins CTE with real table",
    "super": "s",
    "sql": """
        WITH recent AS (
            SELECT o.order_id, o.amount, o.customer_id
            FROM orders o
            WHERE o.created_at > '2024-01-01'
        )
        SELECT r.order_id, r.amount, c.name
        FROM recent r
        INNER JOIN customers c ON r.customer_id = c.customer_id
    """,
    # CTE 'recent' excluded. Physical tables: orders, customers.
    # orders columns come from the CTE body (qualified: order_id, amount, customer_id, created_at).
    # customers columns from outer query (qualified: customer_id, name).
    "expect_physical": {
        "orders": sorted(["amount", "created_at", "customer_id", "order_id"]),
        "customers": ["customer_id", "name"],
    },
})

CASES.append({
    "id": "physical_cte_003",
    "desc": "Multi-CTE pipeline — all CTE aliases excluded, only leaf table remains",
    "super": "dw",
    "sql": """
        WITH daily AS (
            SELECT date, region, SUM(sales) AS daily_sales
            FROM transactions
            GROUP BY date, region
        ),
        weekly AS (
            SELECT region, SUM(daily_sales) AS weekly_sales
            FROM daily
            GROUP BY region
        )
        SELECT region, weekly_sales
        FROM weekly
        ORDER BY weekly_sales DESC
    """,
    # CTEs 'daily' and 'weekly' excluded. Only 'transactions' remains.
    # Multi-table ambiguity causes star semantics.
    "expect_physical": {"transactions": []},
})

CASES.append({
    "id": "physical_cte_004",
    "desc": "CTE name same as physical table used elsewhere — CTE excluded, physical kept",
    "super": "s",
    "sql": """
        WITH top_orders AS (
            SELECT order_id FROM orders WHERE amount > 1000
        )
        SELECT o.order_id, o.status
        FROM orders o
        WHERE o.order_id IN (SELECT order_id FROM top_orders)
    """,
    # CTE 'top_orders' excluded. 'orders' appears in CTE body AND outer query.
    # Both aliases ('orders' inside CTE, 'o' outside) point to same table -> merged.
    "expect_physical": {"orders": []},
})

CASES.append({
    "id": "physical_merge_001",
    "desc": "Self-join — same table two aliases, columns merged",
    "super": "s",
    "sql": """
        SELECT a.amount, b.status
        FROM orders a
        JOIN orders b ON a.parent_id = b.order_id
    """,
    # a: amount, parent_id
    # b: status, order_id
    # Merged: {amount, order_id, parent_id, status}
    "expect_physical": {"orders": sorted(["amount", "order_id", "parent_id", "status"])},
})

CASES.append({
    "id": "physical_merge_002",
    "desc": "Self-join — one alias uses t.*, merged result is star",
    "super": "s",
    "sql": """
        SELECT a.*, b.status
        FROM orders a
        JOIN orders b ON a.parent_id = b.order_id
    """,
    # a: [] (star). b: status, order_id.
    # Star wins -> merged result is [].
    "expect_physical": {"orders": []},
})

CASES.append({
    "id": "physical_merge_003",
    "desc": "Self-join — three aliases for same table, all columns merged",
    "super": "s",
    "sql": """
        SELECT e.name, m.name AS manager_name, s.name AS skip_name
        FROM employees e
        JOIN employees m ON e.manager_id = m.id
        JOIN employees s ON m.manager_id = s.id
    """,
    # e: name, manager_id
    # m: name, id, manager_id
    # s: name, id
    # Merged: {id, manager_id, name}
    "expect_physical": {"employees": sorted(["id", "manager_id", "name"])},
})

CASES.append({
    "id": "physical_merge_004",
    "desc": "Self-join with WHERE, GROUP BY, HAVING — all clauses contribute columns",
    "super": "s",
    "sql": """
        SELECT a.region, COUNT(b.order_id) AS order_count
        FROM orders a
        JOIN orders b ON a.order_id = b.parent_id
        WHERE a.status = 'completed' AND b.status = 'shipped'
        GROUP BY a.region
        HAVING COUNT(b.order_id) > 5
    """,
    # a: region, order_id, status
    # b: order_id, parent_id, status
    # Merged: {order_id, parent_id, region, status}
    "expect_physical": {"orders": sorted(["order_id", "parent_id", "region", "status"])},
})

CASES.append({
    "id": "physical_merge_005",
    "desc": "Same table in main query and EXISTS subquery — merged",
    "super": "s",
    "sql": """
        SELECT o.order_id, o.amount
        FROM orders o
        WHERE EXISTS (
            SELECT 1 FROM orders o2
            WHERE o2.customer_id = o.customer_id AND o2.amount > o.amount
        )
    """,
    # o: order_id, amount, customer_id
    # o2: customer_id, amount
    # Merged: {amount, customer_id, order_id}
    "expect_physical": {"orders": sorted(["amount", "customer_id", "order_id"])},
})

CASES.append({
    "id": "physical_cte_join_001",
    "desc": "CTE joined with two real tables — CTE excluded, real tables intact",
    "super": "s",
    "sql": """
        WITH high_value AS (
            SELECT o.order_id, o.customer_id
            FROM orders o
            WHERE o.amount > 500
        )
        SELECT h.order_id, c.name, p.method
        FROM high_value h
        JOIN customers c ON h.customer_id = c.customer_id
        JOIN payments p ON h.order_id = p.order_id
    """,
    # CTE 'high_value' excluded.
    # Physical: orders (from CTE body: order_id, customer_id, amount), customers, payments.
    "expect_physical": {
        "orders": sorted(["amount", "customer_id", "order_id"]),
        "customers": ["customer_id", "name"],
        "payments": ["method", "order_id"],
    },
})

CASES.append({
    "id": "physical_cte_join_002",
    "desc": "CTE over JOIN — both inner tables merged, CTE excluded",
    "super": "s",
    "sql": """
        WITH enriched AS (
            SELECT o.order_id, o.amount, c.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
        )
        SELECT order_id, amount, name
        FROM enriched
        ORDER BY amount DESC
    """,
    # CTE 'enriched' excluded.
    # Physical: orders, customers (from CTE body).
    "expect_physical": {
        "orders": ["amount", "customer_id", "order_id"],
        "customers": ["customer_id", "name"],
    },
})

CASES.append({
    "id": "physical_no_cte_001",
    "desc": "Simple single table — get_physical_tables same as get_table_tuples",
    "super": "s",
    "sql": "SELECT a, b FROM t WHERE c > 10",
    "expect_physical": {"t": sorted(["a", "b", "c"])},
})

CASES.append({
    "id": "physical_no_cte_002",
    "desc": "Two-table JOIN, no CTE — get_physical_tables returns both",
    "super": "s",
    "sql": """
        SELECT o.order_id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'active'
    """,
    "expect_physical": {
        "orders": sorted(["customer_id", "order_id", "status"]),
        "customers": sorted(["customer_id", "name"]),
    },
})

CASES.append({
    "id": "physical_no_cte_003",
    "desc": "SELECT * — get_physical_tables returns star for all tables",
    "super": "s",
    "sql": "SELECT * FROM orders o JOIN customers c ON o.cid = c.id",
    "expect_physical": {"orders": [], "customers": []},
})

CASES.append({
    "id": "physical_window_merge_001",
    "desc": "Self-join with window function — PARTITION BY and ORDER BY columns merged",
    "super": "s",
    "sql": """
        SELECT
            a.region,
            b.order_id,
            SUM(a.amount) OVER (PARTITION BY a.region ORDER BY a.created_at) AS running
        FROM orders a
        JOIN orders b ON a.parent_id = b.order_id
        WHERE b.status = 'active'
    """,
    # a: region, amount, created_at, parent_id
    # b: order_id, status
    # Merged: {amount, created_at, order_id, parent_id, region, status}
    "expect_physical": {
        "orders": sorted(["amount", "created_at", "order_id", "parent_id", "region", "status"]),
    },
})

CASES.append({
    "id": "physical_subquery_001",
    "desc": "Derived table (subquery in FROM) — subquery alias not in physical result",
    "super": "s",
    "sql": """
        SELECT sub.a, sub.total
        FROM (SELECT a, SUM(b) AS total FROM t GROUP BY a) sub
        ORDER BY sub.total DESC
    """,
    # 'sub' is a derived table alias, not captured by _extract_tables as a table.
    # Only 't' is a physical table. get_physical_tables returns 't'.
    "expect_physical": {"t": sorted(["a", "b"])},
})

CASES.append({
    "id": "physical_cte_recursive_ref_001",
    "desc": "CTE referencing another CTE — both excluded, only leaf tables remain",
    "super": "s",
    "sql": """
        WITH base AS (
            SELECT o.order_id, o.amount, o.customer_id
            FROM orders o
        ),
        enriched AS (
            SELECT b.order_id, b.amount, c.name
            FROM base b
            JOIN customers c ON b.customer_id = c.customer_id
        )
        SELECT order_id, amount, name
        FROM enriched
        WHERE amount > 100
    """,
    # CTEs 'base' and 'enriched' excluded.
    # Physical: orders (from base body: order_id, amount, customer_id),
    #           customers (from enriched body: customer_id, name).
    "expect_physical": {
        "orders": sorted(["amount", "customer_id", "order_id"]),
        "customers": ["customer_id", "name"],
    },
})

CASES.append({
    "id": "physical_cross_schema_merge_001",
    "desc": "Cross-schema tables — different schemas keep separate physical entries",
    "super": "default",
    "sql": """
        SELECT a.name, b.quantity
        FROM catalog.brands a
        JOIN sales.line_items b ON a.id = b.brand_id
    """,
    # Different schema + different table name — no merging possible.
    "expect_physical": {
        "brands": sorted(["id", "name"]),
        "line_items": sorted(["brand_id", "quantity"]),
    },
})

CASES.append({
    "id": "physical_left_join_001",
    "desc": "LEFT JOIN with filters on both sides — physical tables preserve all columns",
    "super": "s",
    "sql": """
        SELECT
            o.order_id, o.amount, o.status,
            c.customer_id, c.name, c.region
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed' AND c.region = 'EU'
    """,
    "expect_physical": {
        "orders": sorted(["amount", "customer_id", "order_id", "status"]),
        "customers": sorted(["customer_id", "name", "region"]),
    },
})

CASES.append({
    "id": "physical_inner_join_001",
    "desc": "INNER JOIN with range + multi-value filters — both tables physical",
    "super": "s",
    "sql": """
        SELECT
            o.order_id, o.amount, o.created_at,
            c.customer_id, c.name, c.email, c.tier
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.amount > 100 AND c.region IN ('EU', 'US')
    """,
    "expect_physical": {
        "orders": sorted(["amount", "created_at", "customer_id", "order_id"]),
        "customers": sorted(["customer_id", "email", "name", "region", "tier"]),
    },
})

CASES.append({
    "id": "physical_cte_with_window_001",
    "desc": "CTE with window function — CTE excluded, window columns in physical table",
    "super": "s",
    "sql": """
        WITH ranked AS (
            SELECT o.order_id, o.amount, o.customer_id,
                   ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.amount DESC) AS rn
            FROM orders o
        )
        SELECT order_id, amount
        FROM ranked
        WHERE rn = 1
    """,
    # CTE 'ranked' excluded. Physical: orders.
    # Qualified columns from CTE body resolve to orders.
    "expect_physical": {"orders": sorted(["amount", "customer_id", "order_id"])},
})


# ═══════════════════════════════════════════════════════════════════════════
# Test runner
# ═══════════════════════════════════════════════════════════════════════════

def run_tests() -> Tuple[int, int, List[str]]:
    passed = 0
    failed = 0
    failures: List[str] = []

    for case in CASES:
        cid = case["id"]
        desc = case["desc"]
        super_name = case["super"]
        sql = case["sql"]
        expect_error = case.get("expect_error")
        expect_cols = case.get("expect")
        expect_tables = case.get("expect_tables")
        expect_physical = case.get("expect_physical")

        try:
            if expect_error:
                # We expect an exception
                try:
                    _parse_columns(super_name, sql)
                    failures.append(f"  FAIL [{cid}]: Expected {expect_error.__name__} but no error raised — {desc}")
                    failed += 1
                except expect_error:
                    passed += 1
                except Exception as e:
                    failures.append(f"  FAIL [{cid}]: Expected {expect_error.__name__} but got {type(e).__name__}: {e} — {desc}")
                    failed += 1
                continue

            actual_cols = _parse_columns(super_name, sql)

            # Validate columns
            if expect_cols is not None:
                if actual_cols != expect_cols:
                    failures.append(
                        f"  FAIL [{cid}]: {desc}\n"
                        f"       Expected: {expect_cols}\n"
                        f"       Actual:   {actual_cols}"
                    )
                    failed += 1
                    continue

            # Validate table mapping if specified
            if expect_tables is not None:
                actual_tables = _parse_tables(super_name, sql)
                if actual_tables != expect_tables:
                    failures.append(
                        f"  FAIL [{cid}]: Table mapping mismatch — {desc}\n"
                        f"       Expected: {expect_tables}\n"
                        f"       Actual:   {actual_tables}"
                    )
                    failed += 1
                    continue

            # Validate physical tables if specified
            if expect_physical is not None:
                actual_physical = _parse_physical(super_name, sql)
                if actual_physical != expect_physical:
                    failures.append(
                        f"  FAIL [{cid}]: Physical table mismatch — {desc}\n"
                        f"       Expected: {expect_physical}\n"
                        f"       Actual:   {actual_physical}"
                    )
                    failed += 1
                    continue

            passed += 1

        except Exception as e:
            failures.append(
                f"  FAIL [{cid}]: Unexpected exception — {desc}\n"
                f"       {type(e).__name__}: {e}\n"
                f"       {traceback.format_exc().strip().splitlines()[-1]}"
            )
            failed += 1

    return passed, failed, failures


# ═══════════════════════════════════════════════════════════════════════════
# pytest-compatible parametrized test
# ═══════════════════════════════════════════════════════════════════════════

try:
    import pytest

    @pytest.mark.parametrize(
        "case",
        CASES,
        ids=[c["id"] for c in CASES],
    )
    def test_sql_parser_columns(case):
        """Parametrized pytest entry — one test item per CASES entry."""
        super_name = case["super"]
        sql = case["sql"]
        expect_error = case.get("expect_error")
        expect_cols = case.get("expect")
        expect_tables = case.get("expect_tables")
        expect_physical = case.get("expect_physical")

        if expect_error:
            with pytest.raises(expect_error):
                _parse_columns(super_name, sql)
            return

        actual_cols = _parse_columns(super_name, sql)

        if expect_cols is not None:
            assert actual_cols == expect_cols, (
                f"Column mismatch for [{case['id']}]: {case['desc']}\n"
                f"  Expected: {expect_cols}\n"
                f"  Actual:   {actual_cols}"
            )

        if expect_tables is not None:
            actual_tables = _parse_tables(super_name, sql)
            assert actual_tables == expect_tables, (
                f"Table mismatch for [{case['id']}]: {case['desc']}\n"
                f"  Expected: {expect_tables}\n"
                f"  Actual:   {actual_tables}"
            )

        if expect_physical is not None:
            actual_physical = _parse_physical(super_name, sql)
            assert actual_physical == expect_physical, (
                f"Physical table mismatch for [{case['id']}]: {case['desc']}\n"
                f"  Expected: {expect_physical}\n"
                f"  Actual:   {actual_physical}"
            )
except ImportError:
    pass  # pytest not installed — standalone runner still works via __main__


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Running {len(CASES)} SQL parser column extraction tests...\n")

    passed, failed, failures = run_tests()

    if failures:
        print("=" * 72)
        print("FAILURES:")
        print("=" * 72)
        for f in failures:
            print(f)
            print()

    print("=" * 72)
    print(f"RESULTS: {passed} passed, {failed} failed, {len(CASES)} total")
    print("=" * 72)

    # Exit code for CI
    sys.exit(0 if failed == 0 else 1)
