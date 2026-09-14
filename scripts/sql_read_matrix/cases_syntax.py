from __future__ import annotations

from collections import defaultdict
from datetime import date

from .models import Dataset, QueryCase


def build_cases(data: Dataset) -> list[QueryCase]:
    cases = []
    orders = sorted(data["orders"], key=lambda row: row["oid"])
    selected = [row for row in orders if row["oid"] <= 8]
    pair_rows = [(row["oid"], row["qty"]) for row in selected]
    base = "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid"

    def add(name, category, sql, columns, expected, ordered=True, error_contains=()):
        cases.append(QueryCase(
            case_id=f"syntax_{name}", category=category, sql=sql,
            columns=tuple(columns), expected=list(expected), ordered=ordered,
            error_contains=tuple(error_contains),
        ))

    variants = [
        ("terminal_semicolon", base + ";"),
        ("semicolon_trailing_whitespace", base + ";  \n\t"),
        ("trailing_whitespace", base + "  \n\t"),
        ("mixed_case", "sElEcT OID AS oid, QTY AS qty fRoM orders wHeRe OID <= 8 oRdEr By OID"),
        ("leading_whitespace", " \n\t" + base),
        ("trailing_line_comment", base + " -- result rows end here"),
        ("trailing_block_comment", base + " /* completed statement */"),
        ("leading_line_comment", "-- named-table query\n" + base),
        ("inline_block_comment", "SELECT oid, /* projection boundary */ qty FROM orders WHERE oid <= 8 ORDER BY oid"),
        ("semicolon_then_comment", base + "; -- terminated named-table query\n"),
        ("empty_statement_after_semicolon", base + ";;"),
    ]
    for name, sql in variants:
        add(name, "sql_surface_syntax", sql, ("oid", "qty"), pair_rows)

    literal_cases = [
        ("literal_select", "SELECT 42 AS answer, CAST(NULL AS INTEGER) AS missing, TRUE AS flag",
         ("answer", "missing", "flag"), [(42, None, True)]),
        ("literal_select_semicolon", "SELECT 6 * 7 AS answer;", ("answer",), [(42,)]),
        ("literal_cte", "WITH constants AS (SELECT 6 AS n, 7 AS m) SELECT n * m AS answer FROM constants",
         ("answer",), [(42,)]),
        ("literal_union", "SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n",
         ("n",), [(1,), (2,), (3,)]),
        ("literal_scalar_subquery", "SELECT (SELECT 9) + 1 AS answer", ("answer",), [(10,)]),
        ("literal_cte_declared_columns", "WITH constants(n, label) AS (SELECT 5, 'fixed') SELECT n + 2 AS value, label FROM constants",
         ("value", "label"), [(7, "fixed")]),
    ]
    # Table-free SELECTs are an EXPLICIT capability limit, not a gap to close,
    # so these are classified as expected rejections (STREAD-009).
    #
    # This is the reclassification the issue's acceptance criteria call for —
    # "record a consistent capability error and update the test classification
    # explicitly" — and deliberately NOT the thing they forbid: the expected
    # values below are untouched, and no observed output was substituted for
    # them. They stay on record as what these queries would return IF the
    # capability were ever supported. Flipping the classification changes only
    # whether the refusal counts as a failure.
    #
    # Why it stays refused: the read path exists to read this library's tables,
    # and every protection a row or column has lives in a view the reader
    # builds over a snapshot — a query with no snapshot is outside all of it.
    # Admission requires every FROM/JOIN source to be a named table, but a
    # table-free SELECT has no FROM, so that rule passes vacuously and DuckDB's
    # *scalar* file readers (read_text, read_blob) sit in the projection where
    # nothing else checks them. This error is what blocks them, so supporting
    # table-free SELECTs needs a projection-function guard first.
    literal_refusal = ("reads no table",)
    for name, sql, columns, expected in literal_cases:
        add(name, "sql_literal_source", sql, columns, expected,
            error_contains=literal_refusal)

    limits = [
        ("limit_basic", "LIMIT 3", pair_rows[:3]),
        ("limit_offset", "LIMIT 3 OFFSET 2", pair_rows[2:5]),
        ("offset_before_limit", "OFFSET 2 LIMIT 3", pair_rows[2:5]),
        ("offset_fetch_next", "OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY", pair_rows[2:5]),
        ("fetch_first", "FETCH FIRST 3 ROWS ONLY", pair_rows[:3]),
        ("limit_parenthesized", "LIMIT (3)", pair_rows[:3]),
        ("offset_parenthesized", "LIMIT 3 OFFSET (2)", pair_rows[2:5]),
        ("limit_all", "LIMIT ALL", pair_rows),
        ("limit_zero", "LIMIT 0", []),
        ("offset_past_end", "OFFSET 1000", []),
    ]
    for name, suffix, expected in limits:
        add(name, "sql_limit_syntax", base + " " + suffix, ("oid", "qty"), expected)

    add("nested_subquery_limit", "sql_nested_limit",
        "SELECT oid, qty FROM (SELECT oid, qty FROM orders ORDER BY oid LIMIT 4) AS picked ORDER BY oid DESC",
        ("oid", "qty"), list(reversed(pair_rows[:4])))
    add("nested_cte_limit", "sql_nested_limit",
        "WITH picked AS (SELECT oid, qty FROM orders ORDER BY oid LIMIT 4) SELECT oid, qty FROM picked ORDER BY oid DESC",
        ("oid", "qty"), list(reversed(pair_rows[:4])))
    add("nested_outer_limit", "sql_nested_limit",
        "SELECT oid, qty FROM (SELECT oid, qty FROM orders ORDER BY oid LIMIT 6) AS picked ORDER BY oid DESC LIMIT 2",
        ("oid", "qty"), list(reversed(pair_rows[:6]))[:2])

    add("quoted_alias_spaces", "sql_quoted_identifier",
        'SELECT "source".oid AS "Order Number", "source".qty AS "Units Count" '
        'FROM orders AS "source" WHERE "source".oid <= 8 ORDER BY "Order Number"',
        ("Order Number", "Units Count"), pair_rows)
    add("quoted_alias_escaped_quote", "sql_quoted_identifier",
        'SELECT oid AS "odd""alias", qty AS units FROM orders WHERE oid <= 8 ORDER BY "odd""alias"',
        ('odd"alias', "units"), pair_rows)
    add("quoted_schema_table_columns", "sql_quoted_identifier",
        'SELECT "o"."oid" AS "oid", "o"."qty" AS "qty" FROM "warehouse"."orders" AS "o" '
        'WHERE "o"."oid" <= 8 ORDER BY "o"."oid"',
        ("oid", "qty"), pair_rows)

    grouping_rows = [row for row in orders if row["oid"] <= 24]
    modes = [
        ("sets", "GROUPING SETS ((region, status), (region), ())", [("region", "status"), ("region",), ()]),
        ("rollup", "ROLLUP (region, status)", [("region", "status"), ("region",), ()]),
        ("cube", "CUBE (region, status)", [("region", "status"), ("region",), ("status",), ()]),
    ]
    for name, grouping_sql, included_sets in modes:
        expected = []
        for included in included_sets:
            groups = defaultdict(list)
            for row in grouping_rows:
                groups[tuple(row[column] for column in included)].append(row)
            for key, rows in groups.items():
                values = dict(zip(included, key))
                expected.append((values.get("region"), values.get("status"),
                                 int("region" not in included), int("status" not in included), len(rows)))
        query = (
            "SELECT region, status, GROUPING(region) AS region_total, "
            "GROUPING(status) AS status_total, COUNT(*) AS n "
            f"FROM warehouse.orders WHERE oid <= 24 GROUP BY {grouping_sql}"
        )
        for suffix, limit in (("no_limit", ""), ("explicit_limit", " LIMIT 100")):
            add(f"grouping_{name}_{suffix}", "sql_grouping_limit", query + limit,
                ("region", "status", "region_total", "status_total", "n"), expected, ordered=False)

    add("limit_comment_between_keyword_and_count", "sql_limit_syntax",
        base + " LIMIT /* fixed bound */ 3", ("oid", "qty"), pair_rows[:3])

    add("date_subtraction_alternative", "sql_expression_alternative",
        "SELECT eid, DATE '2024-03-15' - event_date AS result FROM warehouse.events ORDER BY eid",
        ("eid", "result"),
        [(row["eid"], (date(2024, 3, 15) - row["event_date"]).days)
         for row in sorted(data["events"], key=lambda row: row["eid"])])
    add("boolean_is_null_alternative", "sql_expression_alternative",
        "SELECT eid, flag IS NULL AS result FROM warehouse.events ORDER BY eid",
        ("eid", "result"),
        [(row["eid"], row["flag"] is None)
         for row in sorted(data["events"], key=lambda row: row["eid"])])

    if len(cases) != 42:
        raise AssertionError(f"Expected 42 syntax cases, got {len(cases)}")
    if len({case.case_id for case in cases}) != len(cases) or len({case.sql for case in cases}) != len(cases):
        raise AssertionError("Syntax case IDs and SQL inputs must be unique")
    return cases
