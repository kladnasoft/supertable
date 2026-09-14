from __future__ import annotations

from collections import defaultdict
from typing import Any

from .models import Dataset, QueryCase


def build_cases(data: Dataset) -> list[QueryCase]:
    cases: list[QueryCase] = []
    orders = data["orders"]
    ledger = data["ledger"]
    evolving = data["evolving"]
    nulls = data["nulls"]
    numbers = data["numbers"]
    order_columns = tuple(orders[0])
    ledger_columns = ("lid", "value", "revision")
    eu_columns = ("amount", "cid", "oid", "qty", "region", "status")
    eu_orders = [row for row in orders if row["region"] == "eu"]
    amount_orders = [
        row for row in orders if row["amount"] is not None and row["amount"] >= 200
    ]

    def project(rows, columns):
        return [tuple(row[column] for column in columns) for row in rows]

    def sql_sum(values):
        present = [value for value in values if value is not None]
        return sum(present) if present else None

    def add(
        name: str,
        category: str,
        sql: str,
        columns: tuple[str, ...],
        expected: list[tuple[Any, ...]],
        *,
        ordered: bool = True,
        role: str = "superadmin",
        expected_types: dict[str, str] | None = None,
        min_pruned_files: int = 0,
        note: str = "",
    ) -> None:
        cases.append(
            QueryCase(
                case_id=f"controls_{name}",
                category=category,
                sql=sql,
                columns=columns,
                expected=expected,
                ordered=ordered,
                role=role,
                expected_types=expected_types or {},
                min_pruned_files=min_pruned_files,
                note=note,
            )
        )

    def reject(name: str, category: str, sql: str, *messages: str, role="superadmin"):
        cases.append(
            QueryCase(
                case_id=f"controls_{name}",
                category=category,
                sql=sql,
                columns=(),
                expected=[],
                role=role,
                error_contains=tuple(messages),
            )
        )

    ledger_sorted = sorted(ledger, key=lambda row: row["lid"])
    add(
        "ledger_logical_rows",
        "native_tombstones",
        "SELECT lid, value, revision FROM ledger ORDER BY lid",
        ledger_columns,
        project(ledger_sorted, ledger_columns),
    )
    add(
        "ledger_upsert_replacements",
        "native_tombstones",
        "SELECT lid, value, revision FROM ledger WHERE lid IN (3, 9, 18) ORDER BY lid",
        ledger_columns,
        project([row for row in ledger_sorted if row["lid"] in (3, 9, 18)], ledger_columns),
    )
    add(
        "ledger_deleted_ids",
        "native_tombstones",
        "SELECT lid, value FROM ledger WHERE lid IN (5, 11) ORDER BY lid",
        ("lid", "value"),
        [],
    )
    add(
        "ledger_aggregate_uniqueness",
        "native_tombstones",
        "SELECT COUNT(*) AS n, COUNT(DISTINCT lid) AS unique_ids, SUM(value) AS total, "
        "MIN(lid) AS first_id, MAX(lid) AS last_id FROM ledger",
        ("n", "unique_ids", "total", "first_id", "last_id"),
        [
            (
                len(ledger),
                len({row["lid"] for row in ledger}),
                sum(row["value"] for row in ledger),
                min(row["lid"] for row in ledger),
                max(row["lid"] for row in ledger),
            )
        ],
    )
    by_revision: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in ledger:
        by_revision[row["revision"]].append(row)
    revision_totals = [
        (revision, len(rows), sum(row["value"] for row in rows))
        for revision, rows in sorted(by_revision.items())
    ]
    add(
        "ledger_revision_groups",
        "native_tombstones",
        "SELECT revision, COUNT(*) AS n, SUM(value) AS total FROM ledger "
        "GROUP BY revision ORDER BY revision",
        ("revision", "n", "total"),
        revision_totals,
    )
    add(
        "ledger_replacement_predicate",
        "native_tombstones",
        "SELECT lid, value FROM ledger WHERE value > 1000 ORDER BY lid",
        ("lid", "value"),
        project([row for row in ledger_sorted if row["value"] > 1000], ("lid", "value")),
    )
    first_revision = by_revision[1]
    add(
        "ledger_old_revision_hidden",
        "native_tombstones",
        "SELECT COUNT(*) AS n, SUM(value) AS total FROM ledger WHERE revision = 1",
        ("n", "total"),
        [(len(first_revision), sum(row["value"] for row in first_revision))],
    )
    add(
        "ledger_cte_aggregate",
        "native_tombstones",
        "WITH visible AS (SELECT lid, value, revision FROM ledger) "
        "SELECT revision, COUNT(*) AS n, SUM(value) AS total FROM visible "
        "GROUP BY revision ORDER BY revision",
        ("revision", "n", "total"),
        revision_totals,
    )
    ledger_number_join = [
        (row["lid"], row["value"], number["n"])
        for row in ledger_sorted
        for number in numbers
        if row["lid"] == number["n"]
    ]
    add(
        "ledger_join_after_deletes",
        "native_tombstones",
        "SELECT l.lid, l.value, n.n FROM ledger AS l "
        "JOIN numbers AS n ON l.lid = n.n ORDER BY l.lid",
        ("lid", "value", "n"),
        ledger_number_join,
    )
    add(
        "ledger_range_descending",
        "native_tombstones",
        "SELECT lid, value FROM ledger WHERE lid BETWEEN 2 AND 12 ORDER BY lid DESC",
        ("lid", "value"),
        project(
            [row for row in reversed(ledger_sorted) if 2 <= row["lid"] <= 12],
            ("lid", "value"),
        ),
    )

    evolving_sorted = sorted(evolving, key=lambda row: row["eid"])
    add(
        "evolving_star_union_schema",
        "schema_evolution",
        "SELECT * FROM evolving ORDER BY eid",
        ("eid", "value", "extra"),
        project(evolving_sorted, ("eid", "value", "extra")),
        expected_types={"eid": "Int64", "value": "Int64", "extra": "String"},
    )
    add(
        "evolving_new_column_projection",
        "schema_evolution",
        "SELECT extra FROM evolving ORDER BY eid",
        ("extra",),
        project(evolving_sorted, ("extra",)),
        expected_types={"extra": "String"},
    )
    add(
        "evolving_missing_column_is_null",
        "schema_evolution",
        "SELECT eid, value FROM evolving WHERE extra IS NULL ORDER BY eid",
        ("eid", "value"),
        project([row for row in evolving_sorted if row["extra"] is None], ("eid", "value")),
    )
    add(
        "evolving_populated_column",
        "schema_evolution",
        "SELECT eid, extra FROM evolving WHERE extra IS NOT NULL ORDER BY eid",
        ("eid", "extra"),
        project([row for row in evolving_sorted if row["extra"] is not None], ("eid", "extra")),
    )
    add(
        "evolving_coalesce_old_files",
        "schema_evolution",
        "SELECT eid, COALESCE(extra, 'legacy') AS detail FROM evolving ORDER BY eid",
        ("eid", "detail"),
        [(row["eid"], row["extra"] if row["extra"] is not None else "legacy") for row in evolving_sorted],
    )
    add(
        "evolving_null_grouping",
        "schema_evolution",
        "SELECT extra IS NULL AS absent, COUNT(*) AS n FROM evolving "
        "GROUP BY extra IS NULL ORDER BY absent",
        ("absent", "n"),
        [(absent, sum((row["extra"] is None) == absent for row in evolving)) for absent in (False, True)],
    )
    add(
        "evolving_new_column_equality",
        "schema_evolution",
        "SELECT eid, extra FROM evolving WHERE extra = 'extra_5' ORDER BY eid",
        ("eid", "extra"),
        project([row for row in evolving_sorted if row["extra"] == "extra_5"], ("eid", "extra")),
    )

    null_sorted = sorted(nulls, key=lambda row: row["nid"])
    null_types = {"nid": "Int64", "value": "Int64", "label": "String"}
    add(
        "typed_null_star",
        "typed_nulls",
        "SELECT * FROM nulls ORDER BY nid",
        ("nid", "value", "label"),
        project(null_sorted, ("nid", "value", "label")),
        expected_types=null_types,
    )
    add(
        "typed_null_aggregates",
        "typed_nulls",
        "SELECT COUNT(*) AS n, COUNT(value) AS value_n, COUNT(label) AS label_n, "
        "SUM(value) AS total, MIN(value) AS low, MAX(value) AS high, AVG(value) AS mean FROM nulls",
        ("n", "value_n", "label_n", "total", "low", "high", "mean"),
        [(len(nulls), 0, 0, None, None, None, None)],
    )
    add(
        "typed_null_is_null",
        "typed_nulls",
        "SELECT nid FROM nulls WHERE value IS NULL AND label IS NULL ORDER BY nid",
        ("nid",),
        project(null_sorted, ("nid",)),
    )
    add(
        "typed_null_is_not_null",
        "typed_nulls",
        "SELECT nid, value FROM nulls WHERE value IS NOT NULL ORDER BY nid",
        ("nid", "value"),
        [],
        expected_types={"nid": "Int64", "value": "Int64"},
    )
    add(
        "typed_null_coalesce",
        "typed_nulls",
        "SELECT nid, COALESCE(value, 7) AS filled, COALESCE(label, 'unset') AS text "
        "FROM nulls ORDER BY nid",
        ("nid", "filled", "text"),
        [(row["nid"], 7, "unset") for row in null_sorted],
    )
    add(
        "typed_null_distinct",
        "typed_nulls",
        "SELECT DISTINCT value FROM nulls",
        ("value",),
        [(None,)],
        expected_types={"value": "Int64"},
    )
    add(
        "typed_null_comparison_unknown",
        "typed_nulls",
        "SELECT nid FROM nulls WHERE value = NULL ORDER BY nid",
        ("nid",),
        [],
    )

    add(
        "empty_where_false",
        "empty_results",
        "SELECT oid, amount FROM orders WHERE FALSE ORDER BY oid",
        ("oid", "amount"),
        [],
        expected_types={"oid": "Int64", "amount": "Int64"},
    )
    add(
        "empty_outside_file_ranges",
        "empty_results",
        "SELECT oid, amount FROM orders WHERE oid > 1000 ORDER BY oid",
        ("oid", "amount"),
        [],
        expected_types={"oid": "Int64", "amount": "Int64"},
    )
    add(
        "empty_typed_null_projection",
        "empty_results",
        "SELECT value, label FROM nulls WHERE 1 = 0",
        ("value", "label"),
        [],
        expected_types={"value": "Int64", "label": "String"},
    )
    add(
        "empty_limit_zero",
        "empty_results",
        "SELECT oid, region FROM orders LIMIT 0",
        ("oid", "region"),
        [],
        expected_types={"oid": "Int64", "region": "String"},
    )
    add(
        "empty_physical_projection",
        "empty_results",
        "SELECT eid FROM empty_table",
        ("eid",),
        [],
        expected_types={"eid": "Int64"},
        note="An existing table with no physical rows still has a queryable declared schema.",
    )
    add(
        "empty_physical_count",
        "empty_results",
        "SELECT COUNT(*) AS n FROM empty_table",
        ("n",),
        [(0,)],
        note="COUNT over an existing empty table has one result row containing zero.",
    )

    orders_sorted = sorted(orders, key=lambda row: row["oid"])
    add(
        "public_star_hides_system_columns",
        "system_columns",
        "SELECT * FROM orders ORDER BY oid LIMIT 7",
        order_columns,
        project(orders_sorted[:7], order_columns),
    )
    add(
        "qualified_star_pruned_public_schema",
        "system_columns",
        "SELECT o.* FROM orders AS o WHERE o.oid > 210 ORDER BY o.oid",
        order_columns,
        project([row for row in orders_sorted if row["oid"] > 210], order_columns),
        min_pruned_files=1,
        note="Eight ordered 30-row chunks guarantee at least one file outside oid > 210.",
    )
    add(
        "cte_star_hides_system_columns",
        "system_columns",
        "WITH visible AS (SELECT * FROM orders) SELECT * FROM visible ORDER BY oid LIMIT 5",
        order_columns,
        project(orders_sorted[:5], order_columns),
    )
    add(
        "lowercase_explicit_aliases",
        "aliases",
        "SELECT o.oid AS order_id, o.amount AS total FROM orders AS o "
        "WHERE o.oid BETWEEN 8 AND 12 ORDER BY order_id",
        ("order_id", "total"),
        project([row for row in orders_sorted if 8 <= row["oid"] <= 12], ("oid", "amount")),
    )
    add(
        "quoted_table_and_output_aliases",
        "aliases",
        'SELECT "source".oid AS "Order ID", "source".amount AS "Amount Value" '
        'FROM orders AS "source" WHERE "source".oid <= 3 ORDER BY "Order ID"',
        ("Order ID", "Amount Value"),
        project(orders_sorted[:3], ("oid", "amount")),
    )
    add(
        "quoted_reserved_table_alias",
        "aliases",
        'SELECT "order".oid AS "select", "order".qty AS "Count" '
        'FROM orders AS "order" WHERE "order".oid = 7',
        ("select", "Count"),
        project([row for row in orders if row["oid"] == 7], ("oid", "qty")),
    )

    reject("preflight_missing_table", "preflight", "SELECT * FROM matrix_missing_table", "Table not found:")
    reject("preflight_missing_super", "preflight", "SELECT * FROM matrix_missing_super.orders", "SuperTable not found:")
    reject(
        "preflight_missing_join_target",
        "preflight",
        "SELECT o.oid FROM orders AS o JOIN matrix_missing_table AS m ON o.oid = m.oid",
        "Table not found:",
    )
    reject(
        "preflight_missing_cte_source",
        "preflight",
        "WITH missing AS (SELECT oid FROM matrix_missing_table) SELECT oid FROM missing",
        "Table not found:",
    )
    reject("preflight_show_stats_missing", "preflight", "SHOW STATS matrix_missing_table", "Table not found:")

    reject("reject_update", "sql_admission", "UPDATE orders SET qty = 1 WHERE oid = 1", "UPDATE is not permitted on the read path")
    reject("reject_delete", "sql_admission", "DELETE FROM orders WHERE oid = 1", "DELETE is not permitted on the read path")
    reject("reject_insert", "sql_admission", "INSERT INTO orders (oid) VALUES (999)", "INSERT is not permitted on the read path")
    reject("reject_create", "sql_admission", "CREATE TABLE matrix_admission_probe (id INTEGER)", "CREATE is not permitted on the read path")
    reject("reject_drop", "sql_admission", "DROP TABLE matrix_admission_probe", "DROP is not permitted on the read path")
    reject("reject_multiple_statements", "sql_admission", "SELECT oid FROM orders; SELECT cid FROM customers", "only a single statement may be submitted")
    reject("reject_parquet_table_function", "sql_admission", "SELECT * FROM read_parquet('/matrix-no-such-file.parquet')", "only named tables may be queried; table functions")
    reject("reject_range_table_function", "sql_admission", "SELECT * FROM range(4)", "only named tables may be queried; table functions")
    reject("reject_explain_delete", "sql_admission", "EXPLAIN DELETE FROM orders WHERE oid = 1", "EXPLAIN is only supported for SELECT statements")
    reject("reject_show_stats_shape", "sql_admission", "SHOW STATS orders WHERE oid = 1", "SHOW STATS expects a table reference")
    reject("reject_malformed_select", "sql_admission", "SELECT oid FROM orders WHERE (", "could not parse query:")
    reject("reject_describe", "sql_admission", "DESCRIBE orders", "DESCRIBE is not permitted on the read path")

    eu_sorted = sorted(eu_orders, key=lambda row: row["oid"])
    amount_sorted = sorted(amount_orders, key=lambda row: row["oid"])
    add(
        "rbac_eu_filter_column_not_projected",
        "rbac_rows",
        "SELECT oid FROM orders ORDER BY oid",
        ("oid",),
        project(eu_sorted, ("oid",)),
        role="eu_reader",
    )
    add(
        "rbac_eu_star_allowlist",
        "rbac_columns",
        "SELECT * FROM orders ORDER BY oid",
        eu_columns,
        project(eu_sorted, eu_columns),
        role="eu_reader",
    )
    add(
        "rbac_eu_aggregate",
        "rbac_rows",
        "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders",
        ("n", "total"),
        [(len(eu_orders), sql_sum(row["amount"] for row in eu_orders))],
        role="eu_reader",
    )
    add(
        "rbac_eu_query_and_policy_intersection",
        "rbac_rows",
        "SELECT oid, amount FROM orders WHERE amount >= 0 ORDER BY oid",
        ("oid", "amount"),
        project(
            [row for row in eu_sorted if row["amount"] is not None and row["amount"] >= 0],
            ("oid", "amount"),
        ),
        role="eu_reader",
    )
    eu_status_groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in eu_orders:
        eu_status_groups[row["status"]].append(row)
    eu_group_totals = [
        (status, len(rows), sql_sum(row["amount"] for row in rows))
        for status, rows in sorted(eu_status_groups.items(), key=lambda pair: (pair[0] is None, pair[0] or ""))
    ]
    add(
        "rbac_eu_grouped_aggregate",
        "rbac_rows",
        "SELECT status, COUNT(*) AS n, SUM(amount) AS total FROM orders "
        "GROUP BY status ORDER BY status NULLS LAST",
        ("status", "n", "total"),
        eu_group_totals,
        role="eu_reader",
    )
    add(
        "rbac_eu_self_join_aliases",
        "rbac_rows",
        "SELECT a.oid, b.amount FROM orders AS a JOIN orders AS b ON a.oid = b.oid ORDER BY a.oid",
        ("oid", "amount"),
        project(eu_sorted, ("oid", "amount")),
        role="eu_reader",
    )
    add(
        "rbac_eu_cte",
        "rbac_rows",
        "WITH scoped AS (SELECT oid, region, amount FROM orders) "
        "SELECT oid, amount FROM scoped ORDER BY oid",
        ("oid", "amount"),
        project(eu_sorted, ("oid", "amount")),
        role="eu_reader",
    )
    add(
        "rbac_eu_cte_aggregate",
        "rbac_rows",
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) "
        "SELECT region, total FROM totals ORDER BY region",
        ("region", "total"),
        [("eu", sql_sum(row["amount"] for row in eu_orders))],
        role="eu_reader",
    )
    add(
        "rbac_eu_union_branches",
        "rbac_rows",
        "SELECT oid FROM orders WHERE oid <= 90 UNION ALL "
        "SELECT oid FROM orders WHERE oid >= 180 ORDER BY oid",
        ("oid",),
        project([row for row in eu_sorted if row["oid"] <= 90 or row["oid"] >= 180], ("oid",)),
        role="eu_reader",
    )
    add(
        "rbac_amount_filter_column_not_projected",
        "rbac_rows",
        "SELECT oid FROM orders ORDER BY oid",
        ("oid",),
        project(amount_sorted, ("oid",)),
        role="amount_reader",
    )
    add(
        "rbac_amount_aggregate",
        "rbac_rows",
        "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders",
        ("n", "total"),
        [(len(amount_orders), sum(row["amount"] for row in amount_orders))],
        role="amount_reader",
    )
    add(
        "rbac_amount_cte_renamed_column",
        "rbac_rows",
        "WITH allowed AS (SELECT oid, amount AS gross FROM orders) "
        "SELECT oid, gross FROM allowed ORDER BY oid",
        ("oid", "gross"),
        project(amount_sorted, ("oid", "amount")),
        role="amount_reader",
    )
    add(
        "rbac_amount_self_join",
        "rbac_rows",
        "SELECT a.oid, b.amount FROM orders AS a JOIN orders AS b ON a.oid = b.oid ORDER BY a.oid",
        ("oid", "amount"),
        project(amount_sorted, ("oid", "amount")),
        role="amount_reader",
    )

    column_denial = "You don't have permission to columns:"
    for name, sql in (
        ("direct", "SELECT note FROM orders"),
        ("predicate", "SELECT oid FROM orders WHERE note = 'Alpha'"),
        ("ordering", "SELECT oid FROM orders ORDER BY note"),
        ("aggregate", "SELECT SUM(LENGTH(note)) AS total FROM orders"),
        ("cte", "WITH hidden AS (SELECT oid, note FROM orders) SELECT note FROM hidden"),
        ("join", "SELECT a.oid, b.note FROM orders AS a JOIN orders AS b ON a.oid = b.oid"),
    ):
        reject(f"rbac_denied_column_{name}", "rbac_columns", sql, column_denial, role="eu_reader")
    reject(
        "rbac_amount_denied_region_predicate",
        "rbac_columns",
        "SELECT oid FROM orders WHERE region = 'eu'",
        column_denial,
        role="amount_reader",
    )
    table_denial = "You don't have permission to read table 'orders'"
    for name, sql in (
        ("direct", "SELECT oid FROM orders"),
        ("count", "SELECT COUNT(*) AS n FROM orders"),
        ("join", "SELECT c.cid FROM customers AS c JOIN orders AS o ON c.cid = o.cid"),
        ("cte", "WITH hidden AS (SELECT oid FROM orders) SELECT oid FROM hidden"),
        ("union", "SELECT cid FROM customers UNION SELECT cid FROM orders"),
    ):
        reject(f"rbac_denied_table_{name}", "rbac_table_access", sql, table_denial, role="denied_reader")
    reject(
        "rbac_disabled_role",
        "rbac_table_access",
        "SELECT oid FROM orders",
        "Role 'disabled_reader' is disabled.",
        role="disabled_reader",
    )
    reject(
        "rbac_missing_role",
        "rbac_table_access",
        "SELECT oid FROM orders",
        "Invalid or nonexistent role: matrix_missing_role",
        role="matrix_missing_role",
    )

    return cases
