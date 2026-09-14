from __future__ import annotations

from collections import Counter, defaultdict
from itertools import product
from typing import Any, Callable

from .models import Dataset, QueryCase


def _eq(left: Any, right: Any) -> bool:
    return left is not None and right is not None and left == right


def _ordered(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return sorted(rows, key=lambda row: tuple((value is None, value) for value in row))


def _aggregate(values: list[Any], operation: str) -> Any:
    present = [value for value in values if value is not None]
    if operation == "count_rows":
        return len(values)
    if operation == "count":
        return len(present)
    if not present:
        return None
    if operation == "sum":
        return sum(present)
    if operation == "min":
        return min(present)
    if operation == "max":
        return max(present)
    raise ValueError(operation)


def _in(value: Any, candidates: list[Any]) -> bool | None:
    if not candidates:
        return False
    if value is None:
        return None
    if any(_eq(value, other) for other in candidates):
        return True
    return None if None in candidates else False


def build_cases(data: Dataset) -> list[QueryCase]:
    orders = data["orders"]
    customers = data["customers"]
    items = data["items"]
    numbers = data["numbers"]
    cases: list[QueryCase] = []

    def add(
        case_id: str,
        category: str,
        sql: str,
        columns: tuple[str, ...],
        expected: list[tuple[Any, ...]],
        *,
        ordered: bool = True,
    ) -> None:
        cases.append(QueryCase(
            case_id=f"rel_{case_id}", category=category, sql=sql,
            columns=columns, expected=_ordered(expected) if ordered else expected,
            ordered=ordered,
        ))

    for bound in (12, 24, 50, 100, 180, 240):
        selected = [row for row in orders if row["oid"] <= bound]
        for kind in ("INNER", "LEFT", "RIGHT", "FULL OUTER"):
            pairs = [(o, c) for o in selected for c in customers if _eq(o["cid"], c["cid"])]
            expected = [(o["oid"], c["cid"], c["name"]) for o, c in pairs]
            if kind in ("LEFT", "FULL OUTER"):
                matched = {o["oid"] for o, _ in pairs}
                expected += [(o["oid"], None, None) for o in selected if o["oid"] not in matched]
            if kind in ("RIGHT", "FULL OUTER"):
                matched = {c["cid"] for _, c in pairs}
                expected += [(None, c["cid"], c["name"]) for c in customers if c["cid"] not in matched]
            add(
                f"{kind.lower().replace(' ', '_')}_customers_{bound}", "joins_outer",
                "SELECT o.oid AS oid, c.cid AS customer_id, c.name AS customer_name "
                f"FROM orders o {kind} JOIN customers c ON o.cid = c.cid AND o.oid <= {bound} "
                f"WHERE o.oid <= {bound} OR o.oid IS NULL "
                "ORDER BY oid NULLS LAST, customer_id NULLS LAST",
                ("oid", "customer_id", "customer_name"), expected,
            )

        for kind in ("INNER", "LEFT"):
            expected = []
            for o in selected:
                matches = [c for c in customers if _eq(o["cid"], c["cid"]) and o["region"] == c["region"]]
                expected += [(o["oid"], c["cid"], o["region"]) for c in matches]
                if kind == "LEFT" and not matches:
                    expected.append((o["oid"], None, o["region"]))
            add(
                f"composite_{kind.lower()}_{bound}", "joins_composite",
                "SELECT o.oid AS oid, c.cid AS customer_id, o.region AS region "
                f"FROM orders o {kind} JOIN customers c ON o.cid = c.cid AND o.region = c.region "
                f"WHERE o.oid <= {bound} ORDER BY oid, customer_id NULLS LAST",
                ("oid", "customer_id", "region"), expected,
            )

    relations: tuple[tuple[str, str, Callable[[int, int], bool]], ...] = (
        ("lt", "a.n < b.n", lambda a, b: a < b),
        ("le", "a.n <= b.n", lambda a, b: a <= b),
        ("ne", "a.n <> b.n", lambda a, b: a != b),
        ("band", "b.n BETWEEN a.n - 2 AND a.n + 2", lambda a, b: a - 2 <= b <= a + 2),
    )
    for lower, upper in ((-5, 5), (-10, 0), (0, 10), (-15, 15)):
        for name, predicate, relation in relations:
            expected = [
                (a["n"], b["n"]) for a, b in product(numbers, repeat=2)
                if lower <= a["n"] <= upper and lower <= b["n"] <= upper and relation(a["n"], b["n"])
            ]
            add(
                f"non_equi_{name}_{lower}_{upper}", "joins_non_equi",
                f"SELECT a.n AS left_n, b.n AS right_n FROM numbers a JOIN numbers b ON {predicate} "
                f"WHERE a.n BETWEEN {lower} AND {upper} AND b.n BETWEEN {lower} AND {upper} "
                "ORDER BY left_n, right_n",
                ("left_n", "right_n"), expected,
            )

    for bound in (12, 24, 48, 72, 96):
        selected = [o for o in orders if o["oid"] <= bound]
        expected = [(a["oid"], b["oid"], a["cid"]) for a, b in product(selected, repeat=2)
                    if _eq(a["cid"], b["cid"]) and a["oid"] < b["oid"]]
        add(
            f"self_orders_{bound}", "joins_self",
            "SELECT a.oid AS left_oid, b.oid AS right_oid, a.cid AS cid "
            "FROM orders a JOIN orders b ON a.cid = b.cid AND a.oid < b.oid "
            f"WHERE a.oid <= {bound} AND b.oid <= {bound} ORDER BY left_oid, right_oid",
            ("left_oid", "right_oid", "cid"), expected,
        )

    for bound in (10, 20, 31):
        selected = [n for n in numbers if n["rid"] <= bound]
        pairs = [(a["rid"], b["rid"]) for a, b in product(selected, repeat=2)
                 if a["nullable_n"] == b["nullable_n"]]
        add(
            f"null_safe_self_{bound}", "joins_null_safe",
            "SELECT a.rid AS left_id, b.rid AS right_id FROM numbers a JOIN numbers b "
            "ON a.nullable_n IS NOT DISTINCT FROM b.nullable_n "
            f"WHERE a.rid <= {bound} AND b.rid <= {bound} ORDER BY left_id, right_id",
            ("left_id", "right_id"), pairs,
        )

    for bound in (60, 120, 240):
        for kind in ("INNER", "LEFT"):
            expected = []
            for o in orders:
                if o["oid"] > bound:
                    continue
                matches = [i for i in items if i["oid"] == o["oid"]]
                expected += [(o["oid"], i["iid"], i["units"]) for i in matches]
                if kind == "LEFT" and not matches:
                    expected.append((o["oid"], None, None))
            add(
                f"items_{kind.lower()}_{bound}", "joins_one_to_many",
                "SELECT o.oid AS oid, i.iid AS iid, i.units AS units "
                f"FROM orders o {kind} JOIN items i ON o.oid = i.oid "
                f"WHERE o.oid <= {bound} ORDER BY oid, iid NULLS LAST",
                ("oid", "iid", "units"), expected,
            )
        expected = [(o["oid"], c["cid"], i["iid"]) for o in orders for c in customers for i in items
                    if o["oid"] <= bound and _eq(o["cid"], c["cid"]) and o["oid"] == i["oid"]]
        add(
            f"three_way_{bound}", "joins_multiway",
            "SELECT o.oid AS oid, c.cid AS cid, i.iid AS iid "
            "FROM orders o JOIN customers c ON o.cid = c.cid JOIN items i ON o.oid = i.oid "
            f"WHERE o.oid <= {bound} ORDER BY oid, cid, iid",
            ("oid", "cid", "iid"), expected,
        )

    for bound in (2, 4, 8, 16):
        add(
            f"cross_{bound}", "joins_cross",
            "SELECT c.cid AS cid, n.n AS n FROM customers c CROSS JOIN numbers n "
            f"WHERE c.cid <= {bound} AND n.n BETWEEN -2 AND 2 ORDER BY cid, n",
            ("cid", "n"), [(c["cid"], n["n"]) for c in customers for n in numbers
                            if c["cid"] <= bound and -2 <= n["n"] <= 2],
        )

    for bound in (20, 60, 120, 240):
        for kind in ("SEMI", "ANTI"):
            want_match = kind == "SEMI"
            expected = [(o["oid"], o["cid"]) for o in orders if o["oid"] <= bound
                        and any(_eq(o["cid"], c["cid"]) for c in customers) == want_match]
            add(
                f"{kind.lower()}_customers_{bound}", "joins_semi_anti",
                f"SELECT o.oid AS oid, o.cid AS cid FROM orders o {kind} JOIN customers c ON o.cid = c.cid "
                f"WHERE o.oid <= {bound} ORDER BY oid",
                ("oid", "cid"), expected,
            )
            expected = [(o["oid"],) for o in orders if o["oid"] <= bound
                        and any(i["oid"] == o["oid"] and i["price"] >= 80 for i in items) == want_match]
            add(
                f"{kind.lower()}_expensive_items_{bound}", "joins_semi_anti",
                f"SELECT o.oid AS oid FROM orders o {kind} JOIN items i ON o.oid = i.oid AND i.price >= 80 "
                f"WHERE o.oid <= {bound} ORDER BY oid",
                ("oid",), expected,
            )

    for bound in (20, 80, 240):
        for negated in (False, True):
            label = "not_exists" if negated else "exists"
            keyword = "NOT EXISTS" if negated else "EXISTS"
            expected = [(o["oid"], o["cid"]) for o in orders if o["oid"] <= bound
                        and any(_eq(o["cid"], c["cid"]) and c["active"] for c in customers) != negated]
            add(
                f"{label}_active_customer_{bound}", "subquery_exists",
                f"SELECT o.oid AS oid, o.cid AS cid FROM orders o WHERE o.oid <= {bound} "
                f"AND {keyword} (SELECT 1 FROM customers c WHERE c.cid = o.cid AND c.active) ORDER BY oid",
                ("oid", "cid"), expected,
            )
            expected = [(o["oid"],) for o in orders if o["oid"] <= bound
                        and any(i["oid"] == o["oid"] and i["units"] >= 3 for i in items) != negated]
            add(
                f"{label}_items_{bound}", "subquery_exists",
                f"SELECT o.oid AS oid FROM orders o WHERE o.oid <= {bound} "
                f"AND {keyword} (SELECT 1 FROM items i WHERE i.oid = o.oid AND i.units >= 3) ORDER BY oid",
                ("oid",), expected,
            )

    for group in range(4):
        for remove_nulls in (False, True):
            rhs = [n["nullable_n"] for n in numbers if n["grp"] == group
                   and (not remove_nulls or n["nullable_n"] is not None)]
            suffix = " AND b.nullable_n IS NOT NULL" if remove_nulls else ""
            for negated in (False, True):
                keyword = "NOT IN" if negated else "IN"
                desired = False if negated else True
                expected = [(n["rid"], n["nullable_n"]) for n in numbers
                            if _in(n["nullable_n"], rhs) is desired]
                add(
                    f"membership_g{group}_nonnull{int(remove_nulls)}_neg{int(negated)}", "subquery_membership_nulls",
                    "SELECT a.rid AS rid, a.nullable_n AS nullable_n FROM numbers a "
                    f"WHERE a.nullable_n {keyword} (SELECT b.nullable_n FROM numbers b WHERE b.grp = {group}{suffix}) "
                    "ORDER BY rid",
                    ("rid", "nullable_n"), expected,
                )

    for predicate, rhs_name, rhs in (
        ("b.n > 100", "empty", []),
        ("b.nullable_n IS NULL", "only_null", [None for n in numbers if n["nullable_n"] is None]),
        ("b.n BETWEEN -2 AND 2", "mixed", [n["nullable_n"] for n in numbers if -2 <= n["n"] <= 2]),
    ):
        for negated in (False, True):
            keyword = "NOT IN" if negated else "IN"
            desired = False if negated else True
            add(
                f"membership_{rhs_name}_neg{int(negated)}", "subquery_membership_nulls",
                "SELECT a.rid AS rid, a.nullable_n AS nullable_n FROM numbers a "
                f"WHERE a.nullable_n {keyword} (SELECT b.nullable_n FROM numbers b WHERE {predicate}) ORDER BY rid",
                ("rid", "nullable_n"), [(n["rid"], n["nullable_n"]) for n in numbers
                                         if _in(n["nullable_n"], rhs) is desired],
            )

    aggregate_specs = (
        ("count_rows", "COUNT(*)", "order_count"),
        ("count", "COUNT(o.amount)", "amount_count"),
        ("sum", "SUM(o.amount)", "amount_sum"),
        ("min", "MIN(o.amount)", "amount_min"),
        ("max", "MAX(o.amount)", "amount_max"),
    )
    for bound in (24, 60, 120, 240):
        for operation, aggregate_sql, column in aggregate_specs:
            expected = []
            for c in customers:
                values = [o["amount"] for o in orders if _eq(o["cid"], c["cid"]) and o["oid"] <= bound]
                expected.append((c["cid"], _aggregate(values, operation)))
            add(
                f"correlated_{operation}_{bound}", "subquery_scalar_correlated",
                f"SELECT c.cid AS cid, (SELECT {aggregate_sql} FROM orders o "
                f"WHERE o.cid = c.cid AND o.oid <= {bound}) AS {column} "
                "FROM customers c ORDER BY cid",
                ("cid", column), expected,
            )

    for bound in (5, 10, 15):
        rhs = [n["n"] for n in numbers if n["n"] >= bound]
        expected = [(o["oid"], max(rhs)) for o in orders if o["oid"] <= bound]
        add(
            f"uncorrelated_max_{bound}", "subquery_scalar",
            f"SELECT o.oid AS oid, (SELECT MAX(n.n) FROM numbers n WHERE n.n >= {bound}) AS max_n "
            f"FROM orders o WHERE o.oid <= {bound} ORDER BY oid",
            ("oid", "max_n"), expected,
        )
        add(
            f"scalar_empty_{bound}", "subquery_scalar",
            "SELECT c.cid AS cid, (SELECT n.n FROM numbers n WHERE n.n > 100) AS missing_n "
            f"FROM customers c WHERE c.cid <= {bound} ORDER BY cid",
            ("cid", "missing_n"), [(c["cid"], None) for c in customers if c["cid"] <= bound],
        )

    for bound in (24, 60, 120, 240):
        grouped: dict[Any, list[Any]] = defaultdict(list)
        for o in orders:
            if o["oid"] <= bound:
                grouped[o["cid"]].append(o["amount"])
        expected = [(cid, len(values), _aggregate(values, "sum")) for cid, values in grouped.items()]
        add(
            f"cte_chain_aggregate_{bound}", "cte_chain",
            f"WITH chosen AS (SELECT cid, amount FROM orders WHERE oid <= {bound}), "
            "totals AS (SELECT cid, COUNT(*) AS n, SUM(amount) AS total FROM chosen GROUP BY cid), "
            "finished AS (SELECT cid, n, total FROM totals) "
            "SELECT cid, n, total FROM finished ORDER BY cid NULLS LAST",
            ("cid", "n", "total"), expected,
        )
        selected = [o for o in orders if o["oid"] <= bound]
        expected = [(a["oid"], b["oid"]) for a, b in product(selected, repeat=2)
                    if _eq(a["cid"], b["cid"]) and a["oid"] < b["oid"]]
        add(
            f"cte_repeated_join_{bound}", "cte_repeated",
            f"WITH chosen AS (SELECT oid, cid FROM orders WHERE oid <= {bound}) "
            "SELECT a.oid AS left_oid, b.oid AS right_oid FROM chosen a JOIN chosen b "
            "ON a.cid = b.cid AND a.oid < b.oid ORDER BY left_oid, right_oid",
            ("left_oid", "right_oid"), expected,
        )
        expected = [(o["oid"], o["cid"]) for o in selected if o["cid"] is not None
                    and sum(1 for other in selected if _eq(o["cid"], other["cid"])) >= 2]
        add(
            f"cte_repeated_membership_{bound}", "cte_repeated",
            f"WITH chosen AS (SELECT oid, cid FROM orders WHERE oid <= {bound}), "
            "frequent AS (SELECT cid FROM chosen GROUP BY cid HAVING COUNT(*) >= 2) "
            "SELECT oid, cid FROM chosen WHERE cid IN (SELECT cid FROM frequent) ORDER BY oid",
            ("oid", "cid"), expected,
        )

    operators = ("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")
    for column in ("grp", "nullable_n", "n"):
        for boundary in (-5, 0, 5):
            left = Counter((n[column],) for n in numbers if n["n"] <= boundary)
            right = Counter((n[column],) for n in numbers if n["n"] >= boundary - 4)
            for operator in operators:
                if operator == "UNION":
                    expected = list(left.keys() | right.keys())
                elif operator == "UNION ALL":
                    expected = list((left + right).elements())
                elif operator == "INTERSECT":
                    expected = list(left.keys() & right.keys())
                elif operator == "INTERSECT ALL":
                    expected = list((left & right).elements())
                elif operator == "EXCEPT":
                    expected = list(left.keys() - right.keys())
                else:
                    expected = list((left - right).elements())
                add(
                    f"set_{column}_{boundary}_{operator.lower().replace(' ', '_')}", "set_operations",
                    f"SELECT {column} AS v FROM numbers WHERE n <= {boundary} {operator} "
                    f"SELECT {column} AS v FROM numbers WHERE n >= {boundary - 4} ORDER BY v NULLS LAST",
                    ("v",), expected,
                )

    for bound in (8, 16, 24):
        left = [(n["grp"], n["nullable_n"]) for n in numbers if n["rid"] <= bound]
        right = [(n["grp"], n["nullable_n"]) for n in numbers if n["rid"] >= 8]
        add(
            f"set_two_columns_union_all_{bound}", "set_operations_multicolumn",
            f"SELECT grp, nullable_n FROM numbers WHERE rid <= {bound} UNION ALL "
            "SELECT grp, nullable_n FROM numbers WHERE rid >= 8",
            ("grp", "nullable_n"), left + right, ordered=False,
        )
        add(
            f"set_two_columns_intersect_{bound}", "set_operations_multicolumn",
            f"SELECT grp, nullable_n FROM numbers WHERE rid <= {bound} INTERSECT "
            "SELECT grp, nullable_n FROM numbers WHERE rid >= 8",
            ("grp", "nullable_n"), list(set(left) & set(right)), ordered=False,
        )

    ids = [case.case_id for case in cases]
    sqls = [case.sql for case in cases]
    assert len(ids) == len(set(ids))
    assert len(sqls) == len(set(sqls))
    assert len(cases) >= 130
    assert all(len(case.expected) < 3000 for case in cases)
    return cases
