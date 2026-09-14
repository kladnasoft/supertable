from __future__ import annotations

import operator

from .models import Dataset, QueryCase


def build_cases(data: Dataset) -> list[QueryCase]:
    cases = []
    orders = data["orders"]
    numbers = data["numbers"]

    def add(name, sql, columns, rows, category="predicates", **kwargs):
        cases.append(QueryCase("scalar_" + name, category, sql, tuple(columns), list(rows), **kwargs))

    comparisons = {"eq": ("=", operator.eq), "ne": ("<>", operator.ne),
                   "lt": ("<", operator.lt), "le": ("<=", operator.le),
                   "gt": (">", operator.gt), "ge": (">=", operator.ge)}
    for col, bounds in (("oid", [-1, 0, 1, 29, 30, 31, 119, 120, 121, 210, 240, 241]),
                        ("amount", [-101, -100, -1, 0, 100, 200, 500, 600, 601]),
                        ("qty", [0, 1, 3, 5, 6])):
        for op_name, (op, fn) in comparisons.items():
            for bound in bounds:
                expected = [(r["oid"], r[col]) for r in orders if r[col] is not None and fn(r[col], bound)]
                add(f"{col}_{op_name}_{str(bound).replace('-', 'm')}",
                    f"SELECT oid, {col} AS tested FROM orders WHERE {col} {op} {bound} ORDER BY oid",
                    ("oid", "tested"), expected, ordered=True)
    for lo, hi in [(-1, 0), (1, 30), (30, 31), (29, 61), (91, 150), (181, 239), (240, 240), (241, 999), (120, 30)]:
        for negate in (False, True):
            add(f"between_{lo}_{hi}_{negate}",
                f"SELECT oid FROM orders WHERE oid {'NOT ' if negate else ''}BETWEEN {lo} AND {hi} ORDER BY oid",
                ("oid",), [(r["oid"],) for r in orders if ((lo <= r["oid"] <= hi) != negate)], ordered=True)
    for i, values in enumerate([(1,), (1, 30, 31), (15, 15, 200), (-1, 0, 241), (29, 30, 31, 59, 60, 61)]):
        for negate in (False, True):
            text = ", ".join(map(str, values))
            add(f"in_{i}_{negate}", f"SELECT oid FROM orders WHERE oid {'NOT ' if negate else ''}IN ({text}) ORDER BY oid",
                ("oid",), [(r["oid"],) for r in orders if ((r["oid"] in values) != negate)], ordered=True)
    for col in ("cid", "amount", "score", "active", "status", "note"):
        for neg in (False, True):
            add(f"null_{col}_{neg}", f"SELECT oid FROM orders WHERE {col} IS {'NOT ' if neg else ''}NULL ORDER BY oid",
                ("oid",), [(r["oid"],) for r in orders if ((r[col] is not None) if neg else (r[col] is None))], ordered=True)
    for bound in (0, 30, 90, 180, 239):
        add(f"reversed_{bound}", f"SELECT oid FROM orders WHERE {bound} < oid ORDER BY oid",
            ("oid",), [(r["oid"],) for r in orders if r["oid"] > bound], ordered=True)
        add(f"and_or_{bound}",
            f"SELECT oid FROM orders WHERE (oid > {bound} AND region = 'eu') OR (amount IS NULL AND qty = 3) ORDER BY oid",
            ("oid",), [(r["oid"],) for r in orders if (r["oid"] > bound and r["region"] == "eu") or (r["amount"] is None and r["qty"] == 3)], ordered=True)
        add(f"not_or_{bound}",
            f"SELECT oid FROM orders WHERE NOT (oid <= {bound} OR qty = 3) ORDER BY oid", ("oid",),
            [(r["oid"],) for r in orders if r["oid"] > bound and r["qty"] != 3], ordered=True)
    truth_cases = [("true", "active IS TRUE", lambda x: x is True),
                   ("false", "active IS FALSE", lambda x: x is False),
                   ("not_true", "active IS NOT TRUE", lambda x: x is not True),
                   ("not_false", "active IS NOT FALSE", lambda x: x is not False),
                   ("bare", "active", lambda x: x is True),
                   ("not", "NOT active", lambda x: x is False)]
    for name, predicate, fn in truth_cases:
        add("bool_" + name, f"SELECT oid FROM orders WHERE {predicate} ORDER BY oid", ("oid",),
            [(r["oid"],) for r in orders if fn(r["active"])], ordered=True, category="boolean")
    for name, predicate, fn in [
        ("in_null", "cid IN (1, 2, NULL)", lambda x: x in (1, 2)),
        ("not_in_null", "cid NOT IN (1, 2, NULL)", lambda x: False),
        ("equal_null", "amount = NULL", lambda x: False),
        ("different_null", "amount <> NULL", lambda x: False),
    ]:
        col = "cid" if "cid" in predicate else "amount"
        add(name, f"SELECT oid FROM orders WHERE {predicate} ORDER BY oid", ("oid",),
            [(r["oid"],) for r in orders if fn(r[col])], ordered=True, category="null_semantics")
    for value in (None, 1, 99):
        sql_value = "NULL" if value is None else str(value)
        for neg in (False, True):
            add(f"distinct_from_{value}_{neg}",
                f"SELECT oid FROM orders WHERE cid IS {'NOT ' if neg else ''}DISTINCT FROM {sql_value} ORDER BY oid",
                ("oid",), [(r["oid"],) for r in orders if ((r["cid"] == value) if neg else (r["cid"] != value))],
                ordered=True, category="null_semantics")
    text_exprs = [
        ("upper", "UPPER(note)", lambda x: None if x is None else x.upper()),
        ("lower", "LOWER(note)", lambda x: None if x is None else x.lower()),
        ("length", "LENGTH(note)", lambda x: None if x is None else len(x)),
        ("left", "LEFT(note, 3)", lambda x: None if x is None else x[:3]),
        ("right", "RIGHT(note, 2)", lambda x: None if x is None else x[-2:]),
        ("substr", "SUBSTRING(note, 2, 3)", lambda x: None if x is None else x[1:4]),
        ("replace", "REPLACE(note, 'a', 'X')", lambda x: None if x is None else x.replace("a", "X")),
        ("coalesce", "COALESCE(note, '<missing>')", lambda x: "<missing>" if x is None else x),
        ("concat", "note || '!'", lambda x: None if x is None else x + "!"),
        ("concat_null", "CONCAT(note, '!')", lambda x: (x or "") + "!"),
    ]
    for name, expr, fn in text_exprs:
        add("text_" + name, f"SELECT oid, {expr} AS result FROM orders WHERE oid <= 16 ORDER BY oid",
            ("oid", "result"), [(r["oid"], fn(r["note"])) for r in orders[:16]], ordered=True, category="strings")
    for name, pred, fn in [
        ("nocase", "note = 'alpha'", lambda x: x is not None and x.lower() == "alpha"),
        ("like", "note LIKE 'A%'", lambda x: x is not None and x.startswith("A")),
        ("ilike", "note ILIKE 'a%'", lambda x: x is not None and x.lower().startswith("a")),
        ("unicode", "note = '東京'", lambda x: x == "東京"),
        ("apostrophe", "note = 'O''Reilly'", lambda x: x == "O'Reilly"),
        ("empty", "note = ''", lambda x: x == ""),
        ("contains", "CONTAINS(note, '_')", lambda x: x is not None and "_" in x),
        ("prefix", "STARTS_WITH(note, 'a_')", lambda x: x is not None and x.startswith("a_")),
        ("suffix", "ENDS_WITH(note, '%')", lambda x: x is not None and x.endswith("%")),
    ]:
        add("text_pred_" + name, f"SELECT oid FROM orders WHERE {pred} ORDER BY oid", ("oid",),
            [(r["oid"],) for r in orders if fn(r["note"])], ordered=True, category="strings")
    for offset, limit in [(0, 1), (0, 10), (1, 1), (15, 7), (239, 10), (240, 5), (500, 3), (0, 0)]:
        add(f"limit_{offset}_{limit}", f"SELECT oid FROM orders ORDER BY oid LIMIT {limit} OFFSET {offset}",
            ("oid",), [(r["oid"],) for r in orders[offset:offset + limit]], ordered=True, category="ordering")
    add("nulls_first", "SELECT oid, amount FROM orders ORDER BY amount ASC NULLS FIRST, oid", ("oid", "amount"),
        [(r["oid"], r["amount"]) for r in sorted(orders, key=lambda r: (r["amount"] is not None, r["amount"] or 0, r["oid"]))],
        ordered=True, category="ordering")
    add("nulls_last_desc", "SELECT oid, amount FROM orders ORDER BY amount DESC NULLS LAST, oid", ("oid", "amount"),
        [(r["oid"], r["amount"]) for r in sorted(orders, key=lambda r: (r["amount"] is None, -(r["amount"] or 0), r["oid"]))],
        ordered=True, category="ordering")
    exprs = [("add", "n + 3", lambda n: n + 3), ("subtract", "n - 7", lambda n: n - 7),
             ("multiply", "n * n", lambda n: n * n), ("divide", "n / 4.0", lambda n: n / 4.0),
             ("absolute", "ABS(n)", abs), ("negate", "-n", lambda n: -n),
             ("power", "POWER(n, 2)", lambda n: float(n * n)),
             ("case", "CASE WHEN n < 0 THEN 'negative' WHEN n = 0 THEN 'zero' ELSE 'positive' END",
              lambda n: "negative" if n < 0 else "zero" if n == 0 else "positive")]
    for name, expr, fn in exprs:
        add("expr_" + name, f"SELECT n, {expr} AS result FROM numbers ORDER BY n", ("n", "result"),
            [(r["n"], fn(r["n"])) for r in numbers], ordered=True, category="scalar_expressions")
    return cases
