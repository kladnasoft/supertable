# route: supertable.tests.pruning.queries
"""A generated corpus of SQL, built to attack pruning from every direction.

Pruning is only allowed to drop a file that provably holds no matching row, so
every query here has a ground truth: the same query with pruning disabled.
The corpus exists to find the cases where those two disagree.

The generator is deliberately biased toward where bugs actually live:

  * **file boundaries.** A literal landing exactly on the edge between two
    files is the case that separates a correct comparison from an off-by-one or
    a timezone shift. Every time-based family sweeps boundary, boundary±1s,
    boundary±1day, and points inside and outside the span.
  * **literal FORM, not just value.** ``TIMESTAMP '...'``, a bare string,
    ``DATE '...'``, ``CAST``, an ISO ``T`` separator, a trailing ``Z``, a
    fractional part — these take different paths through the parser and reach
    the pruner as different lanes. The same instant written six ways must prune
    identically.
  * **cross-table filters.** A predicate on one table should be able to prune
    files of ANOTHER table through a join edge. That is the hardest thing to
    get right and the easiest to get silently wrong.

Every query is tagged with a family so a failure points at a mechanism rather
than at one string.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

from supertable.tests.pruning import dataset as D

Query = Tuple[str, str, str]        # (id, family, sql)


# --------------------------------------------------------------------------
# Interesting instants: file edges are where pruning decisions flip
# --------------------------------------------------------------------------

def _interesting_times() -> List[datetime]:
    out: List[datetime] = []
    for i in (0, 1, 6, 12, 23):
        lo, hi = D._slice_bounds(i, D.FACT_FILES)
        out += [lo, hi, lo + timedelta(seconds=1), lo - timedelta(seconds=1),
                lo + timedelta(hours=12), hi - timedelta(microseconds=1)]
    span_lo = D.ANCHOR - timedelta(days=D.SPAN_DAYS)
    out += [span_lo - timedelta(days=5), D.ANCHOR + timedelta(days=5),
            span_lo, D.ANCHOR]
    return out


TIMES = _interesting_times()


def _spread(n: int) -> List[str]:
    """``n`` literals spread across the WHOLE span, not clustered at the start.

    ``TIMES`` is grouped by file, so a plain ``TIMES[:n]`` slice only ever
    reaches the first two files — every predicate built from it matches
    essentially the whole table and prunes nothing. That made the join, CTE and
    aggregate families report 0% pruned and look like a pruning bug when the
    real cause was the corpus. Sampling with a stride keeps late boundaries in,
    where pruning actually has something to do.
    """
    step = max(1, len(TIMES) // n)
    picked = TIMES[::step][:n]
    return [t.strftime("%Y-%m-%d %H:%M:%S") for t in picked]


def _forms(dt: datetime) -> List[Tuple[str, str]]:
    """The same instant, written the ways SQL authors actually write it."""
    d = dt.strftime("%Y-%m-%d")
    s = dt.strftime("%Y-%m-%d %H:%M:%S")
    return [
        ("ts_cast", f"TIMESTAMP '{s}'"),
        ("bare_str", f"'{s}'"),
        ("bare_date", f"'{d}'"),
        ("date_cast", f"DATE '{d}'"),
        ("iso_t", f"'{dt.strftime('%Y-%m-%dT%H:%M:%S')}'"),
        ("cast_fn", f"CAST('{s}' AS TIMESTAMP)"),
        ("frac", f"TIMESTAMP '{s}.000000'"),
    ]


# --------------------------------------------------------------------------
# Families
# --------------------------------------------------------------------------

def _time_predicates() -> Iterator[Query]:
    """One column, one operator, one literal form — the atom of pruning."""
    n = 0
    for col in ("event_ts", "event_tstz", "event_date"):
        for dt in TIMES:
            for form_name, lit in _forms(dt):
                # A date column compared to a timestamp literal is a real
                # pattern and a distinct lane; keep it.
                for op in (">=", ">", "<=", "<", "="):
                    n += 1
                    yield (f"time_{col}_{form_name}_{op}_{n}", "time_predicate",
                           f"SELECT count(*) AS n, sum(qty) AS q FROM facts "
                           f"WHERE {col} {op} {lit}")


def _time_ranges() -> Iterator[Query]:
    n = 0
    for col in ("event_ts", "event_tstz", "event_date"):
        for i in range(0, len(TIMES) - 1, 2):
            lo, hi = sorted((TIMES[i], TIMES[i + 1]))
            ls, hs = lo.strftime("%Y-%m-%d %H:%M:%S"), hi.strftime("%Y-%m-%d %H:%M:%S")
            n += 1
            yield (f"range_between_{col}_{n}", "time_range",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE {col} BETWEEN TIMESTAMP '{ls}' AND TIMESTAMP '{hs}'")
            n += 1
            yield (f"range_and_{col}_{n}", "time_range",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE {col} >= '{ls}' AND {col} < '{hs}'")
            n += 1
            yield (f"range_or_{col}_{n}", "time_range",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE {col} < '{ls}' OR {col} > '{hs}'")
            n += 1
            yield (f"range_not_{col}_{n}", "time_range",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE NOT ({col} BETWEEN '{ls}' AND '{hs}')")


def _numeric_and_string() -> Iterator[Query]:
    n = 0
    for col, vals in (("fact_id", (0, 1, 4000, 47999, 95999, 96000, -5)),
                      ("qty", (1, 20, 39, 40, 0)),
                      ("amount", (0.0, 50.5, 200.0, 100000.0)),
                      ("cust_id", (0, 2999, 5999, 6000))):
        for v in vals:
            for op in (">=", ">", "<=", "<", "=", "!="):
                n += 1
                yield (f"num_{col}_{op}_{n}", "numeric",
                       f"SELECT count(*) AS n FROM facts WHERE {col} {op} {v}")
    for col, vals in (("status", D.STATUSES + ["nope"]),
                      ("region", D.COUNTRIES + ["XX"])):
        for v in vals:
            n += 1
            yield (f"str_eq_{col}_{n}", "string",
                   f"SELECT count(*) AS n FROM facts WHERE {col} = '{v}'")
            n += 1
            yield (f"str_ne_{col}_{n}", "string",
                   f"SELECT count(*) AS n FROM facts WHERE {col} <> '{v}'")
    for pat in ("pa%", "%id", "%e%", "z%", "____"):
        n += 1
        yield (f"str_like_{n}", "string",
               f"SELECT count(*) AS n FROM facts WHERE status LIKE '{pat}'")
        n += 1
        yield (f"str_notlike_{n}", "string",
               f"SELECT count(*) AS n FROM facts WHERE status NOT LIKE '{pat}'")


def _null_semantics() -> Iterator[Query]:
    """`score` is all-null in some files, partly null in others."""
    n = 0
    for expr in ("score IS NULL", "score IS NOT NULL",
                 "score > 50", "score <= 50", "score IS NULL OR score > 60",
                 "COALESCE(score, -1) < 0", "score BETWEEN 40 AND 60",
                 "NOT (score > 50)"):
        n += 1
        yield (f"null_{n}", "null_semantics",
               f"SELECT count(*) AS n, sum(qty) AS q FROM facts WHERE {expr}")
        n += 1
        yield (f"null_ts_{n}", "null_semantics",
               f"SELECT count(*) AS n FROM facts "
               f"WHERE ({expr}) AND event_ts >= '2025-06-01'")


def _in_lists() -> Iterator[Query]:
    n = 0
    for vals in (("paid",), ("paid", "open"), tuple(D.STATUSES), ("nope",)):
        lst = ", ".join(f"'{v}'" for v in vals)
        n += 1
        yield (f"in_status_{n}", "in_list",
               f"SELECT count(*) AS n FROM facts WHERE status IN ({lst})")
        n += 1
        yield (f"notin_status_{n}", "in_list",
               f"SELECT count(*) AS n FROM facts WHERE status NOT IN ({lst})")
    for ids in ((1,), (1, 2, 3), tuple(range(0, 96000, 4000)), (999999,)):
        lst = ", ".join(str(v) for v in ids)
        n += 1
        yield (f"in_id_{n}", "in_list",
               f"SELECT count(*) AS n FROM facts WHERE fact_id IN ({lst})")
    for dt in TIMES[:8]:
        s = dt.strftime("%Y-%m-%d %H:%M:%S")
        n += 1
        yield (f"in_ts_{n}", "in_list",
               f"SELECT count(*) AS n FROM facts "
               f"WHERE event_ts IN (TIMESTAMP '{s}', TIMESTAMP '{s}')")


def _compound() -> Iterator[Query]:
    n = 0
    times = _spread(10)
    for a in times[:5]:
        for b in times[5:]:
            n += 1
            yield (f"cmp_and_{n}", "compound",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE event_ts >= '{a}' AND status = 'paid' AND qty > 10")
            n += 1
            yield (f"cmp_or_{n}", "compound",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE event_ts >= '{a}' OR event_ts < '{b}'")
            n += 1
            yield (f"cmp_mixed_{n}", "compound",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE (event_ts >= '{a}' AND region IN ('DE','AT')) "
                   f"OR (event_ts < '{b}' AND qty > 30)")
            n += 1
            yield (f"cmp_not_{n}", "compound",
                   f"SELECT count(*) AS n FROM facts "
                   f"WHERE NOT (event_ts < '{a}') AND NOT (status = 'open')")


def _functions_in_predicate() -> Iterator[Query]:
    """Function-wrapped columns must NOT be pruned on — a classic unsound case."""
    n = 0
    for expr in ("EXTRACT(year FROM event_ts) = 2025",
                 "EXTRACT(month FROM event_ts) IN (1, 2)",
                 "date_trunc('month', event_ts) >= '2025-06-01'",
                 "CAST(event_ts AS DATE) >= DATE '2025-06-01'",
                 "event_ts + INTERVAL 1 DAY >= '2025-06-01'",
                 "event_ts - INTERVAL 30 DAY < '2025-06-01'",
                 "upper(status) = 'PAID'",
                 "length(region) = 2",
                 "abs(qty) > 20",
                 "qty * 2 > 40",
                 "amount / 2 < 50"):
        n += 1
        yield (f"fn_{n}", "function_predicate",
               f"SELECT count(*) AS n FROM facts WHERE {expr}")


def _joins() -> Iterator[Query]:
    """Cross-table filters: a predicate on one side should prune the other."""
    n = 0
    times = _spread(9)
    joins = (("INNER JOIN", ""), ("LEFT JOIN", ""), ("RIGHT JOIN", ""),
             ("FULL JOIN", ""))
    for jt, _ in joins:
        for t in times:
            n += 1
            yield (f"join2_{n}", "join_2",
                   f"SELECT count(*) AS n, sum(f.qty) AS q FROM facts f "
                   f"{jt} customers c ON f.cust_id = c.cust_id "
                   f"WHERE f.event_ts >= '{t}'")
            n += 1
            yield (f"join2_dimfilter_{n}", "join_2",
                   f"SELECT count(*) AS n FROM facts f "
                   f"{jt} customers c ON f.cust_id = c.cust_id "
                   f"WHERE c.tier = 'gold' AND f.event_ts < '{t}'")
            n += 1
            yield (f"join2_onfilter_{n}", "join_2",
                   f"SELECT count(*) AS n FROM facts f "
                   f"{jt} customers c ON f.cust_id = c.cust_id "
                   f"AND c.country = 'DE' WHERE f.event_ts >= '{t}'")
    for t in times:
        n += 1
        yield (f"join3_{n}", "join_3",
               f"SELECT count(*) AS n, sum(f.amount) AS a FROM facts f "
               f"JOIN customers c ON f.cust_id = c.cust_id "
               f"JOIN products p ON f.prod_id = p.prod_id "
               f"WHERE f.event_ts >= '{t}' AND p.category = 'tools'")
        n += 1
        yield (f"join4_{n}", "join_4",
               f"SELECT count(*) AS n FROM facts f "
               f"JOIN customers c ON f.cust_id = c.cust_id "
               f"JOIN products p ON f.prod_id = p.prod_id "
               f"JOIN events e ON e.cust_id = c.cust_id "
               f"WHERE f.event_ts >= '{t}' AND e.occurred_ts >= '{t}'")
        n += 1
        yield (f"join_factfact_{n}", "join_fact_fact",
               f"SELECT count(*) AS n FROM facts f "
               f"JOIN events e ON f.cust_id = e.cust_id "
               f"WHERE f.event_ts >= '{t}' AND e.kind = 'purchase'")
        n += 1
        yield (f"join_using_{n}", "join_2",
               f"SELECT count(*) AS n FROM facts "
               f"JOIN customers USING (cust_id) WHERE event_ts >= '{t}'")
        n += 1
        yield (f"join_cross_time_{n}", "join_cross_time",
               f"SELECT count(*) AS n FROM facts f "
               f"JOIN events e ON f.cust_id = e.cust_id "
               f"WHERE f.event_ts >= '{t}' AND e.occurred_ts < '{t}'")


def _subqueries() -> Iterator[Query]:
    n = 0
    times = _spread(8)
    for t in times:
        n += 1
        yield (f"sub_in_{n}", "subquery_in",
               f"SELECT count(*) AS n FROM facts WHERE cust_id IN "
               f"(SELECT cust_id FROM customers WHERE tier = 'gold') "
               f"AND event_ts >= '{t}'")
        n += 1
        yield (f"sub_notin_{n}", "subquery_in",
               f"SELECT count(*) AS n FROM facts WHERE cust_id NOT IN "
               f"(SELECT cust_id FROM customers WHERE country = 'DE') "
               f"AND event_ts >= '{t}'")
        n += 1
        yield (f"sub_exists_{n}", "subquery_exists",
               f"SELECT count(*) AS n FROM facts f WHERE EXISTS "
               f"(SELECT 1 FROM events e WHERE e.cust_id = f.cust_id "
               f"AND e.occurred_ts >= '{t}')")
        n += 1
        yield (f"sub_notexists_{n}", "subquery_exists",
               f"SELECT count(*) AS n FROM facts f WHERE NOT EXISTS "
               f"(SELECT 1 FROM events e WHERE e.cust_id = f.cust_id) "
               f"AND f.event_ts >= '{t}'")
        n += 1
        yield (f"sub_scalar_{n}", "subquery_scalar",
               f"SELECT count(*) AS n FROM facts WHERE qty > "
               f"(SELECT avg(qty) FROM facts WHERE event_ts >= '{t}')")
        n += 1
        yield (f"sub_derived_{n}", "derived_table",
               f"SELECT count(*) AS n FROM (SELECT * FROM facts "
               f"WHERE event_ts >= '{t}') s WHERE s.status = 'paid'")
        n += 1
        yield (f"sub_derived_join_{n}", "derived_table",
               f"SELECT count(*) AS n FROM "
               f"(SELECT cust_id, qty FROM facts WHERE event_ts >= '{t}') s "
               f"JOIN customers c ON s.cust_id = c.cust_id WHERE c.tier <> 'free'")


def _ctes() -> Iterator[Query]:
    n = 0
    times = _spread(8)
    for t in times:
        n += 1
        yield (f"cte_{n}", "cte",
               f"WITH w AS (SELECT * FROM facts WHERE event_ts >= '{t}') "
               f"SELECT count(*) AS n, sum(qty) AS q FROM w")
        n += 1
        yield (f"cte_two_{n}", "cte",
               f"WITH a AS (SELECT * FROM facts WHERE event_ts >= '{t}'), "
               f"b AS (SELECT * FROM customers WHERE tier = 'gold') "
               f"SELECT count(*) AS n FROM a JOIN b ON a.cust_id = b.cust_id")
        n += 1
        yield (f"cte_nested_{n}", "cte",
               f"WITH a AS (SELECT * FROM facts WHERE event_ts >= '{t}'), "
               f"b AS (SELECT * FROM a WHERE qty > 10) "
               f"SELECT count(*) AS n FROM b")
        n += 1
        yield (f"cte_agg_{n}", "cte",
               f"WITH a AS (SELECT cust_id, sum(qty) AS q FROM facts "
               f"WHERE event_ts >= '{t}' GROUP BY cust_id) "
               f"SELECT count(*) AS n, sum(q) AS tot FROM a WHERE q > 5")
        n += 1
        yield (f"cte_three_{n}", "cte",
               f"WITH a AS (SELECT * FROM facts WHERE event_ts >= '{t}'), "
               f"b AS (SELECT * FROM events WHERE occurred_ts >= '{t}'), "
               f"c AS (SELECT * FROM customers WHERE country IN ('DE','AT')) "
               f"SELECT count(*) AS n FROM a JOIN c ON a.cust_id = c.cust_id "
               f"JOIN b ON b.cust_id = c.cust_id")


def _aggregates_and_sets() -> Iterator[Query]:
    n = 0
    times = _spread(6)
    for t in times:
        n += 1
        yield (f"agg_group_{n}", "aggregate",
               f"SELECT region, count(*) AS n, sum(qty) AS q FROM facts "
               f"WHERE event_ts >= '{t}' GROUP BY region ORDER BY region")
        n += 1
        yield (f"agg_having_{n}", "aggregate",
               f"SELECT status, count(*) AS n FROM facts WHERE event_ts >= '{t}' "
               f"GROUP BY status HAVING count(*) > 100 ORDER BY status")
        n += 1
        yield (f"agg_minmax_{n}", "aggregate",
               f"SELECT min(event_ts) AS lo, max(event_ts) AS hi, count(*) AS n "
               f"FROM facts WHERE event_ts >= '{t}'")
        n += 1
        yield (f"agg_distinct_{n}", "aggregate",
               f"SELECT count(DISTINCT cust_id) AS n FROM facts WHERE event_ts >= '{t}'")
        n += 1
        yield (f"set_union_{n}", "set_op",
               f"SELECT count(*) AS n FROM ("
               f"SELECT fact_id FROM facts WHERE event_ts >= '{t}' "
               f"UNION ALL SELECT fact_id FROM facts WHERE qty > 35) u")
        n += 1
        yield (f"set_union_distinct_{n}", "set_op",
               f"SELECT count(*) AS n FROM ("
               f"SELECT cust_id FROM facts WHERE event_ts >= '{t}' "
               f"UNION SELECT cust_id FROM events WHERE occurred_ts >= '{t}') u")
        n += 1
        yield (f"set_except_{n}", "set_op",
               f"SELECT count(*) AS n FROM ("
               f"SELECT cust_id FROM facts WHERE event_ts >= '{t}' "
               f"EXCEPT SELECT cust_id FROM customers WHERE tier = 'free') u")
        n += 1
        yield (f"set_intersect_{n}", "set_op",
               f"SELECT count(*) AS n FROM ("
               f"SELECT cust_id FROM facts WHERE event_ts >= '{t}' "
               f"INTERSECT SELECT cust_id FROM events WHERE kind = 'click') u")
        n += 1
        yield (f"window_{n}", "window",
               f"SELECT count(*) AS n FROM (SELECT fact_id, "
               f"row_number() OVER (PARTITION BY region ORDER BY event_ts) AS rn "
               f"FROM facts WHERE event_ts >= '{t}') w WHERE rn <= 3")
        n += 1
        yield (f"case_{n}", "case_expr",
               f"SELECT count(*) AS n FROM facts WHERE "
               f"CASE WHEN qty > 20 THEN event_ts ELSE NULL END >= '{t}'")


_FAMILIES = (
    _time_predicates, _time_ranges, _numeric_and_string, _null_semantics,
    _in_lists, _compound, _functions_in_predicate, _joins, _subqueries,
    _ctes, _aggregates_and_sets,
)


def all_queries() -> List[Query]:
    out: List[Query] = []
    for fam in _FAMILIES:
        out.extend(fam())
    return out
