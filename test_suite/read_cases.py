# route: test_suite.read_cases
"""The SQL surface the read path must answer correctly.

Every case is run against SuperTable and against a bare DuckDB holding the same
logical rows, and the two results must be identical. Cases are grouped by the
feature they exercise so a failure names a capability rather than a query.

``ordered=True`` cases carry an explicit ORDER BY and are compared positionally;
everything else is compared as a multiset, because the read path makes no
ordering guarantee without ORDER BY and pinning an incidental order would make
the suite fail on an unrelated plan change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass(frozen=True)
class ReadCase:
    case_id: str
    feature: str
    sql: str
    ordered: bool = False
    #: Cases whose shape is worth checking in every execution mode, not just
    #: the default one. Kept to a representative subset so the suite stays fast.
    all_modes: bool = False


def _c(case_id, feature, sql, ordered=False, all_modes=False) -> ReadCase:
    return ReadCase(case_id, feature, " ".join(sql.split()), ordered, all_modes)


def build_cases() -> List[ReadCase]:
    cases: List[ReadCase] = []
    add = cases.append

    # ---------------- scalar aggregates ----------------
    add(_c("agg_count_star", "aggregate", "SELECT COUNT(*) AS n FROM facts", all_modes=True))
    add(_c("agg_count_column", "aggregate", "SELECT COUNT(score) AS n FROM facts"))
    add(_c("agg_count_distinct", "aggregate", "SELECT COUNT(DISTINCT grp) AS n FROM facts"))
    add(_c("agg_sum", "aggregate", "SELECT SUM(qty) AS total FROM facts"))
    add(_c("agg_min_max", "aggregate", "SELECT MIN(amount) AS lo, MAX(amount) AS hi FROM facts"))
    add(_c("agg_avg", "aggregate", "SELECT AVG(qty) AS mean FROM facts"))
    add(_c("agg_avg_with_nulls", "aggregate", "SELECT AVG(score) AS mean FROM facts"))
    add(_c("agg_stddev_samp", "aggregate", "SELECT ROUND(STDDEV_SAMP(amount), 6) AS s FROM facts"))
    add(_c("agg_stddev_pop", "aggregate", "SELECT ROUND(STDDEV_POP(amount), 6) AS s FROM facts"))
    add(_c("agg_var_samp", "aggregate", "SELECT ROUND(VAR_SAMP(qty), 6) AS v FROM facts"))
    add(_c("agg_var_pop", "aggregate", "SELECT ROUND(VAR_POP(qty), 6) AS v FROM facts"))
    add(_c("agg_median", "aggregate", "SELECT MEDIAN(qty) AS m FROM facts"))
    add(_c("agg_quantile", "aggregate",
           "SELECT ROUND(QUANTILE_CONT(amount, 0.9), 6) AS p90 FROM facts"))
    add(_c("agg_all_at_once", "aggregate", """
        SELECT COUNT(*) AS n, SUM(qty) AS s, MIN(qty) AS lo, MAX(qty) AS hi,
               ROUND(AVG(qty), 6) AS mean, ROUND(STDDEV_SAMP(qty), 6) AS sd
        FROM facts"""))
    add(_c("agg_filter_clause", "aggregate",
           "SELECT COUNT(*) FILTER (WHERE qty > 5) AS big FROM facts"))
    add(_c("agg_sum_of_expression", "aggregate",
           "SELECT ROUND(SUM(amount * qty), 4) AS weighted FROM facts"))
    add(_c("agg_empty_result", "aggregate",
           "SELECT COUNT(*) AS n, SUM(qty) AS s, AVG(qty) AS a FROM facts WHERE fid < 0"))

    # ---------------- GROUP BY / HAVING ----------------
    add(_c("group_single", "group_by",
           "SELECT grp, COUNT(*) AS n FROM facts GROUP BY grp ORDER BY grp",
           ordered=True, all_modes=True))
    add(_c("group_multi", "group_by", """
        SELECT grp, region, COUNT(*) AS n, SUM(qty) AS q
        FROM facts GROUP BY grp, region ORDER BY grp, region""", ordered=True))
    add(_c("group_having", "group_by", """
        SELECT grp, COUNT(*) AS n FROM facts GROUP BY grp
        HAVING COUNT(*) > 40 ORDER BY grp""", ordered=True))
    add(_c("group_having_on_aggregate", "group_by", """
        SELECT region, ROUND(AVG(amount), 4) AS mean FROM facts
        GROUP BY region HAVING AVG(amount) > 0 ORDER BY region""", ordered=True))
    add(_c("group_by_alias", "group_by", """
        SELECT grp AS category, COUNT(*) AS n FROM facts
        GROUP BY category ORDER BY category""", ordered=True))
    add(_c("group_by_expression", "group_by", """
        SELECT qty % 3 AS bucket, COUNT(*) AS n FROM facts
        GROUP BY qty % 3 ORDER BY bucket""", ordered=True))
    add(_c("group_by_ordinal", "group_by", """
        SELECT region, COUNT(*) AS n FROM facts GROUP BY 1 ORDER BY 1""", ordered=True))
    add(_c("group_by_all", "group_by", """
        SELECT region, COUNT(*) AS n FROM facts GROUP BY ALL ORDER BY region""",
           ordered=True))

    # ---------------- grouping extensions ----------------
    add(_c("grouping_rollup", "grouping_sets", """
        SELECT grp, region, COUNT(*) AS n FROM facts
        GROUP BY ROLLUP (grp, region) ORDER BY grp NULLS LAST, region NULLS LAST""",
           ordered=True))
    add(_c("grouping_cube", "grouping_sets", """
        SELECT grp, region, COUNT(*) AS n FROM facts
        GROUP BY CUBE (grp, region) ORDER BY grp NULLS LAST, region NULLS LAST""",
           ordered=True))
    add(_c("grouping_sets", "grouping_sets", """
        SELECT grp, region, COUNT(*) AS n FROM facts
        GROUP BY GROUPING SETS ((grp), (region), ())
        ORDER BY grp NULLS LAST, region NULLS LAST""", ordered=True))
    add(_c("grouping_rollup_with_limit", "grouping_sets", """
        SELECT grp, COUNT(*) AS n FROM facts GROUP BY ROLLUP (grp) LIMIT 100"""))
    add(_c("grouping_flag", "grouping_sets", """
        SELECT grp, GROUPING(grp) AS is_total, COUNT(*) AS n FROM facts
        GROUP BY ROLLUP (grp) ORDER BY is_total, grp NULLS LAST""", ordered=True))

    # ---------------- window functions ----------------
    add(_c("window_row_number", "window", """
        SELECT fid, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY fid) AS rn
        FROM facts ORDER BY grp, fid""", ordered=True, all_modes=True))
    add(_c("window_rank_dense", "window", """
        SELECT fid, qty, RANK() OVER (ORDER BY qty DESC, fid) AS rk,
               DENSE_RANK() OVER (ORDER BY qty DESC) AS drk
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_lag_lead", "window", """
        SELECT fid, qty, LAG(qty) OVER (ORDER BY fid) AS prev,
               LEAD(qty) OVER (ORDER BY fid) AS nxt
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_lag_with_default", "window", """
        SELECT fid, LAG(qty, 2, -1) OVER (ORDER BY fid) AS prev2
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_running_total", "window", """
        SELECT fid, SUM(qty) OVER (ORDER BY fid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_moving_average", "window", """
        SELECT fid, ROUND(AVG(amount) OVER (ORDER BY fid
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), 6) AS smoothed
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_partition_aggregate", "window", """
        SELECT fid, grp, SUM(qty) OVER (PARTITION BY grp) AS grp_total,
               ROUND(AVG(qty) OVER (PARTITION BY grp), 6) AS grp_mean
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_ntile", "window", """
        SELECT fid, NTILE(4) OVER (ORDER BY amount, fid) AS quartile
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_first_last_value", "window", """
        SELECT grp, fid,
               FIRST_VALUE(fid) OVER (PARTITION BY grp ORDER BY fid) AS first_fid,
               LAST_VALUE(fid) OVER (PARTITION BY grp ORDER BY fid
                   RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_fid
        FROM facts ORDER BY grp, fid""", ordered=True))
    add(_c("window_percent_rank", "window", """
        SELECT fid, ROUND(PERCENT_RANK() OVER (ORDER BY qty, fid), 6) AS pr,
               ROUND(CUME_DIST() OVER (ORDER BY qty, fid), 6) AS cd
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("window_qualify", "window", """
        SELECT fid, grp FROM facts
        QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY fid) <= 3
        ORDER BY grp, fid""", ordered=True))
    add(_c("window_distinct_count_over", "window", """
        SELECT grp, COUNT(*) OVER (PARTITION BY grp) AS in_group FROM facts
        ORDER BY grp, fid""", ordered=True))

    # ---------------- date / timestamp ----------------
    add(_c("date_filter_lower", "date",
           "SELECT COUNT(*) AS n FROM facts WHERE event_date >= DATE '2025-01-01'",
           all_modes=True))
    add(_c("date_filter_range", "date", """
        SELECT COUNT(*) AS n FROM facts
        WHERE event_date BETWEEN DATE '2024-06-01' AND DATE '2024-12-31'"""))
    add(_c("date_filter_exact", "date",
           "SELECT COUNT(*) AS n FROM facts WHERE event_date = DATE '2024-07-04'"))
    add(_c("date_extract_year", "date", """
        SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) AS n
        FROM facts GROUP BY yr ORDER BY yr""", ordered=True))
    add(_c("date_extract_parts", "date", """
        SELECT fid, EXTRACT(month FROM event_date) AS mo,
               EXTRACT(day FROM event_date) AS dy,
               EXTRACT(dow FROM event_date) AS dow
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("date_trunc_month", "date", """
        SELECT DATE_TRUNC('month', event_date) AS mo, COUNT(*) AS n,
               SUM(qty) AS q FROM facts GROUP BY 1 ORDER BY 1""", ordered=True))
    add(_c("date_aggregate_minmax", "date",
           "SELECT MIN(event_date) AS first_day, MAX(event_date) AS last_day FROM facts"))
    add(_c("date_subtraction", "date", """
        SELECT fid, DATE '2025-01-01' - event_date AS days_before
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("date_year_month_group", "date", """
        SELECT EXTRACT(year FROM event_date) AS yr,
               EXTRACT(month FROM event_date) AS mo,
               COUNT(*) AS n, ROUND(AVG(amount), 6) AS mean
        FROM facts GROUP BY yr, mo ORDER BY yr, mo""", ordered=True))
    add(_c("timestamp_filter", "date",
           "SELECT COUNT(*) AS n FROM facts WHERE event_ts < TIMESTAMP '2024-06-01 00:00:00'"))
    add(_c("timestamp_extract_hour", "date", """
        SELECT EXTRACT(hour FROM event_ts) AS hr, COUNT(*) AS n
        FROM facts GROUP BY hr ORDER BY hr""", ordered=True))
    add(_c("timestamp_trunc_day", "date", """
        SELECT DATE_TRUNC('day', event_ts) AS d, COUNT(*) AS n
        FROM facts GROUP BY 1 ORDER BY 1 LIMIT 40""", ordered=True))

    # ---------------- joins ----------------
    add(_c("join_inner", "join", """
        SELECT f.fid, f.grp, d.label FROM facts f
        JOIN dims d ON f.grp = d.grp ORDER BY f.fid""", ordered=True, all_modes=True))
    add(_c("join_left_unmatched", "join", """
        SELECT f.fid, f.grp, d.label FROM facts f
        LEFT JOIN dims d ON f.grp = d.grp ORDER BY f.fid""", ordered=True))
    add(_c("join_right_unmatched", "join", """
        SELECT d.grp, d.label, COUNT(f.fid) AS n FROM facts f
        RIGHT JOIN dims d ON f.grp = d.grp GROUP BY d.grp, d.label
        ORDER BY d.grp""", ordered=True))
    add(_c("join_full_outer", "join", """
        SELECT f.grp AS fact_grp, d.grp AS dim_grp, COUNT(*) AS n FROM facts f
        FULL OUTER JOIN dims d ON f.grp = d.grp
        GROUP BY 1, 2 ORDER BY 1 NULLS LAST, 2 NULLS LAST""", ordered=True))
    add(_c("join_aggregate_weighted", "join", """
        SELECT d.label, ROUND(SUM(f.qty * d.weight), 6) AS weighted FROM facts f
        JOIN dims d ON f.grp = d.grp GROUP BY d.label ORDER BY d.label""", ordered=True))
    add(_c("join_self", "join", """
        SELECT a.fid, b.fid AS peer FROM facts a JOIN facts b
        ON a.grp = b.grp AND a.fid = b.fid - 4 ORDER BY a.fid LIMIT 50""", ordered=True))
    add(_c("join_cross_small", "join", """
        SELECT d1.grp AS l, d2.grp AS r FROM dims d1 CROSS JOIN dims d2
        ORDER BY l, r""", ordered=True))
    add(_c("join_using", "join", """
        SELECT fid, grp, label FROM facts JOIN dims USING (grp) ORDER BY fid""",
           ordered=True))
    add(_c("join_semi_exists", "join", """
        SELECT COUNT(*) AS n FROM facts f
        WHERE EXISTS (SELECT 1 FROM dims d WHERE d.grp = f.grp)"""))
    add(_c("join_anti_not_exists", "join", """
        SELECT COUNT(*) AS n FROM facts f
        WHERE NOT EXISTS (SELECT 1 FROM dims d WHERE d.grp = f.grp)"""))

    # ---------------- set operations ----------------
    add(_c("set_union_all", "set_ops", """
        SELECT grp FROM facts WHERE qty = 1
        UNION ALL SELECT grp FROM facts WHERE qty = 2"""))
    add(_c("set_union_distinct", "set_ops", """
        SELECT grp FROM facts WHERE region = 'eu'
        UNION SELECT grp FROM facts WHERE region = 'us' ORDER BY grp""", ordered=True))
    add(_c("set_intersect", "set_ops", """
        SELECT grp FROM facts WHERE region = 'eu'
        INTERSECT SELECT grp FROM facts WHERE region = 'us' ORDER BY grp""", ordered=True))
    add(_c("set_except", "set_ops", """
        SELECT grp FROM facts
        EXCEPT SELECT grp FROM dims WHERE grp <> 'omega' ORDER BY grp""", ordered=True))

    # ---------------- subqueries and CTEs ----------------
    add(_c("subquery_scalar", "subquery", """
        SELECT fid, qty FROM facts
        WHERE qty > (SELECT AVG(qty) FROM facts) ORDER BY fid""", ordered=True))
    add(_c("subquery_in", "subquery", """
        SELECT COUNT(*) AS n FROM facts
        WHERE grp IN (SELECT grp FROM dims WHERE weight > 1)"""))
    add(_c("subquery_not_in", "subquery", """
        SELECT COUNT(*) AS n FROM facts
        WHERE grp NOT IN (SELECT grp FROM dims WHERE weight > 1)"""))
    add(_c("subquery_correlated", "subquery", """
        SELECT fid, qty FROM facts f WHERE qty = (
            SELECT MAX(qty) FROM facts x WHERE x.grp = f.grp
        ) ORDER BY fid""", ordered=True))
    add(_c("subquery_derived_table", "subquery", """
        SELECT grp, n FROM (
            SELECT grp, COUNT(*) AS n FROM facts GROUP BY grp
        ) t WHERE n > 40 ORDER BY grp""", ordered=True))
    add(_c("cte_single", "cte", """
        WITH per_group AS (SELECT grp, SUM(qty) AS q FROM facts GROUP BY grp)
        SELECT grp, q FROM per_group ORDER BY grp""", ordered=True, all_modes=True))
    add(_c("cte_chained", "cte", """
        WITH a AS (SELECT grp, region, SUM(qty) AS q FROM facts GROUP BY grp, region),
             b AS (SELECT grp, SUM(q) AS total FROM a GROUP BY grp)
        SELECT grp, total FROM b ORDER BY grp""", ordered=True))
    add(_c("cte_joined_to_table", "cte", """
        WITH top AS (SELECT grp, COUNT(*) AS n FROM facts GROUP BY grp)
        SELECT t.grp, t.n, d.label FROM top t JOIN dims d ON t.grp = d.grp
        ORDER BY t.grp""", ordered=True))
    add(_c("cte_with_window", "cte", """
        WITH ranked AS (
            SELECT fid, grp, qty,
                   ROW_NUMBER() OVER (PARTITION BY grp ORDER BY qty DESC, fid) AS rn
            FROM facts)
        SELECT grp, fid, qty FROM ranked WHERE rn = 1 ORDER BY grp""", ordered=True))

    # ---------------- expressions, NULLs, strings, casts ----------------
    add(_c("expr_case", "expression", """
        SELECT fid, CASE WHEN qty = 0 THEN 'none' WHEN qty < 5 THEN 'low'
                         ELSE 'high' END AS band
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("expr_case_aggregate", "expression", """
        SELECT SUM(CASE WHEN region = 'eu' THEN qty ELSE 0 END) AS eu_qty,
               COUNT(CASE WHEN score IS NULL THEN 1 END) AS missing_scores
        FROM facts"""))
    add(_c("expr_coalesce", "expression", """
        SELECT fid, COALESCE(score, -1.0) AS filled FROM facts ORDER BY fid""",
           ordered=True))
    add(_c("expr_nullif", "expression", """
        SELECT fid, NULLIF(qty, 0) AS nonzero FROM facts ORDER BY fid""", ordered=True))
    add(_c("null_is_null", "null_semantics",
           "SELECT COUNT(*) AS n FROM facts WHERE score IS NULL", all_modes=True))
    add(_c("null_is_not_null", "null_semantics",
           "SELECT COUNT(*) AS n FROM facts WHERE note IS NOT NULL"))
    add(_c("null_three_valued", "null_semantics",
           "SELECT COUNT(*) AS n FROM facts WHERE score > 5 OR score IS NULL"))
    add(_c("null_is_distinct_from", "null_semantics", """
        SELECT COUNT(*) AS n FROM facts WHERE score IS DISTINCT FROM 0.0"""))
    add(_c("null_ordering", "null_semantics", """
        SELECT fid, score FROM facts ORDER BY score NULLS FIRST, fid LIMIT 40""",
           ordered=True))
    add(_c("string_functions", "string", """
        SELECT fid, UPPER(grp) AS up, LENGTH(grp) AS len,
               SUBSTRING(grp, 1, 2) AS head, grp || '-' || region AS joined
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("string_like", "string",
           "SELECT COUNT(*) AS n FROM facts WHERE note LIKE 'note-alpha%'"))
    add(_c("string_unicode", "string",
           "SELECT COUNT(*) AS n FROM facts WHERE note LIKE '%é'"))
    add(_c("cast_numeric", "cast", """
        SELECT fid, CAST(amount AS INTEGER) AS truncated,
               CAST(qty AS DOUBLE) / 2 AS halved
        FROM facts ORDER BY fid""", ordered=True))
    add(_c("distinct_values", "distinct",
           "SELECT DISTINCT grp, region FROM facts ORDER BY grp, region", ordered=True))
    add(_c("distinct_on_expression", "distinct",
           "SELECT DISTINCT qty % 4 AS bucket FROM facts ORDER BY bucket", ordered=True))

    # ---------------- ordering and row bounds ----------------
    add(_c("order_limit", "row_bounds",
           "SELECT fid, qty FROM facts ORDER BY qty DESC, fid LIMIT 10", ordered=True,
           all_modes=True))
    add(_c("order_limit_offset", "row_bounds",
           "SELECT fid FROM facts ORDER BY fid LIMIT 10 OFFSET 25", ordered=True))
    add(_c("order_multi_direction", "row_bounds",
           "SELECT fid, grp, qty FROM facts ORDER BY grp ASC, qty DESC, fid ASC LIMIT 30",
           ordered=True))
    add(_c("order_by_expression", "row_bounds",
           "SELECT fid, amount FROM facts ORDER BY ABS(amount), fid LIMIT 20", ordered=True))
    add(_c("limit_zero", "row_bounds", "SELECT fid FROM facts ORDER BY fid LIMIT 0",
           ordered=True))
    add(_c("fetch_first", "row_bounds",
           "SELECT fid FROM facts ORDER BY fid FETCH FIRST 5 ROWS ONLY", ordered=True))

    # ---------------- tombstone / update visibility ----------------
    # These are the cases where the physical table and the logical table differ,
    # so they fail loudly if the deletion vector or an upsert is mishandled.
    add(_c("tombstone_deleted_keys_absent", "tombstone", """
        SELECT COUNT(*) AS n FROM facts
        WHERE fid IN (7, 23, 55, 99, 140, 177, 190)""", all_modes=True))
    add(_c("tombstone_updated_rows_current", "tombstone", """
        SELECT fid, amount FROM facts WHERE fid IN (3, 17, 42, 88, 120, 151)
        ORDER BY fid""", ordered=True, all_modes=True))
    add(_c("tombstone_no_duplicate_keys", "tombstone", """
        SELECT COUNT(*) AS total, COUNT(DISTINCT fid) AS distinct_keys FROM facts"""))

    return cases
