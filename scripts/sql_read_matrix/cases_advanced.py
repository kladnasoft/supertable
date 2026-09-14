from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable

from .models import Dataset, QueryCase


def _values(rows, column):
    return [row[column] for row in rows if row[column] is not None]


def _sum(values):
    return sum(values) if values else None


def _mean(values):
    return sum(values) / len(values) if values else None


def _variance(values, sample=False):
    if len(values) <= int(sample):
        return None
    mean = sum(values) / len(values)
    return sum((value - mean) ** 2 for value in values) / (len(values) - int(sample))


def _quantile(values, probability):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    low = math.floor(index)
    high = math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def _aggregate(values, name):
    clean = [value for value in values if value is not None]
    if name == "count":
        return len(clean)
    if name == "sum":
        return _sum(clean)
    if name == "avg":
        return _mean(clean)
    if name == "min":
        return min(clean) if clean else None
    if name == "max":
        return max(clean) if clean else None
    raise ValueError(name)


def _partition(rows, columns):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[column] for column in columns)].append(row)
    return groups


def _order_key(value, descending=False):
    return (value is None, -value if descending and value is not None else value)


def _sql_and(left, right):
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return True


def _sql_or(left, right):
    if left is True or right is True:
        return True
    if left is None or right is None:
        return None
    return False


def build_cases(data: Dataset) -> list[QueryCase]:
    cases = []
    orders = data["orders"]
    numbers = data["numbers"]
    events = data["events"]

    def add(name, category, sql, columns, expected, ordered=False):
        cases.append(QueryCase(
            case_id=f"advanced_{name}", category=category, sql=sql,
            columns=tuple(columns), expected=list(expected), ordered=ordered,
        ))

    selections: list[tuple[str, str, Callable[[dict], bool]]] = [
        ("all", "TRUE", lambda row: True),
        ("eu", "region = 'eu'", lambda row: row["region"] == "eu"),
        ("positive", "amount > 0", lambda row: row["amount"] is not None and row["amount"] > 0),
        ("negative", "amount < 0", lambda row: row["amount"] is not None and row["amount"] < 0),
        ("null_amount", "amount IS NULL", lambda row: row["amount"] is None),
        ("first_row", "oid = 1", lambda row: row["oid"] == 1),
        ("none", "oid < 0", lambda row: False),
    ]
    for label, predicate, select in selections:
        rows = [row for row in orders if select(row)]
        amounts = _values(rows, "amount")
        quantities = _values(rows, "qty")
        scores = _values(rows, "score")
        flags = _values(rows, "active")
        add(f"aggregate_{label}", "aggregate", f"""
            SELECT COUNT(*) AS rows_n, COUNT(amount) AS amount_n,
                   SUM(amount) AS amount_sum, MIN(amount) AS amount_min,
                   MAX(amount) AS amount_max, AVG(amount) AS amount_avg,
                   COUNT(DISTINCT qty) AS qty_distinct
            FROM warehouse.orders WHERE {predicate}
        """, ("rows_n", "amount_n", "amount_sum", "amount_min", "amount_max", "amount_avg", "qty_distinct"),
            [(len(rows), len(amounts), _sum(amounts), min(amounts) if amounts else None,
              max(amounts) if amounts else None, _mean(amounts), len(set(quantities)))])
        add(f"distinct_aggregate_{label}", "distinct_aggregate", f"""
            SELECT COUNT(DISTINCT amount) AS amount_n,
                   SUM(DISTINCT qty) AS qty_sum, AVG(DISTINCT score) AS score_avg,
                   COUNT(DISTINCT status) AS status_n
            FROM warehouse.orders WHERE {predicate}
        """, ("amount_n", "qty_sum", "score_avg", "status_n"),
            [(len(set(amounts)), _sum(list(set(quantities))), _mean(list(set(scores))), len(set(_values(rows, "status"))))])
        add(f"conditional_aggregate_{label}", "conditional_aggregate", f"""
            SELECT COUNT(*) FILTER (WHERE active IS TRUE) AS active_n,
                   COUNT(*) FILTER (WHERE active IS FALSE) AS inactive_n,
                   SUM(amount) FILTER (WHERE qty >= 3) AS large_qty_sum,
                   SUM(CASE WHEN status = 'paid' THEN COALESCE(amount, 0) ELSE 0 END) AS paid_sum,
                   BOOL_AND(active) AS all_active, BOOL_OR(active) AS any_active
            FROM warehouse.orders WHERE {predicate}
        """, ("active_n", "inactive_n", "large_qty_sum", "paid_sum", "all_active", "any_active"),
            [(sum(row["active"] is True for row in rows), sum(row["active"] is False for row in rows),
              _sum([row["amount"] for row in rows if row["qty"] >= 3 and row["amount"] is not None]),
              _sum([(row["amount"] or 0) if row["status"] == "paid" else 0 for row in rows]),
              all(flags) if flags else None, any(flags) if flags else None)])
        add(f"statistical_{label}", "statistical_aggregate", f"""
            SELECT VAR_POP(score) AS population_variance, VAR_SAMP(score) AS sample_variance,
                   STDDEV_POP(score) AS population_stddev, MEDIAN(score) AS median_score,
                   QUANTILE_CONT(score, 0.25) AS lower_quartile,
                   QUANTILE_CONT(score, 0.75) AS upper_quartile
            FROM warehouse.orders WHERE {predicate}
        """, ("population_variance", "sample_variance", "population_stddev", "median_score", "lower_quartile", "upper_quartile"),
            [(_variance(scores), _variance(scores, True), math.sqrt(_variance(scores)) if scores else None,
              _quantile(scores, 0.5), _quantile(scores, 0.25), _quantile(scores, 0.75))])

    group_columns = [("region",), ("status",), ("qty",), ("active",), ("region", "status"), ("region", "qty")]
    for columns in group_columns:
        label = "_".join(columns)
        groups = _partition(orders, columns)
        group_sql = ", ".join(columns)
        expected = [key + (len(rows), _sum(_values(rows, "amount")), _mean(_values(rows, "score")),
                           len(set(_values(rows, "cid")))) for key, rows in groups.items()]
        add(f"group_{label}", "group_by", f"""
            SELECT {group_sql}, COUNT(*) AS n, SUM(amount) AS total_amount,
                   AVG(score) AS mean_score, COUNT(DISTINCT cid) AS customer_n
            FROM warehouse.orders GROUP BY {group_sql}
        """, columns + ("n", "total_amount", "mean_score", "customer_n"), expected)
        for threshold in (10, 45):
            add(f"having_{label}_{threshold}", "having", f"""
                SELECT {group_sql}, COUNT(*) AS n, SUM(amount) AS total_amount
                FROM warehouse.orders GROUP BY {group_sql}
                HAVING COUNT(*) >= {threshold} AND SUM(amount) > 0
            """, columns + ("n", "total_amount"),
                [key + (len(rows), _sum(_values(rows, "amount"))) for key, rows in groups.items()
                 if len(rows) >= threshold and _values(rows, "amount") and sum(_values(rows, "amount")) > 0])
        add(f"group_filter_{label}", "conditional_aggregate", f"""
            SELECT {group_sql}, COUNT(*) FILTER (WHERE amount IS NULL) AS missing_amounts,
                   SUM(amount) FILTER (WHERE active IS TRUE) AS active_total,
                   COUNT(DISTINCT CASE WHEN qty >= 3 THEN cid END) AS large_customers
            FROM warehouse.orders GROUP BY {group_sql}
        """, columns + ("missing_amounts", "active_total", "large_customers"),
            [key + (sum(row["amount"] is None for row in rows),
                    _sum([row["amount"] for row in rows if row["active"] is True and row["amount"] is not None]),
                    len({row["cid"] for row in rows if row["qty"] >= 3 and row["cid"] is not None}))
             for key, rows in groups.items()])

    grouping_modes = [
        ("sets", "GROUPING SETS ((region, status), (region), ())", [("region", "status"), ("region",), ()]),
        ("rollup", "ROLLUP (region, status)", [("region", "status"), ("region",), ()]),
        ("cube", "CUBE (region, status)", [("region", "status"), ("region",), ("status",), ()]),
    ]
    for name, group_expression, sets in grouping_modes:
        rows_out = []
        for included in sets:
            for key, rows in _partition(orders, included).items():
                values = dict(zip(included, key))
                rows_out.append((values.get("region"), values.get("status"),
                                 int("region" not in included), int("status" not in included),
                                 len(rows), _sum(_values(rows, "amount"))))
        add(f"grouping_{name}", "grouping_sets", f"""
            SELECT region, status, GROUPING(region) AS region_total,
                   GROUPING(status) AS status_total, COUNT(*) AS n, SUM(amount) AS total_amount
            FROM warehouse.orders GROUP BY {group_expression}
        """, ("region", "status", "region_total", "status_total", "n", "total_amount"), rows_out)

    for divisor in (2, 3, 5):
        groups = defaultdict(list)
        for row in orders:
            groups[row["qty"] % divisor].append(row)
        add(f"group_expression_mod_{divisor}", "group_expression", f"""
            SELECT qty % {divisor} AS bucket, COUNT(*) AS n,
                   SUM(COALESCE(amount, 0) * qty) AS weighted_sum
            FROM warehouse.orders GROUP BY qty % {divisor} HAVING COUNT(*) > 0
        """, ("bucket", "n", "weighted_sum"),
            [(key, len(rows), sum((row["amount"] or 0) * row["qty"] for row in rows)) for key, rows in groups.items()])

    for table, column in (("nulls", "value"), ("empty_table", "eid"), ("numbers", "nullable_n")):
        rows = data[table]
        values = _values(rows, column)
        add(f"null_aggregate_{table}", "null_aggregate", f"""
            SELECT COUNT(*) AS all_n, COUNT({column}) AS nonnull_n,
                   SUM({column}) AS total, AVG({column}) AS mean,
                   MIN({column}) AS low, MAX({column}) AS high,
                   COALESCE(SUM({column}), -999) AS total_default
            FROM warehouse.{table}
        """, ("all_n", "nonnull_n", "total", "mean", "low", "high", "total_default"),
            [(len(rows), len(values), _sum(values), _mean(values), min(values) if values else None,
              max(values) if values else None, sum(values) if values else -999)])
    for predicate, matches, name in (("COUNT(value) = 0", True, "zero_count"), ("SUM(value) IS NULL", True, "null_sum"),
                                     ("SUM(value) = 0", False, "zero_sum")):
        add(f"null_having_{name}", "null_aggregate", f"""
            SELECT COUNT(*) AS n, SUM(value) AS total FROM warehouse.nulls HAVING {predicate}
        """, ("n", "total"), [(len(data["nulls"]), None)] if matches else [])

    window_tables = [("numbers", numbers, "rid", "grp", "n"),
                     ("orders", orders, "oid", "region", "qty"),
                     ("events", events, "eid", "flag", "money")]
    for table, rows, identity, partition_column, sort_column in window_tables:
        for partitioned in (False, True):
            prefix = f"{table}_{'partitioned' if partitioned else 'global'}"
            partitions = _partition(rows, (partition_column,) if partitioned else ())
            partition_sql = f"PARTITION BY {partition_column} " if partitioned else ""
            rank_rows = []
            ntile_rows = {bins: [] for bins in (2, 3, 7)}
            for members in partitions.values():
                members = sorted(members, key=lambda row: (_order_key(row[sort_column]), row[identity]))
                distinct_values = []
                positions = defaultdict(list)
                for index, row in enumerate(members):
                    value = row[sort_column]
                    if value not in distinct_values:
                        distinct_values.append(value)
                    positions[value].append(index)
                for index, row in enumerate(members):
                    value = row[sort_column]
                    first = positions[value][0]
                    rank_rows.append((row[identity], index + 1, first + 1, distinct_values.index(value) + 1,
                                      first / (len(members) - 1) if len(members) > 1 else 0.0,
                                      (positions[value][-1] + 1) / len(members)))
                for bins in ntile_rows:
                    width, remainder = divmod(len(members), bins)
                    assignments = []
                    for bucket in range(1, bins + 1):
                        assignments += [bucket] * (width + int(bucket <= remainder))
                    ntile_rows[bins].extend((row[identity], assignments[index]) for index, row in enumerate(members))
            add(f"rank_{prefix}", "window_rank", f"""
                SELECT {identity},
                       ROW_NUMBER() OVER ({partition_sql}ORDER BY {sort_column} NULLS LAST, {identity}) AS row_no,
                       RANK() OVER ({partition_sql}ORDER BY {sort_column} NULLS LAST) AS rank_no,
                       DENSE_RANK() OVER ({partition_sql}ORDER BY {sort_column} NULLS LAST) AS dense_no,
                       PERCENT_RANK() OVER ({partition_sql}ORDER BY {sort_column} NULLS LAST) AS percent_rank,
                       CUME_DIST() OVER ({partition_sql}ORDER BY {sort_column} NULLS LAST) AS cumulative_dist
                FROM warehouse.{table} ORDER BY {identity}
            """, (identity, "row_no", "rank_no", "dense_no", "percent_rank", "cumulative_dist"), sorted(rank_rows), True)
            for bins, expected in ntile_rows.items():
                add(f"ntile_{prefix}_{bins}", "window_ntile", f"""
                    SELECT {identity}, NTILE({bins}) OVER (
                        {partition_sql}ORDER BY {sort_column} NULLS LAST, {identity}) AS tile
                    FROM warehouse.{table} ORDER BY {identity}
                """, (identity, "tile"), sorted(expected), True)
            for offset in (1, 2, 5):
                shifted = []
                for members in partitions.values():
                    members = sorted(members, key=lambda row: row[identity])
                    for index, row in enumerate(members):
                        previous = members[index - offset][sort_column] if index >= offset else None
                        following = members[index + offset][sort_column] if index + offset < len(members) else None
                        shifted.append((row[identity], previous, following))
                add(f"lag_lead_{prefix}_{offset}", "window_navigation", f"""
                    SELECT {identity}, LAG({sort_column}, {offset}) OVER (
                        {partition_sql}ORDER BY {identity}) AS previous_value,
                        LEAD({sort_column}, {offset}) OVER (
                        {partition_sql}ORDER BY {identity}) AS next_value
                    FROM warehouse.{table} ORDER BY {identity}
                """, (identity, "previous_value", "next_value"), sorted(shifted), True)

    for partitioned in (False, True):
        name = "partitioned" if partitioned else "global"
        partitions = _partition(numbers, ("grp",) if partitioned else ())
        partition_sql = "PARTITION BY grp " if partitioned else ""
        frames = [("cumulative", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", None, 0),
                  ("one_each", "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", 1, 1),
                  ("two_preceding", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", 2, 0),
                  ("two_following", "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING", 0, 2),
                  ("whole", "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", None, None),
                  ("previous_only", "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING", 2, -1)]
        for frame_name, frame_sql, preceding, following in frames:
            outputs = {aggregate: [] for aggregate in ("sum", "avg", "count", "min", "max")}
            first_last = []
            for members in partitions.values():
                members = sorted(members, key=lambda row: row["rid"])
                for index, row in enumerate(members):
                    start = 0 if preceding is None else max(0, index - preceding)
                    stop = len(members) if following is None else min(len(members), index + following + 1)
                    values = [member["nullable_n"] for member in members[start:stop]]
                    for aggregate in outputs:
                        outputs[aggregate].append((row["rid"], _aggregate(values, aggregate)))
                    first_last.append((row["rid"], values[0] if values else None, values[-1] if values else None,
                                       values[1] if len(values) >= 2 else None))
            add(f"frame_aggregates_{name}_{frame_name}", "window_frame", f"""
                SELECT rid,
                       SUM(nullable_n) OVER w AS total,
                       AVG(nullable_n) OVER w AS mean,
                       COUNT(nullable_n) OVER w AS nonnull_n,
                       MIN(nullable_n) OVER w AS low,
                       MAX(nullable_n) OVER w AS high
                FROM warehouse.numbers
                WINDOW w AS ({partition_sql}ORDER BY rid {frame_sql})
                ORDER BY rid
            """, ("rid", "total", "mean", "nonnull_n", "low", "high"),
                [(rid, *[dict(outputs[aggregate])[rid] for aggregate in ("sum", "avg", "count", "min", "max")])
                 for rid in sorted(row["rid"] for row in numbers)], True)
            add(f"frame_navigation_{name}_{frame_name}", "window_frame_navigation", f"""
                SELECT rid, FIRST_VALUE(nullable_n) OVER w AS first_value,
                       LAST_VALUE(nullable_n) OVER w AS last_value,
                       NTH_VALUE(nullable_n, 2) OVER w AS second_value
                FROM warehouse.numbers
                WINDOW w AS ({partition_sql}ORDER BY rid {frame_sql})
                ORDER BY rid
            """, ("rid", "first_value", "last_value", "second_value"), sorted(first_last), True)

    for threshold in (1, 2, 4):
        groups = _partition(orders, ("region",))
        rows_out = []
        for members in groups.values():
            for index, row in enumerate(sorted(members, key=lambda row: (_order_key(row["amount"], True), row["oid"]))):
                if index < threshold:
                    rows_out.append((row["oid"], row["region"], row["amount"], index + 1))
        add(f"qualify_row_number_{threshold}", "qualify", f"""
            SELECT oid, region, amount,
                   ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC NULLS LAST, oid) AS rn
            FROM warehouse.orders QUALIFY rn <= {threshold} ORDER BY oid
        """, ("oid", "region", "amount", "rn"), sorted(rows_out), True)
        rows_out = []
        for members in groups.values():
            quantities = sorted({row["qty"] for row in members}, reverse=True)
            for row in members:
                rank = quantities.index(row["qty"]) + 1
                if rank <= threshold:
                    rows_out.append((row["oid"], row["region"], row["qty"], rank))
        add(f"qualify_dense_rank_{threshold}", "qualify", f"""
            SELECT oid, region, qty,
                   DENSE_RANK() OVER (PARTITION BY region ORDER BY qty DESC) AS dr
            FROM warehouse.orders QUALIFY dr <= {threshold} ORDER BY oid
        """, ("oid", "region", "qty", "dr"), sorted(rows_out), True)

    for offset in (1, 3, 40):
        ordered = sorted(numbers, key=lambda row: row["rid"])
        expected = [(row["rid"], ordered[i - offset]["nullable_n"] if i >= offset else 999,
                     ordered[i + offset]["nullable_n"] if i + offset < len(ordered) else -999)
                    for i, row in enumerate(ordered)]
        add(f"navigation_defaults_{offset}", "window_navigation", f"""
            SELECT rid, LAG(nullable_n, {offset}, 999) OVER (ORDER BY rid) AS previous_value,
                   LEAD(nullable_n, {offset}, -999) OVER (ORDER BY rid) AS next_value
            FROM warehouse.numbers ORDER BY rid
        """, ("rid", "previous_value", "next_value"), expected, True)

    for size in (0, 1, 4):
        expected = []
        for row in numbers:
            members = [member for member in numbers if row["n"] - size <= member["n"] <= row["n"] + size]
            expected.append((row["rid"], sum(member["n"] for member in members), len(members)))
        add(f"range_frame_{size}", "window_range", f"""
            SELECT rid, SUM(n) OVER (ORDER BY n RANGE BETWEEN {size} PRECEDING AND {size} FOLLOWING) AS total,
                   COUNT(*) OVER (ORDER BY n RANGE BETWEEN {size} PRECEDING AND {size} FOLLOWING) AS n_rows
            FROM warehouse.numbers ORDER BY rid
        """, ("rid", "total", "n_rows"), sorted(expected), True)

    groups = _partition(orders, ("region",))
    add("window_distinct_filtered", "window_partition", """
        SELECT oid, COUNT(DISTINCT status) OVER (PARTITION BY region) AS statuses,
               SUM(amount) FILTER (WHERE active IS TRUE) OVER (PARTITION BY region) AS active_amount
        FROM warehouse.orders ORDER BY oid
    """, ("oid", "statuses", "active_amount"),
        [(row["oid"], len(set(_values(groups[(row["region"],)], "status"))),
          _sum([member["amount"] for member in groups[(row["region"],)]
                if member["active"] is True and member["amount"] is not None])) for row in orders], True)

    numeric_expressions = [
        ("cast_small", "CAST(n AS SMALLINT)", lambda row: row["n"]),
        ("cast_double", "CAST(n AS DOUBLE)", lambda row: float(row["n"])),
        ("cast_decimal", "CAST(n AS DECIMAL(12, 3))", lambda row: Decimal(row["n"])),
        ("cast_text", "CAST(n AS VARCHAR)", lambda row: str(row["n"])),
        ("abs", "ABS(n)", lambda row: abs(row["n"])),
        ("sign", "SIGN(n)", lambda row: (row["n"] > 0) - (row["n"] < 0)),
        ("square", "POWER(n, 2)", lambda row: float(row["n"] ** 2)),
        ("sqrt_abs", "SQRT(ABS(n))", lambda row: math.sqrt(abs(row["n"]))),
        ("floor_thirds", "FLOOR(n / 3.0)", lambda row: math.floor(row["n"] / 3)),
        ("ceil_thirds", "CEIL(n / 3.0)", lambda row: math.ceil(row["n"] / 3)),
        ("truncate_thirds", "TRUNC(n / 3.0)", lambda row: math.trunc(row["n"] / 3)),
        ("remainder_three", "n % 3", lambda row: row["n"] - math.trunc(row["n"] / 3) * 3),
        ("round_halves", "ROUND(n / 2.0, 0)", lambda row: (Decimal(row["n"]) / 2).quantize(Decimal(1), rounding=ROUND_HALF_UP)),
        ("nullif_zero", "NULLIF(n, 0)", lambda row: row["n"] if row["n"] != 0 else None),
        ("divide_nullif", "n / NULLIF(n, 0)", lambda row: 1.0 if row["n"] else None),
        ("coalesce", "COALESCE(nullable_n, 777)", lambda row: row["nullable_n"] if row["nullable_n"] is not None else 777),
        ("nested_coalesce", "COALESCE(NULLIF(nullable_n, n), n * n)", lambda row: row["n"] ** 2),
        ("greatest", "GREATEST(n, nullable_n, 0)", lambda row: max([row["n"], 0] + ([] if row["nullable_n"] is None else [row["nullable_n"]]))),
        ("least", "LEAST(n, nullable_n, 0)", lambda row: min([row["n"], 0] + ([] if row["nullable_n"] is None else [row["nullable_n"]]))),
        ("case_nested", "CASE WHEN nullable_n IS NULL THEN -999 WHEN n < 0 THEN ABS(n) * 2 ELSE n + 10 END",
         lambda row: -999 if row["nullable_n"] is None else abs(row["n"]) * 2 if row["n"] < 0 else row["n"] + 10),
        ("boolean_cast", "CAST(n <> 0 AS INTEGER)", lambda row: int(row["n"] != 0)),
        ("nullable_arithmetic", "(nullable_n + 2) * 3 - n", lambda row: None if row["nullable_n"] is None else (row["nullable_n"] + 2) * 3 - row["n"]),
    ]
    for name, expression, oracle in numeric_expressions:
        add(f"numeric_{name}", "numeric_expression", f"SELECT rid, {expression} AS result FROM warehouse.numbers ORDER BY rid",
            ("rid", "result"), [(row["rid"], oracle(row)) for row in numbers], True)

    def try_int(text):
        try:
            return int(text)
        except ValueError:
            return None

    for name, expression, oracle in [
        ("try_integer", "TRY_CAST(code AS INTEGER)", lambda row: try_int(row["code"])),
        ("try_integer_default", "COALESCE(TRY_CAST(code AS INTEGER), 999)", lambda row: try_int(row["code"]) if try_int(row["code"]) is not None else 999),
        ("try_decimal", "TRY_CAST(code AS DECIMAL(10, 2))", lambda row: Decimal(try_int(row["code"])) if try_int(row["code"]) is not None else None),
    ]:
        add(f"cast_{name}", "try_cast", f"SELECT oid, {expression} AS result FROM warehouse.orders ORDER BY oid",
            ("oid", "result"), [(row["oid"], oracle(row)) for row in orders], True)

    date_expressions = [
        ("year", "EXTRACT(YEAR FROM event_date)", lambda row: row["event_date"].year),
        ("month", "EXTRACT(MONTH FROM event_date)", lambda row: row["event_date"].month),
        ("day", "EXTRACT(DAY FROM event_date)", lambda row: row["event_date"].day),
        ("day_of_year", "EXTRACT(DOY FROM event_date)", lambda row: row["event_date"].timetuple().tm_yday),
        ("day_of_week", "EXTRACT(DOW FROM event_date)", lambda row: (row["event_date"].weekday() + 1) % 7),
        ("iso_week", "EXTRACT(WEEK FROM event_date)", lambda row: row["event_date"].isocalendar().week),
        ("quarter", "EXTRACT(QUARTER FROM event_date)", lambda row: (row["event_date"].month - 1) // 3 + 1),
        ("date_add", "event_date + 3", lambda row: row["event_date"] + timedelta(days=3)),
        ("date_subtract", "event_date - 2", lambda row: row["event_date"] - timedelta(days=2)),
        ("date_diff", "DATE_DIFF('day', event_date, DATE '2024-03-15')", lambda row: (date(2024, 3, 15) - row["event_date"]).days),
        ("month_start", "CAST(DATE_TRUNC('month', event_date) AS DATE)", lambda row: row["event_date"].replace(day=1)),
        ("week_start", "CAST(DATE_TRUNC('week', event_date) AS DATE)", lambda row: row["event_date"] - timedelta(days=row["event_date"].weekday())),
        ("format_date", "STRFTIME(event_date, '%Y-%m-%d')", lambda row: row["event_date"].isoformat()),
        ("timestamp_date", "CAST(event_time AS DATE)", lambda row: row["event_time"].date()),
        ("timestamp_hour", "EXTRACT(HOUR FROM event_time)", lambda row: row["event_time"].hour),
        ("timestamp_minute", "EXTRACT(MINUTE FROM event_time)", lambda row: row["event_time"].minute),
        ("timestamp_add", "event_time + INTERVAL '90 minutes'", lambda row: row["event_time"] + timedelta(minutes=90)),
        ("timestamp_subtract", "event_time - INTERVAL '1 day'", lambda row: row["event_time"] - timedelta(days=1)),
        ("timestamp_floor_day", "DATE_TRUNC('day', event_time)", lambda row: row["event_time"].replace(hour=0, minute=0, second=0, microsecond=0)),
        ("timestamp_floor_hour", "DATE_TRUNC('hour', event_time)", lambda row: row["event_time"].replace(minute=0, second=0, microsecond=0)),
        ("timestamp_epoch", "CAST(EPOCH(event_time) AS BIGINT)", lambda row: int((row["event_time"] - datetime(1970, 1, 1)).total_seconds())),
        ("date_as_timestamp", "CAST(event_date AS TIMESTAMP)", lambda row: datetime.combine(row["event_date"], datetime.min.time())),
    ]
    for name, expression, oracle in date_expressions:
        add(f"date_{name}", "date_timestamp", f"SELECT eid, {expression} AS result FROM warehouse.events ORDER BY eid",
            ("eid", "result"), [(row["eid"], oracle(row)) for row in events], True)

    for point in (date(2024, 2, 29), date(2024, 3, 1), date(2024, 3, 15)):
        add(f"date_boundary_{point.isoformat().replace('-', '')}", "date_boundary",
            f"SELECT eid, event_date FROM warehouse.events WHERE event_date <= DATE '{point.isoformat()}' ORDER BY eid",
            ("eid", "event_date"), [(row["eid"], row["event_date"]) for row in events if row["event_date"] <= point], True)

    money_expressions = [
        ("identity", "money", lambda value: value),
        ("add", "money + CAST(0.125 AS DECIMAL(12, 3))", lambda value: value + Decimal("0.125")),
        ("multiply", "money * CAST(2 AS DECIMAL(4, 0))", lambda value: value * 2),
        ("negative", "-money", lambda value: -value),
        ("round_cents", "ROUND(money, 2)", lambda value: value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        ("floor", "FLOOR(money)", lambda value: math.floor(value)),
        ("ceil", "CEIL(money)", lambda value: math.ceil(value)),
        ("truncate_integer", "CAST(TRUNC(money) AS BIGINT)", lambda value: int(value)),
        ("double", "CAST(money AS DOUBLE)", lambda value: float(value)),
    ]
    for name, expression, oracle in money_expressions:
        add(f"decimal_{name}", "decimal", f"SELECT eid, {expression} AS result FROM warehouse.events ORDER BY eid",
            ("eid", "result"), [(row["eid"], oracle(row["money"]) if row["money"] is not None else None) for row in events], True)
    money = _values(events, "money")
    add("decimal_aggregates", "decimal", """
        SELECT COUNT(money) AS n, SUM(money) AS total, AVG(money) AS mean,
               MIN(money) AS low, MAX(money) AS high, SUM(DISTINCT money) AS distinct_total
        FROM warehouse.events
    """, ("n", "total", "mean", "low", "high", "distinct_total"),
        [(len(money), sum(money), float(sum(money) / len(money)), min(money), max(money), sum(set(money)))])
    groups = _partition(events, ("flag",))
    add("decimal_group_null_flag", "decimal", """
        SELECT flag, COUNT(*) AS n, SUM(money) AS total, COUNT(money) AS nonnull_n
        FROM warehouse.events GROUP BY flag HAVING COUNT(*) >= 1
    """, ("flag", "n", "total", "nonnull_n"),
        [key + (len(rows), _sum(_values(rows, "money")), len(_values(rows, "money"))) for key, rows in groups.items()])

    bool_expressions = [
        ("is_true", "flag IS TRUE", lambda row: row["flag"] is True),
        ("is_false", "flag IS FALSE", lambda row: row["flag"] is False),
        ("is_unknown", "flag IS UNKNOWN", lambda row: row["flag"] is None),
        ("not", "NOT flag", lambda row: None if row["flag"] is None else not row["flag"]),
        ("and", "flag AND eid % 2 = 0", lambda row: _sql_and(row["flag"], row["eid"] % 2 == 0)),
        ("or", "flag OR eid % 2 = 0", lambda row: _sql_or(row["flag"], row["eid"] % 2 == 0)),
        ("cast", "CAST(flag AS INTEGER)", lambda row: None if row["flag"] is None else int(row["flag"])),
        ("default", "COALESCE(flag, FALSE)", lambda row: row["flag"] if row["flag"] is not None else False),
        ("case", "CASE flag WHEN TRUE THEN 'yes' WHEN FALSE THEN 'no' ELSE 'missing' END", lambda row: "yes" if row["flag"] is True else "no" if row["flag"] is False else "missing"),
        ("nullif", "NULLIF(flag, TRUE)", lambda row: False if row["flag"] is False else None),
    ]
    for name, expression, oracle in bool_expressions:
        add(f"boolean_{name}", "boolean", f"SELECT eid, {expression} AS result FROM warehouse.events ORDER BY eid",
            ("eid", "result"), [(row["eid"], oracle(row)) for row in events], True)

    ids = [case.case_id for case in cases]
    queries = [" ".join(case.sql.split()) for case in cases]
    if len(ids) != len(set(ids)) or len(queries) != len(set(queries)):
        raise AssertionError("Advanced query IDs and SQL must be unique")
    if len(cases) < 130:
        raise AssertionError(f"Expected at least 130 advanced cases, got {len(cases)}")
    return cases
