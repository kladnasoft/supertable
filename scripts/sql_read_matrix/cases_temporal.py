from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import operator

from .models import Dataset, QueryCase


def build_cases(data: Dataset) -> list[QueryCase]:
    cases = []
    dates = data["temporal_dates"]
    timestamps = data["temporal_naive"]

    def add(name, sql, columns, expected, category):
        cases.append(QueryCase("temporal_" + name, category, sql, tuple(columns), list(expected), ordered=True))

    def selected(name, table, column, predicate, expected_predicate, category, expression=None):
        projection = "eid" if expression is None else "eid, " + expression
        columns = ("eid",) if expression is None else ("eid", "tested")
        rows = data[table]
        expected = [(row["eid"],) if expression is None else (row["eid"], row[column])
                    for row in rows if expected_predicate(row[column])]
        add(name, f"SELECT {projection} FROM {table} WHERE {predicate} ORDER BY eid", columns, expected, category)

    def literal(value):
        kind = "TIMESTAMP" if isinstance(value, datetime) else "DATE"
        return f"{kind} '{value.isoformat(sep=' ') if isinstance(value, datetime) else value.isoformat()}'"

    comparisons = {"eq": ("=", operator.eq), "ne": ("<>", operator.ne),
                   "lt": ("<", operator.lt), "le": ("<=", operator.le),
                   "gt": (">", operator.gt), "ge": (">=", operator.ge)}
    date_bounds = [date(1899, 12, 31), date(1900, 3, 1), date(1969, 12, 31), date(1970, 1, 1),
                   date(1999, 12, 31), date(2000, 2, 29), date(2023, 2, 28), date(2024, 2, 28),
                   date(2024, 2, 29), date(2024, 3, 1), date(2038, 1, 19), date(2100, 3, 1)]
    time_bounds = [datetime(1900, 3, 1), datetime(1969, 12, 31, 23, 59, 59, 999999),
                   datetime(1970, 1, 1), datetime(1970, 1, 1, microsecond=1),
                   datetime(2000, 2, 29), datetime(2024, 2, 28, 23, 59, 59, 999999),
                   datetime(2024, 2, 29), datetime(2024, 2, 29, 12, 34, 56, 123456),
                   datetime(2024, 2, 29, 12, 34, 56, 123457), datetime(2024, 2, 29, 23, 59, 59, 999999),
                   datetime(2024, 3, 1), datetime(2100, 3, 1)]
    configurations = [("date", "temporal_dates", "event_date", date_bounds),
                      ("timestamp", "temporal_naive", "event_time", time_bounds)]
    for prefix, table, column, bounds in configurations:
        for i, bound in enumerate(bounds):
            for op_name, (op, fn) in comparisons.items():
                selected(f"{prefix}_{op_name}_{i}", table, column, f"{column} {op} {literal(bound)}",
                         lambda value, bound=bound, fn=fn: value is not None and fn(value, bound),
                         "temporal_comparison", f"{column} AS tested")
        range_pairs = [(bounds[2], bounds[3]), (bounds[4], bounds[5]), (bounds[6], bounds[9]),
                       (bounds[7], bounds[8]), (bounds[8], bounds[8]), (bounds[9], bounds[7])]
        for i, (lo, hi) in enumerate(range_pairs):
            predicates = [("between", f"{column} BETWEEN {literal(lo)} AND {literal(hi)}", lambda v, lo=lo, hi=hi: lo <= v <= hi),
                          ("not_between", f"{column} NOT BETWEEN {literal(lo)} AND {literal(hi)}", lambda v, lo=lo, hi=hi: not lo <= v <= hi),
                          ("half_open", f"{column} >= {literal(lo)} AND {column} < {literal(hi)}", lambda v, lo=lo, hi=hi: lo <= v < hi),
                          ("open_closed", f"{column} > {literal(lo)} AND {column} <= {literal(hi)}", lambda v, lo=lo, hi=hi: lo < v <= hi)]
            for label, predicate, fn in predicates:
                selected(f"{prefix}_{label}_{i}", table, column, predicate,
                         lambda v, fn=fn: v is not None and fn(v), "temporal_ranges")
        groups = [(bounds[2], bounds[3]), (bounds[5], bounds[8], bounds[8]),
                  (bounds[0], bounds[4], bounds[11]), (bounds[7], bounds[8], bounds[9])]
        for i, group in enumerate(groups):
            for negated in (False, True):
                for include_null in (False, True):
                    values = ", ".join(literal(value) for value in group) + (", NULL" if include_null else "")
                    selected(f"{prefix}_in_{i}_{negated}_{include_null}", table, column,
                             f"{column} {'NOT ' if negated else ''}IN ({values})",
                             lambda v, group=group, negated=negated, include_null=include_null:
                             v is not None and (False if negated and include_null else ((v not in group) if negated else (v in group))),
                             "temporal_membership")
        boundary = bounds[8]
        predicates = [("null", f"{column} IS NULL", lambda v: v is None),
                      ("not_null", f"{column} IS NOT NULL", lambda v: v is not None),
                      ("or_null", f"{column} < {literal(boundary)} OR {column} IS NULL", lambda v: v is None or v < boundary),
                      ("not_or_null", f"NOT ({column} < {literal(boundary)} OR {column} IS NULL)", lambda v: v is not None and v >= boundary),
                      ("distinct", f"{column} IS DISTINCT FROM {literal(boundary)}", lambda v: v != boundary),
                      ("not_distinct", f"{column} IS NOT DISTINCT FROM {literal(boundary)}", lambda v: v == boundary),
                      ("equals_null", f"{column} = NULL", lambda v: False),
                      ("coalesce", f"COALESCE({column}, {literal(boundary)}) = {literal(boundary)}", lambda v: v is None or v == boundary)]
        for name, predicate, fn in predicates:
            selected(f"{prefix}_{name}", table, column, predicate, fn, "temporal_null_semantics")
        for index in (3, 5, 8, 9):
            bound = bounds[index]
            selected(f"{prefix}_reversed_{index}", table, column, f"{literal(bound)} <= {column}",
                     lambda v, bound=bound: v is not None and bound <= v, "temporal_reversed_comparison")

    date_parts = [("year", lambda v: v.year, (1900, 2000, 2024, 2100)),
                  ("month", lambda v: v.month, (1, 2, 3, 12)),
                  ("day", lambda v: v.day, (1, 28, 29, 31)),
                  ("dow", lambda v: (v.weekday() + 1) % 7, (0, 6)),
                  ("isodow", lambda v: v.isoweekday(), (1, 7)),
                  ("doy", lambda v: v.timetuple().tm_yday, (59, 60, 61, 366))]
    for prefix, table, column, _ in configurations:
        for part, fn, values in date_parts:
            for value in values:
                selected(f"{prefix}_extract_{part}_{value}", table, column, f"EXTRACT({part} FROM {column}) = {value}",
                         lambda v, fn=fn, value=value: v is not None and fn(v) == value, "temporal_datepart_filters")
    for part, fn, values in [("hour", lambda v: v.hour, (0, 12, 23)),
                             ("minute", lambda v: v.minute, (0, 34, 59)),
                             ("microseconds", lambda v: v.second * 1000000 + v.microsecond, (0, 1, 56123456, 59999999))]:
        for value in values:
            selected(f"timestamp_extract_{part}_{value}", "temporal_naive", "event_time",
                     f"EXTRACT({part} FROM event_time) = {value}",
                     lambda v, fn=fn, value=value: v is not None and fn(v) == value, "temporal_subsecond_filters")

    for i, day in enumerate((date(1900, 3, 1), date(1970, 1, 1), date(2000, 2, 29), date(2024, 2, 29), date(2024, 3, 1), date(2100, 3, 1))):
        selected(f"timestamp_cast_date_{i}", "temporal_naive", "event_time", f"CAST(event_time AS DATE) = {literal(day)}",
                 lambda v, day=day: v is not None and v.date() == day, "temporal_cast_filters")
        stamp = datetime.combine(day, time())
        selected(f"date_cast_timestamp_{i}", "temporal_dates", "event_date", f"CAST(event_date AS TIMESTAMP) = {literal(stamp)}",
                 lambda v, day=day: v == day, "temporal_cast_filters")
    for prefix, table, column, _ in configurations:
        for unit in ("year", "quarter", "month", "week", "day"):
            bound = date(2024, 2, 29)
            def truncate(value, unit=unit):
                value = value.date() if isinstance(value, datetime) else value
                if unit == "year":
                    return value.replace(month=1, day=1)
                if unit == "quarter":
                    return value.replace(month=(value.month - 1) // 3 * 3 + 1, day=1)
                if unit == "month":
                    return value.replace(day=1)
                if unit == "week":
                    return value - timedelta(days=value.weekday())
                return value
            selected(f"{prefix}_trunc_{unit}", table, column,
                     f"DATE_TRUNC('{unit}', {column}) = {literal(truncate(bound))}",
                     lambda v, truncate=truncate, bound=bound: v is not None and truncate(v) == truncate(bound),
                     "temporal_truncation_filters")

    reference = date(2024, 2, 28)
    for unit, difference, values in [
            ("day", lambda v: (v - reference).days, (-1, 0, 1, 2)),
            ("month", lambda v: (v.year - reference.year) * 12 + v.month - reference.month, (-1, 0, 1)),
            ("year", lambda v: v.year - reference.year, (-1, 0, 1))]:
        for value in values:
            selected(f"date_diff_{unit}_{value}", "temporal_dates", "event_date",
                     f"DATE_DIFF('{unit}', {literal(reference)}, event_date) = {value}",
                     lambda v, difference=difference, value=value: v is not None and difference(v) == value,
                     "temporal_datediff_filters")
    stamp_reference = datetime(2024, 2, 29)
    for delta in (-1, 0, 1, 999999, 86400000000):
        selected(f"timestamp_diff_microseconds_{delta}", "temporal_naive", "event_time",
                 f"DATE_DIFF('microseconds', {literal(stamp_reference)}, event_time) = {delta}",
                 lambda v, delta=delta: v is not None and (v - stamp_reference) == timedelta(microseconds=delta),
                 "temporal_datediff_filters")
    for days in (-1, 1, 2, 365):
        selected(f"date_interval_days_{days}", "temporal_dates", "event_date",
                 f"event_date + INTERVAL '{days} days' = TIMESTAMP '2024-03-01 00:00:00'",
                 lambda v, days=days: v is not None and v + timedelta(days=days) == date(2024, 3, 1),
                 "temporal_interval_filters")
    for micros in (-1, 1, 1000000, 86400000000):
        selected(f"timestamp_interval_microseconds_{micros}", "temporal_naive", "event_time",
                 f"event_time + INTERVAL '{micros} microseconds' = TIMESTAMP '2024-03-01 00:00:00'",
                 lambda v, micros=micros: v is not None and v + timedelta(microseconds=micros) == datetime(2024, 3, 1),
                 "temporal_interval_filters")

    for prefix, table, column, _ in configurations:
        valid = [row for row in data[table] if row[column] is not None]
        by_year = defaultdict(list)
        for row in valid:
            by_year[row[column].year].append(row)
        add(f"{prefix}_aggregate_year", f"SELECT EXTRACT(year FROM {column}) AS yr, COUNT(*) AS n, MIN({column}) AS first_value, MAX({column}) AS last_value FROM {table} WHERE {column} IS NOT NULL GROUP BY yr ORDER BY yr",
            ("yr", "n", "first_value", "last_value"),
            [(year, len(rows), min(row[column] for row in rows), max(row[column] for row in rows)) for year, rows in sorted(by_year.items())], "temporal_aggregates")
        add(f"{prefix}_cte_filter", f"WITH dated AS (SELECT eid, {column}, EXTRACT(year FROM {column}) AS yr FROM {table}) SELECT eid FROM dated WHERE yr = 2024 AND {column} >= DATE '2024-02-29' ORDER BY eid",
            ("eid",), [(row["eid"],) for row in valid if row[column].year == 2024 and (row[column].date() if isinstance(row[column], datetime) else row[column]) >= date(2024, 2, 29)], "temporal_cte_filters")
        first = [sorted(rows, key=lambda row: (row[column], row["eid"]))[0] for rows in by_year.values()]
        add(f"{prefix}_window_first", f"SELECT eid FROM {table} WHERE {column} IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY EXTRACT(year FROM {column}) ORDER BY {column}, eid) = 1 ORDER BY eid",
            ("eid",), [(row["eid"],) for row in sorted(first, key=lambda row: row["eid"])], "temporal_windows")
        add(f"{prefix}_correlated_min", f"SELECT a.eid FROM {table} a WHERE a.{column} = (SELECT MIN(b.{column}) FROM {table} b WHERE b.grp = a.grp) ORDER BY a.eid",
            ("eid",), [(row["eid"],) for row in valid if row[column] == min(other[column] for other in valid if other["grp"] == row["grp"])], "temporal_correlated_filters")
    joined = [(stamp["eid"], day["eid"]) for stamp in timestamps for day in dates
              if stamp["event_time"] is not None and day["event_date"] is not None and stamp["event_time"].date() == day["event_date"]]
    add("join_date_cast", "SELECT t.eid AS timestamp_id, d.eid AS date_id FROM temporal_naive t JOIN temporal_dates d ON CAST(t.event_time AS DATE) = d.event_date ORDER BY timestamp_id, date_id",
        ("timestamp_id", "date_id"), sorted(joined), "temporal_joins")
    add("join_date_range", "SELECT t.eid AS timestamp_id, d.eid AS date_id FROM temporal_naive t JOIN temporal_dates d ON t.event_time >= CAST(d.event_date AS TIMESTAMP) AND t.event_time < CAST(d.event_date AS TIMESTAMP) + INTERVAL '1 day' ORDER BY timestamp_id, date_id",
        ("timestamp_id", "date_id"), sorted(joined), "temporal_joins")
    add("join_exists", "SELECT d.eid FROM temporal_dates d WHERE EXISTS (SELECT 1 FROM temporal_naive t WHERE CAST(t.event_time AS DATE) = d.event_date) ORDER BY d.eid",
        ("eid",), [(day["eid"],) for day in dates if day["event_date"] is not None and any(stamp["event_time"] is not None and stamp["event_time"].date() == day["event_date"] for stamp in timestamps)], "temporal_joins")
    return cases
