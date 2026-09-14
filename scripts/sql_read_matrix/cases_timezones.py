from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import operator
from zoneinfo import ZoneInfo

from .models import Dataset, QueryCase


UTC = timezone.utc
ZONE_DAYS = {
    "UTC": (date(2024, 1, 1), date(2024, 3, 31), date(2024, 10, 27)),
    "Europe/Budapest": (date(2024, 1, 1), date(2024, 3, 31), date(2024, 10, 27)),
    "America/New_York": (date(2024, 1, 1), date(2024, 3, 10), date(2024, 11, 3)),
    "Asia/Kathmandu": (date(2024, 1, 1), date(2024, 3, 31), date(2024, 10, 27)),
    "Australia/Lord_Howe": (date(2024, 1, 1), date(2024, 4, 7), date(2024, 10, 6)),
    "Pacific/Apia": (date(2011, 12, 29), date(2011, 12, 31), date(2024, 1, 1)),
    "Pacific/Kiritimati": (date(2024, 1, 1), date(2024, 3, 31), date(2024, 10, 27)),
}


def _literal(value):
    return "TIMESTAMPTZ '" + value.isoformat(sep=" ", timespec="microseconds") + "'"


def _key(value):
    return str(value).replace("/", "_").replace("-", "").lower()


def build_cases(data: Dataset) -> list[QueryCase]:
    cases = []
    rows = data["tz_instants"]

    def add(name, predicate, selected=None, columns=("tid",), projection="tid",
            zone="UTC", category="timezone_filters", table="tz_instants", note=""):
        source = data[table]
        expected = [(row["tid"],) for row in source if predicate(row)] if selected is None else selected
        cases.append(QueryCase(
            "tz_" + name, category,
            f"SELECT {projection} FROM {table} WHERE {predicate.sql} ORDER BY tid",
            columns, expected, ordered=True, session_timezone=zone, note=note,
        ))

    def pred(sql, function):
        function.sql = sql
        return function

    for zone_name, days in ZONE_DAYS.items():
        zone = ZoneInfo(zone_name)
        zone_key = _key(zone_name)
        for day in days:
            tomorrow = day + timedelta(days=1)
            lo = datetime.combine(day, time(), zone).astimezone(UTC)
            hi = datetime.combine(tomorrow, time(), zone).astimezone(UTC)
            key = f"{zone_key}_{_key(day)}"
            duration = (hi - lo).total_seconds() / 3600
            note = f"Independent ZoneInfo local-day bounds; this day spans {duration:g} elapsed hours."
            native = lambda r, lo=lo, hi=hi: lo <= r["instant"] < hi
            for name, sql in (
                ("explicit_bounds", f"instant >= {_literal(lo)} AND instant < {_literal(hi)}"),
                ("session_date_bounds", f"instant >= DATE '{day}' AND instant < DATE '{tomorrow}'"),
                ("session_timestamp_bounds", f"instant >= TIMESTAMP '{day} 00:00:00' AND instant < TIMESTAMP '{tomorrow} 00:00:00'"),
                ("session_date_cast", f"CAST(instant AS DATE) = DATE '{day}'"),
                ("at_timezone_date", f"CAST(instant AT TIME ZONE '{zone_name}' AS DATE) = DATE '{day}'"),
            ):
                add(key + "_" + name, pred(sql, native), zone=zone_name, note=note)
            add(key + "_closed_between", pred(
                f"instant BETWEEN {_literal(lo)} AND {_literal(hi)}",
                lambda r, lo=lo, hi=hi: lo <= r["instant"] <= hi), zone=zone_name)
            add(key + "_local_morning", pred(
                f"CAST(instant AT TIME ZONE '{zone_name}' AS DATE) = DATE '{day}' "
                f"AND EXTRACT(HOUR FROM instant AT TIME ZONE '{zone_name}') < 6",
                lambda r, day=day, zone=zone: r["instant"].astimezone(zone).date() == day
                and r["instant"].astimezone(zone).hour < 6), zone=zone_name)
            add(key + "_utc_wall_date", pred(
                f"wall_utc >= DATE '{day}' AND wall_utc < DATE '{tomorrow}'",
                lambda r, day=day: r["wall_utc"].date() == day), zone=zone_name,
                category="timezone_naive_timestamps")

        day = days[-1]
        lo = datetime.combine(day, time(), zone).astimezone(UTC)
        hi = datetime.combine(day + timedelta(days=1), time(), zone).astimezone(UTC)
        scope = pred(f"instant >= {_literal(lo)} AND instant < {_literal(hi)}",
                     lambda r, lo=lo, hi=hi: lo <= r["instant"] < hi)
        selected_rows = [r for r in rows if scope(r)]
        add(zone_key + "_local_projection", scope,
            [(r["tid"], r["instant"].astimezone(zone).replace(tzinfo=None),
              r["instant"].astimezone(zone).date(), r["instant"].astimezone(zone).hour)
             for r in selected_rows],
            ("tid", "local_time", "local_date", "local_hour"),
            f"tid, instant AT TIME ZONE '{zone_name}' AS local_time, "
            f"CAST(instant AS DATE) AS local_date, EXTRACT(HOUR FROM instant) AS local_hour",
            zone=zone_name, category="timezone_projection")
        add(zone_key + "_session_cast_timestamp", scope,
            [(r["tid"], r["instant"].astimezone(zone).replace(tzinfo=None)) for r in selected_rows],
            ("tid", "local_time"), "tid, CAST(instant AS TIMESTAMP) AS local_time",
            zone=zone_name, category="timezone_projection")
        add(zone_key + "_session_offsets", scope,
            [(r["tid"], int(r["instant"].astimezone(zone).utcoffset().total_seconds())) for r in selected_rows],
            ("tid", "offset_seconds"), "tid, EXTRACT(TIMEZONE FROM instant) AS offset_seconds",
            zone=zone_name, category="timezone_offsets")
        add(zone_key + "_aware_projection", scope,
            [(r["tid"], r["instant"]) for r in selected_rows],
            ("tid", "instant"), "tid, instant", zone=zone_name, category="timezone_projection")
        add(zone_key + "_utc_epoch_us", scope,
            [(r["tid"], (r["instant"] - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1))
             for r in selected_rows], ("tid", "epoch_microseconds"),
            "tid, EPOCH_US(instant) AS epoch_microseconds", zone=zone_name,
            category="timezone_offsets")
        noon = datetime(2024, 1, 1, 12)
        add(zone_key + "_naive_to_aware", pred("wall_utc = TIMESTAMP '2024-01-01 12:00:00'",
            lambda r: r["wall_utc"] == noon),
            [(r["tid"], noon.replace(tzinfo=zone).astimezone(UTC)) for r in rows if r["wall_utc"] == noon],
            ("tid", "converted"), "tid, CAST(wall_utc AS TIMESTAMPTZ) AS converted",
            zone=zone_name, category="timezone_naive_timestamps",
            note="Noon is unambiguous and existent in every tested zone.")
        add(zone_key + "_explicit_wall_to_aware", pred("wall_utc = TIMESTAMP '2024-01-01 12:00:00'",
            lambda r: r["wall_utc"] == noon),
            [(r["tid"], noon.replace(tzinfo=zone).astimezone(UTC)) for r in rows if r["wall_utc"] == noon],
            ("tid", "converted"), f"tid, wall_utc AT TIME ZONE '{zone_name}' AS converted",
            zone=zone_name, category="timezone_naive_timestamps")
        add(zone_key + "_day_disagreement", pred(
            "CAST(instant AS DATE) <> utc_day",
            lambda r, zone=zone: r["instant"].astimezone(zone).date() != r["utc_day"]),
            zone=zone_name, category="timezone_local_midnight")

    targets = [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 31, 1, tzinfo=UTC),
               datetime(2024, 10, 27, 1, tzinfo=UTC)]
    offsets = (-600, -300, 0, 120, 345, 630, 840)
    for ti, target in enumerate(targets):
        for offset in offsets:
            equivalent = target.astimezone(timezone(timedelta(minutes=offset)))
            for name, sql_op, fn in (("eq", "=", operator.eq), ("lt", "<", operator.lt),
                                     ("ge", ">=", operator.ge)):
                add(f"offset_{ti}_{offset}_{name}", pred(
                    f"instant {sql_op} {_literal(equivalent)}",
                    lambda r, target=target, fn=fn: fn(r["instant"], target)),
                    category="timezone_offset_literals", note="Different explicit offsets denote the same UTC instant.")
    for ti, target in enumerate(targets):
        bound = target + timedelta(microseconds=1)
        for name, sql_op, fn in (("eq", "=", operator.eq), ("ne", "<>", operator.ne),
                                 ("lt", "<", operator.lt), ("le", "<=", operator.le),
                                 ("gt", ">", operator.gt), ("ge", ">=", operator.ge)):
            add(f"microsecond_{ti}_{name}", pred(f"instant {sql_op} {_literal(bound)}",
                lambda r, bound=bound, fn=fn: fn(r["instant"], bound)), category="timezone_microseconds")

    for name, sql, fn in (
        ("is_null", "nullable_instant IS NULL", lambda r: r["nullable_instant"] is None),
        ("not_null", "nullable_instant IS NOT NULL", lambda r: r["nullable_instant"] is not None),
        ("null_equal", "nullable_instant = NULL", lambda r: False),
        ("not_in_null", "nullable_instant NOT IN (TIMESTAMPTZ '2024-01-01 00:00:00+00', NULL)", lambda r: False),
    ):
        add(name, pred(sql, fn), category="timezone_nulls")

    for zone_name in ZONE_DAYS:
        zone = ZoneInfo(zone_name)
        add("stored_zone_" + _key(zone_name), pred("tid > 0", lambda r: True),
            [(r["tid"], r["instant"].astimezone(UTC), r["instant"].astimezone(zone).date())
             for r in data["tz_budapest_storage"]],
            ("tid", "instant", "local_date"), "tid, instant, CAST(instant AS DATE) AS local_date",
            zone=zone_name, table="tz_budapest_storage", category="timezone_storage",
            note="Physical Arrow schema declares Europe/Budapest; values must retain their absolute instants.")
    add("apia_skipped_local_date", pred(
        "CAST(instant AT TIME ZONE 'Pacific/Apia' AS DATE) = DATE '2011-12-30'",
        lambda r: r["instant"].astimezone(ZoneInfo("Pacific/Apia")).date() == date(2011, 12, 30)),
        zone="Pacific/Apia", category="timezone_calendar_discontinuity",
        note="Apia skipped this civil date. No nonexistent local literal is converted to an instant.")
    for zone_name, fall_day, repeated_hour in (
        ("Europe/Budapest", date(2024, 10, 27), 2),
        ("America/New_York", date(2024, 11, 3), 1),
        ("Australia/Lord_Howe", date(2024, 4, 7), 1),
    ):
        zone = ZoneInfo(zone_name)
        add("fold_" + _key(zone_name), pred(
            f"CAST(instant AT TIME ZONE '{zone_name}' AS DATE) = DATE '{fall_day}' "
            f"AND EXTRACT(HOUR FROM instant AT TIME ZONE '{zone_name}') = {repeated_hour}",
            lambda r, zone=zone, fall_day=fall_day, repeated_hour=repeated_hour:
                r["instant"].astimezone(zone).date() == fall_day
                and r["instant"].astimezone(zone).hour == repeated_hour),
            zone=zone_name, category="timezone_dst_fold",
            note="Both absolute occurrences of repeated wall-clock times remain distinct result rows.")
    for zone_name, wall in (
        ("Europe/Budapest", datetime(2024, 10, 27, 2)),
        ("America/New_York", datetime(2024, 11, 3, 1)),
        ("Australia/Lord_Howe", datetime(2024, 4, 7, 1, 30)),
    ):
        zone = ZoneInfo(zone_name)
        for fold in (0, 1):
            instant = wall.replace(tzinfo=zone, fold=fold)
            target = instant.astimezone(UTC)
            add(f"fold_explicit_{_key(zone_name)}_{fold}", pred(
                f"instant = {_literal(instant)}",
                lambda r, target=target: r["instant"] == target),
                zone=zone_name, category="timezone_dst_fold",
                note="An explicit UTC offset chooses one occurrence of the repeated local timestamp.")
        add("fold_wall_equality_" + _key(zone_name), pred(
            f"instant AT TIME ZONE '{zone_name}' = TIMESTAMP '{wall.isoformat(sep=' ')}'",
            lambda r, zone=zone, wall=wall: r["instant"].astimezone(zone).replace(tzinfo=None) == wall),
            zone=zone_name, category="timezone_dst_fold",
            note="A wall-time equality matches both folds; no ambiguous wall time is converted to an instant.")
    for zone_name, day, hour, max_minute in (
        ("Europe/Budapest", date(2024, 3, 31), 2, 60),
        ("America/New_York", date(2024, 3, 10), 2, 60),
        ("Australia/Lord_Howe", date(2024, 10, 6), 2, 30),
    ):
        zone = ZoneInfo(zone_name)
        add("spring_gap_" + _key(zone_name), pred(
            f"CAST(instant AT TIME ZONE '{zone_name}' AS DATE) = DATE '{day}' "
            f"AND EXTRACT(HOUR FROM instant AT TIME ZONE '{zone_name}') = {hour} "
            f"AND EXTRACT(MINUTE FROM instant AT TIME ZONE '{zone_name}') < {max_minute}",
            lambda r, zone=zone, day=day, hour=hour, max_minute=max_minute:
                r["instant"].astimezone(zone).date() == day
                and r["instant"].astimezone(zone).hour == hour
                and r["instant"].astimezone(zone).minute < max_minute),
            zone=zone_name, category="timezone_dst_gap",
            note="The skipped local interval contains no instant; Lord Howe skips only 30 minutes.")
    return cases
