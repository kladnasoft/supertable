from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .models import Dataset, QueryCase


UTC = timezone.utc
ZONES = (
    "UTC", "Europe/Budapest", "America/New_York", "Asia/Kathmandu",
    "Australia/Lord_Howe", "Pacific/Apia", "Pacific/Kiritimati",
)


def build_cases(data: Dataset) -> list[QueryCase]:
    rows = data["tz_instants"]
    cases = []
    midnight = datetime(2024, 1, 1)
    tomorrow = midnight + timedelta(days=1)
    noon = midnight + timedelta(hours=12)
    utc_midnight = midnight.replace(tzinfo=UTC)
    january_lo = datetime(2023, 12, 30)
    january_hi = datetime(2024, 1, 3)
    january_sql = (
        "wall_utc >= TIMESTAMP '2023-12-30 00:00:00' "
        "AND wall_utc < TIMESTAMP '2024-01-03 00:00:00'"
    )

    def add(zone_name, name, sql, predicate, category="timezone_literal_coercion", minimum=0, note=""):
        cases.append(QueryCase(
            case_id="coercion_" + zone_name.lower().replace("/", "_") + "_" + name,
            category=category,
            sql=f"SELECT tid FROM tz_instants WHERE {sql} ORDER BY tid",
            columns=("tid",),
            expected=[(row["tid"],) for row in rows if predicate(row)],
            ordered=True,
            session_timezone=zone_name,
            min_pruned_files=minimum,
            note=note,
        ))

    for zone_name in ZONES:
        zone = ZoneInfo(zone_name)
        local_lo = midnight.replace(tzinfo=zone).astimezone(UTC)
        local_hi = tomorrow.replace(tzinfo=zone).astimezone(UTC)

        def in_local_day(row):
            return local_lo <= row["instant"] < local_hi

        for name, lo, hi in (
            ("bare_date_aware", "2024-01-01", "2024-01-02"),
            ("bare_datetime_aware", "2024-01-01 00:00:00", "2024-01-02 00:00:00"),
            ("bare_iso_local_aware", "2024-01-01T00:00:00", "2024-01-02T00:00:00"),
        ):
            add(zone_name, name, f"instant >= '{lo}' AND instant < '{hi}'", in_local_day,
                note="An offset-free string compared with TIMESTAMPTZ denotes session-local time.")

        add(zone_name, "bare_date_naive",
            "wall_utc >= '2024-01-01' AND wall_utc < '2024-01-02'",
            lambda row: midnight <= row["wall_utc"] < tomorrow,
            note="A string compared with TIMESTAMP denotes naive wall time independently of the session zone.")

        for name, literal in (
            ("bare_iso_z", "'2024-01-01T00:00:00Z'"),
            ("bare_short_offset", "'2024-01-01 00:00:00+00'"),
            ("bare_compact_offset", "'2024-01-01 00:00:00+0000'"),
            ("typed_iso_z", "TIMESTAMPTZ '2024-01-01T00:00:00Z'"),
        ):
            add(zone_name, name, f"instant = {literal}",
                lambda row: row["instant"] == utc_midnight,
                category="timezone_offset_spelling",
                note="Explicit zero-offset spellings identify the same absolute UTC instant in every session zone.")

        add(zone_name, "bare_offset_naive",
            "wall_utc = '2024-01-01T12:00:00+05:45'",
            lambda row: row["wall_utc"] == noon,
            note="DuckDB TIMESTAMP coercion retains the stated wall time and discards the input offset.")

        noon_instant = noon.replace(tzinfo=zone).astimezone(UTC)
        add(zone_name, "naive_column_aware_equality",
            f"wall_utc = TIMESTAMPTZ '{noon_instant.isoformat(sep=' ')}'",
            lambda row: row["wall_utc"] == noon,
            category="timezone_reverse_coercion",
            note="The naive column is interpreted in the session zone when compared with an aware literal; January noon is unambiguous.")

        def january(row):
            return january_lo <= row["wall_utc"] < january_hi

        def naive_as_instant(row):
            return row["wall_utc"].replace(tzinfo=zone).astimezone(UTC)

        add(zone_name, "naive_column_aware_range",
            january_sql + " AND wall_utc >= TIMESTAMPTZ '2024-01-01 00:00:00+00' "
            "AND wall_utc < TIMESTAMPTZ '2024-01-02 00:00:00+00'",
            lambda row: january(row) and utc_midnight <= naive_as_instant(row) < utc_midnight + timedelta(days=1),
            category="timezone_reverse_coercion",
            note="A UTC interval filters session-local naive values; the independent oracle converts only unambiguous January dates.")

        add(zone_name, "aware_naive_column_equality", january_sql + " AND instant = wall_utc",
            lambda row: january(row) and row["instant"] == naive_as_instant(row),
            category="timezone_column_coercion",
            note="Identical stored clock fields represent equal instants only when the session-local UTC offset is zero.")
        add(zone_name, "aware_naive_column_ordering", january_sql + " AND instant < wall_utc",
            lambda row: january(row) and row["instant"] < naive_as_instant(row),
            category="timezone_column_coercion",
            note="Column-to-column comparison must account for the session-local interpretation of the naive timestamp.")

        add(zone_name, "session_date_pruning_guard",
            "instant >= DATE '2024-01-01' AND instant < DATE '2024-01-02'",
            in_local_day,
            category="timezone_pruning_guard", minimum=1,
            note="The sorted fixture spans 2011 and several 2024 seasons; this supported date interval must prune at least one file while retaining every matching row.")

    return cases
