from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


UTC = timezone.utc


def build_tables():
    centers = [
        datetime(2011, 12, 30, 10, tzinfo=UTC),
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 3, 10, 7, tzinfo=UTC),
        datetime(2024, 3, 31, 1, tzinfo=UTC),
        datetime(2024, 4, 6, 15, tzinfo=UTC),
        datetime(2024, 10, 5, 15, 30, tzinfo=UTC),
        datetime(2024, 10, 27, 1, tzinfo=UTC),
        datetime(2024, 11, 3, 6, tzinfo=UTC),
    ]
    instants = set()
    for center in centers:
        instants.update(center + timedelta(minutes=30 * i) for i in range(-72, 73))
        instants.update(center + timedelta(microseconds=i) for i in (-2, -1, 1, 2))
    for day in ((2024, 1, 1), (2024, 3, 31), (2024, 10, 27)):
        center = datetime(*day, tzinfo=UTC)
        for zone_name in ("Europe/Budapest", "America/New_York", "Asia/Kathmandu",
                          "Australia/Lord_Howe", "Pacific/Apia", "Pacific/Kiritimati"):
            midnight = center.replace(tzinfo=ZoneInfo(zone_name)).astimezone(UTC)
            instants.update(midnight + timedelta(microseconds=i) for i in (-1, 0, 1))
    rows = [
        {"tid": i, "instant": instant, "wall_utc": instant.replace(tzinfo=None),
         "utc_day": instant.date(), "nullable_instant": None if i % 7 == 0 else instant}
        for i, instant in enumerate(sorted(instants), 1)
    ]
    budapest = ZoneInfo("Europe/Budapest")
    local_storage = [{"tid": row["tid"], "instant": row["instant"].astimezone(budapest)}
                     for row in rows if row["instant"].year == 2024
                     and row["instant"].month in (3, 10)]
    return {"tz_instants": rows, "tz_budapest_storage": local_storage}


def arrow_schemas():
    import pyarrow as pa

    return {
        "tz_instants": pa.schema([
            ("tid", pa.int64()), ("instant", pa.timestamp("us", tz="UTC")),
            ("wall_utc", pa.timestamp("us")), ("utc_day", pa.date32()),
            ("nullable_instant", pa.timestamp("us", tz="UTC")),
        ]),
        "tz_budapest_storage": pa.schema([
            ("tid", pa.int64()), ("instant", pa.timestamp("us", tz="Europe/Budapest")),
        ]),
    }
