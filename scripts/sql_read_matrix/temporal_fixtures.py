from __future__ import annotations

from datetime import date, datetime, time, timedelta


def build_tables():
    anchors = [date(1900, 3, 1), date(1969, 12, 31), date(1970, 1, 1),
               date(1999, 12, 31), date(2000, 2, 29), date(2023, 2, 28),
               date(2023, 12, 31), date(2024, 2, 29), date(2024, 3, 31),
               date(2024, 10, 27), date(2024, 12, 31), date(2038, 1, 19),
               date(2050, 6, 30), date(2100, 3, 1)]
    dates = sorted({anchor + timedelta(days=offset) for anchor in anchors for offset in range(-3, 4)})
    dates += [date(2000, 2, 29), date(2024, 2, 29), date(2024, 2, 29)]
    dates = sorted(dates) + [None] * 4
    days = [{"eid": i, "event_date": value, "grp": i % 4, "amount": i * 7 - 50}
            for i, value in enumerate(dates, 1)]
    moments = []
    for day in anchors:
        midnight = datetime.combine(day, time())
        moments.extend(midnight + timedelta(microseconds=offset) for offset in
                       (-86400000001, -1, 0, 1, 999999, 1000000, 43200000000,
                        86399999998, 86399999999, 86400000000, 86400000001))
    moments += [datetime(2024, 2, 29, 12, 34, 56, 123456),
                datetime(2024, 2, 29, 12, 34, 56, 123456),
                datetime(2024, 2, 29, 12, 34, 56, 123457)]
    moments = sorted(moments) + [None] * 5
    timestamps = [{"eid": i, "event_time": value, "grp": i % 5, "amount": i * 3 - 80}
                  for i, value in enumerate(moments, 1)]
    return {"temporal_dates": days, "temporal_naive": timestamps}


def arrow_schemas():
    import pyarrow as pa
    return {
        "temporal_dates": pa.schema([("eid", pa.int64()), ("event_date", pa.date32()),
                                      ("grp", pa.int64()), ("amount", pa.int64())]),
        "temporal_naive": pa.schema([("eid", pa.int64()), ("event_time", pa.timestamp("us")),
                                      ("grp", pa.int64()), ("amount", pa.int64())]),
    }
