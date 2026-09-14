from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from .models import Dataset


def build_dataset(include_temporal: bool = False) -> Dataset:
    regions = ("eu", "us", "apac")
    statuses = ("new", "paid", "cancelled", "shipped")
    notes = ("Alpha", "beta", "", "O'Reilly", "München", "東京", None, "a_b%")
    orders = [
        {
            "oid": i,
            "cid": None if i % 17 == 0 else (99 if i % 19 == 0 else i % 12 + 1),
            "region": regions[(i // 30) % 3],
            "status": None if i % 13 == 0 else statuses[i % 4],
            "amount": None if i % 11 == 0 else (i * 37) % 701 - 100,
            "qty": i % 5 + 1,
            "score": None if i % 9 == 0 else (i % 17) / 4.0,
            "active": None if i % 7 == 0 else i % 2 == 0,
            "created": date(2024, 1, 1) + timedelta(days=i % 90),
            "happened": datetime(2024, 1, 1) + timedelta(days=i % 40, hours=i % 24, minutes=i % 60),
            "note": notes[i % len(notes)],
            "code": "bad" if i % 10 == 0 else str(i % 31 - 15),
        }
        for i in range(1, 241)
    ]
    customers = [
        {
            "cid": i,
            "name": f"customer_{i:02d}",
            "region": regions[i % 3],
            "credit": None if i % 5 == 0 else i * 100,
            "active": i % 3 != 0,
        }
        for i in range(1, 17)
    ]
    items = [
        {
            "iid": i,
            "oid": i * 7 % 250 + 1,
            "sku": ("alpha", "beta", "gamma", "delta")[i % 4],
            "units": i % 4 + 1,
            "price": (i * 13) % 91 + 10,
        }
        for i in range(1, 481)
    ]
    numbers = [
        {"rid": i + 16, "n": i, "grp": (i + 15) % 4,
         "nullable_n": None if i % 5 == 0 else i,
         "label": None if i % 6 == 0 else f"v{abs(i):02d}"}
        for i in range(-15, 16)
    ]
    events = [
        {"eid": i, "event_date": date(2024, 2, 27) + timedelta(days=i),
         "event_time": datetime(2024, 2, 27, 23, 30) + timedelta(hours=13 * i),
         "money": None if i % 7 == 0 else Decimal(i - 10) / Decimal(8),
         "flag": None if i % 5 == 0 else i % 2 == 0,
         "label": notes[i % len(notes)]}
        for i in range(1, 25)
    ]
    ledger = [{"lid": i, "value": i * 10, "revision": 1} for i in range(1, 25)]
    for row in ledger:
        if row["lid"] in (3, 9, 18):
            row.update(value=row["value"] + 1000, revision=2)
    ledger = [row for row in ledger if row["lid"] not in (5, 11)]
    ledger += [{"lid": i, "value": i * 10, "revision": 1} for i in (25, 26)]
    evolving = [{"eid": i, "value": i * 10, "extra": None if i < 4 else f"extra_{i}"} for i in range(1, 7)]
    nulls = [{"nid": i, "value": None, "label": None} for i in range(1, 9)]
    result = {"orders": orders, "customers": customers, "items": items,
            "numbers": numbers, "events": events, "ledger": ledger,
            "evolving": evolving, "nulls": nulls, "empty_table": []}
    if include_temporal:
        from .temporal_fixtures import build_tables as temporal_tables
        from .timezone_fixtures import build_tables as timezone_tables
        result.update(temporal_tables())
        result.update(timezone_tables())
    return result


def arrow_schemas(include_temporal: bool = False):
    import pyarrow as pa
    result = {
        "orders": pa.schema([("oid", pa.int64()), ("cid", pa.int64()), ("region", pa.string()),
            ("status", pa.string()), ("amount", pa.int64()), ("qty", pa.int64()),
            ("score", pa.float64()), ("active", pa.bool_()), ("created", pa.date32()),
            ("happened", pa.timestamp("us")), ("note", pa.string()), ("code", pa.string())]),
        "customers": pa.schema([("cid", pa.int64()), ("name", pa.string()), ("region", pa.string()),
            ("credit", pa.int64()), ("active", pa.bool_())]),
        "items": pa.schema([("iid", pa.int64()), ("oid", pa.int64()), ("sku", pa.string()),
            ("units", pa.int64()), ("price", pa.int64())]),
        "numbers": pa.schema([("rid", pa.int64()), ("n", pa.int64()), ("grp", pa.int64()),
            ("nullable_n", pa.int64()), ("label", pa.string())]),
        "events": pa.schema([("eid", pa.int64()), ("event_date", pa.date32()),
            ("event_time", pa.timestamp("us")), ("money", pa.decimal128(12, 3)),
            ("flag", pa.bool_()), ("label", pa.string())]),
        "ledger": pa.schema([("lid", pa.int64()), ("value", pa.int64()), ("revision", pa.int64())]),
        "evolving": pa.schema([("eid", pa.int64()), ("value", pa.int64()), ("extra", pa.string())]),
        "nulls": pa.schema([("nid", pa.int64()), ("value", pa.int64()), ("label", pa.string())]),
        "empty_table": pa.schema([("eid", pa.int64())]),
    }
    if include_temporal:
        from .temporal_fixtures import arrow_schemas as temporal_schemas
        from .timezone_fixtures import arrow_schemas as timezone_schemas
        result.update(temporal_schemas())
        result.update(timezone_schemas())
    return result
