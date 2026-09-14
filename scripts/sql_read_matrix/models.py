from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class QueryCase:
    case_id: str
    category: str
    sql: str
    columns: tuple[str, ...]
    expected: list[tuple[Any, ...]]
    ordered: bool = False
    role: str = "superadmin"
    error_contains: tuple[str, ...] = ()
    expected_types: dict[str, str] = field(default_factory=dict)
    min_pruned_files: int = 0
    note: str = ""
    session_timezone: str = "UTC"

    def __post_init__(self):
        if not self.ordered:
            object.__setattr__(self, "expected", sorted(self.expected, key=repr))


Dataset = dict[str, list[dict[str, Any]]]


def load_saved_cases(path):
    import json
    from datetime import date, datetime
    from decimal import Decimal
    from pathlib import Path

    def decode(value):
        if set(value) == {"type", "value"}:
            parsers = {"date": date.fromisoformat, "datetime": datetime.fromisoformat, "decimal": Decimal}
            if value["type"] in parsers:
                return parsers[value["type"]](value["value"])
        return value

    records = json.loads(Path(path).read_text(), object_hook=decode)
    for record in records:
        record["columns"] = tuple(record["columns"])
        record["expected"] = [tuple(row) for row in record["expected"]]
    return [QueryCase(**record) for record in records]
