# route: test_suite.shadow
"""An independent model of what the table should contain.

THE POINT OF THIS FILE

The suite proves SuperTable correct, so its expectations cannot come from
SuperTable. This module is a plain-Python reimplementation of the write
semantics — a dict and some arithmetic, no parquet, no Redis, no SQL — and the
checksums the suite asserts are computed from *it*. If the two ever disagree,
one of them is wrong, and they share no code.

THE SEMANTICS MODELLED HERE WERE MEASURED, NOT ASSUMED

Each rule below was established by running the real writer and reading the
result back. Two of them are easy to get wrong from first principles and would
have made this model quietly disagree with a correct implementation:

  * ``newer_than`` is **strictly** greater. Re-writing a key at its current
    revision is rejected, not applied.
  * A single write carrying the **same key twice** keeps *both* rows: the
    writer does not deduplicate within an incoming batch, and there is no
    read-side dedup to clean it up afterwards. A dict-shaped model cannot
    represent that, so the workload never generates it — see
    ``WriteWorkload._keys_for``.

APPEND HAS NO DEDUP

``overwrite_columns=[]`` is a pure append. Writing an existing key that way
produces a genuine duplicate row that stays visible. Only ``overwrite_columns``
(upsert) and ``delete_only`` remove anything, by tombstoning the superseded
``__rowid__``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

#: Business columns of the workload table. ``rev`` doubles as the ``newer_than``
#: watermark so stale-update rejection can be exercised.
COLUMNS: Tuple[str, ...] = ("id", "name", "amount", "qty", "grp", "rev", "event_date")

#: The primary key. A single column keeps the model honest and readable; the
#: writer's key handling is exercised by using it as ``overwrite_columns``.
KEY = "id"


def canonical(row: Dict[str, object]) -> str:
    """Serialize a row so equal rows hash equal, across processes and types.

    ``default=str`` normalises ``date``/``Decimal`` to their textual form, which
    is what makes a Python-side row and a parquet round-tripped row comparable
    without the model having to know about Arrow types.
    """
    return json.dumps(
        {k: row[k] for k in COLUMNS},
        sort_keys=True, separators=(",", ":"), default=str,
    )


def row_checksum(row: Dict[str, object]) -> str:
    """Stable per-row digest. Short prefix: enough to separate, easy to eyeball."""
    return hashlib.sha256(canonical(row).encode()).hexdigest()[:16]


def table_checksum(rows: Iterable[Dict[str, object]]) -> str:
    """Order-independent digest of a whole table.

    Sorted per-row digests, so the result does not depend on row order — the
    read path makes no ordering guarantee without an explicit ORDER BY, and a
    checksum that drifted with plan changes would be useless.
    """
    digests = sorted(row_checksum(r) for r in rows)
    return hashlib.sha256("|".join(digests).encode()).hexdigest()


@dataclass
class Shadow:
    """The expected table contents, keyed by primary key.

    Also records a fingerprint after every transaction. The suite asserts only
    at the end, as specified — but when the end disagrees, the per-transaction
    trail is what identifies the transaction that diverged instead of leaving a
    120-transaction haystack.
    """

    rows: Dict[object, Dict[str, object]] = field(default_factory=dict)
    history: List[Dict[str, object]] = field(default_factory=list)

    # ---------------- state transitions ----------------

    def insert(self, batch: List[Dict[str, object]]) -> None:
        """Append rows whose keys are absent.

        Restricted to absent keys deliberately: appending an existing key is a
        duplicate-producing operation this dict cannot represent (see module
        docstring), so the workload never asks for it.
        """
        for row in batch:
            key = row[KEY]
            assert key not in self.rows, (
                f"insert would duplicate key {key!r}; a dict model cannot "
                f"represent the two rows the writer would keep"
            )
            self.rows[key] = dict(row)

    def upsert(self, batch: List[Dict[str, object]]) -> None:
        """Replace matching keys, insert the rest. Mirrors overwrite_columns=[KEY]."""
        for row in batch:
            self.rows[row[KEY]] = dict(row)

    def delete(self, keys: List[object]) -> None:
        """Remove keys that exist; a key that does not exist is a no-op."""
        for key in keys:
            self.rows.pop(key, None)

    def upsert_if_newer(self, batch: List[Dict[str, object]], watermark: str) -> None:
        """Apply a row only when its watermark strictly exceeds the stored one.

        Strictly: an equal watermark is rejected. A key that is absent has no
        watermark to beat and is inserted.
        """
        for row in batch:
            key = row[KEY]
            current = self.rows.get(key)
            if current is None or row[watermark] > current[watermark]:
                self.rows[key] = dict(row)

    # ---------------- observation ----------------

    def record(self, index: int, kind: str, detail: object) -> None:
        self.history.append({
            "transaction": index,
            "kind": kind,
            "detail": detail,
            "row_count": len(self.rows),
            "checksum": self.checksum(),
        })

    def checksum(self) -> str:
        return table_checksum(self.rows.values())

    def sorted_rows(self) -> List[Tuple]:
        """Rows as tuples in key order, for an ordered comparison."""
        return [
            tuple(self.rows[key][column] for column in COLUMNS)
            for key in sorted(self.rows)
        ]

    def checksums_by_key(self) -> Dict[object, str]:
        return {key: row_checksum(row) for key, row in self.rows.items()}

    def divergence(self, actual_rows: List[Dict[str, object]]) -> Optional[str]:
        """Explain the first difference against *actual_rows*, or None if equal.

        Produces the diagnosis the checksum assertion cannot: which keys are
        missing, which were resurrected, and which differ in a column — plus the
        transaction after which the expected checksum last matched.
        """
        actual = {row[KEY]: row for row in actual_rows}
        if len(actual) != len(actual_rows):
            duplicated = len(actual_rows) - len(actual)
            return (f"{duplicated} duplicate key(s) in the result: the reader "
                    f"returned more than one row for the same {KEY}")

        missing = sorted(set(self.rows) - set(actual))
        if missing:
            return (f"{len(missing)} row(s) missing, e.g. {KEY}={missing[:5]} — "
                    f"a live row was dropped (over-aggressive tombstone or prune)")

        extra = sorted(set(actual) - set(self.rows))
        if extra:
            return (f"{len(extra)} row(s) resurrected, e.g. {KEY}={extra[:5]} — "
                    f"a deleted or superseded row came back")

        for key in sorted(self.rows):
            expected_row, actual_row = self.rows[key], actual[key]
            for column in COLUMNS:
                if str(expected_row[column]) != str(actual_row[column]):
                    return (f"{KEY}={key} column {column!r}: "
                            f"expected {expected_row[column]!r}, "
                            f"got {actual_row[column]!r} — a superseded version "
                            f"of the row is being served")
        return None

    def last_matching_transaction(self, checksum: str) -> Optional[int]:
        """The latest transaction whose expected checksum equals *checksum*.

        If the final read matches the state as of transaction N, everything
        after N failed to take effect — which names the culprit directly.
        """
        for entry in reversed(self.history):
            if entry["checksum"] == checksum:
                return int(entry["transaction"])
        return None
