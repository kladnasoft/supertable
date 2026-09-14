# route: test_suite.workload
"""A seeded, randomized stream of write transactions.

Randomized so the suite explores interleavings nobody thought to write down;
seeded so any failure is reproducible exactly. The seed is printed on failure
and can be pinned with ``--seed``.

Each transaction is applied to the real table *and* to the shadow model, which
records a fingerprint afterwards. Nothing is asserted here — the suite checks
once at the end, and the recorded trail is what localises a divergence.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from .shadow import COLUMNS, KEY, Shadow

#: Transaction mix. Weighted so the table grows early and then churns, and so
#: the operations that remove data are common enough to stress tombstones
#: rather than appearing once.
OPERATIONS: Tuple[Tuple[str, int], ...] = (
    ("insert", 22),
    ("insert_many", 10),
    ("update", 22),
    ("update_many", 10),
    ("delete", 14),
    ("delete_many", 6),
    ("upsert_new", 6),        # upsert whose key is absent -> an insert
    ("delete_missing", 4),    # no-op; must not disturb anything
    ("stale_update", 6),      # rejected by newer_than; must not disturb anything
)

GROUPS = ("alpha", "beta", "gamma", "delta")
BASE_DATE = date(2024, 1, 1)


@dataclass
class Transaction:
    index: int
    kind: str
    detail: object


class WriteWorkload:
    """Generates and applies the transaction stream."""

    def __init__(self, seed: int, transactions: int):
        self.rng = random.Random(seed)
        self.seed = seed
        self.transactions = transactions
        self.shadow = Shadow()
        self.applied: List[Transaction] = []
        self._next_id = 1
        self._kinds: Dict[str, int] = {}

    # ---------------- row construction ----------------

    def _fresh_key(self) -> int:
        key = self._next_id
        self._next_id += 1
        return key

    def _make_row(self, key: int, revision: int) -> Dict[str, object]:
        """A row with enough type variety for the read half to be meaningful.

        ``amount`` is a float and ``qty`` an int so numeric aggregates
        (AVG/STDDEV vs SUM/MIN/MAX) exercise both lanes; ``event_date`` spans
        ~2 years so date filters and date grouping have something to cut.
        """
        return {
            "id": key,
            "name": f"row-{key:04d}-v{revision}",
            "amount": round(self.rng.uniform(-500, 5000), 2),
            "qty": self.rng.randint(0, 50),
            "grp": self.rng.choice(GROUPS),
            "rev": revision,
            "event_date": BASE_DATE + timedelta(days=self.rng.randint(0, 730)),
        }

    def _keys_for(self, count: int) -> List[object]:
        """Pick *count* DISTINCT live keys.

        Distinctness is required, not cosmetic: a single write carrying the
        same key twice keeps both rows (measured), which a dict-shaped model
        cannot represent. Sampling without replacement keeps the model exact.
        """
        live = sorted(self.shadow.rows)
        if not live:
            return []
        return self.rng.sample(live, min(count, len(live)))

    # ---------------- the stream ----------------

    def _choose(self) -> str:
        kinds = [k for k, _ in OPERATIONS]
        weights = [w for _, w in OPERATIONS]
        # The table must be non-empty for anything that targets existing rows;
        # early transactions therefore insert.
        if len(self.shadow.rows) < 8:
            return "insert_many" if self.rng.random() < 0.5 else "insert"
        return self.rng.choices(kinds, weights=weights, k=1)[0]

    def run(self, apply_fn) -> None:
        """Generate the stream, handing each transaction to *apply_fn*.

        ``apply_fn(kind, payload)`` performs the real write. The shadow is
        updated only after it returns, so a writer that raises cannot leave the
        model claiming a change that never happened.
        """
        for index in range(1, self.transactions + 1):
            kind = self._choose()
            detail = self._dispatch(index, kind, apply_fn)
            self._kinds[kind] = self._kinds.get(kind, 0) + 1
            self.applied.append(Transaction(index, kind, detail))
            self.shadow.record(index, kind, detail)

    def _dispatch(self, index: int, kind: str, apply_fn) -> object:
        if kind in ("insert", "insert_many"):
            count = 1 if kind == "insert" else self.rng.randint(2, 6)
            batch = [self._make_row(self._fresh_key(), 1) for _ in range(count)]
            apply_fn("append", batch)
            self.shadow.insert(batch)
            return [r[KEY] for r in batch]

        if kind in ("update", "update_many"):
            count = 1 if kind == "update" else self.rng.randint(2, 5)
            keys = self._keys_for(count)
            if not keys:
                return self._dispatch(index, "insert", apply_fn)
            batch = [
                self._make_row(key, int(self.shadow.rows[key]["rev"]) + 1)
                for key in keys
            ]
            apply_fn("upsert", batch)
            self.shadow.upsert(batch)
            return keys

        if kind in ("delete", "delete_many"):
            count = 1 if kind == "delete" else self.rng.randint(2, 5)
            keys = self._keys_for(count)
            if not keys:
                return self._dispatch(index, "insert", apply_fn)
            apply_fn("delete", keys)
            self.shadow.delete(keys)
            return keys

        if kind == "upsert_new":
            batch = [self._make_row(self._fresh_key(), 1)]
            apply_fn("upsert", batch)
            self.shadow.upsert(batch)
            return [r[KEY] for r in batch]

        if kind == "delete_missing":
            # A key that was never allocated, so it cannot collide with a live
            # or previously-deleted row.
            absent = self._next_id + 100_000
            apply_fn("delete", [absent])
            self.shadow.delete([absent])       # no-op, asserts the model agrees
            return [absent]

        if kind == "stale_update":
            keys = self._keys_for(1)
            if not keys:
                return self._dispatch(index, "insert", apply_fn)
            key = keys[0]
            current = int(self.shadow.rows[key]["rev"])
            # Equal or lower is rejected (measured: newer_than is strict), so
            # this transaction must leave the table exactly as it was.
            stale_revision = max(1, current - self.rng.randint(0, 1))
            batch = [self._make_row(key, stale_revision)]
            apply_fn("upsert_if_newer", batch)
            self.shadow.upsert_if_newer(batch, "rev")
            return {"key": key, "stored_rev": current, "attempted_rev": stale_revision}

        raise AssertionError(f"unknown transaction kind {kind!r}")

    # ---------------- reporting ----------------

    def summary(self) -> Dict[str, object]:
        return {
            "seed": self.seed,
            "transactions": len(self.applied),
            "by_kind": dict(sorted(self._kinds.items())),
            "final_row_count": len(self.shadow.rows),
            "keys_allocated": self._next_id - 1,
            "final_checksum": self.shadow.checksum(),
        }
