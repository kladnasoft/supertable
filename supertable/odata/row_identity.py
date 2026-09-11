# route: supertable.odata.row_identity
"""Stable row identities for OData entity keys.

OData needs every row to have a key that means the same row tomorrow. The lake
already has one: ``__rowid__``, assigned once at insert from a per-table Redis
counter (``reserve_rowids``, an INCRBY) and carried unchanged through every
later rewrite, because data files are immutable and versioned.

So this module does not create identities. It answers one question a server
must not guess at: **can this snapshot's rowids be trusted as keys?**

WHY THE CHEAP PROOF, AND NOT THE THOROUGH ONE

An earlier design sealed per-resource integrity facts on every write — null
count, min/max, distinct count, and a SHA-256 over eight bytes per row. That is
O(rows) on the write path, paid by every writer whether or not anyone ever
serves OData from the table.

This proves the same property from two numbers that already exist:

    live rows in the snapshot  <=  rowids ever reserved

Rowids are handed out by INCRBY and never reused, so if the table holds no more
live rows than the counter has issued, it cannot be serving a duplicate key.
The watermark costs one dict entry on write; the row count is already in the
snapshot. Nothing is hashed and nothing is scanned.

WHAT THIS DELIBERATELY DOES NOT DO

It does not run ``count(DISTINCT __rowid__)``. A previous version called that
from the tombstone view for EVERY query — not just OData ones — which put an
uncached distinct-count SEMI-JOIN across all deletion-vector files on the
normal read path. Uniqueness is a writer invariant; verifying it on every read
is paying forever for a bug that would be a one-off.

The trade is explicit: a writer bug that issued duplicate rowids would be
caught here only as a count mismatch, not identified row by row. That is the
right trade for a property the allocator makes structurally true.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

#: Snapshot key holding the highest rowid ever reserved for the table.
WATERMARK_KEY = "rowid_high_watermark"

#: The system column that carries the identity.
ROWID_COLUMN = "__rowid__"


@dataclass(frozen=True)
class IdentityVerdict:
    """Why a snapshot can — or cannot — back OData entity keys."""

    stable: bool
    reason: str
    watermark: int = 0
    live_rows: int = 0

    def __bool__(self) -> bool:
        return self.stable


def snapshot_live_rows(snapshot: Dict[str, Any]) -> int:
    """Rows visible in this snapshot: resource rows minus deleted ones."""
    resources = snapshot.get("resources") or []
    physical = sum(int(r.get("rows") or 0) for r in resources
                   if isinstance(r, dict))
    deleted = int(snapshot.get("tombstone_rows") or 0)
    return max(0, physical - deleted)


def snapshot_watermark(snapshot: Dict[str, Any]) -> Optional[int]:
    """The recorded high watermark, or None on a snapshot written before it."""
    value = snapshot.get(WATERMARK_KEY)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def verify_stable_identity(snapshot: Dict[str, Any]) -> IdentityVerdict:
    """Decide whether this snapshot's ``__rowid__`` values are usable as keys.

    Fails CLOSED. A snapshot written before the watermark existed cannot prove
    anything, so it is refused rather than assumed good — serving an unstable
    key silently is worse than refusing to serve one.
    """
    watermark = snapshot_watermark(snapshot)
    if watermark is None:
        return IdentityVerdict(
            False,
            "snapshot predates rowid_high_watermark; rewrite the table (any "
            "write records it) before serving OData from it",
        )

    live = snapshot_live_rows(snapshot)
    if watermark < 0:
        return IdentityVerdict(False, f"negative watermark {watermark}",
                               watermark, live)
    if live > watermark:
        # More live rows than ids ever issued: some id must be duplicated.
        return IdentityVerdict(
            False,
            f"{live} live rows exceed {watermark} reserved ids — rowids are "
            f"not unique in this snapshot",
            watermark, live,
        )
    return IdentityVerdict(True, "ok", watermark, live)


def next_watermark(previous: Optional[Dict[str, Any]],
                   start_rowid: int, inserted_rows: int) -> int:
    """The watermark to record after a write that reserved a rowid block.

    Monotonic by construction: a delete-only write reserves nothing and carries
    the previous value forward, so the watermark never goes backwards even
    though the live row count does.
    """
    prior = 0
    if previous:
        prior = snapshot_watermark(previous) or 0
    if inserted_rows > 0 and start_rowid > 0:
        return max(prior, start_rowid + inserted_rows - 1)
    return prior


def identity_column_present(schema: Any) -> bool:
    """Whether a snapshot schema carries the identity column.

    The column is written by the writer and hidden from query output by the
    tombstone view, so it is absent from a user-facing schema but present in
    the snapshot's own. Both are checked by callers for different reasons.
    """
    if isinstance(schema, dict):        # {name: type}, the snapshot's own form
        return ROWID_COLUMN in schema
    for field in schema or []:          # [{"name": ..., "type": ...}]
        if isinstance(field, dict) and field.get("name") == ROWID_COLUMN:
            return True
    return False
