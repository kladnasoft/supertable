# route: supertable.odata.continuation
"""Continuation-based pagination — ``$skiptoken``, not ``OFFSET``.

WHY NOT OFFSET

``LIMIT n OFFSET k`` is wrong here for two independent reasons, and both bite
in production rather than in testing:

  * it is not stable. The lake is append-and-merge, so a write between page 3
    and page 4 shifts every later row by one. The client then skips a row or
    sees one twice, and nothing reports an error.
  * it is not cheap. ``OFFSET 900000`` still produces and discards 900,000
    rows. Page cost grows with page number, so the last page of an export is
    the most expensive one — exactly backwards.

A continuation token instead records WHERE the last page stopped. The next
page asks for rows after that point, which is a predicate the read path can
prune on: ``__rowid__ > 41234`` against per-file min/max stats skips whole
files instead of reading and discarding them.

WHY __rowid__ IS THE CURSOR

It is the only column that is unique, never reused, and never rewritten.
Ordering by it is not the ordering a user asked for — OData's ``$orderby`` is
applied by the server on top — but a cursor must be ordered by something
totally ordered and stable, and a user column is neither.

TOKENS ARE OPAQUE BUT NOT SECRET

The token is base64 of a small JSON blob. Opaque so clients cannot construct
one and depend on its shape; not encrypted, because it contains nothing a
client did not already send. It carries a checksum so a corrupted or
hand-edited token is rejected with a clear error instead of silently
returning the wrong page.

It also pins the query it belongs to. Feeding page 2 of one query into a
different query would otherwise resume at an unrelated offset and return
plausible, wrong data.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from supertable.odata.row_identity import ROWID_COLUMN

_TOKEN_VERSION = 1


class InvalidContinuation(ValueError):
    """The token is malformed, corrupted, or belongs to another query."""


def _fingerprint(table: str, query: str) -> str:
    """Identifies the query a token belongs to.

    Short on purpose: it is a mismatch detector, not a security control. A
    collision would let one query resume another, which is why the table name
    is included separately rather than folded into the same hash input.
    """
    material = f"{table}\x00{query}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:16]


@dataclass(frozen=True)
class Continuation:
    """Where a page stopped, and which query it belongs to."""

    table: str
    fingerprint: str
    after_rowid: int
    page_size: int
    rows_served: int = 0

    def encode(self) -> str:
        payload = {
            "v": _TOKEN_VERSION, "t": self.table, "f": self.fingerprint,
            "a": self.after_rowid, "p": self.page_size, "n": self.rows_served,
        }
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
        blob = json.dumps({"d": payload, "c": digest}, separators=(",", ":"))
        return base64.urlsafe_b64encode(blob.encode("utf-8")).decode("ascii").rstrip("=")


def decode(token: str, *, table: str, query: str) -> Continuation:
    """Parse a token and prove it belongs to this query.

    Every failure mode is its own message. "Invalid token" tells an operator
    nothing; "belongs to a different query" tells them a client is reusing a
    token across requests, which is a real and confusing bug to hit.
    """
    if not token:
        raise InvalidContinuation("empty continuation token")
    try:
        padded = token + "=" * (-len(token) % 4)
        blob = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidContinuation(f"continuation token is not decodable: {e}")

    if not isinstance(blob, dict) or "d" not in blob or "c" not in blob:
        raise InvalidContinuation("continuation token has no payload")

    payload = blob["d"]
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12] != blob["c"]:
        raise InvalidContinuation("continuation token failed its checksum")

    if int(payload.get("v", 0)) != _TOKEN_VERSION:
        raise InvalidContinuation(
            f"continuation token version {payload.get('v')} is not supported")

    if payload.get("t") != table:
        raise InvalidContinuation(
            f"continuation token is for table {payload.get('t')!r}, not {table!r}")
    if payload.get("f") != _fingerprint(table, query):
        raise InvalidContinuation(
            "continuation token belongs to a different query — a token cannot "
            "be reused across queries, it would resume at an unrelated row")

    return Continuation(
        table=table, fingerprint=str(payload["f"]),
        after_rowid=int(payload["a"]), page_size=int(payload["p"]),
        rows_served=int(payload.get("n") or 0),
    )


def first_page(table: str, query: str, page_size: int) -> Continuation:
    """The cursor for a request that arrived without a token."""
    if page_size <= 0:
        raise ValueError(f"page_size must be positive, got {page_size}")
    return Continuation(table=table, fingerprint=_fingerprint(table, query),
                        after_rowid=0, page_size=page_size)


def advance(cursor: Continuation, last_rowid: int,
            rows_in_page: int) -> Continuation:
    """The cursor for the next page, after serving one."""
    return Continuation(
        table=cursor.table, fingerprint=cursor.fingerprint,
        after_rowid=int(last_rowid), page_size=cursor.page_size,
        rows_served=cursor.rows_served + int(rows_in_page),
    )


def page_predicate(cursor: Continuation) -> Optional[str]:
    """The SQL that resumes where the last page stopped.

    ``None`` for the first page rather than ``__rowid__ > 0``: an always-true
    predicate would still be parsed, planned and evaluated against every file's
    stats for nothing.
    """
    if cursor.after_rowid <= 0:
        return None
    return f"{ROWID_COLUMN} > {int(cursor.after_rowid)}"


def apply_to_sql(sql: str, cursor: Continuation) -> str:
    """Wrap a query so it returns exactly one page, resumable.

    The cursor predicate and ordering are applied OUTSIDE the caller's SQL, in
    a derived table. Injecting them into the original text would mean parsing
    and rewriting arbitrary SQL — and would break the moment the query already
    has its own ORDER BY, LIMIT, GROUP BY or set operation.

    One row beyond the page is fetched deliberately: it is how the caller knows
    whether a next page exists without running a second count query. See
    ``split_page``.
    """
    predicate = page_predicate(cursor)
    where = f"WHERE {predicate}" if predicate else ""
    return (
        f"SELECT * FROM ({sql}) AS _odata_page {where} "
        f"ORDER BY {ROWID_COLUMN} "
        f"LIMIT {int(cursor.page_size) + 1}"
    )


def split_page(rows: Any, cursor: Continuation) -> Dict[str, Any]:
    """Turn an over-fetched result into a page plus the cursor that follows it.

    ``apply_to_sql`` asks for page_size + 1 rows. If that extra row came back
    there is more data, and it is dropped rather than served — it belongs to
    the next page.

    The next cursor is returned HERE, derived from the trimmed page, because
    deriving it separately is the mistake this shape exists to prevent: call
    ``advance`` on the untrimmed result and the cursor skips past a row that
    was never served, which loses it silently. A caller should never see the
    over-fetched row at all.

    Accepts a polars frame or a list of mappings; ``next_token`` is None when
    the page is the last one, which is exactly what a server puts in
    ``@odata.nextLink``.
    """
    height = rows.height if hasattr(rows, "height") else len(rows)
    has_more = height > cursor.page_size
    page = rows[:cursor.page_size] if has_more else rows
    page_len = page.height if hasattr(page, "height") else len(page)

    next_cursor = None
    if has_more and page_len:
        last = _last_rowid(page)
        if last is not None:
            next_cursor = advance(cursor, last, page_len)

    return {
        "rows": page,
        "has_more": has_more,
        "count": page_len,
        "next_cursor": next_cursor,
        "next_token": next_cursor.encode() if next_cursor else None,
    }


def _last_rowid(page: Any) -> Optional[int]:
    """The identity of the final row, from a polars frame or a list of dicts."""
    try:
        if hasattr(page, "get_column"):
            values = page.get_column(ROWID_COLUMN).to_list()
            return int(values[-1]) if values else None
        last = page[-1]
        if isinstance(last, dict):
            return int(last[ROWID_COLUMN])
        return int(last)
    except (KeyError, IndexError, TypeError, ValueError):
        return None
