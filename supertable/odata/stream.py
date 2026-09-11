# route: supertable.odata.stream
"""``query_odata_sql_stream`` — the one read entry point the OData service needs.

The service owns the OData protocol: EDM types, entity identities, $metadata,
response serialisation, and the continuation position it hands back to clients.
None of that belongs here. What it cannot do for itself is read the lake — and
it must read it in a specific way:

  * as Arrow batches, streamed, never materialised. A page is bounded but the
    query behind it is not, and a server that buffers the whole result before
    trimming to a page has the memory profile of a full export per request.
  * bounded. ``max_total_rows`` stops the read once the page plus its
    look-ahead row exists, so a $top=10 against a billion rows costs a page.
  * cancellable and deadlined, because an HTTP client that disconnects must not
    leave a query running.
  * resumable from a keyset boundary, so page N+1 does not re-read pages 1..N.

KEYSET RESUMPTION, NOT OFFSET

The boundary the service passes is its own ORDER BY plus a row identity as the
final tiebreaker:

    {"order": [{"column": "name", "direction": "asc", "value": "..."}, ...],
     "row_identity": 41234}

which becomes the standard lexicographic keyset predicate

    (a > a0)
    OR (a = a0 AND b < b0)                     -- b is DESC
    OR (a = a0 AND b = b0 AND __rowid__ > r0)

Two things make this the only correct form. The identity tiebreaker is required
because ORDER BY columns are rarely unique, and without it a page boundary that
lands inside a run of equal values either repeats or drops rows. And the
comparison direction must follow each term's own direction, or a DESC column
silently pages backwards.

NULLS are deliberately unhandled in the boundary: a NULL sort value cannot be
compared with > or < and would produce a predicate that is neither true nor
false, quietly ending the feed early. The stream refuses instead.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence

from supertable.config.defaults import logger
from supertable.odata.row_identity import ROWID_COLUMN

#: The service's internal name for the identity column. It renames on its side;
#: this is here so the contract is visible from the library too.
SERVICE_ROWID_ALIAS = "__supertable_odata_rowid__"


class ODataPolicyChanged(PermissionError):
    """Policy moved between the server's check and this read.

    A PermissionError subclass on purpose: the service already catches
    PermissionError to turn a mid-flight policy change into a continuation
    conflict rather than a 500.
    """


def _sql_literal(value: Any) -> str:
    """Render a boundary value as a SQL literal.

    Only the types a sort key can actually hold. Anything else raises rather
    than being coerced — a boundary rendered wrong does not fail loudly, it
    silently returns the wrong page.
    """
    if value is None:
        raise ValueError(
            "continuation boundary contains NULL; a NULL sort value cannot be "
            "compared with > or < and would end the feed early"
        )
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def keyset_predicate(boundary: Optional[Dict[str, Any]]) -> Optional[str]:
    """Translate the service's continuation boundary into SQL.

    Returns None for a first page — an always-true predicate would still be
    parsed and planned against every file's statistics for nothing.
    """
    if not boundary:
        return None

    terms: Sequence[Dict[str, Any]] = boundary.get("order") or []
    identity = boundary.get("row_identity")
    if identity is None:
        raise ValueError(
            "continuation boundary has no row_identity; ORDER BY columns are "
            "not unique, so a boundary without a tiebreaker repeats or drops "
            "rows at the page edge"
        )

    quoted: List[str] = []
    for term in terms:
        column = str(term.get("column") or "").strip()
        if not column:
            raise ValueError("continuation boundary has an unnamed order term")
        direction = str(term.get("direction") or "asc").strip().lower()
        if direction not in ("asc", "desc"):
            raise ValueError(f"unknown sort direction {direction!r}")
        quoted.append(column)

    clauses: List[str] = []
    for i, term in enumerate(terms):
        column = quoted[i]
        direction = str(term.get("direction") or "asc").lower()
        op = ">" if direction == "asc" else "<"
        literal = _sql_literal(term.get("value"))
        equals = " AND ".join(
            f'"{quoted[j]}" = {_sql_literal(terms[j].get("value"))}'
            for j in range(i)
        )
        strict = f'"{column}" {op} {literal}'
        clauses.append(f"({equals} AND {strict})" if equals else f"({strict})")

    # Final tiebreaker: identical on every sort column, resume after the row.
    equals_all = " AND ".join(
        f'"{quoted[j]}" = {_sql_literal(terms[j].get("value"))}'
        for j in range(len(terms))
    )
    tail = f'"{ROWID_COLUMN}" > {int(identity)}'
    clauses.append(f"({equals_all} AND {tail})" if equals_all else f"({tail})")

    return " OR ".join(clauses)


def _bounded_sql(sql: str, boundary: Optional[Dict[str, Any]],
                 max_total_rows: Optional[int]) -> str:
    """Wrap the service's SQL with the resume predicate and the row bound.

    Wrapped, never rewritten. The service builds its own query — with its own
    ORDER BY, projection and filters — and editing that text would mean parsing
    arbitrary SQL and would break the moment it contains a set operation or its
    own LIMIT.
    """
    predicate = keyset_predicate(boundary)
    if not predicate and not max_total_rows:
        return sql
    where = f"WHERE {predicate}" if predicate else ""
    limit = f"LIMIT {int(max_total_rows)}" if max_total_rows else ""
    return f"SELECT * FROM ({sql}) AS _odata_src {where} {limit}".strip()


class ODataStream:
    """Arrow batches for one page, plus the handle that must outlive them.

    Exposes ``schema``, iteration, and ``close`` — the three things the service
    uses. ``close`` is not optional: the underlying handle holds per-query views
    open on a shared DuckDB connection.
    """

    def __init__(self, handle, *, cancel_event: Optional[threading.Event] = None,
                 deadline: Optional[float] = None,
                 max_total_rows: Optional[int] = None):
        self._handle = handle
        self._cancel = cancel_event
        self._deadline = deadline
        self._max_rows = max_total_rows
        self.rows_emitted = 0
        self._closed = False

    @property
    def schema(self):
        return self._handle.schema

    def __iter__(self) -> Iterator[Any]:
        try:
            for batch in self._handle.batches():
                # Checked between batches, which is also the cancellation
                # granularity the caller controls via batch size.
                if self._cancel is not None and self._cancel.is_set():
                    logger.debug("[odata.stream] cancelled by caller")
                    break
                if self._deadline and time.monotonic() > self._deadline:
                    logger.debug("[odata.stream] deadline exceeded")
                    break
                self.rows_emitted += batch.num_rows
                yield batch
                if self._max_rows and self.rows_emitted >= self._max_rows:
                    break
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._handle.cancel()
        except Exception:
            pass
        try:
            self._handle.close()
        except Exception:
            pass


def query_odata_sql_stream(
    organization: str,
    super_name: str,
    sql: str,
    role_name: str,
    *,
    max_total_rows: Optional[int] = None,
    timeout_sec: Optional[float] = None,
    source: str = "odata",
    out: Optional[Dict[str, Any]] = None,
    cancel_event: Optional[threading.Event] = None,
    expected_effective_policy_fingerprint: Optional[str] = None,
    continuation_boundary: Optional[Dict[str, Any]] = None,
    engine: Any = None,
) -> ODataStream:
    """Stream one bounded OData page as Arrow batches.

    ``out`` is populated with ``role_policy_fingerprint``,
    ``effective_policy_fingerprint`` and ``selected_engine`` so the server can
    re-verify policy after the rows have been consumed.

    Raises :class:`ODataPolicyChanged` (a PermissionError) when
    ``expected_effective_policy_fingerprint`` no longer matches — checked
    BEFORE reading, so a revoked grant cannot leak a single row.
    """
    from supertable.data_reader import DataReader
    from supertable.odata.policy import query_sql_policy_fingerprint

    fingerprints = query_sql_policy_fingerprint(
        organization, super_name, sql, role_name, engine=engine,
    )
    if (expected_effective_policy_fingerprint
            and fingerprints["effective_policy_fingerprint"]
            != expected_effective_policy_fingerprint):
        raise ODataPolicyChanged(
            "effective policy changed since the previous page was served"
        )

    if out is not None:
        out.update(fingerprints)

    paged_sql = _bounded_sql(sql, continuation_boundary, max_total_rows)
    reader = DataReader(super_name=super_name, organization=organization,
                        query=paged_sql, source=source)
    handle = reader.stream(role_name=role_name, expose_rowid=True)

    if out is not None:
        # Recorded after the read is planned, so it names the engine that will
        # actually run rather than the one AUTO would have guessed earlier.
        out["selected_engine"] = getattr(
            getattr(reader, "plan_stats", None), "stats", None,
        ) and _engine_from_stats(reader.plan_stats) or "duckdb"

    deadline = (time.monotonic() + float(timeout_sec)) if timeout_sec else None
    return ODataStream(handle, cancel_event=cancel_event, deadline=deadline,
                       max_total_rows=max_total_rows)


def _engine_from_stats(plan_stats: Any) -> Optional[str]:
    for entry in getattr(plan_stats, "stats", None) or []:
        if isinstance(entry, dict) and "ENGINE" in entry:
            return str(entry["ENGINE"])
    return None
