# route: supertable.odata.discovery
"""Keyed table discovery — what a server can expose as an OData entity set.

An OData ``$metadata`` document has to declare, for every entity set, a key.
A table with no usable key cannot be exposed at all: clients address single
entities by key, and a server that invents one produces URLs that break the
moment rows move between files.

So discovery is not "list the tables". It is "list the tables that can be
served, and say why the others cannot" — the second half matters, because
otherwise an operator sees a table missing from ``$metadata`` with no
explanation.

Every table here is keyed by ``__rowid__``. The lake has no user-declared
primary keys; ``overwrite_columns`` looks like one but is a per-write merge
instruction, not a table-level constraint, and nothing stops two writes using
different columns. Treating it as a key would let a client address an entity
that silently stops existing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from supertable.odata import row_identity as RI


@dataclass(frozen=True)
class EntitySet:
    """One table a server may expose, with the facts needed to declare it."""

    name: str
    key: str
    columns: List[Dict[str, str]] = field(default_factory=list)
    rows: int = 0
    snapshot_version: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "key": self.key, "columns": self.columns,
            "rows": self.rows, "snapshot_version": self.snapshot_version,
        }


@dataclass(frozen=True)
class Unservable:
    """A table that exists but cannot be exposed, and the reason."""

    name: str
    reason: str

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "reason": self.reason}


#: Columns the lake adds for its own bookkeeping. They are stripped from the
#: query output by the tombstone view, so a server must not declare them —
#: except the key, which is declared deliberately.
SYSTEM_COLUMNS = frozenset({"__rowid__", "__timestamp__"})


def _visible_columns(schema: Any) -> List[Dict[str, str]]:
    """User-facing columns, in snapshot order, with system columns removed.

    The snapshot stores the schema as a ``{name: type}`` dict, whose insertion
    order is the column order. The list-of-dicts form is also accepted because
    older snapshots and the Spark-facing schema use it — getting this wrong is
    silent: a table simply reports "no user-visible columns" and vanishes from
    $metadata, which is how this was found.
    """
    out: List[Dict[str, str]] = []
    if isinstance(schema, dict):
        for name, dtype in schema.items():
            if not name or name in SYSTEM_COLUMNS:
                continue
            out.append({"name": str(name), "type": str(dtype)})
        return out
    for item in schema or []:
        if isinstance(item, dict):
            name = item.get("name")
            if not name or name in SYSTEM_COLUMNS:
                continue
            out.append({"name": str(name), "type": str(item.get("type", ""))})
    return out


def describe_table(name: str, snapshot: Optional[Dict[str, Any]]) -> Any:
    """Classify one table as an EntitySet or an Unservable, with a reason."""
    if not snapshot:
        return Unservable(name, "no snapshot")

    verdict = RI.verify_stable_identity(snapshot)
    if not verdict:
        return Unservable(name, verdict.reason)

    columns = _visible_columns(snapshot.get("schema"))
    if not columns:
        # An empty table is servable; a table with no non-system columns is
        # not — there would be nothing to project.
        return Unservable(name, "no user-visible columns")

    return EntitySet(
        name=name,
        key=RI.ROWID_COLUMN,
        columns=columns,
        rows=verdict.live_rows,
        snapshot_version=int(snapshot.get("snapshot_version") or 0),
    )


def discover(snapshots: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """Split a set of tables into what can and cannot be served.

    Takes snapshots the caller has already loaded rather than fetching them:
    the server holds them for its own reasons, and re-reading would double the
    catalog traffic on every ``$metadata`` request.

    Returns both halves. A server renders the first and logs the second; the
    reasons are the difference between "that table is missing" and "that table
    needs one write to record its watermark".
    """
    servable: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for name in sorted(snapshots):
        described = describe_table(name, snapshots[name])
        if isinstance(described, EntitySet):
            servable.append(described.to_dict())
        else:
            skipped.append(described.to_dict())
    return {"entity_sets": servable, "skipped": skipped}
