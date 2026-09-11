# route: supertable.odata
"""OData integration helpers.

These support a server; they are not one. There is no HTTP here, no routing,
and no OData URL parsing — only the three things a server cannot correctly
invent on its own:

  row_identity   can this snapshot's __rowid__ be trusted as an entity key?
  discovery      which tables can be exposed, and why the others cannot
  continuation   $skiptoken paging that survives concurrent writes

Each answers a question where a wrong guess is silent: an unstable key gives
URLs that resolve to different rows over time, an invented key exposes a table
that breaks on the first merge, and OFFSET paging skips or repeats rows when
someone writes mid-export.
"""

from supertable.odata.continuation import (
    Continuation,
    InvalidContinuation,
    advance,
    apply_to_sql,
    decode,
    first_page,
    page_predicate,
    split_page,
)
from supertable.odata.discovery import EntitySet, Unservable, describe_table, discover
from supertable.odata.row_identity import (
    IdentityVerdict,
    ROWID_COLUMN,
    WATERMARK_KEY,
    next_watermark,
    snapshot_live_rows,
    snapshot_watermark,
    verify_stable_identity,
)

__all__ = [
    "Continuation", "InvalidContinuation", "advance", "apply_to_sql", "decode",
    "first_page", "page_predicate", "split_page",
    "EntitySet", "Unservable", "describe_table", "discover",
    "IdentityVerdict", "ROWID_COLUMN", "WATERMARK_KEY", "next_watermark",
    "snapshot_live_rows", "snapshot_watermark", "verify_stable_identity",
]
