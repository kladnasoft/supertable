from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional, Set


class PredInterval(NamedTuple):
    """A closed/open range of values a column is *allowed* to take, derived from
    a SQL WHERE predicate, used for read-path file pruning.

    ``lane`` is one of ``"numeric"`` / ``"string"`` / ``"timestamp"`` and selects
    which stored stats lane the interval is compared against (numeric unifies the
    stored ``bigint``/``double`` lanes).  ``lo``/``hi`` are the bounds with
    ``None`` meaning unbounded (-inf / +inf); ``lo_incl``/``hi_incl`` mark
    whether each bound is inclusive.  Examples: ``col = 5`` →
    ``(lane, 5, True, 5, True)``; ``col > 5`` → ``(lane, 5, False, None, True)``.
    """
    lane: str
    lo: object
    lo_incl: bool
    hi: object
    hi_incl: bool


@dataclass
class TableDefinition:
    super_name: str
    simple_name: str
    alias: str
    columns: List[str] = field(default_factory=list)

@dataclass
class SuperSnapshot:
    super_name: str
    simple_name: str
    simple_version: int
    files: List[str] = field(default_factory=list)
    columns: Set[str] = field(default_factory=set)


@dataclass
class RbacViewDef:
    """RBAC view definition for a single table alias.

    Produced by restrict_read_access(), consumed by executors to create
    a filtered view on top of the reflection table.

    - allowed_columns: columns the role can see, or ["*"] for unrestricted.
    - where_clause: SQL WHERE predicate from role filters, or "" if none.
    """
    allowed_columns: List[str] = field(default_factory=lambda: ["*"])
    where_clause: str = ""


@dataclass
class TombstoneDef:
    """Tombstone (deletion-vector) definition for a single table alias.

    Produced by DataReader from the snapshot's ``tombstone`` pointer,
    consumed by executors to create a view that anti-joins the data on
    ``__rowid__`` against the deletion-vector parquet, hiding rows that
    have been logically deleted but not yet physically compacted.

    - tombstone_path: storage path of the deletion-vector parquet
      (columns ``__file__`` + ``__rowid__``).  ``None`` means no
      tombstone exists, so no anti-join is applied.  This may be a
      presigned/object-store URL that rotates per request, so it is
      *not* stable enough to use as a cache key.
    - cache_key: the bare, stable storage key of the same deletion-vector
      parquet (no presign).  Stable across pure appends (carry-forward
      returns the previous tombstone), so DuckDB engines use it to key the
      materialised deletion-vector table cache.  ``None`` disables caching
      for this alias (falls back to inline ``read_parquet``).
    """
    tombstone_path: Optional[str] = None
    cache_key: Optional[str] = None


@dataclass
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]
    # Max last_updated_ms across all snapshots.  Used by the engine
    # auto-picker to distinguish fresh (still-churning) data from stable
    # data where caching pays off.  0 means unknown.
    freshness_ms: int = 0
    # alias -> RbacViewDef.  Empty dict means no RBAC filtering.
    rbac_views: Dict[str, RbacViewDef] = field(default_factory=dict)
    # alias -> TombstoneDef.  Empty dict means no tombstone filtering.
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)