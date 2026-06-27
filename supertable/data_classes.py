from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


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
      tombstone exists, so no anti-join is applied.
    """
    tombstone_path: Optional[str] = None


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