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
class DedupViewDef:
    """Dedup-on-read view definition for a single table alias.

    Produced by DataReader from table config in Redis, consumed by executors
    to create a ROW_NUMBER window view that keeps only the latest row per
    primary key combination.

    - primary_keys: columns that form the composite key for dedup.
    - order_column: column to ORDER BY DESC (default ``__timestamp__``).
    - visible_columns: columns to expose in the dedup view.  When empty or
      ["*"], all columns except ``__rn__`` are exposed.  When set, only
      these columns are projected (hiding PK / order cols that the user
      did not request).
    """
    primary_keys: List[str] = field(default_factory=list)
    order_column: str = "__timestamp__"
    visible_columns: List[str] = field(default_factory=list)


@dataclass
class TombstoneDef:
    """Tombstone definition for a single table alias.

    Produced by DataReader from the snapshot's ``tombstones`` block,
    consumed by executors to create a filtering view that excludes
    soft-deleted keys.

    - primary_keys: columns that form the composite key.
    - deleted_keys: list of key tuples (each a list of scalars).
    """
    primary_keys: List[str] = field(default_factory=list)
    deleted_keys: List = field(default_factory=list)


@dataclass
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]
    # alias -> RbacViewDef.  Empty dict means no RBAC filtering.
    rbac_views: Dict[str, RbacViewDef] = field(default_factory=dict)
    # alias -> DedupViewDef.  Empty dict means no dedup-on-read.
    dedup_views: Dict[str, DedupViewDef] = field(default_factory=dict)
    # alias -> TombstoneDef.  Empty dict means no tombstone filtering.
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)