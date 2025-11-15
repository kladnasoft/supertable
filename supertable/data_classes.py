from dataclasses import dataclass, field
from typing import List, Set


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
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]