"""Deterministic fixture authoring for the characterization suite.

A :class:`Scenario` is a fully declarative description of one sealed test case:
the physical parquet files (with FIXED ``__timestamp__`` values and an explicit
row + file order), the catalog metadata (primary keys, dedup flag, tombstones),
and the read to perform (structured projection/filters or an explicit SQL).

``materialize_scenario`` writes the immutable input artifacts to disk:

    <scenario_dir>/input/<file>.parquet      # one per FileSpec, byte-frozen
    <scenario_dir>/input/catalog.json        # catalog/snapshot/tombstones/schema

These inputs are the compatibility corpus.  They are authored to remain valid
after an internal ``__rowid`` column is introduced (they contain only the public
columns plus the existing internal ``__timestamp__``); a future reader must keep
reading them correctly.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import polars as pl

# Fixed clock anchor — NEVER the wall clock.  All scenario timestamps are
# expressed relative to this so golden bytes are reproducible.
EPOCH = _dt.datetime(2026, 1, 1, 10, 0, 0, tzinfo=_dt.timezone.utc)


def ts(micros: int = 0, *, seconds: int = 0, tz: bool = True) -> _dt.datetime:
    """A fixed, deterministic timestamp offset from :data:`EPOCH`.

    ``micros`` adds microseconds (to exercise microsecond precision); ``seconds``
    adds whole seconds.  ``tz=False`` returns a naive datetime (same wall instant)
    to characterise tz / no-tz handling.
    """
    v = EPOCH + _dt.timedelta(seconds=seconds, microseconds=micros)
    return v if tz else v.replace(tzinfo=None)


# Map a small set of friendly type names to polars dtypes for fixture authoring.
_DTYPES: Dict[str, Any] = {
    "int": pl.Int64,
    "int32": pl.Int32,
    "bigint": pl.Int64,
    "long": pl.Int64,
    "str": pl.Utf8,
    "string": pl.Utf8,
    "bool": pl.Boolean,
    "float": pl.Float64,
    "double": pl.Float64,
    "date": pl.Date,
    "timestamp": pl.Datetime(time_unit="us", time_zone="UTC"),
    "timestamp_naive": pl.Datetime(time_unit="us", time_zone=None),
    "decimal": pl.Decimal(precision=38, scale=9),
}

# Polars dtype -> Spark-ish schema string used in the snapshot payload schema.
_POLARS_TO_SCHEMASTR = {
    "Int64": "Int64",
    "Int32": "Int32",
    "Utf8": "String",
    "String": "String",
    "Boolean": "Boolean",
    "Float64": "Float64",
    "Float32": "Float32",
    "Date": "Date",
}


def _dtype(name_or_dtype: Any) -> Any:
    if isinstance(name_or_dtype, str):
        if name_or_dtype not in _DTYPES:
            raise ValueError(f"unknown fixture dtype {name_or_dtype!r}")
        return _DTYPES[name_or_dtype]
    return name_or_dtype


@dataclass
class FileSpec:
    """One physical parquet file.  ``columns`` preserves output column order;
    ``rows`` preserves physical row order (duplicates intentional)."""

    name: str
    columns: Dict[str, Any]          # {col: friendly-type-or-polars-dtype}
    rows: List[Dict[str, Any]]

    def to_polars(self) -> pl.DataFrame:
        schema = {c: _dtype(t) for c, t in self.columns.items()}
        data = {c: [r.get(c) for r in self.rows] for c in self.columns}
        return pl.DataFrame(data, schema=schema)


@dataclass
class TableSpec:
    simple_name: str
    primary_keys: List[str]
    files: List[FileSpec]
    dedup_on_read: bool = True
    deleted_keys: List[List[Any]] = field(default_factory=list)  # tombstone key-tuples
    schema: Optional[Dict[str, str]] = None  # explicit schema-string map override

    def derived_schema(self) -> Dict[str, str]:
        if self.schema is not None:
            return dict(self.schema)
        # Union across ALL files in first-appearance order, mirroring what the
        # writer stores after schema-evolving appends.  The snapshot schema is
        # authoritative for column existence (the reader's preflight rejects a
        # selected column absent from it), so a column added in a later file
        # must appear here for the added-column read to be characterizable.
        out: Dict[str, str] = {}
        for f in self.files:
            df = f.to_polars()
            for col, dt in zip(df.columns, df.dtypes):
                if col in out:
                    continue
                base = str(dt)
                if base.startswith("Datetime"):
                    out[col] = "Datetime(time_unit='us', time_zone='UTC')"
                else:
                    out[col] = _POLARS_TO_SCHEMASTR.get(base, base)
        return out


@dataclass
class ErrorExpectation:
    """Sealed expectation for an error scenario: a stable category, a message
    substring, and the phase the failure must occur in.  Never the full
    traceback or any absolute path (those are environment-dependent)."""

    category: str                 # e.g. "RuntimeError", "PermissionError"
    message_substring: str        # stable fragment that must appear
    phase: str                    # "catalog" | "planning" | "execution"


@dataclass
class Scenario:
    scenario_id: str              # unique; maps to tests/golden/<scenario_id>/
    category: str                 # basic | multi_file | timestamp | keys | ...
    description: str
    tables: List[TableSpec]
    # read spec — either explicit sql OR structured projection/filters
    sql: Optional[str] = None
    table_name: Optional[str] = None   # for structured reads; defaults to tables[0]
    projection: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    ordered: bool = False         # ordered comparison (use with ORDER BY)
    limit: int = 10_000
    expect_error: Optional[ErrorExpectation] = None
    organization: str = "charorg"
    super_name: Optional[str] = None  # defaults to f"super_{scenario_id}"
    # Optional post-materialize tamper (error scenarios): receives the scenario's
    # ``input`` dir so a fixture can diverge from its catalog (delete/corrupt a
    # parquet, malform catalog.json).  Excluded from equality/repr; applied once
    # by the generator after materialize and baked into the sealed input on disk.
    corrupt_inputs: Optional[Callable[[Path], None]] = field(
        default=None, repr=False, compare=False
    )

    def super(self) -> str:
        return self.super_name or f"super_{self.scenario_id}"

    def target_table(self) -> str:
        return self.table_name or self.tables[0].simple_name


# --------------------------------------------------------------------------
# Materialization
# --------------------------------------------------------------------------

def _physical_name(table: "TableSpec", file: "FileSpec") -> str:
    """Physical parquet filename, namespaced by table so same-named files across
    tables (e.g. each table's ``f1``) never collide in the flat ``input`` dir."""
    return f"{table.simple_name}__{file.name}.parquet"


def catalog_dict(scenario: Scenario) -> dict:
    """The human-readable, frozen catalog manifest for a scenario."""
    tables = {}
    for t in scenario.tables:
        tables[t.simple_name] = {
            "primary_keys": t.primary_keys,
            "dedup_on_read": t.dedup_on_read,
            "schema": t.derived_schema(),
            "resources": [
                {"file": _physical_name(t, f), "rows": len(f.rows),
                 "columns": len(f.columns)}
                for f in t.files
            ],
            "tombstones": {
                "deleted_keys": [list(k) for k in t.deleted_keys],
                "primary_keys": t.primary_keys,
            },
        }
    return {
        "organization": scenario.organization,
        "super_name": scenario.super(),
        "tables": tables,
    }


def materialize_scenario(scenario: Scenario, scenario_dir: str | Path) -> Path:
    """Write input parquet files + catalog.json under ``<scenario_dir>/input``.

    Returns the path to the written ``catalog.json``.
    """
    base = Path(scenario_dir)
    inp = base / "input"
    inp.mkdir(parents=True, exist_ok=True)

    # Clear any stale parquet so removed files don't linger in a reseal.
    for old in inp.glob("*.parquet"):
        old.unlink()

    for t in scenario.tables:
        for f in t.files:
            f.to_polars().write_parquet(inp / _physical_name(t, f))

    catalog_path = inp / "catalog.json"
    catalog_path.write_bytes(
        json.dumps(catalog_dict(scenario), ensure_ascii=False, indent=2).encode("utf-8")
    )
    return catalog_path
