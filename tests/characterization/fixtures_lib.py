"""Deterministic fixture authoring for the characterization suite.

A :class:`Scenario` is a fully declarative description of one sealed test case:
the physical parquet files (with FIXED ``__timestamp__`` values and an explicit
row + file order), the catalog metadata (primary keys, dedup flag, tombstones),
and the read to perform (structured projection/filters or an explicit SQL).

``materialize_scenario`` writes the immutable input artifacts to disk:

    <scenario_dir>/input/<file>.parquet           # one per FileSpec, byte-frozen
    <scenario_dir>/input/<table>__tombstone.parquet  # deletion vector (if any)
    <scenario_dir>/input/catalog.json             # catalog/snapshot/tombstones/schema

These inputs are the compatibility corpus, authored as valid deletion-vector
lakehouse state: each physical row carries a stable internal ``__rowid__`` (plus
the internal ``__timestamp__``), and the tombstone deletion-vector parquet
(columns ``__file__`` + ``__rowid__``) lists the rows removed at write time. The
read path removes a row solely by anti-joining its ``__rowid__`` against that
vector — there is no read-time key-collapse dedup.
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

# Internal physical columns the writer injects.  Fixtures author them directly so
# the sealed inputs are valid deletion-vector lakehouse state: every physical row
# carries a stable ``__rowid__`` and the read path removes a row solely by
# anti-joining its ``__rowid__`` against the tombstone deletion-vector parquet
# (columns ``__file__`` + ``__rowid__``).  There is no read-time key-collapse
# dedup: an overwrite/delete is a WRITE-time decision recorded as tombstoned
# rowids; same-key rows that are not tombstoned are legitimate appends and all
# survive.
TS_COL = "__timestamp__"
ROWID_COL = "__rowid__"
TOMBSTONE_FILE_COL = "__file__"


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


def _tombstone_name(table: "TableSpec") -> str:
    """Physical filename of a table's deletion-vector parquet (empty -> unused)."""
    return f"{table.simple_name}__tombstone.parquet"


def _assign_rowids_and_tombstones(table: "TableSpec"):
    """Deterministically assign a ``__rowid__`` to every physical row and compute
    the tombstoned ``(physical_file, rowid)`` pairs the writer would have produced.

    Row ids are a single sequential block across the table's files (file order,
    then row order) — mirroring the catalog's monotonic rowid reservation, and
    globally unique so the read-side anti-join on ``__rowid__`` targets exactly
    one physical row.

    Tombstones encode the WRITE-time intent the scenarios declare:

    * **delete** (``deleted_keys``): SQL ``=`` semantics — the leading
      ``len(primary_keys)`` components must equal a row's key; a NULL component
      never matches (``NULL = NULL`` is false), so a NULL-keyed row cannot be
      targeted and an over-long tuple matches on its leading components only.
    * **supersede** (``dedup_on_read``): within a primary-key partition (NULLs
      partition together, as ``PARTITION BY`` does) every row with a strictly
      smaller ``__timestamp__`` than the partition max is tombstoned; rows tied at
      the max all survive (equal timestamps are appends, not overwrites).
    """
    pk = list(table.primary_keys)
    n = len(pk)

    rowids_by_file: Dict[str, List[int]] = {}
    rows_meta: List[dict] = []
    rid = 0
    for f in table.files:
        ids: List[int] = []
        for row in f.rows:
            rid += 1
            ids.append(rid)
            rows_meta.append({
                "file": _physical_name(table, f),
                "rowid": rid,
                "key": tuple(row.get(k) for k in pk),
                "ts": row.get(TS_COL),
            })
        rowids_by_file[f.name] = ids

    tomb: set = set()

    for dk in table.deleted_keys:
        target = list(dk)[:n]
        if len(target) < n or any(c is None for c in target):
            continue
        for m in rows_meta:
            if list(m["key"][:n]) == target:
                tomb.add(m["rowid"])

    if table.dedup_on_read:
        groups: Dict[tuple, List[dict]] = {}
        for m in rows_meta:
            groups.setdefault(m["key"], []).append(m)

        def _order(m: dict):
            # NULL timestamp ranks below any real timestamp (never the max).
            return (1, m["ts"]) if m["ts"] is not None else (0,)

        for members in groups.values():
            if len(members) <= 1:
                continue
            top = max(_order(m) for m in members)
            for m in members:
                if _order(m) < top:
                    tomb.add(m["rowid"])

    pairs = [(m["file"], m["rowid"]) for m in rows_meta if m["rowid"] in tomb]
    return rowids_by_file, pairs


def catalog_dict(scenario: Scenario) -> dict:
    """The human-readable, frozen catalog manifest for a scenario."""
    tables = {}
    for t in scenario.tables:
        _, tomb_pairs = _assign_rowids_and_tombstones(t)
        tables[t.simple_name] = {
            "primary_keys": t.primary_keys,
            "dedup_on_read": t.dedup_on_read,
            "schema": t.derived_schema(),
            "resources": [
                {"file": _physical_name(t, f), "rows": len(f.rows),
                 "columns": len(f.columns)}
                for f in t.files
            ],
            # Deletion-vector pointer: the read path anti-joins __rowid__ against
            # this parquet.  None when no row is tombstoned.
            "tombstone_file": _tombstone_name(t) if tomb_pairs else None,
            "tombstone_rows": len(tomb_pairs),
            # Retained for human readability of the frozen manifest; the read path
            # no longer consumes key-based tombstones (rowid DV is authoritative).
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
        rowids_by_file, tomb_pairs = _assign_rowids_and_tombstones(t)
        for f in t.files:
            df = f.to_polars().with_columns(
                pl.Series(ROWID_COL, rowids_by_file[f.name], dtype=pl.Int64)
            )
            df.write_parquet(inp / _physical_name(t, f))
        if tomb_pairs:
            dv = pl.DataFrame(
                {
                    TOMBSTONE_FILE_COL: [fn for fn, _ in tomb_pairs],
                    ROWID_COL: [int(r) for _, r in tomb_pairs],
                },
                schema={TOMBSTONE_FILE_COL: pl.Utf8, ROWID_COL: pl.Int64},
            )
            dv.write_parquet(inp / _tombstone_name(t))

    catalog_path = inp / "catalog.json"
    catalog_path.write_bytes(
        json.dumps(catalog_dict(scenario), ensure_ascii=False, indent=2).encode("utf-8")
    )
    return catalog_path
