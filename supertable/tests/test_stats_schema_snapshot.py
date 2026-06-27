"""Snapshot seal for the external column-statistics artifact.

The read/write pruning consumers all depend on the exact shape of the stats
parquet (``STATS_SCHEMA``) and on the values the extractor
(``_stats_rows_for_metadata``) writes into it.  The read-path characterization
golden suite (``tests/golden``) seals the read *view* behavior — it predates the
stats artifact and does NOT cover this schema.  This module pins the schema and a
representative built artifact so accidental drift (a renamed/added/reordered
column, a dtype change, or a lane-routing regression) fails loudly instead of
silently breaking pruning.

If you intend to change ``STATS_SCHEMA`` or the extractor, update the expected
values here deliberately — that is the signal a stats-format migration needs.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pyarrow.parquet as pq

from supertable.processing import STATS_SCHEMA, _stats_rows_for_metadata


# The frozen contract: exact column order + dtypes of the stats artifact.
_EXPECTED_SCHEMA: list[tuple[str, pl.DataType]] = [
    ("file_path", pl.Utf8),
    ("row_group_id", pl.Int64),
    ("column_name", pl.Utf8),
    ("physical_type", pl.Utf8),
    ("logical_type", pl.Utf8),
    ("min_bigint", pl.Int64),
    ("max_bigint", pl.Int64),
    ("min_double", pl.Float64),
    ("max_double", pl.Float64),
    ("min_timestamp", pl.Datetime("us")),
    ("max_timestamp", pl.Datetime("us")),
    ("min_string", pl.Utf8),
    ("max_string", pl.Utf8),
    ("null_count", pl.Int64),
    ("row_group_rows", pl.Int64),
    ("stats_available", pl.Boolean),
    ("min_is_exact", pl.Boolean),
    ("max_is_exact", pl.Boolean),
]


def test_stats_schema_is_frozen():
    assert list(STATS_SCHEMA.items()) == _EXPECTED_SCHEMA


def test_built_artifact_matches_schema_and_routes_lanes(tmp_path):
    # One file, one row group, four lanes (bigint / string / double / timestamp).
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "amount": [1.5, 2.5, 3.5],
            "ts": [
                datetime(2026, 1, 1),
                datetime(2026, 1, 2),
                datetime(2026, 1, 3),
            ],
        },
        schema={
            "id": pl.Int64,
            "name": pl.Utf8,
            "amount": pl.Float64,
            "ts": pl.Datetime("us"),
        },
    )
    path = str(tmp_path / "snap.parquet")
    pq.write_table(df.to_arrow(), path, write_statistics=True)

    rows = _stats_rows_for_metadata(path, pq.read_metadata(path))
    built = pl.DataFrame(rows, schema=STATS_SCHEMA)

    # Schema of the built artifact is byte-identical to the contract.
    assert list(built.schema.items()) == _EXPECTED_SCHEMA
    # One row per (row_group × column): single row group, four columns.
    assert built.height == 4

    by_col = {r["column_name"]: r for r in built.iter_rows(named=True)}
    assert set(by_col) == {"id", "name", "amount", "ts"}

    # Lane routing: each dtype lands in exactly its lane, stats_available True.
    idr = by_col["id"]
    assert (idr["min_bigint"], idr["max_bigint"]) == (1, 3)
    assert idr["min_double"] is None and idr["min_string"] is None
    assert idr["stats_available"] is True
    assert idr["min_is_exact"] is True and idr["max_is_exact"] is True

    nm = by_col["name"]
    assert (nm["min_string"], nm["max_string"]) == ("a", "c")
    assert nm["min_bigint"] is None

    amt = by_col["amount"]
    assert (amt["min_double"], amt["max_double"]) == (1.5, 3.5)

    tsr = by_col["ts"]
    assert tsr["min_timestamp"] is not None and tsr["max_timestamp"] is not None
    assert tsr["stats_available"] is True


def test_system_columns_never_emitted(tmp_path):
    # __rowid__ / __timestamp__ must never leak into the stats artifact.
    df = pl.DataFrame(
        {"id": [1, 2], "__rowid__": [10, 11], "__timestamp__": [100, 101]}
    )
    path = str(tmp_path / "sys.parquet")
    pq.write_table(df.to_arrow(), path, write_statistics=True)
    rows = _stats_rows_for_metadata(path, pq.read_metadata(path))
    emitted = {r["column_name"] for r in rows}
    assert emitted == {"id"}
