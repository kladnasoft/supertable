"""Fail-closed read regression for ambiguous physical tombstone identity."""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from supertable.data_classes import TombstoneDef
from supertable.engine.engine_common import (
    TombstoneCache,
    create_reflection_view,
    create_tombstone_view,
)
from supertable.processing import tombstone_digest


@pytest.mark.parametrize("cached", [False, True])
def test_active_dv_rejects_duplicate_rowid_within_referenced_file(
        tmp_path, cached,
):
    """One DV pair must never hide two rows in its physical source file."""
    data_path = tmp_path / "duplicate-source.parquet"
    dv_path = tmp_path / "dv.parquet"
    resource_key = "org/s/tables/t/data/duplicate-source.parquet"

    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": pl.Series([7, 7], dtype=pl.Int64),
    }).write_parquet(data_path)
    dv = pl.DataFrame({
        "__file__": [resource_key],
        "__rowid__": pl.Series([7], dtype=pl.Int64),
    })
    dv.write_parquet(dv_path)

    con = duckdb.connect()
    create_reflection_view(
        con, "src", [str(data_path)], resource_keys=[resource_key],
    )
    tomb = TombstoneDef(
        tombstone_path=str(dv_path),
        cache_key="org/s/tables/t/tombstone/dv.parquet",
        expected_rows=1,
        tombstone_digest=tombstone_digest(dv),
        resource_keys=(resource_key,),
        snapshot_resource_keys=(resource_key,),
    )
    dv_table = None
    if cached:
        dv_table = TombstoneCache(capacity=1).acquire(
            con,
            tomb.cache_key,
            tomb.tombstone_path,
            expected_rows=tomb.expected_rows,
            expected_digest=tomb.tombstone_digest,
        )

    with pytest.raises(RuntimeError, match="duplicate __rowid__"):
        create_tombstone_view(
            con, "src", "live", tomb, dv_table=dv_table,
        )


def test_same_rowid_in_different_files_remains_valid(tmp_path):
    """Composite file identity permits the same row id in distinct files."""
    paths = [tmp_path / "a.parquet", tmp_path / "b.parquet"]
    keys = ["raw/a.parquet", "raw/b.parquet"]
    for ident, path in enumerate(paths, start=1):
        pl.DataFrame({
            "id": [ident],
            "__rowid__": pl.Series([7], dtype=pl.Int64),
        }).write_parquet(path)
    dv_path = tmp_path / "dv.parquet"
    dv = pl.DataFrame({
        "__file__": [keys[0]],
        "__rowid__": pl.Series([7], dtype=pl.Int64),
    })
    dv.write_parquet(dv_path)

    con = duckdb.connect()
    create_reflection_view(
        con, "src", [str(path) for path in paths], resource_keys=keys,
    )
    tomb = TombstoneDef(
        tombstone_path=str(dv_path),
        cache_key="raw/dv.parquet",
        expected_rows=1,
        tombstone_digest=tombstone_digest(dv),
        resource_keys=tuple(keys),
        snapshot_resource_keys=tuple(keys),
    )
    create_tombstone_view(con, "src", "live", tomb)

    assert con.execute("SELECT id FROM live ORDER BY id").fetchall() == [(2,)]
