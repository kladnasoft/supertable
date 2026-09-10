"""Parquet footer statistics must always be written.

Every data file SuperTable writes must embed per-row-group, per-column
statistics in its footer (min/max/null_count).  This is what lets DuckDB skip
row groups during filtered scans (predicate pushdown), so disabling it would
silently regress query performance.  These tests lock the guarantee from two
angles: the real written bytes must carry footer stats, and the explicit
``write_statistics=True`` kwarg must never be dropped or flipped.
"""

from __future__ import annotations

import io
import os

import tempfile
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow.parquet as pq


_MOD = "supertable.processing"


def _df(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)




class TestParquetStatisticsAlwaysWritten:
    """write_statistics must never be disabled."""

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_written_file_footer_has_statistics(self, mock_gs, mock_gen):
        """Behavioral: read the bytes we actually wrote and assert the footer
        carries min/max for every column in every row group."""
        from supertable.processing import write_parquet_and_collect_resources

        captured: dict = {}
        mock_stor = MagicMock()
        mock_stor.exists.return_value = True
        mock_stor.size.return_value = 1234
        mock_stor.write_bytes.side_effect = lambda path, data: captured.update(data=data)
        mock_gs.return_value = mock_stor

        df = _df(id=[3, 1, 2], val=["c", "a", "b"])
        write_parquet_and_collect_resources(
            write_df=df,
            overwrite_columns=["id"],
            data_dir="/data",
            new_resources=[],
            compression_level=10,
        )

        assert "data" in captured, "primary write path must call write_bytes"
        meta = pq.read_metadata(io.BytesIO(captured["data"]))
        assert meta.num_row_groups >= 1
        for rg in range(meta.num_row_groups):
            row_group = meta.row_group(rg)
            for c in range(row_group.num_columns):
                col = row_group.column(c)
                assert col.is_stats_set, f"{col.path_in_schema}: no statistics in footer"
                assert col.statistics is not None
                assert col.statistics.has_min_max

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_statistics_flag_cannot_be_disabled(self, mock_gs, mock_gen):
        """Guard: the encoder must be asked for statistics explicitly.

        Encoding goes through polars' writer (``DataFrame.write_parquet``),
        not ``pq.write_table`` — this asserts the kwarg on whichever writer is
        actually in use, so dropping or flipping it fails loudly.
        """
        from supertable.processing import write_parquet_and_collect_resources

        mock_stor = MagicMock()
        mock_stor.exists.return_value = True
        mock_stor.size.return_value = 1234
        mock_gs.return_value = mock_stor

        seen: dict = {}
        real_write = pl.DataFrame.write_parquet

        def _spy(self, file, **kwargs):
            seen.update(kwargs)
            return real_write(self, file, **kwargs)

        with patch.object(pl.DataFrame, "write_parquet", _spy):
            write_parquet_and_collect_resources(
                write_df=_df(id=[1, 2], val=["a", "b"]),
                overwrite_columns=["id"],
                data_dir="/data",
                new_resources=[],
                compression_level=10,
            )

        assert seen, "the polars writer must be the encode path"
        assert seen.get("statistics") is True
        assert seen.get("compression") == "zstd"

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_duckdb_filtered_scans_are_correct_across_row_groups(self, mock_gs, mock_gen):
        """DuckDB must read these files correctly *while* pruning on them.

        Footer stats being present (previous test) is necessary but not
        sufficient: if the recorded min/max disagreed with the data, DuckDB
        would skip row groups that actually contain matching rows and silently
        return too few. So this drives predicates that straddle row-group
        boundaries and asserts exact results — wrong stats produce wrong
        answers here, which is the failure that matters.

        Note this does not assert row groups were *skipped*. DuckDB reports
        post-filter cardinality, so the plan looks identical pruned or not;
        proving the skip needs an I/O or timing measurement, which does not
        belong in a unit test. (Measured separately: ~8x faster on a narrow
        predicate over a 163-row-group file.) Do not use
        ``parquet_metadata()`` to check this — it surfaces only the deprecated
        v1 ``min``/``max`` columns and reports NULL for files written here,
        while the reader prunes on ``min_value``/``max_value``.
        """
        import duckdb
        from supertable.processing import write_parquet_and_collect_resources

        captured: dict = {}
        mock_stor = MagicMock()
        mock_stor.exists.return_value = True
        mock_stor.size.return_value = 1234
        mock_stor.write_bytes.side_effect = lambda path, data: captured.update(data=data)
        mock_gs.return_value = mock_stor

        rows = 5_000
        df = _df(id=list(range(rows)), val=[f"v{i}" for i in range(rows)])

        # Small row groups so several exist without a slow test.
        with patch(f"{_MOD}._PARQUET_ROW_GROUP_SIZE", 500):
            write_parquet_and_collect_resources(
                write_df=df, overwrite_columns=["id"], data_dir="/data",
                new_resources=[], compression_level=1,
            )

        meta = pq.read_metadata(io.BytesIO(captured["data"]))
        assert meta.num_row_groups >= 8, "need several row groups to prove pruning"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as fh:
            fh.write(captured["data"])
            path = fh.name
        try:
            con = duckdb.connect()
            # The file reads back intact.
            assert con.execute(
                f"SELECT count(*), sum(id) FROM parquet_scan('{path}')"
            ).fetchone() == (rows, sum(range(rows)))

            # Predicates inside, spanning, and straddling row-group edges
            # (row groups are 500 wide here).
            for lo, hi in [(10, 20), (499, 501), (0, 0), (4999, 4999),
                           (250, 1750), (1999, 2000), (0, rows - 1)]:
                got = con.execute(
                    f"SELECT count(*), min(id), max(id) FROM parquet_scan('{path}') "
                    f"WHERE id BETWEEN {lo} AND {hi}"
                ).fetchone()
                assert got == (hi - lo + 1, lo, hi), (
                    f"filtered scan wrong for [{lo},{hi}]: {got} — "
                    f"footer min/max disagree with the data"
                )

            # A value in no row group must return nothing, not an error.
            assert con.execute(
                f"SELECT count(*) FROM parquet_scan('{path}') WHERE id = {rows + 5}"
            ).fetchone()[0] == 0
        finally:
            os.unlink(path)
