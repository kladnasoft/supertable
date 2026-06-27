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
    @patch(f"{_MOD}.pq.write_table")
    def test_write_statistics_flag_cannot_be_disabled(self, mock_write_table, mock_gs, mock_gen):
        """Guard: the explicit write_statistics=True kwarg must be passed to
        pq.write_table.  Fails loudly if someone flips or drops it."""
        from supertable.processing import write_parquet_and_collect_resources

        mock_stor = MagicMock()
        mock_stor.exists.return_value = True
        mock_stor.size.return_value = 1234
        mock_gs.return_value = mock_stor

        write_parquet_and_collect_resources(
            write_df=_df(id=[1, 2], val=["a", "b"]),
            overwrite_columns=["id"],
            data_dir="/data",
            new_resources=[],
            compression_level=10,
        )

        assert mock_write_table.called
        assert mock_write_table.call_args.kwargs.get("write_statistics") is True
