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
import pytest


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

    def test_write_statistics_flag_cannot_be_disabled(self):
        """Guard: the explicit write_statistics=True kwarg must be passed to
        pq.write_table.  Fails loudly if someone flips or drops it."""
        from supertable.processing import write_parquet_and_collect_resources

        with (
            patch(f"{_MOD}.generate_filename", return_value="data.parquet"),
            patch(f"{_MOD}._get_storage") as mock_gs,
            patch(f"{_MOD}.pq.write_table", wraps=pq.write_table) as mock_write_table,
        ):
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


class TestDataParquetWriteMetadata:
    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_byte_upload_uses_exact_payload_size_without_head(self, mock_gs, _mock_gen):
        from supertable.processing import write_parquet_and_collect_resources

        storage = MagicMock()
        uploaded = {}
        storage.write_bytes.side_effect = lambda path, data: uploaded.update(
            path=path, data=data
        )
        mock_gs.return_value = storage
        resources = []

        write_parquet_and_collect_resources(
            write_df=_df(id=[1, 2], value=["a", "b"]),
            overwrite_columns=[],
            data_dir="/data",
            new_resources=resources,
            compression_level=1,
        )

        assert resources[0]["file_size"] == len(uploaded["data"])
        storage.size.assert_not_called()
        # Directory setup, upload and metadata all use one pinned backend.
        mock_gs.assert_called_once_with()

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_parquet_only_backend_requires_size_metadata(self, mock_gs, _mock_gen):
        from supertable.processing import write_parquet_and_collect_resources

        class ParquetOnlyStorage:
            def makedirs(self, _path):
                pass

            def write_parquet(self, _table, _path):
                pass

            def size(self, _path):
                raise OSError("metadata unavailable")

        mock_gs.return_value = ParquetOnlyStorage()

        with pytest.raises(OSError, match="metadata unavailable"):
            write_parquet_and_collect_resources(
                write_df=_df(id=[1]),
                overwrite_columns=[],
                data_dir="/data",
                new_resources=[],
                compression_level=1,
            )

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_constant_batch_timestamp_does_not_sort(self, mock_gs, _mock_gen):
        from supertable.processing import write_parquet_and_collect_resources
        from supertable.utils.profiler import Profiler

        storage = MagicMock()
        mock_gs.return_value = storage
        profiler = Profiler()
        timestamp = pl.datetime(2026, 1, 1, time_zone="UTC")
        frame = _df(id=[3, 1, 2]).with_columns(timestamp.alias("__timestamp__"))

        write_parquet_and_collect_resources(
            frame, [], "/data", [], compression_level=1, profiler=profiler
        )

        assert "write.sort" not in profiler.timings

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_compaction_timestamp_range_remains_sorted(self, mock_gs, _mock_gen):
        from supertable.processing import write_parquet_and_collect_resources
        from supertable.utils.profiler import Profiler

        storage = MagicMock()
        mock_gs.return_value = storage
        profiler = Profiler()
        frame = _df(id=[3, 1, 2], __timestamp__=[3, 1, 2]).with_columns(
            pl.col("__timestamp__").cast(pl.Datetime("us"))
        )

        write_parquet_and_collect_resources(
            frame, [], "/data", [], compression_level=1, profiler=profiler
        )

        assert profiler.counts["write.sort.n"] == 1
