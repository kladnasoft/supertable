"""Unit tests for the external column-statistics artifact ("stats parquet").

Covers footer→rows extraction per physical/logical type (int/float/date/
timestamp/string/bool), decimal → ``stats_available=False``, null handling,
system-column exclusion, the copy-forward assembly in :func:`build_stats_file`
(drop sunset-file rows + append new rows), and the never-overwrite versioned
filename guarantee.
"""

from __future__ import annotations

import io
from datetime import datetime, date, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable.processing import (
    STATS_SCHEMA,
    build_stats_file,
    extract_stats_rows,
    _route_stats,
    _stats_rows_for_metadata,
)

_MOD = "supertable.processing"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _md_from_df(df: pl.DataFrame):
    """Write *df* to an in-memory parquet (with footer stats) and return its
    parsed ``FileMetaData`` — exactly what the extractor consumes."""
    tbl = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(tbl, buf, write_statistics=True)
    return pq.read_metadata(io.BytesIO(buf.getvalue()))


def _rows_by_col(df: pl.DataFrame, file_path: str = "f.parquet") -> dict:
    rows = _stats_rows_for_metadata(file_path, _md_from_df(df))
    return {r["column_name"]: r for r in rows}


class _FakeStorage:
    """Minimal in-memory storage backend for build_stats_file round-trips."""

    def __init__(self):
        self.store: dict = {}

    def exists(self, path: str) -> bool:
        return path in self.store or path.endswith("stats")

    def makedirs(self, path: str) -> None:
        pass

    def write_bytes(self, path: str, data: bytes) -> None:
        self.store[path] = data

    def read_bytes(self, path: str) -> bytes:
        return self.store[path]

    def read_parquet(self, path: str, columns=None):
        # columns is a projection hint; reading full here is correct (callers
        # select the subset they need) and avoids errors on heterogeneous files.
        return pq.read_table(io.BytesIO(self.store[path]))

    def size(self, path: str) -> int:
        return len(self.store[path])


# ---------------------------------------------------------------------------
# Footer → rows extraction: type routing
# ---------------------------------------------------------------------------

class TestStatsRoutingByType:

    def test_int_routes_to_bigint(self):
        row = _rows_by_col(pl.DataFrame({"a": [3, 1, 2]}))["a"]
        assert row["stats_available"] is True
        assert (row["min_bigint"], row["max_bigint"]) == (1, 3)
        assert row["min_double"] is None and row["min_timestamp"] is None
        assert row["min_string"] is None
        assert row["min_is_exact"] is True and row["max_is_exact"] is True

    def test_float_routes_to_double(self):
        row = _rows_by_col(pl.DataFrame({"a": [2.5, 1.5, 3.5]}))["a"]
        assert row["stats_available"] is True
        assert (row["min_double"], row["max_double"]) == (1.5, 3.5)
        assert row["min_bigint"] is None

    def test_bool_routes_to_bigint_zero_one(self):
        row = _rows_by_col(pl.DataFrame({"a": [True, False, True]}))["a"]
        assert row["stats_available"] is True
        assert (row["min_bigint"], row["max_bigint"]) == (0, 1)

    def test_string_routes_to_string(self):
        row = _rows_by_col(pl.DataFrame({"a": ["c", "a", "b"]}))["a"]
        assert row["stats_available"] is True
        assert (row["min_string"], row["max_string"]) == ("a", "c")
        assert row["min_bigint"] is None and row["min_double"] is None

    def test_date_routes_to_timestamp_midnight(self):
        row = _rows_by_col(pl.DataFrame({"a": [date(2026, 1, 5), date(2026, 1, 1)]}))["a"]
        assert row["stats_available"] is True
        assert row["min_timestamp"] == datetime(2026, 1, 1, 0, 0, 0)
        assert row["max_timestamp"] == datetime(2026, 1, 5, 0, 0, 0)

    def test_timestamp_routes_to_timestamp(self):
        df = pl.DataFrame(
            {"a": [datetime(2026, 1, 1, 10, 0, 0), datetime(2026, 1, 2, 11, 30, 0)]},
            schema={"a": pl.Datetime("us")},
        )
        row = _rows_by_col(df)["a"]
        assert row["stats_available"] is True
        assert row["min_timestamp"] == datetime(2026, 1, 1, 10, 0, 0)
        assert row["max_timestamp"] == datetime(2026, 1, 2, 11, 30, 0)

    def test_tz_aware_timestamp_normalized_to_utc_naive(self):
        df = pl.DataFrame(
            {"a": [datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)]},
            schema={"a": pl.Datetime("us", "UTC")},
        )
        row = _rows_by_col(df)["a"]
        assert row["stats_available"] is True
        assert row["min_timestamp"] == datetime(2026, 1, 1, 10, 0, 0)
        assert row["min_timestamp"].tzinfo is None

    def test_decimal_marks_stats_unavailable(self):
        df = pl.DataFrame(
            {"a": [Decimal("1.50"), Decimal("9.90")]},
            schema={"a": pl.Decimal(precision=10, scale=2)},
        )
        row = _rows_by_col(df)["a"]
        assert row["stats_available"] is False
        # No typed min/max are populated for an unsupported type.
        assert row["min_bigint"] is None and row["max_double"] is None
        assert row["min_string"] is None and row["min_timestamp"] is None


class TestRouteStatsHelper:
    """Direct routing unit tests (independent of parquet roundtrip)."""

    def _stat(self, mn, mx, logical=None):
        s = MagicMock()
        s.min, s.max = mn, mx
        s.logical_type = logical
        return s

    def test_decimal_value_unsupported(self):
        cat, _, _ = _route_stats(self._stat(Decimal("1"), Decimal("2")))
        assert cat is None

    def test_bytes_unsupported(self):
        cat, _, _ = _route_stats(self._stat(b"a", b"b"))
        assert cat is None


# ---------------------------------------------------------------------------
# Null handling & system-column exclusion
# ---------------------------------------------------------------------------

class TestStatsNullAndSystemColumns:

    def test_null_count_recorded(self):
        row = _rows_by_col(pl.DataFrame({"a": [1, None, 3]}))["a"]
        assert row["null_count"] == 1
        assert row["stats_available"] is True
        assert (row["min_bigint"], row["max_bigint"]) == (1, 3)

    def test_rowid_and_timestamp_excluded(self):
        df = pl.DataFrame(
            {
                "a": [1, 2],
                "__rowid__": [10, 11],
                "__timestamp__": [datetime(2026, 1, 1), datetime(2026, 1, 2)],
            },
            schema={
                "a": pl.Int64,
                "__rowid__": pl.Int64,
                "__timestamp__": pl.Datetime("us"),
            },
        )
        cols = set(_rows_by_col(df).keys())
        assert cols == {"a"}

    def test_row_group_rows_and_file_path(self):
        rows = _stats_rows_for_metadata("path/to/x.parquet", _md_from_df(pl.DataFrame({"a": [1, 2, 3]})))
        assert all(r["file_path"] == "path/to/x.parquet" for r in rows)
        assert all(r["row_group_rows"] == 3 for r in rows)


# ---------------------------------------------------------------------------
# extract_stats_rows (storage-backed)
# ---------------------------------------------------------------------------

class TestExtractStatsRows:

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_reads_footer_of_each_file(self, _mock_exists, mock_gs):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        tbl = df.to_arrow()
        buf = io.BytesIO()
        pq.write_table(tbl, buf, write_statistics=True)
        stor = MagicMock()
        stor.read_bytes.return_value = buf.getvalue()
        mock_gs.return_value = stor

        out = extract_stats_rows(["one.parquet"])
        assert out.height == 2  # one row per column
        assert set(out["column_name"].to_list()) == {"a", "b"}
        assert list(out.columns) == list(STATS_SCHEMA.keys())

    def test_empty_input_returns_schema_frame(self):
        out = extract_stats_rows([])
        assert out.height == 0
        assert list(out.columns) == list(STATS_SCHEMA.keys())

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_footer_md_cache_skips_readback(self, _mock_exists, mock_gs):
        """R1: a cached footer is consumed in place — no storage round-trip."""
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        buf = io.BytesIO()
        pq.write_table(df.to_arrow(), buf, write_statistics=True)
        md = pq.read_metadata(io.BytesIO(buf.getvalue()))

        stor = MagicMock()
        mock_gs.return_value = stor

        out = extract_stats_rows(["one.parquet"], footer_md_cache={"one.parquet": md})
        assert out.height == 2
        assert set(out["column_name"].to_list()) == {"a", "b"}
        stor.read_bytes.assert_not_called()  # the whole point: no re-download

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_cached_footer_matches_readback(self, _mock_exists, mock_gs):
        """R1 correctness invariant: for the SAME bytes, the cached-footer path
        and the readback path yield IDENTICAL stats rows.  In production the
        cached footer is parsed from the exact bytes uploaded via write_bytes, so
        this equivalence guarantees R1 changes no stats value."""
        df = pl.DataFrame({"a": [3, 1, 2], "b": ["c", "a", "b"]})
        buf = io.BytesIO()
        pq.write_table(df.to_arrow(), buf, write_statistics=True)
        raw = buf.getvalue()

        stor = MagicMock()
        stor.read_bytes.return_value = raw
        mock_gs.return_value = stor

        readback = extract_stats_rows(["one.parquet"])
        cached = extract_stats_rows(
            ["one.parquet"], footer_md_cache={"one.parquet": pq.read_metadata(io.BytesIO(raw))}
        )
        assert readback.equals(cached)

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_partial_cache_falls_back_for_uncached_path(self, _mock_exists, mock_gs):
        """A path absent from the cache must still be read back (mixed write_bytes
        / re-encode batches), while the cached one skips the read."""
        df = pl.DataFrame({"a": [1, 2]})
        buf = io.BytesIO()
        pq.write_table(df.to_arrow(), buf, write_statistics=True)
        raw = buf.getvalue()
        md = pq.read_metadata(io.BytesIO(raw))

        stor = MagicMock()
        stor.read_bytes.return_value = raw
        mock_gs.return_value = stor

        out = extract_stats_rows(
            ["cached.parquet", "uncached.parquet"], footer_md_cache={"cached.parquet": md}
        )
        assert set(out["file_path"].to_list()) == {"cached.parquet", "uncached.parquet"}
        stor.read_bytes.assert_called_once()  # only the uncached path hit storage


# ---------------------------------------------------------------------------
# build_stats_file: copy-forward + versioning
# ---------------------------------------------------------------------------

class TestBuildStatsFile:

    def _stats_df(self, file_path: str, value: int) -> pl.DataFrame:
        base = {k: [None] for k in STATS_SCHEMA}
        base["file_path"] = [file_path]
        base["row_group_id"] = [0]
        base["column_name"] = ["a"]
        base["physical_type"] = ["INT64"]
        base["logical_type"] = [""]
        base["min_bigint"] = [value]
        base["max_bigint"] = [value]
        base["null_count"] = [0]
        base["row_group_rows"] = [1]
        base["stats_available"] = [True]
        base["min_is_exact"] = [True]
        base["max_is_exact"] = [True]
        return pl.DataFrame(base, schema=STATS_SCHEMA)

    @patch(f"{_MOD}._get_storage")
    def test_no_change_carries_forward(self, mock_gs):
        mock_gs.return_value = _FakeStorage()
        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path="t/stats/prev.parquet",
            new_rows=None,
            removed_files=set(),
            compression_level=1,
        )
        assert path == "t/stats/prev.parquet"
        assert combined is None

    @patch(f"{_MOD}._get_storage")
    def test_first_write_creates_versioned_file(self, mock_gs):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path=None,
            new_rows=self._stats_df("fileA.parquet", 5),
            removed_files=set(),
            compression_level=1,
        )
        assert path is not None and path.startswith("t/stats/")
        assert path.endswith(".parquet") and "stats" in path
        assert combined.height == 1
        assert path in stor.store  # an actual file was written

    @patch(f"{_MOD}._safe_exists", return_value=True)
    @patch(f"{_MOD}._get_storage")
    def test_copy_forward_drops_sunset_and_appends_new(self, mock_gs, _ex):
        stor = _FakeStorage()
        mock_gs.return_value = stor

        # Seed a previous stats file holding rows for fileA + fileB.
        prev = pl.concat(
            [self._stats_df("fileA.parquet", 1), self._stats_df("fileB.parquet", 2)],
            how="vertical",
        )
        prev_path = "t/stats/prev.parquet"
        stor.write_bytes(prev_path, _serialize(prev))

        # New write sunsets fileA and writes fileC.
        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path=prev_path,
            new_rows=self._stats_df("fileC.parquet", 3),
            removed_files={"fileA.parquet"},
            compression_level=1,
        )
        files = set(combined["file_path"].to_list())
        assert files == {"fileB.parquet", "fileC.parquet"}
        assert path != prev_path  # never overwrites the previous artifact

    @patch(f"{_MOD}._safe_exists", return_value=True)
    @patch(f"{_MOD}._get_storage")
    def test_removed_only_rewrites_without_new_rows(self, mock_gs, _ex):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        prev = pl.concat(
            [self._stats_df("fileA.parquet", 1), self._stats_df("fileB.parquet", 2)],
            how="vertical",
        )
        prev_path = "t/stats/prev.parquet"
        stor.write_bytes(prev_path, _serialize(prev))

        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path=prev_path,
            new_rows=None,
            removed_files={"fileA.parquet"},
            compression_level=1,
        )
        assert path != prev_path
        assert set(combined["file_path"].to_list()) == {"fileB.parquet"}


def _serialize(df: pl.DataFrame) -> bytes:
    tbl = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(tbl, buf, write_statistics=True)
    return buf.getvalue()
