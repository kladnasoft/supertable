"""Projection-aware read sizing in :class:`DataEstimator`.

Regression + contract for the estimator over-counting fix: a query that selects
a few columns from a wide table used to be charged the WHOLE on-disk file size
of every surviving file (all columns), inflating the AUTO-engine size estimate
by 10-100x and mis-routing small reads to Pro/Spark.  The estimator now charges
only the selected columns' on-disk (compressed) bytes:

  * ``_selected_columns`` resolves the projected column set (``SELECT *`` and
    unknown/only-system projections mean "whole table" -> no savings);
  * ``_projected_bytes_index`` sums per-column ``compressed_bytes`` from the
    stats artifact per file (the precise "Tier 3" path), trusting only files
    whose matched rows are all non-NULL (an older carried-forward row -> fall
    back rather than under-count to zero);
  * ``_ratio_bytes`` is the type-width fallback used when per-column bytes are
    unavailable (old stats file / no stats), scaling the whole-file size by the
    selected columns' width share;
  * ``_type_width`` maps stored type names to rough per-value widths.

These are pure helpers, so the tests build the estimator with ``__new__`` (no
Redis / storage) and exercise the sizing math directly.
"""
from __future__ import annotations

import types

import polars as pl
import pytest

from supertable.engine.data_estimator import DataEstimator
from supertable.processing import ROWID_COL, TIMESTAMP_COL


def _est(tables=None, predicate_constraints=None) -> DataEstimator:
    est = DataEstimator.__new__(DataEstimator)
    est.tables = tables or []
    est.predicate_constraints = predicate_constraints or {}
    return est


def _td(super_name, simple_name, columns):
    # Minimal stand-in for TableDefinition (only these 3 attrs are read).
    return types.SimpleNamespace(
        super_name=super_name, simple_name=simple_name, columns=list(columns)
    )


class TestSelectedColumns:

    def test_select_star_returns_none(self):
        # [] columns == SELECT * / t.* == whole table (no projection savings).
        est = _est([_td("demo", "facts", [])])
        assert est._selected_columns("demo", "facts") is None

    def test_specific_columns_lowercased(self):
        est = _est([_td("demo", "facts", ["Client", "Amount"])])
        assert est._selected_columns("demo", "facts") == {"client", "amount"}

    def test_system_columns_stripped(self):
        # The read view hides __rowid__/__timestamp__, so they're never scanned.
        est = _est([_td("demo", "facts", ["amount", ROWID_COL, TIMESTAMP_COL])])
        assert est._selected_columns("demo", "facts") == {"amount"}

    def test_union_across_occurrences(self):
        est = _est([_td("demo", "facts", ["a"]), _td("demo", "facts", ["b"])])
        assert est._selected_columns("demo", "facts") == {"a", "b"}

    def test_star_in_any_occurrence_wins(self):
        est = _est([_td("demo", "facts", ["a"]), _td("demo", "facts", [])])
        assert est._selected_columns("demo", "facts") is None

    def test_unknown_table_returns_none(self):
        est = _est([_td("demo", "facts", ["a"])])
        assert est._selected_columns("demo", "other") is None

    def test_only_system_columns_returns_none(self):
        est = _est([_td("demo", "facts", [ROWID_COL, TIMESTAMP_COL])])
        assert est._selected_columns("demo", "facts") is None


class TestTypeWidth:

    @pytest.mark.parametrize(
        "type_name,width",
        [
            ("BIGINT", 8), ("bigint", 8), ("INT64", 8), ("LONG", 8),
            ("integer", 4), ("INTEGER", 4), ("INT", 4), ("INT32", 4),
            ("smallint", 2), ("INT16", 2), ("tinyint", 1), ("INT8", 1),
            ("DOUBLE", 8), ("float64", 8), ("FLOAT", 4), ("real", 4),
            ("BOOLEAN", 1), ("bool", 1), ("TIMESTAMP", 8), ("datetime", 8),
            ("DATE", 4), ("VARCHAR", 16), ("TEXT", 16), ("utf8", 16),
            ("DECIMAL(10,2)", 8), ("", 8), (None, 8), ("weirdtype", 8),
        ],
    )
    def test_widths(self, type_name, width):
        assert _est()._type_width(type_name) == width


class TestProjectedBytesIndex:

    def _stats(self, rows):
        return pl.DataFrame(
            rows,
            schema={
                "file_path": pl.Utf8,
                "column_name": pl.Utf8,
                "compressed_bytes": pl.Int64,
            },
        )

    def test_sums_only_selected_columns_per_file(self):
        # The core win: a huge string column is NOT charged when only the two
        # small columns are selected (300 bytes vs the ~100 KB whole file).
        est = _est()
        stats = self._stats([
            {"file_path": "f1", "column_name": "a", "compressed_bytes": 100},
            {"file_path": "f1", "column_name": "b", "compressed_bytes": 200},
            {"file_path": "f1", "column_name": "big_str", "compressed_bytes": 100_000},
        ])
        tier3, proj = est._projected_bytes_index(stats, {"a", "b"})
        assert tier3 == {"f1"}
        assert proj == {"f1": 300}

    def test_case_insensitive_column_match(self):
        est = _est()
        stats = self._stats([
            {"file_path": "f1", "column_name": "Amount", "compressed_bytes": 50},
        ])
        _, proj = est._projected_bytes_index(stats, {"amount"})
        assert proj == {"f1": 50}

    def test_sums_across_row_groups(self):
        # Two row groups of the same column in one file -> summed.
        est = _est()
        stats = self._stats([
            {"file_path": "f1", "column_name": "a", "compressed_bytes": 40},
            {"file_path": "f1", "column_name": "a", "compressed_bytes": 60},
        ])
        _, proj = est._projected_bytes_index(stats, {"a"})
        assert proj == {"f1": 100}

    def test_null_bytes_excludes_file_from_tier3(self):
        # A carried-forward pre-upgrade row (NULL) must not be counted as zero;
        # the file is dropped from the trusted set so the caller falls back.
        est = _est()
        stats = self._stats([
            {"file_path": "f1", "column_name": "a", "compressed_bytes": None},
            {"file_path": "f2", "column_name": "a", "compressed_bytes": 10},
        ])
        tier3, proj = est._projected_bytes_index(stats, {"a"})
        assert tier3 == {"f2"}
        assert proj == {"f2": 10}

    def test_missing_compressed_bytes_column(self):
        # An old stats artifact without the column -> no precise data at all.
        est = _est()
        stats = pl.DataFrame({"file_path": ["f1"], "column_name": ["a"]})
        assert est._projected_bytes_index(stats, {"a"}) == (set(), {})

    def test_no_matching_columns(self):
        est = _est()
        stats = self._stats([
            {"file_path": "f1", "column_name": "a", "compressed_bytes": 10},
        ])
        assert est._projected_bytes_index(stats, {"zzz"}) == (set(), {})

    def test_none_or_empty_stats(self):
        est = _est()
        assert est._projected_bytes_index(None, {"a"}) == (set(), {})
        empty = self._stats([])
        assert est._projected_bytes_index(empty, {"a"}) == (set(), {})


class TestRatioBytes:

    def test_scales_by_selected_type_width_share(self):
        est = _est()
        # 3 bigint (8 each) + 1 varchar (16); select the 3 bigints.
        # share = (8+8+8) / (8+8+8+16) = 24/40 = 0.6 -> 1000 * 0.6 = 600
        schema_types = {"a": "BIGINT", "b": "BIGINT", "c": "BIGINT", "blob": "VARCHAR"}
        assert est._ratio_bytes("f1", {"f1": 1000}, {"a", "b", "c"}, schema_types) == 600

    def test_no_schema_types_returns_full(self):
        est = _est()
        assert est._ratio_bytes("f1", {"f1": 500}, {"a"}, {}) == 500

    def test_zero_file_size_returns_zero(self):
        est = _est()
        assert est._ratio_bytes("f1", {"f1": 0}, {"a"}, {"a": "BIGINT"}) == 0

    def test_system_columns_excluded_from_denominator(self):
        est = _est()
        # Only 'a' is a real column; rowid/ts are stripped -> selecting 'a'
        # charges the whole file (a is 100% of the scannable width).
        schema_types = {"a": "BIGINT", ROWID_COL: "BIGINT", TIMESTAMP_COL: "TIMESTAMP"}
        assert est._ratio_bytes("f1", {"f1": 800}, {"a"}, schema_types) == 800

    def test_selected_not_in_schema_returns_full(self):
        est = _est()
        # Defensive: selected col absent from schema -> can't scale, keep full.
        assert est._ratio_bytes("f1", {"f1": 700}, {"ghost"}, {"a": "BIGINT"}) == 700
