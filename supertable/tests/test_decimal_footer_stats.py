"""STREAD-011: a Decimal column must not abort the write while reading stats.

``_route_stats`` read ``stat.min``/``stat.max`` and only *then* checked whether
the column was a decimal it had no use for. Polars writes a Decimal column as
INT64 carrying a DECIMAL logical type and the footer advertises
``has_min_max = True``, but PyArrow cannot decode bounds in that encoding:
touching ``stat.min`` raises ``ArrowNotImplementedError: Cannot extract
statistics for type``. So a table with one Decimal column could not be ingested
at all, even though the decimal values themselves round-trip intact.

The fix is ordering — rule the type out before reading the values. Decimal was
already deliberately unsupported for pruning (routing it through double is
lossy and could exclude a file that does hold a matching row), so nothing about
which files prune changes here; the write simply stops reading bounds it was
always going to discard.

Both decimal encodings are covered because they fail differently: polars-native
is INT64 + DECIMAL logical type (undecodable), while PyArrow's is
FIXED_LEN_BYTE_ARRAY whose bounds decode fine — so the value-based check is
still reachable and is not dead code.
"""

from __future__ import annotations

import decimal

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable.processing import _route_stats, _stats_rows_for_metadata

ROWS = {
    "eid": [1, 2, 3],
    "amt": [decimal.Decimal("1.500"), decimal.Decimal("-2.250"), decimal.Decimal("3.000")],
    "note": ["a", "b", "c"],
    "val": [1.5, 2.5, 3.5],
}
SCHEMA = {"eid": pl.Int64, "amt": pl.Decimal(12, 3), "note": pl.String, "val": pl.Float64}


@pytest.fixture(params=[False, True], ids=["polars_native", "pyarrow"])
def footer(request, tmp_path):
    """Parquet footer for each decimal encoding.

    ``use_pyarrow=False`` is the encoding the audit hit; ``True`` is the one
    whose bounds decode, kept so the value-based guard stays exercised.
    """
    path = tmp_path / f"dec_{request.param}.parquet"
    pl.DataFrame(ROWS, schema=SCHEMA).write_parquet(path, use_pyarrow=request.param)
    return pq.read_metadata(path)


def _column(footer, name):
    index = next(
        i for i in range(footer.num_columns) if footer.schema.column(i).name == name
    )
    return footer.row_group(0).column(index).statistics


class TestDecimalStatsDoNotAbortTheWrite:

    def test_routing_a_decimal_column_does_not_raise(self, footer):
        """The regression: this raised ArrowNotImplementedError."""
        assert _route_stats(_column(footer, "amt")) == (None, None, None)

    def test_the_whole_footer_converts_to_stats_rows(self, footer):
        """The write path's actual call site, not just the helper.

        ``_stats_rows_for_metadata`` iterates every column, so one undecodable
        decimal took the entire stats artifact — and therefore the write — down
        with it.
        """
        rows = _stats_rows_for_metadata("data/part-0.parquet", footer)
        assert {r["column_name"] for r in rows} == set(ROWS)

    def test_the_decimal_column_is_recorded_but_never_prunable(self, footer):
        """Conservative by design: no usable range means it cannot exclude a file.

        The column still appears in the artifact (so per-column
        ``compressed_bytes`` stays available for projection sizing) — it just
        carries no bounds.
        """
        rows = _stats_rows_for_metadata("data/part-0.parquet", footer)
        amt = next(r for r in rows if r["column_name"] == "amt")
        assert amt["stats_available"] is False
        assert amt["min_bigint"] is None and amt["max_bigint"] is None
        assert amt["min_double"] is None and amt["max_double"] is None

    @pytest.mark.parametrize("column,category,minimum", [
        ("eid", "bigint", 1),
        ("note", "string", "a"),
        ("val", "double", 1.5),
    ])
    def test_the_other_columns_keep_their_stats(self, footer, column, category, minimum):
        """A decimal sharing the file must not cost its neighbours their bounds."""
        assert _route_stats(_column(footer, column))[:2] == (category, minimum)


class TestDecimalValuesAreUnaffected:

    def test_values_and_type_round_trip_exactly(self, tmp_path):
        """The data was never the problem — only reading its footer stats was.

        Asserted so a future "fix" that changes the physical encoding to make
        statistics readable has to justify itself against this.
        """
        path = tmp_path / "roundtrip.parquet"
        original = pl.DataFrame(ROWS, schema=SCHEMA)
        original.write_parquet(path, use_pyarrow=False)
        read_back = pl.read_parquet(path)
        assert read_back["amt"].dtype == pl.Decimal(12, 3)
        assert read_back["amt"].to_list() == ROWS["amt"]
