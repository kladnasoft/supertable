"""
Regression and edge-case tests for the column-order bug in
``supertable.processing._align_to_schema``.

# Background

The original implementation projected via ``df.with_columns(exprs)``, which
preserves *df*'s original column order and appends new columns at the end.
That violated the precondition of the subsequent
``polars.concat(..., how="vertical_relaxed")`` call (which matches columns
*positionally*), causing production failures of the form::

    polars.exceptions.ComputeError:
        schema names differ: got param_chosen_payment_method, expected param_content

The failure is **structural**: any time two parquet files in the same
compaction have different column subsets (or different orderings), the
positional concat would explode.

The fix re-implements ``_align_to_schema`` with ``df.select(exprs)`` so the
output column order is **always** ``list(target_schema.keys())``, regardless
of what *df*'s own column order happened to be.  A new
``concat_many_with_union`` helper is also tested here — it computes the union
schema once across N frames and aligns/concats in a single pass (useful when
merging many parquet files with dynamic column sets, e.g. GA4 ``param_*``).

# What these tests pin

  1. _align_to_schema returns columns in **exact** target order (regression).
  2. concat_with_union succeeds when both frames have different column subsets,
     including the exact failure pattern observed in production.
  3. concat_many_with_union behaves like an N-way fold of concat_with_union
     but with a single schema-union pass.
  4. Type widening, null filling, and empty-frame handling work as documented.
  5. The loop pattern used inside ``process_files_with_overlap`` /
     ``process_files_without_overlap`` (repeated pairwise concat) no longer
     fails on schema-divergent inputs.
"""

from __future__ import annotations

import os
import random
from typing import List

import polars as pl
import pytest

# Make sure environment is set up before importing the package, mirroring
# the convention used in test_redis_key_prefix.py.
os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.processing import (  # noqa: E402
    _align_to_schema,
    _union_schema,
    _union_schema_many,
    concat_with_union,
    concat_many_with_union,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)


# ===========================================================================
# 1. _align_to_schema column-order invariant
# ===========================================================================

class TestAlignToSchemaColumnOrder:
    """The post-condition: result.columns == list(target_schema.keys())."""

    def test_output_order_matches_target_when_columns_match(self):
        df = _df(b=[1], a=[2])  # df order: [b, a]
        target = {"a": pl.Int64, "b": pl.Int64}  # target order: [a, b]
        result = _align_to_schema(df, target)
        assert result.columns == ["a", "b"]

    def test_output_order_matches_target_when_missing_columns(self):
        df = _df(b=[1])
        target = {"a": pl.Utf8, "b": pl.Int64}
        result = _align_to_schema(df, target)
        assert result.columns == ["a", "b"]
        assert result["a"].to_list() == [None]

    def test_output_order_matches_target_when_subset(self):
        df = _df(a=[1], b=[2], c=[3])
        target = {"b": pl.Int64, "a": pl.Int64}  # target picks only b, a
        result = _align_to_schema(df, target)
        assert result.columns == ["b", "a"]
        # Column 'c' is dropped because target doesn't include it
        assert "c" not in result.columns

    def test_data_preserved_under_reordering(self):
        df = _df(b=[10, 20], a=[1, 2])
        target = {"a": pl.Int64, "b": pl.Int64}
        result = _align_to_schema(df, target)
        assert result["a"].to_list() == [1, 2]
        assert result["b"].to_list() == [10, 20]

    def test_empty_target_returns_df_unchanged(self):
        df = _df(a=[1])
        result = _align_to_schema(df, {})
        assert result.equals(df)

    def test_dtype_cast_applied_and_order_kept(self):
        df = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int32), "y": [3, 4]})
        target = {"y": pl.Int64, "x": pl.Float64}
        result = _align_to_schema(df, target)
        assert result.columns == ["y", "x"]
        assert result["x"].dtype == pl.Float64
        assert result["y"].dtype == pl.Int64

    def test_all_missing_columns(self):
        df = pl.DataFrame(schema={"existing": pl.Int64})
        target = {"new1": pl.Utf8, "new2": pl.Float64}
        result = _align_to_schema(df, target)
        assert result.columns == ["new1", "new2"]
        # Original frame had zero rows → result also has zero rows
        assert result.height == 0


# ===========================================================================
# 2. concat_with_union — production-bug reproduction
# ===========================================================================

class TestConcatWithUnionProductionBug:
    """The exact failure pattern seen in GA4 compaction logs."""

    def test_disjoint_columns_with_alphabetical_a(self):
        # Frame 'a' has both columns in alphabetical order (the engine's
        # deterministic dump order).  Frame 'b' is missing the first one.
        a = _df(param_chosen_payment_method=["paypal"], param_content=["home"])
        b = _df(param_content=["detail", "list"])
        result = concat_with_union(a, b)
        assert result.height == 3
        assert set(result.columns) == {"param_chosen_payment_method", "param_content"}

    def test_disjoint_columns_with_alphabetical_b(self):
        # Symmetric: now 'b' is the one carrying the wider schema.
        a = _df(param_content=["home", "detail"])
        b = _df(param_chosen_payment_method=["paypal"], param_content=["list"])
        result = concat_with_union(a, b)
        assert result.height == 3
        assert set(result.columns) == {"param_chosen_payment_method", "param_content"}

    def test_each_frame_has_exclusive_column(self):
        # Each frame contributes a column the other lacks.
        a = _df(only_in_a=["x"], shared=["s1"])
        b = _df(shared=["s2"], only_in_b=["y"])
        result = concat_with_union(a, b)
        assert result.height == 2
        assert set(result.columns) == {"only_in_a", "shared", "only_in_b"}

    def test_existing_frame_has_reverse_order(self):
        # 'a' has cols [x, y]; 'b' has the same cols but in order [y, x].
        # Without the fix, both would survive _align_to_schema unchanged,
        # and the positional concat would fail.
        a = _df(x=[1], y=[2])
        b = _df(y=[3], x=[4])
        result = concat_with_union(a, b)
        assert result.height == 2
        assert result["x"].to_list() == [1, 4]
        assert result["y"].to_list() == [2, 3]

    def test_concat_with_union_is_order_independent(self):
        # The result of (a + b) and (b + a) should contain the same rows
        # (in different vertical orders, but with the same column set).
        a = _df(only_in_a=["x"], shared=["s1"])
        b = _df(shared=["s2"], only_in_b=["y"])
        r1 = concat_with_union(a, b)
        r2 = concat_with_union(b, a)
        assert set(r1.columns) == set(r2.columns)
        # Stack rows and compare as multisets
        r1_rows = set(map(tuple, r1.select(sorted(r1.columns)).rows()))
        r2_rows = set(map(tuple, r2.select(sorted(r2.columns)).rows()))
        assert r1_rows == r2_rows


# ===========================================================================
# 3. concat_with_union — empty / single-frame edge cases
# ===========================================================================

class TestConcatWithUnionEdgeCases:

    def test_empty_a_returns_b(self):
        a = pl.DataFrame(schema={"id": pl.Int64})
        b = _df(id=[1, 2])
        result = concat_with_union(a, b)
        assert result.height == 2

    def test_empty_b_returns_a(self):
        a = _df(id=[1, 2])
        b = pl.DataFrame(schema={"id": pl.Int64})
        result = concat_with_union(a, b)
        assert result.height == 2

    def test_both_empty(self):
        a = pl.DataFrame(schema={"id": pl.Int64})
        b = pl.DataFrame(schema={"id": pl.Int64})
        result = concat_with_union(a, b)
        assert result.height == 0

    def test_type_widening_int_float(self):
        a = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int32)})
        b = pl.DataFrame({"x": pl.Series([2.5], dtype=pl.Float64)})
        result = concat_with_union(a, b)
        assert result.height == 2
        assert result["x"].dtype == pl.Float64

    def test_type_widening_int_string(self):
        a = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int64)})
        b = pl.DataFrame({"x": pl.Series(["hello"], dtype=pl.Utf8)})
        result = concat_with_union(a, b)
        assert result.height == 2
        assert result["x"].dtype == pl.Utf8
        # The Int64 1 is cast strict=False; for int → str the cast succeeds.
        assert result["x"].to_list() == ["1", "hello"]


# ===========================================================================
# 4. _union_schema_many — preserves first-appearance order
# ===========================================================================

class TestUnionSchemaMany:

    def test_single_frame(self):
        f = _df(a=[1], b=[2])
        target = _union_schema_many([f])
        assert list(target.keys()) == ["a", "b"]

    def test_two_frames_first_appearance_order(self):
        a = _df(x=[1], y=[2])
        b = _df(y=[3], z=[4])
        target = _union_schema_many([a, b])
        # x is from a, y from both (a wins), z from b → [x, y, z]
        assert list(target.keys()) == ["x", "y", "z"]

    def test_three_frames_first_appearance_order(self):
        f1 = _df(c=[1])
        f2 = _df(a=[2], c=[3])
        f3 = _df(b=[4])
        target = _union_schema_many([f1, f2, f3])
        # c first appears in f1 → first; a from f2 → second; b from f3 → third
        assert list(target.keys()) == ["c", "a", "b"]

    def test_dtype_widening_across_three_frames(self):
        f1 = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int8)})
        f2 = pl.DataFrame({"x": pl.Series([2], dtype=pl.Int32)})
        f3 = pl.DataFrame({"x": pl.Series([3], dtype=pl.Int64)})
        target = _union_schema_many([f1, f2, f3])
        # All ints widen to Int64
        assert target["x"] == pl.Int64

    def test_dtype_widening_to_utf8_across_three_frames(self):
        f1 = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int64)})
        f2 = pl.DataFrame({"x": pl.Series([1.0], dtype=pl.Float64)})
        f3 = pl.DataFrame({"x": pl.Series(["s"], dtype=pl.Utf8)})
        target = _union_schema_many([f1, f2, f3])
        assert target["x"] == pl.Utf8

    def test_empty_list_returns_empty_schema(self):
        target = _union_schema_many([])
        assert target == {}

    def test_pairwise_helper_matches_many(self):
        a = _df(x=[1], y=[2])
        b = _df(y=[3], z=[4])
        assert _union_schema(a, b) == _union_schema_many([a, b])


# ===========================================================================
# 5. concat_many_with_union — N-way merge
# ===========================================================================

class TestConcatManyWithUnion:

    def test_three_frames_disjoint_columns(self):
        f1 = _df(a=[1])
        f2 = _df(b=[2])
        f3 = _df(c=[3])
        result = concat_many_with_union([f1, f2, f3])
        assert result.height == 3
        assert set(result.columns) == {"a", "b", "c"}

    def test_three_frames_dynamic_columns_ga4_style(self):
        # Each "parquet" has a different subset of param_* columns,
        # in alphabetical-by-engine order.  This mirrors the production
        # workload that triggered the bug.
        f_purchase = _df(
            event_id=[1, 2],
            param_chosen_payment_method=["paypal", "card"],
            param_content=["home", "detail"],
            param_value=[10.0, 20.0],
        )
        f_view = _df(
            event_id=[3, 4, 5],
            param_content=["list", "detail", "home"],
            param_page_location=["/a", "/b", "/c"],
        )
        f_click = _df(
            event_id=[6],
            param_button_id=["x"],
            param_content=["banner"],
        )
        result = concat_many_with_union([f_purchase, f_view, f_click])
        assert result.height == 6
        # All param_* columns from all frames present
        for col in (
            "event_id",
            "param_chosen_payment_method",
            "param_content",
            "param_value",
            "param_page_location",
            "param_button_id",
        ):
            assert col in result.columns
        # Sanity: param_content fully populated (all three frames had it)
        assert result["param_content"].null_count() == 0
        # Sanity: param_button_id null everywhere except the click frame
        assert result["param_button_id"].null_count() == 5

    def test_all_empty_frames_returns_empty_with_union_schema(self):
        f1 = pl.DataFrame(schema={"a": pl.Int64})
        f2 = pl.DataFrame(schema={"b": pl.Utf8})
        result = concat_many_with_union([f1, f2])
        assert result.height == 0
        assert set(result.columns) == {"a", "b"}

    def test_no_frames_returns_empty_zero_column(self):
        result = concat_many_with_union([])
        assert result.height == 0
        assert result.columns == []

    def test_single_frame_returns_projected_copy(self):
        f = _df(a=[1, 2], b=[3, 4])
        result = concat_many_with_union([f])
        assert result.height == 2
        assert result.columns == ["a", "b"]

    def test_skips_empty_in_middle(self):
        f1 = _df(a=[1])
        f2 = pl.DataFrame(schema={"a": pl.Int64, "b": pl.Utf8})  # empty
        f3 = _df(a=[2], b=["x"])
        result = concat_many_with_union([f1, f2, f3])
        # Empty frame is skipped → no NULL row introduced
        assert result.height == 2
        assert set(result.columns) == {"a", "b"}

    def test_equivalent_to_repeated_concat_with_union(self):
        # The same frames merged via N-way must yield the same rows as
        # progressive pairwise application (the pattern used inside
        # process_files_with_overlap / process_files_without_overlap).
        frames = [
            _df(a=[1], b=["x"]),
            _df(b=["y"], c=[2.0]),
            _df(a=[3], c=[4.0], d=[True]),
        ]
        n_way = concat_many_with_union(frames)
        pairwise = frames[0]
        for f in frames[1:]:
            pairwise = concat_with_union(pairwise, f)
        # Same column set
        assert set(n_way.columns) == set(pairwise.columns)
        # Same rows when compared column-by-column
        n_rows = set(map(tuple, n_way.select(sorted(n_way.columns)).rows()))
        p_rows = set(map(tuple, pairwise.select(sorted(pairwise.columns)).rows()))
        assert n_rows == p_rows


# ===========================================================================
# 6. Cascading pairwise merge — simulates the in-codebase loop
# ===========================================================================

class TestCascadingPairwiseMerge:
    """The streaming-flush callers (process_files_with_overlap etc.) use a
    loop ``merged = concat_with_union(merged, file_i)``.  This pattern must
    work for arbitrary file orderings and column-subset combinations."""

    def test_five_files_random_column_subsets(self):
        rng = random.Random(42)
        all_columns = ["event_id", "param_a", "param_b", "param_c", "param_d", "param_e"]
        # Build 5 frames each with a random subset of param_* columns + event_id
        frames: List[pl.DataFrame] = []
        for i in range(5):
            subset = ["event_id"] + rng.sample(
                ["param_a", "param_b", "param_c", "param_d", "param_e"],
                k=rng.randint(1, 5),
            )
            # Permute column order so different frames hit different branches
            # of _align_to_schema
            rng.shuffle(subset)
            data = {c: [f"v{i}_{c}"] if c != "event_id" else [i] for c in subset}
            frames.append(pl.DataFrame(data))

        # Fold via the in-code loop pattern
        merged = frames[0]
        for f in frames[1:]:
            merged = concat_with_union(merged, f)

        assert merged.height == 5
        assert set(merged.columns) == set(all_columns)

    def test_loop_order_does_not_change_result(self):
        # Three frames; merge in two different orders.  Should yield equal
        # row multisets.
        from collections import Counter

        a = _df(x=[1], y=["a"])
        b = _df(y=["b"], z=[2.0])
        c = _df(x=[3], z=[4.0])

        forward = concat_with_union(concat_with_union(a, b), c)
        reverse = concat_with_union(concat_with_union(c, b), a)

        cols = sorted(forward.columns)
        # Use Counter (multiset) rather than sorted(): rows contain None
        # values which are unorderable, but they are hashable so a multiset
        # comparison works.
        f_rows = Counter(map(tuple, forward.select(cols).rows()))
        r_rows = Counter(map(tuple, reverse.select(cols).rows()))
        assert f_rows == r_rows


# ===========================================================================
# 7. Production-error regression — pin the exact failure pattern
# ===========================================================================

class TestProductionErrorRegression:
    """Reproduce the exact symptom from the GA4 compaction logs.

    Before the fix this test would raise::

        polars.exceptions.ComputeError:
            schema names differ: got param_chosen_payment_method, expected param_content

    After the fix it must complete cleanly.
    """

    def test_no_schema_names_differ_error(self):
        # Composite schema: many shared columns + one column that exists in
        # 'a' only (mimicking a purchase-event slice that has payment_method
        # while a view-only slice does not).
        a = _df(
            event_id=[1],
            event_name=["purchase"],
            param_chosen_payment_method=["paypal"],
            param_content=["home"],
            param_currency=["EUR"],
            param_value=[42.0],
        )
        b = _df(
            event_id=[2, 3],
            event_name=["page_view", "page_view"],
            param_content=["list", "detail"],
            param_currency=[None, None],
            param_value=[None, None],
        )

        # Must not raise
        result = concat_with_union(a, b)

        assert result.height == 3
        assert "param_chosen_payment_method" in result.columns
        # Rows from 'b' must have null for the column that didn't exist in 'b'
        # (we don't depend on row ordering after concat — filter instead)
        b_rows_with_null_pcm = result.filter(
            pl.col("event_name") == "page_view"
        )["param_chosen_payment_method"].null_count()
        assert b_rows_with_null_pcm == 2
