"""
An upsert or delete matches its candidate files several at a time, and finds
what it always found.

``identify_deleted_rowids`` ran one semi join for every candidate file.  A join
of a small frame costs what the thread pool costs to start it (4.6 ms on a
16-thread pool, for a hundred rows as for a hundred thousand), and a table
under steady upserts holds dozens of small files that no key range rules out:
a 100-key upsert took 174 ms with under twenty live files and 741 ms with
eighty or more.

The files are now read one at a time, as before, and joined several at a time:
files that follow each other and have the same types in the key and row-id
columns are matched by one join, up to ``_OVERLAP_JOIN_BATCH_BYTES`` of those
columns.

Should a batch of files fail to be joined together, each of them is joined on
its own, the way it always was: the batch is a faster way to the same pairs,
never a reason for a write to fail.

The function as it was is kept below, word for word, and every case here is
run through both.  The pairs must be the same frame: the same rows in the same
order with the same types.  The counters a write reports must be the same, but
for the number of joins, which is what changed.  What a file raises must be
what it raised.
"""
from __future__ import annotations

import os
import random
from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    ROWID_COL,
    TOMBSTONE_FILE_COL,
    delete_pairs_to_list,
    empty_delete_pairs,
    identify_deleted_rowids,
    resolve_overwrite_writes,
)
from supertable.storage.local_storage import LocalStorage  # noqa: E402
from supertable.utils.profiler import Profiler, get_null_profiler  # noqa: E402

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# The function as it was at cd932c5 (3.5.8), unchanged
# ---------------------------------------------------------------------------

def _identify_as_it_was(
        df: pl.DataFrame,
        overlapping_files: Set[Tuple[str, bool, int]],
        overwrite_columns: List[str],
        file_cache: Optional[Dict[str, pl.DataFrame]] = None,
        profiler: Optional[Profiler] = None,
        read_columns: Optional[List[str]] = None,
):
    polars = pl
    _read_parquet_safe = proc._read_parquet_safe

    p = profiler or get_null_profiler()
    parts: List[polars.DataFrame] = []
    if not overwrite_columns:
        return empty_delete_pairs()

    key_cols = [c for c in overwrite_columns if c in df.columns]
    if key_cols != list(overwrite_columns):
        # Not all predicate columns present in the incoming df — nothing to match.
        return empty_delete_pairs()
    with p.span("delete.incoming_keys"):
        incoming_keys = df.select(overwrite_columns).unique()

    for file, has_overlap, file_size in overlapping_files:
        if not has_overlap:
            continue
        p.add("delete_files_seen", 1)

        if file_cache is not None and file in file_cache:
            existing_df = file_cache.get(file)
        else:
            existing_df = _read_parquet_safe(file, profiler=p, file_size=file_size, columns=read_columns)
        if existing_df is None:
            continue
        if ROWID_COL not in existing_df.columns:
            continue
        if not all(c in existing_df.columns for c in overwrite_columns):
            continue

        with p.span("delete.semi_join"):
            # nulls_equal=True so a NULL in an overwrite key matches an existing
            # NULL (null-safe overwrite/delete), unlike SQL's NULL != NULL.
            matched = existing_df.join(
                incoming_keys, on=overwrite_columns, how="semi", nulls_equal=True
            )
        if matched.height == 0:
            continue

        part = (
            matched.select(polars.col(ROWID_COL).cast(polars.Int64))
            .drop_nulls()
            .with_columns(polars.lit(file, dtype=polars.Utf8).alias(TOMBSTONE_FILE_COL))
            .select(TOMBSTONE_FILE_COL, ROWID_COL)
        )
        if part.height:
            parts.append(part)
            p.add("delete_rows_matched", part.height)

    if not parts:
        return empty_delete_pairs()
    return polars.concat(parts, how="vertical")


# ---------------------------------------------------------------------------
# Running a case through both
# ---------------------------------------------------------------------------

#: Counters that say what was done and must not move.
SAME_COUNTERS = ("delete_files_seen", "delete_rows_matched", "delete.incoming_keys.n",
                 "files_read", "rows_read", "bytes_read", "io.exists.n", "io.read_parquet.n")


def _outcome(fn, *args, **kwargs):
    """(frame, counts, timing names) or (exception type, counts so far, timing names)."""
    prof = Profiler()
    try:
        out = fn(*args, profiler=prof, **kwargs)
    except Exception as exc:  # noqa: BLE001 - the type is the result
        return type(exc), prof.emit_counts(), set(prof.emit_timings())
    return out, prof.emit_counts(), set(prof.emit_timings())


def both(df, files, keys, cache=None, read_columns=None):
    """Run old and new; assert they agree; return (pairs frame, old counts, new counts)."""
    kwargs = {}
    if cache is not None:
        kwargs["file_cache"] = cache
    if read_columns is not None:
        kwargs["read_columns"] = read_columns
    was, was_counts, was_timings = _outcome(_identify_as_it_was, df, files, keys, **kwargs)
    now, now_counts, now_timings = _outcome(identify_deleted_rowids, df, files, keys, **kwargs)

    if isinstance(was, type):
        assert now is was, f"old raised {was.__name__}, new gave {now!r}"
        return was, was_counts, now_counts

    assert not isinstance(now, type), f"new raised {now.__name__}, old gave {was.shape}"
    assert now.schema == was.schema, (now.schema, was.schema)
    assert now.columns == [TOMBSTONE_FILE_COL, ROWID_COL]
    # equals(): same rows, in the same order
    assert now.equals(was), (
        f"pairs differ\nold: {delete_pairs_to_list(was)}\nnew: {delete_pairs_to_list(now)}")

    for name in SAME_COUNTERS:
        assert now_counts.get(name) == was_counts.get(name), (
            name, was_counts.get(name), now_counts.get(name))
    joins_was = was_counts.get("delete.semi_join.n", 0)
    joins_now = now_counts.get("delete.semi_join.n", 0)
    assert joins_now <= joins_was
    assert (joins_now == 0) == (joins_was == 0)
    assert now_timings == was_timings
    # nothing but the number of joins may differ
    assert {k: v for k, v in now_counts.items() if k != "delete.semi_join.n"} == \
           {k: v for k, v in was_counts.items() if k != "delete.semi_join.n"}
    return now, was_counts, now_counts


def frame(ids, rowids, key_dtype=pl.Int64, rowid_dtype=pl.Int64, **more):
    data = {"id": pl.Series(list(ids), dtype=key_dtype),
            ROWID_COL: pl.Series(list(rowids), dtype=rowid_dtype)}
    for name, values in more.items():
        data[name] = values
    return pl.DataFrame(data)


def keys_of(*ids, dtype=pl.Int64):
    return pl.DataFrame({"id": pl.Series(list(ids), dtype=dtype)})


def listed(cache, overlap=True):
    """A LIST of candidates, in the order of *cache*: the order is then known."""
    return [(name, overlap, 100) for name in cache]


@pytest.fixture
def bound(monkeypatch):
    """Set the byte bound of a batch for one test."""
    def _set(value):
        monkeypatch.setattr(proc, "_OVERLAP_JOIN_BATCH_BYTES", value)
    return _set


# ---------------------------------------------------------------------------
# 1. The pairs are the pairs one join a file gave
# ---------------------------------------------------------------------------

class TestThePairsAreWhatOneJoinAFileGave:

    def test_matches_across_files_keep_file_order_and_row_order(self):
        cache = {
            "c.parquet": frame([5, 1, 9, 1], [40, 41, 42, 43]),
            "a.parquet": frame([1, 2, 3], [10, 11, 12]),
            "b.parquet": frame([7, 9, 9], [20, 21, 22]),
        }
        pairs, was, now = both(keys_of(1, 9), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [
            ("c.parquet", 41), ("c.parquet", 42), ("c.parquet", 43),
            ("a.parquet", 10), ("b.parquet", 21), ("b.parquet", 22),
        ]
        assert was["delete.semi_join.n"] == 3 and now["delete.semi_join.n"] == 1

    def test_a_set_of_candidates_gives_the_same_frame(self):
        cache = {f"f{i}.parquet": frame([i, i + 1, 3], [i * 10, i * 10 + 1, i * 10 + 2]) for i in range(9)}
        both(keys_of(3, 4), set(listed(cache)), ["id"], cache)

    def test_one_file(self):
        cache = {"f.parquet": frame([1, 1, 1, 2], [10, 20, 30, 40])}
        pairs, was, now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("f.parquet", 10), ("f.parquet", 20), ("f.parquet", 30)]
        assert was["delete.semi_join.n"] == now["delete.semi_join.n"] == 1

    def test_no_file_matches(self):
        cache = {"a.parquet": frame([1, 2], [10, 20]), "b.parquet": frame([3], [30])}
        pairs, _was, _now = both(keys_of(99), listed(cache), ["id"], cache)
        assert pairs.height == 0 and pairs.schema == empty_delete_pairs().schema

    def test_only_the_middle_file_matches(self):
        cache = {"a.parquet": frame([1], [10]), "b.parquet": frame([2], [20]), "c.parquet": frame([3], [30])}
        pairs, _was, _now = both(keys_of(2), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("b.parquet", 20)]

    def test_a_file_without_rows_between_files_with_rows(self):
        cache = {
            "a.parquet": frame([1, 2], [10, 11]),
            "empty.parquet": frame([], []),
            "b.parquet": frame([2, 1], [20, 21]),
            "empty2.parquet": frame([], []),
        }
        pairs, _was, _now = both(keys_of(1, 2), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [
            ("a.parquet", 10), ("a.parquet", 11), ("b.parquet", 20), ("b.parquet", 21)]

    def test_no_incoming_rows(self):
        cache = {"a.parquet": frame([1, 2], [10, 20]), "b.parquet": frame([1], [30])}
        pairs, _was, _now = both(keys_of(), listed(cache), ["id"], cache)
        assert pairs.height == 0

    def test_the_same_file_listed_twice_is_matched_twice(self):
        cache = {"a.parquet": frame([1, 2], [10, 20])}
        files = [("a.parquet", True, 100), ("a.parquet", True, 100)]
        pairs, _was, _now = both(keys_of(1), files, ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("a.parquet", 10)]

    def test_duplicate_incoming_keys_match_once(self):
        cache = {"a.parquet": frame([1, 2], [10, 20]), "b.parquet": frame([1], [30])}
        both(keys_of(1, 1, 1), listed(cache), ["id"], cache)


# ---------------------------------------------------------------------------
# 2. NULL keys, composite keys, the row id as a key
# ---------------------------------------------------------------------------

class TestKeys:

    def test_null_key_matches_null(self):
        cache = {"a.parquet": frame([1, None, 2], [10, 20, 30]),
                 "b.parquet": frame([None, None], [40, 50])}
        pairs, _was, _now = both(keys_of(None), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 20), ("b.parquet", 40), ("b.parquet", 50)]

    def test_composite_key_with_a_null_component(self):
        def f(a, b, r):
            return pl.DataFrame({"a": a, "b": b, ROWID_COL: r},
                                schema={"a": pl.Int64, "b": pl.Utf8, ROWID_COL: pl.Int64})
        cache = {"x.parquet": f([1, 1, 2], [None, "x", "y"], [10, 20, 30]),
                 "y.parquet": f([1, 2], [None, "y"], [40, 50])}
        incoming = pl.DataFrame({"a": [1, 2], "b": [None, "y"]}, schema={"a": pl.Int64, "b": pl.Utf8})
        pairs, _was, _now = both(incoming, listed(cache), ["a", "b"], cache)
        assert delete_pairs_to_list(pairs) == [
            ("x.parquet", 10), ("x.parquet", 30), ("y.parquet", 40), ("y.parquet", 50)]

    def test_string_keys(self):
        def f(ks, r):
            return pl.DataFrame({"id": ks, ROWID_COL: r}, schema={"id": pl.Utf8, ROWID_COL: pl.Int64})
        cache = {"x.parquet": f(["a", "b", None], [1, 2, 3]), "y.parquet": f(["b", "c"], [4, 5])}
        both(pl.DataFrame({"id": ["b", None]}, schema={"id": pl.Utf8}), listed(cache), ["id"], cache)

    def test_the_row_id_itself_as_the_key(self):
        cache = {"a.parquet": pl.DataFrame({ROWID_COL: [10, 20, 30]}),
                 "b.parquet": pl.DataFrame({ROWID_COL: [40, 50]})}
        incoming = pl.DataFrame({ROWID_COL: [20, 50, 99]})
        pairs, _was, _now = both(incoming, listed(cache), [ROWID_COL], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 20), ("b.parquet", 50)]

    def test_a_key_column_named_like_the_position_column(self):
        name = "__st_row_position__"
        def f(ks, r):
            return pl.DataFrame({name: ks, ROWID_COL: r})
        cache = {"a.parquet": f([1, 2], [10, 20]), "b.parquet": f([2, 3], [30, 40])}
        pairs, _was, _now = both(pl.DataFrame({name: [2]}), listed(cache), [name], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 20), ("b.parquet", 30)]

    def test_an_unused_name_is_found(self):
        assert proc._unused_column_name(["a", "b"]) == "__st_row_position__"
        assert proc._unused_column_name(["__st_row_position__", "__st_row_position___"]) == "__st_row_position____"

    def test_no_overwrite_columns(self):
        cache = {"a.parquet": frame([1], [10])}
        pairs, _was, _now = both(keys_of(1), listed(cache), [], cache)
        assert pairs.height == 0

    def test_a_key_column_the_incoming_frame_lacks(self):
        cache = {"a.parquet": frame([1], [10])}
        pairs, _was, _now = both(pl.DataFrame({"other": [1]}), listed(cache), ["id"], cache)
        assert pairs.height == 0


# ---------------------------------------------------------------------------
# 3. Files that are skipped, and row ids that are not there
# ---------------------------------------------------------------------------

class TestWhatIsLeftOut:

    def test_a_file_without_row_ids_between_two_that_have_them(self):
        cache = {"a.parquet": frame([1], [10]),
                 "legacy.parquet": pl.DataFrame({"id": [1, 1]}),
                 "b.parquet": frame([1], [30])}
        pairs, was, now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("b.parquet", 30)]
        assert was["delete_files_seen"] == now["delete_files_seen"] == 3

    def test_a_file_without_the_key_column(self):
        cache = {"a.parquet": frame([1], [10]),
                 "other.parquet": pl.DataFrame({"x": [1], ROWID_COL: [20]}),
                 "b.parquet": frame([1], [30])}
        pairs, _was, _now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("b.parquet", 30)]

    def test_candidates_that_do_not_overlap_are_not_read(self):
        cache = {"a.parquet": frame([1], [10]), "b.parquet": frame([1], [20])}
        files = [("a.parquet", False, 100), ("b.parquet", True, 100)]
        pairs, was, now = both(keys_of(1), files, ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("b.parquet", 20)]
        assert was["delete_files_seen"] == now["delete_files_seen"] == 1

    def test_a_file_that_is_gone(self):
        cache = {"a.parquet": frame([1], [10]), "b.parquet": frame([1], [30])}
        files = [("a.parquet", True, 100), ("gone.parquet", True, 100), ("b.parquet", True, 100)]
        with patch.object(proc, "_read_parquet_safe", return_value=None) as read:
            pairs, _was, _now = both(keys_of(1), files, ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("b.parquet", 30)]
        assert read.call_count == 2          # once by the old function, once by the new

    def test_rows_without_a_row_id_are_left_out(self):
        cache = {"a.parquet": frame([1, 1, 1], [10, None, 30]),
                 "b.parquet": frame([1, 2], [None, 50]),
                 "c.parquet": frame([1], [None])}
        pairs, _was, _now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("a.parquet", 30)]

    def test_every_matched_row_lacks_a_row_id(self):
        cache = {"a.parquet": frame([1], [None]), "b.parquet": frame([1, 1], [None, None])}
        pairs, was, now = both(keys_of(1), listed(cache), ["id"], cache)
        assert pairs.height == 0
        assert "delete_rows_matched" not in was and "delete_rows_matched" not in now

    @pytest.mark.parametrize("dtype", [pl.Int32, pl.UInt32, pl.Int16, pl.UInt8])
    def test_row_ids_of_another_integer_type_come_out_as_int64(self, dtype):
        cache = {"a.parquet": frame([1, 2], [10, 20], rowid_dtype=dtype),
                 "b.parquet": frame([1], [30], rowid_dtype=dtype)}
        pairs, _was, _now = both(keys_of(1), listed(cache), ["id"], cache)
        assert pairs.schema[ROWID_COL] == pl.Int64
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("b.parquet", 30)]

    def test_a_row_id_that_does_not_fit_raises_as_it_did(self):
        big = pl.DataFrame({"id": [1], ROWID_COL: pl.Series([2 ** 63 + 5], dtype=pl.UInt64)})
        cache = {"a.parquet": frame([1], [10]), "big.parquet": big}
        was, _wc, _nc = both(keys_of(1), listed(cache), ["id"], cache)
        assert isinstance(was, type) and issubclass(was, Exception)

    def test_a_row_id_that_does_not_fit_but_is_not_matched(self):
        big = pl.DataFrame({"id": [7], ROWID_COL: pl.Series([2 ** 63 + 5], dtype=pl.UInt64)})
        cache = {"a.parquet": frame([1], [10]), "big.parquet": big}
        pairs, _was, _now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10)]


# ---------------------------------------------------------------------------
# 4. Files whose columns have other types are not put together
# ---------------------------------------------------------------------------

class TestTypes:

    def test_key_types_that_alternate(self):
        cache = {}
        for i, dtype in enumerate([pl.Int64, pl.Int32, pl.Int64, pl.Int64, pl.Int32, pl.Int32]):
            cache[f"f{i}.parquet"] = frame([1, i, 2], [i * 10, i * 10 + 1, i * 10 + 2], key_dtype=dtype)
        pairs, was, now = both(keys_of(1, 2), listed(cache), ["id"], cache)
        assert was["delete.semi_join.n"] == 6
        assert now["delete.semi_join.n"] == 4     # runs of one type: 1, 1, 2, 2 files
        assert [f for f, _ in delete_pairs_to_list(pairs)][0] == "f0.parquet"

    def test_row_id_types_that_differ(self):
        cache = {"a.parquet": frame([1], [10], rowid_dtype=pl.Int64),
                 "b.parquet": frame([1], [20], rowid_dtype=pl.Int32),
                 "c.parquet": frame([1], [30], rowid_dtype=pl.Int64)}
        pairs, _was, now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [("a.parquet", 10), ("b.parquet", 20), ("c.parquet", 30)]
        assert now["delete.semi_join.n"] == 3

    def test_a_key_type_the_join_refuses_raises_as_it_did(self):
        text = pl.DataFrame({"id": ["1"], ROWID_COL: [20]})
        cache = {"a.parquet": frame([1], [10]), "text.parquet": text, "b.parquet": frame([1], [30])}
        was, _wc, _nc = both(keys_of(1), listed(cache), ["id"], cache)
        assert isinstance(was, type) and issubclass(was, Exception)

    def test_a_file_of_another_type_without_rows_still_meets_its_join(self):
        text = pl.DataFrame({"id": [], ROWID_COL: []}, schema={"id": pl.Utf8, ROWID_COL: pl.Int64})
        cache = {"a.parquet": frame([1], [10]), "text.parquet": text}
        both(keys_of(1), listed(cache), ["id"], cache)       # agree, whatever polars does with it

    def test_a_frame_that_is_not_a_polars_frame_is_asked_what_it_was_asked(self):
        class Stand(pl.DataFrame):
            pass
        cache = {"a.parquet": frame([1], [10]),
                 "stand.parquet": Stand(frame([1, 2, 1], [20, 21, 22])),
                 "b.parquet": frame([1], [30]),
                 "c.parquet": frame([1], [40])}
        pairs, _was, now = both(keys_of(1), listed(cache), ["id"], cache)
        assert delete_pairs_to_list(pairs) == [
            ("a.parquet", 10), ("stand.parquet", 20), ("stand.parquet", 22),
            ("b.parquet", 30), ("c.parquet", 40)]
        assert now["delete.semi_join.n"] == 3     # a, the stand-in, then b and c together


# ---------------------------------------------------------------------------
# 5. The bound
# ---------------------------------------------------------------------------

class TestTheBound:

    def _cache(self, n=40, rows=50):
        return {f"f{i:02d}.parquet": frame([(i * 7 + j) % 97 for j in range(rows)],
                                           [i * 1000 + j for j in range(rows)])
                for i in range(n)}

    def test_a_hundred_small_files_are_one_join(self):
        cache = self._cache(n=100, rows=100)
        _pairs, was, now = both(keys_of(*range(0, 97, 5)), listed(cache), ["id"], cache)
        assert was["delete.semi_join.n"] == 100
        assert now["delete.semi_join.n"] == 1

    def test_a_bound_of_one_byte_is_a_join_a_file(self, bound):
        bound(1)
        cache = self._cache()
        _pairs, was, now = both(keys_of(3, 5, 8), listed(cache), ["id"], cache)
        assert now["delete.semi_join.n"] == was["delete.semi_join.n"] == 40

    @pytest.mark.parametrize("limit", [1, 500, 801, 1600, 1601, 4000, 10_000, 8 * MIB])
    def test_the_pairs_do_not_depend_on_the_bound(self, bound, limit):
        bound(limit)
        cache = self._cache()
        both(keys_of(3, 5, 8, 11, 96), listed(cache), ["id"], cache)

    def test_what_is_put_together_fits_the_bound(self, bound):
        limit = 2500
        bound(limit)
        cache = self._cache()                      # 50 rows x 2 x 8 bytes = 800 bytes a file
        seen = []
        real = proc._delete_pairs_of_batch

        def spy(frames, files, *rest):
            seen.append([int(f.estimated_size()) for f in frames])
            return real(frames, files, *rest)

        with patch.object(proc, "_delete_pairs_of_batch", side_effect=spy):
            identify_deleted_rowids(keys_of(3), listed(cache), ["id"], file_cache=cache)
        assert sum(len(sizes) for sizes in seen) == 40
        assert all(sum(sizes) <= limit for sizes in seen)
        assert max(len(sizes) for sizes in seen) == 3

    def test_files_that_fill_the_bound_exactly_go_together(self, bound):
        bound(1600)                                # two files of 800 bytes, to the byte
        cache = self._cache()
        _pairs, _was, now = both(keys_of(3, 5, 8), listed(cache), ["id"], cache)
        assert now["delete.semi_join.n"] == 20
        bound(1599)                                # one byte short of two files
        _pairs, _was, now = both(keys_of(3, 5, 8), listed(cache), ["id"], cache)
        assert now["delete.semi_join.n"] == 40

    def test_a_file_over_the_bound_is_joined_on_its_own(self, bound):
        bound(1000)
        large = frame(list(range(500)), list(range(5000, 5500)))        # 8,000 bytes
        cache = {"a.parquet": frame([1], [10]), "large.parquet": large,
                 "b.parquet": frame([1], [30]), "c.parquet": frame([2], [40])}
        seen = []
        real = proc._delete_pairs_of_batch

        def spy(frames, files, *rest):
            seen.append(list(files))
            return real(frames, files, *rest)

        with patch.object(proc, "_delete_pairs_of_batch", side_effect=spy):
            pairs = identify_deleted_rowids(keys_of(1, 2), listed(cache), ["id"], file_cache=cache)
        assert seen == [["a.parquet"], ["large.parquet"], ["b.parquet", "c.parquet"]]
        assert pairs.equals(_identify_as_it_was(keys_of(1, 2), listed(cache), ["id"], file_cache=cache))

    def test_only_the_key_and_row_id_columns_wait(self):
        wide = {f"f{i}.parquet": frame([i, 1], [i * 10, i * 10 + 1],
                                       payload=["x" * 1000, "y" * 1000], more=[1.5, 2.5])
                for i in range(5)}
        seen = []
        real = proc._delete_pairs_of_batch

        def spy(frames, files, *rest):
            seen.extend(f.columns for f in frames)
            return real(frames, files, *rest)

        with patch.object(proc, "_delete_pairs_of_batch", side_effect=spy):
            both(keys_of(1), listed(wide), ["id"], wide)
        assert seen and all(cols == ["id", ROWID_COL] for cols in seen)

    def test_the_default_bound(self):
        assert proc._OVERLAP_JOIN_BATCH_BYTES == 8 * MIB


# ---------------------------------------------------------------------------
# 6. When several files cannot be joined together, each is joined on its own
# ---------------------------------------------------------------------------

def _refusing_several(calls=None):
    """_delete_pairs_of_batch that fails for more than one file."""
    real = proc._delete_pairs_of_batch

    def refuse(frames, files, *rest):
        if calls is not None:
            calls.append(list(files))
        if len(frames) > 1:
            raise RuntimeError("these files cannot be put end to end")
        return real(frames, files, *rest)
    return refuse


class TestWhenSeveralFilesCannotBeJoinedTogether:

    def _cache(self):
        return {"a.parquet": frame([1, 2], [10, 11]), "b.parquet": frame([2, 3], [20, 21]),
                "c.parquet": frame([9], [30]), "d.parquet": frame([1, 1], [40, 41])}

    def test_each_is_joined_on_its_own_and_the_pairs_are_the_same(self):
        cache = self._cache()
        prof = Profiler()
        with patch.object(proc, "_delete_pairs_of_batch", side_effect=_refusing_several()):
            now = identify_deleted_rowids(keys_of(1, 2), listed(cache), ["id"],
                                          file_cache=cache, profiler=prof)
        was = _identify_as_it_was(keys_of(1, 2), listed(cache), ["id"], file_cache=cache)
        assert now.equals(was)
        counts = prof.emit_counts()
        assert counts["delete_join_one_by_one"] == 1
        assert counts["delete.semi_join.n"] == 4          # a join a file, as it was
        assert counts["delete_rows_matched"] == was.height == 5
        assert counts["delete_files_seen"] == 4

    def test_nothing_is_counted_when_the_files_join(self):
        cache = self._cache()
        prof = Profiler()
        identify_deleted_rowids(keys_of(1, 2), listed(cache), ["id"], file_cache=cache, profiler=prof)
        assert "delete_join_one_by_one" not in prof.emit_counts()

    def test_one_file_that_cannot_be_joined_is_not_tried_again(self):
        cache = {"a.parquet": frame([1], [10])}

        def refuse(*_a, **_k):
            raise RuntimeError("no")

        with patch.object(proc, "_delete_pairs_of_batch", side_effect=refuse), \
                patch.object(proc, "_delete_pairs_of_one_file") as one_by_one:
            with pytest.raises(RuntimeError):
                identify_deleted_rowids(keys_of(1), listed(cache), ["id"], file_cache=cache)
        assert one_by_one.call_count == 0

    def test_what_a_file_raises_on_its_own_is_what_is_raised(self):
        """Two files of a key type the join refuses: together, then one by one, then the error."""
        def text(ks, r):
            return pl.DataFrame({"id": ks, ROWID_COL: r}, schema={"id": pl.Utf8, ROWID_COL: pl.Int64})
        cache = {"t1.parquet": text(["1"], [10]), "t2.parquet": text(["2"], [20])}
        was, _wc, _nc = both(keys_of(1), listed(cache), ["id"], cache)
        assert isinstance(was, type) and issubclass(was, Exception)

    def test_the_files_after_a_refused_batch_are_still_matched_in_order(self, bound):
        bound(40)                                  # a.parquet + b.parquet fill it; c and d follow
        cache = self._cache()
        calls: List[List[str]] = []
        with patch.object(proc, "_delete_pairs_of_batch", side_effect=_refusing_several(calls)):
            now = identify_deleted_rowids(keys_of(1, 2, 9), listed(cache), ["id"], file_cache=cache)
        assert now.equals(_identify_as_it_was(keys_of(1, 2, 9), listed(cache), ["id"], file_cache=cache))
        assert [f for f, _ in delete_pairs_to_list(now)] == [
            "a.parquet", "a.parquet", "b.parquet", "c.parquet", "d.parquet", "d.parquet"]
        assert len(calls) >= 2

    @pytest.mark.parametrize("seed", range(60))
    def test_random_cases_agree_when_no_two_files_join(self, seed, bound):
        incoming, files, key_cols, cache, limit = _random_case(seed)
        bound(limit)
        was = _outcome(_identify_as_it_was, incoming, files, key_cols, file_cache=cache)[0]
        with patch.object(proc, "_delete_pairs_of_batch", side_effect=_refusing_several()):
            now = _outcome(identify_deleted_rowids, incoming, files, key_cols, file_cache=cache)[0]
        if isinstance(was, type):
            assert now is was
        else:
            assert not isinstance(now, type) and now.equals(was)


# ---------------------------------------------------------------------------
# 7. Random cases
# ---------------------------------------------------------------------------

def _random_case(seed: int):
    rng = random.Random(seed)
    composite = rng.random() < 0.3
    key_cols = ["id", "k2"] if composite else ["id"]
    domain = [None, 0, 1, 2, 3, 4, 5, 6, 7]
    n_files = rng.randint(0, 14)
    cache: Dict[str, pl.DataFrame] = {}
    files = []
    rowid = 0
    for i in range(n_files):
        name = f"f{i:02d}.parquet"
        rows = rng.choice([0, 1, 2, 5, 17, 60])
        kind = rng.random()
        key_dtype = rng.choice([pl.Int64, pl.Int64, pl.Int64, pl.Int32])
        rowid_dtype = rng.choice([pl.Int64, pl.Int64, pl.Int64, pl.Int32])
        data = {"id": pl.Series([rng.choice(domain) for _ in range(rows)], dtype=key_dtype)}
        if composite:
            data["k2"] = pl.Series([rng.choice([None, "a", "b"]) for _ in range(rows)], dtype=pl.Utf8)
        if kind < 0.08:
            pass                                             # no row id: legacy file
        else:
            ids = [None if rng.random() < 0.05 else rowid + j for j in range(rows)]
            data[ROWID_COL] = pl.Series(ids, dtype=rowid_dtype)
            rowid += rows
        if 0.08 <= kind < 0.14:
            del data["id"]                                   # no key column
        if rng.random() < 0.3:
            data["payload"] = pl.Series([f"p{j}" for j in range(rows)], dtype=pl.Utf8)
        cache[name] = pl.DataFrame(data)
        files.append((name, rng.random() < 0.9, rng.randint(1, 10_000)))
        if rng.random() < 0.05:
            files.append(files[-1])                          # listed twice
    n_keys = rng.choice([0, 1, 2, 4, 9])
    incoming = {"id": pl.Series([rng.choice(domain) for _ in range(n_keys)], dtype=pl.Int64)}
    if composite:
        incoming["k2"] = pl.Series([rng.choice([None, "a", "b"]) for _ in range(n_keys)], dtype=pl.Utf8)
    limit = rng.choice([1, 64, 300, 1000, 5000, 8 * MIB])
    as_set = rng.random() < 0.3
    return pl.DataFrame(incoming), (set(files) if as_set else files), key_cols, cache, limit


@pytest.mark.parametrize("seed", range(300))
def test_random_cases_agree(seed, bound):
    incoming, files, key_cols, cache, limit = _random_case(seed)
    bound(limit)
    both(incoming, files, key_cols, cache)


# ---------------------------------------------------------------------------
# 8. Through real files, as a write reads them
# ---------------------------------------------------------------------------

@pytest.fixture
def local_storage():
    with patch("supertable.processing._get_storage", return_value=LocalStorage()):
        yield


def _write(directory, name, df):
    path = str(directory / name)
    pq.write_table(df.to_arrow(), path)
    return (path, True, (directory / name).stat().st_size)


class TestThroughRealFiles:

    def _table(self, tmp_path, n=12):
        files = []
        for i in range(n):
            df = pl.DataFrame({
                "id": [i * 3 + j for j in range(6)],
                "rev": [i for _ in range(6)],
                "payload": [f"row-{i}-{j}" for j in range(6)],
                ROWID_COL: [i * 100 + j for j in range(6)],
            })
            files.append(_write(tmp_path, f"data-{i:02d}.parquet", df))
        return files

    def test_files_are_read_once_each_and_only_their_key_columns(self, tmp_path, local_storage):
        files = self._table(tmp_path)
        incoming = pl.DataFrame({"id": [0, 4, 7, 20, 33, 999]})
        pairs, was, now = both(incoming, files, ["id"], read_columns=["id", ROWID_COL])
        assert pairs.height > 0
        assert was["files_read"] == now["files_read"] == 12
        assert was["delete.semi_join.n"] == 12 and now["delete.semi_join.n"] == 1

    def test_whole_files_read_without_a_projection(self, tmp_path, local_storage):
        files = self._table(tmp_path, n=5)
        both(pl.DataFrame({"id": [1, 2, 3, 4, 5]}), files, ["id"])

    def test_an_upsert_resolves_to_the_pairs_it_did(self, tmp_path, local_storage):
        """resolve_overwrite_writes with the probe off (the default) is the caller."""
        files = set(self._table(tmp_path))
        incoming = pl.DataFrame({"id": [0, 4, 7, 20, 33, 999], "rev": [9] * 6, "payload": ["n"] * 6})
        prof = Profiler()
        filtered, pairs = resolve_overwrite_writes(
            incoming_df=incoming, overlapping_files=files, overwrite_columns=["id"], profiler=prof)
        was = _identify_as_it_was(incoming, files, ["id"], read_columns=["id", ROWID_COL])
        assert filtered.equals(incoming)
        assert pairs.equals(was)
        assert prof.emit_counts()["overwrite_resolve_fallback"] == 1
        assert prof.emit_counts()["delete.semi_join.n"] == 1

    def test_a_newer_than_upsert_resolves_to_the_pairs_it_did(self, tmp_path, local_storage):
        """With a watermark the files are read once, kept, and matched from what was kept."""
        files = set(self._table(tmp_path))
        incoming = pl.DataFrame({"id": [0, 4, 7, 20, 33, 999], "rev": [0, 5, 1, 99, 99, 1],
                                 "payload": ["n"] * 6})
        prof = Profiler()
        filtered, pairs = resolve_overwrite_writes(
            incoming_df=incoming, overlapping_files=files, overwrite_columns=["id"],
            newer_than_col="rev", profiler=prof)
        cache: Dict[str, pl.DataFrame] = {}
        expected_filtered = proc.filter_stale_incoming_rows(
            incoming_df=incoming, overlapping_files=files, overwrite_columns=["id"],
            newer_than_col="rev", file_cache=cache, read_columns=["id", "rev", ROWID_COL])
        was = _identify_as_it_was(expected_filtered, files, ["id"], file_cache=cache,
                                  read_columns=["id", "rev", ROWID_COL])
        assert filtered.equals(expected_filtered)
        assert pairs.equals(was)
        assert prof.emit_counts()["files_read"] == 12       # not read a second time
