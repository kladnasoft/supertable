"""
What a read works out of a table's statistics is kept with the statistics.

Two steps of every read look at the table's statistics frame:

* pruning needs each file's bounds for the columns the query constrains
  (``prune_files_by_predicates``);
* projection sizing needs each file's stored bytes for the columns the query
  names (``DataEstimator._projected_bytes_index``).

Both worked it out of the frame again for every read, with polars, on a frame
of a few hundred rows, where one polars call costs milliseconds on its thread
pool: 6 to 9 ms for pruning and 7 to 14 ms for sizing, of every read that has
a predicate and names its columns.

Neither answer depends on the values a query compares with: only on the frame
and on the columns.  So it is derived once for a frame and kept with it
(``processing.stats_derived``).  A frame is never changed in place, and a
write makes a new frame, so what was derived cannot go stale; it is found
through that frame object and nothing else, and it goes when the frame goes.

The tests count how often the frame is worked through, and compare every
answer with the one a fresh frame gives.
"""

from __future__ import annotations

import gc
import threading
import weakref
from datetime import datetime

import polars as pl
import pytest

import supertable.processing as processing
from supertable.data_classes import PredInterval
from supertable.engine.data_estimator import DataEstimator
from supertable.processing import (
    STATS_SCHEMA,
    prune_files_by_predicates,
    stats_derived,
)

FILES = [f"acme/lake/orders/data/part-{i}.parquet" for i in range(6)]


def _stats(files=FILES, shift=0) -> pl.DataFrame:
    """Six files of 100 ids each, a day each, three columns; two row groups."""
    rows = []
    for index, path in enumerate(files):
        for group in (0, 1):
            low = (index + shift) * 100 + group * 50
            rows.append({"file_path": path, "row_group_id": group, "column_name": "id",
                         "min_bigint": low, "max_bigint": low + 49,
                         "compressed_bytes": 400, "stats_available": True})
            rows.append({"file_path": path, "row_group_id": group, "column_name": "ts",
                         "min_timestamp": datetime(2026, 1, 1 + index, 12 * group),
                         "max_timestamp": datetime(2026, 1, 1 + index, 12 * group + 11),
                         "compressed_bytes": 300, "stats_available": True})
            rows.append({"file_path": path, "row_group_id": group, "column_name": "Region",
                         "min_string": "apac", "max_string": "us",
                         "compressed_bytes": 50 + index, "stats_available": True})
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def _equals(value) -> PredInterval:
    return PredInterval("numeric", value, True, value, True)


@pytest.fixture(autouse=True)
def nothing_kept():
    processing.forget_stats_derived()
    yield
    processing.forget_stats_derived()


@pytest.fixture
def worked_through(monkeypatch):
    """How often a frame is worked through for bounds, by columns."""
    calls = []
    real = processing._bounds_index

    def counting(frame, columns):
        calls.append(tuple(columns))
        return real(frame, columns)

    monkeypatch.setattr(processing, "_bounds_index", counting)
    return calls


# --------------------------------------------------------------------------
# stats_derived itself
# --------------------------------------------------------------------------

def test_a_value_is_derived_once_for_a_frame_and_a_key():
    frame, built = _stats(), []

    def build():
        built.append(1)
        return {"made": len(built)}

    first = stats_derived(frame, ("bounds", ("id",)), build)
    assert stats_derived(frame, ("bounds", ("id",)), build) is first
    assert built == [1]
    # another key of the same frame, and the same key of another frame
    assert stats_derived(frame, ("bounds", ("ts",)), build) == {"made": 2}
    assert stats_derived(_stats(), ("bounds", ("id",)), build) == {"made": 3}
    assert stats_derived(frame, ("bounds", ("id",)), build) is first


def test_an_equal_frame_is_another_frame():
    """Found through the object, not through what it holds: two frames with
    the same rows are two versions of the statistics as far as this goes."""
    one, other = _stats(), _stats()
    assert one.equals(other)
    assert stats_derived(one, "k", lambda: "of one") == "of one"
    assert stats_derived(other, "k", lambda: "of the other") == "of the other"
    assert stats_derived(one, "k", lambda: "again") == "of one"


def test_no_frame_derives_every_time():
    built = []
    for _ in range(2):
        stats_derived(None, "k", lambda: built.append(1))
    assert built == [1, 1]
    assert processing._STATS_DERIVED == {}


def test_what_fails_to_derive_is_not_kept():
    frame = _stats()

    def broken():
        raise ValueError("no such column")

    with pytest.raises(ValueError):
        stats_derived(frame, "k", broken)
    assert stats_derived(frame, "k", lambda: "now it works") == "now it works"


def test_a_frame_keeps_its_most_recent_keys_only():
    frame = _stats()
    limit = processing._STATS_DERIVED_MAX_PER_FRAME
    for number in range(limit + 3):
        stats_derived(frame, ("k", number), lambda number=number: number)
    kept = processing._STATS_DERIVED[id(frame)][1]
    assert list(kept) == [("k", number) for number in range(3, limit + 3)]
    # asking for one keeps it from being the next to go
    stats_derived(frame, ("k", 3), lambda: "not built")
    stats_derived(frame, ("k", "new"), lambda: "new")
    assert ("k", 3) in kept and ("k", 4) not in kept


def test_what_was_derived_goes_with_its_frame():
    frame = _stats()
    stats_derived(frame, "k", lambda: {"big": "index"})
    gone = weakref.ref(frame)
    del frame
    gc.collect()
    assert gone() is None                                     # nothing kept the frame alive
    # ... and the next frame to be kept clears the dead one out
    other = _stats()
    stats_derived(other, "k", lambda: 1)
    assert list(processing._STATS_DERIVED) == [id(other)]


def test_a_new_object_at_an_old_address_starts_empty():
    """An id is used again once its object is gone: the entry is checked
    against the frame itself."""
    frame = _stats()
    stats_derived(frame, "k", lambda: "of the first")
    stale = processing._STATS_DERIVED[id(frame)]
    other = _stats(shift=1)
    processing._STATS_DERIVED[id(other)] = stale              # as if the address were reused
    assert stats_derived(other, "k", lambda: "of the second") == "of the second"
    assert stats_derived(other, "k", lambda: "not built") == "of the second"


def test_readers_at_once_get_one_answer_each():
    frame, results, errors = _stats(), [], []
    barrier = threading.Barrier(8)

    def read(number):
        try:
            barrier.wait(timeout=10)
            for _ in range(200):
                results.append(stats_derived(frame, ("k", number % 3), lambda: number % 3))
        except Exception as error:                            # pragma: no cover
            errors.append(error)

    threads = [threading.Thread(target=read, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert errors == [] and len(results) == 1600 and set(results) == {0, 1, 2}


# --------------------------------------------------------------------------
# Pruning
# --------------------------------------------------------------------------

def test_the_bounds_are_read_from_the_frame_once(worked_through):
    frame = _stats()
    for wanted in (150, 420, 77, 150):
        kept = prune_files_by_predicates(FILES, frame, [{"id": _equals(wanted)}])
        assert kept == [FILES[wanted // 100]]
    assert worked_through == [("id",)]                        # four reads, one pass


def test_other_columns_are_read_once_each(worked_through):
    frame = _stats()
    day = PredInterval("timestamp", datetime(2026, 1, 3), True, datetime(2026, 1, 4), False)
    for _ in range(3):
        # a time without a zone is widened by the hours a session's zone can
        # be off by (by fewer once the process knows its zone), so a
        # neighbouring day or two may stay
        kept = prune_files_by_predicates(FILES, frame, [{"ts": day}])
        assert FILES[2] in kept and set(kept) <= set(FILES[1:4])
        assert prune_files_by_predicates(FILES, frame, [{"id": _equals(510)}]) == [FILES[5]]
        assert prune_files_by_predicates(
            FILES, frame, [{"id": _equals(210), "ts": day}]) == [FILES[2]]
    assert sorted(worked_through) == [("id",), ("id", "ts"), ("ts",)]


def test_new_statistics_are_read_anew(worked_through):
    """A write makes a new frame: nothing of the old one answers for it."""
    before, after = _stats(), _stats(shift=1)
    assert prune_files_by_predicates(FILES, before, [{"id": _equals(150)}]) == [FILES[1]]
    assert prune_files_by_predicates(FILES, after, [{"id": _equals(150)}]) == [FILES[0]]
    assert prune_files_by_predicates(FILES, before, [{"id": _equals(150)}]) == [FILES[1]]
    assert worked_through == [("id",), ("id",)]


@pytest.mark.parametrize("occurrences", [
    [{"id": _equals(250)}],
    [{"id": PredInterval("numeric", 120, True, 340, False)}],
    [{"id": _equals(50)}, {"id": _equals(550)}],                         # two places the table is read
    [{"ts": PredInterval("timestamp", datetime(2026, 1, 2, 6), True, None, True)}],
    [{"Region": PredInterval("string", "eu", True, "eu", True)}],
    [{"id": _equals(250), "Region": PredInterval("string", "zz", True, "zz", True)}],
    [{"id": _equals(99_999)}],                                             # no file can match
    [{"nope": _equals(1)}],                                                # a column without statistics
    [{"id": _equals(250)}, {}],                                            # one place has no constraint
], ids=["a key", "a range", "two occurrences", "an open time range", "a string",
        "two columns", "nothing matches", "an unknown column", "an unconstrained occurrence"])
def test_a_kept_index_answers_as_a_fresh_frame_does(occurrences):
    frame = _stats()
    first = prune_files_by_predicates(list(FILES), frame, occurrences)
    again = prune_files_by_predicates(list(FILES), frame, occurrences)
    fresh = prune_files_by_predicates(list(FILES), _stats(), occurrences)
    assert first == again == fresh
    assert set(first) <= set(FILES) and first


def test_a_read_that_matches_no_file_keeps_its_schema_cover(monkeypatch):
    """Pruning that leaves nothing hands back the fewest files that still show
    every column; which columns a file has is kept with the frame as well."""
    frame = _stats()
    grouped = []
    real = pl.DataFrame.group_by

    def counting(self, *args, **kwargs):
        grouped.append(1)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "group_by", counting)
    for _ in range(3):
        assert prune_files_by_predicates(FILES, frame, [{"id": _equals(99_999)}]) == [FILES[0]]
    assert grouped == [1]


def test_the_frame_is_what_it_was():
    frame = _stats()
    copy = frame.clone()
    prune_files_by_predicates(FILES, frame, [{"id": _equals(150)}])
    prune_files_by_predicates(FILES, frame, [{"id": _equals(99_999)}])
    assert frame.equals(copy)


# --------------------------------------------------------------------------
# Projection sizing
# --------------------------------------------------------------------------

def _estimator() -> DataEstimator:
    return DataEstimator.__new__(DataEstimator)


def test_the_stored_bytes_are_summed_once_for_a_frame(monkeypatch):
    summed = []
    real = DataEstimator._sum_projected_bytes

    def counting(frame, columns):
        summed.append(frozenset(columns))
        return real(frame, columns)

    monkeypatch.setattr(DataEstimator, "_sum_projected_bytes", staticmethod(counting))
    frame = _stats()
    first = _estimator()._projected_bytes_index(frame, {"id", "ts"})
    for _ in range(3):                                        # another query, another estimator
        again = _estimator()._projected_bytes_index(frame, {"ts", "id"})
        assert again[0] is first[0] and again[1] is first[1]
    assert first[0] == set(FILES)
    assert first[1] == {path: 2 * (400 + 300) for path in FILES}
    assert summed == [frozenset({"id", "ts"})]

    # other columns, and the column name's case as the table spells it
    other = _estimator()._projected_bytes_index(frame, {"region"})
    assert other[1] == {path: 2 * (50 + index) for index, path in enumerate(FILES)}
    assert summed == [frozenset({"id", "ts"}), frozenset({"region"})]


def test_new_statistics_are_summed_anew():
    before = _stats()
    after = before.with_columns(pl.col("compressed_bytes") * 2)
    estimator = _estimator()
    assert estimator._projected_bytes_index(before, {"id"})[1][FILES[0]] == 800
    assert estimator._projected_bytes_index(after, {"id"})[1][FILES[0]] == 1600
    assert estimator._projected_bytes_index(before, {"id"})[1][FILES[0]] == 800


def test_a_frame_without_the_bytes_keeps_nothing():
    """The cases answered before the frame is looked at stay as they were."""
    estimator = _estimator()
    assert estimator._projected_bytes_index(None, {"id"}) == (set(), {})
    assert estimator._projected_bytes_index(_stats().clear(), {"id"}) == (set(), {})
    assert estimator._projected_bytes_index(
        _stats().drop("compressed_bytes"), {"id"}) == (set(), {})
    assert processing._STATS_DERIVED == {}
