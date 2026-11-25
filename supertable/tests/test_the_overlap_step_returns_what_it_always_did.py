"""
The overlap step of a write returns, and counts, what it always did.

``find_overlapping_files`` runs on every write over every file of the table.
For an append it resolved the table's limits again for each file, and
``prune_not_overlapping_files_by_threshold`` and the counters after it walked
the candidates seven times between them: 4 ms of a write to a table of 3,000
files, for a result an append does not use.

The two functions now do the same work in fewer passes, and a write without
overwrite columns asks for the counters alone (``want_candidates=False``): the
set of every small file is then not built at all.  The two functions as they
were are kept below, word for word, and every case here is run through both:
the set returned, the counters and the names of the timings a write reports,
and what a malformed input raises, must be the same.
"""
from __future__ import annotations

import os
import random
import time
from typing import Dict, List, Optional, Set, Tuple

import polars as pl
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    _resolve_limits,
    find_overlapping_files,
    prune_not_overlapping_files_by_threshold,
)
from supertable.utils.profiler import Profiler, get_null_profiler  # noqa: E402

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# The two functions as they were at a6f88ae, unchanged
# ---------------------------------------------------------------------------

def _prune_as_it_was(overlapping_files, table_config=None):
    max_mem, max_files = _resolve_limits(table_config)

    total_size = sum(item[2] for item in overlapping_files)
    total_false = len([item for item in overlapping_files if item[1] is False])

    result: Set[Tuple[str, bool, int]] = set([item for item in overlapping_files if item[1] is True])

    if total_size > max_mem or total_false >= max_files:
        for item in overlapping_files:
            if item[1] is False:
                result.add(item)

    return result


def _find_as_it_was(last_simple_table, df, overwrite_columns, locking=None,
                    table_config=None, profiler=None):
    p = profiler or get_null_profiler()
    resources = last_simple_table.get("resources", {}) or {}
    overlapping_files: Set[Tuple[str, bool, int]] = set()
    p.add("resources_total", len(resources))

    if overwrite_columns:
        t0 = time.perf_counter()
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            overlapping_files.add((file, True, file_size))
        p.mark("overlap.scan_resources", t0)

    else:
        for resource in resources:
            file = resource["file"]
            file_size = int(resource.get("file_size") or 0)
            _max_mem, _ = _resolve_limits(table_config)
            if file_size < _max_mem:
                overlapping_files.add((file, False, file_size))

    with p.span("overlap.prune"):
        overlapping_files = _prune_as_it_was(overlapping_files, table_config=table_config)

    p.add("overlap_files_true", sum(1 for _, ov, _ in overlapping_files if ov))
    p.add("overlap_files_false", sum(1 for _, ov, _ in overlapping_files if not ov))
    p.add("overlap_files_total_bytes", sum(sz for _, _, sz in overlapping_files))

    return overlapping_files


FRAME = pl.DataFrame({"id": [1]})


def _both(snapshot, overwrite_columns, table_config=None):
    """Run a case through the function as it was and as it is; return what
    each returned and reported, or what each raised."""
    outcomes = []
    for function in (_find_as_it_was, find_overlapping_files):
        profiler = Profiler()
        try:
            returned = function(snapshot, FRAME, overwrite_columns, locking=None,
                                table_config=table_config, profiler=profiler)
            outcome = ("returned", returned, dict(profiler.counts), sorted(profiler.timings))
        except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
            outcome = ("raised", type(error), str(error), dict(profiler.counts))
        outcomes.append(outcome)
    return outcomes


def _resources(rng: random.Random, count: int, *, duplicates: bool = False) -> List[Dict]:
    resources = []
    for i in range(count):
        kind = rng.random()
        if kind < 0.6:
            size = rng.randint(0, 200_000)                 # small
        elif kind < 0.8:
            size = rng.randint(15 * MIB, 17 * MIB)         # around the limit
        else:
            size = rng.randint(17 * MIB, 200 * MIB)        # large
        resource = {"file": f"data/f{i}.parquet", "file_size": size, "rows": rng.randint(1, 10 ** 6)}
        shape = rng.random()
        if shape < 0.05:
            del resource["file_size"]
        elif shape < 0.10:
            resource["file_size"] = None
        elif shape < 0.15:
            resource["file_size"] = str(size)
        resources.append(resource)
    if duplicates and resources:
        resources.extend(rng.choice(resources) for _ in range(rng.randint(1, 5)))
        rng.shuffle(resources)
    return resources


CONFIGS: List[Optional[dict]] = [
    None, {},
    {"max_overlapping_files": 2}, {"max_overlapping_files": 3}, {"max_overlapping_files": 10 ** 9},
    {"max_memory_chunk_size": 1}, {"max_memory_chunk_size": 64 * 1024},
    {"max_memory_chunk_size": 1 << 50, "max_overlapping_files": 1 << 40},
    {"max_memory_chunk_size": 100_000, "max_overlapping_files": 50, "max_tombstone_rows": 7},
]


# ---------------------------------------------------------------------------
# find_overlapping_files
# ---------------------------------------------------------------------------

class TestTheStepOfAWrite:

    @pytest.mark.parametrize("overwrite_columns", [[], ["id"]])
    def test_random_tables_under_every_configuration(self, overwrite_columns):
        rng = random.Random(len(overwrite_columns))
        compared = gate_open = 0
        for round_ in range(300):
            count = rng.choice([0, 1, 2, 3, 50, 99, 100, 101, 250])
            snapshot = {"resources": _resources(rng, count, duplicates=round_ % 7 == 0)}
            for config in CONFIGS:
                was, now = _both(snapshot, overwrite_columns, config)
                assert now == was
                assert was[0] == "returned"
                compared += 1
                gate_open += bool(was[1]) and not overwrite_columns
        assert compared == 300 * len(CONFIGS)
        if not overwrite_columns:
            assert gate_open > 100, "appends whose small files were returned must be among them"

    def test_the_shapes_a_snapshot_can_have(self):
        for snapshot in ({}, {"resources": None}, {"resources": []}, {"resources": {}},
                         {"resources": [{"file": "a", "file_size": 5}]},
                         {"resources": [{"file": "a"}]},
                         {"resources": [{"file": None, "file_size": 5}]},
                         {"resources": ({"file": "a", "file_size": 5},)}):
            for overwrite_columns in ([], ["id"], None, ""):
                was, now = _both(snapshot, overwrite_columns)
                assert now == was
                assert was[0] == "returned"

    def test_the_counters_of_an_append_over_many_small_files(self):
        """The case the cost was measured on: the result is unused, the
        counters go into the write's monitoring record."""
        snapshot = {"resources": [{"file": f"f{i}", "file_size": 4000, "rows": 100}
                                  for i in range(3000)]}
        was, now = _both(snapshot, [], {"max_overlapping_files": 1 << 40,
                                        "max_memory_chunk_size": 1 << 50})
        assert now == was
        assert was[1] == set() and was[2] == {"resources_total": 3000, "overlap.prune.n": 1}
        was, now = _both(snapshot, [])
        assert now == was
        assert len(was[1]) == 3000
        assert was[2] == {"resources_total": 3000, "overlap.prune.n": 1,
                          "overlap_files_false": 3000, "overlap_files_total_bytes": 12_000_000}

    def test_the_counters_of_an_upsert(self):
        snapshot = {"resources": [{"file": f"f{i}", "file_size": 1000 + i} for i in range(40)]}
        was, now = _both(snapshot, ["id"])
        assert now == was
        assert was[2] == {"resources_total": 40, "overlap.scan_resources.n": 1,
                          "overlap.prune.n": 1, "overlap_files_true": 40,
                          "overlap_files_total_bytes": sum(1000 + i for i in range(40))}
        assert was[3] == ["overlap.prune", "overlap.scan_resources"]

    @pytest.mark.parametrize("overwrite_columns", [[], ["id"]])
    def test_what_a_malformed_resource_raises(self, overwrite_columns):
        for resources in (
            [{"file_size": 5}],                                    # no file
            [{"file": "a", "file_size": 5}, {"file_size": 5}],     # the second has none
            [{"file": "a", "file_size": "five"}],                  # a size that is not one
            [{"file": "a", "file_size": [1]}],
            ["not a resource"],
            [None],
        ):
            was, now = _both({"resources": resources}, overwrite_columns)
            assert now == was
            assert was[0] == "raised"

    @pytest.mark.parametrize("overwrite_columns", [[], ["id"]])
    def test_what_a_malformed_configuration_raises(self, overwrite_columns):
        bad = {"max_memory_chunk_size": "sixteen"}
        for resources in ([], [{"file": "a", "file_size": 5}],
                          [{"file_size": 5}],                      # and a malformed resource first
                          [{"file": "a", "file_size": "five"}]):
            was, now = _both({"resources": resources}, overwrite_columns, bad)
            assert now == was
            assert was[0] == "raised"

    def test_the_limits_are_resolved_once_for_an_append_not_once_a_file(self, monkeypatch):
        calls = []
        real = proc._resolve_limits

        def counting(table_config):
            calls.append(table_config)
            return real(table_config)

        monkeypatch.setattr(proc, "_resolve_limits", counting)
        snapshot = {"resources": [{"file": f"f{i}", "file_size": 4000} for i in range(500)]}

        find_overlapping_files(snapshot, FRAME, [], table_config={"max_overlapping_files": 7})

        assert len(calls) == 2, "once for the files, once for the gate"
        assert calls == [{"max_overlapping_files": 7}] * 2

    def test_without_a_profiler_it_returns_the_same_set(self):
        rng = random.Random(5)
        snapshot = {"resources": _resources(rng, 150)}
        for overwrite_columns in ([], ["id"]):
            assert find_overlapping_files(snapshot, FRAME, overwrite_columns) == _find_as_it_was(
                snapshot, FRAME, overwrite_columns)


# ---------------------------------------------------------------------------
# find_overlapping_files(want_candidates=False): what an append asks for
# ---------------------------------------------------------------------------

def _counters_only(snapshot, overwrite_columns, table_config=None):
    """The case as the function was, and as an append now calls it."""
    outcomes = []
    for function, extra in ((_find_as_it_was, {}), (find_overlapping_files, {"want_candidates": False})):
        profiler = Profiler()
        try:
            returned = function(snapshot, FRAME, overwrite_columns, locking=None,
                                table_config=table_config, profiler=profiler, **extra)
            outcome = ("returned", returned, dict(profiler.counts), sorted(profiler.timings))
        except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
            outcome = ("raised", type(error), str(error), dict(profiler.counts))
        outcomes.append(outcome)
    return outcomes


class TestAnAppendThatWantsTheCountersOnly:

    def test_it_records_what_the_step_always_recorded(self):
        rng = random.Random(77)
        recorded_files = recorded_nothing = with_duplicates = 0
        for round_ in range(400):
            count = rng.choice([0, 1, 2, 3, 50, 99, 100, 101, 250])
            duplicates = round_ % 5 == 0
            snapshot = {"resources": _resources(rng, count, duplicates=duplicates)}
            for config in CONFIGS:
                was, now = _counters_only(snapshot, [], config)
                assert was[0] == now[0] == "returned"
                assert now[2] == was[2], "the counters"
                assert now[3] == was[3], "the timings that are reported"
                assert now[1] == set(), "and no set is handed back"
                recorded_files += "overlap_files_false" in was[2]
                recorded_nothing += "overlap_files_false" not in was[2]
                with_duplicates += duplicates and bool(snapshot["resources"])
        assert recorded_files > 300 and recorded_nothing > 300 and with_duplicates > 100

    def test_a_file_listed_twice_is_counted_once_as_the_set_counted_it(self):
        twice = {"file": "f1", "file_size": 4000}
        snapshot = {"resources": [twice, {"file": "f2", "file_size": 4000}, dict(twice)]}
        config = {"max_overlapping_files": 2}

        was, now = _counters_only(snapshot, [], config)

        assert now[2] == was[2] == {"resources_total": 3, "overlap.prune.n": 1,
                                    "overlap_files_false": 2, "overlap_files_total_bytes": 8000}

    def test_the_same_file_at_two_sizes_is_two_candidates_as_before(self):
        snapshot = {"resources": [{"file": "f1", "file_size": 4000}, {"file": "f1", "file_size": 5000}]}

        was, now = _counters_only(snapshot, [], {"max_overlapping_files": 2})

        assert now[2] == was[2]
        assert was[2]["overlap_files_false"] == 2 and was[2]["overlap_files_total_bytes"] == 9000

    def test_with_overwrite_columns_the_argument_changes_nothing(self):
        rng = random.Random(3)
        for _ in range(50):
            snapshot = {"resources": _resources(rng, rng.choice([0, 3, 120]))}
            was, now = _counters_only(snapshot, ["id"], rng.choice(CONFIGS))
            assert now == was
            assert len(was[1]) == len({(r["file"], int(r.get("file_size") or 0))
                                       for r in snapshot["resources"]})

    @pytest.mark.parametrize("resources", [
        [{"file_size": 5}],
        [{"file": "a", "file_size": 5}, {"file_size": 5}],
        [{"file": "a", "file_size": "five"}],
        [{"file": ["unhashable"], "file_size": 5}],
        [{"file": ["unhashable"], "file_size": 5}, {"file_size": 5}],
        [{"file": "a", "file_size": [1]}],
        ["not a resource"],
        [None],
    ])
    def test_a_malformed_resource_raises_what_it_raised(self, resources):
        was, now = _counters_only({"resources": resources}, [])
        assert now == was
        assert was[0] == "raised"

    def test_a_malformed_configuration_raises_what_it_raised(self):
        bad = {"max_overlapping_files": "many"}
        for resources in ([], [{"file": "a", "file_size": 5}], [{"file_size": 5}]):
            was, now = _counters_only({"resources": resources}, [], bad)
            assert now == was
            assert was[0] == "raised"

    def test_the_shapes_a_snapshot_can_have(self):
        for snapshot in ({}, {"resources": None}, {"resources": []}, {"resources": {}},
                         {"resources": [{"file": "a"}]}, {"resources": [{"file": None, "file_size": 5}]},
                         {"resources": ({"file": "a", "file_size": 5},)}):
            was, now = _counters_only(snapshot, [])
            assert (now[0], now[2], now[3]) == (was[0], was[2], was[3])

    def test_the_edges_of_what_counts_and_of_the_gate(self):
        """A file exactly at the chunk size is not a small file; the gate
        opens above the chunk size in bytes, and at the file limit in count.
        Both ways through the step agree with the step as it was."""
        config = {"max_memory_chunk_size": 1000, "max_overlapping_files": 3}
        cases = {
            "one byte under the chunk size": ([999], {}),
            "exactly the chunk size": ([1000], {}),
            "bytes exactly at the chunk size": ([500, 500], {}),
            "one byte over": ([500, 501], {"overlap_files_false": 2, "overlap_files_total_bytes": 1001}),
            "two files, few bytes": ([1, 1], {}),
            "the third file": ([1, 1, 1], {"overlap_files_false": 3, "overlap_files_total_bytes": 3}),
            "a file at the chunk size beside small ones": (
                [1000, 400, 400], {}),
            "a large file beside enough small bytes": (
                [5000, 600, 600], {"overlap_files_false": 2, "overlap_files_total_bytes": 1200}),
        }
        for label, (sizes, expected) in cases.items():
            snapshot = {"resources": [{"file": f"f{i}", "file_size": size}
                                      for i, size in enumerate(sizes)]}
            was, now = _counters_only(snapshot, [], config)
            assert now[2] == was[2], label
            assert now[1] == set(), label
            assert {k: v for k, v in was[2].items() if k.startswith("overlap_files")} == expected, label
            # and the way that builds the set: the same set, the same counters
            full_was, full_now = _both(snapshot, [], config)
            assert full_now == full_was, label
            assert full_now[2] == was[2], label
            assert len(full_now[1]) == expected.get("overlap_files_false", 0), label

    def test_it_never_builds_the_set(self, monkeypatch):
        """Unless a file is listed twice, the pruning is not even called."""
        calls = []
        real = proc.prune_not_overlapping_files_by_threshold
        monkeypatch.setattr(proc, "prune_not_overlapping_files_by_threshold",
                            lambda *a, **k: (calls.append(1), real(*a, **k))[1])
        snapshot = {"resources": [{"file": f"f{i}", "file_size": 4000} for i in range(300)]}

        find_overlapping_files(snapshot, FRAME, [], want_candidates=False)
        assert calls == []
        find_overlapping_files(snapshot, FRAME, [])
        assert calls == [1]
        find_overlapping_files({"resources": snapshot["resources"] * 2}, FRAME, [],
                               want_candidates=False)
        assert calls == [1, 1], "a file listed twice is counted the long way"


# ---------------------------------------------------------------------------
# prune_not_overlapping_files_by_threshold
# ---------------------------------------------------------------------------

def _candidates(rng: random.Random, count: int) -> List[Tuple]:
    flags = [True, False, False, False, None, 0, 1, "yes", ""]
    return [(f"f{rng.randint(0, count)}", rng.choice(flags), rng.randint(0, 20 * MIB))
            for _ in range(count)]


class TestThePruning:

    def test_random_candidates_under_every_configuration(self):
        rng = random.Random(11)
        kept_some = kept_none = 0
        for _ in range(400):
            items = _candidates(rng, rng.choice([0, 1, 2, 3, 30, 99, 100, 101, 300]))
            for container in (set, list, tuple, frozenset):
                candidates = container(items)
                for config in CONFIGS:
                    was = _prune_as_it_was(candidates, config)
                    now = prune_not_overlapping_files_by_threshold(candidates, config)
                    assert now == was
                    assert type(now) is set
                    kept_some += bool(was)
                    kept_none += not was
        assert kept_some > 1000 and kept_none > 1000

    def test_only_the_flags_true_and_false_are_candidates_as_before(self):
        """An item whose flag is neither is dropped, gate open or not."""
        items = {("a", True, 1), ("b", False, 1), ("c", None, 1), ("d", 0, 1), ("e", 1, 1)}
        open_gate = {"max_overlapping_files": 1}
        shut_gate = {"max_overlapping_files": 10, "max_memory_chunk_size": 1000}
        assert prune_not_overlapping_files_by_threshold(items, open_gate) == {
            ("a", True, 1), ("b", False, 1)} == _prune_as_it_was(items, open_gate)
        assert prune_not_overlapping_files_by_threshold(items, shut_gate) == {
            ("a", True, 1)} == _prune_as_it_was(items, shut_gate)

    def test_the_gate_is_the_same_at_its_two_edges(self):
        config = {"max_memory_chunk_size": 1000, "max_overlapping_files": 3}
        for sizes, expected_open in (
            ([500, 500], False),          # 1,000 is not more than 1,000
            ([500, 501], True),
            ([1, 1, 1], True),            # the third small file
            ([1, 1], False),
        ):
            items = {(f"f{i}", False, size) for i, size in enumerate(sizes)}
            now = prune_not_overlapping_files_by_threshold(items, config)
            assert now == _prune_as_it_was(items, config)
            assert bool(now) is expected_open

    def test_sizes_of_overlapping_files_count_towards_the_gate_as_before(self):
        config = {"max_memory_chunk_size": 1000, "max_overlapping_files": 10}
        items = {("big", True, 5000), ("small", False, 1)}
        assert prune_not_overlapping_files_by_threshold(items, config) == items == _prune_as_it_was(
            items, config)

    def test_it_returns_a_new_set_and_leaves_the_one_it_was_given(self):
        items = {("a", True, 1), ("b", False, 1)}
        before = set(items)
        result = prune_not_overlapping_files_by_threshold(items, {"max_overlapping_files": 1})
        assert result == items and result is not items
        assert items == before

    def test_a_size_that_is_not_a_number_is_refused_as_before(self):
        items = [("a", False, None)]
        with pytest.raises(TypeError):
            _prune_as_it_was(items)
        with pytest.raises(TypeError):
            prune_not_overlapping_files_by_threshold(items)
