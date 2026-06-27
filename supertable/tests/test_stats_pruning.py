"""Stats-driven file pruning for overwrite / delete (consumer 5a).

The write path narrows the set of files it must open during an overwrite/delete
by comparing the incoming dataframe's per-key-column range ("df-probe") against
the external stats artifact, dropping files that *provably* hold none of the
incoming keys.

The pruning contract is one-directional and tested from every angle:

  * **Primitives** — interval overlap, dtype→lane routing, probe derivation,
    stored-row lane extraction.
  * **Decision matrix** — AND-within-row-group, OR-across-row-groups, and every
    "retain on uncertainty" branch (missing file / row-group / column stat,
    ``stats_available`` False, lane mismatch, no constraints, empty stats).
  * **No-false-negative property** — randomized fuzz proving the pruned set is
    always a superset of the files that actually contain a matching key.
  * **Differential identity** — feeding the pruned set vs the full set into the
    real :func:`identify_deleted_rowids` yields byte-identical tombstone pairs,
    so pruning is a pure performance optimisation with zero behavioural change.
"""

from __future__ import annotations

import io
import random
from datetime import datetime, date, timezone
from decimal import Decimal
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable.processing import (
    STATS_SCHEMA,
    _intervals_overlap,
    _probe_lane_for_dtype,
    _normalise_probe_bounds,
    _stored_lane,
    _stats_rows_for_metadata,
    probe_ranges_from_df,
    prune_overlapping_files_by_stats,
    identify_deleted_rowids,
)

_MOD = "supertable.processing"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _overlap_set(paths, size: int = 1000):
    """An overwrite/delete candidate set (every file has_overlap=True)."""
    return {(p, True, size) for p in paths}


def _stored_stats(files: dict, row_group_size: int | None = None) -> pl.DataFrame:
    """Derive the stored stats frame from each file's *real* parquet footer.

    Mirrors exactly what ``build_stats_file`` persists (it calls the same
    ``_stats_rows_for_metadata``), so the stored min/max are the genuine footer
    stats — and therefore numerically identical to the df-probe min/max. This
    is the whole point of df-probe equivalence.
    """
    rows = []
    for fp, df in files.items():
        buf = io.BytesIO()
        kwargs = {"write_statistics": True}
        if row_group_size is not None:
            kwargs["row_group_size"] = row_group_size
        pq.write_table(df.to_arrow(), buf, **kwargs)
        md = pq.read_metadata(io.BytesIO(buf.getvalue()))
        rows.extend(_stats_rows_for_metadata(fp, md))
    if not rows:
        return pl.DataFrame(schema=STATS_SCHEMA)
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def _srow(fp, rg, col, *, lane="bigint", mn=None, mx=None,
          stats_available=True):
    """Hand-build a single stored stats row dict (for decision-matrix tests)."""
    base = {k: None for k in STATS_SCHEMA}
    base.update(
        file_path=fp, row_group_id=rg, column_name=col,
        physical_type="INT64", logical_type="",
        null_count=0, row_group_rows=1,
        stats_available=stats_available, min_is_exact=True, max_is_exact=True,
    )
    if stats_available:
        base[f"min_{lane}"] = mn
        base[f"max_{lane}"] = mx
    return base


def _stats_df(rows) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


# ---------------------------------------------------------------------------
# 1. _intervals_overlap primitive
# ---------------------------------------------------------------------------

class TestIntervalsOverlap:

    @pytest.mark.parametrize("a_lo,a_hi,b_lo,b_hi,expected", [
        (1, 5, 3, 9, True),     # partial overlap
        (1, 5, 5, 9, True),     # touch at a single point (inclusive)
        (1, 5, 6, 9, False),    # disjoint, a below b
        (6, 9, 1, 5, False),    # disjoint, a above b
        (1, 10, 3, 4, True),    # b fully inside a
        (3, 4, 1, 10, True),    # a fully inside b
        (5, 5, 5, 5, True),     # both degenerate equal points
        (5, 5, 6, 6, False),    # adjacent degenerate points
    ])
    def test_int_ranges(self, a_lo, a_hi, b_lo, b_hi, expected):
        assert _intervals_overlap(a_lo, a_hi, b_lo, b_hi) is expected

    def test_float_ranges(self):
        assert _intervals_overlap(1.0, 2.5, 2.5, 3.0) is True
        assert _intervals_overlap(1.0, 2.4, 2.5, 3.0) is False

    def test_string_ranges(self):
        assert _intervals_overlap("a", "m", "k", "z") is True
        assert _intervals_overlap("a", "f", "g", "z") is False

    def test_datetime_ranges(self):
        a = (datetime(2026, 1, 1), datetime(2026, 1, 10))
        assert _intervals_overlap(*a, datetime(2026, 1, 10), datetime(2026, 2, 1)) is True
        assert _intervals_overlap(*a, datetime(2026, 1, 11), datetime(2026, 2, 1)) is False

    def test_symmetric(self):
        # overlap is symmetric in (a, b)
        for _ in range(200):
            a_lo = random.randint(-50, 50); a_hi = a_lo + random.randint(0, 50)
            b_lo = random.randint(-50, 50); b_hi = b_lo + random.randint(0, 50)
            assert (_intervals_overlap(a_lo, a_hi, b_lo, b_hi)
                    == _intervals_overlap(b_lo, b_hi, a_lo, a_hi))


# ---------------------------------------------------------------------------
# 2. dtype → lane routing & bound normalisation
# ---------------------------------------------------------------------------

class TestProbeLaneForDtype:

    @pytest.mark.parametrize("dtype,lane", [
        (pl.Int8, "bigint"), (pl.Int16, "bigint"),
        (pl.Int32, "bigint"), (pl.Int64, "bigint"),
        (pl.UInt8, "bigint"), (pl.UInt16, "bigint"), (pl.UInt32, "bigint"),
        (pl.Boolean, "bigint"),
        (pl.Float32, "double"), (pl.Float64, "double"),
        (pl.Date, "timestamp"),
        (pl.Utf8, "string"),
    ])
    def test_supported(self, dtype, lane):
        assert _probe_lane_for_dtype(dtype) == lane

    def test_datetime_variants(self):
        assert _probe_lane_for_dtype(pl.Datetime("us")) == "timestamp"
        assert _probe_lane_for_dtype(pl.Datetime("ns")) == "timestamp"
        assert _probe_lane_for_dtype(pl.Datetime("us", "UTC")) == "timestamp"

    def test_uint64_unsupported(self):
        # UInt64 can exceed Int64 → never prune on it.
        assert _probe_lane_for_dtype(pl.UInt64) is None

    def test_decimal_unsupported(self):
        assert _probe_lane_for_dtype(pl.Decimal(10, 2)) is None

    def test_binary_and_misc_unsupported(self):
        assert _probe_lane_for_dtype(pl.Binary) is None
        assert _probe_lane_for_dtype(pl.Time) is None

    def test_normalise_bounds_timestamp_tz(self):
        lo, hi = _normalise_probe_bounds(
            "timestamp",
            datetime(2026, 1, 1, 10, tzinfo=timezone.utc),
            datetime(2026, 1, 2, 10, tzinfo=timezone.utc),
        )
        assert lo == datetime(2026, 1, 1, 10) and lo.tzinfo is None


# ---------------------------------------------------------------------------
# 3. probe_ranges_from_df
# ---------------------------------------------------------------------------

class TestProbeRangesFromDf:

    def test_int_and_string_lanes(self):
        df = pl.DataFrame({"a": [3, 1, 2], "b": ["c", "a", "b"]})
        pr = probe_ranges_from_df(df, ["a", "b"])
        assert pr["a"] == ("bigint", 1, 3)
        assert pr["b"] == ("string", "a", "c")

    def test_float_lane(self):
        df = pl.DataFrame({"a": [2.5, 1.5, 3.5]})
        assert probe_ranges_from_df(df, ["a"])["a"] == ("double", 1.5, 3.5)

    def test_bool_lane(self):
        df = pl.DataFrame({"a": [True, False, True]})
        assert probe_ranges_from_df(df, ["a"])["a"] == ("bigint", 0, 1)

    def test_date_lane_midnight(self):
        df = pl.DataFrame({"a": [date(2026, 1, 5), date(2026, 1, 1)]})
        assert probe_ranges_from_df(df, ["a"])["a"] == (
            "timestamp", datetime(2026, 1, 1), datetime(2026, 1, 5))

    def test_timestamp_lane(self):
        df = pl.DataFrame(
            {"a": [datetime(2026, 1, 1, 10), datetime(2026, 1, 2, 11)]},
            schema={"a": pl.Datetime("us")},
        )
        assert probe_ranges_from_df(df, ["a"])["a"] == (
            "timestamp", datetime(2026, 1, 1, 10), datetime(2026, 1, 2, 11))

    def test_tz_aware_normalised(self):
        df = pl.DataFrame(
            {"a": [datetime(2026, 1, 1, 10, tzinfo=timezone.utc)]},
            schema={"a": pl.Datetime("us", "UTC")},
        )
        lane, lo, hi = probe_ranges_from_df(df, ["a"])["a"]
        assert lane == "timestamp" and lo.tzinfo is None and lo == datetime(2026, 1, 1, 10)

    def test_null_disables_column(self):
        # A NULL key could match a file whose footer range excludes NULL, so the
        # column must not prune (overwrite equality is null-safe).
        df = pl.DataFrame({"a": [1, None, 3]})
        assert probe_ranges_from_df(df, ["a"])["a"] is None

    def test_uint64_disables_column(self):
        df = pl.DataFrame({"a": [1, 2, 3]}, schema={"a": pl.UInt64})
        assert probe_ranges_from_df(df, ["a"])["a"] is None

    def test_decimal_disables_column(self):
        df = pl.DataFrame(
            {"a": [Decimal("1.5"), Decimal("2.5")]},
            schema={"a": pl.Decimal(10, 2)},
        )
        assert probe_ranges_from_df(df, ["a"])["a"] is None

    def test_missing_column_disables(self):
        df = pl.DataFrame({"a": [1, 2]})
        assert probe_ranges_from_df(df, ["z"])["z"] is None

    def test_empty_df_disables(self):
        df = pl.DataFrame({"a": []}, schema={"a": pl.Int64})
        assert probe_ranges_from_df(df, ["a"])["a"] is None


# ---------------------------------------------------------------------------
# 4. _stored_lane
# ---------------------------------------------------------------------------

class TestStoredLane:

    def test_bigint(self):
        assert _stored_lane(_srow("f", 0, "a", lane="bigint", mn=1, mx=5)) == ("bigint", 1, 5)

    def test_double(self):
        assert _stored_lane(_srow("f", 0, "a", lane="double", mn=1.5, mx=5.5)) == ("double", 1.5, 5.5)

    def test_string(self):
        assert _stored_lane(_srow("f", 0, "a", lane="string", mn="a", mx="z")) == ("string", "a", "z")

    def test_timestamp(self):
        r = _srow("f", 0, "a", lane="timestamp",
                  mn=datetime(2026, 1, 1), mx=datetime(2026, 1, 5))
        assert _stored_lane(r) == ("timestamp", datetime(2026, 1, 1), datetime(2026, 1, 5))

    def test_zero_bound_is_not_treated_as_missing(self):
        # min=0 must still register (is-not-None check, not truthiness).
        assert _stored_lane(_srow("f", 0, "a", lane="bigint", mn=0, mx=0)) == ("bigint", 0, 0)

    def test_stats_unavailable_returns_none(self):
        assert _stored_lane(_srow("f", 0, "a", stats_available=False)) is None

    def test_no_lane_populated_returns_none(self):
        row = {k: None for k in STATS_SCHEMA}
        row["stats_available"] = True
        assert _stored_lane(row) is None


# ---------------------------------------------------------------------------
# 5. prune_overlapping_files_by_stats — decision matrix
# ---------------------------------------------------------------------------

class TestPruneDecisionMatrix:

    def _two_file_stats(self):
        # fileA: a∈[1,5]; fileB: a∈[100,200]
        return _stats_df([
            _srow("A", 0, "a", mn=1, mx=5),
            _srow("B", 0, "a", mn=100, mx=200),
        ])

    def test_drops_non_overlapping_file(self):
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A", "B"]), self._two_file_stats(), {"a": ("bigint", 2, 3)})
        assert {f for f, _, _ in kept} == {"A"}

    def test_keeps_both_when_probe_spans_both(self):
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A", "B"]), self._two_file_stats(), {"a": ("bigint", 3, 150)})
        assert {f for f, _, _ in kept} == {"A", "B"}

    def test_drops_all_when_probe_outside_every_range(self):
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A", "B"]), self._two_file_stats(), {"a": ("bigint", 500, 600)})
        assert kept == set()

    def test_no_constraints_returns_input_unchanged(self):
        files = _overlap_set(["A", "B"])
        assert prune_overlapping_files_by_stats(files, self._two_file_stats(), {"a": None}) == files

    def test_empty_stats_returns_input_unchanged(self):
        files = _overlap_set(["A", "B"])
        empty = pl.DataFrame(schema=STATS_SCHEMA)
        assert prune_overlapping_files_by_stats(files, empty, {"a": ("bigint", 500, 600)}) == files

    def test_none_stats_returns_input_unchanged(self):
        files = _overlap_set(["A", "B"])
        assert prune_overlapping_files_by_stats(files, None, {"a": ("bigint", 500, 600)}) == files

    def test_missing_file_in_stats_is_retained(self):
        # fileC has no stats rows → cannot prove absence → retained.
        files = _overlap_set(["A", "C"])
        kept = prune_overlapping_files_by_stats(
            files, self._two_file_stats(), {"a": ("bigint", 500, 600)})
        assert {f for f, _, _ in kept} == {"C"}

    def test_or_across_row_groups(self):
        # fileA rg0 a∈[1,5], rg1 a∈[50,60]; probe hits only rg1 → file kept.
        stats = _stats_df([
            _srow("A", 0, "a", mn=1, mx=5),
            _srow("A", 1, "a", mn=50, mx=60),
        ])
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A"]), stats, {"a": ("bigint", 55, 58)})
        assert {f for f, _, _ in kept} == {"A"}

    def test_and_within_row_group(self):
        # rg0: a∈[1,5], b∈[10,20]; probe a hits but b misses → row group excluded.
        stats = _stats_df([
            _srow("A", 0, "a", mn=1, mx=5),
            _srow("A", 0, "b", mn=10, mx=20),
        ])
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A"]), stats,
            {"a": ("bigint", 2, 3), "b": ("bigint", 100, 200)})
        assert kept == set()

    def test_missing_column_stat_retains_row_group(self):
        # Constrain on b, but stats only carry a → b can't exclude → retained.
        stats = _stats_df([_srow("A", 0, "a", mn=1, mx=5)])
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A"]), stats, {"b": ("bigint", 500, 600)})
        assert {f for f, _, _ in kept} == {"A"}

    def test_stats_unavailable_retains(self):
        stats = _stats_df([_srow("A", 0, "a", stats_available=False)])
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A"]), stats, {"a": ("bigint", 500, 600)})
        assert {f for f, _, _ in kept} == {"A"}

    def test_lane_mismatch_retains(self):
        # Stored lane is string, probe lane is bigint → can't compare → retain.
        stats = _stats_df([_srow("A", 0, "a", lane="string", mn="x", mx="z")])
        kept = prune_overlapping_files_by_stats(
            _overlap_set(["A"]), stats, {"a": ("bigint", 1, 2)})
        assert {f for f, _, _ in kept} == {"A"}

    def test_has_overlap_false_passthrough(self):
        # Pure-compaction candidates (has_overlap=False) are never pruned.
        files = {("A", False, 10)}
        stats = self._two_file_stats()
        kept = prune_overlapping_files_by_stats(files, stats, {"a": ("bigint", 500, 600)})
        assert kept == files

    def test_records_pruned_count_on_profiler(self):
        from supertable.utils.profiler import Profiler
        prof = Profiler()
        prune_overlapping_files_by_stats(
            _overlap_set(["A", "B"]), self._two_file_stats(),
            {"a": ("bigint", 2, 3)}, profiler=prof)
        counts = prof.emit_counts()
        assert counts.get("stats_pruned_files") == 1


# ---------------------------------------------------------------------------
# 6. No-false-negative property (randomized)
# ---------------------------------------------------------------------------

class TestNoFalseNegativeProperty:
    """The pruned set must always be a superset of the files that actually
    contain a matching incoming key — pruning may never drop a file that holds
    a match. Verified over many random scenarios using *real* footer stats so
    probe and stored ranges are genuinely consistent."""

    def _random_int_file(self, rng, n_rows):
        return pl.DataFrame({"a": [rng.randint(0, 100) for _ in range(n_rows)]})

    @pytest.mark.parametrize("seed", range(40))
    def test_pruned_superset_of_true_matches(self, seed):
        rng = random.Random(seed)
        n_files = rng.randint(1, 6)
        files = {f"f{i}.parquet": self._random_int_file(rng, rng.randint(1, 30))
                 for i in range(n_files)}
        rg_size = rng.choice([None, 5, 10])
        stats = _stored_stats(files, row_group_size=rg_size)

        # Incoming keys: a random contiguous-ish set of integers.
        lo = rng.randint(0, 100)
        hi = lo + rng.randint(0, 40)
        incoming = pl.DataFrame({"a": list(range(lo, hi + 1))})
        probe = probe_ranges_from_df(incoming, ["a"])

        kept = prune_overlapping_files_by_stats(
            _overlap_set(files.keys()), stats, probe)
        kept_names = {f for f, _, _ in kept}

        key_set = set(range(lo, hi + 1))
        true_matches = {
            name for name, df in files.items()
            if set(df["a"].to_list()) & key_set
        }
        # Every file that truly contains a matching key must be retained.
        assert true_matches <= kept_names, (
            f"dropped a file with a real match: missing={true_matches - kept_names}")


# ---------------------------------------------------------------------------
# 7. Differential identity vs unpruned (the behavioural guarantee)
# ---------------------------------------------------------------------------

class TestDifferentialTombstoneIdentity:
    """Feeding the pruned candidate set into the real ``identify_deleted_rowids``
    must produce byte-identical ``(file, rowid)`` pairs as feeding the full set —
    proving pruning changes nothing observable, only the work done."""

    def _file_with_rowids(self, keys, rowid_start):
        return pl.DataFrame({
            "a": list(keys),
            "__rowid__": [rowid_start + i for i in range(len(keys))],
            "val": [f"v{k}" for k in keys],
        })

    def _run(self, files: dict, incoming: pl.DataFrame, rg_size=None):
        # __rowid__ is a system column the production extractor drops, so the
        # stored stats carry only the key columns — same as the write path.
        stats = _stored_stats(files, row_group_size=rg_size)

        all_files = _overlap_set(files.keys())
        cache = dict(files)  # file_cache: serve dfs from memory, no storage read

        probe = probe_ranges_from_df(incoming, ["a"])
        pruned = prune_overlapping_files_by_stats(all_files, stats, probe)

        unpruned_pairs = sorted(identify_deleted_rowids(
            incoming, all_files, ["a"], file_cache=cache))
        pruned_pairs = sorted(identify_deleted_rowids(
            incoming, pruned, ["a"], file_cache=cache))
        return unpruned_pairs, pruned_pairs, all_files, pruned

    def test_disjoint_file_is_pruned_without_changing_result(self):
        files = {
            "A.parquet": self._file_with_rowids([1, 2, 3], 0),
            "B.parquet": self._file_with_rowids([100, 101, 102], 100),
        }
        incoming = pl.DataFrame({"a": [2, 3]})
        unpruned, pruned, all_f, kept = self._run(files, incoming)
        assert unpruned == pruned
        assert {f for f, _, _ in kept} == {"A.parquet"}  # B actually dropped

    def test_match_in_both_files_keeps_both(self):
        files = {
            "A.parquet": self._file_with_rowids([1, 2, 3], 0),
            "B.parquet": self._file_with_rowids([3, 4, 5], 100),
        }
        incoming = pl.DataFrame({"a": [3]})
        unpruned, pruned, all_f, kept = self._run(files, incoming)
        assert unpruned == pruned
        assert {f for f, _, _ in kept} == {"A.parquet", "B.parquet"}

    @pytest.mark.parametrize("seed", range(40))
    def test_random_differential_identity(self, seed):
        rng = random.Random(1000 + seed)
        files = {}
        rid = 0
        for i in range(rng.randint(1, 5)):
            keys = [rng.randint(0, 80) for _ in range(rng.randint(1, 25))]
            files[f"f{i}.parquet"] = self._file_with_rowids(keys, rid)
            rid += 1000
        lo = rng.randint(0, 80)
        incoming = pl.DataFrame({"a": list(range(lo, lo + rng.randint(0, 30) + 1))})
        unpruned, pruned, _all, _kept = self._run(
            files, incoming, rg_size=rng.choice([None, 4, 8]))
        assert unpruned == pruned


# ---------------------------------------------------------------------------
# 8. Edge scenarios on the integration shape
# ---------------------------------------------------------------------------

class TestEdgeScenarios:

    def test_null_key_disables_all_pruning(self):
        # With a NULL in the key column, no file may be pruned even if ranges
        # look disjoint — the probe column resolves to None.
        files = {
            "A.parquet": pl.DataFrame({"a": [1, 2, 3]}),
            "B.parquet": pl.DataFrame({"a": [100, 200]}),
        }
        stats = _stored_stats(files)
        incoming = pl.DataFrame({"a": [500, None, 600]})
        probe = probe_ranges_from_df(incoming, ["a"])
        assert probe["a"] is None
        kept = prune_overlapping_files_by_stats(_overlap_set(files.keys()), stats, probe)
        assert {f for f, _, _ in kept} == {"A.parquet", "B.parquet"}

    def test_string_lane_pruning(self):
        files = {
            "A.parquet": pl.DataFrame({"k": ["alpha", "beta", "gamma"]}),
            "B.parquet": pl.DataFrame({"k": ["xena", "yuki", "zara"]}),
        }
        stats = _stored_stats(files)
        incoming = pl.DataFrame({"k": ["beta"]})
        probe = probe_ranges_from_df(incoming, ["k"])
        kept = prune_overlapping_files_by_stats(_overlap_set(files.keys()), stats, probe)
        assert {f for f, _, _ in kept} == {"A.parquet"}

    def test_timestamp_lane_pruning(self):
        files = {
            "A.parquet": pl.DataFrame(
                {"t": [datetime(2026, 1, 1), datetime(2026, 1, 5)]},
                schema={"t": pl.Datetime("us")}),
            "B.parquet": pl.DataFrame(
                {"t": [datetime(2026, 6, 1), datetime(2026, 6, 5)]},
                schema={"t": pl.Datetime("us")}),
        }
        stats = _stored_stats(files)
        incoming = pl.DataFrame(
            {"t": [datetime(2026, 1, 3)]}, schema={"t": pl.Datetime("us")})
        probe = probe_ranges_from_df(incoming, ["t"])
        kept = prune_overlapping_files_by_stats(_overlap_set(files.keys()), stats, probe)
        assert {f for f, _, _ in kept} == {"A.parquet"}

    def test_float_lane_pruning(self):
        files = {
            "A.parquet": pl.DataFrame({"x": [1.0, 2.0, 3.0]}),
            "B.parquet": pl.DataFrame({"x": [100.5, 200.5]}),
        }
        stats = _stored_stats(files)
        incoming = pl.DataFrame({"x": [2.5]})
        probe = probe_ranges_from_df(incoming, ["x"])
        kept = prune_overlapping_files_by_stats(_overlap_set(files.keys()), stats, probe)
        assert {f for f, _, _ in kept} == {"A.parquet"}
