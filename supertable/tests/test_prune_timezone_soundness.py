# route: supertable.tests.test_prune_timezone_soundness
"""Pruning must not drop a file because of a timezone the literal never named.

The stats artifact stores timestamps as UTC instants. DuckDB, comparing a naive
literal against a ``TIMESTAMPTZ`` column, resolves that literal in the SESSION
timezone — so ``TIMESTAMP '2025-12-02 00:00:00'`` at +01:00 denotes the instant
``2025-12-01 23:00:00Z``, an hour earlier than the same text read as UTC.

Comparing the literal directly against UTC stats pruned files whose final hour
genuinely matched. Measured on a 100-file, 10M-row table: 1,323 rows silently
lost from a 30-day window, and 2.8% of the answer on a narrow one. The error
window is exactly the UTC offset, so it cannot reproduce on a UTC+0 machine —
which is why a green CI never caught it.

The lane recorded in the stats is ``"timestamp"`` for naive and zone-aware
columns alike, so the pruner cannot tell them apart, and the session timezone
belongs to the engine. The bound is therefore widened to every instant it could
denote. These tests pin that the widening is wide enough in both directions and
that it does not quietly disable pruning altogether.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from supertable.processing import (
    _MAX_UTC_OFFSET_EAST,
    _MAX_UTC_OFFSET_WEST,
    _pred_overlaps_stored,
    _widen_naive_timestamp_bounds,
)


class _Pred:
    """Minimal stand-in for PredInterval."""

    def __init__(self, lane, lo=None, hi=None, lo_incl=True, hi_incl=True):
        self.lane, self.lo, self.hi = lane, lo, hi
        self.lo_incl, self.hi_incl = lo_incl, hi_incl


def _ts(*a):
    return datetime(*a)


# --------------------------------------------------------------------------
# The exact production failure
# --------------------------------------------------------------------------

def test_the_regression_file_is_retained():
    """The real file, the real predicate, from the 10M-row table.

    Stored UTC max 23:59:51 is BEFORE the literal read as UTC, but AFTER it
    read at +01:00 (23:00:00Z) — where 1,323 rows really do match.
    """
    stored = ("timestamp", _ts(2025, 11, 29, 0, 0), _ts(2025, 12, 1, 23, 59, 51))
    pred = _Pred("timestamp", lo=_ts(2025, 12, 2, 0, 0))
    assert _pred_overlaps_stored(pred, stored) is True


def test_bare_string_literal_carries_the_same_ambiguity():
    """`event_ts >= '2025-12-02'` parses naive too, so it must widen as well."""
    stored = ("timestamp", _ts(2025, 11, 29, 0, 0), _ts(2025, 12, 1, 23, 59, 51))
    assert _pred_overlaps_stored(_Pred("string", lo="2025-12-02"), stored) is True


# --------------------------------------------------------------------------
# The widening covers every real zone, in both directions
# --------------------------------------------------------------------------

@pytest.mark.parametrize("offset_hours", [14, 13, 12, 5.5, 1, 0, -5, -12])
def test_no_real_timezone_can_hide_a_matching_row(offset_hours):
    """For any zone, a file whose max is just inside the window is kept.

    The literal L read at offset O is the instant L - O. A file ending one
    second after that instant contains a matching row, whatever O is.
    """
    literal = _ts(2025, 12, 2, 0, 0)
    instant = literal - timedelta(hours=offset_hours)
    stored = ("timestamp", instant - timedelta(days=3),
              instant + timedelta(seconds=1))
    assert _pred_overlaps_stored(_Pred("timestamp", lo=literal), stored) is True


@pytest.mark.parametrize("offset_hours", [14, 1, 0, -5, -12])
def test_upper_bound_widens_the_other_way(offset_hours):
    """`col <= L` is ambiguous in the opposite direction."""
    literal = _ts(2025, 12, 2, 0, 0)
    instant = literal - timedelta(hours=offset_hours)
    stored = ("timestamp", instant - timedelta(seconds=1),
              instant + timedelta(days=3))
    assert _pred_overlaps_stored(_Pred("timestamp", hi=literal), stored) is True


def test_widening_spans_the_full_offset_range():
    lo, hi = _widen_naive_timestamp_bounds(_ts(2025, 6, 1), _ts(2025, 6, 2))
    assert lo == _ts(2025, 6, 1) - _MAX_UTC_OFFSET_EAST
    assert hi == _ts(2025, 6, 2) + _MAX_UTC_OFFSET_WEST
    assert _MAX_UTC_OFFSET_EAST >= timedelta(hours=14), "Kiribati is +14:00"
    assert _MAX_UTC_OFFSET_WEST >= timedelta(hours=12), "Baker Island is -12:00"


def test_zone_aware_bounds_are_left_alone():
    """An explicit offset is unambiguous; widening it would only lose pruning."""
    aware = datetime(2025, 12, 2, tzinfo=timezone.utc)
    assert _widen_naive_timestamp_bounds(aware, aware) == (aware, aware)


def test_none_bounds_survive():
    assert _widen_naive_timestamp_bounds(None, None) == (None, None)


# --------------------------------------------------------------------------
# Soundness must not become "keep everything"
# --------------------------------------------------------------------------

def test_pruning_still_happens_beyond_the_offset_window():
    """A file ending well before the bound is still dropped.

    Without this the fix would be indistinguishable from disabling pruning.
    """
    stored = ("timestamp", _ts(2025, 1, 1), _ts(2025, 1, 5))
    pred = _Pred("timestamp", lo=_ts(2025, 12, 2, 0, 0))
    assert _pred_overlaps_stored(pred, stored) is False


def test_only_the_offset_window_is_newly_retained():
    """Just outside max-offset + 1s, the file is still pruned."""
    literal = _ts(2025, 12, 2, 0, 0)
    just_outside = literal - _MAX_UTC_OFFSET_EAST - timedelta(seconds=1)
    stored = ("timestamp", just_outside - timedelta(days=1), just_outside)
    assert _pred_overlaps_stored(_Pred("timestamp", lo=literal), stored) is False
