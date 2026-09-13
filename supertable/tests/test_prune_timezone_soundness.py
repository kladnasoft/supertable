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
    reset_session_timezone,
    set_session_timezone,
)


@pytest.fixture(autouse=True)
def _no_session_timezone():
    """Every test in this module states its own timezone assumption.

    The pruner narrows its padding once the engine reports which zone it
    resolves naive literals in, and that report lands in a module global. Any
    test in the same process that opens a DuckDB connection publishes it — so
    without this reset these tests would measure whatever zone the host
    machine happens to be in, and pass or fail by geography. That is not
    hypothetical: it is how they first failed.

    The tests below exercise the *unknown-zone* fallback, whose contract is
    unchanged — cover every offset on earth. ``TestKnownSessionZone`` sets a
    zone explicitly and asserts the narrower one.
    """
    reset_session_timezone()
    yield
    reset_session_timezone()


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


# --------------------------------------------------------------------------
# A bare string is cast to the COLUMN's type, which may drop the time
# --------------------------------------------------------------------------

def test_text_lower_bound_is_floored_to_the_day():
    """`event_date >= '2025-04-05 23:59:59'` matches every row on the 5th.

    DuckDB casts the string to the column type. Against a DATE column that
    discards the time, so the predicate is really `>= DATE '2025-04-05'`.
    Holding the full 23:59:59 pruned the matching day away — 277 rows lost on a
    96k-row table, found by the generated pruning corpus.
    """
    stored = ("timestamp", _ts(2025, 3, 20), _ts(2025, 4, 5))   # max is midnight
    pred = _Pred("string", lo="2025-04-05 23:59:59")
    assert _pred_overlaps_stored(pred, stored) is True


def test_timestamp_literal_is_not_floored():
    """The explicit-cast form genuinely differs and must not be widened.

        DATE '2025-04-05' >= '2025-04-05 23:59:59'            -> True
        DATE '2025-04-05' >= TIMESTAMP '2025-04-05 23:59:59'  -> False

    An explicit TIMESTAMP promotes the DATE to midnight rather than truncating
    the literal, so flooring here would only give up pruning for nothing.
    """
    from supertable.processing import _floor_text_lower_bound_to_day

    lit = _ts(2025, 4, 5, 23, 59, 59)
    assert _floor_text_lower_bound_to_day(lit) == _ts(2025, 4, 5)
    # ...but the timestamp lane never routes through it: with only the timezone
    # widening, a file ending a day earlier is still pruned.
    stored = ("timestamp", _ts(2025, 3, 20), _ts(2025, 4, 3))
    assert _pred_overlaps_stored(_Pred("timestamp", lo=lit), stored) is False


def test_flooring_does_not_disable_pruning():
    """A file ending well before the literal's day is still dropped."""
    stored = ("timestamp", _ts(2025, 1, 1), _ts(2025, 1, 5))
    assert _pred_overlaps_stored(
        _Pred("string", lo="2025-04-05 23:59:59"), stored) is False


# --------------------------------------------------------------------------
# When the engine reports its zone, the padding narrows to that zone
# --------------------------------------------------------------------------

class TestKnownSessionZone:
    """The padding above is sound but blunt: 26 hours to cover every zone.

    A server does not run in every zone at once. Once the engine reports the
    one it resolves naive literals in, the padding collapses to that zone's
    own offset range — 2 hours on Europe/Budapest, none at all on UTC. The
    saving is largest exactly where it matters: a 24-hour window was scanning
    50 hours of range, 2.08x what was asked for.

    Soundness is unchanged in kind. A naive literal ``L`` still denotes either
    ``L`` (naive column) or ``L - offset`` (zone-aware column), and the bound
    still has to contain both. It just no longer has to contain offsets the
    session can never produce.
    """

    LITERAL = datetime(2026, 7, 15, 12, 0, 0)      # summer: DST in play

    @pytest.mark.parametrize("zone,lo_pad_h,hi_pad_h", [
        ("UTC", 0, 0),
        ("Europe/London", 1, 0),            # +0/+1
        ("Europe/Budapest", 2, 0),          # +1/+2
        ("Asia/Kolkata", 5.5, 0),           # +5:30, no DST
        ("Asia/Tokyo", 9, 0),               # +9, no DST
        ("Pacific/Kiritimati", 14, 0),      # +14, the eastern extreme
        ("America/New_York", 0, 5),         # -5/-4
        ("America/St_Johns", 0, 3.5),       # -3:30/-2:30
        ("Pacific/Midway", 0, 11),          # -11
    ])
    def test_padding_matches_the_zones_own_offset_range(self, zone, lo_pad_h,
                                                        hi_pad_h):
        set_session_timezone(zone)
        lo, hi = _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL)

        assert (self.LITERAL - lo) == timedelta(hours=lo_pad_h)
        assert (hi - self.LITERAL) == timedelta(hours=hi_pad_h)

    @pytest.mark.parametrize("zone", [
        "UTC", "Europe/London", "Europe/Budapest", "Asia/Kolkata",
        "Asia/Tokyo", "Pacific/Kiritimati", "America/New_York",
        "America/St_Johns", "Pacific/Midway", "Australia/Lord_Howe",
    ])
    def test_the_bound_still_contains_every_reading_of_the_literal(self, zone):
        """The soundness property itself, per zone.

        Both readings must fall inside the bound: the literal as a naive
        instant, and the literal resolved in this zone — at either DST state,
        since the padding window can straddle a transition.
        """
        from zoneinfo import ZoneInfo

        set_session_timezone(zone)
        lo, hi = _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL)

        z = ZoneInfo(zone)
        readings = [self.LITERAL]                       # naive column
        for probe in (self.LITERAL, self.LITERAL.replace(month=1),
                      self.LITERAL.replace(month=7)):
            readings.append(self.LITERAL - z.utcoffset(probe))   # tz-aware

        for instant in readings:
            assert lo <= instant <= hi, (
                f"{zone}: {instant} escapes [{lo}, {hi}] — a file holding a "
                f"matching row could be pruned"
            )

    def test_a_dst_transition_inside_the_window_is_covered(self):
        """Europe/Budapest switches on the last Sunday of October.

        A literal landing on the transition must still be padded by the wider
        of the two offsets, or the hour that moves is unprotected.
        """
        set_session_timezone("Europe/Budapest")
        on_transition = datetime(2026, 10, 25, 2, 30, 0)
        lo, _ = _widen_naive_timestamp_bounds(on_transition, on_transition)

        assert (on_transition - lo) == timedelta(hours=2), "must use +2, not +1"

    def test_the_real_production_file_is_still_retained(self):
        """The regression this whole guard exists for, under the narrow bound.

        Budapest is exactly the +01:00 session where 1,323 rows went missing.
        Narrowing the padding must not bring that back.
        """
        set_session_timezone("Europe/Budapest")
        stored = ("timestamp", _ts(2025, 11, 29, 0, 0),
                  _ts(2025, 12, 1, 23, 59, 51))
        assert _pred_overlaps_stored(
            _Pred("timestamp", lo=_ts(2025, 12, 2, 0, 0)), stored) is True

    def test_utc_needs_no_padding_at_all(self):
        """The common server deployment pays nothing for this ambiguity."""
        set_session_timezone("UTC")
        assert _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL) == (
            self.LITERAL, self.LITERAL)

    def test_the_narrow_bound_prunes_strictly_more(self):
        """The point of the change, stated as file counts.

        Hourly files, a "from hour 48" predicate: the planet-wide bound keeps
        everything from hour 34 onward, the zone-aware one from hour 46.
        """
        base = datetime(2026, 1, 15, 0, 0)
        files = [("timestamp", base + timedelta(hours=h),
                  base + timedelta(hours=h + 1)) for h in range(72)]
        pred = _Pred("timestamp", lo=base + timedelta(hours=48))

        reset_session_timezone()
        wide = sum(1 for f in files if _pred_overlaps_stored(pred, f))
        set_session_timezone("Europe/Budapest")
        narrow = sum(1 for f in files if _pred_overlaps_stored(pred, f))

        assert narrow < wide, f"expected fewer files, got {narrow} vs {wide}"
        assert (wide, narrow) == (39, 27)

    @pytest.mark.parametrize("bad", ["Not/AZone", "", "   ", None])
    def test_an_unusable_zone_falls_back_to_the_planet_wide_bound(self, bad):
        set_session_timezone(bad)
        lo, hi = _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL)
        assert (self.LITERAL - lo) == _MAX_UTC_OFFSET_EAST
        assert (hi - self.LITERAL) == _MAX_UTC_OFFSET_WEST

    def test_two_conflicting_zones_revert_to_the_planet_wide_bound(self):
        """One global cannot describe two sessions, so it stops guessing."""
        set_session_timezone("UTC")
        set_session_timezone("Asia/Tokyo")
        lo, hi = _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL)
        assert (self.LITERAL - lo) == _MAX_UTC_OFFSET_EAST
        assert (hi - self.LITERAL) == _MAX_UTC_OFFSET_WEST

    def test_the_same_zone_reported_twice_is_not_a_conflict(self):
        """Every connection reports; that must not disable the optimisation."""
        set_session_timezone("UTC")
        set_session_timezone("UTC")
        assert _widen_naive_timestamp_bounds(self.LITERAL, self.LITERAL) == (
            self.LITERAL, self.LITERAL)

    def test_a_zone_aware_bound_is_still_left_alone(self):
        """An explicit offset is unambiguous; padding it would be wrong."""
        set_session_timezone("Europe/Budapest")
        aware = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert _widen_naive_timestamp_bounds(aware, aware) == (aware, aware)


class TestEnginePublishesItsZone:
    """The link between the engine and the pruner, which unit tests cannot see.

    Everything above sets the zone by hand. If the engine never reported one,
    all of it would still pass while production quietly stayed on the
    planet-wide bound — the optimisation present in tests and absent in fact.
    """

    def test_opening_a_connection_publishes_the_session_zone(self):
        import tempfile

        from supertable.engine.engine_common import new_duckdb_connection
        from supertable.processing import _session_timezone_name

        reset_session_timezone()
        con = new_duckdb_connection(tempfile.mkdtemp())
        try:
            reported = con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
        finally:
            con.close()

        from supertable import processing as _p
        assert _p._session_timezone_name == reported, (
            "the engine must publish the zone it actually resolves literals "
            "in, or the pruner silently keeps the widest possible bound"
        )

    def test_the_published_zone_actually_narrows_the_bound(self):
        """Publishing is only useful if the pruner then uses it."""
        import tempfile

        from supertable.engine.engine_common import new_duckdb_connection
        from supertable import processing as _p

        reset_session_timezone()
        wide_lo, wide_hi = _widen_naive_timestamp_bounds(
            datetime(2026, 7, 15, 12, 0), datetime(2026, 7, 15, 12, 0))

        con = new_duckdb_connection(tempfile.mkdtemp())
        con.close()
        lo, hi = _widen_naive_timestamp_bounds(
            datetime(2026, 7, 15, 12, 0), datetime(2026, 7, 15, 12, 0))

        assert (hi - lo) <= (wide_hi - wide_lo), (
            "publishing a zone must never widen the bound"
        )
        if _p._session_timezone_name not in (None, "UTC"):
            assert (hi - lo) < (wide_hi - wide_lo), (
                f"zone {_p._session_timezone_name} should narrow 26h, got "
                f"{(hi - lo).total_seconds() / 3600:.1f}h"
            )

    def test_a_publish_failure_leaves_the_safe_bound(self):
        """Connection setup must not depend on this, and must not half-apply."""
        from unittest.mock import MagicMock

        from supertable.engine.engine_common import _publish_session_timezone

        reset_session_timezone()
        broken = MagicMock()
        broken.execute.side_effect = RuntimeError("no such setting")

        _publish_session_timezone(broken)          # must not raise

        lo, hi = _widen_naive_timestamp_bounds(
            datetime(2026, 7, 15, 12, 0), datetime(2026, 7, 15, 12, 0))
        assert (hi - lo) == _MAX_UTC_OFFSET_EAST + _MAX_UTC_OFFSET_WEST
