"""Unit tests for :func:`supertable.utils.helper.hourly_partition_subpath`.

The write path spreads each NEW (immutable, versioned) tombstone and stats
artifact under a Hive-style ``year=/month=/day=/hour=`` UTC subdir so that no
single folder accumulates hundreds of thousands of files under heavy writes
(which slows directory listing and per-file creation on object stores and real
filesystems alike).  These tests pin the exact string shape — zero-padding,
segment order, ``key=value`` Hive style — and the two calling conventions: an
explicit *ts* is used verbatim, and the default is the current UTC wall clock.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from supertable.utils.helper import hourly_partition_subpath

_MOD = "supertable.utils.helper"


def test_exact_format_utc():
    ts = datetime(2026, 7, 1, 17, 5, 42, tzinfo=timezone.utc)
    assert hourly_partition_subpath(ts) == "year=2026/month=07/day=01/hour=17"


def test_zero_pads_month_day_hour_single_digits():
    # 2026-01-02 03:00 — every field but the year is a single digit and must
    # be left-padded to two chars so lexical sort == chronological sort.
    ts = datetime(2026, 1, 2, 3, 0, 0, tzinfo=timezone.utc)
    assert hourly_partition_subpath(ts) == "year=2026/month=01/day=02/hour=03"


def test_midnight_hour_is_00_not_dropped():
    ts = datetime(2026, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert hourly_partition_subpath(ts) == "year=2026/month=12/day=31/hour=00"


def test_uses_provided_fields_verbatim_no_tz_conversion():
    # An explicit ts is read field-by-field; the function does NOT re-project to
    # UTC.  hour=17 stays hour=17 regardless of tzinfo.  The write path always
    # hands in UTC-derived times, so this simply pins the "verbatim" contract.
    ts = datetime(2026, 7, 1, 17, 5, tzinfo=timezone.utc)
    assert hourly_partition_subpath(ts).endswith("hour=17")


def test_default_is_current_utc():
    fixed = datetime(2030, 3, 9, 8, 15, tzinfo=timezone.utc)
    with patch(f"{_MOD}.datetime") as m:
        m.now.return_value = fixed
        out = hourly_partition_subpath()
    # Defaults to datetime.now(timezone.utc) — UTC basis, matching the
    # millisecond token generate_filename() stamps onto the same artifact.
    m.now.assert_called_once_with(timezone.utc)
    assert out == "year=2030/month=03/day=09/hour=08"


def test_shape_is_four_hive_segments_in_order():
    parts = hourly_partition_subpath(
        datetime(2026, 7, 1, 17, 0, tzinfo=timezone.utc)
    ).split("/")
    assert len(parts) == 4
    assert [p.split("=", 1)[0] for p in parts] == ["year", "month", "day", "hour"]
