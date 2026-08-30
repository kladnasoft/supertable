from __future__ import annotations

import pytest

from supertable.utils.timer import Timer


def test_capture_timing_records_explicit_duration_without_resetting():
    timer = Timer(show_timing=False)
    timer._capture_start_time = 123.0

    timer.capture_timing("CONNECTION_SETUP", 0.01234567)

    assert timer.timings == [{"CONNECTION_SETUP": 0.012346}]
    assert timer._capture_start_time == 123.0


@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf")])
def test_capture_timing_rejects_invalid_duration(value):
    timer = Timer(show_timing=False)

    with pytest.raises(ValueError, match="finite and nonnegative"):
        timer.capture_timing("QUERY_EXECUTE", value)


def test_capture_aggregated_timing_keeps_many_fetch_attempts_constant_size():
    timer = Timer(show_timing=False)

    for _ in range(10_000):
        timer.capture_aggregated_timing("RESULT_FETCH", 0.000001)

    assert timer.timings == [{"RESULT_FETCH": 0.01}]
    assert timer.timing_occurrences("RESULT_FETCH") == 10_000
