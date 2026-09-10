"""The benchmark harness must be trustworthy before its numbers mean anything.

Two parts carry all the risk:

* the **seal**, which decides whether two versions behaved identically.  A seal
  that is too strict cries wolf on unchanged code; one that is too loose lets a
  behaviour change through while reporting a speed-up.
* the **comparison verdict**, which decides what counts as a regression.  It has
  to survive measurement noise, single-sample write phases, and the fact that a
  time-boxed phase always takes about as long as its time box.

These tests pin both.  They import nothing from ``supertable`` and touch no
storage, so they run anywhere.
"""
from __future__ import annotations

from benchmarks._harness import (
    seal_ordered_rows,
    seal_rows,
    summarize_ms,
)
from benchmarks.compare import compare, exit_code


COLUMNS = ["id", "name", "amount"]


def _digest(rows, columns=COLUMNS):
    return seal_rows(columns, rows)["digest"]


# ── seal: what it must ignore ────────────────────────────────────────

def test_unordered_seal_ignores_row_order():
    """Reads carry no ordering guarantee, so row order is not behaviour."""
    a = _digest([[1, "a", 10.0], [2, "b", 20.0]])
    b = _digest([[2, "b", 20.0], [1, "a", 10.0]])
    assert a == b


def test_unordered_seal_ignores_column_order():
    a = seal_rows(["id", "name"], [[1, "a"]])["digest"]
    b = seal_rows(["name", "id"], [["a", 1]])["digest"]
    assert a == b


def test_seal_treats_int_and_equal_float_alike():
    """The read path coerces ints to floats through numpy; that is not a
    behaviour change and must not break the seal."""
    assert _digest([[1, "a", 10]]) == _digest([[1, "a", 10.0]])


def test_seal_rounds_float_noise():
    """The last bits of a sum depend on aggregation order, not on behaviour."""
    assert _digest([[1, "a", 10.000000001]]) == _digest([[1, "a", 10.0]])


def test_seal_excludes_write_time_system_columns():
    """``__rowid__`` and ``__timestamp__`` are allocation- and write-time
    dependent, so including them would make every run differ from every other."""
    with_system = seal_rows(
        ["id", "__rowid__", "__timestamp__"], [[1, 999, "2026-01-01"]],
    )["digest"]
    without = seal_rows(["id"], [[1]])["digest"]
    assert with_system == without


# ── seal: what it must catch ─────────────────────────────────────────

def test_seal_detects_changed_value():
    assert _digest([[1, "a", 10.0]]) != _digest([[1, "a", 10.5]])


def test_seal_detects_missing_row():
    assert _digest([[1, "a", 1.0], [2, "b", 2.0]]) != _digest([[1, "a", 1.0]])


def test_seal_detects_duplicated_row():
    """A row returned twice is a real defect the row count alone could miss."""
    one = _digest([[1, "a", 1.0]])
    two = _digest([[1, "a", 1.0], [1, "a", 1.0]])
    assert one != two


def test_seal_distinguishes_null_from_empty_string():
    assert _digest([[1, None, 1.0]]) != _digest([[1, "", 1.0]])


def test_ordered_seal_is_sensitive_to_order():
    """ORDER BY ... LIMIT scenarios must detect a different top-N."""
    a = seal_ordered_rows(COLUMNS, [[1, "a", 1.0], [2, "b", 2.0]])["digest"]
    b = seal_ordered_rows(COLUMNS, [[2, "b", 2.0], [1, "a", 1.0]])["digest"]
    assert a != b


# ── timing summary ───────────────────────────────────────────────────

def test_summarize_reports_distribution_not_just_a_mean():
    summary = summarize_ms([10.0, 20.0, 30.0, 40.0, 100.0])
    assert summary["n"] == 5
    assert summary["min"] == 10.0
    assert summary["max"] == 100.0
    assert summary["p95"] >= summary["p50"]


def test_summarize_handles_single_sample():
    summary = summarize_ms([42.0])
    assert summary["n"] == 1 and summary["stdev"] == 0.0


# ── comparison verdicts ──────────────────────────────────────────────

def _run(scenarios, version="1.0.0"):
    return {
        "suite": "read", "profile": "local", "scale": "smoke",
        "environment": {
            "supertable_version": version, "profile": "local",
            "storage_type": "LOCAL", "processor": "x", "cpu_count": 8,
            "platform": "p", "git_describe": version, "git_dirty": False,
        },
        "dataset": {"fingerprint": "abc"},
        "scenarios": scenarios,
    }


def _scenario(sid, p50, *, stdev=0.0, n=5, seal="same", metrics=None,
              status="ok", expectations=None):
    return {
        "id": sid, "status": status,
        "timings_ms": {"p50": p50, "p95": p50, "stdev": stdev, "n": n},
        "seal": {"digest": seal, "row_count": 1},
        "metrics": metrics or {},
        "expectations": expectations or {},
    }


def test_seal_change_is_reported_as_correctness_not_speed():
    """A scenario that changed behaviour must not be given a timing verdict."""
    base = _run([_scenario("s", 100.0, seal="aaa")])
    cand = _run([_scenario("s", 50.0, seal="bbb")], version="2.0.0")
    report = compare(base, cand)

    assert len(report["correctness_drift"]) == 1
    assert report["correctness_drift"][0]["seal_changed"] is True
    assert report["performance"] == []           # not judged on speed
    assert exit_code(report) == 2


def test_new_expectation_failure_is_correctness_drift():
    base = _run([_scenario("s", 100.0, expectations={"k": {"ok": True}})])
    cand = _run([_scenario("s", 100.0, expectations={"k": {"ok": False}})])
    report = compare(base, cand)
    assert report["correctness_drift"][0]["new_expectation_failures"] == ["k"]
    assert exit_code(report) == 2


def test_real_slowdown_is_reported():
    base = _run([_scenario("s", 100.0, stdev=1.0)])
    cand = _run([_scenario("s", 200.0, stdev=1.0)])
    report = compare(base, cand)
    assert report["performance"][0]["verdict"] == "regressed"
    assert exit_code(report) == 1


def test_noisy_baseline_suppresses_a_false_regression():
    """A swing inside the baseline's own spread is not a regression."""
    base = _run([_scenario("s", 100.0, stdev=30.0)])   # 2*stdev = 60% band
    cand = _run([_scenario("s", 140.0, stdev=30.0)])   # +40%, inside the band
    report = compare(base, cand)
    assert report["performance"][0]["verdict"] == "unchanged"
    assert exit_code(report) == 0


def test_single_sample_scenarios_get_a_wider_bar():
    """Write phases run once, so there is no spread to reason from."""
    base = _run([_scenario("s", 100.0, stdev=0.0, n=1)])
    cand = _run([_scenario("s", 115.0, stdev=0.0, n=1)])   # +15%, under 25%
    report = compare(base, cand)
    assert report["performance"][0]["single_sample"] is True
    assert report["performance"][0]["verdict"] == "unchanged"


def test_time_boxed_phase_is_judged_on_throughput_not_wall_time():
    """A 60s phase always takes ~60s; what changed is how much fit inside."""
    base = _run([_scenario("s", 60_000.0, n=1,
                           metrics={"total_rows_per_second": 1000.0})])
    cand = _run([_scenario("s", 90_000.0, n=1,          # wall time way up ...
                           metrics={"total_rows_per_second": 1010.0})])  # ... work steady
    report = compare(base, cand)
    entry = report["performance"][0]
    assert entry["judged_on"] == "total_rows_per_second"
    assert entry["verdict"] == "unchanged"


def test_throughput_collapse_is_a_regression():
    base = _run([_scenario("s", 60_000.0, n=1,
                           metrics={"total_rows_per_second": 1000.0})])
    cand = _run([_scenario("s", 60_000.0, n=1,
                           metrics={"total_rows_per_second": 400.0})])
    report = compare(base, cand)
    assert report["performance"][0]["verdict"] == "regressed"
    assert exit_code(report) == 1


def test_scenario_that_starts_erroring_is_correctness_drift():
    base = _run([_scenario("s", 100.0)])
    cand = _run([_scenario("s", 100.0, status="error")])
    report = compare(base, cand)
    assert report["correctness_drift"][0]["candidate_status"] == "error"
    assert exit_code(report) == 2


def test_incomparable_environments_are_flagged():
    """Timings across different backends are noise, not signal."""
    base = _run([_scenario("s", 100.0)])
    cand = _run([_scenario("s", 100.0)])
    cand["environment"]["storage_type"] = "MINIO"
    cand["environment"]["profile"] = "minio"
    report = compare(base, cand)
    assert any("storage" in w for w in report["comparability_warnings"])


def test_added_and_missing_scenarios_are_listed():
    base = _run([_scenario("old", 100.0)])
    cand = _run([_scenario("new", 100.0)])
    report = compare(base, cand)
    assert report["missing_scenarios"] == ["old"]
    assert report["added_scenarios"] == ["new"]
