from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from supertable.benchmarks.benchmark_random_write_trace import (
    REPORT_SCHEMA,
    build_trace,
    compare_reports,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "supertable" / "benchmarks" / "benchmark_random_write_trace.py"


def _replay(trace):
    state = {}
    for operation in trace["steps"]:
        if operation["kind"] == "delete":
            for row in operation["rows"]:
                state.pop(row[0], None)
        else:
            for row_id, value, category in operation["rows"]:
                state[row_id] = (value, category)
        assert len(state) == operation["expected_rows_after"]
    return state


def test_trace_is_deterministic_and_has_every_mutation_kind():
    left = build_trace(seed=91, initial_rows=20, operations=8, batch_rows=3)
    right = build_trace(seed=91, initial_rows=20, operations=8, batch_rows=3)
    changed = build_trace(seed=92, initial_rows=20, operations=8, batch_rows=3)

    assert left == right
    assert left["trace_digest"] != changed["trace_digest"]
    assert {step["kind"] for step in left["steps"]} >= {
        "initial_append", "append", "upsert", "delete",
    }
    state = _replay(left)
    values = [value for value, _category in state.values()]
    assert left["expected"]["row_count"] == len(state)
    assert left["expected"]["value_sum"] == sum(values)
    assert left["expected"]["value_avg_hex"] == float(
        sum(values) / len(values)
    ).hex()


def _report(*, oracle_match: bool, digest: str = "trace"):
    return {
        "schema": REPORT_SCHEMA,
        "provenance": {"label": "test"},
        "trace": {"digest": digest},
        "operations": [],
        "summary": {"all": {}, "production_profiler": {"timings": {}}},
        "final_read": {
            "actual_records_digest": "records",
            "correctness": {
                "oracle_match": oracle_match,
                "mismatches": [] if oracle_match else [{"field": "row_count"}],
            },
        },
    }


def test_compare_blocks_when_both_versions_agree_on_the_same_wrong_result():
    comparison = compare_reports(
        _report(oracle_match=False),
        _report(oracle_match=False),
    )

    assert comparison["gate_passed"] is False
    assert any("baseline disagrees" in blocker for blocker in comparison["blockers"])
    assert any("candidate disagrees" in blocker for blocker in comparison["blockers"])


def test_compare_requires_identical_trace():
    comparison = compare_reports(
        _report(oracle_match=True, digest="a"),
        _report(oracle_match=True, digest="b"),
    )

    assert comparison["gate_passed"] is False
    assert "baseline and candidate trace digests differ" in comparison["blockers"]


def test_tiny_real_datawriter_trace_captures_profiler_and_matches_oracle(tmp_path):
    output = tmp_path / "report.json"
    work_root = tmp_path / "work"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "run",
            "--package-root", str(REPO_ROOT),
            "--work-root", str(work_root),
            "--output", str(output),
            "--label", "pytest-head",
            "--revision", "HEAD",
            "--initial-rows", "12",
            "--operations", "3",
            "--batch-rows", "2",
            "--sample-interval-ms", "2",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["final_read"]["correctness"] == {
        "mismatches": [], "oracle_match": True,
    }
    assert report["final_read"]["actual_records_digest"] == (
        report["final_read"]["expected_records_digest"]
    )
    assert report["features"]["production_monitor_payload_capture"] is True
    assert [operation["kind"] for operation in report["operations"]] == [
        "initial_append", "append", "upsert", "delete",
    ]
    for operation in report["operations"]:
        assert operation["telemetry"]["wall_seconds"] > 0
        assert operation["telemetry"]["cpu_seconds"] > 0
        assert operation["telemetry"]["rss"]["max_bytes"] > 0
        assert operation["production_monitor"]["payload"]["timings"]
        assert operation["production_monitor"]["payload"]["counts"]
