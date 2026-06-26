"""Non-blocking performance benchmarks — measurement, never a correctness gate.

Skipped unless ``--run-perf``.  This suite NEVER asserts a latency threshold and
NEVER compares against the logical goldens: its output is written separately to
``tests/perf_results/`` so a slow (or fast) machine can never flip a
characterization result.  The golden / compatibility suites own correctness; this
owns observability.

Per result-scenario it records, into one JSON artifact:

  * corpus shape   — input file count, total input rows, tombstone key count;
  * read latency   — wall-clock of the full production read (min / median over
                     ``SUPERTABLE_PERF_REPEATS`` runs, default 3);
  * peak memory    — ``tracemalloc`` peak during a single read;
  * output size    — produced row count;
  * a real DuckDB ``EXPLAIN ANALYZE`` of the parquet *reflection scan* (the I/O
    layer the engine's reflection step runs — captured over the same files; the
    full view-chain plan would need engine instrumentation, noted as such).

Errors are recorded per-scenario and do NOT fail the run (benchmarks are
non-blocking); a summary line reports any so breakage is still visible.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

import pytest

from tests.characterization.current_reader import load_catalog, read_scenario
from tests.characterization.paths import GOLDEN_ROOT, PERF_ROOT
from tests.characterization.scenarios import ALL_SCENARIOS

_RESULT_SCENARIOS = [s for s in ALL_SCENARIOS if s.expect_error is None]


def _corpus_stats(scenario) -> dict:
    catalog_path = GOLDEN_ROOT / scenario.scenario_id / "input" / "catalog.json"
    catalog = load_catalog(catalog_path)
    table = catalog["tables"][scenario.target_table()]
    resources = table.get("resources", [])
    return {
        "input_files": len(resources),
        "input_rows_total": sum(int(r.get("rows", 0) or 0) for r in resources),
        "tombstone_deleted_keys": len(table.get("tombstones", {}).get("deleted_keys", [])),
    }


def _reflection_explain_analyze(scenario) -> str:
    """Real DuckDB ``EXPLAIN ANALYZE`` over the scenario's parquet reflection scan.

    Mirrors the engine's reflection (``parquet_scan([...], union_by_name=TRUE,
    HIVE_PARTITIONING=FALSE)``) at the I/O layer.  Best-effort: returns the error
    text on any failure so the benchmark stays non-blocking.
    """
    import duckdb

    catalog_path = GOLDEN_ROOT / scenario.scenario_id / "input" / "catalog.json"
    catalog = load_catalog(catalog_path)
    catalog_dir = catalog_path.parent
    table = catalog["tables"][scenario.target_table()]
    files = [str((catalog_dir / r["file"]).resolve()) for r in table.get("resources", [])]
    if not files:
        return "(no parquet files to scan)"
    files_sql = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    sql = (
        f"EXPLAIN ANALYZE SELECT * FROM parquet_scan([{files_sql}], "
        f"union_by_name=TRUE, HIVE_PARTITIONING=FALSE)"
    )
    con = duckdb.connect()
    try:
        rows = con.execute(sql).fetchall()
        return "\n".join(str(r[-1]) for r in rows)
    except Exception as exc:  # noqa: BLE001 - non-blocking
        return f"(explain failed: {type(exc).__name__}: {exc})"
    finally:
        con.close()


def _measure_one(scenario, repeats: int) -> dict:
    record = {"scenario_id": scenario.scenario_id, "category": scenario.category}
    record.update(_corpus_stats(scenario))

    timings = []
    output_rows = None
    error = None
    for _ in range(max(1, repeats)):
        t0 = time.perf_counter()
        try:
            res = read_scenario(scenario, GOLDEN_ROOT, engine="duckdb")
            timings.append(time.perf_counter() - t0)
            output_rows = res.row_count
        except Exception as exc:  # noqa: BLE001 - record, never fail the benchmark
            error = f"{type(exc).__name__}: {exc}"
            break

    if error is None:
        # Peak memory over a single fresh read.
        tracemalloc.start()
        try:
            read_scenario(scenario, GOLDEN_ROOT, engine="duckdb")
            _, peak = tracemalloc.get_traced_memory()
        except Exception as exc:  # noqa: BLE001
            peak = 0
            error = f"{type(exc).__name__}: {exc}"
        finally:
            tracemalloc.stop()

        record["output_row_count"] = output_rows
        record["read_seconds_min"] = round(min(timings), 6)
        record["read_seconds_median"] = round(statistics.median(timings), 6)
        record["peak_memory_bytes"] = int(peak)
        record["duckdb_reflection_explain_analyze"] = _reflection_explain_analyze(scenario)

    if error is not None:
        record["error"] = error
    return record


@pytest.mark.perf
def test_perf_benchmarks(request):
    if not request.config.getoption("--run-perf"):
        pytest.skip("performance benchmarks require --run-perf")

    repeats = int(os.environ.get("SUPERTABLE_PERF_REPEATS", "3"))
    records = [_measure_one(s, repeats) for s in _RESULT_SCENARIOS]

    artifact = {
        "generated_ms": int(time.time() * 1000),
        "engine": "duckdb",
        "python": sys.version.split()[0],
        "repeats": repeats,
        "scenario_count": len(records),
        "scenarios": records,
    }

    PERF_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    (PERF_ROOT / f"perf_{stamp}.json").write_bytes(
        json.dumps(artifact, ensure_ascii=False, indent=2).encode("utf-8")
    )
    # Stable pointer to the most recent run for easy diffing / CI upload.
    (PERF_ROOT / "perf_latest.json").write_bytes(
        json.dumps(artifact, ensure_ascii=False, indent=2).encode("utf-8")
    )

    errored = [r["scenario_id"] for r in records if "error" in r]
    measured = [r for r in records if "error" not in r]
    if measured:
        slowest = max(measured, key=lambda r: r["read_seconds_median"])
        print(
            f"\n[perf] measured {len(measured)}/{len(records)} scenarios; "
            f"slowest={slowest['scenario_id']} "
            f"({slowest['read_seconds_median'] * 1000:.1f} ms median); "
            f"artifact=tests/perf_results/perf_{stamp}.json"
        )
    if errored:
        # Non-blocking: surface but do not fail (golden suites own correctness).
        print(f"[perf] WARNING: {len(errored)} scenario(s) errored: {errored}")

    # The only assertion is that the benchmark harness itself ran end-to-end.
    assert len(records) == len(_RESULT_SCENARIOS)
