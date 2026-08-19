from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
from pathlib import Path

import pytest

from supertable.engine.benchmarks.corpus import (
    CorpusSpec,
    build_workloads,
    generated_metric_statistics,
    normalize_tiers,
    normalize_workloads,
    plan_workload,
    prepare_corpus,
    repeated_manifest_paths,
)
from supertable.engine.benchmarks.islanddb import build_parser, selected_tiers
from supertable.engine.benchmarks.runner import (
    BenchmarkParityError,
    BenchmarkWorkerError,
    ComparisonConfig,
    _cgroup_v2_memory_telemetry,
    _counter_delta,
    _duckdb_memory_limit_text,
    _execute_one,
    _profile_metrics,
    _proc_io_counters,
    _validate_cold_physical_read,
    assert_exact_parity,
    assert_independent_oracle,
    canonical_frame,
    compare_manifest,
    islanddb_available,
    result_digest,
    run_isolated_worker,
    summarize_series,
)


def test_profile_metrics_preserves_corrected_island_telemetry(tmp_path):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps({
        "engine": "islanddb",
        "estimated_candidate_row_groups": 16,
        "estimated_candidate_row_groups_complete": True,
        "planned_row_groups": 38,
        "planned_row_groups_complete": True,
        "observed_row_groups": None,
        "observed_row_groups_measured": False,
        "execution_outcome": "completed",
        "result_complete": True,
        "physical_read_scope": "linux_proc_self_io_block_read_delta",
        "rss_baseline_bytes": 100,
        "rss_peak_bytes": 250,
        "rss_final_bytes": 175,
        "rss_peak_delta_bytes": 150,
        "rss_retained_delta_bytes": 75,
        "rss_measured": True,
        "phase_timings_ms": {"producer_active_ms": 12.5},
        "profile_persist_ms": None,
        "profile_persist_ms_measured": False,
    }), encoding="utf-8")

    metrics = _profile_metrics(path)
    assert metrics["estimated_candidate_row_groups"] == 16
    assert metrics["planned_row_groups"] == 38
    assert metrics["observed_row_groups"] is None
    assert metrics["observed_row_groups_measured"] is False
    assert metrics["result_complete"] is True
    assert metrics["rss_peak_bytes"] == 250
    assert metrics["phase_timings_ms"]["producer_active_ms"] == {
        "$float": float(12.5).hex(),
    }
    assert metrics["profile_persist_ms"] is None


def _tiny_spec(*, seed: int = 17) -> CorpusSpec:
    return CorpusSpec(
        tier="smoke",
        target_bytes=96 * 1024,
        seed=seed,
        payload_columns=2,
        payload_width=16,
        batch_rows=128,
        row_group_target_bytes=8 * 1024,
        shard_target_bytes=24 * 1024,
    )


@pytest.fixture()
def tiny_manifest(tmp_path):
    return prepare_corpus(tmp_path / "corpus", _tiny_spec(), check_disk=False)


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_tier_aliases_and_large_opt_in():
    assert normalize_tiers(
        ["KB,64MiB", "100MB", "1GB", "10gib", "50GB"]
    ) == [
        "kb",
        "mb",
        "100mib",
        "1gib",
        "10gib",
        "50gib",
    ]

    parser = build_parser()
    args = parser.parse_args(["--100mb"])
    assert selected_tiers(args) == ["100mib"]
    assert CorpusSpec.for_tier("100MB").target_bytes == 100 * 1024**2

    args = parser.parse_args(["--1gb"])
    with pytest.raises(ValueError, match="opt-in"):
        selected_tiers(args)

    args = parser.parse_args(["--1gb", "--10gb", "--50gb", "--allow-large"])
    assert selected_tiers(args) == ["1gib", "10gib", "50gib"]


def test_generated_metric_statistics_matches_formula_and_repetition():
    rows = 4_097
    values = [
        (row_id * 48_271 + 17) % 1_000_003
        for row_id in range(rows)
    ]
    oracle = generated_metric_statistics(rows, source_repeat=3)

    assert oracle["columns"] == [
        "row_count",
        "metric_non_null_count",
        "metric_null_count",
        "metric_sum",
        "metric_avg",
        "metric_min",
        "metric_max",
    ]
    assert oracle["row"] == [
        rows * 3,
        rows * 3,
        0,
        sum(values) * 3,
        sum(values) / rows,
        min(values),
        max(values),
    ]

    full_period = generated_metric_statistics(1_000_003)
    assert full_period["row"][3] == 1_000_003 * 1_000_002 // 2
    assert full_period["row"][-2:] == [0, 1_000_002]

    with pytest.raises(ValueError, match="total_rows"):
        generated_metric_statistics(0)
    with pytest.raises(ValueError, match="source_repeat"):
        generated_metric_statistics(1, source_repeat=0)


def test_aggregate_stats_plan_has_independent_generator_oracle(tiny_manifest):
    workload = build_workloads(tiny_manifest["total_rows"])["aggregate_stats"]
    plan = plan_workload(tiny_manifest, workload)

    assert normalize_workloads(["aggregate_stats"]) == ["aggregate_stats"]
    assert plan["required_columns"] == ["metric"]
    assert "COUNT(*) AS row_count" in plan["sql"]
    assert "SUM(metric) AS metric_sum" in plan["sql"]
    assert "MIN(metric) AS metric_min" in plan["sql"]
    assert "MAX(metric) AS metric_max" in plan["sql"]
    assert plan["result_postprocess"] == "generated_metric_formula_v1"
    assert plan["independent_oracle"] == generated_metric_statistics(
        tiny_manifest["total_rows"]
    )


def test_full_scan_projects_every_public_column(tiny_manifest):
    workload = build_workloads(
        tiny_manifest["total_rows"], payload_columns=2,
    )["full_scan"]
    plan = plan_workload(tiny_manifest, workload)

    assert plan["required_columns"] == [
        "id", "event_ts", "metric", "dimension", "payload_00", "payload_01",
    ]
    assert plan["files_after_prune"] == plan["files_before_prune"]
    assert plan["row_groups_pushdown_eligible"] == plan["row_groups_after_file_prune"]
    assert plan["estimated_pushdown_bytes"] == plan["estimated_reflection_bytes"]
    assert plan["projected_source_fraction"] >= 0.5
    assert plan["decoded_row_width"] == 8 + 8 + 8 + 4 + 2 * 16
    assert plan["estimated_decoded_bytes"] == (
        plan["candidate_rows"] * plan["decoded_row_width"]
    )
    assert plan["decoded_estimate_complete"] is True
    assert all(
        f"MAX({column})" in plan["sql"] for column in plan["required_columns"]
    )
    assert "COUNT(*) AS row_count" in plan["sql"]


def test_spill_group_projects_all_columns_and_requests_island_streaming(
    tiny_manifest,
):
    workload = build_workloads(
        tiny_manifest["total_rows"], payload_columns=2,
    )["spill_group"]
    plan = plan_workload(tiny_manifest, workload)

    assert normalize_workloads(["spill_group"]) == ["spill_group"]
    assert plan["required_columns"] == [
        "id", "event_ts", "metric", "dimension", "payload_00", "payload_01",
    ]
    assert plan["files_after_prune"] == plan["files_before_prune"]
    assert plan["estimated_pushdown_bytes"] == plan["estimated_reflection_bytes"]
    assert plan["island_streaming_result"] is True
    assert plan["sql"].startswith("SELECT dimension, COUNT(id) AS id_count")
    assert "COUNT(event_ts) AS event_ts_count" in plan["sql"]
    assert "COUNT(metric) AS metric_count" in plan["sql"]
    assert "COUNT(payload_00) AS payload_00_count" in plan["sql"]
    assert "COUNT(payload_01) AS payload_01_count" in plan["sql"]
    assert "COUNT(dimension)" not in plan["sql"]
    assert plan["sql"].endswith(
        "FROM events GROUP BY dimension ORDER BY dimension"
    )


def test_spill_group_dispatches_only_island_through_bounded_stream(
    tiny_manifest,
):
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    from supertable.engine.engine_enum import Engine

    plan = plan_workload(
        tiny_manifest,
        build_workloads(
            tiny_manifest["total_rows"], payload_columns=2,
        )["spill_group"],
    )

    class FakeStream:
        def __init__(self):
            self.max_bytes = None
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            self.closed = True

        def collect_table(self, *, max_bytes):
            self.max_bytes = max_bytes
            return pa.table({"dimension": pa.array([0], type=pa.int32())})

    class FakeExecutor:
        def __init__(self):
            self.stream = FakeStream()
            self.streaming_calls = 0
            self.materialized_calls = 0

        def execute_stream(self, **kwargs):
            self.streaming_calls += 1
            return self.stream, "islanddb"

        def execute(self, **kwargs):
            self.materialized_calls += 1
            return pd.DataFrame({"dimension": pd.Series([0], dtype="int32")}), "duckdb"

    island_executor = FakeExecutor()
    island = _execute_one(island_executor, Engine.ISLANDDB, plan, 0)
    assert island_executor.streaming_calls == 1
    assert island_executor.materialized_calls == 0
    assert island_executor.stream.closed is True
    assert 0 < island_executor.stream.max_bytes <= 64 * 1024**2
    assert island["result_mode"] == "arrow_stream"

    duck_executor = FakeExecutor()
    duck = _execute_one(duck_executor, Engine.DUCKDB, plan, 0)
    assert duck_executor.streaming_calls == 0
    assert duck_executor.materialized_calls == 1
    assert duck["result_mode"] == "pandas"


def test_source_repeat_is_explicit_and_preserves_unique_backing_metrics(tiny_manifest):
    workload = build_workloads(
        tiny_manifest["total_rows"], payload_columns=2,
    )["full_scan"]
    once = plan_workload(tiny_manifest, workload)
    with repeated_manifest_paths(tiny_manifest, 5) as repeated_manifest:
        repeated_paths = [
            str(entry["path"]) for entry in repeated_manifest["files"]
        ]
        assert len(set(repeated_paths)) == len(repeated_paths)
        repeated = plan_workload(repeated_manifest, workload)
        original = Path(tiny_manifest["manifest_path"]).parent / str(
            tiny_manifest["files"][0]["path"]
        )
        alias = Path(repeated_manifest["files"][len(tiny_manifest["files"])]["path"])
        assert original.stat().st_ino == alias.stat().st_ino
        alias_root = alias.parent.parent

    assert repeated["source_repeat"] == 5
    assert repeated["unique_source_bytes"] == once["source_bytes"]
    assert repeated["source_bytes"] == once["source_bytes"] * 5
    assert repeated["unique_estimated_pushdown_bytes"] == once[
        "estimated_pushdown_bytes"
    ]
    assert repeated["estimated_pushdown_bytes"] == once[
        "estimated_pushdown_bytes"
    ] * 5
    assert repeated["files_before_prune"] == once["files_before_prune"] * 5
    assert repeated["unique_files_before_prune"] == once["files_before_prune"]
    assert repeated["candidate_rows"] == once["candidate_rows"] * 5
    assert repeated["source_repeat_mode"] == "distinct_hardlink_aliases"
    assert not alias_root.exists()

    with pytest.raises(ValueError, match="source_repeat"):
        with repeated_manifest_paths(tiny_manifest, 0):
            pass


def test_memory_limit_rendering_is_duckdb_valid_and_exact_for_8gib():
    assert _duckdb_memory_limit_text(8 * 1024**3) == "8GiB"
    assert _duckdb_memory_limit_text(768 * 1024**2) == "768MiB"
    with pytest.raises(ValueError, match="positive"):
        _duckdb_memory_limit_text(0)

    with pytest.raises(ValueError, match="source_repeat"):
        ComparisonConfig(source_repeat=0)


def test_cgroup_v2_memory_telemetry_reads_own_contained_counters(tmp_path):
    root = tmp_path / "cgroup"
    current = root / "bench.scope"
    current.mkdir(parents=True)
    proc = tmp_path / "self.cgroup"
    proc.write_text("0::/bench.scope\n", encoding="utf-8")
    values = {
        "memory.current": "1234\n",
        "memory.peak": "5678\n",
        "memory.max": "max\n",
        "memory.swap.current": "12\n",
        "memory.swap.peak": "34\n",
        "memory.swap.max": "0\n",
        "memory.events": "low 1\nhigh 2\nmax 3\noom 4\noom_kill 5\n",
        "memory.stat": "anon 100\nfile 200\n",
        "memory.pressure": "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        "io.stat": "8:0 rbytes=10 wbytes=20 rios=1 wios=2\n",
    }
    for name, value in values.items():
        (current / name).write_text(value, encoding="utf-8")

    telemetry = _cgroup_v2_memory_telemetry(
        proc_cgroup=proc, cgroup_root=root,
    )

    assert telemetry["available"] is True
    assert telemetry["path"] == "/bench.scope"
    assert telemetry["memory_current_bytes"] == 1234
    assert telemetry["memory_peak_bytes"] == 5678
    assert telemetry["memory_max_bytes"] is None
    assert telemetry["memory_max_raw"] == "max"
    assert telemetry["swap_max_bytes"] == 0
    assert telemetry["memory_events"]["oom_kill"] == 5
    assert telemetry["memory_stat"] == {"anon": 100, "file": 200}
    assert telemetry["memory_pressure"].startswith("some ")
    assert telemetry["io_stat"].startswith("8:0 ")


def test_cgroup_v2_memory_telemetry_rejects_path_escape(tmp_path):
    root = tmp_path / "cgroup"
    root.mkdir()
    (tmp_path / "outside").mkdir()
    proc = tmp_path / "self.cgroup"
    proc.write_text("0::/../outside\n", encoding="utf-8")

    telemetry = _cgroup_v2_memory_telemetry(
        proc_cgroup=proc, cgroup_root=root,
    )

    assert telemetry["available"] is False
    assert telemetry["reason"].startswith("cgroup_path_invalid:")


def test_process_io_counters_and_non_negative_delta(tmp_path):
    counters = tmp_path / "io"
    counters.write_text(
        "rchar: 10\nwchar: 20\nread_bytes: 30\ninvalid: x\n",
        encoding="ascii",
    )
    assert _proc_io_counters(counters) == {
        "rchar": 10, "wchar": 20, "read_bytes": 30,
    }
    assert _counter_delta(
        {"rchar": 10, "read_bytes": 40},
        {"rchar": 25, "read_bytes": 30},
    ) == {"rchar": 15, "read_bytes": 0}


def test_physical_read_gate_uses_unique_backing_for_repeated_sources():
    verification = _validate_cold_physical_read(
        engine_name="duckdb",
        plan={
            "estimated_pushdown_bytes": 500,
            "unique_estimated_pushdown_bytes": 100,
        },
        cold_advice={"supported": True, "errors": 0},
        sample={"process_io_delta": {"read_bytes": 99}},
        minimum_fraction=0.99,
    )
    assert verification["passed"] is True
    assert verification["expected_projected_bytes"] == 100

    with pytest.raises(BenchmarkWorkerError, match="refusing"):
        _validate_cold_physical_read(
            engine_name="islanddb",
            plan={"unique_estimated_pushdown_bytes": 100},
            cold_advice={"supported": True, "errors": 0},
            sample={"process_io_delta": {"read_bytes": 98}},
            minimum_fraction=0.99,
        )


def test_corpus_is_byte_deterministic_and_reusable(tmp_path):
    spec = _tiny_spec(seed=91)
    first = prepare_corpus(tmp_path / "a", spec, check_disk=False)
    second = prepare_corpus(tmp_path / "b", spec, check_disk=False)

    assert first["actual_source_bytes"] >= spec.target_bytes
    assert first["total_rows"] == second["total_rows"]
    assert len(first["files"]) >= 2
    assert [entry["bytes"] for entry in first["files"]] == [
        entry["bytes"] for entry in second["files"]
    ]
    first_root = Path(first["manifest_path"]).parent
    second_root = Path(second["manifest_path"]).parent
    assert [
        _file_digest(first_root / entry["path"]) for entry in first["files"]
    ] == [
        _file_digest(second_root / entry["path"]) for entry in second["files"]
    ]

    reused = prepare_corpus(tmp_path / "a", spec, check_disk=False)
    assert reused["reused"] is True
    assert reused["actual_source_bytes"] == first["actual_source_bytes"]


def test_workload_plan_reports_source_pruning_and_pushdown_estimates(tiny_manifest):
    workloads = build_workloads(tiny_manifest["total_rows"])
    point = plan_workload(tiny_manifest, workloads["point"])
    projection = plan_workload(tiny_manifest, workloads["projection"])
    no_match = plan_workload(tiny_manifest, workloads["no_match"])
    five_columns = plan_workload(tiny_manifest, workloads["range_1pct_5cols"])

    assert point["source_bytes"] == tiny_manifest["actual_source_bytes"]
    assert 0 < point["candidate_source_bytes"] < point["source_bytes"]
    assert 0 < point["estimated_pushdown_bytes"] <= point["estimated_reflection_bytes"]
    assert point["files_pruned"] > 0

    assert projection["candidate_source_bytes"] == projection["source_bytes"]
    assert projection["estimated_pushdown_bytes"] == projection["estimated_reflection_bytes"]
    assert projection["files_pruned"] == 0

    assert no_match["candidate_source_bytes"] == no_match["source_bytes"]
    assert no_match["estimated_reflection_bytes"] > 0
    assert no_match["estimated_pushdown_bytes"] == 0
    assert len(five_columns["required_columns"]) == 5
    assert five_columns["estimated_pushdown_bytes"] > 0


def test_exact_parity_includes_dtype_and_value():
    pd = pytest.importorskip("pandas")
    duck_frame = pd.DataFrame({"n": pd.Series([1], dtype="int64")})
    same_frame = pd.DataFrame({"n": pd.Series([1], dtype="int64")})
    wrong_dtype = pd.DataFrame({"n": pd.Series([1], dtype="int32")})

    duck = canonical_frame(duck_frame)
    same = canonical_frame(same_frame)
    wrong = canonical_frame(wrong_dtype)
    duck_result = {"result": duck, "result_digest": result_digest(duck)}
    same_result = {"result": same, "result_digest": result_digest(same)}
    wrong_result = {"result": wrong, "result_digest": result_digest(wrong)}

    assert assert_exact_parity(duck_result, same_result, label="smoke") == result_digest(duck)
    with pytest.raises(BenchmarkParityError, match="differs from DuckDB"):
        assert_exact_parity(duck_result, wrong_result, label="smoke")


def test_independent_oracle_rejects_two_engines_agreeing_on_wrong_result():
    pd = pytest.importorskip("pandas")
    oracle = generated_metric_statistics(37)
    frame = pd.DataFrame({
        column: pd.Series([value], dtype=dtype)
        for column, dtype, value in zip(
            oracle["columns"], oracle["dtypes"], oracle["row"], strict=True,
        )
    })
    correct = canonical_frame(frame)
    wrong = copy.deepcopy(correct)
    wrong["rows"][0][3] += 1
    agreed_wrong = {
        "result": wrong,
        "result_digest": result_digest(wrong),
    }

    with pytest.raises(
        BenchmarkParityError,
        match=r"wrong_engines=duckdb,islanddb",
    ):
        assert_independent_oracle(
            {"duckdb": agreed_wrong, "islanddb": agreed_wrong},
            {"independent_oracle": oracle},
            label="smoke/aggregate_stats",
        )

    correct_result = {
        "result": correct,
        "result_digest": result_digest(correct),
    }
    evidence = assert_independent_oracle(
        {"duckdb": correct_result, "islanddb": correct_result},
        {"independent_oracle": oracle},
        label="smoke/aggregate_stats",
    )
    assert evidence is not None
    assert evidence["matched_engines"] == ["duckdb", "islanddb"]
    assert evidence["expected_result_digest"] == result_digest(correct)


def test_summarize_series_reports_warm_resource_distributions():
    series = {
        "samples": [
            {
                "temperature": "cold",
                "wall_seconds": 4.0,
                "cpu_seconds": 2.0,
                "rss_peak_bytes": 90,
                "rss_peak_delta_bytes": 9,
                "process_io_delta": {"read_bytes": 50},
            },
            {
                "temperature": "warm",
                "wall_seconds": 1.0,
                "cpu_seconds": 0.5,
                "rss_peak_bytes": 100,
                "rss_peak_delta_bytes": 10,
                "process_io_delta": {"read_bytes": 100, "write_bytes": 5},
            },
            {
                "temperature": "warm",
                "wall_seconds": 2.0,
                "cpu_seconds": 2.0,
                "rss_peak_bytes": 120,
                "rss_peak_delta_bytes": 20,
                "process_io_delta": {"read_bytes": 200},
            },
            {
                "temperature": "warm",
                "wall_seconds": 3.0,
                "cpu_seconds": 4.5,
                "rss_peak_bytes": 140,
                "rss_peak_delta_bytes": 30,
                "process_io_delta": {"read_bytes": 400, "write_bytes": 15},
            },
        ]
    }

    summary = summarize_series(series)

    assert summary["cold_cpu_seconds"] == 2.0
    assert summary["cold_mean_cpu_cores"] == 0.5
    assert summary["warm_samples"] == 3
    assert summary["warm_wall_seconds_min"] == 1.0
    assert summary["warm_wall_seconds_mean"] == 2.0
    assert summary["warm_wall_seconds_median"] == 2.0
    assert summary["warm_wall_seconds_max"] == 3.0
    assert summary["warm_wall_seconds_p95"] == pytest.approx(2.9)
    assert summary["warm_wall_seconds_stddev"] == pytest.approx(math.sqrt(2 / 3))
    assert summary["warm_wall_seconds_cv"] == pytest.approx(math.sqrt(2 / 3) / 2)
    assert summary["warm_cpu_seconds_mean"] == pytest.approx(7 / 3)
    assert summary["warm_mean_cpu_cores_mean"] == 1.0
    assert summary["warm_rss_peak_bytes_mean"] == 120.0
    assert summary["warm_rss_peak_delta_bytes_max"] == 30.0
    read_io = summary["warm_process_io_delta_summary"]["read_bytes"]
    assert read_io["samples"] == 3
    assert read_io["mean"] == pytest.approx(700 / 3)
    write_io = summary["warm_process_io_delta_summary"]["write_bytes"]
    assert write_io["samples"] == 2
    assert write_io["min"] == 5.0
    assert write_io["max"] == 15.0


def test_compare_stops_before_timing_on_parity_failure(tiny_manifest, tmp_path):
    calls = []

    def fake_worker(request, **kwargs):
        calls.append((request["purpose"], request["engine"]))
        value = 1 if request["engine"] == "duckdb" else 2
        canonical = {"columns": ["value"], "dtypes": ["int64"], "rows": [[value]]}
        return {
            "result": canonical,
            "result_digest": result_digest(canonical),
            "samples": [],
        }

    with pytest.raises(BenchmarkParityError):
        compare_manifest(
            tiny_manifest,
            cache_root=tmp_path / "cache",
            home_root=tmp_path / "home",
            config=ComparisonConfig(warm_repeats=1, workloads=("point",)),
            worker_runner=fake_worker,
        )
    assert calls == [("parity", "duckdb"), ("parity", "islanddb")]


def test_compare_stops_when_both_engines_agree_against_independent_oracle(
    tiny_manifest,
    tmp_path,
):
    pd = pytest.importorskip("pandas")
    calls = []

    def fake_worker(request, **kwargs):
        calls.append((request["purpose"], request["engine"]))
        oracle = request["plan"]["independent_oracle"]
        frame = pd.DataFrame({
            column: pd.Series([value], dtype=dtype)
            for column, dtype, value in zip(
                oracle["columns"], oracle["dtypes"], oracle["row"], strict=True,
            )
        })
        canonical = canonical_frame(frame)
        canonical["rows"][0][3] += 1
        return {
            "result": canonical,
            "result_digest": result_digest(canonical),
            "samples": [],
        }

    with pytest.raises(
        BenchmarkParityError,
        match=r"wrong_engines=duckdb,islanddb",
    ):
        compare_manifest(
            tiny_manifest,
            cache_root=tmp_path / "cache",
            home_root=tmp_path / "home",
            config=ComparisonConfig(
                warm_repeats=1,
                workloads=("aggregate_stats",),
            ),
            worker_runner=fake_worker,
        )
    assert calls == [("parity", "duckdb"), ("parity", "islanddb")]


def test_explicit_duckdb_production_worker_reports_cold_and_warm(tiny_manifest, tmp_path):
    plan = plan_workload(
        tiny_manifest, build_workloads(tiny_manifest["total_rows"])["point"]
    )
    result = run_isolated_worker(
        {
            "purpose": "smoke",
            "engine": "duckdb",
            "plan": plan,
            "warm_repeats": 1,
            "cold_mode": "process",
            "memory_limit_bytes": 256 * 1024**2,
            "threads": 2,
            "disable_caches": True,
        },
        cache_dir=tmp_path / "cache",
        home_dir=tmp_path / "home",
        timeout_seconds=120,
    )

    assert result["engine_value"] == "duckdb"
    assert result["execution_context"]["duckdb_threads"] == 2
    assert result["execution_context"]["polars_thread_pool_size"] == 2
    assert result["execution_context"]["configured_threads"] == 2
    assert result["execution_context"]["caches_disabled"] is True
    assert result["execution_context"]["duckdb_http_metadata_cache_env"] == "false"
    assert result["execution_context"]["duckdb_enable_external_file_cache"] is False
    assert result["execution_context"]["duckdb_temp_directory"]
    assert result["execution_context"]["configured_memory_limit_bytes"] == 256 * 1024**2
    assert result["execution_context"]["duckdb_memory_limit_env"] == "256MiB"
    assert result["execution_context"]["island_max_memory_bytes_env"] == str(
        256 * 1024**2
    )
    assert "256" in result["execution_context"]["duckdb_memory_limit"]
    assert result["execution_context"]["island_memory_limit_bytes"] <= 256 * 1024**2
    assert result["execution_context"]["island_query_memory_fraction"] == 1.0
    assert result["execution_context"]["island_global_memory_fraction"] == 1.0
    assert "available" in result["execution_context"]["cgroup_v2"]
    event_delta = result["execution_context"]["cgroup_memory_event_delta"] or {}
    assert event_delta.get("oom", 0) == 0
    assert event_delta.get("oom_kill", 0) == 0
    assert result["execution_context"]["polars_thread_pool_size"] >= 1
    assert [sample["temperature"] for sample in result["samples"]] == ["cold", "warm"]
    assert all(sample["result_digest"] == result["result_digest"] for sample in result["samples"])
    assert result["samples"][0]["wall_seconds"] > 0
    profile = result["samples"][0]["engine_profile"]
    # Production hardening may suppress raw DuckDB profiles because physical
    # filenames can contain bearer URLs. Retain metrics when safely emitted,
    # while process/cgroup telemetry remains mandatory either way.
    assert not profile or "total_bytes_read" in profile


@pytest.mark.skipif(not islanddb_available(), reason="Engine.ISLANDDB is not implemented")
def test_explicit_islanddb_matches_duckdb_in_production_workers(tiny_manifest, tmp_path):
    plan = plan_workload(
        tiny_manifest, build_workloads(tiny_manifest["total_rows"])["range_1pct"]
    )

    def run(engine: str):
        return run_isolated_worker(
            {
                "purpose": "smoke-parity",
                "engine": engine,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
            },
            cache_dir=tmp_path / "shared-cache",
            home_dir=tmp_path / "home" / engine,
            timeout_seconds=120,
        )

    duck = run("duckdb")
    island = run("islanddb")
    assert assert_exact_parity(duck, island, label="production-smoke") == duck["result_digest"]
    assert island["samples"][0]["engine"] == "islanddb"


@pytest.mark.skipif(not islanddb_available(), reason="Engine.ISLANDDB is not implemented")
def test_aggregate_stats_matches_both_engines_and_independent_oracle(
    tiny_manifest,
    tmp_path,
):
    plan = plan_workload(
        tiny_manifest,
        build_workloads(
            tiny_manifest["total_rows"], payload_columns=2,
        )["aggregate_stats"],
    )

    def run(engine: str):
        return run_isolated_worker(
            {
                "purpose": "aggregate-stats-smoke-parity",
                "engine": engine,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
                "memory_limit_bytes": 256 * 1024**2,
                "threads": 2,
                "disable_caches": True,
            },
            cache_dir=tmp_path / "shared-cache" / engine,
            home_dir=tmp_path / "home" / engine,
            timeout_seconds=120,
        )

    results = {engine: run(engine) for engine in ("duckdb", "islanddb")}
    evidence = assert_independent_oracle(
        results,
        plan,
        label="smoke/aggregate_stats",
    )
    assert evidence is not None
    digest = assert_exact_parity(
        results["duckdb"],
        results["islanddb"],
        label="smoke/aggregate_stats",
    )
    assert digest == evidence["expected_result_digest"]


@pytest.mark.skipif(not islanddb_available(), reason="Engine.ISLANDDB is not implemented")
def test_full_scan_matches_in_production_workers(tiny_manifest, tmp_path):
    workload = build_workloads(
        tiny_manifest["total_rows"], payload_columns=2,
    )["full_scan"]

    def run(engine: str):
        return run_isolated_worker(
            {
                "purpose": "full-scan-smoke-parity",
                "engine": engine,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
                "memory_limit_bytes": 256 * 1024**2,
            },
            cache_dir=tmp_path / "shared-cache",
            home_dir=tmp_path / "home" / engine,
            timeout_seconds=120,
        )

    with repeated_manifest_paths(tiny_manifest, 2) as repeated_manifest:
        plan = plan_workload(repeated_manifest, workload)
        duck = run("duckdb")
        island = run("islanddb")
    assert assert_exact_parity(duck, island, label="full-scan-smoke") == duck[
        "result_digest"
    ]
    assert len(duck["result"]["rows"]) == 1
    assert len(duck["result"]["rows"][0]) == 7
    assert duck["result"]["rows"][0][0] == tiny_manifest["total_rows"] * 2


@pytest.mark.skipif(not islanddb_available(), reason="Engine.ISLANDDB is not implemented")
def test_spill_group_streaming_matches_duckdb_in_production_workers(
    tiny_manifest, tmp_path,
):
    plan = plan_workload(
        tiny_manifest,
        build_workloads(
            tiny_manifest["total_rows"], payload_columns=2,
        )["spill_group"],
    )

    def run(engine: str):
        return run_isolated_worker(
            {
                "purpose": "spill-group-smoke-parity",
                "engine": engine,
                "plan": plan,
                "warm_repeats": 0,
                "cold_mode": "process",
                "memory_limit_bytes": 256 * 1024**2,
                "threads": 2,
            },
            cache_dir=tmp_path / "shared-cache",
            home_dir=tmp_path / "home" / engine,
            timeout_seconds=120,
        )

    duck = run("duckdb")
    island = run("islanddb")

    assert assert_exact_parity(duck, island, label="spill-group-smoke") == duck[
        "result_digest"
    ]
    assert duck["samples"][0]["result_mode"] == "pandas"
    assert island["samples"][0]["result_mode"] == "arrow_stream"
    assert 0 < len(island["result"]["rows"]) <= 1_024
    assert island["samples"][0]["plan_stats"]["RESULT_MODE"] == "arrow_stream"
