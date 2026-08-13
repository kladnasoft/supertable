from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pytest

from supertable.engine.benchmarks.corpus import (
    CorpusSpec,
    build_workloads,
    normalize_tiers,
    plan_workload,
    prepare_corpus,
)
from supertable.engine.benchmarks.islanddb import build_parser, selected_tiers
from supertable.engine.benchmarks.runner import (
    BenchmarkParityError,
    ComparisonConfig,
    assert_exact_parity,
    canonical_frame,
    compare_manifest,
    islanddb_available,
    result_digest,
    run_isolated_worker,
)


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
    assert normalize_tiers(["KB,64MiB", "1GB", "10gib"]) == [
        "kb",
        "mb",
        "1gib",
        "10gib",
    ]

    parser = build_parser()
    args = parser.parse_args(["--1gb"])
    with pytest.raises(ValueError, match="opt-in"):
        selected_tiers(args)

    args = parser.parse_args(["--1gb", "--10gb", "--allow-large"])
    assert selected_tiers(args) == ["1gib", "10gib"]


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


def test_compare_stops_before_timing_on_parity_failure(tiny_manifest, tmp_path):
    calls = []

    def fake_worker(request, **kwargs):
        calls.append((request["purpose"], request["engine"]))
        value = 1 if request["engine"] == "duckdb_lite" else 2
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
    assert calls == [("parity", "duckdb_lite"), ("parity", "islanddb")]


def test_explicit_duckdb_production_worker_reports_cold_and_warm(tiny_manifest, tmp_path):
    plan = plan_workload(
        tiny_manifest, build_workloads(tiny_manifest["total_rows"])["point"]
    )
    result = run_isolated_worker(
        {
            "purpose": "smoke",
            "engine": "duckdb_lite",
            "plan": plan,
            "warm_repeats": 1,
            "cold_mode": "process",
        },
        cache_dir=tmp_path / "cache",
        home_dir=tmp_path / "home",
        timeout_seconds=120,
    )

    assert result["engine_value"] == "duckdb_lite"
    assert [sample["temperature"] for sample in result["samples"]] == ["cold", "warm"]
    assert all(sample["result_digest"] == result["result_digest"] for sample in result["samples"])
    assert result["samples"][0]["wall_seconds"] > 0
    assert "total_bytes_read" in result["samples"][0]["engine_profile"]


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

    duck = run("duckdb_lite")
    island = run("islanddb")
    assert assert_exact_parity(duck, island, label="production-smoke") == duck["result_digest"]
    assert island["samples"][0]["engine"] == "islanddb"
