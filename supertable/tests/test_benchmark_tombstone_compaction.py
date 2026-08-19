from __future__ import annotations

import pytest

from supertable.benchmarks.benchmark_tombstone_compaction import (
    DEFAULT_TARGET_BYTES,
    FILE_COUNT,
    LATEST_PUBLIC_COLUMNS,
    _allocate_tombstones,
    _calibration_bounds,
    _counter_delta,
    _filter_compatible_kwargs,
    prepare_corpus,
    run_benchmark,
)


@pytest.mark.parametrize("fused", [False, True])
def test_twenty_file_tombstone_compaction_benchmark_is_correct(fused):
    result = run_benchmark(rows_per_file=128, workers=4, label="smoke", fused=fused)

    assert result["configuration"]["file_count"] == FILE_COUNT == 20
    assert result["configuration"]["target_bytes"] == DEFAULT_TARGET_BYTES
    assert result["corpus"]["input_files"] == 20
    assert result["corpus"]["tombstone_rows"] == 20 * 32
    assert result["configuration"]["fused"] is fused
    if fused:
        assert result["summary"]["fused_candidates"] == 20
        assert result["summary"]["fused_residual_tombstone_rows"] == 0
        assert result["summary"]["intermediate_successor_files"] == 0
        assert result["summary"]["intermediate_bytes_reread"] == 0
        assert set(result["phases"]) == {"fused_compaction", "final_metadata"}
    else:
        assert result["summary"]["phase_a_survivor_files"] == 20
        assert result["summary"]["phase_b_candidates"] == 20
        assert result["summary"]["skipped_large_phase_a_successors"] == 0
        assert result["summary"]["intermediate_successor_files"] == 20
    assert result["summary"]["final_rows"] == 20 * 96
    assert result["summary"]["final_files"] >= 1

    diversity = result["corpus"]["diversity"]
    assert diversity["files_with_reordered_columns"]
    assert diversity["files_with_missing_latest_columns"]
    assert diversity["files_with_legacy_extra_columns"]
    assert diversity["newest_public_columns"] == list(LATEST_PUBLIC_COLUMNS)

    assert result["correctness"]["authoritative_projection"]["match"] is True
    assert result["correctness"]["physical_union"]["match"] is True
    authoritative = result["correctness"]["authoritative_projection"]["actual"]
    assert authoritative["rows"] == 20 * 96
    assert authoritative["columns"] == list(LATEST_PUBLIC_COLUMNS)
    assert [field["name"] for field in authoritative["schema"]] == list(
        LATEST_PUBLIC_COLUMNS
    )
    assert len(authoritative["sha256"]) == 64

    assert result["summary"]["total_bytes_read"] > 0
    assert result["summary"]["total_bytes_written"] > 0
    assert result["summary"]["read_amplification_vs_final_bytes"] >= 1.0
    assert result["summary"]["write_amplification_vs_final_bytes"] >= 1.0

    for phase in result["phases"].values():
        assert phase["wall_seconds"] >= 0
        assert phase["cpu_seconds"] >= 0
        assert phase["rss_peak_bytes"] > 0

    data_phases = (
        [result["phases"]["fused_compaction"]]
        if fused
        else [
            result["phases"]["tombstone_rewrite"],
            result["phases"]["small_file_merge"],
        ]
    )
    for phase in data_phases:
        assert phase["telemetry"]["counts"]["files_read"] == 20
        assert phase["telemetry"]["counts"]["rows_read"] > 0
        assert phase["telemetry"]["counts"]["files_written"] >= 1
        assert phase["throughput"]["aggregate_parquet_encode_seconds"] > 0

    metadata = result["phases"]["final_metadata"]
    assert metadata["stats_rows"] > 0
    assert metadata["final_footer_cache_hits"] == result["summary"]["final_files"]
    assert metadata["telemetry"]["timings_seconds"].get("stats.read_footer", 0) == 0


def test_exact_tombstones_are_distributed_across_every_file():
    allocation = _allocate_tombstones([101, 203, 307], 137)

    assert allocation == [23, 45, 69]
    assert sum(allocation) == 137
    assert all(0 < dead < rows for dead, rows in zip(allocation, [101, 203, 307]))


def test_production_shape_allocates_exactly_one_million_across_fifteen_files():
    rows = [500_000] * 15
    allocation = _allocate_tombstones(rows, 1_000_000)

    assert len(allocation) == 15
    assert sum(allocation) == 1_000_000
    assert max(allocation) - min(allocation) <= 1
    assert all(0 < dead < physical for dead, physical in zip(allocation, rows))


@pytest.mark.parametrize("total", [0, 2, 609])
def test_exact_tombstone_allocation_rejects_impossible_totals(total):
    with pytest.raises(ValueError):
        _allocate_tombstones([101, 203, 307], total)


def test_proc_counter_delta_is_non_negative_and_intersection_only():
    assert _counter_delta(
        {"read_bytes": 10, "write_bytes": 20, "old_only": 1},
        {"read_bytes": 18, "write_bytes": 15, "new_only": 9},
    ) == {"read_bytes": 8, "write_bytes": 0}
    assert _counter_delta(None, {"read_bytes": 1}) is None


def test_compatibility_filter_drops_kwargs_missing_from_old_revision():
    def old_compact(snapshot, data_dir, profiler=None):
        return snapshot, data_dir, profiler

    filtered = _filter_compatible_kwargs(
        old_compact,
        {
            "snapshot": {"resources": []},
            "data_dir": "/tmp/out",
            "profiler": object(),
            "footer_md_out": {},
            "tombstone_df": object(),
            "return_residual": True,
        },
    )

    assert set(filtered) == {"snapshot", "data_dir", "profiler"}


def test_calibration_bounds_stay_below_small_file_threshold():
    lower, upper = _calibration_bounds(
        int(15.75 * 1024 * 1024), 0.01, DEFAULT_TARGET_BYTES
    )

    assert lower < upper < DEFAULT_TARGET_BYTES


def test_checksummed_corpus_can_be_reused_at_the_same_path(tmp_path):
    corpus_dir = tmp_path / "shared-corpus"
    compact_target = 256 * 1024
    prepared = prepare_corpus(
        str(corpus_dir),
        rows_per_file=128,
        file_count=3,
        tombstone_rows=137,
        compression_level=1,
        target_bytes=compact_target,
        input_file_target_bytes=128 * 1024,
        input_size_tolerance=0.1,
        calibration_max_attempts=8,
    )

    result = run_benchmark(
        file_count=3,
        target_bytes=compact_target,
        input_corpus_dir=str(corpus_dir),
        fused=False,
    )

    assert prepared["tombstone_rows"] == 137
    assert all(
        entry["lower_bytes"] <= entry["bytes"] <= entry["upper_bytes"]
        for entry in result["corpus"]["input_size_calibration"]["files"]
    )
    assert result["corpus"]["input_size_calibration"]["all_within_target"] is True
    assert result["corpus"]["mode"] == "shared_manifest"
    assert result["corpus"]["manifest_sha256"] == prepared["sha256"]
    assert result["correctness"]["aggregates"]["match"] is True


@pytest.mark.parametrize("fused", [False, True])
def test_parameterized_exact_tombstone_corpus_and_aggregates(fused):
    result = run_benchmark(
        rows_per_file=128,
        file_count=3,
        tombstone_rows=137,
        workers=2,
        label="parameterized-smoke",
        fused=fused,
    )

    assert result["configuration"]["file_count"] == 3
    assert result["corpus"]["input_files"] == 3
    assert result["corpus"]["tombstone_rows"] == 137
    assert sum(result["corpus"]["tombstones_per_file"]) == 137
    assert all(value > 0 for value in result["corpus"]["tombstones_per_file"])
    assert result["summary"]["final_rows"] == 3 * 128 - 137
    assert result["correctness"]["aggregates"]["match"] is True
    assert result["correctness"]["aggregates"]["actual"]["row_count"] == 3 * 128 - 137

    for phase in result["phases"].values():
        assert "proc_io_delta" in phase
