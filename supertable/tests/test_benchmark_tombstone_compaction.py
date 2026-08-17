from __future__ import annotations

import pytest

from supertable.benchmarks.benchmark_tombstone_compaction import (
    DEFAULT_TARGET_BYTES,
    FILE_COUNT,
    LATEST_PUBLIC_COLUMNS,
    run_benchmark,
)


@pytest.mark.parametrize("fused", [False, True])
def test_twenty_file_tombstone_compaction_benchmark_is_correct(fused):
    result = run_benchmark(
        rows_per_file=128, workers=4, label="smoke", fused=fused
    )

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
