from __future__ import annotations

import json
import time
from dataclasses import replace

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.engine.engine_enum import Engine
from supertable.engine.query_observations import (
    QueryObservation,
    QueryObservationStore,
    canonical_sql_shape,
    normalize_query_profile,
    redact_text,
    sanitize_execution_timings,
    sanitize_profile,
)
from supertable.engine.plan_stats import PlanStats


SHAPE = "1" * 16
SIGNATURE = "2" * 16


def _stats(*, fallback: bool = False) -> PlanStats:
    stats = PlanStats()
    stats.add_stat({"REFLECTIONS": 12})
    stats.add_stat({"ROW_GROUP_SCAN_SIZE": 1_000})
    stats.add_stat({"DECODED_SIZE": 3_000})
    stats.add_stat({"DECODED_SIZE_COMPLETE": True})
    stats.add_stat({
        "AUTO_ROUTING": {
            "policy_version": "adaptive-v1",
            "selected_engine": "islanddb",
            "feature_signature": SIGNATURE,
            "features": {
                "query_shape_hash": SHAPE,
                "effective_scan_bytes": 1_000,
                "decoded_bytes": 3_000,
                "decoded_bytes_complete": True,
                "total_files": 12,
                "selected_row_groups": 7,
            },
        },
    })
    stats.add_stat({
        "AUTO_ROUTING_OUTCOME": {
            "selected_engine": "islanddb",
            "actual_engine": "duckdb" if fallback else "islanddb",
            "fallback": fallback,
        },
    })
    stats.add_stat({"ENGINE": "duckdb" if fallback else "islanddb"})
    return stats


def _profile(*, fallback: bool = False, status: str = "ok"):
    return normalize_query_profile(
        query="SELECT value FROM orders WHERE tenant = 'private'",
        requested_engine="auto",
        timing=[{"EXECUTING_QUERY": 0.7}, {"TOTAL_EXECUTE": 1.2}],
        plan_stats=_stats(fallback=fallback),
        status=status,
        result_shape=(10, 1),
        engine_profile=(
            {"latency": 0.4, "total_bytes_read": 700}
            if fallback else {
                "engine": "islanddb",
                "elapsed_ms": 250.0,
                "elapsed_scope": (
                    "engine_after_admission_through_stream_close_"
                    "excludes_facade_and_profile_persist"
                ),
                "execution_outcome": "completed",
                "result_complete": True,
                "cpu_time_ms": 180.0,
                "cpu_time_measured": True,
                "cpu_time_scope": "process_cpu_delta",
                "logical_scan_bytes": 1_000,
                "logical_scan_bytes_complete": True,
                "physical_read_bytes": 512,
                "physical_read_bytes_measured": True,
                "physical_read_scope": "linux_proc_self_io_block_read_delta",
                "peak_memory_bytes": 8_192,
                "peak_memory_scope": "process_rss_delta",
                "rss_baseline_bytes": 100_000,
                "rss_peak_bytes": 108_192,
                "rss_final_bytes": 104_096,
                "rss_peak_delta_bytes": 8_192,
                "rss_retained_delta_bytes": 4_096,
                "rss_measured": True,
                "rss_scope": "process_rss_sampled_10ms",
                "spill_bytes": 2_048,
                "spill_bytes_measured": True,
                "observed_rows_scanned": 77,
                "observed_rows_scanned_measured": True,
                "result_rows": 10,
                "result_rows_scope": "arrow_output_rows",
                "result_bytes": 321,
                "result_bytes_scope": "arrow_output_batch_logical_nbytes",
                "estimated_candidate_files": 11,
                "estimated_candidate_files_complete": True,
                "estimated_candidate_row_groups": 5,
                "estimated_candidate_row_groups_complete": True,
                "planned_row_groups": 9,
                "planned_row_groups_complete": True,
                "estimated_candidate_rows": 88,
                "estimated_candidate_rows_complete": True,
                "cache": {
                    "range_remote_bytes": 600,
                    "range_cache_hit_bytes": 400,
                },
            }
        ),
    )


def _observation(
    duration_us: int = 250_000, *, signature: str = SIGNATURE,
    recorded_at_ms: int | None = None,
) -> QueryObservation:
    item = QueryObservation.from_profile(
        _profile(), query_id="query-1",
        recorded_at_ms=recorded_at_ms or int(time.time() * 1000),
    )
    return replace(
        item, feature_signature=signature, duration_us=duration_us,
        observation_id=f"obs-{signature}-{duration_us}-{time.time_ns()}",
    )


def test_profile_sanitizer_removes_presign_userinfo_and_secret_keys():
    profile = {
        "Filename(s)": (
            "https://user:password@minio:9000/bucket/data.parquet?"
            "X-Amz-Credential=admin&X-Amz-Signature=topsecret"
        ),
        "nested": {"authorization": "Bearer private", "ok": "value"},
    }

    rendered = json.dumps(sanitize_profile(profile), sort_keys=True)

    for secret in (
        "password", "bucket", "data.parquet", "admin", "topsecret",
        "Bearer private",
    ):
        assert secret not in rendered
    assert "minio" not in rendered
    assert "authorization" not in rendered
    assert '"_redacted_fields"' in rendered


def test_profile_sanitizer_is_depth_item_and_byte_bounded():
    oversized = {str(index): "x" * 5_000 for index in range(500)}
    clean = sanitize_profile(oversized, max_bytes=2_000)
    encoded = json.dumps(clean, separators=(",", ":")).encode()
    assert len(encoded) <= 2_000


def test_execution_timing_sanitizer_keeps_only_bounded_fixed_numeric_events():
    clean = sanitize_execution_timings([
        {"CONNECTION_SETUP": 0.0125},
        {"EXECUTING_QUERY": 0},
        {"TOTAL_EXECUTE": 1.5},
        {"REMOTE_PHASE": "https://storage.invalid/private?token=secret"},
        {"QUERY_EXECUTE": True},
        {"RESULT_FETCH": -1},
        {"RESULT_MATERIALIZE": float("nan")},
        {"CLEANUP": float("inf")},
    ])

    assert clean == [
        {"CONNECTION_SETUP": 0.0125},
        {"EXECUTING_QUERY": 0.0},
        {"TOTAL_EXECUTE": 1.5},
    ]
    rendered = json.dumps(clean)
    assert "REMOTE_PHASE" not in rendered
    assert "storage.invalid" not in rendered
    assert "secret" not in rendered


def test_execution_timing_sanitizer_drops_huge_finite_durations():
    assert sanitize_execution_timings([
        {"RESULT_FETCH": 1e308},
        {"QUERY_EXECUTE": 0.25},
    ]) == [{"QUERY_EXECUTE": 0.25}]


def test_execution_timing_sanitizer_honors_byte_cap_for_one_mapping():
    timings = {
        event: 1.234567
        for event in (
            "QUERY_PREPARATION", "ESTIMATE", "TOMBSTONE_RESOLUTION",
            "CONNECTION_SETUP", "REFLECTION_PREPARE", "TOMBSTONE_PREPARE",
            "RBAC_PREPARE", "ENGINE_PREPARE", "QUERY_EXECUTE",
            "RESULT_STREAM_SETUP", "RESULT_FETCH", "RESULT_MATERIALIZE",
            "CLEANUP", "EXECUTING_QUERY", "TOTAL_EXECUTE",
        )
    }

    clean = sanitize_execution_timings(timings, max_bytes=256)

    assert len(json.dumps(clean, separators=(",", ":")).encode()) <= 256
    assert len(clean) < len(timings)


def test_normalized_fetch_phase_uses_aggregated_occurrence_count():
    stats = PlanStats()
    stats.add_stat({"RESULT_FETCH_OCCURRENCES": 12})

    profile = normalize_query_profile(
        query="SELECT 1",
        requested_engine="duckdb",
        timing=[{"RESULT_FETCH": 0.125}],
        plan_stats=stats,
        status="ok",
        result_shape=(1, 1),
        engine_profile=None,
    ).as_dict()

    fetch = next(
        phase for phase in profile["pipeline_phases"]
        if phase["phase"] == "result_fetch"
    )
    assert fetch["duration_us"] == 125_000
    assert fetch["occurrences"] == 12


def test_normalized_v3_retains_typed_phases_pruning_cache_and_engine_detail():
    stats = PlanStats()
    for item in (
        {"FILES_BEFORE_PRUNE": 10},
        {"FILES_PRUNED": 7},
        {"FILES_KEPT": 3},
        {"STATS_LOAD_DURATION_MS": 1.25},
        {"STATS_LOAD_OCCURRENCES": 3},
        {"STATS_FILTER_DURATION_MS": 0.5},
        {"STATS_FILTER_OCCURRENCES": 2},
        {"PRUNE_DURATION_MS": 2.5},
        {"PREDICATE_PRUNE_OCCURRENCES": 4},
        {"ROW_GROUP_PRUNE_DURATION_MS": 0.25},
        {"ROW_GROUP_PRUNE_OCCURRENCES": 2},
        {"JOIN_EDGES": 1},
        {"JOIN_FILES_PRUNED": 2},
        {"JOIN_PRUNE_ITERATIONS": 3},
        {"JOIN_PRUNE_DURATION_MS": 0.75},
        {"JOIN_PRUNE_OCCURRENCES": 1},
        {"PRUNE_COUNTS": {"stats_cache_hit": 4, "stats_cache_miss": 1}},
        {"FILE_CACHE_REQUESTED_FILES": 3},
        {"FILE_CACHE_HITS": 2},
        {"FILE_CACHE_COVERAGE_RATIO": 2 / 3},
        # The last PlanStats value wins, so completed streams reach the
        # normalizer with the finalizer's marker.
        {"RESULT_MODE": "arrow_stream_final"},
        {"RESULT_BYTES": 512},
        {"RESULT_BATCH_LIMIT": {"max_rows": 256, "max_bytes": 4096}},
        {"ENGINE": "islanddb"},
    ):
        stats.add_stat(item)
    profile = normalize_query_profile(
        query="SELECT * FROM orders",
        requested_engine="islanddb",
        timing=[
            {"QUERY_PREPARATION": 0.01},
            {"ESTIMATE": 0.02},
            {"CONNECTION_SETUP": 0.003},
            {"EXECUTING_QUERY": 0.2},
            {"TOTAL_EXECUTE": 0.25},
        ],
        plan_stats=stats,
        status="ok",
        result_shape=(3, 2),
        engine_profile={
            "engine": "islanddb",
            "elapsed_ms": 200,
            "elapsed_scope": (
                "engine_after_admission_through_stream_close_"
                "excludes_facade_and_profile_persist"
            ),
            "execution_outcome": "completed",
            "result_complete": True,
            "cache": {
                "range_cache_hit_chunks": 5,
                "range_cache_hit_bytes": 2048,
                "range_remote_bytes": 0,
            },
            "phase_timings_ms": {
                "range_cache_setup_ms": 1.25,
                "relation_prepare_and_eager_integrity_ms": 8.5,
                "unknown_secret_phase": 99,
            },
            "profile_persist_ms": 0.5,
            "profile_persist_ms_measured": True,
            "profile_persist_succeeded": True,
            "planned_files": 3,
            "planned_files_complete": True,
            "planned_rows": 500,
            "planned_rows_complete": True,
            "result_batches": 2,
            "rss_sample_interval_ms": 10,
            "resources": {
                "advice": "island_in_memory",
                "cpu_workers": 2,
                "io_workers": 4,
                "batch_bytes": 4096,
                "reason": "must not be persisted",
            },
            "spill": {
                "triggered": False,
                "directory_metadata": "/private/spill/path",
            },
        },
    ).as_dict()

    assert profile["schema_version"] == 3
    assert profile["result_mode"] == "arrow_stream"
    assert profile["result_batch_max_rows"] == 256
    assert profile["result_batch_max_bytes"] == 4096
    assert profile["result_rows"] == 3
    assert profile["result_rows_scope"] == "arrow_output_rows"
    assert profile["result_bytes"] == 512
    assert profile["result_bytes_scope"] == (
        "arrow_output_batch_logical_nbytes"
    )
    assert profile["pruning"] == {
        "files_before_prune": 10,
        "files_pruned": 7,
        "files_kept": 3,
        "join_edges": 1,
        "join_files_pruned": 2,
        "join_iterations": 3,
        "stats_cache_hits": 4,
        "stats_cache_misses": 1,
        "enabled": True,
    }
    assert profile["cache_detail"]["whole_object"] == {
        "requested_files": 3,
        "hits": 2,
        "coverage_ratio_ppm": 666667,
    }
    assert profile["cache_detail"]["range"]["cache_hit_chunks"] == 5
    assert profile["engine_detail"]["kind"] == "islanddb"
    assert profile["engine_detail"]["planned_files"] == 3
    assert profile["engine_detail"]["resource_plan"] == {
        "cpu_workers": 2,
        "io_workers": 4,
        "batch_bytes": 4096,
        "advice": "island_in_memory",
    }
    rendered = json.dumps(profile, sort_keys=True)
    assert "unknown_secret_phase" not in rendered
    assert "must not be persisted" not in rendered
    assert "/private/spill/path" not in rendered
    phase_map = {
        (item["phase"], item["scope"], item["aggregation"]): item
        for item in profile["pipeline_phases"]
    }
    assert phase_map[("engine_connect", "engine", "exclusive")]["duration_us"] == 3000
    assert phase_map[("stats_load", "planning", "nested")]["duration_us"] == 1250
    assert phase_map[("stats_load", "planning", "nested")]["occurrences"] == 3
    assert phase_map[("stats_filter", "planning", "nested")]["duration_us"] == 500
    assert phase_map[("predicate_prune", "planning", "nested")]["duration_us"] == 2500
    assert phase_map[("predicate_prune", "planning", "nested")]["occurrences"] == 4
    assert phase_map[("row_group_prune", "planning", "nested")]["duration_us"] == 250
    assert phase_map[("range_cache_setup", "engine", "nested")]["duration_us"] == 1250
    assert phase_map[("profile_persist", "delivery", "exclusive")]["duration_us"] == 500


def test_result_mode_is_unknown_until_execution_records_delivery_mode():
    stats = _stats(fallback=True)
    unknown = normalize_query_profile(
        query="SELECT * FROM orders",
        requested_engine="duckdb",
        timing=[],
        plan_stats=stats,
        status="error",
        result_shape=None,
        engine_profile=None,
    )
    assert unknown.result_mode == "unknown"

    stats.add_stat({"RESULT_MODE": "materialized"})
    materialized = normalize_query_profile(
        query="SELECT * FROM orders",
        requested_engine="duckdb",
        timing=[],
        plan_stats=stats,
        status="ok",
        result_shape=(0, 1),
        engine_profile=None,
    )
    assert materialized.result_mode == "materialized"


def test_total_files_distinguishes_unknown_from_complete_empty_inventory():
    stats = PlanStats()
    stats.add_stat({"ENGINE": "duckdb"})
    unknown = normalize_query_profile(
        query="SELECT * FROM orders",
        requested_engine="duckdb",
        timing=[],
        plan_stats=stats,
        status="error",
        result_shape=None,
        engine_profile=None,
    )
    assert unknown.total_files == 0
    assert unknown.total_files_complete is False

    stats.add_stat({"REFLECTIONS": 0})
    complete_empty = normalize_query_profile(
        query="SELECT * FROM orders",
        requested_engine="duckdb",
        timing=[],
        plan_stats=stats,
        status="ok",
        result_shape=None,
        engine_profile=None,
    )
    assert complete_empty.total_files == 0
    assert complete_empty.total_files_complete is True


@pytest.mark.parametrize("evaluated", [False, True])
def test_profile_sanitizer_retains_island_plan_evaluation(evaluated):
    clean = sanitize_profile({
        "auto_routing": {
            "features": {"island_plan_evaluated": evaluated},
        },
    })

    assert clean == {
        "auto_routing": {
            "features": {"island_plan_evaluated": evaluated},
        },
    }


def test_sql_shape_erases_literals_and_never_uses_raw_parse_fallback():
    shaped = canonical_sql_shape(
        "SELECT * FROM orders WHERE email='private@example.com' AND id=42"
    )
    assert "private@example.com" not in shaped
    assert "42" not in shaped
    assert shaped.count("?") == 2

    malformed = canonical_sql_shape("SELECT 'private' FROM (")
    assert "private" not in malformed
    assert malformed.startswith("<unparsed-sql sha256=")


def test_redact_text_scrubs_url_and_loose_secret_assignments():
    clean = redact_text(
        "open https://u:p@host/x?sig=hello&x=1 token=world password=hunter2"
    )
    assert clean == (
        "open https://host/<redacted-path> token=<redacted> password=<redacted>"
    )


@pytest.mark.parametrize(
    "diagnostic, secret",
    [
        ("Authorization: Bearer PROFILE_AUTH_SECRET", "PROFILE_AUTH_SECRET"),
        ("Cookie: session=PROFILE_COOKIE_SECRET", "PROFILE_COOKIE_SECRET"),
        ("X-Api-Key: PROFILE_API_SECRET", "PROFILE_API_SECRET"),
        ('{"access_token":"PROFILE_BODY_SECRET"}', "PROFILE_BODY_SECRET"),
    ],
)
def test_redact_text_scrubs_header_and_json_body_credentials(
    diagnostic, secret,
) -> None:
    rendered = redact_text(diagnostic)

    assert secret not in rendered
    assert "<redacted>" in rendered


def test_profile_sanitizer_redacts_unsigned_non_http_object_paths() -> None:
    clean = sanitize_profile({
        "source": "s3a://u:p@bucket.invalid/TENANT_PATH_TOKEN/data.parquet",
        "nested": [
            "abfss://container@account.invalid/OTHER_PATH_TOKEN/file.parquet"
            "?QUERY_TOKEN=1#FRAGMENT_TOKEN"
        ],
    })
    rendered = json.dumps(clean, sort_keys=True)

    assert "bucket.invalid" not in rendered
    assert "account.invalid" not in rendered
    for secret in (
        "TENANT_PATH_TOKEN", "OTHER_PATH_TOKEN", "data.parquet",
        "file.parquet", "QUERY_TOKEN", "FRAGMENT_TOKEN",
    ):
        assert secret not in rendered


def test_redact_text_handles_non_http_object_urls() -> None:
    rendered = redact_text(
        "failed s3a://u:p@bucket.invalid/PATH_TOKEN/file.parquet"
        "?QUERY_TOKEN=yes#FRAGMENT_TOKEN"
    )

    assert rendered == "failed s3a://bucket.invalid/<redacted-path>"


def test_normalization_prefers_engine_latency_and_tracks_provenance():
    profile = _profile()
    assert profile.engine_wall_us == 250_000
    assert profile.total_wall_us == 1_200_000
    assert profile.duration_source == "engine_profile"
    assert profile.duration_measured is True
    assert profile.duration_scope.endswith(
        "excludes_facade_and_profile_persist"
    )
    assert profile.actual_scan_bytes == 600
    assert profile.actual_scan_bytes_measured is True
    assert profile.actual_scan_bytes_scope == "remote_fetch_bytes"
    assert profile.cpu_time_us == 180_000
    assert profile.cpu_time_measured is True
    assert profile.cpu_time_scope == "process_cpu_delta"
    assert profile.logical_scan_bytes == 1_000
    assert profile.logical_scan_bytes_complete is True
    assert profile.physical_read_bytes == 512
    assert profile.physical_read_bytes_measured is True
    assert profile.physical_read_scope == (
        "linux_proc_self_io_block_read_delta"
    )
    assert profile.peak_memory_bytes == 8_192
    assert profile.peak_memory_scope == "process_rss_delta"
    assert profile.rss_baseline_bytes == 100_000
    assert profile.rss_peak_bytes == 108_192
    assert profile.rss_final_bytes == 104_096
    assert profile.rss_peak_delta_bytes == 8_192
    assert profile.rss_retained_delta_bytes == 4_096
    assert profile.rss_measured is True
    assert profile.spill_bytes == 2_048
    assert profile.spill_bytes_measured is True
    assert profile.rows_scanned == 77
    assert profile.rows_scanned_measured is True
    assert profile.total_files == 11
    assert profile.selected_row_groups == 5
    assert profile.selected_row_groups_complete is True
    assert profile.planned_row_groups == 9
    assert profile.planned_row_groups_complete is True
    assert profile.candidate_rows == 88
    assert profile.candidate_rows_complete is True
    assert profile.result_rows_measured is True
    assert profile.result_rows_scope == "materialized_result_shape"
    assert profile.result_bytes == 321
    assert profile.result_bytes_measured is True
    assert profile.result_bytes_scope == "arrow_output_batch_logical_nbytes"
    assert profile.execution_outcome == "completed"
    assert profile.result_complete is True
    assert profile.estimated_scan_bytes == 1_000
    assert profile.work_bytes == 4_000
    assert profile.requested_engine == "auto"
    assert profile.selected_engine == "islanddb"
    assert profile.actual_engine == "islanddb"
    assert profile.feature_signature == SIGNATURE


def test_duckdb_engine_bytes_are_not_mislabeled_as_physical_block_io():
    profile = normalize_query_profile(
        query="SELECT 1",
        requested_engine="duckdb",
        timing=[{"TOTAL_EXECUTE": 0.5}],
        plan_stats=_stats(fallback=True),
        status="ok",
        result_shape=(1, 1),
        engine_profile={
            "latency": 0.4,
            "total_bytes_read": 20_185_624,
            "cumulative_rows_scanned": 123,
        },
    )

    assert profile.actual_scan_bytes == 20_185_624
    assert profile.actual_scan_bytes_measured is True
    assert profile.actual_scan_bytes_scope == (
        "duckdb_engine_profile_total_bytes_read"
    )
    assert profile.physical_read_bytes == 0
    assert profile.physical_read_bytes_measured is False
    assert profile.physical_read_scope == "unknown"
    assert profile.rows_scanned == 123
    assert profile.rows_scanned_measured is True


def test_island_plan_stats_profile_is_lossless_fallback_with_provenance():
    stats = _stats()
    stats.add_stat({
        "ISLAND_TELEMETRY": {
            "engine": "islanddb",
            "elapsed_ms": 12.0,
            "elapsed_scope": (
                "engine_after_admission_through_stream_close_"
                "excludes_facade_and_profile_persist"
            ),
            "execution_outcome": "completed",
            "result_complete": True,
            "planned_files": 2,
            "planned_files_complete": True,
            "planned_row_groups": 10,
            "planned_row_groups_complete": True,
            "planned_rows": 100,
            "planned_rows_complete": True,
            "physical_read_bytes": 4_096,
            "physical_read_bytes_measured": True,
            "physical_read_scope": "linux_proc_self_io_block_read_delta",
            "result_rows": 7,
            "result_rows_scope": "arrow_output_rows",
            "result_bytes": 512,
            "result_bytes_scope": "arrow_output_batch_logical_nbytes",
        },
    })
    stats.add_stat({
        "ISLAND_CACHE": {
            "range_remote_bytes": 2_048,
            "range_cache_hit_bytes": 1_024,
        },
    })
    stats.add_stat({
        "ISLAND_RESOURCES": {
            "advice": "island_spill",
            "cpu_workers": 2,
            "memory_budget_bytes": 65_536,
            "reason": "private planner explanation",
        },
    })
    stats.add_stat({
        "ISLAND_SPILL": {
            "triggered": True,
            "budget_bytes": 32_768,
            "directory": "/private/spill",
        },
    })
    profile = normalize_query_profile(
        query="SELECT value FROM orders",
        requested_engine="auto",
        timing=[],
        plan_stats=stats,
        status="ok",
        result_shape=None,
        engine_profile=None,
    )

    assert profile.duration_measured is True
    assert profile.planned_row_groups == 10
    assert profile.planned_row_groups_complete is True
    assert profile.physical_read_bytes == 4_096
    assert profile.physical_read_bytes_measured is True
    assert profile.actual_scan_bytes == 2_048
    assert profile.actual_scan_bytes_measured is True
    assert profile.actual_scan_bytes_scope == "remote_fetch_bytes"
    assert profile.result_rows == 7
    assert profile.result_rows_measured is True
    assert profile.result_rows_scope == "arrow_output_rows"
    assert profile.result_bytes == 512
    assert profile.result_bytes_scope == "arrow_output_batch_logical_nbytes"
    assert profile.engine_detail["resource_plan"] == {
        "cpu_workers": 2,
        "memory_budget_bytes": 65_536,
        "advice": "island_spill",
    }
    assert profile.engine_detail["spill_plan"] == {
        "triggered": True,
        "budget_bytes": 32_768,
    }
    assert "private planner explanation" not in json.dumps(profile.engine_detail)
    assert "/private/spill" not in json.dumps(profile.engine_detail)


def test_island_current_profile_preserves_post_persistence_telemetry():
    stats = _stats()
    stats.add_stat({
        "ISLAND_TELEMETRY": {
            "engine": "islanddb",
            "phase_timings_ms": {
                "engine_elapsed_excluding_profile_persist_ms": 12.0,
                "profile_persist_ms": 0.75,
            },
            "profile_persist_ms": 0.75,
            "profile_persist_ms_measured": True,
            "profile_persist_succeeded": True,
        },
    })

    profile = normalize_query_profile(
        query="SELECT value FROM orders",
        requested_engine="islanddb",
        timing=[],
        plan_stats=stats,
        status="ok",
        result_shape=(1, 1),
        engine_profile={
            "engine": "islanddb",
            "phase_timings_ms": {
                "engine_elapsed_excluding_profile_persist_ms": 12.0,
            },
            "profile_persist_ms": None,
            "profile_persist_ms_measured": False,
            "profile_persist_succeeded": None,
        },
    ).as_dict()

    persisted = next(
        phase for phase in profile["pipeline_phases"]
        if phase["phase"] == "profile_persist"
    )
    assert persisted["duration_us"] == 750
    assert profile["engine_detail"]["profile_persist_succeeded"] is True


def test_legacy_island_candidate_rows_are_not_runtime_measurements():
    profile = normalize_query_profile(
        query="SELECT value FROM orders",
        requested_engine="islanddb",
        timing=[],
        plan_stats=PlanStats(),
        status="ok",
        result_shape=(1, 1),
        engine_profile={
            "engine": "islanddb",
            "elapsed_ms": 1.0,
            "result_complete": True,
            # Historical Island profiles set these from estimator metadata;
            # they never represented a universal native scanner counter.
            "rows_scanned": 999,
            "rows_scanned_measured": True,
        },
    )

    assert profile.rows_scanned == 0
    assert profile.rows_scanned_measured is False


def test_partial_island_stream_cannot_become_adaptive_feedback():
    profile = normalize_query_profile(
        query="SELECT value FROM orders",
        requested_engine="auto",
        timing=[{"TOTAL_EXECUTE": 0.5}],
        plan_stats=_stats(),
        status="ok",
        result_shape=(3, 1),
        engine_profile={
            "engine": "islanddb",
            "elapsed_scope": "engine_after_admission_through_stream_close",
            "execution_outcome": "closed_early",
            "result_complete": False,
            "logical_scan_bytes": 1_000,
            "logical_scan_bytes_complete": True,
        },
    )
    observation = QueryObservation.from_profile(profile, query_id="partial")

    assert profile.execution_outcome == "closed_early"
    assert profile.result_complete is False
    assert profile.duration_measured is False
    assert observation.feedback_eligible is False


def test_missing_island_completion_proof_cannot_train_from_outer_timer():
    profile = normalize_query_profile(
        query="SELECT value FROM orders",
        requested_engine="auto",
        timing=[{"EXECUTING_QUERY": 0.2}],
        plan_stats=_stats(),
        status="ok",
        result_shape=(1, 1),
        engine_profile=None,
    )

    assert profile.actual_engine == "islanddb"
    assert profile.result_complete is None
    assert profile.duration_measured is False
    assert QueryObservation.from_profile(
        profile, query_id="missing-profile",
    ).feedback_eligible is False


def test_forced_failed_and_fallback_profiles_remain_feedback_ineligible():
    fallback = QueryObservation.from_profile(
        _profile(fallback=True), query_id="fallback",
    )
    assert fallback.fallback is True
    assert fallback.feedback_eligible is False

    failed = QueryObservation.from_profile(
        _profile(status="error"), query_id="failure",
    )
    assert failed.status == "error"
    assert failed.feedback_eligible is False

    forced_profile = normalize_query_profile(
        query="SELECT 1", requested_engine="duckdb",
        timing=[{"EXECUTING_QUERY": 0.01}], plan_stats=_stats(),
        status="ok", result_shape=(1, 1), engine_profile={},
    )
    forced = QueryObservation.from_profile(forced_profile, query_id="forced")
    assert forced.forced is True
    assert forced.feedback_eligible is False


def test_failed_attempt_normalizes_nested_engine_request_without_training():
    stats = PlanStats()
    stats.add_stat({
        "ENGINE_REQUEST": {
            "requested_engine": "auto",
            "selected_engine": "islanddb",
            "forced": False,
        },
    })
    stats.add_stat({
        "ENGINE_ATTEMPT": {
            "engine": "islanddb",
            "stage": "primary",
        },
    })
    profile = normalize_query_profile(
        query="SELECT 1",
        requested_engine="auto",
        timing=[{"EXECUTING_QUERY": 0.01}],
        plan_stats=stats,
        status="error",
        result_shape=(0, 0),
        engine_profile={},
    )

    assert profile.requested_engine == "auto"
    assert profile.selected_engine == "islanddb"
    assert profile.actual_engine == "islanddb"
    assert profile.forced is False
    assert QueryObservation.from_profile(
        profile, query_id="failed",
    ).feedback_eligible is False


def test_store_enforces_sample_ttl_and_signature_cardinality_bounds():
    redis = fakeredis.FakeStrictRedis(decode_responses=True)
    store = QueryObservationStore(
        "acme", redis, max_samples=3, ttl_days=2, min_samples=3,
        max_signatures=2,
    )
    now = int(time.time() * 1000)
    signatures = ["a" * 16, "b" * 16, "c" * 16]
    for signature in signatures:
        for offset in range(5):
            assert store.record(_observation(
                100 + offset, signature=signature,
                recorded_at_ms=now + offset,
            ))

    index = RK.query_observation_index("acme")
    assert redis.zcard(index) <= 2
    oldest = RK.query_observation_samples("acme", signatures[0], "islanddb")
    assert redis.exists(oldest) == 0
    newest = RK.query_observation_samples("acme", signatures[-1], "islanddb")
    assert redis.zcard(newest) == 3
    assert 0 < redis.ttl(newest) <= 2 * 24 * 60 * 60


def test_history_is_chronological_ewma_and_ignores_corrupt_mismatched_rows():
    redis = fakeredis.FakeStrictRedis(decode_responses=True)
    store = QueryObservationStore(
        "acme", redis, max_samples=10, ttl_days=2, min_samples=3,
    )
    now = int(time.time() * 1000)
    durations = [100, 100, 100, 1_000, 1_000]
    for offset, duration in enumerate(durations):
        assert store.record(_observation(
            duration, recorded_at_ms=now + offset,
        ))

    key = RK.query_observation_samples("acme", SIGNATURE, "islanddb")
    redis.zadd(key, {"not-json": now + 6})
    wrong = replace(_observation(999, recorded_at_ms=now + 7), query_shape_hash="f" * 16)
    redis.zadd(key, {json.dumps(wrong.as_dict()): now + 7})

    history = store.load_history(SHAPE, SIGNATURE)[Engine.ISLANDDB]
    assert history.sample_count == 5
    assert 550 <= history.ewma_duration_us <= 700
    assert history.ewma_duration_us > sum(durations) // len(durations)
    assert history.ewma_work_bytes == 4_000
    assert history.feature_signature == SIGNATURE


def test_history_provider_is_exact_bucket_and_redis_failures_are_nonfatal():
    redis = fakeredis.FakeStrictRedis(decode_responses=True)
    store = QueryObservationStore("acme", redis, min_samples=3)
    for duration in (100, 200, 300):
        assert store.record(_observation(duration))

    class Features:
        query_shape_hash = SHAPE

        @staticmethod
        def feature_signature():
            return SIGNATURE

    assert Engine.ISLANDDB in store.history_provider(Features())
    assert store.load_history(SHAPE, "f" * 16) == {}

    class BrokenRedis:
        def zrangebyscore(self, *args, **kwargs):
            raise OSError("redis down")

    broken = QueryObservationStore("acme", BrokenRedis())
    assert broken.load_history(SHAPE, SIGNATURE) == {}
