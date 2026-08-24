from __future__ import annotations

from supertable.engine.adaptive_router import (
    AdaptiveEngineRouter,
    EngineHistory,
    RoutingAvailability,
    RoutingFeatures,
    query_shape_hash,
)
from supertable.engine.engine_enum import Engine


MIB = 1024 ** 2
GIB = 1024 ** 3


def _features(scan: int, **changes) -> RoutingFeatures:
    values = {
        "reflection_bytes": scan,
        "effective_scan_bytes": scan,
        "decoded_bytes": scan,
        "total_files": 1,
        "selected_row_groups": 1,
        "source_bytes_complete": True,
        "row_group_bytes_complete": True,
        "decoded_bytes_complete": True,
    }
    values.update(changes)
    return RoutingFeatures(**values)


def _availability(**changes) -> RoutingAvailability:
    values = {
        "island_enabled": False,
        "island_supported": False,
        "spark_available": False,
    }
    values.update(changes)
    return RoutingAvailability(**values)


def test_small_query_prefers_lite_after_cost_race():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(4 * MIB), _availability(),
    )

    assert decision.engine is Engine.DUCKDB
    assert "lowest predicted latency" in decision.reason


def test_stable_medium_projection_prefers_bounded_island():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(
            512 * MIB,
            decoded_bytes=256 * MIB,
            island_advice="island_in_memory",
            island_cpu_workers=4,
            island_io_workers=8,
        ),
        _availability(island_enabled=True, island_supported=True),
    )

    assert decision.engine is Engine.ISLANDDB


def test_fresh_medium_query_avoids_cache_churn_and_prefers_lite():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(
            512 * MIB,
            data_is_fresh=True,
            freshness_age_seconds=10,
            island_advice="island_in_memory",
        ),
        _availability(island_enabled=True, island_supported=True),
    )

    assert decision.engine is Engine.DUCKDB


def test_spark_startup_cost_keeps_near_floor_query_on_pro():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(1 * GIB),
        _availability(
            spark_available=True,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1 * GIB,
        ),
    )

    assert decision.engine is Engine.DUCKDB


def test_large_query_amortizes_spark_startup():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(2 * GIB),
        _availability(
            spark_available=True,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1 * GIB,
        ),
    )

    assert decision.engine is Engine.SPARK_SQL


def test_tombstone_hard_gate_excludes_spark():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(20 * GIB, has_active_tombstone=True),
        _availability(
            spark_available=True,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1,
        ),
    )

    assert decision.engine is not Engine.SPARK_SQL
    spark = next(c for c in decision.candidates if c.engine is Engine.SPARK_SQL)
    assert not spark.eligible
    assert any("tombstone" in reason for reason in spark.rejection_reasons)


def test_spark_semantic_gate_cannot_be_overridden_by_low_cost_history():
    features = _features(20 * GIB)
    history = {
        Engine.SPARK_SQL: EngineHistory(
            sample_count=100,
            success_count=100,
            ewma_duration_us=1,
            ewma_work_bytes=20 * GIB,
            feature_signature=features.feature_signature(),
        ),
    }
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        features,
        _availability(
            spark_available=True,
            spark_semantics_supported=False,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1,
        ),
        history=history,
    )

    assert decision.engine is not Engine.SPARK_SQL
    spark = next(c for c in decision.candidates if c.engine is Engine.SPARK_SQL)
    assert any("semantic" in reason for reason in spark.rejection_reasons)


def test_linked_bearer_gate_keeps_auto_off_spark_with_explicit_reason():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(20 * GIB, island_advice="route_spark"),
        _availability(
            spark_available=True,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1,
            spark_linked_bearer_safe=False,
        ),
    )

    assert decision.engine is Engine.DUCKDB
    spark = next(c for c in decision.candidates if c.engine is Engine.SPARK_SQL)
    assert not spark.eligible
    assert any(
        "provider-linked bearer" in reason
        for reason in spark.rejection_reasons
    )


def test_linked_bearer_gate_keeps_auto_off_island_with_explicit_reason():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(
            512 * MIB,
            island_advice="island_in_memory",
            island_cpu_workers=4,
            island_io_workers=4,
        ),
        _availability(
            island_enabled=True,
            island_supported=True,
            island_linked_bearer_safe=False,
        ),
    )

    assert decision.engine is Engine.DUCKDB
    island = next(c for c in decision.candidates if c.engine is Engine.ISLANDDB)
    assert not island.eligible
    assert any(
        "provider-linked bearer" in reason
        for reason in island.rejection_reasons
    )


def test_incomplete_estimate_uses_conservative_pro_without_cost_guess():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(1, source_bytes_complete=False),
        _availability(
            island_enabled=True,
            island_supported=True,
            spark_available=True,
            spark_min_scan_bytes=0,
        ),
    )

    assert decision.engine is Engine.DUCKDB
    assert "incomplete" in decision.reason


def test_incomplete_fresh_estimate_retains_lite_escape_hatch():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(1, source_bytes_complete=False, data_is_fresh=True),
        _availability(),
    )

    assert decision.engine is Engine.DUCKDB


def test_native_route_spark_is_hard_operator_memory_gate():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(32 * MIB, island_advice="route_spark"),
        _availability(
            island_enabled=True,
            island_supported=True,
            spark_available=True,
            fitting_spark_clusters=1,
            spark_min_scan_bytes=1,
        ),
    )

    assert decision.engine is Engine.SPARK_SQL
    assert "distributed" in decision.reason


def test_native_route_spark_falls_back_to_pro_when_fleet_is_unavailable():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(32 * MIB, island_advice="route_spark"),
        _availability(island_enabled=True, island_supported=True),
    )

    assert decision.engine is Engine.DUCKDB


def test_native_stream_warning_never_demotes_to_lite():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(1 * MIB, island_advice="stream_result"),
        _availability(island_enabled=True, island_supported=True),
    )

    assert decision.engine is Engine.DUCKDB


def test_history_is_bounded_and_cannot_override_safety_gate():
    features = _features(20 * GIB, has_active_tombstone=True)
    history = {
        Engine.SPARK_SQL: EngineHistory(
            sample_count=100,
            success_count=100,
            ewma_duration_us=1,
            ewma_work_bytes=1,
            feature_signature=features.feature_signature(),
        ),
    }
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        features,
        _availability(spark_available=True, spark_min_scan_bytes=1),
        history=history,
    )

    assert decision.engine is not Engine.SPARK_SQL
    spark = next(c for c in decision.candidates if c.engine is Engine.SPARK_SQL)
    assert spark.history_weight_permille == 0


def test_successful_history_can_adapt_performance_choice():
    baseline = AdaptiveEngineRouter(island_min_bytes=100 * MIB)
    features = _features(64 * MIB)
    availability = _availability()
    assert baseline.decide(features, availability).engine is Engine.DUCKDB

    decision = baseline.decide(
        features,
        availability,
        history={
            Engine.DUCKDB: EngineHistory(
                sample_count=100,
                success_count=100,
                ewma_duration_us=1_000,
                ewma_work_bytes=128 * MIB,
                feature_signature=features.feature_signature(),
            ),
        },
    )

    assert decision.engine is Engine.DUCKDB


def test_decision_payload_is_json_safe_and_contains_all_candidates():
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        _features(4 * MIB), _availability(),
    )
    payload = decision.as_plan_stat()["AUTO_ROUTING"]

    assert payload["policy_version"] == "adaptive-v1"
    assert len(payload["feature_signature"]) == 16
    assert payload["selected_engine"] == "duckdb"
    assert payload["features"]["effective_scan_bytes"] == 4 * MIB
    assert {item["engine"] for item in payload["candidates"]} == {
        "duckdb", "duckdb", "islanddb", "spark_sql",
    }


def test_query_shape_hash_erases_literals_but_keeps_predicate_structure():
    first = query_shape_hash("SELECT id FROM events WHERE account_id = 10")
    second = query_shape_hash("select id from events where account_id=999")
    different = query_shape_hash("SELECT id FROM events WHERE tenant_id = 10")

    assert first == second
    assert first != different


def test_query_shape_features_capture_blocking_operators_and_limit():
    from supertable.engine.adaptive_router import analyze_query_shape

    shape = analyze_query_shape(
        "SELECT tenant, COUNT(*) FROM events GROUP BY tenant "
        "ORDER BY COUNT(*) DESC LIMIT 20"
    )

    assert shape.has_group_by
    assert shape.has_sort
    assert shape.has_aggregate
    assert shape.literal_limit == 20
    assert shape.projected_expressions == 2


def test_history_from_different_feature_bucket_is_ignored():
    features = _features(64 * MIB)
    decision = AdaptiveEngineRouter(island_min_bytes=100 * MIB).decide(
        features,
        _availability(),
        history={
            Engine.DUCKDB: EngineHistory(
                sample_count=100,
                success_count=100,
                ewma_duration_us=1,
                ewma_work_bytes=128 * MIB,
                feature_signature="different-bucket",
            ),
        },
    )

    assert decision.engine is Engine.DUCKDB


def test_history_signature_isolates_deletion_freshness_and_worker_capacity():
    base = _features(
        64 * MIB,
        query_shape_hash="0123456789abcdef",
        candidate_rows=1000,
        island_advice="island_in_memory",
        island_cpu_workers=4,
        island_io_workers=8,
    )

    assert base.feature_signature() != _features(
        64 * MIB,
        query_shape_hash="0123456789abcdef",
        candidate_rows=1000,
        island_advice="island_in_memory",
        island_cpu_workers=4,
        island_io_workers=8,
        has_active_tombstone=True,
    ).feature_signature()
    assert base.feature_signature() != _features(
        64 * MIB,
        query_shape_hash="0123456789abcdef",
        candidate_rows=1000,
        island_advice="island_in_memory",
        island_cpu_workers=1,
        island_io_workers=2,
    ).feature_signature()
    assert base.feature_signature() != _features(
        64 * MIB,
        query_shape_hash="0123456789abcdef",
        candidate_rows=1000,
        island_advice="island_in_memory",
        island_cpu_workers=4,
        island_io_workers=8,
        data_is_fresh=True,
    ).feature_signature()


def test_constrained_island_cpu_is_visible_in_candidate_cost():
    router = AdaptiveEngineRouter(island_min_bytes=100 * MIB)
    availability = _availability(island_enabled=True, island_supported=True)
    four = router.decide(
        _features(
            512 * MIB,
            decoded_bytes=512 * MIB,
            island_advice="island_in_memory",
            island_cpu_workers=4,
        ),
        availability,
    )
    one = router.decide(
        _features(
            512 * MIB,
            decoded_bytes=512 * MIB,
            island_advice="island_in_memory",
            island_cpu_workers=1,
        ),
        availability,
    )

    four_island = next(c for c in four.candidates if c.engine is Engine.ISLANDDB)
    one_island = next(c for c in one.candidates if c.engine is Engine.ISLANDDB)
    assert "constrained_cpu_capacity" not in four_island.components_us
    assert one_island.components_us["constrained_cpu_capacity"] > 0
    assert one_island.estimated_cost_us > four_island.estimated_cost_us
