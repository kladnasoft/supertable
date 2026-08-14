from __future__ import annotations

import json
import time
from dataclasses import replace

import fakeredis

from supertable import redis_keys as RK
from supertable.engine.engine_enum import Engine
from supertable.engine.query_observations import (
    QueryObservation,
    QueryObservationStore,
    canonical_sql_shape,
    normalize_query_profile,
    redact_text,
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
                "cpu_time_ms": 180.0,
                "logical_scan_bytes": 1_000,
                "logical_scan_bytes_complete": True,
                "physical_read_bytes": 512,
                "physical_read_bytes_measured": True,
                "peak_memory_bytes": 8_192,
                "peak_memory_scope": "process_rss_delta",
                "spill_bytes": 2_048,
                "spill_bytes_measured": True,
                "rows_scanned": 77,
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

    for secret in ("password", "admin", "topsecret", "Bearer private"):
        assert secret not in rendered
    assert "https://minio:9000/bucket/data.parquet?<redacted>" in rendered
    assert '"authorization": "<redacted>"' in rendered


def test_profile_sanitizer_is_depth_item_and_byte_bounded():
    oversized = {str(index): "x" * 5_000 for index in range(500)}
    clean = sanitize_profile(oversized, max_bytes=2_000)
    encoded = json.dumps(clean, separators=(",", ":")).encode()
    assert len(encoded) <= 2_000


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
        "open https://host/x?<redacted> token=<redacted> password=<redacted>"
    )


def test_normalization_prefers_engine_latency_and_tracks_provenance():
    profile = _profile()
    assert profile.engine_wall_us == 250_000
    assert profile.total_wall_us == 1_200_000
    assert profile.duration_source == "engine_profile"
    assert profile.duration_measured is True
    assert profile.actual_scan_bytes == 600
    assert profile.actual_scan_bytes_measured is True
    assert profile.cpu_time_us == 180_000
    assert profile.logical_scan_bytes == 1_000
    assert profile.logical_scan_bytes_complete is True
    assert profile.physical_read_bytes == 512
    assert profile.physical_read_bytes_measured is True
    assert profile.peak_memory_bytes == 8_192
    assert profile.peak_memory_scope == "process_rss_delta"
    assert profile.spill_bytes == 2_048
    assert profile.spill_bytes_measured is True
    assert profile.rows_scanned == 77
    assert profile.estimated_scan_bytes == 1_000
    assert profile.work_bytes == 4_000
    assert profile.requested_engine == "auto"
    assert profile.selected_engine == "islanddb"
    assert profile.actual_engine == "islanddb"
    assert profile.feature_signature == SIGNATURE


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
