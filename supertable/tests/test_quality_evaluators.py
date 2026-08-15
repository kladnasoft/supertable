from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from supertable.quality.anomaly import (
    detect_anomalies,
    evaluate_deep_checks,
    evaluate_table_metadata_checks,
    summarize_check_outcomes,
)


def _checks(**enabled):
    return {
        check_id: {"enabled": True, "threshold": threshold}
        for check_id, threshold in enabled.items()
    }


def test_freshness_and_size_use_snapshot_metadata_not_hidden_columns():
    now = datetime(2026, 8, 14, 12, tzinfo=timezone.utc)
    outcomes = evaluate_table_metadata_checks(
        _checks(T2=24, T5=50),
        last_modified_at="2026-08-13T06:00:00Z",
        current_size_bytes=160,
        previous_size_bytes=100,
        now=now,
    )
    assert [outcome["check_id"] for outcome in outcomes] == ["T2", "T5"]
    assert [outcome["status"] for outcome in outcomes] == ["warning", "warning"]
    assert outcomes[0]["value"] == 30
    assert outcomes[1]["value"] == 60


def test_freshness_accepts_snapshot_epoch_nanoseconds():
    now = datetime(2026, 8, 14, 12, tzinfo=timezone.utc)
    modified = datetime(2026, 8, 14, 11, tzinfo=timezone.utc)
    outcomes = evaluate_table_metadata_checks(
        _checks(T2=2),
        last_modified_at=int(modified.timestamp() * 1_000_000_000),
        current_size_bytes=None,
        previous_size_bytes=None,
        now=now,
    )
    assert outcomes[0]["status"] == "ok"
    assert outcomes[0]["value"] == 1


def test_size_first_run_records_baseline_without_claiming_pass():
    outcome = evaluate_table_metadata_checks(
        _checks(T5=50),
        last_modified_at=None,
        current_size_bytes=100,
        previous_size_bytes=None,
    )[0]
    assert outcome["status"] == "skipped"
    assert outcome["applicable"] is True
    assert outcome["evaluated"] is False
    assert outcome["reason"] == "baseline_unavailable"


def test_missing_current_metadata_is_an_error_not_a_pass():
    outcomes = evaluate_table_metadata_checks(
        _checks(T2=24, T5=50),
        last_modified_at=None,
        current_size_bytes=None,
        previous_size_bytes=100,
    )
    assert [outcome["status"] for outcome in outcomes] == ["error", "error"]
    summary = summarize_check_outcomes(outcomes)
    assert summary == {
        "configured": 2,
        "passed": 0,
        "warnings": 0,
        "critical": 0,
        "errors": 2,
        "skipped": 0,
        "not_applicable": 0,
    }


def _deep_profile(**updates):
    value = "a"
    minimum = "a"
    maximum = "a"
    profile = {
        "total_rows": 100,
        "non_nulls": 90,
        "distinct_vals": 1,
        "avg_length": 5.0,
        "min_length": 1,
        "max_length": 12,
        "median_length": 5,
        "shannon_entropy": 3.0,
        "topx_values": [{
            "value": value,
            "value_sha256": hashlib.sha256(value.encode()).hexdigest(),
            "value_char_length": len(value),
            "value_byte_length": len(value.encode()),
            "value_truncated": False,
            "freq": 90,
        }],
        "topx_coverage_pct": 100.0,
        "buckets": [{
            "bucket_id": bucket_id,
            "bucket_min": minimum,
            "bucket_min_sha256": hashlib.sha256(minimum.encode()).hexdigest(),
            "bucket_min_char_length": len(minimum),
            "bucket_min_byte_length": len(minimum.encode()),
            "bucket_min_truncated": False,
            "bucket_max": maximum,
            "bucket_max_sha256": hashlib.sha256(maximum.encode()).hexdigest(),
            "bucket_max_char_length": len(maximum),
            "bucket_max_byte_length": len(maximum.encode()),
            "bucket_max_truncated": False,
            "freq": 9,
        } for bucket_id in range(1, 11)],
        "placeholder_rate": 0.12,
        "p25_value": 2,
        "p75_value": 8,
    }
    profile.update(updates)
    return profile


def _numeric_deep_profile(**updates):
    profile = _deep_profile()
    profile.update({
        "topx_values": [{"value": 2, "freq": 90}],
        "buckets": [{
            "bucket_id": bucket_id,
            "bucket_min": 2,
            "bucket_max": 2,
            "freq": 9,
        } for bucket_id in range(1, 11)],
    })
    profile.update(updates)
    return profile


def test_deep_checks_have_per_id_evaluated_outcomes():
    checks = _checks(D1=None, D2=20, D3=None, D4=None, D5=30, D7=10)
    previous_string = _deep_profile(shannon_entropy=2.0, placeholder_rate=0.01)
    string_outcomes = evaluate_deep_checks(
        _deep_profile(), previous_string, "string", checks, "label",
    )
    by_id = {outcome["check_id"]: outcome for outcome in string_outcomes}
    assert by_id["D1"]["status"] == "ok"
    assert by_id["D2"]["status"] == "critical"
    assert by_id["D3"]["status"] == "ok"
    assert by_id["D4"]["status"] == "ok"
    assert by_id["D5"]["status"] == "not_applicable"
    assert by_id["D7"]["status"] == "warning"
    assert by_id["D7"]["value"] == 12


def test_numeric_iqr_comparison_and_string_only_checks_are_not_applicable():
    checks = _checks(D1=None, D2=20, D3=None, D4=None, D5=30, D7=10)
    outcomes = evaluate_deep_checks(
        _numeric_deep_profile(shannon_entropy=2.0, p25_value=2, p75_value=8),
        _numeric_deep_profile(shannon_entropy=2.0, p25_value=2, p75_value=6),
        "numeric",
        checks,
        "amount",
    )
    by_id = {outcome["check_id"]: outcome for outcome in outcomes}
    assert by_id["D1"]["status"] == "not_applicable"
    assert by_id["D3"]["status"] == "ok"
    assert by_id["D4"]["status"] == "ok"
    assert by_id["D5"]["status"] == "warning"
    assert by_id["D5"]["value"] == 50
    assert by_id["D7"]["status"] == "not_applicable"


def test_comparison_checks_need_a_baseline_but_tracking_checks_do_not():
    checks = _checks(D1=None, D2=20, D3=None, D4=None, D5=30, D7=20)
    outcomes = evaluate_deep_checks(_deep_profile(), None, "string", checks, "label")
    by_id = {outcome["check_id"]: outcome for outcome in outcomes}
    assert by_id["D1"]["evaluated"] is True
    assert by_id["D2"]["status"] == "skipped"
    assert by_id["D2"]["reason"] == "baseline_unavailable"
    assert by_id["D7"]["evaluated"] is True


@pytest.mark.parametrize(
    ("total_rows", "non_nulls"),
    [
        (-1, 0),
        (1.5, 1),
        (2, -1),
        (1, 2),
        (float("nan"), 0),
        (2, 1.5),
        (True, 1),
    ],
)
def test_deep_common_envelope_requires_exact_consistent_counts(
    total_rows,
    non_nulls,
):
    outcomes = evaluate_deep_checks(
        _numeric_deep_profile(
            total_rows=total_rows,
            non_nulls=non_nulls,
        ),
        None,
        "numeric",
        _checks(D3=None, D4=None),
        "amount",
    )

    assert {item["check_id"] for item in outcomes} == {"D3", "D4"}
    assert all(item["status"] == "error" for item in outcomes)
    assert all(item["evaluated"] is False for item in outcomes)
    assert all(item["reason"] == "missing_metric" for item in outcomes)


@pytest.mark.parametrize(
    "distinct_vals",
    [-1, 1.5, 91, float("inf"), True, 0],
)
def test_deep_common_envelope_rejects_impossible_distinct_counts(distinct_vals):
    outcomes = evaluate_deep_checks(
        _numeric_deep_profile(distinct_vals=distinct_vals),
        None,
        "numeric",
        _checks(D3=None, D4=None),
        "amount",
    )
    assert all(item["status"] == "error" for item in outcomes)
    assert all(item["reason"] == "missing_metric" for item in outcomes)


@pytest.mark.parametrize("category", ["numeric", "string"])
@pytest.mark.parametrize(("total_rows", "non_nulls"), [(0, 0), (5, 0)])
def test_deep_empty_and_all_null_columns_are_successful_skips(
    category,
    total_rows,
    non_nulls,
):
    outcomes = evaluate_deep_checks(
        {
            "total_rows": total_rows,
            "non_nulls": non_nulls,
            "distinct_vals": 0,
            "topx_values": None,
            "topx_coverage_pct": None,
            "buckets": None,
        },
        None,
        category,
        _checks(D3=None, D4=None),
        "value",
    )

    assert len(outcomes) == 2
    assert all(item["status"] == "skipped" for item in outcomes)
    assert all(item["reason"] == "empty_input" for item in outcomes)


@pytest.mark.parametrize(
    ("check_id", "updates"),
    [
        ("D3", {"topx_values": "opaque"}),
        ("D3", {"topx_values": [{"value": 1}]}),
        ("D3", {
            "topx_values": [{"value": 1, "freq": 20}],
            "topx_coverage_pct": 99,
        }),
        ("D3", {
            "topx_values": [
                {"value": 2, "freq": 10},
                {"value": 1, "freq": 20},
            ],
            "distinct_vals": 2,
            "topx_coverage_pct": 33.33,
        }),
        ("D4", {"buckets": "opaque"}),
        ("D4", {"buckets": [{
            "bucket_id": 1, "bucket_min": 1, "bucket_max": 2,
        }]}),
        ("D4", {"buckets": [{
            "bucket_id": 2, "bucket_min": 1, "bucket_max": 2, "freq": 90,
        }]}),
        ("D4", {"buckets": [{
            "bucket_id": 1, "bucket_min": 1, "bucket_max": 2, "freq": 89,
        }]}),
    ],
    ids=[
        "d3-opaque", "d3-wrong-keys", "d3-bad-coverage", "d3-bad-order",
        "d4-opaque", "d4-wrong-keys", "d4-bad-id", "d4-bad-frequency-sum",
    ],
)
def test_deep_structured_tracking_rejects_malformed_values(check_id, updates):
    profile = _numeric_deep_profile(**updates)
    outcome = evaluate_deep_checks(
        profile,
        None,
        "numeric",
        _checks(**{check_id: None}),
        "amount",
    )[0]

    assert outcome["status"] == "error"
    assert outcome["evaluated"] is False
    assert outcome["reason"] == "missing_metric"


def test_d3_length_must_match_bounded_distinct_value_count():
    outcome = evaluate_deep_checks(
        _numeric_deep_profile(
            distinct_vals=2,
            topx_values=[{"value": 2, "freq": 90}],
            topx_coverage_pct=100,
        ),
        None,
        "numeric",
        _checks(D3=None),
        "amount",
    )[0]
    assert outcome["status"] == "error"
    assert outcome["reason"] == "missing_metric"


def test_d4_requires_exact_ntile_bucket_count_and_quotient_remainder_order():
    # NTILE(10) over 23 rows yields 3,3,3,2,2,2,2,2,2,2 in ID order.
    valid = [{
        "bucket_id": bucket_id,
        "bucket_min": bucket_id,
        "bucket_max": bucket_id,
        "freq": 3 if bucket_id <= 3 else 2,
    } for bucket_id in range(1, 11)]
    profile = _numeric_deep_profile(
        total_rows=23,
        non_nulls=23,
        distinct_vals=10,
        buckets=valid,
    )
    assert evaluate_deep_checks(
        profile, None, "numeric", _checks(D4=None), "amount",
    )[0]["status"] == "ok"

    wrong_order = [dict(bucket) for bucket in valid]
    wrong_order[0]["freq"], wrong_order[-1]["freq"] = (
        wrong_order[-1]["freq"], wrong_order[0]["freq"],
    )
    profile["buckets"] = wrong_order
    outcome = evaluate_deep_checks(
        profile, None, "numeric", _checks(D4=None), "amount",
    )[0]
    assert outcome["status"] == "error"
    assert outcome["reason"] == "missing_metric"


def test_deep_string_tracking_accepts_bounded_truncated_identity_shape():
    preview = "x" * 256
    profile = _deep_profile(
        topx_values=[{
            "value": preview,
            "value_sha256": "a" * 64,
            "value_char_length": 300,
            "value_byte_length": 300,
            "value_truncated": True,
            "freq": 90,
        }],
        topx_coverage_pct=100,
    )

    outcome = evaluate_deep_checks(
        profile,
        None,
        "string",
        _checks(D3=None),
        "label",
    )[0]
    assert outcome["status"] == "ok"
    assert outcome["evaluated"] is True


def test_deep_string_tracking_rejects_wrong_preview_identity_fields():
    malformed = _deep_profile()
    malformed["topx_values"][0]["value_sha256"] = "not-a-sha256"
    malformed["buckets"][0].pop("bucket_max_byte_length")

    outcomes = evaluate_deep_checks(
        malformed,
        None,
        "string",
        _checks(D3=None, D4=None),
        "label",
    )
    assert [item["status"] for item in outcomes] == ["error", "error"]
    assert all(item["reason"] == "missing_metric" for item in outcomes)


def test_summary_counters_are_mutually_exclusive_and_truthful():
    outcomes = [
        {"status": "ok", "evaluated": True, "applicable": True},
        {"status": "warning", "evaluated": True, "applicable": True},
        {"status": "critical", "evaluated": True, "applicable": True},
        {"status": "error", "evaluated": False, "applicable": True},
        {"status": "skipped", "evaluated": False, "applicable": True},
        {"status": "not_applicable", "evaluated": False, "applicable": False},
    ]
    summary = summarize_check_outcomes(outcomes)
    assert summary == {
        "configured": 6,
        "passed": 1,
        "warnings": 1,
        "critical": 1,
        "errors": 1,
        "skipped": 2,
        "not_applicable": 1,
    }
    assert summary["configured"] == sum(
        summary[key] for key in ("passed", "warnings", "critical", "errors", "skipped")
    )


def test_quick_anomalies_ignore_corrupt_nonfinite_legacy_metrics():
    checks = _checks(T1=30, C1=5, C2=50, C3=None, C5=5, C6=2)
    current = {
        "total": 20,
        "columns": {
            "amount": {
                "category": "numeric",
                "null_rate": 20.0,
                "distinct": 20,
                "min": -10.0,
                "max": 10.0,
                "avg": 5.0,
                "stddev": 1.0,
                "zero_rate": 20.0,
                "negative_rate": 20.0,
            },
        },
    }
    previous = {
        "total": float("nan"),
        "columns": {
            "amount": {
                "category": "numeric",
                "null_rate": float("nan"),
                "distinct": float("inf"),
                "min": float("-inf"),
                "max": float("inf"),
                "avg": float("nan"),
                "stddev": float("inf"),
                "zero_rate": float("nan"),
                "negative_rate": -1.0,
            },
        },
    }

    assert detect_anomalies(current, previous, checks) == []


def test_quick_anomalies_still_detect_valid_metric_changes():
    checks = _checks(T1=30, C1=5, C2=50, C3=None, C5=5, C6=2)
    previous = {
        "total": 10,
        "columns": {
            "amount": {
                "category": "numeric",
                "null_rate": 0.0,
                "distinct": 5,
                "min": 0.0,
                "max": 5.0,
                "avg": 0.0,
                "stddev": 1.0,
                "zero_rate": 0.0,
                "negative_rate": 0.0,
            },
        },
    }
    current = {
        "total": 20,
        "columns": {
            "amount": {
                "category": "numeric",
                "null_rate": 20.0,
                "distinct": 10,
                "min": -1.0,
                "max": 10.0,
                "avg": 5.0,
                "stddev": 1.0,
                "zero_rate": 20.0,
                "negative_rate": 20.0,
            },
        },
    }

    ids = {item["check_id"] for item in detect_anomalies(current, previous, checks)}
    assert ids == {"A1", "A2", "A3", "A4", "A5", "A5_C5"}


def test_c3_compares_bigint_and_decimal_boundaries_without_float_rounding():
    checks = _checks(C3=None)
    previous = {
        "total": 1,
        "columns": {
            "large": {
                "category": "numeric",
                "min": 9_007_199_254_740_992,
                "max": 9_007_199_254_740_992,
            },
            "precise": {
                "category": "numeric",
                "min": "12345678901234567890.1234567890",
                "max": "12345678901234567890.1234567890",
            },
        },
    }
    current = {
        "total": 1,
        "columns": {
            "large": {
                "category": "numeric",
                "min": 9_007_199_254_740_993,
                "max": 9_007_199_254_740_993,
            },
            "precise": {
                "category": "numeric",
                "min": "12345678901234567890.1234567889",
                "max": "12345678901234567890.1234567891",
            },
        },
    }

    c3 = [
        item for item in detect_anomalies(current, previous, checks)
        if item["check_id"] == "A5"
    ]
    assert {item["column"] for item in c3} == {"large", "precise"}
