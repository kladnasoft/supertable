from __future__ import annotations

from dataclasses import dataclass
import json
import time

import fakeredis
import pandas as pd
import pytest

from supertable.quality import scheduler


ORG = "dq-org"
SUPER = "dq-lake"
TABLE = "facts"


class MemoryDQConfig:
    def __init__(self, *, checks=None, latest=None, columns=None, rules=None, table=None):
        self.checks = checks or {}
        self.latest = latest
        self.columns = dict(columns or {})
        self.rules = list(rules or [])
        self.table = table or {}
        self.anomalies = []

    def get_effective_config(self, _table):
        return {"checks": self.checks, **self.table}

    def get_latest(self, _table):
        return self.latest

    def set_latest(self, _table, value):
        self.latest = value
        return True

    def get_latest_column(self, _table, column):
        return self.columns.get(column)

    def set_latest_column(self, _table, column, value):
        self.columns[column] = value
        return True

    def set_anomalies(self, _table, value):
        self.anomalies = value
        return True

    def list_rules_for_table(self, _table):
        return self.rules


class FakeMetaReader:
    schema = {"amount": "BIGINT", "label": "VARCHAR"}
    stats = [{
        "last_updated_ms": 1_786_665_600_000,
        "resources": [{"file_size": 40}, {"file_size": 60}],
    }]

    def __init__(self, **_kwargs):
        pass

    def get_table_schema(self, _table, _role):
        return [dict(self.schema)]

    def get_table_stats(self, _table, _role):
        return self.stats


@dataclass
class FakeExecution:
    frame: pd.DataFrame
    ok: bool = True
    status: str = "ok"
    message: str | None = None

    def require_success(self):
        if not self.ok:
            raise RuntimeError(self.message or "query failed")
        return self.frame


class SelectiveReadFaultRedis:
    def __init__(self, inner, *, get_key=None, smembers=False):
        self.inner = inner
        self.get_key = get_key
        self.fail_smembers = smembers

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def get(self, key):
        if self.get_key is not None and key == self.get_key:
            raise RuntimeError("injected GET failure")
        return self.inner.get(key)

    def smembers(self, key):
        if self.fail_smembers:
            raise RuntimeError("injected SMEMBERS failure")
        return self.inner.smembers(key)


@pytest.fixture
def redis_client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def fake_meta(monkeypatch):
    FakeMetaReader.schema = {"amount": "BIGINT", "label": "VARCHAR"}
    FakeMetaReader.stats = [{
        "last_updated_ms": int(time.time() * 1000),
        "resources": [{"file_size": 40}, {"file_size": 60}],
    }]
    monkeypatch.setattr("supertable.meta_reader.MetaReader", FakeMetaReader)
    monkeypatch.setattr("supertable.quality.history.write_history", lambda *_a, **_k: True)


def test_notify_ingest_keeps_scalar_marker_and_independent_mode_generations(
    redis_client,
):
    from supertable.quality.config import DQConfig

    config = DQConfig(redis_client, ORG, SUPER)
    assert config.set_schedule({
        "enabled": True,
        "post_ingest": True,
        "post_ingest_quick": True,
        "post_ingest_custom": True,
        "post_ingest_deep": True,
    })

    scheduler.notify_ingest(redis_client, ORG, SUPER, TABLE)

    scalar = redis_client.get(scheduler._pending_key(ORG, SUPER, TABLE))
    assert scalar
    assert {
        redis_client.get(scheduler._pending_key(ORG, SUPER, TABLE, mode))
        for mode in scheduler.QUALITY_MODES
    } == {scalar}
    assert 0 < redis_client.ttl(scheduler._pending_key(ORG, SUPER, TABLE)) <= 600
    assert all(
        redis_client.ttl(scheduler._pending_key(ORG, SUPER, TABLE, mode)) == -1
        for mode in scheduler.QUALITY_MODES
    )


def test_pending_generation_compare_delete_never_loses_new_ingest(redis_client):
    key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(key, "new-generation")

    assert not scheduler._consume_pending_generation(
        redis_client, key, "old-generation",
    )
    assert redis_client.get(key) == "new-generation"
    assert scheduler._consume_pending_generation(
        redis_client, key, "new-generation",
    )
    assert redis_client.get(key) is None


def test_ambiguous_script_error_never_falls_back_to_non_atomic_lock_delete():
    class AmbiguousRedis:
        def __init__(self):
            self.value = "owner"
            self.get_called = False
            self.delete_called = False

        def eval(self, *_args, **_kwargs):
            raise ConnectionError("reply lost after unknown server state")

        def get(self, _key):
            self.get_called = True
            # The historical fallback could observe owner, then race with
            # this successor before DEL.
            self.value = "successor"
            return "owner"

        def delete(self, _key):
            self.delete_called = True
            self.value = None
            return 1

    redis = AmbiguousRedis()

    assert not scheduler._delete_if_value(redis, "running", "owner")
    assert redis.value == "owner"
    assert redis.get_called is False
    assert redis.delete_called is False


def test_success_cooldown_is_per_mode_and_failure_uses_retry_backoff(
    redis_client,
    monkeypatch,
):
    dqc = MemoryDQConfig()
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_a, **_k: scheduler._success("quick"),
    )
    monkeypatch.setattr(
        scheduler,
        "_run_custom_check",
        lambda *_a, **_k: scheduler._success("custom"),
    )
    monkeypatch.setattr(
        scheduler,
        "_run_deep_check",
        lambda *_a, **_k: scheduler._failed("deep", "engine error"),
    )

    quick = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "quick", dqc, 300,
    )
    custom = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "custom", dqc, 300,
    )
    deep = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "deep", dqc, 300,
    )

    assert quick.successful and custom.successful
    assert deep.state == "failed"
    assert redis_client.exists(scheduler._cooldown_key(ORG, SUPER, TABLE, "quick"))
    assert redis_client.exists(scheduler._cooldown_key(ORG, SUPER, TABLE, "custom"))
    assert not redis_client.exists(scheduler._cooldown_key(ORG, SUPER, TABLE, "deep"))
    assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, "deep"))
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))


def test_quick_sql_error_status_is_failure_and_never_published(
    redis_client,
    fake_meta,
    monkeypatch,
):
    dqc = MemoryDQConfig()
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: FakeExecution(
            pd.DataFrame(), ok=False, status="error", message="binder error",
        ),
    )

    outcome = scheduler._run_quick_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.state == "failed"
    assert "binder error" in outcome.message
    assert dqc.latest is None


def test_quick_counts_only_quick_checks_and_preserves_deep_column_data(
    redis_client,
    fake_meta,
    monkeypatch,
):
    dqc = MemoryDQConfig(
        checks={
            "T2": {"enabled": True, "threshold": 24},
            "T5": {"enabled": True, "threshold": 50},
            "C1": {"enabled": True, "threshold": 5},
            "D1": {"enabled": True, "threshold": None},
        },
        columns={"amount": {"deep": {"sentinel": "preserve"}}},
    )
    frame = pd.DataFrame([{
        "__total": 2,
        "__present_amount": 2,
        "__distinct_amount": 2,
        "__min_amount": 1.0,
        "__max_amount": 2.0,
        "__avg_amount": 1.5,
        "__stddev_amount": 0.7071,
        "__zero_amount": 0,
        "__neg_amount": 0,
        "__present_label": 2,
        "__distinct_label": 2,
    }])
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: FakeExecution(frame),
    )

    outcome = scheduler._run_quick_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.successful
    quick = dqc.latest["mode_results"]["quick"]
    # T2/T5 are table evaluations; C1 is evaluated independently for each of
    # the two visible columns.
    assert quick["total_checks"] == 4
    assert quick["passed"] == 1
    assert quick["skipped"] == 3
    assert dqc.columns["amount"]["deep"] == {"sentinel": "preserve"}
    assert dqc.latest["configured_checks"] == (
        dqc.latest["passed"]
        + dqc.latest["warnings"]
        + dqc.latest["critical"]
        + dqc.latest["errors"]
        + dqc.latest["skipped"]
    )


def test_legacy_hidden_incremental_config_is_forced_to_full_public_scan(
    redis_client,
    fake_meta,
    monkeypatch,
):
    FakeMetaReader.schema = {
        "amount": "BIGINT",
        "__timestamp__": "TIMESTAMP",
    }
    dqc = MemoryDQConfig(table={
        "scope": "incremental",
        "incremental_column": "__timestamp__",
    })
    sql_seen = []
    frame = pd.DataFrame([{
        "__total": 1,
        "__present_amount": 1,
        "__distinct_amount": 1,
        "__min_amount": 2.0,
        "__max_amount": 2.0,
        "__avg_amount": 2.0,
        "__stddev_amount": None,
        "__zero_amount": 0,
        "__neg_amount": 0,
    }])

    def execute(_org, _sup, sql):
        sql_seen.append(sql)
        return FakeExecution(frame)

    monkeypatch.setattr(scheduler, "_execute_quality_statement", execute)

    outcome = scheduler._run_quick_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.successful
    assert len(sql_seen) == 1
    assert '"__timestamp__"' not in sql_seen[0]
    assert "\nWHERE " not in sql_seen[0]
    assert dqc.latest["incremental_column"] is None
    assert dqc.latest["incremental_watermarks"] == {}


def test_custom_sql_error_cannot_be_evaluated_as_zero_violations(
    redis_client,
    fake_meta,
    monkeypatch,
):
    dqc = MemoryDQConfig(rules=[{
        "rule_id": "limit",
        "rule_type": "column_max",
        "table_name": TABLE,
        "column_name": "amount",
        "threshold": 10,
        "severity": "warning",
    }])
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: FakeExecution(
            pd.DataFrame(), ok=False, status="error", message="scan failed",
        ),
    )

    outcome = scheduler._run_custom_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.state == "failed"
    assert "scan failed" in outcome.message
    assert dqc.latest is None


def test_deep_baseline_is_skipped_not_passed_and_preserves_quick_mode(
    redis_client,
    fake_meta,
    monkeypatch,
):
    quick_mode = {
        "checked_at": "q",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [],
        "outcomes": [],
    }
    dqc = MemoryDQConfig(
        checks={"D5": {"enabled": True, "threshold": 30}},
        latest={
            "checked_at": "q",
            "check_type": "quick",
            "parsed": {"total": 2, "columns": {}},
            "mode_results": {"quick": quick_mode},
        },
    )
    numeric = pd.DataFrame([{
        "total_rows": 2,
        "non_nulls": 2,
        "distinct_vals": 2,
        "p25_value": 1.25,
        "p75_value": 1.75,
    }])
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: FakeExecution(numeric),
    )

    outcome = scheduler._run_deep_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.successful
    assert set(dqc.latest["mode_results"]) == {"quick", "deep"}
    deep = dqc.latest["mode_results"]["deep"]
    # D5/numeric has no prior IQR yet; D5/string is explicitly N/A.
    assert deep["passed"] == 0
    assert deep["skipped"] == 2
    assert deep["not_applicable"] == 1
    assert dqc.latest["configured_checks"] == 3


def test_mode_publication_preserves_other_modes_and_counter_invariant():
    dqc = MemoryDQConfig()
    quick = {
        "checked_at": "quick-time",
        "total_checks": 3,
        "evaluated": 2,
        "passed": 1,
        "warnings": 1,
        "critical": 0,
        "errors": 0,
        "skipped": 1,
        "anomalies": [{"check_id": "A1", "severity": "warning"}],
        "outcomes": [],
    }
    custom = {
        "checked_at": "custom-time",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "anomalies": [],
        "outcomes": [],
        "rule_results": [{"rule_id": "r1", "status": "ok"}],
    }

    first = scheduler._publish_mode_latest(
        dqc,
        TABLE,
        "quick",
        quick,
        base_updates={"parsed": {"total": 1, "columns": {}}},
    )
    final = scheduler._publish_mode_latest(dqc, TABLE, "custom", custom)

    assert first is not None and final is not None
    assert set(final["mode_results"]) == {"quick", "custom"}
    assert final["rule_results"] == custom["rule_results"]
    assert final["configured_checks"] == 4
    assert final["configured_checks"] == (
        final["passed"]
        + final["warnings"]
        + final["critical"]
        + final["errors"]
        + final["skipped"]
    )


def test_incomplete_resource_sizes_are_not_reported_as_zero():
    class Metadata:
        def get_table_stats(self, *_args):
            return [{
                "last_updated_ms": 1_786_665_600_000,
                "resources": [{"file_size": 20}, {}],
            }]

    modified, size = scheduler._table_metadata(Metadata(), TABLE)

    assert modified == 1_786_665_600_000
    assert size is None


def test_snapshot_metadata_reports_only_sealed_live_row_count():
    class Metadata:
        def __init__(self, snapshot):
            self.snapshot = snapshot

        def get_table_stats(self, *_args):
            return [self.snapshot]

    complete = {
        "last_updated_ms": 1_786_665_600_000,
        "resources": [
            {"file_size": 20, "rows": 3},
            {"file_size": 30, "rows": 4},
        ],
        "tombstone": "deletions.parquet",
        "tombstone_rows": 2,
    }
    assert scheduler._table_snapshot_metadata(Metadata(complete), TABLE) == (
        1_786_665_600_000,
        50,
        5,
    )

    ambiguous = dict(complete, tombstone=None, tombstone_rows=2)
    assert scheduler._table_snapshot_metadata(Metadata(ambiguous), TABLE) == (
        1_786_665_600_000,
        50,
        None,
    )


def test_empty_public_schema_is_a_valid_schema_drift_baseline():
    from supertable.quality.anomaly import detect_schema_drift

    checks = {"T3": {"enabled": True, "threshold": None}}
    anomalies = detect_schema_drift(
        [("new_column", "BIGINT")],
        [],
        checks,
    )
    assert len(anomalies) == 1
    assert anomalies[0]["check_id"] == "A_T3"
    assert "added: new_column" in anomalies[0]["message"]

    outcomes = scheduler._quick_profile_outcomes(
        {"total": 1, "columns": {}},
        {"schema": [], "parsed": {"total": 0, "columns": {}}},
        [("new_column", "BIGINT")],
        checks,
        anomalies,
    )
    assert len(outcomes) == 1
    assert outcomes[0]["check_id"] == "T3"
    assert outcomes[0]["status"] == "warning"
    assert outcomes[0]["evaluated"] is True


@pytest.mark.parametrize(
    ("check_id", "previous_columns"),
    [
        (
            "C1",
            {"new_number": {"category": "numeric", "distinct": 2}},
        ),
        (
            "C2",
            {"new_number": {"category": "numeric", "distinct": 0}},
        ),
        (
            "C3",
            {
                "new_number": {
                    "category": "date",
                    "min": "2025-01-01",
                    "max": "2025-01-02",
                },
            },
        ),
        (
            "C5",
            {
                "old_text": {
                    "category": "string",
                    "null_rate": 0.0,
                    "distinct": 2,
                },
            },
        ),
        (
            "C6",
            {
                "new_number": {
                    "category": "numeric",
                    "avg": float("nan"),
                    "stddev": 1.0,
                },
            },
        ),
    ],
)
def test_quick_comparisons_never_pass_without_compatible_metric_baseline(
    check_id,
    previous_columns,
):
    current = {
        "total": 2,
        "columns": {
            "new_number": {
                "category": "numeric",
                "null_rate": 0.0,
                "distinct": 2,
                "min": 1.0,
                "max": 2.0,
                "zero_rate": 0.0,
                "negative_rate": 0.0,
                "avg": 1.5,
            },
        },
    }
    previous = {
        "parsed": {"total": 2, "columns": previous_columns},
        "schema": [["new_number", "BIGINT"]],
    }

    outcomes = scheduler._quick_profile_outcomes(
        current,
        previous,
        [("new_number", "BIGINT")],
        {check_id: {"enabled": True, "threshold": None}},
        [],
    )

    assert len(outcomes) == 1
    assert outcomes[0]["check_id"] == check_id
    assert outcomes[0]["status"] == "skipped"
    assert outcomes[0]["evaluated"] is False
    assert outcomes[0]["reason"] == "baseline_unavailable"


@pytest.mark.parametrize(
    "prior_total",
    [float("nan"), float("inf"), float("-inf"), -1, 0, 1.5, "10", True],
)
def test_t1_never_passes_with_invalid_or_empty_prior_row_count(prior_total):
    outcomes = scheduler._quick_profile_outcomes(
        {"total": 10, "columns": {}},
        {"parsed": {"total": prior_total, "columns": {}}},
        [],
        {"T1": {"enabled": True, "threshold": 30}},
        [],
    )

    assert outcomes == [{
        "check_id": "T1",
        "status": "skipped",
        "applicable": True,
        "evaluated": False,
        "message": "Baseline recorded for T1; comparison starts on the next run",
        "threshold": 30,
        "column": None,
        "reason": "baseline_unavailable",
    }]


def test_c3_never_passes_with_unparseable_prior_date_extremum():
    current = {
        "total": 1,
        "columns": {
            "event_time": {
                "category": "date",
                "min": "2026-08-15T10:00:00Z",
                "max": "2026-08-15T10:00:00Z",
            },
        },
    }
    previous = {
        "parsed": {
            "total": 1,
            "columns": {
                "event_time": {
                    "category": "date",
                    "min": "not-a-date",
                    "max": "2026-08-14T10:00:00Z",
                },
            },
        },
    }

    outcomes = scheduler._quick_profile_outcomes(
        current,
        previous,
        [("event_time", "TIMESTAMP")],
        {"C3": {"enabled": True, "threshold": None}},
        [],
    )

    assert len(outcomes) == 1
    assert outcomes[0]["check_id"] == "C3"
    assert outcomes[0]["status"] == "skipped"
    assert outcomes[0]["evaluated"] is False
    assert outcomes[0]["reason"] == "baseline_unavailable"


def test_quick_comparison_outcomes_are_isolated_by_check_and_column():
    names = ("alerting", "quiet", "new_column", "corrupt_baseline")

    def profile(name):
        return {
            "column_name": name,
            "category": "numeric",
            "null_rate": 5.0,
            "distinct": 10,
            "min": 1,
            "max": 10,
            "uniqueness": 50.0,
            "zero_rate": 1.0,
            "negative_rate": 2.0,
            "avg": 5.0,
            "stddev": 1.0,
            "moments_certified": True,
        }

    current_columns = {name: profile(name) for name in names}
    previous_columns = {
        "alerting": profile("alerting"),
        "quiet": profile("quiet"),
        "corrupt_baseline": {
            **profile("corrupt_baseline"),
            "null_rate": float("nan"),
            "distinct": 0,
            "min": "not-a-number",
            "zero_rate": float("inf"),
        },
    }
    checks = {
        "C1": {"enabled": True, "threshold": 5},
        "C2": {"enabled": True, "threshold": 50},
        "C3": {"enabled": True, "threshold": None},
        "C5": {"enabled": True, "threshold": 5},
    }
    anomalies = [
        {
            "check_id": anomaly_id,
            "column": "alerting",
            "severity": "warning",
            "message": f"alerting {check_id}",
        }
        for check_id, anomaly_id in (
            ("C1", "A2"),
            ("C2", "A4"),
            ("C3", "A5"),
            ("C5", "A5_C5"),
        )
    ]

    outcomes = scheduler._quick_profile_outcomes(
        {"total": 10, "columns": current_columns},
        {"parsed": {"total": 10, "columns": previous_columns}},
        [(name, "BIGINT") for name in names],
        checks,
        anomalies,
    )
    by_key = {(item["check_id"], item["column"]): item for item in outcomes}

    assert len(outcomes) == len(checks) * len(names)
    for check_id in checks:
        assert by_key[(check_id, "alerting")]["status"] == "warning"
        assert by_key[(check_id, "alerting")]["evaluated"] is True
        assert by_key[(check_id, "quiet")]["status"] == "ok"
        assert by_key[(check_id, "quiet")]["evaluated"] is True
        assert by_key[(check_id, "new_column")]["status"] == "skipped"
        assert by_key[(check_id, "new_column")]["reason"] == "baseline_unavailable"
        assert by_key[(check_id, "corrupt_baseline")]["status"] == "skipped"
        assert by_key[(check_id, "corrupt_baseline")]["reason"] == "baseline_unavailable"


def test_c6_mixed_certified_wide_new_and_corrupt_columns_are_independent():
    names = ("alerting", "quiet", "new_column", "corrupt_baseline", "wide")

    def profile(name):
        return {
            "column_name": name,
            "category": "numeric",
            "avg": 10.0,
            "stddev": 2.0,
            "moments_certified": True,
        }

    current_columns = {name: profile(name) for name in names}
    current_columns["wide"] = {
        **profile("wide"),
        "avg": None,
        "stddev": None,
        "moments_certified": False,
    }
    previous_columns = {
        "alerting": profile("alerting"),
        "quiet": profile("quiet"),
        "corrupt_baseline": {
            **profile("corrupt_baseline"),
            "stddev": float("nan"),
        },
        "wide": profile("wide"),
    }

    outcomes = scheduler._quick_profile_outcomes(
        {"total": 10, "columns": current_columns},
        {"parsed": {"total": 10, "columns": previous_columns}},
        [(name, "DECIMAL(38, 10)") for name in names],
        {"C6": {"enabled": True, "threshold": 2}},
        [{
            "check_id": "A3",
            "column": "alerting",
            "severity": "critical",
            "message": "alerting mean drift",
        }],
    )
    by_column = {item["column"]: item for item in outcomes}

    assert by_column["alerting"]["status"] == "critical"
    assert by_column["quiet"]["status"] == "ok"
    assert by_column["new_column"]["reason"] == "baseline_unavailable"
    assert by_column["corrupt_baseline"]["reason"] == "baseline_unavailable"
    assert by_column["wide"]["reason"] == "uncertified_precision"
    assert scheduler._summary_from_outcomes(outcomes) == {
        "total_checks": 5,
        "evaluated": 2,
        "passed": 1,
        "warnings": 0,
        "critical": 1,
        "errors": 0,
        "skipped": 3,
        "not_applicable": 0,
    }


def test_quick_tracking_and_no_applicable_checks_have_bounded_outcomes():
    outcomes = scheduler._quick_profile_outcomes(
        {
            "total": 2,
            "columns": {
                "valid": {"category": "string", "uniqueness": 50.0},
                "missing": {"category": "string", "uniqueness": None},
            },
        },
        None,
        [("valid", "VARCHAR"), ("missing", "VARCHAR")],
        {
            "C3": {"enabled": True, "threshold": None},
            "C4": {"enabled": True, "threshold": None},
            "C5": {"enabled": True, "threshold": 5},
            "C6": {"enabled": True, "threshold": 2},
        },
        [],
    )
    grouped = {
        check_id: [item for item in outcomes if item["check_id"] == check_id]
        for check_id in ("C3", "C4", "C5", "C6")
    }

    assert [(item["column"], item["status"]) for item in grouped["C4"]] == [
        ("valid", "ok"),
        ("missing", "skipped"),
    ]
    for check_id in ("C3", "C5", "C6"):
        assert len(grouped[check_id]) == 1
        assert grouped[check_id][0]["column"] is None
        assert grouped[check_id][0]["status"] == "not_applicable"


def test_legacy_pending_migration_is_persistent_and_never_drops_a_conflict(
    redis_client,
):
    scalar_key = scheduler._pending_key(ORG, SUPER, TABLE)
    quick_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    custom_key = scheduler._pending_key(ORG, SUPER, TABLE, "custom")
    redis_client.set(scalar_key, "legacy", ex=600)
    redis_client.set(quick_key, "newer")

    scheduler._migrate_legacy_pending(
        redis_client, ORG, SUPER, TABLE, ("quick", "custom"),
    )

    assert redis_client.get(scalar_key) == "legacy"
    assert redis_client.get(quick_key) == "newer"
    assert redis_client.get(custom_key) == "legacy"
    assert redis_client.ttl(custom_key) == -1

    assert scheduler._consume_pending_generation(redis_client, quick_key, "newer")
    scheduler._migrate_legacy_pending(
        redis_client, ORG, SUPER, TABLE, ("quick", "custom"),
    )
    assert redis_client.get(scalar_key) is None
    assert redis_client.get(quick_key) == "legacy"
    assert redis_client.ttl(quick_key) == -1


def test_failed_attempt_is_visible_without_destroying_last_success_and_clears(
    redis_client,
    monkeypatch,
):
    prior_mode = {
        "checked_at": "prior-success",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [],
        "outcomes": [],
    }
    dqc = MemoryDQConfig(latest={
        "checked_at": "prior-success",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "status": "ok",
        "mode_results": {"quick": prior_mode},
    })
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_a, **_k: scheduler._failed("quick", "binder exploded"),
    )

    failed = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "quick", dqc, 30,
    )

    assert failed.state == "failed"
    assert dqc.latest["mode_results"]["quick"] == prior_mode
    assert dqc.latest["mode_attempts"]["quick"]["state"] == "failed"
    assert dqc.latest["mode_attempts"]["quick"]["message"] == "binder exploded"
    assert dqc.latest["failed_modes"] == ["quick"]
    assert dqc.latest["results_stale"] is True
    assert dqc.latest["status"] == "error"
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )

    redis_client.delete(scheduler._retry_key(ORG, SUPER, TABLE, "quick"))
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_a, **_k: scheduler._success("quick", evaluated=1, passed=1),
    )
    succeeded = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "quick", dqc, 30,
    )

    assert succeeded.successful
    assert dqc.latest["mode_results"]["quick"] == prior_mode
    assert dqc.latest["mode_attempts"]["quick"]["state"] == "success"
    assert dqc.latest["failed_modes"] == []
    assert dqc.latest["results_stale"] is False
    assert dqc.latest["status"] == "ok"


def test_disabling_last_deep_check_publishes_empty_mode_and_clears_columns(
    redis_client,
    fake_meta,
):
    quick_mode = {
        "checked_at": "q",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [],
        "outcomes": [],
    }
    stale_deep = {
        "checked_at": "old-deep",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 0,
        "warnings": 0,
        "critical": 1,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [{"check_id": "D5", "severity": "critical"}],
        "outcomes": [],
    }
    dqc = MemoryDQConfig(
        checks={"D5": {"enabled": False, "threshold": 30}},
        latest={
            "checked_at": "q",
            "parsed": {"total": 2, "columns": {}},
            "mode_results": {"quick": quick_mode, "deep": stale_deep},
        },
        columns={
            "amount": {
                "checked_at": "q",
                "deep": {"sentinel": "stale"},
                "deep_checked_at": "old-deep",
            },
        },
    )

    outcome = scheduler._run_deep_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.successful
    assert dqc.latest["mode_results"]["deep"]["total_checks"] == 0
    assert dqc.latest["mode_results"]["deep"]["anomalies"] == []
    assert dqc.latest["status"] == "ok"
    assert "deep" not in dqc.columns["amount"]
    assert "deep_checked_at" not in dqc.columns["amount"]


def test_removing_last_custom_rule_publishes_empty_mode_and_clears_stale_data(
    redis_client,
    fake_meta,
):
    stale_custom = {
        "checked_at": "old-custom",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 0,
        "warnings": 0,
        "critical": 1,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [{"check_id": "R_old", "severity": "critical"}],
        "outcomes": [{"rule_id": "old", "status": "critical"}],
        "rule_results": [{"rule_id": "old", "status": "critical"}],
    }
    dqc = MemoryDQConfig(
        latest={
            "checked_at": "q",
            "parsed": {"total": 2, "columns": {}},
            "mode_results": {"custom": stale_custom},
            "rule_results": stale_custom["rule_results"],
        },
        columns={
            "amount": {
                "custom": {"old": True},
                "custom_checked_at": "old-custom",
                "custom_rule_results": [{"rule_id": "old"}],
            },
        },
        rules=[],
    )

    outcome = scheduler._run_custom_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )

    assert outcome.successful
    assert dqc.latest["mode_results"]["custom"]["total_checks"] == 0
    assert dqc.latest["rule_results"] == []
    assert dqc.latest["anomalies"] == []
    assert dqc.latest["status"] == "ok"
    assert dqc.columns["amount"] == {}


def test_scheduler_reaches_empty_mode_cleanup_after_last_checks_are_removed(
    redis_client,
    monkeypatch,
):
    class ScheduledConfig(MemoryDQConfig):
        def get_schedule(self):
            return {
                "enabled": True,
                "post_ingest": False,
                "quick_cron": "*/1 * * * *",
                "deep_cron": "*/1 * * * *",
                "custom_cron": "*/1 * * * *",
                "cooldown_seconds": 30,
            }

        def get_table_schedule(self, _table):
            return None

    dqc = ScheduledConfig(
        checks={"D5": {"enabled": False}},
        latest={
            "mode_results": {
                "deep": {
                    "checked_at": "old-deep",
                    "total_checks": 1,
                    "disabled": False,
                },
                "custom": {
                    "checked_at": "old-custom",
                    "total_checks": 1,
                    "rule_results": [{"rule_id": "removed"}],
                },
            },
        },
        rules=[],
    )
    attempted = []
    monkeypatch.setattr(
        "supertable.redis_connector.create_redis_client",
        lambda: redis_client,
    )
    monkeypatch.setattr(
        "supertable.quality.config.DQConfig",
        lambda *_args, **_kwargs: dqc,
    )
    monkeypatch.setattr(scheduler, "_discover_dq_pairs", lambda _r: [(ORG, SUPER)])
    monkeypatch.setattr(scheduler, "_list_tables", lambda *_a: [TABLE])
    monkeypatch.setattr(
        scheduler,
        "_try_run_check",
        lambda _r, _o, _s, _t, mode, _d, _c, **_kwargs: (
            attempted.append(mode) or scheduler._success(mode)
        ),
    )

    scheduler._scheduler_tick({}, {}, {})

    assert attempted == ["quick", "deep", "custom"]


def test_history_projection_uses_only_requested_mode_timestamp_and_counters():
    latest = {
        "checked_at": "quick-time",
        "total_checks": 9,
        "passed": 7,
        "warnings": 1,
        "critical": 1,
        "anomalies": [{"check_id": "quick"}, {"check_id": "deep"}],
        "rule_results": [{"rule_id": "unrelated"}],
        "mode_results": {
            "quick": {
                "checked_at": "quick-time",
                "total_checks": 7,
                "evaluated": 7,
                "passed": 7,
                "warnings": 0,
                "critical": 0,
                "errors": 0,
                "skipped": 0,
                "not_applicable": 0,
                "anomalies": [],
            },
            "deep": {
                "checked_at": "deep-time",
                "total_checks": 2,
                "evaluated": 2,
                "passed": 0,
                "warnings": 1,
                "critical": 1,
                "errors": 0,
                "skipped": 0,
                "not_applicable": 0,
                "anomalies": [{"check_id": "deep"}],
            },
        },
    }

    projected = scheduler._mode_history_document(latest, "deep")

    assert projected["checked_at"] == "deep-time"
    assert projected["total_checks"] == 2
    assert projected["passed"] == 0
    assert projected["warnings"] == 1
    assert projected["critical"] == 1
    assert projected["anomalies"] == [{"check_id": "deep"}]
    assert projected["rule_results"] == []
    assert projected["status"] == "critical"


def test_lease_loss_cannot_cool_down_or_consume_pending_generation(
    redis_client,
    monkeypatch,
):
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "old-generation")
    dqc = MemoryDQConfig()

    def lose_lease(*_args, lease_guard=None, **_kwargs):
        assert lease_guard is not None
        redis_client.set(lease_guard.key, "successor", ex=300)
        return scheduler._success("quick")

    monkeypatch.setattr(scheduler, "_run_quick_check", lose_lease)

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        300,
        pending_generation="old-generation",
    )

    assert outcome.state == "failed"
    assert "lease" in outcome.message
    assert redis_client.get(pending_key) == "old-generation"
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )
    assert redis_client.get(scheduler._running_key(ORG, SUPER, TABLE)) == "successor"


def test_success_finalization_consumes_only_the_generation_it_started_with(
    redis_client,
    monkeypatch,
):
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "old-generation")
    dqc = MemoryDQConfig()

    def concurrent_ingest(*_args, **_kwargs):
        redis_client.set(pending_key, "new-generation")
        return scheduler._success("quick")

    monkeypatch.setattr(scheduler, "_run_quick_check", concurrent_ingest)

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        300,
        pending_generation="old-generation",
    )

    assert outcome.successful
    assert redis_client.get(pending_key) == "new-generation"
    assert redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )


def test_success_marker_is_durable_before_cooldown_and_pending_finalization(
    redis_client,
    monkeypatch,
):
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation")
    dqc = MemoryDQConfig(latest={
        "status": "error",
        "results_stale": True,
        "failed_modes": ["quick"],
        "mode_attempts": {
            "quick": {
                "state": "failed",
                "message": "old binder failure",
            },
        },
    })
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_a, **_k: scheduler._success("quick", evaluated=1, passed=1),
    )
    original_finalize = scheduler._finalize_success_if_owned

    def finalize_then_reacquire(r, guard, *args, **kwargs):
        finalized = original_finalize(r, guard, *args, **kwargs)
        assert finalized
        # Reproduce ownership disappearing immediately after cooldown and
        # exact-generation consumption have committed.
        r.set(guard.key, "successor", ex=300)
        return True

    monkeypatch.setattr(
        scheduler,
        "_finalize_success_if_owned",
        finalize_then_reacquire,
    )

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        300,
        pending_generation="generation",
    )

    assert outcome.successful
    assert redis_client.get(pending_key) is None
    assert redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )
    assert dqc.latest["mode_attempts"]["quick"]["state"] == "success"
    assert dqc.latest["failed_modes"] == []
    assert dqc.latest["results_stale"] is False
    assert dqc.latest["status"] == "ok"
    assert redis_client.get(scheduler._running_key(ORG, SUPER, TABLE)) == "successor"


def test_renewal_failure_marks_lease_lost(monkeypatch):
    class OneRenewal:
        def wait(self, _interval):
            return False

    lost = scheduler.threading.Event()
    monkeypatch.setattr(scheduler, "_expire_if_value", lambda *_a, **_k: False)

    scheduler._renew_running_lease(
        object(), "running", "token", OneRenewal(), lost,
    )

    assert lost.is_set()


def test_lease_thread_start_failure_releases_lock_and_records_attempt(
    redis_client,
    monkeypatch,
):
    class BrokenThread:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            raise RuntimeError("thread quota")

    dqc = MemoryDQConfig()
    monkeypatch.setattr(scheduler.threading, "Thread", BrokenThread)

    outcome = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "quick", dqc, 30,
    )

    assert outcome.state == "failed"
    assert "thread quota" in outcome.message
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))
    assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, "quick"))
    assert dqc.latest["mode_attempts"]["quick"]["state"] == "failed"


def test_atomic_result_bundle_refuses_publication_after_lease_reacquisition(
    redis_client,
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    dqc = DQConfig(redis_client, ORG, SUPER)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner", ex=300)
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")
    original_get_latest = dqc.get_latest

    def reacquire_after_read(table_name):
        current = original_get_latest(table_name)
        redis_client.set(running_key, "successor", ex=300)
        return current

    monkeypatch.setattr(dqc, "get_latest", reacquire_after_read)
    record = {
        "checked_at": "now",
        "total_checks": 1,
        "evaluated": 1,
        "passed": 1,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "anomalies": [],
        "outcomes": [],
    }

    with pytest.raises(scheduler._LeaseLostError):
        scheduler._publish_mode_latest(
            dqc,
            TABLE,
            "quick",
            record,
            lease_guard=guard,
        )

    assert redis_client.get(dqc._key("latest", TABLE)) is None
    assert redis_client.get(dqc._key("anomalies", TABLE)) is None


def test_transient_schedule_read_during_ingest_preserves_unresolved_generation(
    redis_client,
):
    from supertable.quality.config import DQConfig

    healthy = DQConfig(redis_client, ORG, SUPER)
    assert healthy.set_schedule({
        "enabled": True,
        "post_ingest": True,
        "post_ingest_quick": True,
        "post_ingest_custom": False,
        "post_ingest_deep": False,
    })
    fault = SelectiveReadFaultRedis(
        redis_client,
        get_key=healthy._key("schedule"),
    )

    scheduler.notify_ingest(fault, ORG, SUPER, TABLE)

    unresolved_key = scheduler._unresolved_pending_key(ORG, SUPER, TABLE)
    generation = redis_client.get(unresolved_key)
    assert generation is not None
    assert redis_client.ttl(unresolved_key) == -1
    assert redis_client.get(
        scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    ) is None

    scheduler._resolve_unresolved_pending(
        redis_client, ORG, SUPER, TABLE, ("quick",),
    )
    assert redis_client.get(unresolved_key) is None
    assert redis_client.get(
        scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    ) == generation


def test_latest_read_uncertainty_preserves_prior_modes_and_only_sets_retry(
    redis_client,
    fake_meta,
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    healthy = DQConfig(redis_client, ORG, SUPER)
    latest_key = healthy._key("latest", TABLE)
    prior = {
        "checked_at": "prior",
        "mode_results": {
            "deep": {"checked_at": "d", "total_checks": 1},
            "custom": {"checked_at": "c", "total_checks": 1},
        },
    }
    raw_prior = json.dumps(prior)
    redis_client.set(latest_key, raw_prior)
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation")
    fault = SelectiveReadFaultRedis(redis_client, get_key=latest_key)
    dqc = DQConfig(fault, ORG, SUPER)
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("query must not execute after uncertain latest read")
        ),
    )

    outcome = scheduler._try_run_check(
        fault,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        30,
        pending_generation="generation",
    )

    assert outcome.state == "failed"
    assert redis_client.get(latest_key) == raw_prior
    assert redis_client.get(pending_key) == "generation"
    assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, "quick"))
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )


def test_success_merge_read_uncertainty_preserves_result_and_denies_cooldown(
    redis_client,
    fake_meta,
    monkeypatch,
):
    from supertable.quality.config import DQConfig, DQConfigReadError

    dqc = DQConfig(redis_client, ORG, SUPER)
    original_get_latest = dqc.get_latest
    latest_reads = 0

    def fail_third_latest_read(table_name):
        nonlocal latest_reads
        latest_reads += 1
        if latest_reads == 3:
            raise DQConfigReadError("injected success-merge GET failure")
        return original_get_latest(table_name)

    monkeypatch.setattr(dqc, "get_latest", fail_third_latest_read)
    frame = pd.DataFrame([{
        "__total": 2,
        "__present_amount": 2,
        "__distinct_amount": 2,
        "__min_amount": 1.0,
        "__max_amount": 2.0,
        "__avg_amount": 1.5,
        "__stddev_amount": 0.7071,
        "__zero_amount": 0,
        "__neg_amount": 0,
        "__present_label": 2,
        "__distinct_label": 2,
    }])
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_a, **_k: FakeExecution(frame),
    )

    outcome = scheduler._try_run_check(
        redis_client, ORG, SUPER, TABLE, "quick", dqc, 30,
    )

    assert outcome.state == "failed"
    assert latest_reads >= 4
    latest = original_get_latest(TABLE)
    assert latest["mode_results"]["quick"]["checked_at"]
    assert latest["mode_attempts"]["quick"]["state"] == "failed"
    assert "success-merge GET failure" in latest["last_attempt_error"]
    assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, "quick"))
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )


def test_rule_inventory_failure_is_failed_attempt_not_empty_custom_success(
    redis_client,
):
    from supertable.quality.config import DQConfig

    healthy = DQConfig(redis_client, ORG, SUPER)
    prior = {
        "checked_at": "prior",
        "mode_results": {
            "custom": {
                "checked_at": "c",
                "total_checks": 1,
                "evaluated": 1,
                "passed": 1,
                "warnings": 0,
                "critical": 0,
                "errors": 0,
                "skipped": 0,
                "not_applicable": 0,
                "anomalies": [],
                "rule_results": [{"rule_id": "keep", "status": "ok"}],
            },
        },
    }
    healthy.set_latest(TABLE, prior)
    fault = SelectiveReadFaultRedis(redis_client, smembers=True)
    dqc = DQConfig(fault, ORG, SUPER)

    outcome = scheduler._try_run_check(
        fault, ORG, SUPER, TABLE, "custom", dqc, 30,
    )

    assert outcome.state == "failed"
    latest = healthy.get_latest(TABLE)
    assert latest["mode_results"] == prior["mode_results"]
    assert latest["mode_attempts"]["custom"]["state"] == "failed"
    assert latest["results_stale"] is True
    assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, "custom"))
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "custom")
    )


def test_tick_schedule_read_failure_records_retries_and_no_cooldowns(
    redis_client,
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    healthy = DQConfig(redis_client, ORG, SUPER)
    schedule_key = healthy._key("schedule")
    fault = SelectiveReadFaultRedis(redis_client, get_key=schedule_key)
    monkeypatch.setattr(
        "supertable.redis_connector.create_redis_client", lambda: fault,
    )
    monkeypatch.setattr(scheduler, "_discover_dq_pairs", lambda _r: [(ORG, SUPER)])
    monkeypatch.setattr(scheduler, "_list_tables", lambda *_a: [TABLE])

    scheduler._scheduler_tick({}, {}, {})

    latest = healthy.get_latest(TABLE)
    assert latest["failed_modes"] == ["custom", "deep", "quick"]
    for mode in scheduler.QUALITY_MODES:
        assert redis_client.exists(scheduler._retry_key(ORG, SUPER, TABLE, mode))
        assert not redis_client.exists(
            scheduler._cooldown_key(ORG, SUPER, TABLE, mode)
        )


def test_tick_schedule_failures_are_isolated_between_pairs_and_tables(
    redis_client,
    monkeypatch,
):
    from supertable.quality.config import DQConfigReadError

    pair_reads = []

    class PairConfig(MemoryDQConfig):
        def __init__(self, org):
            super().__init__()
            self.org = org

        def get_schedule(self):
            pair_reads.append(self.org)
            if self.org == "bad-org":
                raise DQConfigReadError("malformed global schedule")
            return {"enabled": False}

    monkeypatch.setattr(
        "supertable.redis_connector.create_redis_client",
        lambda: redis_client,
    )
    monkeypatch.setattr(
        "supertable.quality.config.DQConfig",
        lambda _r, org, _sup: PairConfig(org),
    )
    monkeypatch.setattr(
        scheduler,
        "_discover_dq_pairs",
        lambda _r: [("bad-org", SUPER), ("good-org", SUPER)],
    )
    monkeypatch.setattr(scheduler, "_list_tables", lambda *_a: [])

    scheduler._scheduler_tick({}, {}, {})
    assert pair_reads == ["bad-org", "good-org"]

    table_reads = []

    class TableConfig(MemoryDQConfig):
        def get_schedule(self):
            return {"enabled": True}

        def get_table_schedule(self, table_name):
            table_reads.append(table_name)
            if table_name == "bad-table":
                raise DQConfigReadError("malformed table schedule")
            return {"enabled": False}

    table_config = TableConfig()
    monkeypatch.setattr(
        "supertable.quality.config.DQConfig",
        lambda *_args, **_kwargs: table_config,
    )
    monkeypatch.setattr(
        scheduler, "_discover_dq_pairs", lambda _r: [(ORG, SUPER)],
    )
    monkeypatch.setattr(
        scheduler, "_list_tables", lambda *_a: ["bad-table", "good-table"],
    )
    monkeypatch.setattr(
        scheduler,
        "_record_tick_config_failure",
        lambda *_args, **_kwargs: None,
    )

    scheduler._scheduler_tick({}, {}, {})
    assert table_reads == ["bad-table", "good-table"]


def test_legacy_public_incremental_config_is_forced_to_full_scan_without_where(
    redis_client,
    fake_meta,
    monkeypatch,
):
    FakeMetaReader.schema = {
        "event_time": "TIMESTAMP",
        "amount": "BIGINT",
    }
    dqc = MemoryDQConfig(
        table={
            "scope": "incremental",
            "incremental_column": "event_time",
        },
    )
    sql_seen = []
    frame = pd.DataFrame([{
        "__total": 1,
        "__present_event_time": 1,
        "__distinct_event_time": 1,
        "__min_event_time": "2025-01-03T00:00:00",
        "__max_event_time": "2025-01-03T00:00:00",
        "__present_amount": 1,
        "__distinct_amount": 1,
        "__min_amount": 5.0,
        "__max_amount": 5.0,
        "__avg_amount": 5.0,
        "__stddev_amount": None,
        "__zero_amount": 0,
        "__neg_amount": 0,
    }])

    def execute(_org, _sup, sql):
        sql_seen.append(sql)
        return FakeExecution(frame)

    monkeypatch.setattr(scheduler, "_execute_quality_statement", execute)

    outcome = scheduler._run_quick_check(
        redis_client, ORG, SUPER, TABLE, dqc,
    )
    assert outcome.successful
    assert len(sql_seen) == 1
    assert '"event_time"' in sql_seen[0]
    assert "\nWHERE " not in sql_seen[0]
    assert dqc.latest["incremental_column"] is None
    assert dqc.latest["incremental_watermarks"] == {}
