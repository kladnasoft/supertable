"""Real-write data-quality scheduler and AUTO-engine characterization matrix.

This is deliberately broader than the SQL-builder unit tests.  It writes a
real merge-on-read table (and therefore real private ``__rowid__`` and
``__timestamp__`` storage columns), asks the scheduler to drain one ingest
generation through all three quality modes, and observes the genuine AUTO
engine decisions without replacing query results.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
import json
import uuid

import pyarrow as pa

from supertable.data_writer import DataWriter
from supertable.meta_reader import MetaReader
from supertable.quality import scheduler
from supertable.quality.config import BUILTIN_CHECKS, DQConfig
from supertable.quality.execution import execute_quality_sql
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable


ORG = "quality-scheduler-e2e-org"
SUPER = "quality_scheduler_e2e_lake"
ROLE = "superadmin"
ALL_BUILTIN_IDS = frozenset(BUILTIN_CHECKS)


def _new_table_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _all_checks_enabled() -> dict:
    return {
        "checks": {
            check_id: {
                "enabled": True,
                "threshold": definition.get("threshold"),
            }
            for check_id, definition in BUILTIN_CHECKS.items()
        }
    }


def _rows() -> pa.Table:
    start = datetime(2026, 8, 14, 8, tzinfo=timezone.utc)
    values = [
        (1, -5, "alpha"),
        (2, 0, "beta"),
        (3, 3, "alpha"),
        (4, 5, "unknown"),
        (5, 8, "beta"),
        (6, 13, None),
        (7, 21, "alpha"),
        (8, 34, "beta"),
        (9, 55, "unknown"),
        (10, 89, "alpha"),
        (11, None, "beta"),
        (12, 1, "alpha"),
    ]
    return pa.Table.from_pylist(
        [
            {
                "id": row_id,
                "amount": amount,
                "ratio": None if amount is None else amount / 10.0,
                "label": label,
                "segment": "primary" if row_id % 2 else "secondary",
                "event_time": start + timedelta(minutes=row_id),
                "local_time": time(hour=row_id % 24, minute=row_id % 60),
                "elapsed": timedelta(seconds=row_id),
            }
            for row_id, amount, label in values
        ],
        schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.int64()),
            pa.field("ratio", pa.float64()),
            pa.field("label", pa.string()),
            pa.field("segment", pa.dictionary(pa.int8(), pa.string())),
            pa.field("event_time", pa.timestamp("us", tz="UTC")),
            pa.field("local_time", pa.time64("us")),
            pa.field("elapsed", pa.duration("us")),
        ]),
    )


def _configure_all_modes(dqc: DQConfig) -> None:
    assert dqc.set_global_config(_all_checks_enabled(), updated_by="e2e")
    assert dqc.set_schedule({
        "enabled": True,
        "post_ingest": True,
        "post_ingest_quick": True,
        "post_ingest_deep": True,
        "post_ingest_custom": True,
        "quick_cron": "0 */4 * * *",
        "deep_cron": "0 2 * * *",
        "custom_cron": "0 */6 * * *",
        "cooldown_seconds": 180,
    })


def _add_valid_custom_rules(dqc: DQConfig, table: str) -> None:
    fqn = f"{SUPER}.{table}"
    rules = [
        {
            "rule_type": "column_min",
            "table_name": table,
            "column_name": "amount",
            "threshold": -100,
            "description": "amount floor",
        },
        {
            "rule_type": "column_max",
            "table_name": table,
            "column_name": "amount",
            "threshold": 100,
            "description": "amount ceiling",
        },
        {
            "rule_type": "null_rate_max",
            "table_name": table,
            "column_name": "amount",
            "threshold": 20,
            "description": "amount completeness",
        },
        {
            "rule_type": "row_count_min",
            "table_name": table,
            "threshold": 10,
            "description": "minimum volume",
        },
        {
            "rule_type": "distinct_in",
            "table_name": table,
            "column_name": "label",
            "expected_values": ["alpha", "beta", "unknown"],
            "description": "known labels",
        },
        {
            "rule_type": "custom_sql",
            "table_name": table,
            "sql": (
                f"SELECT COUNT(*) AS violations FROM {fqn} "
                'WHERE "amount" < -100'
            ),
            "threshold": 0,
            "description": "custom amount floor",
        },
    ]
    for rule in rules:
        created = dqc.create_rule(deepcopy(rule), created_by="e2e")
        assert created.get("rule_id")


def test_real_write_all_quality_modes_are_truthful_and_auto_certified(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_matrix")
    SuperTable(SUPER, ORG)
    dqc = DQConfig(fake, ORG, SUPER)
    _configure_all_modes(dqc)

    # Make AUTO prefer IslandDB for every byte range.  Capability/resource
    # proof must still veto IslandDB for any unsupported complete statement.
    assert RedisCatalog().set_auto_routing_policy(ORG, [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ])

    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(),
        overwrite_columns=["id"],
    )
    _add_valid_custom_rules(dqc, table)

    # A real write stores merge-on-read columns physically, but role-scoped
    # metadata and quality queries expose only the public logical view.
    stored_schema = MetaReader(SUPER, ORG).get_table_schema(table, ROLE)[0]
    assert {"__rowid__", "__timestamp__"}.isdisjoint(stored_schema)
    assert set(_rows().column_names).issubset(stored_schema)

    mode_keys = {
        mode: scheduler._pending_key(ORG, SUPER, table, mode)
        for mode in scheduler.QUALITY_MODES
    }
    # Snapshot commit persists one unresolved generation atomically; the
    # scheduler resolves configured mode keys under pinned catalog identity.
    unresolved_key = scheduler._unresolved_pending_key(ORG, SUPER, table)
    assert fake.get(unresolved_key) is not None
    assert fake.get(scheduler._pending_key(ORG, SUPER, table)) is None
    assert all(fake.get(key) is None for key in mode_keys.values())

    executions = []
    original_execute = scheduler._execute_quality_statement

    def observe_execution(org: str, sup: str, sql: str, **kwargs):
        result = original_execute(org, sup, sql, **kwargs)
        executions.append((sql, result))
        return result

    attempts = []
    original_try = scheduler._try_run_check

    def observe_attempt(
        r, org, sup, table_name, mode, config, cooldown, **kwargs,
    ):
        if table_name == table:
            pending_before = {
                candidate
                for candidate, key in mode_keys.items()
                if r.get(key) is not None
            }
        else:
            pending_before = set()
        outcome = original_try(
            r, org, sup, table_name, mode, config, cooldown, **kwargs,
        )
        if table_name == table:
            attempts.append((mode, pending_before, outcome))
        return outcome

    monkeypatch.setattr(scheduler, "_execute_quality_statement", observe_execution)
    monkeypatch.setattr(scheduler, "_try_run_check", observe_attempt)

    scheduler._scheduler_tick({}, {}, {})

    assert [mode for mode, _, _ in attempts] == ["quick", "deep", "custom"]
    assert attempts[0][1] == {"quick", "deep", "custom"}
    assert attempts[1][1] == {"deep", "custom"}
    assert attempts[2][1] == {"custom"}
    assert all(outcome.successful for _, _, outcome in attempts), [
        (mode, outcome.state, outcome.message) for mode, _, outcome in attempts
    ]

    assert all(fake.get(key) is None for key in mode_keys.values())
    assert fake.get(scheduler._pending_key(ORG, SUPER, table)) is None
    assert fake.get(unresolved_key) is None
    for mode in scheduler.QUALITY_MODES:
        key = scheduler._cooldown_key(ORG, SUPER, table, mode)
        assert fake.get(key) is not None
        assert 0 < fake.ttl(key) <= 180

    latest = dqc.get_latest(table)
    assert latest is not None
    assert set(latest["mode_results"]) == {"quick", "deep", "custom"}
    for mode, record in latest["mode_results"].items():
        assert record["errors"] == 0, (mode, record)
        assert record["total_checks"] == (
            record["evaluated"] + record["errors"] + record["skipped"]
        )
    assert latest["errors"] == 0
    assert latest["total_checks"] == (
        latest["evaluated"] + latest["errors"] + latest["skipped"]
    )
    # Seal the actual MetaReader/Polars spellings through both scheduler modes:
    # Float64 and Categorical are scalar-profiled, while relative temporal
    # values are explicit unsupported/N/A rather than broken C3 baselines.
    assert latest["parsed"]["columns"]["ratio"]["category"] == "numeric"
    assert latest["parsed"]["columns"]["segment"]["category"] == "string"
    assert latest["parsed"]["columns"]["local_time"]["category"] == "other"
    assert latest["parsed"]["columns"]["elapsed"]["category"] == "other"
    assert dqc.get_latest_column(table, "ratio")["deep"]["category"] == "numeric"
    assert dqc.get_latest_column(table, "segment")["deep"]["category"] == "string"

    observed_builtin_ids = {
        outcome["check_id"]
        for mode in ("quick", "deep")
        for outcome in latest["mode_results"][mode]["outcomes"]
    }
    assert observed_builtin_ids == ALL_BUILTIN_IDS
    assert len(latest["mode_results"]["custom"]["rule_results"]) == 6

    # Drift checks deliberately record a baseline on their first successful
    # pass.  Clear only the two mode-specific cooldowns and run them again so
    # this matrix proves that every applicable built-in is *evaluated*, not
    # merely present in the configured/outcome inventory.
    for mode in ("quick", "deep"):
        fake.delete(scheduler._cooldown_key(ORG, SUPER, table, mode))
        repeated = scheduler._try_run_check(
            fake, ORG, SUPER, table, mode, dqc, 180,
        )
        assert repeated.successful, (mode, repeated.message)

    latest = dqc.get_latest(table)
    assert latest is not None
    repeated_outcomes = [
        outcome
        for mode in ("quick", "deep")
        for outcome in latest["mode_results"][mode]["outcomes"]
    ]
    for check_id in ALL_BUILTIN_IDS:
        matching = [
            outcome for outcome in repeated_outcomes
            if outcome["check_id"] == check_id
        ]
        assert matching, check_id
        assert any(outcome.get("evaluated") is True for outcome in matching), (
            check_id,
            matching,
        )
    assert latest["errors"] == 0

    assert executions
    assert all(result.ok for _, result in executions)
    assert all(result.requested_engine == "auto" for _, result in executions)
    hidden_columns = (
        "__rowid__",
        "__timestamp__",
        "__file__",
        "__supertable_source_file__",
        "__supertable_scan_filename__",
    )
    assert all(
        all(f'"{hidden}"' not in sql for hidden in hidden_columns)
        for sql, _ in executions
    )
    # At least one simple predicate is certified for IslandDB, while richer
    # profile SQL is rejected statically and runs directly on DuckDB.
    assert any(result.actual_engine == "islanddb" for _, result in executions)
    assert any(result.actual_engine == "duckdb" for _, result in executions)
    for _, result in executions:
        if result.actual_engine == "islanddb":
            assert result.island_supported is True
        if result.island_supported is False:
            assert result.selected_engine == "duckdb"
            assert result.actual_engine == "duckdb"
            assert result.fallback is False
            assert result.island_certification_reasons


def test_legacy_private_incremental_scope_degrades_to_safe_full_scan(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_legacy_incremental")
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(),
        overwrite_columns=["id"],
    )
    dqc = DQConfig(fake, ORG, SUPER)

    # Simulate an old deployment that persisted the now-private storage
    # timestamp before configuration validation existed.
    fake.set(
        dqc._key("config", table),
        json.dumps({
            "scope": "incremental",
            "incremental_column": "__timestamp__",
        }),
    )
    observed_sql = []
    original_execute = scheduler._execute_quality_statement

    def observe_execution(org: str, sup: str, sql: str):
        observed_sql.append(sql)
        return original_execute(org, sup, sql)

    monkeypatch.setattr(scheduler, "_execute_quality_statement", observe_execution)
    outcome = scheduler._try_run_check(
        fake, ORG, SUPER, table, "quick", dqc, 30,
    )

    assert outcome.successful, outcome.message
    assert observed_sql
    assert all('"__timestamp__"' not in sql for sql in observed_sql)
    effective = dqc.get_effective_config(table)
    assert effective["scope"] == "full"
    assert "incremental_column" not in effective


def test_scheduler_quotes_hyphenated_supertable_identifier_end_to_end(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    hyphen_super = f"quality-hyphen-{uuid.uuid4().hex[:8]}"
    table = "facts"
    SuperTable(hyphen_super, ORG)
    DataWriter(super_name=hyphen_super, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=pa.Table.from_pylist(
            [
                {"id": 1, "amount": -1, "label": "a"},
                {"id": 2, "amount": 2, "label": "b"},
            ],
            schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("amount", pa.int64(), nullable=False),
                pa.field("label", pa.string(), nullable=False),
            ]),
        ),
        overwrite_columns=[],
    )
    dqc = DQConfig(fake, ORG, hyphen_super)
    assert dqc.set_global_config(_all_checks_enabled(), updated_by="hyphen-e2e")
    dqc.create_rule({
        "rule_type": "column_min",
        "table_name": table,
        "column_name": "amount",
        "threshold": -10,
    })
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: True,
    )
    observed_sql = []
    original_execute = scheduler._execute_quality_statement

    def observe(org: str, sup: str, sql: str, **kwargs):
        observed_sql.append(sql)
        return original_execute(org, sup, sql, **kwargs)

    monkeypatch.setattr(scheduler, "_execute_quality_statement", observe)
    outcomes = {
        mode: scheduler._try_run_check(
            fake, ORG, hyphen_super, table, mode, dqc, 30,
        )
        for mode in scheduler.QUALITY_MODES
    }
    assert all(outcome.successful for outcome in outcomes.values()), {
        mode: outcome.message for mode, outcome in outcomes.items()
    }
    quoted = f'"{hyphen_super}"."{table}"'
    assert observed_sql
    assert all(quoted in sql for sql in observed_sql)


def test_wide_exact_numeric_scheduler_keeps_boundaries_and_skips_uncertified_moments(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_wide_exact")
    SuperTable(SUPER, ORG)
    rows = pa.Table.from_pylist(
        [
            {
                "large_value": 9_007_199_254_740_992,
                "precise_value": Decimal("12345678901234567890.1234567890"),
            },
            {
                "large_value": 9_007_199_254_740_993,
                "precise_value": Decimal("12345678901234567890.1234567891"),
            },
        ],
        schema=pa.schema([
            pa.field("large_value", pa.int64(), nullable=False),
            pa.field("precise_value", pa.decimal128(38, 10), nullable=False),
        ]),
    )
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=rows,
        overwrite_columns=[],
    )
    assert RedisCatalog().set_auto_routing_policy(ORG, [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ])
    dqc = DQConfig(fake, ORG, SUPER)
    assert dqc.set_global_config(_all_checks_enabled(), updated_by="wide-e2e")
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: True,
    )

    for mode in ("quick", "deep"):
        first = scheduler._try_run_check(fake, ORG, SUPER, table, mode, dqc, 30)
        assert first.successful, (mode, first.message)
        fake.delete(scheduler._cooldown_key(ORG, SUPER, table, mode))
        second = scheduler._try_run_check(fake, ORG, SUPER, table, mode, dqc, 30)
        assert second.successful, (mode, second.message)

    latest = dqc.get_latest(table)
    assert latest is not None
    quick_columns = latest["parsed"]["columns"]
    large = quick_columns["large_value"]
    precise = quick_columns["precise_value"]
    assert large["distinct"] == 2
    assert (large["min"], large["max"]) == (
        9_007_199_254_740_992,
        9_007_199_254_740_993,
    )
    assert (precise["min"], precise["max"]) == (
        "12345678901234567890.1234567890",
        "12345678901234567890.1234567891",
    )
    assert not large["moments_certified"]
    assert not precise["moments_certified"]
    assert large["avg"] is None and large["stddev"] is None
    assert precise["avg"] is None and precise["stddev"] is None

    quick_outcomes = {
        outcome["check_id"]: outcome
        for outcome in latest["mode_results"]["quick"]["outcomes"]
    }
    assert quick_outcomes["C3"]["evaluated"] is True
    assert quick_outcomes["C6"]["status"] == "skipped"
    assert quick_outcomes["C6"]["reason"] == "uncertified_precision"

    deep_outcomes = latest["mode_results"]["deep"]["outcomes"]
    d5 = [outcome for outcome in deep_outcomes if outcome["check_id"] == "D5"]
    assert len(d5) == 2
    assert all(outcome["status"] == "skipped" for outcome in d5)
    assert all(outcome["reason"] == "unsupported_precision" for outcome in d5)
    for check_id in ("D3", "D4"):
        exact_tracking = [
            outcome for outcome in deep_outcomes
            if outcome["check_id"] == check_id
        ]
        assert len(exact_tracking) == 2
        assert all(outcome["evaluated"] is True for outcome in exact_tracking)

    large_deep = dqc.get_latest_column(table, "large_value")["deep"]
    precise_deep = dqc.get_latest_column(table, "precise_value")["deep"]
    assert large_deep["distinct_vals"] == 2
    assert precise_deep["distinct_vals"] == 2
    assert {item["value"] for item in large_deep["topx_values"]} == {
        9_007_199_254_740_992,
        9_007_199_254_740_993,
    }
    assert {item["value"] for item in precise_deep["topx_values"]} == {
        "12345678901234567890.1234567890",
        "12345678901234567890.1234567891",
    }


def test_legacy_private_custom_rule_fails_before_sql_and_never_cools_down(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_legacy_custom")
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(),
        overwrite_columns=["id"],
    )
    dqc = DQConfig(fake, ORG, SUPER)
    rule_id = "legacy_private_column"
    fake.set(
        dqc._key("rules", "doc", rule_id),
        json.dumps({
            "rule_id": rule_id,
            "rule_type": "column_min",
            "table_name": table,
            "column_name": "__rowid__",
            "threshold": 0,
            "enabled": True,
        }),
    )
    fake.sadd(dqc._key("rules", "index"), rule_id)

    executed = []

    def forbidden_execution(*args, **kwargs):
        executed.append((args, kwargs))
        return execute_quality_sql(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_execute_quality_statement", forbidden_execution)
    outcome = scheduler._try_run_check(
        fake, ORG, SUPER, table, "custom", dqc, 30,
    )

    assert outcome.state == "failed"
    assert outcome.errors == 1
    # Configuration failures remain distinguishable by a bounded class name,
    # without reflecting rule/schema details into the scheduler result.
    assert outcome.message == "quality check failed; error_type=DQConfigReadError"
    assert executed == []
    assert fake.get(scheduler._cooldown_key(ORG, SUPER, table, "custom")) is None
    assert fake.get(scheduler._retry_key(ORG, SUPER, table, "custom")) is not None


def test_custom_string_extremum_is_rejected_at_typed_runtime_boundary(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_string_aggregate")
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(),
        overwrite_columns=["id"],
    )
    dqc = DQConfig(fake, ORG, SUPER)
    created = dqc.create_rule({
        "rule_type": "custom_sql",
        "table_name": table,
        "sql": f"SELECT MAX(label) AS violations FROM {SUPER}.{table}",
        "description": "unbounded string result",
    })
    assert created["rule_id"]

    executed = []
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *args, **kwargs: executed.append((args, kwargs)),
    )
    outcome = scheduler._try_run_check(
        fake, ORG, SUPER, table, "custom", dqc, 30,
    )
    assert outcome.state == "failed"
    assert "non_numeric_aggregate" in outcome.message
    assert executed == []
    assert fake.get(scheduler._cooldown_key(ORG, SUPER, table, "custom")) is None


def test_distinct_in_rejects_incompatible_expected_type_before_sql(
    hermetic_fakeredis, monkeypatch,
):
    fake = hermetic_fakeredis
    table = _new_table_name("quality_distinct_type")
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(),
        overwrite_columns=["id"],
    )
    dqc = DQConfig(fake, ORG, SUPER)
    # Schema-independent ingress cannot know that amount is numeric.  The
    # scheduler's pinned-schema boundary must reject the string before SQL.
    created = dqc.create_rule({
        "rule_type": "distinct_in",
        "table_name": table,
        "column_name": "amount",
        "expected_values": ["1"],
    })
    assert created["rule_id"]

    executed = []
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *args, **kwargs: executed.append((args, kwargs)),
    )
    outcome = scheduler._try_run_check(
        fake, ORG, SUPER, table, "custom", dqc, 30,
    )
    assert outcome.state == "failed"
    assert "expected_value_type" in outcome.message
    assert executed == []
    assert fake.get(scheduler._cooldown_key(ORG, SUPER, table, "custom")) is None


def test_zero_public_column_table_uses_metadata_and_reports_explicit_na(
    hermetic_fakeredis, monkeypatch,
):
    """A hidden-only physical schema must never produce an empty SQL view."""

    fake = hermetic_fakeredis
    table = _new_table_name("quality_zero_public_columns")
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=pa.Table.from_arrays([], schema=pa.schema([])),
        overwrite_columns=[],
    )
    dqc = DQConfig(fake, ORG, SUPER)
    assert dqc.set_global_config(_all_checks_enabled(), updated_by="e2e-zero")
    created = dqc.create_rule({
        "rule_type": "row_count_min",
        "table_name": table,
        "threshold": 0,
        "description": "empty table is allowed",
    })
    assert created["rule_id"]

    stored_schema = MetaReader(SUPER, ORG).get_table_schema(table, ROLE)[0]
    assert set(stored_schema).issubset({"__rowid__", "__timestamp__"})

    executed = []

    def forbidden_execution(*args, **kwargs):
        executed.append((args, kwargs))
        raise AssertionError("zero-column checks must not construct a SQL read view")

    monkeypatch.setattr(scheduler, "_execute_quality_statement", forbidden_execution)
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: True,
    )

    outcomes = {
        mode: scheduler._try_run_check(
            fake, ORG, SUPER, table, mode, dqc, 30,
        )
        for mode in scheduler.QUALITY_MODES
    }
    assert all(outcome.successful for outcome in outcomes.values()), {
        mode: (outcome.state, outcome.message)
        for mode, outcome in outcomes.items()
    }
    assert executed == []
    assert all(
        fake.get(scheduler._cooldown_key(ORG, SUPER, table, mode)) is not None
        for mode in scheduler.QUALITY_MODES
    )

    latest = dqc.get_latest(table)
    assert latest is not None
    assert latest["errors"] == 0
    sealed_rows = scheduler._table_snapshot_metadata(
        MetaReader(SUPER, ORG), table,
    )[2]
    assert sealed_rows is not None
    assert latest["row_count"] == sealed_rows
    assert set(latest["mode_results"]) == set(scheduler.QUALITY_MODES)

    quick = latest["mode_results"]["quick"]
    quick_by_id = {outcome["check_id"]: outcome for outcome in quick["outcomes"]}
    assert set(quick_by_id) == {"T1", "T2", "T3", "T5", "C1", "C2", "C3", "C4", "C5", "C6"}
    assert all(
        quick_by_id[check_id]["status"] == "not_applicable"
        for check_id in ("C1", "C2", "C3", "C4", "C5", "C6")
    )

    deep = latest["mode_results"]["deep"]
    deep_by_id = {outcome["check_id"]: outcome for outcome in deep["outcomes"]}
    assert set(deep_by_id) == {"D1", "D2", "D3", "D4", "D5", "D7"}
    assert all(
        outcome["status"] == "not_applicable"
        for outcome in deep_by_id.values()
    )

    custom = latest["mode_results"]["custom"]
    assert custom["errors"] == 0
    assert custom["evaluated"] == 1
    assert custom["rule_results"][0]["status"] == "ok"
    assert custom["rule_results"][0]["value"] == sealed_rows
