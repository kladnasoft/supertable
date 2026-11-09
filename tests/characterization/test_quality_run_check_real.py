"""``run_check`` on the real checks: a success has recorded its result.

The checks caught their own failures, a schema read, a statement or a rule,
logged them and returned, so ``run_check`` counted the run a success. And
every statement failed: the reader returns polars frames, and the checks
called pandas' ``.empty`` and ``to_dict(orient=...)`` on them. So every quick,
deep and custom run said success and recorded nothing, and a violated
critical rule was published as ``ok``.

Nothing is stubbed: the real writer, reader and RBAC, the catalog's Lua on
fakeredis, and LOCAL storage. The table has three rows, one ``name`` null.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from supertable.data_writer import DataWriter
from supertable.quality import checker, run_check
from supertable.quality.config import DQConfig
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "dq"
ROLE = "superadmin"


@pytest.fixture
def dqc(hermetic_fakeredis):
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE, simple_name="orders", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3], "name": ["a", None, "c"]}))
    return DQConfig(hermetic_fakeredis, ORG, SUPER)


def _run(dqc, mode, table="orders"):
    return run_check(dqc.r, ORG, SUPER, table, mode, dqc, 0)


def _rule(dqc, **rule):
    writer = DQConfig(dqc.r, ORG, SUPER, actor_role_name=ROLE)
    return writer.create_rule({"table_name": "orders", **rule})


def _enable_deep(dqc, table="orders"):
    dqc.set_table_config(table, {"checks": {"D1": {"enabled": True}}})


def test_a_quick_check_records_the_profile(dqc):
    assert _run(dqc, "quick").state == "success"

    latest = dqc.get_latest("orders")
    assert (latest["check_type"], latest["row_count"]) == ("quick", 3)
    assert latest["parsed"]["columns"]["name"]["null_count"] == 1
    assert dqc.get_latest_column("orders", "id")["max"] == 3.0


def test_an_empty_table_is_profiled_rather_than_failed(dqc):
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE, simple_name="empty", overwrite_columns=[],
        data=pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)}))

    assert _run(dqc, "quick", table="empty").state == "success"
    assert dqc.get_latest("empty")["row_count"] == 0


def test_a_deep_check_records_every_column(dqc):
    _enable_deep(dqc)

    assert _run(dqc, "deep").state == "success"
    assert dqc.get_latest_column("orders", "id")["deep"]["total_rows"] == 3
    assert dqc.get_latest_column("orders", "name")["deep"]["non_nulls"] == 2


def test_every_check_runs_on_a_supertable_named_with_a_hyphen(hermetic_fakeredis):
    """The checks named their table ``sup.table`` unquoted. A SuperTable may
    be named with a hyphen -- Studio names a project's
    ``<project>_<env>_<layer>``, so a project called ``medicare-example`` has
    ``medicare-example_dev_bronze`` -- and the statement parsed it as a
    subtraction, so every quick, deep and custom check on it failed."""
    sup = "medicare-example_dev_bronze"
    SuperTable(sup, ORG)
    DataWriter(super_name=sup, organization=ORG).write(
        role_name=ROLE, simple_name="orders", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3], "name": ["a", None, "c"]}))
    dqc = DQConfig(hermetic_fakeredis, ORG, sup)
    _enable_deep(dqc)
    DQConfig(hermetic_fakeredis, ORG, sup, actor_role_name=ROLE).create_rule(
        {"table_name": "orders", "rule_type": "row_count_min", "threshold": 1})

    def run(mode):
        return run_check(hermetic_fakeredis, ORG, sup, "orders", mode, dqc, 0).state

    assert run("quick") == "success"
    assert dqc.get_latest("orders")["row_count"] == 3
    assert run("deep") == "success"
    assert dqc.get_latest_column("orders", "name")["deep"]["non_nulls"] == 2
    assert run("custom") == "success"
    assert [(rule["rule_type"], rule["status"])
            for rule in dqc.get_latest("orders")["rule_results"]] == [
        ("row_count_min", "ok")]

def test_a_violated_rule_is_an_anomaly_and_a_kept_one_passes(dqc):
    violated = _rule(dqc, rule_type="null_rate_max", column_name="name",
                     threshold=0, severity="critical")
    _rule(dqc, rule_type="row_count_min", threshold=1)

    assert _run(dqc, "custom").state == "success"
    latest = dqc.get_latest("orders")
    assert (latest["status"], latest["total_checks"], latest["passed"],
            latest["critical"]) == ("critical", 2, 1, 1)
    assert [a["check_id"] for a in latest["anomalies"]] == [
        f"R_{violated['rule_id']}"]
    assert dqc.get_anomalies("orders") == latest["anomalies"]


def test_a_rule_that_cannot_run_fails_the_check_and_records_nothing(dqc):
    """A rule on a missing column: the reader answers with an error status
    and an empty frame, which read as no rows made the rule pass."""
    assert _run(dqc, "quick").state == "success"
    before = dqc.get_latest("orders")
    _rule(dqc, rule_type="null_rate_max", column_name="nosuch", threshold=0,
          severity="critical")
    _rule(dqc, rule_type="row_count_min", threshold=1)

    assert _run(dqc, "custom").state == "failed"
    assert dqc.get_latest("orders") == before


def test_a_rule_that_never_ran_is_not_counted_as_passed(dqc):
    """A rule stored without the role it runs as is skipped, not elevated;
    it was still counted as a passed check, one more on every run."""
    dqc.r.set(dqc._key("rules", "doc", "rule_legacy"), json.dumps({
        "rule_id": "rule_legacy", "rule_type": "row_count_min", "threshold": 1,
        "table_name": "orders", "enabled": True}))
    dqc.r.sadd(dqc._key("rules", "index"), "rule_legacy")
    _rule(dqc, rule_type="row_count_min", threshold=1)

    for _ in range(2):
        assert _run(dqc, "custom").state == "success"
        latest = dqc.get_latest("orders")
        assert (latest["total_checks"], latest["passed"],
                len(latest["rule_results"])) == (1, 1, 1)


def test_a_missing_table_fails(dqc):
    _enable_deep(dqc, table="nosuch")

    for mode in ("quick", "deep"):
        assert _run(dqc, mode, table="nosuch").state == "failed"


def test_a_quick_statement_that_fails_fails_the_check(dqc, monkeypatch):
    monkeypatch.setattr(checker, "build_quick_sql",
                        lambda *args: f"SELECT nosuch FROM {SUPER}.orders")

    assert _run(dqc, "quick").state == "failed"
    assert dqc.get_latest("orders") is None


def test_a_deep_column_that_fails_fails_the_check_after_the_others(dqc, monkeypatch):
    _enable_deep(dqc)
    monkeypatch.setattr(checker, "build_deep_string_sql",
                        lambda table, column: f"SELECT nosuch FROM {table}")

    assert _run(dqc, "deep").state == "failed"
    assert dqc.get_latest_column("orders", "id")["deep"]["total_rows"] == 3
    assert dqc.get_latest_column("orders", "name") is None


@pytest.mark.parametrize("mode", ["quick", "deep", "custom"])
def test_a_result_that_cannot_be_written_fails_the_check(dqc, monkeypatch, mode):
    """DQConfig's setters log a Redis error and return False."""
    _enable_deep(dqc)
    _rule(dqc, rule_type="row_count_min", threshold=1)
    for setter in ("set_latest", "set_latest_column"):
        monkeypatch.setattr(DQConfig, setter, lambda self, *args: False)

    assert _run(dqc, mode).state == "failed"
