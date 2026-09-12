"""A quality rule must not be a way to read data its author cannot read.

A rule is not inert configuration: it is SQL the scheduler later executes.
``custom_sql`` is passed through verbatim, and every rule used to run as
``superadmin`` — so anyone able to register one could do

    {"rule_type": "custom_sql", "sql": "SELECT * FROM salaries"}

and have it read with every row filter, column mask and table grant bypassed,
with the rows then published to ``latest:{table}`` and the DQ history table
where the author could read them back. Registering a rule was itself ungated.

Two changes close it, and both are asserted here:

  * writing a rule requires WRITE on the table it targets, and
  * the scheduler executes the rule as the role that registered it.

The profiler is the deliberate exception and is covered too: it runs
library-generated SQL over one declared table and legitimately reads
unfiltered, because a null rate computed through one role's row filters would
be a wrong answer rather than a safer one.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.quality.checker import (  # noqa: E402
    _num, _sql_str, build_custom_rule_sql, build_quick_sql,
    evaluate_custom_rule,
)
from supertable.quality.config import DQConfig  # noqa: E402
from supertable.quality.scheduler import PROFILER_ROLE  # noqa: E402

ORG = "test_org"
SUP = "test_super"


@pytest.fixture()
def r():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def allow_write():
    """Let the authority check pass, recording what it was asked."""
    with patch("supertable.rbac.access_control.check_write_access") as m:
        yield m


@pytest.fixture()
def deny_write():
    with patch("supertable.rbac.access_control.check_write_access",
               side_effect=PermissionError("denied")) as m:
        yield m


def _rule(**over):
    rule = {"rule_type": "custom_sql", "table_name": "orders",
            "sql": "SELECT COUNT(*) AS violations FROM orders"}
    rule.update(over)
    return rule


# --------------------------------------------------------------------------
# Writing a rule needs authority over the table it targets
# --------------------------------------------------------------------------

def test_creating_a_rule_without_an_actor_is_refused(r):
    with pytest.raises(PermissionError, match="no actor role was supplied"):
        DQConfig(r, ORG, SUP).create_rule(_rule())


def test_creating_a_rule_checks_write_on_the_target_table(r, allow_write):
    DQConfig(r, ORG, SUP, actor_role_name="etl").create_rule(_rule())

    assert allow_write.call_args.kwargs["table_name"] == "orders"
    assert allow_write.call_args.kwargs["role_name"] == "etl"


def test_a_role_without_write_on_the_table_cannot_register_a_rule(r, deny_write):
    with pytest.raises(PermissionError, match="denied"):
        DQConfig(r, ORG, SUP, actor_role_name="nosy").create_rule(_rule())

    assert DQConfig(r, ORG, SUP).list_rules() == [], "nothing may persist"


def test_a_wildcard_rule_is_checked_against_the_wildcard(r, allow_write):
    """``table_name: "*"`` runs against every table, so it needs "*"."""
    DQConfig(r, ORG, SUP, actor_role_name="etl").create_rule(
        _rule(table_name="*"),
    )
    assert allow_write.call_args.kwargs["table_name"] == "*"


def test_a_rule_with_no_table_defaults_to_the_wildcard(r, allow_write):
    DQConfig(r, ORG, SUP, actor_role_name="etl").create_rule(
        {"rule_type": "row_count_min", "threshold": 1},
    )
    assert allow_write.call_args.kwargs["table_name"] == "*"


@pytest.mark.parametrize("op", ["update", "delete"])
def test_updating_and_deleting_also_need_authority(r, allow_write, op):
    dqc = DQConfig(r, ORG, SUP, actor_role_name="etl")
    rule = dqc.create_rule(_rule())

    unauthorised = DQConfig(r, ORG, SUP)          # no actor
    with pytest.raises(PermissionError):
        if op == "update":
            unauthorised.update_rule(rule["rule_id"], {"severity": "critical"})
        else:
            unauthorised.delete_rule(rule["rule_id"])


def test_retargeting_a_rule_checks_both_tables(r, allow_write):
    """Checking only one side leaves a hole in the other direction.

    Only the new table: a role with WRITE on ``public`` could retarget a rule
    that runs against ``salaries``. Only the old: it could aim an existing
    rule at a table it has no grant on.
    """
    dqc = DQConfig(r, ORG, SUP, actor_role_name="etl")
    rule = dqc.create_rule(_rule())
    allow_write.reset_mock()

    dqc.update_rule(rule["rule_id"], {"table_name": "salaries"})

    checked = {c.kwargs["table_name"] for c in allow_write.call_args_list}
    assert checked == {"orders", "salaries"}


def test_deleting_an_unknown_rule_is_checked_against_the_wildcard(r, allow_write):
    """So a narrow role cannot probe which rule ids exist."""
    DQConfig(r, ORG, SUP, actor_role_name="etl").delete_rule("rule_nope")
    assert allow_write.call_args.kwargs["table_name"] == "*"


def test_a_failed_write_is_not_reported_as_success(r, allow_write):
    """The Redis error used to be logged and swallowed.

    The caller got the rule dict back as though it had persisted.
    """
    dqc = DQConfig(r, ORG, SUP, actor_role_name="etl")
    with patch.object(r, "set", side_effect=RuntimeError("redis down")):
        with pytest.raises(RuntimeError, match="redis down"):
            dqc.create_rule(_rule())


# --------------------------------------------------------------------------
# The rule records, and the scheduler uses, the registering role
# --------------------------------------------------------------------------

def test_the_registering_role_is_recorded_on_the_rule(r, allow_write):
    rule = DQConfig(r, ORG, SUP, actor_role_name="etl").create_rule(_rule())
    assert rule["created_by_role"] == "etl"

    stored = DQConfig(r, ORG, SUP).get_rule(rule["rule_id"])
    assert stored["created_by_role"] == "etl"


def test_editing_a_rule_restamps_it_to_the_editor(r, allow_write):
    """An edited rule runs on the editor's authority, not the author's."""
    dqc_a = DQConfig(r, ORG, SUP, actor_role_name="alice")
    rule = dqc_a.create_rule(_rule())

    dqc_b = DQConfig(r, ORG, SUP, actor_role_name="bob")
    updated = dqc_b.update_rule(rule["rule_id"], {"severity": "critical"})

    assert updated["created_by_role"] == "bob"


def test_the_scheduler_runs_a_rule_as_its_registering_role(r, allow_write):
    """The core assertion: not superadmin, the rule's own role."""
    from supertable.quality import scheduler as sched

    dqc = DQConfig(r, ORG, SUP, actor_role_name="etl")
    dqc.create_rule(_rule())

    seen = {}

    class _Reader:
        def __init__(self, **kw):
            self.query_plan_manager = None
        def execute(self, role_name=None, **kw):
            seen["role"] = role_name
            return (None, "OK", "")

    with patch("supertable.data_reader.DataReader", _Reader):
        sched._run_custom_check(r, ORG, SUP, "orders", dqc)

    assert seen.get("role") == "etl"
    assert seen.get("role") != PROFILER_ROLE


def test_a_rule_without_a_recorded_role_is_skipped_not_elevated(r):
    """Legacy rules must not fall back to superadmin.

    There is no way to tell whose authority such a rule carries, and
    "unknown" must not resolve to "unrestricted".
    """
    from supertable.quality import scheduler as sched

    # Write a pre-existing rule straight past the gate, as an older build did.
    dqc = DQConfig(r, ORG, SUP)
    import json
    rid = "rule_legacy"
    r.set(dqc._key("rules", "doc", rid), json.dumps(
        {"rule_id": rid, "rule_type": "custom_sql", "table_name": "orders",
         "sql": "SELECT * FROM salaries", "enabled": True},
    ))
    r.sadd(dqc._key("rules", "index"), rid)

    executed = []

    class _Reader:
        def __init__(self, **kw):
            self.query_plan_manager = None
        def execute(self, role_name=None, **kw):
            executed.append(role_name)
            return (None, "OK", "")

    with patch("supertable.data_reader.DataReader", _Reader):
        sched._run_custom_check(r, ORG, SUP, "orders", dqc)

    assert executed == [], "a rule with no role must not run at all"


def test_the_profiler_identity_is_separate_from_the_rule_identity():
    """Named apart so the two cannot be conflated again by accident."""
    assert PROFILER_ROLE == "superadmin"


# --------------------------------------------------------------------------
# SQL injection through rule fields
# --------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [
    "0 OR 1=1", "1; DROP TABLE orders", "1 UNION SELECT 1",
    "", "abc", None, True, False, [], {},
])
def test_a_non_numeric_threshold_cannot_reach_the_sql(bad):
    """``WHERE {col} < {threshold}`` interpolated the value raw.

    Reachable without ``custom_sql`` at all, and it used to run elevated.
    """
    sql = build_custom_rule_sql(
        {"rule_type": "column_min", "column_name": "amount", "threshold": bad},
        "sup.orders",
    )
    assert sql is None, f"threshold {bad!r} produced SQL: {sql}"


@pytest.mark.parametrize("good,expected", [
    (5, 5.0), (5.5, 5.5), ("5", 5.0), (" 7 ", 7.0), (-3, -3.0), (0, 0.0),
])
def test_numeric_thresholds_still_work(good, expected):
    sql = build_custom_rule_sql(
        {"rule_type": "column_min", "column_name": "amount", "threshold": good},
        "sup.orders",
    )
    assert sql is not None and str(expected) in sql


def test_booleans_are_not_numbers():
    """``bool`` is an ``int`` subclass, so True would become 1 silently."""
    assert _num(True) is None and _num(False) is None


def test_expected_values_cannot_break_out_of_the_literal():
    """``f"'{v}'"`` let an apostrophe close the string and add SQL."""
    sql = build_custom_rule_sql(
        {"rule_type": "distinct_in", "column_name": "region",
         "expected_values": ["EU", "x') OR 1=1 --", "it's"]},
        "sup.orders",
    )
    # Not a substring check: the escaped literal legitimately *contains*
    # "OR 1=1 --", so asserting its absence would fail on correct output.
    # Parse instead and compare the literal values the engine will see.
    import sqlglot

    literals = [n.this for n in sqlglot.parse_one(sql, read="duckdb")
                .find_all(sqlglot.exp.Literal) if n.is_string]
    assert literals == ["EU", "x') OR 1=1 --", "it's"], (
        "each value must survive as exactly one string literal"
    )


def test_the_incremental_timestamp_cannot_break_out_either():
    """Same class of bug, and this one builds SQL for the elevated path."""
    sql = build_quick_sql("sup.orders", [("id", "BIGINT")],
                          "updated_at", "2020-01-01' OR '1'='1")
    import sqlglot

    literals = [n.this for n in sqlglot.parse_one(sql, read="duckdb")
                .find_all(sqlglot.exp.Literal) if n.is_string]
    assert "2020-01-01' OR '1'='1" in literals, (
        "the timestamp must survive as one literal, not become a predicate"
    )


@pytest.mark.parametrize("value,expected", [
    ("plain", "'plain'"), ("it's", "'it''s'"), ("", "''"),
    ("a'b'c", "'a''b''c'"), (5, "'5'"),
])
def test_sql_str_escapes_by_doubling(value, expected):
    assert _sql_str(value) == expected


def test_evaluation_agrees_with_the_builder_on_thresholds():
    """A threshold the builder refuses must not be compared against here.

    A string would raise TypeError on ``<=`` and fail the rule with an error
    that says nothing about why.
    """
    out = evaluate_custom_rule(
        {"rule_type": "row_count_min", "threshold": "not a number"},
        [{"row_count": 5}],
    )
    assert isinstance(out, dict) and "status" in out
