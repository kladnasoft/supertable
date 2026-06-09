"""
Tests for the monitoring-sink loop guard.

When an orchestrator drains a monitoring partition and writes the
records back into ``__writes__``, that write must NOT emit a fresh
``writes`` monitor metric — otherwise every drain cycle generates
one new metric for tomorrow's flush, producing a slow but real
self-amplification loop.

Same logic applies to plan_extender on the read side: a SELECT
targeting a sink table must not generate a ``plans`` metric.

This file pins both guards.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.monitoring.partitions import MONITORING_SINK_TABLES  # noqa: E402


# ===========================================================================
# 1. data_writer.py — _MONITORING_SINK_TABLES guard
# ===========================================================================


class TestDataWriterImportsCanonicalSet:

    def test_data_writer_uses_canonical_sink_set(self):
        """data_writer imports the set from monitoring.partitions —
        not a local copy that could drift."""
        from supertable import data_writer as dw_mod
        # The module references the canonical set via the import.
        # Spot-check that the imported symbol is the same object.
        assert dw_mod.MONITORING_SINK_TABLES is MONITORING_SINK_TABLES


# ===========================================================================
# 2. plan_extender — _query_targets_sink_table parser
# ===========================================================================


class TestPlanExtenderSinkTableDetection:

    def test_empty_string_returns_false(self):
        from supertable.plan_extender import _query_targets_sink_table
        assert _query_targets_sink_table("") is False
        assert _query_targets_sink_table(None) is False  # type: ignore[arg-type]

    def test_sink_table_detected(self):
        from supertable.plan_extender import _query_targets_sink_table
        assert _query_targets_sink_table("__writes__") is True
        assert _query_targets_sink_table("__reads__") is True
        assert _query_targets_sink_table("__mcp__") is True
        assert _query_targets_sink_table("__plans__") is True

    def test_non_sink_table_returns_false(self):
        from supertable.plan_extender import _query_targets_sink_table
        assert _query_targets_sink_table("users") is False
        assert _query_targets_sink_table("orders") is False
        assert _query_targets_sink_table("_writes_") is False  # single underscore

    def test_comma_joined_with_sink_table_anywhere_returns_true(self):
        from supertable.plan_extender import _query_targets_sink_table
        # The format ``data_reader.execute()`` uses: comma-joined list of
        # simple_name strings.
        assert _query_targets_sink_table("users, orders, __writes__") is True
        assert _query_targets_sink_table("__writes__, users") is True
        assert _query_targets_sink_table("users,__writes__,orders") is True

    def test_comma_joined_all_non_sink_returns_false(self):
        from supertable.plan_extender import _query_targets_sink_table
        assert _query_targets_sink_table("users, orders, inventory") is False

    def test_handles_whitespace_robustly(self):
        from supertable.plan_extender import _query_targets_sink_table
        assert _query_targets_sink_table("  __writes__  ") is True
        assert _query_targets_sink_table("\t__reads__\n") is True


# ===========================================================================
# 3. plan_extender — extend_execution_plan skips sink-table queries
# ===========================================================================


class TestPlanExtenderSinkGuard:

    def _build_qpm(self, table_name: str):
        qpm = MagicMock()
        qpm.query_id = "qid_test"
        qpm.query_hash = "hash_test"
        qpm.organization = "org"
        qpm.super_name = "sup"
        qpm.role_name = "r"
        qpm.original_table = table_name
        qpm.query = "SELECT 1"
        qpm.query_plan_path = None  # don't try to read a plan file
        qpm.query_profile = None
        return qpm

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_sink_table_query_skips_log_metric(self, MockMW):
        """A SELECT FROM __writes__ must not generate a plans metric."""
        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        qpm = self._build_qpm("__writes__")
        extend_execution_plan(
            query_plan_manager=qpm,
            role_name="r",
            timing={},
            plan_stats=PlanStats(),
            status="ok",
            message="",
            result_shape=(0, 0),
        )
        # MonitoringWriter must NOT have been instantiated for a sink-table query
        MockMW.assert_not_called()

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_non_sink_table_query_emits_log_metric(self, MockMW):
        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        # Plumb the context manager + log_metric call through the mock
        mock_mw = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mw
        MockMW.return_value.__exit__.return_value = False

        qpm = self._build_qpm("orders")
        extend_execution_plan(
            query_plan_manager=qpm,
            role_name="r",
            timing={},
            plan_stats=PlanStats(),
            status="ok",
            message="",
            result_shape=(0, 0),
        )
        MockMW.assert_called_once_with(organization="org", monitor_type="plans")
        mock_mw.log_metric.assert_called_once()

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_mixed_targets_with_sink_skip(self, MockMW):
        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        qpm = self._build_qpm("users, __reads__, orders")
        extend_execution_plan(
            query_plan_manager=qpm,
            role_name="r",
            timing={},
            plan_stats=PlanStats(),
            status="ok",
            message="",
            result_shape=(0, 0),
        )
        # Even though "users" and "orders" are non-sink, the presence of
        # __reads__ in the join means the query is touching sink data —
        # skip the metric to avoid the amplification loop.
        MockMW.assert_not_called()
