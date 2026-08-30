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
import json
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
    def test_typed_telemetry_survives_monitoring_payload_without_secrets(
        self, MockMW,
    ):
        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        mock_mw = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mw
        MockMW.return_value.__exit__.return_value = False
        qpm = self._build_qpm("orders")
        qpm.source_type = "api"
        plan_stats = PlanStats()
        plan_stats.add_stat({"ENGINE": "duckdb"})
        plan_stats.add_stat({"FILES_BEFORE_PRUNE": 10})
        plan_stats.add_stat({"FILES_PRUNED": 7})
        plan_stats.add_stat({"FILES_KEPT": 3})
        plan_stats.add_stat({"PRUNE_DURATION_MS": 2.5})

        extend_execution_plan(
            query_plan_manager=qpm,
            role_name="r",
            timing=[
                {"QUERY_PREPARATION": 0.01},
                {"CONNECTION_SETUP": 0.005},
                {"EXECUTING_QUERY": 0.2},
                {"TOTAL_EXECUTE": 0.25},
                {"REMOTE_PHASE": "https://private.invalid/x?token=secret"},
            ],
            plan_stats=plan_stats,
            status="ok",
            message="",
            result_shape=(3, 2),
        )

        payload = mock_mw.log_metric.call_args.args[0]
        timings = json.loads(payload["execution_timings"])
        normalized = json.loads(payload["normalized_profile"])
        assert timings == [
            {"QUERY_PREPARATION": 0.01},
            {"CONNECTION_SETUP": 0.005},
            {"EXECUTING_QUERY": 0.2},
            {"TOTAL_EXECUTE": 0.25},
        ]
        assert normalized["schema_version"] == 3
        assert normalized["pruning"]["files_pruned"] == 7
        assert any(
            phase["phase"] == "engine_connect"
            and phase["duration_us"] == 5000
            for phase in normalized["pipeline_phases"]
        )
        rendered = json.dumps(payload, sort_keys=True)
        assert "private.invalid" not in rendered
        assert "REMOTE_PHASE" not in rendered
        assert "token=secret" not in rendered

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_remote_path_tokens_never_reach_monitoring_payload(
        self, MockMW, tmp_path,
    ):
        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        remote = (
            "https://URL_USER:URL_PASSWORD@storage.invalid/REMOTE_PATH_TOKEN/"
            "data.parquet?QUERY_TOKEN=yes#FRAGMENT_TOKEN"
        )
        local_path = "/srv/private/LOCAL_PATH_TOKEN/profile.json"
        sql_literal = "SQL_SSN_LITERAL"
        rbac_literal = "RBAC_TENANT_LITERAL"
        auth_secret = "MONITOR_AUTH_SECRET"
        cookie_secret = "MONITOR_COOKIE_SECRET"
        api_secret = "MONITOR_API_SECRET"
        body_secret = "MONITOR_BODY_SECRET"
        plan_path = tmp_path / "query-profile.json"
        plan_path.write_text(
            json.dumps({
                "query_name": (
                    f"SELECT ssn FROM orders WHERE ssn='{sql_literal}'"
                ),
                "operator": {
                    "extra_info": {
                        "Filename": remote,
                        "Filters": f"tenant = '{rbac_literal}'",
                        "local_path": local_path,
                        "headers": (
                            f"Authorization: Bearer {auth_secret}\n"
                            f"Cookie: session={cookie_secret}\n"
                            f"X-Api-Key: {api_secret}"
                        ),
                        "response": (
                            f'{{"access_token":"{body_secret}"}}'
                        ),
                    },
                    "operator_timing": 0.125,
                },
            }),
            encoding="utf-8",
        )
        mock_mw = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mw
        MockMW.return_value.__exit__.return_value = False
        qpm = self._build_qpm("orders")
        qpm.query_plan_path = str(plan_path)
        qpm.source_type = "api"
        qpm.query = (
            f"SELECT ssn FROM orders WHERE ssn='{sql_literal}'"
        )
        plan_stats = PlanStats()
        plan_stats.add_stat({"REMOTE_SOURCE": remote})

        extend_execution_plan(
            query_plan_manager=qpm,
            role_name="r",
            timing={"REMOTE_PHASE": remote},
            plan_stats=plan_stats,
            status="error",
            message=(
                f"backend failed at {remote}; tenant='{rbac_literal}'; "
                f"Authorization: Bearer {auth_secret}"
            ),
            result_shape=(0, 0),
        )

        payload = mock_mw.log_metric.call_args.args[0]
        persisted = json.dumps(payload, sort_keys=True)
        assert "_redacted_fields" in persisted
        assert "<redacted-diagnostic" in persisted
        assert "operator_timing" in persisted
        for secret in (
            "URL_USER", "URL_PASSWORD", "REMOTE_PATH_TOKEN", "data.parquet",
            "QUERY_TOKEN", "FRAGMENT_TOKEN",
            "LOCAL_PATH_TOKEN", local_path, sql_literal, rbac_literal,
            auth_secret, cookie_secret, api_secret, body_secret,
        ):
            assert secret not in persisted
        assert not plan_path.exists()

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_monitoring_backpressure_still_deletes_raw_plan(self, MockMW, tmp_path):
        from supertable.engine.plan_stats import PlanStats
        from supertable.monitoring_writer import MonitoringBackpressureError
        from supertable.plan_extender import extend_execution_plan

        plan_path = tmp_path / "query-profile.json"
        plan_path.write_text("{}", encoding="utf-8")
        mock_mw = MagicMock()
        mock_mw.log_metric.side_effect = MonitoringBackpressureError("spool full")
        MockMW.return_value.__enter__.return_value = mock_mw
        qpm = self._build_qpm("orders")
        qpm.query_plan_path = str(plan_path)

        with pytest.raises(MonitoringBackpressureError):
            extend_execution_plan(
                query_plan_manager=qpm,
                role_name="r",
                timing={},
                plan_stats=PlanStats(),
                status="ok",
                message="",
                result_shape=(1, 1),
            )

        assert not plan_path.exists()

    @patch("supertable.plan_extender.MonitoringWriter")
    def test_unknown_shape_preserves_stream_profile_result_rows(self, MockMW):
        """Display zeros must not overwrite measured streaming counters."""
        import json

        from supertable.engine.plan_stats import PlanStats
        from supertable.plan_extender import extend_execution_plan

        mock_mw = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mw
        MockMW.return_value.__exit__.return_value = False
        plan_stats = PlanStats()
        plan_stats.add_stat({"ENGINE": "islanddb"})
        plan_stats.add_stat({
            "ISLAND_TELEMETRY": {
                "engine": "islanddb",
                "elapsed_ms": 1.0,
                "elapsed_scope": (
                    "engine_after_admission_through_stream_close_"
                    "excludes_facade_and_profile_persist"
                ),
                "execution_outcome": "completed",
                "result_complete": True,
                "result_rows": 7,
                "result_rows_scope": "arrow_output_rows",
                "result_bytes": 56,
                "result_bytes_scope": "arrow_output_batch_logical_nbytes",
            },
        })

        extend_execution_plan(
            query_plan_manager=self._build_qpm("orders"),
            role_name="r",
            timing={},
            plan_stats=plan_stats,
            status="ok",
            message="",
            result_shape=None,
        )

        payload = mock_mw.log_metric.call_args.args[0]
        normalized = json.loads(payload["normalized_profile"])
        assert payload["result_rows"] == 7
        assert normalized["result_rows"] == 7
        assert normalized["result_rows_measured"] is True
        assert normalized["result_rows_scope"] == "arrow_output_rows"

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
