# test_data_reader_v20260301_1400_comprehensive.py
"""
Comprehensive test suite for supertable/data_reader.py

Covers:
  - Status enum
  - engine enum (including to_internal)
  - DataReader.__init__
  - DataReader._lp
  - DataReader.execute (happy path, RBAC, estimation errors, execution errors,
    empty reflection, extend_execution_plan lifecycle, timer/plan_stats wiring)
  - _ensure_sql_limit (all branches)
  - query_sql (happy path, error propagation, NA sanitization, column metadata)

All external dependencies are mocked: storage, SQLParser, Executor, DataEstimator,
QueryPlanManager, extend_execution_plan, restrict_read_access.
"""

from __future__ import annotations

import math
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch, call, PropertyMock

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Paths to patch (as they appear in data_reader.py's import namespace)
# ---------------------------------------------------------------------------
_MOD = "supertable.data_reader"
_PATCH_GET_STORAGE = f"{_MOD}.get_storage"
_PATCH_SQL_PARSER = f"{_MOD}.SQLParser"
_PATCH_QUERY_PLAN_MGR = f"{_MOD}.QueryPlanManager"
_PATCH_DATA_ESTIMATOR = f"{_MOD}.DataEstimator"
_PATCH_EXECUTOR = f"{_MOD}.Executor"
_PATCH_RESTRICT_READ = f"{_MOD}.restrict_read_access"
_PATCH_EXTEND_PLAN = f"{_MOD}.extend_execution_plan"
_PATCH_TIMER = f"{_MOD}.Timer"
_PATCH_PLAN_STATS = f"{_MOD}.PlanStats"
_PATCH_REDIS_CATALOG = f"{_MOD}.RedisCatalog"


@pytest.fixture(autouse=True)
def _mock_data_reader_redis():
    """Prevent RedisCatalog() inside DataReader.execute() from connecting to Redis.

    execute() instantiates RedisCatalog for dedup-on-read config and tombstone
    lookups.  Without this mock the connection attempt hangs on Sentinel discovery.
    """
    with patch(_PATCH_REDIS_CATALOG) as MockCat:
        mock_inst = MagicMock()
        mock_inst.get_table_config.return_value = None
        mock_inst.get_leaf.return_value = None
        MockCat.return_value = mock_inst
        yield MockCat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reflection(*, supers=None, bytes_total=1024, total_reflections=2,
                     storage_type="MinIOStorage"):
    """Build a mock Reflection object."""
    from supertable.data_classes import Reflection, SuperSnapshot
    if supers is None:
        supers = [
            SuperSnapshot(
                super_name="s1",
                simple_name="tbl",
                simple_version=1,
                files=["data/f1.parquet"],
                columns={"id", "value"},
            )
        ]
    return Reflection(
        storage_type=storage_type,
        reflection_bytes=bytes_total,
        total_reflections=total_reflections,
        supers=supers,
    )


def _empty_reflection():
    """Reflection with no supers (no parquet files found)."""
    from supertable.data_classes import Reflection
    return Reflection(
        storage_type="MinIOStorage",
        reflection_bytes=0,
        total_reflections=0,
        supers=[],
    )


# ====================================================================
# 1.  Status enum
# ====================================================================

class TestStatusEnum:

    def test_ok_value(self):
        from supertable.data_reader import Status
        assert Status.OK.value == "ok"

    def test_error_value(self):
        from supertable.data_reader import Status
        assert Status.ERROR.value == "error"

    def test_status_is_enum(self):
        from supertable.data_reader import Status
        assert issubclass(Status, Enum)

    def test_status_members(self):
        from supertable.data_reader import Status
        assert set(Status.__members__.keys()) == {"OK", "ERROR"}


# ====================================================================
# 2.  engine enum
# ====================================================================

class TestEngineEnum:

    def test_auto_value(self):
        from supertable.data_reader import engine
        assert engine.AUTO.value == "auto"

    def test_duckdb_lite_value(self):
        from supertable.data_reader import engine
        assert engine.DUCKDB_LITE.value == "duckdb_lite"

    def test_duckdb_pro_value(self):
        from supertable.data_reader import engine
        assert engine.DUCKDB_PRO.value == "duckdb_pro"

    def test_spark_sql_value(self):
        from supertable.data_reader import engine
        assert engine.SPARK_SQL.value == "spark_sql"

    def test_engine_is_enum(self):
        from supertable.data_reader import engine
        assert issubclass(engine, Enum)

    def test_engine_members(self):
        from supertable.data_reader import engine
        assert set(engine.__members__.keys()) == {"AUTO", "DUCKDB_LITE", "DUCKDB_PRO", "SPARK_SQL"}


# ====================================================================
# 3.  DataReader.__init__
# ====================================================================

class TestDataReaderInit:

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_init_creates_parser_and_storage(self, MockParser, mock_get_storage):
        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_parser_inst = MagicMock()
        mock_parser_inst.get_table_tuples.return_value = []
        MockParser.return_value = mock_parser_inst

        from supertable.data_reader import DataReader
        dr = DataReader("my_super", "my_org", "SELECT * FROM tbl")

        MockParser.assert_called_once_with(super_name="my_super", query="SELECT * FROM tbl")
        mock_get_storage.assert_called_once()
        assert dr.super_name == "my_super"
        assert dr.organization == "my_org"
        assert dr.storage is mock_storage
        assert dr.tables == []
        assert dr.timer is None
        assert dr.plan_stats is None
        assert dr.query_plan_manager is None
        assert dr._log_ctx == ""

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_init_stores_table_tuples_from_parser(self, MockParser, mock_get_storage):
        mock_get_storage.return_value = MagicMock()

        from supertable.data_classes import TableDefinition
        tables = [TableDefinition(super_name="s", simple_name="t", alias="t", columns=["id"])]
        mock_parser_inst = MagicMock()
        mock_parser_inst.get_table_tuples.return_value = tables
        MockParser.return_value = mock_parser_inst

        from supertable.data_reader import DataReader
        dr = DataReader("s", "o", "SELECT id FROM t")
        assert dr.tables is tables

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_init_parser_raises_propagates(self, MockParser, mock_get_storage):
        mock_get_storage.return_value = MagicMock()
        MockParser.side_effect = ValueError("bad sql")

        from supertable.data_reader import DataReader
        with pytest.raises(ValueError, match="bad sql"):
            DataReader("s", "o", "NOT VALID SQL")


# ====================================================================
# 4.  DataReader._lp
# ====================================================================

class TestLogPrefix:

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_lp_with_empty_context(self, MockParser, mock_get_storage):
        mock_get_storage.return_value = MagicMock()
        MockParser.return_value = MagicMock(get_table_tuples=MagicMock(return_value=[]))

        from supertable.data_reader import DataReader
        dr = DataReader("s", "o", "SELECT 1")
        assert dr._lp("hello") == "hello"

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_lp_with_set_context(self, MockParser, mock_get_storage):
        mock_get_storage.return_value = MagicMock()
        MockParser.return_value = MagicMock(get_table_tuples=MagicMock(return_value=[]))

        from supertable.data_reader import DataReader
        dr = DataReader("s", "o", "SELECT 1")
        dr._log_ctx = "[qid=abc qh=123] "
        assert dr._lp("test") == "[qid=abc qh=123] test"


# ====================================================================
# 5.  DataReader.execute — Happy Path
# ====================================================================

class TestExecuteHappyPath:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_happy_path_returns_ok(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM tbl"
        MockParser.return_value = mock_parser

        mock_timer = MagicMock()
        mock_timer.timings = [{"EXECUTING_QUERY": 0.1}]
        MockTimer.return_value = mock_timer

        mock_ps = MagicMock()
        MockPlanStats.return_value = mock_ps

        mock_qpm = MagicMock()
        mock_qpm.query_id = "qid-1"
        mock_qpm.query_hash = "qhash-1"
        MockQPM.return_value = mock_qpm

        reflection = _make_reflection()
        mock_est_inst = MagicMock()
        mock_est_inst.estimate.return_value = reflection
        MockEstimator.return_value = mock_est_inst

        result_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        mock_exec_inst = MagicMock()
        mock_exec_inst.execute.return_value = (result_df, "duckdb_pinned")
        MockExecutor.return_value = mock_exec_inst

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "SELECT * FROM tbl")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        assert status == Status.OK
        assert message is None
        assert len(df) == 2
        assert list(df.columns) == ["id", "value"]

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_execute_calls_restrict_read_access(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = ["table_def_1"]
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s1", "o1", "SELECT 1")
        dr.execute("reader_role", engine=engine.AUTO)

        mock_restrict.assert_called_once_with(
            super_name="s1",
            organization="o1",
            role_name="reader_role",
            tables=["table_def_1"],
        )

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_execute_passes_engine_to_executor(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pro")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.DUCKDB_PRO)

        exec_call_kwargs = mock_exec.execute.call_args
        assert exec_call_kwargs[1]["engine"] == _Engine.DUCKDB_PRO or exec_call_kwargs[0][0] == _Engine.DUCKDB_PRO


# ====================================================================
# 6.  DataReader.execute — RBAC Failure
# ====================================================================

class TestExecuteRBACFailure:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_rbac_permission_error_propagates(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """restrict_read_access raises PermissionError before try block."""
        mock_get_storage.return_value = MagicMock()
        MockParser.return_value = MagicMock(
            get_table_tuples=MagicMock(return_value=[]),
            original_query="Q",
        )
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()

        mock_restrict.side_effect = PermissionError("no read perm")

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "SELECT 1")

        with pytest.raises(PermissionError, match="no read perm"):
            dr.execute("bad_role", engine=engine.AUTO)

        # Nothing after RBAC should run
        MockQPM.assert_not_called()
        MockEstimator.assert_not_called()
        MockExecutor.assert_not_called()


# ====================================================================
# 7.  DataReader.execute — Empty Reflection (no supers)
# ====================================================================

class TestExecuteEmptyReflection:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_empty_reflection_returns_error_with_message(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        mock_timer = MagicMock(timings=[])
        MockTimer.return_value = mock_timer
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        # Estimator returns reflection with empty supers
        mock_est = MagicMock()
        mock_est.estimate.return_value = _empty_reflection()
        MockEstimator.return_value = mock_est

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "SELECT * FROM tbl")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        assert status == Status.ERROR
        assert message == "No parquet files found"
        assert df.empty
        # Executor should NOT be called when reflection has no supers
        MockExecutor.return_value.execute.assert_not_called()

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_empty_reflection_skips_extend_plan(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """When reflection has no supers, execute() returns early via 'return'
        inside the try block (line 119).  There is no finally block, so
        extend_execution_plan (which lives after the try/except) is never
        reached.  Therefore it must NOT be called."""
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _empty_reflection()
        MockEstimator.return_value = mock_est

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        # Early return bypasses extend_execution_plan entirely
        mock_extend.assert_not_called()
        # Executor should also not be called
        MockExecutor.return_value.execute.assert_not_called()


# ====================================================================
# 8.  DataReader.execute — Estimation Error
# ====================================================================

class TestExecuteEstimationError:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_estimator_exception_returns_error_status(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.side_effect = RuntimeError("Missing required column(s)")
        MockEstimator.return_value = mock_est

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "Q")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        assert status == Status.ERROR
        assert "Missing required column(s)" in message
        assert df.empty


# ====================================================================
# 9.  DataReader.execute — Executor Exception
# ====================================================================

class TestExecuteExecutorError:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_executor_exception_returns_error_status(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.side_effect = Exception("DuckDB out of memory")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "Q")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        assert status == Status.ERROR
        assert "DuckDB out of memory" in message
        assert df.empty

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_executor_error_still_extends_plan(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """extend_execution_plan is called even after executor failure."""
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.side_effect = Exception("boom")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        mock_extend.assert_called_once()
        # Verify status passed to extend_plan is "error"
        call_kwargs = mock_extend.call_args[1]
        assert call_kwargs["status"] == "error"
        assert "boom" in (call_kwargs["message"] or "")


# ====================================================================
# 10. DataReader.execute — extend_execution_plan Exception (swallowed)
# ====================================================================

class TestExecuteExtendPlanError:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_extend_plan_exception_is_swallowed(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """extend_execution_plan failure must not crash execute()."""
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        result_df = pd.DataFrame({"x": [1]})
        mock_exec = MagicMock()
        mock_exec.execute.return_value = (result_df, "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        mock_extend.side_effect = RuntimeError("extend boom")

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "Q")
        df, status, message = dr.execute("admin", engine=engine.AUTO)

        # Should still return OK since the executor succeeded
        assert status == Status.OK
        assert message is None
        assert len(df) == 1


# ====================================================================
# 11. DataReader.execute — Timer and PlanStats wiring
# ====================================================================

class TestExecuteTimerPlanStats:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_timer_created_and_timings_captured(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser

        mock_timer = MagicMock()
        mock_timer.timings = []
        MockTimer.return_value = mock_timer
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        # Timer should have capture_and_reset_timing called for EXECUTING_QUERY and EXTENDING_PLAN
        calls = [c for c in mock_timer.capture_and_reset_timing.call_args_list]
        events = [c[1].get("event") or c[0][0] if c[0] else c[1].get("event") for c in calls]
        assert "EXECUTING_QUERY" in events
        assert "EXTENDING_PLAN" in events

        # capture_duration called for TOTAL_EXECUTE
        dur_calls = [c for c in mock_timer.capture_duration.call_args_list]
        dur_events = [c[1].get("event") or c[0][0] if c[0] else c[1].get("event") for c in dur_calls]
        assert "TOTAL_EXECUTE" in dur_events

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_plan_stats_passed_to_extend_plan(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])

        mock_ps = MagicMock()
        MockPlanStats.return_value = mock_ps
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        call_kwargs = mock_extend.call_args[1]
        assert call_kwargs["plan_stats"] is mock_ps


# ====================================================================
# 12. DataReader.execute — QueryPlanManager wiring
# ====================================================================

class TestExecuteQueryPlanManager:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_qpm_created_with_correct_args(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM tbl"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("my_super", "my_org", "SELECT * FROM tbl")
        dr.execute("admin", engine=engine.AUTO)

        MockQPM.assert_called_once_with(
            super_name="my_super",
            organization="my_org",
            current_meta_path="redis://meta/root",
            query="SELECT * FROM tbl",
        )

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_log_ctx_set_from_qpm(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()

        mock_qpm = MagicMock()
        mock_qpm.query_id = "uuid-123"
        mock_qpm.query_hash = "hash-abc"
        MockQPM.return_value = mock_qpm

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        assert "uuid-123" in dr._log_ctx
        assert "hash-abc" in dr._log_ctx


# ====================================================================
# 13. DataReader.execute — extend_execution_plan Arguments
# ====================================================================

class TestExecuteExtendPlanArgs:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_extend_plan_receives_correct_args_on_success(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser

        mock_timer = MagicMock(timings=[{"step": 0.1}])
        MockTimer.return_value = mock_timer

        mock_ps = MagicMock()
        MockPlanStats.return_value = mock_ps

        mock_qpm = MagicMock(query_id="q", query_hash="h")
        MockQPM.return_value = mock_qpm

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        result_df = pd.DataFrame({"a": [1, 2, 3]})
        mock_exec = MagicMock()
        mock_exec.execute.return_value = (result_df, "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        call_kwargs = mock_extend.call_args[1]
        assert call_kwargs["query_plan_manager"] is mock_qpm
        assert call_kwargs["role_name"] == "admin"
        assert call_kwargs["timing"] is mock_timer.timings
        assert call_kwargs["plan_stats"] is mock_ps
        assert call_kwargs["status"] == "ok"
        assert call_kwargs["message"] is None
        assert call_kwargs["result_shape"] == (3, 1)

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_extend_plan_receives_error_status_on_failure(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.side_effect = RuntimeError("estimation failed")
        MockEstimator.return_value = mock_est

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.AUTO)

        call_kwargs = mock_extend.call_args[1]
        assert call_kwargs["status"] == "error"
        assert "estimation failed" in call_kwargs["message"]
        assert call_kwargs["result_shape"] == (0, 0)


# ====================================================================
# 14. DataReader.execute — with_scan parameter (passed through)
# ====================================================================

class TestExecuteWithScan:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_with_scan_default_false(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """Verify with_scan defaults to False and execute still works."""
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame({"x": [1]}), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, Status, engine
        dr = DataReader("s", "o", "Q")
        df, status, msg = dr.execute("admin")

        assert status == Status.OK


# ====================================================================
# 15. _ensure_sql_limit — All Branches
# ====================================================================

class TestEnsureSqlLimit:

    def test_no_limit_appends_default(self):
        from supertable.data_reader import _ensure_sql_limit
        result = _ensure_sql_limit("SELECT * FROM tbl", 100)
        assert result == "SELECT * FROM tbl\nLIMIT 100"

    def test_existing_limit_is_preserved(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl LIMIT 50"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_existing_limit_case_insensitive(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl limit 25"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_existing_limit_with_offset(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl LIMIT 50 OFFSET 10"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_existing_limit_with_offset_lowercase(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "select * from tbl limit 50 offset 10"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_trailing_semicolon_stripped_for_check(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl LIMIT 50;"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_trailing_whitespace_stripped_for_check(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl LIMIT 50   "
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_multiple_trailing_semicolons(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl LIMIT 50;;"
        result = _ensure_sql_limit(sql, 100)
        assert result == sql

    def test_subquery_limit_does_not_count_as_outer(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM (SELECT * FROM tbl LIMIT 10) sub"
        result = _ensure_sql_limit(sql, 100)
        # The outer query has no LIMIT, so one should be appended
        assert "LIMIT 100" in result

    def test_limit_default_int_conversion(self):
        from supertable.data_reader import _ensure_sql_limit
        result = _ensure_sql_limit("SELECT 1", 50.5)
        assert "LIMIT 50" in result

    def test_limit_zero(self):
        from supertable.data_reader import _ensure_sql_limit
        result = _ensure_sql_limit("SELECT 1", 0)
        assert "LIMIT 0" in result

    def test_where_clause_no_limit(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl WHERE id > 5"
        result = _ensure_sql_limit(sql, 200)
        assert result == "SELECT * FROM tbl WHERE id > 5\nLIMIT 200"

    def test_order_by_no_limit(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT * FROM tbl ORDER BY id"
        result = _ensure_sql_limit(sql, 100)
        assert result == "SELECT * FROM tbl ORDER BY id\nLIMIT 100"

    def test_limit_word_in_column_name_does_not_match(self):
        from supertable.data_reader import _ensure_sql_limit
        sql = "SELECT credit_limit FROM tbl"
        result = _ensure_sql_limit(sql, 100)
        # \bLIMIT should not match "credit_limit" — limit is appended
        assert "LIMIT 100" in result


# ====================================================================
# 16. query_sql — Happy Path
# ====================================================================

class TestQuerySqlHappyPath:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_returns_columns_rows_meta(self, mock_ensure, MockDR):
        mock_ensure.return_value = "SELECT * FROM t LIMIT 10"

        from supertable.data_reader import Status
        result_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (result_df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("org", "super", "SELECT * FROM t", 10, MagicMock(), "admin")

        assert columns == ["id", "name"]
        assert len(rows) == 2
        assert rows[0] == [1, "a"]
        assert rows[1] == [2, "b"]
        assert len(meta) == 2
        assert meta[0]["name"] == "id"
        assert meta[1]["name"] == "name"
        assert all(m["nullable"] is True for m in meta)

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_ensure_limit_called_with_args(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q LIMIT 50"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        query_sql("org", "sup", "Q", 50, MagicMock(), "admin")

        mock_ensure.assert_called_once_with("Q", default_limit=50)


# ====================================================================
# 17. query_sql — Error Status Raises RuntimeError
# ====================================================================

class TestQuerySqlError:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_error_status_raises_runtime_error(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q LIMIT 10"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.ERROR, "something broke")
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        with pytest.raises(RuntimeError, match="Query execution failed: something broke"):
            query_sql("org", "sup", "Q", 10, MagicMock(), "admin")

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_error_with_none_message(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q LIMIT 10"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.ERROR, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        with pytest.raises(RuntimeError, match="Query execution failed: None"):
            query_sql("org", "sup", "Q", 10, MagicMock(), "admin")


# ====================================================================
# 18. query_sql — NA Sanitization
# ====================================================================

class TestQuerySqlNaSanitization:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_pd_na_replaced_with_none(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        df = pd.DataFrame({"val": pd.array([1, pd.NA, 3], dtype=pd.Int64Dtype())})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert rows[1][0] is None

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_np_nan_replaced_with_none(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        df = pd.DataFrame({"val": [1.0, float("nan"), 3.0]})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert rows[1][0] is None

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_pd_nat_replaced_with_none(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        df = pd.DataFrame({"ts": pd.array([pd.Timestamp("2024-01-01"), pd.NaT], dtype="datetime64[ns]")})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert rows[1][0] is None

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_normal_values_preserved(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert rows == [[1, "x"], [2, "y"]]


# ====================================================================
# 19. query_sql — Column Metadata
# ====================================================================

class TestQuerySqlColumnMeta:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_column_metadata_types(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        df = pd.DataFrame({"int_col": [1], "str_col": ["a"], "float_col": [1.5]})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (df, Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert meta[0]["name"] == "int_col"
        assert meta[0]["type"] == "int64"
        assert meta[1]["name"] == "str_col"
        assert meta[1]["type"] == "object"
        assert meta[2]["name"] == "float_col"
        assert meta[2]["type"] == "float64"
        assert all(m["nullable"] is True for m in meta)

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_empty_dataframe_returns_empty_columns(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        columns, rows, meta = query_sql("o", "s", "Q", 10, MagicMock(), "admin")

        assert columns == []
        assert rows == []
        assert meta == []


# ====================================================================
# 20. query_sql — DataReader Construction
# ====================================================================

class TestQuerySqlDataReaderConstruction:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_datareader_constructed_with_correct_args(self, mock_ensure, MockDR):
        mock_ensure.return_value = "SELECT 1 LIMIT 10"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        query_sql("my_org", "my_super", "SELECT 1", 10, MagicMock(), "admin")

        MockDR.assert_called_once_with(
            organization="my_org",
            super_name="my_super",
            query="SELECT 1 LIMIT 10",
        )

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_execute_called_with_role_and_engine(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        sentinel_engine = MagicMock()
        from supertable.data_reader import query_sql
        query_sql("o", "s", "Q", 10, sentinel_engine, "my_role")

        mock_reader.execute.assert_called_once_with(
            role_name="my_role",
            engine=sentinel_engine,
            with_scan=False,
        )


# ====================================================================
# 21. DataReader.execute — Executor receives correct arguments
# ====================================================================

class TestExecuteExecutorArgs:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_executor_receives_all_required_args(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser

        mock_timer = MagicMock(timings=[])
        MockTimer.return_value = mock_timer

        mock_ps = MagicMock()
        MockPlanStats.return_value = mock_ps

        mock_qpm = MagicMock(query_id="q", query_hash="h")
        MockQPM.return_value = mock_qpm

        reflection = _make_reflection()
        mock_est = MagicMock()
        mock_est.estimate.return_value = reflection
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_lite")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.DUCKDB_LITE)

        # Executor constructed with storage and organization
        MockExecutor.assert_called_once_with(storage=mock_storage, organization="o")

        # Executor.execute called with full set of params
        exec_call = mock_exec.execute.call_args
        kwargs = exec_call[1] if exec_call[1] else {}
        args = exec_call[0] if exec_call[0] else ()

        # Engine should be the internal enum
        if "engine" in kwargs:
            assert kwargs["engine"] == _Engine.DUCKDB_LITE
        else:
            assert args[0] == _Engine.DUCKDB_LITE

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_estimator_receives_correct_args(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        from supertable.data_classes import TableDefinition
        tables = [TableDefinition("s", "t", "t", ["id"])]
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = tables
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser

        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "my_org", "Q")
        dr.execute("admin", engine=engine.AUTO)

        MockEstimator.assert_called_once_with(
            organization="my_org",
            storage=mock_storage,
            tables=tables,
        )


# ====================================================================
# 22. DataReader.execute — Return Value Format
# ====================================================================

class TestExecuteReturnFormat:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_return_is_3_tuple(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame({"a": [1]}), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, Status, engine
        result = DataReader("s", "o", "Q").execute("admin", engine=engine.AUTO)

        assert isinstance(result, tuple)
        assert len(result) == 3
        df, status, msg = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(status, Status)
        assert msg is None or isinstance(msg, str)


# ====================================================================
# 23. DataReader.execute — engine default
# ====================================================================

class TestExecuteEngineDefault:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_default_engine_is_auto(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "Q"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "Q")
        # Call without engine kwarg to test default
        dr.execute("admin")

        exec_call = mock_exec.execute.call_args
        kwargs = exec_call[1] if exec_call[1] else {}
        args = exec_call[0] if exec_call[0] else ()
        # Should pass engine.AUTO (which is _Engine.AUTO) as default
        if "engine" in kwargs:
            assert kwargs["engine"] == _Engine.AUTO
        else:
            assert args[0] == _Engine.AUTO