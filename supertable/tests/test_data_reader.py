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

import logging
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


@pytest.fixture(autouse=True)
def _isolate_binding_stability_guard():
    """These orchestration tests use unconstrained SQLParser/RBAC mocks.

    The guard's schema-dependent deny behavior has executable coverage in the
    engine suite; patch it here so unrelated lifecycle assertions do not depend
    on MagicMock's fabricated ``get_binding_ambiguities`` return value.
    """
    with patch(f"{_MOD}.validate_rbac_binding_stability") as guard:
        yield guard


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

    def test_duckdb_value(self):
        from supertable.data_reader import engine
        assert engine.DUCKDB.value == "duckdb"

    def test_duckdb_value(self):
        from supertable.data_reader import engine
        assert engine.DUCKDB.value == "duckdb"

    def test_islanddb_value(self):
        from supertable.data_reader import engine
        assert engine.ISLANDDB.value == "islanddb"

    def test_spark_sql_value(self):
        from supertable.data_reader import engine
        assert engine.SPARK_SQL.value == "spark_sql"

    def test_engine_is_enum(self):
        from supertable.data_reader import engine
        assert issubclass(engine, Enum)

    def test_engine_members(self):
        from supertable.data_reader import engine
        assert set(engine.__members__.keys()) == {
            "AUTO", "DUCKDB", "DUCKDB", "ISLANDDB", "SPARK_SQL",
        }


# ====================================================================
# 3.  DataReader.__init__
# ====================================================================

class TestDataReaderInit:

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_init_creates_storage(self, MockParser, mock_get_storage):
        # DataReader.__init__ now defers SQLParser construction to execute()
        # (the parser needs the engine's dialect, which isn't known at init
        # time). Init just stashes inputs and grabs the storage handle.
        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        from supertable.data_reader import DataReader
        dr = DataReader("my_super", "my_org", "SELECT * FROM tbl")

        # Parser is not constructed in __init__ anymore.
        MockParser.assert_not_called()
        mock_get_storage.assert_called_once()
        assert dr.super_name == "my_super"
        assert dr.organization == "my_org"
        assert dr.query == "SELECT * FROM tbl"
        assert dr.storage is mock_storage
        assert dr.timer is None
        assert dr.plan_stats is None
        assert dr.query_plan_manager is None
        assert dr._log_ctx == ""

    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_init_does_not_eagerly_parse(self, MockParser, mock_get_storage):
        # The parser is built lazily inside execute(), so a bad SQL string
        # does not raise from __init__.
        mock_get_storage.return_value = MagicMock()
        MockParser.side_effect = ValueError("bad sql")

        from supertable.data_reader import DataReader
        # Should NOT raise — parser is not invoked here.
        dr = DataReader("s", "o", "NOT VALID SQL")
        assert dr.query == "NOT VALID SQL"
        MockParser.assert_not_called()

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_execute_parser_rejection_returns_clean_error_status(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        # Parser errors are now surfaced during execute(), not __init__.
        mock_get_storage.return_value = MagicMock()
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockParser.side_effect = ValueError("bad sql")

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "NOT VALID SQL")
        frame, status, message = dr.execute("admin", engine=engine.AUTO)
        assert frame.empty
        assert status.value == "error"
        assert message == "Query is invalid or unsupported"


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

        # restrict_read_access is now invoked with both ``tables`` (parser
        # alias-level tuples) and ``physical_tables`` (post-CTE physical
        # tables). The parser mock returns MagicMock for get_physical_tables.
        mock_restrict.assert_called_once_with(
            super_name="s1",
            organization="o1",
            role_name="reader_role",
            tables=["table_def_1"],
            physical_tables=mock_parser.get_physical_tables.return_value,
            aggregate_children=None,
            expected_role_policy_fingerprint=None,
            policy_fingerprints_out={},
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
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.DUCKDB)

        exec_call_kwargs = mock_exec.execute.call_args
        assert exec_call_kwargs[1]["engine"] == _Engine.DUCKDB or exec_call_kwargs[0][0] == _Engine.DUCKDB


# ====================================================================
# 5b. DataReader.execute — Tombstone deletion-vector path resolution
# ====================================================================

class TestExecuteTombstoneResolution:
    """Regression: the tombstone deletion-vector pointer must be resolved
    through the SAME path as the data files (``estimator._to_duckdb_path``,
    see data_estimator.py:426). Data files are presigned for object stores;
    the tombstone key used to be embedded raw, so DuckDB/Spark could not read
    it. These lock in 'if the data is presigned, the tombstone is too'.
    """

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_tombstone_pointer_resolved_through_same_resolver_as_data(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
        _mock_data_reader_redis,
    ):
        from types import SimpleNamespace
        from supertable.data_classes import SuperSnapshot

        td = SimpleNamespace(alias="t", super_name="s", simple_name="tbl")
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [td]
        mock_parser.original_query = "SELECT * FROM tbl"
        MockParser.return_value = mock_parser

        mock_get_storage.return_value = MagicMock()
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        # A newer leaf may exist by the time execution wiring runs.  The reader
        # must use the pointer pinned by the estimator, not re-read this leaf.
        raw_tomb_key = "o/s/tables/tbl/tombstone/123_abc_deleted.parquet"
        _mock_data_reader_redis.return_value.get_leaf.return_value = {
            "payload": {"tombstone": "newer/dv.parquet"},
        }

        # Estimator resolver presigns (object-store behaviour) — the SAME method
        # the estimator applies to data files at data_estimator.py:426.
        reflection = _make_reflection(supers=[SuperSnapshot(
            super_name="s",
            simple_name="tbl",
            simple_version=1,
            files=["data/f1.parquet"],
            columns={"id"},
            snapshot_path="snap-v1.json",
            tombstone_key=raw_tomb_key,
            tombstone_rows=7,
            tombstone_digest="0" * 64,
        )])
        mock_est = MagicMock()
        mock_est.estimate.return_value = reflection
        mock_est._to_duckdb_path.side_effect = lambda k: f"https://signed/{k}"
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "SELECT * FROM tbl")
        dr.execute("admin", engine=engine.AUTO)

        # The raw key was routed through the resolver (not embedded as-is) ...
        mock_est._to_duckdb_path.assert_called_once_with(raw_tomb_key)
        # ... and the RESOLVED (presigned) value is what lands in the reflection.
        assert "t" in reflection.tombstone_views
        assert (
            reflection.tombstone_views["t"].tombstone_path
            == f"https://signed/{raw_tomb_key}"
        )
        # cache_key is the BARE (pre-presign) key — stable across appends,
        # so the DuckDB DV-table cache keys on it, not the rotating URL.
        assert reflection.tombstone_views["t"].cache_key == raw_tomb_key
        assert reflection.tombstone_views["t"].expected_rows == 7
        _mock_data_reader_redis.return_value.get_leaf.assert_not_called()

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_local_storage_tombstone_left_unchanged(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
        _mock_data_reader_redis,
    ):
        # LOCAL contract: no presign → _to_duckdb_path returns the key as-is,
        # so the tombstone path stays readable from disk (golden-test path).
        from types import SimpleNamespace
        from supertable.data_classes import SuperSnapshot

        td = SimpleNamespace(alias="t", super_name="s", simple_name="tbl")
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [td]
        mock_parser.original_query = "SELECT * FROM tbl"
        MockParser.return_value = mock_parser

        mock_get_storage.return_value = MagicMock()
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        raw_tomb_key = "local/dir/tombstone/x_deleted.parquet"
        _mock_data_reader_redis.return_value.get_leaf.return_value = {
            "payload": {"tombstone": raw_tomb_key},
        }

        reflection = _make_reflection(storage_type="LocalStorage", supers=[SuperSnapshot(
            super_name="s",
            simple_name="tbl",
            simple_version=1,
            files=["data/f1.parquet"],
            columns={"id"},
            tombstone_key=raw_tomb_key,
            tombstone_rows=1,
            tombstone_digest="0" * 64,
        )])
        mock_est = MagicMock()
        mock_est.estimate.return_value = reflection
        mock_est._to_duckdb_path.side_effect = lambda k: k  # LOCAL no-op
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "SELECT * FROM tbl")
        dr.execute("admin", engine=engine.AUTO)

        assert reflection.tombstone_views["t"].tombstone_path == raw_tomb_key
        # LOCAL: no presign, so cache_key equals the (also unchanged) path.
        assert reflection.tombstone_views["t"].cache_key == raw_tomb_key


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
        assert message == "Query execution failed"
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
        assert message == "Query execution failed"
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
        assert call_kwargs["message"] == "Query execution failed"


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

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_monitoring_durability_failure_is_explicit_post_execution(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        from supertable.monitoring_writer import (
            MonitoringBackpressureError,
            MonitoringPostExecutionError,
        )

        mock_get_storage.return_value = MagicMock()
        parser = MagicMock()
        parser.get_table_tuples.return_value = []
        parser.original_query = "Q"
        MockParser.return_value = parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(
            query_id="q-monitor", query_hash="h",
            organization="o", super_name="s",
        )
        estimator = MagicMock()
        estimator.estimate.return_value = _make_reflection()
        MockEstimator.return_value = estimator
        MockExecutor.return_value.execute.return_value = (
            pd.DataFrame({"x": [1]}), "duckdb_pinned",
        )
        mock_extend.side_effect = MonitoringBackpressureError("spool full")

        from supertable.data_reader import DataReader, engine
        reader = DataReader("s", "o", "Q")
        with pytest.raises(MonitoringPostExecutionError) as raised:
            reader.execute("admin", engine=engine.AUTO)

        assert raised.value.execution_completed is True
        assert raised.value.query_id == "q-monitor"
        assert raised.value.status == "ok"


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
        assert call_kwargs["message"] == "Query execution failed"
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

    def test_fetch_rows_only_below_cap_is_preserved_including_zero(self):
        from supertable.data_reader import _ensure_sql_limit

        for sql in (
            "SELECT * FROM tbl FETCH FIRST ROW ONLY",
            "SELECT * FROM tbl FETCH FIRST 3 ROWS ONLY",
            "SELECT * FROM tbl FETCH NEXT 0 ROWS ONLY",
            "SELECT * FROM tbl OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
        ):
            assert _ensure_sql_limit(sql, 5) == sql

    def test_fetch_rows_only_over_cap_is_clamped_without_widening(self):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        sql = "SELECT * FROM range(20) OFFSET 2 ROWS FETCH NEXT 9 ROWS ONLY"
        bounded = _ensure_sql_limit(sql, 5)

        assert "LIMIT 5 OFFSET 2" in bounded
        assert duckdb.connect().execute(bounded).fetchall() == [
            (2,), (3,), (4,), (5,), (6,),
        ]

    def test_fetch_null_is_unbounded_and_clamped_to_server_cap(self):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        sql = "SELECT * FROM range(20) OFFSET 2 ROWS FETCH NEXT NULL ROWS ONLY"
        bounded = _ensure_sql_limit(sql, 5)

        assert duckdb.connect().execute(bounded).fetchall() == [
            (2,), (3,), (4,), (5,), (6,),
        ]

    @pytest.mark.parametrize(
        "clause",
        ["LIMIT (NULL)", "FETCH NEXT (NULL) ROWS ONLY"],
    )
    def test_parenthesized_null_is_unbounded_and_clamped(self, clause):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        bounded = _ensure_sql_limit(f"SELECT * FROM range(20) {clause}", 5)

        assert duckdb.connect().execute(bounded).fetchall() == [
            (0,), (1,), (2,), (3,), (4,),
        ]

    def test_parenthesized_all_stays_invalid_instead_of_becoming_bounded(self):
        from supertable.data_reader import _ensure_sql_limit

        with pytest.raises(ValueError, match="LIMIT"):
            _ensure_sql_limit("SELECT * FROM tbl LIMIT (ALL)", 5)

    @pytest.mark.parametrize("modifier", ["WITH TIES", "PERCENT ROWS ONLY"])
    def test_non_hard_fetch_modifier_is_rejected_not_silently_rewritten(self, modifier):
        from supertable.data_reader import _ensure_sql_limit

        if modifier == "WITH TIES":
            sql = "SELECT * FROM tbl ORDER BY id FETCH FIRST 3 ROWS WITH TIES"
        else:
            sql = "SELECT * FROM tbl FETCH FIRST 10 PERCENT ROWS ONLY"

        with pytest.raises(ValueError, match="bounded read path"):
            _ensure_sql_limit(sql, 5)

    def test_fetch_zero_with_duplicate_columns_and_semicolon_is_preserved(self):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        sql = (
            "SELECT range AS x, range + 1 AS x FROM range(3) "
            "FETCH FIRST 0 ROWS ONLY;"
        )
        bounded = _ensure_sql_limit(sql, 5)

        assert bounded == sql
        result = duckdb.connect().execute(bounded)
        assert result.fetchall() == []
        assert [item[0] for item in result.description] == ["x", "x"]

    def test_clamped_fetch_preserves_duplicate_output_names_and_semantics(self):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        sql = (
            "SELECT range AS x, range + 1 AS x FROM range(20) "
            "FETCH FIRST 9 ROWS ONLY;"
        )
        bounded = _ensure_sql_limit(sql, 5)

        result = duckdb.connect().execute(bounded)
        assert result.fetchall() == [
            (0, 1), (1, 2), (2, 3), (3, 4), (4, 5),
        ]
        assert [item[0] for item in result.description] == ["x", "x"]

    @pytest.mark.parametrize(
        ("bound", "expected_rows"),
        [
            ("0 + 0", 0),
            ("CAST(2 AS INTEGER)", 2),
            ("(3)", 3),
            ("1 * 2", 2),
        ],
    )
    def test_valid_constant_limit_expression_below_cap_is_not_widened(
        self, bound, expected_rows,
    ):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        sql = f"SELECT * FROM range(20) LIMIT {bound}"
        bounded = _ensure_sql_limit(sql, 5)

        assert bounded == sql
        assert len(duckdb.connect().execute(bounded).fetchall()) == expected_rows

    def test_constant_limit_expression_over_cap_is_safely_clamped(self):
        import duckdb
        from supertable.data_reader import _ensure_sql_limit

        bounded = _ensure_sql_limit(
            "SELECT * FROM range(20) LIMIT (3 * 3)", 5,
        )

        assert len(duckdb.connect().execute(bounded).fetchall()) == 5

    @pytest.mark.parametrize(
        "bound",
        [
            "?",
            "random()",
            "2147483647 + 1",
            "CAST(999 AS TINYINT)",
            "9223372036854775808",
            "-1",
        ],
    )
    def test_unproven_or_invalid_limit_expression_fails_closed(self, bound):
        from supertable.data_reader import _ensure_sql_limit

        with pytest.raises(ValueError, match="LIMIT"):
            _ensure_sql_limit(f"SELECT * FROM tbl LIMIT {bound}", 5)

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
        with pytest.raises(RuntimeError, match=r"^Query execution failed$") as exc_info:
            query_sql("org", "sup", "Q", 10, MagicMock(), "admin")
        assert "something broke" not in str(exc_info.value)

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_error_with_none_message(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q LIMIT 10"

        from supertable.data_reader import Status
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.ERROR, None)
        MockDR.return_value = mock_reader

        from supertable.data_reader import query_sql
        with pytest.raises(RuntimeError, match=r"^Query execution failed$"):
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
            source="sdk",
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

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_source_is_forwarded_to_datareader(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"
        from supertable.data_reader import Status, query_sql
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        query_sql("o", "s", "Q", 10, MagicMock(), "admin", source="mcp")

        assert MockDR.call_args.kwargs["source"] == "mcp"

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_out_dict_receives_query_identity(self, mock_ensure, MockDR):
        mock_ensure.return_value = "Q"
        from supertable.data_reader import Status, query_sql
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        mock_reader.query_plan_manager.query_id = "qid-123"
        mock_reader.query_plan_manager.query_hash = "qh-abc"
        MockDR.return_value = mock_reader

        out = {}
        query_sql("o", "s", "Q", 10, MagicMock(), "admin", source="mcp", out=out)

        assert out == {"query_id": "qid-123", "query_hash": "qh-abc"}


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
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "Q")
        dr.execute("admin", engine=engine.DUCKDB)

        # Executor receives the bounded AUTO history provider in addition to
        # the pre-existing storage/organization execution context.
        MockExecutor.assert_called_once()
        constructor = MockExecutor.call_args.kwargs
        assert constructor["storage"] is mock_storage
        assert constructor["organization"] == "o"
        assert callable(constructor["auto_history_provider"])

        # Executor.execute called with full set of params
        exec_call = mock_exec.execute.call_args
        kwargs = exec_call[1] if exec_call[1] else {}
        args = exec_call[0] if exec_call[0] else ()

        # Engine should be the internal enum
        if "engine" in kwargs:
            assert kwargs["engine"] == _Engine.DUCKDB
        else:
            assert args[0] == _Engine.DUCKDB

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
        physical_tables = [TableDefinition("s", "t", "t", ["id"])]
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = tables
        mock_parser.get_physical_tables.return_value = physical_tables
        mock_parser.get_predicate_constraints.return_value = {}
        mock_parser.get_join_edges.return_value = []
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

        # DataEstimator now receives physical_tables (post-CTE), which the
        # parser exposes via get_physical_tables().
        MockEstimator.assert_called_once_with(
            organization="my_org",
            storage=mock_storage,
            tables=physical_tables,
            predicate_constraints={},
            join_edges=[],
            join_pruning_lanes=frozenset({"numeric"}),
            plan_stats=MockPlanStats.return_value,
            require_odata_identity=False,
            require_bounded_resource_estimates=True,
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


# ====================================================================
# 24. DataReader.execute — EXPLAIN routing (system_query classifier)
# ====================================================================

class TestExecuteExplainRouting:

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_explain_passes_flags_and_forces_lite(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM t"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame({"plan": ["x"]}), "duckdb")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, Status, engine
        from supertable.engine.executor import Engine as _Engine
        # Request IslandDB — EXPLAIN must override it to DuckDB.
        dr = DataReader("s", "o", "EXPLAIN SELECT * FROM t")
        df, status, msg = dr.execute("admin", engine=engine.DUCKDB)

        assert status == Status.OK
        # Parser is built on the INNER select only (EXPLAIN prefix stripped).
        assert MockParser.call_args.kwargs["query"] == "SELECT * FROM t"
        # Executor receives explain flags and is pinned to DuckDB.
        ekw = mock_exec.execute.call_args.kwargs
        assert ekw["explain"] is True
        assert ekw["explain_options"] == ""
        assert ekw["engine"] == _Engine.DUCKDB

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_explain_analyze_is_rejected_before_executor(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT 1"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        dr = DataReader("s", "o", "EXPLAIN ANALYZE SELECT 1")
        frame, status, message = dr.execute("admin", engine=engine.AUTO)

        assert frame.empty
        assert status.value == "error"
        assert "EXPLAIN ANALYZE" in message
        mock_exec.execute.assert_not_called()

    @patch(_PATCH_EXTEND_PLAN)
    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_QUERY_PLAN_MGR)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_PLAN_STATS)
    @patch(_PATCH_TIMER)
    @patch(_PATCH_GET_STORAGE)
    @patch(_PATCH_SQL_PARSER)
    def test_plain_select_passes_explain_false(
        self, MockParser, mock_get_storage, MockTimer, MockPlanStats,
        mock_restrict, MockQPM, MockEstimator, MockExecutor, mock_extend,
    ):
        """Regression guard: ordinary SELECT routes with explain=False and the
        engine the caller requested (no DuckDB override)."""
        mock_get_storage.return_value = MagicMock()
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM t"
        MockParser.return_value = mock_parser
        MockTimer.return_value = MagicMock(timings=[])
        MockPlanStats.return_value = MagicMock()
        MockQPM.return_value = MagicMock(query_id="q", query_hash="h")

        mock_est = MagicMock()
        mock_est.estimate.return_value = _make_reflection()
        MockEstimator.return_value = mock_est

        mock_exec = MagicMock()
        mock_exec.execute.return_value = (pd.DataFrame(), "duckdb")
        MockExecutor.return_value = mock_exec

        from supertable.data_reader import DataReader, engine
        from supertable.engine.executor import Engine as _Engine
        dr = DataReader("s", "o", "SELECT * FROM t")
        dr.execute("admin", engine=engine.DUCKDB)

        ekw = mock_exec.execute.call_args.kwargs
        assert ekw["explain"] is False
        assert ekw["explain_options"] == ""
        assert ekw["engine"] == _Engine.DUCKDB  # not overridden


# ====================================================================
# 25. DataReader.execute — rejected / malformed commands
# ====================================================================

class TestExecuteRejectedCommands:

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_no_table_returns_error(
        self, mock_get_storage, MockParser, MockEstimator, MockExecutor,
    ):
        mock_get_storage.return_value = MagicMock()
        from supertable.data_reader import DataReader, Status
        dr = DataReader("s", "o", "SHOW STATS")
        df, status, msg = dr.execute("admin")

        assert status == Status.ERROR
        assert msg == "Query is invalid or unsupported"
        assert df.empty
        # Rejected before any pipeline work.
        MockParser.assert_not_called()
        MockEstimator.assert_not_called()
        MockExecutor.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_GET_STORAGE)
    def test_explain_non_select_returns_error(
        self, mock_get_storage, MockParser, MockEstimator, MockExecutor,
    ):
        mock_get_storage.return_value = MagicMock()
        from supertable.data_reader import DataReader, Status
        dr = DataReader("s", "o", "EXPLAIN DELETE FROM t")
        df, status, msg = dr.execute("admin")

        assert status == Status.ERROR
        assert msg == "Query is invalid or unsupported"
        MockExecutor.assert_not_called()
        MockEstimator.assert_not_called()


# ====================================================================
# 26. DataReader.execute — SHOW STATS short-circuit
# ====================================================================

class TestExecuteShowStats:

    def test_stats_context_detects_linked_share_filter_in_atomic_leaf(self):
        from supertable.data_reader import DataReader

        reader = DataReader.__new__(DataReader)
        reader.organization = "o"
        with patch(_PATCH_REDIS_CATALOG) as Catalog:
            Catalog.return_value.get_leaf.return_value = {
                "version": 1,
                "payload": {
                    "snapshot_version": 1,
                    "schema": {"id": "Int64"},
                    "resources": [],
                    "tombstone": None,
                    "tombstone_rows": 0,
                    "tombstone_digest": None,
                    "stats_file": "private/full-stats.parquet",
                    "_row_filter": "tenant = 'shared'",
                },
            }
            assert reader._resolve_latest_stats_context(
                "mysuper", "mytable",
            ) == ("private/full-stats.parquet", True)

    @pytest.mark.parametrize(
        "outer_filter",
        ["tenant = 'shared'", {"malformed": True}],
    )
    def test_stats_context_fails_closed_on_outer_leaf_filter(
        self, outer_filter,
    ):
        from supertable.data_reader import DataReader

        reader = DataReader.__new__(DataReader)
        reader.organization = "o"
        with patch(_PATCH_REDIS_CATALOG) as Catalog:
            Catalog.return_value.get_leaf.return_value = {
                "version": 1,
                "_row_filter": outer_filter,
                "payload": {
                    "snapshot_version": 1,
                    "schema": {"id": "Int64"},
                    "resources": [],
                    "tombstone": None,
                    "tombstone_rows": 0,
                    "tombstone_digest": None,
                    "stats_file": "private/full-stats.parquet",
                    "_row_filter": None,
                },
            }
            assert reader._resolve_latest_stats_context(
                "mysuper", "mytable",
            ) == ("private/full-stats.parquet", True)

    def test_stats_context_detects_conflicting_direct_and_nested_filters(self):
        from supertable.data_reader import DataReader

        snapshot = {
            "snapshot_version": 1,
            "schema": {"id": "Int64"},
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "stats_file": "private/full-stats.parquet",
        }
        reader = DataReader.__new__(DataReader)
        reader.organization = "o"
        with patch(_PATCH_REDIS_CATALOG) as Catalog:
            Catalog.return_value.get_leaf.return_value = {
                "version": 1,
                "payload": {
                    **snapshot,
                    "_row_filter": None,
                    "snapshot": {
                        **snapshot,
                        "_row_filter": "tenant = 'nested'",
                    },
                },
            }
            assert reader._resolve_latest_stats_context(
                "mysuper", "mytable",
            ) == ("private/full-stats.parquet", True)

    def test_stats_context_loads_heavy_policy_when_cache_marker_is_missing(self):
        """A tombstone-complete legacy cache cannot hide a storage filter."""
        from supertable.data_reader import DataReader

        cached = {
            "snapshot_version": 1,
            "schema": {"id": "Int64", "tenant": "String"},
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "stats_file": "cached/full-stats.parquet",
        }
        heavy = {
            **cached,
            "stats_file": "heavy/full-stats.parquet",
            "_row_filter": "tenant = 'shared'",
        }
        reader = DataReader.__new__(DataReader)
        reader.organization = "o"
        with (
            patch(_PATCH_REDIS_CATALOG) as Catalog,
            patch("supertable.super_table.SuperTable") as SuperTable,
        ):
            Catalog.return_value.get_leaf.return_value = {
                "version": 1,
                "path": "snapshots/v1.json",
                "payload": cached,
            }
            SuperTable.return_value.read_simple_table_snapshot.return_value = heavy

            assert reader._resolve_latest_stats_context(
                "mysuper", "mytable",
            ) == ("heavy/full-stats.parquet", True)
            SuperTable.return_value.read_simple_table_snapshot.assert_called_once_with(
                "snapshots/v1.json"
            )

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_returns_stats_frame(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        import polars as pl
        from supertable.processing import STATS_SCHEMA

        # In-memory stats frame with the canonical schema. The bounded
        # diagnostic loader is covered separately, so stub it here and
        # exercise the handler's resolve -> project -> to_pandas path directly.
        rows = [{
            "file_path": "data/f1.parquet", "row_group_id": 0,
            "column_name": "id", "physical_type": "INT64", "logical_type": "",
            "min_bigint": 1, "max_bigint": 99,
            "min_double": None, "max_double": None,
            "min_timestamp": None, "max_timestamp": None,
            "min_string": None, "max_string": None,
            "null_count": 0, "row_group_rows": 99,
            "stats_available": True, "min_is_exact": True, "max_is_exact": True,
        }]
        stats_df = pl.DataFrame(rows, schema=STATS_SCHEMA)

        mock_get_storage.return_value = MagicMock()
        from supertable.data_reader import DataReader, Status

        with patch.object(
            DataReader, "_resolve_latest_stats_context",
            return_value=("redis://stats/v1", 1, False),
        ), patch(
            "supertable.processing.load_bounded_stats_diagnostic",
            return_value=stats_df,
        ):
            dr = DataReader("mysuper", "o", "SHOW STATS mysuper.mytable")
            df, status, msg = dr.execute("admin")

        assert status == Status.OK
        assert msg is None
        assert list(df.columns) == list(STATS_SCHEMA.keys())
        assert len(df) == 1
        assert df.iloc[0]["column_name"] == "id"
        assert df.iloc[0]["max_bigint"] == 99
        # RBAC table-gate enforced; engine pipeline skipped entirely.
        mock_restrict.assert_called_once()
        MockEstimator.assert_not_called()
        MockExecutor.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_no_artifact_returns_empty_schema_frame(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        from supertable.processing import STATS_SCHEMA
        mock_get_storage.return_value = MagicMock()
        from supertable.data_reader import DataReader, Status

        # No stats pointer -> empty frame carrying the schema columns, OK status.
        with patch.object(
            DataReader,
            "_resolve_latest_stats_context",
            return_value=(None, None, False),
        ):
            dr = DataReader("mysuper", "o", "SHOW STATS mytable")
            df, status, msg = dr.execute("admin")

        assert status == Status.OK
        assert msg is None
        assert df.empty
        assert list(df.columns) == list(STATS_SCHEMA.keys())

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_rejects_oversized_row_seal_before_storage_or_pandas(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        import polars as pl
        from supertable.data_reader import DataReader, Status
        from supertable.processing import MAX_SHOW_STATS_ROWS

        storage = MagicMock()
        mock_get_storage.return_value = storage
        with patch.object(
            DataReader,
            "_resolve_latest_stats_context",
            return_value=(
                "private/stats/v1.parquet",
                MAX_SHOW_STATS_ROWS + 1,
                False,
            ),
        ), patch.object(pl.DataFrame, "to_pandas") as to_pandas:
            dr = DataReader("mysuper", "o", "SHOW STATS mytable")
            result, status, message = dr.execute("admin")

        assert status is Status.ERROR
        assert result.empty
        assert message == "SHOW STATS artifact is unavailable"
        storage.stat_object.assert_not_called()
        storage.download_to_file.assert_not_called()
        storage.read_parquet.assert_not_called()
        to_pandas.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_rejects_oversized_object_before_read_or_pandas(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        import polars as pl
        from supertable.data_reader import DataReader, Status
        from supertable.processing import MAX_SHOW_STATS_OBJECT_BYTES
        from supertable.storage.storage_interface import ObjectMetadata

        storage = MagicMock()
        storage.stat_object.return_value = ObjectMetadata(
            size=MAX_SHOW_STATS_OBJECT_BYTES + 1,
            version="immutable-v1",
        )
        mock_get_storage.return_value = storage
        with patch.object(
            DataReader,
            "_resolve_latest_stats_context",
            return_value=("private/stats/v1.parquet", 1, False),
        ), patch.object(pl.DataFrame, "to_pandas") as to_pandas:
            dr = DataReader("mysuper", "o", "SHOW STATS mytable")
            result, status, message = dr.execute("admin")

        assert status is Status.ERROR
        assert result.empty
        assert message == "SHOW STATS artifact is unavailable"
        storage.stat_object.assert_called_once_with(
            "private/stats/v1.parquet",
        )
        storage.download_to_file.assert_not_called()
        storage.read_parquet.assert_not_called()
        to_pandas.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_artifact_failure_redacts_physical_url(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor, caplog,
    ):
        from supertable.data_reader import DataReader, Status

        secret_url = (
            "https://objects.invalid/private/stats.parquet?signature=secret"
        )
        mock_get_storage.return_value = MagicMock()
        with caplog.at_level(
            logging.ERROR, logger="supertable.config.defaults",
        ):
            with patch.object(
                DataReader,
                "_resolve_latest_stats_context",
                return_value=(secret_url, 1, False),
            ), patch(
                "supertable.processing.load_bounded_stats_diagnostic",
                side_effect=RuntimeError(f"download failed for {secret_url}"),
            ):
                dr = DataReader("mysuper", "o", "SHOW STATS mytable")
                result, status, message = dr.execute("admin")

        assert status is Status.ERROR
        assert result.empty
        assert message == "SHOW STATS artifact is unavailable"
        assert secret_url not in caplog.text + str(message)
        assert "signature=secret" not in caplog.text
        assert "error_type=RuntimeError" in caplog.text

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_rbac_denial_propagates(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        mock_get_storage.return_value = MagicMock()
        mock_restrict.side_effect = PermissionError("no access to mytable")
        from supertable.data_reader import DataReader

        dr = DataReader("mysuper", "o", "SHOW STATS mytable")
        with pytest.raises(PermissionError, match="no access to mytable"):
            dr.execute("reader_role")

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_rejects_rbac_row_filter_before_artifact_resolution(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        from supertable.data_classes import RbacViewDef
        from supertable.data_reader import DataReader

        mock_get_storage.return_value = MagicMock()
        mock_restrict.return_value = {
            "mytable": RbacViewDef(
                allowed_columns=["salary"],
                where_clause="department = 'public'",
            ),
        }
        with patch.object(
            DataReader, "_resolve_latest_stats_context",
        ) as resolve_stats, patch(
            "supertable.processing.load_bounded_stats_diagnostic",
        ) as load_stats:
            dr = DataReader("mysuper", "o", "SHOW STATS mytable")
            with pytest.raises(
                PermissionError,
                match="unavailable under the effective access policy",
            ) as denied:
                dr.execute("reader_role")

        assert "department" not in str(denied.value)
        resolve_stats.assert_not_called()
        load_stats.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_rejects_linked_share_filter_before_artifact_load(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor,
    ):
        from supertable.data_reader import DataReader

        mock_get_storage.return_value = MagicMock()
        mock_restrict.return_value = {}
        secret_path = "private/tenant/full-snapshot-stats.parquet"
        with patch.object(
            DataReader, "_resolve_latest_stats_context",
            return_value=(secret_path, 1, True),
        ), patch(
            "supertable.processing.load_bounded_stats_diagnostic",
        ) as load_stats:
            dr = DataReader("mysuper", "o", "SHOW STATS mytable")
            with pytest.raises(
                PermissionError,
                match="unavailable under the effective access policy",
            ) as denied:
                dr.execute("share_reader")

        assert secret_path not in str(denied.value)
        load_stats.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_policy_resolution_failure_is_generic_and_fails_closed(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor, caplog,
    ):
        from supertable.data_reader import DataReader

        mock_get_storage.return_value = MagicMock()
        mock_restrict.return_value = {}
        secret_path = "s3://private-bucket/share/snapshot-v7.json?token=secret"
        with caplog.at_level(
            logging.ERROR, logger="supertable.config.defaults",
        ):
            with patch.object(
                DataReader, "_resolve_latest_stats_context",
                side_effect=RuntimeError(f"unable to read {secret_path}"),
            ), patch(
                "supertable.processing.load_bounded_stats_diagnostic",
            ) as load_stats:
                dr = DataReader("mysuper", "o", "SHOW STATS mytable")
                with pytest.raises(
                    PermissionError,
                    match="unavailable under the effective access policy",
                ) as denied:
                    dr.execute("share_reader")

        assert secret_path not in caplog.text + str(denied.value)
        assert "token=secret" not in caplog.text
        assert "error_type=RuntimeError" in caplog.text
        load_stats.assert_not_called()

    @patch(_PATCH_EXECUTOR)
    @patch(_PATCH_DATA_ESTIMATOR)
    @patch(_PATCH_SQL_PARSER)
    @patch(_PATCH_RESTRICT_READ)
    @patch(_PATCH_GET_STORAGE)
    def test_show_stats_missing_table_returns_error(
        self, mock_get_storage, mock_restrict, MockParser, MockEstimator,
        MockExecutor, _mock_data_reader_redis,
    ):
        mock_get_storage.return_value = MagicMock()
        # Make the existence pre-flight fail.
        _mock_data_reader_redis.return_value.root_exists.return_value = False
        from supertable.data_reader import DataReader, Status

        dr = DataReader("mysuper", "o", "SHOW STATS mytable")
        df, status, msg = dr.execute("admin")

        assert status == Status.ERROR
        assert df.empty
        # Access check never reached (existence fails first).
        mock_restrict.assert_not_called()


# ====================================================================
# 27. query_sql — LIMIT guard skips non-SELECT commands
# ====================================================================

class TestQuerySqlLimitGuard:

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_limit_appended_for_select(self, mock_ensure, MockDR):
        mock_ensure.return_value = "SELECT 1 LIMIT 10"
        from supertable.data_reader import Status, query_sql
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        query_sql("org", "sup", "SELECT 1", 10, MagicMock(), "admin")
        mock_ensure.assert_called_once_with("SELECT 1", default_limit=10)

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_limit_skipped_for_show_stats(self, mock_ensure, MockDR):
        from supertable.data_reader import Status, query_sql
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        query_sql("org", "sup", "SHOW STATS s.t", 10, MagicMock(), "admin")
        mock_ensure.assert_not_called()
        # Raw SQL forwarded unchanged to the reader.
        assert MockDR.call_args.kwargs["query"] == "SHOW STATS s.t"

    @patch(f"{_MOD}.DataReader")
    @patch(f"{_MOD}._ensure_sql_limit")
    def test_limit_skipped_for_explain(self, mock_ensure, MockDR):
        from supertable.data_reader import Status, query_sql
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.OK, None)
        MockDR.return_value = mock_reader

        query_sql("org", "sup", "EXPLAIN SELECT 1", 10, MagicMock(), "admin")
        mock_ensure.assert_not_called()
        assert MockDR.call_args.kwargs["query"] == "EXPLAIN SELECT 1"
