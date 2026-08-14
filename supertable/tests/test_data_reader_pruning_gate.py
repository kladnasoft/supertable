"""Master-switch regression for read-path SQL pruning analysis."""
from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock

import pandas as pd

from supertable import data_reader as reader_mod
from supertable.config.settings import settings
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.data_classes import PredInterval


def test_auto_and_spark_drop_timezone_dependent_timestamp_constraints():
    key = ("s", "t")
    numeric = PredInterval("numeric", 7, True, 7, True)
    numeric_cast = PredInterval("numeric_cast", 8, True, 8, True)
    date = PredInterval("date", object(), True, object(), True)
    timestamp = PredInterval("timestamp", None, True, object(), False)
    constraints = {key: [{
        "id": numeric,
        "cast_id": numeric_cast,
        "event_date": date,
        "event_time": timestamp,
    }]}

    # Explicit DuckDB compares the same naïve footer/literal representation.
    assert reader_mod._engine_safe_predicate_constraints(
        constraints, reader_mod.engine.DUCKDB,
    ) is constraints

    # AUTO can route to Spark after estimation; both modes keep the exact
    # integer predicate but discard the timezone-dependent timestamp one.
    expected = {key: [{"id": numeric}]}
    assert reader_mod._engine_safe_predicate_constraints(
        constraints, reader_mod.engine.AUTO,
    ) == expected
    assert reader_mod._engine_safe_predicate_constraints(
        constraints, reader_mod.engine.SPARK_SQL,
    ) == expected


def test_auto_and_spark_join_pruning_uses_only_common_exact_lane():
    assert reader_mod._engine_safe_join_pruning_lanes(
        reader_mod.engine.AUTO,
    ) == {"numeric"}
    assert reader_mod._engine_safe_join_pruning_lanes(
        reader_mod.engine.SPARK_SQL,
    ) == {"numeric"}
    assert reader_mod._engine_safe_join_pruning_lanes(
        reader_mod.engine.DUCKDB,
    ) == {"numeric", "date", "timestamp", "timestamptz"}


def test_spark_temporal_only_occurrence_disables_shared_table_predicate():
    key = ("s", "t")
    constraints = {
        key: [
            {"event_time": PredInterval("timestamp", object(), True, None, True)},
            {"id": PredInterval("numeric", 7, True, 7, True)},
        ]
    }

    # The first alias becomes unconstrained after the unsafe lane is removed.
    # The executor shares one file list across aliases, so applying the second
    # alias's id=7 constraint alone would be unsound.
    assert reader_mod._engine_safe_predicate_constraints(
        constraints, reader_mod.engine.SPARK_SQL,
    ) == {}


def test_spark_parser_rejects_wrapping_integer_cast_constraint():
    constraints = reader_mod.SQLParser(
        "s",
        "SELECT * FROM s.t WHERE k = CAST(2147483648 AS INT)",
        "spark",
    ).get_predicate_constraints()

    # With Spark ANSI mode disabled this cast wraps to -2147483648.  Treating
    # the source literal as +2147483648 would prune the contributing file.
    assert constraints[("s", "t")] == [{}]


def test_disabled_pruning_skips_predicate_and_join_analysis(monkeypatch):
    """The disabled gate avoids both scope walks and still executes the SELECT."""
    monkeypatch.setattr(
        reader_mod,
        "settings",
        dataclasses.replace(settings, SUPERTABLE_READ_PRUNING_ENABLED=False),
    )

    storage = MagicMock(name="storage")
    monkeypatch.setattr(reader_mod, "get_storage", MagicMock(return_value=storage))

    parser = MagicMock(name="parser")
    parser.original_query = "SELECT 1"
    parser.get_table_tuples.return_value = []
    parser.get_physical_tables.return_value = []
    parser.get_predicate_constraints.side_effect = AssertionError(
        "predicate analysis must be skipped",
    )
    parser.get_join_edges.side_effect = AssertionError(
        "join analysis must be skipped",
    )
    monkeypatch.setattr(
        reader_mod, "SQLParser", MagicMock(return_value=parser),
    )

    timer = MagicMock(timings=[])
    monkeypatch.setattr(reader_mod, "Timer", MagicMock(return_value=timer))
    plan_stats = MagicMock(name="plan_stats")
    monkeypatch.setattr(
        reader_mod, "PlanStats", MagicMock(return_value=plan_stats),
    )
    monkeypatch.setattr(reader_mod, "restrict_read_access", MagicMock(return_value={}))
    monkeypatch.setattr(
        reader_mod,
        "QueryPlanManager",
        MagicMock(return_value=MagicMock(query_id="q", query_hash="h")),
    )
    monkeypatch.setattr(reader_mod, "RedisCatalog", MagicMock())
    monkeypatch.setattr(reader_mod, "extend_execution_plan", MagicMock())

    reflection = Reflection(
        storage_type="LocalStorage",
        reflection_bytes=100,
        total_reflections=1,
        supers=[
            SuperSnapshot(
                super_name="s",
                simple_name="t",
                simple_version=1,
                files=["t/f.parquet"],
                columns={"id"},
            )
        ],
    )
    estimator = MagicMock(name="estimator")
    estimator.estimate.return_value = reflection
    estimator_cls = MagicMock(return_value=estimator)
    monkeypatch.setattr(reader_mod, "DataEstimator", estimator_cls)

    expected = pd.DataFrame({"id": [1]})
    executor = MagicMock(name="executor")
    executor.execute.return_value = (expected, "duckdb")
    executor_cls = MagicMock(return_value=executor)
    monkeypatch.setattr(reader_mod, "Executor", executor_cls)

    result, status, message = reader_mod.DataReader(
        "s", "o", "SELECT 1",
    ).execute("admin", engine=reader_mod.engine.DUCKDB)

    pd.testing.assert_frame_equal(result, expected)
    assert status is reader_mod.Status.OK
    assert message is None
    parser.get_predicate_constraints.assert_not_called()
    parser.get_join_edges.assert_not_called()
    estimator_cls.assert_called_once_with(
        organization="o",
        storage=storage,
        tables=[],
        predicate_constraints={},
        join_edges=[],
        join_pruning_lanes={"numeric", "date", "timestamp", "timestamptz"},
        plan_stats=plan_stats,
    )
    estimator.estimate.assert_called_once_with()
    executor.execute.assert_called_once()
