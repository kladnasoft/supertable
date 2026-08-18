from __future__ import annotations

import pandas as pd
import pytest

from supertable.data_reader import Status
from supertable.engine.engine_enum import Engine
from supertable.engine.plan_stats import PlanStats
from supertable.quality.execution import (
    QualitySQLExecutionError,
    execute_quality_sql,
)


class _Reader:
    response = (pd.DataFrame(), Status.ERROR, "unset")
    stats = ()
    error = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.plan_stats = PlanStats()
        self.plan_stats.stats.extend(self.stats)

    def execute(self, **kwargs):
        self.execute_kwargs = kwargs
        if self.error is not None:
            raise self.error
        return self.response


def _reader_factory(*, response, stats=(), error=None):
    class Reader(_Reader):
        pass

    Reader.response = response
    Reader.stats = stats
    Reader.error = error
    return Reader


def test_auto_duckdb_route_preserves_whole_query_certification_reason():
    frame = pd.DataFrame({"__total": [3]})
    reader = _reader_factory(
        response=(frame, Status.OK, None),
        stats=(
            {
                "ENGINE_CAPABILITY": {
                    "engine": "islanddb",
                    "supported": False,
                    "scope": "complete_query_static_semantics",
                    "reasons": ["unsupported SQL nodes: Distinct, TryCast"],
                },
            },
            {
                "ENGINE_REQUEST": {
                    "requested_engine": "auto",
                    "selected_engine": "duckdb",
                    "forced": False,
                },
            },
            {
                "AUTO_ROUTING_OUTCOME": {
                    "selected_engine": "duckdb",
                    "actual_engine": "duckdb",
                    "fallback": False,
                },
            },
            {"ENGINE": "duckdb"},
        ),
    )

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT COUNT(DISTINCT id) FROM sup.t",
        reader_factory=reader,
    )

    assert result.ok is True
    assert result.requested_engine == "auto"
    assert result.selected_engine == "duckdb"
    assert result.actual_engine == "duckdb"
    assert result.fallback is False
    assert result.island_supported is False
    assert result.island_certification_scope == "complete_query_static_semantics"
    assert result.island_certification_reasons == (
        "unsupported SQL nodes: Distinct, TryCast",
    )
    assert result.require_success() is frame


def test_native_island_success_is_not_silently_duckdb_parity():
    frame = pd.DataFrame({"row_count": [3]})
    reader = _reader_factory(
        response=(frame, Status.OK, None),
        stats=(
            {
                "ENGINE_CAPABILITY": {
                    "engine": "islanddb",
                    "supported": True,
                    "scope": "complete_query_static_semantics",
                    "reasons": [],
                },
            },
            {
                "ENGINE_REQUEST": {
                    "requested_engine": "islanddb",
                    "selected_engine": "islanddb",
                    "forced": True,
                },
            },
            {"ENGINE": "islanddb"},
        ),
    )

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT COUNT(*) AS row_count FROM sup.t",
        engine=Engine.ISLANDDB,
        reader_factory=reader,
    )

    assert result.ok is True
    assert result.actual_engine == "islanddb"
    assert result.island_supported is True
    assert result.fallback is False


def test_failed_query_cannot_be_evaluated_as_an_empty_success():
    reader = _reader_factory(
        response=(pd.DataFrame(), Status.ERROR, "binder error"),
        stats=(
            {
                "ENGINE_REQUEST": {
                    "requested_engine": "islanddb",
                    "selected_engine": "islanddb",
                    "forced": True,
                },
            },
            {"ENGINE_ATTEMPT": {"engine": "islanddb", "stage": "primary"}},
        ),
    )

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT bad() FROM sup.t",
        engine="islanddb",
        reader_factory=reader,
    )

    assert result.ok is False
    assert result.actual_engine is None
    assert result.island_supported is False
    with pytest.raises(QualitySQLExecutionError, match="binder error"):
        result.require_success()


def test_rbac_or_reader_exception_is_a_structured_failure():
    reader = _reader_factory(
        response=(pd.DataFrame(), Status.OK, None),
        error=PermissionError("quality role denied"),
    )

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT COUNT(*) FROM sup.t",
        reader_factory=reader,
    )

    assert result.ok is False
    assert result.status == "error"
    assert result.actual_engine is None
    assert result.message == "quality role denied"


def test_reader_is_tagged_as_quality_source_and_auto_is_default():
    seen = {}

    class Reader(_Reader):
        response = (pd.DataFrame({"n": [1]}), Status.OK, None)

        def __init__(self, **kwargs):
            seen["init"] = kwargs
            super().__init__(**kwargs)

        def execute(self, **kwargs):
            seen["execute"] = kwargs
            return super().execute(**kwargs)

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT 1 AS n FROM sup.t",
        reader_factory=Reader,
    )

    assert result.ok
    assert seen["init"]["source"] == "quality"
    assert seen["execute"]["engine"] is Engine.AUTO


def test_bounded_collection_capability_is_forwarded_as_a_boolean():
    seen = {}

    class Reader(_Reader):
        response = (pd.DataFrame({"n": [1]}), Status.OK, None)

        def __init__(self, **kwargs):
            seen.update(kwargs)
            super().__init__(**kwargs)

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT 1 AS n FROM sup.t",
        reader_factory=Reader,
        allow_bounded_collection_aggregates=True,
    )

    assert result.ok
    assert seen["_allow_bounded_collection_aggregates"] is True


def test_reader_construction_failure_is_structured_without_plan_stats():
    def broken_reader(**_kwargs):
        raise RuntimeError("reader construction failed")

    result = execute_quality_sql(
        organization="org",
        super_name="sup",
        sql="SELECT 1 AS n FROM sup.t",
        reader_factory=broken_reader,
    )

    assert result.ok is False
    assert result.status == "error"
    assert result.message == "reader construction failed"
    assert result.actual_engine is None
    assert result.plan_stats == ()
