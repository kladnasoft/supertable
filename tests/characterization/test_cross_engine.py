"""Cross-engine characterization for Spark SQL's supported read contract.

This is an **integration** suite: it is skipped unless ``--run-spark`` is
passed. Once explicitly enabled, a missing host or an unavailable/unhealthy
fleet is a test failure, never a skip.

The 32 result scenarios without an active deletion vector execute through the
REAL ``SPARK_SQL`` engine and must match the SAME ``expected/result.json`` that
DuckDB sealed.  Spark cannot yet bind its resolved source paths to the stable
logical ``__file__`` keys stored in a deletion vector.  The fixed 24-scenario
active-DV partition therefore exercises the same public read facade but must
fail closed with the exact composite-identity error before cluster selection or
connection.  The explicit partition is checked against every sealed catalog so
new fixtures cannot be silently reclassified.

Why skip-by-default: the Spark path needs a running Thrift server that can see
the (local-path) parquet fixtures on a shared filesystem.  That is a deployment
concern, not a unit-test concern, so the suite skips only when ``--run-spark``
is absent.  Once the flag is supplied, missing or unhealthy infrastructure is
a hard failure. Configure the fleet via environment:

    SUPERTABLE_SPARK_THRIFT_HOST   (required with --run-spark)
    SUPERTABLE_SPARK_THRIFT_PORT   (default 10000)

The canonical :class:`TableResult` normalization (timestamps -> ISO-UTC, decimals
-> exact strings, nulls -> None) absorbs benign representational differences
between engines; a genuine logical divergence still fails loudly with a diff.
"""

from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock

import pytest

from tests.characterization.comparison import assert_table_result_matches_golden
from tests.characterization.current_reader import load_catalog, read_scenario
from tests.characterization.paths import GOLDEN_ROOT
from tests.characterization.scenarios import ALL_SCENARIOS

_RESULT_SCENARIOS = [s for s in ALL_SCENARIOS if s.expect_error is None]
_ACTIVE_DV_SCENARIO_IDS = frozenset(
    {
        "basic_reinsert_reconciled",
        "basic_reinsert_tombstone_persists",
        "basic_update_delete",
        "delete_null_nonkey_value",
        "delete_overlong_tombstone_tuple",
        "delete_twice_idempotent",
        "delete_unconditional_vs_newer_row",
        "key_composite",
        "key_empty_string",
        "key_full_null",
        "key_null_component",
        "key_string",
        "multi_file_dedup",
        "multi_file_delete_marker_other_file",
        "multi_file_interleaved_keys",
        "multi_file_order_reversed",
        "multi_file_update_chain_3",
        "query_count_star",
        "query_predicate_obsolete_version",
        "ts_future_then_earlier",
        "ts_microsecond_precision",
        "ts_naive_timezone",
        "ts_null_timestamp",
        "ts_out_of_order_physical",
    }
)
_SPARK_ACTIVE_DV_ERROR = (
    "Spark deletion-vector reads require composite source-file + row-id "
    "identity and are not supported safely"
)
_PUBLIC_QUERY_ERROR = "Query execution failed"

_SUPPORTED_RESULT_SCENARIOS = [
    scenario
    for scenario in _RESULT_SCENARIOS
    if scenario.scenario_id not in _ACTIVE_DV_SCENARIO_IDS
]
_ACTIVE_DV_SCENARIOS = [
    scenario
    for scenario in _RESULT_SCENARIOS
    if scenario.scenario_id in _ACTIVE_DV_SCENARIO_IDS
]


def _sealed_active_dv_scenario_ids() -> frozenset[str]:
    active = set()
    for scenario in _RESULT_SCENARIOS:
        catalog = load_catalog(
            GOLDEN_ROOT / scenario.scenario_id / "input" / "catalog.json"
        )
        tables = catalog.get("tables")
        if not isinstance(tables, dict):
            raise AssertionError(
                f"{scenario.scenario_id}: sealed catalog has no table mapping"
            )
        for table_name, table in tables.items():
            if not isinstance(table, dict):
                raise AssertionError(
                    f"{scenario.scenario_id}.{table_name}: invalid sealed table"
                )
            pointer = table.get("tombstone_file")
            rows = table.get("tombstone_rows", 0)
            if pointer is None:
                if rows != 0:
                    raise AssertionError(
                        f"{scenario.scenario_id}.{table_name}: tombstone rows "
                        "without a deletion-vector pointer"
                    )
                continue
            if not isinstance(pointer, str) or not pointer:
                raise AssertionError(
                    f"{scenario.scenario_id}.{table_name}: invalid deletion-vector pointer"
                )
            if type(rows) is not int or rows <= 0:
                raise AssertionError(
                    f"{scenario.scenario_id}.{table_name}: active deletion vector "
                    "must have a positive row count"
                )
            active.add(scenario.scenario_id)
    return frozenset(active)


@pytest.fixture(scope="session")
def spark_capability_partition(sealed_manifest_ok) -> None:
    """Prove the explicit capability partition matches the sealed inputs."""
    sealed_ids = _sealed_active_dv_scenario_ids()
    assert sealed_ids == _ACTIVE_DV_SCENARIO_IDS, (
        "Spark active-deletion-vector capability partition drifted: "
        f"declared_only={sorted(_ACTIVE_DV_SCENARIO_IDS - sealed_ids)}, "
        f"sealed_only={sorted(sealed_ids - _ACTIVE_DV_SCENARIO_IDS)}"
    )
    assert len(_SUPPORTED_RESULT_SCENARIOS) == 32
    assert len(_ACTIVE_DV_SCENARIOS) == 24


@pytest.fixture(scope="session")
def spark_cluster(request, spark_capability_partition) -> dict:
    """Cluster config for the cross-engine read, sourced from environment.

    The suite skips only when ``--run-spark`` is absent. Once enabled, a
    missing or unhealthy configured fleet fails the session. The zero byte
    window accepts any job size (reads are forced to Spark, so routing size is
    irrelevant).
    """
    if not request.config.getoption("--run-spark"):
        pytest.skip("cross-engine Spark tests require --run-spark")
    host = os.environ.get("SUPERTABLE_SPARK_THRIFT_HOST")
    if not host:
        pytest.fail(
            "--run-spark requires SUPERTABLE_SPARK_THRIFT_HOST",
            pytrace=False,
        )
    cluster = {
        "cluster_id": "characterization-spark",
        "name": "characterization",
        "thrift_host": host,
        "thrift_port": int(os.environ.get("SUPERTABLE_SPARK_THRIFT_PORT", "10000")),
        "status": "active",
        "min_bytes": 0,
        "max_bytes": 0,
        "s3_enabled": False,
    }

    # Enabling the integration gate is an assertion that the configured fleet
    # is healthy.  Probe it once up front; connection/query failures must fail
    # the run instead of being converted to per-scenario skips.
    from supertable.engine.spark_thrift import (
        SparkThriftExecutor,
        _execute_with_stmt_timeout,
    )
    from supertable.utils.diagnostic_redaction import safe_exception_type

    executor = SparkThriftExecutor(organization="characterization")
    connection = None
    cursor = None
    smoke_error_type = None
    try:
        connection = executor._get_connection_with_deadline(
            cluster,
            deadline_monotonic=time.monotonic() + 30.0,
            cancel_event=None,
        )
        cursor = connection.cursor()
        timed_out = threading.Event()
        _execute_with_stmt_timeout(
            cursor,
            "SELECT 1",
            connection,
            30,
            timed_out,
            "[characterization-smoke] ",
        )
        assert cursor.fetchmany(2) == [(1,)]
        assert not timed_out.is_set()
    except Exception as exc:
        # Do not fail while handling ``exc``: pytest would render the implicit
        # exception context and leak backend-specific prose or paths. Retain
        # only the safe type label, clean up, then fail outside the handler.
        smoke_error_type = safe_exception_type(exc)
    finally:
        if cursor is not None:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                try:
                    close_cursor()
                except Exception:
                    pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    if smoke_error_type is not None:
        pytest.fail(
            f"Spark characterization smoke failed; error_type={smoke_error_type}",
            pytrace=False,
        )
    return cluster


@pytest.mark.spark
@pytest.mark.parametrize(
    "scenario", _SUPPORTED_RESULT_SCENARIOS, ids=lambda s: s.scenario_id
)
def test_spark_matches_golden(scenario, spark_cluster, sealed_manifest_ok):
    actual = read_scenario(
        scenario, GOLDEN_ROOT, engine="spark", spark_cluster=spark_cluster
    )
    assert_table_result_matches_golden(
        actual,
        GOLDEN_ROOT / scenario.scenario_id / "expected",
        ordered=scenario.ordered,
    )


@pytest.mark.spark
@pytest.mark.parametrize("scenario", _ACTIVE_DV_SCENARIOS, ids=lambda s: s.scenario_id)
def test_spark_active_deletion_vector_fails_closed(
    scenario,
    spark_cluster,
    sealed_manifest_ok,
    monkeypatch,
):
    from supertable.engine.spark_thrift import SparkThriftExecutor

    original_execute = SparkThriftExecutor.execute
    inner_errors = []

    def capture_execute(self, *args, **kwargs):
        try:
            return original_execute(self, *args, **kwargs)
        except BaseException as exc:
            inner_errors.append(exc)
            raise

    select_cluster = MagicMock(
        side_effect=AssertionError(
            "active deletion vector reached Spark cluster selection"
        )
    )
    get_connection = MagicMock(
        side_effect=AssertionError(
            "active deletion vector reached Spark connection setup"
        )
    )
    monkeypatch.setattr(SparkThriftExecutor, "execute", capture_execute)
    monkeypatch.setattr(SparkThriftExecutor, "_select_cluster", select_cluster)
    monkeypatch.setattr(SparkThriftExecutor, "_get_connection", get_connection)

    with pytest.raises(RuntimeError) as public_error:
        read_scenario(
            scenario,
            GOLDEN_ROOT,
            engine="spark",
            spark_cluster=spark_cluster,
        )

    assert str(public_error.value) == _PUBLIC_QUERY_ERROR
    assert len(inner_errors) == 1
    assert type(inner_errors[0]) is RuntimeError
    assert str(inner_errors[0]) == _SPARK_ACTIVE_DV_ERROR
    select_cluster.assert_not_called()
    get_connection.assert_not_called()
