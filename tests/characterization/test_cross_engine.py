"""Cross-engine characterization — Spark SQL must reproduce the sealed goldens.

This is an **integration** suite: it is skipped unless ``--run-spark`` is passed
AND a Spark Thrift fleet is reachable.  When enabled it reads each sealed
scenario through the REAL ``SPARK_SQL`` engine and compares the canonical result
against the SAME ``expected/result.json`` that DuckDB sealed — so the two engines
are held to one logical contract.

Why skip-by-default: the Spark path needs a running Thrift server that can see
the (local-path) parquet fixtures on a shared filesystem.  That is a deployment
concern, not a unit-test concern, so a developer machine without Spark stays
green.  Configure the fleet via environment:

    SUPERTABLE_SPARK_THRIFT_HOST   (required to run; else the suite skips)
    SUPERTABLE_SPARK_THRIFT_PORT   (default 10000)

The canonical :class:`TableResult` normalization (timestamps -> ISO-UTC, decimals
-> exact strings, nulls -> None) absorbs benign representational differences
between engines; a genuine logical divergence still fails loudly with a diff.
"""

from __future__ import annotations

import os

import pytest

from tests.characterization.comparison import assert_table_result_matches_golden
from tests.characterization.current_reader import read_scenario
from tests.characterization.paths import GOLDEN_ROOT
from tests.characterization.scenarios import ALL_SCENARIOS

_RESULT_SCENARIOS = [s for s in ALL_SCENARIOS if s.expect_error is None]

# Substrings that mean "Spark fleet not available here" -> skip rather than fail,
# so enabling --run-spark without a reachable Thrift server does not red the run.
_UNAVAILABLE_MARKERS = (
    "No active Spark cluster",
    "PyHive is required",
    "Could not connect",
    "Connection refused",
    "TTransportException",
    "thrift",
)


@pytest.fixture(scope="session")
def spark_cluster(request) -> dict:
    """Cluster config for the cross-engine read, sourced from environment.

    Skips the whole suite unless ``--run-spark`` is given and a Thrift host is
    configured.  ``min_bytes/max_bytes = 0`` make the cluster accept any job
    size (reads are forced to Spark, so the size window is irrelevant anyway).
    """
    if not request.config.getoption("--run-spark"):
        pytest.skip("cross-engine Spark tests require --run-spark")
    host = os.environ.get("SUPERTABLE_SPARK_THRIFT_HOST")
    if not host:
        pytest.skip("set SUPERTABLE_SPARK_THRIFT_HOST to run cross-engine Spark tests")
    return {
        "cluster_id": "characterization-spark",
        "name": "characterization",
        "thrift_host": host,
        "thrift_port": int(os.environ.get("SUPERTABLE_SPARK_THRIFT_PORT", "10000")),
        "status": "active",
        "min_bytes": 0,
        "max_bytes": 0,
        "s3_enabled": False,
    }


@pytest.mark.spark
@pytest.mark.parametrize("scenario", _RESULT_SCENARIOS, ids=lambda s: s.scenario_id)
def test_spark_matches_golden(scenario, spark_cluster, sealed_manifest_ok):
    try:
        actual = read_scenario(
            scenario, GOLDEN_ROOT, engine="spark", spark_cluster=spark_cluster
        )
    except Exception as exc:  # noqa: BLE001 - classify fleet-unavailable vs real failure
        msg = str(exc)
        if any(m.lower() in msg.lower() for m in _UNAVAILABLE_MARKERS):
            pytest.skip(f"Spark fleet unavailable ({type(exc).__name__}: {msg[:120]})")
        raise
    assert_table_result_matches_golden(
        actual,
        GOLDEN_ROOT / scenario.scenario_id / "expected",
        ordered=scenario.ordered,
    )
