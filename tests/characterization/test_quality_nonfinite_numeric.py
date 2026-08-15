"""Quality profiling remains defined for IEEE non-finite numeric values."""
from __future__ import annotations

import math
from decimal import Decimal

import pyarrow as pa
import pytest

from supertable.data_writer import DataWriter
from supertable.engine.engine_enum import Engine
from supertable.quality.checker import (
    build_custom_rule_sql,
    build_deep_numeric_sql,
    build_quick_sql,
    parse_quick_result,
)
from supertable.quality.execution import execute_quality_sql
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable


ORG = "quality-nonfinite-org"
SUPER = "quality_nonfinite_lake"
TABLE = "measurements"
ROLE = "superadmin"
FQN = f"{SUPER}.{TABLE}"
SCHEMA = [("id", "BIGINT"), ("value", "DOUBLE")]
EXACT_TABLE = "exact_measurements"
EXACT_FQN = f"{SUPER}.{EXACT_TABLE}"
EXACT_SCHEMA = [
    ("id", "BIGINT"),
    ("large_value", "BIGINT"),
    ("precise_value", "DECIMAL(38,10)"),
]


@pytest.fixture
def nonfinite_table():
    SuperTable(SUPER, ORG)
    data = pa.Table.from_pylist(
        [
            {"id": 1, "value": float("-inf")},
            {"id": 2, "value": -1.0},
            {"id": 3, "value": 0.0},
            {"id": 4, "value": 2.0},
            {"id": 5, "value": float("inf")},
            {"id": 6, "value": float("nan")},
            {"id": 7, "value": None},
        ],
        schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.float64()),
        ]),
    )
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=TABLE,
        data=data,
        overwrite_columns=[],
    )
    # Make the certification boundary observable: these generated statements
    # must still choose DuckDB even when the performance policy prefers Island.
    assert RedisCatalog().set_auto_routing_policy(ORG, [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ])
    return FQN


@pytest.fixture
def exact_numeric_table():
    SuperTable(SUPER, ORG)
    data = pa.Table.from_pylist(
        [
            {
                "id": 101,
                "large_value": 9_007_199_254_740_992,
                "precise_value": Decimal("12345678901234567890.1234567890"),
            },
            {
                "id": 102,
                "large_value": 9_007_199_254_740_993,
                "precise_value": Decimal("12345678901234567890.1234567891"),
            },
            {
                "id": 103,
                "large_value": 9_007_199_254_740_993,
                "precise_value": Decimal("12345678901234567890.1234567891"),
            },
        ],
        schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("large_value", pa.int64(), nullable=False),
            pa.field("precise_value", pa.decimal128(38, 10), nullable=False),
        ]),
    )
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=EXACT_TABLE,
        data=data,
        overwrite_columns=[],
    )
    assert RedisCatalog().set_auto_routing_policy(ORG, [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ])
    return EXACT_FQN


def _execute(sql: str, engine: Engine):
    return execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=engine,
    )


def _assert_direct_duck(result):
    assert result.ok, result.message
    assert result.selected_engine == "duckdb"
    assert result.actual_engine == "duckdb"
    assert result.fallback is False
    assert result.island_supported is False
    assert result.island_certification_reasons


def test_quick_profile_excludes_nan_and_infinities_from_numeric_metrics(
    nonfinite_table,
):
    sql = build_quick_sql(nonfinite_table, SCHEMA)
    result = _execute(sql, Engine.AUTO)
    _assert_direct_duck(result)

    parsed = parse_quick_result(result.frame.iloc[0].to_dict(), SCHEMA)
    value = parsed["columns"]["value"]

    # Present/distinct describe the stored logical values. Numeric statistics
    # use only the finite population [-1, 0, 2], so one bad float cannot make
    # the complete quality run fail or poison its boundaries.
    assert parsed["total"] == 7
    assert value["present"] == 6
    assert value["distinct"] == 6
    assert value["min"] == pytest.approx(-1.0)
    assert value["max"] == pytest.approx(2.0)
    assert value["avg"] == pytest.approx(0.3333)
    assert value["stddev"] == pytest.approx(1.5275)
    assert value["zero_rate"] == pytest.approx(14.29)
    assert value["negative_rate"] == pytest.approx(14.29)


def test_deep_profile_excludes_nan_and_infinities_from_distribution_metrics(
    nonfinite_table,
):
    sql = build_deep_numeric_sql(nonfinite_table, "value", "Float64")
    result = _execute(sql, Engine.AUTO)
    _assert_direct_duck(result)
    row = result.frame.iloc[0]

    assert row["total_rows"] == 7
    assert row["non_nulls"] == 3
    assert row["distinct_vals"] == 3
    assert row["avg_value"] == pytest.approx(1.0 / 3.0, abs=5e-5)
    assert row["var_value"] == pytest.approx(7.0 / 3.0, abs=5e-5)
    assert row["stddev_value"] == pytest.approx(math.sqrt(7.0 / 3.0), abs=5e-5)
    assert row["min_value"] == pytest.approx(-1.0)
    assert row["max_value"] == pytest.approx(2.0)
    assert row["median_value"] == pytest.approx(0.0)
    assert row["p25_value"] == pytest.approx(-0.5)
    assert row["p75_value"] == pytest.approx(1.0)
    assert row["shannon_entropy"] == pytest.approx(math.log2(3.0))
    assert row["uniqueness"] == pytest.approx(1.0)
    assert row["topx_coverage_pct"] == pytest.approx(100.0)
    assert row["zero_or_null_rate"] == pytest.approx(2.0 / 7.0)
    assert row["negative_rate"] == pytest.approx(1.0 / 7.0)

    top_values = row["topx_values"]
    # Equal-frequency ties use the value as a stable secondary ordering key,
    # so history and DuckDB-oracle comparisons are byte reproducible.
    assert [float(item["value"]) for item in top_values] == [-1.0, 0.0, 2.0]
    assert [int(item["freq"]) for item in top_values] == [1, 1, 1]


def test_quick_profile_preserves_adjacent_bigints_and_decimal_extrema(
    exact_numeric_table,
):
    sql = build_quick_sql(exact_numeric_table, EXACT_SCHEMA)
    assert 'TRY_CAST("large_value" AS DOUBLE)' not in sql
    assert 'TRY_CAST("precise_value" AS DOUBLE)' not in sql
    result = _execute(sql, Engine.AUTO)
    _assert_direct_duck(result)

    parsed = parse_quick_result(result.frame.iloc[0].to_dict(), EXACT_SCHEMA)
    large = parsed["columns"]["large_value"]
    precise = parsed["columns"]["precise_value"]
    assert large["distinct"] == 2
    assert int(large["min"]) == 9_007_199_254_740_992
    assert int(large["max"]) == 9_007_199_254_740_993
    assert large["moments_certified"] is False
    assert large["avg"] is None
    assert large["stddev"] is None
    # DECIMAL/HUGEINT scalar columns would be coerced to float64 by fetchdf;
    # the profiler deliberately crosses that boundary as an exact string.
    assert precise["distinct"] == 2
    assert precise["min"] == "12345678901234567890.1234567890"
    assert precise["max"] == "12345678901234567890.1234567891"
    assert precise["moments_certified"] is False
    assert precise["avg"] is None
    assert precise["stddev"] is None


@pytest.mark.parametrize(
    ("column", "column_type", "expected_min", "expected_max"),
    [
        (
            "large_value",
            "BIGINT",
            "9007199254740992",
            "9007199254740993",
        ),
        (
            "precise_value",
            "DECIMAL(38,10)",
            "12345678901234567890.1234567890",
            "12345678901234567890.1234567891",
        ),
    ],
)
def test_deep_profile_groups_and_orders_exact_numeric_values_without_double_collapse(
    exact_numeric_table, column, column_type, expected_min, expected_max,
):
    sql = build_deep_numeric_sql(exact_numeric_table, column, column_type)
    assert f'TRY_CAST("{column}" AS DOUBLE)' not in sql
    result = _execute(sql, Engine.AUTO)
    _assert_direct_duck(result)
    row = result.frame.iloc[0]

    assert int(row["distinct_vals"]) == 2
    assert bool(row["moments_certified"]) is False
    assert row["avg_value"] != row["avg_value"]  # pandas NULL/NaN, not trusted
    assert row["stddev_value"] != row["stddev_value"]
    assert row["p25_value"] is None or row["p25_value"] != row["p25_value"]
    assert row["p75_value"] is None or row["p75_value"] != row["p75_value"]
    assert str(row["min_value"]) == expected_min
    assert str(row["max_value"]) == expected_max
    top_values = list(row["topx_values"])
    assert [int(item["freq"]) for item in top_values] == [2, 1]
    assert [str(item["value"]) for item in top_values] == [expected_max, expected_min]


@pytest.mark.parametrize(
    ("rule_type", "expected_violations"),
    [("column_min", 3), ("column_max", 3)],
)
def test_certified_custom_comparisons_keep_duck_island_nonfinite_parity(
    nonfinite_table,
    rule_type,
    expected_violations,
):
    sql = build_custom_rule_sql({
        "rule_type": rule_type,
        "column_name": "value",
        "threshold": 0.5,
    }, nonfinite_table, SCHEMA)
    assert sql is not None

    duck = _execute(sql, Engine.DUCKDB)
    island = _execute(sql, Engine.ISLANDDB)
    assert duck.ok, duck.message
    assert island.ok, island.message
    assert island.actual_engine == "islanddb"
    assert island.island_supported is True
    assert island.island_certification_reasons == ()
    assert int(duck.frame.iloc[0]["violations"]) == expected_violations
    assert int(island.frame.iloc[0]["violations"]) == expected_violations
