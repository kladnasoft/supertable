"""Real DataWriter quality-SQL parity and AUTO certification boundary."""
from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

from supertable.data_writer import DataWriter
from supertable.engine.engine_enum import Engine
from supertable.quality.checker import (
    build_custom_rule_sql,
    build_deep_numeric_sql,
    build_deep_string_sql,
    build_quick_sql,
)
from supertable.quality.execution import execute_quality_sql
from supertable.quality.serialization import normalize_json_value
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable


ORG = "quality-engine-org"
SUPER = "quality_engine_lake"
TABLE = "facts"
ROLE = "superadmin"
FQN = f"{SUPER}.{TABLE}"
SCHEMA = [
    ("id", "BIGINT"),
    ("amount", "BIGINT"),
    ("label", "VARCHAR"),
]


@pytest.fixture
def quality_table():
    SuperTable(SUPER, ORG)
    rows = pa.Table.from_pylist(
        [
            {"id": 1, "amount": -2, "label": "Alpha"},
            {"id": 2, "amount": 0, "label": "beta"},
            {"id": 3, "amount": 7, "label": "Alpha"},
            {"id": 4, "amount": None, "label": "unknown"},
            {"id": 5, "amount": 11, "label": None},
        ],
        schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.int64()),
            pa.field("label", pa.string()),
        ]),
    )
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=TABLE,
        data=rows,
        overwrite_columns=[],
    )
    # Force every byte range toward IslandDB *only when* all correctness and
    # resource gates pass. Unsupported statements must still route DuckDB.
    assert RedisCatalog().set_auto_routing_policy(ORG, [
        {"min_bytes": 0, "max_bytes": None, "engine": "islanddb"},
    ])
    return FQN


SUPPORTED_STRUCTURED_SQL = (
    f"SELECT COUNT(*) AS violations FROM {FQN} WHERE \"amount\" < 0",
    f"SELECT COUNT(*) AS violations FROM {FQN} WHERE \"amount\" > 100",
)


@pytest.mark.parametrize("sql", SUPPORTED_STRUCTURED_SQL)
def test_supported_structured_quality_sql_has_exact_duck_island_parity(
    quality_table, sql,
):
    duck = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.DUCKDB,
    )
    island = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.ISLANDDB,
    )

    assert duck.ok, duck.message
    assert island.ok, island.message
    assert duck.actual_engine == "duckdb"
    assert island.actual_engine == "islanddb"
    assert island.island_supported is True
    assert island.island_certification_reasons == ()
    assert_frame_equal(
        duck.frame.reset_index(drop=True),
        island.frame.reset_index(drop=True),
        check_dtype=False,
    )


def _unsupported_quality_sql() -> list[tuple[str, str, bool]]:
    custom_null_rate = build_custom_rule_sql({
        "rule_type": "null_rate_max",
        "column_name": "amount",
        "threshold": 20,
    }, FQN)
    custom_distinct = build_custom_rule_sql({
        "rule_type": "distinct_in",
        "column_name": "label",
        "expected_values": ["Alpha", "beta"],
    }, FQN)
    assert custom_null_rate is not None
    assert custom_distinct is not None
    return [
        ("quick", build_quick_sql(FQN, SCHEMA), False),
        (
            "deep_numeric",
            build_deep_numeric_sql(FQN, "amount", "BIGINT"),
            True,
        ),
        ("deep_string", build_deep_string_sql(FQN, "label"), True),
        ("custom_null_rate", custom_null_rate, False),
        ("custom_distinct", custom_distinct, False),
    ]


@pytest.mark.parametrize(
    "name,sql,allow_bounded_collection_aggregates",
    _unsupported_quality_sql(),
)
def test_auto_routes_every_uncertified_quality_statement_to_duckdb(
    quality_table, name, sql, allow_bounded_collection_aggregates,
):
    oracle = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.DUCKDB,
        allow_bounded_collection_aggregates=(
            allow_bounded_collection_aggregates
        ),
    )
    automatic = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.AUTO,
        allow_bounded_collection_aggregates=(
            allow_bounded_collection_aggregates
        ),
    )

    assert oracle.ok, f"{name}: {oracle.message}"
    assert automatic.ok, f"{name}: {automatic.message}"
    assert automatic.selected_engine == "duckdb"
    assert automatic.actual_engine == "duckdb"
    assert automatic.fallback is False
    assert automatic.island_supported is False
    assert automatic.island_certification_scope == (
        "complete_query_static_semantics"
    )
    assert automatic.island_certification_reasons, name
    if name == "custom_distinct":
        assert list(automatic.frame.columns) == ["unexpected_count"]
        assert len(automatic.frame) == 1
        assert int(automatic.frame.iloc[0]["unexpected_count"]) == 1
    assert_frame_equal(
        oracle.frame.reset_index(drop=True),
        automatic.frame.reset_index(drop=True),
        check_dtype=False,
    )


def test_auto_policy_uses_island_only_after_whole_query_certification(
    quality_table,
):
    sql = (
        f"SELECT COUNT(*) AS violations FROM {FQN} "
        'WHERE "amount" < 0'
    )

    result = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.AUTO,
    )

    assert result.ok, result.message
    assert result.selected_engine == "islanddb"
    assert result.actual_engine == "islanddb"
    assert result.island_supported is True
    assert result.island_certification_scope == (
        "complete_query_static_semantics"
    )


def test_deep_string_output_bounds_huge_values_with_exact_identity(quality_table):
    table = "huge_labels"
    fqn = f"{SUPER}.{table}"
    first = ("\u00e9" * 500_000) + "a"
    second = ("\u00e9" * 500_000) + "b"
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=pa.Table.from_pylist(
            [
                {"id": 1, "label": first},
                {"id": 2, "label": second},
                {"id": 3, "label": second},
            ],
            schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("label", pa.string(), nullable=False),
            ]),
        ),
        overwrite_columns=[],
    )

    result = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=build_deep_string_sql(fqn, "label"),
        role_name=ROLE,
        engine=Engine.AUTO,
        allow_bounded_collection_aggregates=True,
    )
    assert result.ok, result.message
    assert result.actual_engine == "duckdb"
    row = result.frame.iloc[0].to_dict()
    top_values = list(row["topx_values"])
    assert len(top_values) == 2
    expected = {
        hashlib.sha256(value.encode("utf-8")).hexdigest(): (value, frequency)
        for value, frequency in ((first, 1), (second, 2))
    }
    for item in top_values:
        original, frequency = expected[item["value_sha256"]]
        assert item["value"] == original[:256]
        assert item["value_truncated"] is True
        assert int(item["value_char_length"]) == len(original)
        assert int(item["value_byte_length"]) == len(original.encode("utf-8"))
        assert int(item["freq"]) == frequency

    buckets = list(row["buckets"])
    assert buckets
    for bucket in buckets:
        for boundary in ("min", "max"):
            assert len(bucket[f"bucket_{boundary}"]) <= 256
            assert len(bucket[f"bucket_{boundary}_sha256"]) == 64
            assert bucket[f"bucket_{boundary}_truncated"] is True

    payload = json.dumps(normalize_json_value(row), allow_nan=False).encode("utf-8")
    assert len(payload) < 100_000
    assert first.encode("utf-8") not in payload
    assert second.encode("utf-8") not in payload


def test_auto_uses_island_when_certified_sql_resource_proof_is_complete(
    quality_table,
):
    sql = build_custom_rule_sql({
        "rule_type": "row_count_min",
        "threshold": 1,
    }, FQN)
    assert sql is not None

    oracle = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.DUCKDB,
    )
    automatic = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.AUTO,
    )

    assert oracle.ok, oracle.message
    assert automatic.ok, automatic.message
    assert automatic.island_supported is True
    assert automatic.selected_engine == "islanddb"
    assert automatic.actual_engine == "islanddb"
    assert automatic.fallback is False
    assert_frame_equal(oracle.frame, automatic.frame, check_dtype=False)


@pytest.mark.parametrize(
    "name,sql,allow_bounded_collection_aggregates",
    _unsupported_quality_sql(),
)
def test_explicit_island_rejection_remains_visible_and_is_not_called_parity(
    quality_table, name, sql, allow_bounded_collection_aggregates,
):
    result = execute_quality_sql(
        organization=ORG,
        super_name=SUPER,
        sql=sql,
        role_name=ROLE,
        engine=Engine.ISLANDDB,
        allow_bounded_collection_aggregates=(
            allow_bounded_collection_aggregates
        ),
    )

    assert result.ok is False, name
    assert result.actual_engine is None
    assert result.island_supported is False
    assert result.island_certification_reasons, name
    assert result.frame.empty
