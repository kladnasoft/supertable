from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import fakeredis
import numpy as np
import pandas as pd
import pytest

from supertable import redis_keys as RK
from supertable.quality import scheduler
from supertable.quality.config import DQConfig
from supertable.quality.history import build_history_row
from supertable.quality.serialization import (
    DEFAULT_MAX_SCALAR_BYTES,
    JSONNormalizationError,
    normalize_json_value,
)


ORG = "dq-json-org"
SUPER = "dq-json-lake"
TABLE = "events"


def _seed_live_catalog(redis_client):
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        json.dumps({"version": 0, "ts": 1}),
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        json.dumps({
            "version": 0,
            "ts": 1,
            "path": f"{ORG}/{SUPER}/{TABLE}/snapshot.json",
        }),
    )


@dataclass
class FakeExecution:
    frame: pd.DataFrame
    ok: bool = True
    status: str = "ok"
    message: str | None = None

    def require_success(self) -> pd.DataFrame:
        return self.frame


class OneColumnMetaReader:
    column_name = "value"
    column_type = "VARCHAR"

    def __init__(self, **_kwargs):
        pass

    def get_table_schema(self, _table, _role):
        return [{self.column_name: self.column_type}]


def _string_preview(value: str, prefix: str) -> dict:
    encoded = value.encode("utf-8")
    return {
        prefix: value,
        f"{prefix}_sha256": hashlib.sha256(encoded).hexdigest(),
        f"{prefix}_char_length": len(value),
        f"{prefix}_byte_length": len(encoded),
        f"{prefix}_truncated": False,
    }


def test_json_normalizer_is_lossless_deterministic_and_strict():
    large = np.uint64(2**64 - 1)
    timestamp = pd.Timestamp("2026-08-15T12:34:56.123456789+02:00")
    value = {
        "structured": np.array(
            [{
                "large": large,
                "decimal": Decimal("1.2300"),
                "missing": np.float64("nan"),
                "infinite": np.float64("inf"),
            }],
            dtype=object,
        ),
        "tuple": (np.int64(7), np.bool_(True)),
        "date": date(2026, 8, 15),
        "datetime": datetime(
            2026, 8, 15, 12, 34, 56,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        "timestamp": timestamp,
    }

    normalized = normalize_json_value(value)

    assert normalized["structured"] == [{
        "large": 2**64 - 1,
        "decimal": "1.2300",
        "missing": None,
        "infinite": None,
    }]
    assert normalized["tuple"] == [7, True]
    assert normalized["date"] == "2026-08-15"
    assert normalized["datetime"] == "2026-08-15T10:34:56+00:00"
    assert normalized["timestamp"] == "2026-08-15T10:34:56.123456789+00:00"
    # Strict JSON must never rely on the non-standard NaN/Infinity extension.
    json.dumps(normalized, allow_nan=False)
    assert normalize_json_value(value) == normalized


def test_json_normalizer_rejects_cycles_and_enforces_bounds():
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(JSONNormalizationError, match="cycle"):
        normalize_json_value(cyclic)

    with pytest.raises(JSONNormalizationError, match="maximum depth"):
        normalize_json_value([[[1]]], max_depth=2)

    with pytest.raises(JSONNormalizationError, match="maximum node count"):
        normalize_json_value([1, 2, 3], max_nodes=3)


def test_json_normalizer_enforces_scalar_and_total_utf8_bytes_before_dump():
    # UTF-8 bytes, not Python character count: each value below is two bytes.
    assert normalize_json_value(
        "éé", max_scalar_bytes=4, max_total_bytes=6,
    ) == "éé"
    with pytest.raises(JSONNormalizationError, match="maximum scalar UTF-8"):
        normalize_json_value(
            "ééé", max_scalar_bytes=4, max_total_bytes=100,
        )

    huge = "x" * (DEFAULT_MAX_SCALAR_BYTES + 1)
    with pytest.raises(JSONNormalizationError, match="maximum scalar UTF-8"):
        normalize_json_value(huge)

    # Every scalar fits independently; container punctuation and the combined
    # encoded values exceed the complete compact-JSON budget.
    aggregate = {"first": "x" * 20, "second": "y" * 20}
    encoded = json.dumps(
        aggregate, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")
    assert normalize_json_value(
        aggregate,
        max_scalar_bytes=20,
        max_total_bytes=len(encoded),
    ) == aggregate
    with pytest.raises(JSONNormalizationError, match="maximum total UTF-8"):
        normalize_json_value(
            aggregate,
            max_scalar_bytes=20,
            max_total_bytes=len(encoded) - 1,
        )


def test_json_normalizer_rejects_binary_values_without_stringifying_them():
    for value in (
        b"small",
        bytearray(b"x" * (DEFAULT_MAX_SCALAR_BYTES + 1)),
        memoryview(b"large-binary-value"),
    ):
        with pytest.raises(JSONNormalizationError, match="binary values"):
            normalize_json_value(value)


def test_redis_and_history_reject_oversized_scalars_before_publication():
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    _seed_live_catalog(fake)
    dqc = DQConfig(fake, ORG, SUPER)
    oversized = "x" * (DEFAULT_MAX_SCALAR_BYTES + 1)

    assert not dqc.set_latest(TABLE, {"value": oversized})
    assert dqc.get_latest(TABLE) is None

    with pytest.raises(JSONNormalizationError, match="maximum scalar UTF-8"):
        build_history_row(TABLE, "deep", {
            "anomalies": [{"value": oversized}],
            "parsed": {"total": 1, "columns": {}},
        })


@pytest.mark.parametrize(
    ("column_type", "topx_values", "buckets", "expected_topx", "expected_buckets"),
    [
        (
            "DECIMAL(38, 4)",
            np.array([
                {"value": np.int64(2**53 + 1), "freq": np.int64(3)},
                {"value": Decimal("1.2300"), "freq": np.int64(1)},
            ], dtype=object),
            np.array([
                {
                    "bucket_id": np.int64(bucket_id),
                    "bucket_min": (
                        Decimal("1.2300")
                        if bucket_id == 1 else np.int64(2**53 + 1)
                    ),
                    "bucket_max": (
                        Decimal("1.2300")
                        if bucket_id == 1 else np.int64(2**53 + 1)
                    ),
                    "freq": np.int64(1),
                }
                for bucket_id in range(1, 5)
            ], dtype=object),
            [
                {"value": 2**53 + 1, "freq": 3},
                {"value": "1.2300", "freq": 1},
            ],
            [{
                "bucket_id": bucket_id,
                "bucket_min": "1.2300" if bucket_id == 1 else 2**53 + 1,
                "bucket_max": "1.2300" if bucket_id == 1 else 2**53 + 1,
                "freq": 1,
            } for bucket_id in range(1, 5)],
        ),
        (
            "VARCHAR",
            np.array([
                {
                    **_string_preview("zeta", "value"),
                    "freq": np.int64(3),
                },
                {
                    **_string_preview("alpha", "value"),
                    "freq": np.int64(1),
                },
            ], dtype=object),
            np.array([
                {
                    "bucket_id": np.int64(bucket_id),
                    **_string_preview(
                        "alpha" if bucket_id == 1 else "zeta", "bucket_min",
                    ),
                    **_string_preview(
                        "alpha" if bucket_id == 1 else "zeta", "bucket_max",
                    ),
                    "freq": np.int64(1),
                }
                for bucket_id in range(1, 5)
            ], dtype=object),
            [
                {**_string_preview("zeta", "value"), "freq": 3},
                {**_string_preview("alpha", "value"), "freq": 1},
            ],
            [{
                "bucket_id": bucket_id,
                **_string_preview(
                    "alpha" if bucket_id == 1 else "zeta", "bucket_min",
                ),
                **_string_preview(
                    "alpha" if bucket_id == 1 else "zeta", "bucket_max",
                ),
                "freq": 1,
            } for bucket_id in range(1, 5)],
        ),
    ],
    ids=["numeric", "string"],
)
def test_deep_d3_d4_structures_survive_redis_roundtrip(
    monkeypatch,
    column_type,
    topx_values,
    buckets,
    expected_topx,
    expected_buckets,
):
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    _seed_live_catalog(fake)
    dqc = DQConfig(fake, ORG, SUPER)
    assert dqc.set_global_config({
        "checks": {
            "D3": {"enabled": True, "threshold": None},
            "D4": {"enabled": True, "threshold": None},
        },
    })

    OneColumnMetaReader.column_type = column_type
    monkeypatch.setattr("supertable.meta_reader.MetaReader", OneColumnMetaReader)
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: True,
    )

    frame = pd.DataFrame([{
        "total_rows": np.int64(4),
        "non_nulls": np.int64(4),
        "distinct_vals": np.int64(2),
        "topx_coverage_pct": np.float64(100.0),
        "topx_values": topx_values,
        "buckets": buckets,
    }])
    # Seal the reported pandas shape that caused the regression.
    materialized = frame.to_dict(orient="records")[0]
    assert isinstance(materialized["topx_values"], np.ndarray)
    assert isinstance(materialized["buckets"], np.ndarray)
    monkeypatch.setattr(
        scheduler,
        "_execute_quality_statement",
        lambda *_args, **_kwargs: FakeExecution(frame),
    )

    outcome = scheduler._run_deep_check(
        fake, ORG, SUPER, TABLE, dqc,
    )
    assert outcome.successful, outcome.message

    # Read through DQConfig so the assertions exercise the JSON/Redis roundtrip,
    # not merely the scheduler's in-memory value.
    column = dqc.get_latest_column(TABLE, "value")
    assert column is not None
    assert column["deep"]["topx_values"] == expected_topx
    assert column["deep"]["buckets"] == expected_buckets
    assert all(isinstance(item, dict) for item in column["deep"]["topx_values"])
    assert all(isinstance(item, dict) for item in column["deep"]["buckets"])

    latest = dqc.get_latest(TABLE)
    assert latest is not None
    deep_outcomes = {
        item["check_id"]: item
        for item in latest["mode_results"]["deep"]["outcomes"]
    }
    assert deep_outcomes["D3"]["value"]["topx_values"] == expected_topx
    assert deep_outcomes["D4"]["value"] == expected_buckets


def test_history_uses_the_same_structured_normalization():
    latest = {
        "checked_at": "2026-08-15T00:00:00+00:00",
        "anomalies": [{
            "check_id": "D4",
            "value": np.array([
                {"bucket_id": np.int64(1), "freq": np.int64(2)},
            ], dtype=object),
        }],
        "parsed": {"total": np.int64(2), "columns": {}},
        "rule_results": [],
    }

    row = build_history_row(TABLE, "deep", latest)
    anomalies = json.loads(row["anomalies_json"])
    assert anomalies[0]["value"] == [{"bucket_id": 1, "freq": 2}]
