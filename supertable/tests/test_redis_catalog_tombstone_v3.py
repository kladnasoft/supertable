"""Focused Redis protocol coverage for tombstone format 3."""

from __future__ import annotations

import hashlib
import json
from unittest.mock import MagicMock

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog


ORG = "org"
SUP = "lake"
SIMPLE = "table"
LEAF_TOKEN = "leaf-token"
NAMESPACE_TOKEN = "namespace-token"
TOMBSTONE_PREFIX = f"{ORG}/{SUP}/tables/{SIMPLE}/tombstone/"


def _catalog():
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    return RedisCatalog(redis_client=fake), fake


def _seed_root_and_lock(fake) -> None:
    fake.set(
        RK.meta_root(ORG, SUP),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_leaf(ORG, SUP, SIMPLE), LEAF_TOKEN, ex=30)


def _seed_live_v1_snapshot(fake, *, floor: int = 100) -> None:
    _seed_root_and_lock(fake)
    fake.set(
        RK.meta_leaf(ORG, SUP, SIMPLE),
        json.dumps({
            "version": 4,
            "ts": 1,
            "path": "snapshots/4.json",
            "payload": {
                "snapshot_version": 4,
                "schema": [],
                "resources": [],
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
                "rowid_high_watermark": floor,
                "_row_filter": None,
            },
        }),
    )


def _v3_payload(*, active: bool = True):
    payload = {
        "snapshot_version": 5,
        "resources": [],
        "tombstone_format": 3,
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    if active:
        payload.update({
            "tombstone": f"{TOMBSTONE_PREFIX}generation/deleted.parquet",
            "tombstone_rows": 3,
            "tombstone_digest": "0" * 64,
        })
    return payload


@pytest.mark.parametrize(
    ("format_marker", "fleet_key"),
    [
        (2, "dv_v2_reader_fleet_confirmed"),
        (3, "dv_v3_reader_fleet_confirmed"),
    ],
)
def test_table_config_accepts_exact_exclusive_fleet_pair(
        format_marker, fleet_key,
):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)

    assert catalog.set_table_config(
        ORG,
        SUP,
        SIMPLE,
        {
            "deletion_vector_format": format_marker,
            fleet_key: True,
        },
        lock_token=LEAF_TOKEN,
    )

    restored = catalog.get_table_config(ORG, SUP, SIMPLE)
    assert restored is not None
    assert restored["deletion_vector_format"] == format_marker
    assert type(restored["deletion_vector_format"]) is int
    assert restored[fleet_key] is True
    assert type(restored["modified_ms"]) is int


@pytest.mark.parametrize(
    "config",
    [
        {"deletion_vector_format": 3},
        {"dv_v3_reader_fleet_confirmed": True},
        {
            "deletion_vector_format": 3,
            "dv_v3_reader_fleet_confirmed": False,
        },
        {
            "deletion_vector_format": 3,
            "dv_v3_reader_fleet_confirmed": 1,
        },
        {
            "deletion_vector_format": "3",
            "dv_v3_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": True,
            "dv_v3_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": 3,
            "dv_v2_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": 2,
            "dv_v3_reader_fleet_confirmed": True,
        },
        {
            "deletion_vector_format": 3,
            "dv_v2_reader_fleet_confirmed": True,
            "dv_v3_reader_fleet_confirmed": True,
        },
    ],
)
def test_table_config_rejects_partial_coerced_or_mixed_v3_activation(config):
    catalog, fake = _catalog()

    with pytest.raises(ValueError, match="DV-v3 activation"):
        catalog.set_table_config(
            ORG, SUP, SIMPLE, config, lock_token=LEAF_TOKEN,
        )
    assert not fake.exists(RK.meta_table_config(ORG, SUP, SIMPLE))


def test_initial_compact_begin_accepts_exact_v3_pin_before_reserving():
    catalog, fake = _catalog()
    _seed_root_and_lock(fake)
    fake.set(RK.lock_namespace(ORG, SUP), NAMESPACE_TOKEN, ex=30)
    raw = (
        '{"deletion_vector_format":3,'
        '"dv_v3_reader_fleet_confirmed":true}'
    )
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), raw)

    context = catalog.begin_table_mutation(
        ORG,
        SUP,
        SIMPLE,
        lock_token=LEAF_TOKEN,
        namespace_token=NAMESPACE_TOKEN,
        reserve_count=2,
    )

    assert context["table_config"] == {
        "deletion_vector_format": 3,
        "dv_v3_reader_fleet_confirmed": True,
    }
    assert context["rowid_reservation"] == (1, 2)
    assert context["_initial_compact_begin_calls"] == 1


@pytest.mark.parametrize(
    "raw",
    [
        '{"deletion_vector_format":3}',
        '{"dv_v3_reader_fleet_confirmed":true}',
        (
            '{"deletion_vector_format":3.0,'
            '"dv_v3_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":3e0,'
            '"dv_v3_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion\\u005fvector_format":3,'
            '"dv_v3_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":3,'
            '"dv_v3_reader_fleet_confirmed":true,'
            '"dv_v2_reader_fleet_confirmed":true}'
        ),
    ],
)
def test_initial_compact_begin_rejects_nonexact_v3_pin_without_rowids(raw):
    catalog, fake = _catalog()
    _seed_root_and_lock(fake)
    fake.set(RK.lock_namespace(ORG, SUP), NAMESPACE_TOKEN, ex=30)
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), raw)

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.begin_table_mutation(
            ORG,
            SUP,
            SIMPLE,
            lock_token=LEAF_TOKEN,
            namespace_token=NAMESPACE_TOKEN,
            reserve_count=2,
        )

    assert not fake.exists(RK.meta_rowid_seq(ORG, SUP, SIMPLE))


@pytest.mark.parametrize(
    "raw",
    [
        (
            '{"deletion_vector_format":3,'
            '"deletion_vector\\u005fformat":3,'
            '"dv_v3_reader_fleet_confirmed":true}'
        ),
        (
            '{"deletion_vector_format":3,'
            '"dv_v3_reader_fleet_confirmed":true,'
            '"dv_v3_reader_fleet\\u005fconfirmed":true}'
        ),
        (
            '{"deletion_vector_format":3,'
            '"dv_v2_reader_fleet_confirmed":true,'
            '"dv_v3_reader_fleet_confirmed":true}'
        ),
    ],
)
def test_general_begin_lua_rejects_ambiguous_v3_pin_before_rowids(raw):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), raw)

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        catalog.begin_table_mutation(
            ORG,
            SUP,
            SIMPLE,
            lock_token=LEAF_TOKEN,
            reserve_count=2,
        )

    assert not fake.exists(RK.meta_rowid_seq(ORG, SUP, SIMPLE))


@pytest.mark.parametrize("fast_path", [False, True])
@pytest.mark.parametrize("active", [False, True])
def test_both_lua_commit_paths_accept_exact_v3_parquet_state(
        fast_path, active,
):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    kwargs = {"expected_mirrors": []}
    if fast_path:
        kwargs["expected_mirror_pin"] = None

    assert catalog.commit_snapshot(
        ORG,
        SUP,
        SIMPLE,
        _v3_payload(active=active),
        "snapshots/5.json",
        expected_version=4,
        expected_path="snapshots/4.json",
        lock_token=LEAF_TOKEN,
        **kwargs,
    ) == (5, 10)

    stored = json.loads(fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)))
    assert stored["payload"]["tombstone_format"] == 3


@pytest.mark.parametrize("fast_path", [False, True])
@pytest.mark.parametrize(
    "wire_change",
    [
        {"tombstone": f"{TOMBSTONE_PREFIX}generation/deleted.json"},
        {"tombstone": "org/lake/tables/other/tombstone/deleted.parquet"},
        {"tombstone_rows": 1.5},
        {"tombstone_digest": "A" * 64},
    ],
)
def test_both_lua_commit_paths_repeat_v3_artifact_invariants(
        monkeypatch, fast_path, wire_change,
):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    before = (
        fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)),
        fake.get(RK.meta_root(ORG, SUP)),
    )
    monkeypatch.setattr(
        "supertable.redis_catalog.snapshot_cache_payload",
        lambda value: {**value, **wire_change, "_row_filter": None},
    )
    kwargs = {"expected_mirrors": []}
    if fast_path:
        kwargs["expected_mirror_pin"] = None

    with pytest.raises(ValueError, match="invalid snapshot payload"):
        catalog.commit_snapshot(
            ORG,
            SUP,
            SIMPLE,
            _v3_payload(),
            "snapshots/5.json",
            expected_version=4,
            expected_path="snapshots/4.json",
            lock_token=LEAF_TOKEN,
            **kwargs,
        )

    assert before == (
        fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)),
        fake.get(RK.meta_root(ORG, SUP)),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"tombstone": f"{TOMBSTONE_PREFIX}deleted.json"},
        {"tombstone": "org/lake/tables/other/tombstone/deleted.parquet"},
        {"tombstone_rows": 0},
        {"tombstone_rows": True},
        {"tombstone_digest": None},
        {"tombstone_digest": "A" * 64},
    ],
)
def test_python_commit_validator_rejects_invalid_v3_state(changes):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    payload = {**_v3_payload(), **changes}

    with pytest.raises(ValueError, match="deletion-vector state|namespace"):
        catalog.commit_snapshot(
            ORG,
            SUP,
            SIMPLE,
            payload,
            "snapshots/5.json",
            expected_version=4,
            expected_path="snapshots/4.json",
            lock_token=LEAF_TOKEN,
            expected_mirrors=[],
        )


@pytest.mark.parametrize("prepared", [False, True])
def test_mutation_begin_reuses_exact_v3_cached_floor(prepared):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    leaf = json.loads(fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)))
    leaf["payload"].update({
        "tombstone_format": 3,
        "tombstone": f"{TOMBSTONE_PREFIX}deleted.parquet",
        "tombstone_rows": 3,
        "tombstone_digest": "0" * 64,
    })
    fake.set(RK.meta_leaf(ORG, SUP, SIMPLE), json.dumps(leaf))
    pin = (
        catalog.prepare_table_mutation_leaf(ORG, SUP, SIMPLE)
        if prepared
        else None
    )

    context = catalog.begin_table_mutation(
        ORG,
        SUP,
        SIMPLE,
        lock_token=LEAF_TOKEN,
        reserve_count=2,
        prepared_leaf=pin,
    )

    assert context["validated_snapshot"]["tombstone_format"] == 3
    assert context["rowid_floor"] == 100
    assert context["rowid_reservation"] == (101, 102)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tombstone_format", 3.0),
        ("tombstone_rows", 1.0),
        ("snapshot_version", 4.0),
    ],
)
def test_unprepared_begin_rejects_integral_float_v3_tokens_without_rowids(
        field, value,
):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    leaf = json.loads(fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)))
    leaf["payload"].update({
        "tombstone_format": 3,
        "tombstone": f"{TOMBSTONE_PREFIX}deleted.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": "0" * 64,
    })
    leaf["payload"][field] = value
    fake.set(RK.meta_leaf(ORG, SUP, SIMPLE), json.dumps(leaf))

    context = catalog.begin_table_mutation(
        ORG,
        SUP,
        SIMPLE,
        lock_token=LEAF_TOKEN,
        reserve_count=2,
    )

    assert "validated_snapshot" not in context
    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq(ORG, SUP, SIMPLE))


def test_prepared_foreign_v3_cache_cannot_authorize_rowids():
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    leaf = json.loads(fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)))
    leaf["payload"].update({
        "tombstone_format": 3,
        "tombstone": "org/lake/tables/other/tombstone/deleted.parquet",
        "tombstone_rows": 3,
        "tombstone_digest": "0" * 64,
    })
    fake.set(RK.meta_leaf(ORG, SUP, SIMPLE), json.dumps(leaf))
    pin = catalog.prepare_table_mutation_leaf(ORG, SUP, SIMPLE)

    context = catalog.begin_table_mutation(
        ORG,
        SUP,
        SIMPLE,
        lock_token=LEAF_TOKEN,
        reserve_count=2,
        prepared_leaf=pin,
    )

    assert "validated_snapshot" not in context
    assert context["rowid_floor"] is None
    assert context["rowid_reservation"] is None
    assert not fake.exists(RK.meta_rowid_seq(ORG, SUP, SIMPLE))


def test_normal_v3_commit_does_not_hash_content(monkeypatch):
    catalog, fake = _catalog()
    _seed_live_v1_snapshot(fake)
    monkeypatch.setattr(
        hashlib,
        "sha256",
        MagicMock(side_effect=AssertionError("Redis hashed tombstone content")),
    )

    assert catalog.commit_snapshot(
        ORG,
        SUP,
        SIMPLE,
        _v3_payload(),
        "snapshots/5.json",
        expected_version=4,
        expected_path="snapshots/4.json",
        lock_token=LEAF_TOKEN,
        expected_mirrors=[],
    ) == (5, 10)
