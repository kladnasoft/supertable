"""Focused proof for the expected-absent compact mutation boundary."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import fakeredis
import pytest
import redis

from supertable import redis_keys as RK
from supertable.errors import LockLostError, SnapshotCommitConflictError
from supertable.redis_catalog import (
    DeletionIntentConflictError,
    ReadOnlyCatalogError,
    RedisCatalog,
)


ORG = "org"
SUP = "lake"
SIMPLE = "table"
LEAF_TOKEN = "leaf-token"
NAMESPACE_TOKEN = "namespace-token"


def _catalog(catalog_type=RedisCatalog):
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    return catalog_type(redis_client=fake), fake


def _seed_initial(fake) -> None:
    fake.set(
        RK.meta_root(ORG, SUP),
        json.dumps({"version": 9, "ts": 1, "read_only": False}),
    )
    fake.set(RK.lock_namespace(ORG, SUP), NAMESPACE_TOKEN, ex=30)
    fake.set(RK.lock_leaf(ORG, SUP, SIMPLE), LEAF_TOKEN, ex=30)


def _seed_live_leaf(fake, *, floor: int = 20) -> None:
    fake.set(
        RK.meta_leaf(ORG, SUP, SIMPLE),
        json.dumps({
            "version": 4,
            "ts": 1,
            "path": "snap/4.json",
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


def _begin(catalog, *, count: int = 4):
    return catalog.begin_table_mutation(
        ORG,
        SUP,
        SIMPLE,
        lock_token=LEAF_TOKEN,
        namespace_token=NAMESPACE_TOKEN,
        reserve_count=count,
    )


def _sequence(fake):
    return fake.get(RK.meta_rowid_seq(ORG, SUP, SIMPLE))


def test_exact_catalog_stable_absence_uses_only_compact_boundary(monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    config_raw = json.dumps({"primary_keys": ["id"]})
    mirrors_raw = ' { "formats" : [ "delta" ], "ts" : 7 } '
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), config_raw)
    fake.set(RK.meta_mirrors(ORG, SUP), mirrors_raw)
    compact = MagicMock(wraps=catalog._begin_initial_table_mutation)
    general = MagicMock(
        side_effect=AssertionError("general mutation boundary was used"),
    )
    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", compact)
    monkeypatch.setattr(catalog, "_begin_table_mutation", general)

    context = _begin(catalog, count=4)

    assert context == {
        "leaf": None,
        "table_config": {"primary_keys": ["id"]},
        "mirrors": ["DELTA"],
        "mirror_pin": mirrors_raw,
        "rowid_floor": 0,
        "rowid_reservation": (1, 4),
        "_initial_compact_begin_calls": 1,
        "_initial_compact_begin_pin_retries": 0,
        "_initial_compact_begin_general_fallbacks": 0,
    }
    assert compact.call_count == 1
    general.assert_not_called()
    assert _sequence(fake) == "4"
    assert fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)) is None


def test_catalog_subclass_retains_general_boundary(monkeypatch):
    class CatalogAdapter(RedisCatalog):
        pass

    catalog, fake = _catalog(CatalogAdapter)
    _seed_initial(fake)
    compact = MagicMock(
        side_effect=AssertionError("subclass used built-in-only compact path"),
    )
    general = MagicMock(wraps=catalog._begin_table_mutation)
    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", compact)
    monkeypatch.setattr(catalog, "_begin_table_mutation", general)

    assert _begin(catalog, count=2)["rowid_reservation"] == (1, 2)
    compact.assert_not_called()
    assert general.call_count == 1


def test_creator_race_falls_back_to_general_without_reserving(monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    compact_script = catalog._begin_initial_table_mutation
    general = MagicMock(wraps=catalog._begin_table_mutation)

    def create_then_begin(*, keys, args):
        _seed_live_leaf(fake, floor=20)
        return compact_script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", create_then_begin)
    monkeypatch.setattr(catalog, "_begin_table_mutation", general)

    context = _begin(catalog, count=4)

    assert context["leaf"]["version"] == 4
    assert context["rowid_floor"] == 20
    assert context["rowid_reservation"] is None
    assert context["_initial_compact_begin_calls"] == 1
    assert context["_initial_compact_begin_pin_retries"] == 0
    assert context["_initial_compact_begin_general_fallbacks"] == 1
    assert general.call_count == 1
    assert general.call_args.kwargs["args"][1] == "0"
    assert _sequence(fake) is None


def test_valid_pin_race_repins_once_then_reserves_once(monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    compact_script = catalog._begin_initial_table_mutation
    compact = MagicMock()

    def change_once(*, keys, args):
        if compact.call_count == 1:
            fake.set(
                RK.meta_table_config(ORG, SUP, SIMPLE),
                json.dumps({"primary_keys": ["new"]}),
            )
            fake.set(
                RK.meta_mirrors(ORG, SUP),
                json.dumps({"formats": ["iceberg"], "ts": 2}),
            )
        return compact_script(keys=keys, args=args)

    compact.side_effect = change_once
    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", compact)
    general = MagicMock(
        side_effect=AssertionError("pin race used the general boundary"),
    )
    monkeypatch.setattr(catalog, "_begin_table_mutation", general)

    context = _begin(catalog, count=3)

    assert compact.call_count == 2
    general.assert_not_called()
    assert context["table_config"] == {"primary_keys": ["new"]}
    assert context["mirrors"] == ["ICEBERG"]
    assert context["rowid_reservation"] == (1, 3)
    assert context["_initial_compact_begin_calls"] == 2
    assert context["_initial_compact_begin_pin_retries"] == 1
    assert context["_initial_compact_begin_general_fallbacks"] == 0
    assert _sequence(fake) == "3"


@pytest.mark.parametrize("kind", ["config", "mirrors"])
def test_pin_race_to_invalid_document_fails_before_reservation(kind, monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    compact_script = catalog._begin_initial_table_mutation
    calls = 0

    def corrupt_once(*, keys, args):
        nonlocal calls
        calls += 1
        if calls == 1:
            key = (
                RK.meta_table_config(ORG, SUP, SIMPLE)
                if kind == "config"
                else RK.meta_mirrors(ORG, SUP)
            )
            fake.set(key, "[]")
        return compact_script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", corrupt_once)

    with pytest.raises((RuntimeError, ValueError)):
        _begin(catalog)
    assert calls == 2
    assert _sequence(fake) is None


def test_repeated_pin_churn_fails_closed_without_general_or_reservation(monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    compact_script = catalog._begin_initial_table_mutation
    calls = 0

    def churn(*, keys, args):
        nonlocal calls
        calls += 1
        fake.set(
            RK.meta_table_config(ORG, SUP, SIMPLE),
            json.dumps({"generation": calls}),
        )
        return compact_script(keys=keys, args=args)

    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", churn)
    general = MagicMock(side_effect=AssertionError("churn used general begin"))
    monkeypatch.setattr(catalog, "_begin_table_mutation", general)

    with pytest.raises(SnapshotCommitConflictError, match="changed repeatedly"):
        _begin(catalog)
    assert calls == 2
    general.assert_not_called()
    assert _sequence(fake) is None


def test_reply_loss_after_compact_reservation_is_never_retried(monkeypatch):
    catalog, fake = _catalog()
    _seed_initial(fake)
    compact_script = catalog._begin_initial_table_mutation
    calls = 0

    def reserve_then_lose_reply(*, keys, args):
        nonlocal calls
        calls += 1
        compact_script(keys=keys, args=args)
        raise redis.TimeoutError("reply lost after INCRBY")

    monkeypatch.setattr(
        catalog, "_begin_initial_table_mutation", reserve_then_lose_reply,
    )
    with pytest.raises(redis.TimeoutError, match="reply lost"):
        _begin(catalog, count=4)
    assert calls == 1
    assert _sequence(fake) == "4"
    assert fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)) is None

    monkeypatch.setattr(catalog, "_begin_initial_table_mutation", compact_script)
    retry = _begin(catalog, count=2)
    assert retry["rowid_floor"] == 4
    assert retry["rowid_reservation"] == (5, 6)


@pytest.mark.parametrize(
    "failure,error_type",
    [
        ("leaf-lock", LockLostError),
        ("namespace-lock", LockLostError),
        ("namespace-intent", DeletionIntentConflictError),
        ("simple-intent", DeletionIntentConflictError),
        ("missing-root", FileNotFoundError),
        ("wrong-root-type", RuntimeError),
        ("corrupt-root", RuntimeError),
        ("read-only", ReadOnlyCatalogError),
        ("readonly-clone", ReadOnlyCatalogError),
        ("replica", ReadOnlyCatalogError),
    ],
)
def test_fence_and_root_failures_precede_malformed_pin_without_burn(
        failure, error_type,
):
    catalog, fake = _catalog()
    _seed_initial(fake)
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), "[]")
    if failure == "leaf-lock":
        fake.set(RK.lock_leaf(ORG, SUP, SIMPLE), "other")
    elif failure == "namespace-lock":
        fake.set(RK.lock_namespace(ORG, SUP), "other")
    elif failure == "namespace-intent":
        fake.set(RK.meta_namespace_deletion_intent(ORG, SUP), "pending")
    elif failure == "simple-intent":
        fake.set(RK.meta_simple_deletion_intent(ORG, SUP, SIMPLE), "pending")
    elif failure == "missing-root":
        fake.delete(RK.meta_root(ORG, SUP))
    elif failure == "wrong-root-type":
        fake.delete(RK.meta_root(ORG, SUP))
        fake.rpush(RK.meta_root(ORG, SUP), "bad")
    elif failure == "corrupt-root":
        fake.set(RK.meta_root(ORG, SUP), "[]")
    elif failure == "read-only":
        fake.set(
            RK.meta_root(ORG, SUP),
            json.dumps({"version": 9, "ts": 1, "read_only": True}),
        )
    elif failure == "readonly-clone":
        fake.set(
            RK.meta_root(ORG, SUP),
            json.dumps({
                "version": 9,
                "ts": 1,
                "read_only": True,
                "clone_type": "readonly",
                "cloned_from": "source",
            }),
        )
    else:
        fake.set(
            RK.meta_root(ORG, SUP),
            json.dumps({
                "version": 9,
                "ts": 1,
                "read_only": True,
                "clone_type": "replica",
                "cloned_from": "source",
                "replica_tables": ["table"],
            }),
        )

    with pytest.raises(error_type):
        _begin(catalog)
    assert _sequence(fake) is None


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "[]",
        "{",
        "{\"value\":NaN}",
        "{\"value\":1e999}",
        "{\"value\":123456789012345678901234567890}",
        "{\"value\":\"\\ud800\"}",
        "{\"nested\":[{\"value\":\"\\udfff\"}]}",
        "{\"\\ud800\":\"value\"}",
        "{\"deletion_vector_format\":2}",
        (
            "{\"deletion_vector_format\":true,"
            "\"dv_v2_reader_fleet_confirmed\":true}"
        ),
        (
            "{\"deletion_vector_format\":2.0,"
            "\"dv_v2_reader_fleet_confirmed\":true}"
        ),
        (
            "{\"deletion_vector_format\":2e0,"
            "\"dv_v2_reader_fleet_confirmed\":true}"
        ),
        (
            "{\"deletion\\u005fvector_format\":2,"
            "\"dv_v2_reader_fleet_confirmed\":true}"
        ),
        (
            "{\"deletion_vector_format\":2,"
            "\"deletion\\u005fvector_format\":2,"
            "\"dv_v2_reader_fleet_confirmed\":true}"
        ),
        "{\"primary_keys\":[\"a\"],\"primary_keys\":[\"b\"]}",
    ],
)
def test_malformed_or_ambiguous_config_never_burns_rowids(raw):
    catalog, fake = _catalog()
    _seed_initial(fake)
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), raw)

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        _begin(catalog)
    assert _sequence(fake) is None


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "[]",
        "{}",
        "{\"formats\":{},\"ts\":1}",
        "{\"formats\":[],\"ts\":true}",
        "{\"formats\":[],\"ts\":1.0}",
        "{\"formats\":[],\"ts\":1e0}",
        "{\"formats\":[],\"ts\":-0}",
        "{\"formats\":[1],\"ts\":1}",
        "{\"formats\":[\"unknown\"],\"ts\":1}",
        "{\"formats\":[\"delta\",\"DELTA\"],\"ts\":1}",
        "{\"formats\":[],\"formats\":[\"DELTA\"],\"ts\":1}",
        "{\"formats\":[],\"ts\":1,\"t\\u0073\":2}",
        "{\"f\\u006frmats\":[],\"ts\":1}",
        "{\"formats\":[],\"t\\u0073\":1}",
        "{\"formats\":[],\"ts\":NaN}",
        "{\"formats\":[],\"ts\":1,\"extra\":1e999}",
        "{\"formats\":[],\"ts\":1,\"extra\":\"\\ud800\"}",
    ],
)
def test_invalid_or_ambiguous_mirror_pin_never_burns_rowids(raw):
    catalog, fake = _catalog()
    _seed_initial(fake)
    fake.set(RK.meta_mirrors(ORG, SUP), raw)

    with pytest.raises(ValueError, match="Mirror configuration is invalid"):
        _begin(catalog)
    assert _sequence(fake) is None


@pytest.mark.parametrize(
    "key_kind,error_type,match",
    [
        ("leaf", RuntimeError, "Corrupt Redis leaf JSON"),
        ("config", RuntimeError, "Corrupt table configuration"),
        ("mirrors", ValueError, "Mirror configuration is invalid"),
        ("rowid", RuntimeError, "Corrupt Redis rowid sequence"),
    ],
)
def test_wrong_redis_types_are_typed_and_never_burn(
        key_kind, error_type, match,
):
    catalog, fake = _catalog()
    _seed_initial(fake)
    keys = {
        "leaf": RK.meta_leaf(ORG, SUP, SIMPLE),
        "config": RK.meta_table_config(ORG, SUP, SIMPLE),
        "mirrors": RK.meta_mirrors(ORG, SUP),
        "rowid": RK.meta_rowid_seq(ORG, SUP, SIMPLE),
    }
    fake.rpush(keys[key_kind], "wrong-type")

    with pytest.raises(error_type, match=match):
        _begin(catalog)
    if key_kind != "rowid":
        assert _sequence(fake) is None
    else:
        assert fake.lrange(keys[key_kind], 0, -1) == ["wrong-type"]


@pytest.mark.parametrize(
    "initial,count,floor,reservation,final",
    [
        (None, 4, 0, (1, 4), "4"),
        ("7", 4, 7, (8, 11), "11"),
        (str((1 << 53) + 17), 3, (1 << 53) + 17,
         ((1 << 53) + 18, (1 << 53) + 20), str((1 << 53) + 20)),
        (str((1 << 63) - 1), 0, (1 << 63) - 1, None,
         str((1 << 63) - 1)),
    ],
)
def test_initial_rowid_decimal_semantics_are_exact(
        initial, count, floor, reservation, final,
):
    catalog, fake = _catalog()
    _seed_initial(fake)
    if initial is not None:
        fake.set(RK.meta_rowid_seq(ORG, SUP, SIMPLE), initial)

    context = _begin(catalog, count=count)

    assert context["rowid_floor"] == floor
    assert context["rowid_reservation"] == reservation
    assert _sequence(fake) == final


@pytest.mark.parametrize("raw", ["-1", "+1", "1.0", "1e0", "x", ""])
def test_corrupt_rowid_decimal_is_rejected_without_change(raw):
    catalog, fake = _catalog()
    _seed_initial(fake)
    fake.set(RK.meta_rowid_seq(ORG, SUP, SIMPLE), raw)

    with pytest.raises(RuntimeError, match="Corrupt Redis rowid sequence"):
        _begin(catalog)
    assert _sequence(fake) == raw


def test_signed_int64_overflow_leaves_orphan_sequence_unchanged():
    catalog, fake = _catalog()
    _seed_initial(fake)
    maximum = str((1 << 63) - 1)
    fake.set(RK.meta_rowid_seq(ORG, SUP, SIMPLE), maximum)

    with pytest.raises(redis.ResponseError, match="overflow|increment|range"):
        _begin(catalog, count=1)
    assert _sequence(fake) == maximum
    assert fake.get(RK.meta_leaf(ORG, SUP, SIMPLE)) is None


def test_overlong_decimal_floor_maps_to_runtime_error_without_mutation():
    catalog, fake = _catalog()
    _seed_initial(fake)
    overlong = "9" * 5000
    fake.set(RK.meta_rowid_seq(ORG, SUP, SIMPLE), overlong)

    with pytest.raises(RuntimeError, match="initial rowid reservation"):
        _begin(catalog, count=0)
    assert _sequence(fake) == overlong


@pytest.mark.parametrize(
    "reply",
    [
        [0, "", "", "", "1", "0", "1", "0", "4"],
        [0, "leaf", "", "", "1", "0", "1", "0", "4", "0"],
        [0, "", "changed", "", "1", "0", "1", "0", "4", "0"],
        [0, "", "", "changed", "1", "0", "1", "0", "4", "0"],
        [0, "", "", "", "0", "0", "1", "0", "4", "0"],
        [0, "", "", "", "1", "00", "1", "0", "4", "0"],
        [0, "", "", "", "1", "0", "0", "0", "0", "0"],
        [0, "", "", "", "1", "0", "1", "1", "4", "0"],
        [0, "", "", "", "1", "0", "1", "0", "5", "0"],
        [0, "", "", "", "1", "0", "1", "0", "4", "1"],
    ],
)
def test_compact_reply_must_match_exact_ten_field_absent_contract(
        reply, monkeypatch,
):
    catalog, fake = _catalog()
    _seed_initial(fake)
    monkeypatch.setattr(
        catalog,
        "_begin_initial_table_mutation",
        MagicMock(return_value=reply),
    )

    with pytest.raises(RuntimeError, match="table mutation context|rowid"):
        _begin(catalog, count=4)
    assert _sequence(fake) is None


@pytest.mark.parametrize("lost_lock", [False, True])
def test_invalid_utf8_pin_uses_zero_reserve_general_fence(lost_lock):
    catalog, fake = _catalog()
    _seed_initial(fake)
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), b"\xff")
    if lost_lock:
        fake.set(RK.lock_leaf(ORG, SUP, SIMPLE), "other")
        error_type = LockLostError
    else:
        error_type = RuntimeError

    with pytest.raises(error_type):
        _begin(catalog, count=4)
    assert _sequence(fake) is None


def test_deep_json_pin_is_rejected_after_fences_without_rowid_burn():
    catalog, fake = _catalog()
    _seed_initial(fake)
    deep = '{"nested":' + ("[" * 1500) + "0" + ("]" * 1500) + "}"
    fake.set(RK.meta_table_config(ORG, SUP, SIMPLE), deep)

    with pytest.raises(RuntimeError, match="Corrupt table configuration"):
        _begin(catalog, count=4)
    assert _sequence(fake) is None
