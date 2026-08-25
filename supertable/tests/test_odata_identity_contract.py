from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import inspect
import struct
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

import duckdb
import polars as pl
import pytest

from supertable.data_classes import (
    Reflection,
    RbacViewDef,
    ResourceObjectSeal,
    SuperSnapshot,
    TombstoneDef,
)
from supertable import data_reader as data_reader_module
from supertable.data_reader import _odata_identity_binding
from supertable.engine import duckdb_engine as duckdb_engine_module
from supertable.engine.engine_enum import Engine
from supertable.engine.duckdb_engine import (
    _append_protected_odata_identity,
    _prepare_protected_odata_query,
    _protected_odata_nonfinite_guard_query,
)
from supertable.engine.engine_common import (
    SOURCE_FILE_COL,
    create_rbac_view,
    create_tombstone_view,
)
from supertable.engine import engine_common as engine_common_module
from supertable.row_identity import (
    ODATA_INTERNAL_ROWID_COLUMN,
    ResourceRowIdIntegritySeal,
    snapshot_proves_stable_rowids,
)
from supertable.processing import write_parquet_and_collect_resources
from supertable.odata_continuation import (
    bind_odata_continuation_boundary,
    odata_float_order_columns,
    validate_odata_continuation_boundary,
)
from supertable.utils.sql_parser import SQLParser


def _modern_snapshot(**updates):
    footer_a = "a" * 64
    footer_b = "b" * 64
    snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "int64", "tenant": "int64"},
        "resources": [
            {
                "file": "org/lake/tables/t/data/a.parquet",
                "rows": 2,
                "footer_sha256": footer_a,
                "rowid_integrity": {
                    "version": 1,
                    "rows": 2,
                    "nonnull": 2,
                    "unique": 2,
                    "minimum": 1,
                    "maximum": 2,
                    "digest": "1" * 64,
                    "footer_sha256": footer_a,
                },
            },
            {
                "file": "org/lake/tables/t/data/b.parquet",
                "rows": 1,
                "footer_sha256": footer_b,
                "rowid_integrity": {
                    "version": 1,
                    "rows": 1,
                    "nonnull": 1,
                    "unique": 1,
                    "minimum": 3,
                    "maximum": 3,
                    "digest": "2" * 64,
                    "footer_sha256": footer_b,
                },
            },
        ],
        "rowid_high_watermark": 5,
    }
    snapshot.update(updates)
    return snapshot


def test_stable_rowid_contract_is_modern_local_and_manifest_complete():
    assert snapshot_proves_stable_rowids(_modern_snapshot()) is True
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(rowid_high_watermark=None),
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(rowid_high_watermark=2),
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(), {"payload": {"_linked_share": "link-1"}},
    ) is False
    assert snapshot_proves_stable_rowids(
        _modern_snapshot(resources=[{"file": "x", "rows": True}]),
    ) is False
    corrupt = _modern_snapshot()
    corrupt["resources"][0]["rowid_integrity"]["unique"] = 1
    assert snapshot_proves_stable_rowids(corrupt) is False
    corrupt = _modern_snapshot()
    corrupt["resources"][1]["rowid_integrity"]["maximum"] = 6
    assert snapshot_proves_stable_rowids(corrupt) is False
    legacy = _modern_snapshot()
    for resource in legacy["resources"]:
        resource.pop("rowid_integrity")
    assert snapshot_proves_stable_rowids(legacy) is False


def _rowid_seal(rows, minimum, maximum, marker="a"):
    return ResourceRowIdIntegritySeal(
        version=1,
        rows=rows,
        nonnull=rows,
        unique=rows,
        minimum=minimum,
        maximum=maximum,
        digest=marker * 64,
        footer_sha256=marker * 64,
    )


def test_writer_publishes_physical_order_rowid_seal_tied_to_footer():
    storage = MagicMock()
    uploaded = {}
    storage.write_bytes.side_effect = (
        lambda path, data: uploaded.update({path: bytes(data)})
    )
    resources = []
    with (
        patch("supertable.processing._get_storage", return_value=storage),
        patch(
            "supertable.processing.generate_filename",
            return_value="rowids.parquet",
        ),
    ):
        write_parquet_and_collect_resources(
            pl.DataFrame({"id": [20, 10], "__rowid__": [7, 3]}),
            ["id"],
            "/table/data",
            resources,
            compression_level=1,
        )

    assert len(resources) == 1
    resource = resources[0]
    seal = resource["rowid_integrity"]
    expected_digest = hashlib.sha256(
        b"supertable-rowid-integrity-v1\0"
        + struct.pack(">q", 3)
        + struct.pack(">q", 7)
    ).hexdigest()
    assert seal == {
        "version": 1,
        "rows": 2,
        "nonnull": 2,
        "unique": 2,
        "minimum": 3,
        "maximum": 7,
        "digest": expected_digest,
        "footer_sha256": resource["footer_sha256"],
    }
    assert resource["file"] in uploaded


@pytest.mark.parametrize(
    "rowids",
    [
        pl.Series("__rowid__", [1, 1], dtype=pl.Int64),
        pl.Series("__rowid__", [1, None], dtype=pl.Int64),
        pl.Series("__rowid__", [0, 2], dtype=pl.Int64),
        pl.Series("__rowid__", [1, 2], dtype=pl.Int32),
    ],
    ids=["duplicate", "null", "nonpositive", "noncanonical-type"],
)
def test_writer_rejects_invalid_present_rowid_before_upload(rowids):
    storage = MagicMock()
    resources = []
    with (
        patch("supertable.processing._get_storage", return_value=storage),
        patch(
            "supertable.processing.generate_filename",
            return_value="invalid-rowids.parquet",
        ),
        pytest.raises(ValueError, match="__rowid__"),
    ):
        write_parquet_and_collect_resources(
            pl.DataFrame({"id": [1, 2], "__rowid__": rowids}),
            [],
            "/table/data",
            resources,
        )
    storage.write_bytes.assert_not_called()
    assert resources == []


def test_tombstone_view_exposes_only_fixed_odata_identity_when_opted_in():
    con = duckdb.connect()
    con.execute(
        'CREATE TABLE src(id BIGINT, tenant BIGINT, "__rowid__" BIGINT, '
        '"__timestamp__" BIGINT)'
    )
    con.execute("INSERT INTO src VALUES (10, 1, 101, 1), (20, 2, 202, 1)")

    create_tombstone_view(con, "src", "ordinary", None)
    assert [row[0] for row in con.execute("DESCRIBE ordinary").fetchall()] == [
        "id", "tenant",
    ]

    create_tombstone_view(
        con,
        "src",
        "keyed",
        None,
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
        odata_rowid_high_watermark=202,
        odata_resource_keys=["f"],
        odata_resource_rows={"f": 2},
        odata_rowid_integrity_seals={"f": _rowid_seal(2, 101, 202)},
    )
    assert con.execute(
        f'SELECT id, "{ODATA_INTERNAL_ROWID_COLUMN}" FROM keyed ORDER BY id'
    ).fetchall() == [(10, 101), (20, 202)]
    described = [row[0] for row in con.execute("DESCRIBE keyed").fetchall()]
    assert "__rowid__" not in described
    assert "__timestamp__" not in described


@pytest.mark.parametrize(
    ("rowids", "high_watermark"),
    [
        ([1, 1], 2),
        ([1, None], 2),
        ([0, 2], 2),
        ([1, 3], 2),
    ],
)
def test_odata_identity_rejects_physical_rowid_corruption(
    rowids, high_watermark,
):
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.executemany(
        "INSERT INTO src VALUES (?, ?)",
        [(index, rowid) for index, rowid in enumerate(rowids)],
    )
    with pytest.raises(RuntimeError, match="physical snapshot"):
        create_tombstone_view(
            con,
            "src",
            "keyed",
            None,
            preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
            odata_rowid_high_watermark=high_watermark,
            odata_resource_keys=["f"],
            odata_resource_rows={"f": 2},
            # The persisted metadata claims a healthy object. The independent
            # backend scan must still catch substitution/corruption.
            odata_rowid_integrity_seals={
                "f": _rowid_seal(2, 1, 2),
            },
        )


def test_legacy_odata_identity_is_physically_verified_without_cached_seal():
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    create_tombstone_view(
        con,
        "src",
        "keyed",
        None,
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
        odata_rowid_high_watermark=2,
        odata_resource_keys=["legacy"],
        odata_resource_rows={"legacy": 2},
        odata_rowid_integrity_seals={},
    )
    assert con.execute("SELECT * FROM keyed ORDER BY id").fetchall() == [
        (10, 1), (20, 2),
    ]


def test_rowid_seal_without_object_identity_is_rescanned_after_substitution():
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    proof = {
        "preserve_rowid_as": ODATA_INTERNAL_ROWID_COLUMN,
        "odata_rowid_high_watermark": 2,
        "odata_resource_keys": ["mutable-local-resource"],
        "odata_resource_rows": {"mutable-local-resource": 2},
        "odata_rowid_integrity_seals": {
            "mutable-local-resource": _rowid_seal(2, 1, 2),
        },
        "odata_cache_namespace": (
            "org", "tests", "MutableStorage", "1" * 32,
        ),
    }
    create_tombstone_view(con, "src", "first", None, **proof)
    con.execute("DELETE FROM src")
    con.execute("INSERT INTO src VALUES (10, 1), (20, 1)")

    with pytest.raises(RuntimeError, match="physical snapshot"):
        create_tombstone_view(con, "src", "second", None, **proof)


def test_odata_positive_proof_cache_key_is_fixed_size_at_max_fanout():
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1)")
    resource_count = engine_common_module._ODATA_ROWID_MAX_RESOURCES
    keys = [f"fixed-cache-resource-{index}" for index in range(resource_count)]
    rows = {key: int(index == 0) for index, key in enumerate(keys)}
    object_seals = {
        key: ResourceObjectSeal(size=1, etag=f"etag-{index}")
        for index, key in enumerate(keys)
    }
    enforced_read_identities = {
        key: f"local-cache-v1:{index:064x}"
        for index, key in enumerate(keys)
    }

    create_tombstone_view(
        con,
        "src",
        "keyed",
        None,
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
        odata_rowid_high_watermark=1,
        odata_resource_keys=keys,
        odata_resource_rows=rows,
        odata_rowid_integrity_seals={},
        odata_resource_object_seals=object_seals,
        odata_enforced_read_identities=enforced_read_identities,
        odata_cache_namespace=(
            "org", "tests", "ImmutableStorage", "2" * 32,
        ),
        odata_read_identity_enforced=True,
    )

    with engine_common_module._odata_rowid_proof_cache_lock:
        cache_key = next(reversed(engine_common_module._odata_rowid_proof_cache))
    assert cache_key[0] == "odata-rowid-proof-v4"
    assert len(cache_key) == 2
    assert len(cache_key[1]) == 64
    assert "fixed-cache-resource" not in repr(cache_key)

    with pytest.raises(RuntimeError, match="resource proof"):
        create_tombstone_view(
            con,
            "src",
            "too_many",
            None,
            preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
            odata_rowid_high_watermark=1,
            odata_resource_keys=[*keys, "one-too-many"],
            odata_resource_rows={},
            odata_rowid_integrity_seals={},
        )


@pytest.mark.parametrize(
    ("object_seal", "read_identity_enforced"),
    [
        (ResourceObjectSeal(size=1, version="provider-version-1"), True),
        (ResourceObjectSeal(size=1, last_modified_ns=123456789), True),
        (ResourceObjectSeal(size=1, etag="declared-but-not-enforced"), False),
    ],
    ids=["version-only", "mtime-only", "etag-not-enforced-by-read"],
)
def test_unenforceable_object_metadata_never_caches_rowid_proof(
    object_seal, read_identity_enforced,
):
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    proof = {
        "preserve_rowid_as": ODATA_INTERNAL_ROWID_COLUMN,
        "odata_rowid_high_watermark": 2,
        "odata_resource_keys": ["provider/object.parquet"],
        "odata_resource_rows": {"provider/object.parquet": 2},
        "odata_rowid_integrity_seals": {
            "provider/object.parquet": _rowid_seal(2, 1, 2),
        },
        "odata_resource_object_seals": {
            "provider/object.parquet": object_seal,
        },
        "odata_cache_namespace": (
            "org", "tests", "ConditionalStorage", "3" * 32,
        ),
        "odata_enforced_read_identities": {
            "provider/object.parquet": f"local-cache-v1:{'e' * 64}",
        },
        # An ETag is useful only when the actual read enforces If-Match; an
        # identity-capable path likewise cannot enforce a condition absent
        # from version/mtime-only metadata.
        "odata_read_identity_enforced": read_identity_enforced,
    }
    create_tombstone_view(con, "src", "first", None, **proof)
    con.execute("DELETE FROM src")
    con.execute("INSERT INTO src VALUES (10, 1), (20, 1)")

    with pytest.raises(RuntimeError, match="physical snapshot"):
        create_tombstone_view(con, "src", "second", None, **proof)


def test_odata_cache_requires_complete_identity_enforcement_for_global_proof():
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    keys = ["provider/a.parquet", "provider/b.parquet"]
    proof = {
        "preserve_rowid_as": ODATA_INTERNAL_ROWID_COLUMN,
        "odata_rowid_high_watermark": 2,
        "odata_resource_keys": keys,
        "odata_resource_rows": {key: 1 for key in keys},
        "odata_rowid_integrity_seals": {
            keys[0]: _rowid_seal(1, 1, 1, "a"),
            keys[1]: _rowid_seal(1, 2, 2, "b"),
        },
        "odata_resource_object_seals": {
            keys[0]: ResourceObjectSeal(size=1, etag="etag-a"),
            keys[1]: ResourceObjectSeal(size=1, etag="etag-b"),
        },
        "odata_cache_namespace": (
            "org", "tests", "ConditionalReader", "5" * 32,
        ),
        "odata_read_identity_enforced": True,
    }
    partial_enforcement = {
        keys[0]: f"local-cache-v1:{'a' * 64}",
    }
    with engine_common_module._odata_rowid_proof_cache_lock:
        engine_common_module._odata_rowid_proof_cache.clear()
    create_tombstone_view(
        con,
        "src",
        "partial_first",
        None,
        **proof,
        odata_enforced_read_identities=partial_enforcement,
    )
    con.execute("DELETE FROM src")
    con.execute("INSERT INTO src VALUES (10, 1), (20, 1)")
    with pytest.raises(RuntimeError, match="physical snapshot"):
        create_tombstone_view(
            con,
            "src",
            "partial_second",
            None,
            **proof,
            odata_enforced_read_identities=partial_enforcement,
        )
    with engine_common_module._odata_rowid_proof_cache_lock:
        assert not engine_common_module._odata_rowid_proof_cache

    con.execute("DELETE FROM src")
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    complete_enforcement = {
        keys[0]: f"local-cache-v1:{'a' * 64}",
        keys[1]: f"local-cache-v1:{'b' * 64}",
    }
    create_tombstone_view(
        con,
        "src",
        "complete_first",
        None,
        **proof,
        odata_enforced_read_identities=complete_enforcement,
    )
    with engine_common_module._odata_rowid_proof_cache_lock:
        assert len(engine_common_module._odata_rowid_proof_cache) == 1

    # A real identity-enforcing reader cannot observe substituted bytes under
    # these same route identities. Simulating mutation here proves the second
    # call takes the positive-cache path only after global enforcement was
    # complete; the partial case above was forced through the aggregate.
    con.execute("DELETE FROM src")
    con.execute("INSERT INTO src VALUES (10, 1), (20, 1)")
    create_tombstone_view(
        con,
        "src",
        "complete_second",
        None,
        **proof,
        odata_enforced_read_identities=complete_enforcement,
    )


def test_odata_cache_binds_provider_link_policy_and_publication_authority():
    con = duckdb.connect()
    con.execute('CREATE TABLE src(id BIGINT, "__rowid__" BIGINT)')
    con.execute("INSERT INTO src VALUES (10, 1), (20, 2)")
    key = "provider/object.parquet"
    baseline = {
        "preserve_rowid_as": ODATA_INTERNAL_ROWID_COLUMN,
        "odata_rowid_high_watermark": 2,
        "odata_resource_keys": [key],
        "odata_resource_rows": {key: 2},
        "odata_rowid_integrity_seals": {key: _rowid_seal(2, 1, 2)},
        "odata_resource_object_seals": {
            key: ResourceObjectSeal(size=1, etag="immutable-etag"),
        },
        "odata_cache_namespace": (
            "consumer", "tests", "LinkedConditionalReader", "4" * 32,
        ),
        "odata_read_identity_enforced": True,
        "odata_enforced_read_identities": {
            key: f"share-relay-v1:{'f' * 64}",
        },
    }
    authorities = [
        ("a" * 64, "1" * 64, 7),
        ("b" * 64, "1" * 64, 7),  # provider resource identity
        ("b" * 64, "2" * 64, 7),  # provider/link/policy seal
        ("b" * 64, "2" * 64, 8),  # provider publication order
    ]
    with engine_common_module._odata_rowid_proof_cache_lock:
        engine_common_module._odata_rowid_proof_cache.clear()
    for index, (provider_digest, policy_digest, generation) in enumerate(
        authorities
    ):
        create_tombstone_view(
            con,
            "src",
            f"authority_{index}",
            None,
            **baseline,
            odata_resource_cache_identities={
                key: f"share-cache-v1:{provider_digest}",
            },
            odata_share_policy_fingerprint=policy_digest,
            odata_share_publication_generation=generation,
        )

    with engine_common_module._odata_rowid_proof_cache_lock:
        cache_keys = list(engine_common_module._odata_rowid_proof_cache)
    assert len(cache_keys) == len(authorities)
    assert len(set(cache_keys)) == len(authorities)


def test_ordinary_duckdb_query_does_not_resolve_or_pass_odata_proof(
    tmp_path, monkeypatch,
):
    path = tmp_path / "ordinary.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        [str(path)],
        {"id"},
        resource_keys=[str(path)],
    )
    reflection = Reflection("local", 1, 2, [snapshot])
    parser = SQLParser("shop", "SELECT id FROM events ORDER BY id", "duckdb")
    captured = []
    original = duckdb_engine_module.create_tombstone_view

    def capture(*args, **kwargs):
        captured.append(dict(kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(
        duckdb_engine_module, "create_tombstone_view", capture,
    )
    result = duckdb_engine_module.DuckDB().execute(
        reflection,
        parser,
        SimpleNamespace(
            temp_dir=str(tmp_path),
            query_plan_path=str(tmp_path / "plan.json"),
        ),
        lambda _event: None,
        timeout_sec=10,
    )

    assert result["id"].tolist() == [1, 2]
    assert len(captured) == 1
    assert captured[0]["preserve_rowid_as"] is None
    assert not any(key.startswith("odata_") for key in captured[0])


def test_protected_duckdb_direct_scan_never_claims_identity_enforcement(
    tmp_path, monkeypatch,
):
    path = tmp_path / "protected.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    key = str(path)
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        [key],
        {"id"},
        resource_keys=[key],
        column_types={"id": "Int64"},
        snapshot_resource_keys=[key],
        stable_rowid_contract=True,
        rowid_high_watermark=2,
        resource_row_counts={key: 2},
        resource_rowid_integrity_seals={key: _rowid_seal(2, 1, 2)},
        resource_object_seals={
            key: ResourceObjectSeal(size=path.stat().st_size, etag="etag-1"),
        },
        resource_cache_identities=[None],
    )
    reflection = Reflection("local", 1, 2, [snapshot])
    reflection.odata_identity_aliases = {
        "events": ODATA_INTERNAL_ROWID_COLUMN,
    }
    parser = SQLParser("shop", "SELECT id FROM events ORDER BY id", "duckdb")
    captured = []
    original = duckdb_engine_module.create_tombstone_view

    def capture(*args, **kwargs):
        captured.append(dict(kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(
        duckdb_engine_module, "create_tombstone_view", capture,
    )
    stream = duckdb_engine_module.DuckDB(organization="consumer").execute_stream(
        reflection,
        parser,
        SimpleNamespace(
            temp_dir=str(tmp_path),
            query_plan_path=str(tmp_path / "protected-plan.json"),
        ),
        lambda _event: None,
        timeout_sec=10,
    )
    result_ids = [
        value
        for batch in stream
        for value in batch.column(batch.schema.get_field_index("id")).to_pylist()
    ]

    assert result_ids == [1, 2]
    assert captured[0]["odata_read_identity_enforced"] is False
    assert captured[0]["odata_resource_cache_identities"] == {key: None}
    namespace = captured[0]["odata_cache_namespace"]
    assert namespace[:3] == ("consumer", "builtins", "NoneType")
    assert len(namespace[3]) == 32
    assert set(namespace[3]) <= set("0123456789abcdef")


def test_protected_duckdb_enables_cache_only_from_complete_relay_attestation(
    tmp_path, monkeypatch,
):
    path = tmp_path / "relay-protected.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": pl.Series([1, 2], dtype=pl.Int64),
        "__timestamp__": [1, 1],
    }).write_parquet(path)
    key = str(path)
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        [key],
        {"id"},
        resource_keys=[key],
        column_types={"id": "Int64"},
        snapshot_resource_keys=[key],
        stable_rowid_contract=True,
        rowid_high_watermark=2,
        resource_row_counts={key: 2},
        resource_rowid_integrity_seals={key: _rowid_seal(2, 1, 2)},
        resource_object_seals={
            key: ResourceObjectSeal(size=path.stat().st_size, etag="etag-1"),
        },
        resource_cache_identities=[None],
    )
    reflection = Reflection("local", 1, 2, [snapshot])
    reflection.odata_identity_aliases = {
        "events": ODATA_INTERNAL_ROWID_COLUMN,
    }
    relay_identity = f"local-cache-v1:{'d' * 64}"

    def attest_all_resources(candidate, **_kwargs):
        attested_snapshot = replace(
            candidate.supers[0],
            resource_relay_cache_identities={key: relay_identity},
        )
        return (
            replace(candidate, supers=[attested_snapshot]),
            duckdb_engine_module.StableRelayLease(),
        )

    monkeypatch.setattr(
        duckdb_engine_module,
        "alias_stable_remote_paths",
        attest_all_resources,
    )
    captured = []
    original = duckdb_engine_module.create_tombstone_view

    def capture(*args, **kwargs):
        captured.append(dict(kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(
        duckdb_engine_module, "create_tombstone_view", capture,
    )
    parser = SQLParser("shop", "SELECT id FROM events ORDER BY id", "duckdb")
    stream = duckdb_engine_module.DuckDB(organization="consumer").execute_stream(
        reflection,
        parser,
        SimpleNamespace(
            temp_dir=str(tmp_path),
            query_plan_path=str(tmp_path / "relay-proof-plan.json"),
        ),
        lambda _event: None,
        timeout_sec=10,
    )
    assert [
        value
        for batch in stream
        for value in batch.column(batch.schema.get_field_index("id")).to_pylist()
    ] == [1, 2]
    assert captured[0]["odata_read_identity_enforced"] is True
    assert captured[0]["odata_enforced_read_identities"] == {
        key: relay_identity,
    }


def test_protected_duckdb_rejects_nonfinite_float_key_before_first_page(
    tmp_path,
):
    path = tmp_path / "nonfinite-order.parquet"
    pl.DataFrame({
        "score": [1.0, 2.0, float("nan")],
        "__rowid__": pl.Series([1, 2, 3], dtype=pl.Int64),
        "__timestamp__": [1, 1, 1],
    }).write_parquet(path)
    key = str(path)
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        [key],
        {"score"},
        resource_keys=[key],
        column_types={"score": "Float64"},
        snapshot_resource_keys=[key],
        stable_rowid_contract=True,
        rowid_high_watermark=3,
        resource_row_counts={key: 3},
        resource_rowid_integrity_seals={key: _rowid_seal(3, 1, 3)},
        resource_cache_identities=[None],
    )
    reflection = Reflection("local", 1, 3, [snapshot])
    reflection.odata_identity_aliases = {
        "events": ODATA_INTERNAL_ROWID_COLUMN,
    }
    parser = SQLParser(
        "shop",
        "SELECT score FROM events ORDER BY score LIMIT 1",
        "duckdb",
    )

    with pytest.raises(
        RuntimeError,
        match="DuckDB protected OData order validation failed",
    ):
        duckdb_engine_module.DuckDB(organization="consumer").execute_stream(
            reflection,
            parser,
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "nonfinite-plan.json"),
            ),
            lambda _event: None,
            timeout_sec=10,
        )


@pytest.mark.parametrize("predicate", ["id < 50", "id > 100"])
def test_odata_global_proof_rejects_duplicate_in_other_predicate_file(
    tmp_path, predicate,
):
    paths = []
    for name, identifier in (("low", 10), ("high", 110)):
        path = tmp_path / f"{name}.parquet"
        pl.DataFrame({
            "id": [identifier],
            # Each resource is locally healthy; only the table-global domain
            # reveals the duplicate that would destabilize later pages.
            "__rowid__": pl.Series([1], dtype=pl.Int64),
            "__timestamp__": [1],
        }).write_parquet(path)
        paths.append(str(path))
    rowid_seals = {
        paths[0]: _rowid_seal(1, 1, 1, "a"),
        paths[1]: _rowid_seal(1, 1, 1, "b"),
    }
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        list(paths),
        {"id"},
        resource_keys=list(paths),
        column_types={"id": "Int64"},
        snapshot_resource_keys=list(paths),
        stable_rowid_contract=True,
        rowid_high_watermark=2,
        resource_row_counts={key: 1 for key in paths},
        resource_rowid_integrity_seals=rowid_seals,
        resource_cache_identities=[None, None],
    )
    reflection = Reflection("local", 2, 2, [snapshot])
    reflection.odata_identity_aliases = {
        "events": ODATA_INTERNAL_ROWID_COLUMN,
    }
    parser = SQLParser(
        "shop",
        f"SELECT id FROM events WHERE {predicate} ORDER BY id",
        "duckdb",
    )

    with pytest.raises(RuntimeError, match="managed query setup"):
        duckdb_engine_module.DuckDB(organization="consumer").execute_stream(
            reflection,
            parser,
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "global-proof-plan.json"),
            ),
            lambda _event: None,
            timeout_sec=10,
        )


def test_protected_duckdb_rejects_estimator_pruned_global_proof(tmp_path):
    selected = tmp_path / "selected.parquet"
    pruned = tmp_path / "pruned.parquet"
    pl.DataFrame({
        "id": [110], "__rowid__": [2], "__timestamp__": [1],
    }).write_parquet(selected)
    pl.DataFrame({
        "id": [10], "__rowid__": [1], "__timestamp__": [1],
    }).write_parquet(pruned)
    selected_key = str(selected)
    snapshot = SuperSnapshot(
        "shop",
        "events",
        1,
        [selected_key],
        {"id"},
        resource_keys=[selected_key],
        column_types={"id": "Int64"},
        snapshot_resource_keys=[selected_key, str(pruned)],
        stable_rowid_contract=True,
        rowid_high_watermark=2,
        resource_row_counts={selected_key: 1},
        resource_rowid_integrity_seals={
            selected_key: _rowid_seal(1, 2, 2),
        },
        resource_cache_identities=[None],
    )
    reflection = Reflection("local", 1, 1, [snapshot])
    reflection.odata_identity_aliases = {
        "events": ODATA_INTERNAL_ROWID_COLUMN,
    }
    parser = SQLParser(
        "shop",
        "SELECT id FROM events WHERE id > 100 ORDER BY id",
        "duckdb",
    )

    with pytest.raises(RuntimeError, match="managed query setup"):
        duckdb_engine_module.DuckDB(organization="consumer").execute_stream(
            reflection,
            parser,
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "incomplete-proof-plan.json"),
            ),
            lambda _event: None,
            timeout_sec=10,
        )


def test_protected_identity_survives_rbac_after_row_and_column_filters():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE keyed(id BIGINT, tenant BIGINT, secret VARCHAR, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute("INSERT INTO keyed VALUES (10, 1, 'a', 101), (20, 2, 'b', 202)")
    create_rbac_view(
        con,
        "keyed",
        "scoped",
        RbacViewDef(
            allowed_columns=["id", "tenant"],
            excluded_columns=["tenant"],
            where_clause='"tenant" = 1',
        ),
        required_internal_columns=[ODATA_INTERNAL_ROWID_COLUMN],
    )
    assert con.execute("SELECT * FROM scoped").fetchall() == [(10, 101)]
    assert [row[0] for row in con.execute("DESCRIBE scoped").fetchall()] == [
        "id", ODATA_INTERNAL_ROWID_COLUMN,
    ]


def test_protected_identity_is_projected_only_after_tombstone_filtering():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE src(id BIGINT, "__rowid__" BIGINT, '
        f'"{SOURCE_FILE_COL}" VARCHAR)'
    )
    con.execute("INSERT INTO src VALUES (10, 101, 'f'), (20, 202, 'f')")
    con.execute('CREATE TABLE dv("__file__" VARCHAR, "__rowid__" BIGINT)')
    con.execute("INSERT INTO dv VALUES ('f', 101)")
    create_tombstone_view(
        con,
        "src",
        "live",
        TombstoneDef(
            tombstone_path="validated-by-private-table",
            expected_rows=1,
            resource_keys=("f",),
            snapshot_resource_keys=("f",),
        ),
        dv_table="dv",
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
        odata_rowid_high_watermark=202,
        odata_resource_keys=["f"],
        odata_resource_rows={"f": 2},
        odata_rowid_integrity_seals={"f": _rowid_seal(2, 101, 202)},
    )
    assert con.execute("SELECT * FROM live").fetchall() == [(20, 202)]


def test_continuation_seek_runs_over_tombstone_then_rbac_protected_rows():
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE src(name VARCHAR, tenant BIGINT, "__rowid__" BIGINT, '
        f'"{SOURCE_FILE_COL}" VARCHAR)'
    )
    con.execute(
        "INSERT INTO src VALUES "
        "('a', 1, 1, 'f'), "
        "('a', 1, 2, 'f'), "  # deleted, otherwise after the boundary
        "('a', 2, 3, 'f'), "  # unauthorized, otherwise after the boundary
        "('b', 1, 4, 'f'), "
        "('c', 2, 5, 'f')"    # unauthorized later order value
    )
    con.execute('CREATE TABLE dv("__file__" VARCHAR, "__rowid__" BIGINT)')
    con.execute("INSERT INTO dv VALUES ('f', 2)")
    create_tombstone_view(
        con,
        "src",
        "live",
        TombstoneDef(
            tombstone_path="validated-by-private-table",
            expected_rows=1,
            resource_keys=("f",),
            snapshot_resource_keys=("f",),
        ),
        dv_table="dv",
        preserve_rowid_as=ODATA_INTERNAL_ROWID_COLUMN,
        odata_rowid_high_watermark=5,
        odata_resource_keys=["f"],
        odata_resource_rows={"f": 5},
        odata_rowid_integrity_seals={"f": _rowid_seal(5, 1, 5)},
    )
    create_rbac_view(
        con,
        "live",
        "scoped",
        RbacViewDef(
            allowed_columns=["name", "tenant"],
            excluded_columns=["tenant"],
            where_clause='"tenant" = 1',
        ),
        required_internal_columns=[ODATA_INTERNAL_ROWID_COLUMN],
    )
    sql, parameters = _prepare_protected_odata_query(
        "SELECT name FROM scoped ORDER BY name ASC LIMIT 50",
        ODATA_INTERNAL_ROWID_COLUMN,
        _boundary([{
            "column": "name",
            "direction": "asc",
            "value": {"type": "string", "value": "a"},
        }], row_identity=1),
    )
    # The seek is evaluated over ``scoped``.  Neither the deleted id=2 nor
    # tenant-2 ids 3/5 can influence or enter the continuation result.
    assert con.execute(sql, parameters).fetchall() == [("b", 4)]


def test_protected_query_adds_identity_as_final_deterministic_tie_breaker():
    sql = _append_protected_odata_identity(
        'SELECT "name" FROM reflected ORDER BY "name" DESC LIMIT 3 OFFSET 2',
        ODATA_INTERNAL_ROWID_COLUMN,
    )
    assert (
        f'SELECT "name", "{ODATA_INTERNAL_ROWID_COLUMN}" FROM reflected '
        f'ORDER BY "name" DESC NULLS LAST, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" ASC NULLS LAST LIMIT 3 OFFSET 2'
        == sql
    )
    with pytest.raises(RuntimeError):
        _append_protected_odata_identity(
            "SELECT count(*) FROM reflected",
            ODATA_INTERNAL_ROWID_COLUMN,
        )
    with pytest.raises(RuntimeError):
        create_tombstone_view(
            duckdb.connect(),
            "missing",
            "bad",
            SimpleNamespace(tombstone_path=None),
            preserve_rowid_as="__supertable_attacker_column__",
        )


def test_odata_identity_sql_contract_accepts_only_one_direct_row_projection():
    parser = SQLParser(
        "lake",
        'SELECT "id" FROM "orders" WHERE "id" > 1 ORDER BY "id" LIMIT 5',
        "duckdb",
    )
    assert _odata_identity_binding(parser) == "orders"

    for sql in (
        "SELECT count(*) FROM orders",
        "SELECT o.id FROM orders o JOIN users u ON o.id = u.id",
        "SELECT id + 1 FROM orders",
        "SELECT id FROM lake",
    ):
        candidate = SQLParser("lake", sql, "duckdb")
        with pytest.raises(ValueError):
            _odata_identity_binding(candidate)


def _boundary(order, row_identity=2):
    return validate_odata_continuation_boundary({
        "version": 1,
        "order": order,
        "row_identity": row_identity,
    })


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {},
        {"version": True, "order": [], "row_identity": 1},
        {"version": 1, "order": (), "row_identity": 1},
        {"version": 1, "order": [], "row_identity": 0},
        {
            "version": 1,
            "order": [{
                "column": "name",
                "direction": "ascending",
                "value": {"type": "string", "value": "a"},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "int64", "value": "01"},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "float64", "value": 1},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "float64", "value": float("inf")},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "n",
                "direction": "asc",
                "value": {"type": "null", "value": None},
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [
                {
                    "column": "Name",
                    "direction": "asc",
                    "value": {"type": "null"},
                },
                {
                    "column": "name",
                    "direction": "desc",
                    "value": {"type": "null"},
                },
            ],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "created",
                "direction": "asc",
                "value": {
                    "type": "datetime", "value": "2026-08-24T12:00:00",
                },
            }],
            "row_identity": 1,
        },
        {
            "version": 1,
            "order": [{
                "column": "created",
                "direction": "asc",
                "value": {
                    "type": "timestamp",
                    "value": "2026-08-24T12:00:00+00:00",
                },
            }],
            "row_identity": 1,
        },
    ],
)
def test_odata_continuation_rejects_malformed_or_ambiguous_state(raw):
    if raw is None:
        assert validate_odata_continuation_boundary(raw) is None
    else:
        with pytest.raises(ValueError):
            validate_odata_continuation_boundary(raw)


def test_odata_continuation_typed_values_bind_without_text_interpolation():
    boundary = _boundary([
        {
            "column": "nullable",
            "direction": "asc",
            "value": {"type": "null"},
        },
        {
            "column": "local_time",
            "direction": "asc",
            "value": {
                "type": "timestamp", "value": "2026-08-24T12:34:56.123456",
            },
        },
        {
            "column": "instant",
            "direction": "desc",
            "value": {
                "type": "datetime", "value": "2026-08-24T12:34:56+00:00",
            },
        },
        {
            "column": "amount",
            "direction": "asc",
            "value": {"type": "decimal", "value": "1234.50"},
        },
        {
            "column": "uid",
            "direction": "asc",
            "value": {
                "type": "uuid",
                "value": "12345678-1234-5678-9234-567812345678",
            },
        },
        {
            "column": "payload",
            "direction": "desc",
            "value": {"type": "binary", "value": "AAEC/w=="},
        },
    ])
    assert boundary is not None
    values = [term.value.value for term in boundary.order]
    assert values[0] is None
    assert isinstance(values[1], duckdb.TimestampNanosecondValue)
    assert values[1].object == "2026-08-24T12:34:56.123456"
    assert isinstance(values[2], duckdb.TimestampTimeZoneValue)
    assert values[2].object == "2026-08-24T12:34:56+00:00"
    assert values[3:] == [
        Decimal("1234.50"),
        UUID("12345678-1234-5678-9234-567812345678"),
        b"\x00\x01\x02\xff",
    ]
    con = duckdb.connect()
    row = con.execute(
        "SELECT ?::TIMESTAMP, ?::TIMESTAMPTZ, ?::DECIMAL(10, 2), "
        "?::UUID, ?::BLOB",
        values[1:],
    ).fetchone()
    assert row[0] == datetime(2026, 8, 24, 12, 34, 56, 123456)
    assert row[1].astimezone(timezone.utc) == datetime(
        2026, 8, 24, 12, 34, 56, tzinfo=timezone.utc,
    )
    assert row[2:] == tuple(values[3:])


def test_odata_nanosecond_timestamp_boundary_binds_without_precision_loss():
    boundary = _boundary([{
        "column": "observed_at",
        "direction": "asc",
        "value": {
            "type": "timestamp",
            "value": "2023-07-22T04:26:40.123456789",
        },
    }], row_identity=2)
    assert boundary is not None
    parameter = boundary.order[0].value.value
    assert isinstance(parameter, duckdb.TimestampNanosecondValue)
    assert parameter.object == "2023-07-22T04:26:40.123456789"

    sql, parameters = _prepare_protected_odata_query(
        "SELECT observed_at FROM reflected ORDER BY observed_at ASC LIMIT 50",
        ODATA_INTERNAL_ROWID_COLUMN,
        boundary,
    )
    assert all(
        isinstance(item, duckdb.TimestampNanosecondValue)
        for item in parameters[:-1]
    )
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE reflected(observed_at TIMESTAMP_NS, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute(
        "INSERT INTO reflected VALUES "
        "(TIMESTAMP_NS '2023-07-22 04:26:40.123456788', 1), "
        "(TIMESTAMP_NS '2023-07-22 04:26:40.123456789', 2), "
        "(TIMESTAMP_NS '2023-07-22 04:26:40.123456790', 3)"
    )
    assert [
        row[-1] for row in con.execute(sql, parameters).fetchall()
    ] == [3]


@pytest.mark.parametrize(
    "value",
    [
        "2023-07-22T04:26:40.123456789+00:00",
        "2023-07-22T04:26:40.123456789Z",
        "2023-07-22T06:56:40.123456789+02:30",
    ],
)
def test_odata_nanosecond_timezone_boundaries_from_core_are_accepted(value):
    boundary = _boundary([{
        "column": "observed_at",
        "direction": "asc",
        "value": {"type": "datetime", "value": value},
    }])
    assert boundary is not None
    parameter = boundary.order[0].value.value
    assert isinstance(parameter, duckdb.TimestampTimeZoneValue)
    assert parameter.object == value


def test_odata_boundary_type_is_bound_to_pinned_order_column_type():
    parser = SQLParser(
        "lake",
        "SELECT score FROM events ORDER BY score LIMIT 1",
        "duckdb",
    )
    boundary = _boundary([{
        "column": "score",
        "direction": "asc",
        "value": {"type": "float64", "value": 1.5},
    }])
    assert boundary is not None

    rebound = bind_odata_continuation_boundary(
        parser._parsed,
        boundary,
        column_types={"score": "Float64"},
    )
    assert rebound is not None
    assert odata_float_order_columns(
        parser._parsed, {"score": "Double"},
    ) == ("score",)

    with pytest.raises(ValueError, match="does not match ORDER BY"):
        bind_odata_continuation_boundary(
            parser._parsed,
            boundary,
            column_types={"score": "Int64"},
        )
    with pytest.raises(ValueError, match="physical type"):
        bind_odata_continuation_boundary(
            parser._parsed,
            boundary,
            column_types={},
        )


@pytest.mark.parametrize("nonfinite", [float("nan"), float("inf"), -float("inf")])
def test_odata_float_order_guard_rejects_nonfinite_beyond_page_limit(nonfinite):
    sql = (
        "SELECT score FROM reflected WHERE tenant = 1 "
        "ORDER BY score ASC LIMIT 1"
    )
    guard = _protected_odata_nonfinite_guard_query(sql, ("score",))
    assert guard is not None
    # LIMIT is retained only as an existence bound; the original page LIMIT is
    # removed before validation, so a bad later key cannot evade page one.
    assert "tenant = 1" in guard
    assert "NOT ISFINITE(score)" in guard

    con = duckdb.connect()
    con.execute("CREATE TABLE reflected(score DOUBLE, tenant BIGINT)")
    con.execute(
        "INSERT INTO reflected VALUES (?, 1), (?, 1), (?, 1)",
        [1.0, 2.0, nonfinite],
    )
    assert con.execute(guard).fetchone() == (1,)

    # Non-finite values outside the user-filtered OData domain are irrelevant.
    con.execute("DELETE FROM reflected")
    con.execute(
        "INSERT INTO reflected VALUES (?, 1), (?, 2), (NULL, 1)",
        [1.0, nonfinite],
    )
    assert con.execute(guard).fetchone() is None


@pytest.mark.parametrize(
    "value",
    [
        "2023-07-22T04:26:40.1",
        "2023-07-22T04:26:40.1234567",
        "2023-07-22T04:26:40.12345678",
        "2023-02-29T04:26:40.123456789",
        "2023-07-22T24:00:00.123456789",
    ],
)
def test_odata_timestamp_requires_canonical_valid_core_isoformat(value):
    with pytest.raises(ValueError, match="temporal value"):
        _boundary([{
            "column": "observed_at",
            "direction": "asc",
            "value": {"type": "timestamp", "value": value},
        }])


@pytest.mark.parametrize(
    ("sql", "order", "row_identity", "expected_ids"),
    [
        (
            "SELECT name FROM reflected ORDER BY name ASC LIMIT 50",
            [{
                "column": "name", "direction": "asc",
                "value": {"type": "string", "value": "a"},
            }],
            2,
            [3, 4, 5, 6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name DESC LIMIT 50",
            [{
                "column": "name", "direction": "desc",
                "value": {"type": "string", "value": "b"},
            }],
            4,
            [1, 2, 3, 5, 6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name ASC LIMIT 50",
            [{
                "column": "name", "direction": "asc",
                "value": {"type": "null"},
            }],
            5,
            [6],
        ),
        (
            "SELECT name FROM reflected ORDER BY name DESC LIMIT 50",
            [{
                "column": "name", "direction": "desc",
                "value": {"type": "null"},
            }],
            5,
            [6],
        ),
    ],
)
def test_odata_seek_handles_ties_nulls_and_both_directions(
    sql, order, row_identity, expected_ids,
):
    prepared, parameters = _prepare_protected_odata_query(
        sql,
        ODATA_INTERNAL_ROWID_COLUMN,
        _boundary(order, row_identity=row_identity),
    )
    assert prepared.count("NULLS LAST") == len(order) + 1
    con = duckdb.connect()
    # Prove the query's explicit semantics do not inherit mutable connection
    # defaults from a prior tenant/query.
    con.execute("SET default_null_order='NULLS_FIRST'")
    con.execute(
        f'CREATE TABLE reflected(name VARCHAR, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute(
        "INSERT INTO reflected VALUES "
        "('a', 1), ('a', 2), ('a', 3), ('b', 4), (NULL, 5), (NULL, 6)"
    )
    rows = con.execute(prepared, parameters).fetchall()
    assert [row[-1] for row in rows] == expected_ids


def test_odata_seek_mixed_direction_tuple_uses_identity_only_for_exact_ties():
    boundary = _boundary([
        {
            "column": "name", "direction": "asc",
            "value": {"type": "string", "value": "a"},
        },
        {
            "column": "score", "direction": "desc",
            "value": {"type": "int64", "value": "2"},
        },
    ], row_identity=2)
    sql, parameters = _prepare_protected_odata_query(
        "SELECT name, score FROM reflected "
        "ORDER BY name ASC, score DESC LIMIT 50",
        ODATA_INTERNAL_ROWID_COLUMN,
        boundary,
    )
    assert "'a'" not in sql
    assert parameters == ["a", "a", 2, 2, 2]
    con = duckdb.connect()
    con.execute(
        f'CREATE TABLE reflected(name VARCHAR, score BIGINT, '
        f'"{ODATA_INTERNAL_ROWID_COLUMN}" BIGINT)'
    )
    con.execute(
        "INSERT INTO reflected VALUES "
        "('a', 3, 1), ('a', 2, 2), ('a', 2, 3), ('a', 1, 4), "
        "('b', 9, 5), (NULL, 9, 6)"
    )
    assert [row[-1] for row in con.execute(sql, parameters).fetchall()] == [
        3, 4, 5, 6,
    ]


def test_odata_boundary_must_exactly_match_reparsed_order_and_has_no_offset():
    parser = SQLParser(
        "lake",
        "SELECT name FROM orders ORDER BY name DESC LIMIT 5",
        "duckdb",
    )
    matching = _boundary([{
        "column": "NAME", "direction": "desc",
        "value": {"type": "string", "value": "a"},
    }])
    rebound = bind_odata_continuation_boundary(parser._parsed, matching)
    assert rebound is not None and rebound.order[0].column == "name"

    for boundary in (
        _boundary([]),
        _boundary([{
            "column": "other", "direction": "desc",
            "value": {"type": "string", "value": "a"},
        }]),
        _boundary([{
            "column": "name", "direction": "asc",
            "value": {"type": "string", "value": "a"},
        }]),
    ):
        with pytest.raises(ValueError, match="order"):
            bind_odata_continuation_boundary(parser._parsed, boundary)

    offset_parser = SQLParser(
        "lake",
        "SELECT name FROM orders ORDER BY name DESC LIMIT 5 OFFSET 1",
        "duckdb",
    )
    with pytest.raises(ValueError, match="OFFSET"):
        bind_odata_continuation_boundary(offset_parser._parsed, matching)

    for sql in (
        "SELECT name FROM orders ORDER BY lower(name)",
        "SELECT name FROM orders ORDER BY name NULLS FIRST",
        "SELECT name FROM orders ORDER BY name, name",
    ):
        with pytest.raises(ValueError):
            _odata_identity_binding(SQLParser("lake", sql, "duckdb"))


def test_ordinary_sql_stream_cannot_activate_continuation_or_hidden_identity():
    assert "continuation_boundary" not in inspect.signature(
        data_reader_module.query_sql_stream
    ).parameters
    assert "continuation_boundary" in inspect.signature(
        data_reader_module.query_odata_sql_stream
    ).parameters
    boundary = _boundary([])
    with pytest.raises(ValueError, match="trusted OData stream"):
        data_reader_module.query_sql_stream(
            organization="org",
            super_name="lake",
            sql="SELECT name FROM orders",
            engine=Engine.DUCKDB,
            role_name="reader",
            max_total_rows=10,
            timeout_sec=10,
            _odata_identity=True,
            _odata_continuation_boundary=boundary,
        )
    with pytest.raises(RuntimeError, match="present in user SQL"):
        _append_protected_odata_identity(
            f'SELECT "{ODATA_INTERNAL_ROWID_COLUMN}" FROM reflected',
            ODATA_INTERNAL_ROWID_COLUMN,
        )
