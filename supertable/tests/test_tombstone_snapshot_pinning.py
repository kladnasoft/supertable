"""Focused read-path tests for snapshot-pinned deletion-vectors."""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import fakeredis
import pandas as pd
import pytest

from supertable import redis_keys as RK
from supertable.data_classes import Reflection, SuperSnapshot, TableDefinition
from supertable.engine.engine_enum import Engine
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_interface import ObjectMetadata
from supertable.tombstone_manifest_v2 import (
    TombstoneManifestV2,
    TombstoneSegment,
)
from supertable.utils.snapshot import (
    collect_share_row_filters,
    complete_snapshot_payload,
)


_PINNED_DIGEST = "0" * 64


def _seal_mock_manifest(storage, manifest_key: str, body: bytes) -> None:
    metadata = ObjectMetadata(size=len(body), version="manifest-v1")
    storage.stat_object.side_effect = lambda key: (
        metadata
        if key == manifest_key
        else (_ for _ in ()).throw(KeyError(key))
    )

    def _read_range(key, offset, length, *, expected=None):
        assert key == manifest_key
        assert expected == metadata
        return body[offset:offset + length]

    storage.read_range.side_effect = _read_range


class _LocalLikeStorage:
    """Explicit local resolver, independent of the developer's cloud env."""

    @staticmethod
    def to_duckdb_path(key):
        return key


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"_row_filter": None},
        {"_row_filter": ""},
        {"_row_filter": " \t\n"},
    ],
)
def test_blank_share_policy_markers_are_canonical_unrestricted(document):
    """Catalog-compatible no-policy markers must agree across read paths."""
    assert collect_share_row_filters(document) == ()


@pytest.mark.parametrize("marker", [False, 0, [], {}])
def test_nonstring_share_policy_markers_fail_closed(marker):
    """Only the catalog's documented no-policy markers are unrestricted."""
    with pytest.raises(RuntimeError, match="policy metadata is invalid"):
        collect_share_row_filters({"_row_filter": marker})


def test_cached_snapshot_versions_are_bounded_to_lua_exact_increment_range():
    payload = {
        "snapshot_version": 2**53 - 1,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }
    assert complete_snapshot_payload(
        payload,
        expected_version=2**53 - 1,
        require_policy_marker=True,
    ) == payload

    beyond_lua_exact_increment = {**payload, "snapshot_version": 2**53}
    assert complete_snapshot_payload(
        beyond_lua_exact_increment,
        expected_version=2**53,
        require_policy_marker=True,
    ) is None


def test_estimator_pins_tombstone_from_path_only_snapshot(monkeypatch):
    """Legacy leaf publication must load DV metadata from the heavy JSON."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")

    heavy_snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": "tombstone/v4.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": _PINNED_DIGEST,
    }

    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/v4.json",
        "version": 4,
        "ts": 44,
        # Backward-compatible/path-only leaf: deliberately no payload.
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)

    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = heavy_snapshot
    monkeypatch.setattr(estimator_module, "SuperTable", lambda *a, **k: super_table)

    estimator = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )
    reflection = estimator.estimate()

    assert len(reflection.supers) == 1
    snapshot = reflection.supers[0]
    assert len(snapshot.files) == 1
    assert snapshot.files[0] == "data/v4.parquet"
    assert snapshot.resource_keys == ["data/v4.parquet"]
    assert snapshot.snapshot_path == "snapshots/v4.json"
    assert snapshot.tombstone_key == "tombstone/v4.parquet"
    assert snapshot.tombstone_rows == 1
    assert snapshot.tombstone_digest == _PINNED_DIGEST
    super_table.read_simple_table_snapshot.assert_called_once_with(
        "snapshots/v4.json"
    )


def test_estimator_reads_authoritative_pre_dv_snapshot(monkeypatch):
    """Historical heavy JSON may omit the entire deletion-vector state."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    heavy_snapshot = {
        "snapshot_version": 1,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v1.parquet", "file_size": 123, "rows": 2},
        ],
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/v1.json",
        "version": 1,
        "ts": 11,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = heavy_snapshot
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )

    reflection = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    snapshot = reflection.supers[0]
    assert snapshot.files == ["data/v1.parquet"]
    assert snapshot.tombstone_key is None
    assert snapshot.tombstone_rows == 0
    assert snapshot.tombstone_digest is None
    assert snapshot.tombstone_format is None


def test_estimator_pins_explicit_v2_format_with_manifest_pointer(monkeypatch):
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    snapshot = {
        "snapshot_version": 5,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v5.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": "org/s/tables/t/tombstone/manifest.json",
        "tombstone_rows": 1,
        "tombstone_digest": _PINNED_DIGEST,
        "tombstone_format": 2,
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/v5.json",
        "version": 5,
        "ts": 55,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = snapshot
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )

    reflection = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    pinned = reflection.supers[0]
    assert pinned.tombstone_key == snapshot["tombstone"]
    assert pinned.tombstone_rows == 1
    assert pinned.tombstone_digest == _PINNED_DIGEST
    assert pinned.tombstone_format == 2


def test_estimator_partial_leaf_falls_back_to_heavy_active_tombstone(monkeypatch):
    """A partial Redis cache may not erase the heavy snapshot's DV state."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    partial_payload = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        # A legacy/partial leaf omitted all tombstone fields.
    }
    heavy_snapshot = {
        **partial_payload,
        "tombstone": "tombstone/v4.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": _PINNED_DIGEST,
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t", "path": "snapshots/v4.json", "version": 4,
        "ts": 44, "payload": partial_payload,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = heavy_snapshot
    monkeypatch.setattr(estimator_module, "SuperTable", lambda *a, **k: super_table)

    reflection = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    pinned = reflection.supers[0]
    assert pinned.tombstone_key == "tombstone/v4.parquet"
    assert pinned.tombstone_rows == 1
    assert pinned.tombstone_digest == _PINNED_DIGEST
    super_table.read_simple_table_snapshot.assert_called_once_with(
        "snapshots/v4.json"
    )


def test_complete_cache_without_policy_marker_loads_authoritative_filter(monkeypatch):
    """A tombstone-complete legacy cache may still omit share authorization."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    cached = {
        "snapshot_version": 4,
        "schema": {"id": "Int64", "tenant_id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    heavy = {**cached, "_row_filter": "tenant_id = 7"}
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t", "path": "snapshots/v4.json", "version": 4,
        "ts": 44, "payload": cached,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = heavy
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )

    reflection = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    assert reflection.supers[0].share_row_filter == "tenant_id = 7"
    super_table.read_simple_table_snapshot.assert_called_once_with(
        "snapshots/v4.json"
    )


@pytest.mark.parametrize("policy_location", ["payload", "snapshot"])
def test_estimator_rejects_malformed_nonnull_share_policy(
    monkeypatch, policy_location,
):
    """Corrupt share metadata may never become an unrestricted snapshot."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }
    if policy_location == "payload":
        leaf_payload = {**snapshot, "_row_filter": {"invalid": True}}
        heavy_snapshot = snapshot
    else:
        # Force immutable resolution and put the malformed marker there.
        leaf_payload = {
            "snapshot_version": 4,
            "schema": {"id": "Int64"},
            "resources": snapshot["resources"],
        }
        heavy_snapshot = {**snapshot, "_row_filter": ["invalid"]}

    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t", "path": "snapshots/v4.json", "version": 4,
        "ts": 44, "payload": leaf_payload,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = heavy_snapshot
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )

    estimator = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )
    with pytest.raises(RuntimeError, match="policy metadata is invalid"):
        estimator.estimate()


@pytest.mark.parametrize(
    ("outer_filter", "expected_filter", "error"),
    [
        ("tenant_id = 7", "tenant_id = 7", None),
        ({"invalid": True}, None, "policy metadata is invalid"),
    ],
)
def test_outer_leaf_share_policy_survives_catalog_enumeration(
    monkeypatch, outer_filter, expected_filter, error,
):
    """The catalog must not erase an outer linked-share policy marker."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(
        RK.meta_root("org", "s"),
        json.dumps({"version": 1, "ts": 1}),
    )
    payload = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": None,
    }
    client.set(
        RK.meta_leaf("org", "s", "t"),
        json.dumps({
            "version": 4,
            "ts": 44,
            "path": "snapshots/v4.json",
            "payload": payload,
            "_row_filter": outer_filter,
        }),
    )
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: MagicMock(),
    )
    estimator = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )

    if error is not None:
        with pytest.raises(RuntimeError, match=error):
            estimator.estimate()
        return
    reflection = estimator.estimate()
    assert reflection.supers[0].share_row_filter == expected_filter


def test_conflicting_share_policy_wrappers_are_combined_fail_closed(monkeypatch):
    """No supported policy wrapper may silently replace another restriction."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    payload = {
        "snapshot_version": 4,
        "schema": {"id": "Int64", "tenant_id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": "tenant_id > 0",
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/v4.json",
        "version": 4,
        "ts": 44,
        "payload": payload,
        "_row_filter": "tenant_id < 10",
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: MagicMock(),
    )

    reflection = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    assert reflection.supers[0].share_row_filter == (
        "(tenant_id < 10) AND (tenant_id > 0)"
    )


def test_direct_and_nested_share_policy_wrappers_are_both_enforced(monkeypatch):
    """A complete direct cache must not hide its nested legacy restriction."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    nested = {
        "snapshot_version": 4,
        "schema": {"id": "Int64", "tenant_id": "Int64"},
        "resources": [
            {"file": "data/v4.parquet", "file_size": 123, "rows": 2},
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "_row_filter": "tenant_id < 10",
    }
    payload = {
        **nested,
        "_row_filter": "tenant_id > 0",
        "snapshot": nested,
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/v4.json",
        "version": 4,
        "ts": 44,
        "payload": payload,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: MagicMock(),
    )

    reflection = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    ).estimate()

    assert reflection.supers[0].share_row_filter == (
        "(tenant_id > 0) AND (tenant_id < 10)"
    )


def test_estimator_accepts_authoritative_zero_resource_snapshot(monkeypatch):
    """Metadata-only delete-all remains a readable typed empty table."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t",
        "path": "snapshots/empty.json",
        "version": 5,
        "ts": 55,
        "payload": {
            "snapshot_version": 5,
            "schema": {"id": "Int64", "name": "String"},
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "_row_filter": None,
        },
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: MagicMock(),
    )
    estimator = estimator_module.DataEstimator(
        "org",
        _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )

    reflection = estimator.estimate()

    assert reflection.total_reflections == 0
    assert reflection.reflection_bytes == 0
    assert len(reflection.supers) == 1
    snapshot = reflection.supers[0]
    assert snapshot.files == []
    assert snapshot.resource_keys == []
    assert snapshot.snapshot_resource_keys == []
    assert snapshot.column_types == {"id": "Int64", "name": "String"}


def test_estimator_rejects_active_pointer_with_zero_rows(monkeypatch):
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    bad_snapshot = {
        "snapshot_version": 1,
        "schema": {"id": "Int64"},
        "resources": [{"file": "data/f.parquet", "file_size": 1}],
        "tombstone": "tombstone/empty.parquet",
        "tombstone_rows": 0,
        "tombstone_digest": _PINNED_DIGEST,
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t", "path": "snapshots/bad.json", "version": 1, "ts": 1,
        "payload": bad_snapshot,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = bad_snapshot
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )
    estimator = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )

    with pytest.raises(RuntimeError, match="positive row count"):
        estimator.estimate()


@pytest.mark.parametrize("bad_count", ["1", True, -1, 1.0])
def test_estimator_rejects_malformed_pointerless_tombstone_count(
        monkeypatch, bad_count,
):
    """Pointerless metadata may not erase evidence of physical dead rows."""
    estimator_module = importlib.import_module("supertable.engine.data_estimator")
    bad_snapshot = {
        "snapshot_version": 1,
        "schema": {"id": "Int64"},
        "resources": [{"file": "data/f.parquet", "file_size": 1}],
        "tombstone": None,
        "tombstone_rows": bad_count,
        "tombstone_digest": None,
    }
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = [{
        "simple": "t", "path": "snapshots/bad.json", "version": 1, "ts": 1,
        "payload": bad_snapshot,
    }]
    monkeypatch.setattr(estimator_module, "RedisCatalog", lambda: catalog)
    super_table = MagicMock()
    super_table.read_simple_table_snapshot.return_value = bad_snapshot
    monkeypatch.setattr(
        estimator_module, "SuperTable", lambda *a, **k: super_table,
    )
    estimator = estimator_module.DataEstimator(
        "org", _LocalLikeStorage(),
        [TableDefinition("s", "t", "t", columns=["id"])],
    )

    with pytest.raises(RuntimeError, match="Invalid tombstone row count"):
        estimator.estimate()


def _install_reader_fakes(monkeypatch, reflection, *, resolver):
    reader_module = importlib.import_module("supertable.data_reader")

    td = TableDefinition("s", "t", "t", columns=[])
    parser = MagicMock()
    parser.get_table_tuples.return_value = [td]
    parser.get_physical_tables.return_value = [td]
    parser.get_predicate_constraints.return_value = {}
    parser.get_join_edges.return_value = []
    parser.original_query = "SELECT * FROM t"
    monkeypatch.setattr(reader_module, "SQLParser", lambda *a, **k: parser)

    catalog = MagicMock()
    catalog.root_exists.return_value = True
    catalog.leaf_exists.return_value = True
    # If the obsolete post-estimate lookup returns a newer pointer, the exact
    # S0/S1 hybrid that loses an upserted row would be constructed.
    catalog.get_leaf.return_value = {
        "payload": {"tombstone": "tombstone/v2.parquet"},
    }
    monkeypatch.setattr(reader_module, "RedisCatalog", lambda: catalog)

    estimator = MagicMock()
    estimator.estimate.return_value = reflection
    estimator._to_duckdb_path.side_effect = resolver
    monkeypatch.setattr(reader_module, "DataEstimator", lambda *a, **k: estimator)

    executor = MagicMock()
    executor.execute.return_value = (pd.DataFrame({"id": [1]}), "duckdb")
    monkeypatch.setattr(reader_module, "Executor", lambda *a, **k: executor)
    monkeypatch.setattr(reader_module, "get_storage", lambda: MagicMock())
    monkeypatch.setattr(reader_module, "restrict_read_access", lambda **k: {})
    monkeypatch.setattr(
        reader_module,
        "QueryPlanManager",
        lambda **k: SimpleNamespace(
            query_id="q", query_hash="h", original_table="", source_type="",
            temp_dir="/tmp", query_plan_path="/tmp/plan.json",
        ),
    )
    monkeypatch.setattr(reader_module, "Timer", lambda: MagicMock(timings=[]))
    monkeypatch.setattr(reader_module, "PlanStats", MagicMock)
    monkeypatch.setattr(reader_module, "extend_execution_plan", lambda **k: None)
    return reader_module, catalog, estimator, executor


def test_reader_uses_coherent_pinned_pointer_without_second_leaf_lookup(monkeypatch):
    """A concurrent S1 commit cannot attach S1's DV to S0's files."""
    pinned_key = "tombstone/v1.parquet"
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 1, ["data/v1.parquet"], {"id"},
            snapshot_path="snapshots/v1.json",
            tombstone_key=pinned_key,
            tombstone_rows=1,
            tombstone_digest=_PINNED_DIGEST,
            resource_keys=["data/v1.parquet"],
        )],
    )
    reader_module, catalog, estimator, executor = _install_reader_fakes(
        monkeypatch,
        reflection,
        resolver=lambda key: f"signed://{key}",
    )

    result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.OK
    assert message is None
    assert result["id"].tolist() == [1]
    catalog.get_leaf.assert_not_called()
    estimator._to_duckdb_path.assert_called_once_with(pinned_key)
    wired = executor.execute.call_args.kwargs["reflection"].tombstone_views["t"]
    assert wired.cache_key == pinned_key
    assert wired.tombstone_path == f"signed://{pinned_key}"
    assert wired.expected_rows == 1
    assert wired.tombstone_digest == _PINNED_DIGEST
    assert wired.resource_keys == ("data/v1.parquet",)


def test_reader_loads_v2_manifest_once_for_self_join_and_resolves_segments(
        monkeypatch,
):
    segment_key = "org/s/tables/t/tombstone/segment-a.parquet"
    manifest_key = "org/s/tables/t/tombstone/manifest.json"
    manifest = TombstoneManifestV2(
        organization="org",
        super_name="s",
        simple_name="t",
        base_snapshot_version=4,
        snapshot_version=5,
        total_rows=1,
        segments=(TombstoneSegment(
            file=segment_key,
            rows=1,
            file_size=123,
            digest="1" * 64,
        ),),
    )
    body = manifest.canonical_bytes()
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 5, ["data/v5.parquet"], {"id"},
            snapshot_path="snapshots/v5.json",
            tombstone_key=manifest_key,
            tombstone_rows=1,
            tombstone_digest=manifest.digest(),
            resource_keys=["data/v5.parquet"],
            snapshot_resource_keys=["data/v5.parquet"],
            tombstone_format=2,
        )],
    )
    reader_module, catalog, estimator, executor = _install_reader_fakes(
        monkeypatch,
        reflection,
        resolver=lambda key: f"/resolved/{key.rsplit('/', 1)[-1]}",
    )

    class _SelfJoinParser:
        original_query = "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id"

        def __init__(self, *args, **kwargs):
            self._tables = [
                TableDefinition("s", "t", "a", columns=["id"]),
                TableDefinition("s", "t", "b", columns=["id"]),
            ]

        def get_table_tuples(self):
            return list(self._tables)

        def get_physical_tables(self):
            return list(self._tables)

        @staticmethod
        def get_predicate_constraints():
            return {}

        @staticmethod
        def get_join_edges():
            return []

    monkeypatch.setattr(reader_module, "SQLParser", _SelfJoinParser)
    reader = reader_module.DataReader(
        "s",
        "org",
        "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id",
    )
    reader.storage.size.side_effect = {
        manifest_key: len(body),
        segment_key: 123,
    }.__getitem__
    _seal_mock_manifest(reader.storage, manifest_key, body)

    result, status, message = reader.execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.OK
    assert message is None
    assert result["id"].tolist() == [1]
    reader.storage.read_bytes.assert_not_called()
    reader.storage.read_range.assert_called_once()
    assert reader.storage.stat_object.call_count == 2
    reader.storage.size.assert_called_once_with(segment_key)
    assert [item.args for item in estimator._to_duckdb_path.call_args_list] == [
        (manifest_key,),
        (segment_key,),
    ]
    views = executor.execute.call_args.kwargs["reflection"].tombstone_views
    assert set(views) == {"a", "b"}
    for definition in views.values():
        assert definition.tombstone_path == "/resolved/manifest.json"
        assert definition.cache_key == manifest_key
        assert definition.tombstone_format == 2
        assert len(definition.segments) == 1
        assert definition.segments[0].cache_key == segment_key
        assert definition.segments[0].tombstone_path == "/resolved/segment-a.parquet"
    catalog.get_leaf.assert_not_called()


@pytest.mark.parametrize("failure", ["size", "resolution"])
def test_reader_aborts_when_any_v2_segment_cannot_be_proved(
        monkeypatch, failure,
):
    segment_key = "org/s/tables/t/tombstone/segment-a.parquet"
    manifest_key = "org/s/tables/t/tombstone/manifest.json"
    manifest = TombstoneManifestV2(
        organization="org",
        super_name="s",
        simple_name="t",
        base_snapshot_version=4,
        snapshot_version=5,
        total_rows=1,
        segments=(TombstoneSegment(
            file=segment_key,
            rows=1,
            file_size=123,
            digest="1" * 64,
        ),),
    )
    body = manifest.canonical_bytes()
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 5, ["data/v5.parquet"], {"id"},
            tombstone_key=manifest_key,
            tombstone_rows=1,
            tombstone_digest=manifest.digest(),
            resource_keys=["data/v5.parquet"],
            snapshot_resource_keys=["data/v5.parquet"],
            tombstone_format=2,
        )],
    )

    def _resolve(key):
        if failure == "resolution" and key == segment_key:
            raise OSError("segment resolver unavailable")
        return f"/resolved/{key.rsplit('/', 1)[-1]}"

    reader_module, _catalog, _estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=_resolve,
    )
    reader = reader_module.DataReader("s", "org", "SELECT * FROM t")
    reader.storage.size.side_effect = {
        manifest_key: len(body),
        segment_key: (122 if failure == "size" else 123),
    }.__getitem__
    _seal_mock_manifest(reader.storage, manifest_key, body)

    result, status, message = reader.execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.ERROR
    assert result.empty
    if failure == "size":
        assert "segment size does not match" in message
    else:
        assert "Unable to resolve required deletion-vector" in message
    executor.execute.assert_not_called()


def test_reader_fails_closed_when_pinned_tombstone_resolution_fails(monkeypatch):
    reflection = Reflection(
        storage_type="object",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 8, ["data/v8.parquet"], {"id"},
            tombstone_key="tombstone/v8.parquet",
            tombstone_rows=3,
            tombstone_digest=_PINNED_DIGEST,
        )],
    )

    def _fail(_key):
        raise OSError("presigner unavailable")

    reader_module, catalog, _estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=_fail,
    )
    result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.ERROR
    assert result.empty
    assert "Unable to resolve required deletion-vector" in message
    executor.execute.assert_not_called()
    catalog.get_leaf.assert_not_called()


def test_object_store_bare_key_fallback_is_not_accepted_as_resolved(monkeypatch):
    reflection = Reflection(
        storage_type="MinioStorage",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 8, ["s3://bucket/data/v8.parquet"], {"id"},
            tombstone_key="tombstone/v8.parquet",
            tombstone_rows=3,
            tombstone_digest=_PINNED_DIGEST,
        )],
    )
    reader_module, _catalog, _estimator, executor = _install_reader_fakes(
        monkeypatch,
        reflection,
        # Models the resolver's final fallback after presigning/backend URL
        # construction failed: it simply hands the bare object key back.
        resolver=lambda key: key,
    )

    result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.ERROR
    assert result.empty
    assert "Unable to resolve required deletion-vector" in message
    executor.execute.assert_not_called()


def test_snapshot_without_tombstone_remains_backward_compatible(monkeypatch):
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, ["data/v1.parquet"], {"id"})],
    )
    reader_module, catalog, estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=lambda key: key,
    )

    _result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.OK
    assert message is None
    estimator._to_duckdb_path.assert_not_called()
    executor.execute.assert_called_once()
    catalog.get_leaf.assert_not_called()


def test_reader_accepts_exact_empty_v2_without_synthesizing_tombstone(monkeypatch):
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=10,
        total_reflections=1,
        supers=[SuperSnapshot(
            "s",
            "t",
            5,
            ["data/v5.parquet"],
            {"id"},
            tombstone_key=None,
            tombstone_rows=0,
            tombstone_digest=None,
            tombstone_format=2,
        )],
    )
    reader_module, _catalog, estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=lambda key: key,
    )

    _result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.OK
    assert message is None
    estimator._to_duckdb_path.assert_not_called()
    wired = executor.execute.call_args.kwargs["reflection"]
    assert wired.tombstone_views == {}


def test_reader_executes_authoritative_zero_resource_reflection(monkeypatch):
    reflection = Reflection(
        storage_type="local",
        reflection_bytes=0,
        total_reflections=0,
        supers=[SuperSnapshot(
            "s", "t", 5, [], {"id"}, [],
            column_types={"id": "Int64"},
            snapshot_resource_keys=[],
            snapshot_path="snapshots/empty.json",
        )],
    )
    reader_module, catalog, estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=lambda key: key,
    )
    executor.execute.return_value = (
        pd.DataFrame({"id": pd.Series(dtype="int64")}), "duckdb",
    )

    result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.OK
    assert message is None
    assert result.empty
    estimator._to_duckdb_path.assert_not_called()
    executor.execute.assert_called_once()
    catalog.get_leaf.assert_not_called()


def test_reader_rejects_pinned_pointer_with_zero_rows(monkeypatch):
    reflection = Reflection(
        storage_type="local", reflection_bytes=1, total_reflections=1,
        supers=[SuperSnapshot(
            "s", "t", 1, ["data/f.parquet"], {"id"}, ["data/f.parquet"],
            tombstone_key="tombstone/empty.parquet",
            tombstone_rows=0,
            tombstone_digest=_PINNED_DIGEST,
        )],
    )
    reader_module, _catalog, estimator, executor = _install_reader_fakes(
        monkeypatch, reflection, resolver=lambda key: key,
    )

    result, status, message = reader_module.DataReader(
        "s", "org", "SELECT * FROM t"
    ).execute("admin", engine=Engine.DUCKDB)

    assert status is reader_module.Status.ERROR
    assert result.empty
    assert "positive row count" in message
    estimator._to_duckdb_path.assert_not_called()
    executor.execute.assert_not_called()
