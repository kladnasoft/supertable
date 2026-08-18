"""Focused read-path tests for snapshot-pinned deletion-vectors."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from supertable.data_classes import Reflection, SuperSnapshot, TableDefinition
from supertable.engine.engine_enum import Engine


_PINNED_DIGEST = "0" * 64


class _LocalLikeStorage:
    """No URL helpers: estimator leaves local/bare paths unchanged."""


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
