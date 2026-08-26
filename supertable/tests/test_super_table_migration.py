import copy
import hashlib
import json
import os
from types import SimpleNamespace
from unittest.mock import Mock, patch

import fakeredis
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import redis

import supertable.super_table as super_table_module
import supertable.processing as processing_module
from supertable import redis_keys as RK
from supertable.errors import LockLostError
from supertable.redis_catalog import ReadOnlyCatalogError, RedisCatalog
from supertable.processing import (
    ROWID_COL,
    TOMBSTONE_FILE_COL,
    TOMBSTONE_SCHEMA,
    persist_tombstone_manifest_v2,
    persist_tombstone_segment_v2,
    persist_tombstone_v3_frame,
    stats_seal_for_metadata,
    tombstone_digest,
)
from supertable.row_identity import snapshot_proves_stable_rowids
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_table import SuperTable
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V2,
    TOMBSTONE_FORMAT_V3,
    load_tombstone_manifest_v2,
)


def _configure_mock_snapshot_reads(storage, payload_for_path):
    def encoded(path):
        return json.dumps(
            payload_for_path(path), separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")

    def stat_object(path):
        payload = encoded(path)
        return ObjectMetadata(
            size=len(payload),
            etag=hashlib.sha256(payload).hexdigest(),
        )

    def read_range(path, start, length, *, expected=None):
        payload = encoded(path)
        assert expected == stat_object(path)
        return payload[start:start + length]

    storage.stat_object.side_effect = stat_object
    storage.read_range.side_effect = read_range


def test_migrate_legacy_metadata_rebuilds_stats_and_publishes_successor(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/a.parquet"
    storage.write_parquet(pa.table({"id": [1, 2]}), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [{"file": data_path}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    assert result["migrated_tables"] == ["facts"]
    leaf = catalog.get_leaf("org", "lake", "facts")
    root = catalog.get_root("org", "lake")
    assert leaf is not None
    published = leaf["payload"]
    assert published["tombstone_digest"] is None
    assert published["stats_file"] is not None
    assert storage.exists(published["stats_file"])
    assert published["stats_rows"] > 0
    assert published["previous_snapshot"].endswith("old.json")
    assert published["last_updated_ms"] == leaf["ts"]
    assert leaf["version"] == 5
    assert root is not None and root["version"] == 10
    assert client.get(RK.lock_namespace("org", "lake")) is None
    assert client.get(RK.lock_leaf("org", "lake", "facts")) is None


def test_migration_rejects_stats_that_cannot_pass_its_readback_bound(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/a.parquet"
    storage.write_parquet(pa.table({"id": [1, 2]}), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {"id": "Int64"},
        "resources": [{"file": data_path}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    monkeypatch.setattr(
        processing_module,
        "MAX_SHOW_STATS_DECODED_BYTES",
        256,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="decoded data"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migrate_legacy_metadata_updates_existing_leaf_with_real_catalog():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9, "ts": 1, "read_only": False,
    }))
    client.set(RK.meta_leaf("org", "lake", "facts"), json.dumps({
        "version": 4,
        "ts": 1,
        "path": old_path,
        "payload": snapshot,
    }))

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)):
        result = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    root = catalog.get_root("org", "lake")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None
    assert leaf["version"] == 5
    assert leaf["path"] != old_path
    assert leaf["payload"]["previous_snapshot"] == old_path
    assert leaf["payload"]["snapshot_version"] == 5
    assert root is not None and root["version"] == 10
    assert client.get(RK.lock_namespace("org", "lake")) is None
    assert client.get(RK.lock_leaf("org", "lake", "facts")) is None


def test_migrate_legacy_metadata_is_idempotent_after_version_marker():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda path: (
        snapshot
        if path == old_path
        else catalog.get_leaf("org", "lake", "facts")["payload"]
    ))
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)):
        first = st.migrate_legacy_metadata()
        second = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert first["migrated_tables"] == ["facts"]
    assert second["migrated_tables"] == []
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["_legacy_metadata_migration_version"] == 1
    assert st.storage.write_json.call_count == 1


def test_migration_marker_does_not_bypass_snapshot_shape_validation():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "snapshot_version": 4,
        "_legacy_metadata_migration_version": 1,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "stats_file": None,
        "stats_rows": 0,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(ValueError, match="lacks required current fields"):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_skips_already_current_unmarked_snapshot():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == []
    assert leaf is not None and leaf["version"] == 4
    assert leaf["path"] == old_path
    st.storage.write_json.assert_not_called()


def test_migration_does_not_trust_current_field_names_without_valid_seals(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    data_path = "org/lake/tables/facts/data/current.parquet"
    storage.write_parquet(pa.table({"value": [1]}), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {"value": "Int64"},
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 99,
            "columns": 1,
            "footer_sha256": "0" * 64,
            "stats_rows": 0,
            "stats_digest": "0" * 64,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 99,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="rows disagrees with Parquet"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_migration_rebuilds_current_schema_that_disagrees_with_footer(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    data_path = "org/lake/tables/facts/data/current.parquet"
    storage.write_parquet(pa.table({"value": ["text"]}), data_path)
    object_metadata = storage.stat_object(data_path)
    footer = pq.read_metadata(tmp_path / data_path)
    stats_seal = stats_seal_for_metadata(data_path, footer)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {"value": "Int64"},
        "resources": [{
            "file": data_path,
            "rows": 1,
            "file_size": object_metadata.size,
            "columns": 1,
            "object_seal": {
                "size": object_metadata.size,
                "version": object_metadata.version,
                "etag": object_metadata.etag,
                "last_modified_ns": object_metadata.last_modified_ns,
                "checksum_sha256": object_metadata.checksum_sha256,
            },
            "footer_sha256": stats_seal.footer_sha256,
            "stats_rows": stats_seal.stats_rows,
            "stats_digest": stats_seal.stats_digest,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
        "_legacy_metadata_migration_version": 1,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None
    assert leaf["payload"]["schema"] == {"value": "String"}


def test_migration_rebuilds_missing_current_statistics_artifact(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "stats_file": "org/lake/tables/facts/stats/missing.parquet",
        "stats_rows": 1,
        "_row_filter": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None
    assert leaf["payload"]["stats_file"] is None
    assert leaf["payload"]["stats_rows"] == 0
    assert leaf["payload"]["rowid_high_watermark"] == 0
    assert snapshot_proves_stable_rowids(leaf["payload"]) is True


def test_migration_bounds_columns_for_zero_row_group_resources(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/wide-empty.parquet"
    columns = [f"column_{index}" for index in range(100)]
    physical_path = tmp_path / data_path
    physical_path.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(
        physical_path,
        pa.schema([pa.field(name, pa.int64()) for name in columns]),
    ):
        pass
    assert pq.read_metadata(tmp_path / data_path).num_row_groups == 0
    snapshot = {
        "snapshot_version": 4,
        "schema": {name: "Int64" for name in columns},
        "resources": [{"file": data_path}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_COLUMN_CHUNKS",
        50,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="footer fan-out"):
        st.migrate_legacy_metadata()


def test_migration_marker_in_redis_cache_cannot_bypass_sealed_snapshot_read():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    cached = {"_legacy_metadata_migration_version": 1}
    _seed_migration_catalog(catalog, client, cached, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.storage.stat_object.side_effect = RuntimeError("sealed snapshot unavailable")
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="sealed snapshot unavailable"):
        st.migrate_legacy_metadata()

    st.storage.stat_object.assert_called_once_with(old_path)
    st.storage.write_json.assert_not_called()


def test_migration_rejects_complete_redis_cache_that_differs_from_storage():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    cached = copy.deepcopy(snapshot)
    cached["schema"] = {"forged": "Int64"}
    _seed_migration_catalog(catalog, client, cached, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="cache disagrees with storage"):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_accepts_only_unavoidable_redis_lua_cache_rounding():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    rowid_floor = (1 << 53) + 1
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": rowid_floor,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    cached = copy.deepcopy(snapshot)
    cached["schema"] = []
    cached["rowid_high_watermark"] = int(float(rowid_floor))
    _seed_migration_catalog(catalog, client, cached, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    assert result["migrated_tables"] == []
    st.storage.write_json.assert_not_called()


def test_migration_rejects_current_snapshot_version_that_disagrees_with_leaf():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {"snapshot_version": 3, "schema": [], "resources": []}
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(ValueError, match="snapshot_version disagrees"):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_rejects_malformed_current_tombstone_state():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 5,
        "tombstone_digest": "not-a-digest",
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(ValueError):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_validates_active_tombstone_before_skipping_current_snapshot():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": "org/lake/tables/facts/tombstone/current.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": "0" * 64,
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(ValueError, match="row count exceeds"):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_cannot_publish_after_namespace_lease_is_lost():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.storage.write_json.side_effect = lambda *_args, **_kwargs: client.set(
        RK.lock_namespace("org", "lake"), "replacement-owner", ex=60,
    )
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)), \
         pytest.raises(LockLostError, match="namespace fencing lock"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4
    assert leaf["path"] == old_path


def test_current_migration_rechecks_namespace_lease_after_validation(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog
    original_validate = st._validate_legacy_resources

    def validate_then_lose_lease(*args, **kwargs):
        result = original_validate(*args, **kwargs)
        client.set(
            RK.lock_namespace("org", "lake"),
            "replacement-owner",
            ex=60,
        )
        return result

    monkeypatch.setattr(
        st,
        "_validate_legacy_resources",
        validate_then_lose_lease,
    )

    with pytest.raises(LockLostError, match="namespace"):
        st.migrate_legacy_metadata()

    st.storage.write_json.assert_not_called()


def test_migration_reconciles_disconnect_after_atomic_commit(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    original_commit = catalog.commit_snapshot

    def commit_then_disconnect(*args, **kwargs):
        original_commit(*args, **kwargs)
        raise redis.ConnectionError("response was lost after commit")

    monkeypatch.setattr(catalog, "commit_snapshot", commit_then_disconnect)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)):
        result = st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["_legacy_metadata_migration_version"] == 1


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("version", 6),
        ("path", "org/lake/tables/facts/snapshots/wrong.json"),
        ("commit_id", "wrong-commit"),
    ],
)
def test_migration_does_not_reconcile_ambiguous_commit_with_wrong_identity(
    field,
    wrong_value,
    monkeypatch,
):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    original_commit = catalog.commit_snapshot

    def commit_then_change_identity(*args, **kwargs):
        original_commit(*args, **kwargs)
        leaf_key = RK.meta_leaf("org", "lake", "facts")
        committed = json.loads(client.get(leaf_key))
        committed[field] = wrong_value
        client.set(leaf_key, json.dumps(committed))
        raise redis.ConnectionError("response was lost after commit")

    monkeypatch.setattr(
        catalog,
        "commit_snapshot",
        commit_then_change_identity,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with patch(
        "supertable.processing.extract_stats_rows",
        return_value=pl.DataFrame(),
    ), patch(
        "supertable.processing.build_stats_file",
        return_value=(None, None),
    ), pytest.raises(redis.ConnectionError, match="response was lost"):
        st.migrate_legacy_metadata()


def test_migration_rejects_oversized_snapshot_before_reading_it():
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = Mock()
    old_path = "org/lake/tables/facts/snapshots/old.json"
    leaf = {"simple": "facts", "path": old_path, "version": 4}
    st.catalog.acquire_namespace_lock.return_value = "namespace-lock"
    st.catalog.acquire_simple_lock.return_value = "simple-lock"
    st.catalog.get_root.return_value = {"version": 1, "read_only": False}
    st.catalog.get_mirrors.return_value = []
    st.catalog.scan_leaf_items.return_value = iter([leaf])
    st.catalog.begin_table_mutation.return_value = {
        "leaf": leaf,
        "mirrors": [],
        "mirror_pin": None,
    }
    st.storage.stat_object.return_value = ObjectMetadata(
        size=8 * 1024 * 1024 + 1,
        etag="oversized",
    )

    with pytest.raises(RuntimeError, match="invalid size or identity"):
        st.migrate_legacy_metadata()

    st.storage.read_range.assert_not_called()
    st.storage.write_json.assert_not_called()
    st.catalog.commit_snapshot.assert_not_called()


def test_migration_deduplicates_scan_results_and_requests_bounded_batches():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "rowid_high_watermark": 0,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    scanned_leaf = {
        "simple": "facts",
        "path": old_path,
        "version": 4,
        "payload": snapshot,
    }
    catalog.scan_leaf_items = Mock(
        return_value=iter([scanned_leaf, scanned_leaf]),
    )
    original_begin = catalog.begin_table_mutation
    catalog.begin_table_mutation = Mock(wraps=original_begin)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    result = st.migrate_legacy_metadata()

    assert result["migrated_tables"] == []
    catalog.scan_leaf_items.assert_called_once_with(
        "org",
        "lake",
        count=1000,
        batch_size=1,
        max_scan_calls=1000,
    )
    assert catalog.begin_table_mutation.call_count == 2


def test_migration_rejects_unbounded_table_index_before_storage_io(monkeypatch):
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = Mock()
    st.catalog.acquire_namespace_lock.return_value = "namespace-lock"
    st.catalog.get_root.return_value = {"version": 1, "read_only": False}
    st.catalog.get_mirrors.return_value = []
    st.catalog.scan_leaf_items.return_value = iter([
        {
            "simple": f"table-{index}",
            "path": f"org/lake/tables/table-{index}/snapshots/current.json",
            "version": 1,
        }
        for index in range(3)
    ])
    monkeypatch.setattr(super_table_module, "_MAX_MIGRATION_TABLES", 2)

    with pytest.raises(ValueError, match="table-index safety bound"):
        st.migrate_legacy_metadata()

    st.catalog.scan_leaf_items.assert_called_once_with(
        "org",
        "lake",
        count=1000,
        batch_size=1,
        max_scan_calls=1000,
    )
    st.catalog.acquire_simple_lock.assert_not_called()
    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def test_catalog_leaf_scan_separates_scan_pages_from_payload_batches(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    _seed_migration_catalog(
        catalog,
        client,
        snapshot,
        "org/lake/tables/facts/snapshots/current.json",
    )
    client.set(RK.meta_leaf("org", "lake", "other"), json.dumps({
        "version": 2,
        "ts": 1,
        "path": "org/lake/tables/other/snapshots/current.json",
        "payload": snapshot,
    }))
    scan_options = []
    fetch_batches = []
    original_scan = catalog._scan_leaf_keys_raw
    original_fetch = catalog._fetch_batch

    def tracked_scan(*args, **kwargs):
        scan_options.append(kwargs)
        yield from original_scan(*args, **kwargs)

    def tracked_fetch(keys):
        fetch_batches.append(tuple(keys))
        yield from original_fetch(keys)

    monkeypatch.setattr(catalog, "_scan_leaf_keys_raw", tracked_scan)
    monkeypatch.setattr(catalog, "_fetch_batch", tracked_fetch)

    items = list(catalog.scan_leaf_items(
        "org",
        "lake",
        count=512,
        batch_size=1,
        max_scan_calls=7,
    ))

    assert {item["simple"] for item in items} == {"facts", "other"}
    assert scan_options == [{
        "allowed": None,
        "count": 512,
        "max_scan_calls": 7,
    }]
    assert len(fetch_batches) == 2
    assert all(len(batch) == 1 for batch in fetch_batches)


def test_catalog_leaf_scan_fails_closed_at_scan_call_bound(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    scan = Mock(return_value=(1, []))
    monkeypatch.setattr(client, "scan", scan)

    with pytest.raises(RuntimeError, match="SCAN exceeded its call safety bound"):
        list(catalog._scan_leaf_keys_raw(
            "org",
            "lake",
            allowed=None,
            count=512,
            max_scan_calls=2,
        ))

    assert scan.call_count == 2


def test_migration_rejects_mirrors_before_reading_or_writing_storage():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {"snapshot_version": 4, "schema": [], "resources": []}
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    catalog.set_mirrors("org", "lake", ["PARQUET"], now_ms=2)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="mirror-enabled"):
        st.migrate_legacy_metadata()

    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def test_migration_rejects_mirror_enabled_after_namespace_preflight(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {"snapshot_version": 4, "schema": [], "resources": []}
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    original_begin = catalog.begin_table_mutation

    def enable_before_pin(*args, **kwargs):
        catalog.set_mirrors("org", "lake", ["PARQUET"], now_ms=2)
        return original_begin(*args, **kwargs)

    monkeypatch.setattr(catalog, "begin_table_mutation", enable_before_pin)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="mirror-enabled"):
        st.migrate_legacy_metadata()

    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def test_migration_rejects_read_only_namespace_before_storage_io():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    snapshot = {"snapshot_version": 4, "schema": [], "resources": []}
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    root = json.loads(client.get(RK.meta_root("org", "lake")))
    root["read_only"] = True
    client.set(RK.meta_root("org", "lake"), json.dumps(root))

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = catalog

    with pytest.raises(ReadOnlyCatalogError):
        st.migrate_legacy_metadata()

    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def _seed_migration_catalog(catalog, client, snapshot, old_path):
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9, "ts": 1, "read_only": False,
    }))
    client.set(RK.meta_leaf("org", "lake", "facts"), json.dumps({
        "version": 4,
        "ts": 1,
        "path": old_path,
        "payload": snapshot,
    }))


def test_migration_rebuilds_exact_resource_seals_from_footer(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
    }), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [{"file": data_path}],
        "rowid_high_watermark": 11,
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = storage.read_json(leaf["path"])
    resource = migrated["resources"][0]
    assert resource["rows"] == 2
    assert resource["file_size"] == storage.size(data_path)
    assert resource["columns"] == 2
    observed = storage.stat_object(data_path)
    assert resource["object_seal"] == {
        "size": observed.size,
        "version": observed.version,
        "etag": observed.etag,
        "last_modified_ns": observed.last_modified_ns,
        "checksum_sha256": observed.checksum_sha256,
    }
    expected_stats = stats_seal_for_metadata(
        data_path,
        pq.read_metadata(tmp_path / data_path),
    )
    assert resource["footer_sha256"] == expected_stats.footer_sha256
    assert resource["stats_rows"] == expected_stats.stats_rows
    assert resource["stats_digest"] == expected_stats.stats_digest
    assert "rowid_high_watermark" not in migrated
    assert snapshot_proves_stable_rowids(migrated) is False


def test_migration_rejects_resource_metadata_that_disagrees_with_parquet(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
    }), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [{"file": data_path, "rows": 99}],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    with pytest.raises(RuntimeError, match="rows disagrees with Parquet"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_migration_republishes_valid_v2_manifest_for_successor(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
    }), data_path)
    tombstone_dir = "org/lake/tables/facts/tombstone"
    deletion_frame = pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_path],
        ROWID_COL: [1],
    }, schema=TOMBSTONE_SCHEMA)
    segment = persist_tombstone_segment_v2(
        tombstone_dir,
        deletion_frame,
        3,
        storage=storage,
    )
    manifest_path, manifest = persist_tombstone_manifest_v2(
        tombstone_dir,
        organization="org",
        super_name="lake",
        simple_name="facts",
        base_snapshot_version=3,
        snapshot_version=4,
        segments=(segment,),
        storage=storage,
    )
    snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [{"file": data_path}],
        "tombstone": manifest_path,
        "tombstone_rows": 1,
        "tombstone_digest": manifest.digest(),
        "tombstone_format": TOMBSTONE_FORMAT_V2,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    assert migrated["tombstone_format"] == TOMBSTONE_FORMAT_V2
    assert migrated["tombstone"] != manifest_path
    successor_manifest = load_tombstone_manifest_v2(
        storage.read_bytes(migrated["tombstone"]),
        expected_organization="org",
        expected_super_name="lake",
        expected_simple_name="facts",
        pinned_snapshot_version=5,
        expected_total_rows=1,
        expected_digest=migrated["tombstone_digest"],
        expected_segment_prefix=tombstone_dir + "/",
        require_canonical_json=True,
    )
    assert successor_manifest.base_snapshot_version == 4
    assert successor_manifest.segments == manifest.segments


@pytest.mark.parametrize(
    "tombstone_format",
    [TOMBSTONE_FORMAT_V1, TOMBSTONE_FORMAT_V3],
)
def test_migration_accepts_valid_v1_and_v3_tombstone_artifacts(
    tmp_path,
    tombstone_format,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({"value": [1, 2]}), data_path)
    tombstone_dir = "org/lake/tables/facts/tombstone"
    deletion_frame = pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_path],
        ROWID_COL: [1],
    }, schema=TOMBSTONE_SCHEMA)
    if tombstone_format == TOMBSTONE_FORMAT_V1:
        tombstone_path = tombstone_dir + "/legacy.parquet"
        storage.write_parquet(deletion_frame.to_arrow(), tombstone_path)
        digest = tombstone_digest(deletion_frame)
    else:
        tombstone_path, _frame, state = persist_tombstone_v3_frame(
            tombstone_dir,
            deletion_frame,
            3,
            storage=storage,
        )
        assert tombstone_path is not None
        digest = state.root_digest
    snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [{"file": data_path}],
        "tombstone": tombstone_path,
        "tombstone_rows": 1,
        "tombstone_digest": digest,
        "tombstone_format": tombstone_format,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    assert migrated["tombstone_format"] == tombstone_format
    assert migrated["tombstone"] == tombstone_path
    assert migrated["tombstone_rows"] == 1
    assert migrated["tombstone_digest"] == digest


def test_migration_rejects_tombstone_symlink_outside_table_namespace(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/current.parquet"
    pointer = "org/lake/tables/facts/tombstone/escape.parquet"
    outside = "org/lake/tables/other/tombstone/target.parquet"
    storage.write_parquet(pa.table({"value": [1]}), data_path)
    storage.write_parquet(pa.table({
        TOMBSTONE_FILE_COL: pa.array([], type=pa.string()),
        ROWID_COL: pa.array([], type=pa.int64()),
    }), outside)
    physical_pointer = tmp_path / pointer
    physical_pointer.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(tmp_path / outside, physical_pointer)
    snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [{"file": data_path}],
        "tombstone": pointer,
        "tombstone_rows": 1,
        "tombstone_digest": "0" * 64,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="physical table namespace"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_migration_rejects_snapshot_symlink_outside_table_namespace(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/escape.json"
    outside = "org/lake/tables/other/snapshots/target.json"
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    storage.write_json(outside, snapshot)
    physical_snapshot = tmp_path / old_path
    physical_snapshot.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(tmp_path / outside, physical_snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="physical table namespace"):
        st.migrate_legacy_metadata()

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


@pytest.mark.parametrize(
    ("tombstone_format", "suffix"),
    [
        (TOMBSTONE_FORMAT_V1, "parquet"),
        (TOMBSTONE_FORMAT_V2, "json"),
        (TOMBSTONE_FORMAT_V3, "parquet"),
    ],
)
def test_migration_rejects_oversized_tombstone_rows_before_object_read(
    tombstone_format, suffix,
):
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    snapshot = {
        "tombstone": f"org/lake/tables/facts/tombstone/huge.{suffix}",
        "tombstone_rows": 1_000_001,
        "tombstone_digest": "0" * 64,
        "tombstone_format": tombstone_format,
    }

    with pytest.raises(ValueError, match="row count exceeds"):
        st._migrate_legacy_tombstone(
            simple="facts",
            snapshot=snapshot,
            version=4,
            allowed_files={"org/lake/tables/facts/data/current.parquet"},
            available_rows=2_000_000,
        )

    st.storage.stat_object.assert_not_called()
    st.storage.read_range.assert_not_called()


def test_migration_rejects_oversized_tombstone_object_before_range_read():
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.storage.is_local_storage.return_value = False
    st.storage.stat_object.return_value = ObjectMetadata(
        size=64 * 1024 * 1024 + 1,
        etag="oversized-tombstone",
    )
    snapshot = {
        "tombstone": "org/lake/tables/facts/tombstone/huge.parquet",
        "tombstone_rows": 1,
        "tombstone_digest": "0" * 64,
    }

    with pytest.raises(ValueError, match="object exceeds"):
        st._migrate_legacy_tombstone(
            simple="facts",
            snapshot=snapshot,
            version=4,
            allowed_files={"org/lake/tables/facts/data/current.parquet"},
            available_rows=1,
        )

    st.storage.read_range.assert_not_called()
