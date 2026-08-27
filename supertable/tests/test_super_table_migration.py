import copy
import hashlib
import io
import json
import os
import subprocess
from datetime import datetime, time, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import fakeredis
import numpy as np
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
    load_tombstone,
    stats_seal_for_metadata,
    tombstone_digest,
)
from supertable.data_writer import DataWriter
from supertable.row_identity import snapshot_proves_stable_rowids
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_table import SuperTable
from supertable.utils.profiler import Profiler
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V2,
    TOMBSTONE_FORMAT_V3,
    load_tombstone_manifest_v2,
)


def _configure_mock_snapshot_reads(storage, payload_for_path):
    written_payloads = {}

    def payload(path):
        if path in written_payloads:
            return written_payloads[path]
        return payload_for_path(path)

    def encoded(path):
        return json.dumps(
            payload(path), separators=(",", ":"), allow_nan=False,
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
    storage.write_json.side_effect = lambda path, document: written_payloads.__setitem__(
        path, copy.deepcopy(document),
    )


@pytest.mark.parametrize("confirmation", [None, False, 1, "yes"])
def test_migration_requires_exact_offline_confirmation_before_io(confirmation):
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = Mock()

    kwargs = (
        {} if confirmation is None
        else {"confirm_system_offline": confirmation}
    )
    with pytest.raises(ValueError, match="confirm_system_offline=True"):
        st.migrate_legacy_metadata(**kwargs)

    st.catalog.acquire_namespace_lock.assert_not_called()
    st.storage.assert_not_called()


def test_migration_rejects_an_empty_namespace_as_invocation_error():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 0,
        "ts": 1,
        "read_only": False,
    }))
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="contains no tables"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def test_migration_requires_exact_expected_table_inventory_before_io():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    snapshot = {"snapshot_version": 4, "schema": [], "resources": []}
    old_path = "org/lake/tables/facts/snapshots/old.json"
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="table inventory does not match"):
        st.migrate_legacy_metadata(
            confirm_system_offline=True,
            expected_tables={"other"},
        )

    st.storage.stat_object.assert_not_called()
    st.storage.write_json.assert_not_called()


def test_migrate_legacy_metadata_rebuilds_stats_and_publishes_successor(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/a.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 3,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

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


def test_migration_refuses_to_publish_a_proofless_successor(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/unsupported.json"
    data_path = "org/lake/tables/facts/data/unsupported.parquet"
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

    with pytest.raises(ValueError, match="stable row-ID proof"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4
    assert leaf["path"] == old_path


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        result = st.migrate_legacy_metadata(confirm_system_offline=True)

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
        first = st.migrate_legacy_metadata(confirm_system_offline=True)
        second = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert first["migrated_tables"] == ["facts"]
    assert second["migrated_tables"] == []
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["_legacy_metadata_migration_version"] == 2
    assert snapshot_proves_stable_rowids(leaf["payload"]) is True
    assert st.storage.write_json.call_count == 1


def test_migration_upgrades_previous_marker_before_treating_table_as_current():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/previous-migration.json"
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
        "stats_file": None,
        "stats_rows": 0,
        "rowid_high_watermark": 0,
        "_row_filter": None,
        "_legacy_metadata_migration_version": 1,
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)):
        result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["_legacy_metadata_migration_version"] == 2
    assert snapshot_proves_stable_rowids(leaf["payload"]) is True


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

    st.storage.write_json.assert_not_called()


def test_current_migration_marker_cannot_bypass_rowid_integrity_proof():
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 5,
        "last_updated_ms": 1,
        "previous_snapshot": "org/lake/tables/facts/snapshots/old.json",
        "schema": {"id": "Int64"},
        "resources": [{
            "file": "org/lake/tables/facts/data/a.parquet",
            "rows": 1,
            "file_size": 1,
            "columns": 3,
            "footer_sha256": "a" * 64,
            "stats_rows": 0,
            "stats_digest": "b" * 64,
            "rowid_integrity": {
                "version": 1,
                "rows": 1,
                "nonnull": 1,
                "unique": 1,
                "minimum": 11,
                "maximum": 11,
                "digest": "c" * 64,
                "footer_sha256": "a" * 64,
            },
        }],
        # This floor is below a live physical row and must not become writer
        # recovery authority merely because the migration marker is current.
        "rowid_high_watermark": 10,
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "tombstone_format": TOMBSTONE_FORMAT_V3,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
        "_legacy_metadata_migration_version": 2,
    }

    with pytest.raises(ValueError, match="invalid stable row-ID proof"):
        SuperTable._legacy_metadata_migration_required(
            simple="facts",
            table_dir="org/lake/tables/facts",
            version=5,
            snapshot=snapshot,
        )


@pytest.mark.parametrize("migration_version", [1, 2])
def test_migration_marker_cannot_omit_rowid_integrity_proof(
    migration_version,
):
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 5,
        "last_updated_ms": 1,
        "previous_snapshot": "org/lake/tables/facts/snapshots/old.json",
        "schema": {},
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "tombstone_format": TOMBSTONE_FORMAT_V3,
        "stats_file": None,
        "stats_rows": 0,
        "_row_filter": None,
        "_legacy_metadata_migration_version": migration_version,
    }

    with pytest.raises(ValueError, match="lacks required current fields"):
        SuperTable._legacy_metadata_migration_required(
            simple="facts",
            table_dir="org/lake/tables/facts",
            version=5,
            snapshot=snapshot,
        )


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

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_migration_rebuilds_current_schema_that_disagrees_with_footer(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    data_path = "org/lake/tables/facts/data/current.parquet"
    storage.write_parquet(pa.table({
        "value": ["text"],
        "__rowid__": pa.array([1], type=pa.int64()),
    }), data_path)
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
            "columns": 2,
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
            "rowid_integrity": {
                "version": 1,
                "rows": 1,
                "nonnull": 1,
                "unique": 1,
                "minimum": 1,
                "maximum": 1,
                "digest": hashlib.sha256(
                    b"supertable-rowid-integrity-v1\0"
                    + (1).to_bytes(8, "big", signed=True)
                ).hexdigest(),
                "footer_sha256": stats_seal.footer_sha256,
            },
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
        "stats_file": None,
        "stats_rows": 0,
        "rowid_high_watermark": 1,
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

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

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

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

    st.storage.write_json.assert_not_called()


def test_migration_rejects_v2_4_operational_cache_that_differs_from_storage(
    tmp_path,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage,
        client,
        catalog,
    )
    leaf_key = RK.meta_leaf("org", "lake", "facts")
    cached_leaf = json.loads(client.get(leaf_key))
    cached_leaf["payload"]["tombstone"] = None
    cached_leaf["payload"]["tombstone_rows"] = 0
    client.set(leaf_key, json.dumps(cached_leaf))
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="cache disagrees with storage"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_accepts_v2_4_path_only_leaf(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    _seed_authentic_v2_4_active_table(
        storage,
        client,
        catalog,
    )
    leaf_key = RK.meta_leaf("org", "lake", "facts")
    path_only_leaf = json.loads(client.get(leaf_key))
    path_only_leaf.pop("payload")
    client.set(leaf_key, json.dumps(path_only_leaf))
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["tombstone_format"] == TOMBSTONE_FORMAT_V3
    assert snapshot_proves_stable_rowids(leaf["payload"]) is True


def test_migration_rejects_redis_cjson_unsafe_rowid_floor():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/current.json"
    rowid_floor = 10**14
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
    _seed_migration_catalog(catalog, client, cached, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    st.catalog = catalog

    with pytest.raises(ValueError, match="Redis JSON safe integer"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    st.storage.write_json.assert_not_called()


def test_migration_rejects_nested_redis_cjson_unsafe_integer(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage,
        client,
        catalog,
    )
    snapshot = storage.read_json(old_path)
    snapshot["lineage"] = {
        "source": {"opaque_generation": 10**14},
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="Redis JSON safe integer range"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_allocator_uses_redis_cjson_exact_integer_boundary():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.catalog = catalog
    key = RK.meta_rowid_seq("org", "lake", "facts")

    client.set(key, str(10**14 - 1))
    assert st._read_v2_4_rowid_sequence(simple="facts") == 10**14 - 1

    client.set(key, str(10**14))
    with pytest.raises(ValueError, match="Redis snapshot limit"):
        st._read_v2_4_rowid_sequence(simple="facts")


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    _configure_mock_snapshot_reads(st.storage, lambda _path: snapshot)
    record_snapshot_write = st.storage.write_json.side_effect

    def write_then_replace_namespace_lock(path, document):
        record_snapshot_write(path, document)
        client.set(RK.lock_namespace("org", "lake"), "replacement-owner", ex=60)

    st.storage.write_json.side_effect = write_then_replace_namespace_lock
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)), \
         pytest.raises(LockLostError, match="namespace fencing lock"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["_legacy_metadata_migration_version"] == 2


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
        st.migrate_legacy_metadata(confirm_system_offline=True)


def test_migration_does_not_reconcile_ambiguous_commit_with_wrong_payload(
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
    }
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    original_commit = catalog.commit_snapshot

    def commit_then_corrupt_payload(*args, **kwargs):
        original_commit(*args, **kwargs)
        leaf_key = RK.meta_leaf("org", "lake", "facts")
        committed = json.loads(client.get(leaf_key))
        committed["payload"]["schema"] = {"corrupt": "Int64"}
        client.set(leaf_key, json.dumps(committed))
        raise redis.ConnectionError("response was lost after commit")

    monkeypatch.setattr(
        catalog,
        "commit_snapshot",
        commit_then_corrupt_payload,
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
        st.migrate_legacy_metadata(confirm_system_offline=True)


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == []
    catalog.scan_leaf_items.assert_called_once_with(
        "org",
        "lake",
        count=1000,
        batch_size=1,
        max_scan_calls=1000,
    )
    # One deduplicated table is pinned at the start and end of both the
    # read-only namespace preflight and the publication pass.
    assert catalog.begin_table_mutation.call_count == 4


def test_migration_preflights_every_table_before_first_publication():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9, "ts": 1, "read_only": False,
    }))
    valid_path = "org/lake/tables/alpha/snapshots/old.json"
    invalid_path = "org/lake/tables/omega/snapshots/old.json"
    valid_snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
    }
    invalid_snapshot = {
        **valid_snapshot,
        "schema": "not-a-schema",
    }
    for simple, path, snapshot in (
        ("alpha", valid_path, valid_snapshot),
        ("omega", invalid_path, invalid_snapshot),
    ):
        client.set(RK.meta_leaf("org", "lake", simple), json.dumps({
            "version": 4,
            "ts": 1,
            "path": path,
            "payload": snapshot,
        }))
    catalog.scan_leaf_items = Mock(return_value=iter([
        {"simple": "alpha", "path": valid_path, "version": 4},
        {"simple": "omega", "path": invalid_path, "version": 4},
    ]))
    catalog.commit_snapshot = Mock(wraps=catalog.commit_snapshot)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = Mock()
    snapshots = {
        valid_path: valid_snapshot,
        invalid_path: invalid_snapshot,
    }
    _configure_mock_snapshot_reads(st.storage, snapshots.__getitem__)
    st.catalog = catalog

    with patch("supertable.processing.extract_stats_rows", return_value=pl.DataFrame()), \
         patch("supertable.processing.build_stats_file", return_value=(None, None)), \
         pytest.raises(ValueError, match="schema is invalid"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    alpha = catalog.get_leaf("org", "lake", "alpha")
    omega = catalog.get_leaf("org", "lake", "omega")
    assert alpha is not None and alpha["version"] == 4
    assert omega is not None and omega["version"] == 4
    st.storage.write_json.assert_not_called()
    st.catalog.commit_snapshot.assert_not_called()


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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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


def _seed_v2_4_arrow_table(storage, client, catalog, table, schema):
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/v2-4-types.parquet"
    storage.write_parquet(table, data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": schema,
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": table.num_rows,
            "columns": table.num_columns,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    return snapshot


def _stable_resource_for_test(tmp_path, storage, data_path, rowids):
    footer = pq.read_metadata(tmp_path / data_path)
    stats_seal = stats_seal_for_metadata(data_path, footer)
    digest = hashlib.sha256(b"supertable-rowid-integrity-v1\0")
    for rowid in rowids:
        digest.update(int(rowid).to_bytes(8, "big", signed=True))
    return {
        "file": data_path,
        "file_size": storage.size(data_path),
        "rows": len(rowids),
        "columns": len(footer.schema.to_arrow_schema()),
        "footer_sha256": stats_seal.footer_sha256,
        "stats_rows": stats_seal.stats_rows,
        "stats_digest": stats_seal.stats_digest,
        "rowid_integrity": {
            "version": 1,
            "rows": len(rowids),
            "nonnull": len(rowids),
            "unique": len(set(rowids)),
            "minimum": min(rowids) if rowids else None,
            "maximum": max(rowids) if rowids else None,
            "digest": digest.hexdigest(),
            "footer_sha256": stats_seal.footer_sha256,
        },
    }


def _seed_authentic_v2_4_active_table(storage, client, catalog):
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    old_tombstone = "org/lake/tables/facts/tombstone/deleted-v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "payload": pa.array([b"a", b"x" * 257], type=pa.binary()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    legacy_vector = pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_path],
        ROWID_COL: [10],
    }, schema=TOMBSTONE_SCHEMA)
    storage.write_parquet(legacy_vector.to_arrow(), old_tombstone)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "payload": "Binary",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 4,
        }],
        "tombstone": old_tombstone,
        "tombstone_rows": 1,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    return old_path


def _seed_named_authentic_v2_4_table(storage, client, simple, rowid_start):
    table_dir = f"org/lake/tables/{simple}"
    old_path = f"{table_dir}/snapshots/v2-4.json"
    data_path = f"{table_dir}/data/v2-4.parquet"
    old_tombstone = f"{table_dir}/tombstone/deleted-v2-4.parquet"
    rowids = [rowid_start, rowid_start + 1]
    storage.write_parquet(pa.table({
        "id": pa.array(rowids, type=pa.int64()),
        "payload": pa.array([b"deleted", b"live"], type=pa.binary()),
        "__rowid__": pa.array(rowids, type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    storage.write_parquet(pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_path],
        ROWID_COL: [rowid_start],
    }, schema=TOMBSTONE_SCHEMA).to_arrow(), old_tombstone)
    snapshot = {
        "simple_name": simple,
        "location": table_dir,
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "payload": "Binary",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 4,
        }],
        "tombstone": old_tombstone,
        "tombstone_rows": 1,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    client.set(RK.meta_leaf("org", "lake", simple), json.dumps({
        "version": 4,
        "ts": 1,
        "path": old_path,
        "payload": snapshot,
    }))
    client.set(
        RK.meta_rowid_seq("org", "lake", simple),
        str(rowid_start + 1),
    )
    return old_path


def test_migration_rebuilds_v2_4_footer_seals_without_unsafe_object_seal(
    tmp_path,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "snapshot_version": 4,
        "schema": {
            "value": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{"file": data_path}],
        "tombstone": None,
        "tombstone_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = storage.read_json(leaf["path"])
    resource = migrated["resources"][0]
    assert resource["rows"] == 2
    assert resource["file_size"] == storage.size(data_path)
    assert resource["columns"] == 3
    # Local/provider nanosecond timestamps cannot survive Redis Lua cjson
    # exactly, so the optional object seal is intentionally not published.
    assert "object_seal" not in resource
    expected_stats = stats_seal_for_metadata(
        data_path,
        pq.read_metadata(tmp_path / data_path),
    )
    assert resource["footer_sha256"] == expected_stats.footer_sha256
    assert resource["stats_rows"] == expected_stats.stats_rows
    assert resource["stats_digest"] == expected_stats.stats_digest
    assert migrated["rowid_high_watermark"] == 11
    assert snapshot_proves_stable_rowids(migrated) is True


def test_migration_accepts_authentic_v2_4_system_schema_pair(tmp_path):
    """v2.4 stored its two writer-owned columns in the snapshot schema."""
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1, 2], type=pa.int32()),
        "payload": pa.array([b"a", b"x" * 257], type=pa.binary()),
        "label": pa.array(["é", "𐍈𐍈"], type=pa.string()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            # A final zero-row v2.4 write could widen the declared schema
            # while retaining the older Int32 resource. Caller-supplied
            # __rowid__ remained in that empty frame, so the full system pair
            # cannot be used to infer that this schema came from data files.
            "id": "Int64",
            "payload": "Binary",
            "label": "String",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "schemaString": json.dumps({
            "type": "struct",
            "fields": {
                "id": "Int64",
                "payload": "Binary",
                "label": "String",
                "__rowid__": "Int64",
                "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
            },
        }, separators=(",", ":")),
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 5,
        }],
        # This is the exact empty deletion-vector state written by v2.4.
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    # v2.4 may have reserved IDs that never reached a committed data file.
    # The migration must retain that allocator boundary instead of deriving
    # the successor floor only from the surviving physical rows.
    client.set(RK.meta_rowid_seq("org", "lake", "facts"), "99")

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)
    rerun = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    assert rerun["migrated_tables"] == []
    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["payload"]["schema"] == {
        "id": "Int64",
        "payload": "Binary",
        "label": "String",
    }
    assert leaf["payload"]["schemaString"] == json.dumps({
        "type": "struct",
        "fields": {
            "id": "Int64", "payload": "Binary", "label": "String",
        },
    }, separators=(",", ":"))
    migrated = leaf["payload"]
    assert migrated["rowid_high_watermark"] == 99
    assert snapshot_proves_stable_rowids(migrated) is True
    assert migrated["resources"][0]["rowid_integrity"] == {
        "version": 1,
        "rows": 2,
        "nonnull": 2,
        "unique": 2,
        "minimum": 10,
        "maximum": 11,
        "digest": hashlib.sha256(
            b"supertable-rowid-integrity-v1\0"
            + (10).to_bytes(8, "big", signed=True)
            + (11).to_bytes(8, "big", signed=True)
        ).hexdigest(),
        "footer_sha256": migrated["resources"][0]["footer_sha256"],
    }
    assert migrated["resources"][0]["column_max_value_bytes"] == {
        "payload": 257,
        "label": 8,
    }
    assert "object_seal" not in migrated["resources"][0]
    persisted = storage.read_json(leaf["path"])
    assert persisted["_row_filter"] is None

    token = catalog.acquire_simple_lock("org", "lake", "facts")
    assert token
    try:
        assert catalog.reserve_rowids_at_least(
            "org", "lake", "facts", 1, 99, lock_token=token,
        ) == (100, 100)
    finally:
        catalog.release_simple_lock("org", "lake", "facts", token)


def test_migration_accepts_v2_4_empty_final_write_schema(tmp_path):
    """An empty v2.4 write owns the final schema without new data files."""
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage,
        client,
        catalog,
    )
    snapshot = storage.read_json(old_path)
    snapshot["schema"].pop("__rowid__")
    # v2.4 applied last-write-wins schema metadata even when the final write
    # had no rows and therefore produced no replacement Parquet resource.
    snapshot["schema"]["id"] = "Float64"
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    assert migrated["schema"] == {"id": "Float64", "payload": "Binary"}
    assert migrated["tombstone_format"] == TOMBSTONE_FORMAT_V3
    assert migrated["rowid_high_watermark"] == 11
    assert snapshot_proves_stable_rowids(migrated) is True


@pytest.mark.parametrize(
    "declared_change",
    ["incompatible_type", "missing_column"],
)
def test_migration_rejects_v2_4_schema_incompatible_with_retained_data(
    tmp_path, declared_change,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    snapshot = storage.read_json(old_path)
    snapshot["schema"].pop("__rowid__")
    if declared_change == "incompatible_type":
        snapshot["schema"]["id"] = "String"
    else:
        snapshot["schema"]["new_column"] = "Int64"
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="not query-compatible"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_converts_authentic_v2_4_active_vector_to_v3(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    old_tombstone = "org/lake/tables/facts/tombstone/deleted-v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    legacy_vector = pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_path],
        ROWID_COL: [10],
    }, schema=TOMBSTONE_SCHEMA)
    storage.write_parquet(legacy_vector.to_arrow(), old_tombstone)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 3,
        }],
        # Exact active v2.4 state: no digest and no format discriminator.
        "tombstone": old_tombstone,
        "tombstone_rows": 1,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    assert migrated["tombstone_format"] == TOMBSTONE_FORMAT_V3
    assert migrated["tombstone"] != old_tombstone
    assert migrated["tombstone_rows"] == 1
    assert migrated["tombstone_digest"] == hashlib.sha256(
        storage.read_bytes(migrated["tombstone"]),
    ).hexdigest()
    converted = pl.from_arrow(storage.read_parquet(migrated["tombstone"]))
    assert converted.equals(legacy_vector)
    assert storage.exists(old_tombstone)


def test_migrated_v2_4_rows_and_first_current_allocation_are_semantic(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    _seed_authentic_v2_4_active_table(storage, client, catalog)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    vector = load_tombstone(
        migrated["tombstone"],
        allow_cache=False,
        required=True,
        expected_rows=migrated["tombstone_rows"],
        expected_digest=migrated["tombstone_digest"],
        allowed_files={data_path},
        tombstone_format=migrated["tombstone_format"],
        storage=storage,
    )
    assert vector is not None
    physical = pl.from_arrow(storage.read_parquet(data_path)).with_columns(
        pl.lit(data_path).alias(TOMBSTONE_FILE_COL),
    )
    visible = physical.join(
        vector,
        on=[TOMBSTONE_FILE_COL, ROWID_COL],
        how="anti",
    )
    assert visible.select(["id", "payload"]).to_dicts() == [{
        "id": 2,
        "payload": b"x" * 257,
    }]

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = st
    writer.catalog = catalog
    token = catalog.acquire_simple_lock("org", "lake", "facts")
    assert token
    try:
        assert writer._reserve_snapshot_rowids(
            snapshot=migrated,
            simple_name="facts",
            count=1,
            profiler=Profiler(),
            lock_token=token,
        ) == (12, 12)
    finally:
        catalog.release_simple_lock("org", "lake", "facts", token)


def test_multitable_v2_4_migration_resumes_after_second_publish_failure(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9,
        "ts": 1,
        "read_only": False,
    }))
    original_paths = {
        simple: _seed_named_authentic_v2_4_table(
            storage,
            client,
            simple,
            rowid_start,
        )
        for simple, rowid_start in (("alpha", 10), ("omega", 20))
    }
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    real_commit = catalog.commit_snapshot
    commit_calls = 0

    def fail_second_publication(*args, **kwargs):
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise RuntimeError("injected second-table publication failure")
        return real_commit(*args, **kwargs)

    monkeypatch.setattr(catalog, "commit_snapshot", fail_second_publication)
    with pytest.raises(RuntimeError, match="injected second-table"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    interrupted = {
        simple: catalog.get_leaf("org", "lake", simple)
        for simple in original_paths
    }
    assert sorted(leaf["version"] for leaf in interrupted.values()) == [4, 5]
    for simple, leaf in interrupted.items():
        if leaf["version"] == 4:
            assert leaf["path"] == original_paths[simple]
        else:
            assert leaf["payload"]["_legacy_metadata_migration_version"] == 2

    monkeypatch.setattr(catalog, "commit_snapshot", real_commit)
    resumed = st.migrate_legacy_metadata(confirm_system_offline=True)
    assert len(resumed["migrated_tables"]) == 1
    for simple in original_paths:
        leaf = catalog.get_leaf("org", "lake", simple)
        assert leaf is not None and leaf["version"] == 5
        assert leaf["payload"]["_legacy_metadata_migration_version"] == 2
        assert leaf["payload"]["tombstone_format"] == TOMBSTONE_FORMAT_V3
        assert snapshot_proves_stable_rowids(leaf["payload"]) is True

    assert st.migrate_legacy_metadata(
        confirm_system_offline=True,
    )["migrated_tables"] == []


def test_multitable_preflight_rejects_oversized_successor_before_any_commit(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9,
        "ts": 1,
        "read_only": False,
    }))
    alpha_path = _seed_named_authentic_v2_4_table(
        storage, client, "alpha", 10,
    )

    simple = "omega"
    table_dir = f"org/lake/tables/{simple}"
    omega_path = f"{table_dir}/snapshots/v2-4.json"
    resources = []
    timestamp_type = pa.timestamp("us", tz="UTC")
    for index in range(20):
        data_path = f"{table_dir}/data/part-{index:02d}.parquet"
        storage.write_parquet(pa.table({
            "id": pa.array([index], type=pa.int64()),
            "__rowid__": pa.array([100 + index], type=pa.int64()),
            "__timestamp__": pa.array(
                [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                type=timestamp_type,
            ),
        }), data_path)
        resources.append({
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 1,
            "columns": 3,
        })
    omega_snapshot = {
        "simple_name": simple,
        "location": table_dir,
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": resources,
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(omega_path, omega_snapshot)
    client.set(RK.meta_leaf("org", "lake", simple), json.dumps({
        "version": 4,
        "ts": 1,
        "path": omega_path,
        "payload": omega_snapshot,
    }))
    client.set(RK.meta_rowid_seq("org", "lake", simple), "119")

    snapshot_limit = 8 * 1024
    assert storage.size(alpha_path) < snapshot_limit
    assert storage.size(omega_path) < snapshot_limit
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_SNAPSHOT_BYTES",
        snapshot_limit,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="successor snapshot exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    for name, original_path in (("alpha", alpha_path), ("omega", omega_path)):
        leaf = catalog.get_leaf("org", "lake", name)
        assert leaf is not None
        assert leaf["version"] == 4
        assert leaf["path"] == original_path


def test_multitable_preflight_reserves_redis_root_version_headroom(
    tmp_path,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": (1 << 53) - 2,
        "ts": 1,
        "read_only": False,
    }))
    original_paths = {
        simple: _seed_named_authentic_v2_4_table(
            storage,
            client,
            simple,
            rowid_start,
        )
        for simple, rowid_start in (("alpha", 10), ("omega", 20))
    }
    catalog.commit_snapshot = Mock(wraps=catalog.commit_snapshot)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="root version headroom"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    for simple, original_path in original_paths.items():
        leaf = catalog.get_leaf("org", "lake", simple)
        assert leaf is not None
        assert leaf["version"] == 4
        assert leaf["path"] == original_path
    catalog.commit_snapshot.assert_not_called()


def test_multitable_preflight_rejects_oversized_schema_before_any_commit(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9,
        "ts": 1,
        "read_only": False,
    }))
    alpha_path = "org/lake/tables/alpha/snapshots/v2-4.json"
    alpha_snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
    }
    storage.write_json(alpha_path, alpha_snapshot)
    client.set(RK.meta_leaf("org", "lake", "alpha"), json.dumps({
        "version": 4,
        "ts": 1,
        "path": alpha_path,
        "payload": alpha_snapshot,
    }))
    omega_path = _seed_named_authentic_v2_4_table(
        storage, client, "omega", 20,
    )
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_SCHEMA_BYTES",
        24,
        raising=False,
    )
    catalog.commit_snapshot = Mock(wraps=catalog.commit_snapshot)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="successor schema exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    for name, original_path in (("alpha", alpha_path), ("omega", omega_path)):
        leaf = catalog.get_leaf("org", "lake", name)
        assert leaf is not None
        assert leaf["version"] == 4
        assert leaf["path"] == original_path
    catalog.commit_snapshot.assert_not_called()


def test_multitable_preflight_projects_v2_manifest_successor_path(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9,
        "ts": 1,
        "read_only": False,
    }))

    alpha_path = "org/lake/tables/alpha/snapshots/v2-4.json"
    alpha_snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
    }
    storage.write_json(alpha_path, alpha_snapshot)
    client.set(RK.meta_leaf("org", "lake", "alpha"), json.dumps({
        "version": 4,
        "ts": 1,
        "path": alpha_path,
        "payload": alpha_snapshot,
    }))

    simple = "omega"
    table_dir = f"org/lake/tables/{simple}"
    data_path = f"{table_dir}/data/current.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
    }), data_path)
    tombstone_dir = f"{table_dir}/tombstone"
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
    generated_manifest_path, manifest = persist_tombstone_manifest_v2(
        tombstone_dir,
        organization="org",
        super_name="lake",
        simple_name=simple,
        base_snapshot_version=3,
        snapshot_version=4,
        segments=(segment,),
        storage=storage,
    )
    short_manifest_path = f"{tombstone_dir}/m.json"
    storage.write_bytes(
        short_manifest_path,
        storage.read_bytes(generated_manifest_path),
    )
    omega_path = f"{table_dir}/snapshots/current.json"
    omega_snapshot = {
        "snapshot_version": 4,
        "schema": {"value": "Int64"},
        "resources": [
            _stable_resource_for_test(
                tmp_path, storage, data_path, [1, 2],
            ),
        ],
        "rowid_high_watermark": 2,
        "_row_filter": None,
        "tombstone": short_manifest_path,
        "tombstone_rows": 1,
        "tombstone_digest": manifest.digest(),
        "tombstone_format": TOMBSTONE_FORMAT_V2,
    }
    storage.write_json(omega_path, omega_snapshot)
    client.set(RK.meta_leaf("org", "lake", simple), json.dumps({
        "version": 4,
        "ts": 1,
        "path": omega_path,
        "payload": omega_snapshot,
    }))
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_SNAPSHOT_BYTES",
        1400,
    )
    catalog.commit_snapshot = Mock(wraps=catalog.commit_snapshot)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="successor snapshot exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    for name, original_path in (("alpha", alpha_path), (simple, omega_path)):
        leaf = catalog.get_leaf("org", "lake", name)
        assert leaf is not None
        assert leaf["version"] == 4
        assert leaf["path"] == original_path
    catalog.commit_snapshot.assert_not_called()


def test_migration_does_not_publish_corrupt_new_v3_vector(tmp_path):
    class CorruptNewVectorStorage(LocalStorage):
        def write_bytes(self, path, data):
            super().write_bytes(path, data)
            if "deleted-v3" in os.path.basename(path):
                super().write_bytes(path, b"corrupt-v3-vector")

    storage = CorruptNewVectorStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="generated deletion vector failed readback"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_rechecks_source_snapshot_identity_before_publication(tmp_path):
    class ReplacedSourceSnapshotStorage(LocalStorage):
        source_path = None
        replace_on_stats_write = False
        replaced = False

        def write_bytes(self, path, data):
            super().write_bytes(path, data)
            if (
                self.replace_on_stats_write
                and not self.replaced
                and "/stats/" in path
            ):
                replacement = self.read_json(self.source_path)
                replacement["last_updated_ms"] = 999
                super().write_json(self.source_path, replacement)
                self.replaced = True

    storage = ReplacedSourceSnapshotStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    storage.source_path = old_path
    storage.replace_on_stats_write = True
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="source snapshot changed"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert storage.replaced is True
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_does_not_publish_corrupt_new_snapshot(tmp_path):
    class CorruptNewSnapshotStorage(LocalStorage):
        corrupt_successor = False

        def write_json(self, path, data):
            super().write_json(path, data)
            if self.corrupt_successor and "/snapshots/" in path:
                super().write_bytes(path, b'{"truncated":')

    storage = CorruptNewSnapshotStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    storage.corrupt_successor = True
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="generated snapshot failed readback"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_rejects_v2_4_vector_with_wrong_physical_file_mapping(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_a = "org/lake/tables/facts/data/a.parquet"
    data_b = "org/lake/tables/facts/data/b.parquet"
    timestamp_type = pa.timestamp("us", tz="UTC")
    storage.write_parquet(pa.table({
        "id": pa.array([1], type=pa.int64()),
        "__rowid__": pa.array([10], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            type=timestamp_type,
        ),
    }), data_a)
    storage.write_parquet(pa.table({
        "id": pa.array([2], type=pa.int64()),
        "__rowid__": pa.array([20], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 2, tzinfo=timezone.utc)],
            type=timestamp_type,
        ),
    }), data_b)
    tombstone_path = "org/lake/tables/facts/tombstone/deleted-v2-4.parquet"
    # Both components are individually plausible, but rowid 10 physically
    # belongs to data_a. The current composite reader would not delete it if
    # this corrupt legacy pair were trusted.
    storage.write_parquet(pl.DataFrame({
        TOMBSTONE_FILE_COL: [data_b],
        ROWID_COL: [10],
    }, schema=TOMBSTONE_SCHEMA).to_arrow(), tombstone_path)
    resources = [
        {
            "file": path,
            "file_size": storage.size(path),
            "rows": 1,
            "columns": 3,
        }
        for path in (data_a, data_b)
    ]
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": resources,
        "tombstone": tombstone_path,
        "tombstone_rows": 1,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)

    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="does not identify a physical row"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_migration_rejects_duplicate_physical_v2_4_rowids(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/duplicate-rowids.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([10, 10], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 3,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="reuses a v2.4 row ID"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_page_scan_is_bound_to_the_footer_object_identity(tmp_path):
    storage = LocalStorage(str(tmp_path))
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1], type=pa.int64()),
        "__rowid__": pa.array([10], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    observed = storage.stat_object(data_path)
    footer = pq.read_metadata(tmp_path / data_path)
    stats_seal = stats_seal_for_metadata(data_path, footer)
    resource = {
        "file": data_path,
        "rows": 1,
        "file_size": observed.size,
        "columns": 3,
        # Model a footer/object generation pinned immediately before the
        # storage key was replaced with the currently readable generation.
        "object_seal": {
            "size": observed.size,
            "version": observed.version,
            "etag": "stale-object-generation",
            "last_modified_ns": observed.last_modified_ns,
            "checksum_sha256": observed.checksum_sha256,
        },
        "footer_sha256": stats_seal.footer_sha256,
        "stats_rows": stats_seal.stats_rows,
        "stats_digest": stats_seal.stats_digest,
    }
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage

    scan_result = None
    try:
        with pytest.raises(RuntimeError, match="changed before its full scan"):
            scan_result = st._scan_v2_4_resources(
                simple="facts",
                resources=[resource],
                footer_cache={data_path: footer},
            )
    finally:
        if scan_result is not None:
            directory, connection, *_rest = scan_result
            connection.close()
            directory.cleanup()


def test_v2_4_page_scan_commits_rowids_once_per_bounded_group(
    tmp_path,
    monkeypatch,
):
    import sqlite3

    class CountingConnection:
        def __init__(self, inner):
            self.inner = inner
            self.commit_calls = 0

        def execute(self, *args, **kwargs):
            return self.inner.execute(*args, **kwargs)

        def executemany(self, *args, **kwargs):
            return self.inner.executemany(*args, **kwargs)

        def commit(self):
            self.commit_calls += 1
            return self.inner.commit()

        def close(self):
            return self.inner.close()

    real_connect = sqlite3.connect
    monkeypatch.setattr(
        sqlite3,
        "connect",
        lambda *args, **kwargs: CountingConnection(
            real_connect(*args, **kwargs),
        ),
    )
    storage = LocalStorage(str(tmp_path))
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    row_count = 32
    storage.write_parquet(pa.table({
        "id": pa.array(range(row_count), type=pa.int64()),
        "__rowid__": pa.array(range(1, row_count + 1), type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * row_count,
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    observed = storage.stat_object(data_path)
    footer = pq.read_metadata(tmp_path / data_path)
    stats_seal = stats_seal_for_metadata(data_path, footer)
    resource = {
        "file": data_path,
        "rows": row_count,
        "file_size": observed.size,
        "columns": 3,
        "object_seal": {
            "size": observed.size,
            "version": observed.version,
            "etag": observed.etag,
            "last_modified_ns": observed.last_modified_ns,
            "checksum_sha256": observed.checksum_sha256,
        },
        "footer_sha256": stats_seal.footer_sha256,
        "stats_rows": stats_seal.stats_rows,
        "stats_digest": stats_seal.stats_digest,
    }
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage

    directory, connection, *_rest = st._scan_v2_4_resources(
        simple="facts",
        resources=[resource],
        footer_cache={data_path: footer},
    )
    try:
        assert connection.commit_calls == 1
        assert [
            row[1]
            for row in connection.execute("PRAGMA table_info(rowids)")
        ] == ["value", "resource_id"]
        assert connection.execute(
            "SELECT file FROM resources"
        ).fetchall() == [(data_path,)]
    finally:
        connection.close()
        directory.cleanup()


def test_v2_4_page_scan_requires_temporary_disk_capacity(
    tmp_path,
    monkeypatch,
):
    import shutil

    storage = LocalStorage(str(tmp_path))
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1], type=pa.int64()),
        "__rowid__": pa.array([1], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    observed = storage.stat_object(data_path)
    footer = pq.read_metadata(tmp_path / data_path)
    stats_seal = stats_seal_for_metadata(data_path, footer)
    resource = {
        "file": data_path,
        "rows": 1,
        "file_size": observed.size,
        "columns": 3,
        "object_seal": {
            "size": observed.size,
            "version": observed.version,
            "etag": observed.etag,
            "last_modified_ns": observed.last_modified_ns,
            "checksum_sha256": observed.checksum_sha256,
        },
        "footer_sha256": stats_seal.footer_sha256,
        "stats_rows": stats_seal.stats_rows,
        "stats_digest": stats_seal.stats_digest,
    }
    real_disk_usage = shutil.disk_usage
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda path: real_disk_usage(path)._replace(free=1),
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage

    with pytest.raises(RuntimeError, match="temporary disk capacity"):
        st._scan_v2_4_resources(
            simple="facts",
            resources=[resource],
            footer_cache={data_path: footer},
        )


def test_v2_4_source_is_rechecked_after_generated_artifacts(tmp_path):
    class ReplaceSourceAfterStatsStorage(LocalStorage):
        armed = False
        source_path = "org/lake/tables/facts/data/v2-4.parquet"

        def write_bytes(self, path, data):
            super().write_bytes(path, data)
            if self.armed and "/stats/" in path:
                self.armed = False
                super().write_parquet(pa.table({
                    "id": pa.array([101, 102], type=pa.int64()),
                    "payload": pa.array([b"changed", b"source"], type=pa.binary()),
                    "__rowid__": pa.array([10, 11], type=pa.int64()),
                    "__timestamp__": pa.array(
                        [
                            datetime(2026, 1, 1, tzinfo=timezone.utc),
                            datetime(2026, 1, 2, tzinfo=timezone.utc),
                        ],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }), self.source_path)

    storage = ReplaceSourceAfterStatsStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    storage.armed = True
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="changed after its full scan"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_v2_4_migration_does_not_apply_show_stats_display_limits(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/v2-4.parquet"
    storage.write_parquet(pa.table({
        "id": pa.array([1], type=pa.int64()),
        "value": pa.array([100], type=pa.int64()),
        "__rowid__": pa.array([10], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "id": "Int64",
            "value": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 1,
            "columns": 4,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    # These limits belong to interactive SHOW STATS only. The writer and
    # offline migration must support a complete artifact larger than them.
    monkeypatch.setattr(processing_module, "MAX_SHOW_STATS_ROWS", 1)
    monkeypatch.setattr(processing_module, "MAX_SHOW_STATS_DECODED_BYTES", 1)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["payload"]["stats_rows"] == 2


def test_v2_4_migration_bounds_stats_materialization_before_build(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(
        storage, client, catalog,
    )
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_STATS_DECODED_BYTES",
        1024,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="statistics materialization exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    assert leaf["version"] == 4
    assert leaf["path"] == old_path


def test_v2_4_migration_reads_generated_stats_in_bounded_batches(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    _seed_authentic_v2_4_active_table(storage, client, catalog)
    monkeypatch.setattr(
        storage,
        "read_parquet",
        Mock(side_effect=AssertionError("whole-object read is forbidden")),
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["stats_rows"] == 2


def test_v2_4_migration_rejects_footer_ranges_not_proven_by_pages(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    _seed_authentic_v2_4_active_table(storage, client, catalog)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    real_stats_rows = processing_module._stats_rows_for_metadata
    source_footer_digest = processing_module.parquet_footer_sha256(
        pq.read_metadata(tmp_path / "org/lake/tables/facts/data/v2-4.parquet"),
    )

    def narrow_id_footer_range(*args, **kwargs):
        rows = real_stats_rows(*args, **kwargs)
        if processing_module.parquet_footer_sha256(args[1]) != source_footer_digest:
            return rows
        for row in rows:
            if row["column_name"] == "id":
                row["min_bigint"] = 999
                row["max_bigint"] = 999
                row["stats_available"] = True
        return rows

    monkeypatch.setattr(
        processing_module,
        "_stats_rows_for_metadata",
        narrow_id_footer_range,
    )
    with pytest.raises(
        RuntimeError,
        match="footer statistics disagree with decoded data",
    ):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_rejects_nested_footer_ranges_not_proven_by_pages(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/nested.parquet"
    storage.write_parquet(pa.table({
        "nested": pa.StructArray.from_arrays(
            [pa.array([1, 2], type=pa.int64())],
            names=["x"],
        ),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "nested": "Struct({'x': Int64})",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            "columns": 3,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    real_stats_rows = processing_module._stats_rows_for_metadata
    source_footer_digest = processing_module.parquet_footer_sha256(
        pq.read_metadata(tmp_path / data_path),
    )

    def forge_nested_range(*args, **kwargs):
        rows = real_stats_rows(*args, **kwargs)
        if processing_module.parquet_footer_sha256(args[1]) != source_footer_digest:
            return rows
        for row in rows:
            if row["column_name"] == "nested.x":
                row["min_bigint"] = 999
                row["max_bigint"] = 999
                row["stats_available"] = True
        return rows

    monkeypatch.setattr(
        processing_module,
        "_stats_rows_for_metadata",
        forge_nested_range,
    )
    with pytest.raises(
        RuntimeError,
        match="footer statistics disagree with decoded data",
    ):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_accepts_list_array_and_duration_columns(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series(
            [[1, None], None, [], [3, 4]],
            dtype=pl.List(pl.Int64),
        ),
        "pair": pl.Series(
            [[1, 2], [3, 4], [5, 6], [7, 8]],
            dtype=pl.Array(pl.Int64, 2),
        ),
        "elapsed": pl.Series(
            [
                timedelta(microseconds=3),
                None,
                timedelta(microseconds=7),
                timedelta(microseconds=-2),
            ],
            dtype=pl.Duration("us"),
        ),
        "__rowid__": pl.Series([10, 11, 12, 13], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                datetime(2026, 1, 4, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "items": "List(Int64)",
        "pair": "Array(Int64, shape=(2,))",
        "elapsed": "Duration(time_unit='us')",
    }
    assert snapshot_proves_stable_rowids(leaf["payload"]) is True


def test_v2_4_migration_accepts_time_and_null_declared_types(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "clock": pl.Series(
            [time(1, 2, 3), None],
            dtype=pl.Time,
        ),
        "nothing": pl.Series([None, None], dtype=pl.Null),
        "__rowid__": pl.Series([10, 11], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "clock": "Time",
        "nothing": "Null",
    }


@pytest.mark.parametrize(
    ("writer", "shape"),
    [("arrow", (2,)), ("polars", (2,)), ("polars", (2, 2))],
    ids=["arrow-1d", "polars-1d", "polars-2d"],
)
def test_v2_4_migration_accepts_nullable_fixed_size_arrays(
    tmp_path, writer, shape,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    values = (
        [None, [1, 2], [3, None]]
        if shape == (2,)
        else [[[1, 2], [3, 4]], None, [[5, 6], [7, 8]]]
    )
    frame = pl.DataFrame({
        "pair": pl.Series(
            values,
            dtype=pl.Array(pl.Int64, shape),
        ),
        "__rowid__": pl.Series([10, 11, 12], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    encoded = io.BytesIO()
    if writer == "arrow":
        pq.write_table(frame.to_arrow(), encoded, write_statistics=True)
    else:
        frame.write_parquet(encoded, statistics=True)
    data_path = "org/lake/tables/facts/data/v2-4-types.parquet"
    storage.write_bytes(data_path, encoded.getvalue())
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "pair": f"Array(Int64, shape={shape!r})",
    }


def test_v2_4_migration_accepts_nullable_array_nested_in_struct(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    nested_type = pl.Struct({"pair": pl.Array(pl.Int64, 2)})
    frame = pl.DataFrame({
        "nested": pl.Series(
            [{"pair": [1, 2]}, None, {"pair": [3, 4]}],
            dtype=nested_type,
        ),
        "__rowid__": pl.Series([10, 11, 12], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    encoded = io.BytesIO()
    frame.write_parquet(encoded, statistics=True)
    data_path = "org/lake/tables/facts/data/v2-4-types.parquet"
    storage.write_bytes(data_path, encoded.getvalue())
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {"nested": str(nested_type)}


def test_v2_4_fixed_width_array_proof_is_batched(tmp_path, monkeypatch):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    row_count = 100
    frame = pl.DataFrame({
        "pair": pl.Series(
            [[value, value + 1] for value in range(row_count)],
            dtype=pl.Array(pl.Int64, 2),
        ),
        "__rowid__": pl.Series(
            list(range(10, 10 + row_count)),
            dtype=pl.Int64,
        ),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * row_count,
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_write_table = pq.write_table
    proof_writes = 0

    def count_proof_writes(*args, **kwargs):
        nonlocal proof_writes
        proof_writes += 1
        return original_write_table(*args, **kwargs)

    monkeypatch.setattr(pq, "write_table", count_proof_writes)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    # One bounded proof batch per validation pass, with headroom for codec
    # implementation details. The old path performed two writes per row.
    assert proof_writes <= 8


def test_v2_4_variable_width_array_proof_is_batched(tmp_path, monkeypatch):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    row_count = 100
    frame = pl.DataFrame({
        "pair": pl.Series(
            [[f"left-{value}", f"right-{value}"] for value in range(row_count)],
            dtype=pl.Array(pl.String, 2),
        ),
        "__rowid__": pl.Series(
            list(range(10, 10 + row_count)),
            dtype=pl.Int64,
        ),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * row_count,
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_write_table = pq.write_table
    original_popen = subprocess.Popen
    proof_writes = 0
    worker_invocations = []

    def count_proof_writes(*args, **kwargs):
        nonlocal proof_writes
        proof_writes += 1
        return original_write_table(*args, **kwargs)

    def record_worker_invocation(*args, **kwargs):
        worker_invocations.append((list(args[0]), dict(kwargs.get("env", {}))))
        return original_popen(*args, **kwargs)

    monkeypatch.setattr(pq, "write_table", count_proof_writes)
    monkeypatch.setattr(subprocess, "Popen", record_worker_invocation)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    assert proof_writes <= 8
    assert worker_invocations
    assert all(
        environment.get("POLARS_MAX_THREADS") == "1"
        and 'parallel="none"' in command[2]
        and 'low_memory=True' in command[2]
        and 'cache=False' in command[2]
        for command, environment in worker_invocations
    )


def test_v2_4_migration_rejects_oversized_array_page_before_decode(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    value = "x" * (5 * 1024 * 1024)
    frame = pl.DataFrame({
        "pair": pl.Series(
            [[f"{value}a", f"{value}b"]],
            dtype=pl.Array(pl.String, 2),
        ),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    snapshot = _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    data_path = snapshot["resources"][0]["file"]
    footer = pq.read_metadata(tmp_path / data_path)
    assert footer.row_group(0).total_byte_size > 8 * 1024 * 1024

    def unexpected_decode(*args, **kwargs):
        raise AssertionError("oversized fixed Array reached the decoder")

    monkeypatch.setattr(pl, "scan_parquet", unexpected_decode)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="Array page exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_isolates_array_dictionary_amplification(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    value = "x" * (1024 * 1024)
    width = 16
    frame = pl.DataFrame({
        "items": pl.Series(
            [[value] * width],
            dtype=pl.Array(pl.String, width),
        ),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    snapshot = _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    data_path = snapshot["resources"][0]["file"]
    footer = pq.read_metadata(tmp_path / data_path)
    assert footer.row_group(0).total_byte_size < 8 * 1024 * 1024
    assert frame.estimated_size() > 8 * 1024 * 1024

    def unexpected_parent_decode(*args, **kwargs):
        raise AssertionError("amplified Array reached the parent decoder")

    monkeypatch.setattr(pl, "scan_parquet", unexpected_parent_decode)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="logical batch exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_array_worker_has_a_progress_deadline(tmp_path, monkeypatch):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    monkeypatch.setattr(
        super_table_module,
        "_MAX_MIGRATION_ARRAY_WORKER_STALL_SECONDS",
        0.0,
        raising=False,
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(TimeoutError, match="Array worker stalled"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_array_worker_samples_memory_before_ready_output(
    tmp_path, monkeypatch,
):
    import builtins
    import select as select_module

    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_open = builtins.open
    status_reads = 0

    def record_status_reads(file, *args, **kwargs):
        nonlocal status_reads
        if str(file).startswith("/proc/") and str(file).endswith("/status"):
            status_reads += 1
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", record_status_reads)
    monkeypatch.setattr(
        select_module,
        "select",
        lambda readable, _writable, _errors, _timeout: (readable, [], []),
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    assert status_reads >= 2


def test_v2_4_array_worker_rejects_peak_memory_over_limit(
    tmp_path, monkeypatch,
):
    import builtins

    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_open = builtins.open

    def peak_status(file, *args, **kwargs):
        if str(file).startswith("/proc/") and str(file).endswith("/status"):
            return io.StringIO(
                "Name:\tpython\n"
                "State:\tR (running)\n"
                "VmHWM:\t262145 kB\n"
                "VmRSS:\t1 kB\n"
            )
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", peak_status)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="logical batch exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_array_worker_uses_private_batch_directory(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_read_parquet = pl.read_parquet
    observed_modes = []

    def record_proof_permissions(source, *args, **kwargs):
        if str(source).endswith("proof.parquet"):
            observed_modes.append((
                os.stat(os.path.dirname(str(source))).st_mode & 0o777,
                os.stat(source).st_mode & 0o777,
            ))
        return original_read_parquet(source, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", record_proof_permissions)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == ["facts"]
    assert observed_modes
    assert set(observed_modes) == {(0o700, 0o600)}


def test_v2_4_array_worker_checks_encoded_bound_before_parent_decode(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_getsize = os.path.getsize
    original_read_parquet = pl.read_parquet

    def oversized_proof(path):
        if str(path).endswith("proof.parquet"):
            return 16 * 1024 * 1024 + 1
        return original_getsize(path)

    def reject_unbounded_proof(source, *args, **kwargs):
        if str(source).endswith("proof.parquet"):
            raise AssertionError("oversized proof reached parent decoder")
        return original_read_parquet(source, *args, **kwargs)

    monkeypatch.setattr(os.path, "getsize", oversized_proof)
    monkeypatch.setattr(pl, "read_parquet", reject_unbounded_proof)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(RuntimeError, match="isolated batch contract failed"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_array_worker_closes_immediately_after_parent_proof_failure(
    tmp_path, monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
        "__rowid__": pl.Series([10], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    original_popen = subprocess.Popen
    workers = []
    worker_paths = []

    def record_worker(*args, **kwargs):
        command = list(args[0])
        process = original_popen(*args, **kwargs)
        workers.append(process)
        worker_paths.extend(command[3:6])
        return process

    def fail_parent_proof(*args, **kwargs):
        raise RuntimeError("forced parent proof failure")

    monkeypatch.setattr(subprocess, "Popen", record_worker)
    monkeypatch.setattr(pq, "write_table", fail_parent_proof)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    try:
        with pytest.raises(RuntimeError) as retained_failure:
            st.migrate_legacy_metadata(confirm_system_offline=True)
        assert retained_failure.traceback is not None
        assert workers
        assert all(process.poll() is not None for process in workers)
        assert not any(os.path.exists(path) for path in worker_paths)
        assert not any(
            os.path.exists(os.path.dirname(path)) for path in worker_paths
        )
    finally:
        for process in workers:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=2)


def test_v2_4_migration_accepts_large_fixed_width_array_row_group(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    row_count = 20_000
    width = 64
    values = np.arange(row_count * width, dtype=np.float64).reshape(
        row_count,
        width,
    )
    frame = pl.DataFrame({
        "vector": pl.Series(
            "vector",
            values,
            dtype=pl.Array(pl.Float64, width),
        ),
        "__rowid__": pl.Series(
            np.arange(10, 10 + row_count, dtype=np.int64),
            dtype=pl.Int64,
        ),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * row_count,
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    snapshot = _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    data_path = snapshot["resources"][0]["file"]
    footer = pq.read_metadata(tmp_path / data_path)
    assert footer.row_group(0).total_byte_size > 8 * 1024 * 1024
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5


def test_v2_4_migration_rejects_an_oversized_indivisible_row(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    table = pa.table({
        "payload": pa.array([b"x" * (8 * 1024 * 1024 + 1)], type=pa.binary()),
        "__rowid__": pa.array([10], type=pa.int64()),
        "__timestamp__": pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        table,
        {
            "payload": "Binary",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="row exceeds the migration decode limit"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_accepts_nested_list_definition_levels(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    nested_type = pa.list_(pa.struct([pa.field("value", pa.int64())]))
    table = pa.table({
        "nested": pa.array(
            [
                None,
                [],
                [None],
                [{"value": None}, {"value": 2}],
            ],
            type=nested_type,
        ),
        "__rowid__": pa.array([10, 11, 12, 13], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                datetime(2026, 1, 4, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        table,
        {
            "nested": "List(Struct({'value': Int64}))",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "nested": "List(Struct({'value': Int64}))",
    }


def test_v2_4_migration_accepts_polars_fallback_list_statistics(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "items": pl.Series(
            [None, [], [None], [1, None, 3]],
            dtype=pl.List(pl.Int64),
        ),
        "__rowid__": pl.Series([10, 11, 12, 13], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                datetime(2026, 1, 4, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    encoded = io.BytesIO()
    frame.write_parquet(encoded, statistics=True)
    data_path = "org/lake/tables/facts/data/v2-4-types.parquet"
    storage.write_bytes(data_path, encoded.getvalue())
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {"items": "List(Int64)"}


def test_v2_4_migration_preserves_categorical_and_enum_schema(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "kind": pl.Series(
            ["alpha", "beta", None],
            dtype=pl.Categorical,
        ),
        "state": pl.Series(
            ["new", "done", None],
            dtype=pl.Enum(["new", "done"]),
        ),
        "__rowid__": pl.Series([10, 11, 12], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "kind": "Categorical",
        "state": "Enum(categories=['new', 'done'])",
    }


def test_v2_4_migration_accepts_heterogeneous_append_schemas(tmp_path):
    """v2.4 appended files freely and kept only the latest logical schema."""
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_frame = pl.DataFrame({
        "value": pl.Series([1, 2], dtype=pl.Int64),
        "items": pl.Series([[1], [2, 3]], dtype=pl.List(pl.Int64)),
        "state": pl.Series(
            ["old", "shared"],
            dtype=pl.Enum(["old", "shared"]),
        ),
        "__rowid__": pl.Series([10, 11], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    new_frame = pl.DataFrame({
        "value": pl.Series([3.5, 4.5], dtype=pl.Float64),
        "items": pl.Series([[4.5], [5.5]], dtype=pl.List(pl.Float64)),
        "state": pl.Series(
            ["new", "shared"],
            dtype=pl.Enum(["new", "shared"]),
        ),
        "__rowid__": pl.Series([12, 13], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                datetime(2026, 1, 4, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    resources = []
    for index, frame in enumerate((old_frame, new_frame)):
        data_path = f"org/lake/tables/facts/data/v2-4-{index}.parquet"
        storage.write_parquet(frame.to_arrow(), data_path)
        resources.append({
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        })
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            name: str(dtype) for name, dtype in new_frame.schema.items()
        },
        "resources": resources,
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)
    rerun = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert rerun["migrated_tables"] == []
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "value": "Float64",
        "items": "List(Float64)",
        "state": "Enum(categories=['new', 'shared'])",
    }


def test_v2_4_migration_rejects_heterogeneous_fixed_array_shapes(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    resources = []
    frames = (
        pl.DataFrame({
            "items": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
            "__rowid__": pl.Series([10], dtype=pl.Int64),
            "__timestamp__": pl.Series(
                [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                dtype=pl.Datetime("us", "UTC"),
            ),
        }),
        pl.DataFrame({
            "items": pl.Series([[3, 4, 5]], dtype=pl.Array(pl.Int64, 3)),
            "__rowid__": pl.Series([11], dtype=pl.Int64),
            "__timestamp__": pl.Series(
                [datetime(2026, 1, 2, tzinfo=timezone.utc)],
                dtype=pl.Datetime("us", "UTC"),
            ),
        }),
    )
    for index, frame in enumerate(frames):
        data_path = f"org/lake/tables/facts/data/v2-4-array-{index}.parquet"
        storage.write_parquet(frame.to_arrow(), data_path)
        resources.append({
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        })
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            name: str(dtype) for name, dtype in frames[-1].schema.items()
        },
        "resources": resources,
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="not query-compatible"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4
    assert leaf["path"] == old_path
    assert leaf["payload"] == snapshot


def test_v2_4_migration_accepts_polars_all_null_enum_statistics(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "state": pl.Series(
            [None, None],
            dtype=pl.Enum(["z", "a"]),
        ),
        "__rowid__": pl.Series([10, 11], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    encoded = io.BytesIO()
    frame.write_parquet(encoded, statistics=True)
    data_path = "org/lake/tables/facts/data/v2-4-types.parquet"
    storage.write_bytes(data_path, encoded.getvalue())
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": frame.height,
            "columns": frame.width,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    result = st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert result["migrated_tables"] == ["facts"]
    assert leaf is not None and leaf["version"] == 5
    assert leaf["payload"]["schema"] == {
        "state": "Enum(categories=['z', 'a'])",
    }


@pytest.mark.parametrize("reserved_name", ["__file__", "__supertable_user_data"])
def test_v2_4_migration_rejects_retained_reserved_user_columns(
    tmp_path,
    reserved_name,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    table = pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        reserved_name: pa.array(["old", "data"], type=pa.string()),
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage,
        client,
        catalog,
        table,
        {
            # v2.4 used last-write-wins schema metadata while retaining older
            # resources, so a physical legacy column can be absent here.
            "value": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
    )
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    with pytest.raises(ValueError, match="reserved physical column"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_rejects_unavailable_lane_with_false_all_null_count(
    tmp_path,
    monkeypatch,
):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    _seed_authentic_v2_4_active_table(storage, client, catalog)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    real_stats_rows = processing_module._stats_rows_for_metadata
    source_footer_digest = processing_module.parquet_footer_sha256(
        pq.read_metadata(tmp_path / "org/lake/tables/facts/data/v2-4.parquet"),
    )

    def forge_binary_all_null_count(*args, **kwargs):
        rows = real_stats_rows(*args, **kwargs)
        if processing_module.parquet_footer_sha256(args[1]) != source_footer_digest:
            return rows
        for row in rows:
            if row["column_name"] == "payload":
                assert row["stats_available"] is False
                row["null_count"] = row["row_group_rows"]
        return rows

    monkeypatch.setattr(
        processing_module,
        "_stats_rows_for_metadata",
        forge_binary_all_null_count,
    )
    with pytest.raises(
        RuntimeError,
        match="footer statistics disagree with decoded data",
    ):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_migration_rejects_resource_metadata_that_disagrees_with_parquet(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
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
        st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None and leaf["version"] == 4


def test_v2_4_migration_preserves_logical_column_count_for_structs(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/v2-4.json"
    data_path = "org/lake/tables/facts/data/nested.parquet"
    nested = pa.StructArray.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array(["a", "b"], type=pa.string()),
        ],
        names=["x", "y"],
    )
    storage.write_parquet(pa.table({
        "nested": nested,
        "__rowid__": pa.array([10, 11], type=pa.int64()),
        "__timestamp__": pa.array(
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }), data_path)
    snapshot = {
        "simple_name": "facts",
        "location": "org/lake/tables/facts",
        "snapshot_version": 4,
        "last_updated_ms": 1,
        "previous_snapshot": None,
        "schema": {
            "nested": "Struct({'x': Int64, 'y': String})",
            "__rowid__": "Int64",
            "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')",
        },
        "resources": [{
            "file": data_path,
            "file_size": storage.size(data_path),
            "rows": 2,
            # v2.4 records Arrow's logical top-level field count, not the
            # number of physical Parquet leaves inside the Struct.
            "columns": 3,
        }],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": None,
        "stats_rows": 0,
    }
    storage.write_json(old_path, snapshot)
    _seed_migration_catalog(catalog, client, snapshot, old_path)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog

    st.migrate_legacy_metadata(confirm_system_offline=True)

    leaf = catalog.get_leaf("org", "lake", "facts")
    assert leaf is not None
    migrated = leaf["payload"]
    assert migrated["resources"][0]["columns"] == 3
    assert migrated["schema"] == {
        "nested": "Struct({'x': Int64, 'y': String})",
    }


def test_migration_republishes_valid_v2_manifest_for_successor(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = "org/lake/tables/facts/snapshots/old.json"
    data_path = "org/lake/tables/facts/data/legacy.parquet"
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
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
        "resources": [
            _stable_resource_for_test(
                tmp_path, storage, data_path, [1, 2],
            ),
        ],
        "rowid_high_watermark": 2,
        "_row_filter": None,
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
    st.migrate_legacy_metadata(confirm_system_offline=True)

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
    storage.write_parquet(pa.table({
        "value": pa.array([1, 2], type=pa.int64()),
        "__rowid__": pa.array([1, 2], type=pa.int64()),
    }), data_path)
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
        "resources": [
            _stable_resource_for_test(
                tmp_path, storage, data_path, [1, 2],
            ),
        ],
        "rowid_high_watermark": 2,
        "_row_filter": None,
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

    st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
        st.migrate_legacy_metadata(confirm_system_offline=True)

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
