"""Metadata-known migration failures must precede retained-data decoding."""

import gc
from unittest.mock import Mock

import fakeredis
import pytest

import supertable.simple_table as simple_table_module
import supertable.super_table as super_table_module
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable
from supertable.tests.test_super_table_migration import (
    _seed_authentic_v2_4_active_table,
    _seed_migration_catalog,
)


@pytest.fixture(autouse=True)
def collect_storage_handles_after_test():
    # This autouse fixture starts before the explicitly requested monkeypatch
    # fixture, so collection runs after its wrapped bound methods are restored.
    # Release their reference cycles and LocalStorage directory pins promptly.
    yield
    gc.collect()


def _admission_table(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    old_path = _seed_authentic_v2_4_active_table(storage, client, catalog)
    st = SuperTable.__new__(SuperTable)
    st.organization = "org"
    st.super_name = "lake"
    st.storage = storage
    st.catalog = catalog
    return st, client, old_path


@pytest.mark.parametrize(
    ("module", "limit", "value", "message"),
    [
        (
            super_table_module,
            "_MAX_MIGRATION_STATS_ROWS",
            0,
            "statistics materialization exceeds",
        ),
        (
            super_table_module,
            "_MAX_MIGRATION_STATS_DECODED_BYTES",
            1024,
            "statistics materialization exceeds",
        ),
        (
            simple_table_module,
            "_MAX_RESTORE_TOMBSTONE_ROWS",
            0,
            "deletion-vector row count exceeds",
        ),
        (
            simple_table_module,
            "_MAX_RESTORE_TOMBSTONE_BYTES",
            1,
            "deletion-vector object exceeds",
        ),
        (
            simple_table_module,
            "_MAX_RESTORE_TOMBSTONE_DECODED_BYTES",
            1,
            "deletion vector exceeds its decoded-byte limit",
        ),
    ],
)
def test_legacy_metadata_limits_reject_before_data_scan_or_publication(
    tmp_path, monkeypatch, module, limit, value, message,
):
    st, _client, old_path = _admission_table(tmp_path)
    previous_leaf = st.catalog.get_leaf("org", "lake", "facts")
    previous_root = st.catalog.get_root("org", "lake")
    scan = Mock(side_effect=AssertionError("retained data must not be scanned"))
    monkeypatch.setattr(st, "_scan_v2_4_resources", scan)
    monkeypatch.setattr(module, limit, value)
    writes = Mock(wraps=st.storage.write_bytes)
    json_writes = Mock(wraps=st.storage.write_json)
    monkeypatch.setattr(st.storage, "write_bytes", writes)
    monkeypatch.setattr(st.storage, "write_json", json_writes)

    with pytest.raises(ValueError, match=message):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    scan.assert_not_called()
    writes.assert_not_called()
    json_writes.assert_not_called()
    assert st.catalog.get_leaf("org", "lake", "facts") == previous_leaf
    assert st.catalog.get_root("org", "lake") == previous_root
    assert previous_leaf["path"] == old_path


def test_legacy_tombstone_footer_count_rejects_before_data_scan(
    tmp_path, monkeypatch,
):
    st, client, old_path = _admission_table(tmp_path)
    snapshot = st.storage.read_json(old_path)
    snapshot["tombstone_rows"] = 2
    st.storage.write_json(old_path, snapshot)
    _seed_migration_catalog(st.catalog, client, snapshot, old_path)
    previous_leaf = st.catalog.get_leaf("org", "lake", "facts")
    scan = Mock(side_effect=AssertionError("retained data must not be scanned"))
    monkeypatch.setattr(st, "_scan_v2_4_resources", scan)

    with pytest.raises(ValueError, match="deletion-vector row-count seal is invalid"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    scan.assert_not_called()
    assert st.catalog.get_leaf("org", "lake", "facts") == previous_leaf


def test_legacy_tombstone_count_exceeds_physical_rows_before_data_scan(
    tmp_path, monkeypatch,
):
    st, client, old_path = _admission_table(tmp_path)
    snapshot = st.storage.read_json(old_path)
    snapshot["tombstone_rows"] = 3
    st.storage.write_json(old_path, snapshot)
    _seed_migration_catalog(st.catalog, client, snapshot, old_path)
    scan = Mock(side_effect=AssertionError("retained data must not be scanned"))
    monkeypatch.setattr(st, "_scan_v2_4_resources", scan)

    with pytest.raises(ValueError, match="deletion-vector row count exceeds"):
        st.migrate_legacy_metadata(confirm_system_offline=True)

    scan.assert_not_called()
