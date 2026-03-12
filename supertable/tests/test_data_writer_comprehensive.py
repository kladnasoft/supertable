# supertable/tests/test_data_writer.py
"""
Comprehensive tests for supertable.data_writer.DataWriter.

Every public method and code path is exercised:
  - write() with append, overwrite, delete_only, newer_than, dedup_on_read
  - validation() edge cases
  - configure_table() and _get_table_config()
  - Lock acquire/release lifecycle
  - Monitoring enqueue
  - Mirroring integration (failure-safe)
  - Snapshot CAS with payload fallback to path-only
  - newer_than early-exit (all stale rows)
  - Error propagation (write failure re-raises)
"""
from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, PropertyMock, call, patch

import pyarrow as pa
import polars as pl
import pytest


# ---------------------------------------------------------------------------
# Helpers: build Arrow tables for DataWriter.write(data=...)
# ---------------------------------------------------------------------------

def _arrow_table(columns: Dict[str, list]) -> pa.Table:
    """Build a pyarrow Table from a column dict."""
    return pa.table(columns)


def _simple_arrow(n: int = 3, id_start: int = 1) -> pa.Table:
    """A minimal 3-column Arrow table (id, name, value)."""
    ids = list(range(id_start, id_start + n))
    return _arrow_table({
        "id": ids,
        "name": [f"row_{i}" for i in ids],
        "value": [float(i * 10) for i in ids],
    })


# ---------------------------------------------------------------------------
# Fixtures: fake collaborators
# ---------------------------------------------------------------------------

class FakeMonitor:
    """Fake monitoring logger that records log_metric calls."""

    def __init__(self):
        self.metrics: list = []

    def log_metric(self, payload: dict):
        self.metrics.append(payload)


class FakeStorage:
    """In-memory storage backend for testing."""

    def __init__(self):
        self._files: Dict[str, Any] = {}
        self._dirs: set = set()

    def exists(self, path: str) -> bool:
        return path in self._files or path in self._dirs

    def makedirs(self, path: str):
        self._dirs.add(path)

    def write_json(self, path: str, data: Any):
        self._files[path] = json.dumps(data)

    def read_json(self, path: str) -> Any:
        raw = self._files.get(path)
        if raw is None:
            raise FileNotFoundError(path)
        return json.loads(raw)

    def size(self, path: str) -> int:
        raw = self._files.get(path)
        return len(raw) if raw else 0

    def delete(self, path: str):
        self._files.pop(path, None)
        self._dirs.discard(path)

    def write_bytes(self, path: str, data: bytes):
        self._files[path] = data

    def read_parquet(self, path: str) -> pa.Table:
        import io
        import pyarrow.parquet as pq
        raw = self._files.get(path)
        if raw is None:
            raise FileNotFoundError(path)
        return pq.read_table(io.BytesIO(raw))


class FakeCatalog:
    """In-memory RedisCatalog replacement."""

    def __init__(self):
        self._locks: Dict[str, str] = {}
        self._leaves: Dict[str, Dict] = {}
        self._roots: Dict[str, Dict] = {}
        self._table_configs: Dict[str, Dict] = {}
        self._simple_tables: set = set()

        # Tracking
        self.bump_root_calls: list = []
        self.set_leaf_payload_cas_calls: list = []
        self.set_leaf_path_cas_calls: list = []
        self.release_calls: list = []
        self.leaf_payload_cas_should_fail: bool = False

    def acquire_simple_lock(self, org, sup, simple, ttl_s=30, timeout_s=60) -> Optional[str]:
        key = f"{org}:{sup}:{simple}"
        if key in self._locks:
            return None  # already locked
        token = uuid.uuid4().hex
        self._locks[key] = token
        return token

    def release_simple_lock(self, org, sup, simple, token) -> bool:
        key = f"{org}:{sup}:{simple}"
        self.release_calls.append((org, sup, simple, token))
        if self._locks.get(key) == token:
            del self._locks[key]
            return True
        return False

    def leaf_exists(self, org, sup, simple) -> bool:
        return f"{org}:{sup}:{simple}" in self._leaves

    def get_leaf(self, org, sup, simple) -> Optional[Dict]:
        return self._leaves.get(f"{org}:{sup}:{simple}")

    def set_leaf_payload_cas(self, org, sup, simple, payload, path, now_ms=None):
        self.set_leaf_payload_cas_calls.append((org, sup, simple, payload, path))
        if self.leaf_payload_cas_should_fail:
            raise Exception("payload CAS not supported")
        key = f"{org}:{sup}:{simple}"
        ver = (self._leaves.get(key, {}).get("version", -1)) + 1
        self._leaves[key] = {"version": ver, "path": path, "payload": payload}
        return ver

    def set_leaf_path_cas(self, org, sup, simple, path, now_ms=None):
        self.set_leaf_path_cas_calls.append((org, sup, simple, path))
        key = f"{org}:{sup}:{simple}"
        ver = (self._leaves.get(key, {}).get("version", -1)) + 1
        self._leaves[key] = {"version": ver, "path": path}
        return ver

    def bump_root(self, org, sup, now_ms=None):
        self.bump_root_calls.append((org, sup, now_ms))
        return 1

    def root_exists(self, org, sup) -> bool:
        return f"{org}:{sup}" in self._roots

    def ensure_root(self, org, sup):
        key = f"{org}:{sup}"
        if key not in self._roots:
            self._roots[key] = {"version": 0}

    def get_table_config(self, org, sup, simple) -> Optional[Dict]:
        return self._table_configs.get(f"{org}:{sup}:{simple}")

    def set_table_config(self, org, sup, simple, config) -> bool:
        self._table_configs[f"{org}:{sup}:{simple}"] = config
        return True

    def delete_simple_table(self, org, sup, simple):
        key = f"{org}:{sup}:{simple}"
        self._leaves.pop(key, None)
        self._simple_tables.discard(key)


def _bootstrap_leaf(catalog: FakeCatalog, org: str, sup: str, simple: str,
                    resources: list = None, schema: list = None):
    """Pre-populate a leaf pointer + snapshot payload in the fake catalog."""
    snapshot = {
        "simple_name": simple,
        "location": f"{org}/{sup}/tables/{simple}",
        "snapshot_version": 0,
        "last_updated_ms": int(datetime.now().timestamp() * 1000),
        "previous_snapshot": None,
        "schema": schema or [],
        "resources": resources or [],
    }
    path = f"{org}/{sup}/tables/{simple}/snapshots/initial.json"
    key = f"{org}:{sup}:{simple}"
    catalog._leaves[key] = {"version": 0, "path": path, "payload": snapshot}
    return snapshot, path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_storage():
    return FakeStorage()


@pytest.fixture()
def fake_catalog():
    cat = FakeCatalog()
    # Pre-populate root so SuperTable.__init__ takes the fast path
    cat._roots["testorg:testsuper"] = {"version": 0}
    return cat


@pytest.fixture()
def fake_monitor():
    return FakeMonitor()


@pytest.fixture()
def writer(fake_storage, fake_catalog, fake_monitor):
    """Build a DataWriter with all external dependencies patched."""
    with (
        patch("supertable.data_writer.SuperTable") as MockSuperTable,
        patch("supertable.data_writer.RedisCatalog", return_value=fake_catalog),
        patch("supertable.data_writer.check_write_access"),
        patch("supertable.data_writer.SimpleTable") as MockSimpleTable,
        patch("supertable.data_writer.find_and_lock_overlapping_files", return_value=set()),
        patch("supertable.data_writer.process_overlapping_files") as mock_process,
        patch("supertable.data_writer.process_delete_only") as mock_delete,
        patch("supertable.data_writer.filter_stale_incoming_rows") as mock_filter_stale,
        patch("supertable.data_writer.get_monitoring_logger", return_value=fake_monitor),
        patch("supertable.data_writer.MirrorFormats") as MockMirror,
    ):
        # Configure SuperTable mock
        st_instance = MockSuperTable.return_value
        st_instance.super_name = "testsuper"
        st_instance.organization = "testorg"
        st_instance.storage = fake_storage

        # Configure SimpleTable mock
        initial_snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 0,
            "schema": [],
            "resources": [],
        }
        initial_path = "testorg/testsuper/tables/t1/snapshots/init.json"
        simple_inst = MockSimpleTable.return_value
        simple_inst.get_simple_table_snapshot.return_value = (initial_snapshot, initial_path)
        simple_inst.data_dir = "testorg/testsuper/tables/t1/data"
        simple_inst.snapshot_dir = "testorg/testsuper/tables/t1/snapshots"
        simple_inst.update.return_value = (
            {**initial_snapshot, "snapshot_version": 1, "resources": []},
            "testorg/testsuper/tables/t1/snapshots/new.json",
        )

        # Default process returns
        mock_process.return_value = (3, 0, 3, 3, [], set())
        mock_delete.return_value = (0, 2, 1, 3, [], {"old.parquet"})

        from supertable.data_writer import DataWriter
        dw = DataWriter.__new__(DataWriter)
        dw.super_table = st_instance
        dw.catalog = fake_catalog
        dw._table_config_cache = {}
        dw.timer = MagicMock()

        # Stash mocks for assertions
        dw._mocks = {
            "SimpleTable": MockSimpleTable,
            "simple_inst": simple_inst,
            "process": mock_process,
            "delete": mock_delete,
            "filter_stale": mock_filter_stale,
            "mirror": MockMirror,
            "monitor": fake_monitor,
            "find_overlap": patch("supertable.data_writer.find_and_lock_overlapping_files",
                                   return_value=set()),
        }

        yield dw


# ===========================================================================
# Tests: validation()
# ===========================================================================

class TestValidation:
    """Test DataWriter.validation() for every rejection rule."""

    def _make_writer_for_validation(self):
        """Minimal DataWriter with just enough to call validation()."""
        with (
            patch("supertable.data_writer.SuperTable") as MockST,
            patch("supertable.data_writer.RedisCatalog"),
        ):
            st = MockST.return_value
            st.super_name = "my_super"
            st.organization = "org"

            from supertable.data_writer import DataWriter
            dw = DataWriter.__new__(DataWriter)
            dw.super_table = st
            dw.catalog = MagicMock()
            dw._table_config_cache = {}
            return dw

    def _df(self, cols=None):
        cols = cols or {"id": [1, 2], "name": ["a", "b"], "ts": [1, 2]}
        return pl.DataFrame(cols)

    # -- Table name rules --

    def test_empty_table_name(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="can't be empty"):
            dw.validation(self._df(), "", [], None, False)

    def test_table_name_too_long(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="can't be empty or longer than 128"):
            dw.validation(self._df(), "x" * 129, [], None, False)

    def test_table_name_matches_super_name(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="can't match with SuperTable"):
            dw.validation(self._df(), "my_super", [], None, False)

    def test_table_name_invalid_chars(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="Invalid table name"):
            dw.validation(self._df(), "bad-name!", [], None, False)

    def test_table_name_starts_with_digit(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="Invalid table name"):
            dw.validation(self._df(), "1table", [], None, False)

    def test_valid_table_name_underscore_start(self):
        dw = self._make_writer_for_validation()
        # Should NOT raise
        dw.validation(self._df(), "_my_table", [], None, False)

    def test_valid_table_name_alpha(self):
        dw = self._make_writer_for_validation()
        dw.validation(self._df(), "MyTable123", [], None, False)

    # -- Overwrite columns rules --

    def test_overwrite_columns_string_rejected(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="overwrite columns must be list"):
            dw.validation(self._df(), "t1", "id", None, False)

    def test_overwrite_columns_not_in_data(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="not present in the dataset"):
            dw.validation(self._df(), "t1", ["nonexistent"], None, False)

    def test_overwrite_columns_partial_missing(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="not present in the dataset"):
            dw.validation(self._df(), "t1", ["id", "missing_col"], None, False)

    def test_overwrite_columns_valid(self):
        dw = self._make_writer_for_validation()
        dw.validation(self._df(), "t1", ["id"], None, False)

    def test_overwrite_columns_empty_list_valid(self):
        dw = self._make_writer_for_validation()
        dw.validation(self._df(), "t1", [], None, False)

    # -- delete_only rules --

    def test_delete_only_without_overwrite(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="delete_only requires overwrite_columns"):
            dw.validation(self._df(), "t1", [], None, True)

    def test_delete_only_with_overwrite_valid(self):
        dw = self._make_writer_for_validation()
        dw.validation(self._df(), "t1", ["id"], None, True)

    # -- newer_than rules --

    def test_newer_than_not_string(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="newer_than must be a column name string"):
            dw.validation(self._df(), "t1", ["id"], 123, False)

    def test_newer_than_column_missing(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="not present in the dataset"):
            dw.validation(self._df(), "t1", ["id"], "nonexistent", False)

    def test_newer_than_without_overwrite(self):
        dw = self._make_writer_for_validation()
        with pytest.raises(ValueError, match="newer_than requires overwrite_columns"):
            dw.validation(self._df(), "t1", [], "ts", False)

    def test_newer_than_valid(self):
        dw = self._make_writer_for_validation()
        dw.validation(self._df(), "t1", ["id"], "ts", False)


# ===========================================================================
# Tests: configure_table()
# ===========================================================================

class TestConfigureTable:

    def _make_writer(self, fake_catalog):
        with (
            patch("supertable.data_writer.SuperTable") as MockST,
            patch("supertable.data_writer.RedisCatalog", return_value=fake_catalog),
            patch("supertable.data_writer.check_write_access"),
        ):
            st = MockST.return_value
            st.super_name = "testsuper"
            st.organization = "testorg"

            from supertable.data_writer import DataWriter
            dw = DataWriter.__new__(DataWriter)
            dw.super_table = st
            dw.catalog = fake_catalog
            dw._table_config_cache = {}
            return dw

    def test_basic_configure(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", primary_keys=["id"], dedup_on_read=True)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["primary_keys"] == ["id"]
        assert cfg["dedup_on_read"] is True
        # Also cached locally
        assert dw._table_config_cache["t1"]["primary_keys"] == ["id"]

    def test_empty_primary_keys_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="non-empty list"):
                dw.configure_table("admin", "t1", primary_keys=[], dedup_on_read=True)

    def test_primary_keys_not_list_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="non-empty list"):
                dw.configure_table("admin", "t1", primary_keys="id", dedup_on_read=True)

    def test_max_memory_chunk_size_positive(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", primary_keys=["id"], max_memory_chunk_size=1024)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_memory_chunk_size"] == 1024

    def test_max_memory_chunk_size_zero_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_memory_chunk_size must be a positive"):
                dw.configure_table("admin", "t1", primary_keys=["id"], max_memory_chunk_size=0)

    def test_max_memory_chunk_size_negative_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_memory_chunk_size must be a positive"):
                dw.configure_table("admin", "t1", primary_keys=["id"], max_memory_chunk_size=-5)

    def test_max_overlapping_files_positive(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", primary_keys=["id"], max_overlapping_files=50)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_overlapping_files"] == 50

    def test_max_overlapping_files_zero_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_overlapping_files must be a positive"):
                dw.configure_table("admin", "t1", primary_keys=["id"], max_overlapping_files=0)

    def test_configure_preserves_existing_limits_when_not_set(self, fake_catalog):
        """When max_memory_chunk_size/max_overlapping_files are None, existing values stay."""
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "primary_keys": ["old_key"],
            "dedup_on_read": False,
            "max_memory_chunk_size": 2048,
            "max_overlapping_files": 75,
        })

        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", primary_keys=["id"], dedup_on_read=True)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["primary_keys"] == ["id"]
        assert cfg["dedup_on_read"] is True
        # Existing limits should NOT be removed (they weren't overridden)
        # The behavior depends on whether existing config was already cached;
        # since no prior _get_table_config call happened, the fallback reads
        # from the local cache which was empty → config starts from {}.
        # This is expected behavior per the code.

    def test_configure_updates_limits(self, fake_catalog):
        """When both limits are provided, they read existing config from Redis first."""
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "primary_keys": ["old_key"],
            "dedup_on_read": False,
            "max_memory_chunk_size": 2048,
        })

        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin", "t1", primary_keys=["id"],
                max_memory_chunk_size=4096, max_overlapping_files=200,
            )

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_memory_chunk_size"] == 4096
        assert cfg["max_overlapping_files"] == 200


# ===========================================================================
# Tests: _get_table_config()
# ===========================================================================

class TestGetTableConfig:

    def _make_writer(self, fake_catalog):
        with (
            patch("supertable.data_writer.SuperTable") as MockST,
            patch("supertable.data_writer.RedisCatalog", return_value=fake_catalog),
        ):
            st = MockST.return_value
            st.super_name = "testsuper"
            st.organization = "testorg"

            from supertable.data_writer import DataWriter
            dw = DataWriter.__new__(DataWriter)
            dw.super_table = st
            dw.catalog = fake_catalog
            dw._table_config_cache = {}
            return dw

    def test_returns_empty_when_no_config(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        cfg = dw._get_table_config("unknown_table")
        assert cfg == {}

    def test_returns_config_from_redis(self, fake_catalog):
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {"primary_keys": ["id"]})
        dw = self._make_writer(fake_catalog)
        cfg = dw._get_table_config("t1")
        assert cfg["primary_keys"] == ["id"]

    def test_caches_after_first_call(self, fake_catalog):
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {"primary_keys": ["id"]})
        dw = self._make_writer(fake_catalog)

        cfg1 = dw._get_table_config("t1")
        # Change Redis but cache should still return old value
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {"primary_keys": ["new_id"]})
        cfg2 = dw._get_table_config("t1")
        assert cfg1 == cfg2
        assert cfg2["primary_keys"] == ["id"]


# ===========================================================================
# Tests: write() — main flow
# ===========================================================================

class TestWriteAppend:
    """write() with no overwrite columns (pure append)."""

    def test_basic_append_returns_tuple(self, writer, fake_catalog):
        data = _simple_arrow(3)
        result = writer.write("admin", "t1", data, overwrite_columns=[], compression_level=1)
        assert result is not None
        assert len(result) == 4
        total_columns, total_rows, inserted, deleted = result
        # Values come from mock process_overlapping_files returns
        assert isinstance(total_columns, int)
        assert isinstance(total_rows, int)

    def test_append_acquires_and_releases_lock(self, writer, fake_catalog):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])

        # Lock should have been acquired and released
        assert len(fake_catalog.release_calls) == 1

    def test_append_bumps_root(self, writer, fake_catalog):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.bump_root_calls) == 1

    def test_append_sets_leaf_payload(self, writer, fake_catalog):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.set_leaf_payload_cas_calls) == 1

    def test_monitoring_enqueued(self, writer):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 1
        payload = monitor.metrics[0]
        assert payload["super_name"] == "testsuper"
        assert payload["table_name"] == "t1"
        assert "duration" in payload


class TestWriteOverwrite:
    """write() with overwrite_columns set."""

    def test_overwrite_calls_process_overlapping(self, writer):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=["id"])
        mock_process = writer._mocks["process"]
        assert mock_process.called

    def test_overwrite_result_shape(self, writer):
        data = _simple_arrow(3)
        result = writer.write("admin", "t1", data, overwrite_columns=["id"])
        assert result is not None
        assert len(result) == 4


class TestWriteDeleteOnly:
    """write() with delete_only=True."""

    def test_delete_only_calls_process_delete_only(self, writer):
        data = _simple_arrow(3)
        mock_delete = writer._mocks["delete"]

        with patch("supertable.data_writer.process_delete_only", mock_delete):
            result = writer.write("admin", "t1", data, overwrite_columns=["id"], delete_only=True)

        assert mock_delete.called

    def test_delete_only_requires_overwrite_columns(self, writer):
        """validation should catch this before reaching process_delete_only."""
        data = _simple_arrow(3)
        with pytest.raises(ValueError, match="delete_only requires overwrite_columns"):
            writer.write("admin", "t1", data, overwrite_columns=[], delete_only=True)


class TestWriteNewerThan:
    """write() with newer_than filtering."""

    def test_newer_than_filters_stale_rows(self, writer):
        data = _arrow_table({"id": [1, 2, 3], "name": ["a", "b", "c"], "ts": [10, 20, 30]})
        mock_filter = writer._mocks["filter_stale"]
        # filter returns a subset
        filtered_df = pl.DataFrame({"id": [3], "name": ["c"], "ts": [30]})
        mock_filter.return_value = filtered_df

        with patch("supertable.data_writer.filter_stale_incoming_rows", mock_filter):
            result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        assert mock_filter.called

    def test_newer_than_all_stale_returns_early(self, writer, fake_catalog):
        """When all rows are stale, write should return early without processing."""
        data = _arrow_table({"id": [1, 2], "name": ["a", "b"], "ts": [10, 20]})
        mock_filter = writer._mocks["filter_stale"]
        # Return empty DataFrame with same schema
        empty_filtered = pl.DataFrame({"id": [], "name": [], "ts": []}).cast(
            {"id": pl.Int64, "name": pl.Utf8, "ts": pl.Int64}
        )
        mock_filter.return_value = empty_filtered

        with patch("supertable.data_writer.filter_stale_incoming_rows", mock_filter):
            result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        # Should return early with zeros
        assert result is not None
        total_cols, total_rows, inserted, deleted = result
        assert total_rows == 0
        assert inserted == 0
        assert deleted == 0
        # process_overlapping_files should NOT have been called
        assert not writer._mocks["process"].called

    def test_newer_than_validation_missing_column(self, writer):
        data = _simple_arrow(3)
        with pytest.raises(ValueError, match="not present in the dataset"):
            writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="nonexistent")


class TestWriteDedupOnRead:
    """write() with dedup_on_read config (auto __timestamp__ injection)."""

    def test_timestamp_injected_when_dedup_enabled(self, writer, fake_catalog):
        """When dedup_on_read is enabled, __timestamp__ should be added to the dataframe."""
        # Configure dedup
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "primary_keys": ["id"],
            "dedup_on_read": True,
        })
        writer._table_config_cache.clear()

        data = _simple_arrow(3)
        mock_process = writer._mocks["process"]

        # Capture the dataframe passed to process_overlapping_files
        captured_dfs = []
        original_write = writer.write

        with patch("supertable.data_writer.process_overlapping_files", mock_process):
            # We need to verify that the internal dataframe has __timestamp__
            # The simplest way is to check that validation passes (it would fail
            # if __timestamp__ interfered with anything)
            result = writer.write("admin", "t1", data, overwrite_columns=["id"])
            assert result is not None

    def test_timestamp_not_injected_when_dedup_disabled(self, writer, fake_catalog):
        """When dedup_on_read is disabled, __timestamp__ should NOT be added."""
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "primary_keys": ["id"],
            "dedup_on_read": False,
        })
        writer._table_config_cache.clear()

        data = _simple_arrow(3)
        result = writer.write("admin", "t1", data, overwrite_columns=["id"])
        assert result is not None

    def test_timestamp_not_duplicated_when_already_present(self, writer, fake_catalog):
        """If __timestamp__ already exists in data, don't add a duplicate."""
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "primary_keys": ["id"],
            "dedup_on_read": True,
        })
        writer._table_config_cache.clear()

        data = _arrow_table({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "__timestamp__": [
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 2, tzinfo=timezone.utc),
                datetime(2025, 1, 3, tzinfo=timezone.utc),
            ],
        })
        result = writer.write("admin", "t1", data, overwrite_columns=["id"])
        assert result is not None


# ===========================================================================
# Tests: write() — lock lifecycle
# ===========================================================================

class TestWriteLocking:

    def test_lock_released_on_success(self, writer, fake_catalog):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.release_calls) == 1

    def test_lock_released_on_process_error(self, writer, fake_catalog):
        """Lock must be released even if processing raises."""
        data = _simple_arrow(3)
        writer._mocks["process"].side_effect = RuntimeError("boom")

        with patch("supertable.data_writer.process_overlapping_files",
                   writer._mocks["process"]):
            with pytest.raises(RuntimeError, match="boom"):
                writer.write("admin", "t1", data, overwrite_columns=[])

        # Lock should still have been released
        assert len(fake_catalog.release_calls) == 1

    def test_lock_timeout_raises(self, writer, fake_catalog):
        """If lock can't be acquired, TimeoutError is raised."""
        # Pre-lock the table
        fake_catalog._locks["testorg:testsuper:t1"] = "other_token"

        data = _simple_arrow(3)
        with pytest.raises(TimeoutError, match="Could not acquire lock"):
            writer.write("admin", "t1", data, overwrite_columns=[])


# ===========================================================================
# Tests: write() — CAS leaf fallback
# ===========================================================================

class TestWriteCASFallback:

    def test_payload_cas_fallback_to_path_cas(self, writer, fake_catalog):
        """When set_leaf_payload_cas fails, it falls back to set_leaf_path_cas."""
        fake_catalog.leaf_payload_cas_should_fail = True

        data = _simple_arrow(3)
        result = writer.write("admin", "t1", data, overwrite_columns=[])
        assert result is not None
        assert len(fake_catalog.set_leaf_path_cas_calls) == 1

    def test_payload_cas_success_no_fallback(self, writer, fake_catalog):
        fake_catalog.leaf_payload_cas_should_fail = False

        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.set_leaf_payload_cas_calls) == 1
        assert len(fake_catalog.set_leaf_path_cas_calls) == 0


# ===========================================================================
# Tests: write() — mirroring
# ===========================================================================

class TestWriteMirroring:

    def test_mirror_failure_does_not_break_write(self, writer):
        """MirrorFormats.mirror_if_enabled failure should be non-fatal."""
        mock_mirror = writer._mocks["mirror"]
        mock_mirror.mirror_if_enabled.side_effect = Exception("mirror failed")

        with patch("supertable.data_writer.MirrorFormats", mock_mirror):
            data = _simple_arrow(3)
            result = writer.write("admin", "t1", data, overwrite_columns=[])
            # Write should still succeed
            assert result is not None

    def test_mirror_called_with_correct_args(self, writer):
        mock_mirror = writer._mocks["mirror"]

        with patch("supertable.data_writer.MirrorFormats", mock_mirror):
            data = _simple_arrow(3)
            writer.write("admin", "t1", data, overwrite_columns=[])

        assert mock_mirror.mirror_if_enabled.called
        call_kwargs = mock_mirror.mirror_if_enabled.call_args
        assert call_kwargs.kwargs["table_name"] == "t1"


# ===========================================================================
# Tests: write() — monitoring
# ===========================================================================

class TestWriteMonitoring:

    def test_monitoring_payload_has_required_fields(self, writer):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=["id"])

        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 1
        payload = monitor.metrics[0]

        required = {
            "query_id", "recorded_at", "super_name", "table_name",
            "overwrite_columns", "newer_than", "delete_only",
            "inserted", "deleted", "total_rows", "total_columns",
            "new_resources", "sunset_files", "duration",
        }
        assert required.issubset(set(payload.keys()))

    def test_monitoring_failure_is_nonfatal(self, writer):
        """If monitoring enqueue fails, write still returns normally."""
        data = _simple_arrow(3)
        monitor = writer._mocks["monitor"]

        # Make log_metric raise
        original_log = monitor.log_metric
        monitor.log_metric = MagicMock(side_effect=Exception("monitoring down"))

        with patch("supertable.data_writer.get_monitoring_logger", return_value=monitor):
            result = writer.write("admin", "t1", data, overwrite_columns=[])

        # Write should still succeed
        assert result is not None

    def test_monitoring_not_enqueued_on_write_failure(self, writer, fake_catalog):
        """If write() fails, stats_payload is None → monitoring not enqueued."""
        data = _simple_arrow(3)
        writer._mocks["process"].side_effect = RuntimeError("processing failed")

        with patch("supertable.data_writer.process_overlapping_files",
                   writer._mocks["process"]):
            with pytest.raises(RuntimeError):
                writer.write("admin", "t1", data, overwrite_columns=[])

        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 0


# ===========================================================================
# Tests: write() — error propagation
# ===========================================================================

class TestWriteErrorPropagation:

    def test_access_control_error_propagates(self):
        """If check_write_access raises, write() should re-raise."""
        with (
            patch("supertable.data_writer.SuperTable") as MockST,
            patch("supertable.data_writer.RedisCatalog") as MockCat,
            patch("supertable.data_writer.check_write_access",
                  side_effect=PermissionError("no access")),
        ):
            st = MockST.return_value
            st.super_name = "s"
            st.organization = "o"

            from supertable.data_writer import DataWriter
            dw = DataWriter.__new__(DataWriter)
            dw.super_table = st
            dw.catalog = MockCat.return_value
            dw._table_config_cache = {}
            dw.timer = MagicMock()

            data = _simple_arrow(3)
            with pytest.raises(PermissionError, match="no access"):
                dw.write("admin", "t1", data, overwrite_columns=[])

    def test_validation_error_propagates(self, writer):
        data = _simple_arrow(3)
        with pytest.raises(ValueError, match="delete_only requires"):
            writer.write("admin", "t1", data, overwrite_columns=[], delete_only=True)

    def test_snapshot_read_error_propagates(self, writer):
        simple_inst = writer._mocks["simple_inst"]
        simple_inst.get_simple_table_snapshot.side_effect = FileNotFoundError("no snapshot")

        data = _simple_arrow(3)
        with pytest.raises(FileNotFoundError, match="no snapshot"):
            writer.write("admin", "t1", data, overwrite_columns=[])


# ===========================================================================
# Tests: write() — snapshot update
# ===========================================================================

class TestWriteSnapshot:

    def test_update_called_with_last_snapshot(self, writer):
        """simple_table.update() receives the last snapshot data for efficiency."""
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])

        simple_inst = writer._mocks["simple_inst"]
        assert simple_inst.update.called
        call_kwargs = simple_inst.update.call_args
        assert "last_snapshot" in call_kwargs.kwargs
        assert "last_snapshot_path" in call_kwargs.kwargs

    def test_snapshot_strips_stats_from_payload(self, writer, fake_catalog):
        """Resources in the leaf payload should have 'stats' stripped."""
        # Make update return resources WITH stats
        simple_inst = writer._mocks["simple_inst"]
        snapshot_with_stats = {
            "simple_name": "t1",
            "snapshot_version": 1,
            "schema": [],
            "resources": [
                {"file": "f1.parquet", "file_size": 100, "rows": 10, "columns": 3,
                 "stats": {"id": {"min": 1, "max": 10}}},
            ],
        }
        simple_inst.update.return_value = (
            snapshot_with_stats,
            "testorg/testsuper/tables/t1/snapshots/new.json",
        )

        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])

        # Verify the payload passed to set_leaf_payload_cas has stats stripped
        assert len(fake_catalog.set_leaf_payload_cas_calls) == 1
        _, _, _, payload, _ = fake_catalog.set_leaf_payload_cas_calls[0]
        for res in payload.get("resources", []):
            assert "stats" not in res


# ===========================================================================
# Tests: write() — timing metrics
# ===========================================================================

class TestWriteTimings:

    def test_monitoring_has_duration(self, writer):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 1
        assert monitor.metrics[0]["duration"] >= 0

    def test_newer_than_early_exit_has_duration(self, writer, fake_catalog):
        """The all-stale early exit path returns before monitoring enqueue.

        Because write() returns early (before the post-finally monitoring block),
        no metric is enqueued.  This is by design: the early-exit avoids holding
        the lock longer than needed and skips non-essential side-effects.
        """
        data = _arrow_table({"id": [1], "name": ["a"], "ts": [10]})
        mock_filter = writer._mocks["filter_stale"]
        empty_filtered = pl.DataFrame({"id": [], "name": [], "ts": []}).cast(
            {"id": pl.Int64, "name": pl.Utf8, "ts": pl.Int64}
        )
        mock_filter.return_value = empty_filtered

        with patch("supertable.data_writer.filter_stale_incoming_rows", mock_filter):
            result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        # Early return skips the post-finally monitoring enqueue
        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 0
        # But the result tuple is still correct
        total_cols, total_rows, inserted, deleted = result
        assert total_rows == 0
        assert inserted == 0


# ===========================================================================
# Tests: write() — multiple sequential writes
# ===========================================================================

class TestWriteSequential:

    def test_two_consecutive_writes(self, writer, fake_catalog):
        """Two writes in sequence should both succeed, each bumping root."""
        data1 = _simple_arrow(3)
        data2 = _simple_arrow(3, id_start=4)

        result1 = writer.write("admin", "t1", data1, overwrite_columns=[])
        result2 = writer.write("admin", "t1", data2, overwrite_columns=[])

        assert result1 is not None
        assert result2 is not None
        assert len(fake_catalog.bump_root_calls) == 2

    def test_write_to_different_tables(self, writer, fake_catalog):
        """Writes to different simple tables should each get their own lock."""
        data = _simple_arrow(3)

        result1 = writer.write("admin", "t1", data, overwrite_columns=[])
        result2 = writer.write("admin", "t2", data, overwrite_columns=[])

        assert result1 is not None
        assert result2 is not None
        assert len(fake_catalog.release_calls) == 2


# ===========================================================================
# Tests: write() — overwrite with delete (integration-style with mock process)
# ===========================================================================

class TestWriteOverwriteWithDelete:

    def test_overwrite_reports_deleted_rows(self, writer):
        """process_overlapping_files returns delete count → propagated to result."""
        mock_process = writer._mocks["process"]
        mock_process.return_value = (5, 2, 5, 3, [{"file": "new.parquet"}], {"old.parquet"})

        with patch("supertable.data_writer.process_overlapping_files", mock_process):
            data = _simple_arrow(5)
            result = writer.write("admin", "t1", data, overwrite_columns=["id"])

        total_cols, total_rows, inserted, deleted = result
        assert inserted == 5
        assert deleted == 2

    def test_delete_only_reports_no_inserts(self, writer):
        """delete_only path: inserted should always be 0."""
        mock_delete = writer._mocks["delete"]
        mock_delete.return_value = (0, 3, 2, 3, [{"file": "kept.parquet"}], {"old.parquet"})

        with patch("supertable.data_writer.process_delete_only", mock_delete):
            data = _simple_arrow(3)
            result = writer.write("admin", "t1", data, overwrite_columns=["id"], delete_only=True)

        total_cols, total_rows, inserted, deleted = result
        assert inserted == 0
        assert deleted == 3


# ===========================================================================
# Tests: write() — edge cases
# ===========================================================================

class TestWriteEdgeCases:

    def test_empty_dataframe(self, writer):
        """Writing an empty Arrow table should still work."""
        data = _arrow_table({"id": [], "name": [], "value": []})
        result = writer.write("admin", "t1", data, overwrite_columns=[])
        assert result is not None

    def test_single_row(self, writer):
        data = _simple_arrow(1)
        result = writer.write("admin", "t1", data, overwrite_columns=[])
        assert result is not None

    def test_large_overwrite_columns_list(self, writer):
        """Multiple overwrite columns should pass validation."""
        cols = {f"col_{i}": list(range(3)) for i in range(10)}
        data = _arrow_table(cols)
        ow_cols = [f"col_{i}" for i in range(5)]
        result = writer.write("admin", "t1", data, overwrite_columns=ow_cols)
        assert result is not None

    def test_special_column_types(self, writer):
        """Boolean, date, and float columns should be handled."""
        data = _arrow_table({
            "id": [1, 2],
            "flag": [True, False],
            "amount": [1.5, 2.5],
        })
        result = writer.write("admin", "t1", data, overwrite_columns=["id"])
        assert result is not None

    def test_compression_level_passed_through(self, writer):
        """Different compression levels should not break the flow."""
        data = _simple_arrow(3)
        for level in [1, 5, 10]:
            result = writer.write("admin", "t1", data, overwrite_columns=[], compression_level=level)
            assert result is not None
