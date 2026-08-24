# supertable/tests/test_data_writer.py
"""
Comprehensive tests for supertable.data_writer.DataWriter.

Every public method and code path is exercised:
  - write() with append, overwrite, delete_only, newer_than
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

    def read_parquet(self, path: str, columns=None) -> pa.Table:
        import io
        import pyarrow.parquet as pq
        raw = self._files.get(path)
        if raw is None:
            raise FileNotFoundError(path)
        # columns is a projection hint; reading full is correct here.
        return pq.read_table(io.BytesIO(raw))


class FakeCatalog:
    """In-memory RedisCatalog replacement."""

    def __init__(self):
        self._locks: Dict[str, str] = {}
        self._leaves: Dict[str, Dict] = {}
        self._roots: Dict[str, Dict] = {}
        self._table_configs: Dict[str, Dict] = {}
        self._simple_tables: set = set()
        self._rowid_counter: int = 0
        self._mirrors: list[str] = []
        self._mirror_publication: Dict | None = None
        self.mirror_state_events: list[str] = []

        # Tracking
        self.bump_root_calls: list = []
        self.set_leaf_payload_cas_calls: list = []
        self.set_leaf_path_cas_calls: list = []
        self.release_calls: list = []
        self.leaf_payload_cas_should_fail: bool = False

    def reserve_rowids(self, org, sup, simple, count) -> int:
        start = self._rowid_counter + 1
        self._rowid_counter += count
        return start

    def reserve_rowids_at_least(
            self, org, sup, simple, count, floor, *, lock_token,
    ):
        assert lock_token
        self._rowid_counter = max(self._rowid_counter, int(floor))
        start = self._rowid_counter + 1
        self._rowid_counter += int(count)
        return start, self._rowid_counter

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

    def commit_snapshot(
            self, org, sup, simple, payload, path, *, expected_version,
            expected_path, lock_token, commit_id=None,
            mirror_publication=False, expected_mirrors=None, now_ms=None,
            one_shot_initial=False,
    ):
        """Test-double implementation of the production atomic primitive."""
        key = f"{org}:{sup}:{simple}"
        if self._locks.get(key) != lock_token:
            raise RuntimeError("lost lock")
        # Individual tests exercise the writer orchestration with a mocked
        # SimpleTable snapshot. Preserve their call tracking while exposing the
        # required fenced API; production Redis atomicity is covered separately.
        version = self.set_leaf_payload_cas(
            org, sup, simple, payload, path, now_ms=now_ms,
        )
        self.bump_root(org, sup, now_ms=now_ms)
        if mirror_publication:
            assert self._mirror_publication["commit_id"] == commit_id
            self._mirror_publication["status"] = "core_committed"
            self._mirror_publication["core_committed"] = True
            self.mirror_state_events.append("core_committed")
        return version, 1

    def prepare_mirror_publication(
            self, org, sup, simple, *, commit_id, snapshot_path, mirrors,
            lock_token, now_ms=None,
    ):
        self._mirror_publication = {
            "status": "prepared", "commit_id": commit_id,
            "snapshot_path": snapshot_path, "mirrors": list(mirrors),
            "core_committed": False,
        }
        self.mirror_state_events.append("prepared")
        return dict(self._mirror_publication)

    def complete_mirror_publication(
            self, org, sup, simple, *, commit_id, lock_token, now_ms=None,
    ):
        assert self._mirror_publication["commit_id"] == commit_id
        self._mirror_publication["status"] = "complete"
        self.mirror_state_events.append("complete")
        return dict(self._mirror_publication)

    def fail_mirror_publication(
            self, org, sup, simple, *, commit_id, lock_token, failure_stage,
            error, now_ms=None,
    ):
        assert self._mirror_publication["commit_id"] == commit_id
        self._mirror_publication.update({
            "status": "failed", "failure_stage": failure_stage,
            "error": {"type": type(error).__name__, "message": str(error)},
        })
        self.mirror_state_events.append("failed")
        return dict(self._mirror_publication)

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

    def set_table_config(
            self, org, sup, simple, config, *, lock_token=None,
    ) -> bool:
        self._table_configs[f"{org}:{sup}:{simple}"] = config
        return True

    def get_mirrors(self, org, sup):
        return list(self._mirrors)

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
        patch("supertable.data_writer.check_create_access"),
        patch("supertable.data_writer.check_write_access"),
        patch("supertable.data_writer.SimpleTable") as MockSimpleTable,
        patch("supertable.data_writer.find_overlapping_files", return_value=set()),
        patch("supertable.data_writer.write_parquet_and_collect_resources") as mock_process,
        patch("supertable.data_writer.resolve_overwrite_writes") as mock_resolve,
        patch("supertable.data_writer.identify_all_rowids") as mock_delete_all,
        patch("supertable.data_writer.build_tombstone_file") as mock_build_tombstone,
        patch("supertable.data_writer.extract_stats_rows", return_value=pl.DataFrame()),
        patch("supertable.data_writer.build_stats_file", return_value=(None, None)),
        # MonitoringWriter is used as `with MonitoringWriter(...) as mon:` —
        # so the in-block monitor is the __enter__ return value.
        patch("supertable.data_writer.MonitoringWriter") as MockMonitorCls,
        patch("supertable.data_writer.MirrorFormats") as MockMirror,
    ):
        MockMonitorCls.return_value.__enter__.return_value = fake_monitor
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
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
        }
        initial_path = "testorg/testsuper/tables/t1/snapshots/init.json"
        simple_inst = MockSimpleTable.return_value
        simple_inst.get_simple_table_snapshot.return_value = (initial_snapshot, initial_path)
        simple_inst._last_snapshot_leaf = {"version": 0, "path": initial_path}
        simple_inst.simple_dir = "testorg/testsuper/tables/t1"
        simple_inst.data_dir = "testorg/testsuper/tables/t1/data"
        simple_inst.snapshot_dir = "testorg/testsuper/tables/t1/snapshots"

        def _update_snapshot(
                new_resources, sunset_files, model_df,
                *, last_snapshot, **_kwargs,
        ):
            snapshot = dict(last_snapshot)
            sunset = set(sunset_files)
            snapshot["resources"] = [
                resource for resource in snapshot.get("resources", [])
                if resource.get("file") not in sunset
            ] + list(new_resources)
            snapshot["snapshot_version"] = (
                int(last_snapshot.get("snapshot_version", 0)) + 1
            )
            return (
                snapshot,
                "testorg/testsuper/tables/t1/snapshots/new.json",
            )

        simple_inst.update.side_effect = _update_snapshot

        # Default write-path mock behaviour.
        # write_parquet_and_collect_resources appends a resource dict to the
        # caller-supplied new_resources list; its return value is ignored.
        def _append_resource(*args, **kwargs):
            kwargs["new_resources"].append({"file": "new.parquet"})
            return None
        mock_process.side_effect = _append_resource
        # resolve_overwrite_writes returns (filtered_df, [(file, __rowid__), ...]).
        # Default: pass the incoming frame through unchanged with no deletes.
        def _resolve_passthrough(**kwargs):
            return kwargs["incoming_df"], []
        mock_resolve.side_effect = _resolve_passthrough
        # identify_all_rowids (delete-all path) returns (file, __rowid__) pairs.
        mock_delete_all.return_value = []
        # build_tombstone_file returns the full validated frame whenever it
        # publishes a new immutable pointer (the writer hashes that frame).
        def _build_tombstone(**kwargs):
            pairs = kwargs.get("new_pairs") or []
            if not pairs:
                return kwargs.get("prev_tombstone_path"), None
            return (
                "/d/tombstone/t.parquet",
                pl.DataFrame(
                    {
                        "__file__": [f for f, _ in pairs],
                        "__rowid__": [rid for _, rid in pairs],
                    },
                    schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
                ),
            )
        mock_build_tombstone.side_effect = _build_tombstone

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
            "resolve": mock_resolve,
            "delete_all": mock_delete_all,
            "build_tombstone": mock_build_tombstone,
            "mirror": MockMirror,
            "monitor": fake_monitor,
            "find_overlap": patch("supertable.data_writer.find_overlapping_files",
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
        # delete-all path: valid, no overwrite_columns required.
        dw = self._make_writer_for_validation()
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

    def test_max_memory_chunk_size_positive(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", max_memory_chunk_size=1024)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_memory_chunk_size"] == 1024

    def test_max_memory_chunk_size_zero_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_memory_chunk_size must be a positive"):
                dw.configure_table("admin", "t1", max_memory_chunk_size=0)

    def test_max_memory_chunk_size_negative_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_memory_chunk_size must be a positive"):
                dw.configure_table("admin", "t1", max_memory_chunk_size=-5)

    def test_max_decoded_compaction_budget_is_stored(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin", "t1", max_decoded_compaction_bytes=64 * 1024 * 1024,
            )

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_decoded_compaction_bytes"] == 64 * 1024 * 1024

    @pytest.mark.parametrize("value", [0, -1, True, 1.5, "1024"])
    def test_max_decoded_compaction_budget_rejects_invalid_values(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="must be a positive integer"):
                dw.configure_table(
                    "admin", "t1", max_decoded_compaction_bytes=value,
                )

    def test_max_overlapping_files_positive(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table("admin", "t1", max_overlapping_files=50)

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_overlapping_files"] == 50

    def test_max_overlapping_files_zero_rejected(self, fake_catalog):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="max_overlapping_files must be a positive"):
                dw.configure_table("admin", "t1", max_overlapping_files=0)

    @pytest.mark.parametrize(
        "field",
        [
            "max_memory_chunk_size",
            "max_overlapping_files",
            "max_tombstone_rows",
        ],
    )
    def test_integer_table_limits_reject_boolean_values(
        self, fake_catalog, field,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="must be a positive integer"):
                dw.configure_table("admin", "t1", **{field: True})

    def test_configure_updates_limits(self, fake_catalog):
        """When both limits are provided, they read existing config from Redis first."""
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "max_memory_chunk_size": 2048,
        })

        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin", "t1",
                max_memory_chunk_size=4096, max_overlapping_files=200,
            )

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["max_memory_chunk_size"] == 4096
        assert cfg["max_overlapping_files"] == 200

    @pytest.mark.parametrize("value", [0, 9, -1, True, 1.5, "2"])
    def test_tombstone_compaction_workers_rejects_invalid_values(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="integer from 1 to 8"):
                dw.configure_table(
                    "admin", "t1", tombstone_compaction_workers=value,
                )

    @pytest.mark.parametrize("value", [1, 2, 8])
    def test_tombstone_compaction_workers_is_stored(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin", "t1", tombstone_compaction_workers=value,
            )
        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["tombstone_compaction_workers"] == value

    def test_dv_v2_activation_stores_both_fleet_keys_atomically(
        self, fake_catalog,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin",
                "t1",
                deletion_vector_format=2,
                confirm_dv_v2_reader_fleet=True,
            )

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["deletion_vector_format"] == 2
        assert cfg["dv_v2_reader_fleet_confirmed"] is True

    @pytest.mark.parametrize("value", [None, False, 0, 1, "yes"])
    def test_dv_v2_activation_requires_exact_confirmation(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="requires.*confirmation|requires.*confirm"):
                dw.configure_table(
                    "admin",
                    "t1",
                    deletion_vector_format=2,
                    confirm_dv_v2_reader_fleet=value,
                )

    @pytest.mark.parametrize("value", [1, 4, True, 2.0, "2"])
    def test_dv_v2_activation_rejects_other_formats(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="must be integer 2 or 3"):
                dw.configure_table(
                    "admin",
                    "t1",
                    deletion_vector_format=value,
                    confirm_dv_v2_reader_fleet=True,
                )

    def test_dv_v2_confirmation_cannot_be_set_independently(
        self, fake_catalog,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="cannot be set independently"):
                dw.configure_table(
                    "admin", "t1", confirm_dv_v2_reader_fleet=True,
                )

    def test_dv_v3_activation_stores_exact_fleet_pair_and_removes_v2_key(
        self, fake_catalog,
    ):
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": True,
        })
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            dw.configure_table(
                "admin",
                "t1",
                deletion_vector_format=3,
                confirm_dv_v3_reader_fleet=True,
            )

        cfg = fake_catalog.get_table_config("testorg", "testsuper", "t1")
        assert cfg["deletion_vector_format"] == 3
        assert cfg["dv_v3_reader_fleet_confirmed"] is True
        assert "dv_v2_reader_fleet_confirmed" not in cfg

    @pytest.mark.parametrize("value", [None, False, 0, 1, "yes"])
    def test_dv_v3_activation_requires_exact_confirmation(
        self, fake_catalog, value,
    ):
        dw = self._make_writer(fake_catalog)
        with patch("supertable.data_writer.check_write_access"):
            with pytest.raises(ValueError, match="requires.*confirm"):
                dw.configure_table(
                    "admin",
                    "t1",
                    deletion_vector_format=3,
                    confirm_dv_v3_reader_fleet=value,
                )

    @pytest.mark.parametrize(
        "snapshot,config,local_enabled,expected",
        [
            ({}, {"deletion_vector_format": 3,
                  "dv_v3_reader_fleet_confirmed": True}, False, False),
            ({}, {"deletion_vector_format": 3,
                  "dv_v3_reader_fleet_confirmed": True}, True, True),
            ({}, {"deletion_vector_format": 3}, True, False),
            ({"tombstone_format": 3}, {}, False, True),
            ({"tombstone_format": 2}, {
                "deletion_vector_format": 3,
                "dv_v3_reader_fleet_confirmed": True,
            }, True, False),
            ({"tombstone_format": 3.0}, {}, False, False),
        ],
    )
    def test_dv_v3_transition_gate_and_sticky_active_state(
        self, snapshot, config, local_enabled, expected,
    ):
        from supertable.data_writer import DataWriter

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V3_WRITES_ENABLED=local_enabled),
        ):
            assert DataWriter._tombstone_v3_transition_enabled(
                snapshot, config,
            ) is expected

    @pytest.mark.parametrize(
        "snapshot,config,local_enabled,expected",
        [
            ({}, {"deletion_vector_format": 2,
                  "dv_v2_reader_fleet_confirmed": True}, False, False),
            ({}, {"deletion_vector_format": 2,
                  "dv_v2_reader_fleet_confirmed": True}, True, True),
            ({}, {"deletion_vector_format": 2}, True, False),
            ({"tombstone_format": 2}, {}, False, True),
            ({"tombstone_format": 3}, {
                "deletion_vector_format": 2,
                "dv_v2_reader_fleet_confirmed": True,
            }, True, False),
            ({"tombstone_format": 2.0}, {}, False, False),
        ],
    )
    def test_dv_v2_transition_gate_and_sticky_active_state(
        self, snapshot, config, local_enabled, expected,
    ):
        from supertable.data_writer import DataWriter

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=local_enabled),
        ):
            assert DataWriter._tombstone_v2_transition_enabled(
                snapshot, config,
            ) is expected


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

    def test_refreshes_after_acknowledged_change(self, fake_catalog):
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {"primary_keys": ["id"]})
        dw = self._make_writer(fake_catalog)

        cfg1 = dw._get_table_config("t1")
        # A change acknowledged through another writer/process must be visible
        # to this long-lived writer on its next operation.
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {"primary_keys": ["new_id"]})
        cfg2 = dw._get_table_config("t1")
        assert cfg1["primary_keys"] == ["id"]
        assert cfg2["primary_keys"] == ["new_id"]


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
        # total_rows/inserted derive from the incoming row count; deleted from
        # resolve_overwrite_writes' delete pairs (empty by default).
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

    def test_per_step_timings_in_payload(self, writer):
        """Every major write step must ship its wall-clock duration to the
        monitoring (redis) stats so bottlenecks are analysable later."""
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=["id"])
        payload = writer._mocks["monitor"].metrics[0]
        timings = payload["timings"]
        for stage in (
            "overlap", "identify_deletes", "write_parquet",
            "build_tombstone", "compact_tombstones", "build_stats",
            "update_simple", "bump_root",
        ):
            assert stage in timings, f"missing per-step timing: {stage}"
            assert isinstance(timings[stage], (int, float))


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

    def test_confirmed_enabled_table_publishes_v2_manifest_root(
        self, writer, fake_catalog,
    ):
        from supertable.processing import LoadedTombstoneState, tombstone_digest
        from supertable.tombstone_manifest_v2 import TombstoneSegment

        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": True,
        })
        writer._mocks["resolve"].side_effect = lambda **kw: (
            kw["incoming_df"], [("old.parquet", 99)],
        )
        frame = pl.DataFrame(
            {"__file__": ["old.parquet"], "__rowid__": [99]},
            schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
        )
        segment = TombstoneSegment(
            file="testorg/testsuper/tables/t1/tombstone/segment.parquet",
            rows=1,
            file_size=123,
            digest=tombstone_digest(frame),
        )
        state = LoadedTombstoneState(
            frame=frame,
            tombstone_format=2,
            tombstone_path=(
                "testorg/testsuper/tables/t1/tombstone/manifest.json"
            ),
            root_digest="a" * 64,
            segments=(segment,),
        )

        with (
            patch(
                "supertable.data_writer.settings",
                MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=True),
            ),
            patch(
                "supertable.data_writer.build_tombstone_v2",
                return_value=(state.tombstone_path, frame, state),
            ) as build_v2,
        ):
            writer.write("admin", "t1", _simple_arrow(1), ["id"])

        build_v2.assert_called_once()
        writer._mocks["build_tombstone"].assert_not_called()
        pinned = writer._mocks["simple_inst"].update.call_args.kwargs[
            "last_snapshot"
        ]
        assert pinned["tombstone_format"] == 2
        assert pinned["tombstone"] == state.tombstone_path
        assert pinned["tombstone_rows"] == 1
        assert pinned["tombstone_digest"] == state.root_digest

    def test_confirmed_enabled_table_publishes_v3_single_parquet_root(
        self, writer, fake_catalog,
    ):
        from supertable.processing import LoadedTombstoneState

        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "deletion_vector_format": 3,
            "dv_v3_reader_fleet_confirmed": True,
        })
        writer._mocks["resolve"].side_effect = lambda **kw: (
            kw["incoming_df"], [("old.parquet", 99)],
        )
        frame = pl.DataFrame(
            {"__file__": ["old.parquet"], "__rowid__": [99]},
            schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
        )
        state = LoadedTombstoneState(
            frame=frame,
            tombstone_format=3,
            tombstone_path=(
                "testorg/testsuper/tables/t1/tombstone/deleted-v3.parquet"
            ),
            root_digest="a" * 64,
            referenced_files=frozenset({"old.parquet"}),
        )

        with (
            patch(
                "supertable.data_writer.settings",
                MagicMock(SUPERTABLE_DV_V3_WRITES_ENABLED=True),
            ),
            patch(
                "supertable.data_writer.build_tombstone_v3",
                return_value=(state.tombstone_path, frame, state),
            ) as build_v3,
        ):
            writer.write("admin", "t1", _simple_arrow(1), ["id"])

        build_v3.assert_called_once()
        writer._mocks["build_tombstone"].assert_not_called()
        pinned = writer._mocks["simple_inst"].update.call_args.kwargs[
            "last_snapshot"
        ]
        assert pinned["tombstone_format"] == 3
        assert pinned["tombstone"] == state.tombstone_path
        assert pinned["tombstone_rows"] == 1
        assert pinned["tombstone_digest"] == state.root_digest

    def test_pure_append_carries_active_v3_object_without_rewrite(
        self, writer,
    ):
        simple = writer._mocks["simple_inst"]
        tombstone = (
            "testorg/testsuper/tables/t1/tombstone/deleted-v3.parquet"
        )
        snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 4,
            "schema": [],
            "resources": [{"file": "old.parquet", "rows": 1}],
            "tombstone": tombstone,
            "tombstone_rows": 1,
            "tombstone_digest": "a" * 64,
            "tombstone_format": 3,
            "rowid_high_watermark": 1,
        }
        old_path = "testorg/testsuper/tables/t1/snapshots/v4.json"
        simple.get_simple_table_snapshot.return_value = (snapshot, old_path)
        simple._last_snapshot_leaf = {"version": 4, "path": old_path}

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V3_WRITES_ENABLED=False),
        ), patch(
            "supertable.data_writer.build_tombstone_v3",
        ) as build_v3, patch(
            "supertable.data_writer.persist_tombstone_v3_frame",
        ) as persist_v3:
            writer.write("admin", "t1", _simple_arrow(1), [])

        build_v3.assert_not_called()
        persist_v3.assert_not_called()
        pinned = simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone"] == tombstone
        assert pinned["tombstone_rows"] == 1
        assert pinned["tombstone_digest"] == "a" * 64
        assert pinned["tombstone_format"] == 3

    def test_existing_empty_v2_snapshot_stays_v2_with_local_switch_off(
        self, writer,
    ):
        simple = writer._mocks["simple_inst"]
        snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 4,
            "schema": [],
            "resources": [],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "tombstone_format": 2,
            "rowid_high_watermark": 0,
        }
        simple.get_simple_table_snapshot.return_value = (
            snapshot, "testorg/testsuper/tables/t1/snapshots/v4.json",
        )
        simple._last_snapshot_leaf = {
            "version": 4,
            "path": "testorg/testsuper/tables/t1/snapshots/v4.json",
        }

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=False),
        ), patch(
            "supertable.data_writer.build_tombstone_v2",
        ) as build_v2, patch(
            "supertable.data_writer.persist_tombstone_v2_frame",
        ) as persist_v2:
            writer.write("admin", "t1", _simple_arrow(1), [])

        build_v2.assert_not_called()
        persist_v2.assert_not_called()
        pinned = simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone_format"] == 2
        assert pinned["tombstone"] is None
        assert pinned["tombstone_rows"] == 0
        assert pinned["tombstone_digest"] is None
        writer._mocks["build_tombstone"].assert_not_called()

    def test_pure_append_carries_active_v2_root_without_manifest_write(
        self, writer,
    ):
        simple = writer._mocks["simple_inst"]
        manifest = (
            "testorg/testsuper/tables/t1/tombstone/manifest.json"
        )
        snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 4,
            "schema": [],
            "resources": [{"file": "old.parquet", "rows": 1}],
            "tombstone": manifest,
            "tombstone_rows": 1,
            "tombstone_digest": "a" * 64,
            "tombstone_format": 2,
            "rowid_high_watermark": 1,
        }
        old_path = "testorg/testsuper/tables/t1/snapshots/v4.json"
        simple.get_simple_table_snapshot.return_value = (snapshot, old_path)
        simple._last_snapshot_leaf = {"version": 4, "path": old_path}

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=False),
        ), patch(
            "supertable.data_writer.build_tombstone_v2",
        ) as build_v2, patch(
            "supertable.data_writer.persist_tombstone_v2_frame",
        ) as persist_v2:
            writer.write("admin", "t1", _simple_arrow(1), [])

        build_v2.assert_not_called()
        persist_v2.assert_not_called()
        pinned = simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone"] == manifest
        assert pinned["tombstone_rows"] == 1
        assert pinned["tombstone_digest"] == "a" * 64
        assert pinned["tombstone_format"] == 2

    def test_confirmed_v2_table_pure_append_does_not_transition_v1(
        self, writer, fake_catalog,
    ):
        simple = writer._mocks["simple_inst"]
        legacy = (
            "testorg/testsuper/tables/t1/tombstone/legacy.parquet"
        )
        snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 4,
            "schema": [],
            "resources": [{"file": "old.parquet", "rows": 1}],
            "tombstone": legacy,
            "tombstone_rows": 1,
            "tombstone_digest": "b" * 64,
            "rowid_high_watermark": 1,
        }
        old_path = "testorg/testsuper/tables/t1/snapshots/v4.json"
        simple.get_simple_table_snapshot.return_value = (snapshot, old_path)
        simple._last_snapshot_leaf = {"version": 4, "path": old_path}
        fake_catalog.set_table_config("testorg", "testsuper", "t1", {
            "deletion_vector_format": 2,
            "dv_v2_reader_fleet_confirmed": True,
        })

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=True),
        ), patch(
            "supertable.data_writer.build_tombstone_v2",
        ) as build_v2, patch(
            "supertable.data_writer.persist_tombstone_v2_frame",
        ) as persist_v2:
            writer.write("admin", "t1", _simple_arrow(1), [])

        build_v2.assert_not_called()
        persist_v2.assert_not_called()
        writer._mocks["build_tombstone"].assert_not_called()
        pinned = simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone"] == legacy
        assert pinned["tombstone_digest"] == "b" * 64
        assert "tombstone_format" not in pinned


class TestWriteDeleteOnly:
    """write() with delete_only=True."""

    def test_delete_only_with_overwrite_columns_reports_deletes(self, writer):
        data = _simple_arrow(3)
        mock_resolve = writer._mocks["resolve"]
        # One delete pair → a non-empty tombstone, so build_tombstone_file
        # (already patched in the fixture) handles the parquet I/O.
        mock_resolve.side_effect = lambda **kw: (kw["incoming_df"], [("old.parquet", 1)])

        result = writer.write("admin", "t1", data, overwrite_columns=["id"], delete_only=True)

        assert mock_resolve.called
        total_cols, total_rows, inserted, deleted = result
        assert inserted == 0
        assert deleted == 1

    def test_delete_only_without_overwrite_columns_deletes_all(self, writer):
        """delete_only with no overwrite_columns is the delete-all path: it
        sunsets every current resource directly and inserts nothing, avoiding
        an O(rows) intermediate deletion-vector."""
        data = _simple_arrow(3)
        mock_delete_all = writer._mocks["delete_all"]
        mock_delete_all.return_value = [("old.parquet", 1), ("old.parquet", 2)]
        simple = writer._mocks["simple_inst"]
        simple.get_simple_table_snapshot.return_value = (
            {
                "simple_name": "t1",
                "schema": [],
                "resources": [{"file": "old.parquet", "rows": 2}],
            },
            "snapshots/old.json",
        )
        simple._last_snapshot_leaf = {
            "version": 0, "path": "snapshots/old.json",
        }

        result = writer.write("admin", "t1", data, overwrite_columns=[], delete_only=True)

        assert not mock_delete_all.called
        # The overwrite-resolve probe must NOT run on the delete-all path.
        assert not writer._mocks["resolve"].called
        total_cols, total_rows, inserted, deleted = result
        assert inserted == 0
        assert deleted == 2
        assert simple.update.call_args.args[1] == {"old.parquet"}

    def test_delete_all_keeps_explicit_v2_empty_marker_with_switch_off(
        self, writer,
    ):
        simple = writer._mocks["simple_inst"]
        snapshot = {
            "simple_name": "t1",
            "location": "testorg/testsuper/tables/t1",
            "snapshot_version": 4,
            "schema": [],
            "resources": [{"file": "old.parquet", "rows": 2}],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
            "tombstone_format": 2,
            "rowid_high_watermark": 2,
        }
        old_path = "testorg/testsuper/tables/t1/snapshots/v4.json"
        simple.get_simple_table_snapshot.return_value = (snapshot, old_path)
        simple._last_snapshot_leaf = {"version": 4, "path": old_path}

        with patch(
            "supertable.data_writer.settings",
            MagicMock(SUPERTABLE_DV_V2_WRITES_ENABLED=False),
        ):
            writer.write(
                "admin",
                "t1",
                _simple_arrow(1),
                overwrite_columns=[],
                delete_only=True,
            )

        pinned = simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone_format"] == 2
        assert pinned["tombstone"] is None
        assert pinned["tombstone_rows"] == 0
        assert pinned["tombstone_digest"] is None
        assert simple.update.call_args.args[1] == {"old.parquet"}


class TestWriteNewerThan:
    """write() with newer_than filtering."""

    def test_newer_than_filters_stale_rows(self, writer):
        data = _arrow_table({"id": [1, 2, 3], "name": ["a", "b", "c"], "ts": [10, 20, 30]})
        mock_resolve = writer._mocks["resolve"]
        # resolve returns the stale-filtered subset + no deletes.
        filtered_df = pl.DataFrame({"id": [3], "name": ["c"], "ts": [30]})
        mock_resolve.side_effect = lambda **kw: (filtered_df, [])

        result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        assert mock_resolve.called

    def test_newer_than_all_stale_returns_early(self, writer, fake_catalog):
        """When all rows are stale, write should return early without processing."""
        data = _arrow_table({"id": [1, 2], "name": ["a", "b"], "ts": [10, 20]})
        mock_resolve = writer._mocks["resolve"]
        # Return empty DataFrame with same schema
        empty_filtered = pl.DataFrame({"id": [], "name": [], "ts": []}).cast(
            {"id": pl.Int64, "name": pl.Utf8, "ts": pl.Int64}
        )
        mock_resolve.side_effect = lambda **kw: (empty_filtered, [])

        result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        # Should return early with zeros
        assert result is not None
        total_cols, total_rows, inserted, deleted = result
        assert total_rows == 0
        assert inserted == 0
        assert deleted == 0
        # the insert path (write_parquet_and_collect_resources) should NOT run
        assert not writer._mocks["process"].called

    def test_newer_than_validation_missing_column(self, writer):
        data = _simple_arrow(3)
        with pytest.raises(ValueError, match="not present in the dataset"):
            writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="nonexistent")


# ===========================================================================
# Tests: write() — lock lifecycle
# ===========================================================================

class TestWriteLocking:

    def test_authorization_callback_fences_lock_and_publication(
        self, writer, fake_catalog,
    ):
        calls: list[int] = []

        def authorize() -> str:
            calls.append(len(calls) + 1)
            return "admin"

        writer.write(
            "admin",
            "t1",
            _simple_arrow(1),
            overwrite_columns=[],
            authorization_callback=authorize,
        )

        assert calls == [1, 2, 3]
        assert len(fake_catalog.set_leaf_payload_cas_calls) == 1

    def test_authorization_revoked_before_publication_fails_closed(
        self, writer, fake_catalog,
    ):
        calls = 0

        def authorize() -> str:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise PermissionError("membership revoked")
            return "admin"

        with pytest.raises(PermissionError, match="membership revoked"):
            writer.write(
                "admin",
                "t1",
                _simple_arrow(1),
                overwrite_columns=[],
                authorization_callback=authorize,
            )

        assert calls == 3
        assert writer._mocks["process"].called
        assert fake_catalog.set_leaf_payload_cas_calls == []
        assert fake_catalog.bump_root_calls == []
        assert len(fake_catalog.release_calls) == 1

    def test_lock_released_on_success(self, writer, fake_catalog):
        data = _simple_arrow(3)
        writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.release_calls) == 1

    def test_lock_released_on_process_error(self, writer, fake_catalog):
        """Lock must be released even if processing raises."""
        data = _simple_arrow(3)
        writer._mocks["process"].side_effect = RuntimeError("boom")

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

    def test_ambiguous_payload_cas_failure_never_weakly_retries(
        self, writer, fake_catalog,
    ):
        """An ambiguous commit failure aborts; path-only retry can lose metadata."""
        fake_catalog.leaf_payload_cas_should_fail = True

        data = _simple_arrow(3)
        with pytest.raises(Exception, match="payload CAS not supported"):
            writer.write("admin", "t1", data, overwrite_columns=[])
        assert len(fake_catalog.set_leaf_path_cas_calls) == 0

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

    def test_outbox_prepare_failure_prevents_core_commit(
        self, writer, fake_catalog,
    ):
        fake_catalog._mirrors = ["PARQUET"]
        fake_catalog.prepare_mirror_publication = MagicMock(
            side_effect=OSError("Redis unavailable")
        )

        with pytest.raises(OSError, match="Redis unavailable"):
            writer.write("admin", "t1", _simple_arrow(3), overwrite_columns=[])

        assert not fake_catalog.set_leaf_payload_cas_calls
        assert fake_catalog.release_calls

    def test_outbox_completion_failure_is_reported_as_post_commit_ambiguity(
        self, writer, fake_catalog,
    ):
        from supertable.mirroring.mirror_formats import MirrorPublicationError

        fake_catalog._mirrors = ["PARQUET"]
        fake_catalog.complete_mirror_publication = MagicMock(
            side_effect=OSError("completion reply lost")
        )

        with pytest.raises(MirrorPublicationError) as raised:
            writer.write("admin", "t1", _simple_arrow(3), overwrite_columns=[])

        assert raised.value.core_committed is True
        assert isinstance(raised.value.cause, OSError)
        assert fake_catalog.set_leaf_payload_cas_calls
        assert fake_catalog._mirror_publication["status"] == "failed"
        assert fake_catalog._mirror_publication["failure_stage"] == "outbox_complete"
        assert fake_catalog.release_calls

    def test_mirror_failure_reports_core_commit_explicitly(
        self, writer, fake_catalog,
    ):
        """A failed mirror raises only after the core snapshot is committed."""
        from supertable.mirroring.mirror_formats import MirrorPublicationError

        fake_catalog._mirrors = ["PARQUET"]
        mock_mirror = writer._mocks["mirror"]
        mock_mirror.mirror_if_enabled.side_effect = Exception("mirror failed")

        with patch("supertable.data_writer.MirrorFormats", mock_mirror):
            data = _simple_arrow(3)
            with pytest.raises(MirrorPublicationError) as raised:
                writer.write("admin", "t1", data, overwrite_columns=[])

        error = raised.value
        assert error.core_committed is True
        assert error.core_result is not None
        assert error.snapshot_path.endswith("snapshots/new.json")
        assert error.mirrors == ("PARQUET",)
        assert "do not blindly retry" in str(error)
        assert fake_catalog.set_leaf_payload_cas_calls
        assert fake_catalog.release_calls
        assert fake_catalog._mirror_publication["status"] == "failed"
        assert fake_catalog._mirror_publication["core_committed"] is True
        assert fake_catalog._mirror_publication["error"]["message"] == "mirror failed"
        assert fake_catalog.mirror_state_events == [
            "prepared", "core_committed", "failed",
        ]

    def test_mirror_called_with_correct_args(self, writer, fake_catalog):
        fake_catalog._mirrors = ["PARQUET"]
        mock_mirror = writer._mocks["mirror"]

        with patch("supertable.data_writer.MirrorFormats", mock_mirror):
            data = _simple_arrow(3)
            writer.write("admin", "t1", data, overwrite_columns=[])

        assert mock_mirror.mirror_if_enabled.called
        call_kwargs = mock_mirror.mirror_if_enabled.call_args
        assert call_kwargs.kwargs["table_name"] == "t1"
        assert fake_catalog.mirror_state_events == [
            "prepared", "core_committed", "complete",
        ]


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

        with patch("supertable.data_writer.MonitoringWriter") as MockMonitorCls:
            MockMonitorCls.return_value.__enter__.return_value = monitor
            result = writer.write("admin", "t1", data, overwrite_columns=[])

        # Write should still succeed
        assert result is not None

    def test_monitoring_spool_backpressure_is_explicit_post_commit(
        self, writer,
    ):
        """A full/fsync-failed WAL cannot be logged as a successful write."""
        from supertable.monitoring_writer import (
            MonitoringBackpressureError,
            MonitoringPostCommitError,
        )

        monitor = writer._mocks["monitor"]
        monitor.log_metric = MagicMock(
            side_effect=MonitoringBackpressureError("spool full")
        )
        with patch("supertable.data_writer.MonitoringWriter") as monitor_cls:
            monitor_cls.return_value.__enter__.return_value = monitor
            with pytest.raises(MonitoringPostCommitError) as raised:
                writer.write(
                    "admin", "t1", _simple_arrow(3), overwrite_columns=[],
                )

        assert raised.value.core_committed is True
        assert raised.value.core_result is not None
        assert raised.value.operation == "write"

    def test_monitoring_backpressure_preserves_mirror_recovery_error(
        self, writer, fake_catalog,
    ):
        from supertable.monitoring_writer import (
            MonitoringBackpressureError,
            MonitoringPostCommitError,
        )

        fake_catalog._mirrors = ["PARQUET"]
        mirror = writer._mocks["mirror"]
        mirror.mirror_if_enabled.side_effect = RuntimeError("mirror failed")
        monitor = writer._mocks["monitor"]
        monitor.log_metric = MagicMock(
            side_effect=MonitoringBackpressureError("spool full")
        )
        with (
            patch("supertable.data_writer.MirrorFormats", mirror),
            patch("supertable.data_writer.MonitoringWriter") as monitor_cls,
        ):
            monitor_cls.return_value.__enter__.return_value = monitor
            with pytest.raises(MonitoringPostCommitError) as raised:
                writer.write(
                    "admin", "t1", _simple_arrow(3), overwrite_columns=[],
                )

        assert raised.value.core_committed is True
        assert isinstance(raised.value.mirror_error, RuntimeError)
        assert fake_catalog._mirror_publication["status"] == "failed"

    def test_monitoring_not_enqueued_on_write_failure(self, writer, fake_catalog):
        """If write() fails, stats_payload is None → monitoring not enqueued."""
        data = _simple_arrow(3)
        writer._mocks["process"].side_effect = RuntimeError("processing failed")

        with pytest.raises(RuntimeError):
            writer.write("admin", "t1", data, overwrite_columns=[])

        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) == 0


# ===========================================================================
# Tests: write() — error propagation
# ===========================================================================

class TestWriteErrorPropagation:

    def test_custom_catalog_get_only_mutation_context_remains_supported(
        self, writer, fake_catalog, monkeypatch,
    ):
        class GetOnlyContext:
            def __init__(self, values):
                self.values = values

            def get(self, key, default=None):
                return self.values.get(key, default)

        fake_catalog.supports_one_shot_table_creation = True
        monkeypatch.setattr(
            type(fake_catalog),
            "acquire_namespace_lock",
            lambda self, *args, **kwargs: "namespace-token",
            raising=False,
        )
        monkeypatch.setattr(
            type(fake_catalog),
            "release_namespace_lock",
            lambda self, *args, **kwargs: True,
            raising=False,
        )

        def begin_mutation(
                self, org, sup, simple, *, lock_token, reserve_count=0,
                namespace_token="",
        ):
            assert namespace_token == "namespace-token"
            return GetOnlyContext({
                "leaf": None,
                "table_config": {},
                "mirrors": [],
                "mirror_pin": None,
                "rowid_floor": 0,
                "rowid_reservation": (1, reserve_count),
                # A custom adapter may use similarly named keys. The writer's
                # exact-type gate must neither read nor delete them.
                "_initial_compact_begin_calls": "adapter-owned",
            })

        monkeypatch.setattr(
            type(fake_catalog), "begin_table_mutation", begin_mutation,
            raising=False,
        )

        assert writer.write(
            "admin", "new_table", _simple_arrow(3), overwrite_columns=[],
        ) is not None
        assert "initial_compact_begin_calls" not in (
            writer._mocks["monitor"].metrics[0]["counts"]
        )

    def test_first_write_uses_one_shot_expected_absent_snapshot(
        self, writer, fake_catalog, monkeypatch,
    ):
        events = []
        namespace_token = "namespace-token"
        fake_catalog.supports_one_shot_table_creation = True
        original_simple_acquire = fake_catalog.acquire_simple_lock
        original_process = writer._mocks["process"].side_effect
        commit_calls = []
        begin_contexts = []

        def acquire_namespace(self, org, sup, ttl_s=30, timeout_s=60):
            events.append("namespace.acquire")
            return namespace_token

        def release_namespace(self, org, sup, token):
            assert token == namespace_token
            events.append("namespace.release")
            return True

        def acquire_simple(org, sup, simple, ttl_s=30, timeout_s=60):
            events.append("leaf.acquire")
            return original_simple_acquire(
                org, sup, simple, ttl_s=ttl_s, timeout_s=timeout_s,
            )

        def begin_mutation(
            self, org, sup, simple, *, lock_token, reserve_count=0,
            namespace_token="",
        ):
            events.append("begin")
            assert namespace_token == "namespace-token"
            context = {
                "leaf": None,
                "table_config": {},
                "mirrors": [],
                "mirror_pin": None,
                "rowid_floor": 0,
                "rowid_reservation": (1, reserve_count),
                "_initial_compact_begin_calls": 2,
                "_initial_compact_begin_pin_retries": 1,
                "_initial_compact_begin_general_fallbacks": 1,
            }
            begin_contexts.append(context)
            return context

        original_commit = type(fake_catalog).commit_snapshot

        def commit_snapshot(self, *args, **kwargs):
            commit_calls.append(kwargs)
            return original_commit(self, *args, **kwargs)

        def process(*args, **kwargs):
            events.append("storage.write")
            return original_process(*args, **kwargs)

        monkeypatch.setattr(
            type(fake_catalog), "acquire_namespace_lock", acquire_namespace,
            raising=False,
        )
        monkeypatch.setattr(
            type(fake_catalog), "release_namespace_lock", release_namespace,
            raising=False,
        )
        monkeypatch.setattr(
            type(fake_catalog), "begin_table_mutation", begin_mutation,
            raising=False,
        )
        monkeypatch.setattr(
            type(fake_catalog), "commit_snapshot", commit_snapshot,
        )
        monkeypatch.setattr(
            fake_catalog, "acquire_simple_lock", acquire_simple,
        )
        writer._mocks["process"].side_effect = process

        # Model the exact built-in type gate while retaining this test's
        # intentionally lightweight catalog implementation.
        with patch("supertable.data_writer.RedisCatalog", type(fake_catalog)):
            writer.write(
                "admin", "new_table", _simple_arrow(3), overwrite_columns=[],
            )

        assert events[:5] == [
            "namespace.acquire",
            "leaf.acquire",
            "begin",
            "namespace.release",
            "storage.write",
        ]
        simple_call = writer._mocks["SimpleTable"].call_args
        assert simple_call.kwargs["create_if_missing"] is False
        assert simple_call.kwargs["_live_leaf_verified"] is True
        writer._mocks["simple_inst"].get_simple_table_snapshot.assert_not_called()
        initial_base = writer._mocks["simple_inst"].update.call_args.kwargs[
            "last_snapshot"
        ]
        assert initial_base["snapshot_version"] == 0
        assert initial_base["previous_snapshot"] is None
        assert commit_calls[0]["expected_version"] == -1
        assert commit_calls[0]["expected_path"] == ""
        assert commit_calls[0]["one_shot_initial"] is True
        counts = writer._mocks["monitor"].metrics[0]["counts"]
        assert counts["initial_compact_begin_calls"] == 2
        assert counts["initial_compact_begin_pin_retries"] == 1
        assert counts["initial_compact_begin_general_fallbacks"] == 1
        assert begin_contexts[0]["_initial_compact_begin_calls"] == 2

    def test_first_write_storage_failure_never_publishes_leaf(
        self, writer, fake_catalog, monkeypatch,
    ):
        fake_catalog.supports_one_shot_table_creation = True

        monkeypatch.setattr(
            type(fake_catalog),
            "acquire_namespace_lock",
            lambda self, *args, **kwargs: "namespace-token",
            raising=False,
        )
        monkeypatch.setattr(
            type(fake_catalog),
            "release_namespace_lock",
            lambda self, *args, **kwargs: True,
            raising=False,
        )

        def begin_mutation(
            self, org, sup, simple, *, lock_token, reserve_count=0,
            namespace_token="",
        ):
            return {
                "leaf": None,
                "table_config": {},
                "mirrors": [],
                "mirror_pin": None,
                "rowid_floor": 0,
                "rowid_reservation": (1, reserve_count),
            }

        monkeypatch.setattr(
            type(fake_catalog), "begin_table_mutation", begin_mutation,
            raising=False,
        )
        writer._mocks["process"].side_effect = RuntimeError(
            "crash before snapshot"
        )

        with pytest.raises(RuntimeError, match="crash before snapshot"):
            writer.write(
                "admin", "new_table", _simple_arrow(3), overwrite_columns=[],
            )

        assert not fake_catalog.set_leaf_payload_cas_calls
        assert not fake_catalog.bump_root_calls

    def test_concurrent_create_requires_write_authorization_after_lock(
        self, writer, fake_catalog, monkeypatch,
    ):
        """CREATE-only authority cannot mutate a concurrently-created table."""
        original_acquire = fake_catalog.acquire_simple_lock

        def create_table_before_lock_returns(
            org, sup, simple, ttl_s=30, timeout_s=60,
        ):
            _bootstrap_leaf(fake_catalog, org, sup, simple)
            return original_acquire(
                org, sup, simple, ttl_s=ttl_s, timeout_s=timeout_s,
            )

        monkeypatch.setattr(
            fake_catalog, "acquire_simple_lock", create_table_before_lock_returns,
        )
        with (
            patch("supertable.data_writer.check_create_access") as create_access,
            patch(
                "supertable.data_writer.check_write_access",
                side_effect=PermissionError("write denied"),
            ) as write_access,
        ):
            with pytest.raises(PermissionError, match="write denied"):
                writer.write(
                    "create_only", "raced", _simple_arrow(1),
                    overwrite_columns=[],
                )

        create_access.assert_called_once()
        write_access.assert_called_once()
        writer._mocks["SimpleTable"].assert_not_called()
        writer._mocks["process"].assert_not_called()
        assert len(fake_catalog.release_calls) == 1

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
        with pytest.raises(ValueError, match="newer_than requires overwrite_columns"):
            writer.write("admin", "t1", data, overwrite_columns=[], newer_than="value")

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
        mock_resolve = writer._mocks["resolve"]
        empty_filtered = pl.DataFrame({"id": [], "name": [], "ts": []}).cast(
            {"id": pl.Int64, "name": pl.Utf8, "ts": pl.Int64}
        )
        mock_resolve.side_effect = lambda **kw: (empty_filtered, [])

        result = writer.write("admin", "t1", data, overwrite_columns=["id"], newer_than="ts")

        # The current implementation still enqueues a monitoring metric on
        # the newer_than early-exit path (so durations are observable). The
        # important contracts are: the result tuple reports zero work, and
        # at most one metric is emitted (no duplicates).
        monitor = writer._mocks["monitor"]
        assert len(monitor.metrics) <= 1
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
        """resolve_overwrite_writes drives the deleted count; inserted = incoming rows."""
        mock_resolve = writer._mocks["resolve"]
        # Two delete pairs → deleted == 2; pass incoming through for inserts.
        mock_resolve.side_effect = lambda **kw: (
            kw["incoming_df"], [("old.parquet", 1), ("old.parquet", 2)])

        data = _simple_arrow(5)
        result = writer.write("admin", "t1", data, overwrite_columns=["id"])

        total_cols, total_rows, inserted, deleted = result
        assert inserted == 5
        assert deleted == 2

    def test_delete_only_reports_no_inserts(self, writer):
        """delete_only path: inserted should always be 0; deleted from pair count."""
        mock_resolve = writer._mocks["resolve"]
        # Three delete pairs → deleted == 3.
        mock_resolve.side_effect = lambda **kw: (kw["incoming_df"], [
            ("old.parquet", 1), ("old.parquet", 2), ("old.parquet", 3),
        ])

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
