"""
Comprehensive test suite for supertable/simple_table.py

Covers:
  1. _spark_type_from_polars_dtype — all dtype branches, fallback
  2. _schema_list_from_polars_df — happy path, empty df, no schema attr, schema raises
  3. SimpleTable.__init__
     - Fast path: leaf exists → skip mkdirs/bootstrap
     - Slow path: leaf absent → makedirs, write_json, set_leaf_payload_cas
     - CAS payload fails → fallback to set_leaf_path_cas
     - Directory layout correctness
  4. SimpleTable.init_simple_table
     - Creates dirs that don't exist, skips existing
     - makedirs exception swallowed
     - Writes initial snapshot JSON
     - CAS payload → fallback to CAS path
  5. SimpleTable.delete
     - RBAC check enforced
     - Storage folder deleted + Redis leaf deleted
     - Storage missing → still deletes Redis
     - Storage.delete FileNotFoundError swallowed
  6. SimpleTable.get_simple_table_snapshot
     - Redis leaf has payload with resources → returns directly (no storage read)
     - Redis leaf has payload.snapshot with resources → returns nested snapshot
     - No payload → falls back to storage read_json
     - No leaf pointer → FileNotFoundError
     - Leaf with no path → FileNotFoundError
  7. SimpleTable.update
     - Merges resources, removes sunset, increments version
     - Uses provided last_snapshot (no redundant read)
     - Falls back to get_simple_table_snapshot when not provided
     - Schema from collect_schema preferred, _schema_list_from_polars_df fallback
     - Writes new snapshot to storage, returns (snapshot, path)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch, call, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------
_MOD = "supertable.simple_table"
_P_REDIS_CAT = f"{_MOD}.RedisCatalog"
_P_CHECK_WRITE = f"{_MOD}.check_write_access"
_P_GEN_FILENAME = f"{_MOD}.generate_filename"
_P_COLLECT_SCHEMA = f"{_MOD}.collect_schema"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_super(org: str = "org", sup: str = "sup") -> MagicMock:
    st = MagicMock()
    st.organization = org
    st.super_name = sup
    st.storage = MagicMock()
    return st


def _make_simple(
    simple_name: str = "events",
    org: str = "org",
    sup: str = "sup",
    mock_st: MagicMock | None = None,
    mock_cat: MagicMock | None = None,
):
    """Build a SimpleTable via __new__ with mocked internals (skips __init__)."""
    from supertable.simple_table import SimpleTable

    st = mock_st or _mock_super(org, sup)
    obj = SimpleTable.__new__(SimpleTable)
    obj.super_table = st
    obj.identity = "tables"
    obj.simple_name = simple_name
    obj.storage = st.storage
    obj.catalog = mock_cat or MagicMock()
    obj.simple_dir = f"{org}/{sup}/tables/{simple_name}"
    obj.data_dir = f"{org}/{sup}/tables/{simple_name}/data"
    obj.snapshot_dir = f"{org}/{sup}/tables/{simple_name}/snapshots"
    return obj


# ===========================================================================
# 1. _spark_type_from_polars_dtype
# ===========================================================================

class TestSparkTypeFromPolarsDtype:

    def test_string_types(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Utf8) == "string"
        assert _spark_type_from_polars_dtype(pl.String) == "string"

    def test_boolean(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Boolean) == "boolean"

    def test_signed_integers(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Int8) == "byte"
        assert _spark_type_from_polars_dtype(pl.Int16) == "short"
        assert _spark_type_from_polars_dtype(pl.Int32) == "integer"
        assert _spark_type_from_polars_dtype(pl.Int64) == "long"

    def test_unsigned_integers(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.UInt8) == "short"
        assert _spark_type_from_polars_dtype(pl.UInt16) == "integer"
        assert _spark_type_from_polars_dtype(pl.UInt32) == "long"
        assert _spark_type_from_polars_dtype(pl.UInt64) == "decimal(20,0)"

    def test_floats(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Float32) == "float"
        assert _spark_type_from_polars_dtype(pl.Float64) == "double"

    def test_temporal(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Date) == "date"
        assert _spark_type_from_polars_dtype(pl.Datetime) == "timestamp"

    def test_binary(self):
        import polars as pl
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Binary) == "binary"

    def test_unknown_dtype_fallback(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype("unknown_type") == "string"

    def test_none_fallback(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(None) == "string"


# ===========================================================================
# 2. _schema_list_from_polars_df
# ===========================================================================

class TestSchemaListFromPolarsDf:

    def test_happy_path(self):
        import polars as pl
        from supertable.simple_table import _schema_list_from_polars_df
        df = pl.DataFrame({"id": [1], "name": ["a"]})
        result = _schema_list_from_polars_df(df)
        assert len(result) == 2
        names = {item["name"] for item in result}
        assert names == {"id", "name"}
        for item in result:
            assert "type" in item
            assert item["nullable"] is True
            assert item["metadata"] == {}

    def test_int64_maps_to_long(self):
        import polars as pl
        from supertable.simple_table import _schema_list_from_polars_df
        df = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
        result = _schema_list_from_polars_df(df)
        assert result[0]["type"] == "long"

    def test_empty_df_preserves_schema(self):
        import polars as pl
        from supertable.simple_table import _schema_list_from_polars_df
        df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
        result = _schema_list_from_polars_df(df)
        assert len(result) == 1
        assert result[0]["name"] == "x"
        assert result[0]["type"] == "long"

    def test_no_schema_attribute_returns_empty(self):
        from supertable.simple_table import _schema_list_from_polars_df
        result = _schema_list_from_polars_df("not a dataframe")
        assert result == []

    def test_schema_access_raises_returns_empty(self):
        from supertable.simple_table import _schema_list_from_polars_df
        bad = MagicMock()
        type(bad).schema = PropertyMock(side_effect=RuntimeError("broken"))
        result = _schema_list_from_polars_df(bad)
        assert result == []

    def test_multiple_columns(self):
        import polars as pl
        from supertable.simple_table import _schema_list_from_polars_df
        df = pl.DataFrame({"a": [1.0], "b": [True], "c": ["x"]})
        result = _schema_list_from_polars_df(df)
        types = {item["name"]: item["type"] for item in result}
        assert types["a"] == "double"
        assert types["b"] == "boolean"
        assert types["c"] == "string"


# ===========================================================================
# 3. SimpleTable.__init__
# ===========================================================================

class TestSimpleTableInit:

    @patch(_P_REDIS_CAT)
    def test_fast_path_leaf_exists_skips_init(self, MockCat):
        from supertable.simple_table import SimpleTable

        mock_cat = MagicMock()
        mock_cat.leaf_exists.return_value = True
        MockCat.return_value = mock_cat

        st = _mock_super("org", "sup")
        obj = SimpleTable(st, "events")

        assert obj.simple_name == "events"
        assert obj.super_table is st
        assert obj.storage is st.storage
        mock_cat.leaf_exists.assert_called_once_with("org", "sup", "events")
        # Should NOT call init_simple_table
        st.storage.makedirs.assert_not_called()
        st.storage.write_json.assert_not_called()

    @patch(_P_GEN_FILENAME, return_value="12345_abc_tables.json")
    @patch(_P_REDIS_CAT)
    def test_slow_path_leaf_absent_initializes(self, MockCat, mock_gen):
        from supertable.simple_table import SimpleTable

        mock_cat = MagicMock()
        mock_cat.leaf_exists.return_value = False
        MockCat.return_value = mock_cat

        st = _mock_super("org", "sup")
        st.storage.exists.return_value = False  # dirs don't exist

        obj = SimpleTable(st, "events")

        # makedirs called for each of the 3 dirs
        assert st.storage.makedirs.call_count == 3
        # write_json called for initial snapshot
        st.storage.write_json.assert_called_once()
        snap_path = st.storage.write_json.call_args[0][0]
        assert "snapshots/12345_abc_tables.json" in snap_path
        snap_data = st.storage.write_json.call_args[0][1]
        assert snap_data["simple_name"] == "events"
        assert snap_data["snapshot_version"] == 0
        assert snap_data["resources"] == []
        assert snap_data["previous_snapshot"] is None
        assert snap_data["schema"] == []
        # CAS called
        mock_cat.set_leaf_payload_cas.assert_called_once()

    @patch(_P_GEN_FILENAME, return_value="snap.json")
    @patch(_P_REDIS_CAT)
    def test_slow_path_cas_fallback(self, MockCat, mock_gen):
        """set_leaf_payload_cas fails → falls back to set_leaf_path_cas."""
        from supertable.simple_table import SimpleTable

        mock_cat = MagicMock()
        mock_cat.leaf_exists.return_value = False
        mock_cat.set_leaf_payload_cas.side_effect = TypeError("old catalog")
        MockCat.return_value = mock_cat

        st = _mock_super()
        st.storage.exists.return_value = False

        obj = SimpleTable(st, "tbl")

        mock_cat.set_leaf_path_cas.assert_called_once()

    @patch(_P_REDIS_CAT)
    def test_directory_layout(self, MockCat):
        from supertable.simple_table import SimpleTable

        MockCat.return_value = MagicMock(leaf_exists=MagicMock(return_value=True))
        st = _mock_super("my_org", "my_sup")

        obj = SimpleTable(st, "my_tbl")

        assert obj.simple_dir == "my_org/my_sup/tables/my_tbl"
        assert obj.data_dir == "my_org/my_sup/tables/my_tbl/data"
        assert obj.snapshot_dir == "my_org/my_sup/tables/my_tbl/snapshots"
        assert obj.identity == "tables"


# ===========================================================================
# 4. SimpleTable.init_simple_table
# ===========================================================================

class TestInitSimpleTable:

    @patch(_P_GEN_FILENAME, return_value="init_snap.json")
    def test_creates_missing_dirs(self, mock_gen):
        obj = _make_simple("tbl", "org", "sup")
        obj.storage.exists.return_value = False

        obj.init_simple_table()

        assert obj.storage.makedirs.call_count == 3
        dirs = [c[0][0] for c in obj.storage.makedirs.call_args_list]
        assert "org/sup/tables/tbl" in dirs
        assert "org/sup/tables/tbl/data" in dirs
        assert "org/sup/tables/tbl/snapshots" in dirs

    @patch(_P_GEN_FILENAME, return_value="init_snap.json")
    def test_skips_existing_dirs(self, mock_gen):
        obj = _make_simple()
        obj.storage.exists.return_value = True

        obj.init_simple_table()

        obj.storage.makedirs.assert_not_called()

    @patch(_P_GEN_FILENAME, return_value="init_snap.json")
    def test_makedirs_exception_swallowed(self, mock_gen):
        obj = _make_simple()
        obj.storage.exists.return_value = False
        obj.storage.makedirs.side_effect = OSError("no-op")

        # Should not raise
        obj.init_simple_table()

        # write_json still called
        obj.storage.write_json.assert_called_once()

    @patch(_P_GEN_FILENAME, return_value="snap_file.json")
    def test_writes_initial_snapshot(self, mock_gen):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = True

        obj.init_simple_table()

        obj.storage.write_json.assert_called_once()
        path, data = obj.storage.write_json.call_args[0]
        assert path == "org/sup/tables/events/snapshots/snap_file.json"
        assert data["simple_name"] == "events"
        assert data["snapshot_version"] == 0
        assert data["previous_snapshot"] is None
        assert data["schema"] == []
        assert data["resources"] == []
        assert "last_updated_ms" in data
        assert "location" in data

    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_cas_payload_preferred(self, mock_gen):
        obj = _make_simple("tbl", "org", "sup")
        obj.storage.exists.return_value = True

        obj.init_simple_table()

        obj.catalog.set_leaf_payload_cas.assert_called_once()
        args = obj.catalog.set_leaf_payload_cas.call_args[0]
        assert args[0] == "org"
        assert args[1] == "sup"
        assert args[2] == "tbl"
        # args[3] is snapshot_data dict
        assert isinstance(args[3], dict)
        assert args[3]["simple_name"] == "tbl"
        # args[4] is path
        assert "snap.json" in args[4]

    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_cas_payload_fails_falls_back_to_path(self, mock_gen):
        obj = _make_simple("tbl", "org", "sup")
        obj.storage.exists.return_value = True
        obj.catalog.set_leaf_payload_cas.side_effect = Exception("old redis")

        obj.init_simple_table()

        obj.catalog.set_leaf_path_cas.assert_called_once()
        args = obj.catalog.set_leaf_path_cas.call_args[0]
        assert args[0] == "org"
        assert args[1] == "sup"
        assert args[2] == "tbl"
        assert "snap.json" in args[3]

    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_initial_snapshot_has_location(self, mock_gen):
        obj = _make_simple("tbl", "org", "sup")
        obj.storage.exists.return_value = True

        obj.init_simple_table()

        data = obj.storage.write_json.call_args[0][1]
        assert data["location"] == "org/sup/tables/tbl"

    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_initial_snapshot_last_updated_ms_is_recent(self, mock_gen):
        obj = _make_simple()
        obj.storage.exists.return_value = True

        before_ms = int(datetime.now().timestamp() * 1000)
        obj.init_simple_table()
        after_ms = int(datetime.now().timestamp() * 1000)

        data = obj.storage.write_json.call_args[0][1]
        assert before_ms <= data["last_updated_ms"] <= after_ms


# ===========================================================================
# 5. SimpleTable.delete
# ===========================================================================

class TestSimpleTableDelete:

    @patch(_P_CHECK_WRITE)
    def test_rbac_checked(self, mock_check):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = False

        obj.delete(role_name="admin")

        mock_check.assert_called_once_with(
            super_name="sup",
            organization="org",
            role_name="admin",
            table_name="events",
        )

    @patch(_P_CHECK_WRITE)
    def test_rbac_denied_propagates(self, mock_check):
        mock_check.side_effect = PermissionError("denied")
        obj = _make_simple()

        with pytest.raises(PermissionError, match="denied"):
            obj.delete(role_name="viewer")

        obj.storage.exists.assert_not_called()
        obj.catalog.delete_simple_table.assert_not_called()

    @patch(_P_CHECK_WRITE)
    def test_happy_path_deletes_storage_and_redis(self, mock_check):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = True

        obj.delete(role_name="admin")

        obj.storage.exists.assert_called_once_with("org/sup/tables/events")
        obj.storage.delete.assert_called_once_with("org/sup/tables/events")
        obj.catalog.delete_simple_table.assert_called_once_with("org", "sup", "events")

    @patch(_P_CHECK_WRITE)
    def test_storage_not_exists_still_deletes_redis(self, mock_check):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = False

        obj.delete(role_name="admin")

        obj.storage.delete.assert_not_called()
        obj.catalog.delete_simple_table.assert_called_once()

    @patch(_P_CHECK_WRITE)
    def test_storage_file_not_found_swallowed(self, mock_check):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = True
        obj.storage.delete.side_effect = FileNotFoundError("gone")

        obj.delete(role_name="admin")

        obj.catalog.delete_simple_table.assert_called_once()

    @patch(_P_CHECK_WRITE)
    def test_storage_other_exception_propagates(self, mock_check):
        obj = _make_simple("events", "org", "sup")
        obj.storage.exists.return_value = True
        obj.storage.delete.side_effect = PermissionError("forbidden")

        with pytest.raises(PermissionError, match="forbidden"):
            obj.delete(role_name="admin")

        # Redis should NOT be deleted when storage raises non-FileNotFoundError
        obj.catalog.delete_simple_table.assert_not_called()

    @patch(_P_CHECK_WRITE)
    def test_delete_folder_path_matches_simple_dir(self, mock_check):
        """delete builds its own path from org/sup/identity/simple — matches simple_dir."""
        obj = _make_simple("my_tbl", "my_org", "my_sup")
        obj.storage.exists.return_value = True

        obj.delete(role_name="admin")

        obj.storage.delete.assert_called_once_with("my_org/my_sup/tables/my_tbl")


# ===========================================================================
# 6. SimpleTable.get_simple_table_snapshot
# ===========================================================================

class TestGetSimpleTableSnapshot:

    def test_no_leaf_pointer_raises(self):
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = None

        with pytest.raises(FileNotFoundError, match="No path"):
            obj.get_simple_table_snapshot()

    def test_leaf_no_path_raises(self):
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {"path": None}

        with pytest.raises(FileNotFoundError, match="No path"):
            obj.get_simple_table_snapshot()

    def test_leaf_empty_path_raises(self):
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {"path": ""}

        with pytest.raises(FileNotFoundError, match="No path"):
            obj.get_simple_table_snapshot()

    def test_payload_with_resources_returns_directly(self):
        """Redis leaf has payload.resources → no storage read."""
        obj = _make_simple()
        payload = {"resources": [{"file": "f1"}], "schema": {}}
        obj.catalog.get_leaf.return_value = {
            "path": "/snap.json",
            "payload": payload,
        }

        data, path = obj.get_simple_table_snapshot()

        assert data is payload
        assert path == "/snap.json"
        obj.storage.read_json.assert_not_called()

    def test_payload_nested_snapshot_returns(self):
        """Redis leaf has payload.snapshot.resources → returns nested snapshot."""
        obj = _make_simple()
        snap = {"resources": [{"file": "f2"}]}
        obj.catalog.get_leaf.return_value = {
            "path": "/snap.json",
            "payload": {"snapshot": snap},
        }

        data, path = obj.get_simple_table_snapshot()

        assert data is snap
        assert path == "/snap.json"
        obj.storage.read_json.assert_not_called()

    def test_no_payload_falls_back_to_storage(self):
        """No usable payload → read from storage."""
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {"path": "/snap.json"}
        expected = {"resources": [{"file": "f3"}]}
        obj.storage.read_json.return_value = expected

        data, path = obj.get_simple_table_snapshot()

        assert data == expected
        assert path == "/snap.json"
        obj.storage.read_json.assert_called_once_with("/snap.json")

    def test_payload_without_resources_falls_back(self):
        """Payload exists but no resources list → storage fallback."""
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {
            "path": "/snap.json",
            "payload": {"other": "data"},
        }
        expected = {"resources": []}
        obj.storage.read_json.return_value = expected

        data, path = obj.get_simple_table_snapshot()

        assert data == expected
        obj.storage.read_json.assert_called_once()

    def test_payload_not_dict_falls_back(self):
        """Payload is a string → storage fallback."""
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {
            "path": "/snap.json",
            "payload": "not_a_dict",
        }
        expected = {"resources": []}
        obj.storage.read_json.return_value = expected

        data, path = obj.get_simple_table_snapshot()
        obj.storage.read_json.assert_called_once()

    def test_empty_resources_list_still_counts(self):
        """Payload with resources=[] → returns directly, no storage fallback."""
        obj = _make_simple()
        payload = {"resources": []}
        obj.catalog.get_leaf.return_value = {"path": "/snap.json", "payload": payload}

        data, path = obj.get_simple_table_snapshot()

        assert data is payload
        obj.storage.read_json.assert_not_called()

    def test_correct_catalog_call(self):
        obj = _make_simple("events", "org", "sup")
        obj.catalog.get_leaf.return_value = {"path": "/p", "payload": {"resources": []}}

        obj.get_simple_table_snapshot()

        obj.catalog.get_leaf.assert_called_once_with("org", "sup", "events")


# ===========================================================================
# 7. SimpleTable.update
# ===========================================================================

class TestUpdate:

    @patch(_P_COLLECT_SCHEMA, return_value={"id": "int64", "val": "object"})
    @patch(_P_GEN_FILENAME, return_value="new_snap.json")
    def test_happy_path_with_provided_snapshot(self, mock_gen, mock_schema):
        obj = _make_simple("events", "org", "sup")

        last_snap = {
            "simple_name": "events",
            "snapshot_version": 3,
            "resources": [
                {"file": "keep.parquet"},
                {"file": "sunset.parquet"},
            ],
            "schema": [],
        }
        new_resources = [{"file": "new.parquet", "rows": 100}]
        sunset_files = {"sunset.parquet"}

        result_snap, result_path = obj.update(
            new_resources=new_resources,
            sunset_files=sunset_files,
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old/snap.json",
        )

        # Resources: kept + new, sunset removed
        file_names = {r["file"] for r in result_snap["resources"]}
        assert "keep.parquet" in file_names
        assert "new.parquet" in file_names
        assert "sunset.parquet" not in file_names

        # Version incremented
        assert result_snap["snapshot_version"] == 4

        # Previous snapshot set
        assert result_snap["previous_snapshot"] == "/old/snap.json"

        # last_updated_ms set
        assert "last_updated_ms" in result_snap

        # Schema from collect_schema (dict)
        assert result_snap["schema"] == {"id": "int64", "val": "object"}

        # Written to storage
        obj.storage.write_json.assert_called_once()
        write_path = obj.storage.write_json.call_args[0][0]
        assert write_path == "org/sup/tables/events/snapshots/new_snap.json"
        assert result_path == write_path

    @patch(_P_COLLECT_SCHEMA, return_value={"id": "int64"})
    @patch(_P_GEN_FILENAME, return_value="new.json")
    def test_falls_back_to_get_snapshot_when_not_provided(self, mock_gen, mock_schema):
        obj = _make_simple()
        obj.catalog.get_leaf.return_value = {
            "path": "/existing/snap.json",
            "payload": {
                "resources": [{"file": "f1.parquet"}],
                "simple_name": "events",
                "snapshot_version": 1,
            },
        }

        result_snap, _ = obj.update(
            new_resources=[{"file": "f2.parquet"}],
            sunset_files=set(),
            model_df=MagicMock(),
        )

        assert result_snap["snapshot_version"] == 2
        files = {r["file"] for r in result_snap["resources"]}
        assert files == {"f1.parquet", "f2.parquet"}

    @patch(_P_COLLECT_SCHEMA, return_value={})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_schema_fallback_to_polars_df(self, mock_gen, mock_schema):
        """collect_schema returns empty dict → uses _schema_list_from_polars_df."""
        import polars as pl
        obj = _make_simple()

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "t"}

        result_snap, _ = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=df,
            last_snapshot=last_snap,
            last_snapshot_path="/p",
        )

        # _schema_list_from_polars_df returns list of dicts
        assert isinstance(result_snap["schema"], list)
        assert len(result_snap["schema"]) == 2
        names = {s["name"] for s in result_snap["schema"]}
        assert names == {"id", "val"}

    @patch(_P_COLLECT_SCHEMA, return_value={"x": "int"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_all_sunset_removes_all_old_resources(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {
            "resources": [{"file": "a.parquet"}, {"file": "b.parquet"}],
            "snapshot_version": 5,
            "simple_name": "t",
        }

        result_snap, _ = obj.update(
            new_resources=[{"file": "c.parquet"}],
            sunset_files={"a.parquet", "b.parquet"},
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        assert len(result_snap["resources"]) == 1
        assert result_snap["resources"][0]["file"] == "c.parquet"

    @patch(_P_COLLECT_SCHEMA, return_value={"x": "int"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_empty_sunset_keeps_all(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {
            "resources": [{"file": "a.parquet"}, {"file": "b.parquet"}],
            "snapshot_version": 2,
            "simple_name": "t",
        }

        result_snap, _ = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        assert len(result_snap["resources"]) == 2
        assert result_snap["snapshot_version"] == 3

    @patch(_P_COLLECT_SCHEMA, return_value={})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_version_zero_increments_to_one(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "t"}

        result_snap, _ = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        assert result_snap["snapshot_version"] == 1

    @patch(_P_COLLECT_SCHEMA, return_value={})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_missing_version_defaults_to_zero_then_increments(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {"resources": [], "simple_name": "t"}  # no snapshot_version key

        result_snap, _ = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        assert result_snap["snapshot_version"] == 1

    @patch(_P_COLLECT_SCHEMA, return_value={"x": "int"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_schema_string_is_valid_json(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "t"}

        result_snap, _ = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        parsed = json.loads(result_snap["schemaString"])
        assert parsed["type"] == "struct"
        assert parsed["fields"] == {"x": "int"}

    @patch(_P_COLLECT_SCHEMA, return_value={"x": "int"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_returns_snapshot_and_path(self, mock_gen, mock_schema):
        obj = _make_simple("tbl", "org", "sup")
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "tbl"}

        result_snap, result_path = obj.update(
            new_resources=[],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        assert isinstance(result_snap, dict)
        assert result_path == "org/sup/tables/tbl/snapshots/snap.json"

    @patch(_P_COLLECT_SCHEMA, return_value={"id": "int64"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_last_updated_ms_is_recent(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "t"}

        before = int(datetime.now().timestamp() * 1000)
        result_snap, _ = obj.update(
            new_resources=[], sunset_files=set(), model_df=MagicMock(),
            last_snapshot=last_snap, last_snapshot_path="/old",
        )
        after = int(datetime.now().timestamp() * 1000)

        assert before <= result_snap["last_updated_ms"] <= after

    @patch(_P_COLLECT_SCHEMA, return_value={"id": "int64"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_new_resources_appended_after_kept(self, mock_gen, mock_schema):
        obj = _make_simple()
        last_snap = {
            "resources": [{"file": "old.parquet"}],
            "snapshot_version": 0,
            "simple_name": "t",
        }

        result_snap, _ = obj.update(
            new_resources=[{"file": "new.parquet"}],
            sunset_files=set(),
            model_df=MagicMock(),
            last_snapshot=last_snap,
            last_snapshot_path="/old",
        )

        files = [r["file"] for r in result_snap["resources"]]
        assert files == ["old.parquet", "new.parquet"]

    @patch(_P_COLLECT_SCHEMA, return_value={"id": "int64"})
    @patch(_P_GEN_FILENAME, return_value="snap.json")
    def test_mutates_last_snapshot_dict(self, mock_gen, mock_schema):
        """update() mutates the provided last_snapshot dict in place."""
        obj = _make_simple()
        last_snap = {"resources": [], "snapshot_version": 0, "simple_name": "t"}

        result_snap, _ = obj.update(
            new_resources=[], sunset_files=set(), model_df=MagicMock(),
            last_snapshot=last_snap, last_snapshot_path="/old",
        )

        # result IS the same dict
        assert result_snap is last_snap
        assert last_snap["snapshot_version"] == 1
