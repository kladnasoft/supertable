"""
Comprehensive test suite for supertable/meta_reader.py

Covers:
  1. _super_meta_cache_ttl_s — env var parsing
  2. _prune_dict — key removal
  3. _get_redis_items — Redis SCAN loop, bytes/str keys, exceptions
  4. _try_parse_leaf_meta — None, bytes, str, empty, invalid JSON, valid JSON
  5. _leaf_to_snapshot_like — all nesting paths (direct, payload, payload.snapshot,
     data, snapshot), non-dict input
  6. _schema_to_dict — dict, list [{name,type}], list [single-key], non-list/dict
  7. MetaReader.__init__ — wiring
  8. MetaReader._get_all_tables — scan loop, dedup, bytes keys, exception
  9. MetaReader.get_tables — RBAC filtering
 10. MetaReader.get_table_schema — single table (Redis hit, fallback),
     super-level aggregation, RBAC denial
 11. MetaReader.collect_simple_table_schema — RBAC, FileNotFoundError, happy path
 12. MetaReader.get_table_stats — single table, super-level, RBAC, missing snapshot
 13. MetaReader.get_super_meta — RBAC, Redis leaf optimization, SimpleTable fallback,
     aggregation, cache hit, cache miss, mget failure
 14. list_supers — sorted output, key parsing
 15. list_tables — sorted output, key parsing
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------
_MOD = "supertable.meta_reader"
_P_CHECK_META = f"{_MOD}.check_meta_access"
_P_REDIS_CAT = f"{_MOD}.RedisCatalog"
_P_SUPER_TABLE = f"{_MOD}.SuperTable"
_P_SIMPLE_TABLE = f"{_MOD}.SimpleTable"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reader(
    super_name: str = "sup",
    organization: str = "org",
    mock_st: MagicMock | None = None,
    mock_cat: MagicMock | None = None,
):
    """Build a MetaReader with mocked SuperTable and RedisCatalog."""
    from supertable.meta_reader import MetaReader

    reader = MetaReader.__new__(MetaReader)
    st = mock_st or MagicMock()
    st.super_name = super_name
    st.organization = organization
    reader.super_table = st
    reader.catalog = mock_cat or MagicMock()
    return reader


def _scan_one_batch(*keys: str):
    """Return a mock scan that yields keys in a single batch then terminates."""
    str_keys = list(keys)
    def scan(cursor, match=None, count=None):
        if cursor == 0:
            return (0, str_keys)
        return (0, [])
    return scan


def _scan_two_batches(batch1: list, batch2: list):
    """Return a mock scan that yields keys across two batches."""
    call_count = [0]
    def scan(cursor, match=None, count=None):
        call_count[0] += 1
        if call_count[0] == 1:
            return (1, batch1)
        return (0, batch2)
    return scan


def _snap(schema=None, resources=None, last_updated_ms=0):
    """Build a minimal snapshot dict."""
    return {
        "schema": schema or {},
        "resources": resources or [],
        "last_updated_ms": last_updated_ms,
    }


# ===========================================================================
# 1. _super_meta_cache_ttl_s
# ===========================================================================

class TestSuperMetaCacheTtl:

    def test_default_is_1(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SUPERTABLE_SUPER_META_CACHE_TTL_S", None)
            assert _super_meta_cache_ttl_s() == 1.0

    def test_empty_string(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": ""}):
            assert _super_meta_cache_ttl_s() == 1.0

    def test_custom_value(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "5.5"}):
            assert _super_meta_cache_ttl_s() == 5.5

    def test_zero(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "0"}):
            assert _super_meta_cache_ttl_s() == 0.0

    def test_negative_clamped_to_zero(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "-3"}):
            assert _super_meta_cache_ttl_s() == 0.0

    def test_invalid_string(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "abc"}):
            assert _super_meta_cache_ttl_s() == 1.0

    def test_whitespace_stripped(self):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "  2.0  "}):
            assert _super_meta_cache_ttl_s() == 2.0


# ===========================================================================
# 2. _prune_dict
# ===========================================================================

class TestPruneDict:

    def test_removes_specified_keys(self):
        from supertable.meta_reader import _prune_dict
        d = {"a": 1, "b": 2, "c": 3}
        assert _prune_dict(d, {"b", "c"}) == {"a": 1}

    def test_empty_keys_to_remove(self):
        from supertable.meta_reader import _prune_dict
        d = {"a": 1}
        assert _prune_dict(d, set()) == {"a": 1}

    def test_nonexistent_keys_ignored(self):
        from supertable.meta_reader import _prune_dict
        d = {"a": 1}
        assert _prune_dict(d, {"z"}) == {"a": 1}

    def test_does_not_mutate_original(self):
        from supertable.meta_reader import _prune_dict
        d = {"a": 1, "b": 2}
        result = _prune_dict(d, {"b"})
        assert "b" in d  # original unchanged
        assert "b" not in result

    def test_empty_dict(self):
        from supertable.meta_reader import _prune_dict
        assert _prune_dict({}, {"a"}) == {}


# ===========================================================================
# 3. _get_redis_items
# ===========================================================================

class TestGetRedisItems:

    @patch(_P_REDIS_CAT)
    def test_returns_string_keys(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = _scan_one_batch("supertable:org:s1:meta:root")
        MockCat.return_value = mock_cat

        result = _get_redis_items("supertable:org:*:meta:root")
        assert result == ["supertable:org:s1:meta:root"]

    @patch(_P_REDIS_CAT)
    def test_decodes_bytes_keys(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = _scan_one_batch(b"supertable:org:s1:meta:root")
        MockCat.return_value = mock_cat

        result = _get_redis_items("pattern")
        assert result == ["supertable:org:s1:meta:root"]

    @patch(_P_REDIS_CAT)
    def test_multiple_batches(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = _scan_two_batches(
            ["supertable:org:s1:meta:root"],
            ["supertable:org:s2:meta:root"],
        )
        MockCat.return_value = mock_cat

        result = _get_redis_items("pattern")
        assert len(result) == 2

    @patch(_P_REDIS_CAT)
    def test_redis_exception_returns_empty(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = ConnectionError("down")
        MockCat.return_value = mock_cat

        assert _get_redis_items("pattern") == []

    @patch(_P_REDIS_CAT)
    def test_empty_scan(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.return_value = (0, [])
        MockCat.return_value = mock_cat

        assert _get_redis_items("pattern") == []


# ===========================================================================
# 4. _try_parse_leaf_meta
# ===========================================================================

class TestTryParseLeafMeta:

    def test_none_input(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        assert _try_parse_leaf_meta(None) is None

    def test_bytes_valid_json(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        raw = json.dumps({"resources": []}).encode("utf-8")
        result = _try_parse_leaf_meta(raw)
        assert result == {"resources": []}

    def test_bytearray_valid_json(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        raw = bytearray(json.dumps({"a": 1}).encode("utf-8"))
        assert _try_parse_leaf_meta(raw) == {"a": 1}

    def test_str_valid_json(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        assert _try_parse_leaf_meta('{"x": 42}') == {"x": 42}

    def test_empty_string(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        assert _try_parse_leaf_meta("") is None

    def test_whitespace_only(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        assert _try_parse_leaf_meta("   ") is None

    def test_invalid_json(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        assert _try_parse_leaf_meta("{not json}") is None

    def test_non_string_non_bytes(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        # int gets str()-converted → "123" → valid JSON number → returns 123
        assert _try_parse_leaf_meta(123) == 123

    def test_bytes_with_whitespace(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        raw = b'  {"key": "val"}  '
        assert _try_parse_leaf_meta(raw) == {"key": "val"}


# ===========================================================================
# 5. _leaf_to_snapshot_like
# ===========================================================================

class TestLeafToSnapshotLike:

    def test_non_dict_returns_none(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        assert _leaf_to_snapshot_like("string") is None
        assert _leaf_to_snapshot_like(42) is None
        assert _leaf_to_snapshot_like(None) is None

    def test_direct_resources_key(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        meta = {"resources": [{"file": "f1"}]}
        assert _leaf_to_snapshot_like(meta) is meta

    def test_payload_with_resources(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        payload = {"resources": [{"file": "f1"}]}
        meta = {"payload": payload}
        assert _leaf_to_snapshot_like(meta) is payload

    def test_payload_snapshot_with_resources(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        snap = {"resources": [{"file": "f1"}]}
        meta = {"payload": {"snapshot": snap}}
        assert _leaf_to_snapshot_like(meta) is snap

    def test_data_with_resources(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        data = {"resources": [{"file": "f1"}]}
        meta = {"data": data}
        assert _leaf_to_snapshot_like(meta) is data

    def test_snapshot_with_resources(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        snap = {"resources": [{"file": "f1"}]}
        meta = {"snapshot": snap}
        assert _leaf_to_snapshot_like(meta) is snap

    def test_no_resources_returns_none(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        assert _leaf_to_snapshot_like({"other": "stuff"}) is None

    def test_resources_not_list_returns_none(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        assert _leaf_to_snapshot_like({"resources": "not_a_list"}) is None

    def test_empty_resources_list(self):
        from supertable.meta_reader import _leaf_to_snapshot_like
        meta = {"resources": []}
        assert _leaf_to_snapshot_like(meta) is meta

    def test_priority_direct_over_payload(self):
        """Direct resources takes priority over payload.resources."""
        from supertable.meta_reader import _leaf_to_snapshot_like
        meta = {"resources": [{"file": "direct"}], "payload": {"resources": [{"file": "nested"}]}}
        result = _leaf_to_snapshot_like(meta)
        assert result is meta


# ===========================================================================
# 6. _schema_to_dict
# ===========================================================================

class TestSchemaToDict:

    def test_dict_passthrough(self):
        from supertable.meta_reader import _schema_to_dict
        d = {"col1": "int", "col2": "str"}
        assert _schema_to_dict(d) is d

    def test_list_name_type_format(self):
        from supertable.meta_reader import _schema_to_dict
        schema = [{"name": "id", "type": "int"}, {"name": "val", "type": "str"}]
        assert _schema_to_dict(schema) == {"id": "int", "val": "str"}

    def test_list_single_key_dict_fallback(self):
        from supertable.meta_reader import _schema_to_dict
        schema = [{"col1": "bigint"}]
        assert _schema_to_dict(schema) == {"col1": "bigint"}

    def test_list_mixed_formats(self):
        from supertable.meta_reader import _schema_to_dict
        schema = [
            {"name": "id", "type": "int"},
            {"amount": "double"},
        ]
        result = _schema_to_dict(schema)
        assert result == {"id": "int", "amount": "double"}

    def test_empty_list(self):
        from supertable.meta_reader import _schema_to_dict
        assert _schema_to_dict([]) == {}

    def test_non_dict_items_ignored(self):
        from supertable.meta_reader import _schema_to_dict
        assert _schema_to_dict(["string", 42]) == {}

    def test_non_list_non_dict(self):
        from supertable.meta_reader import _schema_to_dict
        assert _schema_to_dict(42) == {}
        assert _schema_to_dict(None) == {}
        assert _schema_to_dict("str") == {}

    def test_name_none_in_item(self):
        from supertable.meta_reader import _schema_to_dict
        schema = [{"name": None, "type": "int"}, {"name": "ok", "type": "str"}]
        # name=None → skipped (None is not not-None)
        # Actually: item.get("name") returns None → `if name is not None` fails
        assert _schema_to_dict(schema) == {"ok": "str"}

    def test_name_is_integer(self):
        from supertable.meta_reader import _schema_to_dict
        schema = [{"name": 0, "type": "int"}]
        assert _schema_to_dict(schema) == {"0": "int"}


# ===========================================================================
# 7. MetaReader.__init__
# ===========================================================================

class TestMetaReaderInit:

    @patch(_P_REDIS_CAT)
    @patch(_P_SUPER_TABLE)
    def test_init_creates_dependencies(self, MockST, MockCat):
        from supertable.meta_reader import MetaReader
        mock_st = MagicMock()
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        MockCat.return_value = mock_cat

        reader = MetaReader("my_super", "my_org")
        MockST.assert_called_once_with(super_name="my_super", organization="my_org")
        MockCat.assert_called_once()
        assert reader.super_table is mock_st
        assert reader.catalog is mock_cat


# ===========================================================================
# 8. MetaReader._get_all_tables
# ===========================================================================

class TestGetAllTables:

    def test_extracts_table_names_from_keys(self):
        reader = _make_reader("sup", "org")
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:events",
            "supertable:org:sup:meta:leaf:users",
        )
        assert reader._get_all_tables() == ["events", "users"]

    def test_deduplicates_table_names(self):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:events",
            "supertable:org:sup:meta:leaf:events",
        )
        assert reader._get_all_tables() == ["events"]

    def test_handles_bytes_keys(self):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            b"supertable:org:sup:meta:leaf:events",
        )
        assert reader._get_all_tables() == ["events"]

    def test_multi_batch_scan(self):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_two_batches(
            ["supertable:org:sup:meta:leaf:t1"],
            ["supertable:org:sup:meta:leaf:t2"],
        )
        result = reader._get_all_tables()
        assert set(result) == {"t1", "t2"}

    def test_redis_exception_returns_empty(self):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = ConnectionError("down")
        assert reader._get_all_tables() == []

    def test_empty_scan(self):
        reader = _make_reader()
        reader.catalog.r.scan.return_value = (0, [])
        assert reader._get_all_tables() == []

    def test_empty_table_name_skipped(self):
        reader = _make_reader()
        # Key ending with ":" produces empty table name
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:",
            "supertable:org:sup:meta:leaf:valid",
        )
        assert reader._get_all_tables() == ["valid"]


# ===========================================================================
# 9. MetaReader.get_tables
# ===========================================================================

class TestGetTables:

    @patch(_P_CHECK_META)
    def test_returns_accessible_tables(self, mock_check):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
            "supertable:org:sup:meta:leaf:t2",
        )
        # All pass RBAC
        mock_check.return_value = None

        assert reader.get_tables("admin") == ["t1", "t2"]
        assert mock_check.call_count == 2

    @patch(_P_CHECK_META)
    def test_filters_out_denied_tables(self, mock_check):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:public",
            "supertable:org:sup:meta:leaf:secret",
        )
        def side_effect(super_name, organization, role_name, table_name):
            if table_name == "secret":
                raise PermissionError("denied")
        mock_check.side_effect = side_effect

        assert reader.get_tables("viewer") == ["public"]

    @patch(_P_CHECK_META)
    def test_all_denied_returns_empty(self, mock_check):
        reader = _make_reader()
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
        )
        mock_check.side_effect = PermissionError("no")

        assert reader.get_tables("nobody") == []

    @patch(_P_CHECK_META)
    def test_no_tables_returns_empty(self, mock_check):
        reader = _make_reader()
        reader.catalog.r.scan.return_value = (0, [])

        assert reader.get_tables("admin") == []
        mock_check.assert_not_called()


# ===========================================================================
# 10. MetaReader.get_table_schema
# ===========================================================================

class TestGetTableSchema:

    @patch(_P_CHECK_META)
    def test_rbac_denied_returns_none(self, mock_check):
        reader = _make_reader()
        mock_check.side_effect = PermissionError("denied")

        assert reader.get_table_schema("events", "viewer") is None

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_single_table_redis_hit(self, mock_check, MockST):
        """Schema read from Redis leaf — no SimpleTable fallback."""
        reader = _make_reader()
        leaf_data = json.dumps({
            "resources": [{"file": "f1"}],
            "schema": [{"name": "id", "type": "int"}, {"name": "val", "type": "str"}],
        })
        reader.catalog.r.get.return_value = leaf_data.encode()

        result = reader.get_table_schema("events", "admin")
        assert result == [{"id": "int", "val": "str"}]
        MockST.assert_not_called()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_single_table_fallback_to_storage(self, mock_check, MockST):
        """Redis returns no usable leaf → falls back to SimpleTable."""
        reader = _make_reader()
        reader.catalog.r.get.return_value = None

        mock_st_inst = MagicMock()
        mock_st_inst.get_simple_table_snapshot.return_value = (
            {"schema": {"id": "bigint"}, "resources": []},
            "/path",
        )
        MockST.return_value = mock_st_inst

        result = reader.get_table_schema("events", "admin")
        assert result == [{"id": "bigint"}]
        MockST.assert_called_once()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_single_table_storage_file_not_found(self, mock_check, MockST):
        """Storage fallback raises FileNotFoundError → returns [{}]."""
        reader = _make_reader()
        reader.catalog.r.get.return_value = None

        MockST.return_value.get_simple_table_snapshot.side_effect = FileNotFoundError()

        result = reader.get_table_schema("events", "admin")
        assert result == [{}]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_aggregates_schemas(self, mock_check, MockST):
        """table_name == super_name → aggregate schemas across all tables."""
        reader = _make_reader("sup", "org")
        # _get_all_tables returns two tables
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
            "supertable:org:sup:meta:leaf:t2",
        )
        # mget returns leaf data for both
        leaf1 = json.dumps({"resources": [], "schema": [{"name": "id", "type": "int"}]})
        leaf2 = json.dumps({"resources": [], "schema": [{"name": "val", "type": "str"}]})
        reader.catalog.r.mget.return_value = [leaf1.encode(), leaf2.encode()]

        result = reader.get_table_schema("sup", "admin")
        assert result == [{"id": "int", "val": "str"}]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_deduplicates_columns(self, mock_check, MockST):
        """Same column in multiple tables appears once."""
        reader = _make_reader("sup", "org")
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
            "supertable:org:sup:meta:leaf:t2",
        )
        leaf1 = json.dumps({"resources": [], "schema": [{"name": "id", "type": "int"}]})
        leaf2 = json.dumps({"resources": [], "schema": [{"name": "id", "type": "int"}, {"name": "x", "type": "str"}]})
        reader.catalog.r.mget.return_value = [leaf1.encode(), leaf2.encode()]

        result = reader.get_table_schema("sup", "admin")
        assert result == [{"id": "int", "x": "str"}]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_mget_exception_returns_empty_schema(self, mock_check, MockST):
        """mget fails → raws=[] → zip produces no iterations → empty schema."""
        reader = _make_reader("sup", "org")
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
        )
        reader.catalog.r.mget.side_effect = ConnectionError("down")

        mock_st_inst = MagicMock()
        mock_st_inst.get_simple_table_snapshot.return_value = (
            {"schema": {"col": "float"}, "resources": []},
            "/p",
        )
        MockST.return_value = mock_st_inst

        result = reader.get_table_schema("sup", "admin")
        # mget exception → raws=[] → zip(tables, []) → 0 iterations → no fallback
        assert result == [{}]
        MockST.assert_not_called()

    @patch(_P_CHECK_META)
    def test_schema_result_is_sorted(self, mock_check):
        reader = _make_reader()
        leaf_data = json.dumps({
            "resources": [],
            "schema": [{"name": "z_col", "type": "str"}, {"name": "a_col", "type": "int"}],
        })
        reader.catalog.r.get.return_value = leaf_data.encode()

        result = reader.get_table_schema("events", "admin")
        keys = list(result[0].keys())
        assert keys == sorted(keys)


# ===========================================================================
# 11. MetaReader.collect_simple_table_schema
# ===========================================================================

class TestCollectSimpleTableSchema:

    @patch(_P_CHECK_META)
    def test_rbac_denied_returns_without_modifying_set(self, mock_check):
        mock_check.side_effect = PermissionError("denied")
        reader = _make_reader()
        schemas = set()
        reader.collect_simple_table_schema(schemas, "secret", "viewer")
        assert schemas == set()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_file_not_found_returns_without_error(self, mock_check, MockST):
        reader = _make_reader()
        MockST.return_value.get_simple_table_snapshot.side_effect = FileNotFoundError()

        schemas = set()
        reader.collect_simple_table_schema(schemas, "missing", "admin")
        assert schemas == set()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_happy_path_adds_schema_tuple(self, mock_check, MockST):
        reader = _make_reader()
        MockST.return_value.get_simple_table_snapshot.return_value = (
            {"schema": {"id": "int", "val": "str"}, "resources": []},
            "/path",
        )

        schemas = set()
        reader.collect_simple_table_schema(schemas, "events", "admin")
        assert len(schemas) == 1
        schema_tuple = next(iter(schemas))
        assert dict(schema_tuple) == {"id": "int", "val": "str"}

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_deduplicates_identical_schemas(self, mock_check, MockST):
        reader = _make_reader()
        MockST.return_value.get_simple_table_snapshot.return_value = (
            {"schema": {"id": "int"}, "resources": []},
            "/path",
        )

        schemas = set()
        reader.collect_simple_table_schema(schemas, "t1", "admin")
        reader.collect_simple_table_schema(schemas, "t2", "admin")
        # Same schema → set deduplicates
        assert len(schemas) == 1


# ===========================================================================
# 12. MetaReader.get_table_stats
# ===========================================================================

class TestGetTableStats:

    @patch(_P_CHECK_META)
    def test_rbac_denied_returns_empty(self, mock_check):
        mock_check.side_effect = PermissionError("denied")
        reader = _make_reader()
        assert reader.get_table_stats("secret", "viewer") == []

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_single_table_returns_pruned_snapshot(self, mock_check, MockST):
        reader = _make_reader()
        snap_data = {
            "simple_name": "events",
            "schema": [{"name": "id"}],
            "resources": [{"file": "f1"}],
            "previous_snapshot": "/old",
            "location": "/loc",
            "snapshot_version": 3,
        }
        MockST.return_value.get_simple_table_snapshot.return_value = (snap_data, "/path")

        result = reader.get_table_stats("events", "admin")
        assert len(result) == 1
        stat = result[0]
        # Pruned keys should be removed
        assert "previous_snapshot" not in stat
        assert "schema" not in stat
        assert "location" not in stat
        # Non-pruned keys kept
        assert stat["simple_name"] == "events"
        assert stat["snapshot_version"] == 3

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_single_table_file_not_found_returns_empty(self, mock_check, MockST):
        reader = _make_reader()
        MockST.return_value.get_simple_table_snapshot.side_effect = FileNotFoundError()

        assert reader.get_table_stats("missing", "admin") == []

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_aggregates_all_tables(self, mock_check, MockST):
        reader = _make_reader("sup", "org")
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
            "supertable:org:sup:meta:leaf:t2",
        )

        call_count = [0]
        def mock_snap():
            call_count[0] += 1
            return (
                {"simple_name": f"t{call_count[0]}", "resources": [], "schema": {}},
                "/p",
            )
        MockST.return_value.get_simple_table_snapshot.side_effect = lambda: mock_snap()

        result = reader.get_table_stats("sup", "admin")
        assert len(result) == 2

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_skips_missing_tables(self, mock_check, MockST):
        """One table's snapshot is missing → that table skipped, others included."""
        reader = _make_reader("sup", "org")
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
            "supertable:org:sup:meta:leaf:t2",
        )

        call_count = [0]
        def mock_snap_side():
            call_count[0] += 1
            if call_count[0] == 1:
                raise FileNotFoundError("missing")
            return ({"simple_name": "t2", "resources": [], "schema": {}}, "/p")

        MockST.return_value.get_simple_table_snapshot.side_effect = mock_snap_side

        result = reader.get_table_stats("sup", "admin")
        assert len(result) == 1


# ===========================================================================
# 13. MetaReader.get_super_meta
# ===========================================================================

class TestGetSuperMeta:

    def setup_method(self):
        """Clear the module-level cache before each test."""
        import supertable.meta_reader as mod
        with mod._SUPER_META_CACHE_LOCK:
            mod._SUPER_META_CACHE.clear()

    @patch(_P_CHECK_META)
    def test_rbac_denied_returns_none(self, mock_check):
        mock_check.side_effect = PermissionError("no")
        reader = _make_reader()
        assert reader.get_super_meta("viewer") is None

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_happy_path_with_redis_leaf(self, mock_check, MockST, mock_ttl):
        """Leaf data available in Redis → no SimpleTable fallback."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 5, "ts": 9999}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:events",
        )

        leaf = json.dumps({
            "resources": [{"file": "f1", "rows": 100, "file_size": 5000}],
            "last_updated_ms": 1234,
        })
        reader.catalog.r.mget.return_value = [leaf.encode()]

        result = reader.get_super_meta("admin")
        assert result is not None
        sup = result["super"]
        assert sup["name"] == "sup"
        assert sup["files"] == 1
        assert sup["rows"] == 100
        assert sup["size"] == 5000
        assert sup["version"] == 5
        assert sup["updated_utc"] == 9999
        assert len(sup["tables"]) == 1
        assert sup["tables"][0]["name"] == "events"
        assert sup["meta_path"] == "redis://org/sup"
        # No SimpleTable fallback
        MockST.assert_not_called()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_fallback_to_simple_table(self, mock_check, MockST, mock_ttl):
        """Redis leaf has no usable data → SimpleTable fallback."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
        )
        # mget returns non-parseable data
        reader.catalog.r.mget.return_value = [None]

        mock_st = MagicMock()
        mock_st.get_simple_table_snapshot.return_value = (
            {"resources": [{"file": "f", "rows": 50, "file_size": 2000}], "last_updated_ms": 500},
            "/path",
        )
        MockST.return_value = mock_st

        result = reader.get_super_meta("admin")
        assert result["super"]["rows"] == 50
        assert result["super"]["size"] == 2000
        MockST.assert_called_once()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_mget_exception_degrades_gracefully(self, mock_check, mock_ttl):
        """mget raises → falls back to SimpleTable per table."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
        )
        reader.catalog.r.mget.side_effect = ConnectionError("down")

        with patch(_P_SIMPLE_TABLE) as MockST:
            MockST.return_value.get_simple_table_snapshot.return_value = (
                {"resources": [{"file": "f", "rows": 10, "file_size": 100}], "last_updated_ms": 0},
                "/p",
            )
            result = reader.get_super_meta("admin")
            assert result["super"]["rows"] == 10

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_table_exception_skipped(self, mock_check, mock_ttl):
        """FileNotFoundError for a table → that table skipped in totals."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:bad",
            "supertable:org:sup:meta:leaf:good",
        )
        # First leaf → None (will fallback), second → valid
        reader.catalog.r.mget.return_value = [None, json.dumps({
            "resources": [{"file": "f", "rows": 5, "file_size": 50}],
        }).encode()]

        with patch(_P_SIMPLE_TABLE) as MockST:
            MockST.return_value.get_simple_table_snapshot.side_effect = FileNotFoundError()
            result = reader.get_super_meta("admin")
            # "bad" skipped, "good" counted
            assert result["super"]["rows"] == 5
            assert len(result["super"]["tables"]) == 1

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_no_tables_returns_empty_structure(self, mock_check, mock_ttl):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 0, "ts": 0}
        reader.catalog.r.scan.return_value = (0, [])
        reader.catalog.r.mget.return_value = []

        result = reader.get_super_meta("admin")
        assert result["super"]["files"] == 0
        assert result["super"]["rows"] == 0
        assert result["super"]["tables"] == []

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_root_none_uses_defaults(self, mock_check, mock_ttl):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = None
        reader.catalog.r.scan.return_value = (0, [])
        reader.catalog.r.mget.return_value = []

        result = reader.get_super_meta("admin")
        assert result["super"]["version"] == 0

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_multi_resource_aggregation(self, mock_check, mock_ttl):
        """Multiple resources in one table sum correctly."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:t1",
        )
        leaf = json.dumps({
            "resources": [
                {"file": "f1", "rows": 100, "file_size": 1000},
                {"file": "f2", "rows": 200, "file_size": 2000},
            ],
        })
        reader.catalog.r.mget.return_value = [leaf.encode()]

        result = reader.get_super_meta("admin")
        assert result["super"]["files"] == 2
        assert result["super"]["rows"] == 300
        assert result["super"]["size"] == 3000

    @patch(_P_CHECK_META)
    def test_cache_hit_returns_cached_result(self, mock_check):
        """Second call with same root version → cache hit."""
        import supertable.meta_reader as mod

        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 5, "ts": 999}
        reader.catalog.r.scan.side_effect = _scan_one_batch()
        reader.catalog.r.mget.return_value = []

        # First call populates cache
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result1 = reader.get_super_meta("admin")

        # Reset scan to prove it's not called again
        reader.catalog.r.scan.reset_mock()
        reader.catalog.r.scan.side_effect = None
        reader.catalog.r.scan.return_value = (0, ["should_not_appear"])

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result2 = reader.get_super_meta("admin")

        assert result2 is result1
        # scan should NOT have been called for the cache hit
        reader.catalog.r.scan.assert_not_called()

    @patch(_P_CHECK_META)
    def test_cache_miss_on_version_change(self, mock_check):
        """Root version changes → cache miss → fresh scan."""
        import supertable.meta_reader as mod

        reader = _make_reader("sup", "org")
        reader.catalog.r.mget.return_value = []

        # First call with version 1
        reader.catalog.get_root.return_value = {"version": 1, "ts": 100}
        reader.catalog.r.scan.side_effect = _scan_one_batch()

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result1 = reader.get_super_meta("admin")

        # Second call with version 2 → cache miss
        reader.catalog.get_root.return_value = {"version": 2, "ts": 200}
        reader.catalog.r.scan.side_effect = _scan_one_batch(
            "supertable:org:sup:meta:leaf:new_table",
        )
        leaf = json.dumps({"resources": [{"file": "f", "rows": 99, "file_size": 1}]})
        reader.catalog.r.mget.return_value = [leaf.encode()]

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result2 = reader.get_super_meta("admin")

        assert result2 is not result1
        assert result2["super"]["rows"] == 99


# ===========================================================================
# 14. list_supers
# ===========================================================================

class TestListSupers:

    @patch(f"{_MOD}._get_redis_items")
    def test_extracts_and_sorts_super_names(self, mock_items):
        from supertable.meta_reader import list_supers
        mock_items.return_value = [
            "supertable:org:zeta:meta:root",
            "supertable:org:alpha:meta:root",
            "supertable:org:mid:meta:root",
        ]
        result = list_supers("org")
        assert result == ["alpha", "mid", "zeta"]
        mock_items.assert_called_once_with("supertable:org:*:meta:root")

    @patch(f"{_MOD}._get_redis_items")
    def test_empty_returns_empty(self, mock_items):
        from supertable.meta_reader import list_supers
        mock_items.return_value = []
        assert list_supers("org") == []


# ===========================================================================
# 15. list_tables
# ===========================================================================

class TestListTables:

    @patch(f"{_MOD}._get_redis_items")
    def test_extracts_and_sorts_table_names(self, mock_items):
        from supertable.meta_reader import list_tables
        mock_items.return_value = [
            "supertable:org:sup:meta:leaf:users",
            "supertable:org:sup:meta:leaf:events",
            "supertable:org:sup:meta:leaf:logs",
        ]
        result = list_tables("org", "sup")
        assert result == ["events", "logs", "users"]
        mock_items.assert_called_once_with("supertable:org:sup:meta:leaf:*")

    @patch(f"{_MOD}._get_redis_items")
    def test_empty_returns_empty(self, mock_items):
        from supertable.meta_reader import list_tables
        mock_items.return_value = []
        assert list_tables("org", "sup") == []