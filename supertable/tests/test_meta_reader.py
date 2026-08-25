"""
Comprehensive test suite for supertable/meta_reader.py

Covers:
  1. _super_meta_cache_ttl_s — env var parsing
  2. _prune_dict — key removal
  3. _get_redis_items — Redis SCAN loop, bytes/str keys, strict failures
  4. _try_parse_leaf_meta — strict absent/bytes/str/JSON handling
  5. _schema_to_dict — dict, list [{name,type}], list [single-key], non-list/dict
  6. MetaReader.__init__ — wiring
  7. MetaReader._get_all_tables — scan loop, dedup, strict failures
  8. MetaReader.get_tables — RBAC filtering and strict backend failures
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
import logging
import os
import time
import traceback
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
# Settings patching helper
# ---------------------------------------------------------------------------

from dataclasses import replace as _dc_replace
from supertable.config.settings import settings as _settings
from supertable import meta_reader as _meta_reader_module


def _patch_settings(monkeypatch, **overrides):
    """Substitute the per-module ``settings`` binding for the duration of
    a test. ``settings`` is a frozen dataclass so we use dataclasses.replace
    to derive a copy with overrides applied, then patch the binding inside
    the meta_reader module (which imported ``settings`` by name).
    """
    new_settings = _dc_replace(_settings, **overrides)
    monkeypatch.setattr(_meta_reader_module, "settings", new_settings)
    return new_settings


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


def _wire_catalog_scan(catalog, *keys, raise_exc: Exception | None = None):
    """Wire up lifecycle-pinned ``catalog.scan_leaf_items`` for MetaReader.

    Most call sites retain readable full-key fixtures; translate those to the
    validated item shape returned by RedisCatalog. Non-key objects pass through
    so corruption tests can exercise the MetaReader boundary directly.
    """
    def to_item(value):
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if isinstance(value, str) and "meta:leaf:doc:" in value:
            return {"simple": value.rsplit("meta:leaf:doc:", 1)[-1]}
        return value

    if raise_exc is not None:
        catalog.scan_leaf_items.side_effect = raise_exc
        return
    catalog.scan_leaf_items.side_effect = None
    catalog.scan_leaf_items.return_value = iter([to_item(key) for key in keys])


def _wire_catalog_scan_factory(catalog, key_provider):
    """Have ``scan_leaf_items`` invoke ``key_provider()`` each call (used
    by tests that need fresh iterators across multiple ``_get_all_tables``
    invocations)."""
    catalog.scan_leaf_items.side_effect = lambda *a, **kw: iter(
        {
            "simple": (
                key.decode("utf-8") if isinstance(key, bytes) else key
            ).rsplit("meta:leaf:doc:", 1)[-1]
        }
        for key in key_provider()
    )


def _snap(schema=None, resources=None, last_updated_ms=0):
    """Build a minimal snapshot dict."""
    return {
        "schema": schema or {},
        "resources": resources or [],
        "last_updated_ms": last_updated_ms,
    }


def _complete_leaf(
    resources,
    *,
    schema=None,
    tombstone_rows=0,
    tombstone_format=None,
    last_updated_ms=0,
):
    """Build a Redis leaf whose payload is safe for the metadata fast path."""
    has_tombstone = tombstone_rows > 0
    snapshot = {
        "snapshot_version": 1,
        "schema": schema or {},
        "resources": resources,
        "last_updated_ms": last_updated_ms,
        "tombstone": (
            "table/tombstone/manifest.json"
            if has_tombstone and tombstone_format == 2
            else "dv.parquet" if has_tombstone else None
        ),
        "tombstone_rows": tombstone_rows,
        "tombstone_digest": "0" * 64 if has_tombstone else None,
        "_row_filter": None,
    }
    if tombstone_format is not None:
        snapshot["tombstone_format"] = tombstone_format
    return {
        "version": 1,
        "path": "snapshots/v1.json",
        "payload": snapshot,
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

    def test_custom_value(self, monkeypatch):
        # _super_meta_cache_ttl_s reads from settings (frozen dataclass), not
        # the live env. Patch the meta_reader.settings binding instead.
        from supertable.meta_reader import _super_meta_cache_ttl_s
        _patch_settings(monkeypatch, SUPERTABLE_SUPER_META_CACHE_TTL_S=5.5)
        assert _super_meta_cache_ttl_s() == 5.5

    def test_zero(self, monkeypatch):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        # 0 is treated as "use default" by the source (`if not val: return 1.0`).
        _patch_settings(monkeypatch, SUPERTABLE_SUPER_META_CACHE_TTL_S=0.0)
        assert _super_meta_cache_ttl_s() == 1.0

    def test_negative_clamped_to_zero(self, monkeypatch):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        _patch_settings(monkeypatch, SUPERTABLE_SUPER_META_CACHE_TTL_S=-3.0)
        assert _super_meta_cache_ttl_s() == 0.0

    def test_invalid_string(self):
        # Settings parses env vars on load, so an invalid string is already
        # converted to None — the function returns its default.
        from supertable.meta_reader import _super_meta_cache_ttl_s
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "abc"}):
            assert _super_meta_cache_ttl_s() == 1.0

    def test_whitespace_stripped(self, monkeypatch):
        from supertable.meta_reader import _super_meta_cache_ttl_s
        # Env-var whitespace stripping happens in the settings loader; here
        # we just verify the resulting numeric value is honoured.
        _patch_settings(monkeypatch, SUPERTABLE_SUPER_META_CACHE_TTL_S=2.0)
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
        mock_cat.r.scan.side_effect = _scan_one_batch("supertable:org:lakes:s1:meta:root")
        MockCat.return_value = mock_cat

        result = _get_redis_items("supertable:org:*:meta:root")
        assert result == ["supertable:org:lakes:s1:meta:root"]

    @patch(_P_REDIS_CAT)
    def test_decodes_bytes_keys(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = _scan_one_batch(b"supertable:org:lakes:s1:meta:root")
        MockCat.return_value = mock_cat

        result = _get_redis_items("pattern")
        assert result == ["supertable:org:lakes:s1:meta:root"]

    @patch(_P_REDIS_CAT)
    def test_multiple_batches(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = _scan_two_batches(
            ["supertable:org:lakes:s1:meta:root"],
            ["supertable:org:lakes:s2:meta:root"],
        )
        MockCat.return_value = mock_cat

        result = _get_redis_items("pattern")
        assert len(result) == 2

    @patch(_P_REDIS_CAT)
    def test_redis_exception_propagates(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.side_effect = ConnectionError("down")
        MockCat.return_value = mock_cat

        with pytest.raises(ConnectionError, match="down"):
            _get_redis_items("pattern")

    @patch(_P_REDIS_CAT)
    def test_invalid_key_type_is_corruption(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        mock_cat = MagicMock()
        mock_cat.r.scan.return_value = (0, [123])
        MockCat.return_value = mock_cat

        with pytest.raises(RuntimeError, match="invalid key type"):
            _get_redis_items("pattern")

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
        with pytest.raises(RuntimeError, match="empty"):
            _try_parse_leaf_meta("")

    def test_whitespace_only(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        with pytest.raises(RuntimeError, match="empty"):
            _try_parse_leaf_meta("   ")

    def test_invalid_json(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        secret = "signature=META_LEAF_JSON_SENTINEL"
        with pytest.raises(RuntimeError, match="not valid JSON") as raised:
            _try_parse_leaf_meta('{"path":"' + secret)
        rendered = "".join(traceback.format_exception(raised.value))
        assert secret not in rendered
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    def test_non_string_non_bytes(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        with pytest.raises(RuntimeError, match="invalid type"):
            _try_parse_leaf_meta(123)

    def test_json_scalar_is_corruption(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        with pytest.raises(RuntimeError, match="not a JSON object"):
            _try_parse_leaf_meta("123")

    def test_invalid_utf8_is_corruption(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        with pytest.raises(UnicodeDecodeError):
            _try_parse_leaf_meta(b"\xff")

    def test_bytes_with_whitespace(self):
        from supertable.meta_reader import _try_parse_leaf_meta
        raw = b'  {"key": "val"}  '
        assert _try_parse_leaf_meta(raw) == {"key": "val"}


# ===========================================================================
# 5. _schema_to_dict
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
        # MetaReader is read-only by contract — it must pass
        # create_if_missing=False so a missing supertable raises
        # SuperTableNotFoundError instead of being bootstrapped as a
        # side effect of opening the reader.
        MockST.assert_called_once_with(
            super_name="my_super",
            organization="my_org",
            create_if_missing=False,
        )
        MockCat.assert_called_once()
        assert reader.super_table is mock_st
        assert reader.catalog is mock_cat


# ===========================================================================
# 8. MetaReader._get_all_tables
# ===========================================================================

class TestGetAllTables:

    def test_extracts_table_names_from_keys(self):
        reader = _make_reader("sup", "org")
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
            "supertable:org:lakes:sup:meta:leaf:doc:users",
        )
        assert reader._get_all_tables() == ["events", "users"]

    def test_deduplicates_table_names(self):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        assert reader._get_all_tables() == ["events"]

    def test_handles_bytes_keys(self):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            b"supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        assert reader._get_all_tables() == ["events"]

    def test_multi_batch_scan(self):
        # The catalog exposes one pinned item iterator; page boundaries are
        # encapsulated. We just supply both leaves.
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        result = reader._get_all_tables()
        assert set(result) == {"t1", "t2"}

    def test_catalog_exception_propagates(self):
        reader = _make_reader()
        _wire_catalog_scan(reader.catalog, raise_exc=ConnectionError("down"))
        with pytest.raises(ConnectionError, match="down"):
            reader._get_all_tables()

    def test_empty_scan(self):
        reader = _make_reader()
        _wire_catalog_scan(reader.catalog)  # no keys
        assert reader._get_all_tables() == []

    def test_empty_table_name_is_corruption(self):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:",
        )
        with pytest.raises(RuntimeError, match="invalid table name"):
            reader._get_all_tables()

    def test_invalid_catalog_table_name_drops_poisoned_identity(self):
        secret = "api_token=META_TABLE_NAME_SENTINEL"
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:" + secret,
        )

        with pytest.raises(RuntimeError, match="invalid table name") as raised:
            reader._get_all_tables()

        rendered = "".join(traceback.format_exception(raised.value))
        assert secret not in rendered
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None

    @pytest.mark.parametrize(
        "bad_item",
        [
            123,
            None,
            {},
            {"simple": ""},
            {"simple": 123},
        ],
    )
    def test_corrupt_scan_item_propagates(self, bad_item):
        reader = _make_reader()
        reader.catalog.scan_leaf_items.return_value = iter([bad_item])
        with pytest.raises(RuntimeError, match="invalid"):
            reader._get_all_tables()

    def test_table_deletion_state_propagates(self):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        reader.catalog.check_deletion_intent_absent.side_effect = RuntimeError(
            "durable deletion intent",
        )

        with pytest.raises(RuntimeError, match="deletion intent"):
            reader._get_all_tables()


# ===========================================================================
# 9. MetaReader.get_tables
# ===========================================================================

class TestGetTables:

    @patch(_P_CHECK_META)
    def test_returns_accessible_tables(self, mock_check):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        # All pass RBAC
        mock_check.return_value = None

        assert reader.get_tables("admin") == ["t1", "t2"]
        assert mock_check.call_count == 2

    @patch(_P_CHECK_META)
    def test_filters_out_denied_tables(self, mock_check):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:public",
            "supertable:org:lakes:sup:meta:leaf:doc:secret",
        )
        def side_effect(super_name, organization, role_name, table_name):
            if table_name == "secret":
                raise PermissionError("denied")
        mock_check.side_effect = side_effect

        assert reader.get_tables("viewer") == ["public"]

    @patch(_P_CHECK_META)
    def test_all_denied_returns_empty(self, mock_check):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        mock_check.side_effect = PermissionError("no")

        assert reader.get_tables("nobody") == []

    @patch(_P_CHECK_META)
    def test_no_tables_returns_empty(self, mock_check):
        reader = _make_reader()
        _wire_catalog_scan(reader.catalog)

        assert reader.get_tables("admin") == []
        mock_check.assert_not_called()

    @patch(_P_CHECK_META)
    def test_authorization_backend_failure_propagates(self, mock_check):
        reader = _make_reader()
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        mock_check.side_effect = TimeoutError("RBAC unavailable")

        with pytest.raises(TimeoutError, match="RBAC unavailable"):
            reader.get_tables("viewer")


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
        leaf_data = json.dumps(_complete_leaf(
            [{"file": "f1"}],
            schema=[
                {"name": "id", "type": "int"},
                {"name": "val", "type": "str"},
            ],
        ))
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
        # _get_all_tables now goes through catalog.scan_leaf_items
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        # mget returns leaf data for both
        leaf1 = json.dumps(_complete_leaf(
            [], schema=[{"name": "id", "type": "int"}],
        ))
        leaf2 = json.dumps(_complete_leaf(
            [], schema=[{"name": "val", "type": "str"}],
        ))
        reader.catalog.r.mget.return_value = [leaf1.encode(), leaf2.encode()]

        result = reader.get_table_schema("sup", "admin")
        assert result == [{"id": "int", "val": "str"}]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_deduplicates_columns(self, mock_check, MockST):
        """Same column in multiple tables appears once."""
        reader = _make_reader("sup", "org")
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        leaf1 = json.dumps(_complete_leaf(
            [], schema=[{"name": "id", "type": "int"}],
        ))
        leaf2 = json.dumps(_complete_leaf(
            [],
            schema=[
                {"name": "id", "type": "int"},
                {"name": "x", "type": "str"},
            ],
        ))
        reader.catalog.r.mget.return_value = [leaf1.encode(), leaf2.encode()]

        result = reader.get_table_schema("sup", "admin")
        assert result == [{"id": "int", "x": "str"}]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_mget_exception_propagates(self, mock_check, MockST):
        """A metadata-cache outage is not misreported as missing metadata."""
        reader = _make_reader("sup", "org")
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        reader.catalog.r.mget.side_effect = ConnectionError("down")

        with pytest.raises(ConnectionError, match="down"):
            reader.get_table_schema("sup", "admin")
        MockST.assert_not_called()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_super_level_incomplete_mget_is_corruption(self, mock_check, MockST):
        reader = _make_reader("sup", "org")
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        reader.catalog.r.mget.return_value = [None]

        with pytest.raises(RuntimeError, match="incomplete"):
            reader.get_table_schema("sup", "admin")
        MockST.assert_not_called()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_corrupt_leaf_propagates_without_storage_fallback(
        self, mock_check, MockST,
    ):
        reader = _make_reader()
        reader.catalog.r.get.return_value = b"{not-json}"

        with pytest.raises(RuntimeError, match="not valid JSON"):
            reader.get_table_schema("events", "admin")
        MockST.assert_not_called()

    @patch(_P_CHECK_META)
    def test_schema_result_is_sorted(self, mock_check):
        reader = _make_reader()
        leaf_data = json.dumps(_complete_leaf(
            [],
            schema=[
                {"name": "z_col", "type": "str"},
                {"name": "a_col", "type": "int"},
            ],
        ))
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


def test_metadata_failure_logs_never_render_identity_or_backend_text(caplog):
    secret = "api_token=META_SENTINEL;https://metadata.invalid/private"
    table_name = "customer-meta-sentinel"
    role_name = "customer-role-sentinel"
    reader = _make_reader()
    reader.catalog.r.get.return_value = None
    caplog.set_level(logging.DEBUG, logger=_MOD)

    with patch(_P_CHECK_META), patch(_P_SIMPLE_TABLE) as MockST:
        MockST.return_value.get_simple_table_snapshot.side_effect = (
            FileNotFoundError(secret)
        )
        assert reader.get_table_schema(table_name, role_name) == [{}]
        assert reader.get_table_stats(table_name, role_name) == []

    with patch(_P_CHECK_META, side_effect=PermissionError(secret)):
        schemas = set()
        reader.collect_simple_table_schema(schemas, table_name, role_name)
        assert schemas == set()

    rendered = "\n".join(record.getMessage() for record in caplog.records)
    assert secret not in rendered
    assert table_name not in rendered
    assert role_name not in rendered
    assert "snapshot_missing; error_type=FileNotFoundError" in rendered
    assert "metadata_access_denied; error_type=PermissionError" in rendered
    assert all(record.exc_info is None for record in caplog.records)


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
        # Keep the legacy resource list shape without exposing storage paths.
        assert stat["resources"] == [{}]
        assert "file" not in stat["resources"][0]

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_resource_projection_rejects_nested_control_data(
        self, mock_check, MockST,
    ):
        reader = _make_reader()
        MockST.return_value.get_simple_table_snapshot.return_value = ({
            "simple_name": "events",
            "schema": {"id": "long", "name": "string"},
            "resources": [
                {
                    "file": "https://storage.invalid/object?signature=secret",
                    "rows": 1,
                    "file_size": 10,
                    "stats_rows": True,
                    "object_seal": {"etag": "secret"},
                    "integer_domain_bounds": {
                        "id": {"min": 0, "source": "/private/path"},
                    },
                    "column_max_value_bytes": {
                        "id": 8,
                        "unknown": "/private/path",
                    },
                },
                {
                    "rows": 1,
                    "file_size": 11,
                    "column_max_value_bytes": {
                        "name": {"nested": "credential"},
                    },
                },
            ],
            "tombstone": None,
            "tombstone_rows": 0,
            "tombstone_digest": None,
        }, "/path")

        stat = reader.get_table_stats("events", "admin")[0]

        assert stat["resources"] == [
            {
                "rows": 1,
                "file_size": 10,
                "column_max_value_bytes": {"id": 8},
            },
            {"rows": 1, "file_size": 11},
        ]
        serialized = json.dumps(stat)
        assert "secret" not in serialized
        assert "private" not in serialized
        assert "credential" not in serialized

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
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
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
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
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
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )

        leaf = json.dumps(_complete_leaf(
            [{"file": "f1", "rows": 100, "file_size": 5000}],
            last_updated_ms=1234,
        ))
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
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        # mget returns non-parseable data
        reader.catalog.r.mget.return_value = [None]

        mock_st = MagicMock()
        mock_st.get_simple_table_snapshot.return_value = (
            {
                "resources": [{"file": "f", "rows": 50, "file_size": 2000}],
                "last_updated_ms": 500,
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
            },
            "/path",
        )
        MockST.return_value = mock_st

        result = reader.get_super_meta("admin")
        assert result["super"]["rows"] == 50
        assert result["super"]["size"] == 2000
        MockST.assert_called_once()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_mget_exception_propagates(self, mock_check, mock_ttl):
        """A Redis outage must not be reported as empty/partial metadata."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        reader.catalog.r.mget.side_effect = ConnectionError("down")

        with patch(_P_SIMPLE_TABLE) as MockST:
            with pytest.raises(ConnectionError, match="down"):
                reader.get_super_meta("admin")
            MockST.assert_not_called()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_incomplete_mget_is_corruption(self, mock_check, mock_ttl):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
            "supertable:org:lakes:sup:meta:leaf:doc:t2",
        )
        reader.catalog.r.mget.return_value = [None]

        with patch(_P_SIMPLE_TABLE) as MockST:
            with pytest.raises(RuntimeError, match="incomplete"):
                reader.get_super_meta("admin")
            MockST.assert_not_called()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_corrupt_leaf_propagates(self, mock_check, mock_ttl):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        reader.catalog.r.mget.return_value = [b"not-json"]

        with patch(_P_SIMPLE_TABLE) as MockST:
            with pytest.raises(RuntimeError, match="not valid JSON"):
                reader.get_super_meta("admin")
            MockST.assert_not_called()

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_table_exception_skipped(self, mock_check, mock_ttl, caplog):
        """FileNotFoundError for a table → that table skipped in totals."""
        secret = "access_token=META_SUPER_SENTINEL"
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 1000}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:bad",
            "supertable:org:lakes:sup:meta:leaf:doc:good",
        )
        # First leaf → None (will fallback), second → valid
        reader.catalog.r.mget.return_value = [None, json.dumps(_complete_leaf(
            [{"file": "f", "rows": 5, "file_size": 50}],
        )).encode()]

        caplog.set_level(logging.DEBUG, logger=_MOD)
        with patch(_P_SIMPLE_TABLE) as MockST:
            MockST.return_value.get_simple_table_snapshot.side_effect = (
                FileNotFoundError(secret)
            )
            result = reader.get_super_meta("admin")
            # "bad" skipped, "good" counted
            assert result["super"]["rows"] == 5
            assert len(result["super"]["tables"]) == 1
        assert secret not in caplog.text
        assert "snapshot_missing; error_type=FileNotFoundError" in caplog.text
        assert all(record.exc_info is None for record in caplog.records)

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_no_tables_returns_empty_structure(self, mock_check, mock_ttl):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 0, "ts": 0}
        _wire_catalog_scan(reader.catalog)
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
        _wire_catalog_scan(reader.catalog)
        reader.catalog.r.mget.return_value = []

        result = reader.get_super_meta("admin")
        assert result["super"]["version"] == 0

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_CHECK_META)
    def test_multi_resource_aggregation(self, mock_check, mock_ttl):
        """Multiple resources in one table sum correctly."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:t1",
        )
        leaf = json.dumps(_complete_leaf(
            [
                {"file": "f1", "rows": 100, "file_size": 1000},
                {"file": "f2", "rows": 200, "file_size": 2000},
            ],
        ))
        reader.catalog.r.mget.return_value = [leaf.encode()]

        result = reader.get_super_meta("admin")
        assert result["super"]["files"] == 2
        assert result["super"]["rows"] == 300
        assert result["super"]["size"] == 3000

    def test_pre_dv_snapshot_live_rows_are_all_physical_rows(self):
        from supertable.meta_reader import _validated_live_row_count

        assert _validated_live_row_count(
            {"snapshot_version": 1, "schema": {}, "resources": []},
            37,
        ) == 37

    def test_v2_4_empty_snapshot_live_rows_are_all_physical_rows(self):
        from supertable.meta_reader import _validated_live_row_count

        assert _validated_live_row_count(
            {
                "snapshot_version": 1,
                "schema": {},
                "resources": [],
                "tombstone": None,
                "tombstone_rows": 0,
            },
            37,
        ) == 37

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_tombstone_rows_deducted_from_total(self, mock_check, MockST, mock_ttl):
        """Live deletion-vector size is subtracted from physical row sums."""
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        leaf = json.dumps(_complete_leaf(
            [{"file": "f1", "rows": 100, "file_size": 5000}],
            tombstone_rows=30,
        ))
        reader.catalog.r.mget.return_value = [leaf.encode()]

        result = reader.get_super_meta("admin")
        # 100 physical rows - 30 tombstoned = 70 live rows
        assert result["super"]["rows"] == 70
        # Physical file count/size are unaffected by logical deletes.
        assert result["super"]["files"] == 1
        assert result["super"]["size"] == 5000

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_v2_tombstone_rows_deducted_from_total(
        self, mock_check, MockST, mock_ttl,
    ):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        leaf = json.dumps(_complete_leaf(
            [{"file": "f1", "rows": 100, "file_size": 5000}],
            tombstone_rows=30,
            tombstone_format=2,
        ))
        reader.catalog.r.mget.return_value = [leaf.encode()]

        result = reader.get_super_meta("admin")

        assert result["super"]["rows"] == 70

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_tombstone_rows_exceeding_physical_rows_is_rejected(
        self, mock_check, MockST, mock_ttl,
    ):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        leaf = json.dumps(_complete_leaf(
            [{"file": "f1", "rows": 20, "file_size": 5000}],
            tombstone_rows=50,
        ))
        reader.catalog.r.mget.return_value = [leaf.encode()]

        with pytest.raises(RuntimeError, match="exceed physical"):
            reader.get_super_meta("admin")

    @patch(f"{_MOD}._super_meta_cache_ttl_s", return_value=0.0)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_META)
    def test_malformed_tombstone_state_is_never_counted_as_zero(
        self, mock_check, MockST, mock_ttl,
    ):
        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 1, "ts": 0}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:events",
        )
        malformed = {
            "snapshot_version": 1,
            "schema": {},
            "resources": [{"file": "f", "rows": 20, "file_size": 100}],
            "tombstone": None,
            "tombstone_rows": "0",
            "tombstone_digest": None,
            "_row_filter": None,
        }
        # Redis rejects this as a complete cache, then the immutable fallback
        # must reject the same malformed state rather than report 20 live rows.
        reader.catalog.r.mget.return_value = [json.dumps({
            "version": 1,
            "path": "snapshot.json",
            "payload": malformed,
        }).encode()]
        MockST.return_value.get_simple_table_snapshot.return_value = (
            malformed,
            "snapshot.json",
        )

        with pytest.raises(RuntimeError, match="invalid deletion-vector"):
            reader.get_super_meta("admin")

    @patch(_P_CHECK_META)
    def test_cache_hit_returns_cached_result(self, mock_check):
        """Second call with same root version → cache hit."""
        import supertable.meta_reader as mod

        reader = _make_reader("sup", "org")
        reader.catalog.get_root.return_value = {"version": 5, "ts": 999}
        _wire_catalog_scan(reader.catalog)
        reader.catalog.r.mget.return_value = []

        # First call populates cache
        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result1 = reader.get_super_meta("admin")

        # Reset to prove it's not called again
        reader.catalog.scan_leaf_items.reset_mock()
        reader.catalog.scan_leaf_items.side_effect = None
        reader.catalog.scan_leaf_items.return_value = iter([])

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result2 = reader.get_super_meta("admin")

        assert result2 == result1
        assert result2 is not result1
        # Aggregate visibility is re-evaluated before serving the cached
        # payload so a role that lost its final visible child cannot retain
        # access until the metadata TTL expires.
        reader.catalog.scan_leaf_items.assert_called_once_with(
            "org", "sup", count=1000,
        )

    @patch(_P_CHECK_META)
    def test_cache_miss_on_version_change(self, mock_check):
        """Root version changes → cache miss → fresh scan."""
        import supertable.meta_reader as mod

        reader = _make_reader("sup", "org")
        reader.catalog.r.mget.return_value = []

        # First call with version 1
        reader.catalog.get_root.return_value = {"version": 1, "ts": 100}
        _wire_catalog_scan(reader.catalog)

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result1 = reader.get_super_meta("admin")

        # Second call with version 2 → cache miss
        reader.catalog.get_root.return_value = {"version": 2, "ts": 200}
        _wire_catalog_scan(
            reader.catalog,
            "supertable:org:lakes:sup:meta:leaf:doc:new_table",
        )
        leaf = json.dumps(_complete_leaf(
            [{"file": "f", "rows": 99, "file_size": 1}],
        ))
        reader.catalog.r.mget.return_value = [leaf.encode()]

        with patch.dict(os.environ, {"SUPERTABLE_SUPER_META_CACHE_TTL_S": "60"}):
            result2 = reader.get_super_meta("admin")

        assert result2 is not result1
        assert result2["super"]["rows"] == 99


# ===========================================================================
# 14. list_supers
# ===========================================================================

class TestListSupers:

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    @patch(f"{_MOD}._get_redis_items")
    def test_extracts_and_sorts_super_names(
        self, mock_items, MockCat, mock_check,
    ):
        # list_supers now requires a role_name and applies RBAC filtering.
        # Stub check_meta_access to allow everything so we can verify the
        # parsing/sorting behaviour.
        from supertable.meta_reader import list_supers
        from supertable import redis_keys as RK
        roots = [
            RK.meta_root("org", "zeta"),
            RK.meta_root("org", "alpha"),
            RK.meta_root("org", "mid"),
        ]

        mock_items.return_value = roots
        catalog = MagicMock()
        catalog.get_root.side_effect = lambda _org, sup: {
            "version": 1,
            "ts": 1,
            "simple": sup,
        }
        catalog.scan_leaf_items.side_effect = lambda _org, _sup, count: iter(
            [{"simple": "visible"}],
        )
        MockCat.return_value = catalog
        mock_check.return_value = None
        result = list_supers("org", role_name="superadmin")
        assert result == ["alpha", "mid", "zeta"]
        assert mock_items.call_args_list[0].args == (
            RK.meta_root_pattern_for_org("org"),
        )
        assert mock_items.call_count == 1
        assert catalog.get_root.call_count == 3
        assert catalog.scan_leaf_items.call_count == 3

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    @patch(f"{_MOD}._get_redis_items")
    def test_empty_returns_empty(self, mock_items, MockCat, mock_check):
        from supertable.meta_reader import list_supers
        mock_items.return_value = []
        mock_check.return_value = None
        assert list_supers("org", role_name="superadmin") == []
        MockCat.return_value.get_root.assert_not_called()

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    @patch(f"{_MOD}._get_redis_items")
    def test_corrupt_root_document_propagates(
        self, mock_items, MockCat, mock_check,
    ):
        from supertable.meta_reader import list_supers
        from supertable import redis_keys as RK

        mock_items.return_value = [RK.meta_root("org", "broken")]
        MockCat.return_value.get_root.side_effect = RuntimeError(
            "Corrupt Redis root JSON",
        )
        with pytest.raises(RuntimeError, match="Corrupt Redis root JSON"):
            list_supers("org", role_name="admin")
        mock_check.assert_not_called()

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    @patch(f"{_MOD}._get_redis_items")
    def test_namespace_deletion_state_propagates(
        self, mock_items, MockCat, mock_check,
    ):
        from supertable.meta_reader import list_supers
        from supertable import redis_keys as RK

        mock_items.return_value = [RK.meta_root("org", "deleting")]
        catalog = MockCat.return_value
        catalog.get_root.return_value = {"version": 1, "ts": 1}
        catalog.scan_leaf_items.side_effect = RuntimeError(
            "durable deletion intent",
        )
        with pytest.raises(RuntimeError, match="deletion intent"):
            list_supers("org", role_name="admin")
        mock_check.assert_not_called()

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    @patch(f"{_MOD}._get_redis_items")
    def test_malformed_root_scan_key_propagates(
        self, mock_items, MockCat, mock_check,
    ):
        from supertable.meta_reader import list_supers

        mock_items.return_value = [
            "supertable:org:lakes:sup:meta:root:unexpected",
        ]
        with pytest.raises(RuntimeError, match="invalid catalog key"):
            list_supers("org", role_name="admin")
        MockCat.return_value.get_root.assert_not_called()
        mock_check.assert_not_called()


# ===========================================================================
# 15. list_tables
# ===========================================================================

class TestListTables:

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    def test_extracts_and_sorts_table_names(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables
        MockCat.return_value.scan_leaf_items.return_value = iter([
            {"simple": "users"},
            {"simple": "events"},
            {"simple": "logs"},
        ])
        mock_check.return_value = None
        result = list_tables("org", "sup", role_name="superadmin")
        assert result == ["events", "logs", "users"]
        MockCat.return_value.scan_leaf_items.assert_called_once_with(
            "org", "sup", count=1000,
        )

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    def test_replica_uses_catalog_resolution(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables

        # RedisCatalog owns replica resolution and lifecycle pinning. The
        # module-level API must enumerate through it, never raw-scan target
        # keys (which are intentionally absent for a replica).
        MockCat.return_value.scan_leaf_items.return_value = iter([
            {"simple": "source_table"},
        ])
        mock_check.return_value = None
        assert list_tables("org", "replica", "reader") == ["source_table"]
        MockCat.return_value.scan_leaf_items.assert_called_once_with(
            "org", "replica", count=1000,
        )

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    def test_empty_returns_empty(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables
        MockCat.return_value.scan_leaf_items.return_value = iter([])
        mock_check.return_value = None
        assert list_tables("org", "sup", role_name="superadmin") == []

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    def test_leaf_corruption_propagates(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables

        MockCat.return_value.scan_leaf_items.side_effect = RuntimeError(
            "Malformed catalog leaf",
        )
        with pytest.raises(RuntimeError, match="Malformed catalog leaf"):
            list_tables("org", "sup", role_name="admin")
        mock_check.assert_not_called()

    @patch(f"{_MOD}.check_meta_access")
    @patch(_P_REDIS_CAT)
    def test_leaf_deletion_state_propagates(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables

        catalog = MockCat.return_value
        catalog.scan_leaf_items.return_value = iter([{"simple": "gone"}])
        catalog.check_deletion_intent_absent.side_effect = RuntimeError(
            "durable deletion intent",
        )
        with pytest.raises(RuntimeError, match="deletion intent"):
            list_tables("org", "sup", role_name="admin")
        mock_check.assert_not_called()
