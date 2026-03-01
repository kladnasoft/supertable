"""
Comprehensive tests for the dedup-on-read feature:

  1. DataWriter.configure_table()  — RBAC, validation, Redis persistence, cache
  2. DataWriter._get_table_config() — cache-hit / cache-miss / Redis fallback
  3. DataWriter.write() — __timestamp__ injection when dedup_on_read enabled
  4. RedisCatalog.set_table_config() / get_table_config() — Redis key, payload, edge cases

All external dependencies (Redis, Storage, RBAC, Monitoring) are mocked.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import MagicMock, patch, call

import polars as pl
import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Helpers (same conventions as test_supertable_all.py)
# ---------------------------------------------------------------------------

_DW_MOD = "supertable.data_writer"
_RC_MOD = "supertable.redis_catalog"


def _arrow_table(data: dict) -> pa.Table:
    return pa.table(data)


def _polars_df(data: dict) -> pl.DataFrame:
    return pl.DataFrame(data)


def _dummy_snapshot(simple_name="tbl", resources=None, version=0):
    return {
        "simple_name": simple_name,
        "location": f"org/super/tables/{simple_name}",
        "snapshot_version": version,
        "last_updated_ms": 1000,
        "previous_snapshot": None,
        "schema": [],
        "resources": resources or [],
    }


def _resource_entry(file="out.parquet", rows=5, columns=3, file_size=1024, stats=None):
    return {
        "file": file,
        "file_size": file_size,
        "rows": rows,
        "columns": columns,
        "stats": stats or {},
    }


def _make_writer():
    """Create a DataWriter with SuperTable and RedisCatalog mocked."""
    with patch(f"{_DW_MOD}.SuperTable") as mock_super_cls, \
         patch(f"{_DW_MOD}.RedisCatalog") as mock_catalog_cls:
        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.get_table_config.return_value = None
        mock_catalog_cls.return_value = mock_catalog

        from supertable.data_writer import DataWriter
        dw = DataWriter("test_super", "test_org")
    return dw, mock_catalog


def _make_writer_for_write():
    """Create a fully-mocked DataWriter ready for write() calls.
    Returns (dw, mock_catalog, mock_process_overlap) for assertions.
    """
    patches = {
        "super": patch(f"{_DW_MOD}.SuperTable"),
        "catalog": patch(f"{_DW_MOD}.RedisCatalog"),
        "access": patch(f"{_DW_MOD}.check_write_access"),
        "find_overlap": patch(f"{_DW_MOD}.find_and_lock_overlapping_files"),
        "process_overlap": patch(f"{_DW_MOD}.process_overlapping_files"),
        "simple": patch(f"{_DW_MOD}.SimpleTable"),
        "mirror": patch(f"{_DW_MOD}.MirrorFormats"),
        "monitor": patch(f"{_DW_MOD}.get_monitoring_logger"),
    }

    mocks = {k: p.start() for k, p in patches.items()}

    mock_super = MagicMock()
    mock_super.super_name = "test_super"
    mock_super.organization = "test_org"
    mocks["super"].return_value = mock_super

    mock_catalog = MagicMock()
    mock_catalog.acquire_simple_lock.return_value = "lock_token"
    mock_catalog.release_simple_lock.return_value = True
    mock_catalog.get_table_config.return_value = None
    mocks["catalog"].return_value = mock_catalog

    mock_simple = MagicMock()
    mock_simple.data_dir = "/tmp/data"
    snapshot = _dummy_snapshot("tbl", resources=[])
    mock_simple.get_simple_table_snapshot.return_value = (snapshot, "snap.json")
    mock_simple.update.return_value = (snapshot, "new_snap.json")
    mocks["simple"].return_value = mock_simple

    mocks["find_overlap"].return_value = set()
    mocks["process_overlap"].return_value = (
        3, 0, 3, 2, [_resource_entry("new.parquet")], set()
    )

    mock_monitor_inst = MagicMock()
    mocks["monitor"].return_value = mock_monitor_inst

    from supertable.data_writer import DataWriter
    dw = DataWriter("test_super", "test_org")

    class _Ctx:
        catalog = mock_catalog
        process_overlap = mocks["process_overlap"]
        _patches = patches

        @staticmethod
        def stop_all():
            for p in patches.values():
                p.stop()

    return dw, _Ctx()


# ===========================================================================
# 1. configure_table — validation and persistence
# ===========================================================================

class TestConfigureTableValidation:
    """Input validation for configure_table()."""

    def test_empty_primary_keys_raises(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            with pytest.raises(ValueError, match="non-empty list"):
                dw.configure_table("admin", "events", primary_keys=[], dedup_on_read=True)

    def test_primary_keys_not_list_raises(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            with pytest.raises(ValueError, match="non-empty list"):
                dw.configure_table("admin", "events", primary_keys="customer_id", dedup_on_read=True)

    def test_primary_keys_none_raises(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            with pytest.raises(ValueError, match="non-empty list"):
                dw.configure_table("admin", "events", primary_keys=None, dedup_on_read=True)


class TestConfigureTableRBAC:
    """RBAC enforcement on configure_table()."""

    def test_calls_check_write_access(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access") as mock_access:
            dw.configure_table("admin", "events", ["id"], dedup_on_read=True)

            mock_access.assert_called_once_with(
                super_name="test_super",
                organization="test_org",
                role_name="admin",
                table_name="events",
            )

    def test_access_denied_propagates(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access", side_effect=PermissionError("denied")):
            with pytest.raises(PermissionError):
                dw.configure_table("viewer", "events", ["id"], dedup_on_read=True)


class TestConfigureTablePersistence:
    """Redis persistence and local cache update."""

    def test_writes_to_redis(self):
        dw, mock_catalog = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            dw.configure_table("admin", "events", ["cust_id", "order_id"], dedup_on_read=True)

        mock_catalog.set_table_config.assert_called_once_with(
            "test_org", "test_super", "events",
            {"primary_keys": ["cust_id", "order_id"], "dedup_on_read": True},
        )

    def test_updates_local_cache(self):
        dw, _ = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            dw.configure_table("admin", "events", ["id"], dedup_on_read=True)

        assert "events" in dw._table_config_cache
        assert dw._table_config_cache["events"]["dedup_on_read"] is True
        assert dw._table_config_cache["events"]["primary_keys"] == ["id"]

    def test_dedup_defaults_false(self):
        dw, mock_catalog = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            dw.configure_table("admin", "events", ["id"])

        config_arg = mock_catalog.set_table_config.call_args[0][3]
        assert config_arg["dedup_on_read"] is False

    def test_overwrite_existing_config(self):
        dw, mock_catalog = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            dw.configure_table("admin", "events", ["id"], dedup_on_read=True)
            dw.configure_table("admin", "events", ["id", "region"], dedup_on_read=False)

        assert mock_catalog.set_table_config.call_count == 2
        assert dw._table_config_cache["events"]["primary_keys"] == ["id", "region"]
        assert dw._table_config_cache["events"]["dedup_on_read"] is False


# ===========================================================================
# 2. _get_table_config — cache behaviour
# ===========================================================================

class TestGetTableConfigCache:
    """Cache-hit, cache-miss, and Redis fallback."""

    def test_cache_hit_no_redis_call(self):
        dw, mock_catalog = _make_writer()
        dw._table_config_cache["events"] = {"dedup_on_read": True, "primary_keys": ["id"]}

        result = dw._get_table_config("events")

        assert result["dedup_on_read"] is True
        mock_catalog.get_table_config.assert_not_called()

    def test_cache_miss_calls_redis_once(self):
        dw, mock_catalog = _make_writer()
        mock_catalog.get_table_config.return_value = {
            "dedup_on_read": True, "primary_keys": ["id"]
        }

        result1 = dw._get_table_config("events")
        result2 = dw._get_table_config("events")

        assert result1["dedup_on_read"] is True
        assert result2["dedup_on_read"] is True
        # Only one Redis call — second was cache hit
        mock_catalog.get_table_config.assert_called_once_with("test_org", "test_super", "events")

    def test_cache_miss_redis_returns_none(self):
        dw, mock_catalog = _make_writer()
        mock_catalog.get_table_config.return_value = None

        result = dw._get_table_config("events")

        assert result == {}
        # Cached as empty dict — subsequent calls won't hit Redis
        assert dw._table_config_cache["events"] == {}

    def test_different_tables_independent_cache(self):
        dw, mock_catalog = _make_writer()
        mock_catalog.get_table_config.side_effect = lambda org, sup, simple: (
            {"dedup_on_read": True, "primary_keys": ["id"]}
            if simple == "events" else None
        )

        events_cfg = dw._get_table_config("events")
        orders_cfg = dw._get_table_config("orders")

        assert events_cfg["dedup_on_read"] is True
        assert orders_cfg == {}
        assert mock_catalog.get_table_config.call_count == 2

    def test_configure_then_get_uses_cache(self):
        """configure_table populates cache → _get_table_config returns it without Redis."""
        dw, mock_catalog = _make_writer()
        with patch(f"{_DW_MOD}.check_write_access"):
            dw.configure_table("admin", "events", ["id"], dedup_on_read=True)

        # Reset the mock to prove the next call doesn't use Redis
        mock_catalog.get_table_config.reset_mock()

        result = dw._get_table_config("events")
        assert result["dedup_on_read"] is True
        mock_catalog.get_table_config.assert_not_called()


# ===========================================================================
# 3. write() — __timestamp__ injection
# ===========================================================================

class TestWriteTimestampInjection:
    """The write path injects __timestamp__ when dedup_on_read is enabled."""

    def test_timestamp_injected_when_dedup_enabled(self):
        dw, ctx = _make_writer_for_write()
        try:
            # Enable dedup via cache (simulates prior configure_table call)
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            dw.write("admin", "my_table", arrow, ["x"])

            # Inspect the dataframe passed to process_overlapping_files
            call_args = ctx.process_overlap.call_args
            df_passed = call_args[0][0]  # first positional arg
            assert "__timestamp__" in df_passed.columns
            assert df_passed.shape[0] == 3

            # Verify it's a valid UTC datetime
            ts_val = df_passed["__timestamp__"][0]
            assert isinstance(ts_val, datetime)
        finally:
            ctx.stop_all()

    def test_timestamp_not_injected_when_dedup_disabled(self):
        dw, ctx = _make_writer_for_write()
        try:
            # dedup_on_read is False
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": False,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df_passed.columns
        finally:
            ctx.stop_all()

    def test_timestamp_not_injected_when_no_config(self):
        dw, ctx = _make_writer_for_write()
        try:
            # No config at all (empty cache, Redis returns None)
            arrow = _arrow_table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df_passed.columns
        finally:
            ctx.stop_all()

    def test_user_provided_timestamp_preserved(self):
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            user_ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            arrow = _arrow_table({
                "x": [1, 2],
                "y": ["a", "b"],
                "__timestamp__": [user_ts, user_ts],
            })
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df_passed.columns
            # User's timestamp must be preserved, not overwritten
            ts_values = df_passed["__timestamp__"].to_list()
            assert all(t == user_ts for t in ts_values)
        finally:
            ctx.stop_all()

    def test_timestamp_column_width_increases(self):
        """When __timestamp__ is injected, the dataframe gains one column."""
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1], "y": ["a"]})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert df_passed.shape[1] == 3  # x, y, __timestamp__
        finally:
            ctx.stop_all()

    def test_timestamp_all_rows_same_value(self):
        """All rows in a single write should share the same __timestamp__."""
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1, 2, 3, 4, 5]})
            dw.write("admin", "my_table", arrow, [])

            df_passed = ctx.process_overlap.call_args[0][0]
            ts_values = df_passed["__timestamp__"].to_list()
            assert len(set(ts_values)) == 1, "All rows should have identical timestamp"
        finally:
            ctx.stop_all()


class TestWriteDedupFreshInstance:
    """A fresh DataWriter (no prior configure_table) falls back to Redis."""

    def test_fresh_writer_reads_config_from_redis(self):
        dw, ctx = _make_writer_for_write()
        try:
            # Simulate: table was configured by another process earlier
            ctx.catalog.get_table_config.return_value = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1], "y": ["a"]})
            dw.write("admin", "my_table", arrow, ["x"])

            # Redis was called once for config
            ctx.catalog.get_table_config.assert_called_once_with(
                "test_org", "test_super", "my_table"
            )
            # __timestamp__ injected
            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df_passed.columns
        finally:
            ctx.stop_all()

    def test_second_write_uses_cache(self):
        dw, ctx = _make_writer_for_write()
        try:
            ctx.catalog.get_table_config.return_value = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            arrow = _arrow_table({"x": [1], "y": ["a"]})
            dw.write("admin", "my_table", arrow, ["x"])
            dw.write("admin", "my_table", arrow, ["x"])

            # Only ONE Redis call, second write used cache
            ctx.catalog.get_table_config.assert_called_once()
        finally:
            ctx.stop_all()


# ===========================================================================
# 4. Full integration: configure_table → write → verify
# ===========================================================================

class TestConfigureThenWrite:
    """End-to-end: configure_table followed by write."""

    def test_configure_then_write_injects_timestamp(self):
        dw, ctx = _make_writer_for_write()
        try:
            with patch(f"{_DW_MOD}.check_write_access"):
                dw.configure_table("admin", "my_table", ["x"], dedup_on_read=True)

            arrow = _arrow_table({"x": [1, 2], "y": ["a", "b"]})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df_passed.columns
            assert df_passed.shape[0] == 2

            # No Redis read for config — configure_table populated cache
            ctx.catalog.get_table_config.assert_not_called()
        finally:
            ctx.stop_all()

    def test_configure_false_then_write_no_timestamp(self):
        dw, ctx = _make_writer_for_write()
        try:
            with patch(f"{_DW_MOD}.check_write_access"):
                dw.configure_table("admin", "my_table", ["x"], dedup_on_read=False)

            arrow = _arrow_table({"x": [1, 2], "y": ["a", "b"]})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df_passed.columns
        finally:
            ctx.stop_all()

    def test_reconfigure_toggles_behaviour(self):
        """Configure with dedup=True, write (gets ts), reconfigure dedup=False, write (no ts)."""
        dw, ctx = _make_writer_for_write()
        try:
            with patch(f"{_DW_MOD}.check_write_access"):
                dw.configure_table("admin", "my_table", ["x"], dedup_on_read=True)

            arrow = _arrow_table({"x": [1], "y": ["a"]})
            dw.write("admin", "my_table", arrow, ["x"])
            df1 = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df1.columns

            with patch(f"{_DW_MOD}.check_write_access"):
                dw.configure_table("admin", "my_table", ["x"], dedup_on_read=False)

            dw.write("admin", "my_table", arrow, ["x"])
            df2 = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df2.columns
        finally:
            ctx.stop_all()

    def test_write_different_tables_independent_config(self):
        """Table A has dedup, Table B does not."""
        dw, ctx = _make_writer_for_write()
        try:
            with patch(f"{_DW_MOD}.check_write_access"):
                dw.configure_table("admin", "events", ["id"], dedup_on_read=True)

            # "orders" has no config → no dedup
            ctx.catalog.get_table_config.return_value = None

            arrow = _arrow_table({"id": [1], "val": ["a"]})

            dw.write("admin", "events", arrow, ["id"])
            df_events = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df_events.columns

            dw.write("admin", "orders", arrow, [])
            df_orders = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df_orders.columns
        finally:
            ctx.stop_all()


# ===========================================================================
# 5. RedisCatalog — set_table_config / get_table_config
# ===========================================================================

class TestRedisCatalogTableConfig:
    """Unit tests for the Redis catalog table config methods."""

    def _make_catalog(self):
        with patch(f"{_RC_MOD}.RedisConnector"):
            from supertable.redis_catalog import RedisCatalog
            cat = RedisCatalog()
            cat.r = MagicMock()
            return cat

    def test_set_table_config_calls_redis_set(self):
        cat = self._make_catalog()
        result = cat.set_table_config("org", "sup", "events", {
            "primary_keys": ["id"],
            "dedup_on_read": True,
        })
        assert result is True
        cat.r.set.assert_called_once()

        # Verify key format
        key_arg = cat.r.set.call_args[0][0]
        assert key_arg == "supertable:org:sup:meta:table_config:events"

        # Verify payload is valid JSON with expected fields
        payload = json.loads(cat.r.set.call_args[0][1])
        assert payload["primary_keys"] == ["id"]
        assert payload["dedup_on_read"] is True
        assert "modified_ms" in payload

    def test_set_table_config_empty_org_returns_false(self):
        cat = self._make_catalog()
        assert cat.set_table_config("", "sup", "events", {}) is False
        cat.r.set.assert_not_called()

    def test_set_table_config_empty_simple_returns_false(self):
        cat = self._make_catalog()
        assert cat.set_table_config("org", "sup", "", {}) is False
        cat.r.set.assert_not_called()

    def test_get_table_config_returns_parsed_json(self):
        cat = self._make_catalog()
        stored = json.dumps({
            "primary_keys": ["cust_id", "order_id"],
            "dedup_on_read": True,
            "modified_ms": 1700000000000,
        })
        cat.r.get.return_value = stored

        result = cat.get_table_config("org", "sup", "events")

        assert result["primary_keys"] == ["cust_id", "order_id"]
        assert result["dedup_on_read"] is True
        cat.r.get.assert_called_once_with("supertable:org:sup:meta:table_config:events")

    def test_get_table_config_not_found_returns_none(self):
        cat = self._make_catalog()
        cat.r.get.return_value = None

        result = cat.get_table_config("org", "sup", "events")
        assert result is None

    def test_get_table_config_empty_org_returns_none(self):
        cat = self._make_catalog()
        assert cat.get_table_config("", "sup", "events") is None
        cat.r.get.assert_not_called()

    def test_get_table_config_redis_error_returns_none(self):
        import redis as redis_lib
        cat = self._make_catalog()
        cat.r.get.side_effect = redis_lib.RedisError("connection lost")

        result = cat.get_table_config("org", "sup", "events")
        assert result is None

    def test_set_table_config_redis_error_returns_false(self):
        import redis as redis_lib
        cat = self._make_catalog()
        cat.r.set.side_effect = redis_lib.RedisError("connection lost")

        result = cat.set_table_config("org", "sup", "events", {"dedup_on_read": True})
        assert result is False

    def test_set_then_get_roundtrip(self):
        """Simulate a real set→get roundtrip using in-memory dict as Redis."""
        cat = self._make_catalog()
        store = {}

        def fake_set(key, val):
            store[key] = val

        def fake_get(key):
            return store.get(key)

        cat.r.set = fake_set
        cat.r.get = fake_get

        cat.set_table_config("org", "sup", "events", {
            "primary_keys": ["id"],
            "dedup_on_read": True,
        })

        result = cat.get_table_config("org", "sup", "events")
        assert result["primary_keys"] == ["id"]
        assert result["dedup_on_read"] is True
        assert "modified_ms" in result

    def test_overwrite_replaces_config(self):
        """Second set_table_config fully replaces the first."""
        cat = self._make_catalog()
        store = {}
        cat.r.set = lambda k, v: store.update({k: v})
        cat.r.get = lambda k: store.get(k)

        cat.set_table_config("org", "sup", "events", {
            "primary_keys": ["id"],
            "dedup_on_read": True,
        })
        cat.set_table_config("org", "sup", "events", {
            "primary_keys": ["id", "region"],
            "dedup_on_read": False,
        })

        result = cat.get_table_config("org", "sup", "events")
        assert result["primary_keys"] == ["id", "region"]
        assert result["dedup_on_read"] is False


# ===========================================================================
# 6. Edge cases
# ===========================================================================

class TestDedupEdgeCases:

    def test_write_with_empty_dataframe_and_dedup(self):
        """An empty dataframe with dedup enabled should still get __timestamp__."""
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            # Zero-row arrow table
            arrow = pa.table({"x": pa.array([], type=pa.int64()), "y": pa.array([], type=pa.utf8())})
            dw.write("admin", "my_table", arrow, ["x"])

            df_passed = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df_passed.columns
            assert df_passed.shape[0] == 0
        finally:
            ctx.stop_all()

    def test_multiple_tables_only_configured_one_gets_timestamp(self):
        """Only the table with dedup_on_read=True gets __timestamp__ injected."""
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["events"] = {
                "dedup_on_read": True,
                "primary_keys": ["id"],
            }
            dw._table_config_cache["logs"] = {
                "dedup_on_read": False,
                "primary_keys": ["id"],
            }

            arrow = _arrow_table({"id": [1], "val": ["a"]})

            dw.write("admin", "events", arrow, [])
            df1 = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" in df1.columns

            dw.write("admin", "logs", arrow, [])
            df2 = ctx.process_overlap.call_args[0][0]
            assert "__timestamp__" not in df2.columns
        finally:
            ctx.stop_all()

    def test_dedup_with_delete_only(self):
        """delete_only + dedup should still inject __timestamp__."""
        dw, ctx = _make_writer_for_write()
        try:
            dw._table_config_cache["my_table"] = {
                "dedup_on_read": True,
                "primary_keys": ["x"],
            }

            # Mock delete_only path
            with patch(f"{_DW_MOD}.process_delete_only") as mock_delete:
                mock_delete.return_value = (0, 2, 1, 2, [_resource_entry("del.parquet")], {"old.parquet"})

                arrow = _arrow_table({"x": [1, 2], "y": ["a", "b"]})
                dw.write("admin", "my_table", arrow, ["x"], delete_only=True)

                df_passed = mock_delete.call_args[0][0]
                assert "__timestamp__" in df_passed.columns
        finally:
            ctx.stop_all()

    def test_config_key_uses_correct_redis_key_pattern(self):
        from supertable.redis_catalog import _table_config_key
        key = _table_config_key("acme", "warehouse", "events")
        assert key == "supertable:acme:warehouse:meta:table_config:events"

    def test_init_has_empty_cache(self):
        dw, _ = _make_writer()
        assert dw._table_config_cache == {}
