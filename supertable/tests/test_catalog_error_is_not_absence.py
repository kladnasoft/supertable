"""Regression tests for AUDIT_BUGS C3 — a failed check is not a negative result.

``RedisCatalog._leaf_exists_raw`` caught ``RedisError`` and returned ``False``,
so "I could not check" arrived at the caller as "it does not exist".  The write
path then built ``SimpleTable(...)`` with the default ``create_if_missing=True``
and bootstrapped a fresh empty snapshot over the live table — 6 rows to 1,
version 3 to 1, the old files orphaned, and ``write()`` returning success.
Sentinel's 0.5 s socket timeout puts this within reach of ordinary load.

Both halves of the fix are covered here: the catalog must propagate the error,
and the write path must not treat "no leaf" as an invitation to invent a table.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import redis

from supertable.errors import TableNotFoundError
from supertable.storage.tests.fake_object_store import FakeObjectStore


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _catalog(redis_client):
    """A RedisCatalog wired to *redis_client*, skipping connection setup."""
    from supertable.redis_catalog import RedisCatalog

    cat = RedisCatalog.__new__(RedisCatalog)
    cat.r = redis_client
    return cat


def _flaky_redis(error=None):
    """A Redis client whose ``exists`` fails the way a Sentinel timeout does."""
    client = MagicMock(name="redis")
    client.exists.side_effect = error or redis.exceptions.TimeoutError(
        "Timeout reading from socket"
    )
    # get_root() -> replica resolution; unrelated, and must not mask the error.
    client.get.return_value = None
    return client


# ===========================================================================
# Half 1: the catalog must not answer a question it could not ask
# ===========================================================================

class TestCatalogPropagatesRedisErrors:

    def test_leaf_exists_raw_raises_instead_of_returning_false(self):
        cat = _catalog(_flaky_redis())
        with pytest.raises(redis.RedisError):
            cat._leaf_exists_raw("org", "sup", "events")

    def test_leaf_exists_raises_through_the_public_entry_point(self):
        cat = _catalog(_flaky_redis())
        with pytest.raises(redis.RedisError):
            cat.leaf_exists("org", "sup", "events")

    def test_connection_error_propagates_too(self):
        cat = _catalog(_flaky_redis(redis.exceptions.ConnectionError("down")))
        with pytest.raises(redis.RedisError):
            cat.leaf_exists("org", "sup", "events")

    def test_root_exists_raises_instead_of_returning_false(self):
        """The supertable-level twin: ``False`` here re-runs lake bootstrap."""
        cat = _catalog(_flaky_redis())
        with pytest.raises(redis.RedisError):
            cat.root_exists("org", "sup")

    def test_a_healthy_redis_still_answers_normally(self):
        client = MagicMock(name="redis")
        client.get.return_value = None
        client.exists.return_value = 1
        assert _catalog(client).leaf_exists("org", "sup", "events") is True
        client.exists.return_value = 0
        assert _catalog(client).leaf_exists("org", "sup", "events") is False


# ===========================================================================
# Half 1 (consequence): a live table is never re-created
# ===========================================================================

class TestSimpleTableDoesNotBootstrapOnCatalogError:

    def _super_table(self, storage):
        st = MagicMock()
        st.organization = "org"
        st.super_name = "sup"
        st.storage = storage
        return st

    @patch("supertable.simple_table.RedisCatalog")
    def test_transient_redis_error_does_not_overwrite_a_live_table(self, MockCat):
        """The reproduction, at its narrowest: the leaf check fails while the
        table is alive, and the constructor must not mint a v0 snapshot."""
        from supertable.simple_table import SimpleTable

        live_snapshot = "org/sup/tables/events/snapshots/v3.json"
        storage = FakeObjectStore().seed(
            live_snapshot,
            "org/sup/tables/events/data/part-0.parquet",
        )
        catalog = _catalog(_flaky_redis())
        MockCat.return_value = catalog

        with pytest.raises(redis.RedisError):
            # Default create_if_missing=True — exactly how the write path
            # used to call it.
            SimpleTable(self._super_table(storage), "events")

        assert storage.keys() == [
            "org/sup/tables/events/data/part-0.parquet",
            live_snapshot,
        ], "no bootstrap snapshot may be written when the leaf check failed"

    @patch("supertable.simple_table.RedisCatalog")
    def test_a_genuinely_missing_table_is_still_created(self, MockCat):
        """The fix must not break first-write-creates: a catalog that
        positively reports "no leaf" may still bootstrap."""
        from supertable.simple_table import SimpleTable

        storage = FakeObjectStore()
        client = MagicMock(name="redis")
        client.get.return_value = None
        client.exists.return_value = 0  # a real answer, not an error
        catalog = _catalog(client)
        catalog.set_leaf_payload_cas = MagicMock(return_value=0)
        MockCat.return_value = catalog

        SimpleTable(self._super_table(storage), "events")

        assert len(storage.keys()) == 1
        assert storage.keys()[0].startswith("org/sup/tables/events/snapshots/")
        catalog.set_leaf_payload_cas.assert_called_once()


# ===========================================================================
# Half 2: the write path opens, it does not bootstrap
# ===========================================================================

class TestWritePathRefusesToInventTables:

    def _writer(self):
        """A DataWriter with just enough wiring to reach the SimpleTable call."""
        from supertable.data_writer import DataWriter

        dw = DataWriter.__new__(DataWriter)
        st = MagicMock()
        st.organization = "org"
        st.super_name = "sup"
        st.storage = FakeObjectStore()
        dw.super_table = st
        dw.catalog = MagicMock()
        dw.catalog.acquire_simple_lock.return_value = "token"
        dw.catalog.reserve_rowids.return_value = 0
        dw.catalog.get_table_config.return_value = None
        dw._table_config_cache = {}
        dw.timer = MagicMock()
        return dw

    def test_write_opens_with_create_if_missing_false(self):
        """Creating a table must be a decision, never a fallback."""
        import pyarrow as pa
        from supertable.data_writer import DataWriter

        dw = self._writer()
        with (
            patch("supertable.data_writer.check_write_access"),
            patch("supertable.data_writer.SimpleTable") as MockSimple,
        ):
            # Abort right after construction — the call args are the assertion.
            MockSimple.return_value.get_simple_table_snapshot.side_effect = \
                RuntimeError("stop here")
            with pytest.raises(RuntimeError, match="stop here"):
                dw.write("admin", "t1", pa.table({"id": [1]}), overwrite_columns=[])

        assert MockSimple.call_count == 1
        assert MockSimple.call_args.kwargs.get("create_if_missing") is False

    def test_missing_table_falls_back_to_explicit_creation(self):
        """First write to a new name still creates it — but only after the
        catalog positively reported the leaf absent."""
        import pyarrow as pa

        dw = self._writer()
        with (
            patch("supertable.data_writer.check_write_access"),
            patch("supertable.data_writer.SimpleTable") as MockSimple,
        ):
            created = MagicMock()
            created.get_simple_table_snapshot.side_effect = RuntimeError("stop here")
            MockSimple.side_effect = [TableNotFoundError("org", "sup", "t1"), created]

            with pytest.raises(RuntimeError, match="stop here"):
                dw.write("admin", "t1", pa.table({"id": [1]}), overwrite_columns=[])

        assert MockSimple.call_count == 2
        assert MockSimple.call_args_list[0].kwargs.get("create_if_missing") is False
        assert MockSimple.call_args_list[1].kwargs.get("create_if_missing") is True

    def test_catalog_error_aborts_the_write_rather_than_creating(self):
        """A RedisError from the leaf check must reach the caller, not be
        converted into a brand-new empty table."""
        import pyarrow as pa

        dw = self._writer()
        with (
            patch("supertable.data_writer.check_write_access"),
            patch("supertable.data_writer.SimpleTable") as MockSimple,
        ):
            MockSimple.side_effect = redis.exceptions.TimeoutError("socket timeout")
            with pytest.raises(redis.RedisError):
                dw.write("admin", "t1", pa.table({"id": [1]}), overwrite_columns=[])

        # Only the create_if_missing=False attempt was made; no retry that
        # would have created the table.
        assert MockSimple.call_count == 1
        assert MockSimple.call_args.kwargs.get("create_if_missing") is False
